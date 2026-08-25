require("dotenv").config();

const express = require("express");
const mongoose = require("mongoose");
const { Telegraf } = require("telegraf");
const {
  Connection,
  Keypair,
  PublicKey,
  LAMPORTS_PER_SOL
} = require("@solana/web3.js");
const bip39 = require("bip39");
const { derivePath } = require("ed25519-hd-key");
const bs58 = require("bs58");

const app = express();
app.use(express.json());

const PORT = Number(process.env.PORT) || 10000;
const RPC_URL =
  process.env.RPC_URL ||
  "https://api.mainnet-beta.solana.com";

const MONGODB_URI = process.env.MONGODB_URI;
const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const BOT_PRIVATE_KEY = process.env.BOT_PRIVATE_KEY;

const connection = new Connection(RPC_URL, "confirmed");

const TOKEN_PROGRAM_ID = new PublicKey(
  "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
);

const TOKEN_2022_PROGRAM_ID = new PublicKey(
  "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
);

const MODE = "PAPER";
const LIVE_TRADING = false;

let wallet = null;
let bot = null;
let server = null;
let tokenSub = null;
let token2022Sub = null;

const processing = new Set();
const dexRecheckTimers = new Map();

// ======================================================
// STATE
// ======================================================

const state = {
  server: "starting",
  database: "disconnected",
  wallet: "not_loaded",
  solana: "disconnected",
  telegram: "stopped",
  hunter: "stopped",
  security: "idle",
  dex: "idle",

  detected: 0,
  securityScanned: 0,
  dexScanned: 0,
  candidates: 0,

  rpcQueued: 0,
  rpc429: 0,
  rpcRetries: 0,
  errors: 0,

  lastMint: null
};

// ======================================================
// HELPERS
// ======================================================

function log(...x) {
  console.log(new Date().toISOString(), ...x);
}

function errLog(name, err) {
  state.errors++;

  console.error(
    new Date().toISOString(),
    "❌",
    name,
    err?.message || err
  );
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function is429(err) {
  const text =
    `${err?.message || ""} ${JSON.stringify(err || {})}`;

  return (
    text.includes("429") ||
    text.toLowerCase().includes("rate limit")
  );
}

// ======================================================
// RPC QUEUE
// ======================================================

const rpcQueue = [];
let rpcWorkerRunning = false;

let rpcDelayMs = 450;
const RPC_MIN_DELAY = 350;
const RPC_MAX_DELAY = 5000;

function rpcRequest(task) {
  return new Promise((resolve, reject) => {
    rpcQueue.push({
      task,
      resolve,
      reject
    });

    state.rpcQueued =
      rpcQueue.length;

    runRpcWorker().catch(err =>
      errLog("RPC Worker", err)
    );
  });
}

async function runRpcWorker() {
  if (rpcWorkerRunning) return;

  rpcWorkerRunning = true;

  try {
    while (rpcQueue.length) {
      const item =
        rpcQueue.shift();

      state.rpcQueued =
        rpcQueue.length;

      let success = false;
      let lastError = null;

      for (let attempt = 1; attempt <= 5; attempt++) {
        try {
          const result =
            await item.task();

          success = true;

          if (
            rpcDelayMs >
            RPC_MIN_DELAY
          ) {
            rpcDelayMs =
              Math.max(
                RPC_MIN_DELAY,
                rpcDelayMs - 50
              );
          }

          item.resolve(result);
          break;
        } catch (err) {
          lastError = err;

          if (!is429(err)) {
            item.reject(err);
            break;
          }

          state.rpc429++;
          state.rpcRetries++;

          rpcDelayMs =
            Math.min(
              RPC_MAX_DELAY,
              Math.max(
                rpcDelayMs * 2,
                1000
              )
            );

          log(
            `⚠️ RPC 429 | retry ${attempt}/5 | delay ${rpcDelayMs}ms`
          );

          await sleep(
            rpcDelayMs
          );
        }
      }

      if (!success && lastError) {
        item.reject(lastError);
      }

      await sleep(
        rpcDelayMs
      );
    }
  } finally {
    rpcWorkerRunning = false;
  }
}

// ======================================================
// SIMPLE CACHE
// ======================================================

const accountCache = new Map();

function getCache(key) {
  const entry =
    accountCache.get(key);

  if (!entry) return null;

  if (
    Date.now() >
    entry.expiresAt
  ) {
    accountCache.delete(key);
    return null;
  }

  return entry.value;
}

function setCache(
  key,
  value,
  ttlMs = 30000
) {
  accountCache.set(
    key,
    {
      value,
      expiresAt:
        Date.now() + ttlMs
    }
  );
}

// ======================================================
// DATABASE
// ======================================================

const tokenSchema =
  new mongoose.Schema(
    {
      mint: {
        type: String,
        unique: true,
        index: true
      },

      signature: String,
      program: String,

      detectedAt: {
        type: Date,
        default: Date.now
      },

      securityChecked: {
        type: Boolean,
        default: false
      },

      securityScore: Number,

      securityDecision: {
        type: String,
        default: "PENDING"
      },

      mintAuthorityRevoked: Boolean,
      freezeAuthorityRevoked: Boolean,
      decimals: Number,
      supply: String,
      token2022: Boolean,

      dexChecked: {
        type: Boolean,
        default: false
      },

      dexListed: {
        type: Boolean,
        default: false
      },

      dexAttempts: {
        type: Number,
        default: 0
      },

      dexId: String,
      pairAddress: String,

      liquidityUsd: Number,
      volumeM5: Number,
      volumeH1: Number,
      buysM5: Number,
      sellsM5: Number,

      dexScore: Number,

      dexDecision: {
        type: String,
        default: "PENDING"
      },

      finalScore: Number,

      finalDecision: {
        type: String,
        default: "PENDING"
      },

      paperOnly: {
        type: Boolean,
        default: true
      }
    },
    {
      timestamps: true
    }
  );

const FreshToken =
  mongoose.models.FreshToken ||
  mongoose.model(
    "FreshToken",
    tokenSchema
  );

// ======================================================
// HTTP
// ======================================================

app.get("/", (req, res) => {
  res.send(
    "✅ LOMY V4.2 ONLINE | PAPER MODE"
  );
});

app.get("/health", (req, res) => {
  res.json({
    ...state,
    rpcDelayMs,
    mode: MODE,
    liveTrading: LIVE_TRADING,
    uptime:
      Math.floor(
        process.uptime()
      )
  });
});

function startServer() {
  return new Promise(
    (resolve, reject) => {
      server =
        app.listen(
          PORT,
          "0.0.0.0",
          () => {
            state.server =
              "online";

            log(
              "✅ Render server online",
              PORT
            );

            resolve();
          }
        );

      server.on(
        "error",
        reject
      );
    }
  );
}

// ======================================================
// MONGODB
// ======================================================

async function connectDatabase() {
  try {
    if (!MONGODB_URI) {
      throw new Error(
        "MONGODB_URI missing"
      );
    }

    state.database =
      "connecting";

    await mongoose.connect(
      MONGODB_URI,
      {
        serverSelectionTimeoutMS:
          10000
      }
    );

    state.database =
      "connected";

    log(
      "✅ MongoDB connected"
    );
  } catch (err) {
    state.database =
      "error";

    errLog(
      "MongoDB",
      err
    );
  }
}

mongoose.connection.on(
  "disconnected",
  () => {
    state.database =
      "disconnected";
  }
);

mongoose.connection.on(
  "reconnected",
  () => {
    state.database =
      "connected";
  }
);

// ======================================================
// WALLET
// ======================================================

async function loadWallet() {
  try {
    if (!BOT_PRIVATE_KEY) {
      throw new Error(
        "BOT_PRIVATE_KEY missing"
      );
    }

    const key =
      BOT_PRIVATE_KEY.trim();

    if (
      key.includes(" ") &&
      bip39.validateMnemonic(key)
    ) {
      const seed =
        bip39.mnemonicToSeedSync(
          key
        );

      const derived =
        derivePath(
          "m/44'/501'/0'/0'",
          seed.toString("hex")
        ).key;

      wallet =
        Keypair.fromSeed(
          derived
        );
    } else if (
      key.startsWith("[")
    ) {
      wallet =
        Keypair.fromSecretKey(
          Uint8Array.from(
            JSON.parse(key)
          )
        );
    } else {
      wallet =
        Keypair.fromSecretKey(
          bs58.decode(key)
        );
    }

    state.wallet =
      "loaded";

    log(
      "✅ Wallet",
      wallet.publicKey.toString()
    );
  } catch (err) {
    state.wallet =
      "error";

    errLog(
      "Wallet",
      err
    );
  }
}

// ======================================================
// SOLANA TEST
// ======================================================

async function testSolana() {
  if (!wallet) return;

  try {
    const balance =
      await rpcRequest(
        () =>
          connection.getBalance(
            wallet.publicKey
          )
      );

    state.solana =
      "connected";

    log(
      "✅ Solana",
      (
        balance /
        LAMPORTS_PER_SOL
      ).toFixed(6),
      "SOL"
    );
  } catch (err) {
    state.solana =
      "error";

    errLog(
      "Solana",
      err
    );
  }
}

// ======================================================
// SECURITY
// ======================================================

async function securityScan(mint) {
  state.security =
    "scanning";

  try {
    const cached =
      getCache(
        `mint:${mint}`
      );

    let account;

    if (cached) {
      account =
        cached;
    } else {
      account =
        await rpcRequest(
          () =>
            connection
              .getParsedAccountInfo(
                new PublicKey(
                  mint
                ),
                "confirmed"
              )
        );

      setCache(
        `mint:${mint}`,
        account,
        60000
      );
    }

    if (!account.value) {
      throw new Error(
        "Mint unavailable"
      );
    }

    const owner =
      account.value.owner.toString();

    const parsed =
      account.value.data?.parsed;

    if (
      !parsed ||
      parsed.type !== "mint"
    ) {
      throw new Error(
        "Invalid mint"
      );
    }

    const info =
      parsed.info || {};

    const mintRevoked =
      info.mintAuthority == null;

    const freezeRevoked =
      info.freezeAuthority == null;

    const token2022 =
      owner ===
      TOKEN_2022_PROGRAM_ID
        .toString();

    const decimals =
      num(info.decimals);

    const supply =
      String(
        info.supply || "0"
      );

    let score = 50;

    score +=
      mintRevoked
        ? 20
        : -20;

    score +=
      freezeRevoked
        ? 20
        : -25;

    score +=
      decimals >= 0 &&
      decimals <= 18
        ? 5
        : -10;

    score +=
      supply !== "0"
        ? 5
        : -15;

    if (token2022) {
      score -= 5;
    }

    score =
      Math.max(
        0,
        Math.min(
          100,
          score
        )
      );

    let decision =
      "REJECT";

    if (score >= 80) {
      decision =
        "PASS";
    } else if (
      score >= 55
    ) {
      decision =
        "REVIEW";
    }

    await FreshToken.updateOne(
      { mint },
      {
        $set: {
          securityChecked:
            true,

          securityScore:
            score,

          securityDecision:
            decision,

          mintAuthorityRevoked:
            mintRevoked,

          freezeAuthorityRevoked:
            freezeRevoked,

          decimals,
          supply,
          token2022
        }
      }
    );

    state.securityScanned++;

    log(
      `🛡 ${score}/100 ${decision} ${mint}`
    );

    if (
      decision !==
      "REJECT"
    ) {
      await dexScan(
        mint
      );
    }
  } catch (err) {
    errLog(
      `Security ${mint}`,
      err
    );
  } finally {
    state.security =
      "idle";
  }
}

// ======================================================
// DEX
// ======================================================

async function fetchPairs(mint) {
  const controller =
    new AbortController();

  const timeout =
    setTimeout(
      () =>
        controller.abort(),
      8000
    );

  try {
    const response =
      await fetch(
        `https://api.dexscreener.com/token-pairs/v1/solana/${encodeURIComponent(mint)}`,
        {
          headers: {
            Accept:
              "application/json"
          },
          signal:
            controller.signal
        }
      );

    if (!response.ok) {
      throw new Error(
        `DEX HTTP ${response.status}`
      );
    }

    const data =
      await response.json();

    return Array.isArray(data)
      ? data
      : [];
  } finally {
    clearTimeout(
      timeout
    );
  }
}

function bestPool(pairs) {
  return [...pairs]
    .filter(
      p =>
        p?.chainId ===
        "solana"
    )
    .sort(
      (a, b) =>
        num(
          b?.liquidity?.usd
        ) -
        num(
          a?.liquidity?.usd
        )
    )[0] || null;
}

function scheduleDexRecheck(
  mint
) {
  if (
    dexRecheckTimers.has(
      mint
    )
  ) {
    return;
  }

  const timer =
    setTimeout(
      async () => {
        dexRecheckTimers.delete(
          mint
        );

        try {
          await dexScan(
            mint
          );
        } catch (err) {
          errLog(
            "DEX recheck",
            err
          );
        }
      },
      30000
    );

  dexRecheckTimers.set(
    mint,
    timer
  );
}

async function dexScan(mint) {
  state.dex =
    "scanning";

  try {
    const token =
      await FreshToken
        .findOne({
          mint
        })
        .lean();

    if (!token) return;

    const attempts =
      num(
        token.dexAttempts
      ) + 1;

    const pairs =
      await fetchPairs(
        mint
      );

    const pair =
      bestPool(
        pairs
      );

    state.dexScanned++;

    if (!pair) {
      await FreshToken.updateOne(
        { mint },
        {
          $set: {
            dexChecked:
              false,

            dexListed:
              false,

            dexAttempts:
              attempts,

            dexDecision:
              "NO_POOL",

            finalDecision:
              "WAITING_DEX"
          }
        }
      );

      log(
        `⏳ No pool ${mint} attempt ${attempts}`
      );

      if (
        attempts < 5
      ) {
        scheduleDexRecheck(
          mint
        );
      }

      return;
    }

    const liquidity =
      num(
        pair?.liquidity?.usd
      );

    const volumeM5 =
      num(
        pair?.volume?.m5
      );

    const volumeH1 =
      num(
        pair?.volume?.h1
      );

    const buys =
      num(
        pair?.txns?.m5?.buys
      );

    const sells =
      num(
        pair?.txns?.m5?.sells
      );

    let dexScore = 0;

    if (
      liquidity >=
      10000
    ) {
      dexScore += 40;
    } else if (
      liquidity >=
      3000
    ) {
      dexScore += 25;
    }

    if (
      volumeM5 >=
      250
    ) {
      dexScore += 20;
    }

    if (
      volumeH1 > 0
    ) {
      dexScore += 10;
    }

    if (
      buys + sells >=
      5
    ) {
      dexScore += 15;
    }

    if (
      buys > 0 &&
      sells > 0
    ) {
      dexScore += 15;
    }

    dexScore =
      Math.min(
        100,
        dexScore
      );

    let dexDecision =
      "WATCH";

    if (
      liquidity >= 3000 &&
      dexScore >= 60 &&
      sells > 0
    ) {
      dexDecision =
        "PASS";
    }

    const securityScore =
      num(
        token.securityScore
      );

    const finalScore =
      Math.round(
        securityScore *
          0.6 +
        dexScore *
          0.4
      );

    let finalDecision =
      "WATCH";

    if (
      token.securityDecision ===
        "PASS" &&
      dexDecision ===
        "PASS" &&
      finalScore >= 70
    ) {
      finalDecision =
        "CANDIDATE";
    }

    await FreshToken.updateOne(
      { mint },
      {
        $set: {
          dexChecked:
            true,

          dexListed:
            true,

          dexAttempts:
            attempts,

          dexId:
            pair.dexId ||
            null,

          pairAddress:
            pair.pairAddress ||
            null,

          liquidityUsd:
            liquidity,

          volumeM5,
          volumeH1,
          buysM5:
            buys,
          sellsM5:
            sells,

          dexScore,
          dexDecision,
          finalScore,
          finalDecision
        }
      }
    );

    if (
      finalDecision ===
      "CANDIDATE"
    ) {
      state.candidates++;
    }

    log(
      `💧 ${mint} LIQ $${liquidity.toFixed(0)} FINAL ${finalScore} ${finalDecision}`
    );
  } catch (err) {
    errLog(
      `DEX ${mint}`,
      err
    );
  } finally {
    state.dex =
      "idle";
  }
}

// ======================================================
// HUNTER
// ======================================================

function findMint(
  instructions
) {
  if (
    !Array.isArray(
      instructions
    )
  ) {
    return null;
  }

  for (
    const ix
    of instructions
  ) {
    const type =
      ix?.parsed?.type;

    if (
      type ===
        "initializeMint" ||
      type ===
        "initializeMint2"
    ) {
      return (
        ix?.parsed?.info
          ?.mint ||
        null
      );
    }
  }

  return null;
}

async function processLogs(
  event,
  program
) {
  if (
    !event ||
    event.err
  ) {
    return;
  }

  const relevant =
    (event.logs || [])
      .some(
        line =>
          line.includes(
            "Instruction: InitializeMint"
          ) ||
          line.includes(
            "Instruction: InitializeMint2"
          )
      );

  if (!relevant) return;

  const signature =
    event.signature;

  if (
    !signature ||
    processing.has(
      signature
    )
  ) {
    return;
  }

  processing.add(
    signature
  );

  try {
    let tx = null;

    for (
      let i = 0;
      i < 4;
      i++
    ) {
      tx =
        await rpcRequest(
          () =>
            connection
              .getParsedTransaction(
                signature,
                {
                  commitment:
                    "confirmed",

                  maxSupportedTransactionVersion:
                    0
                }
              )
        )
        .catch(
          () => null
        );

      if (tx) break;

      await sleep(
        500 +
        i * 500
      );
    }

    if (!tx) return;

    let mint =
      findMint(
        tx.transaction
          .message
          .instructions
      );

    if (
      !mint &&
      tx.meta
        ?.innerInstructions
    ) {
      for (
        const group
        of tx.meta
          .innerInstructions
      ) {
        mint =
          findMint(
            group.instructions
          );

        if (mint) break;
      }
    }

    if (!mint) return;

    state.detected++;
    state.lastMint =
      mint;

    let token =
      await FreshToken
        .findOne({
          mint
        })
        .lean();

    if (!token) {
      await FreshToken.create(
        {
          mint,
          signature,
          program,
          paperOnly:
            true
        }
      );

      log(
        "🆕 Fresh Mint",
        mint
      );

      token =
        await FreshToken
          .findOne({
            mint
          })
          .lean();
    }

    if (
      !token
        .securityChecked
    ) {
      await securityScan(
        mint
      );
    }
  } catch (err) {
    errLog(
      "Hunter processing",
      err
    );
  } finally {
    processing.delete(
      signature
    );
  }
}

function startHunter() {
  tokenSub =
    connection.onLogs(
      TOKEN_PROGRAM_ID,
      event => {
        processLogs(
          event,
          "SPL_TOKEN"
        ).catch(
          err =>
            errLog(
              "SPL Hunter",
              err
            )
        );
      },
      "confirmed"
    );

  token2022Sub =
    connection.onLogs(
      TOKEN_2022_PROGRAM_ID,
      event => {
        processLogs(
          event,
          "TOKEN_2022"
        ).catch(
          err =>
            errLog(
              "Token2022 Hunter",
              err
            )
        );
      },
      "confirmed"
    );

  state.hunter =
    "running";

  log(
    "🔎 Hunter running"
  );
}

// ======================================================
// RECOVERY
// ======================================================

async function recoverPending() {
  if (
    mongoose.connection
      .readyState !== 1
  ) {
    return;
  }

  try {
    const securityPending =
      await FreshToken
        .find({
          securityChecked:
            false
        })
        .sort({
          detectedAt:
            -1
        })
        .limit(20)
        .select("mint")
        .lean();

    log(
      `♻️ Security recovery ${securityPending.length}`
    );

    for (
      const token
      of securityPending
    ) {
      await securityScan(
        token.mint
      );

      await sleep(
        1000
      );
    }

    const dexPending =
      await FreshToken
        .find({
          securityDecision: {
            $in: [
              "PASS",
              "REVIEW"
            ]
          },

          dexChecked: {
            $ne: true
          }
        })
        .sort({
          detectedAt:
            -1
        })
        .limit(50)
        .select("mint")
        .lean();

    log(
      `💧 DEX recovery ${dexPending.length}`
    );

    for (
      const token
      of dexPending
    ) {
      await dexScan(
        token.mint
      );

      await sleep(
        1000
      );
    }

    log(
      "✅ Recovery completed"
    );
  } catch (err) {
    errLog(
      "Recovery",
      err
    );
  }
}

// ======================================================
// TELEGRAM
// ======================================================

async function startTelegram() {
  if (!TELEGRAM_TOKEN) {
    state.telegram =
      "missing_config";
    return;
  }

  try {
    bot =
      new Telegraf(
        TELEGRAM_TOKEN
      );

    bot.start(
      ctx =>
        ctx.reply(
          "🤖 LOMY V4.2\n\n" +
          "🔎 Hunter ON\n" +
          "🛡 Security ON\n" +
          "💧 DEX ON\n" +
          "🚦 RPC Limiter ON\n" +
          "🧪 PAPER MODE\n" +
          "🔒 NO BUY / NO SELL"
        )
    );

    bot.command(
      "status",
      ctx =>
        ctx.reply(
          `🤖 LOMY V4.2 STATUS\n\n` +
          `🌐 Server: ${state.server}\n` +
          `🗄 Database: ${state.database}\n` +
          `👛 Wallet: ${state.wallet}\n` +
          `⚡ Solana: ${state.solana}\n` +
          `📡 Telegram: ${state.telegram}\n` +
          `🔎 Hunter: ${state.hunter}\n` +
          `🛡 Security: ${state.security}\n` +
          `💧 DEX: ${state.dex}\n\n` +
          `🚦 RPC Queue: ${state.rpcQueued}\n` +
          `⚠️ RPC 429: ${state.rpc429}\n` +
          `🔁 RPC Retries: ${state.rpcRetries}\n` +
          `⏱ RPC Delay: ${rpcDelayMs}ms\n\n` +
          `🧪 PAPER MODE\n` +
          `🔒 LIVE TRADING OFF`
        )
    );

    bot.command(
      "balance",
      async ctx => {
        try {
          const balance =
            await rpcRequest(
              () =>
                connection
                  .getBalance(
                    wallet.publicKey
                  )
            );

          await ctx.reply(
            `💰 الرصيد: ${(balance / LAMPORTS_PER_SOL).toFixed(6)} SOL`
          );
        } catch {
          await ctx.reply(
            "❌ Balance error"
          );
        }
      }
    );

    bot.command(
      "rpc",
      ctx =>
        ctx.reply(
          `🚦 RPC STATUS\n\n` +
          `Queue: ${state.rpcQueued}\n` +
          `429 Errors: ${state.rpc429}\n` +
          `Retries: ${state.rpcRetries}\n` +
          `Current Delay: ${rpcDelayMs}ms\n\n` +
          `✅ Adaptive limiter active`
        )
    );

    bot.command(
      "security",
      async ctx => {
        const last =
          await FreshToken
            .findOne({
              securityChecked:
                true
            })
            .sort({
              updatedAt:
                -1
            })
            .lean();

        if (!last) {
          return ctx.reply(
            "🛡 No security scans yet"
          );
        }

        await ctx.reply(
          `🛡 SECURITY\n\n` +
          `Scanned this run: ${state.securityScanned}\n\n` +
          `Mint:\n${last.mint}\n\n` +
          `Score: ${last.securityScore}/100\n` +
          `Decision: ${last.securityDecision}\n` +
          `Mint Authority: ${
            last.mintAuthorityRevoked
              ? "REVOKED ✅"
              : "ACTIVE ⚠️"
          }\n` +
          `Freeze Authority: ${
            last.freezeAuthorityRevoked
              ? "REVOKED ✅"
              : "ACTIVE ⚠️"
          }\n` +
          `Token-2022: ${
            last.token2022
              ? "YES"
              : "NO"
          }`
        );
      }
    );

    bot.command(
      "dex",
      async ctx => {
        const last =
          await FreshToken
            .findOne({
              dexListed:
                true
            })
            .sort({
              updatedAt:
                -1
            })
            .lean();

        if (!last) {
          return ctx.reply(
            "💧 No listed DEX pools yet"
          );
        }

        await ctx.reply(
          `💧 DEX / LIQUIDITY\n\n` +
          `Mint:\n${last.mint}\n\n` +
          `DEX: ${last.dexId || "Unknown"}\n` +
          `Liquidity: $${num(last.liquidityUsd).toFixed(2)}\n` +
          `Volume M5: $${num(last.volumeM5).toFixed(2)}\n` +
          `Buys: ${last.buysM5 || 0}\n` +
          `Sells: ${last.sellsM5 || 0}\n` +
          `DEX Score: ${last.dexScore}/100\n` +
          `Final Score: ${last.finalScore}/100\n` +
          `Final: ${last.finalDecision}\n\n` +
          `🔒 PAPER ONLY`
        );
      }
    );

    bot.command(
      "stats",
      async ctx => {
        const [
          total,
          pass,
          listed,
          candidates,
          waiting
        ] =
        await Promise.all(
          [
            FreshToken
              .countDocuments(),

            FreshToken
              .countDocuments({
                securityDecision:
                  "PASS"
              }),

            FreshToken
              .countDocuments({
                dexListed:
                  true
              }),

            FreshToken
              .countDocuments({
                finalDecision:
                  "CANDIDATE"
              }),

            FreshToken
              .countDocuments({
                finalDecision:
                  "WAITING_DEX"
              })
          ]
        );

        await ctx.reply(
          `📊 V4.2 STATS\n\n` +
          `Total: ${total}\n` +
          `Security PASS: ${pass} ✅\n` +
          `DEX Listed: ${listed} 💧\n` +
          `Waiting DEX: ${waiting} ⏳\n` +
          `Candidates: ${candidates} 🎯\n` +
          `Errors: ${state.errors}\n\n` +
          `RPC 429: ${state.rpc429}\n` +
          `RPC Retries: ${state.rpcRetries}\n\n` +
          `🧪 PAPER MODE`
        );
      }
    );

    bot.command(
      "candidates",
      async ctx => {
        const tokens =
          await FreshToken
            .find({
              finalDecision:
                "CANDIDATE"
            })
            .sort({
              finalScore:
                -1,
              detectedAt:
                -1
            })
            .limit(5)
            .lean();

        if (
          !tokens.length
        ) {
          return ctx.reply(
            "🎯 لا توجد Candidates حتى الآن"
          );
        }

        let text =
          "🎯 TOP CANDIDATES\n\n";

        tokens.forEach(
          (t, i) => {
            text +=
              `${i + 1}) ${t.mint}\n` +
              `Final: ${t.finalScore}/100\n` +
              `Security: ${t.securityScore}/100\n` +
              `Liquidity: $${num(t.liquidityUsd).toFixed(0)}\n\n`;
          }
        );

        text +=
          "🔒 PAPER ONLY";

        await ctx.reply(
          text
        );
      }
    );

    bot.catch(
      err =>
        errLog(
          "Telegram",
          err
        )
    );

    state.telegram =
      "starting";

    bot.launch()
      .catch(
        err => {
          state.telegram =
            "error";

          errLog(
            "Telegram launch",
            err
          );
        }
      );

    state.telegram =
      "online";

    log(
      "✅ Telegram online"
    );
  } catch (err) {
    state.telegram =
      "error";

    errLog(
      "Telegram start",
      err
    );
  }
}

// ======================================================
// SHUTDOWN
// ======================================================

async function shutdown(
  signal
) {
  log(
    `⚠️ ${signal} shutdown`
  );

  for (
    const timer
    of dexRecheckTimers.values()
  ) {
    clearTimeout(
      timer
    );
  }

  try {
    if (
      tokenSub !== null
    ) {
      await connection
        .removeOnLogsListener(
          tokenSub
        );
    }

    if (
      token2022Sub !==
      null
    ) {
      await connection
        .removeOnLogsListener(
          token2022Sub
        );
    }
  } catch {}

  try {
    if (bot) {
      bot.stop(
        signal
      );
    }
  } catch {}

  try {
    await mongoose
      .disconnect();
  } catch {}

  if (server) {
    server.close(
      () =>
        process.exit(0)
    );

    setTimeout(
      () =>
        process.exit(0),
      5000
    );
  } else {
    process.exit(0);
  }
}

process.on(
  "unhandledRejection",
  reason => {
    errLog(
      "Unhandled rejection",
      reason instanceof Error
        ? reason
        : new Error(
            String(reason)
          )
    );
  }
);

process.on(
  "uncaughtException",
  err => {
    errLog(
      "Uncaught exception",
      err
    );

    setTimeout(
      () =>
        process.exit(1),
      1000
    );
  }
);

process.once(
  "SIGTERM",
  () =>
    shutdown(
      "SIGTERM"
    )
);

process.once(
  "SIGINT",
  () =>
    shutdown(
      "SIGINT"
    )
);

// ======================================================
// MAIN
// ======================================================

async function main() {
  console.log("");
  console.log(
    "============================"
  );
  console.log(
    "🚀 LOMY SOLANA HUNTER V4.2"
  );
  console.log(
    "🚦 ADAPTIVE RPC LIMITER"
  );
  console.log(
    "🧪 PAPER MODE"
  );
  console.log(
    "🔒 NO BUY / NO SELL"
  );
  console.log(
    "============================"
  );

  await startServer();
  await connectDatabase();
  await loadWallet();
  await testSolana();
  await startTelegram();

  if (
    state.solana ===
    "connected"
  ) {
    startHunter();
  }

  recoverPending()
    .catch(
      err =>
        errLog(
          "Recovery background",
          err
        )
    );

  log(
    "✅ LOMY V4.2 STARTED"
  );

  log(
    "🔒 LIVE TRADING DISABLED"
  );
}

main().catch(
  err =>
    errLog(
      "MAIN",
      err
    )
);
