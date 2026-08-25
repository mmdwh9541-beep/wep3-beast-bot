require("dotenv").config();

const express = require("express");
const mongoose = require("mongoose");
const { Telegraf } = require("telegraf");
const https = require("https");
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
const dexRecheck = new Map();

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

  rpc429: 0,
  rpcRetries: 0,

  dexRetries: 0,
  dexNetworkErrors: 0,

  errors: 0,
  lastMint: null
};

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
  return new Promise(resolve => setTimeout(resolve, ms));
}

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function is429(err) {
  const s = String(err?.message || err).toLowerCase();

  return (
    s.includes("429") ||
    s.includes("rate limit")
  );
}

// ======================================================
// RPC LIMITER
// ======================================================

const rpcQueue = [];
let rpcBusy = false;
let rpcDelay = 500;

function rpcCall(fn) {
  return new Promise((resolve, reject) => {
    rpcQueue.push({
      fn,
      resolve,
      reject
    });

    runRpcQueue();
  });
}

async function runRpcQueue() {
  if (rpcBusy) return;

  rpcBusy = true;

  try {
    while (rpcQueue.length) {
      const job = rpcQueue.shift();

      let completed = false;
      let lastError = null;

      for (let i = 1; i <= 5; i++) {
        try {
          const result = await job.fn();

          rpcDelay = Math.max(
            400,
            rpcDelay - 50
          );

          job.resolve(result);
          completed = true;

          break;

        } catch (err) {
          lastError = err;

          if (!is429(err)) {
            break;
          }

          state.rpc429++;
          state.rpcRetries++;

          rpcDelay = Math.min(
            5000,
            rpcDelay * 2
          );

          log(
            `⚠️ RPC 429 retry ${i}/5 delay=${rpcDelay}`
          );

          await sleep(rpcDelay);
        }
      }

      if (!completed) {
        job.reject(lastError);
      }

      await sleep(rpcDelay);
    }
  } finally {
    rpcBusy = false;
  }
}

// ======================================================
// DATABASE
// ======================================================

const tokenSchema = new mongoose.Schema(
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

    securityAttempts: {
      type: Number,
      default: 0
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
  mongoose.model("FreshToken", tokenSchema);

// ======================================================
// SERVER
// ======================================================

app.get("/", (req, res) => {
  res.send(
    "✅ LOMY V4.3 ONLINE | PAPER MODE"
  );
});

app.get("/health", (req, res) => {
  res.json({
    ...state,
    rpcDelay,
    mode: MODE,
    liveTrading: LIVE_TRADING,
    uptime: Math.floor(process.uptime())
  });
});

function startServer() {
  return new Promise((resolve, reject) => {
    server = app.listen(
      PORT,
      "0.0.0.0",
      () => {
        state.server = "online";

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
  });
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

    state.database = "connecting";

    await mongoose.connect(
      MONGODB_URI,
      {
        serverSelectionTimeoutMS: 10000
      }
    );

    state.database = "connected";

    log(
      "✅ MongoDB connected"
    );

  } catch (err) {
    state.database = "error";

    errLog(
      "MongoDB",
      err
    );
  }
}

mongoose.connection.on(
  "disconnected",
  () => {
    state.database = "disconnected";
  }
);

mongoose.connection.on(
  "reconnected",
  () => {
    state.database = "connected";
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
        bip39.mnemonicToSeedSync(key);

      const derived =
        derivePath(
          "m/44'/501'/0'/0'",
          seed.toString("hex")
        ).key;

      wallet =
        Keypair.fromSeed(derived);

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

    state.wallet = "loaded";

    log(
      "✅ Wallet",
      wallet.publicKey.toString()
    );

  } catch (err) {
    state.wallet = "error";

    errLog(
      "Wallet",
      err
    );
  }
}

// ======================================================
// SOLANA
// ======================================================

async function testSolana() {
  if (!wallet) return;

  try {
    const balance =
      await rpcCall(
        () =>
          connection.getBalance(
            wallet.publicKey
          )
      );

    state.solana = "connected";

    log(
      "✅ Solana",
      (
        balance /
        LAMPORTS_PER_SOL
      ).toFixed(6),
      "SOL"
    );

  } catch (err) {
    state.solana = "error";

    errLog(
      "Solana",
      err
    );
  }
}

// ======================================================
// SECURITY WITH RETRY
// ======================================================

async function securityScan(mint) {
  state.security = "scanning";

  try {
    const existing =
      await FreshToken
        .findOne({ mint })
        .lean();

    const attempts =
      num(existing?.securityAttempts) + 1;

    let account = null;

    for (let i = 1; i <= 4; i++) {
      account =
        await rpcCall(
          () =>
            connection.getParsedAccountInfo(
              new PublicKey(mint),
              "confirmed"
            )
        ).catch(
          () => null
        );

      if (account?.value) {
        break;
      }

      log(
        `⏳ Mint unavailable retry ${i}/4 ${mint}`
      );

      await sleep(
        i * 1000
      );
    }

    if (!account?.value) {
      await FreshToken.updateOne(
        { mint },
        {
          $set: {
            securityAttempts: attempts,
            securityDecision:
              "RETRY_LATER"
          }
        }
      );

      log(
        `⚠️ Mint delayed ${mint}`
      );

      return;
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
      TOKEN_2022_PROGRAM_ID.toString();

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

    let decision = "REJECT";

    if (score >= 80) {
      decision = "PASS";
    } else if (score >= 55) {
      decision = "REVIEW";
    }

    await FreshToken.updateOne(
      { mint },
      {
        $set: {
          securityChecked: true,
          securityAttempts: attempts,
          securityScore: score,
          securityDecision: decision,

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
      decision !== "REJECT"
    ) {
      await dexScan(mint);
    }

  } catch (err) {
    errLog(
      `Security ${mint}`,
      err
    );

  } finally {
    state.security = "idle";
  }
}

// ======================================================
// ROBUST HTTPS GET FOR DEX
// ======================================================

function httpsJson(url) {
  return new Promise(
    (resolve, reject) => {

      const request =
        https.get(
          url,
          {
            family: 4,

            headers: {
              Accept:
                "application/json",

              "User-Agent":
                "LOMY-Solana-Hunter/4.3",

              Connection:
                "close"
            },

            timeout: 10000
          },
          response => {

            let body = "";

            response.setEncoding(
              "utf8"
            );

            response.on(
              "data",
              chunk => {
                body += chunk;
              }
            );

            response.on(
              "end",
              () => {

                if (
                  response.statusCode < 200 ||
                  response.statusCode >= 300
                ) {
                  return reject(
                    new Error(
                      `DEX HTTP ${response.statusCode}`
                    )
                  );
                }

                try {
                  resolve(
                    JSON.parse(body)
                  );

                } catch {
                  reject(
                    new Error(
                      "DEX invalid JSON"
                    )
                  );
                }
              }
            );
          }
        );

      request.on(
        "timeout",
        () => {

          request.destroy(
            new Error(
              "DEX timeout"
            )
          );
        }
      );

      request.on(
        "error",
        reject
      );
    }
  );
}

async function fetchPairs(mint) {

  const url =
    "https://api.dexscreener.com/" +
    "token-pairs/v1/solana/" +
    encodeURIComponent(mint);

  let lastError = null;

  for (let attempt = 1; attempt <= 5; attempt++) {

    try {

      const data =
        await httpsJson(url);

      return Array.isArray(data)
        ? data
        : [];

    } catch (err) {

      lastError = err;

      state.dexRetries++;
      state.dexNetworkErrors++;

      const delay =
        Math.min(
          10000,
          1000 *
          Math.pow(
            2,
            attempt - 1
          )
        );

      log(
        `⚠️ DEX retry ${attempt}/5 ${delay}ms | ${err.message}`
      );

      await sleep(delay);
    }
  }

  throw lastError;
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

// ======================================================
// DEX RECHECK
// ======================================================

function scheduleDexRecheck(mint) {

  if (
    dexRecheck.has(mint)
  ) {
    return;
  }

  const timer =
    setTimeout(
      async () => {

        dexRecheck.delete(
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

  dexRecheck.set(
    mint,
    timer
  );
}

// ======================================================
// DEX ENGINE
// ======================================================

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
      liquidity >= 10000
    ) {
      dexScore += 40;

    } else if (
      liquidity >= 3000
    ) {
      dexScore += 25;
    }

    if (
      volumeM5 >= 250
    ) {
      dexScore += 20;
    }

    if (
      volumeH1 > 0
    ) {
      dexScore += 10;
    }

    if (
      buys + sells >= 5
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
      `💧 DEX OK ${mint} | $${liquidity.toFixed(0)} | ${finalDecision}`
    );

  } catch (err) {

    errLog(
      `DEX ${mint}`,
      err
    );

    // Network failure ≠ token rejection

    await FreshToken.updateOne(
      { mint },
      {
        $set: {
          dexChecked:
            false,

          dexDecision:
            "RETRY_LATER",

          finalDecision:
            "WAITING_DEX"
        }
      }
    ).catch(
      () => {}
    );

    scheduleDexRecheck(
      mint
    );

  } finally {

    state.dex =
      "idle";
  }
}

// ======================================================
// HUNTER
// ======================================================

function findMint(instructions) {

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
        ix?.parsed?.info?.mint ||
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
        await rpcCall(
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
      !token.securityChecked
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
        )
        .catch(
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
        )
        .catch(
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
          detectedAt: -1
        })
        .limit(15)
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
        1200
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
          detectedAt: -1
        })
        .limit(30)
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
        1200
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
          "🤖 LOMY V4.3\n\n" +
          "🔎 Hunter ON\n" +
          "🛡 Security Retry ON\n" +
          "💧 DEX Retry ON\n" +
          "🚦 RPC Limiter ON\n" +
          "🧪 PAPER MODE\n" +
          "🔒 NO BUY / NO SELL"
        )
    );

    bot.command(
      "status",
      ctx =>
        ctx.reply(

          `🤖 LOMY V4.3 STATUS\n\n` +

          `🌐 Server: ${state.server}\n` +

          `🗄 Database: ${state.database}\n` +

          `👛 Wallet: ${state.wallet}\n` +

          `⚡ Solana: ${state.solana}\n` +

          `📡 Telegram: ${state.telegram}\n` +

          `🔎 Hunter: ${state.hunter}\n` +

          `🛡 Security: ${state.security}\n` +

          `💧 DEX: ${state.dex}\n\n` +

          `⚠️ RPC 429: ${state.rpc429}\n` +

          `🔁 RPC Retries: ${state.rpcRetries}\n` +

          `🌐 DEX Network Errors: ${state.dexNetworkErrors}\n` +

          `🔄 DEX Retries: ${state.dexRetries}\n\n` +

          `🧪 PAPER MODE\n` +

          `🔒 LIVE TRADING OFF`
        )
    );

    bot.command(
      "balance",
      async ctx => {

        try {

          const balance =
            await rpcCall(
              () =>
                connection.getBalance(
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
      "network",
      ctx =>
        ctx.reply(

          `🌐 NETWORK STATUS\n\n` +

          `RPC 429: ${state.rpc429}\n` +

          `RPC Retries: ${state.rpcRetries}\n` +

          `RPC Delay: ${rpcDelay}ms\n\n` +

          `DEX Network Errors: ${state.dexNetworkErrors}\n` +

          `DEX Retries: ${state.dexRetries}\n\n` +

          `✅ Auto Retry Active`
        )
    );

    bot.command(
      "stats",
      async ctx => {

        const [
          total,
          pass,
          listed,
          waiting,
          candidates
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
                  "WAITING_DEX"
              }),

            FreshToken
              .countDocuments({
                finalDecision:
                  "CANDIDATE"
              })
          ]
        );

        await ctx.reply(

          `📊 V4.3 STATS\n\n` +

          `Total: ${total}\n` +

          `Security PASS: ${pass} ✅\n` +

          `DEX Listed: ${listed} 💧\n` +

          `Waiting DEX: ${waiting} ⏳\n` +

          `Candidates: ${candidates} 🎯\n\n` +

          `Errors: ${state.errors}\n` +

          `DEX Retries: ${state.dexRetries}\n\n` +

          `🧪 PAPER MODE`
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

async function shutdown(signal) {

  log(
    `⚠️ ${signal} shutdown`
  );

  for (
    const timer
    of dexRecheck.values()
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
    "🚀 LOMY SOLANA HUNTER V4.3"
  );
  console.log(
    "🌐 NETWORK RECOVERY ENGINE"
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
    "✅ LOMY V4.3 STARTED"
  );

  log(
    "🔒 LIVE TRADING DISABLED"
  );
}

main()
  .catch(
    err =>
      errLog(
        "MAIN",
        err
      )
  );
