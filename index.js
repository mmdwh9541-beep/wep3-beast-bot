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

// ======================================================
// GLOBAL
// ======================================================

let wallet = null;
let bot = null;
let server = null;
let tokenSub = null;
let token2022Sub = null;

const processing = new Set();
const dexRecheck = new Map();
const whaleRecheck = new Map();

const state = {
  server: "starting",
  database: "disconnected",
  wallet: "not_loaded",
  solana: "disconnected",
  telegram: "stopped",

  hunter: "stopped",
  security: "idle",
  dex: "idle",
  whales: "idle",

  detected: 0,
  securityScanned: 0,
  dexScanned: 0,
  whaleScanned: 0,

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
  return new Promise(r => setTimeout(r, ms));
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

      let done = false;
      let lastError = null;

      for (let attempt = 1; attempt <= 5; attempt++) {

        try {

          const result = await job.fn();

          rpcDelay = Math.max(
            400,
            rpcDelay - 50
          );

          job.resolve(result);

          done = true;
          break;

        } catch (err) {

          lastError = err;

          if (!is429(err)) break;

          state.rpc429++;
          state.rpcRetries++;

          rpcDelay = Math.min(
            5000,
            rpcDelay * 2
          );

          log(
            `⚠️ RPC 429 retry ${attempt}/5 delay=${rpcDelay}`
          );

          await sleep(rpcDelay);
        }
      }

      if (!done) {
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

    // SECURITY

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

    // DEX

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

    // ==================================================
    // WHALE ENGINE V1
    // ==================================================

    whaleChecked: {
      type: Boolean,
      default: false
    },

    whaleCheckedAt: Date,

    whaleAttempts: {
      type: Number,
      default: 0
    },

    whaleScore: Number,

    whaleDecision: {
      type: String,
      default: "PENDING"
    },

    largestHolderPct: Number,
    top5Pct: Number,
    top10Pct: Number,

    previousTop10Pct: Number,
    top10ChangePct: Number,

    whaleTrend: {
      type: String,
      default: "UNKNOWN"
    },

    whaleUniqueOwners: Number,

    whaleHolders: {
      type: Array,
      default: []
    },

    whaleFlags: {
      type: [String],
      default: []
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
// SERVER
// ======================================================

app.get("/", (req, res) => {

  res.send(
    "✅ LOMY V4.4 WHALE ENGINE | PAPER MODE"
  );
});

app.get("/health", (req, res) => {

  res.json({
    ...state,

    rpcDelay,
    rpcQueue: rpcQueue.length,

    mode: MODE,
    liveTrading: LIVE_TRADING,

    uptime:
      Math.floor(process.uptime())
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

    server.on("error", reject);
  });
}

// ======================================================
// MONGODB
// ======================================================

async function connectDatabase() {

  try {

    if (!MONGODB_URI) {
      throw new Error("MONGODB_URI missing");
    }

    state.database = "connecting";

    await mongoose.connect(
      MONGODB_URI,
      {
        serverSelectionTimeoutMS: 10000
      }
    );

    state.database = "connected";

    log("✅ MongoDB connected");

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
      throw new Error("BOT_PRIVATE_KEY missing");
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
// SOLANA TEST
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
// SECURITY
// ======================================================

async function securityScan(mint) {

  state.security = "scanning";

  try {

    const old =
      await FreshToken
        .findOne({ mint })
        .lean();

    const attempts =
      num(old?.securityAttempts) + 1;

    let account = null;

    for (let i = 1; i <= 4; i++) {

      account =
        await rpcCall(
          () =>
            connection
              .getParsedAccountInfo(
                new PublicKey(mint),
                "confirmed"
              )
        )
        .catch(() => null);

      if (account?.value) break;

      await sleep(i * 1000);
    }

    if (!account?.value) {

      await FreshToken.updateOne(
        { mint },
        {
          $set: {
            securityAttempts: attempts,
            securityDecision: "RETRY_LATER"
          }
        }
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
      throw new Error("Invalid mint");
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
      String(info.supply || "0");

    let score = 50;

    score += mintRevoked ? 20 : -20;
    score += freezeRevoked ? 20 : -25;

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
        Math.min(100, score)
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

    if (decision !== "REJECT") {
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
// DEX NETWORK
// ======================================================

function httpsJson(url) {

  return new Promise((resolve, reject) => {

    const req =
      https.get(
        url,
        {
          family: 4,
          timeout: 10000,

          headers: {
            Accept: "application/json",
            "User-Agent":
              "LOMY-Solana-Hunter/4.4",

            Connection: "close"
          }
        },
        response => {

          let body = "";

          response.setEncoding("utf8");

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

    req.on(
      "timeout",
      () => {

        req.destroy(
          new Error(
            "DEX timeout"
          )
        );
      }
    );

    req.on(
      "error",
      reject
    );
  });
}

async function fetchPairs(mint) {

  const url =
    "https://api.dexscreener.com/" +
    "token-pairs/v1/solana/" +
    encodeURIComponent(mint);

  let lastError = null;

  for (
    let attempt = 1;
    attempt <= 5;
    attempt++
  ) {

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
        num(b?.liquidity?.usd) -
        num(a?.liquidity?.usd)
    )[0] || null;
}

function scheduleDexRecheck(mint) {

  if (
    dexRecheck.has(mint)
  ) return;

  const timer =
    setTimeout(
      async () => {

        dexRecheck.delete(mint);

        await dexScan(mint)
          .catch(
            err =>
              errLog(
                "DEX recheck",
                err
              )
          );

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

  state.dex = "scanning";

  try {

    const token =
      await FreshToken
        .findOne({ mint })
        .lean();

    if (!token) return;

    const attempts =
      num(token.dexAttempts) + 1;

    const pairs =
      await fetchPairs(mint);

    const pair =
      bestPool(pairs);

    state.dexScanned++;

    if (!pair) {

      await FreshToken.updateOne(
        { mint },
        {
          $set: {
            dexChecked: false,
            dexListed: false,
            dexAttempts: attempts,
            dexDecision: "NO_POOL",
            finalDecision: "WAITING_DEX"
          }
        }
      );

      if (attempts < 5) {
        scheduleDexRecheck(mint);
      }

      return;
    }

    const liquidity =
      num(pair?.liquidity?.usd);

    const volumeM5 =
      num(pair?.volume?.m5);

    const volumeH1 =
      num(pair?.volume?.h1);

    const buys =
      num(pair?.txns?.m5?.buys);

    const sells =
      num(pair?.txns?.m5?.sells);

    let dexScore = 0;

    if (liquidity >= 10000) {
      dexScore += 40;
    } else if (
      liquidity >= 3000
    ) {
      dexScore += 25;
    }

    if (volumeM5 >= 250) {
      dexScore += 20;
    }

    if (volumeH1 > 0) {
      dexScore += 10;
    }

    if (buys + sells >= 5) {
      dexScore += 15;
    }

    if (buys > 0 && sells > 0) {
      dexScore += 15;
    }

    dexScore =
      Math.min(100, dexScore);

    let dexDecision = "WATCH";

    if (
      liquidity >= 3000 &&
      dexScore >= 60 &&
      sells > 0
    ) {
      dexDecision = "PASS";
    }

    const securityScore =
      num(token.securityScore);

    const finalScore =
      Math.round(
        securityScore * 0.60 +
        dexScore * 0.40
      );

    let finalDecision = "WATCH";

    if (
      token.securityDecision === "PASS" &&
      dexDecision === "PASS" &&
      finalScore >= 70
    ) {
      finalDecision = "CANDIDATE";
    }

    await FreshToken.updateOne(
      { mint },
      {
        $set: {
          dexChecked: true,
          dexListed: true,
          dexAttempts: attempts,

          dexId:
            pair.dexId || null,

          pairAddress:
            pair.pairAddress || null,

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

    log(
      `💧 ${mint} FINAL ${finalScore} ${finalDecision}`
    );

    // ================================================
    // WHALE ENGINE فقط للـCandidate
    // ================================================

    if (
      finalDecision ===
      "CANDIDATE"
    ) {

      whaleScan(mint)
        .catch(
          err =>
            errLog(
              "Whale start",
              err
            )
        );
    }

  } catch (err) {

    errLog(
      `DEX ${mint}`,
      err
    );

    await FreshToken.updateOne(
      { mint },
      {
        $set: {
          dexChecked: false,
          dexDecision: "RETRY_LATER",
          finalDecision: "WAITING_DEX"
        }
      }
    ).catch(() => {});

    scheduleDexRecheck(mint);

  } finally {

    state.dex = "idle";
  }
}

// ======================================================
// WHALE ENGINE V1
// ======================================================

function holderPercentage(
  amount,
  totalSupply
) {

  try {

    const a =
      BigInt(amount || "0");

    const s =
      BigInt(totalSupply || "0");

    if (s <= 0n) return 0;

    // basis points
    const bp =
      (a * 10000n) / s;

    return (
      Number(bp) / 100
    );

  } catch {

    return 0;
  }
}

function calculateWhaleScore(
  data
) {

  let score = 100;

  const flags = [];

  if (
    data.largest >= 25
  ) {

    score -= 45;

    flags.push(
      "VERY_LARGE_SINGLE_HOLDER"
    );

  } else if (
    data.largest >= 15
  ) {

    score -= 30;

    flags.push(
      "LARGE_SINGLE_HOLDER"
    );

  } else if (
    data.largest >= 10
  ) {

    score -= 15;

    flags.push(
      "SINGLE_HOLDER_CAUTION"
    );
  }

  if (
    data.top10 >= 80
  ) {

    score -= 40;

    flags.push(
      "EXTREME_TOP10_CONCENTRATION"
    );

  } else if (
    data.top10 >= 60
  ) {

    score -= 25;

    flags.push(
      "HIGH_TOP10_CONCENTRATION"
    );

  } else if (
    data.top10 >= 45
  ) {

    score -= 10;

    flags.push(
      "MEDIUM_TOP10_CONCENTRATION"
    );
  }

  if (
    data.uniqueOwners < 5
  ) {

    score -= 15;

    flags.push(
      "LOW_OWNER_DIVERSITY"
    );
  }

  if (
    data.change >= 5
  ) {

    score -= 15;

    flags.push(
      "CONCENTRATION_INCREASING"
    );

  } else if (
    data.change <= -5
  ) {

    score += 5;

    flags.push(
      "CONCENTRATION_DECREASING"
    );
  }

  score =
    Math.max(
      0,
      Math.min(
        100,
        score
      )
    );

  let decision;

  if (score >= 75) {
    decision = "SAFE";
  } else if (
    score >= 50
  ) {
    decision = "CAUTION";
  } else {
    decision = "DANGER";
  }

  return {
    score,
    decision,
    flags
  };
}

async function whaleScan(mint) {

  state.whales =
    "scanning";

  try {

    const token =
      await FreshToken
        .findOne({ mint })
        .lean();

    if (!token) return;

    const attempts =
      num(
        token.whaleAttempts
      ) + 1;

    // ================================================
    // إجمالي المعروض
    // ================================================

    const supplyResponse =
      await rpcCall(
        () =>
          connection.getTokenSupply(
            new PublicKey(mint),
            "confirmed"
          )
      );

    const totalSupply =
      String(
        supplyResponse
          ?.value
          ?.amount ||
        token.supply ||
        "0"
      );

    // ================================================
    // أكبر Token Accounts
    // ================================================

    const largestResponse =
      await rpcCall(
        () =>
          connection
            .getTokenLargestAccounts(
              new PublicKey(mint),
              "confirmed"
            )
      );

    const accounts =
      (
        largestResponse
          ?.value || []
      ).slice(0, 10);

    if (!accounts.length) {

      throw new Error(
        "No holder accounts"
      );
    }

    const publicKeys =
      accounts.map(
        h =>
          new PublicKey(
            h.address
          )
      );

    // طلب واحد بدل 10 طلبات
    const parsedAccounts =
      await rpcCall(
        () =>
          connection
            .getMultipleParsedAccounts(
              publicKeys,
              "confirmed"
            )
      );

    const holders = [];

    const owners = new Set();

    for (
      let i = 0;
      i < accounts.length;
      i++
    ) {

      const holder =
        accounts[i];

      const account =
        parsedAccounts
          ?.value?.[i];

      const owner =
        account
          ?.data
          ?.parsed
          ?.info
          ?.owner ||
        "UNKNOWN";

      if (
        owner !== "UNKNOWN"
      ) {
        owners.add(owner);
      }

      const amount =
        String(
          holder.amount || "0"
        );

      holders.push({
        rank: i + 1,

        tokenAccount:
          holder.address.toString(),

        owner,

        amount,

        uiAmount:
          holder.uiAmountString ||
          null,

        percent:
          holderPercentage(
            amount,
            totalSupply
          )
      });
    }

    const largest =
      holders[0]
        ?.percent || 0;

    const top5 =
      holders
        .slice(0, 5)
        .reduce(
          (sum, h) =>
            sum +
            num(h.percent),
          0
        );

    const top10 =
      holders
        .reduce(
          (sum, h) =>
            sum +
            num(h.percent),
          0
        );

    const previous =
      num(
        token.top10Pct
      );

    const change =
      attempts > 1
        ?
        Number(
          (
            top10 -
            previous
          ).toFixed(2)
        )
        :
        0;

    let trend =
      "STABLE";

    if (change >= 2) {
      trend =
        "ACCUMULATION";
    }

    if (change <= -2) {
      trend =
        "DISTRIBUTION";
    }

    const result =
      calculateWhaleScore(
        {
          largest,
          top10,
          uniqueOwners:
            owners.size,
          change
        }
      );

    await FreshToken.updateOne(
      { mint },
      {
        $set: {

          whaleChecked:
            true,

          whaleCheckedAt:
            new Date(),

          whaleAttempts:
            attempts,

          whaleScore:
            result.score,

          whaleDecision:
            result.decision,

          largestHolderPct:
            Number(
              largest.toFixed(2)
            ),

          top5Pct:
            Number(
              top5.toFixed(2)
            ),

          previousTop10Pct:
            attempts > 1
              ? previous
              : null,

          top10Pct:
            Number(
              top10.toFixed(2)
            ),

          top10ChangePct:
            change,

          whaleTrend:
            trend,

          whaleUniqueOwners:
            owners.size,

          whaleHolders:
            holders,

          whaleFlags:
            result.flags
        }
      }
    );

    state.whaleScanned++;

    log(
      `🐋 ${mint} | Whale ${result.score}/100 | ${result.decision} | Top10 ${top10.toFixed(2)}%`
    );

    // V1 فقط 3 snapshots
    if (attempts < 3) {

      scheduleWhaleRecheck(
        mint
      );
    }

  } catch (err) {

    errLog(
      `Whale ${mint}`,
      err
    );

  } finally {

    state.whales =
      "idle";
  }
}

// ======================================================
// WHALE RECHECK
// ======================================================

function scheduleWhaleRecheck(
  mint
) {

  if (
    whaleRecheck.has(
      mint
    )
  ) return;

  const timer =
    setTimeout(
      async () => {

        whaleRecheck.delete(
          mint
        );

        await whaleScan(
          mint
        ).catch(
          err =>
            errLog(
              "Whale recheck",
              err
            )
        );

      },
      120000
    );

  whaleRecheck.set(
    mint,
    timer
  );
}

// ======================================================
// HUNTER
// ======================================================

function findMint(
  instructions
) {

  if (!Array.isArray(instructions)) {
    return null;
  }

  for (const ix of instructions) {

    const type =
      ix?.parsed?.type;

    if (
      type === "initializeMint" ||
      type === "initializeMint2"
    ) {

      return (
        ix?.parsed
          ?.info
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
  ) return;

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
    processing.has(signature)
  ) return;

  processing.add(signature);

  try {

    let tx = null;

    for (let i = 0; i < 4; i++) {

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
        .catch(() => null);

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
    state.lastMint = mint;

    let token =
      await FreshToken
        .findOne({ mint })
        .lean();

    if (!token) {

      await FreshToken.create({
        mint,
        signature,
        program,
        paperOnly: true
      });

      token =
        await FreshToken
          .findOne({ mint })
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
    mongoose.connection.readyState !== 1
  ) return;

  try {

    const whalePending =
      await FreshToken
        .find({
          finalDecision:
            "CANDIDATE",

          whaleChecked: {
            $ne: true
          }
        })
        .sort({
          finalScore: -1
        })
        .limit(10)
        .select("mint")
        .lean();

    log(
      `🐋 Whale recovery ${whalePending.length}`
    );

    for (
      const token
      of whalePending
    ) {

      await whaleScan(
        token.mint
      );

      await sleep(1500);
    }

    const securityPending =
      await FreshToken
        .find({
          securityChecked: false
        })
        .sort({
          detectedAt: -1
        })
        .limit(10)
        .select("mint")
        .lean();

    for (
      const token
      of securityPending
    ) {

      await securityScan(
        token.mint
      );

      await sleep(1200);
    }

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
          "🤖 LOMY V4.4\n\n" +
          "🔎 Hunter ON\n" +
          "🛡 Security ON\n" +
          "💧 DEX ON\n" +
          "🐋 Whale Engine ON\n" +
          "🧪 PAPER MODE\n" +
          "🔒 NO BUY / NO SELL"
        )
    );

    bot.command(
      "status",
      ctx =>
        ctx.reply(

          `🤖 LOMY V4.4 STATUS\n\n` +

          `🌐 Server: ${state.server}\n` +

          `🗄 Database: ${state.database}\n` +

          `👛 Wallet: ${state.wallet}\n` +

          `⚡ Solana: ${state.solana}\n` +

          `📡 Telegram: ${state.telegram}\n\n` +

          `🔎 Hunter: ${state.hunter}\n` +

          `🛡 Security: ${state.security}\n` +

          `💧 DEX: ${state.dex}\n` +

          `🐋 Whales: ${state.whales}\n\n` +

          `RPC 429: ${state.rpc429}\n` +

          `Errors: ${state.errors}\n\n` +

          `🧪 PAPER MODE`
        )
    );

    bot.command(
      "whales",
      async ctx => {

        const tokens =
          await FreshToken
            .find({
              finalDecision:
                "CANDIDATE",

              whaleChecked:
                true
            })
            .sort({
              whaleScore: -1,
              finalScore: -1
            })
            .limit(5)
            .lean();

        if (!tokens.length) {

          return ctx.reply(
            "🐋 WHALE ENGINE\n\n" +
            "لسه مفيش Candidates تم فحص الحيتان عليها."
          );
        }

        let text =
          "🐋 WHALE REPORT\n";

        tokens.forEach(
          (t, i) => {

            text +=

              `\n━━━━━━━━━━━━━━\n` +

              `${i + 1}) ${t.mint}\n\n` +

              `🐋 Whale Score: ${num(t.whaleScore)}/100\n` +

              `Decision: ${t.whaleDecision}\n\n` +

              `👤 Largest: ${num(t.largestHolderPct).toFixed(2)}%\n` +

              `👥 Top 5: ${num(t.top5Pct).toFixed(2)}%\n` +

              `👥 Top 10: ${num(t.top10Pct).toFixed(2)}%\n` +

              `🔄 Change: ${num(t.top10ChangePct).toFixed(2)}%\n` +

              `📈 Trend: ${t.whaleTrend}\n` +

              `👛 Unique Owners: ${t.whaleUniqueOwners || 0}\n`;
          }
        );

        text +=
          "\n🧪 PAPER MODE";

        await ctx.reply(text);
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
              finalScore: -1,
              liquidityUsd: -1
            })
            .limit(10)
            .lean();

        if (!tokens.length) {

          return ctx.reply(
            "🎯 لا توجد Candidates."
          );
        }

        let text =
          `🎯 TOP CANDIDATES (${tokens.length})\n`;

        for (
          let i = 0;
          i < tokens.length;
          i++
        ) {

          const t =
            tokens[i];

          text +=

            `\n━━━━━━━━━━━━━━\n` +

            `${i + 1}) ${t.mint}\n` +

            `🎯 Final: ${num(t.finalScore)}/100\n` +

            `🛡 Security: ${num(t.securityScore)}/100\n` +

            `💧 DEX: ${num(t.dexScore)}/100\n` +

            `🐋 Whale: ${
              t.whaleChecked
                ? `${num(t.whaleScore)}/100 ${t.whaleDecision}`
                : "SCANNING..."
            }\n` +

            `💵 Liquidity: $${num(t.liquidityUsd).toFixed(0)}\n`;
        }

        text +=
          "\n🔒 PAPER ONLY";

        await ctx.reply(text);
      }
    );

    bot.command(
      "stats",
      async ctx => {

        const [
          total,
          candidates,
          whaleChecked,
          safe,
          caution,
          danger
        ] =
        await Promise.all([
          FreshToken.countDocuments(),

          FreshToken.countDocuments({
            finalDecision:
              "CANDIDATE"
          }),

          FreshToken.countDocuments({
            whaleChecked:
              true
          }),

          FreshToken.countDocuments({
            whaleDecision:
              "SAFE"
          }),

          FreshToken.countDocuments({
            whaleDecision:
              "CAUTION"
          }),

          FreshToken.countDocuments({
            whaleDecision:
              "DANGER"
          })
        ]);

        await ctx.reply(

          `📊 V4.4 STATS\n\n` +

          `Total Tokens: ${total}\n` +

          `Candidates: ${candidates} 🎯\n\n` +

          `Whale Scanned: ${whaleChecked} 🐋\n` +

          `SAFE: ${safe} ✅\n` +

          `CAUTION: ${caution} ⚠️\n` +

          `DANGER: ${danger} ❌\n\n` +

          `RPC 429: ${state.rpc429}\n` +

          `Errors: ${state.errors}\n\n` +

          `🧪 PAPER MODE`
        );
      }
    );

    bot.command(
      "network",
      ctx =>
        ctx.reply(
          `🌐 NETWORK\n\n` +
          `RPC Queue: ${rpcQueue.length}\n` +
          `RPC 429: ${state.rpc429}\n` +
          `RPC Retries: ${state.rpcRetries}\n` +
          `RPC Delay: ${rpcDelay}ms\n` +
          `DEX Errors: ${state.dexNetworkErrors}`
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
      "Telegram",
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
    clearTimeout(timer);
  }

  for (
    const timer
    of whaleRecheck.values()
  ) {
    clearTimeout(timer);
  }

  try {

    if (tokenSub !== null) {

      await connection
        .removeOnLogsListener(
          tokenSub
        );
    }

    if (
      token2022Sub !== null
    ) {

      await connection
        .removeOnLogsListener(
          token2022Sub
        );
    }

  } catch {}

  try {

    if (bot) {
      bot.stop(signal);
    }

  } catch {}

  try {

    await mongoose.disconnect();

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
    shutdown("SIGTERM")
);

process.once(
  "SIGINT",
  () =>
    shutdown("SIGINT")
);

// ======================================================
// MAIN
// ======================================================

async function main() {

  console.log("");
  console.log("==============================");
  console.log("🚀 LOMY SOLANA HUNTER V4.4");
  console.log("🐋 WHALE ENGINE V1");
  console.log("🧪 PAPER MODE");
  console.log("🔒 NO BUY / NO SELL");
  console.log("==============================");

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
          "Recovery",
          err
        )
    );

  log(
    "✅ LOMY V4.4 STARTED"
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
