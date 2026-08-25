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

// ======================================================
// ENV
// ======================================================

const PORT = Number(process.env.PORT) || 10000;

const RPC_URL =
  process.env.RPC_URL ||
  "https://api.mainnet-beta.solana.com";

const MONGODB_URI = process.env.MONGODB_URI;
const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const BOT_PRIVATE_KEY = process.env.BOT_PRIVATE_KEY;

// ======================================================
// SAFETY
// ======================================================

const MODE = "PAPER";
const LIVE_TRADING = false;

// ======================================================
// SMART SCORE
// ======================================================

const SCORE_WEIGHTS = {
  security: 0.45,
  dex: 0.35,
  whale: 0.20
};

const SMART_APPROVAL_SCORE = 75;

// ======================================================
// TELEGRAM IPV4
// ======================================================

const telegramAgent = new https.Agent({
  keepAlive: true,
  family: 4,
  timeout: 30000
});

// ======================================================
// SOLANA
// ======================================================

const connection = new Connection(
  RPC_URL,
  {
    commitment: "confirmed",
    confirmTransactionInitialTimeout: 30000
  }
);

const TOKEN_PROGRAM_ID = new PublicKey(
  "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
);

const TOKEN_2022_PROGRAM_ID = new PublicKey(
  "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
);

// ======================================================
// GLOBAL
// ======================================================

let wallet = null;
let bot = null;
let server = null;

let tokenSub = null;
let token2022Sub = null;
let slotSub = null;

let hunterRestarting = false;
let shuttingDown = false;

let lastWsHeartbeat = Date.now();

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
  websocket: "starting",
  security: "idle",
  dex: "idle",
  whales: "idle",

  detected: 0,
  securityScanned: 0,
  dexScanned: 0,
  whaleScanned: 0,

  rpc429: 0,
  rpcRetries: 0,
  rpcDropped: 0,

  dexRetries: 0,
  dexNetworkErrors: 0,

  whaleRetries: 0,
  telegramRetries: 0,

  migrated: 0,

  errors: 0,
  lastMint: null
};

// ======================================================
// HELPERS
// ======================================================

function log(...x) {
  console.log(
    new Date().toISOString(),
    ...x
  );
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
  return new Promise(resolve =>
    setTimeout(resolve, ms)
  );
}

function num(v) {
  const n = Number(v);

  return Number.isFinite(n)
    ? n
    : 0;
}

function is429(err) {
  const text =
    String(
      err?.message || err
    ).toLowerCase();

  return (
    text.includes("429") ||
    text.includes("rate limit") ||
    text.includes("too many requests")
  );
}

function jitter(ms) {
  return (
    ms +
    Math.floor(
      Math.random() * 300
    )
  );
}

// ======================================================
// SMART FINAL SCORE
// ======================================================

function calculateSmartScore(
  securityScore,
  dexScore,
  whaleScore
) {

  const score =
    num(securityScore) *
      SCORE_WEIGHTS.security +

    num(dexScore) *
      SCORE_WEIGHTS.dex +

    num(whaleScore) *
      SCORE_WEIGHTS.whale;

  return Math.max(
    0,
    Math.min(
      100,
      Math.round(score)
    )
  );
}

function smartDecision(
  smartScore,
  whaleDecision
) {

  if (
    whaleDecision === "DANGER"
  ) {
    return "BLOCKED_WHALE";
  }

  if (
    whaleDecision === "CAUTION"
  ) {
    return "WATCH_WHALE";
  }

  if (
    whaleDecision === "SAFE" &&
    smartScore >= SMART_APPROVAL_SCORE
  ) {
    return "APPROVED_CANDIDATE";
  }

  return "WATCH_SCORE";
}

// ======================================================
// RPC SMART QUEUE
// ======================================================

const rpcQueue = [];

let rpcBusy = false;
let rpcDelay = 700;

const RPC_MIN_DELAY = 650;
const RPC_MAX_DELAY = 8000;

const RPC_SOFT_LIMIT = 18;
const RPC_HARD_LIMIT = 35;

function rpcCall(
  fn,
  priority = 1,
  label = "RPC"
) {

  return new Promise(
    (resolve, reject) => {

      if (
        priority === 2 &&
        rpcQueue.length >= RPC_SOFT_LIMIT
      ) {

        state.rpcDropped++;

        return reject(
          new Error(
            "RPC_QUEUE_BUSY"
          )
        );
      }

      rpcQueue.push({
        fn,
        resolve,
        reject,
        priority,
        label,
        createdAt: Date.now()
      });

      rpcQueue.sort(
        (a, b) =>
          a.priority -
          b.priority
      );

      runRpcQueue()
        .catch(
          err =>
            errLog(
              "RPC Worker",
              err
            )
        );
    }
  );
}

async function runRpcQueue() {

  if (rpcBusy) return;

  rpcBusy = true;

  try {

    while (
      rpcQueue.length
    ) {

      const job =
        rpcQueue.shift();

      let done = false;
      let lastError = null;

      for (
        let attempt = 1;
        attempt <= 5;
        attempt++
      ) {

        try {

          const result =
            await job.fn();

          rpcDelay =
            Math.max(
              RPC_MIN_DELAY,
              rpcDelay - 75
            );

          job.resolve(result);

          done = true;
          break;

        } catch (err) {

          lastError = err;

          if (
            !is429(err)
          ) {
            break;
          }

          state.rpc429++;
          state.rpcRetries++;

          rpcDelay =
            Math.min(
              RPC_MAX_DELAY,
              Math.max(
                1400,
                rpcDelay * 1.7
              )
            );

          log(
            `⚠️ RPC 429 | ${job.label} | attempt ${attempt}/5 | delay=${Math.round(rpcDelay)}ms`
          );

          await sleep(
            jitter(
              rpcDelay
            )
          );
        }
      }

      if (!done) {
        job.reject(
          lastError
        );
      }

      if (
        rpcQueue.length >=
        RPC_HARD_LIMIT
      ) {

        rpcDelay =
          Math.max(
            rpcDelay,
            2500
          );
      }

      await sleep(
        jitter(
          rpcDelay
        )
      );
    }

  } finally {

    rpcBusy = false;
  }
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

      // SMART

      preWhaleScore: Number,
      smartScore: Number,
      finalScore: Number,

      finalDecision: {
        type: String,
        default: "PENDING"
      },

      // WHALE

      whaleStatus: {
        type: String,
        default: "PENDING"
      },

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

      whaleLastError: String,

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

app.get(
  "/",
  (req, res) => {

    res.send(
      "✅ LOMY V4.5.1 MIGRATION FIX | PAPER MODE"
    );
  }
);

app.get(
  "/health",
  (req, res) => {

    res.json({
      ...state,

      rpcQueue:
        rpcQueue.length,

      rpcDelay:
        Math.round(
          rpcDelay
        ),

      whaleQueue:
        whaleQueue.length,

      mode:
        MODE,

      liveTrading:
        LIVE_TRADING,

      uptime:
        Math.floor(
          process.uptime()
        )
    });
  }
);

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
              `✅ Render Server Online : ${PORT}`
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

    if (
      !MONGODB_URI
    ) {

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

    if (
      !BOT_PRIVATE_KEY
    ) {

      throw new Error(
        "BOT_PRIVATE_KEY missing"
      );
    }

    const key =
      BOT_PRIVATE_KEY
        .trim();

    if (
      key.includes(" ") &&
      bip39.validateMnemonic(
        key
      )
    ) {

      const seed =
        bip39.mnemonicToSeedSync(
          key
        );

      const derived =
        derivePath(
          "m/44'/501'/0'/0'",
          seed.toString(
            "hex"
          )
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
            JSON.parse(
              key
            )
          )
        );

    } else {

      wallet =
        Keypair.fromSecretKey(
          bs58.decode(
            key
          )
        );
    }

    state.wallet =
      "loaded";

    log(
      "✅ Wallet",
      wallet.publicKey
        .toString()
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
      await rpcCall(
        () =>
          connection
            .getBalance(
              wallet.publicKey
            ),
        0,
        "BALANCE"
      );

    state.solana =
      "connected";

    log(
      "✅ Solana connected",
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
// V4.5.1 MIGRATION FIX
// ======================================================

async function migrateOldSmartScores() {

  if (
    mongoose.connection.readyState !== 1
  ) {
    return;
  }

  try {

    const oldTokens =
      await FreshToken
        .find({
          whaleChecked: true,
          whaleStatus: "DONE"
        })
        .select(
          "_id mint securityScore dexScore whaleScore whaleDecision finalDecision smartScore finalScore"
        )
        .lean();

    log(
      `🧠 Migration found ${oldTokens.length} whale-completed tokens`
    );

    let migrated = 0;
    let approved = 0;
    let blocked = 0;
    let watchWhale = 0;
    let watchScore = 0;

    for (
      const token
      of oldTokens
    ) {

      const smartScore =
        calculateSmartScore(
          token.securityScore,
          token.dexScore,
          token.whaleScore
        );

      const finalDecision =
        smartDecision(
          smartScore,
          token.whaleDecision
        );

      await FreshToken
        .updateOne(
          {
            _id:
              token._id
          },
          {
            $set: {

              smartScore,

              finalScore:
                smartScore,

              finalDecision
            }
          }
        );

      migrated++;

      if (
        finalDecision ===
        "APPROVED_CANDIDATE"
      ) {
        approved++;
      }

      if (
        finalDecision ===
        "BLOCKED_WHALE"
      ) {
        blocked++;
      }

      if (
        finalDecision ===
        "WATCH_WHALE"
      ) {
        watchWhale++;
      }

      if (
        finalDecision ===
        "WATCH_SCORE"
      ) {
        watchScore++;
      }
    }

    state.migrated =
      migrated;

    log(
      `✅ MIGRATION COMPLETE | Total ${migrated} | Approved ${approved} | Blocked ${blocked} | WhaleWatch ${watchWhale} | ScoreWatch ${watchScore}`
    );

  } catch (err) {

    errLog(
      "Smart Score Migration",
      err
    );
  }
}

// ======================================================
// SECURITY ENGINE
// ======================================================

async function securityScan(
  mint
) {

  state.security =
    "scanning";

  try {

    const old =
      await FreshToken
        .findOne({
          mint
        })
        .lean();

    const attempts =
      num(
        old?.securityAttempts
      ) + 1;

    let account = null;

    for (
      let i = 1;
      i <= 4;
      i++
    ) {

      account =
        await rpcCall(
          () =>
            connection
              .getParsedAccountInfo(
                new PublicKey(
                  mint
                ),
                "confirmed"
              ),
          1,
          "SECURITY"
        )
        .catch(
          () => null
        );

      if (
        account?.value
      ) {
        break;
      }

      await sleep(
        i * 1000
      );
    }

    if (
      !account?.value
    ) {

      await FreshToken
        .updateOne(
          {
            mint
          },
          {
            $set: {

              securityAttempts:
                attempts,

              securityDecision:
                "RETRY_LATER"
            }
          }
        );

      return;
    }

    const owner =
      account.value
        .owner
        .toString();

    const parsed =
      account.value
        .data
        ?.parsed;

    if (
      !parsed ||
      parsed.type !==
        "mint"
    ) {

      throw new Error(
        "Invalid mint"
      );
    }

    const info =
      parsed.info || {};

    const mintRevoked =
      info.mintAuthority ==
      null;

    const freezeRevoked =
      info.freezeAuthority ==
      null;

    const token2022 =
      owner ===
      TOKEN_2022_PROGRAM_ID
        .toString();

    const decimals =
      num(
        info.decimals
      );

    const supply =
      String(
        info.supply ||
        "0"
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

    if (
      token2022
    ) {
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

    if (
      score >= 80
    ) {

      decision =
        "PASS";

    } else if (
      score >= 55
    ) {

      decision =
        "REVIEW";
    }

    await FreshToken
      .updateOne(
        {
          mint
        },
        {
          $set: {

            securityChecked:
              true,

            securityAttempts:
              attempts,

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
// DEX HTTPS
// ======================================================

function httpsJson(url) {

  return new Promise(
    (resolve, reject) => {

      const req =
        https.get(
          url,
          {
            family: 4,
            timeout: 10000,

            headers: {

              Accept:
                "application/json",

              "User-Agent":
                "LOMY-Solana-Hunter/4.5.1",

              Connection:
                "close"
            }
          },
          response => {

            let body = "";

            response
              .setEncoding(
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
                  response.statusCode <
                    200 ||
                  response.statusCode >=
                    300
                ) {

                  return reject(
                    new Error(
                      `DEX HTTP ${response.statusCode}`
                    )
                  );
                }

                try {

                  resolve(
                    JSON.parse(
                      body
                    )
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
    }
  );
}

async function fetchPairs(
  mint
) {

  const url =
    "https://api.dexscreener.com/" +
    "token-pairs/v1/solana/" +
    encodeURIComponent(
      mint
    );

  let lastError =
    null;

  for (
    let attempt = 1;
    attempt <= 5;
    attempt++
  ) {

    try {

      const data =
        await httpsJson(
          url
        );

      return Array.isArray(
        data
      )
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
        `⚠️ DEX retry ${attempt}/5`
      );

      await sleep(
        delay
      );
    }
  }

  throw lastError;
}

function bestPool(
  pairs
) {

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
    dexRecheck.has(
      mint
    )
  ) {
    return;
  }

  const timer =
    setTimeout(
      () => {

        dexRecheck
          .delete(
            mint
          );

        dexScan(
          mint
        )
          .catch(
            err =>
              errLog(
                "DEX recheck",
                err
              )
          );

      },
      45000
    );

  dexRecheck.set(
    mint,
    timer
  );
}

// ======================================================
// DEX ENGINE
// ======================================================

async function dexScan(
  mint
) {

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

    if (
      !pair
    ) {

      await FreshToken
        .updateOne(
          {
            mint
          },
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
        pair?.liquidity
          ?.usd
      );

    const volumeM5 =
      num(
        pair?.volume
          ?.m5
      );

    const volumeH1 =
      num(
        pair?.volume
          ?.h1
      );

    const buys =
      num(
        pair?.txns
          ?.m5
          ?.buys
      );

    const sells =
      num(
        pair?.txns
          ?.m5
          ?.sells
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
      liquidity >=
        3000 &&
      dexScore >=
        60 &&
      sells > 0
    ) {

      dexDecision =
        "PASS";
    }

    const securityScore =
      num(
        token.securityScore
      );

    const preWhaleScore =
      Math.round(
        securityScore *
          0.60 +
        dexScore *
          0.40
      );

    let finalDecision =
      "WATCH";

    if (
      token.securityDecision ===
        "PASS" &&
      dexDecision ===
        "PASS" &&
      preWhaleScore >=
        70
    ) {

      finalDecision =
        "CANDIDATE_PENDING_WHALE";
    }

    await FreshToken
      .updateOne(
        {
          mint
        },
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

            preWhaleScore,

            finalScore:
              preWhaleScore,

            finalDecision
          }
        }
      );

    log(
      `💧 ${mint} | PreWhale ${preWhaleScore}/100 | ${finalDecision}`
    );

    if (
      finalDecision ===
      "CANDIDATE_PENDING_WHALE"
    ) {

      queueWhale(
        mint
      );
    }

  } catch (err) {

    errLog(
      `DEX ${mint}`,
      err
    );

    await FreshToken
      .updateOne(
        {
          mint
        },
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
      )
      .catch(
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
// WHALE QUEUE
// ======================================================

const whaleQueue = [];

const whaleQueuedMints =
  new Set();

let whaleWorkerBusy =
  false;

function queueWhale(
  mint
) {

  if (
    whaleQueuedMints.has(
      mint
    )
  ) {
    return;
  }

  whaleQueuedMints.add(
    mint
  );

  whaleQueue.push(
    mint
  );

  FreshToken
    .updateOne(
      {
        mint
      },
      {
        $set: {
          whaleStatus:
            "QUEUED"
        }
      }
    )
    .catch(
      () => {}
    );

  runWhaleWorker()
    .catch(
      err =>
        errLog(
          "Whale Worker",
          err
        )
    );
}

async function runWhaleWorker() {

  if (
    whaleWorkerBusy
  ) {
    return;
  }

  whaleWorkerBusy =
    true;

  try {

    while (
      whaleQueue.length
    ) {

      const mint =
        whaleQueue.shift();

      whaleQueuedMints
        .delete(
          mint
        );

      state.whales =
        "scanning";

      try {

        await whaleScan(
          mint
        );

      } catch (err) {

        state.whaleRetries++;

        errLog(
          `Whale ${mint}`,
          err
        );

        const token =
          await FreshToken
            .findOne({
              mint
            })
            .lean()
            .catch(
              () => null
            );

        const attempts =
          num(
            token?.whaleAttempts
          );

        await FreshToken
          .updateOne(
            {
              mint
            },
            {
              $set: {

                whaleStatus:
                  "RETRY",

                whaleDecision:
                  "RETRY",

                whaleLastError:
                  err?.message ||
                  String(err)
              }
            }
          )
          .catch(
            () => {}
          );

        if (
          attempts < 5
        ) {

          scheduleWhaleRetry(
            mint
          );
        }
      }

      await sleep(
        2500
      );
    }

  } finally {

    whaleWorkerBusy =
      false;

    state.whales =
      "idle";
  }
}

// ======================================================
// WHALE SCORE
// ======================================================

function holderPercentage(
  amount,
  totalSupply
) {

  try {

    const a =
      BigInt(
        amount || "0"
      );

    const s =
      BigInt(
        totalSupply || "0"
      );

    if (
      s <= 0n
    ) {
      return 0;
    }

    const bp =
      (
        a *
        10000n
      ) / s;

    return (
      Number(bp) /
      100
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

  let decision =
    "DANGER";

  if (
    score >= 75
  ) {

    decision =
      "SAFE";

  } else if (
    score >= 50
  ) {

    decision =
      "CAUTION";
  }

  return {
    score,
    decision,
    flags
  };
}

// ======================================================
// WHALE ENGINE
// ======================================================

async function whaleScan(
  mint
) {

  const token =
    await FreshToken
      .findOne({
        mint
      })
      .lean();

  if (!token) return;

  const attempts =
    num(
      token.whaleAttempts
    ) + 1;

  await FreshToken
    .updateOne(
      {
        mint
      },
      {
        $set: {

          whaleStatus:
            "SCANNING",

          whaleAttempts:
            attempts,

          whaleLastError:
            null
        }
      }
    );

  log(
    `🐋 Whale scanning ${mint} attempt ${attempts}`
  );

  const supplyResponse =
    await rpcCall(
      () =>
        connection
          .getTokenSupply(
            new PublicKey(
              mint
            ),
            "confirmed"
          ),
      0,
      "WHALE_SUPPLY"
    );

  const totalSupply =
    String(
      supplyResponse
        ?.value
        ?.amount ||
      token.supply ||
      "0"
    );

  if (
    totalSupply ===
    "0"
  ) {

    throw new Error(
      "Token supply unavailable"
    );
  }

  const largestResponse =
    await rpcCall(
      () =>
        connection
          .getTokenLargestAccounts(
            new PublicKey(
              mint
            ),
            "confirmed"
          ),
      0,
      "WHALE_LARGEST"
    );

  const accounts =
    (
      largestResponse
        ?.value ||
      []
    ).slice(
      0,
      10
    );

  if (
    !accounts.length
  ) {

    throw new Error(
      "No holder accounts"
    );
  }

  const publicKeys =
    accounts.map(
      holder =>
        new PublicKey(
          holder.address
        )
    );

  const parsed =
    await rpcCall(
      () =>
        connection
          .getMultipleParsedAccounts(
            publicKeys,
            "confirmed"
          ),
      0,
      "WHALE_OWNERS"
    );

  const holders = [];
  const owners =
    new Set();

  for (
    let i = 0;
    i < accounts.length;
    i++
  ) {

    const holder =
      accounts[i];

    const account =
      parsed
        ?.value
        ?.[i];

    const owner =
      account
        ?.data
        ?.parsed
        ?.info
        ?.owner ||
      "UNKNOWN";

    if (
      owner !==
      "UNKNOWN"
    ) {

      owners.add(
        owner
      );
    }

    const amount =
      String(
        holder.amount ||
        "0"
      );

    holders.push({
      rank:
        i + 1,

      tokenAccount:
        holder.address
          .toString(),

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
    num(
      holders[0]
        ?.percent
    );

  const top5 =
    holders
      .slice(
        0,
        5
      )
      .reduce(
        (
          total,
          h
        ) =>
          total +
          num(
            h.percent
          ),
        0
      );

  const top10 =
    holders
      .reduce(
        (
          total,
          h
        ) =>
          total +
          num(
            h.percent
          ),
        0
      );

  const previous =
    num(
      token.top10Pct
    );

  const change =
    token.whaleChecked
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

  if (
    change >= 2
  ) {

    trend =
      "ACCUMULATION";

  } else if (
    change <= -2
  ) {

    trend =
      "DISTRIBUTION";
  }

  const result =
    calculateWhaleScore({
      largest,
      top10,

      uniqueOwners:
        owners.size,

      change
    });

  const smartScore =
    calculateSmartScore(
      token.securityScore,
      token.dexScore,
      result.score
    );

  const finalDecision =
    smartDecision(
      smartScore,
      result.decision
    );

  await FreshToken
    .updateOne(
      {
        mint
      },
      {
        $set: {

          whaleStatus:
            "DONE",

          whaleChecked:
            true,

          whaleCheckedAt:
            new Date(),

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
            token.whaleChecked
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
            result.flags,

          whaleLastError:
            null,

          smartScore,

          finalScore:
            smartScore,

          finalDecision
        }
      }
    );

  state.whaleScanned++;

  log(
    `🧠 SMART ${mint} | Security ${num(token.securityScore)} | DEX ${num(token.dexScore)} | Whale ${result.score} | FINAL ${smartScore}/100 | ${finalDecision}`
  );

  log(
    `🐋 DONE ${mint} | ${result.score}/100 ${result.decision} | Top10 ${top10.toFixed(2)}%`
  );

  if (
    attempts < 3
  ) {

    scheduleWhaleSnapshot(
      mint
    );
  }
}

// ======================================================
// WHALE RETRY
// ======================================================

function scheduleWhaleRetry(
  mint
) {

  const key =
    `retry:${mint}`;

  if (
    whaleRecheck.has(
      key
    )
  ) {
    return;
  }

  const timer =
    setTimeout(
      () => {

        whaleRecheck
          .delete(
            key
          );

        queueWhale(
          mint
        );
      },
      60000
    );

  whaleRecheck.set(
    key,
    timer
  );
}

function scheduleWhaleSnapshot(
  mint
) {

  const key =
    `snapshot:${mint}`;

  if (
    whaleRecheck.has(
      key
    )
  ) {
    return;
  }

  const timer =
    setTimeout(
      () => {

        whaleRecheck
          .delete(
            key
          );

        queueWhale(
          mint
        );
      },
      180000
    );

  whaleRecheck.set(
    key,
    timer
  );
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

  lastWsHeartbeat =
    Date.now();

  state.websocket =
    "online";

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

  if (
    !relevant
  ) {
    return;
  }

  if (
    rpcQueue.length >=
    RPC_SOFT_LIMIT
  ) {

    state.rpcDropped++;

    return;
  }

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
      i < 3;
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
              ),
          2,
          "HUNTER_TX"
        )
        .catch(
          () => null
        );

      if (tx) {
        break;
      }

      await sleep(
        700 +
        i * 700
      );
    }

    if (!tx) {
      return;
    }

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

        if (mint) {
          break;
        }
      }
    }

    if (!mint) {
      return;
    }

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

      await FreshToken
        .create({
          mint,
          signature,
          program,
          paperOnly:
            true
        });

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

    if (
      !String(
        err?.message
      ).includes(
        "RPC_QUEUE_BUSY"
      )
    ) {

      errLog(
        "Hunter processing",
        err
      );
    }

  } finally {

    processing.delete(
      signature
    );
  }
}

// ======================================================
// WEBSOCKET
// ======================================================

async function removeHunterSubscriptions() {

  try {

    if (
      tokenSub !== null
    ) {

      await connection
        .removeOnLogsListener(
          tokenSub
        );

      tokenSub = null;
    }

  } catch {}

  try {

    if (
      token2022Sub !==
      null
    ) {

      await connection
        .removeOnLogsListener(
          token2022Sub
        );

      token2022Sub =
        null;
    }

  } catch {}

  try {

    if (
      slotSub !== null
    ) {

      await connection
        .removeSlotChangeListener(
          slotSub
        );

      slotSub = null;
    }

  } catch {}
}

async function startHunter() {

  if (
    hunterRestarting
  ) {
    return;
  }

  try {

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

    slotSub =
      connection
        .onSlotChange(
          () => {

            lastWsHeartbeat =
              Date.now();

            state.websocket =
              "online";
          }
        );

    state.hunter =
      "running";

    state.websocket =
      "online";

    lastWsHeartbeat =
      Date.now();

    log(
      "🔎 Hunter + WebSocket running"
    );

  } catch (err) {

    state.hunter =
      "error";

    state.websocket =
      "error";

    errLog(
      "Hunter startup",
      err
    );
  }
}

async function restartHunter(
  reason
) {

  if (
    hunterRestarting ||
    shuttingDown
  ) {
    return;
  }

  hunterRestarting =
    true;

  state.websocket =
    "reconnecting";

  log(
    `🔄 WebSocket restart: ${reason}`
  );

  try {

    await removeHunterSubscriptions();

    await sleep(
      5000
    );

    await startHunter();

    log(
      "✅ WebSocket reconnected"
    );

  } catch (err) {

    errLog(
      "WebSocket reconnect",
      err
    );

  } finally {

    hunterRestarting =
      false;
  }
}

setInterval(
  () => {

    if (
      shuttingDown
    ) {
      return;
    }

    const silentFor =
      Date.now() -
      lastWsHeartbeat;

    if (
      silentFor >
      60000
    ) {

      restartHunter(
        "heartbeat timeout"
      );
    }

  },
  30000
);

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

    const whales =
      await FreshToken
        .find({
          finalDecision:
            "CANDIDATE_PENDING_WHALE",

          $or: [

            {
              whaleChecked: {
                $ne: true
              }
            },

            {
              whaleStatus:
                "RETRY"
            }
          ]
        })
        .sort({
          preWhaleScore:
            -1
        })
        .limit(10)
        .select("mint")
        .lean();

    log(
      `🐋 Whale recovery ${whales.length}`
    );

    for (
      const token
      of whales
    ) {

      queueWhale(
        token.mint
      );
    }

    await sleep(
      5000
    );

    const security =
      await FreshToken
        .find({
          securityChecked:
            false
        })
        .sort({
          detectedAt:
            -1
        })
        .limit(5)
        .select("mint")
        .lean();

    for (
      const token
      of security
    ) {

      await securityScan(
        token.mint
      );

      await sleep(
        1800
      );
    }

  } catch (err) {

    errLog(
      "Recovery",
      err
    );
  }
}

// ======================================================
// TELEGRAM COMMANDS
// ======================================================

function registerTelegramCommands() {

  bot.start(
    ctx =>
      ctx.reply(

        "🧠 LOMY V4.5.1\n\n" +

        "🔎 Hunter ON\n" +
        "🛡 Security ON\n" +
        "💧 DEX ON\n" +
        "🐋 Whale Engine ON\n" +
        "🧠 Smart Final Score ON\n" +
        "🔄 Migration Fix ON\n" +
        "🌐 Helius RPC ON\n" +
        "📡 Telegram IPv4 ON\n\n" +

        "🧪 PAPER MODE\n" +
        "🔒 NO BUY / NO SELL"
      )
  );

  bot.command(
    "status",
    ctx =>
      ctx.reply(

        `🧠 LOMY V4.5.1 STATUS\n\n` +

        `🌐 Server: ${state.server}\n` +
        `🗄 Database: ${state.database}\n` +
        `👛 Wallet: ${state.wallet}\n` +
        `⚡ Solana: ${state.solana}\n` +
        `📡 Telegram: ${state.telegram}\n` +
        `🔌 WebSocket: ${state.websocket}\n\n` +

        `🔎 Hunter: ${state.hunter}\n` +
        `🛡 Security: ${state.security}\n` +
        `💧 DEX: ${state.dex}\n` +
        `🐋 Whales: ${state.whales}\n\n` +

        `RPC Queue: ${rpcQueue.length}\n` +
        `RPC 429: ${state.rpc429}\n` +
        `RPC Retries: ${state.rpcRetries}\n` +
        `RPC Dropped: ${state.rpcDropped}\n` +
        `RPC Delay: ${Math.round(rpcDelay)}ms\n\n` +

        `🔄 Migrated: ${state.migrated}\n` +
        `Telegram Retries: ${state.telegramRetries}\n\n` +

        `🧪 PAPER MODE\n` +
        `🔒 LIVE TRADING OFF`
      )
  );

  bot.command(
    "network",
    ctx =>
      ctx.reply(

        `🌐 NETWORK STATUS\n\n` +

        `⚡ Solana: ${state.solana}\n` +
        `🔌 WebSocket: ${state.websocket}\n` +
        `📡 Telegram: ${state.telegram}\n\n` +

        `RPC Queue: ${rpcQueue.length}\n` +
        `RPC 429: ${state.rpc429}\n` +
        `RPC Retries: ${state.rpcRetries}\n` +
        `RPC Dropped: ${state.rpcDropped}\n` +
        `RPC Delay: ${Math.round(rpcDelay)}ms\n\n` +

        `🐋 Whale Queue: ${whaleQueue.length}\n` +
        `🐋 Whale Retries: ${state.whaleRetries}\n\n` +

        `DEX Errors: ${state.dexNetworkErrors}\n` +
        `Telegram Retries: ${state.telegramRetries}`
      )
  );

  bot.command(
    "candidates",
    async ctx => {

      const tokens =
        await FreshToken
          .find({
            finalDecision:
              "APPROVED_CANDIDATE"
          })
          .sort({
            smartScore:
              -1,

            liquidityUsd:
              -1
          })
          .limit(10)
          .lean();

      if (
        !tokens.length
      ) {

        return ctx.reply(
          "🎯 لا توجد Approved Candidates حالياً.\n\n🐋 العملة لازم تنجح في فحص الحيتان أولاً."
        );
      }

      let text =
        `✅ APPROVED CANDIDATES (${tokens.length})\n`;

      for (
        let i = 0;
        i < tokens.length;
        i++
      ) {

        const t =
          tokens[i];

        text +=

          `\n━━━━━━━━━━━━━━\n` +

          `${i + 1}) ${t.mint}\n\n` +

          `🧠 Smart Score: ${num(t.smartScore)}/100\n` +

          `🛡 Security: ${num(t.securityScore)}/100\n` +
          `💧 DEX: ${num(t.dexScore)}/100\n` +
          `🐋 Whale: ${num(t.whaleScore)}/100 ${t.whaleDecision}\n\n` +

          `💵 Liquidity: $${num(t.liquidityUsd).toFixed(0)}\n` +

          `👤 Largest: ${num(t.largestHolderPct).toFixed(2)}%\n` +
          `👥 Top10: ${num(t.top10Pct).toFixed(2)}%\n\n` +

          `✅ APPROVED`;
      }

      text +=
        "\n\n🧪 PAPER ONLY";

      await ctx.reply(
        text
      );
    }
  );

  bot.command(
    "blocked",
    async ctx => {

      const tokens =
        await FreshToken
          .find({
            finalDecision:
              "BLOCKED_WHALE"
          })
          .sort({
            updatedAt:
              -1
          })
          .limit(10)
          .lean();

      if (
        !tokens.length
      ) {

        return ctx.reply(
          "✅ مفيش عملات Blocked by Whale حالياً."
        );
      }

      let text =
        `⛔ WHALE BLOCKED (${tokens.length})\n`;

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

          `🧠 Smart: ${num(t.smartScore)}/100\n` +
          `🐋 Whale: ${num(t.whaleScore)}/100 ${t.whaleDecision}\n` +

          `👤 Largest: ${num(t.largestHolderPct).toFixed(2)}%\n` +
          `👥 Top10: ${num(t.top10Pct).toFixed(2)}%\n`;
      }

      text +=
        "\n🔒 BLOCKED FROM APPROVAL";

      await ctx.reply(
        text
      );
    }
  );

  bot.command(
    "whales",
    async ctx => {

      const tokens =
        await FreshToken
          .find({
            whaleStatus:
              "DONE",
            whaleChecked:
              true
          })
          .sort({
            updatedAt:
              -1
          })
          .limit(5)
          .lean();

      if (
        !tokens.length
      ) {

        return ctx.reply(
          "🐋 WHALE ENGINE\n\n⏳ لا توجد نتائج مكتملة حتى الآن."
        );
      }

      let text =
        "🐋 WHALE + SMART REPORT\n";

      for (
        let i = 0;
        i < tokens.length;
        i++
      ) {

        const t =
          tokens[i];

        text +=

          `\n━━━━━━━━━━━━━━\n` +

          `${i + 1}) ${t.mint}\n\n` +

          `🧠 Smart: ${num(t.smartScore)}/100\n` +
          `🎯 Final Decision: ${t.finalDecision}\n\n` +

          `🛡 Security: ${num(t.securityScore)}/100\n` +
          `💧 DEX: ${num(t.dexScore)}/100\n` +
          `🐋 Whale: ${num(t.whaleScore)}/100 ${t.whaleDecision}\n\n` +

          `👤 Largest: ${num(t.largestHolderPct).toFixed(2)}%\n` +
          `👥 Top 5: ${num(t.top5Pct).toFixed(2)}%\n` +
          `👥 Top 10: ${num(t.top10Pct).toFixed(2)}%\n` +

          `🔄 Change: ${num(t.top10ChangePct).toFixed(2)}%\n` +
          `📈 Trend: ${t.whaleTrend}\n` +
          `👛 Owners: ${t.whaleUniqueOwners || 0}\n`;
      }

      text +=
        "\n🧪 PAPER ONLY";

      await ctx.reply(
        text
      );
    }
  );

  bot.command(
    "stats",
    async ctx => {

      const [
        total,
        pendingWhale,
        approved,
        blocked,
        whaleWatch,
        scoreWatch,
        whaleDone,
        safe,
        caution,
        danger
      ] =
      await Promise.all([

        FreshToken
          .countDocuments(),

        FreshToken
          .countDocuments({
            finalDecision:
              "CANDIDATE_PENDING_WHALE"
          }),

        FreshToken
          .countDocuments({
            finalDecision:
              "APPROVED_CANDIDATE"
          }),

        FreshToken
          .countDocuments({
            finalDecision:
              "BLOCKED_WHALE"
          }),

        FreshToken
          .countDocuments({
            finalDecision:
              "WATCH_WHALE"
          }),

        FreshToken
          .countDocuments({
            finalDecision:
              "WATCH_SCORE"
          }),

        FreshToken
          .countDocuments({
            whaleStatus:
              "DONE"
          }),

        FreshToken
          .countDocuments({
            whaleDecision:
              "SAFE"
          }),

        FreshToken
          .countDocuments({
            whaleDecision:
              "CAUTION"
          }),

        FreshToken
          .countDocuments({
            whaleDecision:
              "DANGER"
          })
      ]);

      await ctx.reply(

        `📊 LOMY V4.5.1 STATS\n\n` +

        `Total Tokens: ${total}\n\n` +

        `⏳ Pending Whale: ${pendingWhale}\n` +
        `✅ Approved: ${approved}\n` +
        `⛔ Whale Blocked: ${blocked}\n` +
        `⚠️ Whale Watch: ${whaleWatch}\n` +
        `👀 Score Watch: ${scoreWatch}\n\n` +

        `🐋 Whale DONE: ${whaleDone}\n` +
        `SAFE: ${safe} ✅\n` +
        `CAUTION: ${caution} ⚠️\n` +
        `DANGER: ${danger} ❌\n\n` +

        `🔄 Migrated: ${state.migrated}\n\n` +

        `RPC 429: ${state.rpc429}\n` +
        `RPC Dropped: ${state.rpcDropped}\n` +
        `Errors: ${state.errors}\n\n` +

        `🧪 PAPER MODE`
      );
    }
  );

  bot.command(
    "balance",
    async ctx => {

      if (
        !wallet
      ) {

        return ctx.reply(
          "❌ Wallet unavailable"
        );
      }

      try {

        const balance =
          await rpcCall(
            () =>
              connection
                .getBalance(
                  wallet.publicKey
                ),
            0,
            "BALANCE"
          );

        await ctx.reply(
          `💰 الرصيد: ${(balance / LAMPORTS_PER_SOL).toFixed(6)} SOL`
        );

      } catch {

        await ctx.reply(
          "❌ Balance unavailable"
        );
      }
    }
  );

  bot.catch(
    err =>
      errLog(
        "Telegram command",
        err
      )
  );
}

// ======================================================
// TELEGRAM ENGINE
// ======================================================

async function startTelegram() {

  if (
    !TELEGRAM_TOKEN
  ) {

    state.telegram =
      "missing_config";

    return;
  }

  bot =
    new Telegraf(
      TELEGRAM_TOKEN,
      {
        telegram: {
          agent:
            telegramAgent
        }
      }
    );

  registerTelegramCommands();

  state.telegram =
    "connecting";

  while (
    !shuttingDown
  ) {

    try {

      log(
        "📡 Connecting Telegram via IPv4..."
      );

      const me =
        await bot.telegram
          .getMe();

      log(
        `✅ Telegram API reached: @${me.username || "BOT"}`
      );

      state.telegram =
        "online";

      bot.launch({
        dropPendingUpdates:
          false
      })
      .catch(
        err => {

          state.telegram =
            "error";

          state.telegramRetries++;

          errLog(
            "Telegram polling",
            err
          );
        }
      );

      log(
        "✅ Telegram online"
      );

      return;

    } catch (err) {

      state.telegram =
        "retrying";

      state.telegramRetries++;

      console.error(
        new Date().toISOString(),
        "⚠️ Telegram connection failed",
        {
          message:
            err?.message ||
            null,

          code:
            err?.code ||
            null,

          errno:
            err?.errno ||
            null,

          type:
            err?.type ||
            null,

          cause:
            err?.cause
              ?.message ||
            null,

          causeCode:
            err?.cause
              ?.code ||
            null
        }
      );

      await sleep(
        10000
      );
    }
  }
}

// ======================================================
// SHUTDOWN
// ======================================================

async function shutdown(
  signal
) {

  if (
    shuttingDown
  ) {
    return;
  }

  shuttingDown =
    true;

  log(
    `⚠️ ${signal} safe shutdown`
  );

  try {

    await removeHunterSubscriptions();

  } catch {}

  for (
    const timer
    of dexRecheck.values()
  ) {

    clearTimeout(
      timer
    );
  }

  for (
    const timer
    of whaleRecheck.values()
  ) {

    clearTimeout(
      timer
    );
  }

  try {

    if (bot) {

      bot.stop(
        signal
      );
    }

  } catch {}

  try {

    telegramAgent.destroy();

  } catch {}

  try {

    await mongoose.disconnect();

  } catch {}

  if (
    server
  ) {

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

// ======================================================
// ERRORS
// ======================================================

process.on(
  "unhandledRejection",
  reason => {

    errLog(
      "Unhandled rejection",

      reason instanceof Error
        ?
        reason
        :
        new Error(
          String(
            reason
          )
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
    "================================"
  );

  console.log(
    "🚀 LOMY SOLANA HUNTER V4.5.1"
  );

  console.log(
    "🧠 SMART FINAL SCORE"
  );

  console.log(
    "🔄 MIGRATION FIX"
  );

  console.log(
    "🛡 SECURITY 45%"
  );

  console.log(
    "💧 DEX 35%"
  );

  console.log(
    "🐋 WHALE 20%"
  );

  console.log(
    "⛔ WHALE DANGER = HARD BLOCK"
  );

  console.log(
    "🧪 PAPER MODE"
  );

  console.log(
    "🔒 LIVE TRADING DISABLED"
  );

  console.log(
    "================================"
  );

  await startServer();

  await connectDatabase();

  await loadWallet();

  await testSolana();

  // يصلح نتائج النسخ القديمة
  // بدون أي RPC إضافي
  await migrateOldSmartScores();

  startTelegram()
    .catch(
      err =>
        errLog(
          "Telegram background",
          err
        )
    );

  if (
    state.solana ===
    "connected"
  ) {

    await startHunter();
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
    "✅ LOMY V4.5.1 STARTED"
  );

  log(
    "🧠 SMART FINAL SCORE ACTIVE"
  );

  log(
    "🔄 OLD DATA MIGRATION COMPLETE"
  );

  log(
    "🔒 NO REAL BUY / NO REAL SELL"
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
