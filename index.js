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
const RPC_URL = process.env.RPC_URL || "https://api.mainnet-beta.solana.com";
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
// PAPER TRADING V1
// ======================================================

const PAPER = {
  enabled: true,
  testRun: "V4.6.1",
  accountKey: "v461-main",
  startingBalanceUsd: 20,
  maxOpenTrades: 4,
  positionSizeUsd: 5,
  hardStopPct: 12,
  takeProfitPct: 100,
  trailingActivationPct: 15,
  trailingDistancePct: 8,
  assumedSlippagePct: 1.5,
  assumedFeePct: 0.5,
  monitorMs: 20000,
  maxTradeAgeMinutes: 180,
  minLiquidityUsd: 3000,

  // Continuous Opportunity Engine
  opportunityMs: 60000,
  opportunityBatch: 15,
  cooldownMinutes: 90,
  maxEntriesPerMint24h: 2,

  // Entry quality filter
  minVolumeM5: 300,
  minBuysM5: 5,
  minBuySellRatio: 1.05,
  minPriceChangeM5: -3,
  maxPriceChangeM5: 25
};

// ======================================================
// DUAL MARKET SCANNER V1
// ======================================================

const MARKET_SCANNER = {
  enabled: true,
  scanMs: 10 * 60 * 1000,
  externalRawLimit: 18,
  maxNewEstablishedPerCycle: 3,
  maxDbRescansPerCycle: 4,
  oldAgeMinutes: 60,
  minLiquidityUsd: 10000,
  minVolumeH1Usd: 5000,
  precheckDelayMs: 350
};

// Fresh Hunter throttling protects RPC.
// Established scanner can recover good opportunities later.
const HUNTER_MAX_TX_PER_MINUTE = 25;

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

const connection = new Connection(RPC_URL, {
  commitment: "confirmed",
  confirmTransactionInitialTimeout: 30000
});

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
let paperMonitorTimer = null;
let opportunityTimer = null;
let marketScannerTimer = null;

let hunterRestarting = false;
let shuttingDown = false;
let lastWsHeartbeat = Date.now();

const processing = new Set();
const dexRecheck = new Map();
const whaleRecheck = new Map();
const paperOpening = new Set();

let hunterMinuteWindow = Date.now();
let hunterMinuteCount = 0;
let opportunityBusy = false;
let marketScannerBusy = false;

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
  paper: "starting",

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

  paperOpened: 0,
  paperClosed: 0,
  paperEntryAttempts: 0,
  paperEntrySkips: 0,
  paperLastSkip: null,

  opportunityCycles: 0,

  establishedDiscovered: 0,
  establishedScanned: 0,

  hunterThrottled: 0,

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
  return new Promise(
    resolve =>
      setTimeout(
        resolve,
        ms
      )
  );
}

function num(v) {
  const n =
    Number(v);

  return Number.isFinite(n)
    ? n
    : 0;
}

function is429(err) {
  const text =
    String(
      err?.message ||
      err
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
      Math.random() *
      300
    )
  );
}

function pctChange(
  current,
  base
) {
  if (
    !base ||
    base <= 0
  ) {
    return 0;
  }

  return (
    (
      current -
      base
    ) /
    base
  ) * 100;
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
    num(
      securityScore
    ) *
    SCORE_WEIGHTS.security +

    num(
      dexScore
    ) *
    SCORE_WEIGHTS.dex +

    num(
      whaleScore
    ) *
    SCORE_WEIGHTS.whale;

  return Math.max(
    0,
    Math.min(
      100,
      Math.round(
        score
      )
    )
  );
}

function smartDecision(
  smartScore,
  whaleDecision
) {
  if (
    whaleDecision ===
    "DANGER"
  ) {
    return "BLOCKED_WHALE";
  }

  if (
    whaleDecision ===
    "CAUTION"
  ) {
    return "WATCH_WHALE";
  }

  if (
    whaleDecision ===
      "SAFE" &&
    smartScore >=
      SMART_APPROVAL_SCORE
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
    (
      resolve,
      reject
    ) => {

      if (
        priority === 2 &&
        rpcQueue.length >=
          RPC_SOFT_LIMIT
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
        createdAt:
          Date.now()
      });

      rpcQueue.sort(
        (
          a,
          b
        ) =>
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
  if (
    rpcBusy
  ) {
    return;
  }

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

          job.resolve(
            result
          );

          done = true;

          break;

        } catch (err) {

          lastError =
            err;

          if (
            !is429(
              err
            )
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
                rpcDelay *
                  1.7
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

      if (
        !done
      ) {
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

    rpcBusy =
      false;
  }
}

// ======================================================
// DATABASE MODELS
// ======================================================

const tokenSchema =
  new mongoose.Schema(
    {
      mint: {
        type: String,
        unique: true,
        index: true
      },

      signature:
        String,

      program:
        String,

      origin: {
        type: String,
        default: "FRESH"
      },

      discoveredBy:
        String,

      establishedDiscoveredAt:
        Date,

      detectedAt: {
        type: Date,
        default:
          Date.now
      },

      // SECURITY

      securityChecked: {
        type: Boolean,
        default: false
      },

      securityScore:
        Number,

      securityDecision: {
        type: String,
        default:
          "PENDING"
      },

      securityAttempts: {
        type: Number,
        default: 0
      },

      mintAuthorityRevoked:
        Boolean,

      freezeAuthorityRevoked:
        Boolean,

      decimals:
        Number,

      supply:
        String,

      token2022:
        Boolean,

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

      dexId:
        String,

      pairAddress:
        String,

      priceUsd:
        Number,

      liquidityUsd:
        Number,

      volumeM5:
        Number,

      volumeH1:
        Number,

      buysM5:
        Number,

      sellsM5:
        Number,

      dexScore:
        Number,

      dexDecision: {
        type: String,
        default:
          "PENDING"
      },

      // SMART

      preWhaleScore:
        Number,

      smartScore:
        Number,

      finalScore:
        Number,

      finalDecision: {
        type: String,
        default:
          "PENDING"
      },

      // WHALE

      whaleStatus: {
        type: String,
        default:
          "PENDING"
      },

      whaleChecked: {
        type: Boolean,
        default: false
      },

      whaleCheckedAt:
        Date,

      whaleAttempts: {
        type: Number,
        default: 0
      },

      whaleScore:
        Number,

      whaleDecision: {
        type: String,
        default:
          "PENDING"
      },

      largestHolderPct:
        Number,

      top5Pct:
        Number,

      top10Pct:
        Number,

      previousTop10Pct:
        Number,

      top10ChangePct:
        Number,

      whaleTrend: {
        type: String,
        default:
          "UNKNOWN"
      },

      whaleUniqueOwners:
        Number,

      whaleHolders: {
        type: Array,
        default: []
      },

      whaleFlags: {
        type: [String],
        default: []
      },

      whaleLastError:
        String,

      paperOnly: {
        type: Boolean,
        default: true
      }
    },
    {
      timestamps:
        true
    }
  );

const paperTradeSchema =
  new mongoose.Schema(
    {
      mint: {
        type: String,
        index: true
      },

      testRun: {
        type: String,
        index: true
      },

      source: {
        type: String,
        default:
          "UNKNOWN",
        index: true
      },

      entryTrigger:
        String,

      pairAddress:
        String,

      dexId:
        String,

      status: {
        type: String,
        enum: [
          "OPEN",
          "CLOSED"
        ],
        default:
          "OPEN",
        index: true
      },

      openedAt: {
        type: Date,
        default:
          Date.now
      },

      closedAt:
        Date,

      exitReason:
        String,

      marketEntryPrice:
        Number,

      entryPrice:
        Number,

      currentPrice:
        Number,

      highestPrice:
        Number,

      lowestPrice:
        Number,

      exitMarketPrice:
        Number,

      exitPrice:
        Number,

      allocatedUsd:
        Number,

      quantity:
        Number,

      entryFeeUsd:
        Number,

      exitFeeUsd:
        Number,

      hardStopPrice:
        Number,

      takeProfitPrice:
        Number,

      trailingActive: {
        type: Boolean,
        default: false
      },

      trailingStopPrice:
        Number,

      grossPnlUsd:
        Number,

      netPnlUsd:
        Number,

      pnlPct:
        Number,

      maxRunupPct:
        Number,

      maxDrawdownPct:
        Number,

      securityScore:
        Number,

      dexScore:
        Number,

      whaleScore:
        Number,

      smartScore:
        Number,

      whaleDecision:
        String,

      liquidityAtEntry:
        Number,

      volumeM5AtEntry:
        Number,

      buysM5AtEntry:
        Number,

      sellsM5AtEntry:
        Number,

      buySellRatioAtEntry:
        Number,

      priceChangeM5AtEntry:
        Number,

      priceChangeH1AtEntry:
        Number,

      lastPriceCheckAt:
        Date
    },
    {
      timestamps:
        true
    }
  );

const paperAccountSchema =
  new mongoose.Schema(
    {
      key: {
        type: String,
        unique: true,
        default:
          "main"
      },

      startingBalanceUsd:
        Number,

      cashBalanceUsd:
        Number,

      realizedPnlUsd: {
        type: Number,
        default: 0
      },

      totalTrades: {
        type: Number,
        default: 0
      },

      wins: {
        type: Number,
        default: 0
      },

      losses: {
        type: Number,
        default: 0
      },

      breakeven: {
        type: Number,
        default: 0
      },

      bestTradePct: {
        type: Number,
        default: 0
      },

      worstTradePct: {
        type: Number,
        default: 0
      }
    },
    {
      timestamps:
        true
    }
  );

const FreshToken =
  mongoose.models.FreshToken ||
  mongoose.model(
    "FreshToken",
    tokenSchema
  );

const PaperTrade =
  mongoose.models.PaperTrade ||
  mongoose.model(
    "PaperTrade",
    paperTradeSchema
  );

const PaperAccount =
  mongoose.models.PaperAccount ||
  mongoose.model(
    "PaperAccount",
    paperAccountSchema
  );

// ======================================================
// SERVER
// ======================================================

app.get(
  "/",
  (
    req,
    res
  ) => {

    res.send(
      "✅ LOMY V4.6.1 DUAL MARKET PAPER ENGINE | LIVE OFF"
    );
  }
);

app.get(
  "/health",
  (
    req,
    res
  ) => {

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

      paperEnabled:
        PAPER.enabled,

      uptime:
        Math.floor(
          process.uptime()
        )
    });
  }
);

function startServer() {
  return new Promise(
    (
      resolve,
      reject
    ) => {

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
      BOT_PRIVATE_KEY.trim();

    if (
      key.includes(
        " "
      ) &&
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
      key.startsWith(
        "["
      )
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

  if (
    !wallet
  ) {
    return;
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

    state.solana =
      "connected";

    log(
      "✅ Solana connected",
      (
        balance /
        LAMPORTS_PER_SOL
      ).toFixed(
        6
      ),
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
// V4.5.1 MIGRATION
// ======================================================

async function migrateOldSmartScores() {

  if (
    mongoose.connection
      .readyState !==
    1
  ) {
    return;
  }

  try {

    const oldTokens =
      await FreshToken
        .find({
          whaleChecked:
            true,

          whaleStatus:
            "DONE"
        })
        .select(
          "_id mint securityScore dexScore whaleScore whaleDecision"
        )
        .lean();

    log(
      `🧠 Migration found ${oldTokens.length} whale-completed tokens`
    );

    let migrated =
      0;

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
    }

    state.migrated =
      migrated;

    log(
      `✅ MIGRATION COMPLETE | Total ${migrated}`
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

    let account =
      null;

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
        i *
        1000
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
      parsed.info ||
      {};

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

    let score =
      50;

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

      score -=
        5;
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
      score >=
      80
    ) {

      decision =
        "PASS";

    } else if (
      score >=
      55
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
// DEX NETWORK
// ======================================================

function httpsJson(
  url
) {

  return new Promise(
    (
      resolve,
      reject
    ) => {

      const req =
        https.get(
          url,
          {
            family: 4,

            timeout:
              10000,

            headers: {

              Accept:
                "application/json",

              "User-Agent":
                "LOMY-Solana-Hunter/4.6.1",

              Connection:
                "close"
            }
          },

          response => {

            let body =
              "";

            response
              .setEncoding(
                "utf8"
              );

            response.on(
              "data",
              chunk => {

                body +=
                  chunk;
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

      lastError =
        err;

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

  return [
    ...pairs
  ]
    .filter(
      p =>
        p?.chainId ===
        "solana"
    )
    .sort(
      (
        a,
        b
      ) =>
        num(
          b?.liquidity
            ?.usd
        ) -
        num(
          a?.liquidity
            ?.usd
        )
    )[0] ||
    null;
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

    if (
      !token
    ) {
      return;
    }

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
        attempts <
        5
      ) {

        scheduleDexRecheck(
          mint
        );
      }

      return;
    }

    const priceUsd =
      num(
        pair?.priceUsd
      );

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

    let dexScore =
      0;

    if (
      liquidity >=
      10000
    ) {

      dexScore +=
        40;

    } else if (
      liquidity >=
      3000
    ) {

      dexScore +=
        25;
    }

    if (
      volumeM5 >=
      250
    ) {

      dexScore +=
        20;
    }

    if (
      volumeH1 >
      0
    ) {

      dexScore +=
        10;
    }

    if (
      buys +
      sells >=
      5
    ) {

      dexScore +=
        15;
    }

    if (
      buys >
      0 &&
      sells >
      0
    ) {

      dexScore +=
        15;
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
      sells >
        0
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

            priceUsd,

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
    whaleQueuedMints
      .has(
        mint
      )
  ) {
    return;
  }

  whaleQueuedMints
    .add(
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
            token
              ?.whaleAttempts
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
                  String(
                    err
                  )
              }
            }
          )
          .catch(
            () => {}
          );

        if (
          attempts <
          5
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
        amount ||
        "0"
      );

    const s =
      BigInt(
        totalSupply ||
        "0"
      );

    if (
      s <=
      0n
    ) {
      return 0;
    }

    const bp =
      (
        a *
        10000n
      ) /
      s;

    return (
      Number(
        bp
      ) /
      100
    );

  } catch {

    return 0;
  }
}

function calculateWhaleScore(
  data
) {

  let score =
    100;

  const flags =
    [];

  if (
    data.largest >=
    25
  ) {

    score -=
      45;

    flags.push(
      "VERY_LARGE_SINGLE_HOLDER"
    );

  } else if (
    data.largest >=
    15
  ) {

    score -=
      30;

    flags.push(
      "LARGE_SINGLE_HOLDER"
    );

  } else if (
    data.largest >=
    10
  ) {

    score -=
      15;

    flags.push(
      "SINGLE_HOLDER_CAUTION"
    );
  }

  if (
    data.top10 >=
    80
  ) {

    score -=
      40;

    flags.push(
      "EXTREME_TOP10_CONCENTRATION"
    );

  } else if (
    data.top10 >=
    60
  ) {

    score -=
      25;

    flags.push(
      "HIGH_TOP10_CONCENTRATION"
    );

  } else if (
    data.top10 >=
    45
  ) {

    score -=
      10;

    flags.push(
      "MEDIUM_TOP10_CONCENTRATION"
    );
  }

  if (
    data.uniqueOwners <
    5
  ) {

    score -=
      15;

    flags.push(
      "LOW_OWNER_DIVERSITY"
    );
  }

  if (
    data.change >=
    5
  ) {

    score -=
      15;

    flags.push(
      "CONCENTRATION_INCREASING"
    );

  } else if (
    data.change <=
    -5
  ) {

    score +=
      5;

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
    score >=
    75
  ) {

    decision =
      "SAFE";

  } else if (
    score >=
    50
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
// WHALE ENGINE + SMART DECISION
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

  if (
    !token
  ) {
    return;
  }

  const attempts =
    num(
      token
        .whaleAttempts
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

  const holders =
    [];

  const owners =
    new Set();

  for (
    let i = 0;
    i <
    accounts.length;
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
        ).toFixed(
          2
        )
      )
      :
      0;

  let trend =
    "STABLE";

  if (
    change >=
    2
  ) {

    trend =
      "ACCUMULATION";

  } else if (
    change <=
    -2
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
              largest
                .toFixed(
                  2
                )
            ),

          top5Pct:
            Number(
              top5
                .toFixed(
                  2
                )
            ),

          previousTop10Pct:
            token.whaleChecked
              ?
              previous
              :
              null,

          top10Pct:
            Number(
              top10
                .toFixed(
                  2
                )
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

  if (
    finalDecision ===
    "APPROVED_CANDIDATE"
  ) {

    queuePaperEntry(
      mint,
      "APPROVAL_EVENT"
    )
      .catch(
        err =>
          errLog(
            "Paper entry trigger",
            err
          )
      );
  }

  if (
    attempts <
    3
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
// PAPER ACCOUNT
// ======================================================

async function ensurePaperAccount() {

  let account =
    await PaperAccount
      .findOne({
        key:
          PAPER.accountKey
      });

  if (
    !account
  ) {

    account =
      await PaperAccount
        .create({
          key:
            PAPER.accountKey,

          startingBalanceUsd:
            PAPER.startingBalanceUsd,

          cashBalanceUsd:
            PAPER.startingBalanceUsd,

          realizedPnlUsd:
            0,

          totalTrades:
            0,

          wins:
            0,

          losses:
            0,

          breakeven:
            0,

          bestTradePct:
            0,

          worstTradePct:
            0
        });

    log(
      `🧪 Paper account created with $${PAPER.startingBalanceUsd}`
    );
  }

  return account;
}

async function getPaperSummary() {

  const account =
    await ensurePaperAccount();

  const openTrades =
    await PaperTrade
      .find({
        testRun:
          PAPER.testRun,

        status:
          "OPEN"
      })
      .lean();

  let openValue =
    0;

  let unrealizedPnl =
    0;

  for (
    const t
    of openTrades
  ) {

    const current =
      num(
        t.currentPrice ||
        t.entryPrice
      );

    const marketValue =
      num(
        t.quantity
      ) *
      current;

    openValue +=
      marketValue;

    unrealizedPnl +=
      (
        current -
        num(
          t.entryPrice
        )
      ) *
      num(
        t.quantity
      );
  }

  return {
    account,
    openTrades,
    openValue,
    unrealizedPnl,

    equity:
      num(
        account.cashBalanceUsd
      ) +
      openValue
  };
}

// ======================================================
// PAPER ENTRY
// ======================================================

async function getRecentMintTrades(mint) {
  const since =
    new Date(
      Date.now() -
      24 * 60 * 60 * 1000
    );

  return PaperTrade
    .find({
      mint,
      testRun: PAPER.testRun,
      openedAt: {
        $gte: since
      }
    })
    .sort({
      openedAt: -1
    })
    .lean();
}

async function evaluatePaperEntry(
  token,
  pair
) {
  const marketPrice =
    num(
      pair?.priceUsd
    );

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

  const buysM5 =
    num(
      pair?.txns?.m5?.buys
    );

  const sellsM5 =
    num(
      pair?.txns?.m5?.sells
    );

  const priceChangeM5 =
    num(
      pair?.priceChange?.m5
    );

  const priceChangeH1 =
    num(
      pair?.priceChange?.h1
    );

  const buySellRatio =
    sellsM5 > 0
      ? buysM5 / sellsM5
      : buysM5 > 0
        ? buysM5
        : 0;

  if (
    marketPrice <= 0
  ) {
    return {
      ok: false,
      reason: "INVALID_PRICE"
    };
  }

  if (
    liquidity <
    PAPER.minLiquidityUsd
  ) {
    return {
      ok: false,
      reason: "LOW_LIQUIDITY"
    };
  }

  if (
    volumeM5 <
    PAPER.minVolumeM5
  ) {
    return {
      ok: false,
      reason: "LOW_VOLUME_M5"
    };
  }

  if (
    buysM5 <
    PAPER.minBuysM5
  ) {
    return {
      ok: false,
      reason: "LOW_BUYS_M5"
    };
  }

  if (
    buySellRatio <
    PAPER.minBuySellRatio
  ) {
    return {
      ok: false,
      reason: "WEAK_BUY_SELL_RATIO"
    };
  }

  if (
    priceChangeM5 <
    PAPER.minPriceChangeM5
  ) {
    return {
      ok: false,
      reason: "M5_TOO_WEAK"
    };
  }

  if (
    priceChangeM5 >
    PAPER.maxPriceChangeM5
  ) {
    return {
      ok: false,
      reason: "M5_OVEREXTENDED"
    };
  }

  if (
    token.finalDecision !==
    "APPROVED_CANDIDATE"
  ) {
    return {
      ok: false,
      reason: "NOT_APPROVED"
    };
  }

  if (
    token.whaleDecision !==
    "SAFE"
  ) {
    return {
      ok: false,
      reason: "WHALE_NOT_SAFE"
    };
  }

  return {
    ok: true,
    marketPrice,
    liquidity,
    volumeM5,
    volumeH1,
    buysM5,
    sellsM5,
    buySellRatio,
    priceChangeM5,
    priceChangeH1
  };
}

async function queuePaperEntry(
  mint,
  trigger = "UNKNOWN"
) {

  if (
    !PAPER.enabled ||
    LIVE_TRADING
  ) {
    return;
  }

  if (
    paperOpening.has(
      mint
    )
  ) {
    return;
  }

  paperOpening.add(
    mint
  );

  state.paperEntryAttempts++;

  try {

    const existing =
      await PaperTrade
        .findOne({
          mint,
          testRun:
            PAPER.testRun,
          status:
            "OPEN"
        })
        .lean();

    if (
      existing
    ) {
      return;
    }

    const openCount =
      await PaperTrade
        .countDocuments({
          testRun:
            PAPER.testRun,
          status:
            "OPEN"
        });

    if (
      openCount >=
      PAPER.maxOpenTrades
    ) {
      state.paperEntrySkips++;
      state.paperLastSkip =
        "MAX_OPEN_TRADES";

      return;
    }

    const token =
      await FreshToken
        .findOne({
          mint
        })
        .lean();

    if (
      !token ||
      token.finalDecision !==
      "APPROVED_CANDIDATE"
    ) {
      return;
    }

    // ----------------------------------------------
    // COOLDOWN + MAX ENTRIES / 24H
    // ----------------------------------------------

    const recentTrades =
      await getRecentMintTrades(
        mint
      );

    if (
      recentTrades.length >=
      PAPER.maxEntriesPerMint24h
    ) {
      state.paperEntrySkips++;
      state.paperLastSkip =
        "MAX_MINT_ENTRIES_24H";

      return;
    }

    const lastTrade =
      recentTrades[0];

    if (
      lastTrade
    ) {
      const referenceTime =
        new Date(
          lastTrade.closedAt ||
          lastTrade.openedAt
        ).getTime();

      const elapsedMinutes =
        (
          Date.now() -
          referenceTime
        ) /
        60000;

      if (
        elapsedMinutes <
        PAPER.cooldownMinutes
      ) {
        state.paperEntrySkips++;
        state.paperLastSkip =
          "COOLDOWN";

        return;
      }
    }

    // ----------------------------------------------
    // LIVE DEX REFRESH
    // ----------------------------------------------

    const pairs =
      await fetchPairs(
        mint
      );

    const pair =
      bestPool(
        pairs
      );

    if (
      !pair
    ) {
      state.paperEntrySkips++;
      state.paperLastSkip =
        "NO_PAIR";

      return;
    }

    const entryCheck =
      await evaluatePaperEntry(
        token,
        pair
      );

    if (
      !entryCheck.ok
    ) {
      state.paperEntrySkips++;
      state.paperLastSkip =
        entryCheck.reason;

      log(
        `🧪 ENTRY FILTER ${mint} | ${entryCheck.reason}`
      );

      return;
    }

    const account =
      await ensurePaperAccount();

    const allocatedUsd =
      Math.min(
        PAPER.positionSizeUsd,
        num(
          account.cashBalanceUsd
        )
      );

    if (
      allocatedUsd <
      1
    ) {
      state.paperEntrySkips++;
      state.paperLastSkip =
        "INSUFFICIENT_VIRTUAL_CASH";

      return;
    }

    const entryPrice =
      entryCheck.marketPrice *
      (
        1 +
        PAPER.assumedSlippagePct /
        100
      );

    const entryFeeUsd =
      allocatedUsd *
      (
        PAPER.assumedFeePct /
        100
      );

    const capitalForTokens =
      Math.max(
        0,
        allocatedUsd -
        entryFeeUsd
      );

    const quantity =
      capitalForTokens /
      entryPrice;

    const hardStopPrice =
      entryPrice *
      (
        1 -
        PAPER.hardStopPct /
        100
      );

    const takeProfitPrice =
      entryPrice *
      (
        1 +
        PAPER.takeProfitPct /
        100
      );

    const source =
      token.origin ||
      "FRESH";

    await PaperTrade.create({
      mint,

      testRun:
        PAPER.testRun,

      source,

      entryTrigger:
        trigger,

      pairAddress:
        pair.pairAddress ||
        token.pairAddress ||
        null,

      dexId:
        pair.dexId ||
        token.dexId ||
        null,

      status:
        "OPEN",

      marketEntryPrice:
        entryCheck.marketPrice,

      entryPrice,

      currentPrice:
        entryCheck.marketPrice,

      highestPrice:
        entryCheck.marketPrice,

      lowestPrice:
        entryCheck.marketPrice,

      allocatedUsd,
      quantity,
      entryFeeUsd,

      hardStopPrice,
      takeProfitPrice,

      trailingActive:
        false,

      trailingStopPrice:
        null,

      securityScore:
        token.securityScore,

      dexScore:
        token.dexScore,

      whaleScore:
        token.whaleScore,

      smartScore:
        token.smartScore,

      whaleDecision:
        token.whaleDecision,

      liquidityAtEntry:
        entryCheck.liquidity,

      volumeM5AtEntry:
        entryCheck.volumeM5,

      buysM5AtEntry:
        entryCheck.buysM5,

      sellsM5AtEntry:
        entryCheck.sellsM5,

      buySellRatioAtEntry:
        entryCheck.buySellRatio,

      priceChangeM5AtEntry:
        entryCheck.priceChangeM5,

      priceChangeH1AtEntry:
        entryCheck.priceChangeH1,

      lastPriceCheckAt:
        new Date()
    });

    account.cashBalanceUsd =
      Math.max(
        0,
        num(
          account.cashBalanceUsd
        ) -
        allocatedUsd
      );

    await account.save();

    state.paperOpened++;

    log(
      `🧪 PAPER BUY ${mint} | ${source} | ${trigger} | $${allocatedUsd.toFixed(2)} | Entry=${entryPrice} | M5=${entryCheck.priceChangeM5.toFixed(2)}% | B/S=${entryCheck.buySellRatio.toFixed(2)}`
    );

  } catch (err) {

    errLog(
      `Paper entry ${mint}`,
      err
    );

  } finally {

    paperOpening.delete(
      mint
    );
  }
}

// ======================================================
// PAPER EXIT
// ======================================================

async function closePaperTrade(
  trade,
  marketPrice,
  reason
) {

  if (
    !trade ||
    trade.status !==
    "OPEN"
  ) {
    return;
  }

  const exitPrice =
    marketPrice *
    (
      1 -
      PAPER.assumedSlippagePct /
      100
    );

  const grossProceeds =
    num(
      trade.quantity
    ) *
    exitPrice;

  const exitFeeUsd =
    grossProceeds *
    (
      PAPER.assumedFeePct /
      100
    );

  const netProceeds =
    Math.max(
      0,
      grossProceeds -
      exitFeeUsd
    );

  const netPnlUsd =
    netProceeds -
    num(
      trade.allocatedUsd
    );

  const pnlPct =
    num(
      trade.allocatedUsd
    ) > 0
      ?
      (
        netPnlUsd /
        num(
          trade.allocatedUsd
        )
      ) *
      100
      :
      0;

  trade.status =
    "CLOSED";

  trade.closedAt =
    new Date();

  trade.exitReason =
    reason;

  trade.exitMarketPrice =
    marketPrice;

  trade.exitPrice =
    exitPrice;

  trade.currentPrice =
    marketPrice;

  trade.exitFeeUsd =
    exitFeeUsd;

  trade.grossPnlUsd =
    grossProceeds -
    num(
      trade.allocatedUsd
    );

  trade.netPnlUsd =
    netPnlUsd;

  trade.pnlPct =
    pnlPct;

  await trade.save();

  const account =
    await ensurePaperAccount();

  account.cashBalanceUsd =
    num(
      account.cashBalanceUsd
    ) +
    netProceeds;

  account.realizedPnlUsd =
    num(
      account.realizedPnlUsd
    ) +
    netPnlUsd;

  account.totalTrades =
    num(
      account.totalTrades
    ) +
    1;

  if (
    pnlPct >
    0.05
  ) {
    account.wins =
      num(
        account.wins
      ) +
      1;

  } else if (
    pnlPct <
    -0.05
  ) {
    account.losses =
      num(
        account.losses
      ) +
      1;

  } else {
    account.breakeven =
      num(
        account.breakeven
      ) +
      1;
  }

  account.bestTradePct =
    Math.max(
      num(
        account.bestTradePct
      ),
      pnlPct
    );

  account.worstTradePct =
    Math.min(
      num(
        account.worstTradePct
      ),
      pnlPct
    );

  await account.save();

  state.paperClosed++;

  log(
    `🧪 PAPER SELL ${trade.mint} | ${trade.source || "UNKNOWN"} | ${reason} | PnL ${pnlPct.toFixed(2)}% | $${netPnlUsd.toFixed(2)}`
  );
}

// ======================================================
// PAPER MONITOR
// ======================================================

async function monitorPaperTrades() {

  if (
    !PAPER.enabled ||
    shuttingDown
  ) {
    return;
  }

  state.paper =
    "monitoring";

  try {

    const trades =
      await PaperTrade
        .find({
          testRun:
            PAPER.testRun,
          status:
            "OPEN"
        });

    for (
      const trade
      of trades
    ) {

      try {

        const pairs =
          await fetchPairs(
            trade.mint
          );

        const pair =
          bestPool(
            pairs
          );

        if (
          !pair
        ) {
          continue;
        }

        const marketPrice =
          num(
            pair.priceUsd
          );

        if (
          marketPrice <=
          0
        ) {
          continue;
        }

        const highest =
          Math.max(
            num(
              trade.highestPrice
            ),
            marketPrice
          );

        const lowest =
          trade.lowestPrice
            ?
            Math.min(
              num(
                trade.lowestPrice
              ),
              marketPrice
            )
            :
            marketPrice;

        const runupPct =
          pctChange(
            highest,
            num(
              trade.entryPrice
            )
          );

        const drawdownPct =
          pctChange(
            lowest,
            num(
              trade.entryPrice
            )
          );

        let trailingActive =
          Boolean(
            trade.trailingActive
          );

        let trailingStopPrice =
          num(
            trade.trailingStopPrice
          );

        if (
          !trailingActive &&
          runupPct >=
          PAPER.trailingActivationPct
        ) {
          trailingActive =
            true;
        }

        if (
          trailingActive
        ) {

          const candidateStop =
            highest *
            (
              1 -
              PAPER.trailingDistancePct /
              100
            );

          trailingStopPrice =
            Math.max(
              trailingStopPrice ||
              0,
              candidateStop
            );
        }

        trade.currentPrice =
          marketPrice;

        trade.highestPrice =
          highest;

        trade.lowestPrice =
          lowest;

        trade.maxRunupPct =
          runupPct;

        trade.maxDrawdownPct =
          drawdownPct;

        trade.trailingActive =
          trailingActive;

        trade.trailingStopPrice =
          trailingStopPrice ||
          null;

        trade.lastPriceCheckAt =
          new Date();

        await trade.save();

        const ageMinutes =
          (
            Date.now() -
            new Date(
              trade.openedAt
            ).getTime()
          ) /
          60000;

        if (
          marketPrice <=
          num(
            trade.hardStopPrice
          )
        ) {

          await closePaperTrade(
            trade,
            marketPrice,
            "HARD_STOP"
          );

          continue;
        }

        if (
          marketPrice >=
          num(
            trade.takeProfitPrice
          )
        ) {

          await closePaperTrade(
            trade,
            marketPrice,
            "TAKE_PROFIT_100"
          );

          continue;
        }

        if (
          trailingActive &&
          trailingStopPrice > 0 &&
          marketPrice <=
          trailingStopPrice
        ) {

          await closePaperTrade(
            trade,
            marketPrice,
            "TRAILING_STOP"
          );

          continue;
        }

        if (
          ageMinutes >=
          PAPER.maxTradeAgeMinutes
        ) {

          await closePaperTrade(
            trade,
            marketPrice,
            "TIME_EXIT"
          );

          continue;
        }

      } catch (err) {

        errLog(
          `Paper monitor ${trade.mint}`,
          err
        );
      }

      await sleep(
        600
      );
    }

  } catch (err) {

    errLog(
      "Paper monitor",
      err
    );

  } finally {

    state.paper =
      "idle";
  }
}

// ======================================================
// CONTINUOUS OPPORTUNITY ENGINE
// ======================================================

async function scanApprovedOpportunities() {

  if (
    opportunityBusy ||
    shuttingDown ||
    !PAPER.enabled
  ) {
    return;
  }

  opportunityBusy =
    true;

  state.opportunityCycles++;

  try {

    const openCount =
      await PaperTrade
        .countDocuments({
          testRun:
            PAPER.testRun,
          status:
            "OPEN"
        });

    if (
      openCount >=
      PAPER.maxOpenTrades
    ) {
      return;
    }

    const tokens =
      await FreshToken
        .find({
          finalDecision:
            "APPROVED_CANDIDATE",

          whaleDecision:
            "SAFE",

          liquidityUsd: {
            $gte:
              PAPER.minLiquidityUsd
          }
        })
        .sort({
          smartScore: -1,
          volumeM5: -1,
          liquidityUsd: -1
        })
        .limit(
          PAPER.opportunityBatch
        )
        .lean();

    for (
      const token
      of tokens
    ) {

      const currentOpen =
        await PaperTrade
          .countDocuments({
            testRun:
              PAPER.testRun,
            status:
              "OPEN"
          });

      if (
        currentOpen >=
        PAPER.maxOpenTrades
      ) {
        break;
      }

      await queuePaperEntry(
        token.mint,
        "OPPORTUNITY_RESCAN"
      );

      await sleep(
        350
      );
    }

  } catch (err) {

    errLog(
      "Opportunity Engine",
      err
    );

  } finally {

    opportunityBusy =
      false;
  }
}

function startOpportunityEngine() {

  if (
    opportunityTimer
  ) {
    clearInterval(
      opportunityTimer
    );
  }

  opportunityTimer =
    setInterval(
      () => {

        scanApprovedOpportunities()
          .catch(
            err =>
              errLog(
                "Opportunity interval",
                err
              )
          );

      },
      PAPER.opportunityMs
    );

  scanApprovedOpportunities()
    .catch(
      err =>
        errLog(
          "Opportunity startup",
          err
        )
    );

  log(
    "🎯 Continuous Opportunity Engine ONLINE"
  );
}

// ======================================================
// ESTABLISHED / OLD COIN DISCOVERY
// ======================================================

async function fetchEstablishedProfiles() {

  const endpoints = [
    "https://api.dexscreener.com/token-profiles/latest/v1",
    "https://api.dexscreener.com/token-boosts/top/v1",
    "https://api.dexscreener.com/token-boosts/latest/v1"
  ];

  const results =
    [];

  for (
    const url
    of endpoints
  ) {

    try {

      const data =
        await httpsJson(
          url
        );

      if (
        Array.isArray(
          data
        )
      ) {
        results.push(
          ...data
        );
      }

    } catch (err) {

      errLog(
        "Established discovery",
        err
      );
    }

    await sleep(
      400
    );
  }

  const unique =
    new Map();

  for (
    const item
    of results
  ) {

    if (
      item?.chainId !==
      "solana"
    ) {
      continue;
    }

    const mint =
      item?.tokenAddress;

    if (
      !mint
    ) {
      continue;
    }

    if (
      !unique.has(
        mint
      )
    ) {

      unique.set(
        mint,
        item
      );
    }

    if (
      unique.size >=
      MARKET_SCANNER.externalRawLimit
    ) {
      break;
    }
  }

  return [
    ...unique.values()
  ];
}

async function precheckEstablishedMint(
  mint
) {

  try {

    const pairs =
      await fetchPairs(
        mint
      );

    const pair =
      bestPool(
        pairs
      );

    if (
      !pair
    ) {
      return null;
    }

    const liquidity =
      num(
        pair?.liquidity?.usd
      );

    const volumeH1 =
      num(
        pair?.volume?.h1
      );

    if (
      liquidity <
      MARKET_SCANNER.minLiquidityUsd
    ) {
      return null;
    }

    if (
      volumeH1 <
      MARKET_SCANNER.minVolumeH1Usd
    ) {
      return null;
    }

    return {
      mint,
      pair,
      liquidity,
      volumeH1
    };

  } catch {

    return null;
  }
}

async function scanEstablishedMarket() {

  if (
    !MARKET_SCANNER.enabled ||
    marketScannerBusy ||
    shuttingDown
  ) {
    return;
  }

  marketScannerBusy =
    true;

  try {

    log(
      "🌐 Established Market Scanner cycle"
    );

    const profiles =
      await fetchEstablishedProfiles();

    let added =
      0;

    for (
      const profile
      of profiles
    ) {

      if (
        added >=
        MARKET_SCANNER.maxNewEstablishedPerCycle
      ) {
        break;
      }

      const mint =
        profile.tokenAddress;

      const exists =
        await FreshToken
          .findOne({
            mint
          })
          .select(
            "_id"
          )
          .lean();

      if (
        exists
      ) {
        continue;
      }

      const pre =
        await precheckEstablishedMint(
          mint
        );

      if (
        !pre
      ) {
        continue;
      }

      await FreshToken.create({
        mint,

        signature:
          null,

        program:
          "ESTABLISHED_DISCOVERY",

        origin:
          "ESTABLISHED",

        discoveredBy:
          "DEXSCREENER_MARKET",

        establishedDiscoveredAt:
          new Date(),

        paperOnly:
          true
      });

      state.establishedDiscovered++;

      added++;

      log(
        `🏛️ Established candidate ${mint} | Liquidity $${pre.liquidity.toFixed(0)} | H1 Vol $${pre.volumeH1.toFixed(0)}`
      );

      await securityScan(
        mint
      );

      await sleep(
        1200
      );
    }

    // ----------------------------------------------
    // RE-SCAN OLDER TOKENS ALREADY IN OUR DATABASE
    // ----------------------------------------------

    const oldCutoff =
      new Date(
        Date.now() -
        MARKET_SCANNER.oldAgeMinutes *
        60000
      );

    const oldTokens =
      await FreshToken
        .find({
          createdAt: {
            $lte:
              oldCutoff
          },

          securityDecision:
            "PASS",

          $or: [
            {
              dexDecision:
                "PASS"
            },
            {
              finalDecision:
                "APPROVED_CANDIDATE"
            },
            {
              finalDecision:
                "WATCH"
            },
            {
              finalDecision:
                "WAITING_DEX"
            }
          ]
        })
        .sort({
          updatedAt: 1
        })
        .limit(
          MARKET_SCANNER.maxDbRescansPerCycle
        )
        .select(
          "mint"
        )
        .lean();

    for (
      const token
      of oldTokens
    ) {

      await dexScan(
        token.mint
      );

      state.establishedScanned++;

      await sleep(
        1200
      );
    }

  } catch (err) {

    errLog(
      "Established Market Scanner",
      err
    );

  } finally {

    marketScannerBusy =
      false;
  }
}

function startMarketScanner() {

  if (
    !MARKET_SCANNER.enabled
  ) {
    return;
  }

  if (
    marketScannerTimer
  ) {
    clearInterval(
      marketScannerTimer
    );
  }

  marketScannerTimer =
    setInterval(
      () => {

        scanEstablishedMarket()
          .catch(
            err =>
              errLog(
                "Market scanner interval",
                err
              )
          );

      },
      MARKET_SCANNER.scanMs
    );

  setTimeout(
    () => {

      scanEstablishedMarket()
        .catch(
          err =>
            errLog(
              "Market scanner startup",
              err
            )
        );

    },
    15000
  );

  log(
    "🏛️ Established/Old Coin Scanner ONLINE"
  );
}

// ======================================================
// PAPER ENGINE
// ======================================================

function startPaperEngine() {

  if (
    !PAPER.enabled
  ) {

    state.paper =
      "disabled";

    return;
  }

  state.paper =
    "online";

  if (
    paperMonitorTimer
  ) {
    clearInterval(
      paperMonitorTimer
    );
  }

  paperMonitorTimer =
    setInterval(
      () => {

        monitorPaperTrades()
          .catch(
            err =>
              errLog(
                "Paper interval",
                err
              )
          );

      },
      PAPER.monitorMs
    );

  monitorPaperTrades()
    .catch(
      err =>
        errLog(
          "Paper startup monitor",
          err
        )
    );

  log(
    "🧪 Paper Trading Engine ONLINE | NO REAL ORDERS"
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

function allowHunterTransaction() {

  const now =
    Date.now();

  if (
    now -
    hunterMinuteWindow >=
    60000
  ) {

    hunterMinuteWindow =
      now;

    hunterMinuteCount =
      0;
  }

  if (
    hunterMinuteCount >=
    HUNTER_MAX_TX_PER_MINUTE
  ) {

    state.hunterThrottled++;

    return false;
  }

  hunterMinuteCount++;

  return true;
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
    (
      event.logs ||
      []
    ).some(
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
    !allowHunterTransaction()
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

    let tx =
      null;

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

      if (
        tx
      ) {
        break;
      }

      await sleep(
        700 +
        i * 700
      );
    }

    if (
      !tx
    ) {
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

        if (
          mint
        ) {
          break;
        }
      }
    }

    if (
      !mint
    ) {
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

    if (
      !token
    ) {

      await FreshToken.create({
        mint,
        signature,
        program,

        origin:
          "FRESH",

        discoveredBy:
          "SOLANA_HUNTER",

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
      tokenSub !==
      null
    ) {

      await connection
        .removeOnLogsListener(
          tokenSub
        );

      tokenSub =
        null;
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
      slotSub !==
      null
    ) {

      await connection
        .removeSlotChangeListener(
          slotSub
        );

      slotSub =
        null;
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

        event =>
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
            ),

        "confirmed"
      );

    token2022Sub =
      connection.onLogs(
        TOKEN_2022_PROGRAM_ID,

        event =>
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
            ),

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
      "🔎 Fresh Hunter + WebSocket running"
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
      .readyState !==
    1
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
        .limit(
          10
        )
        .select(
          "mint"
        )
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
        .limit(
          5
        )
        .select(
          "mint"
        )
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
        "🧪 LOMY V4.6.1\n\n" +
        "🔎 Fresh Hunter ON\n" +
        "🏛️ Established Coin Scanner ON\n" +
        "🛡 Security ON\n" +
        "💧 DEX ON\n" +
        "🐋 Whale Engine ON\n" +
        "🧠 Smart Score ON\n" +
        "🎯 Opportunity Engine ON\n" +
        "🧪 Paper Trading ON\n\n" +
        "🔒 LIVE BUY/SELL OFF"
      )
  );

  bot.command(
    "status",
    ctx =>
      ctx.reply(

        `🧪 LOMY V4.6.1 STATUS\n\n` +

        `🌐 Server: ${state.server}\n` +
        `🗄 Database: ${state.database}\n` +
        `👛 Wallet: ${state.wallet}\n` +
        `⚡ Solana: ${state.solana}\n` +
        `📡 Telegram: ${state.telegram}\n` +
        `🔌 WebSocket: ${state.websocket}\n\n` +

        `🔎 Fresh Hunter: ${state.hunter}\n` +
        `🏛️ Established Scanner: ${MARKET_SCANNER.enabled ? "ON" : "OFF"}\n` +
        `🛡 Security: ${state.security}\n` +
        `💧 DEX: ${state.dex}\n` +
        `🐋 Whales: ${state.whales}\n` +
        `🧪 Paper: ${state.paper}\n\n` +

        `Fresh Detected: ${state.detected}\n` +
        `Established Found: ${state.establishedDiscovered}\n` +
        `Old DB Rescans: ${state.establishedScanned}\n` +
        `Opportunity Cycles: ${state.opportunityCycles}\n\n` +

        `RPC Queue: ${rpcQueue.length}\n` +
        `RPC 429: ${state.rpc429}\n` +
        `RPC Retries: ${state.rpcRetries}\n` +
        `RPC Dropped: ${state.rpcDropped}\n` +
        `RPC Delay: ${Math.round(rpcDelay)}ms\n` +
        `Hunter Throttled: ${state.hunterThrottled}\n\n` +

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
        `DEX Errors: ${state.dexNetworkErrors}`
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
            smartScore: -1,
            liquidityUsd: -1
          })
          .limit(
            10
          )
          .lean();

      if (
        !tokens.length
      ) {

        return ctx.reply(
          "🎯 لا توجد Approved Candidates حالياً."
        );
      }

      let text =
        `✅ APPROVED CANDIDATES (${tokens.length})\n`;

      for (
        let i = 0;
        i <
        tokens.length;
        i++
      ) {

        const t =
          tokens[i];

        text +=
          `\n━━━━━━━━━━━━━━\n${i + 1}) ${t.mint}\n` +
          `📂 Source: ${t.origin || "FRESH"}\n` +
          `🧠 Smart: ${num(t.smartScore)}/100\n` +
          `🛡 Security: ${num(t.securityScore)}/100\n` +
          `💧 DEX: ${num(t.dexScore)}/100\n` +
          `🐋 Whale: ${num(t.whaleScore)}/100 ${t.whaleDecision}\n` +
          `💵 Liquidity: $${num(t.liquidityUsd).toFixed(0)}\n`;
      }

      text +=
        "\n🧪 PAPER ONLY";

      await ctx.reply(
        text
      );
    }
  );

  bot.command(
    "paper",
    async ctx => {

      const s =
        await getPaperSummary();

      const a =
        s.account;

      const winRate =
        num(
          a.totalTrades
        ) > 0
          ?
          (
            num(
              a.wins
            ) /
            num(
              a.totalTrades
            )
          ) *
          100
          :
          0;

      await ctx.reply(

        `🧪 PAPER ACCOUNT ${PAPER.testRun}\n\n` +

        `Starting: $${num(a.startingBalanceUsd).toFixed(2)}\n` +
        `Cash: $${num(a.cashBalanceUsd).toFixed(2)}\n` +
        `Open Value: $${s.openValue.toFixed(2)}\n` +
        `Equity: $${s.equity.toFixed(2)}\n` +
        `Realized PnL: $${num(a.realizedPnlUsd).toFixed(2)}\n` +
        `Unrealized PnL: $${s.unrealizedPnl.toFixed(2)}\n\n` +

        `Open Trades: ${s.openTrades.length}/${PAPER.maxOpenTrades}\n` +
        `Closed Trades: ${num(a.totalTrades)}\n` +
        `Wins: ${num(a.wins)}\n` +
        `Losses: ${num(a.losses)}\n` +
        `Win Rate: ${winRate.toFixed(1)}%\n\n` +

        `Entry Attempts: ${state.paperEntryAttempts}\n` +
        `Entry Skips: ${state.paperEntrySkips}\n` +
        `Last Skip: ${state.paperLastSkip || "NONE"}\n\n` +

        `🔒 NO REAL MONEY USED`
      );
    }
  );

  bot.command(
    "positions",
    async ctx => {

      const trades =
        await PaperTrade
          .find({
            testRun:
              PAPER.testRun,
            status:
              "OPEN"
          })
          .sort({
            openedAt: -1
          })
          .limit(
            10
          )
          .lean();

      if (
        !trades.length
      ) {

        return ctx.reply(
          "🧪 لا توجد Paper Positions مفتوحة حالياً."
        );
      }

      let text =
        `🧪 OPEN PAPER POSITIONS (${trades.length})\n`;

      for (
        let i = 0;
        i <
        trades.length;
        i++
      ) {

        const t =
          trades[i];

        const current =
          num(
            t.currentPrice ||
            t.entryPrice
          );

        const pnlPct =
          pctChange(
            current,
            num(
              t.entryPrice
            )
          );

        text +=
          `\n━━━━━━━━━━━━━━\n${i + 1}) ${t.mint}\n` +
          `Source: ${t.source || "UNKNOWN"}\n` +
          `Trigger: ${t.entryTrigger || "UNKNOWN"}\n` +
          `Entry: ${num(t.entryPrice)}\n` +
          `Current: ${current}\n` +
          `PnL: ${pnlPct.toFixed(2)}%\n` +
          `High: ${num(t.highestPrice)}\n` +
          `SL: ${num(t.hardStopPrice)}\n` +
          `Trail: ${
            t.trailingActive
              ? num(
                  t.trailingStopPrice
                )
              : "OFF"
          }\n` +
          `TP: ${num(t.takeProfitPrice)}\n`;
      }

      text +=
        "\n🔒 PAPER ONLY";

      await ctx.reply(
        text
      );
    }
  );

  bot.command(
    "trades",
    async ctx => {

      const trades =
        await PaperTrade
          .find({
            testRun:
              PAPER.testRun,
            status:
              "CLOSED"
          })
          .sort({
            closedAt: -1
          })
          .limit(
            10
          )
          .lean();

      if (
        !trades.length
      ) {

        return ctx.reply(
          "📚 لا توجد Paper Trades مغلقة في الاختبار الجديد حتى الآن."
        );
      }

      let text =
        `📚 LAST PAPER TRADES (${trades.length})\n`;

      for (
        let i = 0;
        i <
        trades.length;
        i++
      ) {

        const t =
          trades[i];

        text +=
          `\n━━━━━━━━━━━━━━\n${i + 1}) ${t.mint}\n` +
          `Source: ${t.source || "UNKNOWN"}\n` +
          `Trigger: ${t.entryTrigger || "UNKNOWN"}\n` +
          `Result: ${num(t.pnlPct).toFixed(2)}% | $${num(t.netPnlUsd).toFixed(2)}\n` +
          `Exit: ${t.exitReason}\n` +
          `Runup: ${num(t.maxRunupPct).toFixed(2)}%\n` +
          `Drawdown: ${num(t.maxDrawdownPct).toFixed(2)}%\n`;
      }

      await ctx.reply(
        text
      );
    }
  );

  bot.command(
    "performance",
    async ctx => {

      const a =
        await ensurePaperAccount();

      const total =
        num(
          a.totalTrades
        );

      const winRate =
        total > 0
          ?
          (
            num(
              a.wins
            ) /
            total
          ) *
          100
          :
          0;

      const returnPct =
        num(
          a.startingBalanceUsd
        ) > 0
          ?
          (
            num(
              a.realizedPnlUsd
            ) /
            num(
              a.startingBalanceUsd
            )
          ) *
          100
          :
          0;

      const sourceStats =
        await PaperTrade.aggregate([
          {
            $match: {
              testRun:
                PAPER.testRun,
              status:
                "CLOSED"
            }
          },
          {
            $group: {
              _id:
                "$source",

              trades: {
                $sum: 1
              },

              pnl: {
                $sum:
                  "$netPnlUsd"
              },

              avgPct: {
                $avg:
                  "$pnlPct"
              }
            }
          }
        ]);

      let sourceText =
        "";

      for (
        const x
        of sourceStats
      ) {

        sourceText +=
          `\n${x._id || "UNKNOWN"}: ${x.trades} trades | $${num(x.pnl).toFixed(2)} | Avg ${num(x.avgPct).toFixed(2)}%`;
      }

      await ctx.reply(

        `📈 PAPER PERFORMANCE ${PAPER.testRun}\n\n` +

        `Trades: ${total}\n` +
        `Wins: ${num(a.wins)}\n` +
        `Losses: ${num(a.losses)}\n` +
        `Breakeven: ${num(a.breakeven)}\n` +
        `Win Rate: ${winRate.toFixed(1)}%\n\n` +

        `Realized PnL: $${num(a.realizedPnlUsd).toFixed(2)}\n` +
        `Return on $${num(a.startingBalanceUsd).toFixed(2)}: ${returnPct.toFixed(2)}%\n` +
        `Best Trade: ${num(a.bestTradePct).toFixed(2)}%\n` +
        `Worst Trade: ${num(a.worstTradePct).toFixed(2)}%\n` +

        `\n📂 BY SOURCE:${sourceText || "\nNo closed trades yet."}\n\n` +

        `🧪 TEST DATA ONLY`
      );
    }
  );

  bot.command(
    "stats",
    async ctx => {

      const [
        total,
        approved,
        fresh,
        established,
        paperOpen,
        paperClosed
      ] =
      await Promise.all([

        FreshToken
          .countDocuments(),

        FreshToken
          .countDocuments({
            finalDecision:
              "APPROVED_CANDIDATE"
          }),

        FreshToken
          .countDocuments({
            origin:
              "FRESH"
          }),

        FreshToken
          .countDocuments({
            origin:
              "ESTABLISHED"
          }),

        PaperTrade
          .countDocuments({
            testRun:
              PAPER.testRun,
            status:
              "OPEN"
          }),

        PaperTrade
          .countDocuments({
            testRun:
              PAPER.testRun,
            status:
              "CLOSED"
          })
      ]);

      await ctx.reply(

        `📊 LOMY V4.6.1 STATS\n\n` +

        `Total Known Tokens: ${total}\n` +
        `🆕 Fresh: ${fresh}\n` +
        `🏛️ Established: ${established}\n` +
        `✅ Approved: ${approved}\n\n` +

        `🧪 Paper Open: ${paperOpen}\n` +
        `📚 Paper Closed: ${paperClosed}\n` +
        `🎯 Entry Attempts: ${state.paperEntryAttempts}\n` +
        `⏭ Entry Skips: ${state.paperEntrySkips}\n\n` +

        `Established Found: ${state.establishedDiscovered}\n` +
        `Old DB Rescans: ${state.establishedScanned}\n` +
        `Opportunity Cycles: ${state.opportunityCycles}\n\n` +

        `RPC 429: ${state.rpc429}\n` +
        `RPC Dropped: ${state.rpcDropped}\n` +
        `Hunter Throttled: ${state.hunterThrottled}\n` +
        `Errors: ${state.errors}\n\n` +

        `🔒 LIVE TRADING OFF`
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

          `💰 Wallet balance: ${(balance / LAMPORTS_PER_SOL).toFixed(6)} SOL\n` +
          `🔒 Paper Engine does not spend it.`
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
            err?.cause?.message ||
            null,

          causeCode:
            err?.cause?.code ||
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

  if (
    paperMonitorTimer
  ) {

    clearInterval(
      paperMonitorTimer
    );
  }

  if (
    opportunityTimer
  ) {

    clearInterval(
      opportunityTimer
    );
  }

  if (
    marketScannerTimer
  ) {

    clearInterval(
      marketScannerTimer
    );
  }

  try {

    if (
      bot
    ) {

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
        process.exit(
          0
        )
    );

    setTimeout(
      () =>
        process.exit(
          0
        ),
      5000
    );

  } else {

    process.exit(
      0
    );
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
    "🚀 LOMY SOLANA HUNTER V4.6.1"
  );

  console.log(
    "🔎 FRESH TOKEN HUNTER"
  );

  console.log(
    "🏛️ ESTABLISHED / OLD COIN SCANNER"
  );

  console.log(
    "🧠 SMART SCORE ENGINE"
  );

  console.log(
    "🐋 WHALE ENGINE"
  );

  console.log(
    "🎯 CONTINUOUS OPPORTUNITY ENGINE"
  );

  console.log(
    "🧪 PAPER TRADING ENGINE"
  );

  console.log(
    `💵 NEW TEST START: $${PAPER.startingBalanceUsd}`
  );

  console.log(
    `🧪 TEST RUN: ${PAPER.testRun}`
  );

  console.log(
    `🛑 SL: ${PAPER.hardStopPct}% | 🎯 TP: ${PAPER.takeProfitPct}%`
  );

  console.log(
    `📈 TRAIL ON: +${PAPER.trailingActivationPct}% | DISTANCE: ${PAPER.trailingDistancePct}%`
  );

  console.log(
    `🏛️ OLD COIN SCAN: EVERY ${MARKET_SCANNER.scanMs / 60000} MIN`
  );

  console.log(
    `🎯 OPPORTUNITY RESCAN: EVERY ${PAPER.opportunityMs / 1000} SEC`
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

  await migrateOldSmartScores();

  await ensurePaperAccount();

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

  startPaperEngine();

  startOpportunityEngine();

  startMarketScanner();

  recoverPending()
    .catch(
      err =>
        errLog(
          "Recovery",
          err
        )
    );

  log(
    "✅ LOMY V4.6.1 STARTED"
  );

  log(
    "🔎 FRESH + 🏛️ ESTABLISHED MARKET ACTIVE"
  );

  log(
    "🧪 NEW PAPER TEST ACTIVE"
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
