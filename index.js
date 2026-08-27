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

const WHALE_RPC_URL =
  process.env.WHALE_RPC_URL ||
  RPC_URL;

const MONGODB_URI =
  process.env.MONGODB_URI;

const TELEGRAM_TOKEN =
  process.env.TELEGRAM_TOKEN;

const BOT_PRIVATE_KEY =
  process.env.BOT_PRIVATE_KEY;

const VERSION = "V4.6.2";
const MODE = "PAPER";
const LIVE_TRADING = false;

const SCORE = {
  security: 0.45,
  dex: 0.35,
  whale: 0.20,
  approval: 75
};

const PAPER = {
  enabled: true,

  testRun: "V4.6.2",
  accountKey: "v462-main",

  startingBalanceUsd: 20,
  positionSizeUsd: 5,
  maxOpenTrades: 4,

  hardStopPct: 12,
  takeProfitPct: 100,

  trailingActivationPct: 15,
  trailingDistancePct: 8,

  assumedSlippagePct: 1.5,
  assumedFeePct: 0.5,

  maxTradeAgeMinutes: 180,

  minLiquidityUsd: 5000,
  minVolumeM5: 300,
  minBuysM5: 5,
  minBuySellRatio: 1.05,

  minPriceChangeM5: -3,
  maxPriceChangeM5: 25,

  cooldownMinutes: 90,
  maxEntriesPerMint24h: 2,

  monitorMs: 30000,
  opportunityMs: 60000
};

const DISCOVERY = {
  enabled: true,

  scanMs: 120000,

  rawLimit: 40,
  processPerCycle: 8,

  minLiquidityUsd: 5000,
  minVolumeH1Usd: 2500,

  freshAgeHours: 6
};

const connection =
  new Connection(
    RPC_URL,
    {
      commitment: "confirmed",
      confirmTransactionInitialTimeout:
        30000
    }
  );

const whaleConnection =
  new Connection(
    WHALE_RPC_URL,
    {
      commitment: "confirmed",
      confirmTransactionInitialTimeout:
        30000
    }
  );

const telegramAgent =
  new https.Agent({
    keepAlive: true,
    family: 4,
    timeout: 30000
  });

let wallet = null;
let bot = null;
let server = null;

let shuttingDown = false;

let paperTimer = null;
let discoveryTimer = null;
let opportunityTimer = null;

let discoveryBusy = false;
let opportunityBusy = false;
let paperBusy = false;

let whaleWorkerBusy = false;

const paperOpening =
  new Set();

const whaleQueued =
  new Set();

const whaleQueue = [];

const state = {
  server: "starting",
  database: "disconnected",
  wallet: "not_loaded",
  solana: "disconnected",
  telegram: "stopped",

  discovery: "idle",
  security: "idle",
  dex: "idle",
  whales: "idle",
  paper: "idle",

  discovered: 0,
  fresh: 0,
  established: 0,

  securityScanned: 0,
  dexScanned: 0,
  whaleScanned: 0,

  paperOpened: 0,
  paperClosed: 0,

  paperEntryAttempts: 0,
  paperEntrySkips: 0,
  lastSkip: null,

  rpc429: 0,
  rpcRetries: 0,
  rpcDropped: 0,

  dexErrors: 0,
  errors: 0,

  discoveryCycles: 0,
  opportunityCycles: 0,

  lastMint: null
};

function log(...args) {
  console.log(
    new Date().toISOString(),
    ...args
  );
}

function errLog(
  name,
  err
) {
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
  ) *
  100;
}

function is429(err) {
  const s =
    String(
      err?.message ||
      err
    ).toLowerCase();

  return (
    s.includes("429") ||
    s.includes("rate limit") ||
    s.includes(
      "too many requests"
    )
  );
}

function pairAgeHours(
  pair
) {
  const created =
    num(
      pair?.pairCreatedAt
    );

  if (
    !created
  ) {
    return 999999;
  }

  return (
    Date.now() -
    created
  ) /
  3600000;
}

const rpcQueue = [];

let rpcBusy = false;
let rpcDelay = 900;

const RPC_MIN_DELAY = 900;
const RPC_MAX_DELAY = 15000;
const RPC_QUEUE_LIMIT = 20;

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
        rpcQueue.length >=
        RPC_QUEUE_LIMIT
      ) {
        state.rpcDropped++;

        return reject(
          new Error(
            "RPC_QUEUE_FULL"
          )
        );
      }

      rpcQueue.push({
        fn,
        priority,
        label,
        resolve,
        reject
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
              "RPC worker",
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

      let lastError =
        null;

      let success =
        false;

      for (
        let attempt = 1;
        attempt <= 4;
        attempt++
      ) {

        try {

          const result =
            await job.fn();

          rpcDelay =
            Math.max(
              RPC_MIN_DELAY,
              rpcDelay - 100
            );

          job.resolve(
            result
          );

          success =
            true;

          break;

        } catch (err) {

          lastError =
            err;

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
                2500,
                rpcDelay * 2
              )
            );

          log(
            `⚠️ RPC 429 ${job.label} attempt ${attempt}/4 wait ${Math.round(rpcDelay)}ms`
          );

          await sleep(
            rpcDelay +
            Math.floor(
              Math.random() *
              500
            )
          );
        }
      }

      if (
        !success
      ) {
        job.reject(
          lastError
        );
      }

      await sleep(
        rpcDelay +
        Math.floor(
          Math.random() *
          250
        )
      );
    }

  } finally {

    rpcBusy =
      false;
  }
}

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
            timeout: 12000,

            headers: {
              Accept:
                "application/json",

              "User-Agent":
                "LOMY-V4.6.2",

              Connection:
                "close"
            }
          },

          res => {

            let body = "";

            res.setEncoding(
              "utf8"
            );

            res.on(
              "data",
              chunk => {
                body +=
                  chunk;
              }
            );

            res.on(
              "end",
              () => {

                if (
                  res.statusCode <
                    200 ||
                  res.statusCode >=
                    300
                ) {
                  return reject(
                    new Error(
                      `HTTP ${res.statusCode}`
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
                      "INVALID_JSON"
                    )
                  );
                }
              }
            );
          }
        );

      req.on(
        "timeout",
        () =>
          req.destroy(
            new Error(
              "HTTP_TIMEOUT"
            )
          )
      );

      req.on(
        "error",
        reject
      );
    }
  );
}

const tokenSchema =
  new mongoose.Schema(
    {
      mint: {
        type: String,
        unique: true,
        index: true
      },

      origin: {
        type: String,
        default: "UNKNOWN"
      },

      discoveredBy:
        String,

      detectedAt: {
        type: Date,
        default: Date.now
      },

      pairCreatedAt:
        Number,

      securityChecked: {
        type: Boolean,
        default: false
      },

      securityScore:
        Number,

      securityDecision: {
        type: String,
        default: "PENDING"
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

      dexChecked: {
        type: Boolean,
        default: false
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

      priceChangeM5:
        Number,

      priceChangeH1:
        Number,

      dexScore:
        Number,

      dexDecision: {
        type: String,
        default: "PENDING"
      },

      preWhaleScore:
        Number,

      whaleStatus: {
        type: String,
        default: "PENDING"
      },

      whaleChecked: {
        type: Boolean,
        default: false
      },

      whaleScore:
        Number,

      whaleDecision: {
        type: String,
        default: "PENDING"
      },

      largestHolderPct:
        Number,

      top5Pct:
        Number,

      top10Pct:
        Number,

      whaleUniqueOwners:
        Number,

      whaleFlags: {
        type: [String],
        default: []
      },

      smartScore:
        Number,

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

      source:
        String,

      entryTrigger:
        String,

      status: {
        type: String,

        enum: [
          "OPEN",
          "CLOSED"
        ],

        default:
          "OPEN",

        index:
          true
      },

      openedAt: {
        type: Date,
        default: Date.now
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

      lastPriceCheckAt:
        Date
    },
    {
      timestamps: true
    }
  );

const paperAccountSchema =
  new mongoose.Schema(
    {
      key: {
        type: String,
        unique: true
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
      timestamps: true
    }
  );

const FreshToken =
  mongoose.models
    .FreshToken ||
  mongoose.model(
    "FreshToken",
    tokenSchema
  );

const PaperTrade =
  mongoose.models
    .PaperTrade ||
  mongoose.model(
    "PaperTrade",
    paperTradeSchema
  );

const PaperAccount =
  mongoose.models
    .PaperAccount ||
  mongoose.model(
    "PaperAccount",
    paperAccountSchema
  );

app.get(
  "/",
  (
    req,
    res
  ) =>
    res.send(
      "✅ LOMY V4.6.2 NETWORK SAFE | PAPER ONLY"
    )
);

app.get(
  "/health",
  (
    req,
    res
  ) =>
    res.json({
      ...state,

      version:
        VERSION,

      mode:
        MODE,

      liveTrading:
        LIVE_TRADING,

      rpcQueue:
        rpcQueue.length,

      rpcDelay,

      whaleQueue:
        whaleQueue.length,

      uptime:
        Math.floor(
          process.uptime()
        )
    })
);

async function startServer() {
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
              `✅ Server online ${PORT}`
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
      "✅ Solana",
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

async function fetchPairs(
  mint
) {
  const url =
    "https://api.dexscreener.com/token-pairs/v1/solana/" +
    encodeURIComponent(
      mint
    );

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

  } catch {

    state.dexErrors++;

    return [];
  }
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

function calculateDexScore(
  pair
) {
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

  let score = 0;

  if (
    liquidity >= 50000
  ) {
    score += 40;
  } else if (
    liquidity >= 10000
  ) {
    score += 35;
  } else if (
    liquidity >= 5000
  ) {
    score += 25;
  }

  if (
    volumeM5 >= 1000
  ) {
    score += 20;
  } else if (
    volumeM5 >= 300
  ) {
    score += 15;
  }

  if (
    volumeH1 >= 5000
  ) {
    score += 10;
  }

  if (
    buys + sells >=
    10
  ) {
    score += 15;
  }

  if (
    buys > sells &&
    sells > 0
  ) {
    score += 15;
  }

  return Math.min(
    100,
    score
  );
}

function calculateSmartScore(
  security,
  dex,
  whale
) {
  return Math.round(
    num(
      security
    ) *
      SCORE.security +
    num(
      dex
    ) *
      SCORE.dex +
    num(
      whale
    ) *
      SCORE.whale
  );
}

function smartDecision(
  score,
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
    score >=
      SCORE.approval
  ) {
    return "APPROVED_CANDIDATE";
  }

  return "WATCH_SCORE";
}

async function securityScan(
  mint
) {
  state.security =
    "scanning";

  try {

    const account =
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
      );

    if (
      !account?.value
    ) {
      return false;
    }

    const parsed =
      account.value
        .data
        ?.parsed;

    if (
      !parsed ||
      parsed.type !==
        "mint"
    ) {
      return false;
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

    score =
      Math.max(
        0,
        Math.min(
          100,
          score
        )
      );

    const decision =
      score >= 80
        ? "PASS"
        :
        score >= 55
          ? "REVIEW"
          : "REJECT";

    await FreshToken
      .updateOne(
        {
          mint
        },
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
            supply
          }
        }
      );

    state.securityScanned++;

    log(
      `🛡 ${mint} ${score}/100 ${decision}`
    );

    return (
      decision ===
      "PASS"
    );

  } catch (err) {

    errLog(
      `Security ${mint}`,
      err
    );

    return false;

  } finally {

    state.security =
      "idle";
  }
}

function holderPct(
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
      s <= 0n
    ) {
      return 0;
    }

    return (
      Number(
        (
          a *
          10000n
        ) /
        s
      ) /
      100
    );

  } catch {
    return 0;
  }
}

function whaleScore(
  data
) {
  let score =
    100;

  const flags = [];

  if (
    data.largest >=
    25
  ) {
    score -= 45;

    flags.push(
      "VERY_LARGE_HOLDER"
    );

  } else if (
    data.largest >=
    15
  ) {
    score -= 30;

    flags.push(
      "LARGE_HOLDER"
    );

  } else if (
    data.largest >=
    10
  ) {
    score -= 15;
  }

  if (
    data.top10 >= 80
  ) {
    score -= 40;

    flags.push(
      "EXTREME_TOP10"
    );

  } else if (
    data.top10 >=
    60
  ) {
    score -= 25;

    flags.push(
      "HIGH_TOP10"
    );

  } else if (
    data.top10 >=
    45
  ) {
    score -= 10;
  }

  if (
    data.owners < 5
  ) {
    score -= 15;
  }

  score =
    Math.max(
      0,
      Math.min(
        100,
        score
      )
    );

  return {
    score,

    decision:
      score >= 75
        ? "SAFE"
        :
        score >= 50
          ? "CAUTION"
          : "DANGER",

    flags
  };
}

function queueWhale(
  mint
) {
  if (
    whaleQueued.has(
      mint
    )
  ) {
    return;
  }

  whaleQueued.add(
    mint
  );

  whaleQueue.push(
    mint
  );

  runWhaleWorker()
    .catch(
      err =>
        errLog(
          "Whale worker",
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

      whaleQueued.delete(
        mint
      );

      await whaleScan(
        mint
      );

      await sleep(
        4000
      );
    }

  } finally {

    whaleWorkerBusy =
      false;

    state.whales =
      "idle";
  }
}

async function getWhaleOwnersBatched(
  publicKeys
) {
  const BATCH_SIZE = 5;

  const allAccounts = [];

  for (
    let start = 0;
    start <
      publicKeys.length;
    start +=
      BATCH_SIZE
  ) {

    const batch =
      publicKeys.slice(
        start,
        start +
          BATCH_SIZE
      );

    const batchNumber =
      Math.floor(
        start /
        BATCH_SIZE
      ) + 1;

    log(
      `🐋 Whale owners batch ${batchNumber} | ${batch.length} accounts`
    );

    const response =
      await rpcCall(
        () =>
          whaleConnection
            .getMultipleParsedAccounts(
              batch,
              "confirmed"
            ),
        0,
        `WHALE_OWNERS_BATCH_${batchNumber}`
      );

    allAccounts.push(
      ...(
        response?.value ||
        []
      )
    );

    if (
      start +
        BATCH_SIZE <
      publicKeys.length
    ) {
      await sleep(
        750
      );
    }
  }

  return allAccounts;
}

async function whaleScan(
  mint
) {
  state.whales =
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

    const supplyResponse =
      await rpcCall(
        () =>
          whaleConnection
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
        "TOKEN_SUPPLY_ZERO"
      );
    }

    const largestResponse =
      await rpcCall(
        () =>
          whaleConnection
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
        "NO_HOLDER_ACCOUNTS"
      );
    }

    const publicKeys =
      accounts.map(
        x =>
          new PublicKey(
            x.address
          )
      );

    const parsedAccounts =
      await getWhaleOwnersBatched(
        publicKeys
      );

    const owners =
      new Set();

    const percentages =
      [];

    for (
      let i = 0;
      i <
      accounts.length;
      i++
    ) {

      const owner =
        parsedAccounts
          ?.[i]
          ?.data
          ?.parsed
          ?.info
          ?.owner;

      if (
        owner
      ) {
        owners.add(
          owner
        );
      }

      percentages.push(
        holderPct(
          accounts[i]
            ?.amount,
          totalSupply
        )
      );
    }

    const largest =
      num(
        percentages[0]
      );

    const top5 =
      percentages
        .slice(
          0,
          5
        )
        .reduce(
          (
            a,
            b
          ) =>
            a + b,
          0
        );

    const top10 =
      percentages
        .slice(
          0,
          10
        )
        .reduce(
          (
            a,
            b
          ) =>
            a + b,
          0
        );

    const result =
      whaleScore({
        largest,
        top10,

        owners:
          owners.size
      });

    const smart =
      calculateSmartScore(
        token.securityScore,
        token.dexScore,
        result.score
      );

    const finalDecision =
      smartDecision(
        smart,
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

            whaleScore:
              result.score,

            whaleDecision:
              result.decision,

            largestHolderPct:
              Number(
                largest.toFixed(
                  2
                )
              ),

            top5Pct:
              Number(
                top5.toFixed(
                  2
                )
              ),

            top10Pct:
              Number(
                top10.toFixed(
                  2
                )
              ),

            whaleUniqueOwners:
              owners.size,

            whaleFlags:
              result.flags,

            smartScore:
              smart,

            finalDecision
          }
        }
      );

    state.whaleScanned++;

    log(
      `🐋 ${mint} | Whale ${result.score} ${result.decision} | Smart ${smart} | ${finalDecision}`
    );

  } catch (err) {

    errLog(
      `Whale ${mint}`,
      err
    );
  }
}

async function analyzeDex(
  mint,
  pair
) {
  state.dex =
    "scanning";

  try {

    const dexScore =
      calculateDexScore(
        pair
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

    const priceChangeM5 =
      num(
        pair?.priceChange
          ?.m5
      );

    const priceChangeH1 =
      num(
        pair?.priceChange
          ?.h1
      );

    const dexDecision =
      (
        liquidity >=
          PAPER.minLiquidityUsd &&
        dexScore >=
          60 &&
        sells >
          0
      )
        ? "PASS"
        : "WATCH";

    const token =
      await FreshToken
        .findOne({
          mint
        })
        .lean();

    const preWhale =
      Math.round(
        num(
          token
            ?.securityScore
        ) *
          0.60 +
        dexScore *
          0.40
      );

    await FreshToken
      .updateOne(
        {
          mint
        },
        {
          $set: {
            dexChecked:
              true,

            dexId:
              pair.dexId ||
              null,

            pairAddress:
              pair.pairAddress ||
              null,

            pairCreatedAt:
              num(
                pair
                  .pairCreatedAt
              ),

            priceUsd:
              num(
                pair.priceUsd
              ),

            liquidityUsd:
              liquidity,

            volumeM5,
            volumeH1,

            buysM5:
              buys,

            sellsM5:
              sells,

            priceChangeM5,
            priceChangeH1,

            dexScore,
            dexDecision,

            preWhaleScore:
              preWhale,

            finalDecision:
              dexDecision ===
                "PASS" &&
              preWhale >=
                70
                ?
                "CANDIDATE_PENDING_WHALE"
                :
                "WATCH"
          }
        }
      );

    state.dexScanned++;

    if (
      dexDecision ===
        "PASS" &&
      preWhale >=
        70
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

  } finally {

    state.dex =
      "idle";
  }
}

async function discoveryFeed() {
  const endpoints = [
    "https://api.dexscreener.com/token-profiles/latest/v1",
    "https://api.dexscreener.com/token-boosts/latest/v1",
    "https://api.dexscreener.com/token-boosts/top/v1"
  ];

  const map =
    new Map();

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
        !Array.isArray(
          data
        )
      ) {
        continue;
      }

      for (
        const item
        of data
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
          !map.has(
            mint
          )
        ) {
          map.set(
            mint,
            item
          );
        }

        if (
          map.size >=
          DISCOVERY.rawLimit
        ) {
          break;
        }
      }

    } catch {

      state.dexErrors++;
    }

    await sleep(
      500
    );
  }

  return [
    ...map.values()
  ];
}

async function runDiscovery() {
  if (
    discoveryBusy ||
    shuttingDown ||
    !DISCOVERY.enabled
  ) {
    return;
  }

  discoveryBusy =
    true;

  state.discovery =
    "scanning";

  state.discoveryCycles++;

  try {

    const profiles =
      await discoveryFeed();

    let processed =
      0;

    for (
      const profile
      of profiles
    ) {

      if (
        processed >=
        DISCOVERY.processPerCycle
      ) {
        break;
      }

      const mint =
        profile.tokenAddress;

      if (
        !mint
      ) {
        continue;
      }

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
        continue;
      }

      const liquidity =
        num(
          pair?.liquidity
            ?.usd
        );

      const volumeH1 =
        num(
          pair?.volume
            ?.h1
        );

      if (
        liquidity <
          DISCOVERY.minLiquidityUsd ||
        volumeH1 <
          DISCOVERY.minVolumeH1Usd
      ) {
        continue;
      }

      const origin =
        pairAgeHours(
          pair
        ) <=
          DISCOVERY.freshAgeHours
          ?
          "FRESH"
          :
          "ESTABLISHED";

      let token =
        await FreshToken
          .findOne({
            mint
          })
          .lean();

      if (
        !token
      ) {

        await FreshToken
          .create({
            mint,

            origin,

            discoveredBy:
              "DEX_DISCOVERY",

            pairCreatedAt:
              num(
                pair
                  .pairCreatedAt
              ),

            paperOnly:
              true
          });

        state.discovered++;

        if (
          origin ===
          "FRESH"
        ) {
          state.fresh++;
        } else {
          state.established++;
        }

        state.lastMint =
          mint;

        log(
          `🔎 ${origin} ${mint} | Liq $${liquidity.toFixed(0)} | H1 $${volumeH1.toFixed(0)}`
        );

        const passed =
          await securityScan(
            mint
          );

        if (
          passed
        ) {
          await analyzeDex(
            mint,
            pair
          );
        }

      } else {

        await analyzeDex(
          mint,
          pair
        );
      }

      processed++;

      await sleep(
        1200
      );
    }

  } catch (err) {

    errLog(
      "Discovery",
      err
    );

  } finally {

    discoveryBusy =
      false;

    state.discovery =
      "idle";
  }
}

function startDiscovery() {
  runDiscovery()
    .catch(
      err =>
        errLog(
          "Discovery startup",
          err
        )
    );

  discoveryTimer =
    setInterval(
      () =>
        runDiscovery()
          .catch(
            err =>
              errLog(
                "Discovery interval",
                err
              )
          ),
      DISCOVERY.scanMs
    );

  log(
    "🔎 Network-safe Fresh + Established Discovery ONLINE"
  );
}

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
      `🧪 New paper test $${PAPER.startingBalanceUsd}`
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

  let openValue = 0;
  let unrealizedPnl = 0;

  for (
    const trade
    of openTrades
  ) {

    const current =
      num(
        trade.currentPrice ||
        trade.entryPrice
      );

    const value =
      num(
        trade.quantity
      ) *
      current;

    openValue +=
      value;

    unrealizedPnl +=
      (
        current -
        num(
          trade.entryPrice
        )
      ) *
      num(
        trade.quantity
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

async function recentMintTrades(
  mint
) {
  const since =
    new Date(
      Date.now() -
      24 *
      60 *
      60 *
      1000
    );

  return PaperTrade
    .find({
      mint,

      testRun:
        PAPER.testRun,

      openedAt: {
        $gte:
          since
      }
    })
    .sort({
      openedAt:
        -1
    })
    .lean();
}

function entryFilter(
  token,
  pair
) {
  const marketPrice =
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

  const priceChangeM5 =
    num(
      pair?.priceChange
        ?.m5
    );

  const buySellRatio =
    sells > 0
      ?
      buys /
      sells
      :
      buys > 0
        ?
        buys
        :
        0;

  if (
    token.finalDecision !==
    "APPROVED_CANDIDATE"
  ) {
    return {
      ok: false,
      reason:
        "NOT_APPROVED"
    };
  }

  if (
    token.whaleDecision !==
    "SAFE"
  ) {
    return {
      ok: false,
      reason:
        "WHALE_NOT_SAFE"
    };
  }

  if (
    marketPrice <= 0
  ) {
    return {
      ok: false,
      reason:
        "INVALID_PRICE"
    };
  }

  if (
    liquidity <
    PAPER.minLiquidityUsd
  ) {
    return {
      ok: false,
      reason:
        "LOW_LIQUIDITY"
    };
  }

  if (
    volumeM5 <
    PAPER.minVolumeM5
  ) {
    return {
      ok: false,
      reason:
        "LOW_VOLUME_M5"
    };
  }

  if (
    buys <
    PAPER.minBuysM5
  ) {
    return {
      ok: false,
      reason:
        "LOW_BUYS"
    };
  }

  if (
    buySellRatio <
    PAPER.minBuySellRatio
  ) {
    return {
      ok: false,
      reason:
        "WEAK_BUY_RATIO"
    };
  }

  if (
    priceChangeM5 <
    PAPER.minPriceChangeM5
  ) {
    return {
      ok: false,
      reason:
        "M5_TOO_WEAK"
    };
  }

  if (
    priceChangeM5 >
    PAPER.maxPriceChangeM5
  ) {
    return {
      ok: false,
      reason:
        "M5_OVEREXTENDED"
    };
  }

  return {
    ok: true,
    marketPrice,
    liquidity,
    volumeM5,
    buys,
    sells,
    buySellRatio,
    priceChangeM5
  };
}

async function queuePaperEntry(
  mint,
  trigger =
    "OPPORTUNITY"
) {
  if (
    !PAPER.enabled ||
    LIVE_TRADING ||
    shuttingDown
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

      state.lastSkip =
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
      !token
    ) {
      return;
    }

    const history =
      await recentMintTrades(
        mint
      );

    if (
      history.length >=
      PAPER.maxEntriesPerMint24h
    ) {
      state.paperEntrySkips++;

      state.lastSkip =
        "MAX_ENTRIES_24H";

      return;
    }

    const lastTrade =
      history[0];

    if (
      lastTrade
    ) {

      const reference =
        new Date(
          lastTrade.closedAt ||
          lastTrade.openedAt
        ).getTime();

      const minutes =
        (
          Date.now() -
          reference
        ) /
        60000;

      if (
        minutes <
        PAPER.cooldownMinutes
      ) {
        state.paperEntrySkips++;

        state.lastSkip =
          "COOLDOWN";

        return;
      }
    }

    const pair =
      bestPool(
        await fetchPairs(
          mint
        )
      );

    if (
      !pair
    ) {
      state.paperEntrySkips++;

      state.lastSkip =
        "NO_PAIR";

      return;
    }

    const check =
      entryFilter(
        token,
        pair
      );

    if (
      !check.ok
    ) {
      state.paperEntrySkips++;

      state.lastSkip =
        check.reason;

      log(
        `🧪 ENTRY FILTER ${mint} | ${check.reason}`
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
      allocatedUsd < 1
    ) {
      state.paperEntrySkips++;

      state.lastSkip =
        "LOW_CASH";

      return;
    }

    const entryPrice =
      check.marketPrice *
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

    const capital =
      Math.max(
        0,
        allocatedUsd -
        entryFeeUsd
      );

    const quantity =
      capital /
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

    await PaperTrade
      .create({
        mint,

        testRun:
          PAPER.testRun,

        source:
          token.origin ||
          "UNKNOWN",

        entryTrigger:
          trigger,

        status:
          "OPEN",

        marketEntryPrice:
          check.marketPrice,

        entryPrice,

        currentPrice:
          check.marketPrice,

        highestPrice:
          check.marketPrice,

        lowestPrice:
          check.marketPrice,

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

        liquidityAtEntry:
          check.liquidity,

        volumeM5AtEntry:
          check.volumeM5,

        buysM5AtEntry:
          check.buys,

        sellsM5AtEntry:
          check.sells,

        buySellRatioAtEntry:
          check.buySellRatio,

        priceChangeM5AtEntry:
          check.priceChangeM5,

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
      `🧪 PAPER BUY ${mint} | ${token.origin} | $${allocatedUsd.toFixed(2)} | M5 ${check.priceChangeM5.toFixed(2)}% | B/S ${check.buySellRatio.toFixed(2)}`
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

  const gross =
    num(
      trade.quantity
    ) *
    exitPrice;

  const exitFee =
    gross *
    (
      PAPER.assumedFeePct /
      100
    );

  const net =
    Math.max(
      0,
      gross -
      exitFee
    );

  const pnlUsd =
    net -
    num(
      trade.allocatedUsd
    );

  const pnlPct =
    num(
      trade.allocatedUsd
    ) > 0
      ?
      pnlUsd /
      num(
        trade.allocatedUsd
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
    exitFee;

  trade.netPnlUsd =
    pnlUsd;

  trade.pnlPct =
    pnlPct;

  await trade.save();

  const account =
    await ensurePaperAccount();

  account.cashBalanceUsd =
    num(
      account.cashBalanceUsd
    ) +
    net;

  account.realizedPnlUsd =
    num(
      account.realizedPnlUsd
    ) +
    pnlUsd;

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
    `🧪 PAPER SELL ${trade.mint} | ${reason} | ${pnlPct.toFixed(2)}% | $${pnlUsd.toFixed(2)}`
  );
}

async function monitorPaperTrades() {
  if (
    paperBusy ||
    shuttingDown ||
    !PAPER.enabled
  ) {
    return;
  }

  paperBusy =
    true;

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

        const pair =
          bestPool(
            await fetchPairs(
              trade.mint
            )
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
          num(
            trade.lowestPrice
          ) > 0
            ?
            Math.min(
              num(
                trade.lowestPrice
              ),
              marketPrice
            )
            :
            marketPrice;

        const runup =
          pctChange(
            highest,
            num(
              trade.entryPrice
            )
          );

        const drawdown =
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

        let trailingStop =
          num(
            trade.trailingStopPrice
          );

        if (
          !trailingActive &&
          runup >=
          PAPER.trailingActivationPct
        ) {
          trailingActive =
            true;
        }

        if (
          trailingActive
        ) {

          const candidate =
            highest *
            (
              1 -
              PAPER.trailingDistancePct /
              100
            );

          trailingStop =
            Math.max(
              trailingStop ||
                0,
              candidate
            );
        }

        trade.currentPrice =
          marketPrice;

        trade.highestPrice =
          highest;

        trade.lowestPrice =
          lowest;

        trade.maxRunupPct =
          runup;

        trade.maxDrawdownPct =
          drawdown;

        trade.trailingActive =
          trailingActive;

        trade.trailingStopPrice =
          trailingStop ||
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
            "TAKE_PROFIT"
          );

          continue;
        }

        if (
          trailingActive &&
          trailingStop > 0 &&
          marketPrice <=
          trailingStop
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
        500
      );
    }

  } catch (err) {

    errLog(
      "Paper monitor",
      err
    );

  } finally {

    paperBusy =
      false;

    state.paper =
      "idle";
  }
}

async function scanOpportunities() {
  if (
    opportunityBusy ||
    shuttingDown
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
          smartScore:
            -1,

          volumeM5:
            -1,

          liquidityUsd:
            -1
        })
        .limit(
          12
        )
        .lean();

    for (
      const token
      of tokens
    ) {

      const count =
        await PaperTrade
          .countDocuments({
            testRun:
              PAPER.testRun,

            status:
              "OPEN"
          });

      if (
        count >=
        PAPER.maxOpenTrades
      ) {
        break;
      }

      await queuePaperEntry(
        token.mint,
        "OPPORTUNITY_RESCAN"
      );

      await sleep(
        400
      );
    }

  } catch (err) {

    errLog(
      "Opportunity",
      err
    );

  } finally {

    opportunityBusy =
      false;
  }
}

function startOpportunityEngine() {
  opportunityTimer =
    setInterval(
      () =>
        scanOpportunities()
          .catch(
            err =>
              errLog(
                "Opportunity interval",
                err
              )
          ),
      PAPER.opportunityMs
    );

  setTimeout(
    () =>
      scanOpportunities()
        .catch(
          err =>
            errLog(
              "Opportunity startup",
              err
            )
        ),
    10000
  );

  log(
    "🎯 Opportunity Engine ONLINE"
  );
}

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

  paperTimer =
    setInterval(
      () =>
        monitorPaperTrades()
          .catch(
            err =>
              errLog(
                "Paper interval",
                err
              )
          ),
      PAPER.monitorMs
    );

  setTimeout(
    () =>
      monitorPaperTrades()
        .catch(
          err =>
            errLog(
              "Paper startup",
              err
            )
        ),
    5000
  );

  log(
    "🧪 Paper Engine ONLINE"
  );
}

function registerTelegramCommands() {
  bot.start(
    ctx =>
      ctx.reply(
        `🤖 LOMY ${VERSION}\n\n` +
        `🔎 Fresh + Established Discovery ON\n` +
        `🛡 Security ON\n` +
        `💧 DEX ON\n` +
        `🐋 Whale Engine ON\n` +
        `🎯 Opportunity Engine ON\n` +
        `🧪 Paper Trading ON\n\n` +
        `🌐 Global Token WebSocket OFF\n` +
        `🔒 LIVE TRADING OFF`
      )
  );

  bot.command(
    "status",
    ctx =>
      ctx.reply(
        `🤖 LOMY ${VERSION} STATUS\n\n` +

        `🌐 Server: ${state.server}\n` +
        `🗄 Database: ${state.database}\n` +
        `👛 Wallet: ${state.wallet}\n` +
        `⚡ Solana: ${state.solana}\n` +
        `📡 Telegram: ${state.telegram}\n\n` +

        `🔎 Discovery: ${state.discovery}\n` +
        `🛡 Security: ${state.security}\n` +
        `💧 DEX: ${state.dex}\n` +
        `🐋 Whales: ${state.whales}\n` +
        `🧪 Paper: ${state.paper}\n\n` +

        `Discovered: ${state.discovered}\n` +
        `Fresh: ${state.fresh}\n` +
        `Established: ${state.established}\n\n` +

        `RPC Queue: ${rpcQueue.length}\n` +
        `RPC 429: ${state.rpc429}\n` +
        `RPC Retries: ${state.rpcRetries}\n` +
        `RPC Dropped: ${state.rpcDropped}\n` +
        `RPC Delay: ${Math.round(rpcDelay)}ms\n\n` +

        `🔒 LIVE TRADING OFF`
      )
  );

  bot.command(
    "network",
    ctx =>
      ctx.reply(
        `🌐 NETWORK SAFE STATUS\n\n` +

        `⚡ Solana: ${state.solana}\n` +
        `📡 Telegram: ${state.telegram}\n` +
        `🔌 Token WebSocket: DISABLED ✅\n\n` +

        `RPC Queue: ${rpcQueue.length}\n` +
        `RPC 429: ${state.rpc429}\n` +
        `RPC Retries: ${state.rpcRetries}\n` +
        `RPC Dropped: ${state.rpcDropped}\n` +
        `RPC Delay: ${Math.round(rpcDelay)}ms\n\n` +

        `DEX Errors: ${state.dexErrors}\n` +
        `Errors: ${state.errors}`
      )
  );

  bot.command(
    "paper",
    async ctx => {

      const summary =
        await getPaperSummary();

      const a =
        summary.account;

      const total =
        num(
          a.totalTrades
        );

      const winRate =
        total > 0
          ?
          num(
            a.wins
          ) /
          total *
          100
          :
          0;

      await ctx.reply(
        `🧪 PAPER ACCOUNT ${VERSION}\n\n` +

        `Starting: $${num(a.startingBalanceUsd).toFixed(2)}\n` +

        `Cash: $${num(a.cashBalanceUsd).toFixed(2)}\n` +

        `Open Value: $${summary.openValue.toFixed(2)}\n` +

        `Equity: $${summary.equity.toFixed(2)}\n` +

        `Realized PnL: $${num(a.realizedPnlUsd).toFixed(2)}\n` +

        `Unrealized PnL: $${summary.unrealizedPnl.toFixed(2)}\n\n` +

        `Open Trades: ${summary.openTrades.length}/${PAPER.maxOpenTrades}\n` +

        `Closed Trades: ${total}\n` +

        `Wins: ${num(a.wins)}\n` +

        `Losses: ${num(a.losses)}\n` +

        `Win Rate: ${winRate.toFixed(1)}%\n\n` +

        `Entry Attempts: ${state.paperEntryAttempts}\n` +

        `Entry Skips: ${state.paperEntrySkips}\n` +

        `Last Skip: ${state.lastSkip || "NONE"}\n\n` +

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
            openedAt:
              -1
          })
          .lean();

      if (
        !trades.length
      ) {
        return ctx.reply(
          "🧪 لا توجد Paper Positions مفتوحة حالياً."
        );
      }

      let text =
        `🧪 OPEN POSITIONS (${trades.length})\n`;

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

        text +=
          `\n━━━━━━━━━━━━━━\n` +

          `${i + 1}) ${t.mint}\n` +

          `Source: ${t.source}\n` +

          `Entry: ${num(t.entryPrice)}\n` +

          `Current: ${current}\n` +

          `PnL: ${pctChange(current, num(t.entryPrice)).toFixed(2)}%\n` +

          `SL: ${num(t.hardStopPrice)}\n` +

          `Trail: ${
            t.trailingActive
              ?
              num(
                t.trailingStopPrice
              )
              :
              "OFF"
          }\n`;
      }

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
            closedAt:
              -1
          })
          .limit(
            10
          )
          .lean();

      if (
        !trades.length
      ) {
        return ctx.reply(
          "📚 لا توجد صفقات مغلقة حتى الآن."
        );
      }

      let text =
        `📚 LAST TRADES (${trades.length})\n`;

      for (
        let i = 0;
        i <
        trades.length;
        i++
      ) {

        const t =
          trades[i];

        text +=
          `\n━━━━━━━━━━━━━━\n` +

          `${i + 1}) ${t.mint}\n` +

          `Source: ${t.source}\n` +

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
          `\n━━━━━━━━━━━━━━\n` +

          `${i + 1}) ${t.mint}\n` +

          `Source: ${t.origin}\n` +

          `🧠 Smart: ${num(t.smartScore)}/100\n` +

          `🛡 Security: ${num(t.securityScore)}/100\n` +

          `💧 DEX: ${num(t.dexScore)}/100\n` +

          `🐋 Whale: ${num(t.whaleScore)}/100 ${t.whaleDecision}\n` +

          `💵 Liquidity: $${num(t.liquidityUsd).toFixed(0)}\n`;
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
          num(
            a.wins
          ) /
          total *
          100
          :
          0;

      const returnPct =
        num(
          a.startingBalanceUsd
        ) > 0
          ?
          num(
            a.realizedPnlUsd
          ) /
          num(
            a.startingBalanceUsd
          ) *
          100
          :
          0;

      const sourceStats =
        await PaperTrade
          .aggregate([
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

      let sources =
        "";

      for (
        const s
        of sourceStats
      ) {
        sources +=
          `\n${s._id || "UNKNOWN"}: ${s.trades} trades | $${num(s.pnl).toFixed(2)} | Avg ${num(s.avgPct).toFixed(2)}%`;
      }

      await ctx.reply(
        `📈 PAPER PERFORMANCE ${VERSION}\n\n` +

        `Trades: ${total}\n` +

        `Wins: ${num(a.wins)}\n` +

        `Losses: ${num(a.losses)}\n` +

        `Win Rate: ${winRate.toFixed(1)}%\n\n` +

        `PnL: $${num(a.realizedPnlUsd).toFixed(2)}\n` +

        `Return: ${returnPct.toFixed(2)}%\n` +

        `Best: ${num(a.bestTradePct).toFixed(2)}%\n` +

        `Worst: ${num(a.worstTradePct).toFixed(2)}%\n\n` +

        `📂 BY SOURCE:${
          sources ||
          "\nNo trades yet."
        }`
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
        open,
        closed
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
        `📊 LOMY ${VERSION} STATS\n\n` +

        `Known Tokens: ${total}\n` +

        `Fresh: ${fresh}\n` +

        `Established: ${established}\n` +

        `Approved: ${approved}\n\n` +

        `Paper Open: ${open}\n` +

        `Paper Closed: ${closed}\n\n` +

        `Discovery Cycles: ${state.discoveryCycles}\n` +

        `Opportunity Cycles: ${state.opportunityCycles}\n\n` +

        `RPC 429: ${state.rpc429}\n` +

        `RPC Dropped: ${state.rpcDropped}\n` +

        `DEX Errors: ${state.dexErrors}\n` +

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
          `💰 Wallet: ${(balance / LAMPORTS_PER_SOL).toFixed(6)} SOL\n` +
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

      const me =
        await bot.telegram
          .getMe();

      log(
        `✅ Telegram @${me.username || "BOT"}`
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

            errLog(
              "Telegram polling",
              err
            );
          }
        );

      return;

    } catch {

      state.telegram =
        "retrying";

      log(
        "⚠️ Telegram retry..."
      );

      await sleep(
        10000
      );
    }
  }
}

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
    `⚠️ ${signal} shutdown`
  );

  if (
    paperTimer
  ) {
    clearInterval(
      paperTimer
    );
  }

  if (
    discoveryTimer
  ) {
    clearInterval(
      discoveryTimer
    );
  }

  if (
    opportunityTimer
  ) {
    clearInterval(
      opportunityTimer
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
    telegramAgent
      .destroy();
  } catch {}

  try {
    await mongoose
      .disconnect();
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

process.on(
  "unhandledRejection",
  reason =>
    errLog(
      "Unhandled rejection",
      reason
    )
);

process.on(
  "uncaughtException",
  err =>
    errLog(
      "Uncaught exception",
      err
    )
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

async function main() {
  console.log("");
  console.log(
    "================================"
  );

  console.log(
    `🚀 LOMY ${VERSION}`
  );

  console.log(
    "🌐 NETWORK SAFE ENGINE"
  );

  console.log(
    "🔎 FRESH + ESTABLISHED DISCOVERY"
  );

  console.log(
    "🛡 SECURITY ENGINE"
  );

  console.log(
    "💧 DEX ENGINE"
  );

  console.log(
    "🐋 WHALE ENGINE - QUICKNODE 5+5 SAFE"
  );

  console.log(
    "🎯 OPPORTUNITY ENGINE"
  );

  console.log(
    "🧪 PAPER TRADING ENGINE"
  );

  console.log(
    "🔌 GLOBAL TOKEN WEBSOCKET DISABLED"
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

  await ensurePaperAccount();

  startTelegram()
    .catch(
      err =>
        errLog(
          "Telegram",
          err
        )
    );

  startPaperEngine();

  startOpportunityEngine();

  startDiscovery();

  log(
    `✅ LOMY ${VERSION} STARTED`
  );

  log(
    "🔎 FRESH + ESTABLISHED SCANNER ACTIVE"
  );

  log(
    "🌐 NETWORK SAFE MODE ACTIVE"
  );

  log(
    "🧪 PAPER TEST ACTIVE"
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
