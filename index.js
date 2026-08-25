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

// ======================================================
// LOMY SOLANA HUNTER V4 COMPACT
// PAPER MODE ONLY - NO BUY / NO SELL
// ======================================================

const app = express();
app.use(express.json());

const PORT = Number(process.env.PORT) || 10000;
const RPC_URL =
  process.env.RPC_URL || "https://api.mainnet-beta.solana.com";

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

// ======================================================
// HARD SAFETY LOCK
// ======================================================

const MODE = "PAPER";
const LIVE_TRADING = false;

// لا توجد أي دالة شراء أو بيع في هذا الملف.

// ======================================================
// STATE
// ======================================================

let wallet = null;
let bot = null;
let server = null;

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
  scanned: 0,
  dexScanned: 0,
  candidates: 0,
  errors: 0,
  lastMint: null
};

function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

function errorLog(title, err) {
  state.errors++;
  console.error(
    new Date().toISOString(),
    "❌",
    title,
    err?.message || err
  );
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

// ======================================================
// DATABASE
// ======================================================

const tokenSchema = new mongoose.Schema(
  {
    mint: {
      type: String,
      required: true,
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

    securityScore: {
      type: Number,
      default: null
    },

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
  { timestamps: true }
);

const FreshToken =
  mongoose.models.FreshToken ||
  mongoose.model("FreshToken", tokenSchema);

// ======================================================
// RENDER HTTP SERVER
// ======================================================

app.get("/", (req, res) => {
  res.status(200).send(
    "✅ LOMY V4 ONLINE | PAPER MODE | NO LIVE TRADING"
  );
});

app.get("/health", (req, res) => {
  res.status(200).json({
    ...state,
    mode: MODE,
    liveTrading: LIVE_TRADING,
    uptime: Math.floor(process.uptime())
  });
});

function startServer() {
  return new Promise((resolve, reject) => {
    server = app.listen(PORT, "0.0.0.0", () => {
      state.server = "online";
      log(`🌐 Render server online PORT ${PORT}`);
      resolve();
    });

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

    await mongoose.connect(MONGODB_URI, {
      serverSelectionTimeoutMS: 10000
    });

    state.database = "connected";
    log("✅ MongoDB connected");
  } catch (err) {
    state.database = "error";
    errorLog("MongoDB", err);
  }
}

mongoose.connection.on("disconnected", () => {
  state.database = "disconnected";
});

mongoose.connection.on("reconnected", () => {
  state.database = "connected";
});

// ======================================================
// WALLET
// ======================================================

async function loadWallet() {
  try {
    if (!BOT_PRIVATE_KEY) {
      throw new Error("BOT_PRIVATE_KEY missing");
    }

    const key = BOT_PRIVATE_KEY.trim();

    if (key.includes(" ") && bip39.validateMnemonic(key)) {
      const seed = bip39.mnemonicToSeedSync(key);

      const derived = derivePath(
        "m/44'/501'/0'/0'",
        seed.toString("hex")
      ).key;

      wallet = Keypair.fromSeed(derived);

    } else if (key.startsWith("[")) {

      wallet = Keypair.fromSecretKey(
        Uint8Array.from(JSON.parse(key))
      );

    } else {

      wallet = Keypair.fromSecretKey(
        bs58.decode(key)
      );
    }

    state.wallet = "loaded";

    log(
      "✅ Wallet:",
      wallet.publicKey.toString()
    );

  } catch (err) {
    state.wallet = "error";
    errorLog("Wallet", err);
  }
}

// ======================================================
// SOLANA TEST
// ======================================================

async function testSolana() {
  if (!wallet) return;

  try {
    const balance = await connection.getBalance(
      wallet.publicKey
    );

    state.solana = "connected";

    log(
      "✅ Solana connected | Balance:",
      (balance / LAMPORTS_PER_SOL).toFixed(6),
      "SOL"
    );

  } catch (err) {
    state.solana = "error";
    errorLog("Solana RPC", err);
  }
}

// ======================================================
// SECURITY ENGINE V2
// ======================================================

async function securityScan(mint) {
  state.security = "scanning";

  try {
    const pubkey = new PublicKey(mint);

    const account =
      await connection.getParsedAccountInfo(
        pubkey,
        "confirmed"
      );

    if (!account.value) {
      throw new Error("Mint account unavailable");
    }

    const owner = account.value.owner.toString();
    const parsed = account.value.data?.parsed;

    if (!parsed || parsed.type !== "mint") {
      throw new Error("Invalid mint account");
    }

    const info = parsed.info || {};

    const mintRevoked =
      info.mintAuthority == null;

    const freezeRevoked =
      info.freezeAuthority == null;

    const token2022 =
      owner === TOKEN_2022_PROGRAM_ID.toString();

    let score = 50;

    score += mintRevoked ? 20 : -20;
    score += freezeRevoked ? 20 : -25;

    const decimals = Number(info.decimals || 0);

    if (decimals >= 0 && decimals <= 18) {
      score += 5;
    } else {
      score -= 10;
    }

    if (String(info.supply || "0") !== "0") {
      score += 5;
    } else {
      score -= 15;
    }

    // Token-2022 يحتاج تحليل أعمق لاحقاً.
    // لذلك لا نعطيه PASS قوي تلقائياً.
    if (token2022) {
      score -= 5;
    }

    score = Math.max(
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
          securityScore: score,
          securityDecision: decision,
          mintAuthorityRevoked: mintRevoked,
          freezeAuthorityRevoked: freezeRevoked,
          decimals,
          supply: String(info.supply || "0"),
          token2022
        }
      }
    );

    state.scanned++;

    log(
      `🛡 ${mint} | Security ${score}/100 | ${decision}`
    );

    if (decision !== "REJECT") {
      await dexScan(mint);
    }

  } catch (err) {
    errorLog(`Security ${mint}`, err);

  } finally {
    state.security = "idle";
  }
}

// ======================================================
// DEX SCREENER
// ======================================================

async function getDexPairs(mint) {
  const controller = new AbortController();

  const timeout = setTimeout(
    () => controller.abort(),
    8000
  );

  try {
    const response = await fetch(
      `https://api.dexscreener.com/token-pairs/v1/solana/${encodeURIComponent(mint)}`,
      {
        headers: {
          Accept: "application/json"
        },
        signal: controller.signal
      }
    );

    if (!response.ok) {
      throw new Error(
        `DEX HTTP ${response.status}`
      );
    }

    const data = await response.json();

    return Array.isArray(data) ? data : [];

  } finally {
    clearTimeout(timeout);
  }
}

function num(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function bestPool(pairs) {
  if (!pairs.length) return null;

  return [...pairs]
    .filter(p => p?.chainId === "solana")
    .sort(
      (a, b) =>
        num(b?.liquidity?.usd) -
        num(a?.liquidity?.usd)
    )[0] || null;
}

// ======================================================
// DEX / LIQUIDITY ENGINE
// ======================================================

async function dexScan(mint) {
  state.dex = "scanning";

  try {
    const pairs = await getDexPairs(mint);
    const pair = bestPool(pairs);

    state.dexScanned++;

    if (!pair) {
      await FreshToken.updateOne(
        { mint },
        {
          $set: {
            dexChecked: true,
            dexListed: false,
            dexDecision: "NO_POOL",
            finalDecision: "WAITING_DEX"
          }
        }
      );

      log(`⏳ No DEX pool: ${mint}`);
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
    } else if (liquidity >= 3000) {
      dexScore += 25;
    }

    if (volumeM5 >= 250) {
      dexScore += 20;
    }

    if (volumeH1 > 0) {
      dexScore += 10;
    }

    if ((buys + sells) >= 5) {
      dexScore += 15;
    }

    if (buys > 0 && sells > 0) {
      dexScore += 15;
    }

    dexScore = Math.min(
      100,
      dexScore
    );

    let dexDecision = "WATCH";

    if (
      liquidity >= 3000 &&
      dexScore >= 60 &&
      sells > 0
    ) {
      dexDecision = "PASS";
    }

    const token =
      await FreshToken.findOne({ mint }).lean();

    const securityScore =
      num(token?.securityScore);

    const finalScore = Math.round(
      securityScore * 0.6 +
      dexScore * 0.4
    );

    let finalDecision = "WATCH";

    if (
      token?.securityDecision === "PASS" &&
      dexDecision === "PASS" &&
      finalScore >= 70
    ) {
      finalDecision = "CANDIDATE";
      state.candidates++;
    }

    await FreshToken.updateOne(
      { mint },
      {
        $set: {
          dexChecked: true,
          dexListed: true,

          dexId: pair.dexId || null,
          pairAddress: pair.pairAddress || null,

          liquidityUsd: liquidity,
          volumeM5,
          volumeH1,

          buysM5: buys,
          sellsM5: sells,

          dexScore,
          dexDecision,

          finalScore,
          finalDecision
        }
      }
    );

    log(
      `💧 ${mint} | $${liquidity.toFixed(0)} liquidity | DEX ${dexScore}/100 | FINAL ${finalScore}/100 ${finalDecision}`
    );

  } catch (err) {
    errorLog(`DEX ${mint}`, err);

  } finally {
    state.dex = "idle";
  }
}

// ======================================================
// FIND INITIALIZE MINT
// ======================================================

function findMint(instructions) {
  if (!Array.isArray(instructions)) {
    return null;
  }

  for (const ix of instructions) {
    const type = ix?.parsed?.type;

    if (
      type === "initializeMint" ||
      type === "initializeMint2"
    ) {
      return ix?.parsed?.info?.mint || null;
    }
  }

  return null;
}

// ======================================================
// PROCESS NEW TOKEN
// ======================================================

const processing = new Set();

async function processLogs(event, program) {
  if (event.err) return;

  const relevant = (event.logs || []).some(
    line =>
      line.includes("Instruction: InitializeMint") ||
      line.includes("Instruction: InitializeMint2")
  );

  if (!relevant) return;

  const signature = event.signature;

  if (!signature || processing.has(signature)) {
    return;
  }

  processing.add(signature);

  try {
    let tx = null;

    for (let attempt = 0; attempt < 4; attempt++) {
      tx = await connection
        .getParsedTransaction(signature, {
          commitment: "confirmed",
          maxSupportedTransactionVersion: 0
        })
        .catch(() => null);

      if (tx) break;

      await sleep(500 + attempt * 500);
    }

    if (!tx) return;

    let mint = findMint(
      tx.transaction.message.instructions
    );

    if (!mint && tx.meta?.innerInstructions) {
      for (const group of tx.meta.innerInstructions) {
        mint = findMint(group.instructions);

        if (mint) break;
      }
    }

    if (!mint) return;

    state.detected++;
    state.lastMint = mint;

    const existing =
      await FreshToken.findOne({ mint }).lean();

    if (!existing) {
      await FreshToken.create({
        mint,
        signature,
        program,
        paperOnly: true
      });

      log("🆕 Fresh Mint:", mint);
    }

    const current =
      existing ||
      await FreshToken.findOne({ mint }).lean();

    if (!current?.securityChecked) {
      await securityScan(mint);
    }

  } catch (err) {
    errorLog("Hunter processing", err);

  } finally {
    processing.delete(signature);
  }
}

// ======================================================
// HUNTER
// ======================================================

function startHunter() {
  connection.onLogs(
    TOKEN_PROGRAM_ID,
    event => {
      processLogs(event, "SPL_TOKEN")
        .catch(err =>
          errorLog("SPL Hunter", err)
        );
    },
    "confirmed"
  );

  connection.onLogs(
    TOKEN_2022_PROGRAM_ID,
    event => {
      processLogs(event, "TOKEN_2022")
        .catch(err =>
          errorLog("Token2022 Hunter", err)
        );
    },
    "confirmed"
  );

  state.hunter = "running";

  log("🔎 Fresh Token Hunter running");
}

// ======================================================
// RECOVER OLD PENDING TOKENS
// ======================================================

async function recoverPending() {
  if (mongoose.connection.readyState !== 1) {
    return;
  }

  try {
    const pending = await FreshToken
      .find({
        securityChecked: false
      })
      .sort({ detectedAt: -1 })
      .limit(50)
      .select("mint")
      .lean();

    log(
      `♻️ Recovering ${pending.length} pending tokens`
    );

    // واحدة وراء واحدة لحماية RPC
    for (const token of pending) {
      await securityScan(token.mint);
      await sleep(500);
    }

  } catch (err) {
    errorLog("Recovery", err);
  }
}

// ======================================================
// TELEGRAM
// ======================================================

async function startTelegram() {
  if (!TELEGRAM_TOKEN) {
    state.telegram = "missing_config";
    return;
  }

  try {
    bot = new Telegraf(TELEGRAM_TOKEN);

    bot.start(ctx =>
      ctx.reply(
        "🤖 LOMY SOLANA HUNTER V4\n\n" +
        "🔎 Hunter: ON\n" +
        "🛡 Security V2: ON\n" +
        "💧 DEX Engine: ON\n" +
        "🧪 PAPER MODE\n" +
        "🔒 NO BUY / NO SELL"
      )
    );

    bot.command("status", async ctx => {
      await ctx.reply(
        `🤖 LOMY V4 STATUS\n\n` +
        `🌐 Server: ${state.server}\n` +
        `🗄 Database: ${state.database}\n` +
        `👛 Wallet: ${state.wallet}\n` +
        `⚡ Solana: ${state.solana}\n` +
        `📡 Telegram: ${state.telegram}\n` +
        `🔎 Hunter: ${state.hunter}\n` +
        `🛡 Security: ${state.security}\n` +
        `💧 DEX: ${state.dex}\n\n` +
        `🧪 Mode: PAPER\n` +
        `🔒 Live Trading: OFF`
      );
    });

    bot.command("balance", async ctx => {
      if (!wallet) {
        return ctx.reply(
          "❌ Wallet unavailable"
        );
      }

      try {
        const balance =
          await connection.getBalance(
            wallet.publicKey
          );

        await ctx.reply(
          `💰 الرصيد: ${(balance / LAMPORTS_PER_SOL).toFixed(6)} SOL`
        );

      } catch {
        await ctx.reply(
          "❌ Balance error"
        );
      }
    });

    bot.command("mode", ctx =>
      ctx.reply(
        "🧪 PAPER MODE\n\n" +
        "❌ BUY: OFF\n" +
        "❌ SELL: OFF\n" +
        "🔎 Hunter: ON\n" +
        "🛡 Security: ON\n" +
        "💧 DEX Analysis: ON"
      )
    );

    bot.command("security", async ctx => {
      const last = await FreshToken
        .findOne({
          securityChecked: true
        })
        .sort({ updatedAt: -1 })
        .lean();

      if (!last) {
        return ctx.reply(
          "🛡 SECURITY V2\n\nNo scans yet."
        );
      }

      await ctx.reply(
        `🛡 SECURITY ENGINE V2\n\n` +
        `Scanned this run: ${state.scanned}\n\n` +
        `Last Mint:\n${last.mint}\n\n` +
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
          last.token2022 ? "YES" : "NO"
        }\n\n` +
        `🔒 PAPER ONLY`
      );
    });

    bot.command("dex", async ctx => {
      const last = await FreshToken
        .findOne({
          dexListed: true
        })
        .sort({ updatedAt: -1 })
        .lean();

      if (!last) {
        return ctx.reply(
          "💧 DEX ENGINE\n\nNo listed pools found yet."
        );
      }

      await ctx.reply(
        `💧 DEX / LIQUIDITY\n\n` +
        `Mint:\n${last.mint}\n\n` +
        `DEX: ${last.dexId || "Unknown"}\n` +
        `Liquidity: $${num(last.liquidityUsd).toFixed(2)}\n` +
        `Volume M5: $${num(last.volumeM5).toFixed(2)}\n` +
        `Buys M5: ${last.buysM5 || 0}\n` +
        `Sells M5: ${last.sellsM5 || 0}\n\n` +
        `DEX Score: ${last.dexScore}/100\n` +
        `DEX: ${last.dexDecision}\n` +
        `Final Score: ${last.finalScore}/100\n` +
        `Final: ${last.finalDecision}\n\n` +
        `🔒 PAPER ONLY`
      );
    });

    bot.command("stats", async ctx => {
      const [
        total,
        securityPass,
        listed,
        candidates,
        rejected
      ] = await Promise.all([
        FreshToken.countDocuments(),
        FreshToken.countDocuments({
          securityDecision: "PASS"
        }),
        FreshToken.countDocuments({
          dexListed: true
        }),
        FreshToken.countDocuments({
          finalDecision: "CANDIDATE"
        }),
        FreshToken.countDocuments({
          finalDecision: "REJECT"
        })
      ]);

      await ctx.reply(
        `📊 LOMY V4 STATS\n\n` +
        `Total Tokens: ${total}\n` +
        `Security PASS: ${securityPass} ✅\n` +
        `DEX Listed: ${listed} 💧\n` +
        `Candidates: ${candidates} 🎯\n` +
        `Rejected: ${rejected} ❌\n` +
        `Errors this run: ${state.errors}\n\n` +
        `🧪 PAPER MODE`
      );
    });

    bot.command("candidates", async ctx => {
      const tokens = await FreshToken
        .find({
          finalDecision: "CANDIDATE"
        })
        .sort({
          finalScore: -1,
          detectedAt: -1
        })
        .limit(5)
        .lean();

      if (!tokens.length) {
        return ctx.reply(
          "🎯 CANDIDATES\n\n" +
          "لا توجد عملات اجتازت الفلاتر حتى الآن.\n\n" +
          "🔒 PAPER ONLY"
        );
      }

      let text =
        "🎯 TOP CANDIDATES\n\n";

      tokens.forEach((t, i) => {
        text +=
          `${i + 1}) ${t.mint}\n` +
          `Final: ${t.finalScore}/100\n` +
          `Security: ${t.securityScore}/100\n` +
          `Liquidity: $${num(t.liquidityUsd).toFixed(0)}\n\n`;
      });

      text += "🔒 PAPER ONLY";

      await ctx.reply(text);
    });

    bot.catch(err => {
      errorLog("Telegram", err);
    });

    state.telegram = "starting";

    await bot.launch();

    state.telegram = "online";

    log("✅ Telegram online");

  } catch (err) {
    state.telegram = "error";
    errorLog("Telegram start", err);
  }
}

// ======================================================
// ERRORS / SHUTDOWN
// ======================================================

process.on("unhandledRejection", reason => {
  errorLog(
    "Unhandled rejection",
    reason instanceof Error
      ? reason
      : new Error(String(reason))
  );
});

process.on("uncaughtException", err => {
  errorLog("Uncaught exception", err);

  // خطأ غير متوقع: نخرج لكي Render يعيد تشغيل الخدمة
  setTimeout(() => process.exit(1), 1000);
});

async function shutdown(signal) {
  log(`⚠️ ${signal} safe shutdown`);

  try {
    if (bot) bot.stop(signal);
  } catch {}

  try {
    await mongoose.disconnect();
  } catch {}

  if (server) {
    server.close(() => process.exit(0));

    setTimeout(
      () => process.exit(0),
      5000
    );
  } else {
    process.exit(0);
  }
}

process.once(
  "SIGTERM",
  () => shutdown("SIGTERM")
);

process.once(
  "SIGINT",
  () => shutdown("SIGINT")
);

// ======================================================
// MAIN
// ======================================================

async function main() {
  console.log("");
  console.log("==============================");
  console.log("🚀 LOMY SOLANA HUNTER V4");
  console.log("🧪 PAPER MODE");
  console.log("🔒 NO BUY / NO SELL");
  console.log("==============================");

  // Render لازم يفتح الـPORT أولاً
  await startServer();

  await connectDatabase();
  await loadWallet();
  await testSolana();

  // Telegram قبل استرجاع الـbacklog حتى يبقى متاح بسرعة
  await startTelegram();

  if (state.solana === "connected") {
    startHunter();
  }

  // استكمال جزء من البيانات القديمة بدون تعطيل التشغيل
  recoverPending().catch(
    err => errorLog("Recovery background", err)
  );

  log("✅ LOMY V4 STARTED");
  log("🔒 LIVE TRADING DISABLED");
}

main().catch(err => {
  errorLog("MAIN", err);
});
