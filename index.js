'use strict';

/**
 * LOMY FOREX V1.0.1 — PAPER TEST
 * Core: Ultra-Fast Scalp Engine V5.1
 * 30 Forex pairs | 1m CLOSED candles | PAPER ONLY
 */

const express = require('express');
const axios = require('axios');
const mongoose = require('mongoose');
const { Telegraf } = require('telegraf');

// ============================================================
// CONFIG
// ============================================================

const VERSION = 'LOMY FOREX V1.0.1';
const MODE = 'PAPER';
const LIVE_TRADING = false;

const PORT = Number(process.env.PORT || 10000);
const MONGODB_URI = process.env.MONGODB_URI || '';
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const TWELVE_DATA_API_KEY = process.env.TWELVE_DATA_API_KEY || '';

const BIQUOTE_BASE = 'https://biquote.io';
const HTTP_TIMEOUT_MS = 12000;
const HISTORY_LIMIT = 80;
const OHLC_CONCURRENCY = 4;

const CLOCK_CHECK_MS = 2000;
const SCAN_AFTER_SECOND = 3;
const QUOTE_POLL_MS = 3000;

const HEARTBEAT_MS = 5 * 60 * 1000;

// ============================================================
// 30 FOREX PAIRS
// ============================================================

const PAIRS = [
  'EURUSD',
  'GBPUSD',
  'USDJPY',
  'USDCHF',
  'AUDUSD',
  'NZDUSD',
  'USDCAD',

  'EURGBP',
  'EURJPY',
  'EURCHF',
  'EURAUD',
  'EURNZD',
  'EURCAD',

  'GBPJPY',
  'GBPCHF',
  'GBPAUD',
  'GBPNZD',
  'GBPCAD',

  'AUDJPY',
  'AUDCHF',
  'AUDNZD',
  'AUDCAD',

  'NZDJPY',
  'NZDCHF',
  'NZDCAD',

  'CADJPY',
  'CADCHF',
  'CHFJPY',

  'GBPSGD',
  'EURSGD'
];

// ============================================================
// ULTRA-FAST SCALP ENGINE V5.1
// ============================================================

const STRATEGY = Object.freeze({
  cmoLen: 9,

  cmoBuyThresh: 30,
  cmoSellThresh: -30,

  volLen: 10,
  volMult: 1.3,

  srLen: 20,

  fastEmaLen: 9,
  slowEmaLen: 21,

  atrLen: 14,
  atrMargin: 0.20,

  riskReward: 2.0,

  bodyRatioMin: 0.50
});

// ============================================================
// PAPER ACCOUNT
// ============================================================

const PAPER = Object.freeze({
  startingBalance: 1000,

  riskPctPerTrade: 0.50,

  maxOpenTrades: 30,

  accountKey: 'lomy-forex-v101-main'
});

// ============================================================
// STATE
// ============================================================

const state = {
  startedAt: new Date(),

  mongoReady: false,
  telegramReady: false,
  marketReady: false,

  initializing: true,

  scanRunning: false,
  quoteRunning: false,

  lastGlobalScanMinute: null,

  lastSignalScanAt: null,
  lastQuotePollAt: null,

  lastMarketError: null,

  totalSignalScans: 0,
  totalQuotePolls: 0,

  rawSignals: 0,
  executedSignals: 0,
  skippedSignals: 0,

  latestQuotes: new Map(),

  openTrades: new Map(),

  pairState: new Map()
};

// ============================================================
// PAIR STATE
// ============================================================

for (const symbol of PAIRS) {
  state.pairState.set(symbol, {
    bars: [],

    lastClosedBarTime: null,

    lastSignal: 0,

    lastAnalysis: null,

    initialized: false,

    errors: 0
  });
}

// ============================================================
// HELPERS
// ============================================================

function n(value, fallback = NaN) {
  const number = Number(value);

  return Number.isFinite(number)
    ? number
    : fallback;
}

function safeError(err) {
  const data = err?.response?.data;

  if (typeof data === 'string') {
    return data.slice(0, 300);
  }

  return (
    data?.message ||
    data?.status ||
    err?.message ||
    String(err)
  );
}

function fmtPrice(value, symbol = '') {
  const number = Number(value);

  if (!Number.isFinite(number)) {
    return 'n/a';
  }

  return number.toFixed(
    symbol.endsWith('JPY')
      ? 3
      : 5
  );
}

function fmtMoney(value) {
  const number = Number(value);

  return Number.isFinite(number)
    ? `$${number.toFixed(2)}`
    : '$0.00';
}

function fmtPct(value) {
  const number = Number(value);

  return Number.isFinite(number)
    ? `${number.toFixed(2)}%`
    : '0.00%';
}

function barTimeMs(bar) {
  const time =
    new Date(bar.openTime).getTime();

  return Number.isFinite(time)
    ? time
    : 0;
}

function minuteKey(date = new Date()) {
  return date
    .toISOString()
    .slice(0, 16);
}

// ============================================================
// NORMALIZE BARS
// ============================================================

function normalizeBars(rawBars) {
  if (!Array.isArray(rawBars)) {
    return [];
  }

  return rawBars
    .map((bar) => ({
      openTime:
        bar.openTime ||
        bar.datetime ||
        bar.timestamp ||
        bar.time,

      open:
        n(bar.open),

      high:
        n(bar.high),

      low:
        n(bar.low),

      close:
        n(bar.close),

      volume:
        n(
          bar.tickVolume,
          n(bar.volume, 0)
        ),

      isOpen:
        bar.isOpen === true
    }))
    .filter((bar) =>
      Boolean(bar.openTime) &&
      [
        bar.open,
        bar.high,
        bar.low,
        bar.close
      ].every(Number.isFinite)
    )
    .sort(
      (a, b) =>
        barTimeMs(a) -
        barTimeMs(b)
    );
}

// ============================================================
// MERGE CLOSED BARS
// ============================================================

function mergeClosedBars(
  existing,
  incoming,
  maxBars = HISTORY_LIMIT
) {
  const map = new Map();

  for (const bar of existing || []) {
    map.set(
      bar.openTime,
      bar
    );
  }

  for (const bar of incoming || []) {
    if (!bar.isOpen) {
      map.set(
        bar.openTime,
        bar
      );
    }
  }

  return [...map.values()]
    .sort(
      (a, b) =>
        barTimeMs(a) -
        barTimeMs(b)
    )
    .slice(-maxBars);
}

// ============================================================
// CONCURRENCY
// ============================================================

async function mapWithConcurrency(
  items,
  concurrency,
  callback
) {
  const results =
    new Array(items.length);

  let cursor = 0;

  async function worker() {
    while (true) {
      const index = cursor++;

      if (index >= items.length) {
        return;
      }

      try {
        results[index] =
          await callback(
            items[index],
            index
          );
      } catch (error) {
        results[index] = {
          error
        };
      }
    }
  }

  await Promise.all(
    Array.from(
      {
        length:
          Math.min(
            concurrency,
            items.length
          )
      },
      () => worker()
    )
  );

  return results;
}

// ============================================================
// SMA
// ============================================================

function sma(values, length) {
  if (
    !Array.isArray(values) ||
    values.length < length
  ) {
    return NaN;
  }

  const window =
    values.slice(-length);

  const total =
    window.reduce(
      (sum, value) =>
        sum + value,
      0
    );

  return total / length;
}

// ============================================================
// EMA
// ============================================================

function emaSeries(values, length) {
  if (
    !Array.isArray(values) ||
    values.length < length
  ) {
    return [];
  }

  const output =
    Array(values.length).fill(NaN);

  let seed = 0;

  for (
    let i = 0;
    i < length;
    i++
  ) {
    seed += values[i];
  }

  let previous =
    seed / length;

  output[length - 1] =
    previous;

  const alpha =
    2 / (length + 1);

  for (
    let i = length;
    i < values.length;
    i++
  ) {
    previous =
      values[i] * alpha +
      previous * (1 - alpha);

    output[i] =
      previous;
  }

  return output;
}

// ============================================================
// RMA
// ============================================================

function rmaSeries(values, length) {
  if (
    !Array.isArray(values) ||
    values.length < length
  ) {
    return [];
  }

  const output =
    Array(values.length).fill(NaN);

  let seed = 0;

  for (
    let i = 0;
    i < length;
    i++
  ) {
    seed += values[i];
  }

  let previous =
    seed / length;

  output[length - 1] =
    previous;

  const alpha =
    1 / length;

  for (
    let i = length;
    i < values.length;
    i++
  ) {
    previous =
      alpha * values[i] +
      (1 - alpha) * previous;

    output[i] =
      previous;
  }

  return output;
}

// ============================================================
// ATR
// ============================================================

function atrValue(bars, length) {
  if (
    !Array.isArray(bars) ||
    bars.length < length + 1
  ) {
    return NaN;
  }

  const trueRanges = [];

  for (
    let i = 0;
    i < bars.length;
    i++
  ) {
    if (i === 0) {
      trueRanges.push(
        bars[i].high -
        bars[i].low
      );

      continue;
    }

    const previousClose =
      bars[i - 1].close;

    trueRanges.push(
      Math.max(
        bars[i].high -
          bars[i].low,

        Math.abs(
          bars[i].high -
          previousClose
        ),

        Math.abs(
          bars[i].low -
          previousClose
        )
      )
    );
  }

  const rma =
    rmaSeries(
      trueRanges,
      length
    );

  return rma[
    rma.length - 1
  ];
}

// ============================================================
// CMO
// ============================================================

function cmoValue(
  closes,
  length
) {
  if (
    !Array.isArray(closes) ||
    closes.length < length + 1
  ) {
    return NaN;
  }

  let gains = 0;
  let losses = 0;

  const start =
    closes.length - length;

  for (
    let i = start;
    i < closes.length;
    i++
  ) {
    const difference =
      closes[i] -
      closes[i - 1];

    if (difference > 0) {
      gains +=
        difference;
    } else if (difference < 0) {
      losses +=
        -difference;
    }
  }

  const denominator =
    gains + losses;

  if (denominator === 0) {
    return 0;
  }

  return (
    100 *
    (gains - losses) /
    denominator
  );
}

// ============================================================
// V5.1 SIGNAL ENGINE
// ============================================================

function analyzeV51(bars) {
  const minimumBars =
    Math.max(
      STRATEGY.slowEmaLen + 2,
      STRATEGY.atrLen + 2,
      STRATEGY.srLen + 2,
      STRATEGY.volLen + 2,
      STRATEGY.cmoLen + 2
    );

  if (
    !Array.isArray(bars) ||
    bars.length < minimumBars
  ) {
    return null;
  }

  const current =
    bars[bars.length - 1];

  const previousBars =
    bars.slice(0, -1);

  const closes =
    bars.map(
      (bar) =>
        bar.close
    );

  const volumes =
    bars.map(
      (bar) =>
        Number.isFinite(bar.volume)
          ? bar.volume
          : 0
    );

  // ==========================================================
  // CANDLE
  // ==========================================================

  const candleBody =
    Math.abs(
      current.close -
      current.open
    );

  const candleRange =
    current.high -
    current.low;

  const bodyRatio =
    candleRange > 0
      ? candleBody /
        candleRange
      : 0;

  const strongBull =
    current.close >
      current.open &&
    bodyRatio >=
      STRATEGY.bodyRatioMin;

  const strongBear =
    current.close <
      current.open &&
    bodyRatio >=
      STRATEGY.bodyRatioMin;

  // ==========================================================
  // VOLUME
  // ==========================================================

  const volumeAverage =
    sma(
      volumes,
      STRATEGY.volLen
    );

  const highVolume =
    Number.isFinite(
      volumeAverage
    ) &&
    volumeAverage > 0 &&
    current.volume >
      volumeAverage *
      STRATEGY.volMult;

  // ==========================================================
  // CMO
  // ==========================================================

  const cmo =
    cmoValue(
      closes,
      STRATEGY.cmoLen
    );

  const bullMomentum =
    Number.isFinite(cmo) &&
    cmo >
      STRATEGY.cmoBuyThresh;

  const bearMomentum =
    Number.isFinite(cmo) &&
    cmo <
      STRATEGY.cmoSellThresh;

  // ==========================================================
  // EMA
  // ==========================================================

  const fastSeries =
    emaSeries(
      closes,
      STRATEGY.fastEmaLen
    );

  const slowSeries =
    emaSeries(
      closes,
      STRATEGY.slowEmaLen
    );

  const fastEMA =
    fastSeries[
      fastSeries.length - 1
    ];

  const slowEMA =
    slowSeries[
      slowSeries.length - 1
    ];

  const bullTrend =
    Number.isFinite(fastEMA) &&
    Number.isFinite(slowEMA) &&
    fastEMA >
      slowEMA;

  const bearTrend =
    Number.isFinite(fastEMA) &&
    Number.isFinite(slowEMA) &&
    fastEMA <
      slowEMA;

  // ==========================================================
  // ATR
  // ==========================================================

  const atr =
    atrValue(
      bars,
      STRATEGY.atrLen
    );

  // ==========================================================
  // SUPPORT / RESISTANCE
  // ==========================================================

  const srWindow =
    previousBars.slice(
      -STRATEGY.srLen
    );

  const support =
    srWindow.length
      ? Math.min(
          ...srWindow.map(
            (bar) =>
              bar.low
          )
        )
      : NaN;

  const resistance =
    srWindow.length
      ? Math.max(
          ...srWindow.map(
            (bar) =>
              bar.high
          )
        )
      : NaN;

  // ==========================================================
  // EXACT V5.1 CONDITIONS
  // ==========================================================

  const rawBuy =
    strongBull &&
    highVolume &&
    bullMomentum &&
    bullTrend;

  const rawSell =
    strongBear &&
    highVolume &&
    bearMomentum &&
    bearTrend;

  return {
    time:
      current.openTime,

    close:
      current.close,

    bodyRatio,

    volume:
      current.volume,

    volAvg:
      volumeAverage,

    highVolume,

    cmo,

    fastEMA,

    slowEMA,

    bullTrend,

    bearTrend,

    atr,

    support,

    resistance,

    rawBuy,

    rawSell
  };
}

// ============================================================
// HISTORICAL LAST SIGNAL
// ============================================================

function deriveLastSignalFromHistory(
  bars
) {
  let lastSignal = 0;

  const minimum =
    Math.max(
      25,
      STRATEGY.slowEmaLen + 2
    );

  for (
    let i = minimum;
    i <= bars.length;
    i++
  ) {
    const analysis =
      analyzeV51(
        bars.slice(0, i)
      );

    if (!analysis) {
      continue;
    }

    if (
      analysis.rawBuy &&
      lastSignal !== 1
    ) {
      lastSignal = 1;
    } else if (
      analysis.rawSell &&
      lastSignal !== -1
    ) {
      lastSignal = -1;
    }
  }

  return lastSignal;
}

// ============================================================
// DATABASE SCHEMAS
// ============================================================

const paperAccountSchema =
  new mongoose.Schema(
    {
      key: {
        type: String,
        unique: true,
        index: true
      },

      version:
        String,

      mode:
        String,

      startingBalance:
        Number,

      balance:
        Number,

      realizedPnl:
        Number,

      totalTrades:
        Number,

      wins:
        Number,

      losses:
        Number,

      breakeven:
        Number,

      createdAt:
        Date,

      updatedAt:
        Date
    },
    {
      minimize: false
    }
  );

const forexTradeSchema =
  new mongoose.Schema(
    {
      version:
        String,

      mode:
        String,

      symbol: {
        type: String,
        index: true
      },

      direction:
        String,

      status: {
        type: String,
        index: true
      },

      signalTime:
        Date,

      openedAt:
        Date,

      closedAt:
        Date,

      signalEntry:
        Number,

      entryPrice:
        Number,

      stopLoss:
        Number,

      takeProfit:
        Number,

      exitPrice:
        Number,

      exitReason:
        String,

      riskDistance:
        Number,

      riskAmount:
        Number,

      pnlR:
        Number,

      pnlUsd:
        Number,

      balanceAfter:
        Number,

      spreadAtEntry:
        Number,

      spreadAtExit:
        Number,

      signal:
        mongoose.Schema.Types.Mixed,

      createdAt:
        Date,

      updatedAt:
        Date
    },
    {
      minimize: false
    }
  );

const signalLogSchema =
  new mongoose.Schema(
    {
      version:
        String,

      symbol:
        String,

      direction:
        String,

      candleTime:
        Date,

      executed:
        Boolean,

      skipReason:
        String,

      analysis:
        mongoose.Schema.Types.Mixed,

      createdAt:
        Date
    },
    {
      minimize: false
    }
  );

const botStateSchema =
  new mongoose.Schema(
    {
      key: {
        type: String,
        unique: true,
        index: true
      },

      telegramChatId:
        String,

      updatedAt:
        Date
    },
    {
      minimize: false
    }
  );

// ============================================================
// MODELS
// ============================================================

const PaperAccount =
  mongoose.models
    .LomyForexPaperAccount ||
  mongoose.model(
    'LomyForexPaperAccount',
    paperAccountSchema
  );

const ForexTrade =
  mongoose.models
    .LomyForexTrade ||
  mongoose.model(
    'LomyForexTrade',
    forexTradeSchema
  );

const SignalLog =
  mongoose.models
    .LomyForexSignal ||
  mongoose.model(
    'LomyForexSignal',
    signalLogSchema
  );

const BotState =
  mongoose.models
    .LomyForexBotState ||
  mongoose.model(
    'LomyForexBotState',
    botStateSchema
  );

let account = null;
let lastChatId = null;

// ============================================================
// MONGODB
// ============================================================

async function initMongo() {
  if (!MONGODB_URI) {
    console.warn(
      '⚠️ MONGODB_URI missing'
    );

    return;
  }

  try {
    await mongoose.connect(
      MONGODB_URI,
      {
        serverSelectionTimeoutMS:
          10000,

        maxPoolSize:
          5
      }
    );

    state.mongoReady = true;

    console.log(
      '✅ MongoDB connected'
    );

    account =
      await PaperAccount.findOne({
        key:
          PAPER.accountKey
      });

    if (!account) {
      account =
        await PaperAccount.create({
          key:
            PAPER.accountKey,

          version:
            VERSION,

          mode:
            MODE,

          startingBalance:
            PAPER.startingBalance,

          balance:
            PAPER.startingBalance,

          realizedPnl:
            0,

          totalTrades:
            0,

          wins:
            0,

          losses:
            0,

          breakeven:
            0,

          createdAt:
            new Date(),

          updatedAt:
            new Date()
        });
    }

    const savedBotState =
      await BotState.findOne({
        key:
          'main'
      }).lean();

    if (
      savedBotState
        ?.telegramChatId
    ) {
      lastChatId =
        savedBotState
          .telegramChatId;
    }

    const openTrades =
      await ForexTrade.find({
        version:
          VERSION,

        mode:
          MODE,

        status:
          'OPEN'
      }).lean();

    for (
      const trade
      of openTrades
    ) {
      state.openTrades.set(
        trade.symbol,
        trade
      );
    }

    console.log(
      `📂 Restored ${openTrades.length} open PAPER trade(s)`
    );
  } catch (err) {
    state.mongoReady =
      false;

    console.error(
      '❌ MongoDB:',
      safeError(err)
    );
  }
}

// ============================================================
// SAVE ACCOUNT
// ============================================================

async function saveAccount() {
  if (
    !state.mongoReady ||
    !account?.save
  ) {
    return;
  }

  account.updatedAt =
    new Date();

  await account.save();
}

// ============================================================
// REMEMBER TELEGRAM CHAT
// ============================================================

async function rememberChatId(
  chatId
) {
  lastChatId =
    String(chatId);

  if (!state.mongoReady) {
    return;
  }

  try {
    await BotState.updateOne(
      {
        key:
          'main'
      },
      {
        $set: {
          telegramChatId:
            String(chatId),

          updatedAt:
            new Date()
        }
      },
      {
        upsert: true
      }
    );
  } catch (err) {
    console.warn(
      '⚠️ Chat ID save:',
      safeError(err)
    );
  }
}

// ============================================================
// SIGNAL LOG
// ============================================================

async function logSignal(
  symbol,
  direction,
  analysis,
  executed,
  skipReason = ''
) {
  if (!state.mongoReady) {
    return;
  }

  try {
    await SignalLog.create({
      version:
        VERSION,

      symbol,

      direction,

      candleTime:
        new Date(
          analysis.time
        ),

      executed,

      skipReason,

      analysis,

      createdAt:
        new Date()
    });
  } catch (err) {
    console.warn(
      '⚠️ Signal log:',
      safeError(err)
    );
  }
}

// ============================================================
// TELEGRAM
// ============================================================

let bot = null;

async function sendTelegram(
  text,
  extra = {}
) {
  if (
    !bot ||
    !lastChatId
  ) {
    return;
  }

  try {
    await bot.telegram.sendMessage(
      lastChatId,
      text,
      extra
    );
  } catch (err) {
    console.warn(
      '⚠️ Telegram send:',
      safeError(err)
    );
  }
}

// ============================================================
// TELEGRAM COMMANDS
// ============================================================

function registerBotCommands() {
  bot.start(
    async (ctx) => {
      await rememberChatId(
        ctx.chat.id
      );

      await ctx.reply(
        `🤖 ${VERSION}\n\n` +
        `🧪 PAPER ONLY\n` +
        `🔒 Live Trading: OFF\n` +
        `📡 Pairs: ${PAIRS.length}\n` +
        `⏱ Entry: 1m CLOSED candle\n` +
        `🧠 Core: Ultra-Fast Scalp Engine V5.1\n\n` +
        `/status\n` +
        `/stats\n` +
        `/balance\n` +
        `/positions\n` +
        `/trades\n` +
        `/pairs`
      );
    }
  );

  bot.command(
    'status',
    async (ctx) => {
      await rememberChatId(
        ctx.chat.id
      );

      const initialized =
        [...state.pairState.values()]
          .filter(
            (pair) =>
              pair.initialized
          )
          .length;

      await ctx.reply(
        `🤖 ${VERSION}\n\n` +

        `Market: ${
          state.marketReady
            ? 'READY'
            : 'WAIT'
        }\n` +

        `MongoDB: ${
          state.mongoReady
            ? 'OK'
            : 'OFF'
        }\n` +

        `Telegram: ${
          state.telegramReady
            ? 'OK'
            : 'OFF'
        }\n` +

        `Pairs: ${initialized}/${PAIRS.length}\n` +

        `Open: ${state.openTrades.size}\n` +

        `Scans: ${state.totalSignalScans}\n` +

        `Signals: ${state.rawSignals}\n` +

        `Executed: ${state.executedSignals}\n` +

        `Skipped: ${state.skippedSignals}\n` +

        `Last Error: ${
          state.lastMarketError ||
          'none'
        }`
      );
    }
  );

  bot.command(
    'balance',
    async (ctx) => {
      await rememberChatId(
        ctx.chat.id
      );

      await ctx.reply(
        `🧪 PAPER ACCOUNT\n\n` +

        `Starting: ${fmtMoney(
          PAPER.startingBalance
        )}\n` +

        `Balance: ${fmtMoney(
          account?.balance ??
          PAPER.startingBalance
        )}\n` +

        `Realized PnL: ${fmtMoney(
          account?.realizedPnl ??
          0
        )}\n` +

        `Open Trades: ${state.openTrades.size}\n\n` +

        `🔒 NO REAL MONEY USED`
      );
    }
  );

  bot.command(
    'stats',
    async (ctx) => {
      await rememberChatId(
        ctx.chat.id
      );

      const total =
        Number(
          account?.totalTrades ||
          0
        );

      const wins =
        Number(
          account?.wins ||
          0
        );

      const winRate =
        total
          ? (
              wins /
              total
            ) * 100
          : 0;

      await ctx.reply(
        `📊 ${VERSION}\n\n` +

        `Closed Trades: ${total}\n` +

        `Wins: ${wins}\n` +

        `Losses: ${Number(
          account?.losses ||
          0
        )}\n` +

        `Breakeven: ${Number(
          account?.breakeven ||
          0
        )}\n` +

        `Win Rate: ${fmtPct(
          winRate
        )}\n` +

        `PnL: ${fmtMoney(
          account?.realizedPnl ||
          0
        )}\n` +

        `Balance: ${fmtMoney(
          account?.balance ??
          PAPER.startingBalance
        )}\n\n` +

        `Raw Signals: ${state.rawSignals}\n` +

        `Executed: ${state.executedSignals}`
      );
    }
  );

  bot.command(
    'positions',
    async (ctx) => {
      await rememberChatId(
        ctx.chat.id
      );

      if (
        !state.openTrades.size
      ) {
        return ctx.reply(
          '📭 No open PAPER positions.'
        );
      }

      const rows =
        [...state.openTrades.values()]
          .map(
            (trade) =>
              `${
                trade.direction ===
                'BUY'
                  ? '🟢'
                  : '🔴'
              } ${trade.symbol} ${trade.direction}\n` +

              `Entry ${fmtPrice(
                trade.entryPrice,
                trade.symbol
              )}\n` +

              `SL ${fmtPrice(
                trade.stopLoss,
                trade.symbol
              )}\n` +

              `TP ${fmtPrice(
                trade.takeProfit,
                trade.symbol
              )}`
          );

      await ctx.reply(
        rows.join('\n\n')
      );
    }
  );

  bot.command(
    'trades',
    async (ctx) => {
      await rememberChatId(
        ctx.chat.id
      );

      if (!state.mongoReady) {
        return ctx.reply(
          'MongoDB is not available.'
        );
      }

      const trades =
        await ForexTrade.find({
          version:
            VERSION,

          status:
            'CLOSED'
        })
          .sort({
            closedAt:
              -1
          })
          .limit(10)
          .lean();

      if (!trades.length) {
        return ctx.reply(
          '📭 No closed PAPER trades yet.'
        );
      }

      await ctx.reply(
        trades
          .map(
            (trade) =>
              `${
                trade.pnlUsd > 0
                  ? '✅'
                  : trade.pnlUsd < 0
                  ? '❌'
                  : '➖'
              } ${trade.symbol} ${trade.direction}\n` +

              `${trade.exitReason}\n` +

              `PnL ${fmtMoney(
                trade.pnlUsd
              )} | ${Number(
                trade.pnlR ||
                0
              ).toFixed(2)}R`
          )
          .join('\n\n')
      );
    }
  );

  bot.command(
    'pairs',
    async (ctx) => {
      await rememberChatId(
        ctx.chat.id
      );

      await ctx.reply(
        `📡 ${PAIRS.length} FOREX PAIRS\n\n` +
        PAIRS.join(', ')
      );
    }
  );
}

// ============================================================
// TELEGRAM INITIALIZATION
// ============================================================

async function initTelegram() {
  if (!TELEGRAM_BOT_TOKEN) {
    console.warn(
      '⚠️ TELEGRAM_BOT_TOKEN missing'
    );

    return;
  }

  try {
    bot =
      new Telegraf(
        TELEGRAM_BOT_TOKEN
      );

    registerBotCommands();

    const me =
      await bot.telegram.getMe();

    state.telegramReady =
      true;

    console.log(
      `✅ Telegram authenticated: @${me.username}`
    );

    // Important:
    // DO NOT await bot.launch().
    bot.launch({
      dropPendingUpdates:
        true
    })
      .then(
        () => {
          console.log(
            'ℹ️ Telegram polling stopped'
          );
        }
      )
      .catch(
        (err) => {
          state.telegramReady =
            false;

          console.error(
            '❌ Telegram polling:',
            safeError(err)
          );
        }
      );

    console.log(
      '✅ Telegram polling started'
    );
  } catch (err) {
    state.telegramReady =
      false;

    console.error(
      '❌ Telegram:',
      safeError(err)
    );
  }
}

// ============================================================
// HTTP CLIENT
// ============================================================

const http =
  axios.create({
    baseURL:
      BIQUOTE_BASE,

    timeout:
      HTTP_TIMEOUT_MS,

    headers: {
      'User-Agent':
        'LOMY-Forex-V1-Paper'
    }
  });

// ============================================================
// OHLC
// ============================================================

async function fetchOhlc(
  symbol,
  limit = 3
) {
  const response =
    await http.get(
      `/api/${symbol}/ohlc`,
      {
        params: {
          interval:
            '1m',

          limit
        }
      }
    );

  const bars =
    normalizeBars(
      response.data?.bars ||
      []
    );

  if (!bars.length) {
    throw new Error(
      `${symbol}: no OHLC bars`
    );
  }

  return bars;
}

// ============================================================
// NORMALIZE QUOTE
// ============================================================

function normalizeQuote(
  symbol,
  raw
) {
  if (
    !raw ||
    typeof raw !== 'object'
  ) {
    return null;
  }

  const bid =
    n(raw.bid);

  const ask =
    n(raw.ask);

  const mid =
    n(
      raw.mid,

      Number.isFinite(bid) &&
      Number.isFinite(ask)
        ? (
            bid +
            ask
          ) / 2
        : NaN
    );

  const fallback =
    Number.isFinite(mid)
      ? mid
      : n(raw.last);

  const finalBid =
    Number.isFinite(bid) &&
    bid > 0
      ? bid
      : fallback;

  const finalAsk =
    Number.isFinite(ask) &&
    ask > 0
      ? ask
      : fallback;

  if (
    !Number.isFinite(finalBid) ||
    !Number.isFinite(finalAsk) ||
    finalBid <= 0 ||
    finalAsk <= 0
  ) {
    return null;
  }

  return {
    symbol,

    bid:
      finalBid,

    ask:
      finalAsk,

    mid:
      Number.isFinite(mid) &&
      mid > 0
        ? mid
        : (
            finalBid +
            finalAsk
          ) / 2,

    spread:
      Math.max(
        0,
        finalAsk -
        finalBid
      ),

    marketState:
      raw.marketState ||
      null,

    stale:
      raw.stale === true,

    quoteAgeSeconds:
      n(
        raw.quoteAgeSeconds,
        NaN
      ),

    timestamp:
      raw.timestamp ||
      raw.time ||
      null
  };
}

// ============================================================
// BATCH QUOTES
// ============================================================

async function fetchLatestQuotes(
  symbols
) {
  if (!symbols?.length) {
    return new Map();
  }

  const params =
    new URLSearchParams();

  for (
    const symbol
    of symbols
  ) {
    params.append(
      'symbols',
      symbol
    );
  }

  const response =
    await http.get(
      `/api/latest?${params.toString()}`
    );

  const raw =
    response.data;

  const output =
    new Map();

  if (Array.isArray(raw)) {
    for (
      const item
      of raw
    ) {
      const symbol =
        String(
          item.symbol ||
          item.name ||
          ''
        )
          .replace('/', '')
          .toUpperCase();

      if (
        !symbols.includes(
          symbol
        )
      ) {
        continue;
      }

      const quote =
        normalizeQuote(
          symbol,
          item
        );

      if (quote) {
        output.set(
          symbol,
          quote
        );
      }
    }

    return output;
  }

  if (
    raw &&
    typeof raw === 'object'
  ) {
    for (
      const [key, value]
      of Object.entries(raw)
    ) {
      const symbol =
        String(
          value?.symbol ||
          value?.name ||
          key
        )
          .replace('/', '')
          .toUpperCase();

      if (
        !symbols.includes(
          symbol
        )
      ) {
        continue;
      }

      const quote =
        normalizeQuote(
          symbol,
          value
        );

      if (quote) {
        output.set(
          symbol,
          quote
        );
      }
    }
  }

  return output;
}

// ============================================================
// SINGLE FRESH QUOTE
// ============================================================

async function ensureQuote(
  symbol
) {
  const cached =
    state.latestQuotes.get(
      symbol
    );

  if (
    cached &&
    cached.stale !== true
  ) {
    return cached;
  }

  try {
    const response =
      await http.get(
        `/api/${symbol}`,
        {
          params: {
            allowStale:
              false
          }
        }
      );

    const quote =
      normalizeQuote(
        symbol,
        response.data
      );

    if (quote) {
      state.latestQuotes.set(
        symbol,
        quote
      );
    }

    return quote;
  } catch (err) {
    console.warn(
      `⚠️ ${symbol} quote: ${safeError(err)}`
    );

    return null;
  }
}

// ============================================================
// INITIALIZE MARKET
// ============================================================

async function initializeMarket() {
  console.log(
    `📡 Initializing ${PAIRS.length} Forex pairs...`
  );

  const results =
    await mapWithConcurrency(
      PAIRS,
      OHLC_CONCURRENCY,
      async (symbol) => {
        const bars =
          await fetchOhlc(
            symbol,
            HISTORY_LIMIT
          );

        const closed =
          bars.filter(
            (bar) =>
              !bar.isOpen
          );

        if (
          closed.length < 25
        ) {
          throw new Error(
            `${symbol}: insufficient closed history (${closed.length})`
          );
        }

        const pair =
          state.pairState.get(
            symbol
          );

        pair.bars =
          closed.slice(
            -HISTORY_LIMIT
          );

        pair.lastClosedBarTime =
          pair.bars[
            pair.bars.length - 1
          ]?.openTime ||
          null;

        pair.lastSignal =
          deriveLastSignalFromHistory(
            pair.bars
          );

        pair.lastAnalysis =
          analyzeV51(
            pair.bars
          );

        pair.initialized =
          true;

        return {
          symbol,

          bars:
            pair.bars.length,

          lastSignal:
            pair.lastSignal
        };
      }
    );

  let successful = 0;

  for (
    let i = 0;
    i < results.length;
    i++
  ) {
    const result =
      results[i];

    const symbol =
      PAIRS[i];

    if (
      result &&
      !result.error
    ) {
      successful++;

      console.log(
        `✅ ${symbol} ready | ${result.bars} closed bars | lastSignal=${result.lastSignal}`
      );
    } else {
      const pair =
        state.pairState.get(
          symbol
        );

      if (pair) {
        pair.errors++;
      }

      console.warn(
        `⚠️ ${symbol} init: ${safeError(
          result?.error ||
          'unknown error'
        )}`
      );
    }
  }

  state.marketReady =
    successful >=
    Math.ceil(
      PAIRS.length *
      0.70
    );

  state.initializing =
    false;

  state.lastMarketError =
    state.marketReady
      ? null
      : `Only ${successful}/${PAIRS.length} pairs initialized`;

  console.log(
    `📡 Market initialized: ${successful}/${PAIRS.length}`
  );

  console.log(
    state.marketReady
      ? '✅ Market engine READY'
      : '⚠️ Market engine PARTIAL/WAIT'
  );
}

// ============================================================
// CALCULATE SL / TP
// ============================================================

function calcLevels(
  direction,
  analysis,
  currentBar
) {
  const entry =
    analysis.close;

  if (
    !Number.isFinite(entry) ||
    !Number.isFinite(
      analysis.atr
    ) ||
    analysis.atr <= 0
  ) {
    return null;
  }

  if (
    direction === 'BUY'
  ) {
    let calculatedSL =
      analysis.support -
      analysis.atr *
      STRATEGY.atrMargin;

    if (
      !Number.isFinite(
        calculatedSL
      ) ||
      calculatedSL >=
        entry
    ) {
      calculatedSL =
        currentBar.low -
        analysis.atr *
        STRATEGY.atrMargin;
    }

    let risk =
      entry -
      calculatedSL;

    if (!(risk > 0)) {
      risk =
        analysis.atr;
    }

    return {
      signalEntry:
        entry,

      stopLoss:
        entry -
        risk,

      takeProfit:
        entry +
        risk *
        STRATEGY.riskReward,

      signalRisk:
        risk
    };
  }

  let calculatedSL =
    analysis.resistance +
    analysis.atr *
    STRATEGY.atrMargin;

  if (
    !Number.isFinite(
      calculatedSL
    ) ||
    calculatedSL <=
      entry
  ) {
    calculatedSL =
      currentBar.high +
      analysis.atr *
      STRATEGY.atrMargin;
  }

  let risk =
    calculatedSL -
    entry;

  if (!(risk > 0)) {
    risk =
      analysis.atr;
  }

  return {
    signalEntry:
      entry,

    stopLoss:
      entry +
      risk,

    takeProfit:
      entry -
      risk *
      STRATEGY.riskReward,

    signalRisk:
      risk
  };
}

// ============================================================
// OPEN PAPER TRADE
// ============================================================

async function openPaperTrade(
  symbol,
  direction,
  analysis,
  currentBar
) {
  if (
    state.openTrades.has(
      symbol
    )
  ) {
    state.skippedSignals++;

    await logSignal(
      symbol,
      direction,
      analysis,
      false,
      'PAIR_ALREADY_OPEN'
    );

    return;
  }

  if (
    state.openTrades.size >=
    PAPER.maxOpenTrades
  ) {
    state.skippedSignals++;

    await logSignal(
      symbol,
      direction,
      analysis,
      false,
      'MAX_OPEN_TRADES'
    );

    return;
  }

  const levels =
    calcLevels(
      direction,
      analysis,
      currentBar
    );

  if (!levels) {
    state.skippedSignals++;

    await logSignal(
      symbol,
      direction,
      analysis,
      false,
      'INVALID_LEVELS'
    );

    return;
  }

  const quote =
    await ensureQuote(
      symbol
    );

  if (!quote) {
    state.skippedSignals++;

    await logSignal(
      symbol,
      direction,
      analysis,
      false,
      'NO_FRESH_QUOTE'
    );

    return;
  }

  const entryPrice =
    direction === 'BUY'
      ? quote.ask
      : quote.bid;

  const riskDistance =
    direction === 'BUY'
      ? entryPrice -
        levels.stopLoss
      : levels.stopLoss -
        entryPrice;

  if (!(riskDistance > 0)) {
    state.skippedSignals++;

    await logSignal(
      symbol,
      direction,
      analysis,
      false,
      'EXECUTION_PRICE_INVALIDATES_SL'
    );

    return;
  }

  const equity =
    Number(
      account?.balance ??
      PAPER.startingBalance
    );

  const riskAmount =
    equity *
    (
      PAPER.riskPctPerTrade /
      100
    );

  const trade = {
    version:
      VERSION,

    mode:
      MODE,

    symbol,

    direction,

    status:
      'OPEN',

    signalTime:
      new Date(
        analysis.time
      ),

    openedAt:
      new Date(),

    closedAt:
      null,

    signalEntry:
      levels.signalEntry,

    entryPrice,

    stopLoss:
      levels.stopLoss,

    takeProfit:
      levels.takeProfit,

    exitPrice:
      null,

    exitReason:
      null,

    riskDistance,

    riskAmount,

    pnlR:
      0,

    pnlUsd:
      0,

    balanceAfter:
      null,

    spreadAtEntry:
      quote.spread,

    spreadAtExit:
      null,

    signal: {
      cmo:
        analysis.cmo,

      bodyRatio:
        analysis.bodyRatio,

      volume:
        analysis.volume,

      volAvg:
        analysis.volAvg,

      fastEMA:
        analysis.fastEMA,

      slowEMA:
        analysis.slowEMA,

      atr:
        analysis.atr,

      support:
        analysis.support,

      resistance:
        analysis.resistance,

      riskReward:
        STRATEGY.riskReward
    },

    createdAt:
      new Date(),

    updatedAt:
      new Date()
  };

  let savedTrade =
    trade;

  if (state.mongoReady) {
    savedTrade =
      (
        await ForexTrade.create(
          trade
        )
      ).toObject();
  }

  state.openTrades.set(
    symbol,
    savedTrade
  );

  state.executedSignals++;

  await logSignal(
    symbol,
    direction,
    analysis,
    true,
    ''
  );

  console.log(
    `${
      direction === 'BUY'
        ? '🟢'
        : '🔴'
    } ${symbol} ${direction} @ ${entryPrice}`
  );

  await sendTelegram(
    `${
      direction === 'BUY'
        ? '🟢'
        : '🔴'
    } <b>${direction} ${symbol}</b>\n\n` +

    `${VERSION} — PAPER\n` +

    `Signal: ${fmtPrice(
      levels.signalEntry,
      symbol
    )}\n` +

    `Entry: ${fmtPrice(
      entryPrice,
      symbol
    )}\n` +

    `SL: ${fmtPrice(
      levels.stopLoss,
      symbol
    )}\n` +

    `TP: ${fmtPrice(
      levels.takeProfit,
      symbol
    )}\n` +

    `R:R: 1:${STRATEGY.riskReward}\n` +

    `CMO: ${analysis.cmo.toFixed(1)}\n` +

    `Body: ${(analysis.bodyRatio * 100).toFixed(0)}%\n` +

    `Volume Spike: YES\n\n` +

    `🔒 PAPER ONLY`,
    {
      parse_mode:
        'HTML'
    }
  );
}

// ============================================================
// CLOSE PAPER TRADE
// ============================================================

async function closePaperTrade(
  symbol,
  exitPrice,
  exitReason,
  quote = null
) {
  const trade =
    state.openTrades.get(
      symbol
    );

  if (!trade) {
    return;
  }

  const sign =
    trade.direction ===
    'BUY'
      ? 1
      : -1;

  const move =
    sign *
    (
      exitPrice -
      trade.entryPrice
    );

  const pnlR =
    move /
    trade.riskDistance;

  const pnlUsd =
    trade.riskAmount *
    pnlR;

  if (!account) {
    account = {
      balance:
        PAPER.startingBalance,

      realizedPnl:
        0,

      totalTrades:
        0,

      wins:
        0,

      losses:
        0,

      breakeven:
        0
    };
  }

  account.balance =
    Number(
      account.balance ||
      0
    ) +
    pnlUsd;

  account.realizedPnl =
    Number(
      account.realizedPnl ||
      0
    ) +
    pnlUsd;

  account.totalTrades =
    Number(
      account.totalTrades ||
      0
    ) + 1;

  if (
    pnlUsd >
    0.000001
  ) {
    account.wins =
      Number(
        account.wins ||
        0
      ) + 1;
  } else if (
    pnlUsd <
    -0.000001
  ) {
    account.losses =
      Number(
        account.losses ||
        0
      ) + 1;
  } else {
    account.breakeven =
      Number(
        account.breakeven ||
        0
      ) + 1;
  }

  const patch = {
    status:
      'CLOSED',

    closedAt:
      new Date(),

    exitPrice,

    exitReason,

    pnlR,

    pnlUsd,

    balanceAfter:
      account.balance,

    spreadAtExit:
      quote?.spread ??
      0,

    updatedAt:
      new Date()
  };

  if (
    state.mongoReady &&
    trade._id
  ) {
    await ForexTrade.updateOne(
      {
        _id:
          trade._id
      },
      {
        $set:
          patch
      }
    );

    await saveAccount();
  }

  state.openTrades.delete(
    symbol
  );

  console.log(
    `${
      pnlUsd >= 0
        ? '✅'
        : '❌'
    } ${symbol} CLOSE ${exitReason} ${pnlR.toFixed(2)}R`
  );

  await sendTelegram(
    `${
      pnlUsd >= 0
        ? '✅'
        : '❌'
    } <b>CLOSE ${symbol} ${trade.direction}</b>\n\n` +

    `Reason: ${exitReason}\n` +

    `Exit: ${fmtPrice(
      exitPrice,
      symbol
    )}\n` +

    `PnL: ${fmtMoney(
      pnlUsd
    )} (${pnlR.toFixed(2)}R)\n` +

    `Paper Balance: ${fmtMoney(
      account.balance
    )}\n\n` +

    `🔒 PAPER ONLY`,
    {
      parse_mode:
        'HTML'
    }
  );
}

// ============================================================
// REVERSE SIGNAL
// ============================================================

async function reverseIfNeeded(
  symbol,
  newDirection
) {
  const trade =
    state.openTrades.get(
      symbol
    );

  if (
    !trade ||
    trade.direction ===
      newDirection
  ) {
    return;
  }

  const quote =
    await ensureQuote(
      symbol
    );

  if (!quote) {
    return;
  }

  const exitPrice =
    trade.direction ===
    'BUY'
      ? quote.bid
      : quote.ask;

  await closePaperTrade(
    symbol,
    exitPrice,
    'REVERSE_SIGNAL',
    quote
  );
}

// ============================================================
// SCAN PAIR
// ============================================================

async function scanPairForNewClosedBar(
  symbol
) {
  const pair =
    state.pairState.get(
      symbol
    );

  if (
    !pair?.initialized
  ) {
    return;
  }

  const latest =
    await fetchOhlc(
      symbol,
      3
    );

  const closed =
    latest.filter(
      (bar) =>
        !bar.isOpen
    );

  if (!closed.length) {
    return;
  }

  const newestClosed =
    closed[
      closed.length - 1
    ];

  if (
    !newestClosed?.openTime
  ) {
    return;
  }

  if (
    pair.lastClosedBarTime &&
    barTimeMs(
      newestClosed
    ) <=
    new Date(
      pair.lastClosedBarTime
    ).getTime()
  ) {
    return;
  }

  pair.bars =
    mergeClosedBars(
      pair.bars,
      closed,
      HISTORY_LIMIT
    );

  pair.lastClosedBarTime =
    newestClosed.openTime;

  const analysis =
    analyzeV51(
      pair.bars
    );

  pair.lastAnalysis =
    analysis;

  if (!analysis) {
    return;
  }

  let direction =
    null;

  if (
    analysis.rawBuy &&
    pair.lastSignal !== 1
  ) {
    direction =
      'BUY';

    pair.lastSignal =
      1;
  } else if (
    analysis.rawSell &&
    pair.lastSignal !== -1
  ) {
    direction =
      'SELL';

    pair.lastSignal =
      -1;
  }

  if (!direction) {
    return;
  }

  state.rawSignals++;

  console.log(
    `🚨 ${symbol} ${direction} SIGNAL | ` +
    `CMO ${analysis.cmo.toFixed(1)} | ` +
    `Body ${(analysis.bodyRatio * 100).toFixed(0)}% | ` +
    `Vol ${analysis.volume}/${analysis.volAvg.toFixed(0)}`
  );

  await reverseIfNeeded(
    symbol,
    direction
  );

  await openPaperTrade(
    symbol,
    direction,
    analysis,
    newestClosed
  );
}

// ============================================================
// RUN 30-PAIR SCAN
// ============================================================

async function runSignalScan() {
  if (
    state.scanRunning ||
    state.initializing
  ) {
    return;
  }

  state.scanRunning =
    true;

  state.lastSignalScanAt =
    new Date();

  state.totalSignalScans++;

  const initializedSymbols =
    PAIRS.filter(
      (symbol) =>
        state.pairState.get(
          symbol
        )?.initialized
    );

  console.log(
    `🔎 Scan #${state.totalSignalScans} | ` +
    `${initializedSymbols.length} pairs | ` +
    `${new Date().toISOString()}`
  );

  try {
    const results =
      await mapWithConcurrency(
        initializedSymbols,
        OHLC_CONCURRENCY,
        async (symbol) =>
          scanPairForNewClosedBar(
            symbol
          )
      );

    const errors =
      results.filter(
        (result) =>
          result?.error
      );

    if (errors.length) {
      state.lastMarketError =
        `${errors.length} pair scan error(s): ` +
        safeError(
          errors[0].error
        );

      console.warn(
        `⚠️ ${state.lastMarketError}`
      );
    } else {
      state.lastMarketError =
        null;
    }

    console.log(
      `✅ Scan #${state.totalSignalScans} complete | ` +
      `raw=${state.rawSignals} ` +
      `executed=${state.executedSignals}`
    );
  } finally {
    state.scanRunning =
      false;
  }
}

// ============================================================
// SCAN CLOCK
// ============================================================

function scanClockLoop() {
  if (state.initializing) {
    return;
  }

  const now =
    new Date();

  if (
    now.getUTCSeconds() <
    SCAN_AFTER_SECOND
  ) {
    return;
  }

  const key =
    minuteKey(now);

  if (
    state.lastGlobalScanMinute ===
    key
  ) {
    return;
  }

  state.lastGlobalScanMinute =
    key;

  runSignalScan()
    .catch(
      (err) => {
        state.lastMarketError =
          safeError(err);

        console.error(
          '❌ Global scan:',
          state.lastMarketError
        );
      }
    );
}

// ============================================================
// FAST EXIT MONITOR
// ============================================================

async function quotePollLoop() {
  if (
    state.quoteRunning ||
    state.initializing ||
    state.openTrades.size === 0
  ) {
    return;
  }

  state.quoteRunning =
    true;

  state.lastQuotePollAt =
    new Date();

  state.totalQuotePolls++;

  try {
    const symbols =
      [...state.openTrades.keys()];

    const quotes =
      await fetchLatestQuotes(
        symbols
      );

    for (
      const [symbol, quote]
      of quotes
    ) {
      state.latestQuotes.set(
        symbol,
        quote
      );
    }

    for (
      const [symbol, trade]
      of [
        ...state.openTrades.entries()
      ]
    ) {
      const quote =
        quotes.get(symbol) ||
        state.latestQuotes.get(
          symbol
        );

      if (
        !quote ||
        quote.stale === true
      ) {
        continue;
      }

      // BUY closes at BID
      if (
        trade.direction ===
        'BUY'
      ) {
        if (
          quote.bid <=
          trade.stopLoss
        ) {
          await closePaperTrade(
            symbol,
            quote.bid,
            'STOP_LOSS',
            quote
          );
        } else if (
          quote.bid >=
          trade.takeProfit
        ) {
          await closePaperTrade(
            symbol,
            quote.bid,
            'TAKE_PROFIT',
            quote
          );
        }
      }

      // SELL closes at ASK
      else {
        if (
          quote.ask >=
          trade.stopLoss
        ) {
          await closePaperTrade(
            symbol,
            quote.ask,
            'STOP_LOSS',
            quote
          );
        } else if (
          quote.ask <=
          trade.takeProfit
        ) {
          await closePaperTrade(
            symbol,
            quote.ask,
            'TAKE_PROFIT',
            quote
          );
        }
      }
    }
  } catch (err) {
    state.lastMarketError =
      `quote: ${safeError(err)}`;

    console.warn(
      '⚠️',
      state.lastMarketError
    );
  } finally {
    state.quoteRunning =
      false;
  }
}

// ============================================================
// EXPRESS
// ============================================================

const app =
  express();

app.disable(
  'x-powered-by'
);

// ============================================================
// HOME
// ============================================================

app.get(
  '/',
  (_req, res) => {
    const initialized =
      [...state.pairState.values()]
        .filter(
          (pair) =>
            pair.initialized
        )
        .length;

    res
      .type('html')
      .send(`
<!doctype html>
<html>
<head>
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>${VERSION}</title>

<style>
body {
  font-family: Arial, sans-serif;
  background: #111;
  color: #eee;
  padding: 24px;
}

.card {
  max-width: 760px;
  margin: auto;
  background: #1b1b1b;
  padding: 22px;
  border-radius: 14px;
}

.ok {
  color: #61d17c;
}

.warn {
  color: #ffcc66;
}
</style>
</head>

<body>

<div class="card">

<h2>${VERSION}</h2>

<p>
<b>Mode:</b>
PAPER ONLY
</p>

<p>
<b>Live Trading:</b>
OFF
</p>

<p>
<b>Market:</b>
<span class="${state.marketReady ? 'ok' : 'warn'}">
${state.marketReady ? 'READY' : 'WAIT'}
</span>
</p>

<p>
<b>Pairs:</b>
${initialized}/${PAIRS.length}
</p>

<p>
<b>Open Trades:</b>
${state.openTrades.size}
</p>

<p>
<b>Paper Balance:</b>
${fmtMoney(
  account?.balance ??
  PAPER.startingBalance
)}
</p>

<p>
<b>Closed Trades:</b>
${Number(
  account?.totalTrades ||
  0
)}
</p>

<p>
<b>Signals:</b>
${state.rawSignals}
raw /
${state.executedSignals}
executed
</p>

<p>
<b>Core:</b>
Ultra-Fast Scalp Engine V5.1
</p>

<p>
<b>Entry:</b>
1-minute CLOSED candle
</p>

<p>
<b>R:R:</b>
1:${STRATEGY.riskReward}
</p>

<p>
🔒 NO REAL MONEY USED
</p>

</div>

</body>
</html>
      `);
  }
);

// ============================================================
// HEALTH
// ============================================================

app.get(
  '/health',
  (_req, res) => {
    const initialized =
      [...state.pairState.values()]
        .filter(
          (pair) =>
            pair.initialized
        )
        .length;

    res.json({
      ok:
        true,

      version:
        VERSION,

      mode:
        MODE,

      liveTrading:
        LIVE_TRADING,

      marketReady:
        state.marketReady,

      mongoReady:
        state.mongoReady,

      telegramReady:
        state.telegramReady,

      initializedPairs:
        initialized,

      totalPairs:
        PAIRS.length,

      openTrades:
        state.openTrades.size,

      totalSignalScans:
        state.totalSignalScans,

      rawSignals:
        state.rawSignals,

      executedSignals:
        state.executedSignals,

      lastSignalScanAt:
        state.lastSignalScanAt,

      lastQuotePollAt:
        state.lastQuotePollAt,

      lastMarketError:
        state.lastMarketError,

      startedAt:
        state.startedAt
    });
  }
);

// ============================================================
// API STATUS
// ============================================================

app.get(
  '/api/status',
  (_req, res) => {
    res.json({
      version:
        VERSION,

      mode:
        MODE,

      liveTrading:
        LIVE_TRADING,

      provider:
        'BiQuote',

      twelveDataConfigured:
        Boolean(
          TWELVE_DATA_API_KEY
        ),

      strategy:
        STRATEGY,

      paper: {
        startingBalance:
          PAPER.startingBalance,

        balance:
          account?.balance ??
          PAPER.startingBalance,

        realizedPnl:
          account?.realizedPnl ??
          0,

        totalTrades:
          account?.totalTrades ??
          0,

        wins:
          account?.wins ??
          0,

        losses:
          account?.losses ??
          0,

        openTrades:
          state.openTrades.size
      },

      engine: {
        pairs:
          PAIRS,

        scans:
          state.totalSignalScans,

        rawSignals:
          state.rawSignals,

        executedSignals:
          state.executedSignals,

        skippedSignals:
          state.skippedSignals
      }
    });
  }
);

// ============================================================
// BOOT
// ============================================================

async function boot() {
  console.log(
    '================================================'
  );

  console.log(
    `🚀 ${VERSION}`
  );

  console.log(
    '🧪 PAPER ONLY — LIVE TRADING OFF'
  );

  console.log(
    `📡 Forex Pairs: ${PAIRS.length}`
  );

  console.log(
    '🧠 Core: Ultra-Fast Scalp Engine V5.1'
  );

  console.log(
    '⏱ Entry: 1-minute CLOSED candle'
  );

  console.log(
    '📊 Primary data: BiQuote'
  );

  console.log(
    `🔑 Twelve Data key: ${
      TWELVE_DATA_API_KEY
        ? 'configured'
        : 'missing'
    }`
  );

  console.log(
    '================================================'
  );

  // Start Render server immediately.
  app.listen(
    PORT,
    () => {
      console.log(
        `🌐 Health server on port ${PORT}`
      );
    }
  );

  // MongoDB
  await initMongo();

  // Telegram
  // Does NOT block market initialization.
  await initTelegram();

  // Market
  try {
    await initializeMarket();
  } catch (err) {
    state.initializing =
      false;

    state.marketReady =
      false;

    state.lastMarketError =
      safeError(err);

    console.error(
      '❌ Market initialization:',
      state.lastMarketError
    );
  }

  // Scan timer
  setInterval(
    scanClockLoop,
    CLOCK_CHECK_MS
  );

  // Fast exit monitor
  setInterval(
    quotePollLoop,
    QUOTE_POLL_MS
  );

  // Heartbeat
  setInterval(
    () => {
      const initialized =
        [...state.pairState.values()]
          .filter(
            (pair) =>
              pair.initialized
          )
          .length;

      console.log(
        `💓 HEARTBEAT | ` +
        `market=${
          state.marketReady
            ? 'READY'
            : 'WAIT'
        } | ` +
        `pairs=${initialized}/${PAIRS.length} | ` +
        `scans=${state.totalSignalScans} | ` +
        `open=${state.openTrades.size} | ` +
        `signals=${state.rawSignals}/${state.executedSignals}`
      );
    },
    HEARTBEAT_MS
  );

  // First scan after initialization
  setTimeout(
    () => {
      runSignalScan()
        .catch(
          (err) => {
            console.error(
              '❌ Startup scan:',
              safeError(err)
            );
          }
        );
    },
    2500
  );

  console.log(
    '✅ LOMY Forex loops started'
  );
}

// ============================================================
// SAFE SHUTDOWN
// ============================================================

async function shutdown(
  signal
) {
  console.log(
    `🛑 ${signal} received`
  );

  try {
    if (bot) {
      bot.stop(signal);
    }
  } catch {}

  try {
    await mongoose.connection.close();
  } catch {}

  process.exit(0);
}

process.once(
  'SIGINT',
  () =>
    shutdown('SIGINT')
);

process.once(
  'SIGTERM',
  () =>
    shutdown('SIGTERM')
);

// ============================================================
// GLOBAL ERROR HANDLING
// ============================================================

process.on(
  'unhandledRejection',
  (reason) => {
    console.error(
      '❌ Unhandled rejection:',
      safeError(reason)
    );
  }
);

process.on(
  'uncaughtException',
  (err) => {
    console.error(
      '❌ Uncaught exception:',
      safeError(err)
    );
  }
);

// ============================================================
// START
// ============================================================

boot()
  .catch(
    (err) => {
      console.error(
        '❌ FATAL:',
        safeError(err)
      );

      process.exit(1);
    }
  );
