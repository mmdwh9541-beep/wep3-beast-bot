'use strict';

/**
 * ============================================================
 * LOMY FOREX V1.0 — PAPER TEST
 * ============================================================
 * Core Strategy:
 * Ultra-Fast Scalp Engine V5.1
 *
 * PAPER ONLY
 * LIVE TRADING = OFF
 *
 * 30 Forex Pairs
 * Entry Timeframe = 1 Minute
 * Signal only from CLOSED candle
 *
 * BUY / SELL signal executes immediately after confirmation.
 * ============================================================
 */

const express = require('express');
const axios = require('axios');
const mongoose = require('mongoose');
const { Telegraf } = require('telegraf');

// ============================================================
// VERSION
// ============================================================

const VERSION = 'LOMY FOREX V1.0';
const MODE = 'PAPER';
const LIVE_TRADING = false;

// ============================================================
// ENVIRONMENT VARIABLES
// ============================================================

const PORT = Number(process.env.PORT || 10000);

const MONGODB_URI =
  process.env.MONGODB_URI || '';

const TELEGRAM_BOT_TOKEN =
  process.env.TELEGRAM_BOT_TOKEN || '';

const TWELVE_DATA_API_KEY =
  process.env.TWELVE_DATA_API_KEY || '';

// ============================================================
// MARKET DATA
// ============================================================

const BIQUOTE_BASE = 'https://biquote.io';

const HTTP_TIMEOUT_MS = 8000;

// Check for new closed 1-minute candles.
const SIGNAL_SCAN_MS = 10000;

// Faster price check for PAPER SL / TP.
const QUOTE_POLL_MS = 3000;

// Enough history for V5.1 indicators.
const HISTORY_LIMIT = 80;

// Keep Render CPU/RAM usage controlled.
const OHLC_CONCURRENCY = 6;

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
// SAME CORE SETTINGS AS TRADINGVIEW INDICATOR
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

  // Risk 0.5% of virtual equity at SL.
  riskPctPerTrade: 0.50,

  // One position maximum per pair.
  // Allows all 30 pairs to participate in the test.
  maxOpenTrades: 30,

  accountKey: 'lomy-forex-v1-main'

});

// ============================================================
// RUNTIME STATE
// ============================================================

const state = {

  startedAt: new Date(),

  mongoReady: false,
  telegramReady: false,
  marketReady: false,

  initializing: true,

  scanRunning: false,
  quoteRunning: false,

  lastSignalScanAt: null,
  lastQuotePollAt: null,

  lastMarketError: null,

  totalSignalScans: 0,
  totalQuotePolls: 0,

  rawSignals: 0,
  executedSignals: 0,
  skippedSignals: 0,

  pairState: new Map(),

  latestQuotes: new Map(),

  openTrades: new Map()

};

// ============================================================
// INITIAL PAIR STATE
// ============================================================

for (const symbol of PAIRS) {

  state.pairState.set(symbol, {

    bars: [],

    lastClosedBarTime: null,

    // Same concept as Pine V5.1.
    // 1 = last BUY
    // -1 = last SELL
    // 0 = none
    lastSignal: 0,

    lastAnalysis: null,

    initialized: false,

    errors: 0

  });

}

// ============================================================
// HELPERS
// ============================================================

function nowIso() {
  return new Date().toISOString();
}

function n(value, fallback = NaN) {

  const number = Number(value);

  return Number.isFinite(number)
    ? number
    : fallback;

}

function fmtPrice(value, symbol = '') {

  if (!Number.isFinite(value)) {
    return 'n/a';
  }

  const isJPY =
    symbol.endsWith('JPY');

  return value.toFixed(
    isJPY ? 3 : 5
  );

}

function fmtMoney(value) {

  return Number.isFinite(value)
    ? `$${value.toFixed(2)}`
    : '$0.00';

}

function fmtPct(value) {

  return Number.isFinite(value)
    ? `${value.toFixed(2)}%`
    : '0.00%';

}

function safeError(err) {

  return (
    err?.response?.data?.message ||
    err?.response?.data?.status ||
    err?.message ||
    String(err)
  );

}

function barTimeMs(bar) {

  const time =
    new Date(bar.openTime).getTime();

  return Number.isFinite(time)
    ? time
    : 0;

}

// ============================================================
// NORMALIZE MARKET BARS
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
        bar.time,

      open: n(bar.open),

      high: n(bar.high),

      low: n(bar.low),

      close: n(bar.close),

      // Forex:
      // Prefer Tick Volume.
      volume: n(
        bar.tickVolume,
        n(bar.volume, 0)
      ),

      isOpen:
        Boolean(bar.isOpen)

    }))
    .filter((bar) =>

      bar.openTime &&

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
// MERGE CLOSED CANDLES
// ============================================================

function mergeClosedBars(
  existing,
  incoming,
  maxBars = HISTORY_LIMIT
) {

  const map =
    new Map();

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
// INDICATORS
// ============================================================

// ============================================================
// SMA
// ============================================================

function sma(values, length) {

  if (values.length < length) {
    return NaN;
  }

  const valuesToUse =
    values.slice(-length);

  const total =
    valuesToUse.reduce(
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
    bars.length <
    length + 1
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

    } else {

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
    closes.length <
    length + 1
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

    } else if (
      difference < 0
    ) {

      losses +=
        -difference;

    }

  }

  const denominator =
    gains + losses;

  if (
    denominator === 0
  ) {

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
      (bar) => bar.close
    );

  const volumes =
    bars.map(
      (bar) =>
        Number.isFinite(bar.volume)
          ? bar.volume
          : 0
    );

  // ==========================================================
  // CANDLE STRENGTH
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

  const bullishCandle =
    current.close >
    current.open;

  const bearishCandle =
    current.close <
    current.open;

  const strongBull =
    bullishCandle &&
    bodyRatio >=
      STRATEGY.bodyRatioMin;

  const strongBear =
    bearishCandle &&
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
  // EMA TREND
  // ==========================================================

  const fast =
    emaSeries(
      closes,
      STRATEGY.fastEmaLen
    );

  const slow =
    emaSeries(
      closes,
      STRATEGY.slowEmaLen
    );

  const fastEMA =
    fast[
      fast.length - 1
    ];

  const slowEMA =
    slow[
      slow.length - 1
    ];

  const bullTrend =

    Number.isFinite(
      fastEMA
    ) &&

    Number.isFinite(
      slowEMA
    ) &&

    fastEMA >
    slowEMA;

  const bearTrend =

    Number.isFinite(
      fastEMA
    ) &&

    Number.isFinite(
      slowEMA
    ) &&

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
  // Pine:
  // support = ta.lowest(low[1], srLen)
  // resistance = ta.highest(high[1], srLen)
  // ==========================================================

  const srWindow =
    previousBars.slice(
      -STRATEGY.srLen
    );

  const support =
    srWindow.length
      ? Math.min(
          ...srWindow.map(
            (bar) => bar.low
          )
        )
      : NaN;

  const resistance =
    srWindow.length
      ? Math.max(
          ...srWindow.map(
            (bar) => bar.high
          )
        )
      : NaN;

  // ==========================================================
  // EXACT V5.1 BUY / SELL LOGIC
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
// FIND LAST HISTORICAL SIGNAL
// Prevent fake signal on startup.
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
// MONGODB SCHEMAS
// ============================================================

const paperAccountSchema =
  new mongoose.Schema({

    key: {
      type: String,
      unique: true,
      index: true
    },

    version: String,

    mode: String,

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

  }, {
    minimize: false
  });

// ============================================================

const forexTradeSchema =
  new mongoose.Schema({

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

  }, {
    minimize: false
  });

// ============================================================

const signalLogSchema =
  new mongoose.Schema({

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

  }, {
    minimize: false
  });

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

// ============================================================

let account = null;

// ============================================================
// MONGODB INITIALIZATION
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

    state.mongoReady =
      true;

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
// SAVE PAPER ACCOUNT
// ============================================================

async function saveAccount() {

  if (
    !state.mongoReady ||
    !account
  ) {
    return;
  }

  account.updatedAt =
    new Date();

  await account.save();

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
      'Signal log:',
      safeError(err)
    );

  }

}

// ============================================================
// TELEGRAM
// ============================================================

let bot = null;

let lastChatId = null;

// ============================================================

function registerBotCommands() {

  if (!bot) {
    return;
  }

  // ==========================================================
  // START
  // ==========================================================

  bot.start(
    async (ctx) => {

      lastChatId =
        ctx.chat.id;

      await ctx.reply(

        `🤖 ${VERSION}\n\n` +

        `🧪 Mode: PAPER ONLY\n` +

        `🔒 Live Trading: OFF\n` +

        `📡 Pairs: ${PAIRS.length}\n` +

        `⏱ Entry: 1 Minute\n` +

        `🧠 Core: Ultra-Fast Scalp Engine V5.1\n\n` +

        `Commands:\n` +

        `/status\n` +

        `/stats\n` +

        `/balance\n` +

        `/positions\n` +

        `/trades\n` +

        `/pairs`

      );

    }
  );

  // ==========================================================
  // STATUS
  // ==========================================================

  bot.command(
    'status',
    async (ctx) => {

      lastChatId =
        ctx.chat.id;

      const initialized =
        [...state.pairState.values()]
          .filter(
            (pair) =>
              pair.initialized
          )
          .length;

      await ctx.reply(

        `🤖 ${VERSION}\n\n` +

        `Mode: ${MODE}\n` +

        `Live Trading: OFF\n\n` +

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
        }\n\n` +

        `Pairs: ${initialized}/${PAIRS.length}\n` +

        `Open Trades: ${state.openTrades.size}\n` +

        `Signal Scans: ${state.totalSignalScans}\n` +

        `Raw Signals: ${state.rawSignals}\n` +

        `Executed: ${state.executedSignals}\n` +

        `Skipped: ${state.skippedSignals}\n\n` +

        `Last Error: ${
          state.lastMarketError ||
          'none'
        }`

      );

    }
  );

  // ==========================================================
  // BALANCE
  // ==========================================================

  bot.command(
    'balance',
    async (ctx) => {

      lastChatId =
        ctx.chat.id;

      const balance =
        account?.balance ??
        PAPER.startingBalance;

      const pnl =
        account?.realizedPnl ??
        0;

      await ctx.reply(

        `🧪 PAPER ACCOUNT\n\n` +

        `Starting: ${fmtMoney(PAPER.startingBalance)}\n` +

        `Balance: ${fmtMoney(balance)}\n` +

        `Realized PnL: ${fmtMoney(pnl)}\n` +

        `Open Trades: ${state.openTrades.size}\n\n` +

        `🔒 NO REAL MONEY USED`

      );

    }
  );

  // ==========================================================
  // STATS
  // ==========================================================

  bot.command(
    'stats',
    async (ctx) => {

      lastChatId =
        ctx.chat.id;

      const stats =
        account || {

          totalTrades: 0,

          wins: 0,

          losses: 0,

          breakeven: 0,

          balance:
            PAPER.startingBalance,

          realizedPnl: 0

        };

      const winRate =
        stats.totalTrades
          ? (
              stats.wins /
              stats.totalTrades
            ) * 100
          : 0;

      await ctx.reply(

        `📊 ${VERSION}\n\n` +

        `Closed Trades: ${stats.totalTrades}\n` +

        `Wins: ${stats.wins}\n` +

        `Losses: ${stats.losses}\n` +

        `Breakeven: ${stats.breakeven || 0}\n` +

        `Win Rate: ${fmtPct(winRate)}\n\n` +

        `PnL: ${fmtMoney(stats.realizedPnl || 0)}\n` +

        `Balance: ${fmtMoney(stats.balance || PAPER.startingBalance)}\n\n` +

        `Raw Signals: ${state.rawSignals}\n` +

        `Executed Signals: ${state.executedSignals}`

      );

    }
  );

  // ==========================================================
  // POSITIONS
  // ==========================================================

  bot.command(
    'positions',
    async (ctx) => {

      lastChatId =
        ctx.chat.id;

      if (
        !state.openTrades.size
      ) {

        return ctx.reply(
          '📭 No open PAPER positions.'
        );

      }

      const rows = [];

      for (
        const trade
        of state.openTrades.values()
      ) {

        rows.push(

          `${
            trade.direction === 'BUY'
              ? '🟢'
              : '🔴'
          } ${trade.symbol} ${trade.direction}\n` +

          `Entry ${fmtPrice(trade.entryPrice, trade.symbol)}\n` +

          `SL ${fmtPrice(trade.stopLoss, trade.symbol)}\n` +

          `TP ${fmtPrice(trade.takeProfit, trade.symbol)}`

        );

      }

      await ctx.reply(
        rows.join('\n\n')
      );

    }
  );

  // ==========================================================
  // TRADES
  // ==========================================================

  bot.command(
    'trades',
    async (ctx) => {

      lastChatId =
        ctx.chat.id;

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
            closedAt: -1
          })
          .limit(10)
          .lean();

      if (!trades.length) {

        return ctx.reply(
          '📭 No closed PAPER trades yet.'
        );

      }

      const rows =
        trades.map(
          (trade) =>

            `${
              trade.pnlUsd > 0
                ? '✅'
                : trade.pnlUsd < 0
                ? '❌'
                : '➖'
            } ${trade.symbol} ${trade.direction}\n` +

            `${trade.exitReason}\n` +

            `PnL ${fmtMoney(trade.pnlUsd)} | ${Number(trade.pnlR || 0).toFixed(2)}R`

        );

      await ctx.reply(
        rows.join('\n\n')
      );

    }
  );

  // ==========================================================
  // PAIRS
  // ==========================================================

  bot.command(
    'pairs',
    async (ctx) => {

      lastChatId =
        ctx.chat.id;

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

    await bot.launch({
      dropPendingUpdates: true
    });

    state.telegramReady =
      true;

    console.log(
      '✅ Telegram bot started'
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
// FETCH OHLC
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
            '1min',

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
// FETCH LIVE QUOTES FOR ALL 30 PAIRS
// ============================================================

async function fetchLatestQuotes() {

  const params =
    new URLSearchParams();

  for (
    const symbol
    of PAIRS
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

  let list = [];

  if (
    Array.isArray(raw)
  ) {

    list = raw;

  } else if (
    Array.isArray(raw?.items)
  ) {

    list =
      raw.items;

  } else if (
    Array.isArray(raw?.data)
  ) {

    list =
      raw.data;

  } else if (
    raw &&
    typeof raw === 'object'
  ) {

    list =
      Object.entries(raw)
        .map(
          ([symbol, value]) => ({

            symbol,

            ...(value || {})

          })
        );

  }

  const output =
    new Map();

  for (
    const quote
    of list
  ) {

    const symbol =
      String(
        quote.symbol ||
        quote.name ||
        ''
      )
        .replace('/', '')
        .toUpperCase();

    const bid =
      n(quote.bid);

    const ask =
      n(quote.ask);

    const last =
      n(
        quote.last,

        Number.isFinite(bid) &&
        Number.isFinite(ask)

          ? (bid + ask) / 2

          : NaN
      );

    if (
      !PAIRS.includes(symbol)
    ) {
      continue;
    }

    if (
      !Number.isFinite(last) &&
      !(
        Number.isFinite(bid) &&
        Number.isFinite(ask)
      )
    ) {
      continue;
    }

    output.set(
      symbol,
      {

        symbol,

        bid:
          Number.isFinite(bid)
            ? bid
            : last,

        ask:
          Number.isFinite(ask)
            ? ask
            : last,

        last,

        time:
          quote.time ||
          quote.timestamp ||
          nowIso()

      }
    );

  }

  return output;

}

// ============================================================
// CONCURRENCY CONTROL
// Keeps Render lightweight.
// ============================================================

async function mapWithConcurrency(
  items,
  concurrency,
  callback
) {

  const results =
    new Array(
      items.length
    );

  let index = 0;

  async function worker() {

    while (true) {

      const currentIndex =
        index++;

      if (
        currentIndex >=
        items.length
      ) {
        break;
      }

      try {

        results[currentIndex] =
          await callback(
            items[currentIndex],
            currentIndex
          );

      } catch (err) {

        results[currentIndex] = {
          error: err
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

      worker
    )

  );

  return results;

}

// ============================================================
// INITIALIZE 30 PAIRS
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
            `${symbol}: insufficient history (${closed.length})`
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

    if (
      result &&
      !result.error
    ) {

      successful++;

      console.log(

        `✅ ${result.symbol} history ${result.bars} bars | lastSignal=${result.lastSignal}`

      );

    } else {

      const symbol =
        PAIRS[i];

      const error =
        result?.error ||
        new Error(
          'unknown initialization error'
        );

      const pair =
        state.pairState.get(
          symbol
        );

      pair.errors++;

      console.warn(

        `⚠️ ${symbol} init: ${safeError(error)}`

      );

    }

  }

  state.marketReady =
    successful >=
    Math.ceil(
      PAIRS.length * 0.7
    );

  state.initializing =
    false;

  console.log(
    `📡 Market initialized: ${successful}/${PAIRS.length}`
  );

}

// ============================================================
// CALCULATE SL / TP
// EXACT V5.1 CONCEPT
// ============================================================

function calcLevels(
  direction,
  analysis,
  currentBar
) {

  const entry =
    analysis.close;

  let stopLoss;

  // ==========================================================
  // BUY
  // ==========================================================

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

      calculatedSL >= entry
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

    stopLoss =
      entry - risk;

    return {

      signalEntry:
        entry,

      stopLoss,

      takeProfit:

        entry +

        risk *
        STRATEGY.riskReward,

      signalRisk:
        risk

    };

  }

  // ==========================================================
  // SELL
  // ==========================================================

  let calculatedSL =

    analysis.resistance +

    analysis.atr *
    STRATEGY.atrMargin;

  if (
    !Number.isFinite(
      calculatedSL
    ) ||

    calculatedSL <= entry
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

  stopLoss =
    entry + risk;

  return {

    signalEntry:
      entry,

    stopLoss,

    takeProfit:

      entry -

      risk *
      STRATEGY.riskReward,

    signalRisk:
      risk

  };

}

// ============================================================
// GET CURRENT QUOTE
// ============================================================

async function ensureQuote(
  symbol
) {

  const cached =
    state.latestQuotes.get(
      symbol
    );

  if (cached) {
    return cached;
  }

  try {

    const response =
      await http.get(
        `/api/${symbol}`
      );

    const quote =
      response.data ||
      {};

    const bid =
      n(
        quote.bid,
        n(quote.last)
      );

    const ask =
      n(
        quote.ask,
        n(quote.last)
      );

    if (
      !Number.isFinite(bid) ||
      !Number.isFinite(ask)
    ) {

      return null;

    }

    const output = {

      symbol,

      bid,

      ask,

      last:
        n(
          quote.last,
          (bid + ask) / 2
        ),

      time:
        quote.time ||
        nowIso()

    };

    state.latestQuotes.set(
      symbol,
      output
    );

    return output;

  } catch {

    return null;

  }

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

  // One active trade per pair.
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

  if (
    ![
      levels.signalEntry,
      levels.stopLoss,
      levels.takeProfit
    ].every(
      Number.isFinite
    )
  ) {

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

  // ==========================================================
  // REALISTIC PAPER ENTRY USING BID / ASK
  // ==========================================================

  const quote =
    await ensureQuote(
      symbol
    );

  const entryPrice =
    quote

      ? (
          direction === 'BUY'
            ? quote.ask
            : quote.bid
        )

      : levels.signalEntry;

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
    account?.balance ??
    PAPER.startingBalance;

  const riskAmount =

    equity *

    (
      PAPER.riskPctPerTrade /
      100
    );

  const spreadAtEntry =
    quote

      ? Math.max(
          0,
          quote.ask -
          quote.bid
        )

      : 0;

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

    spreadAtEntry,

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

  if (
    state.mongoReady
  ) {

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

  // ==========================================================
  // TELEGRAM ENTRY MESSAGE
  // ==========================================================

  const message =

    `${
      direction === 'BUY'
        ? '🟢'
        : '🔴'
    } <b>${direction} ${symbol}</b>\n\n` +

    `LOMY Forex V1 — PAPER\n\n` +

    `Signal: ${fmtPrice(levels.signalEntry, symbol)}\n` +

    `Entry: ${fmtPrice(entryPrice, symbol)}\n` +

    `SL: ${fmtPrice(levels.stopLoss, symbol)}\n` +

    `TP: ${fmtPrice(levels.takeProfit, symbol)}\n\n` +

    `R:R: 1:${STRATEGY.riskReward}\n` +

    `CMO: ${analysis.cmo.toFixed(1)}\n` +

    `Body: ${(analysis.bodyRatio * 100).toFixed(0)}%\n` +

    `Volume Spike: ${
      analysis.highVolume
        ? 'YES'
        : 'NO'
    }\n\n` +

    `🔒 PAPER ONLY`;

  if (
    lastChatId &&
    bot
  ) {

    try {

      await bot.telegram.sendMessage(
        lastChatId,
        message,
        {
          parse_mode:
            'HTML'
        }
      );

    } catch {}

  }

  console.log(

    `${
      direction === 'BUY'
        ? '🟢'
        : '🔴'
    } ${symbol} ${direction} @ ${entryPrice}`

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

  const directionSign =
    trade.direction === 'BUY'
      ? 1
      : -1;

  const move =

    directionSign *

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

  account.balance +=
    pnlUsd;

  account.realizedPnl +=
    pnlUsd;

  account.totalTrades +=
    1;

  if (
    pnlUsd >
    0.000001
  ) {

    account.wins +=
      1;

  } else if (
    pnlUsd <
    -0.000001
  ) {

    account.losses +=
      1;

  } else {

    account.breakeven =
      (
        account.breakeven ||
        0
      ) + 1;

  }

  const spreadAtExit =
    quote

      ? Math.max(
          0,
          quote.ask -
          quote.bid
        )

      : 0;

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

    spreadAtExit,

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

  // ==========================================================
  // TELEGRAM EXIT MESSAGE
  // ==========================================================

  const message =

    `${
      pnlUsd >= 0
        ? '✅'
        : '❌'
    } <b>CLOSE ${symbol} ${trade.direction}</b>\n\n` +

    `Reason: ${exitReason}\n` +

    `Exit: ${fmtPrice(exitPrice, symbol)}\n` +

    `PnL: ${fmtMoney(pnlUsd)} (${pnlR.toFixed(2)}R)\n` +

    `Paper Balance: ${fmtMoney(account.balance)}\n\n` +

    `🔒 PAPER ONLY`;

  if (
    lastChatId &&
    bot
  ) {

    try {

      await bot.telegram.sendMessage(
        lastChatId,
        message,
        {
          parse_mode:
            'HTML'
        }
      );

    } catch {}

  }

  console.log(

    `${
      pnlUsd >= 0
        ? '✅'
        : '❌'
    } ${symbol} CLOSE ${exitReason} ${pnlR.toFixed(2)}R`

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

    trade.direction === 'BUY'

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
// SCAN ONE PAIR
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

  // ==========================================================
  // ONLY NEW CLOSED CANDLE
  // ==========================================================

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

  // ==========================================================
  // EXACT SIGNAL FILTER FROM INDICATOR
  // ==========================================================

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

    `🚨 ${symbol} ${direction} SIGNAL | CMO ${analysis.cmo.toFixed(1)} | Body ${(analysis.bodyRatio * 100).toFixed(0)}%`

  );

  // ==========================================================
  // USER REQUIREMENT:
  // V5.1 CONFIRMED SIGNAL = IMMEDIATE PAPER EXECUTION
  // ==========================================================

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
// 30-PAIR SIGNAL LOOP
// ============================================================

async function signalScanLoop() {

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

  try {

    const results =
      await mapWithConcurrency(

        PAIRS,

        OHLC_CONCURRENCY,

        async (symbol) => {

          try {

            await scanPairForNewClosedBar(
              symbol
            );

          } catch (err) {

            const pair =
              state.pairState.get(
                symbol
              );

            if (pair) {
              pair.errors++;
            }

            throw err;

          }

        }

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

    } else {

      state.lastMarketError =
        null;

      state.marketReady =
        true;

    }

  } catch (err) {

    state.lastMarketError =
      safeError(err);

    console.warn(

      'Signal scan:',

      state.lastMarketError

    );

  } finally {

    state.scanRunning =
      false;

  }

}

// ============================================================
// FAST PAPER EXIT MONITOR
// ============================================================

async function quotePollLoop() {

  if (
    state.quoteRunning ||
    state.initializing
  ) {

    return;

  }

  state.quoteRunning =
    true;

  state.lastQuotePollAt =
    new Date();

  state.totalQuotePolls++;

  try {

    const quotes =
      await fetchLatestQuotes();

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
      of [...state.openTrades.entries()]
    ) {

      const quote =
        state.latestQuotes.get(
          symbol
        );

      if (!quote) {
        continue;
      }

      // ======================================================
      // BUY
      // BUY closes at BID.
      // ======================================================

      if (
        trade.direction === 'BUY'
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

      // ======================================================
      // SELL
      // SELL closes at ASK.
      // ======================================================

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

  } finally {

    state.quoteRunning =
      false;

  }

}

// ============================================================
// EXPRESS SERVER
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

    const stats =
      account || {};

    res
      .type('html')
      .send(
`<!doctype html>
<html>

<head>

<meta name="viewport"
content="width=device-width,initial-scale=1">

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

<p><b>Mode:</b> PAPER ONLY</p>

<p><b>Live Trading:</b> OFF</p>

<p>
<b>Market:</b>
<span class="${state.marketReady ? 'ok' : 'warn'}">
${state.marketReady ? 'READY' : 'WAIT'}
</span>
</p>

<p><b>Pairs:</b> ${PAIRS.length}</p>

<p><b>Open Trades:</b> ${state.openTrades.size}</p>

<p>
<b>Paper Balance:</b>
${fmtMoney(
  stats.balance ??
  PAPER.startingBalance
)}
</p>

<p>
<b>Closed Trades:</b>
${stats.totalTrades ?? 0}
</p>

<p>
<b>Signals Executed:</b>
${state.executedSignals}
</p>

<p>
<b>Core:</b>
Ultra-Fast Scalp Engine V5.1
</p>

<p>
<b>Entry:</b>
1-Minute CLOSED Candle
</p>

<p>
<b>Risk / Reward:</b>
1:${STRATEGY.riskReward}
</p>

<p>
<b>Data:</b>
Forex Market Feed
</p>

<p>
🔒 NO REAL MONEY USED
</p>

</div>

</body>

</html>`
      );

  }
);

// ============================================================
// HEALTH
// ============================================================

app.get(
  '/health',
  (_req, res) => {

    res.json({

      ok: true,

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

      pairs:
        PAIRS.length,

      openTrades:
        state.openTrades.size,

      startedAt:
        state.startedAt,

      lastSignalScanAt:
        state.lastSignalScanAt,

      lastQuotePollAt:
        state.lastQuotePollAt,

      lastMarketError:
        state.lastMarketError

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

        strategy:
          STRATEGY,

        pairs:
          PAIRS,

        rawSignals:
          state.rawSignals,

        executedSignals:
          state.executedSignals,

        skippedSignals:
          state.skippedSignals,

        signalScans:
          state.totalSignalScans,

        quotePolls:
          state.totalQuotePolls

      },

      providers: {

        twelveDataConfigured:
          Boolean(
            TWELVE_DATA_API_KEY
          )

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
    '⏱ Entry: 1-Minute CLOSED Candle'
  );

  console.log(
    '================================================'
  );

  // ==========================================================
  // START RENDER WEB SERVER FIRST
  // ==========================================================

  app.listen(
    PORT,
    () => {

      console.log(
        `🌐 Health server on port ${PORT}`
      );

    }
  );

  // ==========================================================
  // MONGODB
  // ==========================================================

  await initMongo();

  // ==========================================================
  // TELEGRAM
  // ==========================================================

  await initTelegram();

  // ==========================================================
  // FOREX MARKET
  // ==========================================================

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

  // ==========================================================
  // START LOOPS
  // ==========================================================

  setInterval(
    signalScanLoop,
    SIGNAL_SCAN_MS
  );

  setInterval(
    quotePollLoop,
    QUOTE_POLL_MS
  );

  // First checks.
  setTimeout(
    signalScanLoop,
    1500
  );

  setTimeout(
    quotePollLoop,
    2500
  );

  console.log(
    '✅ LOMY Forex loops started'
  );

}

// ============================================================
// SAFE SHUTDOWN
// ============================================================

process.once(
  'SIGINT',
  () => {

    if (bot) {
      bot.stop('SIGINT');
    }

    mongoose.connection
      .close()
      .catch(() => {});

    process.exit(0);

  }
);

process.once(
  'SIGTERM',
  () => {

    if (bot) {
      bot.stop('SIGTERM');
    }

    mongoose.connection
      .close()
      .catch(() => {});

    process.exit(0);

  }
);

// ============================================================
// START
// ============================================================

boot()
  .catch(
    (err) => {

      console.error(
        'FATAL:',
        err
      );

      process.exit(1);

    }
  );
