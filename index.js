'use strict';

/**
 * LOMY FOREX V1.1.0 — PAPER ONLY
 * Timeframe: 15m CLOSED candles
 * Instruments: 30 FX pairs + XAUUSD
 * Primary signal: Ultra-Fast Scalp Engine V5.1
 * Retest + Auto Fibonacci: lightweight confluence only
 */

const express = require('express');
const axios = require('axios');
const mongoose = require('mongoose');
const { Telegraf } = require('telegraf');

const VERSION = 'LOMY FOREX V1.1.0';
const MODE = 'PAPER';
const LIVE_TRADING = false;
const TIMEFRAME = '15m';

const PORT = Number(process.env.PORT || 10000);
const MONGODB_URI = process.env.MONGODB_URI || '';
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';
const TWELVE_DATA_API_KEY = process.env.TWELVE_DATA_API_KEY || '';

const BIQUOTE_BASE = 'https://biquote.io';
const HTTP_TIMEOUT_MS = 10000;
const HISTORY_LIMIT = 120;
const OHLC_CONCURRENCY = 4;
const CLOCK_CHECK_MS = 2000;
const QUOTE_POLL_MS = 3000;
const HEARTBEAT_MS = 5 * 60 * 1000;
const SCAN_AFTER_SECOND = 4;

const INSTRUMENTS = [
  'EURUSD', 'GBPUSD', 'USDJPY', 'USDCHF', 'AUDUSD', 'NZDUSD', 'USDCAD',
  'EURGBP', 'EURJPY', 'EURCHF', 'EURAUD', 'EURNZD', 'EURCAD',
  'GBPJPY', 'GBPCHF', 'GBPAUD', 'GBPNZD', 'GBPCAD',
  'AUDJPY', 'AUDCHF', 'AUDNZD', 'AUDCAD',
  'NZDJPY', 'NZDCHF', 'NZDCAD',
  'CADJPY', 'CADCHF', 'CHFJPY',
  'GBPSGD', 'EURSGD',
  'XAUUSD'
];

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

const QUALITY = Object.freeze({
  structureLookback: 8,
  fibLookback: 40,
  fibToleranceAtr: 0.20,
  maxChaseAtr: 0.50
});

const PAPER = Object.freeze({
  startingBalance: 1000,
  riskPctPerTrade: 0.50,
  maxOpenTrades: 31,
  accountKey: 'lomy-forex-v110-15m-main'
});

const state = {
  startedAt: new Date(),
  mongoReady: false,
  telegramReady: false,
  marketReady: false,
  initializing: true,

  scanRunning: false,
  quoteRunning: false,

  lastGlobalScanSlot: null,
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

for (const symbol of INSTRUMENTS) {
  state.pairState.set(symbol, {
    bars: [],
    lastClosedBarTime: null,
    lastSignal: 0,
    lastAnalysis: null,
    initialized: false,
    errors: 0
  });
}

const nowIso = () => new Date().toISOString();

function n(value, fallback = NaN) {
  const x = Number(value);
  return Number.isFinite(x) ? x : fallback;
}

function safeError(err) {
  return (
    err?.response?.data?.message ||
    err?.response?.data?.status ||
    err?.message ||
    String(err)
  );
}

function fmtPrice(value, symbol = '') {
  if (!Number.isFinite(value)) return 'n/a';

  if (symbol === 'XAUUSD') {
    return value.toFixed(2);
  }

  if (symbol.endsWith('JPY')) {
    return value.toFixed(3);
  }

  return value.toFixed(5);
}

function fmtMoney(value) {
  const x = Number(value);
  return Number.isFinite(x) ? `$${x.toFixed(2)}` : '$0.00';
}

function fmtPct(value) {
  const x = Number(value);
  return Number.isFinite(x) ? `${x.toFixed(2)}%` : '0.00%';
}

function barTimeMs(bar) {
  const value = new Date(bar?.openTime).getTime();
  return Number.isFinite(value) ? value : 0;
}

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

      open: n(bar.open),
      high: n(bar.high),
      low: n(bar.low),
      close: n(bar.close),

      // Forex / Gold:
      // Prefer tickVolume.
      volume: n(
        bar.tickVolume,
        n(bar.volume, 0)
      ),

      isOpen: Boolean(bar.isOpen)
    }))
    .filter((bar) => {
      return (
        bar.openTime &&
        [bar.open, bar.high, bar.low, bar.close]
          .every(Number.isFinite)
      );
    })
    .sort((a, b) => {
      return barTimeMs(a) - barTimeMs(b);
    });
}

function mergeClosedBars(
  existing,
  incoming,
  maxBars = HISTORY_LIMIT
) {
  const map = new Map();

  for (const bar of existing || []) {
    map.set(bar.openTime, bar);
  }

  for (const bar of incoming || []) {
    if (!bar.isOpen) {
      map.set(bar.openTime, bar);
    }
  }

  return [...map.values()]
    .sort((a, b) => {
      return barTimeMs(a) - barTimeMs(b);
    })
    .slice(-maxBars);
}

function utc15mSlot(date = new Date()) {
  const minute = date.getUTCMinutes();
  const slotMinute = minute - (minute % 15);

  return (
    `${date.getUTCFullYear()}-` +
    `${String(date.getUTCMonth() + 1).padStart(2, '0')}-` +
    `${String(date.getUTCDate()).padStart(2, '0')}T` +
    `${String(date.getUTCHours()).padStart(2, '0')}:` +
    `${String(slotMinute).padStart(2, '0')}`
  );
}

// ============================================================
// V5.1 INDICATORS
// ============================================================

function sma(values, length) {
  if (!Array.isArray(values)) {
    return NaN;
  }

  if (values.length < length) {
    return NaN;
  }

  const slice = values.slice(-length);

  return (
    slice.reduce(
      (sum, value) => sum + value,
      0
    ) / length
  );
}

function emaSeries(values, length) {
  if (!Array.isArray(values)) {
    return [];
  }

  if (values.length < length) {
    return [];
  }

  const result =
    Array(values.length).fill(NaN);

  let seed = 0;

  for (let i = 0; i < length; i++) {
    seed += values[i];
  }

  let previous = seed / length;

  result[length - 1] = previous;

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

    result[i] = previous;
  }

  return result;
}

function rmaSeries(values, length) {
  if (!Array.isArray(values)) {
    return [];
  }

  if (values.length < length) {
    return [];
  }

  const result =
    Array(values.length).fill(NaN);

  let seed = 0;

  for (let i = 0; i < length; i++) {
    seed += values[i];
  }

  let previous =
    seed / length;

  result[length - 1] =
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

    result[i] = previous;
  }

  return result;
}

function atrValue(bars, length) {
  if (!Array.isArray(bars)) {
    return NaN;
  }

  if (bars.length < length + 1) {
    return NaN;
  }

  const trueRanges =
    bars.map((bar, index) => {
      if (index === 0) {
        return bar.high - bar.low;
      }

      const previousClose =
        bars[index - 1].close;

      return Math.max(
        bar.high - bar.low,
        Math.abs(
          bar.high - previousClose
        ),
        Math.abs(
          bar.low - previousClose
        )
      );
    });

  const rma =
    rmaSeries(
      trueRanges,
      length
    );

  return rma[
    rma.length - 1
  ];
}

function cmoValue(closes, length) {
  if (!Array.isArray(closes)) {
    return NaN;
  }

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
    const diff =
      closes[i] -
      closes[i - 1];

    if (diff > 0) {
      gains += diff;
    } else if (diff < 0) {
      losses += -diff;
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
// ULTRA-FAST SCALP ENGINE V5.1
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
    bars.map((bar) => {
      return Number.isFinite(
        bar.volume
      )
        ? bar.volume
        : 0;
    });

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

  // Pine:
  // volAvg = ta.sma(volume, 10)
  const volAvg =
    sma(
      volumes,
      STRATEGY.volLen
    );

  const highVolume =
    Number.isFinite(volAvg) &&
    current.volume >
      volAvg *
      STRATEGY.volMult;

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
    fastEMA > slowEMA;

  const bearTrend =
    Number.isFinite(fastEMA) &&
    Number.isFinite(slowEMA) &&
    fastEMA < slowEMA;

  const atr =
    atrValue(
      bars,
      STRATEGY.atrLen
    );

  // Pine:
  // support = lowest(low[1],20)
  // resistance = highest(high[1],20)
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
    time: current.openTime,
    close: current.close,

    bodyRatio,

    volume:
      current.volume,

    volAvg,
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
// RETEST + AUTO FIBONACCI
// QUALITY SCORE ONLY
// ============================================================

function analyzeQualityTools(
  bars,
  direction,
  analysis
) {
  const current =
    bars[bars.length - 1];

  const prior =
    bars.slice(0, -1);

  const structureBars =
    prior.slice(
      -QUALITY.structureLookback
    );

  let structureLevel = NaN;

  let breakout = false;
  let retest = false;

  if (structureBars.length) {
    if (direction === 'BUY') {
      structureLevel =
        Math.max(
          ...structureBars.map(
            (bar) => bar.high
          )
        );

      breakout =
        current.close >
        structureLevel;

      retest =
        breakout &&
        current.low <=
          structureLevel &&
        current.close >
          structureLevel;
    } else {
      structureLevel =
        Math.min(
          ...structureBars.map(
            (bar) => bar.low
          )
        );

      breakout =
        current.close <
        structureLevel;

      retest =
        breakout &&
        current.high >=
          structureLevel &&
        current.close <
          structureLevel;
    }
  }

  const fibBars =
    prior.slice(
      -QUALITY.fibLookback
    );

  let fib = {
    valid: false,

    swingLow: NaN,
    swingHigh: NaN,

    level382: NaN,
    level500: NaN,
    level618: NaN,
    level786: NaN,

    ext1272: NaN,
    ext1618: NaN,

    inGoldenZone: false,
    nearFib: false
  };

  if (fibBars.length >= 10) {
    const swingLow =
      Math.min(
        ...fibBars.map(
          (bar) => bar.low
        )
      );

    const swingHigh =
      Math.max(
        ...fibBars.map(
          (bar) => bar.high
        )
      );

    const range =
      swingHigh -
      swingLow;

    if (range > 0) {
      let level382;
      let level500;
      let level618;
      let level786;

      let ext1272;
      let ext1618;

      if (direction === 'BUY') {
        level382 =
          swingHigh -
          range * 0.382;

        level500 =
          swingHigh -
          range * 0.500;

        level618 =
          swingHigh -
          range * 0.618;

        level786 =
          swingHigh -
          range * 0.786;

        ext1272 =
          swingLow +
          range * 1.272;

        ext1618 =
          swingLow +
          range * 1.618;
      } else {
        level382 =
          swingLow +
          range * 0.382;

        level500 =
          swingLow +
          range * 0.500;

        level618 =
          swingLow +
          range * 0.618;

        level786 =
          swingLow +
          range * 0.786;

        ext1272 =
          swingHigh -
          range * 1.272;

        ext1618 =
          swingHigh -
          range * 1.618;
      }

      const goldenLow =
        Math.min(
          level500,
          level618
        );

      const goldenHigh =
        Math.max(
          level500,
          level618
        );

      const tolerance =
        Number.isFinite(
          analysis.atr
        )
          ? analysis.atr *
            QUALITY.fibToleranceAtr
          : 0;

      const levels = [
        level382,
        level500,
        level618,
        level786
      ];

      fib = {
        valid: true,

        swingLow,
        swingHigh,

        level382,
        level500,
        level618,
        level786,

        ext1272,
        ext1618,

        inGoldenZone:
          current.close >=
            goldenLow -
              tolerance &&
          current.close <=
            goldenHigh +
              tolerance,

        nearFib:
          levels.some(
            (level) => {
              return (
                Math.abs(
                  current.close -
                  level
                ) <=
                tolerance
              );
            }
          )
      };
    }
  }

  let score = 0;

  if (breakout) {
    score += 1;
  }

  if (retest) {
    score += 1;
  }

  if (fib.inGoldenZone) {
    score += 2;
  } else if (fib.nearFib) {
    score += 1;
  }

  let label = 'NEUTRAL';

  if (score >= 3) {
    label = 'STRONG';
  } else if (score >= 1) {
    label = 'GOOD';
  }

  return {
    score,
    label,

    breakout,
    retest,

    structureLevel,

    fib
  };
}

// ============================================================
// DATABASE
// ============================================================

const paperAccountSchema =
  new mongoose.Schema(
    {
      key: {
        type: String,
        unique: true,
        index: true
      },

      version: String,
      mode: String,

      startingBalance: Number,
      balance: Number,
      realizedPnl: Number,

      totalTrades: Number,
      wins: Number,
      losses: Number,
      breakeven: Number,

      createdAt: Date,
      updatedAt: Date
    },
    {
      minimize: false
    }
  );

const forexTradeSchema =
  new mongoose.Schema(
    {
      version: String,
      mode: String,
      timeframe: String,

      symbol: {
        type: String,
        index: true
      },

      direction: String,

      status: {
        type: String,
        index: true
      },

      signalTime: Date,
      openedAt: Date,
      closedAt: Date,

      signalEntry: Number,
      entryPrice: Number,

      stopLoss: Number,
      takeProfit: Number,

      exitPrice: Number,
      exitReason: String,

      riskDistance: Number,
      riskAmount: Number,

      pnlR: Number,
      pnlUsd: Number,

      balanceAfter: Number,

      spreadAtEntry: Number,
      spreadAtExit: Number,

      executionMoveAtr: Number,

      quality:
        mongoose.Schema.Types.Mixed,

      signal:
        mongoose.Schema.Types.Mixed,

      createdAt: Date,
      updatedAt: Date
    },
    {
      minimize: false
    }
  );

const signalLogSchema =
  new mongoose.Schema(
    {
      version: String,
      timeframe: String,

      symbol: String,
      direction: String,

      candleTime: Date,

      executed: Boolean,
      skipReason: String,

      analysis:
        mongoose.Schema.Types.Mixed,

      quality:
        mongoose.Schema.Types.Mixed,

      createdAt: Date
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

      telegramChatId: String,

      updatedAt: Date
    },
    {
      minimize: false
    }
  );

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

async function initMongo() {
  if (!MONGODB_URI) {
    console.warn(
      '⚠️ MONGODB_URI missing — persistence OFF'
    );

    return;
  }

  try {
    await mongoose.connect(
      MONGODB_URI,
      {
        serverSelectionTimeoutMS:
          10000,

        maxPoolSize: 5
      }
    );

    state.mongoReady = true;

    console.log(
      '✅ MongoDB connected'
    );

    account =
      await PaperAccount.findOne({
        key: PAPER.accountKey
      });

    if (!account) {
      account =
        await PaperAccount.create({
          key:
            PAPER.accountKey,

          version: VERSION,
          mode: MODE,

          startingBalance:
            PAPER.startingBalance,

          balance:
            PAPER.startingBalance,

          realizedPnl: 0,

          totalTrades: 0,

          wins: 0,
          losses: 0,
          breakeven: 0,

          createdAt:
            new Date(),

          updatedAt:
            new Date()
        });
    }

    const savedState =
      await BotState.findOne({
        key: 'main'
      }).lean();

    if (
      savedState?.telegramChatId
    ) {
      lastChatId =
        savedState.telegramChatId;
    }

    const openTrades =
      await ForexTrade.find({
        version: VERSION,
        mode: MODE,
        status: 'OPEN'
      }).lean();

    for (
      const trade of openTrades
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
    state.mongoReady = false;

    console.error(
      '❌ MongoDB:',
      safeError(err)
    );
  }
}

async function rememberChatId(
  chatId
) {
  if (!chatId) {
    return;
  }

  lastChatId =
    String(chatId);

  if (!state.mongoReady) {
    return;
  }

  try {
    await BotState.updateOne(
      {
        key: 'main'
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
      'Telegram chat save:',
      safeError(err)
    );
  }
}

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

async function logSignal(
  symbol,
  direction,
  analysis,
  quality,
  executed,
  skipReason = ''
) {
  if (!state.mongoReady) {
    return;
  }

  try {
    await SignalLog.create({
      version: VERSION,
      timeframe: TIMEFRAME,

      symbol,
      direction,

      candleTime:
        new Date(
          analysis.time
        ),

      executed,
      skipReason,

      analysis,
      quality,

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

async function sendTelegram(
  text
) {
  if (
    !bot ||
    !state.telegramReady ||
    !lastChatId
  ) {
    return;
  }

  try {
    await bot.telegram.sendMessage(
      lastChatId,
      text,
      {
        parse_mode: 'HTML'
      }
    );
  } catch (err) {
    console.warn(
      'Telegram send:',
      safeError(err)
    );
  }
}

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
        `📡 Instruments: ${INSTRUMENTS.length} (30 FX + Gold)\n` +
        `⏱ Entry: ${TIMEFRAME} CLOSED candle\n` +
        `🧠 Core: Ultra-Fast Scalp Engine V5.1\n` +
        `📐 Retest + Auto Fibonacci: lightweight score\n\n` +
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
        [
          ...state
            .pairState
            .values()
        ]
          .filter(
            (item) =>
              item.initialized
          )
          .length;

      await ctx.reply(
        `🤖 ${VERSION}\n\n` +
        `Market: ${state.marketReady ? 'READY' : 'WAIT'}\n` +
        `MongoDB: ${state.mongoReady ? 'OK' : 'OFF'}\n` +
        `Telegram: ${state.telegramReady ? 'OK' : 'OFF'}\n` +
        `Timeframe: ${TIMEFRAME}\n` +
        `Instruments: ${initialized}/${INSTRUMENTS.length}\n` +
        `Open: ${state.openTrades.size}\n` +
        `Scans: ${state.totalSignalScans}\n` +
        `Signals: ${state.rawSignals}\n` +
        `Executed: ${state.executedSignals}\n` +
        `Skipped: ${state.skippedSignals}\n` +
        `Last Error: ${state.lastMarketError || 'none'}`
      );
    }
  );

  bot.command(
    'balance',
    async (ctx) => {
      await rememberChatId(
        ctx.chat.id
      );

      const balance =
        Number(
          account?.balance ??
          PAPER.startingBalance
        );

      const pnl =
        Number(
          account?.realizedPnl ??
          0
        );

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

  bot.command(
    'stats',
    async (ctx) => {
      await rememberChatId(
        ctx.chat.id
      );

      const a =
        account || {
          totalTrades: 0,
          wins: 0,
          losses: 0,
          breakeven: 0,
          balance:
            PAPER.startingBalance,
          realizedPnl: 0
        };

      const total =
        Number(
          a.totalTrades || 0
        );

      const wins =
        Number(
          a.wins || 0
        );

      const winRate =
        total
          ? (
              wins /
              total
            ) * 100
          : 0;

      await ctx.reply(
        `📊 ${VERSION} STATS\n\n` +
        `Timeframe: ${TIMEFRAME}\n` +
        `Closed: ${total}\n` +
        `Wins: ${wins}\n` +
        `Losses: ${Number(a.losses || 0)}\n` +
        `Breakeven: ${Number(a.breakeven || 0)}\n` +
        `Win Rate: ${fmtPct(winRate)}\n` +
        `Realized PnL: ${fmtMoney(Number(a.realizedPnl || 0))}\n` +
        `Balance: ${fmtMoney(Number(a.balance || PAPER.startingBalance))}`
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
        [
          ...state
            .openTrades
            .values()
        ].map(
          (trade) => {
            return (
              `${trade.direction === 'BUY' ? '🟢' : '🔴'} ${trade.symbol} ${trade.direction}\n` +
              `Entry ${fmtPrice(trade.entryPrice, trade.symbol)} | ` +
              `SL ${fmtPrice(trade.stopLoss, trade.symbol)} | ` +
              `TP ${fmtPrice(trade.takeProfit, trade.symbol)}`
            );
          }
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
          version: VERSION,
          status: 'CLOSED'
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
          (trade) => {
            const pnl =
              Number(
                trade.pnlUsd || 0
              );

            return (
              `${pnl > 0 ? '✅' : pnl < 0 ? '❌' : '➖'} ` +
              `${trade.symbol} ${trade.direction} | ${trade.exitReason}\n` +
              `PnL ${fmtMoney(pnl)} | ${Number(trade.pnlR || 0).toFixed(2)}R`
            );
          }
        );

      await ctx.reply(
        rows.join('\n\n')
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
        `📡 ${INSTRUMENTS.length} INSTRUMENTS (${TIMEFRAME})\n` +
        INSTRUMENTS.join(', ')
      );
    }
  );
}

async function initTelegram() {
  if (
    !TELEGRAM_BOT_TOKEN
  ) {
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

    // IMPORTANT:
    // Do NOT await bot.launch()
    // otherwise boot can stop here.
    bot.launch({
      dropPendingUpdates: true
    })
      .then(() => {
        console.log(
          '✅ Telegram polling stopped'
        );
      })
      .catch((err) => {
        state.telegramReady =
          false;

        console.error(
          '❌ Telegram polling:',
          safeError(err)
        );
      });

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
// BIQUOTE
// ============================================================

const http =
  axios.create({
    baseURL:
      BIQUOTE_BASE,

    timeout:
      HTTP_TIMEOUT_MS,

    headers: {
      'User-Agent':
        `${VERSION.replace(/\s+/g, '-')}/paper-test`
    }
  });

async function fetchOhlc(
  symbol,
  limit = 4
) {
  const response =
    await http.get(
      `/api/${symbol}/ohlc`,
      {
        params: {
          interval:
            TIMEFRAME,

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
      `${symbol}: no ${TIMEFRAME} OHLC bars`
    );
  }

  return bars;
}

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
        ? (bid + ask) / 2
        : NaN
    );

  if (
    !Number.isFinite(mid) &&
    !(
      Number.isFinite(bid) &&
      Number.isFinite(ask)
    )
  ) {
    return null;
  }

  return {
    symbol,

    bid:
      Number.isFinite(bid)
        ? bid
        : mid,

    ask:
      Number.isFinite(ask)
        ? ask
        : mid,

    mid,

    timestamp:
      raw.timestamp ||
      raw.openTime ||
      nowIso(),

    stale:
      Boolean(raw.stale),

    marketState:
      String(
        raw.marketState ||
        ''
      ).toLowerCase(),

    quoteAgeSeconds:
      n(
        raw.quoteAgeSeconds,
        0
      )
  };
}

async function ensureQuote(
  symbol
) {
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
        response.data ||
        {}
      );

    if (quote) {
      state.latestQuotes.set(
        symbol,
        quote
      );
    }

    return quote;
  } catch {
    return null;
  }
}

async function fetchLatestQuotes(
  symbols
) {
  if (!symbols?.length) {
    return new Map();
  }

  const params =
    new URLSearchParams();

  for (
    const symbol of symbols
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

  let entries = [];

  if (Array.isArray(raw)) {
    entries =
      raw.map(
        (quote) => [
          String(
            quote.symbol ||
            quote.name ||
            ''
          )
            .replace('/', '')
            .toUpperCase(),

          quote
        ]
      );
  } else if (
    Array.isArray(raw?.items)
  ) {
    entries =
      raw.items.map(
        (quote) => [
          String(
            quote.symbol ||
            quote.name ||
            ''
          )
            .replace('/', '')
            .toUpperCase(),

          quote
        ]
      );
  } else if (
    Array.isArray(raw?.data)
  ) {
    entries =
      raw.data.map(
        (quote) => [
          String(
            quote.symbol ||
            quote.name ||
            ''
          )
            .replace('/', '')
            .toUpperCase(),

          quote
        ]
      );
  } else if (
    raw &&
    typeof raw === 'object'
  ) {
    entries =
      Object.entries(raw);
  }

  const result =
    new Map();

  for (
    const [
      rawSymbol,
      quote
    ] of entries
  ) {
    const symbol =
      String(
        rawSymbol ||
        quote?.symbol ||
        quote?.name ||
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

    const normalized =
      normalizeQuote(
        symbol,
        quote
      );

    if (normalized) {
      result.set(
        symbol,
        normalized
      );
    }
  }

  return result;
}

async function mapWithConcurrency(
  items,
  concurrency,
  fn
) {
  const results =
    new Array(
      items.length
    );

  let index = 0;

  async function worker() {
    while (true) {
      const current =
        index++;

      if (
        current >=
        items.length
      ) {
        break;
      }

      try {
        results[current] =
          await fn(
            items[current],
            current
          );
      } catch (err) {
        results[current] = {
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

async function initializeMarket() {
  console.log(
    `📡 Initializing ${INSTRUMENTS.length} instruments on ${TIMEFRAME}...`
  );

  const results =
    await mapWithConcurrency(
      INSTRUMENTS,
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

        const pairState =
          state.pairState.get(
            symbol
          );

        pairState.bars =
          closed.slice(
            -HISTORY_LIMIT
          );

        pairState
          .lastClosedBarTime =
            pairState.bars[
              pairState
                .bars
                .length - 1
            ]?.openTime ||
            null;

        pairState.lastSignal =
          deriveLastSignalFromHistory(
            pairState.bars
          );

        pairState.lastAnalysis =
          analyzeV51(
            pairState.bars
          );

        pairState.initialized =
          true;

        return {
          symbol,
          count:
            pairState
              .bars
              .length
        };
      }
    );

  let ok = 0;

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
      ok++;
    } else {
      const symbol =
        INSTRUMENTS[i];

      state
        .pairState
        .get(symbol)
        .errors++;

      console.warn(
        `⚠️ ${symbol} init: ${safeError(result?.error || 'unknown')}`
      );
    }
  }

  state.marketReady =
    ok >=
    Math.ceil(
      INSTRUMENTS.length *
      0.70
    );

  state.initializing =
    false;

  console.log(
    `✅ Market initialized: ${ok}/${INSTRUMENTS.length}`
  );

  console.log(
    state.marketReady
      ? '✅ Market engine READY'
      : '⚠️ Market engine PARTIAL'
  );
}

// ============================================================
// PAPER EXECUTION
// ============================================================

function calcSignalStop(
  direction,
  analysis,
  currentBar
) {
  const signalEntry =
    analysis.close;

  const atr =
    analysis.atr;

  if (
    !Number.isFinite(
      signalEntry
    ) ||
    !Number.isFinite(atr) ||
    atr <= 0
  ) {
    return null;
  }

  if (direction === 'BUY') {
    let stopLoss =
      analysis.support -
      atr *
      STRATEGY.atrMargin;

    if (
      !Number.isFinite(
        stopLoss
      ) ||
      stopLoss >=
        signalEntry
    ) {
      stopLoss =
        currentBar.low -
        atr *
        STRATEGY.atrMargin;
    }

    if (
      !(stopLoss <
        signalEntry)
    ) {
      stopLoss =
        signalEntry -
        atr;
    }

    return {
      signalEntry,
      stopLoss
    };
  }

  let stopLoss =
    analysis.resistance +
    atr *
    STRATEGY.atrMargin;

  if (
    !Number.isFinite(
      stopLoss
    ) ||
    stopLoss <=
      signalEntry
  ) {
    stopLoss =
      currentBar.high +
      atr *
      STRATEGY.atrMargin;
  }

  if (
    !(stopLoss >
      signalEntry)
  ) {
    stopLoss =
      signalEntry +
      atr;
  }

  return {
    signalEntry,
    stopLoss
  };
}

async function openPaperTrade(
  symbol,
  direction,
  analysis,
  currentBar,
  quality
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
      quality,
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
      quality,
      false,
      'MAX_OPEN_TRADES'
    );

    return;
  }

  const base =
    calcSignalStop(
      direction,
      analysis,
      currentBar
    );

  if (!base) {
    state.skippedSignals++;

    await logSignal(
      symbol,
      direction,
      analysis,
      quality,
      false,
      'INVALID_SIGNAL_LEVELS'
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
      quality,
      false,
      'NO_FRESH_QUOTE'
    );

    return;
  }

  if (
    quote.stale ||
    quote.marketState ===
      'closed'
  ) {
    state.skippedSignals++;

    await logSignal(
      symbol,
      direction,
      analysis,
      quality,
      false,
      'STALE_OR_CLOSED_MARKET'
    );

    return;
  }

  const entryPrice =
    direction === 'BUY'
      ? quote.ask
      : quote.bid;

  const executionMove =
    Math.abs(
      entryPrice -
      base.signalEntry
    );

  const executionMoveAtr =
    analysis.atr > 0
      ? executionMove /
        analysis.atr
      : Infinity;

  // Only protection against
  // a badly delayed execution.
  if (
    executionMoveAtr >
    QUALITY.maxChaseAtr
  ) {
    state.skippedSignals++;

    await logSignal(
      symbol,
      direction,
      analysis,
      quality,
      false,
      `CHASED_${executionMoveAtr.toFixed(2)}ATR`
    );

    console.log(
      `⏭️ ${symbol} ${direction} skipped: chased ${executionMoveAtr.toFixed(2)} ATR`
    );

    return;
  }

  let stopLoss =
    base.stopLoss;

  let riskDistance;

  if (direction === 'BUY') {
    if (
      !(stopLoss <
        entryPrice)
    ) {
      stopLoss =
        entryPrice -
        analysis.atr;
    }

    riskDistance =
      entryPrice -
      stopLoss;
  } else {
    if (
      !(stopLoss >
        entryPrice)
    ) {
      stopLoss =
        entryPrice +
        analysis.atr;
    }

    riskDistance =
      stopLoss -
      entryPrice;
  }

  if (!(riskDistance > 0)) {
    state.skippedSignals++;

    await logSignal(
      symbol,
      direction,
      analysis,
      quality,
      false,
      'INVALID_EXECUTION_RISK'
    );

    return;
  }

  // CRITICAL FIX:
  // TP calculated from actual entry,
  // never from the old signal price.
  const takeProfit =
    direction === 'BUY'
      ? entryPrice +
        riskDistance *
        STRATEGY.riskReward

      : entryPrice -
        riskDistance *
        STRATEGY.riskReward;

  const validGeometry =
    direction === 'BUY'
      ? (
          stopLoss <
            entryPrice &&
          entryPrice <
            takeProfit
        )
      : (
          takeProfit <
            entryPrice &&
          entryPrice <
            stopLoss
        );

  if (!validGeometry) {
    state.skippedSignals++;

    await logSignal(
      symbol,
      direction,
      analysis,
      quality,
      false,
      'INVALID_SL_ENTRY_TP_GEOMETRY'
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

  const spreadAtEntry =
    Math.max(
      0,
      quote.ask -
      quote.bid
    );

  const document = {
    version: VERSION,
    mode: MODE,
    timeframe: TIMEFRAME,

    symbol,
    direction,

    status: 'OPEN',

    signalTime:
      new Date(
        analysis.time
      ),

    openedAt:
      new Date(),

    closedAt: null,

    signalEntry:
      base.signalEntry,

    entryPrice,

    stopLoss,
    takeProfit,

    exitPrice: null,
    exitReason: null,

    riskDistance,
    riskAmount,

    pnlR: 0,
    pnlUsd: 0,

    balanceAfter: null,

    spreadAtEntry,
    spreadAtExit: null,

    executionMoveAtr,

    quality,

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

  let saved =
    document;

  if (state.mongoReady) {
    saved =
      (
        await ForexTrade.create(
          document
        )
      ).toObject();
  }

  state.openTrades.set(
    symbol,
    saved
  );

  state.executedSignals++;

  await logSignal(
    symbol,
    direction,
    analysis,
    quality,
    true,
    ''
  );

  const fibText =
    quality?.fib?.valid
      ? (
          quality.fib
            .inGoldenZone
            ? 'Fib: Golden Zone'

            : quality.fib
                .nearFib
              ? 'Fib: Near level'

              : 'Fib: Neutral'
        )
      : 'Fib: n/a';

  const message =
    `${direction === 'BUY' ? '🟢' : '🔴'} <b>${direction} ${symbol}</b> — PAPER\n` +
    `⏱ ${TIMEFRAME} CLOSED candle\n\n` +

    `Signal: ${fmtPrice(base.signalEntry, symbol)}\n` +
    `Entry: ${fmtPrice(entryPrice, symbol)}\n` +
    `SL: ${fmtPrice(stopLoss, symbol)}\n` +
    `TP: ${fmtPrice(takeProfit, symbol)}\n` +
    `R:R: 1:${STRATEGY.riskReward}\n` +
    `Move: ${executionMoveAtr.toFixed(2)} ATR\n\n` +

    `V5.1: CMO ${analysis.cmo.toFixed(1)} | ` +
    `Body ${(analysis.bodyRatio * 100).toFixed(0)}% | ` +
    `Vol ${analysis.highVolume ? 'YES' : 'NO'}\n` +

    `Retest: ${quality.retest ? 'YES' : 'NO'} | ` +
    `${fibText} | ` +
    `Quality: ${quality.label} (${quality.score})\n\n` +

    `🔒 PAPER ONLY`;

  await sendTelegram(
    message
  );

  console.log(
    `${direction === 'BUY' ? '🟢' : '🔴'} ` +
    `${symbol} ${direction} @ ${entryPrice} | ` +
    `SL ${stopLoss} | TP ${takeProfit} | ` +
    `Q=${quality.score}`
  );
}

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

      realizedPnl: 0,

      totalTrades: 0,

      wins: 0,
      losses: 0,
      breakeven: 0
    };
  }

  account.balance =
    Number(
      account.balance ||
      PAPER.startingBalance
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
    ) +
    1;

  if (pnlUsd > 0.000001) {
    account.wins =
      Number(
        account.wins ||
        0
      ) +
      1;
  } else if (
    pnlUsd < -0.000001
  ) {
    account.losses =
      Number(
        account.losses ||
        0
      ) +
      1;
  } else {
    account.breakeven =
      Number(
        account.breakeven ||
        0
      ) +
      1;
  }

  const patch = {
    status: 'CLOSED',

    closedAt:
      new Date(),

    exitPrice,
    exitReason,

    pnlR,
    pnlUsd,

    balanceAfter:
      account.balance,

    spreadAtExit:
      quote
        ? Math.max(
            0,
            quote.ask -
            quote.bid
          )
        : 0,

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

  await sendTelegram(
    `${pnlUsd >= 0 ? '✅' : '❌'} <b>CLOSE ${symbol} ${trade.direction}</b>\n` +
    `Reason: ${exitReason}\n` +
    `Exit: ${fmtPrice(exitPrice, symbol)}\n` +
    `PnL: ${fmtMoney(pnlUsd)} (${pnlR.toFixed(2)}R)\n` +
    `Paper Balance: ${fmtMoney(Number(account.balance))}\n\n` +
    `🔒 PAPER ONLY`
  );

  console.log(
    `${pnlUsd >= 0 ? '✅' : '❌'} ` +
    `${symbol} CLOSE ${exitReason} ${pnlR.toFixed(2)}R`
  );
}

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
// 15 MINUTE SCANNER
// ============================================================

async function scanInstrument(
  symbol
) {
  const pairState =
    state.pairState.get(
      symbol
    );

  if (
    !pairState?.initialized
  ) {
    return;
  }

  const latest =
    await fetchOhlc(
      symbol,
      4
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
    pairState
      .lastClosedBarTime &&
    barTimeMs(
      newestClosed
    ) <=
      new Date(
        pairState
          .lastClosedBarTime
      ).getTime()
  ) {
    return;
  }

  pairState.bars =
    mergeClosedBars(
      pairState.bars,
      closed,
      HISTORY_LIMIT
    );

  pairState
    .lastClosedBarTime =
      newestClosed.openTime;

  const analysis =
    analyzeV51(
      pairState.bars
    );

  pairState.lastAnalysis =
    analysis;

  if (!analysis) {
    return;
  }

  let direction = null;

  if (
    analysis.rawBuy &&
    pairState.lastSignal !== 1
  ) {
    direction = 'BUY';

    pairState.lastSignal = 1;
  } else if (
    analysis.rawSell &&
    pairState.lastSignal !== -1
  ) {
    direction = 'SELL';

    pairState.lastSignal = -1;
  }

  if (!direction) {
    return;
  }

  state.rawSignals++;

  const quality =
    analyzeQualityTools(
      pairState.bars,
      direction,
      analysis
    );

  console.log(
    `🚨 ${symbol} ${direction} V5.1 | ` +
    `CMO ${analysis.cmo.toFixed(1)} | ` +
    `body ${(analysis.bodyRatio * 100).toFixed(0)}% | ` +
    `Q=${quality.score}`
  );

  await reverseIfNeeded(
    symbol,
    direction
  );

  await openPaperTrade(
    symbol,
    direction,
    analysis,
    newestClosed,
    quality
  );
}

async function runFullSignalScan(
  slot
) {
  if (
    state.scanRunning ||
    state.initializing
  ) {
    return false;
  }

  state.scanRunning = true;

  state.lastSignalScanAt =
    new Date();

  state.totalSignalScans++;

  const scanNumber =
    state.totalSignalScans;

  console.log(
    `🔎 Scan #${scanNumber} | ` +
    `${INSTRUMENTS.length} instruments | ` +
    `${TIMEFRAME} | ` +
    `${nowIso()}`
  );

  try {
    const results =
      await mapWithConcurrency(
        INSTRUMENTS,
        OHLC_CONCURRENCY,
        async (symbol) => {
          await scanInstrument(
            symbol
          );

          return {
            ok: true
          };
        }
      );

    const errors =
      results.filter(
        (result) =>
          result?.error
      );

    if (errors.length) {
      state.lastMarketError =
        `${errors.length} scan error(s): ` +
        safeError(
          errors[0].error
        );

      console.warn(
        `⚠️ Scan #${scanNumber}: ${state.lastMarketError}`
      );
    } else {
      state.lastMarketError =
        null;

      state.marketReady =
        true;
    }

    state.lastGlobalScanSlot =
      slot;

    console.log(
      `✅ Scan #${scanNumber} complete | ` +
      `raw=${state.rawSignals} ` +
      `executed=${state.executedSignals} ` +
      `skipped=${state.skippedSignals}`
    );

    return true;
  } catch (err) {
    state.lastMarketError =
      safeError(err);

    console.warn(
      `❌ Scan #${scanNumber}: ${state.lastMarketError}`
    );

    return false;
  } finally {
    state.scanRunning =
      false;
  }
}

async function signalClockLoop() {
  if (
    state.initializing ||
    state.scanRunning
  ) {
    return;
  }

  const now =
    new Date();

  const minute =
    now.getUTCMinutes();

  const second =
    now.getUTCSeconds();

  // New 15m candle closes at:
  // :00 :15 :30 :45
  if (
    minute % 15 !== 0 ||
    second <
      SCAN_AFTER_SECOND
  ) {
    return;
  }

  const slot =
    utc15mSlot(now);

  if (
    state.lastGlobalScanSlot ===
    slot
  ) {
    return;
  }

  await runFullSignalScan(
    slot
  );
}

// ============================================================
// PAPER EXIT MONITOR
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
      [
        ...state.openTrades.keys()
      ];

    const quotes =
      await fetchLatestQuotes(
        symbols
      );

    for (
      const [
        symbol,
        quote
      ] of quotes
    ) {
      state.latestQuotes.set(
        symbol,
        quote
      );
    }

    for (
      const [
        symbol,
        trade
      ] of [
        ...state.openTrades.entries()
      ]
    ) {
      const quote =
        state.latestQuotes.get(
          symbol
        );

      if (
        !quote ||
        quote.stale ||
        quote.marketState ===
          'closed'
      ) {
        continue;
      }

      // BUY opens at ask,
      // exits at bid.
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
      } else {
        // SELL opens at bid,
        // exits at ask.
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
// EXPRESS
// ============================================================

const app =
  express();

app.disable(
  'x-powered-by'
);

app.get(
  '/',
  (_req, res) => {
    const a =
      account || {};

    res
      .type('html')
      .send(
        `<!doctype html>
<html>
<head>
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>${VERSION}</title>
<style>
body{
font-family:Arial,sans-serif;
background:#111;
color:#eee;
padding:24px
}
.card{
max-width:760px;
margin:auto;
background:#1b1b1b;
padding:22px;
border-radius:14px
}
.ok{color:#61d17c}
.warn{color:#ffcc66}
</style>
</head>
<body>
<div class="card">
<h2>${VERSION}</h2>
<p><b>Mode:</b> PAPER ONLY</p>
<p><b>Market:</b> <span class="${state.marketReady ? 'ok' : 'warn'}">${state.marketReady ? 'READY' : 'WAIT'}</span></p>
<p><b>Timeframe:</b> ${TIMEFRAME} CLOSED candle</p>
<p><b>Instruments:</b> ${INSTRUMENTS.length} (30 FX + XAUUSD)</p>
<p><b>Open trades:</b> ${state.openTrades.size}</p>
<p><b>Paper balance:</b> ${fmtMoney(Number(a.balance ?? PAPER.startingBalance))}</p>
<p><b>Core:</b> Ultra-Fast Scalp Engine V5.1</p>
<p><b>Quality:</b> Lightweight Retest + Auto Fibonacci score</p>
<p><b>Data:</b> BiQuote primary</p>
</div>
</body>
</html>`
      );
  }
);

app.get(
  '/health',
  (_req, res) => {
    res.json({
      ok: true,

      version: VERSION,
      mode: MODE,

      liveTrading:
        LIVE_TRADING,

      timeframe:
        TIMEFRAME,

      marketReady:
        state.marketReady,

      mongoReady:
        state.mongoReady,

      telegramReady:
        state.telegramReady,

      instruments:
        INSTRUMENTS.length,

      openTrades:
        state.openTrades.size,

      scans:
        state.totalSignalScans,

      lastSignalScanAt:
        state.lastSignalScanAt,

      lastQuotePollAt:
        state.lastQuotePollAt,

      lastMarketError:
        state.lastMarketError
    });
  }
);

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

      timeframe:
        TIMEFRAME,

      paper: {
        startingBalance:
          PAPER.startingBalance,

        balance:
          Number(
            account?.balance ??
            PAPER.startingBalance
          ),

        realizedPnl:
          Number(
            account?.realizedPnl ??
            0
          ),

        totalTrades:
          Number(
            account?.totalTrades ??
            0
          ),

        wins:
          Number(
            account?.wins ??
            0
          ),

        losses:
          Number(
            account?.losses ??
            0
          ),

        openTrades:
          state.openTrades.size
      },

      engine: {
        core:
          'Ultra-Fast Scalp Engine V5.1',

        strategy:
          STRATEGY,

        quality:
          QUALITY,

        instruments:
          INSTRUMENTS,

        rawSignals:
          state.rawSignals,

        executedSignals:
          state.executedSignals,

        skippedSignals:
          state.skippedSignals,

        scans:
          state.totalSignalScans
      },

      providers: {
        primary:
          'BiQuote',

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
    `⏱ Timeframe: ${TIMEFRAME} CLOSED candles`
  );

  console.log(
    `📡 Instruments: ${INSTRUMENTS.length} (30 FX + XAUUSD)`
  );

  console.log(
    '🧠 Core: Ultra-Fast Scalp Engine V5.1'
  );

  console.log(
    '📐 Retest + Auto Fibonacci: lightweight scoring'
  );

  console.log(
    '================================================'
  );

  app.listen(
    PORT,
    () => {
      console.log(
        `🌐 Health server on port ${PORT}`
      );
    }
  );

  await initMongo();

  await initTelegram();

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

  setInterval(
    signalClockLoop,
    CLOCK_CHECK_MS
  );

  setInterval(
    quotePollLoop,
    QUOTE_POLL_MS
  );

  setInterval(
    () => {
      console.log(
        `💓 ${VERSION} | ` +
        `market=${state.marketReady ? 'READY' : 'WAIT'} | ` +
        `open=${state.openTrades.size} | ` +
        `scans=${state.totalSignalScans} | ` +
        `signals=${state.rawSignals} | ` +
        `executed=${state.executedSignals}`
      );
    },
    HEARTBEAT_MS
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
    await mongoose
      .connection
      .close();
  } catch {}

  process.exit(0);
}

process.once(
  'SIGINT',
  () => {
    shutdown('SIGINT');
  }
);

process.once(
  'SIGTERM',
  () => {
    shutdown('SIGTERM');
  }
);

process.on(
  'unhandledRejection',
  (err) => {
    console.error(
      'UNHANDLED REJECTION:',
      safeError(err)
    );
  }
);

process.on(
  'uncaughtException',
  (err) => {
    console.error(
      'UNCAUGHT EXCEPTION:',
      safeError(err)
    );
  }
);

boot().catch(
  (err) => {
    console.error(
      'FATAL:',
      safeError(err)
    );

    process.exit(1);
  }
);
