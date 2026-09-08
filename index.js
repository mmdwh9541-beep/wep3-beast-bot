'use strict';

const axios = require('axios');
const express = require('express');
const mongoose = require('mongoose');
const { Telegraf } = require('telegraf');

// ============================================================
// LOMY FOREX V1.4 — GEMINI COMMANDER
// PAPER ONLY
//
// Architecture:
// Market Data -> Technical Intelligence -> Gemini Commander
// -> Hard Risk Guard -> Paper Execution -> AI Management
//
// IMPORTANT:
// R:R = 1:2 means:
// Risk distance = 1R
// Profit distance = 2R
//
// It does NOT mean:
// -1% account loss / +2% account profit.
//
// The percentage below is only a MAXIMUM CAPITAL-RISK SAFETY CAP
// used for position sizing.
// ============================================================

const VERSION = 'LOMY FOREX V1.4 GEMINI COMMANDER';
const MODE = 'PAPER';
const LIVE_TRADING = false;

const PORT = Number(process.env.PORT || 10000);

const TELEGRAM_BOT_TOKEN =
  String(process.env.TELEGRAM_BOT_TOKEN || '').trim();

const MONGODB_URI =
  String(
    process.env.MONGODB_URI ||
    process.env.MONGODB_URI ||
    ''
  ).trim();

const GEMINI_API_KEY =
  String(process.env.GEMINI_API_KEY || '').trim();

const GEMINI_MODEL =
  String(
    process.env.GEMINI_MODEL ||
    'gemini-2.5-flash'
  ).trim();

const BIQUOTE_BASE =
  String(
    process.env.BIQUOTE_BASE_URL ||
    'https://biquote.io'
  ).replace(/\/+$/, '');

const GEMINI_BASE =
  'https://generativelanguage.googleapis.com/v1beta';

// ============================================================
// TIME / MARKET CONFIG
// ============================================================

const TIMEFRAME = '15m';
const TIMEFRAME_MS = 15 * 60 * 1000;

const HISTORY_LIMIT = 260;
const CORE_MIN_HISTORY = 60;
const EMA200_CONTEXT_HISTORY = 200;

const OHLC_CONCURRENCY = 4;

const QUOTE_POLL_MS = 3000;
const SCAN_TIMER_MS = 4000;

const AI_MANAGE_INTERVAL_MS = 60 * 1000;
const AI_MIN_MANAGE_GAP_MS = 5 * 60 * 1000;

// Prevent hammering Gemini when many instruments are processed.
const GEMINI_MIN_CALL_GAP_MS = 850;

const JOURNAL_COLLECTION =
  'lomyforexjournalv14';

// ============================================================
// INSTRUMENTS
// ============================================================

const INSTRUMENTS = [
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
  'EURSGD',

  'XAUUSD'
];

// ============================================================
// IMMUTABLE TRADING RULES
// ============================================================

const RULES = Object.freeze({
  // Exact structural reward/risk relationship.
  riskReward: 2.0,

  // Mechanical break-even.
  breakEvenTriggerR: 0.60,

  // Reject entry if price has moved too far from analyzed close.
  maxEntryMoveAtr: 0.60,

  // Gemini SL must be technically sensible relative to ATR.
  minStopAtr: 0.25,
  maxStopAtr: 6.00,

  // Spread protection.
  maxSpreadRiskFraction: 0.20,

  // Gemini confidence required for BUY/SELL.
  minEntryConfidence: 62,

  // Gemini confidence required for discretionary CLOSE.
  minCloseConfidence: 68
});

// ============================================================
// CAPITAL SAFETY
//
// maxCapitalRiskPct is NOT a profit/loss target.
//
// It only answers:
// "What is the maximum account capital that this PAPER position
// is permitted to lose if its original SL is hit?"
//
// TP is never calculated from this percentage.
// TP is calculated only from price-distance R:R = 1:2.
// ============================================================

const PAPER = Object.freeze({
  startingBalance: 300,

  maxCapitalRiskPct: 1.00,

  portfolioRiskCapPct: 4.00,

  maxOpenTrades: 31,

  accountKey:
    'lomy-forex-v14-gemini-commander-300usd'
});

// ============================================================
// TECHNICAL INTELLIGENCE CONFIG
// ============================================================

const TECH = Object.freeze({
  emaFast: 9,
  emaMedium: 21,
  emaTrend: 50,
  emaLong: 100,
  emaMacro: 200,

  rsiLen: 14,
  cmoLen: 9,

  atrLen: 14,

  adxLen: 14,

  stochasticLen: 14,
  stochasticSmooth: 3,

  rocLen: 12,

  bbLen: 20,
  bbStd: 2,

  keltnerLen: 20,
  keltnerAtrLen: 14,
  keltnerMult: 1.5,

  volumeLen: 20,

  srLen: 40,

  fibLookback: 60,

  structureLookback: 30,

  swingLeft: 3,
  swingRight: 3,

  liquidityLookback: 20,

  vwapLookback: 50,

  mfiLen: 14
});

// ============================================================
// AI CONFIG
// ============================================================

const AI = Object.freeze({
  enabled: true,

  entryCommanderEnabled: true,

  managementEnabled: true,

  memoryClosedTrades: 30,

  temperature: 0.10,

  timeoutMs: 20000
});

// ============================================================
// STATE
// ============================================================

const state = {
  startedAt: new Date(),

  mongoReady: false,
  telegramReady: false,
  marketReady: false,
  geminiReady: false,

  initializing: true,

  scanRunning: false,
  quoteRunning: false,
  aiManageRunning: false,

  lastScanSlot: null,

  lastSignalScanAt: null,
  lastQuotePollAt: null,
  lastAiManageAt: null,

  lastMarketError: null,
  lastAiError: null,

  totalSignalScans: 0,
  totalQuotePolls: 0,

  aiEntryCalls: 0,
  aiManageCalls: 0,

  aiBuyDecisions: 0,
  aiSellDecisions: 0,
  aiNoTradeDecisions: 0,

  aiCloseDecisions: 0,
  aiHoldDecisions: 0,

  executedSignals: 0,
  skippedSignals: 0,

  breakEvenMoves: 0,

  protectionRejects: 0,
  portfolioRiskRejects: 0,

  journalEvents: 0,

  pairState: new Map(),

  latestQuotes: new Map(),

  openTrades: new Map(),

  closingTrades: new Set(),

  aiLastManageByTrade: new Map(),

  geminiQueue: Promise.resolve(),
  geminiLastCallAt: 0
};

for (const symbol of INSTRUMENTS) {
  state.pairState.set(symbol, {
    bars: [],
    lastClosedBarTime: null,
    lastAnalysis: null,
    initialized: false,
    errors: 0
  });
}

// ============================================================
// BASIC HELPERS
// ============================================================

function n(value, fallback = NaN) {
  const x = Number(value);
  return Number.isFinite(x) ? x : fallback;
}

function safeError(error) {
  const data = error?.response?.data;

  if (typeof data === 'string' && data.trim()) {
    return data.slice(0, 500);
  }

  return (
    data?.error?.message ||
    data?.message ||
    data?.error ||
    error?.message ||
    String(error)
  );
}

function clamp(value, minimum, maximum) {
  return Math.max(
    minimum,
    Math.min(maximum, value)
  );
}

function fmtMoney(value) {
  return '$' + n(value, 0).toFixed(2);
}

function fmtPrice(value, symbol = '') {
  if (!Number.isFinite(Number(value))) {
    return 'n/a';
  }

  value = Number(value);

  if (symbol === 'XAUUSD') {
    return value.toFixed(2);
  }

  if (symbol.endsWith('JPY')) {
    return value.toFixed(3);
  }

  return value.toFixed(5);
}

function barTimeMs(bar) {
  const t = new Date(bar.openTime).getTime();
  return Number.isFinite(t) ? t : 0;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function average(values) {
  const clean = values.filter(Number.isFinite);

  if (!clean.length) {
    return NaN;
  }

  return clean.reduce((a, b) => a + b, 0) / clean.length;
}

function sum(values) {
  return values
    .filter(Number.isFinite)
    .reduce((a, b) => a + b, 0);
}

function standardDeviation(values) {
  const clean = values.filter(Number.isFinite);

  if (!clean.length) {
    return NaN;
  }

  const mean = average(clean);

  const variance =
    clean.reduce(
      (total, value) =>
        total + Math.pow(value - mean, 2),
      0
    ) / clean.length;

  return Math.sqrt(variance);
}

function lastFinite(values) {
  for (let i = values.length - 1; i >= 0; i--) {
    if (Number.isFinite(values[i])) {
      return values[i];
    }
  }

  return NaN;
}

function pctChange(from, to) {
  if (
    !Number.isFinite(from) ||
    !Number.isFinite(to) ||
    from === 0
  ) {
    return NaN;
  }

  return ((to - from) / from) * 100;
}

function highestHigh(bars) {
  if (!bars?.length) {
    return NaN;
  }

  return Math.max(...bars.map(bar => bar.high));
}

function lowestLow(bars) {
  if (!bars?.length) {
    return NaN;
  }

  return Math.min(...bars.map(bar => bar.low));
}

// ============================================================
// BAR NORMALIZATION
// ============================================================

function normalizeBars(rawBars) {
  if (!Array.isArray(rawBars)) {
    return [];
  }

  return rawBars
    .map(bar => ({
      openTime:
        bar.openTime ||
        bar.datetime ||
        bar.time ||
        bar.timestamp,

      open: n(bar.open),
      high: n(bar.high),
      low: n(bar.low),
      close: n(bar.close),

      volume: n(
        bar.tickVolume,
        n(bar.volume, 0)
      ),

      isOpen: bar.isOpen === true
    }))
    .filter(
      bar =>
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
        barTimeMs(a) - barTimeMs(b)
    );
}

function closedBarsOnly(bars, interval = TIMEFRAME) {
  const now = Date.now();

  const intervalMs =
    interval === '4h'
      ? 4 * 60 * 60 * 1000
      : interval === '1h'
        ? 60 * 60 * 1000
        : TIMEFRAME_MS;

  return bars.filter(
    bar =>
      !bar.isOpen &&
      barTimeMs(bar) > 0 &&
      barTimeMs(bar) + intervalMs <= now + 5000
  );
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

  const selected = values.slice(-length);

  if (!selected.every(Number.isFinite)) {
    return NaN;
  }

  return sum(selected) / length;
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

  const out =
    new Array(values.length).fill(NaN);

  const k = 2 / (length + 1);

  const seed = values.slice(0, length);

  if (!seed.every(Number.isFinite)) {
    return out;
  }

  out[length - 1] =
    sum(seed) / length;

  for (let i = length; i < values.length; i++) {
    if (
      !Number.isFinite(values[i]) ||
      !Number.isFinite(out[i - 1])
    ) {
      continue;
    }

    out[i] =
      values[i] * k +
      out[i - 1] * (1 - k);
  }

  return out;
}

function emaLast(values, length) {
  return lastFinite(
    emaSeries(values, length)
  );
}

// ============================================================
// RSI — WILDER
// ============================================================

function rsiLast(values, length = 14) {
  if (values.length < length + 1) {
    return NaN;
  }

  let gain = 0;
  let loss = 0;

  for (let i = 1; i <= length; i++) {
    const change =
      values[i] - values[i - 1];

    if (change > 0) {
      gain += change;
    } else {
      loss += Math.abs(change);
    }
  }

  let avgGain = gain / length;
  let avgLoss = loss / length;

  for (let i = length + 1; i < values.length; i++) {
    const change =
      values[i] - values[i - 1];

    const currentGain =
      change > 0 ? change : 0;

    const currentLoss =
      change < 0 ? Math.abs(change) : 0;

    avgGain =
      (
        avgGain * (length - 1) +
        currentGain
      ) / length;

    avgLoss =
      (
        avgLoss * (length - 1) +
        currentLoss
      ) / length;
  }

  if (avgLoss === 0) {
    return avgGain === 0 ? 50 : 100;
  }

  const rs = avgGain / avgLoss;

  return 100 - 100 / (1 + rs);
}

// ============================================================
// CMO
// ============================================================

function cmoLast(values, length) {
  if (values.length < length + 1) {
    return NaN;
  }

  let up = 0;
  let down = 0;

  for (
    let i = values.length - length;
    i < values.length;
    i++
  ) {
    const difference =
      values[i] - values[i - 1];

    if (difference > 0) {
      up += difference;
    } else {
      down += Math.abs(difference);
    }
  }

  const denominator = up + down;

  return denominator === 0
    ? 0
    : 100 * (up - down) / denominator;
}

// ============================================================
// TRUE RANGE / ATR
// ============================================================

function trueRangeSeries(bars) {
  if (!bars || bars.length < 2) {
    return [];
  }

  const output = [];

  for (let i = 1; i < bars.length; i++) {
    const current = bars[i];
    const previousClose = bars[i - 1].close;

    output.push(
      Math.max(
        current.high - current.low,
        Math.abs(current.high - previousClose),
        Math.abs(current.low - previousClose)
      )
    );
  }

  return output;
}

function atrLast(bars, length = 14) {
  const ranges = trueRangeSeries(bars);

  if (ranges.length < length) {
    return NaN;
  }

  let atr =
    sum(ranges.slice(0, length)) / length;

  for (let i = length; i < ranges.length; i++) {
    atr =
      (
        atr * (length - 1) +
        ranges[i]
      ) / length;
  }

  return atr;
}

// ============================================================
// MACD
// ============================================================

function macdLast(
  values,
  fast = 12,
  slow = 26,
  signalLength = 9
) {
  if (values.length < slow + signalLength) {
    return {
      macd: NaN,
      signal: NaN,
      histogram: NaN
    };
  }

  const fastSeries = emaSeries(values, fast);
  const slowSeries = emaSeries(values, slow);

  const macdValues = [];

  for (let i = 0; i < values.length; i++) {
    if (
      Number.isFinite(fastSeries[i]) &&
      Number.isFinite(slowSeries[i])
    ) {
      macdValues.push(
        fastSeries[i] - slowSeries[i]
      );
    }
  }

  if (macdValues.length < signalLength) {
    return {
      macd: NaN,
      signal: NaN,
      histogram: NaN
    };
  }

  const macd = macdValues[macdValues.length - 1];
  const signal = emaLast(macdValues, signalLength);

  return {
    macd,
    signal,
    histogram:
      Number.isFinite(signal)
        ? macd - signal
        : NaN
  };
}

// ============================================================
// STOCHASTIC
// ============================================================

function stochasticLast(
  bars,
  length = 14,
  smooth = 3
) {
  if (bars.length < length + smooth - 1) {
    return {
      k: NaN,
      d: NaN
    };
  }

  const kValues = [];

  for (
    let end = bars.length - smooth;
    end < bars.length;
    end++
  ) {
    const start =
      end - length + 1;

    if (start < 0) {
      continue;
    }

    const window =
      bars.slice(start, end + 1);

    const high =
      highestHigh(window);

    const low =
      lowestLow(window);

    const close =
      bars[end].close;

    const range =
      high - low;

    kValues.push(
      range > 0
        ? ((close - low) / range) * 100
        : 50
    );
  }

  if (!kValues.length) {
    return {
      k: NaN,
      d: NaN
    };
  }

  return {
    k: kValues[kValues.length - 1],
    d: average(kValues)
  };
}

// ============================================================
// WILLIAMS %R
// ============================================================

function williamsRLast(bars, length = 14) {
  if (bars.length < length) {
    return NaN;
  }

  const window =
    bars.slice(-length);

  const high =
    highestHigh(window);

  const low =
    lowestLow(window);

  const close =
    bars[bars.length - 1].close;

  const range =
    high - low;

  if (!(range > 0)) {
    return -50;
  }

  return (
    -100 *
    (high - close) /
    range
  );
}

// ============================================================
// RATE OF CHANGE
// ============================================================

function rocLast(values, length = 12) {
  if (values.length < length + 1) {
    return NaN;
  }

  const current =
    values[values.length - 1];

  const previous =
    values[values.length - 1 - length];

  return pctChange(previous, current);
}

// ============================================================
// BOLLINGER BANDS
// ============================================================

function bollingerLast(
  values,
  length = 20,
  multiplier = 2
) {
  if (values.length < length) {
    return {
      middle: NaN,
      upper: NaN,
      lower: NaN,
      widthPct: NaN,
      position: NaN
    };
  }

  const window =
    values.slice(-length);

  const middle =
    average(window);

  const deviation =
    standardDeviation(window);

  const upper =
    middle +
    deviation * multiplier;

  const lower =
    middle -
    deviation * multiplier;

  const current =
    values[values.length - 1];

  const width =
    upper - lower;

  return {
    middle,
    upper,
    lower,

    widthPct:
      middle !== 0
        ? (width / middle) * 100
        : NaN,

    position:
      width > 0
        ? (current - lower) / width
        : 0.5
  };
}

// ============================================================
// KELTNER CHANNEL
// ============================================================

function keltnerLast(
  bars,
  length = 20,
  atrLength = 14,
  multiplier = 1.5
) {
  if (bars.length < Math.max(length, atrLength) + 1) {
    return {
      middle: NaN,
      upper: NaN,
      lower: NaN,
      position: NaN
    };
  }

  const closes =
    bars.map(bar => bar.close);

  const middle =
    emaLast(closes, length);

  const atr =
    atrLast(bars, atrLength);

  if (
    !Number.isFinite(middle) ||
    !Number.isFinite(atr)
  ) {
    return {
      middle: NaN,
      upper: NaN,
      lower: NaN,
      position: NaN
    };
  }

  const upper =
    middle + atr * multiplier;

  const lower =
    middle - atr * multiplier;

  const current =
    closes[closes.length - 1];

  const width =
    upper - lower;

  return {
    middle,
    upper,
    lower,

    position:
      width > 0
        ? (current - lower) / width
        : 0.5
  };
}

// ============================================================
// DMI / ADX
// ============================================================

function dmiAdx(bars, length = 14) {
  if (bars.length < length * 2 + 2) {
    return {
      adx: NaN,
      plusDI: NaN,
      minusDI: NaN
    };
  }

  const tr = [];
  const plusDM = [];
  const minusDM = [];

  for (let i = 1; i < bars.length; i++) {
    const current = bars[i];
    const previous = bars[i - 1];

    const upMove =
      current.high - previous.high;

    const downMove =
      previous.low - current.low;

    plusDM.push(
      upMove > downMove && upMove > 0
        ? upMove
        : 0
    );

    minusDM.push(
      downMove > upMove && downMove > 0
        ? downMove
        : 0
    );

    tr.push(
      Math.max(
        current.high - current.low,
        Math.abs(current.high - previous.close),
        Math.abs(current.low - previous.close)
      )
    );
  }

  let trSmooth =
    sum(tr.slice(0, length));

  let plusSmooth =
    sum(plusDM.slice(0, length));

  let minusSmooth =
    sum(minusDM.slice(0, length));

  const dx = [];

  let plusDI = NaN;
  let minusDI = NaN;

  for (let i = length; i < tr.length; i++) {
    if (i > length) {
      trSmooth =
        trSmooth -
        trSmooth / length +
        tr[i];

      plusSmooth =
        plusSmooth -
        plusSmooth / length +
        plusDM[i];

      minusSmooth =
        minusSmooth -
        minusSmooth / length +
        minusDM[i];
    }

    plusDI =
      trSmooth > 0
        ? 100 * plusSmooth / trSmooth
        : 0;

    minusDI =
      trSmooth > 0
        ? 100 * minusSmooth / trSmooth
        : 0;

    const denominator =
      plusDI + minusDI;

    dx.push(
      denominator > 0
        ? (
            100 *
            Math.abs(plusDI - minusDI) /
            denominator
          )
        : 0
    );
  }

  if (dx.length < length) {
    return {
      adx: NaN,
      plusDI,
      minusDI
    };
  }

  let adx =
    average(dx.slice(0, length));

  for (let i = length; i < dx.length; i++) {
    adx =
      (
        adx * (length - 1) +
        dx[i]
      ) / length;
  }

  return {
    adx,
    plusDI,
    minusDI
  };
}

// ============================================================
// OBV
// ============================================================

function obvContext(bars, lookback = 20) {
  if (bars.length < 3) {
    return {
      value: 0,
      change: 0,
      direction: 'FLAT'
    };
  }

  const values = [0];

  for (let i = 1; i < bars.length; i++) {
    const previous =
      values[values.length - 1];

    if (bars[i].close > bars[i - 1].close) {
      values.push(
        previous + n(bars[i].volume, 0)
      );
    } else if (bars[i].close < bars[i - 1].close) {
      values.push(
        previous - n(bars[i].volume, 0)
      );
    } else {
      values.push(previous);
    }
  }

  const current =
    values[values.length - 1];

  const previousIndex =
    Math.max(
      0,
      values.length - 1 - lookback
    );

  const previous =
    values[previousIndex];

  const change =
    current - previous;

  return {
    value: current,
    change,

    direction:
      change > 0
        ? 'UP'
        : change < 0
          ? 'DOWN'
          : 'FLAT'
  };
}

// ============================================================
// MFI
// ============================================================

function mfiLast(bars, length = 14) {
  if (bars.length < length + 1) {
    return NaN;
  }

  let positive = 0;
  let negative = 0;

  const start =
    bars.length - length;

  for (let i = start; i < bars.length; i++) {
    if (i <= 0) {
      continue;
    }

    const typical =
      (
        bars[i].high +
        bars[i].low +
        bars[i].close
      ) / 3;

    const previousTypical =
      (
        bars[i - 1].high +
        bars[i - 1].low +
        bars[i - 1].close
      ) / 3;

    const flow =
      typical *
      Math.max(0, n(bars[i].volume, 0));

    if (typical > previousTypical) {
      positive += flow;
    } else if (typical < previousTypical) {
      negative += flow;
    }
  }

  if (negative === 0) {
    return positive > 0 ? 100 : 50;
  }

  const ratio =
    positive / negative;

  return 100 - 100 / (1 + ratio);
}

// ============================================================
// ROLLING VWAP
// ============================================================

function rollingVwap(bars, lookback = 50) {
  if (!bars.length) {
    return NaN;
  }

  const window =
    bars.slice(-lookback);

  let numerator = 0;
  let denominator = 0;

  for (const bar of window) {
    const volume =
      Math.max(0, n(bar.volume, 0));

    const typical =
      (
        bar.high +
        bar.low +
        bar.close
      ) / 3;

    numerator +=
      typical * volume;

    denominator +=
      volume;
  }

  if (denominator <= 0) {
    return NaN;
  }

  return numerator / denominator;
}

// ============================================================
// CANDLE / PRICE ACTION
// ============================================================

function candleContext(bars) {
  if (!bars.length) {
    return null;
  }

  const current =
    bars[bars.length - 1];

  const previous =
    bars.length >= 2
      ? bars[bars.length - 2]
      : null;

  const range =
    current.high - current.low;

  const body =
    Math.abs(
      current.close - current.open
    );

  const bodyRatio =
    range > 0
      ? body / range
      : 0;

  const upperWick =
    range > 0
      ? (
          current.high -
          Math.max(
            current.open,
            current.close
          )
        ) / range
      : 0;

  const lowerWick =
    range > 0
      ? (
          Math.min(
            current.open,
            current.close
          ) -
          current.low
        ) / range
      : 0;

  const closeLocation =
    range > 0
      ? (
          current.close -
          current.low
        ) / range
      : 0.5;

  let bullishEngulfing = false;
  let bearishEngulfing = false;

  if (previous) {
    bullishEngulfing =
      previous.close < previous.open &&
      current.close > current.open &&
      current.open <= previous.close &&
      current.close >= previous.open;

    bearishEngulfing =
      previous.close > previous.open &&
      current.close < current.open &&
      current.open >= previous.close &&
      current.close <= previous.open;
  }

  return {
    direction:
      current.close > current.open
        ? 'BULL'
        : current.close < current.open
          ? 'BEAR'
          : 'DOJI',

    range,
    body,
    bodyRatio,
    upperWick,
    lowerWick,
    closeLocation,

    bullishEngulfing,
    bearishEngulfing,

    bullishRejection:
      lowerWick >= 0.45 &&
      closeLocation >= 0.60,

    bearishRejection:
      upperWick >= 0.45 &&
      closeLocation <= 0.40
  };
}

// ============================================================
// SUPPORT / RESISTANCE
// ============================================================

function supportResistance(
  bars,
  lookback = 40
) {
  if (bars.length < 5) {
    return {
      support: NaN,
      resistance: NaN,
      distanceToSupport: NaN,
      distanceToResistance: NaN
    };
  }

  const current =
    bars[bars.length - 1];

  const prior =
    bars.slice(
      Math.max(
        0,
        bars.length - 1 - lookback
      ),
      -1
    );

  if (!prior.length) {
    return {
      support: NaN,
      resistance: NaN,
      distanceToSupport: NaN,
      distanceToResistance: NaN
    };
  }

  const support =
    lowestLow(prior);

  const resistance =
    highestHigh(prior);

  return {
    support,
    resistance,

    distanceToSupport:
      current.close - support,

    distanceToResistance:
      resistance - current.close
  };
}

// ============================================================
// SWING DETECTION
// ============================================================

function findSwings(
  bars,
  left = 3,
  right = 3
) {
  const highs = [];
  const lows = [];

  if (
    bars.length <
    left + right + 1
  ) {
    return {
      highs,
      lows
    };
  }

  for (
    let i = left;
    i < bars.length - right;
    i++
  ) {
    let isHigh = true;
    let isLow = true;

    for (
      let j = i - left;
      j <= i + right;
      j++
    ) {
      if (j === i) {
        continue;
      }

      if (bars[j].high >= bars[i].high) {
        isHigh = false;
      }

      if (bars[j].low <= bars[i].low) {
        isLow = false;
      }

      if (!isHigh && !isLow) {
        break;
      }
    }

    if (isHigh) {
      highs.push({
        index: i,
        price: bars[i].high,
        time: bars[i].openTime
      });
    }

    if (isLow) {
      lows.push({
        index: i,
        price: bars[i].low,
        time: bars[i].openTime
      });
    }
  }

  return {
    highs,
    lows
  };
}

// ============================================================
// MARKET STRUCTURE — HH/HL/LH/LL + BOS
// ============================================================

function marketStructure(bars) {
  const swings =
    findSwings(
      bars,
      TECH.swingLeft,
      TECH.swingRight
    );

  const recentHighs =
    swings.highs.slice(-2);

  const recentLows =
    swings.lows.slice(-2);

  const current =
    bars[bars.length - 1];

  let highStructure = 'UNKNOWN';
  let lowStructure = 'UNKNOWN';

  if (recentHighs.length >= 2) {
    highStructure =
      recentHighs[1].price >
      recentHighs[0].price
        ? 'HH'
        : 'LH';
  }

  if (recentLows.length >= 2) {
    lowStructure =
      recentLows[1].price >
      recentLows[0].price
        ? 'HL'
        : 'LL';
  }

  let structure = 'MIXED';

  if (
    highStructure === 'HH' &&
    lowStructure === 'HL'
  ) {
    structure = 'BULL';
  } else if (
    highStructure === 'LH' &&
    lowStructure === 'LL'
  ) {
    structure = 'BEAR';
  }

  const lastSwingHigh =
    recentHighs.length
      ? recentHighs[
          recentHighs.length - 1
        ].price
      : NaN;

  const lastSwingLow =
    recentLows.length
      ? recentLows[
          recentLows.length - 1
        ].price
      : NaN;

  const bullishBos =
    Number.isFinite(lastSwingHigh) &&
    current.close > lastSwingHigh;

  const bearishBos =
    Number.isFinite(lastSwingLow) &&
    current.close < lastSwingLow;

  return {
    structure,
    highStructure,
    lowStructure,

    lastSwingHigh,
    lastSwingLow,

    bullishBos,
    bearishBos,

    swingHighCount:
      swings.highs.length,

    swingLowCount:
      swings.lows.length
  };
}

// ============================================================
// LIQUIDITY SWEEP
// ============================================================

function liquidityContext(
  bars,
  lookback = 20
) {
  if (bars.length < lookback + 1) {
    return {
      bullishSweep: false,
      bearishSweep: false,
      priorHigh: NaN,
      priorLow: NaN
    };
  }

  const current =
    bars[bars.length - 1];

  const prior =
    bars.slice(
      -lookback - 1,
      -1
    );

  const priorHigh =
    highestHigh(prior);

  const priorLow =
    lowestLow(prior);

  const bullishSweep =
    current.low < priorLow &&
    current.close > priorLow;

  const bearishSweep =
    current.high > priorHigh &&
    current.close < priorHigh;

  return {
    bullishSweep,
    bearishSweep,
    priorHigh,
    priorLow
  };
}

// ============================================================
// FAIR VALUE GAP
// ============================================================

function fvgContext(bars) {
  if (bars.length < 3) {
    return {
      bullish: false,
      bearish: false,
      bullGapLow: NaN,
      bullGapHigh: NaN,
      bearGapLow: NaN,
      bearGapHigh: NaN
    };
  }

  const first =
    bars[bars.length - 3];

  const third =
    bars[bars.length - 1];

  const bullish =
    third.low > first.high;

  const bearish =
    third.high < first.low;

  return {
    bullish,
    bearish,

    bullGapLow:
      bullish
        ? first.high
        : NaN,

    bullGapHigh:
      bullish
        ? third.low
        : NaN,

    bearGapLow:
      bearish
        ? third.high
        : NaN,

    bearGapHigh:
      bearish
        ? first.low
        : NaN
  };
}

// ============================================================
// FIBONACCI
// ============================================================

function fibonacciContext(
  bars,
  lookback = 60
) {
  if (bars.length < 10) {
    return null;
  }

  const window =
    bars.slice(-lookback);

  const high =
    highestHigh(window);

  const low =
    lowestLow(window);

  const range =
    high - low;

  if (!(range > 0)) {
    return null;
  }

  const current =
    bars[bars.length - 1].close;

  const retracementFromHigh = {
    r382:
      high - range * 0.382,

    r500:
      high - range * 0.500,

    r618:
      high - range * 0.618,

    r786:
      high - range * 0.786
  };

  const retracementFromLow = {
    r382:
      low + range * 0.382,

    r500:
      low + range * 0.500,

    r618:
      low + range * 0.618,

    r786:
      low + range * 0.786
  };

  const extensionUp = {
    e1272:
      low + range * 1.272,

    e1618:
      low + range * 1.618
  };

  const extensionDown = {
    e1272:
      high - range * 1.272,

    e1618:
      high - range * 1.618
  };

  return {
    swingHigh: high,
    swingLow: low,
    range,
    current,

    retracementFromHigh,
    retracementFromLow,

    extensionUp,
    extensionDown
  };
}

// ============================================================
// VOLUME CONTEXT
// ============================================================

function volumeContext(bars) {
  if (!bars.length) {
    return {
      current: 0,
      average: 0,
      ratio: NaN,
      spike: false
    };
  }

  const current =
    n(
      bars[bars.length - 1].volume,
      0
    );

  const prior =
    bars
      .slice(
        -TECH.volumeLen - 1,
        -1
      )
      .map(bar =>
        n(bar.volume, 0)
      );

  const avg =
    average(prior);

  const ratio =
    Number.isFinite(avg) &&
    avg > 0
      ? current / avg
      : NaN;

  return {
    current,
    average:
      Number.isFinite(avg)
        ? avg
        : 0,

    ratio,

    spike:
      Number.isFinite(ratio) &&
      ratio >= 1.5
  };
}

// ============================================================
// VOLATILITY REGIME
// ============================================================

function volatilityContext(bars) {
  const atr =
    atrLast(
      bars,
      TECH.atrLen
    );

  const current =
    bars[bars.length - 1];

  const atrPct =
    Number.isFinite(atr) &&
    current.close > 0
      ? atr / current.close * 100
      : NaN;

  const historical = [];

  const minimum =
    Math.max(
      TECH.atrLen + 2,
      bars.length - 50
    );

  for (
    let i = minimum;
    i <= bars.length;
    i++
  ) {
    const sample =
      bars.slice(0, i);

    const value =
      atrLast(
        sample,
        TECH.atrLen
      );

    if (Number.isFinite(value)) {
      historical.push(value);
    }
  }

  const avgAtr =
    average(historical);

  const ratio =
    Number.isFinite(avgAtr) &&
    avgAtr > 0 &&
    Number.isFinite(atr)
      ? atr / avgAtr
      : NaN;

  let regime = 'NORMAL';

  if (Number.isFinite(ratio)) {
    if (ratio >= 1.50) {
      regime = 'HIGH';
    } else if (ratio <= 0.70) {
      regime = 'LOW';
    }
  }

  return {
    atr,
    atrPct,
    averageAtr: avgAtr,
    atrRatio: ratio,
    regime
  };
}

// ============================================================
// TREND CONTEXT
// ============================================================

function trendContext(bars) {
  const closes =
    bars.map(bar => bar.close);

  const close =
    closes[closes.length - 1];

  const ema9 =
    emaLast(
      closes,
      TECH.emaFast
    );

  const ema21 =
    emaLast(
      closes,
      TECH.emaMedium
    );

  const ema50 =
    emaLast(
      closes,
      TECH.emaTrend
    );

  const ema100 =
    emaLast(
      closes,
      TECH.emaLong
    );

  const ema200 =
    bars.length >=
    EMA200_CONTEXT_HISTORY
      ? emaLast(
          closes,
          TECH.emaMacro
        )
      : NaN;

  let alignment = 'MIXED';

  if (
    Number.isFinite(ema50) &&
    close > ema9 &&
    ema9 > ema21 &&
    ema21 > ema50
  ) {
    alignment = 'BULL';
  } else if (
    Number.isFinite(ema50) &&
    close < ema9 &&
    ema9 < ema21 &&
    ema21 < ema50
  ) {
    alignment = 'BEAR';
  }

  let macro = 'UNKNOWN';

  if (Number.isFinite(ema200)) {
    macro =
      close > ema200
        ? 'BULL'
        : close < ema200
          ? 'BEAR'
          : 'FLAT';
  } else if (Number.isFinite(ema100)) {
    macro =
      close > ema100
        ? 'BULL'
        : 'BEAR';
  }

  return {
    close,
    ema9,
    ema21,
    ema50,
    ema100,
    ema200,
    alignment,
    macro
  };
}

// ============================================================
// MOMENTUM CONTEXT
// ============================================================

function momentumContext(bars) {
  const closes =
    bars.map(bar => bar.close);

  const macd =
    macdLast(closes);

  const stochastic =
    stochasticLast(
      bars,
      TECH.stochasticLen,
      TECH.stochasticSmooth
    );

  return {
    rsi:
      rsiLast(
        closes,
        TECH.rsiLen
      ),

    cmo:
      cmoLast(
        closes,
        TECH.cmoLen
      ),

    macd,

    stochastic,

    williamsR:
      williamsRLast(
        bars,
        TECH.stochasticLen
      ),

    roc:
      rocLast(
        closes,
        TECH.rocLen
      )
  };
}

// ============================================================
// COMPLETE TECHNICAL INTELLIGENCE ENGINE
// ============================================================

function buildTechnicalIntelligence(
  symbol,
  bars
) {
  if (
    !Array.isArray(bars) ||
    bars.length < CORE_MIN_HISTORY
  ) {
    return null;
  }

  const current =
    bars[bars.length - 1];

  const closes =
    bars.map(bar => bar.close);

  const trend =
    trendContext(bars);

  const momentum =
    momentumContext(bars);

  const dmi =
    dmiAdx(
      bars,
      TECH.adxLen
    );

  const volatility =
    volatilityContext(bars);

  const bollinger =
    bollingerLast(
      closes,
      TECH.bbLen,
      TECH.bbStd
    );

  const keltner =
    keltnerLast(
      bars,
      TECH.keltnerLen,
      TECH.keltnerAtrLen,
      TECH.keltnerMult
    );

  const volume =
    volumeContext(bars);

  const obv =
    obvContext(bars);

  const mfi =
    mfiLast(
      bars,
      TECH.mfiLen
    );

  const vwap =
    rollingVwap(
      bars,
      TECH.vwapLookback
    );

  const candles =
    candleContext(bars);

  const sr =
    supportResistance(
      bars,
      TECH.srLen
    );

  const structure =
    marketStructure(bars);

  const liquidity =
    liquidityContext(
      bars,
      TECH.liquidityLookback
    );

  const fvg =
    fvgContext(bars);

  const fibonacci =
    fibonacciContext(
      bars,
      TECH.fibLookback
    );

  const atr =
    volatility.atr;

  const range =
    current.high -
    current.low;

  const rangeAtr =
    Number.isFinite(atr) &&
    atr > 0
      ? range / atr
      : NaN;

  const distanceFromVwapAtr =
    Number.isFinite(vwap) &&
    Number.isFinite(atr) &&
    atr > 0
      ? (
          current.close -
          vwap
        ) / atr
      : NaN;

  let directionalBias = 'MIXED';
  let bullScore = 0;
  let bearScore = 0;

  if (trend.alignment === 'BULL') {
    bullScore += 2;
  }

  if (trend.alignment === 'BEAR') {
    bearScore += 2;
  }

  if (trend.macro === 'BULL') {
    bullScore++;
  }

  if (trend.macro === 'BEAR') {
    bearScore++;
  }

  if (
    Number.isFinite(dmi.plusDI) &&
    Number.isFinite(dmi.minusDI)
  ) {
    if (dmi.plusDI > dmi.minusDI) {
      bullScore++;
    } else if (dmi.minusDI > dmi.plusDI) {
      bearScore++;
    }
  }

  if (
    Number.isFinite(momentum.macd.histogram)
  ) {
    if (momentum.macd.histogram > 0) {
      bullScore++;
    } else if (momentum.macd.histogram < 0) {
      bearScore++;
    }
  }

  if (structure.structure === 'BULL') {
    bullScore += 2;
  }

  if (structure.structure === 'BEAR') {
    bearScore += 2;
  }

  if (structure.bullishBos) {
    bullScore += 2;
  }

  if (structure.bearishBos) {
    bearScore += 2;
  }

  if (liquidity.bullishSweep) {
    bullScore += 2;
  }

  if (liquidity.bearishSweep) {
    bearScore += 2;
  }

  if (candles?.bullishEngulfing) {
    bullScore++;
  }

  if (candles?.bearishEngulfing) {
    bearScore++;
  }

  if (candles?.bullishRejection) {
    bullScore++;
  }

  if (candles?.bearishRejection) {
    bearScore++;
  }

  if (bullScore >= bearScore + 3) {
    directionalBias = 'BULL';
  } else if (bearScore >= bullScore + 3) {
    directionalBias = 'BEAR';
  }

  return {
    symbol,

    timeframe:
      TIMEFRAME,

    barTime:
      current.openTime,

    price: {
      open:
        current.open,

      high:
        current.high,

      low:
        current.low,

      close:
        current.close,

      range,
      rangeAtr
    },

    trend,

    momentum,

    dmi,

    volatility,

    bollinger,

    keltner,

    volume,

    obv,

    mfi,

    vwap,

    distanceFromVwapAtr,

    candles,

    supportResistance:
      sr,

    structure,

    liquidity,

    fvg,

    fibonacci,

    internalBias: {
      directionalBias,
      bullScore,
      bearScore
    }
  };
}

// ============================================================
// END PART 1 / 4
// Next part starts with:
// HTTP CLIENT + MARKET DATA + MONGODB + AI MEMORY + GEMINI QUEUE
// ============================================================
// ============================================================
// PART 2 / 4
// HTTP CLIENT + MARKET DATA + MONGODB + MEMORY + GEMINI
// ============================================================

// ============================================================
// HTTP CLIENT
// ============================================================

const http = axios.create({
  timeout: 12000,

  headers: {
    'User-Agent':
      'LOMY-Forex-Gemini-Commander/1.4'
  }
});

// ============================================================
// MARKET DATA
// ============================================================

async function fetchOhlc(
  symbol,
  interval = TIMEFRAME,
  limit = HISTORY_LIMIT
) {
  const response =
    await http.get(
      `${BIQUOTE_BASE}/api/${encodeURIComponent(symbol)}/ohlc`,
      {
        params: {
          interval,
          limit
        }
      }
    );

  const body = response.data;

  const raw =
    Array.isArray(body)
      ? body
      : Array.isArray(body?.bars)
        ? body.bars
        : Array.isArray(body?.data?.bars)
          ? body.data.bars
          : Array.isArray(body?.data)
            ? body.data
            : [];

  return closedBarsOnly(
    normalizeBars(raw),
    interval
  );
}

function normalizeQuote(
  raw,
  symbol
) {
  const bid =
    n(raw?.bid);

  const ask =
    n(raw?.ask);

  const mid =
    n(
      raw?.mid,
      Number.isFinite(bid) &&
      Number.isFinite(ask)
        ? (bid + ask) / 2
        : NaN
    );

  if (
    ![
      bid,
      ask,
      mid
    ].every(Number.isFinite) ||
    bid <= 0 ||
    ask <= 0 ||
    ask < bid
  ) {
    return null;
  }

  return {
    symbol,

    bid,
    ask,
    mid,

    spread:
      ask - bid,

    timestamp:
      raw?.timestamp ||
      new Date().toISOString()
  };
}

async function fetchSingleQuote(
  symbol
) {
  try {
    const response =
      await http.get(
        `${BIQUOTE_BASE}/api/${encodeURIComponent(symbol)}`,
        {
          params: {
            allowStale: false
          }
        }
      );

    return normalizeQuote(
      response.data?.data ||
      response.data,
      symbol
    );
  } catch (error) {
    state.lastMarketError =
      `${symbol} quote: ${safeError(error)}`;

    return null;
  }
}

async function fetchLatestQuotes(
  symbols
) {
  if (!symbols.length) {
    return new Map();
  }

  try {
    const params =
      new URLSearchParams();

    for (const symbol of symbols) {
      params.append(
        'symbols',
        symbol
      );
    }

    params.append(
      'allowStale',
      'false'
    );

    const response =
      await http.get(
        `${BIQUOTE_BASE}/api/latest?${params.toString()}`
      );

    const body =
      response.data?.data ||
      response.data;

    const output =
      new Map();

    if (Array.isArray(body)) {
      for (const row of body) {
        const symbol =
          String(
            row?.symbol || ''
          ).toUpperCase();

        const quote =
          normalizeQuote(
            row,
            symbol
          );

        if (quote) {
          output.set(
            symbol,
            quote
          );
        }
      }
    } else if (
      body &&
      typeof body === 'object'
    ) {
      for (const symbol of symbols) {
        const raw =
          body[symbol] ||
          body[symbol.toLowerCase()];

        const quote =
          normalizeQuote(
            raw,
            symbol
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
  } catch (error) {
    state.lastMarketError =
      `latest quotes: ${safeError(error)}`;

    return new Map();
  }
}

// ============================================================
// CONCURRENCY
// ============================================================

async function mapWithConcurrency(
  items,
  limit,
  worker
) {
  const results =
    new Array(items.length);

  let next = 0;

  async function run() {
    while (true) {
      const index = next++;

      if (index >= items.length) {
        break;
      }

      try {
        results[index] =
          await worker(
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
            limit,
            items.length
          )
      },
      run
    )
  );

  return results;
}

// ============================================================
// DATABASE SCHEMAS
// ============================================================

const accountSchema =
  new mongoose.Schema(
    {
      accountKey: {
        type: String,
        unique: true,
        index: true
      },

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

      telegramChatId:
        String,

      createdAt:
        Date,

      updatedAt:
        Date
    },
    {
      minimize: false
    }
  );

const tradeSchema =
  new mongoose.Schema(
    {
      version:
        String,

      accountKey: {
        type: String,
        index: true
      },

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

      timeframe:
        String,

      entryPrice:
        Number,

      stopLoss:
        Number,

      initialStopLoss:
        Number,

      takeProfit:
        Number,

      breakEvenTriggerPrice:
        Number,

      breakEvenActive:
        Boolean,

      riskDistance:
        Number,

      riskAmount:
        Number,

      maxCapitalRiskPct:
        Number,

      quantity:
        Number,

      signalPrice:
        Number,

      signalBarTime:
        String,

      openedAt:
        Date,

      closedAt:
        Date,

      exitPrice:
        Number,

      exitReason:
        String,

      pnl:
        Number,

      resultR:
        Number,

      mfeR:
        Number,

      maeR:
        Number,

      mfePrice:
        Number,

      maePrice:
        Number,

      mfeAt:
        Date,

      maeAt:
        Date,

      beActivatedAt:
        Date,

      lastMarkPrice:
        Number,

      lastMarkAt:
        Date,

      aiEntryDecision:
        mongoose.Schema.Types.Mixed,

      aiLastManagement:
        mongoose.Schema.Types.Mixed,

      technicalSnapshot:
        mongoose.Schema.Types.Mixed,

      multiTimeframeSnapshot:
        mongoose.Schema.Types.Mixed
    },
    {
      minimize: false
    }
  );

const signalSchema =
  new mongoose.Schema(
    {
      version:
        String,

      accountKey: {
        type: String,
        index: true
      },

      symbol: {
        type: String,
        index: true
      },

      direction:
        String,

      decision:
        String,

      confidence:
        Number,

      signalPrice:
        Number,

      signalBarTime:
        String,

      aiStopLoss:
        Number,

      calculatedTakeProfit:
        Number,

      createdAt:
        Date,

      executed:
        Boolean,

      skipReason:
        String,

      aiDecision:
        mongoose.Schema.Types.Mixed,

      technicalSnapshot:
        mongoose.Schema.Types.Mixed,

      multiTimeframeSnapshot:
        mongoose.Schema.Types.Mixed
    },
    {
      minimize: false
    }
  );

const journalSchema =
  new mongoose.Schema(
    {
      version:
        String,

      accountKey: {
        type: String,
        index: true
      },

      eventType: {
        type: String,
        index: true
      },

      createdAt: {
        type: Date,
        index: true
      },

      symbol:
        String,

      direction:
        String,

      tradeId:
        mongoose.Schema.Types.ObjectId,

      message:
        String,

      data:
        mongoose.Schema.Types.Mixed
    },
    {
      minimize: false
    }
  );

// ============================================================
// MODELS
// ============================================================

const Account =
  mongoose.models.LomyForexPaperAccountV14 ||
  mongoose.model(
    'LomyForexPaperAccountV14',
    accountSchema,
    'lomyforexpaperaccountsv14'
  );

const Trade =
  mongoose.models.LomyForexTradeV14 ||
  mongoose.model(
    'LomyForexTradeV14',
    tradeSchema,
    'lomyforextradesv14'
  );

const Signal =
  mongoose.models.LomyForexSignalV14 ||
  mongoose.model(
    'LomyForexSignalV14',
    signalSchema,
    'lomyforexsignalsv14'
  );

const Journal =
  mongoose.models.LomyForexJournalV14 ||
  mongoose.model(
    'LomyForexJournalV14',
    journalSchema,
    JOURNAL_COLLECTION
  );

let account = null;
let bot = null;

// ============================================================
// JOURNAL
// ============================================================

async function journal(
  eventType,
  {
    symbol = '',
    direction = '',
    tradeId = null,
    message = '',
    data = {}
  } = {}
) {
  if (!state.mongoReady) {
    return;
  }

  try {
    await Journal.create({
      version:
        VERSION,

      accountKey:
        PAPER.accountKey,

      eventType,

      createdAt:
        new Date(),

      symbol,
      direction,
      tradeId,

      message,
      data
    });

    state.journalEvents++;
  } catch (error) {
    console.error(
      'Journal:',
      safeError(error)
    );
  }
}

// ============================================================
// MONGODB INITIALIZATION
// ============================================================

async function initMongo() {
  if (!MONGODB_URI) {
    throw new Error(
      'MONGODB_URI is missing'
    );
  }

  await mongoose.connect(
    MONGODB_URI,
    {
      serverSelectionTimeoutMS:
        15000
    }
  );

  state.mongoReady = true;

  console.log(
    '✅ MongoDB connected'
  );

  account =
    await Account.findOne({
      accountKey:
        PAPER.accountKey
    });

  if (!account) {
    account =
      await Account.create({
        accountKey:
          PAPER.accountKey,

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

        telegramChatId:
          null,

        createdAt:
          new Date(),

        updatedAt:
          new Date()
      });
  }

  const openTrades =
    await Trade.find({
      accountKey:
        PAPER.accountKey,

      status:
        'OPEN'
    }).lean();

  for (const trade of openTrades) {
    trade.mfeR =
      n(
        trade.mfeR,
        0
      );

    trade.maeR =
      n(
        trade.maeR,
        0
      );

    state.openTrades.set(
      trade.symbol,
      trade
    );
  }

  console.log(
    `✅ Restored ${openTrades.length} open PAPER trade(s)`
  );

  await journal(
    'MONGO_READY',
    {
      message:
        'Mongo connected and V1.4 account restored',

      data: {
        balance:
          account.balance,

        openTrades:
          openTrades.length
      }
    }
  );
}

async function saveAccount() {
  if (!account) {
    return;
  }

  account.updatedAt =
    new Date();

  await account.save();
}

// ============================================================
// BASIC ACCOUNT / RISK HELPERS
// ============================================================

function accountBalance() {
  return n(
    account?.balance,
    PAPER.startingBalance
  );
}

function maxPerTradeCapitalRiskUsd() {
  return (
    accountBalance() *
    PAPER.maxCapitalRiskPct /
    100
  );
}

function portfolioRiskCapUsd() {
  return (
    accountBalance() *
    PAPER.portfolioRiskCapPct /
    100
  );
}

function currentPortfolioRiskUsd() {
  let total = 0;

  for (
    const trade
    of state.openTrades.values()
  ) {
    const entry =
      n(trade.entryPrice);

    const stop =
      n(trade.stopLoss);

    const initialDistance =
      n(trade.riskDistance);

    const originalRiskAmount =
      n(
        trade.riskAmount,
        0
      );

    if (
      !Number.isFinite(entry) ||
      !Number.isFinite(stop) ||
      !(initialDistance > 0) ||
      !(originalRiskAmount > 0)
    ) {
      continue;
    }

    // If SL has reached entry or crossed into profit,
    // no initial account capital remains at risk.
    let remainingDistance;

    if (trade.direction === 'BUY') {
      remainingDistance =
        Math.max(
          0,
          entry - stop
        );
    } else {
      remainingDistance =
        Math.max(
          0,
          stop - entry
        );
    }

    const fraction =
      clamp(
        remainingDistance /
        initialDistance,
        0,
        1
      );

    total +=
      originalRiskAmount *
      fraction;
  }

  return total;
}

// ============================================================
// TELEGRAM
// ============================================================

async function sendTelegram(text) {
  if (
    !bot ||
    !account?.telegramChatId
  ) {
    return;
  }

  try {
    await bot.telegram.sendMessage(
      account.telegramChatId,
      text
    );
  } catch (error) {
    console.error(
      'Telegram send:',
      safeError(error)
    );
  }
}

function pairReadyCount() {
  return [
    ...state.pairState.values()
  ].filter(
    item =>
      item.initialized
  ).length;
}

async function initTelegram() {
  if (!TELEGRAM_BOT_TOKEN) {
    console.warn(
      '⚠️ TELEGRAM_BOT_TOKEN missing'
    );

    return;
  }

  bot =
    new Telegraf(
      TELEGRAM_BOT_TOKEN
    );

  bot.start(
    async ctx => {
      account.telegramChatId =
        String(ctx.chat.id);

      await saveAccount();

      await ctx.reply(
        `✅ ${VERSION}\n` +
        `🧪 PAPER ONLY\n` +
        `Balance: ${fmtMoney(accountBalance())}\n` +
        `R:R = 1:${RULES.riskReward.toFixed(0)}\n` +
        `Capital-risk safety cap: ${PAPER.maxCapitalRiskPct}% / trade\n` +
        `Portfolio safety cap: ${PAPER.portfolioRiskCapPct}%\n` +
        `Gemini: COMMANDER`
      );
    }
  );

  bot.command(
    'status',
    async ctx => {
      await ctx.reply(
        `🤖 ${VERSION}\n` +
        `Mode: PAPER\n` +
        `LIVE: OFF\n` +
        `Market: ${state.marketReady ? 'READY' : 'WAIT'}\n` +
        `Gemini: ${state.geminiReady ? 'READY' : 'WAIT'}\n` +
        `Pairs: ${pairReadyCount()}/${INSTRUMENTS.length}\n` +
        `Open trades: ${state.openTrades.size}\n` +
        `Executed: ${state.executedSignals}\n` +
        `Skipped: ${state.skippedSignals}\n` +
        `AI BUY: ${state.aiBuyDecisions}\n` +
        `AI SELL: ${state.aiSellDecisions}\n` +
        `AI NO_TRADE: ${state.aiNoTradeDecisions}\n` +
        `AI HOLD: ${state.aiHoldDecisions}\n` +
        `AI CLOSE: ${state.aiCloseDecisions}\n` +
        `R:R: 1:${RULES.riskReward.toFixed(0)}\n` +
        `Max capital-risk safety: ${PAPER.maxCapitalRiskPct}%\n` +
        `Portfolio cap: ${PAPER.portfolioRiskCapPct}%`
      );
    }
  );

  bot.command(
    'balance',
    async ctx => {
      await ctx.reply(
        `💰 PAPER ACCOUNT\n` +
        `Balance: ${fmtMoney(accountBalance())}\n` +
        `Realized PnL: ${fmtMoney(account?.realizedPnl)}\n` +
        `Maximum capital at risk / new trade: ${fmtMoney(maxPerTradeCapitalRiskUsd())}\n` +
        `Current portfolio risk: ${fmtMoney(currentPortfolioRiskUsd())}\n` +
        `Portfolio risk cap: ${fmtMoney(portfolioRiskCapUsd())}`
      );
    }
  );

  bot.command(
    'positions',
    async ctx => {
      const positions =
        [
          ...state.openTrades.values()
        ];

      if (!positions.length) {
        await ctx.reply(
          '📭 No open PAPER trades'
        );

        return;
      }

      const text =
        positions
          .map(
            trade =>
              `${trade.symbol} ${trade.direction}\n` +
              `Entry: ${fmtPrice(trade.entryPrice, trade.symbol)}\n` +
              `SL: ${fmtPrice(trade.stopLoss, trade.symbol)}\n` +
              `TP: ${fmtPrice(trade.takeProfit, trade.symbol)}\n` +
              `R:R 1:${RULES.riskReward.toFixed(0)} | ` +
              `BE ${trade.breakEvenActive ? 'ON' : 'OFF'}\n` +
              `MFE ${n(trade.mfeR, 0).toFixed(2)}R | ` +
              `MAE ${n(trade.maeR, 0).toFixed(2)}R`
          )
          .join('\n\n');

      await ctx.reply(text);
    }
  );

  bot.command(
    'stats',
    async ctx => {
      await ctx.reply(
        `📊 STATS\n` +
        `Trades: ${account?.totalTrades || 0}\n` +
        `Wins: ${account?.wins || 0}\n` +
        `Losses: ${account?.losses || 0}\n` +
        `BE: ${account?.breakeven || 0}\n` +
        `Realized: ${fmtMoney(account?.realizedPnl || 0)}\n` +
        `Gemini entry calls: ${state.aiEntryCalls}\n` +
        `Gemini management calls: ${state.aiManageCalls}`
      );
    }
  );

  bot.command(
    'pairs',
    async ctx => {
      await ctx.reply(
        `📡 ${INSTRUMENTS.length} instruments\n` +
        INSTRUMENTS.join(', ')
      );
    }
  );

  bot.command(
    'trades',
    async ctx => {
      const rows =
        await Trade.find({
          accountKey:
            PAPER.accountKey,

          status:
            'CLOSED'
        })
          .sort({
            closedAt: -1
          })
          .limit(10)
          .lean();

      if (!rows.length) {
        await ctx.reply(
          '📭 No closed trades'
        );

        return;
      }

      await ctx.reply(
        rows
          .map(
            trade =>
              `${trade.symbol} ${trade.direction} | ` +
              `${trade.exitReason} | ` +
              `${n(trade.resultR, 0).toFixed(2)}R | ` +
              `${fmtMoney(trade.pnl)} | ` +
              `MFE ${n(trade.mfeR, 0).toFixed(2)}R | ` +
              `MAE ${n(trade.maeR, 0).toFixed(2)}R`
          )
          .join('\n')
      );
    }
  );

  await bot.telegram.getMe();

  state.telegramReady = true;

  console.log(
    '✅ Telegram authenticated'
  );

  bot.launch({
    dropPendingUpdates: true
  })
    .then(
      () => {
        console.log(
          '✅ Telegram polling started'
        );
      }
    )
    .catch(
      error => {
        console.error(
          'Telegram launch:',
          safeError(error)
        );
      }
    );
}

// ============================================================
// AI MEMORY
// ============================================================

async function getAiMemory(
  symbol = ''
) {
  try {
    const baseQuery = {
      accountKey:
        PAPER.accountKey,

      status:
        'CLOSED'
    };

    const projection = {
      symbol: 1,
      direction: 1,
      resultR: 1,
      pnl: 1,
      exitReason: 1,
      mfeR: 1,
      maeR: 1,
      openedAt: 1,
      closedAt: 1,
      aiEntryDecision: 1,
      aiLastManagement: 1
    };

    let rows = [];

    if (symbol) {
      rows =
        await Trade.find(
          {
            ...baseQuery,
            symbol
          },
          projection
        )
          .sort({
            closedAt: -1
          })
          .limit(
            AI.memoryClosedTrades
          )
          .lean();
    }

    if (
      !symbol ||
      rows.length < 8
    ) {
      rows =
        await Trade.find(
          baseQuery,
          projection
        )
          .sort({
            closedAt: -1
          })
          .limit(
            AI.memoryClosedTrades
          )
          .lean();
    }

    const wins =
      rows.filter(
        trade =>
          n(trade.resultR, 0) >
          0.10
      ).length;

    const losses =
      rows.filter(
        trade =>
          n(trade.resultR, 0) <
          -0.10
      ).length;

    const breakeven =
      rows.length -
      wins -
      losses;

    const totalR =
      rows.reduce(
        (total, trade) =>
          total +
          n(
            trade.resultR,
            0
          ),
        0
      );

    const avgR =
      rows.length
        ? totalR / rows.length
        : 0;

    const avgMfe =
      rows.length
        ? (
            rows.reduce(
              (total, trade) =>
                total +
                n(trade.mfeR, 0),
              0
            ) /
            rows.length
          )
        : 0;

    const avgMae =
      rows.length
        ? (
            rows.reduce(
              (total, trade) =>
                total +
                n(trade.maeR, 0),
              0
            ) /
            rows.length
          )
        : 0;

    return {
      count:
        rows.length,

      wins,
      losses,
      breakeven,

      winRate:
        rows.length
          ? wins / rows.length * 100
          : 0,

      totalR,
      avgR,
      avgMfe,
      avgMae,

      recent:
        rows
          .slice(0, 12)
          .map(
            trade => ({
              symbol:
                trade.symbol,

              direction:
                trade.direction,

              resultR:
                n(
                  trade.resultR,
                  0
                ),

              pnl:
                n(
                  trade.pnl,
                  0
                ),

              exitReason:
                trade.exitReason,

              mfeR:
                n(
                  trade.mfeR,
                  0
                ),

              maeR:
                n(
                  trade.maeR,
                  0
                ),

              entryReason:
                trade.aiEntryDecision?.reason ||
                '',

              setup:
                trade.aiEntryDecision?.setup ||
                '',

              managementReason:
                trade.aiLastManagement?.reason ||
                ''
            })
          )
    };
  } catch (error) {
    console.error(
      'AI memory:',
      safeError(error)
    );

    return {
      count: 0,
      wins: 0,
      losses: 0,
      breakeven: 0,
      winRate: 0,
      totalR: 0,
      avgR: 0,
      avgMfe: 0,
      avgMae: 0,
      recent: []
    };
  }
}

// ============================================================
// HIGHER TIMEFRAME TECHNICAL CONTEXT
// ============================================================

function compactTechnicalContext(
  technical
) {
  if (!technical) {
    return null;
  }

  return {
    timeframe:
      technical.timeframe,

    barTime:
      technical.barTime,

    price:
      technical.price,

    trend:
      technical.trend,

    momentum:
      technical.momentum,

    dmi:
      technical.dmi,

    volatility:
      technical.volatility,

    bollinger:
      technical.bollinger,

    keltner:
      technical.keltner,

    volume:
      technical.volume,

    obv:
      technical.obv,

    mfi:
      technical.mfi,

    vwap:
      technical.vwap,

    distanceFromVwapAtr:
      technical.distanceFromVwapAtr,

    candles:
      technical.candles,

    supportResistance:
      technical.supportResistance,

    structure:
      technical.structure,

    liquidity:
      technical.liquidity,

    fvg:
      technical.fvg,

    fibonacci:
      technical.fibonacci,

    internalBias:
      technical.internalBias
  };
}

function buildContextForBars(
  symbol,
  bars,
  timeframe
) {
  if (
    !Array.isArray(bars) ||
    bars.length < CORE_MIN_HISTORY
  ) {
    return null;
  }

  const technical =
    buildTechnicalIntelligence(
      symbol,
      bars
    );

  if (!technical) {
    return null;
  }

  technical.timeframe =
    timeframe;

  return compactTechnicalContext(
    technical
  );
}

async function buildMultiTimeframeContext(
  symbol,
  current15mBars = null
) {
  const result = {
    m15: null,
    h1: null,
    h4: null
  };

  try {
    const memory =
      state.pairState.get(symbol);

    const bars15 =
      current15mBars?.length
        ? current15mBars
        : memory?.bars || [];

    if (
      bars15.length >=
      CORE_MIN_HISTORY
    ) {
      result.m15 =
        buildContextForBars(
          symbol,
          bars15,
          '15m'
        );
    }

    const [
      bars1h,
      bars4h
    ] =
      await Promise.all([
        fetchOhlc(
          symbol,
          '1h',
          220
        ).catch(
          error => {
            console.error(
              `1H ${symbol}:`,
              safeError(error)
            );

            return [];
          }
        ),

        fetchOhlc(
          symbol,
          '4h',
          220
        ).catch(
          error => {
            console.error(
              `4H ${symbol}:`,
              safeError(error)
            );

            return [];
          }
        )
      ]);

    if (
      bars1h.length >=
      CORE_MIN_HISTORY
    ) {
      result.h1 =
        buildContextForBars(
          symbol,
          bars1h,
          '1h'
        );
    }

    if (
      bars4h.length >=
      CORE_MIN_HISTORY
    ) {
      result.h4 =
        buildContextForBars(
          symbol,
          bars4h,
          '4h'
        );
    }
  } catch (error) {
    console.error(
      `MTF ${symbol}:`,
      safeError(error)
    );
  }

  return result;
}

// ============================================================
// GEMINI RESPONSE PARSING
// ============================================================

function extractGeminiText(
  response
) {
  const candidates =
    response?.data?.candidates;

  if (
    !Array.isArray(candidates) ||
    !candidates.length
  ) {
    return '';
  }

  const parts =
    candidates[0]?.content?.parts;

  if (!Array.isArray(parts)) {
    return '';
  }

  return parts
    .map(
      part =>
        typeof part?.text === 'string'
          ? part.text
          : ''
    )
    .join('')
    .trim();
}

function parseJsonFromText(text) {
  if (!text) {
    return null;
  }

  const cleaned =
    String(text)
      .trim()
      .replace(
        /^```json/i,
        ''
      )
      .replace(
        /^```/i,
        ''
      )
      .replace(
        /```$/,
        ''
      )
      .trim();

  try {
    return JSON.parse(cleaned);
  } catch {}

  const first =
    cleaned.indexOf('{');

  const last =
    cleaned.lastIndexOf('}');

  if (
    first !== -1 &&
    last > first
  ) {
    try {
      return JSON.parse(
        cleaned.slice(
          first,
          last + 1
        )
      );
    } catch {}
  }

  return null;
}

// ============================================================
// GEMINI RATE-LIMIT QUEUE
// ============================================================

function enqueueGemini(task) {
  const execute =
    async () => {
      const elapsed =
        Date.now() -
        state.geminiLastCallAt;

      const wait =
        GEMINI_MIN_CALL_GAP_MS -
        elapsed;

      if (wait > 0) {
        await sleep(wait);
      }

      state.geminiLastCallAt =
        Date.now();

      return task();
    };

  const result =
    state.geminiQueue.then(
      execute,
      execute
    );

  // Keep queue alive even if one request fails.
  state.geminiQueue =
    result.catch(() => {});

  return result;
}

// ============================================================
// GEMINI JSON
// FAIL-CLOSED
// ============================================================

async function geminiJson(
  systemInstruction,
  payload
) {
  if (!GEMINI_API_KEY) {
    state.geminiReady = false;

    state.lastAiError =
      'GEMINI_API_KEY missing';

    return null;
  }

  return enqueueGemini(
    async () => {
      const url =
        `${GEMINI_BASE}/models/${encodeURIComponent(GEMINI_MODEL)}:generateContent`;

      try {
        const response =
          await axios.post(
            url,
            {
              system_instruction: {
                parts: [
                  {
                    text:
                      systemInstruction
                  }
                ]
              },

              contents: [
                {
                  role:
                    'user',

                  parts: [
                    {
                      text:
                        JSON.stringify(
                          payload
                        )
                    }
                  ]
                }
              ],

              generationConfig: {
                temperature:
                  AI.temperature,

                response_mime_type:
                  'application/json'
              }
            },
            {
              timeout:
                AI.timeoutMs,

              headers: {
                'Content-Type':
                  'application/json',

                'x-goog-api-key':
                  GEMINI_API_KEY
              }
            }
          );

        const text =
          extractGeminiText(
            response
          );

        const json =
          parseJsonFromText(
            text
          );

        if (
          !json ||
          typeof json !== 'object'
        ) {
          throw new Error(
            'Gemini returned invalid JSON'
          );
        }

        state.geminiReady = true;
        state.lastAiError = null;

        return json;
      } catch (error) {
        state.geminiReady = false;

        state.lastAiError =
          safeError(error);

        console.error(
          'Gemini:',
          state.lastAiError
        );

        return null;
      }
    }
  );
}

// ============================================================
// AI ENTRY COMMANDER
//
// Gemini is now the directional decision maker.
//
// It receives technical intelligence.
// It may choose:
// BUY
// SELL
// NO_TRADE
//
// For BUY/SELL it MUST propose a technical stop-loss PRICE.
//
// It does NOT choose:
// - quantity
// - account risk
// - TP
//
// TP is mechanically calculated by the bot at exactly 2R.
// ============================================================

async function aiEntryCommander(
  symbol,
  technical,
  multiTimeframe,
  quote
) {
  state.aiEntryCalls++;

  if (
    !AI.enabled ||
    !AI.entryCommanderEnabled ||
    !GEMINI_API_KEY
  ) {
    state.aiNoTradeDecisions++;

    return {
      decision:
        'NO_TRADE',

      confidence:
        0,

      stopLoss:
        null,

      reason:
        'AI_UNAVAILABLE_FAIL_CLOSED',

      setup:
        'NONE',

      invalidation:
        '',

      marketRegime:
        'UNCLEAR',

      warnings: [
        'Gemini unavailable'
      ],

      failClosed:
        true
    };
  }

  const memory =
    await getAiMemory(
      symbol
    );

  const systemInstruction = `
You are the trading commander of LOMY FOREX V1.4.

This system is PAPER ONLY.

You are responsible for the directional trading decision.

You receive:
- current bid/ask/spread
- 15-minute technical intelligence
- 1-hour technical intelligence
- 4-hour technical intelligence
- recent closed-trade memory

You must independently decide exactly one:
BUY
SELL
NO_TRADE

Do not mechanically follow the internal directional bias.
The internal bias is only one analytical input.

Analyze confluence and conflict between:
- EMA 9/21/50/100/200
- RSI
- CMO
- MACD
- Stochastic
- Williams %R
- ROC
- ADX and +DI/-DI
- ATR and volatility regime
- Bollinger Bands
- Keltner Channel
- volume ratio/spikes
- OBV
- MFI
- rolling VWAP
- support and resistance
- HH/HL/LH/LL market structure
- BOS
- liquidity sweeps
- fair value gaps
- Fibonacci retracements/extensions
- candle body/wicks/rejection/engulfing
- 15m / 1H / 4H alignment
- recent MFE/MAE and trade outcomes

QUALITY OVER FREQUENCY.
NO_TRADE is a valid and desirable decision when evidence is weak,
conflicted, spread is poor, structure is unclear, or the setup lacks
a technically defensible invalidation point.

If decision is BUY:
stopLoss MUST be a numeric PRICE below the current ask.
Place it at a technically meaningful invalidation level.

If decision is SELL:
stopLoss MUST be a numeric PRICE above the current bid.
Place it at a technically meaningful invalidation level.

The stop must be based on structure, swing, liquidity, support/resistance,
ATR or another defensible technical invalidation.

Do NOT calculate account risk.
Do NOT calculate quantity.
Do NOT choose take profit.
Do NOT change R:R.
Do NOT request wider risk.
Do NOT enable LIVE trading.
Do NOT change code or API settings.

The bot will calculate take-profit mechanically at exactly 2R from
the final validated stop-loss distance.

Return JSON only in this exact conceptual structure:

{
  "decision":"BUY|SELL|NO_TRADE",
  "confidence":0-100,
  "stopLoss":number|null,
  "reason":"concise trading rationale",
  "setup":"short setup name",
  "invalidation":"what invalidates the setup",
  "marketRegime":"TRENDING|RANGING|VOLATILE|TRANSITION|UNCLEAR",
  "trend15m":"BULL|BEAR|MIXED",
  "trend1h":"BULL|BEAR|MIXED",
  "trend4h":"BULL|BEAR|MIXED",
  "warnings":[]
}

Never output APPROVE or REJECT.
Only BUY, SELL or NO_TRADE.
`;

  const payload = {
    version:
      VERSION,

    mode:
      MODE,

    symbol,

    quote: {
      bid:
        quote.bid,

      ask:
        quote.ask,

      mid:
        quote.mid,

      spread:
        quote.spread
    },

    technical15m:
      compactTechnicalContext(
        technical
      ),

    multiTimeframe,

    tradeMemory:
      memory,

    immutableExecutionRules: {
      riskReward:
        `1:${RULES.riskReward}`,

      breakEvenTriggerR:
        RULES.breakEvenTriggerR,

      maxCapitalRiskSafetyPct:
        PAPER.maxCapitalRiskPct,

      portfolioRiskCapPct:
        PAPER.portfolioRiskCapPct,

      liveTrading:
        false,

      note:
        'Capital risk percentage is a safety cap only. TP is determined from price-distance R:R, not account percentage.'
    }
  };

  const response =
    await geminiJson(
      systemInstruction,
      payload
    );

  // ==========================================================
  // FAIL CLOSED:
  // Gemini error/malformed response = NO TRADE.
  // ==========================================================

  if (!response) {
    state.aiNoTradeDecisions++;

    return {
      decision:
        'NO_TRADE',

      confidence:
        0,

      stopLoss:
        null,

      reason:
        'GEMINI_UNAVAILABLE_FAIL_CLOSED',

      setup:
        'NONE',

      invalidation:
        '',

      marketRegime:
        'UNCLEAR',

      warnings: [
        state.lastAiError ||
        'Gemini unavailable'
      ],

      failClosed:
        true
    };
  }

  let decision =
    String(
      response.decision || ''
    )
      .trim()
      .toUpperCase();

  const confidence =
    clamp(
      n(
        response.confidence,
        0
      ),
      0,
      100
    );

  if (
    ![
      'BUY',
      'SELL',
      'NO_TRADE'
    ].includes(decision)
  ) {
    decision =
      'NO_TRADE';
  }

  // Confidence gate belongs to the safety/execution layer.
  if (
    (
      decision === 'BUY' ||
      decision === 'SELL'
    ) &&
    confidence <
    RULES.minEntryConfidence
  ) {
    decision =
      'NO_TRADE';
  }

  let stopLoss =
    n(
      response.stopLoss,
      NaN
    );

  if (decision === 'NO_TRADE') {
    stopLoss = null;
    state.aiNoTradeDecisions++;
  } else if (decision === 'BUY') {
    state.aiBuyDecisions++;
  } else if (decision === 'SELL') {
    state.aiSellDecisions++;
  }

  return {
    decision,
    confidence,
    stopLoss,

    reason:
      String(
        response.reason ||
        'No reason supplied'
      ).slice(0, 1000),

    setup:
      String(
        response.setup ||
        ''
      ).slice(0, 300),

    invalidation:
      String(
        response.invalidation ||
        ''
      ).slice(0, 600),

    marketRegime:
      String(
        response.marketRegime ||
        'UNCLEAR'
      ).toUpperCase(),

    trend15m:
      String(
        response.trend15m ||
        'MIXED'
      ).toUpperCase(),

    trend1h:
      String(
        response.trend1h ||
        'MIXED'
      ).toUpperCase(),

    trend4h:
      String(
        response.trend4h ||
        'MIXED'
      ).toUpperCase(),

    warnings:
      Array.isArray(
        response.warnings
      )
        ? response.warnings
            .slice(0, 10)
            .map(
              warning =>
                String(warning).slice(0, 300)
            )
        : [],

    failClosed:
      false
  };
}

// ============================================================
// VALIDATE GEMINI STOP
//
// AI proposes the technical stop.
// Risk Manager validates it.
//
// Risk Manager NEVER widens or invents a different AI stop.
// If invalid -> reject the trade.
// ============================================================

function validateAiStopLoss(
  decision,
  quote,
  technical
) {
  if (
    !decision ||
    ![
      'BUY',
      'SELL'
    ].includes(
      decision.decision
    )
  ) {
    return {
      valid: false,
      reason: 'NO_DIRECTION'
    };
  }

  const direction =
    decision.decision;

  const stopLoss =
    n(
      decision.stopLoss,
      NaN
    );

  if (!Number.isFinite(stopLoss)) {
    return {
      valid: false,
      reason: 'AI_SL_MISSING'
    };
  }

  const entry =
    direction === 'BUY'
      ? quote.ask
      : quote.bid;

  if (
    direction === 'BUY' &&
    !(stopLoss < entry)
  ) {
    return {
      valid: false,
      reason: 'BUY_SL_NOT_BELOW_ENTRY'
    };
  }

  if (
    direction === 'SELL' &&
    !(stopLoss > entry)
  ) {
    return {
      valid: false,
      reason: 'SELL_SL_NOT_ABOVE_ENTRY'
    };
  }

  const riskDistance =
    Math.abs(
      entry - stopLoss
    );

  const atr =
    n(
      technical?.volatility?.atr,
      NaN
    );

  if (
    !Number.isFinite(atr) ||
    !(atr > 0)
  ) {
    return {
      valid: false,
      reason: 'ATR_UNAVAILABLE'
    };
  }

  const stopAtr =
    riskDistance / atr;

  if (
    stopAtr <
    RULES.minStopAtr
  ) {
    return {
      valid: false,
      reason: 'AI_SL_TOO_TIGHT',
      stopAtr
    };
  }

  if (
    stopAtr >
    RULES.maxStopAtr
  ) {
    return {
      valid: false,
      reason: 'AI_SL_TOO_WIDE',
      stopAtr
    };
  }

  const spreadFraction =
    quote.spread /
    riskDistance;

  if (
    !Number.isFinite(spreadFraction) ||
    spreadFraction >
    RULES.maxSpreadRiskFraction
  ) {
    return {
      valid: false,
      reason: 'SPREAD_TOO_LARGE_VS_RISK',
      spreadFraction
    };
  }

  const takeProfit =
    direction === 'BUY'
      ? (
          entry +
          riskDistance *
          RULES.riskReward
        )
      : (
          entry -
          riskDistance *
          RULES.riskReward
        );

  const breakEvenTriggerPrice =
    direction === 'BUY'
      ? (
          entry +
          riskDistance *
          RULES.breakEvenTriggerR
        )
      : (
          entry -
          riskDistance *
          RULES.breakEvenTriggerR
        );

  const validOrder =
    direction === 'BUY'
      ? (
          stopLoss <
          entry &&
          entry <
          breakEvenTriggerPrice &&
          breakEvenTriggerPrice <
          takeProfit
        )
      : (
          takeProfit <
          breakEvenTriggerPrice &&
          breakEvenTriggerPrice <
          entry &&
          entry <
          stopLoss
        );

  if (!validOrder) {
    return {
      valid: false,
      reason: 'INVALID_PRICE_ORDER'
    };
  }

  return {
    valid: true,

    direction,

    entry,

    stopLoss,

    riskDistance,

    stopAtr,

    spreadFraction,

    takeProfit,

    breakEvenTriggerPrice
  };
}

// ============================================================
// SIGNAL / AI DECISION RECORD
// ============================================================

async function recordSignal(
  {
    symbol,
    technical,
    multiTimeframe,
    aiDecision,
    executionLevels = null,
    executed = false,
    skipReason = ''
  }
) {
  const direction =
    aiDecision?.decision || 'NO_TRADE';

  const signalPrice =
    n(
      technical?.price?.close,
      NaN
    );

  const signalBarTime =
    String(
      technical?.barTime ||
      ''
    );

  if (state.mongoReady) {
    try {
      await Signal.create({
        version:
          VERSION,

        accountKey:
          PAPER.accountKey,

        symbol,

        direction:
          direction === 'NO_TRADE'
            ? ''
            : direction,

        decision:
          direction,

        confidence:
          n(
            aiDecision?.confidence,
            0
          ),

        signalPrice,

        signalBarTime,

        aiStopLoss:
          Number.isFinite(
            n(
              aiDecision?.stopLoss,
              NaN
            )
          )
            ? n(aiDecision.stopLoss)
            : null,

        calculatedTakeProfit:
          executionLevels?.takeProfit ??
          null,

        createdAt:
          new Date(),

        executed,

        skipReason,

        aiDecision,

        technicalSnapshot:
          technical,

        multiTimeframeSnapshot:
          multiTimeframe
      });
    } catch (error) {
      console.error(
        'Signal record:',
        safeError(error)
      );
    }
  }

  await journal(
    executed
      ? 'AI_TRADE_EXECUTED'
      : 'AI_DECISION',
    {
      symbol,

      direction:
        direction === 'NO_TRADE'
          ? ''
          : direction,

      message:
        executed
          ? 'EXECUTED'
          : skipReason ||
            direction,

      data: {
        aiDecision,
        executionLevels,
        technical,
        multiTimeframe,
        skipReason
      }
    }
  );
}

// ============================================================
// AI OPEN-TRADE MANAGEMENT
// ============================================================

async function aiManagementDecision(
  trade,
  quote
) {
  if (
    !AI.enabled ||
    !AI.managementEnabled ||
    !GEMINI_API_KEY
  ) {
    // Mechanical SL/TP/BE remains active.
    return {
      decision:
        'HOLD',

      confidence:
        0,

      reason:
        'AI_UNAVAILABLE_MECHANICAL_PROTECTION_ACTIVE',

      setupInvalidated:
        false,

      failClosed:
        true
    };
  }

  state.aiManageCalls++;

  const [
    multiTimeframe,
    memory
  ] =
    await Promise.all([
      buildMultiTimeframeContext(
        trade.symbol
      ),

      getAiMemory(
        trade.symbol
      )
    ]);

  const r =
    currentR(
      trade,
      quote
    );

  const systemInstruction = `
You manage an already-open PAPER forex trade for LOMY FOREX V1.4.

You may choose only:
HOLD
CLOSE

You may NOT:
- move stop loss
- widen risk
- move take profit
- change R:R
- change quantity
- add to the position
- reverse the position
- open another trade
- enable live trading
- modify account settings

The mechanical Risk Manager always retains control of:
- original SL
- exact 1:2 TP
- +0.60R break-even
- position size
- capital-risk safety limits

Use:
- current R
- MFE / MAE
- original Gemini setup and rationale
- 15m / 1H / 4H technical intelligence
- structure
- momentum
- volatility
- liquidity
- recent trading memory

CLOSE is a discretionary risk-reducing action.
Use CLOSE when the original thesis has materially failed,
a strong structural reversal has developed,
or the market now presents a materially adverse condition.

Do not close a trade merely because of normal noise or a small drawdown.

Return JSON only:

{
  "decision":"HOLD|CLOSE",
  "confidence":0-100,
  "reason":"concise explanation",
  "setupInvalidated":true|false,
  "warnings":[]
}
`;

  const payload = {
    version:
      VERSION,

    symbol:
      trade.symbol,

    direction:
      trade.direction,

    currentTrade: {
      entryPrice:
        trade.entryPrice,

      stopLoss:
        trade.stopLoss,

      initialStopLoss:
        trade.initialStopLoss,

      takeProfit:
        trade.takeProfit,

      currentR:
        r,

      breakEvenActive:
        Boolean(
          trade.breakEvenActive
        ),

      mfeR:
        n(
          trade.mfeR,
          0
        ),

      maeR:
        n(
          trade.maeR,
          0
        ),

      originalAiDecision:
        trade.aiEntryDecision ||
        null
    },

    quote: {
      bid:
        quote.bid,

      ask:
        quote.ask,

      mid:
        quote.mid,

      spread:
        quote.spread
    },

    multiTimeframe,

    memory,

    immutableRules: {
      riskReward:
        '1:2',

      breakEvenTriggerR:
        RULES.breakEvenTriggerR,

      maxCapitalRiskSafetyPct:
        PAPER.maxCapitalRiskPct,

      liveTrading:
        false
    }
  };

  const response =
    await geminiJson(
      systemInstruction,
      payload
    );

  // AI failure must NEVER disable hard trade protection.
  // HOLD means mechanical SL/TP/BE continue normally.
  if (!response) {
    state.aiHoldDecisions++;

    return {
      decision:
        'HOLD',

      confidence:
        0,

      reason:
        'GEMINI_UNAVAILABLE_MECHANICAL_PROTECTION_ACTIVE',

      setupInvalidated:
        false,

      warnings: [
        state.lastAiError ||
        'Gemini unavailable'
      ],

      failClosed:
        true
    };
  }

  let decision =
    String(
      response.decision ||
      ''
    )
      .trim()
      .toUpperCase();

  const confidence =
    clamp(
      n(
        response.confidence,
        0
      ),
      0,
      100
    );

  if (
    ![
      'HOLD',
      'CLOSE'
    ].includes(decision)
  ) {
    decision =
      'HOLD';
  }

  if (
    decision === 'CLOSE' &&
    confidence <
    RULES.minCloseConfidence
  ) {
    decision =
      'HOLD';
  }

  if (decision === 'CLOSE') {
    state.aiCloseDecisions++;
  } else {
    state.aiHoldDecisions++;
  }

  return {
    decision,
    confidence,

    reason:
      String(
        response.reason ||
        'No reason supplied'
      ).slice(0, 1000),

    setupInvalidated:
      response.setupInvalidated === true,

    warnings:
      Array.isArray(
        response.warnings
      )
        ? response.warnings
            .slice(0, 10)
            .map(
              warning =>
                String(warning).slice(0, 300)
            )
        : [],

    failClosed:
      false
  };
}

// ============================================================
// END PART 2 / 4
//
// PART 3 STARTS WITH:
// PAPER EXECUTION
// MFE / MAE
// BREAK EVEN
// SL / TP
// AI TRADE MANAGEMENT
// 15M GEMINI SCANNER
// ============================================================
// ============================================================
// PART 3 / 4
// PAPER EXECUTION + TRADE MANAGEMENT + GEMINI SCANNER
// ============================================================

// ============================================================
// PAPER POSITION SIZING
//
// IMPORTANT:
// 1% is ONLY the maximum account-capital risk safety cap.
// It is NOT the profit target.
//
// TP is based strictly on PRICE DISTANCE:
// R:R = 1:2
// ============================================================

function calculatePositionSize(
  entryPrice,
  stopLoss
) {
  const riskDistance =
    Math.abs(
      entryPrice - stopLoss
    );

  if (
    !Number.isFinite(riskDistance) ||
    riskDistance <= 0
  ) {
    return null;
  }

  const riskAmount =
    maxPerTradeCapitalRiskUsd();

  if (
    !Number.isFinite(riskAmount) ||
    riskAmount <= 0
  ) {
    return null;
  }

  const quantity =
    riskAmount /
    riskDistance;

  if (
    !Number.isFinite(quantity) ||
    quantity <= 0
  ) {
    return null;
  }

  return {
    riskDistance,
    riskAmount,
    quantity
  };
}

// ============================================================
// CHECK WHETHER NEW TRADE IS ALLOWED
// ============================================================

function canOpenNewTrade(
  symbol,
  proposedRiskAmount
) {
  if (MODE !== 'PAPER') {
    return {
      allowed: false,
      reason: 'PAPER_ONLY'
    };
  }

  if (LIVE_TRADING) {
    return {
      allowed: false,
      reason: 'LIVE_TRADING_FORBIDDEN'
    };
  }

  if (
    state.openTrades.has(symbol)
  ) {
    return {
      allowed: false,
      reason: 'SYMBOL_ALREADY_OPEN'
    };
  }

  if (
    state.openTrades.size >=
    PAPER.maxOpenTrades
  ) {
    return {
      allowed: false,
      reason: 'MAX_OPEN_TRADES'
    };
  }

  const currentRisk =
    currentPortfolioRiskUsd();

  const cap =
    portfolioRiskCapUsd();

  if (
    currentRisk +
    proposedRiskAmount >
    cap + 1e-9
  ) {
    return {
      allowed: false,
      reason: 'PORTFOLIO_RISK_CAP',
      currentRisk,
      proposedRiskAmount,
      cap
    };
  }

  return {
    allowed: true,
    currentRisk,
    proposedRiskAmount,
    cap
  };
}

// ============================================================
// OPEN PAPER TRADE
// ============================================================

async function openPaperTrade({
  symbol,
  technical,
  multiTimeframe,
  quote,
  aiDecision,
  executionLevels
}) {
  const {
    direction,
    entry,
    stopLoss,
    riskDistance,
    takeProfit,
    breakEvenTriggerPrice
  } = executionLevels;

  const sizing =
    calculatePositionSize(
      entry,
      stopLoss
    );

  if (!sizing) {
    return {
      opened: false,
      reason: 'POSITION_SIZE_FAILED'
    };
  }

  const riskCheck =
    canOpenNewTrade(
      symbol,
      sizing.riskAmount
    );

  if (!riskCheck.allowed) {
    return {
      opened: false,
      reason: riskCheck.reason
    };
  }

  const tradeData = {
    version:
      VERSION,

    accountKey:
      PAPER.accountKey,

    symbol,

    direction,

    status:
      'OPEN',

    timeframe:
      TIMEFRAME,

    entryPrice:
      entry,

    stopLoss,

    initialStopLoss:
      stopLoss,

    takeProfit,

    breakEvenTriggerPrice,

    breakEvenActive:
      false,

    riskDistance,

    riskAmount:
      sizing.riskAmount,

    maxCapitalRiskPct:
      PAPER.maxCapitalRiskPct,

    quantity:
      sizing.quantity,

    signalPrice:
      technical.price.close,

    signalBarTime:
      String(
        technical.barTime || ''
      ),

    openedAt:
      new Date(),

    closedAt:
      null,

    exitPrice:
      null,

    exitReason:
      '',

    pnl:
      0,

    resultR:
      0,

    mfeR:
      0,

    maeR:
      0,

    mfePrice:
      entry,

    maePrice:
      entry,

    mfeAt:
      new Date(),

    maeAt:
      new Date(),

    beActivatedAt:
      null,

    lastMarkPrice:
      entry,

    lastMarkAt:
      new Date(),

    aiEntryDecision:
      aiDecision,

    aiLastManagement:
      null,

    technicalSnapshot:
      technical,

    multiTimeframeSnapshot:
      multiTimeframe
  };

  let trade;

  if (state.mongoReady) {
    trade =
      await Trade.create(
        tradeData
      );

    trade =
      trade.toObject();
  } else {
    trade = {
      ...tradeData,

      _id:
        `paper_${Date.now()}_${symbol}`
    };
  }

  state.openTrades.set(
    symbol,
    trade
  );

  state.executedSignals++;

  await journal(
    'TRADE_OPEN',
    {
      symbol,
      direction,

      tradeId:
        trade._id,

      message:
        `${direction} PAPER trade opened`,

      data: {
        entry,
        stopLoss,
        takeProfit,
        breakEvenTriggerPrice,

        riskReward:
          `1:${RULES.riskReward}`,

        riskDistance,

        riskAmount:
          sizing.riskAmount,

        maxCapitalRiskPct:
          PAPER.maxCapitalRiskPct,

        quantity:
          sizing.quantity,

        aiDecision
      }
    }
  );

  await sendTelegram(
    `🚀 PAPER TRADE OPENED\n\n` +
    `${symbol} ${direction}\n` +
    `Entry: ${fmtPrice(entry, symbol)}\n` +
    `SL: ${fmtPrice(stopLoss, symbol)}\n` +
    `TP: ${fmtPrice(takeProfit, symbol)}\n` +
    `BE trigger: ${fmtPrice(breakEvenTriggerPrice, symbol)} (+${RULES.breakEvenTriggerR.toFixed(2)}R)\n\n` +
    `R:R = 1:${RULES.riskReward.toFixed(0)}\n` +
    `Capital-risk safety cap: ${PAPER.maxCapitalRiskPct}%\n` +
    `Risk budget: ${fmtMoney(sizing.riskAmount)}\n` +
    `Gemini confidence: ${n(aiDecision.confidence, 0).toFixed(0)}%\n` +
    `Setup: ${aiDecision.setup || 'N/A'}\n` +
    `Reason: ${aiDecision.reason}`
  );

  console.log(
    `🚀 ${symbol} ${direction} | ` +
    `Entry=${fmtPrice(entry, symbol)} | ` +
    `SL=${fmtPrice(stopLoss, symbol)} | ` +
    `TP=${fmtPrice(takeProfit, symbol)} | ` +
    `RR=1:${RULES.riskReward}`
  );

  return {
    opened: true,
    trade
  };
}

// ============================================================
// CURRENT EXIT/MARK PRICE
//
// BUY:
// entry = ASK
// current liquidation/exit = BID
//
// SELL:
// entry = BID
// current liquidation/exit = ASK
//
// This keeps PAPER execution spread-aware.
// ============================================================

function tradeMarkPrice(
  trade,
  quote
) {
  if (trade.direction === 'BUY') {
    return quote.bid;
  }

  return quote.ask;
}

// ============================================================
// CURRENT R
// ============================================================

function currentR(
  trade,
  quote
) {
  const mark =
    tradeMarkPrice(
      trade,
      quote
    );

  const entry =
    n(trade.entryPrice);

  const riskDistance =
    n(trade.riskDistance);

  if (
    !Number.isFinite(mark) ||
    !Number.isFinite(entry) ||
    !Number.isFinite(riskDistance) ||
    riskDistance <= 0
  ) {
    return 0;
  }

  if (trade.direction === 'BUY') {
    return (
      mark - entry
    ) / riskDistance;
  }

  return (
    entry - mark
  ) / riskDistance;
}

// ============================================================
// UPDATE MFE / MAE
// ============================================================

async function updateTradeExcursions(
  trade,
  quote
) {
  const mark =
    tradeMarkPrice(
      trade,
      quote
    );

  const r =
    currentR(
      trade,
      quote
    );

  let changed = false;

  trade.lastMarkPrice =
    mark;

  trade.lastMarkAt =
    new Date();

  if (
    r >
    n(
      trade.mfeR,
      0
    )
  ) {
    trade.mfeR =
      r;

    trade.mfePrice =
      mark;

    trade.mfeAt =
      new Date();

    changed = true;
  }

  if (
    r <
    n(
      trade.maeR,
      0
    )
  ) {
    trade.maeR =
      r;

    trade.maePrice =
      mark;

    trade.maeAt =
      new Date();

    changed = true;
  }

  if (
    state.mongoReady &&
    trade._id
  ) {
    const update = {
      lastMarkPrice:
        trade.lastMarkPrice,

      lastMarkAt:
        trade.lastMarkAt
    };

    if (changed) {
      update.mfeR =
        trade.mfeR;

      update.maeR =
        trade.maeR;

      update.mfePrice =
        trade.mfePrice;

      update.maePrice =
        trade.maePrice;

      update.mfeAt =
        trade.mfeAt;

      update.maeAt =
        trade.maeAt;
    }

    try {
      await Trade.updateOne(
        {
          _id:
            trade._id
        },
        {
          $set:
            update
        }
      );
    } catch (error) {
      console.error(
        `Excursion save ${trade.symbol}:`,
        safeError(error)
      );
    }
  }
}

// ============================================================
// BREAK EVEN
// ============================================================

async function activateBreakEven(
  trade
) {
  if (
    trade.breakEvenActive
  ) {
    return;
  }

  trade.breakEvenActive =
    true;

  // Mechanical SL move to exact entry.
  // Gemini cannot override this.
  trade.stopLoss =
    trade.entryPrice;

  trade.beActivatedAt =
    new Date();

  if (
    state.mongoReady &&
    trade._id
  ) {
    await Trade.updateOne(
      {
        _id:
          trade._id
      },
      {
        $set: {
          breakEvenActive:
            true,

          stopLoss:
            trade.entryPrice,

          beActivatedAt:
            trade.beActivatedAt
        }
      }
    );
  }

  await journal(
    'BREAK_EVEN_ACTIVATED',
    {
      symbol:
        trade.symbol,

      direction:
        trade.direction,

      tradeId:
        trade._id,

      message:
        `Break even activated at +${RULES.breakEvenTriggerR.toFixed(2)}R`,

      data: {
        entryPrice:
          trade.entryPrice,

        newStopLoss:
          trade.entryPrice
      }
    }
  );

  await sendTelegram(
    `🛡️ BREAK EVEN ACTIVATED\n\n` +
    `${trade.symbol} ${trade.direction}\n` +
    `Trigger: +${RULES.breakEvenTriggerR.toFixed(2)}R\n` +
    `SL moved to entry: ${fmtPrice(trade.entryPrice, trade.symbol)}`
  );

  console.log(
    `🛡️ ${trade.symbol} BE ACTIVE`
  );
}

// ============================================================
// PAPER PNL
// ============================================================

function calculateTradePnl(
  trade,
  exitPrice
) {
  const entry =
    n(trade.entryPrice);

  const quantity =
    n(trade.quantity);

  if (
    !Number.isFinite(entry) ||
    !Number.isFinite(exitPrice) ||
    !Number.isFinite(quantity)
  ) {
    return 0;
  }

  const move =
    trade.direction === 'BUY'
      ? exitPrice - entry
      : entry - exitPrice;

  return (
    move *
    quantity
  );
}

function calculateResultR(
  trade,
  exitPrice
) {
  const riskDistance =
    n(trade.riskDistance);

  if (
    !Number.isFinite(riskDistance) ||
    riskDistance <= 0
  ) {
    return 0;
  }

  if (trade.direction === 'BUY') {
    return (
      exitPrice -
      trade.entryPrice
    ) / riskDistance;
  }

  return (
    trade.entryPrice -
    exitPrice
  ) / riskDistance;
}

// ============================================================
// CLOSE PAPER TRADE
// ============================================================

async function closePaperTrade(
  trade,
  exitPrice,
  reason,
  extra = {}
) {
  if (!trade) {
    return null;
  }

  const liveTrade =
    state.openTrades.get(
      trade.symbol
    );

  if (!liveTrade) {
    return null;
  }

  exitPrice =
    n(
      exitPrice,
      NaN
    );

  if (
    !Number.isFinite(exitPrice) ||
    exitPrice <= 0
  ) {
    return null;
  }

  const pnl =
    calculateTradePnl(
      liveTrade,
      exitPrice
    );

  const resultR =
    calculateResultR(
      liveTrade,
      exitPrice
    );

  const closedAt =
    new Date();

  liveTrade.status =
    'CLOSED';

  liveTrade.closedAt =
    closedAt;

  liveTrade.exitPrice =
    exitPrice;

  liveTrade.exitReason =
    reason;

  liveTrade.pnl =
    pnl;

  liveTrade.resultR =
    resultR;

  if (
    extra.aiManagement
  ) {
    liveTrade.aiLastManagement =
      extra.aiManagement;
  }

  state.openTrades.delete(
    liveTrade.symbol
  );

  if (account) {
    account.balance =
      accountBalance() +
      pnl;

    account.realizedPnl =
      n(
        account.realizedPnl,
        0
      ) +
      pnl;

    account.totalTrades =
      n(
        account.totalTrades,
        0
      ) + 1;

    if (resultR > 0.10) {
      account.wins =
        n(
          account.wins,
          0
        ) + 1;
    } else if (resultR < -0.10) {
      account.losses =
        n(
          account.losses,
          0
        ) + 1;
    } else {
      account.breakeven =
        n(
          account.breakeven,
          0
        ) + 1;
    }

    await saveAccount();
  }

  if (
    state.mongoReady &&
    liveTrade._id
  ) {
    try {
      await Trade.updateOne(
        {
          _id:
            liveTrade._id
        },
        {
          $set: {
            status:
              'CLOSED',

            closedAt,

            exitPrice,

            exitReason:
              reason,

            pnl,

            resultR,

            mfeR:
              liveTrade.mfeR,

            maeR:
              liveTrade.maeR,

            mfePrice:
              liveTrade.mfePrice,

            maePrice:
              liveTrade.maePrice,

            aiLastManagement:
              liveTrade.aiLastManagement,

            lastMarkPrice:
              exitPrice,

            lastMarkAt:
              closedAt
          }
        }
      );
    } catch (error) {
      console.error(
        `Trade close DB ${liveTrade.symbol}:`,
        safeError(error)
      );
    }
  }

  await journal(
    'TRADE_CLOSE',
    {
      symbol:
        liveTrade.symbol,

      direction:
        liveTrade.direction,

      tradeId:
        liveTrade._id,

      message:
        reason,

      data: {
        exitPrice,
        pnl,
        resultR,

        mfeR:
          liveTrade.mfeR,

        maeR:
          liveTrade.maeR,

        breakEvenActive:
          liveTrade.breakEvenActive,

        aiManagement:
          extra.aiManagement ||
          null
      }
    }
  );

  const icon =
    resultR > 0.10
      ? '✅'
      : resultR < -0.10
        ? '❌'
        : '➖';

  await sendTelegram(
    `${icon} PAPER TRADE CLOSED\n\n` +
    `${liveTrade.symbol} ${liveTrade.direction}\n` +
    `Reason: ${reason}\n` +
    `Exit: ${fmtPrice(exitPrice, liveTrade.symbol)}\n` +
    `Result: ${resultR.toFixed(2)}R\n` +
    `PnL: ${fmtMoney(pnl)}\n` +
    `MFE: ${n(liveTrade.mfeR, 0).toFixed(2)}R\n` +
    `MAE: ${n(liveTrade.maeR, 0).toFixed(2)}R\n` +
    `Balance: ${fmtMoney(accountBalance())}`
  );

  console.log(
    `${icon} ${liveTrade.symbol} CLOSED | ` +
    `${reason} | ` +
    `${resultR.toFixed(2)}R | ` +
    `${fmtMoney(pnl)}`
  );

  return {
    ...liveTrade,
    pnl,
    resultR,
    exitPrice,
    exitReason:
      reason
  };
}

// ============================================================
// HARD MECHANICAL TRADE PROTECTION
//
// Priority:
// 1. Stop Loss
// 2. Take Profit
// 3. Break Even activation
//
// This runs independently from Gemini.
// ============================================================

async function applyMechanicalProtection(
  trade,
  quote
) {
  const mark =
    tradeMarkPrice(
      trade,
      quote
    );

  if (
    !Number.isFinite(mark)
  ) {
    return {
      closed: false
    };
  }

  // ==========================================================
  // BUY
  // ==========================================================

  if (trade.direction === 'BUY') {
    if (
      mark <=
      n(trade.stopLoss)
    ) {
      const reason =
        trade.breakEvenActive
          ? 'BREAK_EVEN'
          : 'STOP_LOSS';

      await closePaperTrade(
        trade,
        mark,
        reason
      );

      return {
        closed: true,
        reason
      };
    }

    if (
      mark >=
      n(trade.takeProfit)
    ) {
      await closePaperTrade(
        trade,
        mark,
        'TAKE_PROFIT'
      );

      return {
        closed: true,
        reason:
          'TAKE_PROFIT'
      };
    }

    if (
      !trade.breakEvenActive &&
      mark >=
      n(
        trade.breakEvenTriggerPrice
      )
    ) {
      await activateBreakEven(
        trade
      );
    }
  }

  // ==========================================================
  // SELL
  // ==========================================================

  if (trade.direction === 'SELL') {
    if (
      mark >=
      n(trade.stopLoss)
    ) {
      const reason =
        trade.breakEvenActive
          ? 'BREAK_EVEN'
          : 'STOP_LOSS';

      await closePaperTrade(
        trade,
        mark,
        reason
      );

      return {
        closed: true,
        reason
      };
    }

    if (
      mark <=
      n(trade.takeProfit)
    ) {
      await closePaperTrade(
        trade,
        mark,
        'TAKE_PROFIT'
      );

      return {
        closed: true,
        reason:
          'TAKE_PROFIT'
      };
    }

    if (
      !trade.breakEvenActive &&
      mark <=
      n(
        trade.breakEvenTriggerPrice
      )
    ) {
      await activateBreakEven(
        trade
      );
    }
  }

  return {
    closed: false
  };
}

// ============================================================
// AI MANAGEMENT SCHEDULER
// ============================================================

function shouldRunAiManagement(
  trade
) {
  const last =
    state.lastManageAt.get(
      trade.symbol
    ) || 0;

  if (
    Date.now() -
    last <
    AI.managementIntervalMs
  ) {
    return false;
  }

  if (
    state.managementLocks.has(
      trade.symbol
    )
  ) {
    return false;
  }

  return true;
}

// ============================================================
// RUN GEMINI MANAGEMENT
// ============================================================

async function manageTradeWithAi(
  trade,
  quote
) {
  if (
    !shouldRunAiManagement(
      trade
    )
  ) {
    return;
  }

  state.managementLocks.add(
    trade.symbol
  );

  state.lastManageAt.set(
    trade.symbol,
    Date.now()
  );

  try {
    // Trade may have been mechanically closed
    // while waiting in AI queue.
    if (
      !state.openTrades.has(
        trade.symbol
      )
    ) {
      return;
    }

    const decision =
      await aiManagementDecision(
        trade,
        quote
      );

    trade.aiLastManagement =
      decision;

    if (
      state.mongoReady &&
      trade._id
    ) {
      await Trade.updateOne(
        {
          _id:
            trade._id
        },
        {
          $set: {
            aiLastManagement:
              decision
          }
        }
      );
    }

    await journal(
      'AI_MANAGEMENT',
      {
        symbol:
          trade.symbol,

        direction:
          trade.direction,

        tradeId:
          trade._id,

        message:
          decision.decision,

        data:
          decision
      }
    );

    if (
      decision.decision !==
      'CLOSE'
    ) {
      return;
    }

    // Recheck that position still exists.
    if (
      !state.openTrades.has(
        trade.symbol
      )
    ) {
      return;
    }

    const freshQuote =
      await fetchSingleQuote(
        trade.symbol
      );

    if (!freshQuote) {
      return;
    }

    // Mechanical protection ALWAYS gets priority.
    const mechanical =
      await applyMechanicalProtection(
        trade,
        freshQuote
      );

    if (mechanical.closed) {
      return;
    }

    const exitPrice =
      tradeMarkPrice(
        trade,
        freshQuote
      );

    await closePaperTrade(
      trade,
      exitPrice,
      'AI_CLOSE',
      {
        aiManagement:
          decision
      }
    );
  } catch (error) {
    console.error(
      `AI manage ${trade.symbol}:`,
      safeError(error)
    );

    await journal(
      'AI_MANAGEMENT_ERROR',
      {
        symbol:
          trade.symbol,

        direction:
          trade.direction,

        tradeId:
          trade._id,

        message:
          safeError(error)
      }
    );
  } finally {
    state.managementLocks.delete(
      trade.symbol
    );
  }
}

// ============================================================
// OPEN TRADE QUOTE LOOP
// ============================================================

async function quoteLoop() {
  if (state.quoteLoopBusy) {
    return;
  }

  state.quoteLoopBusy = true;

  try {
    const symbols =
      [
        ...state.openTrades.keys()
      ];

    if (!symbols.length) {
      return;
    }

    let quoteMap =
      await fetchLatestQuotes(
        symbols
      );

    // Fallback individually if batch endpoint
    // did not return a symbol.
    const missing =
      symbols.filter(
        symbol =>
          !quoteMap.has(symbol)
      );

    if (missing.length) {
      const fallback =
        await mapWithConcurrency(
          missing,
          4,
          async symbol => ({
            symbol,
            quote:
              await fetchSingleQuote(
                symbol
              )
          })
        );

      for (const item of fallback) {
        if (
          item?.quote &&
          item?.symbol
        ) {
          quoteMap.set(
            item.symbol,
            item.quote
          );
        }
      }
    }

    for (const symbol of symbols) {
      const trade =
        state.openTrades.get(
          symbol
        );

      const quote =
        quoteMap.get(
          symbol
        );

      if (
        !trade ||
        !quote
      ) {
        continue;
      }

      state.latestQuotes.set(
        symbol,
        quote
      );

      await updateTradeExcursions(
        trade,
        quote
      );

      // Hard protection first.
      const protection =
        await applyMechanicalProtection(
          trade,
          quote
        );

      if (protection.closed) {
        continue;
      }

      // Gemini management second.
      // Do not await here sequentially for all trades.
      manageTradeWithAi(
        trade,
        quote
      ).catch(
        error => {
          console.error(
            `Manage background ${symbol}:`,
            safeError(error)
          );
        }
      );
    }
  } catch (error) {
    console.error(
      'Quote loop:',
      safeError(error)
    );
  } finally {
    state.quoteLoopBusy = false;
  }
}

// ============================================================
// INITIALIZE ONE SYMBOL
// ============================================================

async function initializeSymbol(
  symbol
) {
  try {
    const bars =
      await fetchOhlc(
        symbol,
        TIMEFRAME,
        HISTORY_LIMIT
      );

    if (
      bars.length <
      CORE_MIN_HISTORY
    ) {
      throw new Error(
        `Only ${bars.length} bars`
      );
    }

    const latest =
      bars[bars.length - 1];

    const pair = {
      symbol,

      initialized:
        true,

      bars,

      lastClosedBarTime:
        latest.time,

      lastScannedBarTime:
        null,

      lastError:
        null,

      initializedAt:
        new Date()
    };

    state.pairState.set(
      symbol,
      pair
    );

    console.log(
      `✅ ${symbol} | ` +
      `bars=${bars.length} | ` +
      `EMA200=${bars.length >= EMA200_CONTEXT_HISTORY ? 'READY' : 'CONTEXT-WARMUP'}`
    );

    return pair;
  } catch (error) {
    state.pairState.set(
      symbol,
      {
        symbol,

        initialized:
          false,

        bars: [],

        lastClosedBarTime:
          null,

        lastScannedBarTime:
          null,

        lastError:
          safeError(error),

        initializedAt:
          null
      }
    );

    console.error(
      `❌ Init ${symbol}:`,
      safeError(error)
    );

    return null;
  }
}

// ============================================================
// INITIALIZE MARKET
// ============================================================

async function initializeMarket() {
  console.log(
    `📡 Initializing ${INSTRUMENTS.length} instruments...`
  );

  await mapWithConcurrency(
    INSTRUMENTS,
    OHLC_CONCURRENCY,
    initializeSymbol
  );

  const ready =
    pairReadyCount();

  state.marketReady =
    ready > 0;

  console.log(
    `📡 Market initialized: ${ready}/${INSTRUMENTS.length}`
  );

  if (!ready) {
    throw new Error(
      'No instruments initialized'
    );
  }

  await journal(
    'MARKET_READY',
    {
      message:
        `${ready}/${INSTRUMENTS.length} instruments initialized`
    }
  );
}

// ============================================================
// PROCESS NEW CLOSED 15M BAR
//
// IMPORTANT:
// Gemini is the trading commander.
// There is NO legacy directional gate here.
//
// Technical engine -> Gemini -> Risk Manager -> PAPER trade.
// ============================================================

async function processNewClosedBar(
  symbol,
  bars,
  quote
) {
  if (
    state.scanLocks.has(symbol)
  ) {
    return;
  }

  state.scanLocks.add(
    symbol
  );

  try {
    if (
      !Array.isArray(bars) ||
      bars.length <
      CORE_MIN_HISTORY
    ) {
      return;
    }

    const latestBar =
      bars[bars.length - 1];

    const barKey =
      `${symbol}:${latestBar.time}`;

    if (
      state.processedBars.has(
        barKey
      )
    ) {
      return;
    }

    state.processedBars.add(
      barKey
    );

    // Prevent unlimited Set growth.
    if (
      state.processedBars.size >
      10000
    ) {
      state.processedBars.clear();

      state.processedBars.add(
        barKey
      );
    }

    const technical =
      buildTechnicalIntelligence(
        symbol,
        bars
      );

    if (!technical) {
      return;
    }

    technical.timeframe =
      TIMEFRAME;

    state.scannedBars++;

    // One open position per symbol.
    if (
      state.openTrades.has(
        symbol
      )
    ) {
      state.skippedSignals++;

      await journal(
        'SCAN_SKIP',
        {
          symbol,

          message:
            'SYMBOL_ALREADY_OPEN'
        }
      );

      return;
    }

    if (!quote) {
      quote =
        await fetchSingleQuote(
          symbol
        );
    }

    if (!quote) {
      state.skippedSignals++;

      await journal(
        'SCAN_SKIP',
        {
          symbol,

          message:
            'QUOTE_UNAVAILABLE'
        }
      );

      return;
    }

    state.latestQuotes.set(
      symbol,
      quote
    );

    const multiTimeframe =
      await buildMultiTimeframeContext(
        symbol,
        bars
      );

    // ========================================================
    // GEMINI COMMANDS DIRECTION
    // ========================================================

    const aiDecision =
      await aiEntryCommander(
        symbol,
        technical,
        multiTimeframe,
        quote
      );

    if (
      aiDecision.decision ===
      'NO_TRADE'
    ) {
      state.skippedSignals++;

      await recordSignal({
        symbol,
        technical,
        multiTimeframe,
        aiDecision,
        executed: false,

        skipReason:
          aiDecision.failClosed
            ? 'AI_FAIL_CLOSED'
            : 'AI_NO_TRADE'
      });

      console.log(
        `⏭️ ${symbol} | ` +
        `AI=NO_TRADE | ` +
        `${n(aiDecision.confidence, 0).toFixed(0)}% | ` +
        `${aiDecision.reason}`
      );

      return;
    }

    // ========================================================
    // RISK MANAGER VALIDATES AI STOP
    // ========================================================

    const executionLevels =
      validateAiStopLoss(
        aiDecision,
        quote,
        technical
      );

    if (!executionLevels.valid) {
      state.skippedSignals++;

      await recordSignal({
        symbol,
        technical,
        multiTimeframe,
        aiDecision,
        executionLevels: null,
        executed: false,

        skipReason:
          executionLevels.reason
      });

      console.log(
        `🛑 ${symbol} ${aiDecision.decision} rejected | ` +
        `${executionLevels.reason}`
      );

      return;
    }

    const sizing =
      calculatePositionSize(
        executionLevels.entry,
        executionLevels.stopLoss
      );

    if (!sizing) {
      state.skippedSignals++;

      await recordSignal({
        symbol,
        technical,
        multiTimeframe,
        aiDecision,
        executionLevels,
        executed: false,

        skipReason:
          'POSITION_SIZE_FAILED'
      });

      return;
    }

    const riskCheck =
      canOpenNewTrade(
        symbol,
        sizing.riskAmount
      );

    if (!riskCheck.allowed) {
      state.skippedSignals++;

      await recordSignal({
        symbol,
        technical,
        multiTimeframe,
        aiDecision,
        executionLevels,
        executed: false,

        skipReason:
          riskCheck.reason
      });

      console.log(
        `🛑 ${symbol} ${aiDecision.decision} risk reject | ` +
        `${riskCheck.reason}`
      );

      return;
    }

    // ========================================================
    // OPEN PAPER TRADE
    // ========================================================

    const opened =
      await openPaperTrade({
        symbol,
        technical,
        multiTimeframe,
        quote,
        aiDecision,
        executionLevels
      });

    if (!opened.opened) {
      state.skippedSignals++;

      await recordSignal({
        symbol,
        technical,
        multiTimeframe,
        aiDecision,
        executionLevels,
        executed: false,

        skipReason:
          opened.reason
      });

      return;
    }

    await recordSignal({
      symbol,
      technical,
      multiTimeframe,
      aiDecision,
      executionLevels,
      executed: true,
      skipReason: ''
    });
  } catch (error) {
    state.skippedSignals++;

    console.error(
      `Process ${symbol}:`,
      safeError(error)
    );

    await journal(
      'SCAN_ERROR',
      {
        symbol,

        message:
          safeError(error)
      }
    );
  } finally {
    state.scanLocks.delete(
      symbol
    );
  }
}

// ============================================================
// REFRESH ONE SYMBOL
// ============================================================

async function refreshSymbol(
  symbol
) {
  let pair =
    state.pairState.get(
      symbol
    );

  if (!pair?.initialized) {
    await initializeSymbol(
      symbol
    );

    return;
  }

  try {
    const bars =
      await fetchOhlc(
        symbol,
        TIMEFRAME,
        HISTORY_LIMIT
      );

    if (
      bars.length <
      CORE_MIN_HISTORY
    ) {
      throw new Error(
        `Only ${bars.length} bars`
      );
    }

    const latest =
      bars[bars.length - 1];

    pair.bars =
      bars;

    pair.lastError =
      null;

    const newClosedBar =
      latest.time !==
      pair.lastClosedBarTime;

    if (!newClosedBar) {
      return;
    }

    pair.lastClosedBarTime =
      latest.time;

    const quote =
      await fetchSingleQuote(
        symbol
      );

    await processNewClosedBar(
      symbol,
      bars,
      quote
    );

    pair.lastScannedBarTime =
      latest.time;
  } catch (error) {
    pair.lastError =
      safeError(error);

    state.lastMarketError =
      `${symbol}: ${safeError(error)}`;

    console.error(
      `Refresh ${symbol}:`,
      safeError(error)
    );
  }
}

// ============================================================
// MARKET SCANNER
// ============================================================

async function scanLoop() {
  if (state.scanLoopBusy) {
    return;
  }

  state.scanLoopBusy = true;

  try {
    await mapWithConcurrency(
      INSTRUMENTS,
      OHLC_CONCURRENCY,
      refreshSymbol
    );
  } catch (error) {
    console.error(
      'Scan loop:',
      safeError(error)
    );
  } finally {
    state.scanLoopBusy = false;
  }
}

// ============================================================
// MANUAL PAPER CLOSE
// ============================================================

async function manualCloseSymbol(
  symbol,
  reason = 'MANUAL_CLOSE'
) {
  symbol =
    String(symbol || '')
      .toUpperCase();

  const trade =
    state.openTrades.get(
      symbol
    );

  if (!trade) {
    return {
      ok: false,
      reason: 'NO_OPEN_TRADE'
    };
  }

  const quote =
    await fetchSingleQuote(
      symbol
    );

  if (!quote) {
    return {
      ok: false,
      reason: 'QUOTE_UNAVAILABLE'
    };
  }

  const protection =
    await applyMechanicalProtection(
      trade,
      quote
    );

  if (protection.closed) {
    return {
      ok: true,
      reason:
        protection.reason
    };
  }

  const exitPrice =
    tradeMarkPrice(
      trade,
      quote
    );

  const closed =
    await closePaperTrade(
      trade,
      exitPrice,
      reason
    );

  return {
    ok:
      Boolean(closed),

    trade:
      closed
  };
}

// ============================================================
// CLOSE ALL PAPER TRADES
// ============================================================

async function manualCloseAll() {
  const symbols =
    [
      ...state.openTrades.keys()
    ];

  const results = [];

  for (const symbol of symbols) {
    try {
      results.push(
        await manualCloseSymbol(
          symbol,
          'MANUAL_CLOSE_ALL'
        )
      );
    } catch (error) {
      results.push({
        ok: false,
        symbol,
        reason:
          safeError(error)
      });
    }
  }

  return results;
}

// ============================================================
// RUNTIME LOOP START
// ============================================================

function startRuntimeLoops() {
  if (state.loopsStarted) {
    return;
  }

  state.loopsStarted = true;

  setInterval(
    () => {
      quoteLoop().catch(
        error => {
          console.error(
            'Quote timer:',
            safeError(error)
          );
        }
      );
    },
    QUOTE_POLL_MS
  );

  setInterval(
    () => {
      scanLoop().catch(
        error => {
          console.error(
            'Scan timer:',
            safeError(error)
          );
        }
      );
    },
    SCAN_TIMER_MS
  );

  console.log(
    `✅ Quote loop: ${QUOTE_POLL_MS}ms`
  );

  console.log(
    `✅ 15m scanner loop: ${SCAN_TIMER_MS}ms`
  );

  console.log(
    `✅ Gemini management: ${AI.managementIntervalMs}ms/trade`
  );
}

// ============================================================
// END PART 3 / 4
//
// PART 4:
// DASHBOARD
// API ROUTES
// TELEGRAM CLOSE COMMANDS
// HEALTH
// BOOT
// GRACEFUL SHUTDOWN
// ============================================================
// ============================================================
// PART 4 / 4
// DASHBOARD + API + TELEGRAM + BOOT + SHUTDOWN
// ============================================================

// ============================================================
// DASHBOARD HELPERS
// ============================================================

function getOpenTradesArray() {
  return [...state.openTrades.values()];
}

function getOpenFloatingPnl() {
  let total = 0;

  for (const trade of state.openTrades.values()) {
    const quote =
      state.latestQuotes.get(
        trade.symbol
      );

    if (!quote) {
      continue;
    }

    const mark =
      tradeMarkPrice(
        trade,
        quote
      );

    total +=
      calculateTradePnl(
        trade,
        mark
      );
  }

  return total;
}

function getOpenTradesView() {
  return getOpenTradesArray()
    .map(trade => {
      const quote =
        state.latestQuotes.get(
          trade.symbol
        );

      const mark =
        quote
          ? tradeMarkPrice(
              trade,
              quote
            )
          : n(
              trade.lastMarkPrice,
              trade.entryPrice
            );

      const r =
        quote
          ? currentR(
              trade,
              quote
            )
          : 0;

      const floatingPnl =
        Number.isFinite(mark)
          ? calculateTradePnl(
              trade,
              mark
            )
          : 0;

      return {
        id:
          String(
            trade._id || ''
          ),

        symbol:
          trade.symbol,

        direction:
          trade.direction,

        entryPrice:
          trade.entryPrice,

        markPrice:
          mark,

        stopLoss:
          trade.stopLoss,

        initialStopLoss:
          trade.initialStopLoss,

        takeProfit:
          trade.takeProfit,

        breakEvenTriggerPrice:
          trade.breakEvenTriggerPrice,

        breakEvenActive:
          Boolean(
            trade.breakEvenActive
          ),

        riskDistance:
          trade.riskDistance,

        riskAmount:
          trade.riskAmount,

        quantity:
          trade.quantity,

        currentR:
          r,

        floatingPnl,

        mfeR:
          n(
            trade.mfeR,
            0
          ),

        maeR:
          n(
            trade.maeR,
            0
          ),

        openedAt:
          trade.openedAt,

        aiConfidence:
          n(
            trade.aiEntryDecision
              ?.confidence,
            0
          ),

        aiSetup:
          trade.aiEntryDecision
            ?.setup || '',

        aiReason:
          trade.aiEntryDecision
            ?.reason || ''
      };
    });
}

// ============================================================
// STATUS OBJECT
// ============================================================

function buildStatus() {
  const balance =
    accountBalance();

  const floatingPnl =
    getOpenFloatingPnl();

  const equity =
    balance +
    floatingPnl;

  const openRisk =
    currentPortfolioRiskUsd();

  const portfolioCap =
    portfolioRiskCapUsd();

  return {
    version:
      VERSION,

    mode:
      MODE,

    liveTrading:
      LIVE_TRADING,

    paperOnly:
      MODE === 'PAPER' &&
      LIVE_TRADING === false,

    startedAt:
      state.startedAt,

    uptimeSeconds:
      Math.floor(
        process.uptime()
      ),

    marketReady:
      state.marketReady,

    instruments:
      INSTRUMENTS.length,

    instrumentsReady:
      pairReadyCount(),

    timeframe:
      TIMEFRAME,

    balance,

    floatingPnl,

    equity,

    realizedPnl:
      n(
        account?.realizedPnl,
        0
      ),

    startingBalance:
      PAPER.startingBalance,

    totalTrades:
      n(
        account?.totalTrades,
        0
      ),

    wins:
      n(
        account?.wins,
        0
      ),

    losses:
      n(
        account?.losses,
        0
      ),

    breakeven:
      n(
        account?.breakeven,
        0
      ),

    openTrades:
      state.openTrades.size,

    maxOpenTrades:
      PAPER.maxOpenTrades,

    capitalRiskSafetyCapPct:
      PAPER.maxCapitalRiskPct,

    portfolioRiskCapPct:
      PAPER.portfolioRiskCapPct,

    openRiskUsd:
      openRisk,

    portfolioRiskCapUsd:
      portfolioCap,

    portfolioRiskUsedPct:
      portfolioCap > 0
        ? (
            openRisk /
            portfolioCap
          ) * 100
        : 0,

    riskReward:
      `1:${RULES.riskReward}`,

    breakEvenTriggerR:
      RULES.breakEvenTriggerR,

    scannedBars:
      state.scannedBars,

    executedSignals:
      state.executedSignals,

    skippedSignals:
      state.skippedSignals,

    mongoReady:
      state.mongoReady,

    telegramReady:
      Boolean(bot),

    geminiConfigured:
      Boolean(
        GEMINI_API_KEY
      ),

    geminiModel:
      GEMINI_MODEL,

    lastMarketError:
      state.lastMarketError || null
  };
}

// ============================================================
// EXPRESS
// ============================================================

const app =
  express();

app.disable(
  'x-powered-by'
);

app.use(
  express.json({
    limit:
      '250kb'
  })
);

app.use(
  express.urlencoded({
    extended: false
  })
);

// ============================================================
// HEALTH
// ============================================================

app.get(
  '/health',
  (req, res) => {
    res.status(200).json({
      ok: true,

      version:
        VERSION,

      mode:
        MODE,

      liveTrading:
        LIVE_TRADING,

      marketReady:
        state.marketReady,

      openTrades:
        state.openTrades.size,

      uptime:
        Math.floor(
          process.uptime()
        ),

      timestamp:
        new Date()
          .toISOString()
    });
  }
);

// ============================================================
// STATUS API
// ============================================================

app.get(
  '/api/status',
  (req, res) => {
    res.json(
      buildStatus()
    );
  }
);

// ============================================================
// OPEN TRADES API
// ============================================================

app.get(
  '/api/trades',
  (req, res) => {
    res.json({
      openTrades:
        getOpenTradesView()
    });
  }
);

// ============================================================
// RECENT CLOSED TRADES
// ============================================================

app.get(
  '/api/history',
  async (req, res) => {
    try {
      if (!state.mongoReady) {
        return res.json({
          trades: []
        });
      }

      const trades =
        await Trade.find({
          accountKey:
            PAPER.accountKey,

          status:
            'CLOSED'
        })
          .sort({
            closedAt: -1
          })
          .limit(100)
          .lean();

      res.json({
        trades
      });
    } catch (error) {
      res.status(500).json({
        ok: false,
        error:
          safeError(error)
      });
    }
  }
);

// ============================================================
// MANUAL CLOSE ONE
// ============================================================

app.post(
  '/api/close/:symbol',
  async (req, res) => {
    try {
      const symbol =
        String(
          req.params.symbol ||
          ''
        )
          .trim()
          .toUpperCase();

      if (
        !INSTRUMENTS.includes(
          symbol
        )
      ) {
        return res
          .status(400)
          .json({
            ok: false,
            reason:
              'INVALID_SYMBOL'
          });
      }

      const result =
        await manualCloseSymbol(
          symbol,
          'DASHBOARD_MANUAL_CLOSE'
        );

      res.json(
        result
      );
    } catch (error) {
      res.status(500).json({
        ok: false,
        error:
          safeError(error)
      });
    }
  }
);

// ============================================================
// MANUAL CLOSE ALL
// ============================================================

app.post(
  '/api/close-all',
  async (req, res) => {
    try {
      const results =
        await manualCloseAll();

      res.json({
        ok: true,
        results
      });
    } catch (error) {
      res.status(500).json({
        ok: false,
        error:
          safeError(error)
      });
    }
  }
);

// ============================================================
// DASHBOARD
// ============================================================

app.get(
  '/',
  (req, res) => {
    res
      .status(200)
      .type('html')
      .send(`
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta
  name="viewport"
  content="width=device-width, initial-scale=1.0"
>
<title>${VERSION}</title>

<style>
* {
  box-sizing: border-box;
}

body {
  margin: 0;
  padding: 0;
  background: #070b12;
  color: #eef4ff;
  font-family:
    Arial,
    Helvetica,
    sans-serif;
}

.container {
  width: min(1500px, 96%);
  margin: 0 auto;
  padding: 24px 0 40px;
}

.header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 20px;
  flex-wrap: wrap;
  margin-bottom: 22px;
}

.title {
  font-size: 28px;
  font-weight: 800;
}

.subtitle {
  color: #93a4bd;
  margin-top: 7px;
  font-size: 14px;
}

.paper-badge {
  background: #172036;
  border: 1px solid #35528a;
  color: #8ab4ff;
  padding: 10px 16px;
  border-radius: 999px;
  font-weight: 800;
}

.warning {
  background: #20170b;
  border: 1px solid #65491f;
  color: #ffc966;
  border-radius: 12px;
  padding: 13px 16px;
  margin-bottom: 20px;
  line-height: 1.5;
}

.grid {
  display: grid;
  grid-template-columns:
    repeat(
      auto-fit,
      minmax(170px, 1fr)
    );
  gap: 12px;
  margin-bottom: 22px;
}

.card {
  background: #0d1421;
  border: 1px solid #1b2a40;
  border-radius: 14px;
  padding: 16px;
}

.card .label {
  color: #8190a8;
  font-size: 12px;
  text-transform: uppercase;
  letter-spacing: .7px;
  margin-bottom: 9px;
}

.card .value {
  font-size: 23px;
  font-weight: 800;
}

.good {
  color: #55d98b;
}

.bad {
  color: #ff7373;
}

.neutral {
  color: #8ab4ff;
}

.section {
  background: #0d1421;
  border: 1px solid #1b2a40;
  border-radius: 14px;
  overflow: hidden;
  margin-bottom: 20px;
}

.section-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 15px;
  padding: 16px 18px;
  border-bottom: 1px solid #1b2a40;
  flex-wrap: wrap;
}

.section-title {
  font-size: 18px;
  font-weight: 800;
}

button {
  border: 0;
  border-radius: 8px;
  padding: 9px 13px;
  cursor: pointer;
  font-weight: 800;
}

.close-btn {
  background: #55242b;
  color: #ffdce0;
}

.close-all-btn {
  background: #842f3a;
  color: white;
}

.refresh-btn {
  background: #17335b;
  color: #cfe2ff;
}

.table-wrap {
  overflow-x: auto;
}

table {
  width: 100%;
  border-collapse: collapse;
  min-width: 1200px;
}

th,
td {
  text-align: left;
  padding: 12px 13px;
  border-bottom: 1px solid #172338;
  font-size: 13px;
}

th {
  color: #8fa0b9;
  background: #0a101b;
  font-size: 11px;
  text-transform: uppercase;
}

.buy {
  color: #5fe49b;
  font-weight: 800;
}

.sell {
  color: #ff7b7b;
  font-weight: 800;
}

.be {
  color: #ffd369;
  font-weight: 800;
}

.empty {
  color: #718198;
  padding: 28px 18px;
  text-align: center;
}

.footer {
  color: #718198;
  font-size: 12px;
  line-height: 1.7;
  margin-top: 20px;
  text-align: center;
}

.small {
  color: #8190a8;
  font-size: 12px;
}

#connection {
  font-size: 12px;
  color: #8190a8;
}
</style>
</head>

<body>

<div class="container">

  <div class="header">
    <div>
      <div class="title">
        LOMY FOREX V1.3 AI TRADER
      </div>

      <div class="subtitle">
        Gemini Technical Intelligence + Hard Risk Manager
      </div>
    </div>

    <div class="paper-badge">
      🧪 PAPER ONLY
    </div>
  </div>

  <div class="warning">
    LIVE trading is disabled.
    Gemini may decide BUY / SELL / NO_TRADE and may
    recommend closing an existing PAPER position.
    Position sizing, portfolio risk, Stop Loss validation,
    Take Profit 1:2 and Break Even protection are controlled
    mechanically by the bot and cannot be overridden by AI.
  </div>

  <div class="grid">

    <div class="card">
      <div class="label">
        Balance
      </div>
      <div
        class="value"
        id="balance"
      >
        --
      </div>
    </div>

    <div class="card">
      <div class="label">
        Equity
      </div>
      <div
        class="value"
        id="equity"
      >
        --
      </div>
    </div>

    <div class="card">
      <div class="label">
        Floating PnL
      </div>
      <div
        class="value"
        id="floating"
      >
        --
      </div>
    </div>

    <div class="card">
      <div class="label">
        Realized PnL
      </div>
      <div
        class="value"
        id="realized"
      >
        --
      </div>
    </div>

    <div class="card">
      <div class="label">
        Open Trades
      </div>
      <div
        class="value neutral"
        id="openTrades"
      >
        --
      </div>
    </div>

    <div class="card">
      <div class="label">
        Portfolio Risk
      </div>
      <div
        class="value"
        id="portfolioRisk"
      >
        --
      </div>
    </div>

    <div class="card">
      <div class="label">
        R:R
      </div>
      <div
        class="value neutral"
        id="rr"
      >
        1:2
      </div>
    </div>

    <div class="card">
      <div class="label">
        BE Trigger
      </div>
      <div
        class="value neutral"
        id="beTrigger"
      >
        +0.60R
      </div>
    </div>

    <div class="card">
      <div class="label">
        Market
      </div>
      <div
        class="value"
        id="market"
      >
        --
      </div>
    </div>

    <div class="card">
      <div class="label">
        Gemini
      </div>
      <div
        class="value"
        id="gemini"
      >
        --
      </div>
    </div>

  </div>

  <div class="section">

    <div class="section-header">

      <div>
        <div class="section-title">
          Open PAPER Trades
        </div>

        <div
          class="small"
          id="connection"
        >
          Connecting...
        </div>
      </div>

      <div>
        <button
          class="refresh-btn"
          onclick="refreshAll()"
        >
          Refresh
        </button>

        <button
          class="close-all-btn"
          onclick="closeAllTrades()"
        >
          Close All
        </button>
      </div>

    </div>

    <div class="table-wrap">

      <table>

        <thead>
          <tr>
            <th>Symbol</th>
            <th>Side</th>
            <th>Entry</th>
            <th>Mark</th>
            <th>SL</th>
            <th>TP</th>
            <th>R</th>
            <th>PnL</th>
            <th>Risk</th>
            <th>BE</th>
            <th>MFE</th>
            <th>MAE</th>
            <th>AI</th>
            <th>Action</th>
          </tr>
        </thead>

        <tbody id="tradeRows">

          <tr>
            <td
              colspan="14"
              class="empty"
            >
              Loading...
            </td>
          </tr>

        </tbody>

      </table>

    </div>

  </div>

  <div class="footer">
    ${VERSION}
    • PAPER ONLY
    • 15m closed candles
    • Gemini commander
    • Technical SL selected by AI and validated by Risk Manager
    • TP = exactly 2R
    • Break Even = +0.60R
    • Maximum initial capital-risk safety cap = ${PAPER.maxCapitalRiskPct}%
    • Portfolio risk cap = ${PAPER.portfolioRiskCapPct}%
    <br>
    The 1% value is a maximum loss safety budget for position sizing.
    It is NOT a fixed profit/loss target.
  </div>

</div>

<script>

function money(v) {
  const x =
    Number(v || 0);

  return (
    (x >= 0 ? '$' : '-$') +
    Math.abs(x).toFixed(2)
  );
}

function num(v, digits = 5) {
  const x =
    Number(v);

  if (!Number.isFinite(x)) {
    return '--';
  }

  return x.toFixed(digits);
}

function price(v, symbol) {
  const x =
    Number(v);

  if (!Number.isFinite(x)) {
    return '--';
  }

  if (
    symbol === 'XAUUSD'
  ) {
    return x.toFixed(2);
  }

  if (
    String(symbol)
      .endsWith('JPY')
  ) {
    return x.toFixed(3);
  }

  return x.toFixed(5);
}

function pnlClass(v) {
  const x =
    Number(v || 0);

  if (x > 0) {
    return 'good';
  }

  if (x < 0) {
    return 'bad';
  }

  return 'neutral';
}

async function getJson(url, options) {
  const response =
    await fetch(
      url,
      options
    );

  const data =
    await response.json();

  if (!response.ok) {
    throw new Error(
      data.error ||
      data.reason ||
      'Request failed'
    );
  }

  return data;
}

async function loadStatus() {
  const s =
    await getJson(
      '/api/status'
    );

  document.getElementById(
    'balance'
  ).textContent =
    money(s.balance);

  document.getElementById(
    'equity'
  ).textContent =
    money(s.equity);

  const floating =
    document.getElementById(
      'floating'
    );

  floating.textContent =
    money(
      s.floatingPnl
    );

  floating.className =
    'value ' +
    pnlClass(
      s.floatingPnl
    );

  const realized =
    document.getElementById(
      'realized'
    );

  realized.textContent =
    money(
      s.realizedPnl
    );

  realized.className =
    'value ' +
    pnlClass(
      s.realizedPnl
    );

  document.getElementById(
    'openTrades'
  ).textContent =
    String(
      s.openTrades
    );

  document.getElementById(
    'portfolioRisk'
  ).textContent =
    money(
      s.openRiskUsd
    ) +
    ' / ' +
    money(
      s.portfolioRiskCapUsd
    );

  document.getElementById(
    'rr'
  ).textContent =
    s.riskReward;

  document.getElementById(
    'beTrigger'
  ).textContent =
    '+' +
    Number(
      s.breakEvenTriggerR
    ).toFixed(2) +
    'R';

  const market =
    document.getElementById(
      'market'
    );

  market.textContent =
    s.marketReady
      ? (
          s.instrumentsReady +
          '/' +
          s.instruments
        )
      : 'NOT READY';

  market.className =
    'value ' +
    (
      s.marketReady
        ? 'good'
        : 'bad'
    );

  const gemini =
    document.getElementById(
      'gemini'
    );

  gemini.textContent =
    s.geminiConfigured
      ? 'READY'
      : 'OFF';

  gemini.className =
    'value ' +
    (
      s.geminiConfigured
        ? 'good'
        : 'bad'
    );
}

async function loadTrades() {
  const data =
    await getJson(
      '/api/trades'
    );

  const tbody =
    document.getElementById(
      'tradeRows'
    );

  const trades =
    data.openTrades || [];

  if (!trades.length) {
    tbody.innerHTML =
      '<tr>' +
      '<td colspan="14" class="empty">' +
      'No open PAPER trades' +
      '</td>' +
      '</tr>';

    return;
  }

  tbody.innerHTML =
    trades
      .map(t => {

        const sideClass =
          t.direction === 'BUY'
            ? 'buy'
            : 'sell';

        const be =
          t.breakEvenActive
            ? '<span class="be">ACTIVE</span>'
            : 'WAIT';

        return (
          '<tr>' +

          '<td><strong>' +
          t.symbol +
          '</strong></td>' +

          '<td class="' +
          sideClass +
          '">' +
          t.direction +
          '</td>' +

          '<td>' +
          price(
            t.entryPrice,
            t.symbol
          ) +
          '</td>' +

          '<td>' +
          price(
            t.markPrice,
            t.symbol
          ) +
          '</td>' +

          '<td>' +
          price(
            t.stopLoss,
            t.symbol
          ) +
          '</td>' +

          '<td>' +
          price(
            t.takeProfit,
            t.symbol
          ) +
          '</td>' +

          '<td class="' +
          pnlClass(
            t.currentR
          ) +
          '">' +
          Number(
            t.currentR || 0
          ).toFixed(2) +
          'R</td>' +

          '<td class="' +
          pnlClass(
            t.floatingPnl
          ) +
          '">' +
          money(
            t.floatingPnl
          ) +
          '</td>' +

          '<td>' +
          money(
            t.riskAmount
          ) +
          '</td>' +

          '<td>' +
          be +
          '</td>' +

          '<td class="good">' +
          Number(
            t.mfeR || 0
          ).toFixed(2) +
          'R</td>' +

          '<td class="bad">' +
          Number(
            t.maeR || 0
          ).toFixed(2) +
          'R</td>' +

          '<td>' +
          Number(
            t.aiConfidence || 0
          ).toFixed(0) +
          '%</td>' +

          '<td>' +
          '<button class="close-btn" ' +
          'onclick="closeTrade(\\'' +
          t.symbol +
          '\\')">' +
          'Close' +
          '</button>' +
          '</td>' +

          '</tr>'
        );
      })
      .join('');
}

async function refreshAll() {
  const connection =
    document.getElementById(
      'connection'
    );

  try {
    await Promise.all([
      loadStatus(),
      loadTrades()
    ]);

    connection.textContent =
      'Connected • ' +
      new Date()
        .toLocaleTimeString();

    connection.className =
      'good';
  } catch (error) {
    connection.textContent =
      'Connection error: ' +
      error.message;

    connection.className =
      'bad';
  }
}

async function closeTrade(symbol) {
  if (
    !confirm(
      'Close ' +
      symbol +
      ' PAPER trade?'
    )
  ) {
    return;
  }

  try {
    const result =
      await getJson(
        '/api/close/' +
        encodeURIComponent(
          symbol
        ),
        {
          method:
            'POST',

          headers: {
            'Content-Type':
              'application/json'
          }
        }
      );

    if (!result.ok) {
      alert(
        result.reason ||
        'Could not close trade'
      );
    }

    await refreshAll();
  } catch (error) {
    alert(
      error.message
    );
  }
}

async function closeAllTrades() {
  if (
    !confirm(
      'Close ALL open PAPER trades?'
    )
  ) {
    return;
  }

  try {
    await getJson(
      '/api/close-all',
      {
        method:
          'POST',

        headers: {
          'Content-Type':
            'application/json'
        }
      }
    );

    await refreshAll();
  } catch (error) {
    alert(
      error.message
    );
  }
}

refreshAll();

setInterval(
  refreshAll,
  5000
);

</script>

</body>
</html>
      `);
  }
);

// ============================================================
// TELEGRAM COMMANDS
// ============================================================

function setupTelegramCommands() {
  if (!bot) {
    return;
  }

  bot.command(
    'start',
    async ctx => {
      await ctx.reply(
        `${VERSION}\n` +
        `PAPER ONLY\n\n` +
        `/status - Account status\n` +
        `/trades - Open trades\n` +
        `/closeall - Close all PAPER trades`
      );
    }
  );

  bot.command(
    'status',
    async ctx => {
      const s =
        buildStatus();

      await ctx.reply(
        `📊 ${VERSION}\n\n` +

        `Mode: PAPER ONLY\n` +
        `Balance: ${fmtMoney(s.balance)}\n` +
        `Equity: ${fmtMoney(s.equity)}\n` +
        `Floating: ${fmtMoney(s.floatingPnl)}\n` +
        `Realized: ${fmtMoney(s.realizedPnl)}\n\n` +

        `Open trades: ${s.openTrades}\n` +
        `Open risk: ${fmtMoney(s.openRiskUsd)}\n` +
        `Portfolio cap: ${fmtMoney(s.portfolioRiskCapUsd)} (${PAPER.portfolioRiskCapPct}%)\n\n` +

        `R:R: ${s.riskReward}\n` +
        `BE: +${RULES.breakEvenTriggerR.toFixed(2)}R\n` +
        `Max capital-risk safety cap/trade: ${PAPER.maxCapitalRiskPct}%\n\n` +

        `Market: ${s.instrumentsReady}/${s.instruments}\n` +
        `Gemini: ${s.geminiConfigured ? 'READY' : 'OFF'}`
      );
    }
  );

  bot.command(
    'trades',
    async ctx => {
      const trades =
        getOpenTradesView();

      if (!trades.length) {
        await ctx.reply(
          'No open PAPER trades.'
        );

        return;
      }

      const lines =
        trades.map(t => {
          return (
            `${t.symbol} ${t.direction}\n` +
            `Entry ${fmtPrice(t.entryPrice, t.symbol)} | ` +
            `SL ${fmtPrice(t.stopLoss, t.symbol)} | ` +
            `TP ${fmtPrice(t.takeProfit, t.symbol)}\n` +
            `Now ${n(t.currentR, 0).toFixed(2)}R | ` +
            `PnL ${fmtMoney(t.floatingPnl)} | ` +
            `BE ${t.breakEvenActive ? 'ACTIVE' : 'WAIT'}`
          );
        });

      await ctx.reply(
        `📈 OPEN PAPER TRADES\n\n` +
        lines.join(
          '\n\n'
        )
      );
    }
  );

  bot.command(
    'closeall',
    async ctx => {
      if (
        !state.openTrades.size
      ) {
        await ctx.reply(
          'No open PAPER trades.'
        );

        return;
      }

      await ctx.reply(
        'Closing all open PAPER trades...'
      );

      const results =
        await manualCloseAll();

      const closed =
        results.filter(
          x => x.ok
        ).length;

      await ctx.reply(
        `Closed: ${closed}/${results.length}`
      );
    }
  );
}

// ============================================================
// LOAD OPEN TRADES AFTER RESTART
// ============================================================

async function restoreOpenTrades() {
  if (!state.mongoReady) {
    console.log(
      'ℹ️ Mongo unavailable — no persisted trades to restore'
    );

    return;
  }

  try {
    const trades =
      await Trade.find({
        accountKey:
          PAPER.accountKey,

        status:
          'OPEN'
      }).lean();

    for (const trade of trades) {
      if (
        !INSTRUMENTS.includes(
          trade.symbol
        )
      ) {
        continue;
      }

      state.openTrades.set(
        trade.symbol,
        trade
      );
    }

    console.log(
      `♻️ Restored ${state.openTrades.size} open PAPER trades`
    );

    if (
      state.openTrades.size
    ) {
      await sendTelegram(
        `♻️ ${VERSION}\n` +
        `Restored ${state.openTrades.size} open PAPER trade(s) after restart.\n` +
        `Mechanical SL/TP/BE protection resumed.`
      );
    }
  } catch (error) {
    console.error(
      'Restore trades:',
      safeError(error)
    );
  }
}

// ============================================================
// START EXPRESS SERVER
// ============================================================

let httpServer = null;

async function startWebServer() {
  return new Promise(
    (resolve, reject) => {
      httpServer =
        app.listen(
          PORT,
          () => {
            console.log(
              `🌐 Dashboard listening on port ${PORT}`
            );

            resolve();
          }
        );

      httpServer.on(
        'error',
        reject
      );
    }
  );
}

// ============================================================
// START TELEGRAM
// ============================================================

async function startTelegram() {
  if (!bot) {
    console.log(
      'ℹ️ Telegram not configured'
    );

    return;
  }

  setupTelegramCommands();

  try {
    await bot.launch();

    console.log(
      '✅ Telegram bot started'
    );
  } catch (error) {
    console.error(
      'Telegram start:',
      safeError(error)
    );
  }
}

// ============================================================
// STARTUP BANNER
// ============================================================

function printStartupBanner() {
  console.log('');
  console.log(
    '============================================================'
  );
  console.log(
    `🚀 ${VERSION}`
  );
  console.log(
    '🧪 PAPER ONLY — LIVE TRADING OFF'
  );
  console.log(
    '============================================================'
  );
  console.log(
    `Starting balance: ${fmtMoney(PAPER.startingBalance)}`
  );
  console.log(
    `Max capital-risk safety cap/trade: ${PAPER.maxCapitalRiskPct}%`
  );
  console.log(
    `R:R: 1:${RULES.riskReward}`
  );
  console.log(
    `Portfolio risk cap: ${PAPER.portfolioRiskCapPct}%`
  );
  console.log(
    `Break Even: +${RULES.breakEvenTriggerR.toFixed(2)}R`
  );
  console.log(
    `Timeframe: ${TIMEFRAME} closed candles`
  );
  console.log(
    `Instruments: ${INSTRUMENTS.length}`
  );
  console.log(
    `Gemini model: ${GEMINI_MODEL}`
  );
  console.log(
    `Gemini key: ${GEMINI_API_KEY ? 'CONFIGURED' : 'MISSING'}`
  );
  console.log(
    `Mongo: ${MONGODB_URI ? 'CONFIGURED' : 'OPTIONAL/OFF'}`
  );
  console.log(
    `Telegram: ${bot ? 'CONFIGURED' : 'OPTIONAL/OFF'}`
  );
  console.log(
    '============================================================'
  );
  console.log(
    'IMPORTANT: 1% is the maximum account-loss safety budget.'
  );
  console.log(
    'TP is NOT +2% account profit. TP = 2 × price-risk distance.'
  );
  console.log(
    '============================================================'
  );
  console.log('');
}

// ============================================================
// CONFIG VALIDATION
// ============================================================

function validateStartupConfig() {
  if (
    MODE !== 'PAPER'
  ) {
    throw new Error(
      'Security lock: MODE must be PAPER'
    );
  }

  if (
    LIVE_TRADING !== false
  ) {
    throw new Error(
      'Security lock: LIVE_TRADING must be false'
    );
  }

  if (
    RULES.riskReward !== 2
  ) {
    throw new Error(
      'Risk lock: R:R must remain 1:2'
    );
  }

  if (
    PAPER.maxCapitalRiskPct > 1
  ) {
    throw new Error(
      'Risk lock: max capital risk per trade cannot exceed 1%'
    );
  }

  if (
    PAPER.portfolioRiskCapPct > 4
  ) {
    throw new Error(
      'Risk lock: portfolio risk cap cannot exceed 4%'
    );
  }

  if (
    Math.abs(
      RULES.breakEvenTriggerR -
      0.60
    ) > 1e-9
  ) {
    throw new Error(
      'Risk lock: Break Even trigger must remain +0.60R'
    );
  }

  if (
    !Array.isArray(
      INSTRUMENTS
    ) ||
    !INSTRUMENTS.length
  ) {
    throw new Error(
      'No instruments configured'
    );
  }

  if (
    !GEMINI_API_KEY
  ) {
    console.warn(
      '⚠️ GEMINI_API_KEY missing.'
    );

    console.warn(
      '⚠️ Fail-closed mode: NO new trades will be opened.'
    );
  }
}

// ============================================================
// BOOT
// ============================================================

async function boot() {
  try {
    printStartupBanner();

    validateStartupConfig();

    // Start web server early so Render health checks
    // can reach the application during market initialization.
    await startWebServer();

    // Database is optional for runtime.
    // If unavailable, bot continues in memory.
    try {
      await connectMongo();
    } catch (error) {
      console.error(
        'Mongo boot:',
        safeError(error)
      );
    }

    await initializeAccount();

    await restoreOpenTrades();

    await startTelegram();

    try {
      await initializeMarket();
    } catch (error) {
      console.error(
        'Initial market load:',
        safeError(error)
      );

      state.marketReady = false;

      // Do not kill the server.
      // Scanner will keep retrying initialization.
    }

    startRuntimeLoops();

    // Immediate protection check for restored trades.
    if (
      state.openTrades.size
    ) {
      quoteLoop().catch(
        error => {
          console.error(
            'Initial quote protection:',
            safeError(error)
          );
        }
      );
    }

    // Immediate scan refresh.
    scanLoop().catch(
      error => {
        console.error(
          'Initial scan:',
          safeError(error)
        );
      }
    );

    console.log('');
    console.log(
      '============================================================'
    );
    console.log(
      '✅ LOMY FOREX V1.3 IS RUNNING'
    );
    console.log(
      '🧪 PAPER ONLY'
    );
    console.log(
      '🤖 Gemini = Trading Commander'
    );
    console.log(
      '🛡️ Risk Manager = Non-bypassable'
    );
    console.log(
      '🎯 TP = 2R'
    );
    console.log(
      '🔒 Max initial account-risk safety cap = 1%'
    );
    console.log(
      '🛡️ Portfolio risk cap = 4%'
    );
    console.log(
      '⚡ Break Even = +0.60R'
    );
    console.log(
      '============================================================'
    );
    console.log('');

    await journal(
      'SYSTEM_START',
      {
        message:
          `${VERSION} started`,

        data: {
          mode:
            MODE,

          liveTrading:
            LIVE_TRADING,

          startingBalance:
            PAPER.startingBalance,

          riskReward:
            RULES.riskReward,

          maxCapitalRiskPct:
            PAPER.maxCapitalRiskPct,

          portfolioRiskCapPct:
            PAPER.portfolioRiskCapPct,

          breakEvenTriggerR:
            RULES.breakEvenTriggerR,

          timeframe:
            TIMEFRAME,

          instruments:
            INSTRUMENTS.length,

          geminiModel:
            GEMINI_MODEL
        }
      }
    );

    await sendTelegram(
      `🚀 ${VERSION} STARTED\n\n` +
      `🧪 PAPER ONLY\n` +
      `Balance: ${fmtMoney(accountBalance())}\n` +
      `R:R: 1:${RULES.riskReward}\n` +
      `Max capital-risk safety cap: ${PAPER.maxCapitalRiskPct}%\n` +
      `Portfolio cap: ${PAPER.portfolioRiskCapPct}%\n` +
      `BE: +${RULES.breakEvenTriggerR.toFixed(2)}R\n` +
      `Timeframe: ${TIMEFRAME}\n` +
      `Instruments: ${INSTRUMENTS.length}\n` +
      `Gemini: ${GEMINI_API_KEY ? 'READY' : 'FAIL-CLOSED'}`
    );
  } catch (error) {
    console.error(
      '❌ FATAL BOOT ERROR:',
      safeError(error)
    );

    try {
      await sendTelegram(
        `❌ ${VERSION} BOOT ERROR\n\n` +
        safeError(error)
      );
    } catch (_) {}

    process.exitCode = 1;
  }
}

// ============================================================
// GRACEFUL SHUTDOWN
// ============================================================

let shuttingDown = false;

async function gracefulShutdown(
  signal
) {
  if (shuttingDown) {
    return;
  }

  shuttingDown = true;

  console.log(
    `\n🛑 ${signal} received`
  );

  console.log(
    '🛑 Graceful shutdown starting...'
  );

  try {
    await journal(
      'SYSTEM_STOP',
      {
        message:
          `${VERSION} stopped by ${signal}`,

        data: {
          openTrades:
            state.openTrades.size,

          balance:
            accountBalance()
        }
      }
    );
  } catch (_) {}

  try {
    await sendTelegram(
      `🛑 ${VERSION} STOPPING\n` +
      `Signal: ${signal}\n` +
      `Open PAPER trades persisted: ${state.openTrades.size}`
    );
  } catch (_) {}

  try {
    if (bot) {
      bot.stop(
        signal
      );
    }
  } catch (_) {}

  try {
    if (httpServer) {
      await new Promise(
        resolve => {
          httpServer.close(
            () => resolve()
          );

          setTimeout(
            resolve,
            3000
          );
        }
      );
    }
  } catch (_) {}

  try {
    if (
      mongoose.connection
        .readyState !== 0
    ) {
      await mongoose.disconnect();
    }
  } catch (_) {}

  console.log(
    '✅ Shutdown complete'
  );

  process.exit(0);
}

// ============================================================
// PROCESS ERROR HANDLERS
// ============================================================

process.on(
  'SIGINT',
  () => {
    gracefulShutdown(
      'SIGINT'
    );
  }
);

process.on(
  'SIGTERM',
  () => {
    gracefulShutdown(
      'SIGTERM'
    );
  }
);

process.on(
  'unhandledRejection',
  reason => {
    console.error(
      '⚠️ Unhandled rejection:',
      reason instanceof Error
        ? reason.stack ||
          reason.message
        : reason
    );
  }
);

process.on(
  'uncaughtException',
  error => {
    console.error(
      '❌ Uncaught exception:',
      error?.stack ||
      safeError(error)
    );
  }
);

// ============================================================
// SECURITY ASSERTION
//
// There is intentionally NO broker order-placement code
// anywhere in V1.3.
//
// The system can:
// - analyze markets
// - ask Gemini for BUY / SELL / NO_TRADE
// - simulate PAPER trades
// - manage PAPER positions
// - close PAPER positions
//
// It cannot:
// - place LIVE orders
// - withdraw
// - deposit
// - transfer funds
// - modify API permissions
// ============================================================

Object.freeze(
  RULES
);

Object.freeze(
  PAPER
);

// ============================================================
// START APPLICATION
// ============================================================

boot();

// ============================================================
// END PART 4 / 4
// END OF index.js
// ============================================================
