'use strict';

const axios = require('axios');
const express = require('express');
const mongoose = require('mongoose');
const { Telegraf } = require('telegraf');

// ============================================================
// LOMY FOREX V1.5 — GEMINI COMMANDER PRO
// PAPER ONLY
// 50% partial close at +2R, then trail remaining 50%
// ============================================================

const VERSION = 'LOMY FOREX V1.5 GEMINI COMMANDER PRO';
const MODE = 'PAPER';
const LIVE_TRADING = false;

// =========================
// ENVIRONMENT
// =========================

const PORT = Number(process.env.PORT || 10000);

const TELEGRAM_BOT_TOKEN =
  String(process.env.TELEGRAM_BOT_TOKEN || '').trim();

const TWELVE_DATA_API_KEY =
  String(process.env.TWELVE_DATA_API_KEY || '').trim();

const MONGODB_URI =
  String(process.env.MONGODB_URI || '').trim();

const GEMINI_API_KEY =
  String(process.env.GEMINI_API_KEY || '').trim();

const GEMINI_MODEL =
  String(process.env.GEMINI_MODEL || 'gemini-3.6-flash').trim();

const TWELVE_BASE = 'https://api.twelvedata.com';

// =========================
// CORE SETTINGS
// =========================

const TIMEFRAME = '15m';
const TIMEFRAME_MS = 15 * 60 * 1000;

const HISTORY_LIMIT = 260;
const CORE_MIN_HISTORY = 60;
const EMA200_CONTEXT_HISTORY = 200;

const OHLC_CONCURRENCY = 4;

const QUOTE_POLL_MS = 60 * 1000;
const SCAN_TIMER_MS = 60 * 1000;
const AI_MANAGE_INTERVAL_MS = 60 * 1000;

const GEMINI_MIN_CALL_GAP_MS = 850;

const NEWS_REFRESH_MS =
  4 * 60 * 60 * 1000;

const JOURNAL_COLLECTION =
  'lomyforexjournalv15';

// =========================
// INSTRUMENTS
// =========================

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

// =========================
// TRADING RULES
// =========================

const RULES = Object.freeze({

  riskReward: 2.00,

  breakEvenTriggerR: 0.60,

  partialTpTriggerR: 2.00,

  // بعد إغلاق 50% عند +2R
  // نبدأ حماية النصف المتبقي من +1R
  trailingStartStopR: 1.00,

  // كل تقدم إضافي بمقدار 0.5R
  // نحرك الحماية 0.5R
  trailingStepR: 0.50,

  minStopAtr: 0.25,

  maxStopAtr: 6.00,

  maxSpreadRiskFraction: 0.20,

  minEntryConfidence: 62,

  minCloseConfidence: 68
});

// =========================
// PAPER ACCOUNT
// =========================

const PAPER = Object.freeze({

  startingBalance: 300,

  // أقصى مخاطرة أولية للصفقة
  maxCapitalRiskPct: 1.00,

  // أقصى مخاطرة للمحفظة
  portfolioRiskCapPct: 4.00,

  maxOpenTrades: 31,

  accountKey:
    'lomy-forex-v15-gemini-pro-300usd'
});

// =========================
// DYNAMIC RISK
// =========================

const DYNAMIC_RISK = Object.freeze({

  highConfidence: 85,
  highRiskPct: 1.00,

  medConfidence: 75,
  medRiskPct: 0.75,

  lowConfidence: 62,
  lowRiskPct: 0.50
});

// =========================
// TECHNICAL SETTINGS
// =========================

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

  swingLeft: 3,
  swingRight: 3,

  liquidityLookback: 20,

  vwapLookback: 50,

  mfiLen: 14
});

// =========================
// GEMINI SETTINGS
// =========================

const AI = Object.freeze({

  enabled: true,

  entryCommanderEnabled: true,

  managementEnabled: true,

  memoryClosedTrades: 40,

  temperature: 0.10,

  timeoutMs: 20000
});

// =========================
// GLOBAL STATE
// =========================

const state = {

  startedAt: new Date(),

  mongoReady: false,
  telegramReady: false,
  marketReady: false,
  geminiReady: false,

  quoteLoopBusy: false,
  scanLoopBusy: false,
  loopsStarted: false,

  lastMarketError: null,
  lastAiError: null,

  scannedBars: 0,

  aiEntryCalls: 0,
  aiManageCalls: 0,

  aiBuyDecisions: 0,
  aiSellDecisions: 0,
  aiNoTradeDecisions: 0,

  aiCloseDecisions: 0,
  aiHoldDecisions: 0,

  executedSignals: 0,
  skippedSignals: 0,

  journalEvents: 0,

  pairState: new Map(),

  latestQuotes: new Map(),

  openTrades: new Map(),

  processedBars: new Set(),

  scanLocks: new Set(),

  managementLocks: new Set(),

  lastManageAt: new Map()
};

let account = null;

let telegramBot = null;

let economicNews = [];

let lastGeminiCallAt = 0;

// =========================
// HTTP CLIENT
// =========================

const http = axios.create({

  timeout: 20000,

  headers: {
    'User-Agent':
      'LOMY-FOREX-V1.5-GEMINI-COMMANDER-PRO'
  }
});

// =========================
// BASIC UTILITIES
// =========================

function n(value, fallback = 0) {

  const number = Number(value);

  return Number.isFinite(number)
    ? number
    : fallback;
}

function clamp(value, min, max) {

  return Math.max(
    min,
    Math.min(max, value)
  );
}

function sleep(ms) {

  return new Promise(
    resolve => setTimeout(resolve, ms)
  );
}

function sum(values) {

  return values
    .filter(Number.isFinite)
    .reduce(
      (total, value) => total + value,
      0
    );
}

function average(values) {

  const valid =
    values.filter(Number.isFinite);

  return valid.length
    ? sum(valid) / valid.length
    : NaN;
}

function standardDeviation(values) {

  const valid =
    values.filter(Number.isFinite);

  if (!valid.length)
    return NaN;

  const mean =
    average(valid);

  return Math.sqrt(

    valid.reduce(
      (total, value) =>
        total + (value - mean) ** 2,
      0
    ) / valid.length

  );
}

function pctChange(from, to) {

  from = n(from, NaN);
  to = n(to, NaN);

  if (
    !Number.isFinite(from) ||
    !Number.isFinite(to) ||
    from === 0
  ) {
    return NaN;
  }

  return (
    (to - from) /
    Math.abs(from)
  ) * 100;
}

function safeError(error) {

  const data =
    error?.response?.data;

  return (
    data?.error?.message ||
    data?.message ||
    data?.error ||
    error?.message ||
    String(error)
  );
}

function fmtMoney(value) {

  return '$' +
    n(value, 0).toFixed(2);
}

function fmtPrice(value, symbol = '') {

  if (
    !Number.isFinite(Number(value))
  ) {
    return 'n/a';
  }

  value = Number(value);

  if (symbol === 'XAUUSD')
    return value.toFixed(2);

  if (symbol.endsWith('JPY'))
    return value.toFixed(3);

  return value.toFixed(5);
}

function barTimeMs(bar) {

  const time =
    new Date(
      bar?.openTime
    ).getTime();

  return Number.isFinite(time)
    ? time
    : 0;
}

function highestHigh(bars) {

  return bars?.length
    ? Math.max(
        ...bars.map(
          bar => bar.high
        )
      )
    : NaN;
}

function lowestLow(bars) {

  return bars?.length
    ? Math.min(
        ...bars.map(
          bar => bar.low
        )
      )
    : NaN;
}

// =========================
// NORMALIZE MARKET BARS
// =========================

function normalizeBars(rawBars) {

  if (!Array.isArray(rawBars))
    return [];

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

      isOpen:
        bar.isOpen === true

    }))

    .filter(bar =>
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

function intervalMs(interval) {

  if (interval === '4h')
    return 4 * 60 * 60 * 1000;

  if (interval === '1h')
    return 60 * 60 * 1000;

  return TIMEFRAME_MS;
}

function closedBarsOnly(
  bars,
  interval = TIMEFRAME
) {

  const now = Date.now();

  const ms =
    intervalMs(interval);

  return bars.filter(bar =>

    !bar.isOpen &&

    barTimeMs(bar) > 0 &&

    barTimeMs(bar) + ms
      <= now + 5000

  );
}

// =========================
// EMA
// =========================

function emaSeries(
  values,
  length
) {

  if (
    !Array.isArray(values) ||
    values.length < length
  ) {
    return [];
  }

  const out =
    new Array(
      values.length
    ).fill(NaN);

  const k =
    2 / (length + 1);

  const seed =
    values.slice(
      0,
      length
    );

  if (
    !seed.every(
      Number.isFinite
    )
  ) {
    return out;
  }

  out[length - 1] =
    sum(seed) / length;

  for (
    let i = length;
    i < values.length;
    i++
  ) {

    if (
      Number.isFinite(values[i]) &&
      Number.isFinite(out[i - 1])
    ) {

      out[i] =
        values[i] * k +
        out[i - 1] *
        (1 - k);
    }
  }

  return out;
}

function emaLast(
  values,
  length
) {

  const series =
    emaSeries(
      values,
      length
    );

  return series[
    series.length - 1
  ];
}

// =========================
// TRUE RANGE / ATR
// =========================

function trueRangeSeries(bars) {

  const out = [];

  if (
    !bars ||
    bars.length < 2
  ) {
    return out;
  }

  for (
    let i = 1;
    i < bars.length;
    i++
  ) {

    out.push(
      Math.max(

        bars[i].high -
          bars[i].low,

        Math.abs(
          bars[i].high -
          bars[i - 1].close
        ),

        Math.abs(
          bars[i].low -
          bars[i - 1].close
        )
      )
    );
  }

  return out;
}

function atrLast(
  bars,
  length = 14
) {

  const ranges =
    trueRangeSeries(bars);

  if (
    ranges.length < length
  ) {
    return NaN;
  }

  let atr =
    sum(
      ranges.slice(
        0,
        length
      )
    ) / length;

  for (
    let i = length;
    i < ranges.length;
    i++
  ) {

    atr =
      (
        atr *
        (length - 1) +
        ranges[i]
      ) / length;
  }

  return atr;
}
// =========================
// RSI
// =========================

function rsiLast(
  values,
  length = 14
) {

  if (
    !Array.isArray(values) ||
    values.length <= length
  ) {
    return NaN;
  }

  let gains = 0;
  let losses = 0;

  for (
    let i = 1;
    i <= length;
    i++
  ) {

    const change =
      values[i] -
      values[i - 1];

    if (change >= 0)
      gains += change;
    else
      losses +=
        Math.abs(change);
  }

  let avgGain =
    gains / length;

  let avgLoss =
    losses / length;

  for (
    let i = length + 1;
    i < values.length;
    i++
  ) {

    const change =
      values[i] -
      values[i - 1];

    const gain =
      Math.max(change, 0);

    const loss =
      Math.max(-change, 0);

    avgGain =
      (
        avgGain *
        (length - 1) +
        gain
      ) / length;

    avgLoss =
      (
        avgLoss *
        (length - 1) +
        loss
      ) / length;
  }

  if (avgLoss === 0)
    return 100;

  const rs =
    avgGain / avgLoss;

  return (
    100 -
    100 / (1 + rs)
  );
}

// =========================
// CMO
// =========================

function cmoLast(
  values,
  length = 9
) {

  if (
    !Array.isArray(values) ||
    values.length <= length
  ) {
    return NaN;
  }

  let up = 0;
  let down = 0;

  const start =
    values.length - length;

  for (
    let i = start;
    i < values.length;
    i++
  ) {

    const change =
      values[i] -
      values[i - 1];

    if (change > 0)
      up += change;

    if (change < 0)
      down +=
        Math.abs(change);
  }

  const total =
    up + down;

  if (total === 0)
    return 0;

  return (
    100 *
    (up - down) /
    total
  );
}

// =========================
// MACD
// =========================

function macdLast(values) {

  if (
    !values ||
    values.length < 35
  ) {
    return {
      macd: NaN,
      signal: NaN,
      histogram: NaN
    };
  }

  const fast =
    emaSeries(values, 12);

  const slow =
    emaSeries(values, 26);

  const macdValues = [];

  const indexes = [];

  for (
    let i = 0;
    i < values.length;
    i++
  ) {

    if (
      Number.isFinite(fast[i]) &&
      Number.isFinite(slow[i])
    ) {

      macdValues.push(
        fast[i] - slow[i]
      );

      indexes.push(i);
    }
  }

  if (
    macdValues.length < 9
  ) {
    return {
      macd: NaN,
      signal: NaN,
      histogram: NaN
    };
  }

  const signalSeries =
    emaSeries(
      macdValues,
      9
    );

  const macd =
    macdValues[
      macdValues.length - 1
    ];

  const signal =
    signalSeries[
      signalSeries.length - 1
    ];

  return {
    macd,
    signal,
    histogram:
      macd - signal
  };
}

// =========================
// STOCHASTIC
// =========================

function stochasticLast(
  bars,
  length = 14
) {

  if (
    !bars ||
    bars.length < length
  ) {
    return {
      k: NaN,
      d: NaN
    };
  }

  const kValues = [];

  const start =
    Math.max(
      length - 1,
      bars.length - 5
    );

  for (
    let i = start;
    i < bars.length;
    i++
  ) {

    const slice =
      bars.slice(
        i - length + 1,
        i + 1
      );

    const high =
      highestHigh(slice);

    const low =
      lowestLow(slice);

    const range =
      high - low;

    const k =
      range === 0
        ? 50
        : (
            (
              bars[i].close -
              low
            ) /
            range
          ) * 100;

    kValues.push(k);
  }

  const k =
    kValues[
      kValues.length - 1
    ];

  const d =
    average(
      kValues.slice(
        -TECH.stochasticSmooth
      )
    );

  return {
    k,
    d
  };
}

// =========================
// WILLIAMS %R
// =========================

function williamsRLast(
  bars,
  length = 14
) {

  if (
    !bars ||
    bars.length < length
  ) {
    return NaN;
  }

  const slice =
    bars.slice(-length);

  const high =
    highestHigh(slice);

  const low =
    lowestLow(slice);

  const close =
    last(slice).close;

  if (high === low)
    return -50;

  return (
    -100 *
    (
      high - close
    ) /
    (
      high - low
    )
  );
}

// =========================
// ROC
// =========================

function rocLast(
  values,
  length = 12
) {

  if (
    !values ||
    values.length <= length
  ) {
    return NaN;
  }

  const current =
    values[
      values.length - 1
    ];

  const previous =
    values[
      values.length -
      1 -
      length
    ];

  return pctChange(
    previous,
    current
  );
}

// =========================
// BOLLINGER BANDS
// =========================

function bollingerLast(
  values,
  length = 20,
  multiplier = 2
) {

  if (
    !values ||
    values.length < length
  ) {
    return {
      middle: NaN,
      upper: NaN,
      lower: NaN,
      widthPct: NaN
    };
  }

  const slice =
    values.slice(-length);

  const middle =
    average(slice);

  const sd =
    standardDeviation(slice);

  const upper =
    middle +
    multiplier * sd;

  const lower =
    middle -
    multiplier * sd;

  return {
    middle,
    upper,
    lower,
    widthPct:
      middle !== 0
        ? (
            (
              upper - lower
            ) /
            Math.abs(middle)
          ) * 100
        : NaN
  };
}

// =========================
// KELTNER CHANNEL
// =========================

function keltnerLast(bars) {

  if (
    !bars ||
    bars.length <
      Math.max(
        TECH.keltnerLen,
        TECH.keltnerAtrLen + 1
      )
  ) {
    return {
      middle: NaN,
      upper: NaN,
      lower: NaN
    };
  }

  const closes =
    bars.map(
      bar => bar.close
    );

  const middle =
    emaLast(
      closes,
      TECH.keltnerLen
    );

  const atr =
    atrLast(
      bars,
      TECH.keltnerAtrLen
    );

  return {
    middle,

    upper:
      middle +
      TECH.keltnerMult *
      atr,

    lower:
      middle -
      TECH.keltnerMult *
      atr
  };
}

// =========================
// OBV
// =========================

function obvLast(bars) {

  if (
    !bars ||
    bars.length < 2
  ) {
    return 0;
  }

  let obv = 0;

  for (
    let i = 1;
    i < bars.length;
    i++
  ) {

    const volume =
      n(bars[i].volume, 0);

    if (
      bars[i].close >
      bars[i - 1].close
    ) {
      obv += volume;
    }

    else if (
      bars[i].close <
      bars[i - 1].close
    ) {
      obv -= volume;
    }
  }

  return obv;
}

// =========================
// MFI
// =========================

function mfiLast(
  bars,
  length = 14
) {

  if (
    !bars ||
    bars.length <= length
  ) {
    return NaN;
  }

  let positive = 0;
  let negative = 0;

  const start =
    bars.length - length;

  for (
    let i = start;
    i < bars.length;
    i++
  ) {

    const currentTypical =
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
      currentTypical *
      n(bars[i].volume, 0);

    if (
      currentTypical >
      previousTypical
    ) {
      positive += flow;
    }

    else if (
      currentTypical <
      previousTypical
    ) {
      negative += flow;
    }
  }

  if (
    positive === 0 &&
    negative === 0
  ) {
    return 50;
  }

  if (negative === 0)
    return 100;

  const ratio =
    positive / negative;

  return (
    100 -
    100 /
    (1 + ratio)
  );
}

// =========================
// VWAP
// =========================

function vwapLast(
  bars,
  lookback = 50
) {

  if (
    !bars ||
    !bars.length
  ) {
    return NaN;
  }

  const slice =
    bars.slice(-lookback);

  let weighted = 0;
  let volumeTotal = 0;

  for (
    const bar of slice
  ) {

    const typical =
      (
        bar.high +
        bar.low +
        bar.close
      ) / 3;

    const volume =
      n(bar.volume, 0);

    weighted +=
      typical * volume;

    volumeTotal += volume;
  }

  if (volumeTotal <= 0) {

    return average(
      slice.map(
        bar =>
          (
            bar.high +
            bar.low +
            bar.close
          ) / 3
      )
    );
  }

  return (
    weighted /
    volumeTotal
  );
}

// =========================
// CANDLE ANALYSIS
// =========================

function candleContext(bar) {

  if (!bar) {
    return {
      direction: 'UNKNOWN',
      bodyRatio: 0,
      upperWickRatio: 0,
      lowerWickRatio: 0
    };
  }

  const range =
    Math.max(
      bar.high - bar.low,
      Number.EPSILON
    );

  const body =
    Math.abs(
      bar.close -
      bar.open
    );

  const upperWick =
    bar.high -
    Math.max(
      bar.open,
      bar.close
    );

  const lowerWick =
    Math.min(
      bar.open,
      bar.close
    ) -
    bar.low;

  return {

    direction:
      bar.close > bar.open
        ? 'BULLISH'
        : bar.close < bar.open
          ? 'BEARISH'
          : 'DOJI',

    bodyRatio:
      body / range,

    upperWickRatio:
      upperWick / range,

    lowerWickRatio:
      lowerWick / range
  };
}

// =========================
// SUPPORT / RESISTANCE
// =========================

function supportResistance(
  bars,
  length = 40
) {

  if (
    !bars ||
    bars.length < 3
  ) {
    return {
      support: NaN,
      resistance: NaN
    };
  }

  // Exclude current bar so a fresh
  // breakout can be detected.
  const slice =
    bars.slice(
      -(length + 1),
      -1
    );

  return {
    support:
      lowestLow(slice),

    resistance:
      highestHigh(slice)
  };
}

// =========================
// SWINGS / MARKET STRUCTURE
// =========================

function findSwings(
  bars,
  left = 3,
  right = 3
) {

  const highs = [];
  const lows = [];

  if (
    !bars ||
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
    i <
      bars.length - right;
    i++
  ) {

    let swingHigh = true;
    let swingLow = true;

    for (
      let j = 1;
      j <= left;
      j++
    ) {

      if (
        bars[i].high <=
        bars[i - j].high
      ) {
        swingHigh = false;
      }

      if (
        bars[i].low >=
        bars[i - j].low
      ) {
        swingLow = false;
      }
    }

    for (
      let j = 1;
      j <= right;
      j++
    ) {

      if (
        bars[i].high <=
        bars[i + j].high
      ) {
        swingHigh = false;
      }

      if (
        bars[i].low >=
        bars[i + j].low
      ) {
        swingLow = false;
      }
    }

    if (swingHigh) {
      highs.push({
        index: i,
        price: bars[i].high,
        time: bars[i].openTime
      });
    }

    if (swingLow) {
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

function marketStructure(bars) {

  const swings =
    findSwings(
      bars,
      TECH.swingLeft,
      TECH.swingRight
    );

  const highs =
    swings.highs.slice(-2);

  const lows =
    swings.lows.slice(-2);

  let structure =
    'NEUTRAL';

  if (
    highs.length >= 2 &&
    lows.length >= 2
  ) {

    const higherHigh =
      highs[1].price >
      highs[0].price;

    const higherLow =
      lows[1].price >
      lows[0].price;

    const lowerHigh =
      highs[1].price <
      highs[0].price;

    const lowerLow =
      lows[1].price <
      lows[0].price;

    if (
      higherHigh &&
      higherLow
    ) {
      structure =
        'BULLISH';
    }

    else if (
      lowerHigh &&
      lowerLow
    ) {
      structure =
        'BEARISH';
    }
  }

  const close =
    last(bars)?.close;

  const lastSwingHigh =
    last(swings.highs)?.price;

  const lastSwingLow =
    last(swings.lows)?.price;

  let bos = 'NONE';

  if (
    Number.isFinite(close) &&
    Number.isFinite(
      lastSwingHigh
    ) &&
    close >
      lastSwingHigh
  ) {
    bos = 'BULLISH_BOS';
  }

  else if (
    Number.isFinite(close) &&
    Number.isFinite(
      lastSwingLow
    ) &&
    close <
      lastSwingLow
  ) {
    bos = 'BEARISH_BOS';
  }

  return {
    structure,
    bos,
    lastSwingHigh,
    lastSwingLow,
    swingHighCount:
      swings.highs.length,
    swingLowCount:
      swings.lows.length
  };
}
// =========================
// ARRAY LAST ITEM
// =========================

function last(values) {

  if (
    !Array.isArray(values) ||
    !values.length
  ) {
    return undefined;
  }

  return values[
    values.length - 1
  ];
}

// =========================
// DMI / ADX
// =========================

function dmiAdx(
  bars,
  length = 14
) {

  if (
    !bars ||
    bars.length <
      length * 2 + 2
  ) {
    return {
      adx: NaN,
      plusDI: NaN,
      minusDI: NaN
    };
  }

  const tr = [];
  const plusDM = [];
  const minusDM = [];

  for (
    let i = 1;
    i < bars.length;
    i++
  ) {

    const current =
      bars[i];

    const previous =
      bars[i - 1];

    const upMove =
      current.high -
      previous.high;

    const downMove =
      previous.low -
      current.low;

    plusDM.push(
      upMove > downMove &&
      upMove > 0
        ? upMove
        : 0
    );

    minusDM.push(
      downMove > upMove &&
      downMove > 0
        ? downMove
        : 0
    );

    tr.push(
      Math.max(

        current.high -
          current.low,

        Math.abs(
          current.high -
          previous.close
        ),

        Math.abs(
          current.low -
          previous.close
        )
      )
    );
  }

  let smoothTR =
    sum(
      tr.slice(
        0,
        length
      )
    );

  let smoothPlus =
    sum(
      plusDM.slice(
        0,
        length
      )
    );

  let smoothMinus =
    sum(
      minusDM.slice(
        0,
        length
      )
    );

  const dxValues = [];

  let plusDI = NaN;
  let minusDI = NaN;

  for (
    let i = length;
    i < tr.length;
    i++
  ) {

    if (i > length) {

      smoothTR =
        smoothTR -
        smoothTR / length +
        tr[i];

      smoothPlus =
        smoothPlus -
        smoothPlus / length +
        plusDM[i];

      smoothMinus =
        smoothMinus -
        smoothMinus / length +
        minusDM[i];
    }

    plusDI =
      smoothTR > 0
        ? (
            100 *
            smoothPlus /
            smoothTR
          )
        : 0;

    minusDI =
      smoothTR > 0
        ? (
            100 *
            smoothMinus /
            smoothTR
          )
        : 0;

    const denominator =
      plusDI +
      minusDI;

    const dx =
      denominator > 0
        ? (
            100 *
            Math.abs(
              plusDI -
              minusDI
            ) /
            denominator
          )
        : 0;

    dxValues.push(dx);
  }

  if (
    dxValues.length <
    length
  ) {
    return {
      adx: NaN,
      plusDI,
      minusDI
    };
  }

  let adx =
    average(
      dxValues.slice(
        0,
        length
      )
    );

  for (
    let i = length;
    i < dxValues.length;
    i++
  ) {

    adx =
      (
        adx *
        (length - 1) +
        dxValues[i]
      ) / length;
  }

  return {
    adx,
    plusDI,
    minusDI
  };
}

// =========================
// VOLUME CONTEXT
// =========================

function volumeContext(bars) {

  if (
    !bars ||
    !bars.length
  ) {
    return {
      current: 0,
      average: 0,
      ratio: NaN,
      spike: false
    };
  }

  const current =
    n(
      last(bars)?.volume,
      0
    );

  const prior =
    bars
      .slice(
        -(TECH.volumeLen + 1),
        -1
      )
      .map(
        bar =>
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
      n(avg, 0),
    ratio,
    spike:
      Number.isFinite(ratio) &&
      ratio >= 1.50
  };
}

// =========================
// VOLATILITY CONTEXT
// =========================

function volatilityContext(bars) {

  const atr =
    atrLast(
      bars,
      TECH.atrLen
    );

  const current =
    last(bars);

  const atrPct =
    current &&
    current.close > 0 &&
    Number.isFinite(atr)
      ? (
          atr /
          current.close
        ) * 100
      : NaN;

  const historicalAtr = [];

  const start =
    Math.max(
      TECH.atrLen + 2,
      bars.length - 50
    );

  for (
    let i = start;
    i <= bars.length;
    i++
  ) {

    const value =
      atrLast(
        bars.slice(0, i),
        TECH.atrLen
      );

    if (
      Number.isFinite(value)
    ) {
      historicalAtr.push(value);
    }
  }

  const avgAtr =
    average(
      historicalAtr
    );

  const atrRatio =
    Number.isFinite(atr) &&
    Number.isFinite(avgAtr) &&
    avgAtr > 0
      ? atr / avgAtr
      : NaN;

  let regime = 'NORMAL';

  if (
    Number.isFinite(atrRatio)
  ) {

    if (atrRatio >= 1.50)
      regime = 'HIGH';

    else if (
      atrRatio <= 0.70
    )
      regime = 'LOW';
  }

  return {
    atr,
    atrPct,
    averageAtr:
      avgAtr,
    atrRatio,
    regime
  };
}

// =========================
// LIQUIDITY SWEEPS
// =========================

function liquidityContext(
  bars,
  lookback =
    TECH.liquidityLookback
) {

  if (
    !bars ||
    bars.length <
      lookback + 1
  ) {
    return {
      bullishSweep: false,
      bearishSweep: false,
      priorHigh: NaN,
      priorLow: NaN
    };
  }

  const current =
    last(bars);

  const prior =
    bars.slice(
      -(lookback + 1),
      -1
    );

  const priorHigh =
    highestHigh(prior);

  const priorLow =
    lowestLow(prior);

  const bullishSweep =
    current.low <
      priorLow &&
    current.close >
      priorLow;

  const bearishSweep =
    current.high >
      priorHigh &&
    current.close <
      priorHigh;

  return {
    bullishSweep,
    bearishSweep,
    priorHigh,
    priorLow
  };
}

// =========================
// FAIR VALUE GAP
// =========================

function fvgContext(bars) {

  if (
    !bars ||
    bars.length < 3
  ) {
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
    bars[
      bars.length - 3
    ];

  const third =
    last(bars);

  const bullish =
    third.low >
    first.high;

  const bearish =
    third.high <
    first.low;

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

// =========================
// FIBONACCI
// =========================

function fibonacciContext(
  bars,
  lookback =
    TECH.fibLookback
) {

  if (
    !bars ||
    bars.length < 10
  ) {
    return null;
  }

  const slice =
    bars.slice(
      -lookback
    );

  const high =
    highestHigh(slice);

  const low =
    lowestLow(slice);

  const range =
    high - low;

  if (
    !Number.isFinite(range) ||
    range <= 0
  ) {
    return null;
  }

  const current =
    last(bars).close;

  return {

    swingHigh: high,
    swingLow: low,
    current,

    retracementFromHigh: {

      r382:
        high -
        range * 0.382,

      r500:
        high -
        range * 0.500,

      r618:
        high -
        range * 0.618,

      r786:
        high -
        range * 0.786
    },

    retracementFromLow: {

      r382:
        low +
        range * 0.382,

      r500:
        low +
        range * 0.500,

      r618:
        low +
        range * 0.618,

      r786:
        low +
        range * 0.786
    },

    extensionUp: {

      e1272:
        low +
        range * 1.272,

      e1618:
        low +
        range * 1.618
    },

    extensionDown: {

      e1272:
        high -
        range * 1.272,

      e1618:
        high -
        range * 1.618
    }
  };
}

// =========================
// SUPERTREND
// =========================

function supertrendContext(
  bars,
  atrLength = 10,
  multiplier = 3
) {

  if (
    !bars ||
    bars.length <
      atrLength + 5
  ) {
    return {
      direction: 'UNKNOWN',
      value: NaN
    };
  }

  let finalUpper = NaN;
  let finalLower = NaN;
  let supertrend = NaN;
  let previousSupertrend = NaN;

  for (
    let i = atrLength + 1;
    i < bars.length;
    i++
  ) {

    const slice =
      bars.slice(
        0,
        i + 1
      );

    const atr =
      atrLast(
        slice,
        atrLength
      );

    if (
      !Number.isFinite(atr)
    ) {
      continue;
    }

    const current =
      bars[i];

    const previous =
      bars[i - 1];

    const hl2 =
      (
        current.high +
        current.low
      ) / 2;

    const basicUpper =
      hl2 +
      multiplier * atr;

    const basicLower =
      hl2 -
      multiplier * atr;

    if (
      !Number.isFinite(
        finalUpper
      )
    ) {

      finalUpper =
        basicUpper;

      finalLower =
        basicLower;

      supertrend =
        current.close >= hl2
          ? finalLower
          : finalUpper;

      previousSupertrend =
        supertrend;

      continue;
    }

    finalUpper =
      (
        basicUpper <
          finalUpper ||
        previous.close >
          finalUpper
      )
        ? basicUpper
        : finalUpper;

    finalLower =
      (
        basicLower >
          finalLower ||
        previous.close <
          finalLower
      )
        ? basicLower
        : finalLower;

    if (
      previousSupertrend ===
      finalUpper
    ) {

      supertrend =
        current.close <=
          finalUpper
          ? finalUpper
          : finalLower;
    }

    else {

      supertrend =
        current.close >=
          finalLower
          ? finalLower
          : finalUpper;
    }

    previousSupertrend =
      supertrend;
  }

  const close =
    last(bars).close;

  return {

    value:
      supertrend,

    direction:
      Number.isFinite(
        supertrend
      )
        ? (
            close >
              supertrend
              ? 'BULL'
              : 'BEAR'
          )
        : 'UNKNOWN'
  };
}

// =========================
// ICHIMOKU
// =========================

function ichimokuContext(bars) {

  if (
    !bars ||
    bars.length < 52
  ) {
    return {
      tenkan: NaN,
      kijun: NaN,
      spanA: NaN,
      spanB: NaN,
      cloudTop: NaN,
      cloudBottom: NaN,
      bias: 'UNKNOWN'
    };
  }

  function midpoint(length) {

    const slice =
      bars.slice(-length);

    return (
      highestHigh(slice) +
      lowestLow(slice)
    ) / 2;
  }

  const tenkan =
    midpoint(9);

  const kijun =
    midpoint(26);

  const spanA =
    (
      tenkan +
      kijun
    ) / 2;

  const spanB =
    midpoint(52);

  const cloudTop =
    Math.max(
      spanA,
      spanB
    );

  const cloudBottom =
    Math.min(
      spanA,
      spanB
    );

  const close =
    last(bars).close;

  let bias = 'MIXED';

  if (
    close > cloudTop &&
    tenkan > kijun
  ) {
    bias = 'BULL';
  }

  else if (
    close < cloudBottom &&
    tenkan < kijun
  ) {
    bias = 'BEAR';
  }

  return {
    tenkan,
    kijun,
    spanA,
    spanB,
    cloudTop,
    cloudBottom,
    bias
  };
}

// =========================
// CHoCH
// =========================

function chochContext(bars) {

  const swings =
    findSwings(
      bars,
      TECH.swingLeft,
      TECH.swingRight
    );

  const highs =
    swings.highs.slice(-2);

  const lows =
    swings.lows.slice(-2);

  const current =
    last(bars);

  if (
    !current ||
    highs.length < 2 ||
    lows.length < 2
  ) {
    return {
      bullish: false,
      bearish: false,
      direction: 'NONE'
    };
  }

  const previousBearish =
    highs[1].price <
      highs[0].price &&
    lows[1].price <
      lows[0].price;

  const previousBullish =
    highs[1].price >
      highs[0].price &&
    lows[1].price >
      lows[0].price;

  const bullish =
    previousBearish &&
    current.close >
      highs[1].price;

  const bearish =
    previousBullish &&
    current.close <
      lows[1].price;

  return {
    bullish,
    bearish,
    direction:
      bullish
        ? 'BULLISH_CHOCH'
        : bearish
          ? 'BEARISH_CHOCH'
          : 'NONE'
  };
}

// =========================
// TREND CONTEXT
// =========================

function trendContext(bars) {

  const closes =
    bars.map(
      bar => bar.close
    );

  const close =
    last(closes);

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
    closes.length >=
      TECH.emaMacro
      ? emaLast(
          closes,
          TECH.emaMacro
        )
      : NaN;

  let alignment =
    'MIXED';

  if (
    Number.isFinite(ema50) &&
    close > ema9 &&
    ema9 > ema21 &&
    ema21 > ema50
  ) {
    alignment = 'BULL';
  }

  else if (
    Number.isFinite(ema50) &&
    close < ema9 &&
    ema9 < ema21 &&
    ema21 < ema50
  ) {
    alignment = 'BEAR';
  }

  let macro =
    'UNKNOWN';

  if (
    Number.isFinite(ema200)
  ) {

    macro =
      close > ema200
        ? 'BULL'
        : close < ema200
          ? 'BEAR'
          : 'FLAT';
  }

  else if (
    Number.isFinite(ema100)
  ) {

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

// =========================
// MOMENTUM CONTEXT
// =========================

function momentumContext(bars) {

  const closes =
    bars.map(
      bar => bar.close
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

    macd:
      macdLast(closes),

    stochastic:
      stochasticLast(
        bars,
        TECH.stochasticLen
      ),

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
// =========================
// COMPLETE TECHNICAL INTELLIGENCE
// =========================

function buildTechnicalIntelligence(bars) {

  if (
    !Array.isArray(bars) ||
    bars.length < CORE_MIN_HISTORY
  ) {
    return null;
  }

  const closes =
    bars.map(
      bar => bar.close
    );

  const currentBar =
    last(bars);

  const trend =
    trendContext(bars);

  const momentum =
    momentumContext(bars);

  const volatility =
    volatilityContext(bars);

  const dmi =
    dmiAdx(
      bars,
      TECH.adxLen
    );

  const bollinger =
    bollingerLast(
      closes,
      TECH.bbLen,
      TECH.bbStd
    );

  const keltner =
    keltnerLast(bars);

  const volume =
    volumeContext(bars);

  const structure =
    marketStructure(bars);

  const liquidity =
    liquidityContext(bars);

  const fvg =
    fvgContext(bars);

  const fibonacci =
    fibonacciContext(bars);

  const candle =
    candleContext(currentBar);

  const sr =
    supportResistance(
      bars,
      TECH.srLen
    );

  const supertrend =
    supertrendContext(bars);

  const ichimoku =
    ichimokuContext(bars);

  const choch =
    chochContext(bars);

  const vwap =
    vwapLast(
      bars,
      TECH.vwapLookback
    );

  const obv =
    obvLast(bars);

  const mfi =
    mfiLast(
      bars,
      TECH.mfiLen
    );

  let bullishScore = 0;
  let bearishScore = 0;

  // Trend
  if (
    trend.alignment === 'BULL'
  ) bullishScore += 2;

  if (
    trend.alignment === 'BEAR'
  ) bearishScore += 2;

  if (
    trend.macro === 'BULL'
  ) bullishScore += 1;

  if (
    trend.macro === 'BEAR'
  ) bearishScore += 1;

  // Supertrend
  if (
    supertrend.direction === 'BULL'
  ) bullishScore += 1;

  if (
    supertrend.direction === 'BEAR'
  ) bearishScore += 1;

  // Ichimoku
  if (
    ichimoku.bias === 'BULL'
  ) bullishScore += 1;

  if (
    ichimoku.bias === 'BEAR'
  ) bearishScore += 1;

  // DMI
  if (
    Number.isFinite(dmi.adx) &&
    dmi.adx >= 20
  ) {

    if (
      dmi.plusDI >
      dmi.minusDI
    ) {
      bullishScore += 1;
    }

    if (
      dmi.minusDI >
      dmi.plusDI
    ) {
      bearishScore += 1;
    }
  }

  // RSI
  if (
    Number.isFinite(
      momentum.rsi
    )
  ) {

    if (
      momentum.rsi >= 52 &&
      momentum.rsi <= 75
    ) {
      bullishScore += 1;
    }

    if (
      momentum.rsi <= 48 &&
      momentum.rsi >= 25
    ) {
      bearishScore += 1;
    }
  }

  // CMO
  if (
    Number.isFinite(
      momentum.cmo
    )
  ) {

    if (
      momentum.cmo > 0
    ) bullishScore += 1;

    if (
      momentum.cmo < 0
    ) bearishScore += 1;
  }

  // MACD
  if (
    Number.isFinite(
      momentum.macd?.histogram
    )
  ) {

    if (
      momentum.macd.histogram > 0
    ) {
      bullishScore += 1;
    }

    if (
      momentum.macd.histogram < 0
    ) {
      bearishScore += 1;
    }
  }

  // Market structure
  if (
    structure.structure ===
      'BULLISH'
  ) {
    bullishScore += 1;
  }

  if (
    structure.structure ===
      'BEARISH'
  ) {
    bearishScore += 1;
  }

  if (
    structure.bos ===
      'BULLISH_BOS'
  ) {
    bullishScore += 1;
  }

  if (
    structure.bos ===
      'BEARISH_BOS'
  ) {
    bearishScore += 1;
  }

  // CHoCH
  if (
    choch.bullish
  ) bullishScore += 2;

  if (
    choch.bearish
  ) bearishScore += 2;

  // Liquidity sweep
  if (
    liquidity.bullishSweep
  ) bullishScore += 1;

  if (
    liquidity.bearishSweep
  ) bearishScore += 1;

  // Candle
  if (
    candle.direction ===
      'BULLISH' &&
    candle.bodyRatio >= 0.50
  ) {
    bullishScore += 1;
  }

  if (
    candle.direction ===
      'BEARISH' &&
    candle.bodyRatio >= 0.50
  ) {
    bearishScore += 1;
  }

  // VWAP
  if (
    Number.isFinite(vwap)
  ) {

    if (
      currentBar.close > vwap
    ) {
      bullishScore += 1;
    }

    if (
      currentBar.close < vwap
    ) {
      bearishScore += 1;
    }
  }

  const bias =
    bullishScore >
      bearishScore
      ? 'BULL'
      : bearishScore >
          bullishScore
        ? 'BEAR'
        : 'NEUTRAL';

  return {

    barTime:
      currentBar.openTime,

    price:
      currentBar.close,

    bias,

    score: {
      bullish:
        bullishScore,
      bearish:
        bearishScore
    },

    trend,
    momentum,
    volatility,
    dmi,
    bollinger,
    keltner,
    volume,
    structure,
    liquidity,
    fvg,
    fibonacci,
    candle,
    supportResistance: sr,
    supertrend,
    ichimoku,
    choch,
    vwap,
    obv,
    mfi
  };
}

// =========================
// TWELVE DATA SYMBOL FORMAT
// =========================

function toTwelveSymbol(symbol) {

  if (symbol === 'XAUUSD')
    return 'XAU/USD';

  if (
    typeof symbol !==
      'string' ||
    symbol.length !== 6
  ) {
    return symbol;
  }

  return (
    symbol.slice(0, 3) +
    '/' +
    symbol.slice(3, 6)
  );
}

// =========================
// TWELVE DATA TIMEFRAME
// =========================

function toTwelveInterval(
  interval
) {

  if (
    interval === '15m'
  ) return '15min';

  if (
    interval === '1h'
  ) return '1h';

  if (
    interval === '4h'
  ) return '4h';

  return interval;
}

// =========================
// TWELVE DATA ERROR CHECK
// =========================

function assertTwelveResponse(data) {

  if (!data) {
    throw new Error(
      'Twelve Data returned empty response'
    );
  }

  if (
    data.status === 'error'
  ) {
    throw new Error(
      data.message ||
      data.code ||
      'Twelve Data API error'
    );
  }
}

// =========================
// FETCH OHLC FROM TWELVE DATA
// =========================

async function fetchBars(
  symbol,
  interval = TIMEFRAME,
  outputSize = HISTORY_LIMIT
) {

  if (
    !TWELVE_DATA_API_KEY
  ) {
    throw new Error(
      'TWELVE_DATA_API_KEY is missing'
    );
  }

  const twelveSymbol =
    toTwelveSymbol(symbol);

  const twelveInterval =
    toTwelveInterval(interval);

  const response =
    await http.get(
      `${TWELVE_BASE}/time_series`,
      {
        params: {
          symbol:
            twelveSymbol,

          interval:
            twelveInterval,

          outputsize:
            outputSize,

          order:
            'asc',

          timezone:
            'UTC',

          apikey:
            TWELVE_DATA_API_KEY
        }
      }
    );

  const data =
    response.data;

  assertTwelveResponse(data);

  if (
    !Array.isArray(
      data.values
    )
  ) {
    throw new Error(
      `No OHLC values for ${symbol} ${interval}`
    );
  }

  const bars =
    data.values.map(
      item => ({

        openTime:
          item.datetime,

        open:
          n(
            item.open,
            NaN
          ),

        high:
          n(
            item.high,
            NaN
          ),

        low:
          n(
            item.low,
            NaN
          ),

        close:
          n(
            item.close,
            NaN
          ),

        volume:
          n(
            item.volume,
            0
          ),

        isOpen:
          false
      })
    );

  return normalizeBars(
    bars
  );
}

// =========================
// FETCH CURRENT PRICE
// =========================

async function fetchCurrentPrice(
  symbol
) {

  if (
    !TWELVE_DATA_API_KEY
  ) {
    throw new Error(
      'TWELVE_DATA_API_KEY is missing'
    );
  }

  const response =
    await http.get(
      `${TWELVE_BASE}/price`,
      {
        params: {
          symbol:
            toTwelveSymbol(
              symbol
            ),

          apikey:
            TWELVE_DATA_API_KEY
        }
      }
    );

  const data =
    response.data;

  assertTwelveResponse(data);

  const price =
    n(
      data.price,
      NaN
    );

  if (
    !Number.isFinite(price) ||
    price <= 0
  ) {
    throw new Error(
      `Invalid market price for ${symbol}`
    );
  }

  return price;
}

// =========================
// FETCH QUOTE
// =========================

async function fetchQuote(
  symbol
) {

  if (
    !TWELVE_DATA_API_KEY
  ) {
    throw new Error(
      'TWELVE_DATA_API_KEY is missing'
    );
  }

  try {

    const response =
      await http.get(
        `${TWELVE_BASE}/quote`,
        {
          params: {

            symbol:
              toTwelveSymbol(
                symbol
              ),

            apikey:
              TWELVE_DATA_API_KEY
          }
        }
      );

    const data =
      response.data;

    assertTwelveResponse(data);

    const close =
      n(
        data.close,
        NaN
      );

    const bid =
      n(
        data.bid,
        NaN
      );

    const ask =
      n(
        data.ask,
        NaN
      );

    const fallbackPrice =
      Number.isFinite(close)
        ? close
        : await fetchCurrentPrice(
            symbol
          );

    let finalBid = bid;
    let finalAsk = ask;

    // بعض استجابات Twelve Data
    // لا ترجع bid / ask للفوركس
    if (
      !Number.isFinite(
        finalBid
      ) ||
      !Number.isFinite(
        finalAsk
      ) ||
      finalBid <= 0 ||
      finalAsk <= 0
    ) {

      finalBid =
        fallbackPrice;

      finalAsk =
        fallbackPrice;
    }

    const mid =
      (
        finalBid +
        finalAsk
      ) / 2;

    const quote = {

      symbol,

      bid:
        finalBid,

      ask:
        finalAsk,

      mid,

      spread:
        Math.max(
          0,
          finalAsk -
          finalBid
        ),

      fetchedAt:
        new Date()
    };

    state.latestQuotes.set(
      symbol,
      quote
    );

    return quote;

  }

  catch (error) {

    state.lastMarketError =
      safeError(error);

    throw error;
  }
}

// =========================
// EXECUTION PRICE
// =========================

function entryExecutionPrice(
  direction,
  quote
) {

  if (
    direction === 'BUY'
  ) {
    return quote.ask;
  }

  return quote.bid;
}

function exitExecutionPrice(
  direction,
  quote
) {

  if (
    direction === 'BUY'
  ) {
    return quote.bid;
  }

  return quote.ask;
}

// =========================
// MARKET SESSION
// =========================

function getMarketSession(
  date = new Date()
) {

  const hour =
    date.getUTCHours();

  const sessions = [];

  // تقريب UTC
  if (
    hour >= 0 &&
    hour < 9
  ) {
    sessions.push('ASIA');
  }

  if (
    hour >= 7 &&
    hour < 16
  ) {
    sessions.push('LONDON');
  }

  if (
    hour >= 12 &&
    hour < 21
  ) {
    sessions.push('NEW_YORK');
  }

  if (
    sessions.length === 0
  ) {
    sessions.push('OFF_HOURS');
  }

  return {

    utcHour:
      hour,

    sessions,

    londonNewYorkOverlap:
      hour >= 12 &&
      hour < 16
  };
}

// =========================
// ECONOMIC NEWS
// =========================

function normalizeNewsCurrency(
  value
) {

  return String(
    value || ''
  )
    .trim()
    .toUpperCase();
}

function normalizeNewsImpact(
  value
) {

  const text =
    String(
      value || ''
    ).toLowerCase();

  if (
    text.includes('high') ||
    text.includes('red')
  ) {
    return 'HIGH';
  }

  if (
    text.includes('medium') ||
    text.includes('orange')
  ) {
    return 'MEDIUM';
  }

  return 'LOW';
}

async function fetchEconomicNews() {

  try {

    const response =
      await http.get(
        'https://nfs.faireconomy.media/ff_calendar_thisweek.json',
        {
          timeout: 15000
        }
      );

    const rows =
      Array.isArray(
        response.data
      )
        ? response.data
        : [];

    economicNews =
      rows
        .map(item => {

          const timestamp =
            new Date(
              item.date ||
              item.datetime ||
              item.time ||
              item.timestamp
            );

          return {

            title:
              String(
                item.title ||
                item.event ||
                ''
              ),

            country:
              normalizeNewsCurrency(
                item.country
              ),

            impact:
              normalizeNewsImpact(
                item.impact
              ),

            time:
              timestamp
          };
        })

        .filter(item =>
          item.title &&
          Number.isFinite(
            item.time.getTime()
          )
        );

    console.log(
      `[NEWS] loaded ${economicNews.length} events`
    );

  }

  catch (error) {

    console.error(
      '[NEWS] fetch failed:',
      safeError(error)
    );
  }
}

// =========================
// SYMBOL CURRENCIES
// =========================

function symbolCurrencies(
  symbol
) {

  if (
    symbol === 'XAUUSD'
  ) {
    return [
      'XAU',
      'USD'
    ];
  }

  if (
    typeof symbol !==
      'string' ||
    symbol.length < 6
  ) {
    return [];
  }

  return [
    symbol.slice(0, 3),
    symbol.slice(3, 6)
  ];
}

// =========================
// HIGH IMPACT NEWS BLOCK
// =========================

function getNewsBlock(
  symbol,
  now = new Date()
) {

  const currencies =
    symbolCurrencies(
      symbol
    );

  const windowMs =
    30 * 60 * 1000;

  const nowMs =
    now.getTime();

  const blocking =
    economicNews.filter(event => {

      if (
        event.impact !==
          'HIGH'
      ) {
        return false;
      }

      if (
        !currencies.includes(
          event.country
        )
      ) {
        return false;
      }

      const diff =
        Math.abs(
          event.time.getTime() -
          nowMs
        );

      return diff <=
        windowMs;
    });

  return {

    blocked:
      blocking.length > 0,

    events:
      blocking.slice(0, 5)
  };
}

// =========================
// CURRENCY STRENGTH METER
// =========================

function calculateCurrencyStrength() {

  const values =
    new Map();

  const counts =
    new Map();

  for (
    const [
      symbol,
      pair
    ] of state.pairState
  ) {

    // XAU لا يدخل في حساب
    // قوة العملات الأساسية
    if (
      symbol === 'XAUUSD'
    ) {
      continue;
    }

    const bars =
      pair?.bars15m;

    if (
      !Array.isArray(bars) ||
      bars.length < 13
    ) {
      continue;
    }

    const current =
      last(bars)?.close;

    const previous =
      bars[
        bars.length - 13
      ]?.close;

    const change =
      pctChange(
        previous,
        current
      );

    if (
      !Number.isFinite(change)
    ) {
      continue;
    }

    const base =
      symbol.slice(0, 3);

    const quote =
      symbol.slice(3, 6);

    values.set(
      base,
      n(
        values.get(base),
        0
      ) + change
    );

    counts.set(
      base,
      n(
        counts.get(base),
        0
      ) + 1
    );

    values.set(
      quote,
      n(
        values.get(quote),
        0
      ) - change
    );

    counts.set(
      quote,
      n(
        counts.get(quote),
        0
      ) + 1
    );
  }

  const result = {};

  for (
    const [
      currency,
      value
    ] of values
  ) {

    const count =
      Math.max(
        1,
        n(
          counts.get(currency),
          1
        )
      );

    result[currency] =
      value / count;
  }

  return result;
}

function pairStrengthContext(
  symbol
) {

  const strength =
    calculateCurrencyStrength();

  if (
    symbol === 'XAUUSD'
  ) {

    return {
      base: 'XAU',
      quote: 'USD',
      baseStrength: null,
      quoteStrength:
        n(
          strength.USD,
          0
        ),
      differential: null
    };
  }

  const base =
    symbol.slice(0, 3);

  const quote =
    symbol.slice(3, 6);

  const baseStrength =
    n(
      strength[base],
      0
    );

  const quoteStrength =
    n(
      strength[quote],
      0
    );

  return {
    base,
    quote,
    baseStrength,
    quoteStrength,
    differential:
      baseStrength -
      quoteStrength
  };
}

// =========================
// INITIALIZE ONE SYMBOL
// =========================

async function initializeSymbol(
  symbol
) {

  const bars15m =
    await fetchBars(
      symbol,
      '15m',
      HISTORY_LIMIT
    );

  if (
    bars15m.length <
    CORE_MIN_HISTORY
  ) {
    throw new Error(
      `${symbol}: insufficient 15m history`
    );
  }

  const closed =
    closedBarsOnly(
      bars15m,
      '15m'
    );

  if (
    !closed.length
  ) {
    throw new Error(
      `${symbol}: no closed 15m bars`
    );
  }

  const latest =
    last(closed);

  state.pairState.set(
    symbol,
    {

      symbol,

      bars15m:
        closed,

      bars1h: [],

      bars4h: [],

      lastClosedBarTime:
        latest.openTime,

      initializedAt:
        new Date(),

      lastRefreshAt:
        new Date(),

      lastError:
        null
    }
  );

  return true;
}

// =========================
// INITIALIZE MARKET
// =========================

async function initializeMarket() {

  console.log(
    '[MARKET] initializing Twelve Data...'
  );

  if (
    !TWELVE_DATA_API_KEY
  ) {
    throw new Error(
      'TWELVE_DATA_API_KEY is required'
    );
  }

  let success = 0;

  // Sequential initialization is intentional.
  // It reduces the chance of API rate-limit bursts.
  for (
    const symbol of INSTRUMENTS
  ) {

    try {

      await initializeSymbol(
        symbol
      );

      success++;

      console.log(
        `[MARKET] ${symbol} ready`
      );

    }

    catch (error) {

      console.error(
        `[MARKET] ${symbol} failed:`,
        safeError(error)
      );
    }

    await sleep(8000);
  }

  if (success === 0) {

    throw new Error(
      'Market initialization failed for all symbols'
    );
  }

  state.marketReady =
    true;

  console.log(
    `[MARKET] ${success}/${INSTRUMENTS.length} symbols initialized`
  );
      }
// =========================
// MONGODB SCHEMAS
// =========================

const accountSchema =
  new mongoose.Schema(
    {
      accountKey: {
        type: String,
        unique: true,
        required: true
      },

      balance: {
        type: Number,
        required: true
      },

      startingBalance: {
        type: Number,
        required: true
      },

      version: String,
      mode: String
    },
    {
      timestamps: true
    }
  );

const tradeSchema =
  new mongoose.Schema(
    {
      tradeId: {
        type: String,
        unique: true,
        required: true
      },

      symbol: String,

      direction: String,

      status: String,

      entryPrice: Number,

      stopLoss: Number,

      initialStopLoss: Number,

      partialTargetPrice: Number,

      quantity: Number,

      initialQuantity: Number,

      riskAmount: Number,

      riskPct: Number,

      confidence: Number,

      entryReason: String,

      managementReason: String,

      openedAt: Date,

      closedAt: Date,

      exitPrice: Number,

      realizedPartialPnl: {
        type: Number,
        default: 0
      },

      totalPnl: {
        type: Number,
        default: 0
      },

      resultR: Number,

      partialClosed: {
        type: Boolean,
        default: false
      },

      trailingLevelR: {
        type: Number,
        default: 0
      },

      breakEvenActivated: {
        type: Boolean,
        default: false
      },

      maxFavorablePrice: Number,

      maxAdversePrice: Number,

      mfeR: Number,

      maeR: Number,

      aiEntryDecision:
        mongoose.Schema.Types.Mixed,

      technicalSnapshot:
        mongoose.Schema.Types.Mixed
    },
    {
      timestamps: true
    }
  );

const journalSchema =
  new mongoose.Schema(
    {
      type: String,
      symbol: String,
      tradeId: String,
      message: String,
      data:
        mongoose.Schema.Types.Mixed,
      createdAt: {
        type: Date,
        default: Date.now
      }
    },
    {
      collection:
        JOURNAL_COLLECTION
    }
  );

const Account =
  mongoose.models.LomyForexAccountV15 ||
  mongoose.model(
    'LomyForexAccountV15',
    accountSchema
  );

const Trade =
  mongoose.models.LomyForexTradeV15 ||
  mongoose.model(
    'LomyForexTradeV15',
    tradeSchema
  );

const Journal =
  mongoose.models.LomyForexJournalV15 ||
  mongoose.model(
    'LomyForexJournalV15',
    journalSchema
  );

// =========================
// JOURNAL
// =========================

async function journal(
  type,
  data = {}
) {

  state.journalEvents++;

  try {

    if (!state.mongoReady)
      return;

    await Journal.create({
      type,
      symbol:
        data.symbol || null,
      tradeId:
        data.tradeId || null,
      message:
        data.message || '',
      data
    });

  }

  catch (error) {

    console.error(
      '[JOURNAL]',
      safeError(error)
    );
  }
}

// =========================
// INITIALIZE MONGODB
// =========================

async function initMongo() {

  if (!MONGODB_URI) {

    throw new Error(
      'MONGODB_URI is required'
    );
  }

  await mongoose.connect(
    MONGODB_URI
  );

  state.mongoReady = true;

  console.log(
    '[MONGO] connected'
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

        balance:
          PAPER.startingBalance,

        startingBalance:
          PAPER.startingBalance,

        version:
          VERSION,

        mode:
          MODE
      });

    console.log(
      '[ACCOUNT] new paper account created'
    );
  }

  else {

    account.version =
      VERSION;

    account.mode =
      MODE;

    await account.save();

    console.log(
      `[ACCOUNT] restored balance ${fmtMoney(account.balance)}`
    );
  }
}

// =========================
// RESTORE OPEN TRADES
// =========================

async function restoreOpenTrades() {

  if (!state.mongoReady)
    return;

  const trades =
    await Trade.find({
      status: 'OPEN'
    }).lean();

  for (
    const raw of trades
  ) {

    const trade = {
      ...raw,

      initialQuantity:
        Number.isFinite(
          Number(
            raw.initialQuantity
          )
        )
          ? Number(
              raw.initialQuantity
            )
          : Number(
              raw.quantity
            ),

      realizedPartialPnl:
        n(
          raw.realizedPartialPnl,
          0
        ),

      trailingLevelR:
        n(
          raw.trailingLevelR,
          0
        )
    };

    state.openTrades.set(
      trade.tradeId,
      trade
    );
  }

  console.log(
    `[TRADES] restored ${state.openTrades.size} open trades`
  );
}

// =========================
// SAVE ACCOUNT
// =========================

async function saveAccount() {

  if (!account)
    return;

  await account.save();
}

// =========================
// TELEGRAM SEND
// =========================

async function sendTelegram(
  message
) {

  if (
    !telegramBot ||
    !state.telegramReady
  ) {
    return;
  }

  const chatId =
    process.env.TELEGRAM_CHAT_ID;

  if (!chatId)
    return;

  try {

    await telegramBot.telegram.sendMessage(
      chatId,
      message
    );

  }

  catch (error) {

    console.error(
      '[TELEGRAM SEND]',
      safeError(error)
    );
  }
}

// =========================
// TELEGRAM STATUS TEXT
// =========================

function telegramStatusText() {

  const balance =
    account
      ? fmtMoney(
          account.balance
        )
      : 'n/a';

  return [
    VERSION,
    '',
    `Mode: ${MODE}`,
    `Balance: ${balance}`,
    `Open trades: ${state.openTrades.size}`,
    `Market: ${state.marketReady ? 'READY' : 'NOT READY'}`,
    `Mongo: ${state.mongoReady ? 'READY' : 'NOT READY'}`,
    `Gemini: ${state.geminiReady ? 'READY' : 'NOT READY'}`,
    `Scanned bars: ${state.scannedBars}`,
    `Executed: ${state.executedSignals}`,
    `Skipped: ${state.skippedSignals}`
  ].join('\n');
}

// =========================
// INITIALIZE TELEGRAM
// =========================

async function initTelegram() {

  if (
    !TELEGRAM_BOT_TOKEN
  ) {

    console.log(
      '[TELEGRAM] token missing - disabled'
    );

    return;
  }

  try {

    telegramBot =
      new Telegraf(
        TELEGRAM_BOT_TOKEN
      );

    telegramBot.start(
      async ctx => {

        await ctx.reply(
          telegramStatusText()
        );
      }
    );

    telegramBot.command(
      'status',
      async ctx => {

        await ctx.reply(
          telegramStatusText()
        );
      }
    );

    telegramBot.command(
      'balance',
      async ctx => {

        await ctx.reply(
          account
            ? `Balance: ${fmtMoney(account.balance)}`
            : 'Account not ready'
        );
      }
    );

    telegramBot.command(
      'positions',
      async ctx => {

        if (
          state.openTrades.size === 0
        ) {

          await ctx.reply(
            'No open trades.'
          );

          return;
        }

        const lines = [];

        for (
          const trade of
            state.openTrades.values()
        ) {

          lines.push(
            `${trade.symbol} ${trade.direction} | Entry ${fmtPrice(trade.entryPrice, trade.symbol)} | SL ${fmtPrice(trade.stopLoss, trade.symbol)} | Qty ${n(trade.quantity).toFixed(4)}`
          );
        }

        await ctx.reply(
          lines.join('\n')
        );
      }
    );

await telegramBot.telegram.getMe();

telegramBot.launch()
  .catch(error => {
    state.telegramReady = false;

    console.error(
      '[TELEGRAM POLLING]',
      safeError(error)
    );
  });

state.telegramReady = true;

console.log(
  '[TELEGRAM] ready'
);
    }
catch (error) {
  state.telegramReady = false;

  console.error(
    '[TELEGRAM]',
    safeError(error)
  );
}
}

// =========================
// GEMINI RATE CONTROL
// =========================

async function waitForGeminiSlot() {

  const elapsed =
    Date.now() -
    lastGeminiCallAt;

  const wait =
    GEMINI_MIN_CALL_GAP_MS -
    elapsed;

  if (wait > 0) {
    await sleep(wait);
  }

  lastGeminiCallAt =
    Date.now();
}

// =========================
// GEMINI JSON EXTRACTION
// =========================

function extractJson(text) {

  if (
    typeof text !== 'string'
  ) {
    throw new Error(
      'Gemini returned invalid text'
    );
  }

  let cleaned =
    text.trim();

  cleaned =
    cleaned.replace(
      /^```json/i,
      ''
    );

  cleaned =
    cleaned.replace(
      /^```/i,
      ''
    );

  cleaned =
    cleaned.replace(
      /```$/,
      ''
    );

  cleaned =
    cleaned.trim();

  try {

    return JSON.parse(
      cleaned
    );

  }

  catch (_) {

    const start =
      cleaned.indexOf('{');

    const end =
      cleaned.lastIndexOf('}');

    if (
      start === -1 ||
      end === -1 ||
      end <= start
    ) {
      throw new Error(
        'No JSON object in Gemini response'
      );
    }

    return JSON.parse(
      cleaned.slice(
        start,
        end + 1
      )
    );
  }
}

// =========================
// GEMINI REQUEST
// =========================

async function callGemini(
  prompt
) {

  if (
    !GEMINI_API_KEY
  ) {

    state.geminiReady =
      false;

    throw new Error(
      'GEMINI_API_KEY is missing'
    );
  }

  await waitForGeminiSlot();

  try {

    const url =
      `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(GEMINI_MODEL)}:generateContent`;

    const response =
      await http.post(
        url,
        {
          contents: [
            {
              role: 'user',
              parts: [
                {
                  text: prompt
                }
              ]
            }
          ],

          generationConfig: {
            temperature:
              AI.temperature,

            responseMimeType:
              'application/json'
          }
        },
        {
          params: {
            key:
              GEMINI_API_KEY
          },

          timeout:
            AI.timeoutMs
        }
      );

    const text =
      response?.data
        ?.candidates?.[0]
        ?.content?.parts
        ?.map(
          part =>
            part.text || ''
        )
        .join('');

    if (!text) {
      throw new Error(
        'Gemini returned empty response'
      );
    }

    state.geminiReady =
      true;

    state.lastAiError =
      null;

    return extractJson(
      text
    );

  }

  catch (error) {

    state.geminiReady =
      false;

    state.lastAiError =
      safeError(error);

    throw error;
  }
}

// =========================
// RECENT CLOSED-TRADE MEMORY
// =========================

async function getAiMemory(
  symbol
) {

  if (!state.mongoReady)
    return [];

  const rows =
    await Trade.find({
      status: 'CLOSED'
    })
      .sort({
        closedAt: -1
      })
      .limit(
        AI.memoryClosedTrades
      )
      .lean();

  return rows.map(
    trade => ({

      symbol:
        trade.symbol,

      sameSymbol:
        trade.symbol ===
        symbol,

      direction:
        trade.direction,

      confidence:
        trade.confidence,

      resultR:
        n(
          trade.resultR,
          0
        ),

      pnl:
        n(
          trade.totalPnl,
          0
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

      entryReason:
        String(
          trade.entryReason ||
          ''
        ).slice(
          0,
          300
        ),

      managementReason:
        String(
          trade.managementReason ||
          ''
        ).slice(
          0,
          300
        )
    })
  );
}

// =========================
// MULTI-TIMEFRAME CONTEXT
// =========================

async function buildMtfContext(
  symbol,
  bars15m
) {

  let bars1h = [];
  let bars4h = [];

  try {

    bars1h =
      closedBarsOnly(
        await fetchBars(
          symbol,
          '1h',
          220
        ),
        '1h'
      );

  }

  catch (error) {

    console.error(
      `[MTF 1H] ${symbol}:`,
      safeError(error)
    );
  }

  await sleep(8000);

  try {

    bars4h =
      closedBarsOnly(
        await fetchBars(
          symbol,
          '4h',
          220
        ),
        '4h'
      );

  }

  catch (error) {

    console.error(
      `[MTF 4H] ${symbol}:`,
      safeError(error)
    );
  }

  const pair =
    state.pairState.get(
      symbol
    );

  if (pair) {

    pair.bars1h =
      bars1h;

    pair.bars4h =
      bars4h;
  }

  return {

    m15:
      buildTechnicalIntelligence(
        bars15m
      ),

    h1:
      bars1h.length >=
        CORE_MIN_HISTORY
        ? buildTechnicalIntelligence(
            bars1h
          )
        : null,

    h4:
      bars4h.length >=
        CORE_MIN_HISTORY
        ? buildTechnicalIntelligence(
            bars4h
          )
        : null
  };
}

// =========================
// COMPACT AI CONTEXT
// =========================

function compactTechnical(
  technical
) {

  if (!technical)
    return null;

  return {

    price:
      technical.price,

    bias:
      technical.bias,

    score:
      technical.score,

    trend: {
      alignment:
        technical.trend?.alignment,
      macro:
        technical.trend?.macro,
      ema9:
        technical.trend?.ema9,
      ema21:
        technical.trend?.ema21,
      ema50:
        technical.trend?.ema50,
      ema200:
        technical.trend?.ema200
    },

    momentum: {
      rsi:
        technical.momentum?.rsi,
      cmo:
        technical.momentum?.cmo,
      macdHistogram:
        technical.momentum?.macd?.histogram,
      stochastic:
        technical.momentum?.stochastic,
      williamsR:
        technical.momentum?.williamsR,
      roc:
        technical.momentum?.roc
    },

    adx:
      technical.dmi?.adx,

    plusDI:
      technical.dmi?.plusDI,

    minusDI:
      technical.dmi?.minusDI,

    atr:
      technical.volatility?.atr,

    volatilityRegime:
      technical.volatility?.regime,

    structure:
      technical.structure,

    choch:
      technical.choch,

    supertrend:
      technical.supertrend,

    ichimoku: {
      bias:
        technical.ichimoku?.bias,
      tenkan:
        technical.ichimoku?.tenkan,
      kijun:
        technical.ichimoku?.kijun
    },

    liquidity:
      technical.liquidity,

    fvg:
      technical.fvg,

    supportResistance:
      technical.supportResistance,

    volume:
      technical.volume,

    vwap:
      technical.vwap,

    mfi:
      technical.mfi
  };
}

// =========================
// ENTRY COMMANDER
// =========================

async function askEntryCommander({
  symbol,
  quote,
  mtf,
  session,
  strength,
  news,
  memory
}) {

  state.aiEntryCalls++;

  const prompt = `
You are the entry commander for LOMY FOREX V1.5.

This system is PAPER TRADING ONLY.

Your job is to make ONE directional decision:
BUY, SELL, or NO_TRADE.

You must analyze the supplied technical evidence.
You may not invent market data.

If BUY or SELL:
- confidence must be 62 to 100.
- provide a TECHNICAL stop loss price.
- stop loss must be based on structure, volatility, support/resistance, swing or liquidity logic.
- do not calculate position size.
- do not widen risk for convenience.

If evidence is weak, conflicting, spread is unsuitable, news risk is dangerous, or there is no clean technical stop:
return NO_TRADE.

Return JSON only:
{
  "decision":"BUY|SELL|NO_TRADE",
  "confidence":0,
  "stopLoss":null,
  "reason":"short precise reason"
}

DATA:
${JSON.stringify({
    symbol,
    quote,
    session,
    strength,
    news,
    mtf: {
      m15:
        compactTechnical(
          mtf.m15
        ),
      h1:
        compactTechnical(
          mtf.h1
        ),
      h4:
        compactTechnical(
          mtf.h4
        )
    },
    recentClosedTrades:
      memory
  })}
`;

  const result =
    await callGemini(
      prompt
    );

  const decision =
    String(
      result?.decision ||
      'NO_TRADE'
    ).toUpperCase();

  const confidence =
    clamp(
      n(
        result?.confidence,
        0
      ),
      0,
      100
    );

  const stopLoss =
    n(
      result?.stopLoss,
      NaN
    );

  const reason =
    String(
      result?.reason ||
      ''
    ).slice(
      0,
      800
    );

  if (
    ![
      'BUY',
      'SELL',
      'NO_TRADE'
    ].includes(decision)
  ) {

    return {
      decision:
        'NO_TRADE',
      confidence: 0,
      stopLoss: NaN,
      reason:
        'Invalid Gemini decision'
    };
  }

  if (
    decision ===
      'NO_TRADE'
  ) {

    state.aiNoTradeDecisions++;

    return {
      decision,
      confidence,
      stopLoss: NaN,
      reason
    };
  }

  if (
    confidence <
      RULES.minEntryConfidence
  ) {

    state.aiNoTradeDecisions++;

    return {
      decision:
        'NO_TRADE',
      confidence,
      stopLoss: NaN,
      reason:
        `Confidence below ${RULES.minEntryConfidence}%`
    };
  }

  if (
    !Number.isFinite(
      stopLoss
    )
  ) {

    state.aiNoTradeDecisions++;

    return {
      decision:
        'NO_TRADE',
      confidence,
      stopLoss: NaN,
      reason:
        'Gemini did not provide a valid technical stop'
    };
  }

  if (
    decision === 'BUY'
  ) {
    state.aiBuyDecisions++;
  }

  if (
    decision === 'SELL'
  ) {
    state.aiSellDecisions++;
  }

  return {
    decision,
    confidence,
    stopLoss,
    reason
  };
}
// =========================
// DYNAMIC RISK FROM CONFIDENCE
// =========================

function riskPctFromConfidence(
  confidence
) {

  confidence =
    n(confidence, 0);

  if (
    confidence >=
    DYNAMIC_RISK.highConfidence
  ) {
    return Math.min(
      DYNAMIC_RISK.highRiskPct,
      PAPER.maxCapitalRiskPct
    );
  }

  if (
    confidence >=
    DYNAMIC_RISK.medConfidence
  ) {
    return Math.min(
      DYNAMIC_RISK.medRiskPct,
      PAPER.maxCapitalRiskPct
    );
  }

  if (
    confidence >=
    DYNAMIC_RISK.lowConfidence
  ) {
    return Math.min(
      DYNAMIC_RISK.lowRiskPct,
      PAPER.maxCapitalRiskPct
    );
  }

  return 0;
}

// =========================
// QUOTE CURRENCY
// =========================

function quoteCurrency(symbol) {

  if (symbol === 'XAUUSD')
    return 'USD';

  return String(symbol)
    .slice(3, 6)
    .toUpperCase();
}

// =========================
// CURRENCY -> USD RATE
// =========================

async function currencyToUsdRate(
  currency
) {

  currency =
    String(currency || '')
      .toUpperCase();

  if (
    !currency ||
    currency === 'USD'
  ) {
    return 1;
  }

  const directSymbol =
    `${currency}USD`;

  const inverseSymbol =
    `USD${currency}`;

  try {

    const direct =
      await fetchCurrentPrice(
        directSymbol
      );

    if (
      Number.isFinite(direct) &&
      direct > 0
    ) {
      return direct;
    }

  }

  catch (_) {
    // Try inverse below.
  }

  try {

    const inverse =
      await fetchCurrentPrice(
        inverseSymbol
      );

    if (
      Number.isFinite(inverse) &&
      inverse > 0
    ) {
      return 1 / inverse;
    }

  }

  catch (_) {
    // Fail closed below.
  }

  throw new Error(
    `Cannot convert ${currency} to USD`
  );
}

// =========================
// POSITION SIZE
// =========================

async function calculatePositionSize({
  symbol,
  entryPrice,
  stopLoss,
  riskAmount
}) {

  const distance =
    Math.abs(
      entryPrice -
      stopLoss
    );

  if (
    !Number.isFinite(distance) ||
    distance <= 0
  ) {
    throw new Error(
      'Invalid stop distance'
    );
  }

  const quote =
    quoteCurrency(symbol);

  const quoteToUsd =
    await currencyToUsdRate(
      quote
    );

  if (
    !Number.isFinite(
      quoteToUsd
    ) ||
    quoteToUsd <= 0
  ) {
    throw new Error(
      `Invalid ${quote}/USD conversion`
    );
  }

  // Price-distance PnL is denominated
  // in the quote currency.
  const riskPerUnitUsd =
    distance *
    quoteToUsd;

  const quantity =
    riskAmount /
    riskPerUnitUsd;

  if (
    !Number.isFinite(quantity) ||
    quantity <= 0
  ) {
    throw new Error(
      'Invalid calculated quantity'
    );
  }

  return {
    quantity,
    quoteCurrency:
      quote,
    quoteToUsd,
    stopDistance:
      distance,
    riskPerUnitUsd
  };
}

// =========================
// PNL IN USD
// =========================

async function calculatePnlUsd({
  symbol,
  direction,
  entryPrice,
  exitPrice,
  quantity
}) {

  const rawQuotePnl =
    direction === 'BUY'
      ? (
          exitPrice -
          entryPrice
        ) * quantity
      : (
          entryPrice -
          exitPrice
        ) * quantity;

  const quote =
    quoteCurrency(symbol);

  const quoteToUsd =
    await currencyToUsdRate(
      quote
    );

  return (
    rawQuotePnl *
    quoteToUsd
  );
}

// =========================
// PORTFOLIO INITIAL RISK
// =========================

function currentPortfolioRiskUsd() {

  let total = 0;

  for (
    const trade of
      state.openTrades.values()
  ) {

    const initialQuantity =
      Math.max(
        Number.EPSILON,
        n(
          trade.initialQuantity,
          trade.quantity
        )
      );

    const remainingFraction =
      clamp(
        n(
          trade.quantity,
          0
        ) /
        initialQuantity,
        0,
        1
      );

    total +=
      Math.max(
        0,
        n(
          trade.riskAmount,
          0
        )
      ) *
      remainingFraction;
  }

  return total;
}

function portfolioRiskCapUsd() {

  const base =
    account
      ? n(
          account.balance,
          PAPER.startingBalance
        )
      : PAPER.startingBalance;

  return (
    base *
    PAPER.portfolioRiskCapPct /
    100
  );
}

// =========================
// RISK MANAGER
// =========================

async function validateEntryRisk({
  symbol,
  direction,
  confidence,
  technicalStop,
  quote,
  technical
}) {

  if (!account) {

    return {
      approved: false,
      reason:
        'Paper account unavailable'
    };
  }

  if (
    state.openTrades.size >=
    PAPER.maxOpenTrades
  ) {

    return {
      approved: false,
      reason:
        'Maximum open trades reached'
    };
  }

  if (
    state.openTrades.has(
      symbol
    )
  ) {

    return {
      approved: false,
      reason:
        'Symbol already has an open trade'
    };
  }

  const entryPrice =
    entryExecutionPrice(
      direction,
      quote
    );

  const stopLoss =
    n(
      technicalStop,
      NaN
    );

  if (
    !Number.isFinite(
      entryPrice
    ) ||
    !Number.isFinite(
      stopLoss
    )
  ) {

    return {
      approved: false,
      reason:
        'Invalid entry or stop price'
    };
  }

  if (
    direction === 'BUY' &&
    stopLoss >= entryPrice
  ) {

    return {
      approved: false,
      reason:
        'BUY stop must be below entry'
    };
  }

  if (
    direction === 'SELL' &&
    stopLoss <= entryPrice
  ) {

    return {
      approved: false,
      reason:
        'SELL stop must be above entry'
    };
  }

  const stopDistance =
    Math.abs(
      entryPrice -
      stopLoss
    );

  const atr =
    n(
      technical?.volatility?.atr,
      NaN
    );

  if (
    !Number.isFinite(atr) ||
    atr <= 0
  ) {

    return {
      approved: false,
      reason:
        'ATR unavailable'
    };
  }

  const stopAtr =
    stopDistance / atr;

  if (
    stopAtr <
    RULES.minStopAtr
  ) {

    return {
      approved: false,
      reason:
        `Technical stop too tight (${stopAtr.toFixed(2)} ATR)`
    };
  }

  if (
    stopAtr >
    RULES.maxStopAtr
  ) {

    return {
      approved: false,
      reason:
        `Technical stop too wide (${stopAtr.toFixed(2)} ATR)`
    };
  }

  const spread =
    Math.max(
      0,
      n(
        quote.spread,
        0
      )
    );

  if (
    spread >
    stopDistance *
      RULES.maxSpreadRiskFraction
  ) {

    return {
      approved: false,
      reason:
        'Spread is too large relative to stop distance'
    };
  }

  const riskPct =
    riskPctFromConfidence(
      confidence
    );

  if (
    riskPct <= 0 ||
    riskPct >
      PAPER.maxCapitalRiskPct
  ) {

    return {
      approved: false,
      reason:
        'Invalid risk percentage'
    };
  }

  const riskAmount =
    n(
      account.balance,
      0
    ) *
    riskPct /
    100;

  const existingRisk =
    currentPortfolioRiskUsd();

  const cap =
    portfolioRiskCapUsd();

  if (
    existingRisk +
      riskAmount >
    cap + 1e-9
  ) {

    return {
      approved: false,
      reason:
        'Portfolio risk cap would be exceeded'
    };
  }

  let sizing;

  try {

    sizing =
      await calculatePositionSize({
        symbol,
        entryPrice,
        stopLoss,
        riskAmount
      });

  }

  catch (error) {

    return {
      approved: false,
      reason:
        `Sizing failed: ${safeError(error)}`
    };
  }

  const riskDistance =
    stopDistance;

  const partialTargetPrice =
    direction === 'BUY'
      ? (
          entryPrice +
          riskDistance *
          RULES.partialTpTriggerR
        )
      : (
          entryPrice -
          riskDistance *
          RULES.partialTpTriggerR
        );

  return {
    approved: true,

    entryPrice,

    stopLoss,

    riskDistance,

    riskPct,

    riskAmount,

    quantity:
      sizing.quantity,

    initialQuantity:
      sizing.quantity,

    quoteCurrency:
      sizing.quoteCurrency,

    quoteToUsd:
      sizing.quoteToUsd,

    partialTargetPrice,

    rewardRisk:
      RULES.riskReward
  };
}

// =========================
// OPEN PAPER TRADE
// =========================

async function openPaperTrade({
  symbol,
  direction,
  confidence,
  reason,
  aiDecision,
  technical,
  risk
}) {

  if (
    MODE !== 'PAPER' ||
    LIVE_TRADING !== false
  ) {
    throw new Error(
      'Live trading is forbidden'
    );
  }

  if (
    !risk?.approved
  ) {
    throw new Error(
      'Cannot open unapproved trade'
    );
  }

  const tradeId =
    `${symbol}-${Date.now()}-${Math.random()
      .toString(36)
      .slice(2, 8)}`;

  const tradeData = {

    tradeId,

    symbol,

    direction,

    status: 'OPEN',

    entryPrice:
      risk.entryPrice,

    stopLoss:
      risk.stopLoss,

    initialStopLoss:
      risk.stopLoss,

    partialTargetPrice:
      risk.partialTargetPrice,

    quantity:
      risk.quantity,

    initialQuantity:
      risk.initialQuantity,

    riskAmount:
      risk.riskAmount,

    riskPct:
      risk.riskPct,

    confidence,

    entryReason:
      reason,

    managementReason:
      '',

    openedAt:
      new Date(),

    realizedPartialPnl:
      0,

    totalPnl:
      0,

    partialClosed:
      false,

    trailingLevelR:
      0,

    breakEvenActivated:
      false,

    maxFavorablePrice:
      risk.entryPrice,

    maxAdversePrice:
      risk.entryPrice,

    mfeR: 0,

    maeR: 0,

    aiEntryDecision:
      aiDecision,

    technicalSnapshot:
      compactTechnical(
        technical
      )
  };

  const document =
    await Trade.create(
      tradeData
    );

  const trade =
    document.toObject();

  state.openTrades.set(
    symbol,
    trade
  );

  state.executedSignals++;

  await journal(
    'TRADE_OPENED',
    {
      symbol,
      tradeId,
      direction,
      confidence,
      entryPrice:
        risk.entryPrice,
      stopLoss:
        risk.stopLoss,
      riskPct:
        risk.riskPct,
      riskAmount:
        risk.riskAmount,
      quantity:
        risk.quantity,
      partialTargetPrice:
        risk.partialTargetPrice,
      message:
        reason
    }
  );

  await sendTelegram(
    [
      'LOMY PAPER TRADE OPENED',
      `${symbol} ${direction}`,
      `Entry: ${fmtPrice(risk.entryPrice, symbol)}`,
      `SL: ${fmtPrice(risk.stopLoss, symbol)}`,
      `+2R: ${fmtPrice(risk.partialTargetPrice, symbol)}`,
      `Risk: ${risk.riskPct.toFixed(2)}% (${fmtMoney(risk.riskAmount)})`,
      `Confidence: ${confidence.toFixed(0)}%`,
      'Plan: close 50% at +2R, trail remaining 50%.'
    ].join('\n')
  );

  return trade;
}

// =========================
// CURRENT PRICE R
// =========================

function tradePriceR(
  trade,
  price
) {

  const riskDistance =
    Math.abs(
      trade.entryPrice -
      trade.initialStopLoss
    );

  if (
    !Number.isFinite(
      riskDistance
    ) ||
    riskDistance <= 0
  ) {
    return 0;
  }

  if (
    trade.direction === 'BUY'
  ) {

    return (
      price -
      trade.entryPrice
    ) / riskDistance;
  }

  return (
    trade.entryPrice -
    price
  ) / riskDistance;
}

// =========================
// MFE / MAE
// =========================

function updateExcursions(
  trade,
  price
) {

  if (
    trade.direction === 'BUY'
  ) {

    trade.maxFavorablePrice =
      Math.max(
        n(
          trade.maxFavorablePrice,
          trade.entryPrice
        ),
        price
      );

    trade.maxAdversePrice =
      Math.min(
        n(
          trade.maxAdversePrice,
          trade.entryPrice
        ),
        price
      );

  }

  else {

    trade.maxFavorablePrice =
      Math.min(
        n(
          trade.maxFavorablePrice,
          trade.entryPrice
        ),
        price
      );

    trade.maxAdversePrice =
      Math.max(
        n(
          trade.maxAdversePrice,
          trade.entryPrice
        ),
        price
      );
  }

  trade.mfeR =
    Math.max(
      n(trade.mfeR, 0),
      tradePriceR(
        trade,
        trade.maxFavorablePrice
      )
    );

  trade.maeR =
    Math.min(
      n(trade.maeR, 0),
      tradePriceR(
        trade,
        trade.maxAdversePrice
      )
    );
}

// =========================
// STOP PRICE FROM R
// =========================

function stopPriceAtR(
  trade,
  stopR
) {

  const riskDistance =
    Math.abs(
      trade.entryPrice -
      trade.initialStopLoss
    );

  if (
    trade.direction === 'BUY'
  ) {

    return (
      trade.entryPrice +
      riskDistance *
      stopR
    );
  }

  return (
    trade.entryPrice -
    riskDistance *
    stopR
  );
}

// =========================
// MOVE STOP SAFELY
// =========================

function improveStop(
  trade,
  candidate
) {

  if (
    !Number.isFinite(
      candidate
    )
  ) {
    return false;
  }

  if (
    trade.direction === 'BUY'
  ) {

    if (
      candidate >
      trade.stopLoss
    ) {

      trade.stopLoss =
        candidate;

      return true;
    }

    return false;
  }

  if (
    candidate <
    trade.stopLoss
  ) {

    trade.stopLoss =
      candidate;

    return true;
  }

  return false;
      }
// =========================
// CORRECT OPEN-TRADE RESTORE
// =========================

async function restoreOpenTrades() {

  if (!state.mongoReady)
    return;

  const trades =
    await Trade.find({
      status: 'OPEN'
    }).lean();

  state.openTrades.clear();

  for (
    const raw of trades
  ) {

    const trade = {
      ...raw,

      initialQuantity:
        Number.isFinite(
          Number(
            raw.initialQuantity
          )
        )
          ? Number(
              raw.initialQuantity
            )
          : Number(
              raw.quantity
            ),

      realizedPartialPnl:
        n(
          raw.realizedPartialPnl,
          0
        ),

      trailingLevelR:
        n(
          raw.trailingLevelR,
          0
        ),

      partialClosed:
        raw.partialClosed === true,

      breakEvenActivated:
        raw.breakEvenActivated === true
    };

    state.openTrades.set(
      trade.symbol,
      trade
    );
  }

  console.log(
    `[TRADES] restored ${state.openTrades.size} open trades`
  );
}

// =========================
// UPDATE TRADE IN DATABASE
// =========================

async function saveTrade(
  trade
) {

  if (
    !trade ||
    !trade.tradeId
  ) {
    return;
  }

  await Trade.updateOne(
    {
      tradeId:
        trade.tradeId
    },
    {
      $set: {
        status:
          trade.status,

        quantity:
          trade.quantity,

        initialQuantity:
          trade.initialQuantity,

        stopLoss:
          trade.stopLoss,

        partialClosed:
          trade.partialClosed,

        trailingLevelR:
          trade.trailingLevelR,

        breakEvenActivated:
          trade.breakEvenActivated,

        realizedPartialPnl:
          trade.realizedPartialPnl,

        totalPnl:
          trade.totalPnl,

        managementReason:
          trade.managementReason,

        maxFavorablePrice:
          trade.maxFavorablePrice,

        maxAdversePrice:
          trade.maxAdversePrice,

        mfeR:
          trade.mfeR,

        maeR:
          trade.maeR,

        exitPrice:
          trade.exitPrice,

        closedAt:
          trade.closedAt,

        resultR:
          trade.resultR
      }
    }
  );
}

// =========================
// STOP HIT CHECK
// =========================

function isStopHit(
  trade,
  exitPrice
) {

  if (
    !trade ||
    !Number.isFinite(
      exitPrice
    )
  ) {
    return false;
  }

  if (
    trade.direction === 'BUY'
  ) {

    return (
      exitPrice <=
      trade.stopLoss
    );
  }

  return (
    exitPrice >=
    trade.stopLoss
  );
}

// =========================
// PARTIAL + TRAILING LOGIC
// =========================

async function applyMechanicalProtection(
  trade,
  quote
) {

  if (
    !trade ||
    trade.status !== 'OPEN'
  ) {
    return;
  }

  const exitPrice =
    exitExecutionPrice(
      trade.direction,
      quote
    );

  if (
    !Number.isFinite(
      exitPrice
    )
  ) {
    return;
  }

  updateExcursions(
    trade,
    exitPrice
  );

  const currentR =
    tradePriceR(
      trade,
      exitPrice
    );

  let changed =
    false;

  // =========================
  // BREAK-EVEN AT +0.60R
  // =========================

  if (
    !trade.breakEvenActivated &&
    currentR >=
      RULES.breakEvenTriggerR
  ) {

    const breakEvenPrice =
      trade.entryPrice;

    if (
      improveStop(
        trade,
        breakEvenPrice
      )
    ) {

      changed =
        true;
    }

    trade.breakEvenActivated =
      true;

    changed =
      true;

    await journal(
      'BREAK_EVEN',
      {
        symbol:
          trade.symbol,

        tradeId:
          trade.tradeId,

        currentR,

        newStop:
          trade.stopLoss,

        message:
          'Break-even activated at +0.60R'
      }
    );
  }

  // =========================
  // PARTIAL CLOSE AT +2R
  // =========================

  if (
    !trade.partialClosed &&
    currentR >=
      RULES.partialTpTriggerR
  ) {

    const closeQuantity =
      trade.initialQuantity *
      0.50;

    const actualCloseQuantity =
      Math.min(
        closeQuantity,
        trade.quantity
      );

    if (
      actualCloseQuantity > 0
    ) {

      const partialPnl =
        await calculatePnlUsd({
          symbol:
            trade.symbol,

          direction:
            trade.direction,

          entryPrice:
            trade.entryPrice,

          exitPrice,

          quantity:
            actualCloseQuantity
        });

      trade.quantity =
        Math.max(
          0,
          trade.quantity -
          actualCloseQuantity
        );

      trade.realizedPartialPnl =
        n(
          trade.realizedPartialPnl,
          0
        ) +
        partialPnl;

      account.balance =
        n(
          account.balance,
          0
        ) +
        partialPnl;

      await saveAccount();

      trade.partialClosed =
        true;

      // Remaining 50% is now protected at +1R
      const protectedStop =
        stopPriceAtR(
          trade,
          RULES.trailingStartStopR
        );

      improveStop(
        trade,
        protectedStop
      );

      trade.trailingLevelR =
        RULES.trailingStartStopR;

      changed =
        true;

      await journal(
        'PARTIAL_CLOSE',
        {
          symbol:
            trade.symbol,

          tradeId:
            trade.tradeId,

          closeQuantity:
            actualCloseQuantity,

          exitPrice,

          partialPnl,

          remainingQuantity:
            trade.quantity,

          newStop:
            trade.stopLoss,

          message:
            'Closed 50% at +2R and protected remaining 50% at +1R'
        }
      );

      await sendTelegram(
        [
          'LOMY PARTIAL CLOSE',
          `${trade.symbol} ${trade.direction}`,
          'Reached +2R',
          'Closed: 50%',
          `Partial PnL: ${fmtMoney(partialPnl)}`,
          `Remaining: ${n(trade.quantity).toFixed(4)}`,
          `New SL: ${fmtPrice(trade.stopLoss, trade.symbol)}`,
          'Remaining 50% continues with trailing protection.'
        ].join('\n')
      );
    }
  }

  // =========================
  // TRAILING AFTER +2R
  // =========================

  if (
    trade.partialClosed &&
    currentR >
      RULES.partialTpTriggerR
  ) {

    const progressBeyond2R =
      currentR -
      RULES.partialTpTriggerR;

    const completedSteps =
      Math.floor(
        progressBeyond2R /
        RULES.trailingStepR
      );

    const desiredStopR =
      RULES.trailingStartStopR +
      completedSteps *
      RULES.trailingStepR;

    if (
      desiredStopR >
      n(
        trade.trailingLevelR,
        0
      )
    ) {

      const newStop =
        stopPriceAtR(
          trade,
          desiredStopR
        );

      if (
        improveStop(
          trade,
          newStop
        )
      ) {

        trade.trailingLevelR =
          desiredStopR;

        changed =
          true;

        await journal(
          'TRAILING_STOP',
          {
            symbol:
              trade.symbol,

            tradeId:
              trade.tradeId,

            currentR,

            trailingLevelR:
              desiredStopR,

            newStop:
              trade.stopLoss,

            message:
              `Trailing stop moved to +${desiredStopR.toFixed(2)}R`
          }
        );
      }
    }
  }

  if (changed) {

    await saveTrade(
      trade
    );
  }
}

// =========================
// CLOSE PAPER TRADE
// =========================

async function closePaperTrade({
  trade,
  quote,
  reason = 'CLOSE'
}) {

  if (
    !trade ||
    trade.status !== 'OPEN'
  ) {
    return null;
  }

  const currentTrade =
    state.openTrades.get(
      trade.symbol
    );

  if (
    !currentTrade ||
    currentTrade.tradeId !==
      trade.tradeId
  ) {
    return null;
  }

  const exitPrice =
    exitExecutionPrice(
      trade.direction,
      quote
    );

  if (
    !Number.isFinite(
      exitPrice
    )
  ) {
    throw new Error(
      'Invalid exit price'
    );
  }

  updateExcursions(
    trade,
    exitPrice
  );

  const remainingQuantity =
    Math.max(
      0,
      n(
        trade.quantity,
        0
      )
    );

  let remainingPnl = 0;

  if (
    remainingQuantity > 0
  ) {

    remainingPnl =
      await calculatePnlUsd({
        symbol:
          trade.symbol,

        direction:
          trade.direction,

        entryPrice:
          trade.entryPrice,

        exitPrice,

        quantity:
          remainingQuantity
      });
  }

  const partialPnl =
    n(
      trade.realizedPartialPnl,
      0
    );

  const totalPnl =
    partialPnl +
    remainingPnl;

  // Partial PnL was credited when partial close happened.
  // Add only final remaining PnL here.
  account.balance =
    n(
      account.balance,
      0
    ) +
    remainingPnl;

  await saveAccount();

  trade.status =
    'CLOSED';

  trade.exitPrice =
    exitPrice;

  trade.closedAt =
    new Date();

  trade.quantity =
    0;

  trade.totalPnl =
    totalPnl;

  trade.managementReason =
    String(reason).slice(
      0,
      800
    );

  const originalRisk =
    Math.max(
      Number.EPSILON,
      n(
        trade.riskAmount,
        0
      )
    );

  trade.resultR =
    totalPnl /
    originalRisk;

  await saveTrade(
    trade
  );

  state.openTrades.delete(
    trade.symbol
  );

  await journal(
    'TRADE_CLOSED',
    {
      symbol:
        trade.symbol,

      tradeId:
        trade.tradeId,

      direction:
        trade.direction,

      exitPrice,

      remainingPnl,

      partialPnl,

      totalPnl,

      resultR:
        trade.resultR,

      mfeR:
        trade.mfeR,

      maeR:
        trade.maeR,

      message:
        reason
    }
  );

  await sendTelegram(
    [
      'LOMY PAPER TRADE CLOSED',
      `${trade.symbol} ${trade.direction}`,
      `Exit: ${fmtPrice(exitPrice, trade.symbol)}`,
      `Total PnL: ${fmtMoney(totalPnl)}`,
      `Result: ${trade.resultR.toFixed(2)}R`,
      `Balance: ${fmtMoney(account.balance)}`,
      `Reason: ${reason}`
    ].join('\n')
  );

  return trade;
}

// =========================
// AI OPEN-TRADE MANAGEMENT
// =========================

async function askTradeManager({
  trade,
  quote,
  technical,
  memory
}) {

  state.aiManageCalls++;

  const currentPrice =
    exitExecutionPrice(
      trade.direction,
      quote
    );

  const currentR =
    tradePriceR(
      trade,
      currentPrice
    );

  const prompt = `
You manage an EXISTING PAPER forex trade.

You are NOT allowed to change:
- stop loss
- take profit
- trailing stop
- position size
- partial-close rules

Mechanical risk management is controlled by the bot.

Your ONLY decision is:
HOLD or CLOSE.

CLOSE only when the market evidence materially invalidates the trade thesis.
Otherwise HOLD.

Return JSON only:
{
  "decision":"HOLD|CLOSE",
  "confidence":0,
  "reason":"short precise reason"
}

TRADE:
${JSON.stringify({
    symbol:
      trade.symbol,

    direction:
      trade.direction,

    entryPrice:
      trade.entryPrice,

    currentPrice,

    stopLoss:
      trade.stopLoss,

    currentR,

    partialClosed:
      trade.partialClosed,

    trailingLevelR:
      trade.trailingLevelR,

    confidence:
      trade.confidence,

    entryReason:
      trade.entryReason,

    technical:
      compactTechnical(
        technical
      ),

    recentMemory:
      memory
  })}
`;

  const result =
    await callGemini(
      prompt
    );

  const decision =
    String(
      result?.decision ||
      'HOLD'
    ).toUpperCase();

  const confidence =
    clamp(
      n(
        result?.confidence,
        0
      ),
      0,
      100
    );

  const reason =
    String(
      result?.reason ||
      ''
    ).slice(
      0,
      800
    );

  if (
    decision === 'CLOSE' &&
    confidence >=
      RULES.minCloseConfidence
  ) {

    state.aiCloseDecisions++;

    return {
      decision:
        'CLOSE',
      confidence,
      reason
    };
  }

  state.aiHoldDecisions++;

  return {
    decision:
      'HOLD',
    confidence,
    reason
  };
}

// =========================
// MANAGE ONE OPEN TRADE
// =========================

async function manageOpenTrade(
  trade
) {

  if (
    !trade ||
    trade.status !== 'OPEN'
  ) {
    return;
  }

  if (
    state.managementLocks.has(
      trade.symbol
    )
  ) {
    return;
  }

  state.managementLocks.add(
    trade.symbol
  );

  try {

    const quote =
      await fetchQuote(
        trade.symbol
      );

    // Mechanical protection always works,
    // even if Gemini is unavailable.
    await applyMechanicalProtection(
      trade,
      quote
    );

    const stillOpen =
      state.openTrades.get(
        trade.symbol
      );

    if (
      !stillOpen ||
      stillOpen.tradeId !==
        trade.tradeId
    ) {
      return;
    }

    const exitPrice =
      exitExecutionPrice(
        trade.direction,
        quote
      );

    if (
      isStopHit(
        trade,
        exitPrice
      )
    ) {

      await closePaperTrade({
        trade,
        quote,
        reason:
          trade.partialClosed
            ? 'TRAILING_STOP_HIT'
            : trade.breakEvenActivated
              ? 'BREAK_EVEN_STOP_HIT'
              : 'STOP_LOSS_HIT'
      });

      return;
    }

    const now =
      Date.now();

    const lastManage =
      n(
        state.lastManageAt.get(
          trade.symbol
        ),
        0
      );

    if (
      now - lastManage <
      AI_MANAGE_INTERVAL_MS
    ) {
      return;
    }

    state.lastManageAt.set(
      trade.symbol,
      now
    );

    // AI management is optional for an existing trade.
    // Fail-safe: existing mechanical protection remains active.
    if (
      !AI.managementEnabled ||
      !GEMINI_API_KEY
    ) {
      return;
    }

    const pair =
      state.pairState.get(
        trade.symbol
      );

    const bars =
      pair?.bars15m;

    if (
      !Array.isArray(bars) ||
      bars.length <
        CORE_MIN_HISTORY
    ) {
      return;
    }

    const technical =
      buildTechnicalIntelligence(
        bars
      );

    const memory =
      await getAiMemory(
        trade.symbol
      );

    let managerDecision;

    try {

      managerDecision =
        await askTradeManager({
          trade,
          quote,
          technical,
          memory
        });

    }

    catch (error) {

      console.error(
        `[AI MANAGE] ${trade.symbol}:`,
        safeError(error)
      );

      return;
    }

    if (
      managerDecision.decision ===
      'CLOSE'
    ) {

      await closePaperTrade({
        trade,
        quote,
        reason:
          `AI_CLOSE ${managerDecision.confidence.toFixed(0)}%: ${managerDecision.reason}`
      });

      return;
    }

    trade.managementReason =
      `AI_HOLD ${managerDecision.confidence.toFixed(0)}%: ${managerDecision.reason}`;

    await saveTrade(
      trade
    );

  }

  catch (error) {

    console.error(
      `[MANAGE] ${trade.symbol}:`,
      safeError(error)
    );
  }

  finally {

    state.managementLocks.delete(
      trade.symbol
    );
  }
    }
// =========================
// PROCESS NEW CLOSED BAR
// =========================

async function processNewClosedBar(
  symbol,
  bars
) {

  if (
    !Array.isArray(bars) ||
    bars.length <
      CORE_MIN_HISTORY
  ) {
    return;
  }

  if (
    state.scanLocks.has(
      symbol
    )
  ) {
    return;
  }

  state.scanLocks.add(
    symbol
  );

  try {

    const latestBar =
      last(bars);

    if (!latestBar)
      return;

    const barKey =
      `${symbol}:${latestBar.openTime}`;

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

    // Limit memory growth.
    if (
      state.processedBars.size >
      10000
    ) {

      const entries =
        Array.from(
          state.processedBars
        );

      state.processedBars =
        new Set(
          entries.slice(-5000)
        );
    }

    state.scannedBars++;

    // No second trade on same symbol.
    if (
      state.openTrades.has(
        symbol
      )
    ) {

      state.skippedSignals++;

      return;
    }

    if (
      state.openTrades.size >=
      PAPER.maxOpenTrades
    ) {

      state.skippedSignals++;

      return;
    }

    // Gemini is mandatory for NEW entries.
    if (
      !AI.entryCommanderEnabled ||
      !GEMINI_API_KEY
    ) {

      state.skippedSignals++;

      await journal(
        'ENTRY_SKIPPED',
        {
          symbol,
          message:
            'Gemini unavailable - fail closed'
        }
      );

      return;
    }

    const news =
      getNewsBlock(
        symbol
      );

    if (
      news.blocked
    ) {

      state.skippedSignals++;

      await journal(
        'NEWS_BLOCK',
        {
          symbol,
          events:
            news.events,
          message:
            'High-impact news block'
        }
      );

      return;
    }

    let quote;

    try {

      quote =
        await fetchQuote(
          symbol
        );

    }

    catch (error) {

      state.skippedSignals++;

      console.error(
        `[QUOTE] ${symbol}:`,
        safeError(error)
      );

      return;
    }

    const session =
      getMarketSession();

    const strength =
      pairStrengthContext(
        symbol
      );

    let mtf;

    try {

      mtf =
        await buildMtfContext(
          symbol,
          bars
        );

    }

    catch (error) {

      state.skippedSignals++;

      console.error(
        `[MTF] ${symbol}:`,
        safeError(error)
      );

      return;
    }

    if (!mtf?.m15) {

      state.skippedSignals++;

      return;
    }

    const memory =
      await getAiMemory(
        symbol
      );

    let decision;

    try {

      decision =
        await askEntryCommander({
          symbol,
          quote,
          mtf,
          session,
          strength,
          news,
          memory
        });

    }

    catch (error) {

      state.skippedSignals++;

      await journal(
        'AI_ENTRY_ERROR',
        {
          symbol,
          message:
            safeError(error)
        }
      );

      console.error(
        `[AI ENTRY] ${symbol}:`,
        safeError(error)
      );

      return;
    }

    if (
      decision.decision ===
      'NO_TRADE'
    ) {

      state.skippedSignals++;

      await journal(
        'NO_TRADE',
        {
          symbol,
          confidence:
            decision.confidence,
          message:
            decision.reason
        }
      );

      return;
    }

    let risk;

    try {

      risk =
        await validateEntryRisk({
          symbol,

          direction:
            decision.decision,

          confidence:
            decision.confidence,

          technicalStop:
            decision.stopLoss,

          quote,

          technical:
            mtf.m15
        });

    }

    catch (error) {

      state.skippedSignals++;

      console.error(
        `[RISK] ${symbol}:`,
        safeError(error)
      );

      return;
    }

    if (
      !risk.approved
    ) {

      state.skippedSignals++;

      await journal(
        'RISK_REJECT',
        {
          symbol,
          direction:
            decision.decision,
          confidence:
            decision.confidence,
          message:
            risk.reason
        }
      );

      return;
    }

    try {

      await openPaperTrade({
        symbol,

        direction:
          decision.decision,

        confidence:
          decision.confidence,

        reason:
          decision.reason,

        aiDecision:
          decision,

        technical:
          mtf.m15,

        risk
      });

    }

    catch (error) {

      state.skippedSignals++;

      console.error(
        `[OPEN] ${symbol}:`,
        safeError(error)
      );
    }

  }

  finally {

    state.scanLocks.delete(
      symbol
    );
  }
}

// =========================
// REFRESH ONE SYMBOL
// =========================

async function refreshSymbol(
  symbol
) {

  const pair =
    state.pairState.get(
      symbol
    );

  if (!pair)
    return;

  try {

    const rawBars =
      await fetchBars(
        symbol,
        '15m',
        HISTORY_LIMIT
      );

    const closed =
      closedBarsOnly(
        rawBars,
        '15m'
      );

    if (
      closed.length <
      CORE_MIN_HISTORY
    ) {

      pair.lastError =
        'Insufficient closed bars';

      return;
    }

    const latest =
      last(closed);

    const previousTime =
      new Date(
        pair.lastClosedBarTime
      ).getTime();

    const latestTime =
      new Date(
        latest.openTime
      ).getTime();

    pair.bars15m =
      closed;

    pair.lastRefreshAt =
      new Date();

    pair.lastError =
      null;

    if (
      !Number.isFinite(
        latestTime
      )
    ) {
      return;
    }

    if (
      !Number.isFinite(
        previousTime
      ) ||
      latestTime >
        previousTime
    ) {

      pair.lastClosedBarTime =
        latest.openTime;

      await processNewClosedBar(
        symbol,
        closed
      );
    }

  }

  catch (error) {

    pair.lastError =
      safeError(error);

    state.lastMarketError =
      safeError(error);

    console.error(
      `[REFRESH] ${symbol}:`,
      safeError(error)
    );
  }
}

// =========================
// SCAN LOOP
// =========================

async function scanMarket() {

  if (
    state.scanLoopBusy
  ) {
    return;
  }

  state.scanLoopBusy =
    true;

  try {

    for (
      const symbol of
        INSTRUMENTS
    ) {

      if (
        !state.pairState.has(
          symbol
        )
      ) {
        continue;
      }

      await refreshSymbol(
        symbol
      );

      // Twelve Data pacing:
      // ~7.5 requests/minute.
      await sleep(8000);
    }

  }

  finally {

    state.scanLoopBusy =
      false;
  }
}

// =========================
// OPEN TRADE MANAGEMENT LOOP
// =========================

async function manageOpenTrades() {

  if (
    state.quoteLoopBusy
  ) {
    return;
  }

  state.quoteLoopBusy =
    true;

  try {

    const trades =
      Array.from(
        state.openTrades.values()
      );

    for (
      const trade of trades
    ) {

      const stillOpen =
        state.openTrades.get(
          trade.symbol
        );

      if (
        !stillOpen ||
        stillOpen.tradeId !==
          trade.tradeId
      ) {
        continue;
      }

      await manageOpenTrade(
        trade
      );

      // Keep API calls controlled.
      await sleep(8000);
    }

  }

  finally {

    state.quoteLoopBusy =
      false;
  }
}

// =========================
// BOT STATUS
// =========================

function buildStatus() {

  return {

    version:
      VERSION,

    mode:
      MODE,

    liveTrading:
      LIVE_TRADING,

    uptimeSeconds:
      Math.floor(
        (
          Date.now() -
          state.startedAt.getTime()
        ) / 1000
      ),

    ready: {

      mongo:
        state.mongoReady,

      telegram:
        state.telegramReady,

      market:
        state.marketReady,

      gemini:
        state.geminiReady
    },

    account: {

      startingBalance:
        PAPER.startingBalance,

      balance:
        account
          ? n(
              account.balance,
              PAPER.startingBalance
            )
          : null,

      maxTradeRiskPct:
        PAPER.maxCapitalRiskPct,

      portfolioRiskCapPct:
        PAPER.portfolioRiskCapPct,

      currentPortfolioRiskUsd:
        currentPortfolioRiskUsd(),

      portfolioRiskCapUsd:
        portfolioRiskCapUsd()
    },

    trades: {

      open:
        state.openTrades.size,

      max:
        PAPER.maxOpenTrades,

      executed:
        state.executedSignals,

      skipped:
        state.skippedSignals
    },

    ai: {

      entryCalls:
        state.aiEntryCalls,

      manageCalls:
        state.aiManageCalls,

      buy:
        state.aiBuyDecisions,

      sell:
        state.aiSellDecisions,

      noTrade:
        state.aiNoTradeDecisions,

      close:
        state.aiCloseDecisions,

      hold:
        state.aiHoldDecisions,

      lastError:
        state.lastAiError
    },

    market: {

      initializedSymbols:
        state.pairState.size,

      totalSymbols:
        INSTRUMENTS.length,

      scannedBars:
        state.scannedBars,

      lastError:
        state.lastMarketError
    },

    exits: {

      breakEvenAtR:
        RULES.breakEvenTriggerR,

      partialCloseAtR:
        RULES.partialTpTriggerR,

      partialClosePct:
        50,

      remainingPct:
        50,

      trailingStartStopR:
        RULES.trailingStartStopR,

      trailingStepR:
        RULES.trailingStepR
    }
  };
}

// =========================
// EXPRESS WEB SERVER
// =========================

function startWebServer() {

  const app =
    express();

  app.use(
    express.json({
      limit: '1mb'
    })
  );

  app.get(
    '/',
    (req, res) => {

      res.json({
        service:
          VERSION,
        mode:
          MODE,
        status:
          'RUNNING'
      });
    }
  );

  app.get(
    '/health',
    (req, res) => {

      res.status(200).json({
        ok: true,

        version:
          VERSION,

        mode:
          MODE,

        mongoReady:
          state.mongoReady,

        marketReady:
          state.marketReady,

        geminiReady:
          state.geminiReady,

        telegramReady:
          state.telegramReady,

        openTrades:
          state.openTrades.size
      });
    }
  );

  app.get(
    '/api/status',
    (req, res) => {

      res.json(
        buildStatus()
      );
    }
  );

  app.get(
    '/api/trades',
    (req, res) => {

      res.json(
        Array.from(
          state.openTrades.values()
        )
      );
    }
  );

  app.post(
    '/api/trades/:symbol/close',
    async (
      req,
      res
    ) => {

      try {

        const symbol =
          String(
            req.params.symbol ||
            ''
          )
            .trim()
            .toUpperCase();

        const trade =
          state.openTrades.get(
            symbol
          );

        if (!trade) {

          return res
            .status(404)
            .json({
              ok: false,
              error:
                'No open trade for symbol'
            });
        }

        const quote =
          await fetchQuote(
            symbol
          );

        const closed =
          await closePaperTrade({
            trade,
            quote,
            reason:
              'MANUAL_API_CLOSE'
          });

        return res.json({
          ok: true,
          trade:
            closed
        });

      }

      catch (error) {

        return res
          .status(500)
          .json({
            ok: false,
            error:
              safeError(error)
          });
      }
    }
  );

  app.listen(
    PORT,
    '0.0.0.0',
    () => {

      console.log(
        `[WEB] listening on ${PORT}`
      );
    }
  );
}

// =========================
// START BACKGROUND LOOPS
// =========================

function startLoops() {

  if (
    state.loopsStarted
  ) {
    return;
  }

  state.loopsStarted =
    true;

  // Scan immediately after initialization.
  setTimeout(
    () => {
      scanMarket()
        .catch(error =>
          console.error(
            '[SCAN INITIAL]',
            safeError(error)
          )
        );
    },
    5000
  );

  setInterval(
    () => {

      scanMarket()
        .catch(error =>
          console.error(
            '[SCAN LOOP]',
            safeError(error)
          )
        );

    },
    SCAN_TIMER_MS
  );

  setInterval(
    () => {

      manageOpenTrades()
        .catch(error =>
          console.error(
            '[TRADE LOOP]',
            safeError(error)
          )
        );

    },
    QUOTE_POLL_MS
  );

  setInterval(
    () => {

      fetchEconomicNews()
        .catch(error =>
          console.error(
            '[NEWS LOOP]',
            safeError(error)
          )
        );

    },
    NEWS_REFRESH_MS
  );

  console.log(
    '[LOOPS] started'
  );
}

// =========================
// STARTUP VALIDATION
// =========================

function validateStartupConfig() {

  if (
    MODE !== 'PAPER'
  ) {
    throw new Error(
      'MODE must remain PAPER'
    );
  }

  if (
    LIVE_TRADING !== false
  ) {
    throw new Error(
      'LIVE_TRADING must remain false'
    );
  }

  if (
    PAPER.maxCapitalRiskPct >
    1.00
  ) {
    throw new Error(
      'Max capital risk cannot exceed 1%'
    );
  }

  if (
    PAPER.portfolioRiskCapPct >
    4.00
  ) {
    throw new Error(
      'Portfolio risk cap cannot exceed 4%'
    );
  }

  if (
    RULES.riskReward !==
    2.00
  ) {
    throw new Error(
      'Risk/reward must remain 2R'
    );
  }

  if (
    RULES.partialTpTriggerR !==
    2.00
  ) {
    throw new Error(
      'Partial close trigger must remain +2R'
    );
  }

  if (
    RULES.breakEvenTriggerR !==
    0.60
  ) {
    throw new Error(
      'Break-even must remain +0.60R'
    );
  }

  if (
    !TWELVE_DATA_API_KEY
  ) {
    throw new Error(
      'TWELVE_DATA_API_KEY is missing'
    );
  }

  if (
    !MONGODB_URI
  ) {
    throw new Error(
      'MONGODB_URI is missing'
    );
  }

  if (
    !GEMINI_API_KEY
  ) {

    console.warn(
      '[STARTUP] GEMINI_API_KEY missing. New entries will remain disabled.'
    );
  }
}

// =========================
// GRACEFUL SHUTDOWN
// =========================

async function shutdown(
  signal
) {

  console.log(
    `[SHUTDOWN] ${signal}`
  );

  try {

    if (telegramBot) {

      telegramBot.stop(
        signal
      );
    }

  }

  catch (_) {}

  try {

    if (
      mongoose.connection
        .readyState !== 0
    ) {

      await mongoose.disconnect();
    }

  }

  catch (_) {}

  process.exit(0);
}

process.on(
  'SIGTERM',
  () => shutdown('SIGTERM')
);

process.on(
  'SIGINT',
  () => shutdown('SIGINT')
);

// =========================
// UNHANDLED ERRORS
// =========================

process.on(
  'unhandledRejection',
  reason => {

    console.error(
      '[UNHANDLED REJECTION]',
      safeError(reason)
    );
  }
);

process.on(
  'uncaughtException',
  error => {

    console.error(
      '[UNCAUGHT EXCEPTION]',
      safeError(error)
    );
  }
);

// =========================
// BOOT
// =========================

async function boot() {

  console.log(
    '========================================'
  );

  console.log(
    VERSION
  );

  console.log(
    `MODE=${MODE}`
  );

  console.log(
    `LIVE_TRADING=${LIVE_TRADING}`
  );

  console.log(
    '========================================'
  );

  validateStartupConfig();

  // Render web service must bind quickly.
  startWebServer();

  // Mongo/account.
  await initMongo();

  // Restore existing PAPER positions.
  await restoreOpenTrades();

  // Telegram is optional.
  await initTelegram();

  // News must be loaded BEFORE scanning.
  await fetchEconomicNews();

  // Twelve Data market history.
  await initializeMarket();

  // Mark Gemini available only after
  // a real successful request.
  state.geminiReady =
    false;

  startLoops();

  await journal(
    'BOT_STARTED',
    {
      message:
        VERSION,

      mode:
        MODE,

      startingBalance:
        PAPER.startingBalance,

      maxTradeRiskPct:
        PAPER.maxCapitalRiskPct,

      portfolioRiskCapPct:
        PAPER.portfolioRiskCapPct,

      breakEvenR:
        RULES.breakEvenTriggerR,

      partialCloseR:
        RULES.partialTpTriggerR,

      partialClosePct:
        50,

      remainingTrailingPct:
        50
    }
  );

  console.log(
    '[BOOT] LOMY FOREX V1.5 READY'
  );
}

// =========================
// RUN
// =========================

boot().catch(
  error => {

    console.error(
      '[BOOT FATAL]',
      safeError(error)
    );

    process.exit(1);
  }
);
