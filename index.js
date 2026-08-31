'use strict';

const axios = require('axios');
const express = require('express');
const mongoose = require('mongoose');
const { Telegraf } = require('telegraf');

// ============================================================
// LOMY FOREX V1.2.2
// FLEXIBLE WARMUP + SMC + BREAK-EVEN 1:1.2
// PAPER ONLY — NO REAL TRADING
// ============================================================

const VERSION = 'LOMY FOREX V1.2.2';
const MODE = 'PAPER';
const LIVE_TRADING = false;

const PORT = Number(process.env.PORT || 10000);

const TELEGRAM_BOT_TOKEN = String(
  process.env.TELEGRAM_BOT_TOKEN || ''
).trim();

const MONGODB_URI = String(
  process.env.MONGODB_URI || ''
).trim();

const TWELVE_DATA_API_KEY = String(
  process.env.TWELVE_DATA_API_KEY || ''
).trim();

const BIQUOTE_BASE = 'https://biquote.io';

const TIMEFRAME = '15m';
const HISTORY_LIMIT = 260;

// مهم:
// المحرك الأساسي V5.1 لا يحتاج 230 شمعة.
// EMA200 مجرد Context إضافي ولا يجب أن يمنع تشغيل الزوج.
const CORE_MIN_HISTORY = 60;
const EMA200_CONTEXT_HISTORY = 200;

const OHLC_CONCURRENCY = 4;
const QUOTE_POLL_MS = 3000;
const SCAN_TIMER_MS = 4000;

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
// STRATEGY
// ============================================================

const STRATEGY = Object.freeze({
  // Ultra-Fast V5.1
  cmoLen: 9,
  cmoBuyThresh: 30,
  cmoSellThresh: -30,

  volLen: 10,
  volMult: 1.30,

  srLen: 20,

  fastEmaLen: 9,
  slowEmaLen: 21,

  atrLen: 14,
  atrMargin: 0.20,

  bodyRatioMin: 0.50,

  // Money management
  riskReward: 1.20,
  breakEvenTriggerR: 0.60,

  // Anti-chase
  maxEntryMoveAtr: 0.50,

  // Retest + Fibonacci
  retestLookback: 8,
  fibLookback: 40,
  fibToleranceAtr: 0.20,

  // SMC
  smcFastEmaLen: 50,
  smcSlowEmaLen: 200,

  adxLen: 14,
  adxSmooth: 14,
  adxThreshold: 25,

  pivotLeft: 5,
  pivotRight: 5
});

// ============================================================
// PAPER ACCOUNT
// ============================================================

const PAPER = Object.freeze({
  startingBalance: 1000,
  riskPctPerTrade: 0.50,
  maxOpenTrades: 31,

  accountKey: 'lomy-forex-v120-smc-be-12r'
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

  lastScanSlot: null,
  lastSignalScanAt: null,
  lastQuotePollAt: null,

  lastMarketError: null,

  totalSignalScans: 0,
  totalQuotePolls: 0,

  rawSignals: 0,
  executedSignals: 0,
  skippedSignals: 0,

  breakEvenMoves: 0,

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
    ema200Ready: false,

    errors: 0
  });
}

// ============================================================
// HELPERS
// ============================================================

function n(value, fallback = NaN) {
  const x = Number(value);

  return Number.isFinite(x)
    ? x
    : fallback;
}

function safeError(error) {
  return (
    error?.response?.data?.message ||
    error?.response?.data?.error ||
    error?.response?.data?.status ||
    error?.message ||
    String(error)
  );
}

function fmtMoney(value) {
  return '$' + n(value, 0).toFixed(2);
}

function fmtPrice(value, symbol = '') {
  if (!Number.isFinite(value)) {
    return 'n/a';
  }

  if (symbol === 'XAUUSD') {
    return value.toFixed(2);
  }

  if (symbol.endsWith('JPY')) {
    return value.toFixed(3);
  }

  return value.toFixed(5);
}

function clamp(value, minimum, maximum) {
  return Math.max(
    minimum,
    Math.min(maximum, value)
  );
}

function sleep(ms) {
  return new Promise(resolve =>
    setTimeout(resolve, ms)
  );
}

function barTimeMs(bar) {
  const time = new Date(
    bar.openTime
  ).getTime();

  return Number.isFinite(time)
    ? time
    : 0;
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
        bar.tickVolume ??
        bar.tick_volume ??
        bar.volume,
        0
      ),

      tickVolume: n(
        bar.tickVolume ??
        bar.tick_volume ??
        bar.volume,
        0
      ),

      isOpen:
        bar.isOpen === true
    }))
    .filter(bar =>
      bar.openTime &&
      Number.isFinite(bar.open) &&
      Number.isFinite(bar.high) &&
      Number.isFinite(bar.low) &&
      Number.isFinite(bar.close)
    )
    .sort(
      (a, b) =>
        barTimeMs(a) -
        barTimeMs(b)
    );
}

function closedBarsOnly(bars) {
  if (!Array.isArray(bars)) {
    return [];
  }

  return bars.filter(
    bar => !bar.isOpen
  );
}

// ============================================================
// BIQUOTE
// ============================================================

async function fetchOhlc(symbol) {
  const url =
    `${BIQUOTE_BASE}/api/${symbol}/ohlc`;

  const response = await axios.get(
    url,
    {
      params: {
        interval: TIMEFRAME,
        limit: HISTORY_LIMIT
      },

      timeout: 12000,

      headers: {
        Accept: 'application/json'
      }
    }
  );

  const body = response.data;

  let rawBars = [];

  if (Array.isArray(body)) {
    rawBars = body;
  }

  else if (Array.isArray(body?.bars)) {
    rawBars = body.bars;
  }

  else if (
    Array.isArray(body?.data?.bars)
  ) {
    rawBars = body.data.bars;
  }

  else if (
    Array.isArray(body?.data)
  ) {
    rawBars = body.data;
  }

  const normalized =
    normalizeBars(rawBars);

  return closedBarsOnly(
    normalized
  );
}

async function fetchLatestQuotes(
  symbols
) {
  if (
    !Array.isArray(symbols) ||
    symbols.length === 0
  ) {
    return new Map();
  }

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

  const url =
    `${BIQUOTE_BASE}/api/latest?${params.toString()}`;

  const response =
    await axios.get(
      url,
      {
        timeout: 10000,

        headers: {
          Accept: 'application/json'
        }
      }
    );

  const body =
    response.data;

  let items = [];

  if (Array.isArray(body)) {
    items = body;
  }

  else if (
    Array.isArray(body?.data)
  ) {
    items = body.data;
  }

  else if (
    Array.isArray(body?.quotes)
  ) {
    items = body.quotes;
  }

  else if (
    Array.isArray(body?.data?.quotes)
  ) {
    items = body.data.quotes;
  }

  else if (
    body &&
    typeof body === 'object'
  ) {
    for (
      const [
        key,
        value
      ] of Object.entries(body)
    ) {
      if (
        value &&
        typeof value === 'object'
      ) {
        items.push({
          symbol:
            value.symbol || key,

          ...value
        });
      }
    }
  }

  const result =
    new Map();

  for (
    const item
    of items
  ) {
    const symbol =
      String(
        item.symbol ||
        item.instrument ||
        item.ticker ||
        ''
      )
        .replace('/', '')
        .toUpperCase();

    if (!symbol) {
      continue;
    }

    const bid =
      n(item.bid);

    const ask =
      n(item.ask);

    let mid =
      n(item.mid);

    if (
      !Number.isFinite(mid) &&
      Number.isFinite(bid) &&
      Number.isFinite(ask)
    ) {
      mid =
        (bid + ask) / 2;
    }

    if (
      !Number.isFinite(mid)
    ) {
      continue;
    }

    result.set(
      symbol,
      {
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
          item.timestamp ||
          item.datetime ||
          new Date().toISOString()
      }
    );
  }

  return result;
}

// ============================================================
// INDICATORS
// ============================================================

function sma(values, length) {
  if (
    !Array.isArray(values) ||
    values.length < length ||
    length <= 0
  ) {
    return NaN;
  }

  const slice =
    values.slice(-length);

  const total =
    slice.reduce(
      (sum, value) =>
        sum + value,
      0
    );

  return total / length;
}

function ema(values, length) {
  if (
    !Array.isArray(values) ||
    values.length < length ||
    length <= 0
  ) {
    return NaN;
  }

  const multiplier =
    2 / (length + 1);

  let value =
    sma(
      values.slice(
        0,
        length
      ),
      length
    );

  for (
    let i = length;
    i < values.length;
    i++
  ) {
    value =
      (
        values[i] *
        multiplier
      ) +
      (
        value *
        (
          1 -
          multiplier
        )
      );
  }

  return value;
}

function cmo(values, length) {
  if (
    !Array.isArray(values) ||
    values.length <
      length + 1
  ) {
    return NaN;
  }

  let gains = 0;
  let losses = 0;

  const start =
    values.length -
    length;

  for (
    let i = start;
    i < values.length;
    i++
  ) {
    const change =
      values[i] -
      values[i - 1];

    if (
      change > 0
    ) {
      gains += change;
    }

    else {
      losses +=
        Math.abs(change);
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
    (
      gains -
      losses
    ) /
    denominator
  );
}

function trueRange(
  current,
  previous
) {
  if (
    !current ||
    !previous
  ) {
    return NaN;
  }

  return Math.max(
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
  );
}

function atr(
  bars,
  length
) {
  if (
    !Array.isArray(bars) ||
    bars.length <
      length + 1
  ) {
    return NaN;
  }

  const trs = [];

  for (
    let i = 1;
    i < bars.length;
    i++
  ) {
    trs.push(
      trueRange(
        bars[i],
        bars[i - 1]
      )
    );
  }

  if (
    trs.length < length
  ) {
    return NaN;
  }

  let value =
    sma(
      trs.slice(
        0,
        length
      ),
      length
    );

  for (
    let i = length;
    i < trs.length;
    i++
  ) {
    value =
      (
        (
          value *
          (
            length -
            1
          )
        ) +
        trs[i]
      ) /
      length;
  }

  return value;
}

function highest(
  values,
  length
) {
  if (
    !Array.isArray(values) ||
    values.length < length
  ) {
    return NaN;
  }

  return Math.max(
    ...values.slice(
      -length
    )
  );
}

function lowest(
  values,
  length
) {
  if (
    !Array.isArray(values) ||
    values.length < length
  ) {
    return NaN;
  }

  return Math.min(
    ...values.slice(
      -length
    )
  );
}

// ============================================================
// DMI / ADX
// ============================================================

function dmiAdx(
  bars,
  length = 14,
  smoothing = 14
) {
  if (
    !Array.isArray(bars) ||
    bars.length <
      length +
      smoothing +
      2
  ) {
    return {
      plusDI: NaN,
      minusDI: NaN,
      adx: NaN
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
      (
        upMove >
          downMove &&
        upMove > 0
      )
        ? upMove
        : 0
    );

    minusDM.push(
      (
        downMove >
          upMove &&
        downMove > 0
      )
        ? downMove
        : 0
    );

    tr.push(
      trueRange(
        current,
        previous
      )
    );
  }

  if (
    tr.length < length
  ) {
    return {
      plusDI: NaN,
      minusDI: NaN,
      adx: NaN
    };
  }

  let smoothedTR =
    tr
      .slice(
        0,
        length
      )
      .reduce(
        (a, b) =>
          a + b,
        0
      );

  let smoothedPlus =
    plusDM
      .slice(
        0,
        length
      )
      .reduce(
        (a, b) =>
          a + b,
        0
      );

  let smoothedMinus =
    minusDM
      .slice(
        0,
        length
      )
      .reduce(
        (a, b) =>
          a + b,
        0
      );

  const dxValues = [];

  let plusDI =
    (
      smoothedPlus /
      smoothedTR
    ) * 100;

  let minusDI =
    (
      smoothedMinus /
      smoothedTR
    ) * 100;

  let denom =
    plusDI +
    minusDI;

  dxValues.push(
    denom === 0
      ? 0
      : (
          Math.abs(
            plusDI -
            minusDI
          ) /
          denom
        ) * 100
  );

  for (
    let i = length;
    i < tr.length;
    i++
  ) {
    smoothedTR =
      smoothedTR -
      (
        smoothedTR /
        length
      ) +
      tr[i];

    smoothedPlus =
      smoothedPlus -
      (
        smoothedPlus /
        length
      ) +
      plusDM[i];

    smoothedMinus =
      smoothedMinus -
      (
        smoothedMinus /
        length
      ) +
      minusDM[i];

    plusDI =
      (
        smoothedPlus /
        smoothedTR
      ) * 100;

    minusDI =
      (
        smoothedMinus /
        smoothedTR
      ) * 100;

    denom =
      plusDI +
      minusDI;

    dxValues.push(
      denom === 0
        ? 0
        : (
            Math.abs(
              plusDI -
              minusDI
            ) /
            denom
          ) * 100
    );
  }

  const adxValue =
    dxValues.length >=
      smoothing
      ? sma(
          dxValues,
          smoothing
        )
      : NaN;

  return {
    plusDI,
    minusDI,
    adx: adxValue
  };
}

// ============================================================
// STRUCTURE / SMC
// ============================================================

function recentPivotHigh(
  bars,
  left = 5,
  right = 5
) {
  if (
    bars.length <
      left +
      right +
      2
  ) {
    return NaN;
  }

  for (
    let i =
      bars.length -
      1 -
      right;
    i >= left;
    i--
  ) {
    const candidate =
      bars[i].high;

    let valid = true;

    for (
      let j =
        i - left;
      j <=
        i + right;
      j++
    ) {
      if (
        j === i
      ) {
        continue;
      }

      if (
        bars[j].high >=
        candidate
      ) {
        valid = false;
        break;
      }
    }

    if (valid) {
      return candidate;
    }
  }

  return NaN;
}

function recentPivotLow(
  bars,
  left = 5,
  right = 5
) {
  if (
    bars.length <
      left +
      right +
      2
  ) {
    return NaN;
  }

  for (
    let i =
      bars.length -
      1 -
      right;
    i >= left;
    i--
  ) {
    const candidate =
      bars[i].low;

    let valid = true;

    for (
      let j =
        i - left;
      j <=
        i + right;
      j++
    ) {
      if (
        j === i
      ) {
        continue;
      }

      if (
        bars[j].low <=
        candidate
      ) {
        valid = false;
        break;
      }
    }

    if (valid) {
      return candidate;
    }
  }

  return NaN;
}

function liquiditySweep(
  bars,
  direction
) {
  if (
    bars.length < 8
  ) {
    return false;
  }

  const signal =
    bars[
      bars.length -
      1
    ];

  const previous =
    bars.slice(
      -8,
      -1
    );

  if (
    direction === 'BUY'
  ) {
    const previousLow =
      Math.min(
        ...previous.map(
          b => b.low
        )
      );

    return (
      signal.low <
        previousLow &&
      signal.close >
        previousLow
    );
  }

  const previousHigh =
    Math.max(
      ...previous.map(
        b => b.high
      )
    );

  return (
    signal.high >
      previousHigh &&
    signal.close <
      previousHigh
  );
}

function detectFvg(
  bars,
  direction
) {
  if (
    bars.length < 3
  ) {
    return false;
  }

  const a =
    bars[
      bars.length -
      3
    ];

  const b =
    bars[
      bars.length -
      2
    ];

  const c =
    bars[
      bars.length -
      1
    ];

  if (
    direction === 'BUY'
  ) {
    return (
      c.low >
        a.high &&
      b.close >
        b.open
    );
  }

  return (
    c.high <
      a.low &&
    b.close <
      b.open
  );
}

// ============================================================
// RETEST
// ============================================================

function detectRetest(
  bars,
  direction
) {
  const lookback =
    STRATEGY.retestLookback;

  if (
    bars.length <
      lookback +
      2
  ) {
    return {
      breakout: false,
      retest: false
    };
  }

  const signal =
    bars[
      bars.length -
      1
    ];

  const prior =
    bars.slice(
      -lookback - 1,
      -1
    );

  const priorHigh =
    Math.max(
      ...prior.map(
        b => b.high
      )
    );

  const priorLow =
    Math.min(
      ...prior.map(
        b => b.low
      )
    );

  if (
    direction === 'BUY'
  ) {
    const breakout =
      signal.high >
        priorHigh;

    const retest =
      (
        signal.low <=
          priorHigh &&
        signal.close >
          priorHigh
      );

    return {
      breakout,
      retest
    };
  }

  const breakout =
    signal.low <
      priorLow;

  const retest =
    (
      signal.high >=
        priorLow &&
      signal.close <
        priorLow
    );

  return {
    breakout,
    retest
  };
}

// ============================================================
// FIBONACCI
// ============================================================

function fibContext(
  bars,
  price,
  atrValue,
  direction
) {
  const lookback =
    STRATEGY.fibLookback;

  if (
    bars.length < lookback
  ) {
    return {
      nearFib: false,
      golden: false,
      levels: null
    };
  }

  const recent =
    bars.slice(
      -lookback
    );

  const swingHigh =
    Math.max(
      ...recent.map(
        b => b.high
      )
    );

  const swingLow =
    Math.min(
      ...recent.map(
        b => b.low
      )
    );

  const range =
    swingHigh -
    swingLow;

  if (
    range <= 0
  ) {
    return {
      nearFib: false,
      golden: false,
      levels: null
    };
  }

  let levels;

  if (
    direction === 'BUY'
  ) {
    levels = {
      fib382:
        swingHigh -
        range * 0.382,

      fib50:
        swingHigh -
        range * 0.500,

      fib618:
        swingHigh -
        range * 0.618,

      fib786:
        swingHigh -
        range * 0.786,

      ext1272:
        swingHigh +
        range * 0.272,

      ext1618:
        swingHigh +
        range * 0.618
    };
  }

  else {
    levels = {
      fib382:
        swingLow +
        range * 0.382,

      fib50:
        swingLow +
        range * 0.500,

      fib618:
        swingLow +
        range * 0.618,

      fib786:
        swingLow +
        range * 0.786,

      ext1272:
        swingLow -
        range * 0.272,

      ext1618:
        swingLow -
        range * 0.618
    };
  }

  const tolerance =
    Number.isFinite(atrValue)
      ? atrValue *
        STRATEGY.fibToleranceAtr
      : range * 0.01;

  const distances = [
    levels.fib382,
    levels.fib50,
    levels.fib618,
    levels.fib786
  ].map(
    level =>
      Math.abs(
        price -
        level
      )
  );

  const nearFib =
    Math.min(
      ...distances
    ) <= tolerance;

  const goldenLow =
    Math.min(
      levels.fib50,
      levels.fib618
    );

  const goldenHigh =
    Math.max(
      levels.fib50,
      levels.fib618
    );

  const golden =
    (
      price >=
        goldenLow -
        tolerance &&
      price <=
        goldenHigh +
        tolerance
    );

  return {
    nearFib,
    golden,
    levels
  };
}

// ============================================================
// CORE ANALYSIS
// ============================================================

function analyzeMarket(
  symbol,
  bars,
  previousSignal
) {
  if (
    !Array.isArray(bars) ||
    bars.length <
      CORE_MIN_HISTORY
  ) {
    return {
      signal: 'WAIT',
      reason: 'INSUFFICIENT_HISTORY'
    };
  }

  const signalBar =
    bars[
      bars.length -
      1
    ];

  const previousBars =
    bars.slice(
      0,
      -1
    );

  const closes =
    bars.map(
      b => b.close
    );

  const volumes =
    bars.map(
      b =>
        n(
          b.tickVolume,
          b.volume
        )
    );

  const fastEma =
    ema(
      closes,
      STRATEGY.fastEmaLen
    );

  const slowEma =
    ema(
      closes,
      STRATEGY.slowEmaLen
    );

  const ema50 =
    ema(
      closes,
      STRATEGY.smcFastEmaLen
    );

  // EMA200 اختياري.
  const ema200 =
    bars.length >=
      EMA200_CONTEXT_HISTORY
      ? ema(
          closes,
          STRATEGY.smcSlowEmaLen
        )
      : NaN;

  const cmoValue =
    cmo(
      closes,
      STRATEGY.cmoLen
    );

  const atrValue =
    atr(
      bars,
      STRATEGY.atrLen
    );

  const volumeSma =
    sma(
      volumes.slice(
        0,
        -1
      ),
      STRATEGY.volLen
    );

  const signalVolume =
    volumes[
      volumes.length -
      1
    ];

  const body =
    Math.abs(
      signalBar.close -
      signalBar.open
    );

  const range =
    signalBar.high -
    signalBar.low;

  const bodyRatio =
    range > 0
      ? body / range
      : 0;

  const strongBull =
    (
      signalBar.close >
        signalBar.open &&
      bodyRatio >=
        STRATEGY.bodyRatioMin
    );

  const strongBear =
    (
      signalBar.close <
        signalBar.open &&
      bodyRatio >=
        STRATEGY.bodyRatioMin
    );

  const highVolume =
    (
      Number.isFinite(
        volumeSma
      ) &&
      signalVolume >
        volumeSma *
        STRATEGY.volMult
    );

  const bullMomentum =
    cmoValue >
      STRATEGY.cmoBuyThresh;

  const bearMomentum =
    cmoValue <
      STRATEGY.cmoSellThresh;

  const bullTrend =
    fastEma >
      slowEma;

  const bearTrend =
    fastEma <
      slowEma;

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

  let signal =
    'WAIT';

  let nextLastSignal =
    previousSignal;

  if (
    rawBuy &&
    previousSignal !== 1
  ) {
    signal = 'BUY';
    nextLastSignal = 1;
  }

  else if (
    rawSell &&
    previousSignal !== -1
  ) {
    signal = 'SELL';
    nextLastSignal = -1;
  }

  const support =
    lowest(
      previousBars
        .slice(
          -STRATEGY.srLen
        )
        .map(
          b => b.low
        ),
      STRATEGY.srLen
    );

  const resistance =
    highest(
      previousBars
        .slice(
          -STRATEGY.srLen
        )
        .map(
          b => b.high
        ),
      STRATEGY.srLen
    );

  const dmi =
    dmiAdx(
      bars,
      STRATEGY.adxLen,
      STRATEGY.adxSmooth
    );

  let qualityScore = 0;

  const reasons = [];

  if (
    signal === 'BUY'
  ) {
    if (
      Number.isFinite(
        ema50
      )
    ) {
      if (
        Number.isFinite(
          ema200
        )
      ) {
        if (
          ema50 >
            ema200
        ) {
          qualityScore += 1;
          reasons.push(
            'EMA50>EMA200'
          );
        }
      }

      else if (
        signalBar.close >
          ema50
      ) {
        reasons.push(
          'EMA200-WARMUP'
        );
      }
    }

    if (
      Number.isFinite(
        dmi.adx
      ) &&
      dmi.adx >=
        STRATEGY.adxThreshold &&
      dmi.plusDI >
        dmi.minusDI
    ) {
      qualityScore += 1;
      reasons.push(
        'ADX-BULL'
      );
    }

    if (
      liquiditySweep(
        bars,
        'BUY'
      )
    ) {
      qualityScore += 3;
      reasons.push(
        'LIQUIDITY-SWEEP'
      );
    }

    if (
      detectFvg(
        bars,
        'BUY'
      )
    ) {
      qualityScore += 1;
      reasons.push(
        'BULL-FVG'
      );
    }
  }

  if (
    signal === 'SELL'
  ) {
    if (
      Number.isFinite(
        ema50
      )
    ) {
      if (
        Number.isFinite(
          ema200
        )
      ) {
        if (
          ema50 <
            ema200
        ) {
          qualityScore += 1;
          reasons.push(
            'EMA50<EMA200'
          );
        }
      }

      else if (
        signalBar.close <
          ema50
      ) {
        reasons.push(
          'EMA200-WARMUP'
        );
      }
    }

    if (
      Number.isFinite(
        dmi.adx
      ) &&
      dmi.adx >=
        STRATEGY.adxThreshold &&
      dmi.minusDI >
        dmi.plusDI
    ) {
      qualityScore += 1;
      reasons.push(
        'ADX-BEAR'
      );
    }

    if (
      liquiditySweep(
        bars,
        'SELL'
      )
    ) {
      qualityScore += 3;
      reasons.push(
        'LIQUIDITY-SWEEP'
      );
    }

    if (
      detectFvg(
        bars,
        'SELL'
      )
    ) {
      qualityScore += 1;
      reasons.push(
        'BEAR-FVG'
      );
    }
  }

  let retest = {
    breakout: false,
    retest: false
  };

  let fib = {
    nearFib: false,
    golden: false,
    levels: null
  };

  if (
    signal === 'BUY' ||
    signal === 'SELL'
  ) {
    retest =
      detectRetest(
        bars,
        signal
      );

    if (
      retest.breakout
    ) {
      qualityScore += 1;
      reasons.push(
        'BREAKOUT'
      );
    }

    if (
      retest.retest
    ) {
      qualityScore += 1;
      reasons.push(
        'RETEST'
      );
    }

    fib =
      fibContext(
        bars,
        signalBar.close,
        atrValue,
        signal
      );

    if (
      fib.golden
    ) {
      qualityScore += 2;
      reasons.push(
        'FIB-GOLDEN'
      );
    }

    else if (
      fib.nearFib
    ) {
      qualityScore += 1;
      reasons.push(
        'FIB'
      );
    }
  }

  qualityScore =
    clamp(
      qualityScore,
      0,
      12
    );

  const quality =
    qualityScore >= 7
      ? 'STRONG'
      : (
          qualityScore >= 4
            ? 'GOOD'
            : 'NEUTRAL'
        );

  return {
    symbol,

    signal,
    nextLastSignal,

    signalBar,

    signalClose:
      signalBar.close,

    atr:
      atrValue,

    support,
    resistance,

    fastEma,
    slowEma,

    ema50,
    ema200,

    ema200Ready:
      Number.isFinite(
        ema200
      ),

    cmo:
      cmoValue,

    dmi,

    bodyRatio,

    volume:
      signalVolume,

    volumeSma,

    highVolume,

    strongBull,
    strongBear,

    rawBuy,
    rawSell,

    retest,
    fib,

    qualityScore,
    quality,

    reasons
  };
}

// ============================================================
// MONGO MODELS
// ============================================================

const accountSchema =
  new mongoose.Schema(
    {
      key: {
        type: String,
        unique: true,
        index: true
      },

      balance: Number,

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

      breakevens: {
        type: Number,
        default: 0
      },

      totalR: {
        type: Number,
        default: 0
      },

      telegramChatId: String,

      updatedAt: Date
    },
    {
      strict: false
    }
  );

const tradeSchema =
  new mongoose.Schema(
    {
      accountKey: String,

      symbol: String,
      direction: String,

      status: String,

      entryPrice: Number,

      stopLoss: Number,
      initialStopLoss: Number,

      takeProfit: Number,

      breakEvenTriggerPrice:
        Number,

      breakEvenActive:
        Boolean,

      riskDistance:
        Number,

      riskAmount:
        Number,

      quantity:
        Number,

      signalPrice:
        Number,

      signalBarTime:
        String,

      qualityScore:
        Number,

      quality:
        String,

      reasons:
        [String],

      openedAt:
        Date,

      closedAt:
        Date,

      exitPrice:
        Number,

      exitReason:
        String,

      resultR:
        Number,

      pnl:
        Number
    },
    {
      strict: false
    }
  );

const signalSchema =
  new mongoose.Schema(
    {
      accountKey: String,

      symbol: String,
      direction: String,

      signalPrice: Number,

      signalBarTime: String,

      cmo: Number,
      atr: Number,

      bodyRatio: Number,

      qualityScore: Number,
      quality: String,

      reasons: [String],

      createdAt: Date,

      executed: Boolean,

      skipReason: String
    },
    {
      strict: false
    }
  );

const Account =
  mongoose.models.LomyForexAccount ||
  mongoose.model(
    'LomyForexAccount',
    accountSchema
  );

const Trade =
  mongoose.models.LomyForexTrade ||
  mongoose.model(
    'LomyForexTrade',
    tradeSchema
  );

const Signal =
  mongoose.models.LomyForexSignal ||
  mongoose.model(
    'LomyForexSignal',
    signalSchema
  );

let accountCache = null;

// ============================================================
// ACCOUNT
// ============================================================

async function loadAccount() {
  let account =
    await Account.findOne({
      key: PAPER.accountKey
    });

  if (!account) {
    account =
      await Account.create({
        key:
          PAPER.accountKey,

        balance:
          PAPER.startingBalance,

        totalTrades: 0,
        wins: 0,
        losses: 0,
        breakevens: 0,
        totalR: 0,

        updatedAt:
          new Date()
      });
  }

  accountCache =
    account;

  return account;
}

async function saveAccount() {
  if (!accountCache) {
    return;
  }

  accountCache.updatedAt =
    new Date();

  await accountCache.save();
}

// ============================================================
// TELEGRAM
// ============================================================

let bot = null;

function telegramAvailable() {
  return Boolean(
    bot &&
    state.telegramReady &&
    accountCache?.telegramChatId
  );
}

async function telegramSend(
  text
) {
  if (
    !telegramAvailable()
  ) {
    return;
  }

  try {
    await bot.telegram.sendMessage(
      accountCache.telegramChatId,
      text
    );
  }

  catch (error) {
    console.warn(
      'Telegram send error:',
      safeError(error)
    );
  }
}

function registerTelegramCommands() {
  if (!bot) {
    return;
  }

  bot.start(
    async ctx => {
      try {
        accountCache.telegramChatId =
          String(
            ctx.chat.id
          );

        await saveAccount();

        await ctx.reply(
          [
            '🤖 LOMY FOREX',
            VERSION,
            '',
            'PAPER ONLY',
            '',
            '/status',
            '/balance',
            '/stats',
            '/positions',
            '/trades',
            '/pairs'
          ].join('\n')
        );
      }

      catch (error) {
        console.warn(
          safeError(error)
        );
      }
    }
  );

  bot.command(
    'status',
    async ctx => {
      const readyCount =
        [...state.pairState.values()]
          .filter(
            item =>
              item.initialized
          )
          .length;

      await ctx.reply(
        [
          `🤖 ${VERSION}`,
          '',
          `Mode: ${MODE}`,
          `Market: ${
            state.marketReady
              ? 'READY'
              : 'PARTIAL'
          }`,
          `Pairs ready: ${readyCount}/${INSTRUMENTS.length}`,
          `Open trades: ${state.openTrades.size}`,
          `Scans: ${state.totalSignalScans}`,
          `Signals: ${state.rawSignals}`,
          `Executed: ${state.executedSignals}`,
          `Skipped: ${state.skippedSignals}`,
          `BE moves: ${state.breakEvenMoves}`,
          '',
          `TP: 1.2R`,
          `Break-even: +0.6R`
        ].join('\n')
      );
    }
  );

  bot.command(
    'balance',
    async ctx => {
      await ctx.reply(
        [
          '💰 PAPER ACCOUNT',
          '',
          `Balance: ${
            fmtMoney(
              accountCache?.balance
            )
          }`,
          `Risk/trade: ${
            PAPER.riskPctPerTrade
          }%`,
          `Open: ${
            state.openTrades.size
          }`
        ].join('\n')
      );
    }
  );

  bot.command(
    'stats',
    async ctx => {
      await ctx.reply(
        [
          '📊 STATS',
          '',
          `Trades: ${
            accountCache?.totalTrades || 0
          }`,
          `Wins: ${
            accountCache?.wins || 0
          }`,
          `Losses: ${
            accountCache?.losses || 0
          }`,
          `Break-even: ${
            accountCache?.breakevens || 0
          }`,
          `Total R: ${
            n(
              accountCache?.totalR,
              0
            ).toFixed(2)
          }R`
        ].join('\n')
      );
    }
  );

  bot.command(
    'positions',
    async ctx => {
      if (
        state.openTrades.size === 0
      ) {
        await ctx.reply(
          '📭 No open PAPER trades.'
        );

        return;
      }

      const lines = [
        '📌 OPEN POSITIONS',
        ''
      ];

      for (
        const trade
        of state.openTrades.values()
      ) {
        lines.push(
          `${trade.direction} ${trade.symbol}`
        );

        lines.push(
          `Entry: ${
            fmtPrice(
              trade.entryPrice,
              trade.symbol
            )
          }`
        );

        lines.push(
          `SL: ${
            fmtPrice(
              trade.stopLoss,
              trade.symbol
            )
          }`
        );

        lines.push(
          `TP: ${
            fmtPrice(
              trade.takeProfit,
              trade.symbol
            )
          }`
        );

        lines.push(
          trade.breakEvenActive
            ? 'BE: ACTIVE'
            : `BE trigger: ${
                fmtPrice(
                  trade.breakEvenTriggerPrice,
                  trade.symbol
                )
              }`
        );

        lines.push('');
      }

      await ctx.reply(
        lines.join('\n')
      );
    }
  );

  bot.command(
    'trades',
    async ctx => {
      const trades =
        await Trade.find({
          accountKey:
            PAPER.accountKey,
          status: 'CLOSED'
        })
          .sort({
            closedAt: -1
          })
          .limit(10)
          .lean();

      if (
        trades.length === 0
      ) {
        await ctx.reply(
          '📭 No closed trades.'
        );

        return;
      }

      const lines = [
        '🧾 LAST TRADES',
        ''
      ];

      for (
        const trade
        of trades
      ) {
        lines.push(
          `${trade.symbol} ${trade.direction} | ${
            n(
              trade.resultR,
              0
            ).toFixed(2)
          }R | ${
            trade.exitReason
          }`
        );
      }

      await ctx.reply(
        lines.join('\n')
      );
    }
  );

  bot.command(
    'pairs',
    async ctx => {
      const lines = [
        '📡 PAIRS',
        ''
      ];

      for (
        const symbol
        of INSTRUMENTS
      ) {
        const memory =
          state.pairState.get(
            symbol
          );

        lines.push(
          `${memory?.initialized ? '✅' : '⚠️'} ${symbol} | bars=${
            memory?.bars?.length || 0
          } | EMA200=${
            memory?.ema200Ready
              ? 'READY'
              : 'WARMUP'
          }`
        );
      }

      await ctx.reply(
        lines.join('\n')
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

  bot =
    new Telegraf(
      TELEGRAM_BOT_TOKEN
    );

  registerTelegramCommands();

  try {
    const me =
      await bot.telegram.getMe();

    state.telegramReady =
      true;

    console.log(
      `✅ Telegram authenticated: @${me.username}`
    );

    bot.launch({
      dropPendingUpdates: true
    })
      .then(() => {
        console.log(
          '✅ Telegram polling started'
        );
      })
      .catch(error => {
        console.error(
          '❌ Telegram polling:',
          safeError(error)
        );
      });
  }

  catch (error) {
    state.telegramReady =
      false;

    console.error(
      '❌ Telegram auth:',
      safeError(error)
    );
  }
}

// ============================================================
// TRADE RESTORE
// ============================================================

async function restoreOpenTrades() {
  const trades =
    await Trade.find({
      accountKey:
        PAPER.accountKey,

      status: 'OPEN'
    }).lean();

  state.openTrades.clear();

  for (
    const trade
    of trades
  ) {
    state.openTrades.set(
      trade.symbol,
      trade
    );
  }

  console.log(
    `✅ Restored ${trades.length} open PAPER trade(s)`
  );
}

// ============================================================
// SIGNAL JOURNAL
// ============================================================

async function saveSignal(
  analysis,
  executed = false,
  skipReason = ''
) {
  try {
    await Signal.create({
      accountKey:
        PAPER.accountKey,

      symbol:
        analysis.symbol,

      direction:
        analysis.signal,

      signalPrice:
        analysis.signalClose,

      signalBarTime:
        analysis.signalBar?.openTime,

      cmo:
        analysis.cmo,

      atr:
        analysis.atr,

      bodyRatio:
        analysis.bodyRatio,

      qualityScore:
        analysis.qualityScore,

      quality:
        analysis.quality,

      reasons:
        analysis.reasons,

      createdAt:
        new Date(),

      executed,

      skipReason
    });
  }

  catch (error) {
    console.warn(
      'Signal journal error:',
      safeError(error)
    );
  }
}

// ============================================================
// TRADE OPEN
// ============================================================

async function openPaperTrade(
  analysis,
  quote
) {
  const symbol =
    analysis.symbol;

  const direction =
    analysis.signal;

  if (
    state.openTrades.has(
      symbol
    )
  ) {
    state.skippedSignals++;

    await saveSignal(
      analysis,
      false,
      'POSITION_ALREADY_OPEN'
    );

    return;
  }

  if (
    state.openTrades.size >=
      PAPER.maxOpenTrades
  ) {
    state.skippedSignals++;

    await saveSignal(
      analysis,
      false,
      'MAX_OPEN_TRADES'
    );

    return;
  }

  const entryPrice =
    direction === 'BUY'
      ? quote.ask
      : quote.bid;

  if (
    !Number.isFinite(
      entryPrice
    )
  ) {
    state.skippedSignals++;

    await saveSignal(
      analysis,
      false,
      'INVALID_ENTRY_QUOTE'
    );

    return;
  }

  const atrValue =
    analysis.atr;

  if (
    !Number.isFinite(
      atrValue
    ) ||
    atrValue <= 0
  ) {
    state.skippedSignals++;

    await saveSignal(
      analysis,
      false,
      'INVALID_ATR'
    );

    return;
  }

  const entryMove =
    Math.abs(
      entryPrice -
      analysis.signalClose
    );

  if (
    entryMove >
      atrValue *
      STRATEGY.maxEntryMoveAtr
  ) {
    state.skippedSignals++;

    await saveSignal(
      analysis,
      false,
      'ENTRY_TOO_FAR_FROM_SIGNAL'
    );

    console.log(
      `⏭ ${symbol} skipped: entry moved ${
        (
          entryMove /
          atrValue
        ).toFixed(2)
      } ATR`
    );

    return;
  }

  let stopLoss;

  if (
    direction === 'BUY'
  ) {
    stopLoss =
      Number.isFinite(
        analysis.support
      )
        ? (
            analysis.support -
            atrValue *
            STRATEGY.atrMargin
          )
        : (
            analysis.signalClose -
            atrValue
          );

    if (
      !Number.isFinite(
        stopLoss
      ) ||
      stopLoss >= entryPrice
    ) {
      stopLoss =
        entryPrice -
        atrValue;
    }
  }

  else {
    stopLoss =
      Number.isFinite(
        analysis.resistance
      )
        ? (
            analysis.resistance +
            atrValue *
            STRATEGY.atrMargin
          )
        : (
            analysis.signalClose +
            atrValue
          );

    if (
      !Number.isFinite(
        stopLoss
      ) ||
      stopLoss <= entryPrice
    ) {
      stopLoss =
        entryPrice +
        atrValue;
    }
  }

  const riskDistance =
    direction === 'BUY'
      ? (
          entryPrice -
          stopLoss
        )
      : (
          stopLoss -
          entryPrice
        );

  if (
    !Number.isFinite(
      riskDistance
    ) ||
    riskDistance <= 0
  ) {
    state.skippedSignals++;

    await saveSignal(
      analysis,
      false,
      'INVALID_RISK_DISTANCE'
    );

    return;
  }

  const takeProfit =
    direction === 'BUY'
      ? (
          entryPrice +
          riskDistance *
          STRATEGY.riskReward
        )
      : (
          entryPrice -
          riskDistance *
          STRATEGY.riskReward
        );

  const breakEvenTriggerPrice =
    direction === 'BUY'
      ? (
          entryPrice +
          riskDistance *
          STRATEGY.breakEvenTriggerR
        )
      : (
          entryPrice -
          riskDistance *
          STRATEGY.breakEvenTriggerR
        );

  const geometryValid =
    direction === 'BUY'
      ? (
          stopLoss <
            entryPrice &&
          takeProfit >
            entryPrice
        )
      : (
          takeProfit <
            entryPrice &&
          stopLoss >
            entryPrice
        );

  if (
    !geometryValid
  ) {
    state.skippedSignals++;

    await saveSignal(
      analysis,
      false,
      'INVALID_TRADE_GEOMETRY'
    );

    console.warn(
      `⚠️ Invalid geometry ${symbol}`
    );

    return;
  }

  const balance =
    n(
      accountCache?.balance,
      PAPER.startingBalance
    );

  const riskAmount =
    balance *
    (
      PAPER.riskPctPerTrade /
      100
    );

  const quantity =
    riskAmount /
    riskDistance;

  const trade =
    {
      accountKey:
        PAPER.accountKey,

      symbol,
      direction,

      status: 'OPEN',

      entryPrice,

      stopLoss,

      initialStopLoss:
        stopLoss,

      takeProfit,

      breakEvenTriggerPrice,

      breakEvenActive:
        false,

      riskDistance,

      riskAmount,

      quantity,

      signalPrice:
        analysis.signalClose,

      signalBarTime:
        analysis.signalBar?.openTime,

      qualityScore:
        analysis.qualityScore,

      quality:
        analysis.quality,

      reasons:
        analysis.reasons,

      openedAt:
        new Date()
    };

  const document =
    await Trade.create(
      trade
    );

  const saved =
    document.toObject();

  state.openTrades.set(
    symbol,
    saved
  );

  state.executedSignals++;

  await saveSignal(
    analysis,
    true,
    ''
  );

  console.log(
    `✅ OPEN ${symbol} ${direction} | Entry=${fmtPrice(entryPrice, symbol)} | SL=${fmtPrice(stopLoss, symbol)} | TP=${fmtPrice(takeProfit, symbol)} | BE=${fmtPrice(breakEvenTriggerPrice, symbol)}`
  );

  await telegramSend(
    [
      `🚀 PAPER ${direction} ${symbol}`,
      '',
      `Entry: ${fmtPrice(entryPrice, symbol)}`,
      `SL: ${fmtPrice(stopLoss, symbol)}`,
      `TP 1.2R: ${fmtPrice(takeProfit, symbol)}`,
      `BE +0.6R: ${fmtPrice(breakEvenTriggerPrice, symbol)}`,
      '',
      `Signal: ${fmtPrice(analysis.signalClose, symbol)}`,
      `CMO: ${n(analysis.cmo, 0).toFixed(1)}`,
      `Quality: ${analysis.qualityScore}/12 ${analysis.quality}`,
      `SMC: ${
        analysis.reasons.length
          ? analysis.reasons.join(', ')
          : 'none'
      }`
    ].join('\n')
  );
}

// ============================================================
// CLOSE TRADE
// ============================================================

async function closePaperTrade(
  trade,
  exitPrice,
  reason
) {
  const symbol =
    trade.symbol;

  const direction =
    trade.direction;

  const riskDistance =
    n(
      trade.riskDistance
    );

  if (
    !Number.isFinite(
      riskDistance
    ) ||
    riskDistance <= 0
  ) {
    return;
  }

  const move =
    direction === 'BUY'
      ? (
          exitPrice -
          trade.entryPrice
        )
      : (
          trade.entryPrice -
          exitPrice
        );

  const resultR =
    move /
    riskDistance;

  const pnl =
    n(
      trade.riskAmount,
      0
    ) *
    resultR;

  await Trade.updateOne(
    {
      _id:
        trade._id
    },
    {
      $set: {
        status: 'CLOSED',

        exitPrice,

        exitReason:
          reason,

        resultR,

        pnl,

        closedAt:
          new Date()
      }
    }
  );

  state.openTrades.delete(
    symbol
  );

  accountCache.balance =
    n(
      accountCache.balance,
      PAPER.startingBalance
    ) + pnl;

  accountCache.totalTrades =
    n(
      accountCache.totalTrades,
      0
    ) + 1;

  accountCache.totalR =
    n(
      accountCache.totalR,
      0
    ) + resultR;

  if (
    reason ===
      'BREAK_EVEN' ||
    Math.abs(resultR) <
      0.10
  ) {
    accountCache.breakevens =
      n(
        accountCache.breakevens,
        0
      ) + 1;
  }

  else if (
    resultR > 0
  ) {
    accountCache.wins =
      n(
        accountCache.wins,
        0
      ) + 1;
  }

  else {
    accountCache.losses =
      n(
        accountCache.losses,
        0
      ) + 1;
  }

  await saveAccount();

  console.log(
    `${resultR >= 0 ? '✅' : '❌'} CLOSE ${symbol} ${reason} ${resultR.toFixed(2)}R`
  );

  await telegramSend(
    [
      `🏁 ${symbol} CLOSED`,
      '',
      `Reason: ${reason}`,
      `Exit: ${fmtPrice(exitPrice, symbol)}`,
      `Result: ${resultR.toFixed(2)}R`,
      `PnL: ${fmtMoney(pnl)}`,
      `Balance: ${fmtMoney(accountCache.balance)}`
    ].join('\n')
  );
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

  trade.stopLoss =
    trade.entryPrice;

  state.breakEvenMoves++;

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
          trade.entryPrice
      }
    }
  );

  state.openTrades.set(
    trade.symbol,
    trade
  );

  console.log(
    `🛡 BREAK-EVEN ${trade.symbol} | SL moved to ${fmtPrice(trade.entryPrice, trade.symbol)}`
  );

  await telegramSend(
    [
      `🛡 BREAK-EVEN ACTIVATED`,
      '',
      `${trade.direction} ${trade.symbol}`,
      `Entry: ${fmtPrice(trade.entryPrice, trade.symbol)}`,
      `New SL: ${fmtPrice(trade.entryPrice, trade.symbol)}`,
      `TP: ${fmtPrice(trade.takeProfit, trade.symbol)}`
    ].join('\n')
  );
}

// ============================================================
// QUOTE / POSITION MANAGEMENT
// ============================================================

async function manageOpenTrades() {
  if (
    state.quoteRunning ||
    state.openTrades.size === 0
  ) {
    return;
  }

  state.quoteRunning =
    true;

  try {
    const symbols =
      [...state.openTrades.keys()];

    const quotes =
      await fetchLatestQuotes(
        symbols
      );

    state.totalQuotePolls++;

    state.lastQuotePollAt =
      new Date();

    for (
      const symbol
      of symbols
    ) {
      const trade =
        state.openTrades.get(
          symbol
        );

      const quote =
        quotes.get(
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

      const exitPrice =
        trade.direction ===
          'BUY'
          ? quote.bid
          : quote.ask;

      if (
        !Number.isFinite(
          exitPrice
        )
      ) {
        continue;
      }

      // ------------------------------------------
      // BUY
      // ------------------------------------------

      if (
        trade.direction ===
          'BUY'
      ) {
        if (
          !trade.breakEvenActive &&
          exitPrice >=
            trade.breakEvenTriggerPrice
        ) {
          await activateBreakEven(
            trade
          );

          continue;
        }

        if (
          exitPrice <=
            trade.stopLoss
        ) {
          await closePaperTrade(
            trade,
            exitPrice,
            trade.breakEvenActive
              ? 'BREAK_EVEN'
              : 'STOP_LOSS'
          );

          continue;
        }

        if (
          exitPrice >=
            trade.takeProfit
        ) {
          await closePaperTrade(
            trade,
            exitPrice,
            'TAKE_PROFIT'
          );

          continue;
        }
      }

      // ------------------------------------------
      // SELL
      // ------------------------------------------

      else {
        if (
          !trade.breakEvenActive &&
          exitPrice <=
            trade.breakEvenTriggerPrice
        ) {
          await activateBreakEven(
            trade
          );

          continue;
        }

        if (
          exitPrice >=
            trade.stopLoss
        ) {
          await closePaperTrade(
            trade,
            exitPrice,
            trade.breakEvenActive
              ? 'BREAK_EVEN'
              : 'STOP_LOSS'
          );

          continue;
        }

        if (
          exitPrice <=
            trade.takeProfit
        ) {
          await closePaperTrade(
            trade,
            exitPrice,
            'TAKE_PROFIT'
          );

          continue;
        }
      }
    }
  }

  catch (error) {
    console.warn(
      'Quote management error:',
      safeError(error)
    );
  }

  finally {
    state.quoteRunning =
      false;
  }
}

// ============================================================
// REVERSE SIGNAL MANAGEMENT
// ============================================================

async function maybeCloseOnReverseSignal(
  analysis,
  quote
) {
  const trade =
    state.openTrades.get(
      analysis.symbol
    );

  if (!trade) {
    return false;
  }

  const opposite =
    (
      trade.direction === 'BUY' &&
      analysis.signal === 'SELL'
    ) ||
    (
      trade.direction === 'SELL' &&
      analysis.signal === 'BUY'
    );

  if (!opposite) {
    return false;
  }

  const exitPrice =
    trade.direction === 'BUY'
      ? quote.bid
      : quote.ask;

  if (
    !Number.isFinite(
      exitPrice
    )
  ) {
    return false;
  }

  const move =
    trade.direction === 'BUY'
      ? (
          exitPrice -
          trade.entryPrice
        )
      : (
          trade.entryPrice -
          exitPrice
        );

  const currentR =
    move /
    trade.riskDistance;

  // لا نقفل صفقة خسرانة فقط بسبب إشارة عكسية.
  if (
    currentR < 0 &&
    !trade.breakEvenActive
  ) {
    console.log(
      `↩ ${analysis.symbol} reverse signal ignored | trade=${currentR.toFixed(2)}R`
    );

    return false;
  }

  await closePaperTrade(
    trade,
    exitPrice,
    'REVERSE_SIGNAL'
  );

  return true;
}

// ============================================================
// CONCURRENCY
// ============================================================

async function mapWithConcurrency(
  items,
  concurrency,
  handler
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
        return;
      }

      try {
        results[current] =
          await handler(
            items[current],
            current
          );
      }

      catch (error) {
        results[current] = {
          error
        };
      }
    }
  }

  const workers = [];

  for (
    let i = 0;
    i <
      Math.min(
        concurrency,
        items.length
      );
    i++
  ) {
    workers.push(
      worker()
    );
  }

  await Promise.all(
    workers
  );

  return results;
}

// ============================================================
// ANALYZE ONE SYMBOL
// ============================================================

async function analyzeSymbol(
  symbol
) {
  const memory =
    state.pairState.get(
      symbol
    );

  if (!memory) {
    return;
  }

  try {
    const bars =
      await fetchOhlc(
        symbol
      );

    memory.bars =
      bars;

    memory.ema200Ready =
      bars.length >=
      EMA200_CONTEXT_HISTORY;

    if (
      bars.length <
        CORE_MIN_HISTORY
    ) {
      memory.initialized =
        false;

      memory.errors++;

      console.warn(
        `⚠️ ${symbol}: insufficient core history ${bars.length}/${CORE_MIN_HISTORY}`
      );

      return;
    }

    memory.initialized =
      true;

    memory.errors = 0;

    const latestBar =
      bars[
        bars.length -
        1
      ];

    const latestBarTime =
      latestBar?.openTime;

    if (
      !latestBarTime
    ) {
      return;
    }

    // لا نكرر تحليل نفس الشمعة.
    if (
      memory.lastClosedBarTime ===
        latestBarTime
    ) {
      return;
    }

    const analysis =
      analyzeMarket(
        symbol,
        bars,
        memory.lastSignal
      );

    memory.lastClosedBarTime =
      latestBarTime;

    memory.lastAnalysis =
      analysis;

    if (
      analysis.signal ===
        'WAIT'
    ) {
      return;
    }

    state.rawSignals++;

    memory.lastSignal =
      analysis.nextLastSignal;

    const quoteMap =
      await fetchLatestQuotes(
        [symbol]
      );

    const quote =
      quoteMap.get(
        symbol
      );

    if (!quote) {
      state.skippedSignals++;

      await saveSignal(
        analysis,
        false,
        'NO_FRESH_QUOTE'
      );

      console.warn(
        `⚠️ ${symbol}: no fresh quote`
      );

      return;
    }

    state.latestQuotes.set(
      symbol,
      quote
    );

    console.log(
      `📡 ${symbol} ${analysis.signal} V5.1 | CMO ${n(analysis.cmo, 0).toFixed(1)} | body ${(analysis.bodyRatio * 100).toFixed(0)}% | Q=${analysis.qualityScore}`
    );

    const closed =
      await maybeCloseOnReverseSignal(
        analysis,
        quote
      );

    if (closed) {
      return;
    }

    await openPaperTrade(
      analysis,
      quote
    );
  }

  catch (error) {
    memory.errors++;

    state.lastMarketError =
      `${symbol}: ${
        safeError(error)
      }`;

    console.warn(
      `⚠️ SCAN ${symbol}: ${
        safeError(error)
      }`
    );
  }
}

// ============================================================
// FULL SCAN
// ============================================================

async function runFullScan() {
  if (
    state.scanRunning ||
    state.initializing
  ) {
    return;
  }

  state.scanRunning =
    true;

  const beforeRaw =
    state.rawSignals;

  const beforeExecuted =
    state.executedSignals;

  const beforeSkipped =
    state.skippedSignals;

  try {
    state.totalSignalScans++;

    state.lastSignalScanAt =
      new Date();

    console.log(
      `🔎 Scan #${state.totalSignalScans} | ${INSTRUMENTS.length} instruments | ${TIMEFRAME}`
    );

    const results =
      await mapWithConcurrency(
        INSTRUMENTS,
        OHLC_CONCURRENCY,
        async symbol => {
          const memory =
            state.pairState.get(
              symbol
            );

          if (
            !memory?.initialized
          ) {
            return;
          }

          await analyzeSymbol(
            symbol
          );
        }
      );

    const errors =
      results.filter(
        x => x?.error
      );

    if (
      errors.length
    ) {
      console.warn(
        `⚠️ ${errors.length} scan error(s)`
      );
    }

    console.log(
      `✅ Scan complete | raw=${
        state.rawSignals -
        beforeRaw
      } executed=${
        state.executedSignals -
        beforeExecuted
      } skipped=${
        state.skippedSignals -
        beforeSkipped
      }`
    );
  }

  finally {
    state.scanRunning =
      false;
  }
}

// ============================================================
// INITIALIZE MARKET
// ============================================================

async function initializeMarket() {
  state.initializing =
    true;

  state.marketReady =
    false;

  console.log(
    `⏳ Initializing ${INSTRUMENTS.length} instruments on ${TIMEFRAME}...`
  );

  const HARD_INIT_TIMEOUT_MS =
    15000;

  const results =
    await mapWithConcurrency(
      INSTRUMENTS,
      OHLC_CONCURRENCY,

      async symbol => {
        const memory =
          state.pairState.get(
            symbol
          );

        try {
          const bars =
            await Promise.race([
              fetchOhlc(
                symbol
              ),

              new Promise(
                (
                  _,
                  reject
                ) =>
                  setTimeout(
                    () =>
                      reject(
                        new Error(
                          `INIT_TIMEOUT_${HARD_INIT_TIMEOUT_MS}MS`
                        )
                      ),

                    HARD_INIT_TIMEOUT_MS
                  )
              )
            ]);

          memory.bars =
            bars;

          memory.initialized =
            bars.length >=
              CORE_MIN_HISTORY;

          memory.ema200Ready =
            bars.length >=
              EMA200_CONTEXT_HISTORY;

          memory.lastClosedBarTime =
            null;

          if (
            memory.initialized
          ) {
            memory.errors =
              0;

            console.log(
              `✅ INIT ${symbol}: READY | bars=${bars.length} | EMA200=${
                memory.ema200Ready
                  ? 'READY'
                  : 'CONTEXT-WARMUP'
              }`
            );

            return {
              symbol,
              ready: true,
              bars:
                bars.length,
              ema200Ready:
                memory.ema200Ready
            };
          }

          memory.errors++;

          console.warn(
            `⚠️ INIT ${symbol}: CORE-WARMUP | bars=${bars.length}/${CORE_MIN_HISTORY}`
          );

          return {
            symbol,
            ready: false,
            bars:
              bars.length,
            reason:
              'CORE_WARMUP'
          };
        }

        catch (error) {
          memory.initialized =
            false;

          memory.errors++;

          const message =
            safeError(error);

          state.lastMarketError =
            `${symbol}: ${message}`;

          console.error(
            `❌ INIT ${symbol}: ${message}`
          );

          return {
            symbol,
            ready: false,

            bars:
              memory.bars
                ?.length || 0,

            reason:
              message
          };
        }
      }
    );

  const normalizedResults =
    results.map(
      (
        result,
        index
      ) => {
        if (
          result &&
          typeof result ===
            'object' &&
          'ready' in result
        ) {
          return result;
        }

        if (
          result?.error
        ) {
          return {
            symbol:
              INSTRUMENTS[
                index
              ],

            ready:
              false,

            bars: 0,

            reason:
              safeError(
                result.error
              )
          };
        }

        return {
          symbol:
            INSTRUMENTS[
              index
            ],

          ready:
            false,

          bars: 0,

          reason:
            'UNKNOWN_INIT_RESULT'
        };
      }
    );

  const ready =
    normalizedResults.filter(
      result =>
        result.ready
    ).length;

  const failed =
    normalizedResults.filter(
      result =>
        !result.ready
    );

  const minimumReady =
    Math.max(
      1,
      Math.floor(
        INSTRUMENTS.length *
        0.80
      )
    );

  state.marketReady =
    ready >=
      minimumReady;

  state.initializing =
    false;

  console.log(
    `✅ Market initialized: ${ready}/${INSTRUMENTS.length}`
  );

  if (
    failed.length
  ) {
    console.warn(
      `⚠️ Init unavailable (${failed.length}): ` +
      failed
        .map(
          item =>
            `${item.symbol}[${item.bars}:${item.reason}]`
        )
        .join(', ')
    );
  }

  console.log(
    state.marketReady
      ? `✅ Market engine READY | minimum=${minimumReady}`
      : `⚠️ Market engine PARTIAL | ready=${ready} minimum=${minimumReady}`
  );
}

// ============================================================
// SCAN SCHEDULER
// ============================================================

function currentQuarterSlot() {
  const now =
    new Date();

  const minute =
    now.getUTCMinutes();

  const quarter =
    Math.floor(
      minute / 15
    );

  return [
    now.getUTCFullYear(),
    now.getUTCMonth(),
    now.getUTCDate(),
    now.getUTCHours(),
    quarter
  ].join('-');
}

async function scanSchedulerTick() {
  if (
    state.initializing
  ) {
    return;
  }

  const now =
    new Date();

  const minute =
    now.getUTCMinutes();

  const second =
    now.getUTCSeconds();

  const isQuarter =
    minute % 15 === 0;

  if (
    !isQuarter ||
    second < 4
  ) {
    return;
  }

  const slot =
    currentQuarterSlot();

  if (
    state.lastScanSlot ===
      slot
  ) {
    return;
  }

  state.lastScanSlot =
    slot;

  await runFullScan();
}

// ============================================================
// EXPRESS
// ============================================================

const app =
  express();

app.use(
  express.json()
);

app.get(
  '/',
  (
    req,
    res
  ) => {
    res
      .status(200)
      .send(
        `${VERSION} | PAPER | ${
          state.marketReady
            ? 'READY'
            : 'PARTIAL'
        }`
      );
  }
);

app.get(
  '/health',
  (
    req,
    res
  ) => {
    const readyPairs =
      [...state.pairState.values()]
        .filter(
          item =>
            item.initialized
        )
        .length;

    res.status(200).json({
      ok: true,

      version:
        VERSION,

      mode:
        MODE,

      liveTrading:
        LIVE_TRADING,

      uptimeSeconds:
        Math.floor(
          process.uptime()
        ),

      mongoReady:
        state.mongoReady,

      telegramReady:
        state.telegramReady,

      marketReady:
        state.marketReady,

      initializing:
        state.initializing,

      readyPairs:
        `${readyPairs}/${INSTRUMENTS.length}`,

      openTrades:
        state.openTrades.size,

      scans:
        state.totalSignalScans,

      rawSignals:
        state.rawSignals,

      executedSignals:
        state.executedSignals,

      skippedSignals:
        state.skippedSignals,

      breakEvenMoves:
        state.breakEvenMoves,

      lastScanAt:
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
  (
    req,
    res
  ) => {
    const pairs =
      {};

    for (
      const [
        symbol,
        memory
      ]
      of state.pairState
    ) {
      pairs[symbol] = {
        initialized:
          memory.initialized,

        bars:
          memory.bars.length,

        ema200Ready:
          memory.ema200Ready,

        lastClosedBarTime:
          memory.lastClosedBarTime,

        errors:
          memory.errors
      };
    }

    res.json({
      version:
        VERSION,

      mode:
        MODE,

      strategy: {
        timeframe:
          TIMEFRAME,

        riskReward:
          STRATEGY.riskReward,

        breakEvenTriggerR:
          STRATEGY.breakEvenTriggerR,

        coreMinHistory:
          CORE_MIN_HISTORY,

        ema200ContextHistory:
          EMA200_CONTEXT_HISTORY
      },

      state: {
        marketReady:
          state.marketReady,

        initializing:
          state.initializing,

        openTrades:
          state.openTrades.size,

        totalSignalScans:
          state.totalSignalScans,

        totalQuotePolls:
          state.totalQuotePolls,

        rawSignals:
          state.rawSignals,

        executedSignals:
          state.executedSignals,

        skippedSignals:
          state.skippedSignals,

        breakEvenMoves:
          state.breakEvenMoves
      },

      account: {
        balance:
          accountCache?.balance,

        totalTrades:
          accountCache?.totalTrades,

        wins:
          accountCache?.wins,

        losses:
          accountCache?.losses,

        breakevens:
          accountCache?.breakevens,

        totalR:
          accountCache?.totalR
      },

      pairs
    });
  }
);

// ============================================================
// DATABASE
// ============================================================

async function initMongo() {
  if (
    !MONGODB_URI
  ) {
    throw new Error(
      'MONGODB_URI missing'
    );
  }

  await mongoose.connect(
    MONGODB_URI,
    {
      serverSelectionTimeoutMS:
        15000
    }
  );

  state.mongoReady =
    true;

  console.log(
    '✅ MongoDB connected'
  );
}

// ============================================================
// BOOT
// ============================================================

async function boot() {
  console.log(
    '========================================'
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
    '🧠 Core: V5.1 + SMC Sniper + FVG + Liquidity Sweep + Retest + Fibonacci'
  );

  console.log(
    `🎯 R:R 1:${STRATEGY.riskReward} | BE at +${STRATEGY.breakEvenTriggerR}R`
  );

  console.log(
    `📚 Core history: ${CORE_MIN_HISTORY} bars | EMA200 context at ${EMA200_CONTEXT_HISTORY}`
  );

  console.log(
    '========================================'
  );

  app.listen(
    PORT,
    () => {
      console.log(
        `✅ Health server on port ${PORT}`
      );
    }
  );

  await initMongo();

  await loadAccount();

  await restoreOpenTrades();

  await initTelegram();

  await initializeMarket();

  console.log(
    '✅ LOMY Forex loops started'
  );

  // فحص الصفقات المفتوحة كل 3 ثواني.
  setInterval(
    () => {
      manageOpenTrades()
        .catch(
          error =>
            console.warn(
              safeError(error)
            )
        );
    },
    QUOTE_POLL_MS
  );

  // فحص هل دخلنا بداية شمعة 15 دقيقة جديدة.
  setInterval(
    () => {
      scanSchedulerTick()
        .catch(
          error =>
            console.warn(
              safeError(error)
            )
        );
    },
    SCAN_TIMER_MS
  );

  // لو البوت بدأ مباشرة بعد بداية ربع الساعة.
  setTimeout(
    () => {
      scanSchedulerTick()
        .catch(
          error =>
            console.warn(
              safeError(error)
            )
        );
    },
    7000
  );
}

// ============================================================
// SAFE SHUTDOWN
// ============================================================

let shuttingDown =
  false;

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

  console.log(
    `⚠️ ${signal} received`
  );

  try {
    if (bot) {
      bot.stop(
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

process.once(
  'SIGINT',
  () =>
    shutdown(
      'SIGINT'
    )
);

process.once(
  'SIGTERM',
  () =>
    shutdown(
      'SIGTERM'
    )
);

process.on(
  'unhandledRejection',
  error => {
    console.error(
      'Unhandled rejection:',
      safeError(error)
    );
  }
);

process.on(
  'uncaughtException',
  error => {
    console.error(
      'Uncaught exception:',
      safeError(error)
    );
  }
);

// ============================================================
// START
// ============================================================

boot().catch(
  error => {
    console.error(
      '❌ BOOT FAILED:',
      safeError(error)
    );

    process.exit(1);
  }
);
