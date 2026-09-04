'use strict';

const axios = require('axios');
const express = require('express');
const mongoose = require('mongoose');
const { Telegraf } = require('telegraf');

const VERSION = 'LOMY FOREX V1.2.4';
const MODE = 'PAPER';
const LIVE_TRADING = false;

const PORT = Number(process.env.PORT || 10000);
const TELEGRAM_BOT_TOKEN = String(process.env.TELEGRAM_BOT_TOKEN || '').trim();
const MONGODB_URI = String(process.env.MONGODB_URI || '').trim();
const TWELVE_DATA_API_KEY = String(process.env.TWELVE_DATA_API_KEY || '').trim();

const BIQUOTE_BASE = 'https://biquote.io';
const TIMEFRAME = '15m';
const HISTORY_LIMIT = 260;
const CORE_MIN_HISTORY = 60;
const EMA200_CONTEXT_HISTORY = 200;
const OHLC_CONCURRENCY = 4;
const QUOTE_POLL_MS = 3000;
const SCAN_TIMER_MS = 4000;
const JOURNAL_COLLECTION = 'lomyforexjournalv124';

const INSTRUMENTS = [
  'EURUSD','GBPUSD','USDJPY','USDCHF','AUDUSD','NZDUSD','USDCAD',
  'EURGBP','EURJPY','EURCHF','EURAUD','EURNZD','EURCAD',
  'GBPJPY','GBPCHF','GBPAUD','GBPNZD','GBPCAD',
  'AUDJPY','AUDCHF','AUDNZD','AUDCAD',
  'NZDJPY','NZDCHF','NZDCAD','CADJPY','CADCHF','CHFJPY',
  'GBPSGD','EURSGD','XAUUSD'
];

// IMPORTANT: Signal/entry strategy is frozen from V1.2.3.
const STRATEGY = Object.freeze({
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
  riskReward: 1.20,
  breakEvenTriggerR: 0.60,
  maxEntryMoveAtr: 0.50,
  retestLookback: 8,
  fibLookback: 40,
  fibToleranceAtr: 0.20,
  smcFastEmaLen: 50,
  smcSlowEmaLen: 200,
  adxLen: 14,
  adxThreshold: 25,
  pivotLeft: 5,
  pivotRight: 5,
  rejectBullFvgBuys: true,
  buyCmoMax: 80,
  sellCmoMax: -40
});

const PAPER = Object.freeze({
  startingBalance: 300,
  riskPctPerTrade: 0.50,
  portfolioRiskCapPct: 4.00,
  maxOpenTrades: 31,
  accountKey: 'lomy-forex-v124-300usd'
});

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
  protectionRejects: 0,
  portfolioRiskRejects: 0,
  journalEvents: 0,

  pairState: new Map(),
  latestQuotes: new Map(),
  openTrades: new Map(),
  closingTrades: new Set()
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

function n(value, fallback = NaN) {
  const x = Number(value);
  return Number.isFinite(x) ? x : fallback;
}

function safeError(error) {
  return (
    error?.response?.data?.message ||
    error?.response?.data?.error ||
    error?.message ||
    String(error)
  );
}

function clamp(value, minimum, maximum) {
  return Math.max(minimum, Math.min(maximum, value));
}

function fmtMoney(value) {
  return '$' + n(value, 0).toFixed(2);
}

function fmtPrice(value, symbol = '') {
  if (!Number.isFinite(value)) return 'n/a';
  if (symbol === 'XAUUSD') return value.toFixed(2);
  if (symbol.endsWith('JPY')) return value.toFixed(3);
  return value.toFixed(5);
}

function barTimeMs(bar) {
  const t = new Date(bar.openTime).getTime();
  return Number.isFinite(t) ? t : 0;
}

function normalizeBars(rawBars) {
  if (!Array.isArray(rawBars)) return [];

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
        barTimeMs(a) -
        barTimeMs(b)
    );
}

function closedBarsOnly(bars) {
  const now = Date.now();

  const timeframeMs =
    15 *
    60 *
    1000;

  return bars.filter(
    bar =>
      !bar.isOpen &&
      barTimeMs(bar) > 0 &&
      barTimeMs(bar) +
        timeframeMs <=
        now +
        5000
  );
}

function sma(values, length) {
  if (
    !Array.isArray(values) ||
    values.length < length
  ) {
    return NaN;
  }

  const selected =
    values.slice(-length);

  if (
    !selected.every(Number.isFinite)
  ) {
    return NaN;
  }

  return selected.reduce(
    (a, b) => a + b,
    0
  ) / length;
}

function emaSeries(values, length) {
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
    2 /
    (length + 1);

  out[length - 1] =
    values
      .slice(
        0,
        length
      )
      .reduce(
        (a, b) =>
          a + b,
        0
      ) /
    length;

  for (
    let i = length;
    i < values.length;
    i++
  ) {
    out[i] =
      values[i] *
        k +
      out[i - 1] *
        (1 - k);
  }

  return out;
}

function emaLast(values, length) {
  const series =
    emaSeries(
      values,
      length
    );

  return series.length
    ? series[series.length - 1]
    : NaN;
}

function atrLast(bars, length) {
  if (
    bars.length <
    length + 1
  ) {
    return NaN;
  }

  const trueRange = [];

  for (
    let i = 1;
    i < bars.length;
    i++
  ) {
    const current =
      bars[i];

    const previousClose =
      bars[i - 1].close;

    trueRange.push(
      Math.max(
        current.high -
          current.low,

        Math.abs(
          current.high -
          previousClose
        ),

        Math.abs(
          current.low -
          previousClose
        )
      )
    );
  }

  if (
    trueRange.length <
    length
  ) {
    return NaN;
  }

  let atr =
    trueRange
      .slice(
        0,
        length
      )
      .reduce(
        (a, b) =>
          a + b,
        0
      ) /
    length;

  for (
    let i = length;
    i < trueRange.length;
    i++
  ) {
    atr =
      (
        atr *
          (length - 1) +
        trueRange[i]
      ) /
      length;
  }

  return atr;
}

function cmoLast(values, length) {
  if (
    values.length <
    length + 1
  ) {
    return NaN;
  }

  let up = 0;
  let down = 0;

  for (
    let i =
      values.length -
      length;
    i < values.length;
    i++
  ) {
    const difference =
      values[i] -
      values[i - 1];

    if (
      difference >
      0
    ) {
      up += difference;
    } else {
      down +=
        Math.abs(
          difference
        );
    }
  }

  const denominator =
    up + down;

  return denominator === 0
    ? 0
    : 100 *
        (up - down) /
        denominator;
}

function highestHigh(bars) {
  return Math.max(
    ...bars.map(
      bar => bar.high
    )
  );
}

function lowestLow(bars) {
  return Math.min(
    ...bars.map(
      bar => bar.low
    )
  );
}

function dmiAdx(
  bars,
  length = 14
) {
  if (
    bars.length <
    length * 2 + 2
  ) {
    return {
      adx: NaN,
      plusDI: NaN,
      minusDI: NaN
    };
  }

  const trueRange = [];
  const plus = [];
  const minus = [];

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

    plus.push(
      upMove >
        downMove &&
      upMove >
        0
        ? upMove
        : 0
    );

    minus.push(
      downMove >
        upMove &&
      downMove >
        0
        ? downMove
        : 0
    );

    trueRange.push(
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

  let trSmooth =
    trueRange
      .slice(
        0,
        length
      )
      .reduce(
        (a, b) =>
          a + b,
        0
      );

  let plusSmooth =
    plus
      .slice(
        0,
        length
      )
      .reduce(
        (a, b) =>
          a + b,
        0
      );

  let minusSmooth =
    minus
      .slice(
        0,
        length
      )
      .reduce(
        (a, b) =>
          a + b,
        0
      );

  const dx = [];

  let plusDI = NaN;
  let minusDI = NaN;

  for (
    let i = length;
    i < trueRange.length;
    i++
  ) {
    if (
      i >
      length
    ) {
      trSmooth =
        trSmooth -
        trSmooth /
          length +
        trueRange[i];

      plusSmooth =
        plusSmooth -
        plusSmooth /
          length +
        plus[i];

      minusSmooth =
        minusSmooth -
        minusSmooth /
          length +
        minus[i];
    }

    plusDI =
      trSmooth
        ? 100 *
          plusSmooth /
          trSmooth
        : 0;

    minusDI =
      trSmooth
        ? 100 *
          minusSmooth /
          trSmooth
        : 0;

    const denominator =
      plusDI +
      minusDI;

    dx.push(
      denominator
        ? 100 *
          Math.abs(
            plusDI -
            minusDI
          ) /
          denominator
        : 0
    );
  }

  if (
    dx.length <
    length
  ) {
    return {
      adx: NaN,
      plusDI,
      minusDI
    };
  }

  let adx =
    dx
      .slice(
        0,
        length
      )
      .reduce(
        (a, b) =>
          a + b,
        0
      ) /
    length;

  for (
    let i = length;
    i < dx.length;
    i++
  ) {
    adx =
      (
        adx *
          (length - 1) +
        dx[i]
      ) /
      length;
  }

  return {
    adx,
    plusDI,
    minusDI
  };
}

function retestContext(
  prior,
  signal,
  direction
) {
  const window =
    prior.slice(
      -STRATEGY.retestLookback
    );

  if (
    !window.length
  ) {
    return {
      score: 0,
      breakout: false,
      retest: false,
      level: NaN
    };
  }

  const level =
    direction === 'BUY'
      ? highestHigh(window)
      : lowestLow(window);

  let breakout = false;
  let retest = false;

  if (
    direction ===
    'BUY'
  ) {
    breakout =
      signal.close >
      level;

    retest =
      signal.low <=
        level &&
      signal.close >
        level;
  } else {
    breakout =
      signal.close <
      level;

    retest =
      signal.high >=
        level &&
      signal.close <
        level;
  }

  return {
    score:
      Number(breakout) +
      Number(retest),

    breakout,
    retest,
    level
  };
}

function fibContext(
  prior,
  signal,
  atr,
  direction
) {
  const window =
    prior.slice(
      -STRATEGY.fibLookback
    );

  if (
    window.length <
      10 ||
    !Number.isFinite(atr)
  ) {
    return {
      score: 0,
      valid: false,
      goldenZone: false,
      nearFib: false
    };
  }

  const low =
    lowestLow(window);

  const high =
    highestHigh(window);

  const range =
    high - low;

  if (
    !(range > 0)
  ) {
    return {
      score: 0,
      valid: false,
      goldenZone: false,
      nearFib: false
    };
  }

  const levels =
    direction === 'BUY'
      ? {
          level382:
            high -
            range *
              0.382,

          level500:
            high -
            range *
              0.500,

          level618:
            high -
            range *
              0.618,

          level786:
            high -
            range *
              0.786
        }
      : {
          level382:
            low +
            range *
              0.382,

          level500:
            low +
            range *
              0.500,

          level618:
            low +
            range *
              0.618,

          level786:
            low +
            range *
              0.786
        };

  const price =
    signal.close;

  const tolerance =
    atr *
    STRATEGY.fibToleranceAtr;

  const goldenLow =
    Math.min(
      levels.level500,
      levels.level618
    );

  const goldenHigh =
    Math.max(
      levels.level500,
      levels.level618
    );

  const goldenZone =
    price >=
      goldenLow -
        tolerance &&
    price <=
      goldenHigh +
        tolerance;

  const nearFib =
    Object.values(
      levels
    ).some(
      level =>
        Math.abs(
          price -
          level
        ) <=
        tolerance
    );

  return {
    score:
      goldenZone
        ? 2
        : nearFib
          ? 1
          : 0,

    valid: true,
    goldenZone,
    nearFib,
    swingLow: low,
    swingHigh: high,

    ...levels
  };
}

function smcContext(
  bars,
  direction
) {
  const signal =
    bars[bars.length - 1];

  const prior =
    bars.slice(0, -1);

  const closes =
    bars.map(
      bar =>
        bar.close
    );

  const ema50 =
    emaLast(
      closes,
      STRATEGY.smcFastEmaLen
    );

  const ema200 =
    bars.length >=
      EMA200_CONTEXT_HISTORY
      ? emaLast(
          closes,
          STRATEGY.smcSlowEmaLen
        )
      : NaN;

  const dmi =
    dmiAdx(
      bars,
      STRATEGY.adxLen
    );

  const reasons = [];

  let score = 0;

  if (
    Number.isFinite(
      ema200
    )
  ) {
    const aligned =
      direction === 'BUY'
        ? (
          signal.close >
            ema50 &&
          ema50 >
            ema200
        )
        : (
          signal.close <
            ema50 &&
          ema50 <
            ema200
        );

    if (
      aligned
    ) {
      score++;

      reasons.push(
        direction === 'BUY'
          ? 'EMA-TREND-BULL'
          : 'EMA-TREND-BEAR'
      );
    }
  } else {
    reasons.push(
      'EMA200-WARMUP'
    );
  }

  const adxAligned =
    Number.isFinite(
      dmi.adx
    ) &&
    dmi.adx >=
      STRATEGY.adxThreshold &&
    (
      direction === 'BUY'
        ? dmi.plusDI >
          dmi.minusDI
        : dmi.minusDI >
          dmi.plusDI
    );

  if (
    adxAligned
  ) {
    score++;

    reasons.push(
      direction === 'BUY'
        ? 'ADX-BULL'
        : 'ADX-BEAR'
    );
  }

  const sweepWindow =
    prior.slice(-10);

  let liquiditySweep =
    false;

  if (
    sweepWindow.length >=
    5
  ) {
    if (
      direction ===
      'BUY'
    ) {
      const previousLow =
        lowestLow(
          sweepWindow
        );

      liquiditySweep =
        signal.low <
          previousLow &&
        signal.close >
          previousLow;
    } else {
      const previousHigh =
        highestHigh(
          sweepWindow
        );

      liquiditySweep =
        signal.high >
          previousHigh &&
        signal.close <
          previousHigh;
    }
  }

  if (
    liquiditySweep
  ) {
    score += 3;

    reasons.push(
      'LIQUIDITY-SWEEP'
    );
  }

  let bullFvg = false;
  let bearFvg = false;

  if (
    bars.length >=
    3
  ) {
    const first =
      bars[
        bars.length -
        3
      ];

    const third =
      bars[
        bars.length -
        1
      ];

    bullFvg =
      third.low >
      first.high;

    bearFvg =
      third.high <
      first.low;
  }

  if (
    direction ===
      'BUY' &&
    bullFvg
  ) {
    score++;

    reasons.push(
      'BULL-FVG'
    );
  }

  if (
    direction ===
      'SELL' &&
    bearFvg
  ) {
    score++;

    reasons.push(
      'BEAR-FVG'
    );
  }

  return {
    score,
    reasons,
    ema50,
    ema200,
    adx: dmi.adx,
    plusDI: dmi.plusDI,
    minusDI: dmi.minusDI,
    liquiditySweep,
    bullFvg,
    bearFvg
  };
}

function analyzeBars(
  symbol,
  bars,
  memory
) {
  if (
    !Array.isArray(bars) ||
    bars.length <
      CORE_MIN_HISTORY
  ) {
    return null;
  }

  const signal =
    bars[bars.length - 1];

  const prior =
    bars.slice(0, -1);

  const closes =
    bars.map(
      bar =>
        bar.close
    );

  const ema9 =
    emaLast(
      closes,
      STRATEGY.fastEmaLen
    );

  const ema21 =
    emaLast(
      closes,
      STRATEGY.slowEmaLen
    );

  const momentum =
    cmoLast(
      closes,
      STRATEGY.cmoLen
    );

  const atr =
    atrLast(
      bars,
      STRATEGY.atrLen
    );

  const volumeAverage =
    sma(
      prior.map(
        bar =>
          bar.volume
      ),
      STRATEGY.volLen
    );

  const volumeRatio =
    volumeAverage >
      0
      ? signal.volume /
        volumeAverage
      : NaN;

  const range =
    signal.high -
    signal.low;

  const bodyRatio =
    range >
      0
      ? Math.abs(
          signal.close -
          signal.open
        ) /
        range
      : 0;

  const strongBull =
    signal.close >
      signal.open &&
    bodyRatio >=
      STRATEGY.bodyRatioMin;

  const strongBear =
    signal.close <
      signal.open &&
    bodyRatio >=
      STRATEGY.bodyRatioMin;

  const highVolume =
    volumeAverage >
      0 &&
    signal.volume >
      volumeAverage *
      STRATEGY.volMult;

  const rawBuy =
    strongBull &&
    highVolume &&
    momentum >
      STRATEGY.cmoBuyThresh &&
    ema9 >
      ema21;

  const rawSell =
    strongBear &&
    highVolume &&
    momentum <
      STRATEGY.cmoSellThresh &&
    ema9 <
      ema21;

  let direction =
    null;

  if (
    rawBuy &&
    memory.lastSignal !==
      1
  ) {
    direction =
      'BUY';
  }

  if (
    rawSell &&
    memory.lastSignal !==
      -1
  ) {
    direction =
      'SELL';
  }

  if (
    direction ===
    'BUY'
  ) {
    memory.lastSignal =
      1;
  }

  if (
    direction ===
    'SELL'
  ) {
    memory.lastSignal =
      -1;
  }

  const common = {
    symbol,

    signalBarTime:
      signal.openTime,

    signalClose:
      signal.close,

    ema9,
    ema21,

    cmo:
      momentum,

    atr,
    bodyRatio,

    volume:
      signal.volume,

    volumeAvg:
      volumeAverage,

    volumeRatio,

    rawBuy,
    rawSell
  };

  if (
    !direction
  ) {
    return {
      ...common,
      direction: null
    };
  }

  const srWindow =
    prior.slice(
      -STRATEGY.srLen
    );

  const support =
    srWindow.length
      ? lowestLow(
          srWindow
        )
      : signal.low;

  const resistance =
    srWindow.length
      ? highestHigh(
          srWindow
        )
      : signal.high;

  const retest =
    retestContext(
      prior,
      signal,
      direction
    );

  const fib =
    fibContext(
      prior,
      signal,
      atr,
      direction
    );

  const smc =
    smcContext(
      bars,
      direction
    );

  if (
    retest.breakout
  ) {
    smc.reasons.push(
      'BREAKOUT'
    );
  }

  if (
    retest.retest
  ) {
    smc.reasons.push(
      'RETEST'
    );
  }

  if (
    fib.goldenZone
  ) {
    smc.reasons.push(
      'FIB-GOLDEN'
    );
  } else if (
    fib.nearFib
  ) {
    smc.reasons.push(
      'FIB'
    );
  }

  const qualityScore =
    clamp(
      retest.score +
        fib.score +
        smc.score,
      0,
      12
    );

  const quality =
    qualityScore >=
      7
      ? 'STRONG'
      : qualityScore >=
          4
        ? 'GOOD'
        : 'NEUTRAL';

  return {
    ...common,

    direction,
    support,
    resistance,

    retest,
    fib,
    smc,

    reasons:
      smc.reasons,

    qualityScore,
    quality
  };
}

function entryProtectionReason(
  analysis
) {
  if (
    !analysis?.direction
  ) {
    return '';
  }

  const reasons =
    Array.isArray(
      analysis.reasons
    )
      ? analysis.reasons
      : [];

  if (
    analysis.direction ===
    'BUY'
  ) {
    if (
      STRATEGY.rejectBullFvgBuys &&
      reasons.includes(
        'BULL-FVG'
      )
    ) {
      return 'BUY_BULL_FVG_FILTER';
    }

    if (
      Number.isFinite(
        analysis.cmo
      ) &&
      analysis.cmo >=
        STRATEGY.buyCmoMax
    ) {
      return 'BUY_CMO_OVEREXTENDED';
    }
  }

  if (
    analysis.direction ===
      'SELL' &&
    Number.isFinite(
      analysis.cmo
    ) &&
    analysis.cmo >
      STRATEGY.sellCmoMax
  ) {
    return 'SELL_CMO_WEAK';
  }

  return '';
}

const http =
  axios.create({
    timeout: 12000,

    headers: {
      'User-Agent':
        'LOMY-Forex-Paper/1.2.4'
    }
  });

async function fetchOhlc(
  symbol
) {
  const response =
    await http.get(
      `${BIQUOTE_BASE}/api/${encodeURIComponent(symbol)}/ohlc`,
      {
        params: {
          interval:
            TIMEFRAME,

          limit:
            HISTORY_LIMIT
        }
      }
    );

  const body =
    response.data;

  const raw =
    Array.isArray(body)
      ? body
      : Array.isArray(
          body?.bars
        )
        ? body.bars
        : Array.isArray(
            body?.data?.bars
          )
          ? body.data.bars
          : Array.isArray(
              body?.data
            )
            ? body.data
            : [];

  return closedBarsOnly(
    normalizeBars(
      raw
    )
  );
}

function normalizeQuote(
  raw,
  symbol
) {
  const bid =
    n(
      raw?.bid
    );

  const ask =
    n(
      raw?.ask
    );

  const mid =
    n(
      raw?.mid,
      Number.isFinite(bid) &&
      Number.isFinite(ask)
        ? (
          bid +
          ask
        ) /
        2
        : NaN
    );

  if (
    ![
      bid,
      ask,
      mid
    ].every(
      Number.isFinite
    ) ||
    bid <=
      0 ||
    ask <=
      0 ||
    ask <
      bid
  ) {
    return null;
  }

  return {
    symbol,
    bid,
    ask,
    mid,
    spread:
      ask -
      bid,

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
            allowStale:
              false
          }
        }
      );

    return normalizeQuote(
      response.data?.data ||
      response.data,
      symbol
    );
  } catch (
    error
  ) {
    state.lastMarketError =
      `${symbol} quote: ${safeError(error)}`;

    return null;
  }
}

async function fetchLatestQuotes(
  symbols
) {
  if (
    !symbols.length
  ) {
    return new Map();
  }

  try {
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

    if (
      Array.isArray(
        body
      )
    ) {
      for (
        const row
        of body
      ) {
        const symbol =
          String(
            row?.symbol ||
            ''
          ).toUpperCase();

        const quote =
          normalizeQuote(
            row,
            symbol
          );

        if (
          quote
        ) {
          output.set(
            symbol,
            quote
          );
        }
      }
    } else if (
      body &&
      typeof body ===
        'object'
    ) {
      for (
        const symbol
        of symbols
      ) {
        const quote =
          normalizeQuote(
            body[symbol] ||
            body[
              symbol.toLowerCase()
            ],
            symbol
          );

        if (
          quote
        ) {
          output.set(
            symbol,
            quote
          );
        }
      }
    }

    return output;
  } catch (
    error
  ) {
    state.lastMarketError =
      `latest quotes: ${safeError(error)}`;

    return new Map();
  }
}

async function mapWithConcurrency(
  items,
  limit,
  worker
) {
  const results =
    new Array(
      items.length
    );

  let next =
    0;

  async function run() {
    while (
      true
    ) {
      const index =
        next++;

      if (
        index >=
        items.length
      ) {
        break;
      }

      try {
        results[index] =
          await worker(
            items[index],
            index
          );
      } catch (
        error
      ) {
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

      reasons: [
        String
      ],

      volume:
        Number,

      volumeAvg:
        Number,

      volumeRatio:
        Number,

      cmo:
        Number,

      atr:
        Number,

      bodyRatio:
        Number,

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

      analysis:
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

      symbol:
        String,

      direction:
        String,

      signalPrice:
        Number,

      signalBarTime:
        String,

      cmo:
        Number,

      atr:
        Number,

      bodyRatio:
        Number,

      volume:
        Number,

      volumeAvg:
        Number,

      volumeRatio:
        Number,

      qualityScore:
        Number,

      quality:
        String,

      reasons: [
        String
      ],

      createdAt:
        Date,

      executed:
        Boolean,

      skipReason:
        String,

      analysis:
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

// NEW COLLECTION — FIXES OLD key_1 DUPLICATE INDEX
const Account =
  mongoose.models.LomyForexPaperAccountV124 ||
  mongoose.model(
    'LomyForexPaperAccountV124',
    accountSchema,
    'lomyforexpaperaccountsv124'
  );

const Trade =
  mongoose.models.LomyForexTradeV124 ||
  mongoose.model(
    'LomyForexTradeV124',
    tradeSchema,
    'lomyforextrades'
  );

const Signal =
  mongoose.models.LomyForexSignalV124 ||
  mongoose.model(
    'LomyForexSignalV124',
    signalSchema,
    'lomyforexsignals'
  );

const Journal =
  mongoose.models.LomyForexJournalV124 ||
  mongoose.model(
    'LomyForexJournalV124',
    journalSchema,
    JOURNAL_COLLECTION
  );

let account = null;
let bot = null;

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
  } catch (
    error
  ) {
    console.error(
      'Journal:',
      safeError(error)
    );
  }
}

async function initMongo() {
  if (
    !MONGODB_URI
  ) {
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

  state.mongoReady =
    true;

  console.log(
    '✅ MongoDB connected'
  );

  account =
    await Account.findOne({
      accountKey:
        PAPER.accountKey
    });

  if (
    !account
  ) {
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

  for (
    const trade
    of openTrades
  ) {
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
        'Mongo connected and account restored',

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
  if (
    !account
  ) {
    return;
  }

  account.updatedAt =
    new Date();

  await account.save();
}

async function sendTelegram(
  text
) {
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
  } catch (
    error
  ) {
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

function currentPortfolioRiskUsd() {
  let total =
    0;

  for (
    const trade
    of state.openTrades.values()
  ) {
    if (
      trade.breakEvenActive
    ) {
      continue;
    }

    const entry =
      n(
        trade.entryPrice
      );

    const stop =
      n(
        trade.stopLoss
      );

    const distance =
      n(
        trade.riskDistance
      );

    const riskAmount =
      n(
        trade.riskAmount,
        0
      );

    if (
      !Number.isFinite(entry) ||
      !Number.isFinite(stop) ||
      !(distance > 0) ||
      !(riskAmount > 0)
    ) {
      continue;
    }

    const remainingFraction =
      Math.min(
        1,
        Math.max(
          0,
          Math.abs(
            entry -
            stop
          ) /
          distance
        )
      );

    total +=
      riskAmount *
      remainingFraction;
  }

  return total;
}

function portfolioRiskCapUsd() {
  return (
    n(
      account?.balance,
      PAPER.startingBalance
    ) *
    PAPER.portfolioRiskCapPct /
    100
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

  bot.start(
    async ctx => {
      account.telegramChatId =
        String(
          ctx.chat.id
        );

      await saveAccount();

      await ctx.reply(
        `✅ ${VERSION}\n` +
        `PAPER ONLY\n` +
        `Balance: ${fmtMoney(account.balance)}`
      );
    }
  );

  bot.command(
    'status',
    async ctx => {
      await ctx.reply(
        `🤖 ${VERSION}\n` +
        `Mode: PAPER\n` +
        `Market: ${state.marketReady ? 'READY' : 'WAIT'}\n` +
        `Pairs: ${pairReadyCount()}/${INSTRUMENTS.length}\n` +
        `Open: ${state.openTrades.size}\n` +
        `Signals: ${state.rawSignals}\n` +
        `Executed: ${state.executedSignals}\n` +
        `Skipped: ${state.skippedSignals}\n` +
        `Protection rejects: ${state.protectionRejects}\n` +
        `Portfolio rejects: ${state.portfolioRiskRejects}\n` +
        `BE moves: ${state.breakEvenMoves}\n` +
        `Journal: ${state.journalEvents}\n` +
        `Risk: ${PAPER.riskPctPerTrade}% | Portfolio cap: ${PAPER.portfolioRiskCapPct}%`
      );
    }
  );

  bot.command(
    'balance',
    async ctx => {
      await ctx.reply(
        `💰 PAPER BALANCE\n` +
        `Balance: ${fmtMoney(account?.balance)}\n` +
        `Realized PnL: ${fmtMoney(account?.realizedPnl)}\n` +
        `Open risk: ${fmtMoney(currentPortfolioRiskUsd())}/${fmtMoney(portfolioRiskCapUsd())}`
      );
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
        `Realized: ${fmtMoney(account?.realizedPnl || 0)}`
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
    'positions',
    async ctx => {
      const positions =
        [
          ...state.openTrades.values()
        ];

      if (
        !positions.length
      ) {
        await ctx.reply(
          '📭 No open PAPER trades'
        );

        return;
      }

      const message =
        positions
          .map(
            trade =>
              `${trade.symbol} ${trade.direction}\n` +
              `Entry ${fmtPrice(trade.entryPrice, trade.symbol)} | ` +
              `SL ${fmtPrice(trade.stopLoss, trade.symbol)} | ` +
              `TP ${fmtPrice(trade.takeProfit, trade.symbol)}\n` +
              `BE ${trade.breakEvenActive ? 'ON' : 'OFF'} | ` +
              `MFE ${n(trade.mfeR, 0).toFixed(2)}R | ` +
              `MAE ${n(trade.maeR, 0).toFixed(2)}R`
          )
          .join('\n\n');

      await ctx.reply(
        message
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
            closedAt:
              -1
          })
          .limit(10)
          .lean();

      if (
        !rows.length
      ) {
        await ctx.reply(
          '📭 No closed trades'
        );

        return;
      }

      await ctx.reply(
        rows
          .map(
            trade =>
              `${trade.symbol} ${trade.direction} ` +
              `${trade.exitReason} ` +
              `${n(trade.resultR, 0).toFixed(2)}R | ` +
              `MFE ${n(trade.mfeR, 0).toFixed(2)}R | ` +
              `MAE ${n(trade.maeR, 0).toFixed(2)}R`
          )
          .join('\n')
      );
    }
  );

  await bot.telegram.getMe();

  state.telegramReady =
    true;

  console.log(
    '✅ Telegram authenticated'
  );

  bot.launch({
    dropPendingUpdates:
      true
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

async function recordSignal(
  analysis,
  executed,
  skipReason = ''
) {
  const payload = {
    version:
      VERSION,

    accountKey:
      PAPER.accountKey,

    symbol:
      analysis.symbol,

    direction:
      analysis.direction,

    signalPrice:
      analysis.signalClose,

    signalBarTime:
      String(
        analysis.signalBarTime ||
        ''
      ),

    cmo:
      analysis.cmo,

    atr:
      analysis.atr,

    bodyRatio:
      analysis.bodyRatio,

    volume:
      analysis.volume,

    volumeAvg:
      analysis.volumeAvg,

    volumeRatio:
      analysis.volumeRatio,

    qualityScore:
      analysis.qualityScore,

    quality:
      analysis.quality,

    reasons:
      analysis.reasons ||
      [],

    createdAt:
      new Date(),

    executed,

    skipReason,

    analysis
  };

  try {
    await Signal.create(
      payload
    );
  } catch (
    error
  ) {
    console.error(
      'Signal journal:',
      safeError(error)
    );
  }

  await journal(
    executed
      ? 'SIGNAL_EXECUTED'
      : 'SIGNAL_REJECTED',
    {
      symbol:
        analysis.symbol,

      direction:
        analysis.direction,

      message:
        skipReason ||
        'EXECUTED',

      data: {
        signalPrice:
          analysis.signalClose,

        cmo:
          analysis.cmo,

        atr:
          analysis.atr,

        bodyRatio:
          analysis.bodyRatio,

        volume:
          analysis.volume,

        volumeAvg:
          analysis.volumeAvg,

        volumeRatio:
          analysis.volumeRatio,

        qualityScore:
          analysis.qualityScore,

        quality:
          analysis.quality,

        reasons:
          analysis.reasons ||
          [],

        skipReason
      }
    }
  );
}

function buildExecutionLevels(
  analysis,
  quote
) {
  const direction =
    analysis.direction;

  const atr =
    analysis.atr;

  if (
    !Number.isFinite(atr) ||
    atr <= 0
  ) {
    return null;
  }

  const entry =
    direction === 'BUY'
      ? quote.ask
      : quote.bid;

  let stopLoss;

  if (
    direction === 'BUY'
  ) {
    stopLoss =
      n(
        analysis.support
      ) -
      atr *
      STRATEGY.atrMargin;

    if (
      !(stopLoss < entry)
    ) {
      stopLoss =
        analysis.signalClose -
        atr;
    }

    if (
      !(stopLoss < entry)
    ) {
      stopLoss =
        entry -
        atr;
    }
  } else {
    stopLoss =
      n(
        analysis.resistance
      ) +
      atr *
      STRATEGY.atrMargin;

    if (
      !(stopLoss > entry)
    ) {
      stopLoss =
        analysis.signalClose +
        atr;
    }

    if (
      !(stopLoss > entry)
    ) {
      stopLoss =
        entry +
        atr;
    }
  }

  const riskDistance =
    Math.abs(
      entry -
      stopLoss
    );

  if (
    !(riskDistance > 0)
  ) {
    return null;
  }

  const takeProfit =
    direction === 'BUY'
      ? entry +
        riskDistance *
        STRATEGY.riskReward
      : entry -
        riskDistance *
        STRATEGY.riskReward;

  const breakEvenTriggerPrice =
    direction === 'BUY'
      ? entry +
        riskDistance *
        STRATEGY.breakEvenTriggerR
      : entry -
        riskDistance *
        STRATEGY.breakEvenTriggerR;

  const valid =
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

  if (
    !valid
  ) {
    return null;
  }

  return {
    entry,
    stopLoss,
    takeProfit,
    riskDistance,
    breakEvenTriggerPrice
  };
}

async function openPaperTrade(
  analysis,
  quote
) {
  const symbol =
    analysis.symbol;

  const direction =
    analysis.direction;

  if (
    state.openTrades.has(
      symbol
    )
  ) {
    state.skippedSignals++;

    await recordSignal(
      analysis,
      false,
      'POSITION_ALREADY_OPEN'
    );

    return false;
  }

  if (
    state.openTrades.size >=
    PAPER.maxOpenTrades
  ) {
    state.skippedSignals++;

    await recordSignal(
      analysis,
      false,
      'MAX_OPEN_TRADES'
    );

    return false;
  }

  const executionPrice =
    direction === 'BUY'
      ? quote.ask
      : quote.bid;

  const moveFromSignal =
    Math.abs(
      executionPrice -
      analysis.signalClose
    );

  if (
    Number.isFinite(
      analysis.atr
    ) &&
    analysis.atr >
      0 &&
    moveFromSignal >
      analysis.atr *
      STRATEGY.maxEntryMoveAtr
  ) {
    state.skippedSignals++;

    await recordSignal(
      analysis,
      false,
      'ENTRY_TOO_FAR_FROM_SIGNAL'
    );

    return false;
  }

  const levels =
    buildExecutionLevels(
      analysis,
      quote
    );

  if (
    !levels
  ) {
    state.skippedSignals++;

    await recordSignal(
      analysis,
      false,
      'INVALID_LEVELS'
    );

    return false;
  }

  const balance =
    n(
      account?.balance,
      PAPER.startingBalance
    );

  const riskAmount =
    balance *
    PAPER.riskPctPerTrade /
    100;

  const currentRisk =
    currentPortfolioRiskUsd();

  const riskCap =
    portfolioRiskCapUsd();

  if (
    currentRisk +
    riskAmount >
    riskCap +
    0.000000001
  ) {
    state.skippedSignals++;
    state.portfolioRiskRejects++;

    await recordSignal(
      analysis,
      false,
      'PORTFOLIO_RISK_CAP'
    );

    await journal(
      'PORTFOLIO_RISK_REJECT',
      {
        symbol,
        direction,

        message:
          'Portfolio risk cap blocked entry',

        data: {
          balance,
          currentRisk,

          requestedRisk:
            riskAmount,

          riskCap
        }
      }
    );

    return false;
  }

  const quantity =
    riskAmount /
    levels.riskDistance;

  const now =
    new Date();

  const tradeDocument =
    await Trade.create({
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
        levels.entry,

      stopLoss:
        levels.stopLoss,

      initialStopLoss:
        levels.stopLoss,

      takeProfit:
        levels.takeProfit,

      breakEvenTriggerPrice:
        levels.breakEvenTriggerPrice,

      breakEvenActive:
        false,

      riskDistance:
        levels.riskDistance,

      riskAmount,

      quantity,

      signalPrice:
        analysis.signalClose,

      signalBarTime:
        String(
          analysis.signalBarTime ||
          ''
        ),

      qualityScore:
        analysis.qualityScore,

      quality:
        analysis.quality,

      reasons:
        analysis.reasons ||
        [],

      volume:
        analysis.volume,

      volumeAvg:
        analysis.volumeAvg,

      volumeRatio:
        analysis.volumeRatio,

      cmo:
        analysis.cmo,

      atr:
        analysis.atr,

      bodyRatio:
        analysis.bodyRatio,

      openedAt:
        now,

      mfeR:
        0,

      maeR:
        0,

      mfePrice:
        levels.entry,

      maePrice:
        levels.entry,

      mfeAt:
        now,

      maeAt:
        now,

      beActivatedAt:
        null,

      lastMarkPrice:
        levels.entry,

      lastMarkAt:
        now,

      analysis
    });

  const trade =
    tradeDocument.toObject();

  state.openTrades.set(
    symbol,
    trade
  );

  state.executedSignals++;

  await recordSignal(
    analysis,
    true,
    ''
  );

  await journal(
    'TRADE_OPEN',
    {
      symbol,
      direction,

      tradeId:
        trade._id,

      message:
        'Paper trade opened',

      data: {
        entry:
          levels.entry,

        stopLoss:
          levels.stopLoss,

        takeProfit:
          levels.takeProfit,

        breakEvenTriggerPrice:
          levels.breakEvenTriggerPrice,

        riskDistance:
          levels.riskDistance,

        riskAmount,

        quantity,

        portfolioRiskBefore:
          currentRisk,

        portfolioRiskAfter:
          currentRisk +
          riskAmount,

        portfolioRiskCap:
          riskCap,

        cmo:
          analysis.cmo,

        volume:
          analysis.volume,

        volumeAvg:
          analysis.volumeAvg,

        volumeRatio:
          analysis.volumeRatio,

        bodyRatio:
          analysis.bodyRatio,

        quality:
          analysis.quality,

        qualityScore:
          analysis.qualityScore,

        reasons:
          analysis.reasons ||
          []
      }
    }
  );

  await sendTelegram(
    `${direction === 'BUY' ? '🟢' : '🔴'} ${symbol} ${direction} — PAPER\n` +
    `Entry: ${fmtPrice(levels.entry, symbol)}\n` +
    `SL: ${fmtPrice(levels.stopLoss, symbol)}\n` +
    `TP: ${fmtPrice(levels.takeProfit, symbol)} (${STRATEGY.riskReward.toFixed(1)}R)\n` +
    `BE: ${fmtPrice(levels.breakEvenTriggerPrice, symbol)} (+${STRATEGY.breakEvenTriggerR.toFixed(1)}R)\n` +
    `Risk: ${fmtMoney(riskAmount)}\n` +
    `Quality: ${analysis.quality} ${analysis.qualityScore}/12\n` +
    `CMO: ${n(analysis.cmo, 0).toFixed(1)}\n` +
    `Volume: ${
      Number.isFinite(
        analysis.volumeRatio
      )
        ? analysis.volumeRatio.toFixed(2) + 'x'
        : 'n/a'
    }\n` +
    `Reasons: ${
      (
        analysis.reasons ||
        []
      ).join(', ') ||
      'none'
    }\n\n` +
    `🔒 PAPER ONLY`
  );

  return true;
}

function markPriceForTrade(
  trade,
  quote
) {
  return trade.direction ===
    'BUY'
    ? quote.bid
    : quote.ask;
}

function currentR(
  trade,
  quote
) {
  const price =
    markPriceForTrade(
      trade,
      quote
    );

  const move =
    trade.direction ===
    'BUY'
      ? price -
        trade.entryPrice
      : trade.entryPrice -
        price;

  return move /
    trade.riskDistance;
}

async function updateExcursions(
  symbol,
  trade,
  quote
) {
  const price =
    markPriceForTrade(
      trade,
      quote
    );

  const r =
    currentR(
      trade,
      quote
    );

  const now =
    new Date();

  let changed =
    false;

  trade.lastMarkPrice =
    price;

  trade.lastMarkAt =
    now;

  if (
    !Number.isFinite(
      trade.mfeR
    ) ||
    r >
      trade.mfeR
  ) {
    trade.mfeR =
      r;

    trade.mfePrice =
      price;

    trade.mfeAt =
      now;

    changed =
      true;
  }

  if (
    !Number.isFinite(
      trade.maeR
    ) ||
    r <
      trade.maeR
  ) {
    trade.maeR =
      r;

    trade.maePrice =
      price;

    trade.maeAt =
      now;

    changed =
      true;
  }

  state.openTrades.set(
    symbol,
    trade
  );

  if (
    changed
  ) {
    await Trade.updateOne(
      {
        _id:
          trade._id,

        status:
          'OPEN'
      },
      {
        $set: {
          mfeR:
            trade.mfeR,

          maeR:
            trade.maeR,

          mfePrice:
            trade.mfePrice,

          maePrice:
            trade.maePrice,

          mfeAt:
            trade.mfeAt,

          maeAt:
            trade.maeAt,

          lastMarkPrice:
            price,

          lastMarkAt:
            now
        }
      }
    );
  }
}

async function activateBreakEven(
  symbol,
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

  trade.beActivatedAt =
    new Date();

  state.breakEvenMoves++;

  state.openTrades.set(
    symbol,
    trade
  );

  await Trade.updateOne(
    {
      _id:
        trade._id,

      status:
        'OPEN'
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

  await journal(
    'BREAK_EVEN_ACTIVATED',
    {
      symbol,

      direction:
        trade.direction,

      tradeId:
        trade._id,

      message:
        'Stop moved to exact entry',

      data: {
        entry:
          trade.entryPrice,

        mfeR:
          n(
            trade.mfeR,
            0
          ),

        maeR:
          n(
            trade.maeR,
            0
          )
      }
    }
  );

  await sendTelegram(
    `🛡️ BREAK-EVEN ACTIVATED\n` +
    `${symbol} ${trade.direction}\n` +
    `SL moved to Entry: ${fmtPrice(trade.entryPrice, symbol)}`
  );
}

async function closePaperTrade(
  symbol,
  trade,
  exitPrice,
  reason
) {
  if (
    !state.openTrades.has(
      symbol
    ) ||
    state.closingTrades.has(
      symbol
    )
  ) {
    return false;
  }

  state.closingTrades.add(
    symbol
  );

  try {
    const liveTrade =
      state.openTrades.get(
        symbol
      );

    if (
      !liveTrade ||
      String(
        liveTrade._id
      ) !==
      String(
        trade._id
      )
    ) {
      return false;
    }

    trade =
      liveTrade;

    const move =
      trade.direction ===
      'BUY'
        ? exitPrice -
          trade.entryPrice
        : trade.entryPrice -
          exitPrice;

    const resultR =
      move /
      trade.riskDistance;

    const pnl =
      trade.riskAmount *
      resultR;

    const closedAt =
      new Date();

    const update =
      await Trade.updateOne(
        {
          _id:
            trade._id,

          status:
            'OPEN'
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

            stopLoss:
              trade.stopLoss,

            breakEvenActive:
              trade.breakEvenActive,

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

            mfePrice:
              trade.mfePrice,

            maePrice:
              trade.maePrice,

            mfeAt:
              trade.mfeAt,

            maeAt:
              trade.maeAt,

            lastMarkPrice:
              exitPrice,

            lastMarkAt:
              closedAt
          }
        }
      );

    if (
      !update.modifiedCount
    ) {
      return false;
    }

    account.balance =
      n(
        account.balance,
        PAPER.startingBalance
      ) +
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
      ) +
      1;

    if (
      reason ===
        'BREAK_EVEN' ||
      Math.abs(
        resultR
      ) <
        0.10
    ) {
      account.breakeven =
        n(
          account.breakeven,
          0
        ) +
        1;
    } else if (
      pnl >
      0
    ) {
      account.wins =
        n(
          account.wins,
          0
        ) +
        1;
    } else {
      account.losses =
        n(
          account.losses,
          0
        ) +
        1;
    }

    await saveAccount();

    state.openTrades.delete(
      symbol
    );

    const durationMinutes =
      (
        closedAt.getTime() -
        new Date(
          trade.openedAt
        ).getTime()
      ) /
      60000;

    await journal(
      'TRADE_CLOSE',
      {
        symbol,

        direction:
          trade.direction,

        tradeId:
          trade._id,

        message:
          reason,

        data: {
          entryPrice:
            trade.entryPrice,

          exitPrice,

          resultR,

          pnl,

          balance:
            account.balance,

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

          mfePrice:
            trade.mfePrice,

          maePrice:
            trade.maePrice,

          breakEvenActive:
            trade.breakEvenActive,

          durationMinutes
        }
      }
    );

    await sendTelegram(
      `${pnl >= 0 ? '✅' : '❌'} CLOSED ${symbol} ${trade.direction}\n` +
      `Reason: ${reason}\n` +
      `Exit: ${fmtPrice(exitPrice, symbol)}\n` +
      `Result: ${resultR.toFixed(2)}R\n` +
      `PnL: ${fmtMoney(pnl)}\n` +
      `MFE: ${n(trade.mfeR, 0).toFixed(2)}R\n` +
      `MAE: ${n(trade.maeR, 0).toFixed(2)}R\n` +
      `Balance: ${fmtMoney(account.balance)}`
    );

    return true;
  } finally {
    state.closingTrades.delete(
      symbol
    );
  }
}

async function handleReverseSignal(
  analysis,
  quote
) {
  const trade =
    state.openTrades.get(
      analysis.symbol
    );

  if (
    !trade ||
    trade.direction ===
      analysis.direction
  ) {
    return;
  }

  const r =
    currentR(
      trade,
      quote
    );

  if (
    r <
      0 &&
    !trade.breakEvenActive
  ) {
    return;
  }

  const exitPrice =
    markPriceForTrade(
      trade,
      quote
    );

  await closePaperTrade(
    analysis.symbol,
    trade,
    exitPrice,
    'REVERSE_SIGNAL'
  );
}

async function manageOpenTrades() {
  const symbols =
    [
      ...state.openTrades.keys()
    ];

  if (
    !symbols.length ||
    state.quoteRunning
  ) {
    return;
  }

  state.quoteRunning =
    true;

  try {
    state.totalQuotePolls++;

    state.lastQuotePollAt =
      new Date();

    const quotes =
      await fetchLatestQuotes(
        symbols
      );

    for (
      const symbol
      of symbols
    ) {
      let trade =
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

      await updateExcursions(
        symbol,
        trade,
        quote
      );

      trade =
        state.openTrades.get(
          symbol
        ) ||
        trade;

      const exitPrice =
        markPriceForTrade(
          trade,
          quote
        );

      if (
        !trade.breakEvenActive
      ) {
        const hitBreakEven =
          trade.direction ===
          'BUY'
            ? exitPrice >=
              trade.breakEvenTriggerPrice
            : exitPrice <=
              trade.breakEvenTriggerPrice;

        if (
          hitBreakEven
        ) {
          await activateBreakEven(
            symbol,
            trade
          );

          trade =
            state.openTrades.get(
              symbol
            ) ||
            trade;
        }
      }

      const stopHit =
        trade.direction ===
        'BUY'
          ? exitPrice <=
            trade.stopLoss
          : exitPrice >=
            trade.stopLoss;

      const targetHit =
        trade.direction ===
        'BUY'
          ? exitPrice >=
            trade.takeProfit
          : exitPrice <=
            trade.takeProfit;

      if (
        stopHit
      ) {
        await closePaperTrade(
          symbol,
          trade,
          exitPrice,
          trade.breakEvenActive
            ? 'BREAK_EVEN'
            : 'STOP_LOSS'
        );
      } else if (
        targetHit
      ) {
        await closePaperTrade(
          symbol,
          trade,
          exitPrice,
          'TAKE_PROFIT'
        );
      }
    }
  } catch (
    error
  ) {
    state.lastMarketError =
      `quote loop: ${safeError(error)}`;

    console.error(
      'Quote loop:',
      state.lastMarketError
    );

    await journal(
      'ERROR',
      {
        message:
          'manageOpenTrades failed',

        data: {
          error:
            safeError(error)
        }
      }
    );
  } finally {
    state.quoteRunning =
      false;
  }
}

async function scanSymbol(
  symbol
) {
  const memory =
    state.pairState.get(
      symbol
    );

  try {
    const bars =
      await fetchOhlc(
        symbol
      );

    memory.bars =
      bars;

    memory.initialized =
      bars.length >=
      CORE_MIN_HISTORY;

    if (
      !memory.initialized
    ) {
      return {
        symbol,
        status:
          'WARMUP'
      };
    }

    const lastTime =
      bars[
        bars.length -
        1
      ]?.openTime;

    if (
      !lastTime ||
      lastTime ===
        memory.lastClosedBarTime
    ) {
      return {
        symbol,
        status:
          'NO_NEW_BAR'
      };
    }

    memory.lastClosedBarTime =
      lastTime;

    const analysis =
      analyzeBars(
        symbol,
        bars,
        memory
      );

    memory.lastAnalysis =
      analysis;

    if (
      !analysis?.direction
    ) {
      return {
        symbol,
        status:
          'NO_SIGNAL'
      };
    }

    state.rawSignals++;

    const protectionReason =
      entryProtectionReason(
        analysis
      );

    if (
      protectionReason
    ) {
      state.skippedSignals++;
      state.protectionRejects++;

      await recordSignal(
        analysis,
        false,
        protectionReason
      );

      console.log(
        `🛡️ FILTER ${symbol} ${analysis.direction} | ` +
        `${protectionReason} | ` +
        `CMO=${n(analysis.cmo, 0).toFixed(1)} | ` +
        `Q=${analysis.qualityScore}`
      );

      return {
        symbol,
        status:
          'PROTECTED',

        reason:
          protectionReason
      };
    }

    const quote =
      await fetchSingleQuote(
        symbol
      );

    if (
      !quote
    ) {
      state.skippedSignals++;

      await recordSignal(
        analysis,
        false,
        'NO_FRESH_QUOTE'
      );

      return {
        symbol,
        status:
          'NO_QUOTE'
      };
    }

    state.latestQuotes.set(
      symbol,
      quote
    );

    await handleReverseSignal(
      analysis,
      quote
    );

    if (
      state.openTrades.has(
        symbol
      )
    ) {
      state.skippedSignals++;

      await recordSignal(
        analysis,
        false,
        'POSITION_ALREADY_OPEN'
      );

      return {
        symbol,
        status:
          'SKIP_OPEN'
      };
    }

    console.log(
      `🎯 ${symbol} ${analysis.direction} V5.1 | ` +
      `CMO=${n(analysis.cmo, 0).toFixed(1)} | ` +
      `body=${(analysis.bodyRatio * 100).toFixed(0)}% | ` +
      `vol=${
        Number.isFinite(
          analysis.volumeRatio
        )
          ? analysis.volumeRatio.toFixed(2)
          : 'n/a'
      }x | ` +
      `Q=${analysis.qualityScore}`
    );

    const opened =
      await openPaperTrade(
        analysis,
        quote
      );

    return {
      symbol,

      status:
        opened
          ? 'SIGNAL'
          : 'SKIPPED',

      direction:
        analysis.direction
    };
  } catch (
    error
  ) {
    memory.errors++;

    state.lastMarketError =
      `${symbol}: ${safeError(error)}`;

    await journal(
      'ERROR',
      {
        symbol,

        message:
          'scanSymbol failed',

        data: {
          error:
            safeError(error)
        }
      }
    );

    return {
      symbol,

      status:
        'ERROR',

      error:
        safeError(error)
    };
  }
}

async function signalScanLoop() {
  if (
    state.scanRunning ||
    state.initializing ||
    !state.marketReady
  ) {
    return;
  }

  const now =
    new Date();

  const minute =
    now.getUTCMinutes();

  const second =
    now.getUTCSeconds();

  if (
    minute %
      15 !==
      0 ||
    second <
      4
  ) {
    return;
  }

  const slot =
    `${now.getUTCFullYear()}-` +
    `${now.getUTCMonth() + 1}-` +
    `${now.getUTCDate()}-` +
    `${now.getUTCHours()}-` +
    `${Math.floor(minute / 15)}`;

  if (
    state.lastScanSlot ===
    slot
  ) {
    return;
  }

  state.lastScanSlot =
    slot;

  state.scanRunning =
    true;

  state.totalSignalScans++;

  state.lastSignalScanAt =
    new Date();

  const scanNumber =
    state.totalSignalScans;

  const beforeRaw =
    state.rawSignals;

  const beforeExecuted =
    state.executedSignals;

  const beforeSkipped =
    state.skippedSignals;

  try {
    console.log(
      `🔎 Scan #${scanNumber} | ${new Date().toISOString()}`
    );

    await mapWithConcurrency(
      INSTRUMENTS,
      OHLC_CONCURRENCY,
      scanSymbol
    );

    console.log(
      `✅ Scan #${scanNumber} complete | ` +
      `raw=${state.rawSignals - beforeRaw} ` +
      `executed=${state.executedSignals - beforeExecuted} ` +
      `skipped=${state.skippedSignals - beforeSkipped}`
    );
  } catch (
    error
  ) {
    await journal(
      'ERROR',
      {
        message:
          'signalScanLoop failed',

        data: {
          error:
            safeError(error)
        }
      }
    );
  } finally {
    state.scanRunning =
      false;
  }
}

async function initializeMarket() {
  state.initializing =
    true;

  state.marketReady =
    false;

  console.log(
    `⏳ Initializing ${INSTRUMENTS.length} instruments on ${TIMEFRAME}...`
  );

  const timeoutMs =
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
                          `INIT_TIMEOUT_${timeoutMs}MS`
                        )
                      ),
                    timeoutMs
                  )
              )
            ]);

          memory.bars =
            bars;

          memory.initialized =
            bars.length >=
            CORE_MIN_HISTORY;

          memory.lastClosedBarTime =
            bars[
              bars.length -
              1
            ]?.openTime ||
            null;

          if (
            memory.initialized
          ) {
            memory.errors =
              0;

            console.log(
              `✅ INIT ${symbol}: READY | ` +
              `bars=${bars.length} | ` +
              `EMA200=${
                bars.length >=
                EMA200_CONTEXT_HISTORY
                  ? 'READY'
                  : 'CONTEXT-WARMUP'
              }`
            );

            return {
              symbol,
              ready:
                true,

              bars:
                bars.length
            };
          }

          memory.errors++;

          console.warn(
            `⚠️ INIT ${symbol}: WARMUP | bars=${bars.length}/${CORE_MIN_HISTORY}`
          );

          return {
            symbol,
            ready:
              false,

            bars:
              bars.length
          };
        } catch (
          error
        ) {
          memory.initialized =
            false;

          memory.errors++;

          console.error(
            `❌ INIT ${symbol}: ${safeError(error)}`
          );

          return {
            symbol,
            ready:
              false,

            bars:
              0
          };
        }
      }
    );

  const ready =
    results.filter(
      result =>
        result?.ready
    ).length;

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

  console.log(
    state.marketReady
      ? `✅ Market engine READY | minimum=${minimumReady}`
      : `⚠️ Market engine PARTIAL | ready=${ready} minimum=${minimumReady}`
  );

  await journal(
    'MARKET_INIT',
    {
      message:
        state.marketReady
          ? 'Market ready'
          : 'Market partial',

      data: {
        ready,

        total:
          INSTRUMENTS.length,

        minimumReady
      }
    }
  );
}

// ============================================================
// EXPRESS + DASHBOARD
// ============================================================

const app =
  express();

app.disable(
  'x-powered-by'
);

app.use(
  express.json({
    limit:
      '100kb'
  })
);

function openTradeView(
  trade
) {
  const quote =
    state.latestQuotes.get(
      trade.symbol
    );

  let currentPrice =
    n(
      trade.lastMarkPrice,
      trade.entryPrice
    );

  let r =
    0;

  if (
    quote
  ) {
    currentPrice =
      markPriceForTrade(
        trade,
        quote
      );
  }

  if (
    Number.isFinite(
      currentPrice
    ) &&
    n(
      trade.riskDistance,
      0
    ) >
      0
  ) {
    const move =
      trade.direction ===
      'BUY'
        ? currentPrice -
          trade.entryPrice
        : trade.entryPrice -
          currentPrice;

    r =
      move /
      trade.riskDistance;
  }

  const floatingPnl =
    n(
      trade.riskAmount,
      0
    ) *
    r;

  return {
    id:
      String(
        trade._id
      ),

    symbol:
      trade.symbol,

    direction:
      trade.direction,

    entryPrice:
      trade.entryPrice,

    currentPrice,

    stopLoss:
      trade.stopLoss,

    takeProfit:
      trade.takeProfit,

    breakEvenTriggerPrice:
      trade.breakEvenTriggerPrice,

    breakEvenActive:
      Boolean(
        trade.breakEvenActive
      ),

    riskAmount:
      n(
        trade.riskAmount,
        0
      ),

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

    cmo:
      n(
        trade.cmo,
        NaN
      ),

    volumeRatio:
      n(
        trade.volumeRatio,
        NaN
      ),

    bodyRatio:
      n(
        trade.bodyRatio,
        NaN
      ),

    quality:
      trade.quality ||
      'NEUTRAL',

    qualityScore:
      n(
        trade.qualityScore,
        0
      ),

    reasons:
      trade.reasons ||
      [],

    openedAt:
      trade.openedAt,

    durationMinutes:
      Math.max(
        0,
        (
          Date.now() -
          new Date(
            trade.openedAt
          ).getTime()
        ) /
        60000
      ),

    lastMarkAt:
      trade.lastMarkAt ||
      null
  };
}

async function dashboardData() {
  const openTrades =
    [
      ...state.openTrades.values()
    ].map(
      openTradeView
    );

  const unrealizedPnl =
    openTrades.reduce(
      (
        total,
        trade
      ) =>
        total +
        trade.floatingPnl,
      0
    );

  const balance =
    n(
      account?.balance,
      PAPER.startingBalance
    );

  const equity =
    balance +
    unrealizedPnl;

  const recentClosed =
    await Trade.find({
      accountKey:
        PAPER.accountKey,

      status:
        'CLOSED'
    })
      .sort({
        closedAt:
          -1
      })
      .limit(
        20
      )
      .lean();

  const allClosed =
    await Trade.find(
      {
        accountKey:
          PAPER.accountKey,

        status:
          'CLOSED'
      },
      {
        direction:
          1,

        pnl:
          1,

        resultR:
          1
      }
    ).lean();

  const directionStats = {
    BUY: {
      trades:
        0,

      pnl:
        0,

      r:
        0
    },

    SELL: {
      trades:
        0,

      pnl:
        0,

      r:
        0
    }
  };

  for (
    const trade
    of allClosed
  ) {
    const item =
      directionStats[
        trade.direction
      ];

    if (
      !item
    ) {
      continue;
    }

    item.trades++;

    item.pnl +=
      n(
        trade.pnl,
        0
      );

    item.r +=
      n(
        trade.resultR,
        0
      );
  }

  return {
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

    timeframe:
      TIMEFRAME,

    pairsReady:
      pairReadyCount(),

    pairsTotal:
      INSTRUMENTS.length,

    startedAt:
      state.startedAt,

    lastSignalScanAt:
      state.lastSignalScanAt,

    lastQuotePollAt:
      state.lastQuotePollAt,

    lastMarketError:
      state.lastMarketError,

    account: {
      startingBalance:
        PAPER.startingBalance,

      balance,

      equity,

      realizedPnl:
        n(
          account?.realizedPnl,
          0
        ),

      unrealizedPnl,

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

      riskPctPerTrade:
        PAPER.riskPctPerTrade,

      portfolioRiskCapPct:
        PAPER.portfolioRiskCapPct,

      openRiskUsd:
        currentPortfolioRiskUsd(),

      riskCapUsd:
        portfolioRiskCapUsd()
    },

    engine: {
      openTrades:
        openTrades.length,

      rawSignals:
        state.rawSignals,

      executedSignals:
        state.executedSignals,

      skippedSignals:
        state.skippedSignals,

      protectionRejects:
        state.protectionRejects,

      portfolioRiskRejects:
        state.portfolioRiskRejects,

      breakEvenMoves:
        state.breakEvenMoves,

      journalEvents:
        state.journalEvents
    },

    directionStats,

    openTrades,

    recentClosed:
      recentClosed.map(
        trade => ({
          id:
            String(
              trade._id
            ),

          symbol:
            trade.symbol,

          direction:
            trade.direction,

          exitReason:
            trade.exitReason,

          entryPrice:
            trade.entryPrice,

          exitPrice:
            trade.exitPrice,

          pnl:
            n(
              trade.pnl,
              0
            ),

          resultR:
            n(
              trade.resultR,
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

          quality:
            trade.quality,

          qualityScore:
            n(
              trade.qualityScore,
              0
            ),

          closedAt:
            trade.closedAt
        })
      )
  };
}

app.get(
  '/',
  (
    req,
    res
  ) => {
    res
      .type(
        'html'
      )
      .send(
`<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>${VERSION}</title>

<style>
* {
  box-sizing: border-box;
}

body {
  margin: 0;
  background: #081018;
  color: #e8f0f7;
  font-family: Arial, sans-serif;
}

.wrap {
  max-width: 1500px;
  margin: auto;
  padding: 18px;
}

.top {
  display: flex;
  justify-content: space-between;
  gap: 12px;
  align-items: center;
  flex-wrap: wrap;
}

.badge {
  padding: 7px 11px;
  border: 1px solid #27445b;
  border-radius: 20px;
  background: #0d1a25;
}

.ok {
  color: #65e6a5;
}

.bad {
  color: #ff7b7b;
}

.muted {
  color: #8ea4b5;
}

.grid {
  display: grid;
  grid-template-columns: repeat(auto-fit,minmax(170px,1fr));
  gap: 10px;
  margin: 14px 0;
}

.card {
  background: #0d1721;
  border: 1px solid #1c3447;
  border-radius: 12px;
  padding: 14px;
}

.big {
  font-size: 24px;
  font-weight: 700;
  margin-top: 5px;
}

.section {
  margin-top: 18px;
}

.tablewrap {
  overflow: auto;
  border: 1px solid #1c3447;
  border-radius: 12px;
}

table {
  width: 100%;
  border-collapse: collapse;
  min-width: 1050px;
  background: #0d1721;
}

th,
td {
  padding: 10px;
  border-bottom: 1px solid #193044;
  text-align: left;
  white-space: nowrap;
}

th {
  position: sticky;
  top: 0;
  background: #10202d;
}

.buy {
  color: #60e6a0;
  font-weight: 700;
}

.sell {
  color: #ff7b7b;
  font-weight: 700;
}

.pos {
  color: #65e6a5;
}

.neg {
  color: #ff7b7b;
}

.btn {
  border: 0;
  border-radius: 8px;
  padding: 8px 11px;
  cursor: pointer;
  font-weight: 700;
}

.danger {
  background: #d94a4a;
  color: white;
}

.toolbar {
  display: flex;
  gap: 8px;
  align-items: center;
  flex-wrap: wrap;
}

.reason {
  max-width: 260px;
  overflow: hidden;
  text-overflow: ellipsis;
}

.small {
  font-size: 12px;
}

.statusline {
  margin: 8px 0 0;
}

.error {
  background: #3b1919;
  border: 1px solid #7d3030;
  padding: 10px;
  border-radius: 8px;
  display: none;
}
</style>
</head>

<body>

<div class="wrap">

<div class="top">

<div>
<h2 style="margin:0">${VERSION}</h2>

<div class="muted statusline">
PAPER ONLY — LIVE TRADING OFF • 15m CLOSED candles • 31 instruments
</div>
</div>

<div class="toolbar">

<span id="market" class="badge">
Market...
</span>

<button class="btn danger" onclick="closeAll()">
Close All
</button>

</div>
</div>

<div id="err" class="error"></div>

<div class="grid" id="cards"></div>

<div class="grid" id="dircards"></div>

<div class="section">

<h3>Open Positions</h3>

<div class="tablewrap">

<table>

<thead>

<tr>
<th>Symbol</th>
<th>Side</th>
<th>Entry</th>
<th>Current</th>
<th>SL</th>
<th>BE</th>
<th>TP</th>
<th>R</th>
<th>Floating</th>
<th>MFE</th>
<th>MAE</th>
<th>CMO</th>
<th>Vol</th>
<th>Body</th>
<th>Quality</th>
<th>Reasons</th>
<th>Duration</th>
<th>Action</th>
</tr>

</thead>

<tbody id="openRows"></tbody>

</table>

</div>
</div>

<div class="section">

<h3>Recent Closed Trades</h3>

<div class="tablewrap">

<table>

<thead>

<tr>
<th>Symbol</th>
<th>Side</th>
<th>Reason</th>
<th>Entry</th>
<th>Exit</th>
<th>R</th>
<th>PnL</th>
<th>MFE</th>
<th>MAE</th>
<th>Quality</th>
<th>Closed</th>
</tr>

</thead>

<tbody id="closedRows"></tbody>

</table>

</div>
</div>

<div class="section small muted">
Protection frozen:
BUY BULL-FVG blocked •
BUY CMO ≥ 80 blocked •
SELL CMO &gt; -40 blocked •
RR 1.2R •
BE +0.6R •
Risk 0.50% •
Portfolio cap 4%
</div>

</div>

<script>

const money =
  value =>
    (Number(value) || 0)
      .toLocaleString(
        undefined,
        {
          style:
            'currency',

          currency:
            'USD',

          minimumFractionDigits:
            2
        }
      );

const num =
  (
    value,
    digits = 2
  ) =>
    Number.isFinite(
      Number(value)
    )
      ? Number(value).toFixed(
          digits
        )
      : 'n/a';

const price =
  (
    value,
    symbol
  ) => {
    if (
      !Number.isFinite(
        Number(value)
      )
    ) {
      return 'n/a';
    }

    return Number(value).toFixed(
      symbol ===
        'XAUUSD'
        ? 2
        : symbol &&
          symbol.endsWith(
            'JPY'
          )
          ? 3
          : 5
    );
  };

const cls =
  value =>
    Number(value) >=
    0
      ? 'pos'
      : 'neg';

const dur =
  minutes => {
    minutes =
      Number(minutes) ||
      0;

    if (
      minutes <
      60
    ) {
      return Math.round(
        minutes
      ) + 'm';
    }

    const hours =
      Math.floor(
        minutes /
        60
      );

    const remain =
      Math.round(
        minutes %
        60
      );

    return (
      hours +
      'h ' +
      remain +
      'm'
    );
  };

const esc =
  value =>
    String(
      value ??
      ''
    ).replace(
      /[&<>"']/g,
      char => ({
        '&':
          '&amp;',

        '<':
          '&lt;',

        '>':
          '&gt;',

        '"':
          '&quot;',

        "'":
          '&#39;'
      }[char])
    );

async function load() {
  try {
    const response =
      await fetch(
        '/api/dashboard',
        {
          cache:
            'no-store'
        }
      );

    const data =
      await response.json();

    document.getElementById(
      'market'
    ).innerHTML =
      (
        data.marketReady
          ? '● READY'
          : '● WAIT'
      ) +
      ' • ' +
      data.pairsReady +
      '/' +
      data.pairsTotal;

    document.getElementById(
      'market'
    ).className =
      'badge ' +
      (
        data.marketReady
          ? 'ok'
          : 'bad'
      );

    const account =
      data.account;

    const engine =
      data.engine;

    const cards = [
      [
        'Balance',
        money(
          account.balance
        )
      ],

      [
        'Equity',
        money(
          account.equity
        )
      ],

      [
        'Realized',
        money(
          account.realizedPnl
        )
      ],

      [
        'Unrealized',
        money(
          account.unrealizedPnl
        )
      ],

      [
        'Open Risk',
        money(
          account.openRiskUsd
        ) +
        ' / ' +
        money(
          account.riskCapUsd
        )
      ],

      [
        'Open Trades',
        engine.openTrades
      ],

      [
        'Wins / Losses / BE',
        account.wins +
        ' / ' +
        account.losses +
        ' / ' +
        account.breakeven
      ],

      [
        'Signals Exec / Skip',
        engine.executedSignals +
        ' / ' +
        engine.skippedSignals
      ]
    ];

    document.getElementById(
      'cards'
    ).innerHTML =
      cards
        .map(
          item =>
            '<div class="card">' +
            '<div class="muted">' +
            item[0] +
            '</div>' +
            '<div class="big">' +
            item[1] +
            '</div>' +
            '</div>'
        )
        .join('');

    const directionStats =
      data.directionStats;

    document.getElementById(
      'dircards'
    ).innerHTML =
      [
        'BUY',
        'SELL'
      ]
        .map(
          side =>
            '<div class="card">' +
            '<div class="' +
            side.toLowerCase() +
            '">' +
            side +
            ' performance</div>' +
            '<div class="big ' +
            cls(
              directionStats[side].pnl
            ) +
            '">' +
            money(
              directionStats[side].pnl
            ) +
            '</div>' +
            '<div class="muted">' +
            directionStats[side].trades +
            ' closed • ' +
            num(
              directionStats[side].r,
              2
            ) +
            'R</div>' +
            '</div>'
        )
        .join('');

    document.getElementById(
      'openRows'
    ).innerHTML =
      data.openTrades.length
        ? data.openTrades
            .map(
              trade =>
                '<tr>' +

                '<td><b>' +
                esc(
                  trade.symbol
                ) +
                '</b></td>' +

                '<td class="' +
                trade.direction.toLowerCase() +
                '">' +
                trade.direction +
                '</td>' +

                '<td>' +
                price(
                  trade.entryPrice,
                  trade.symbol
                ) +
                '</td>' +

                '<td>' +
                price(
                  trade.currentPrice,
                  trade.symbol
                ) +
                '</td>' +

                '<td>' +
                price(
                  trade.stopLoss,
                  trade.symbol
                ) +
                '</td>' +

                '<td>' +
                (
                  trade.breakEvenActive
                    ? 'ON'
                    : 'OFF'
                ) +
                '</td>' +

                '<td>' +
                price(
                  trade.takeProfit,
                  trade.symbol
                ) +
                '</td>' +

                '<td class="' +
                cls(
                  trade.currentR
                ) +
                '">' +
                num(
                  trade.currentR,
                  2
                ) +
                'R</td>' +

                '<td class="' +
                cls(
                  trade.floatingPnl
                ) +
                '">' +
                money(
                  trade.floatingPnl
                ) +
                '</td>' +

                '<td>' +
                num(
                  trade.mfeR,
                  2
                ) +
                'R</td>' +

                '<td>' +
                num(
                  trade.maeR,
                  2
                ) +
                'R</td>' +

                '<td>' +
                num(
                  trade.cmo,
                  1
                ) +
                '</td>' +

                '<td>' +
                num(
                  trade.volumeRatio,
                  2
                ) +
                'x</td>' +

                '<td>' +
                num(
                  (
                    Number(
                      trade.bodyRatio
                    ) ||
                    0
                  ) *
                  100,
                  0
                ) +
                '%</td>' +

                '<td>' +
                esc(
                  trade.quality
                ) +
                ' ' +
                num(
                  trade.qualityScore,
                  0
                ) +
                '/12</td>' +

                '<td class="reason" title="' +
                esc(
                  (
                    trade.reasons ||
                    []
                  ).join(', ')
                ) +
                '">' +
                esc(
                  (
                    trade.reasons ||
                    []
                  ).join(', ') ||
                  '—'
                ) +
                '</td>' +

                '<td>' +
                dur(
                  trade.durationMinutes
                ) +
                '</td>' +

                '<td>' +
                '<button class="btn danger" onclick="closeOne(\\'' +
                trade.id +
                '\\',\\'' +
                trade.symbol +
                '\\')">Close</button>' +
                '</td>' +

                '</tr>'
            )
            .join('')
        : '<tr><td colspan="18" class="muted">No open PAPER trades</td></tr>';

    document.getElementById(
      'closedRows'
    ).innerHTML =
      data.recentClosed.length
        ? data.recentClosed
            .map(
              trade =>
                '<tr>' +

                '<td><b>' +
                esc(
                  trade.symbol
                ) +
                '</b></td>' +

                '<td class="' +
                trade.direction.toLowerCase() +
                '">' +
                trade.direction +
                '</td>' +

                '<td>' +
                esc(
                  trade.exitReason
                ) +
                '</td>' +

                '<td>' +
                price(
                  trade.entryPrice,
                  trade.symbol
                ) +
                '</td>' +

                '<td>' +
                price(
                  trade.exitPrice,
                  trade.symbol
                ) +
                '</td>' +

                '<td class="' +
                cls(
                  trade.resultR
                ) +
                '">' +
                num(
                  trade.resultR,
                  2
                ) +
                'R</td>' +

                '<td class="' +
                cls(
                  trade.pnl
                ) +
                '">' +
                money(
                  trade.pnl
                ) +
                '</td>' +

                '<td>' +
                num(
                  trade.mfeR,
                  2
                ) +
                'R</td>' +

                '<td>' +
                num(
                  trade.maeR,
                  2
                ) +
                'R</td>' +

                '<td>' +
                esc(
                  trade.quality ||
                  ''
                ) +
                ' ' +
                num(
                  trade.qualityScore,
                  0
                ) +
                '/12</td>' +

                '<td>' +
                new Date(
                  trade.closedAt
                ).toLocaleString() +
                '</td>' +

                '</tr>'
            )
            .join('')
        : '<tr><td colspan="11" class="muted">No closed trades yet</td></tr>';

    document.getElementById(
      'err'
    ).style.display =
      'none';

  } catch (
    error
  ) {
    const box =
      document.getElementById(
        'err'
      );

    box.textContent =
      'Dashboard error: ' +
      error.message;

    box.style.display =
      'block';
  }
}

async function closeOne(
  id,
  symbol
) {
  if (
    !confirm(
      'Close ' +
      symbol +
      ' PAPER trade now?'
    )
  ) {
    return;
  }

  const response =
    await fetch(
      '/api/trades/' +
      encodeURIComponent(
        id
      ) +
      '/close',
      {
        method:
          'POST',

        headers: {
          'Content-Type':
            'application/json'
        },

        body:
          JSON.stringify({
            confirm:
              'CLOSE'
          })
      }
    );

  const result =
    await response.json();

  if (
    !response.ok
  ) {
    alert(
      result.error ||
      'Close failed'
    );
  }

  await load();
}

async function closeAll() {
  if (
    !confirm(
      'Close ALL open PAPER trades?'
    )
  ) {
    return;
  }

  if (
    !confirm(
      'Confirm again: close every open PAPER trade now?'
    )
  ) {
    return;
  }

  const response =
    await fetch(
      '/api/trades/close-all',
      {
        method:
          'POST',

        headers: {
          'Content-Type':
            'application/json'
        },

        body:
          JSON.stringify({
            confirm:
              'CLOSE_ALL'
          })
      }
    );

  const result =
    await response.json();

  if (
    !response.ok
  ) {
    alert(
      result.error ||
      'Close all failed'
    );
  } else {
    alert(
      'Closed: ' +
      result.closed +
      ' | Failed: ' +
      result.failed
    );
  }

  await load();
}

load();

setInterval(
  load,
  3000
);

</script>

</body>
</html>`
      );
  }
);

app.get(
  '/api/dashboard',
  async (
    req,
    res
  ) => {
    try {
      res.json(
        await dashboardData()
      );
    } catch (
      error
    ) {
      res
        .status(500)
        .json({
          error:
            safeError(error)
        });
    }
  }
);

app.post(
  '/api/trades/:id/close',
  async (
    req,
    res
  ) => {
    try {
      if (
        req.body?.confirm !==
        'CLOSE'
      ) {
        return res
          .status(400)
          .json({
            error:
              'Confirmation required'
          });
      }

      const id =
        String(
          req.params.id ||
          ''
        );

      const trade =
        [
          ...state.openTrades.values()
        ].find(
          item =>
            String(
              item._id
            ) ===
            id
        );

      if (
        !trade
      ) {
        return res
          .status(404)
          .json({
            error:
              'Open trade not found'
          });
      }

      const quote =
        await fetchSingleQuote(
          trade.symbol
        );

      if (
        !quote
      ) {
        return res
          .status(503)
          .json({
            error:
              'Fresh quote unavailable'
          });
      }

      state.latestQuotes.set(
        trade.symbol,
        quote
      );

      await updateExcursions(
        trade.symbol,
        trade,
        quote
      );

      const liveTrade =
        state.openTrades.get(
          trade.symbol
        ) ||
        trade;

      const exitPrice =
        markPriceForTrade(
          liveTrade,
          quote
        );

      const closed =
        await closePaperTrade(
          trade.symbol,
          liveTrade,
          exitPrice,
          'MANUAL_CLOSE'
        );

      if (
        !closed
      ) {
        return res
          .status(409)
          .json({
            error:
              'Trade is already closing or closed'
          });
      }

      res.json({
        ok:
          true,

        symbol:
          trade.symbol
      });
    } catch (
      error
    ) {
      await journal(
        'ERROR',
        {
          message:
            'manual close failed',

          data: {
            error:
              safeError(error)
          }
        }
      );

      res
        .status(500)
        .json({
          error:
            safeError(error)
        });
    }
  }
);

app.post(
  '/api/trades/close-all',
  async (
    req,
    res
  ) => {
    try {
      if (
        req.body?.confirm !==
        'CLOSE_ALL'
      ) {
        return res
          .status(400)
          .json({
            error:
              'Confirmation required'
          });
      }

      const trades =
        [
          ...state.openTrades.values()
        ];

      if (
        !trades.length
      ) {
        return res.json({
          ok:
            true,

          closed:
            0,

          failed:
            0
        });
      }

      const quotes =
        await fetchLatestQuotes(
          trades.map(
            trade =>
              trade.symbol
          )
        );

      let closed =
        0;

      let failed =
        0;

      for (
        const originalTrade
        of trades
      ) {
        try {
          const liveTrade =
            state.openTrades.get(
              originalTrade.symbol
            );

          if (
            !liveTrade
          ) {
            continue;
          }

          let quote =
            quotes.get(
              originalTrade.symbol
            );

          if (
            !quote
          ) {
            quote =
              await fetchSingleQuote(
                originalTrade.symbol
              );
          }

          if (
            !quote
          ) {
            failed++;
            continue;
          }

          state.latestQuotes.set(
            originalTrade.symbol,
            quote
          );

          await updateExcursions(
            originalTrade.symbol,
            liveTrade,
            quote
          );

          const latestTrade =
            state.openTrades.get(
              originalTrade.symbol
            ) ||
            liveTrade;

          const exitPrice =
            markPriceForTrade(
              latestTrade,
              quote
            );

          const result =
            await closePaperTrade(
              originalTrade.symbol,
              latestTrade,
              exitPrice,
              'MANUAL_CLOSE'
            );

          if (
            result
          ) {
            closed++;
          } else {
            failed++;
          }
        } catch (
          error
        ) {
          failed++;

          console.error(
            'Close all trade:',
            safeError(error)
          );
        }
      }

      await journal(
        'MANUAL_CLOSE_ALL',
        {
          message:
            'Dashboard Close All',

          data: {
            closed,
            failed
          }
        }
      );

      res.json({
        ok:
          true,

        closed,
        failed
      });
    } catch (
      error
    ) {
      await journal(
        'ERROR',
        {
          message:
            'close-all failed',

          data: {
            error:
              safeError(error)
          }
        }
      );

      res
        .status(500)
        .json({
          error:
            safeError(error)
        });
    }
  }
);

app.get(
  '/health',
  (
    req,
    res
  ) => {
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

      timeframe:
        TIMEFRAME,

      instruments:
        INSTRUMENTS.length,

      openTrades:
        state.openTrades.size,

      protectionRejects:
        state.protectionRejects,

      portfolioRiskRejects:
        state.portfolioRiskRejects,

      breakEvenMoves:
        state.breakEvenMoves,

      journalEvents:
        state.journalEvents,

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

app.get(
  '/api/status',
  (
    req,
    res
  ) => {
    res.json({
      version:
        VERSION,

      mode:
        MODE,

      liveTrading:
        LIVE_TRADING,

      paper: {
        accountKey:
          PAPER.accountKey,

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

        breakeven:
          account?.breakeven ??
          0,

        openTrades:
          state.openTrades.size,

        riskPctPerTrade:
          PAPER.riskPctPerTrade,

        portfolioRiskCapPct:
          PAPER.portfolioRiskCapPct,

        openRiskUsd:
          currentPortfolioRiskUsd(),

        portfolioRiskCapUsd:
          portfolioRiskCapUsd()
      },

      engine: {
        strategy:
          STRATEGY,

        instruments:
          INSTRUMENTS,

        rawSignals:
          state.rawSignals,

        executedSignals:
          state.executedSignals,

        skippedSignals:
          state.skippedSignals,

        protectionRejects:
          state.protectionRejects,

        portfolioRiskRejects:
          state.portfolioRiskRejects,

        signalScans:
          state.totalSignalScans,

        quotePolls:
          state.totalQuotePolls,

        breakEvenMoves:
          state.breakEvenMoves,

        journalEvents:
          state.journalEvents
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
    `💰 New account: ${fmtMoney(PAPER.startingBalance)} | ` +
    `Risk ${PAPER.riskPctPerTrade}% | ` +
    `Portfolio cap ${PAPER.portfolioRiskCapPct}%`
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
    '🛡️ V1.2.3 Protection FROZEN: BUY BULL-FVG BLOCK | BUY CMO>=80 BLOCK | SELL CMO>-40 BLOCK'
  );

  console.log(
    '📓 V1.2.4 Journal + MFE/MAE + Portfolio Risk Cap + Dashboard ON'
  );

  console.log(
    `🎯 R:R 1:${STRATEGY.riskReward.toFixed(1)} | ` +
    `BE at +${STRATEGY.breakEvenTriggerR.toFixed(1)}R`
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

  await journal(
    'BOOT',
    {
      message:
        'LOMY boot',

      data: {
        startingBalance:
          PAPER.startingBalance,

        riskPctPerTrade:
          PAPER.riskPctPerTrade,

        portfolioRiskCapPct:
          PAPER.portfolioRiskCapPct,

        timeframe:
          TIMEFRAME,

        instruments:
          INSTRUMENTS.length
      }
    }
  );

  await initTelegram();

  try {
    await initializeMarket();
  } catch (
    error
  ) {
    state.initializing =
      false;

    state.marketReady =
      false;

    state.lastMarketError =
      safeError(error);

    await journal(
      'ERROR',
      {
        message:
          'Market initialization failed',

        data: {
          error:
            state.lastMarketError
        }
      }
    );

    console.error(
      '❌ Market initialization:',
      state.lastMarketError
    );
  }

  setInterval(
    signalScanLoop,
    SCAN_TIMER_MS
  );

  setInterval(
    manageOpenTrades,
    QUOTE_POLL_MS
  );

  console.log(
    '✅ LOMY Forex loops started'
  );
}

// ============================================================
// SHUTDOWN
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
    `\n🛑 ${signal} received — shutting down`
  );

  try {
    await journal(
      'SHUTDOWN',
      {
        message:
          signal,

        data: {
          balance:
            account?.balance,

          openTrades:
            state.openTrades.size
        }
      }
    );
  } catch {}

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
    await mongoose.connection.close();
  } catch {}

  process.exit(
    0
  );
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

    journal(
      'ERROR',
      {
        message:
          'unhandledRejection',

        data: {
          error:
            safeError(error)
        }
      }
    ).catch(
      () => {}
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

    journal(
      'ERROR',
      {
        message:
          'uncaughtException',

        data: {
          error:
            safeError(error)
        }
      }
    ).catch(
      () => {}
    );
  }
);

// ============================================================
// START
// ============================================================

boot().catch(
  error => {
    console.error(
      'FATAL:',
      safeError(error)
    );

    process.exit(
      1
    );
  }
);
