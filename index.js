'use strict';

const axios = require('axios');
const express = require('express');
const mongoose = require('mongoose');
const { Telegraf } = require('telegraf');

const VERSION = 'LOMY FOREX V1.2.3';
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

  // Money Management
  riskReward: 1.20,
  breakEvenTriggerR: 0.60,

  // Anti-chase
  maxEntryMoveAtr: 0.50,

  // Retest + Fib
  retestLookback: 8,
  fibLookback: 40,
  fibToleranceAtr: 0.20,

  // SMC
  smcFastEmaLen: 50,
  smcSlowEmaLen: 200,

  adxLen: 14,
  adxThreshold: 25,

  pivotLeft: 5,
  pivotRight: 5,

  // ==========================================================
  // V1.2.3 DATA-DRIVEN ENTRY PROTECTION
  // ==========================================================

  // BUY + Bull FVG was weak in V1.2.2 sample.
  rejectBullFvgBuys: true,

  // Reject overextended BUY momentum.
  buyCmoMax: 80,

  // SELL must have CMO <= -40.
  sellCmoMax: -40
});


// ============================================================
// PAPER ACCOUNT
// ============================================================

const PAPER = Object.freeze({

  startingBalance: 1000,

  riskPctPerTrade: 0.50,

  maxOpenTrades: 31,

  accountKey:
    'lomy-forex-v120-smc-be-12r'
});


// ============================================================
// RUNTIME STATE
// ============================================================

const state = {

  startedAt:
    new Date(),

  mongoReady:
    false,

  telegramReady:
    false,

  marketReady:
    false,

  initializing:
    true,

  scanRunning:
    false,

  quoteRunning:
    false,

  lastScanSlot:
    null,

  lastSignalScanAt:
    null,

  lastQuotePollAt:
    null,

  lastMarketError:
    null,

  totalSignalScans:
    0,

  totalQuotePolls:
    0,

  rawSignals:
    0,

  executedSignals:
    0,

  skippedSignals:
    0,

  breakEvenMoves:
    0,

  protectionRejects:
    0,

  pairState:
    new Map(),

  latestQuotes:
    new Map(),

  openTrades:
    new Map()
};


for (const symbol of INSTRUMENTS) {

  state.pairState.set(
    symbol,
    {
      bars: [],

      lastClosedBarTime:
        null,

      lastSignal:
        0,

      lastAnalysis:
        null,

      initialized:
        false,

      errors:
        0
    }
  );
}


// ============================================================
// HELPERS
// ============================================================

function n(
  value,
  fallback = NaN
) {

  const x =
    Number(
      value
    );

  return Number.isFinite(
    x
  )
    ? x
    : fallback;
}


function safeError(
  error
) {

  return (
    error?.response?.data?.message ||
    error?.response?.data?.error ||
    error?.message ||
    String(error)
  );
}


function clamp(
  value,
  minimum,
  maximum
) {

  return Math.max(
    minimum,
    Math.min(
      maximum,
      value
    )
  );
}


function fmtMoney(
  value
) {

  return (
    '$' +
    n(
      value,
      0
    ).toFixed(
      2
    )
  );
}


function fmtPrice(
  value,
  symbol = ''
) {

  if (
    !Number.isFinite(
      value
    )
  ) {
    return 'n/a';
  }


  if (
    symbol ===
    'XAUUSD'
  ) {

    return value.toFixed(
      2
    );
  }


  if (
    symbol.endsWith(
      'JPY'
    )
  ) {

    return value.toFixed(
      3
    );
  }


  return value.toFixed(
    5
  );
}


function barTimeMs(
  bar
) {

  const time =
    new Date(
      bar.openTime
    ).getTime();


  return Number.isFinite(
    time
  )
    ? time
    : 0;
}


// ============================================================
// BAR NORMALIZATION
// ============================================================

function normalizeBars(
  rawBars
) {

  if (
    !Array.isArray(
      rawBars
    )
  ) {
    return [];
  }


  return rawBars

    .map(
      bar => ({

        openTime:
          bar.openTime ||
          bar.datetime ||
          bar.time ||
          bar.timestamp,

        open:
          n(
            bar.open
          ),

        high:
          n(
            bar.high
          ),

        low:
          n(
            bar.low
          ),

        close:
          n(
            bar.close
          ),

        volume:
          n(
            bar.tickVolume,
            n(
              bar.volume,
              0
            )
          ),

        isOpen:
          bar.isOpen ===
          true
      })
    )

    .filter(
      bar =>

        bar.openTime &&

        [
          bar.open,
          bar.high,
          bar.low,
          bar.close
        ].every(
          Number.isFinite
        )
    )

    .sort(
      (
        a,
        b
      ) =>
        barTimeMs(a) -
        barTimeMs(b)
    );
}


function closedBarsOnly(
  bars
) {

  const now =
    Date.now();


  const timeframeMs =
    15 *
    60 *
    1000;


  return bars.filter(
    bar => {

      if (
        bar.isOpen
      ) {
        return false;
      }


      const time =
        barTimeMs(
          bar
        );


      return (
        time >
          0 &&

        time +
          timeframeMs <=
          now +
          5000
      );
    }
  );
}


// ============================================================
// SMA
// ============================================================

function sma(
  values,
  length
) {

  if (
    !Array.isArray(
      values
    ) ||

    values.length <
      length
  ) {
    return NaN;
  }


  const selected =
    values.slice(
      -length
    );


  if (
    !selected.every(
      Number.isFinite
    )
  ) {
    return NaN;
  }


  return selected.reduce(
    (
      total,
      value
    ) =>
      total +
      value,
    0
  ) / length;
}


// ============================================================
// EMA
// ============================================================

function emaSeries(
  values,
  length
) {

  if (
    !Array.isArray(
      values
    ) ||

    values.length <
      length
  ) {
    return [];
  }


  const output =
    new Array(
      values.length
    ).fill(
      NaN
    );


  const multiplier =
    2 /
    (
      length +
      1
    );


  output[
    length -
    1
  ] =
    values
      .slice(
        0,
        length
      )
      .reduce(
        (
          total,
          value
        ) =>
          total +
          value,
        0
      ) /
    length;


  for (
    let i =
      length;

    i <
      values.length;

    i++
  ) {

    output[i] =
      values[i] *
        multiplier +

      output[
        i -
        1
      ] *
        (
          1 -
          multiplier
        );
  }


  return output;
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


  return series.length
    ? series[
        series.length -
        1
      ]
    : NaN;
}


// ============================================================
// ATR
// ============================================================

function atrLast(
  bars,
  length
) {

  if (
    bars.length <
      length +
      1
  ) {
    return NaN;
  }


  const trueRange =
    [];


  for (
    let i =
      1;

    i <
      bars.length;

    i++
  ) {

    const current =
      bars[i];


    const previousClose =
      bars[
        i -
        1
      ].close;


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
        (
          total,
          value
        ) =>
          total +
          value,
        0
      ) /
    length;


  for (
    let i =
      length;

    i <
      trueRange.length;

    i++
  ) {

    atr =
      (
        atr *
          (
            length -
            1
          ) +

        trueRange[i]
      ) /
      length;
  }


  return atr;
}


// ============================================================
// CMO
// ============================================================

function cmoLast(
  values,
  length
) {

  if (
    values.length <
      length +
      1
  ) {
    return NaN;
  }


  let up =
    0;


  let down =
    0;


  for (
    let i =
      values.length -
      length;

    i <
      values.length;

    i++
  ) {

    const difference =
      values[i] -
      values[
        i -
        1
      ];


    if (
      difference >
      0
    ) {

      up +=
        difference;

    } else {

      down +=
        Math.abs(
          difference
        );
    }
  }


  const denominator =
    up +
    down;


  return denominator ===
    0

    ? 0

    : 100 *
      (
        up -
        down
      ) /
      denominator;
}


// ============================================================
// HIGH / LOW
// ============================================================

function highestHigh(
  bars
) {

  return Math.max(
    ...bars.map(
      bar =>
        bar.high
    )
  );
}


function lowestLow(
  bars
) {

  return Math.min(
    ...bars.map(
      bar =>
        bar.low
    )
  );
}


// ============================================================
// DMI / ADX
// ============================================================

function dmiAdx(
  bars,
  length = 14
) {

  if (
    bars.length <
      length *
        2 +
      2
  ) {

    return {
      adx: NaN,
      plusDI: NaN,
      minusDI: NaN
    };
  }


  const trueRange =
    [];


  const plus =
    [];


  const minus =
    [];


  for (
    let i =
      1;

    i <
      bars.length;

    i++
  ) {

    const current =
      bars[i];


    const previous =
      bars[
        i -
        1
      ];


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
        (
          total,
          value
        ) =>
          total +
          value,
        0
      );


  let plusSmooth =
    plus
      .slice(
        0,
        length
      )
      .reduce(
        (
          total,
          value
        ) =>
          total +
          value,
        0
      );


  let minusSmooth =
    minus
      .slice(
        0,
        length
      )
      .reduce(
        (
          total,
          value
        ) =>
          total +
          value,
        0
      );


  const dx =
    [];


  let plusDI =
    NaN;


  let minusDI =
    NaN;


  for (
    let i =
      length;

    i <
      trueRange.length;

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
        (
          total,
          value
        ) =>
          total +
          value,
        0
      ) /
    length;


  for (
    let i =
      length;

    i <
      dx.length;

    i++
  ) {

    adx =
      (
        adx *
          (
            length -
            1
          ) +

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


// ============================================================
// RETEST CONTEXT
// ============================================================

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
    direction ===
    'BUY'

      ? highestHigh(
          window
        )

      : lowestLow(
          window
        );


  let breakout =
    false;


  let retest =
    false;


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
      Number(
        breakout
      ) +
      Number(
        retest
      ),

    breakout,
    retest,
    level
  };
}


// ============================================================
// FIBONACCI CONTEXT
// ============================================================

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

    !Number.isFinite(
      atr
    )
  ) {

    return {
      score: 0,
      valid: false,
      goldenZone: false,
      nearFib: false
    };
  }


  const low =
    lowestLow(
      window
    );


  const high =
    highestHigh(
      window
    );


  const range =
    high -
    low;


  if (
    !(
      range >
      0
    )
  ) {

    return {
      score: 0,
      valid: false,
      goldenZone: false,
      nearFib: false
    };
  }


  const levels =
    direction ===
    'BUY'

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

    valid:
      true,

    goldenZone,
    nearFib,

    swingLow:
      low,

    swingHigh:
      high,

    ...levels
  };
}


// ============================================================
// SMC CONTEXT
// ============================================================

function smcContext(
  bars,
  direction
) {

  const signal =
    bars[
      bars.length -
      1
    ];


  const prior =
    bars.slice(
      0,
      -1
    );


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


  const reasons =
    [];


  let score =
    0;


  // EMA 50 / 200 context
  if (
    Number.isFinite(
      ema200
    )
  ) {

    const aligned =
      direction ===
      'BUY'

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

      score +=
        1;


      reasons.push(
        direction ===
        'BUY'

          ? 'EMA-TREND-BULL'

          : 'EMA-TREND-BEAR'
      );
    }

  } else {

    reasons.push(
      'EMA200-WARMUP'
    );
  }


  // ADX context
  const adxAligned =
    Number.isFinite(
      dmi.adx
    ) &&

    dmi.adx >=
      STRATEGY.adxThreshold &&

    (
      direction ===
      'BUY'

        ? dmi.plusDI >
          dmi.minusDI

        : dmi.minusDI >
          dmi.plusDI
    );


  if (
    adxAligned
  ) {

    score +=
      1;


    reasons.push(
      direction ===
      'BUY'

        ? 'ADX-BULL'

        : 'ADX-BEAR'
    );
  }


  // Liquidity Sweep
  const sweepWindow =
    prior.slice(
      -10
    );


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

    score +=
      3;


    reasons.push(
      'LIQUIDITY-SWEEP'
    );
  }


  // FVG
  let bullFvg =
    false;


  let bearFvg =
    false;


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

    score +=
      1;


    reasons.push(
      'BULL-FVG'
    );
  }


  if (
    direction ===
      'SELL' &&
    bearFvg
  ) {

    score +=
      1;


    reasons.push(
      'BEAR-FVG'
    );
  }


  return {

    score,

    reasons,

    ema50,
    ema200,

    adx:
      dmi.adx,

    plusDI:
      dmi.plusDI,

    minusDI:
      dmi.minusDI,

    liquiditySweep,
    bullFvg,
    bearFvg
  };
}


// ============================================================
// V5.1 ANALYSIS
// ============================================================

function analyzeBars(
  symbol,
  bars,
  memory
) {

  if (
    !Array.isArray(
      bars
    ) ||

    bars.length <
      CORE_MIN_HISTORY
  ) {
    return null;
  }


  const signal =
    bars[
      bars.length -
      1
    ];


  const prior =
    bars.slice(
      0,
      -1
    );


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


  const bullMomentum =
    momentum >
    STRATEGY.cmoBuyThresh;


  const bearMomentum =
    momentum <
    STRATEGY.cmoSellThresh;


  const bullTrend =
    ema9 >
    ema21;


  const bearTrend =
    ema9 <
    ema21;


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
      direction:
        null
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


// ============================================================
// V1.2.3 ENTRY PROTECTION
// ============================================================

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


  // BUY FILTERS
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


  // SELL FILTERS
  if (
    analysis.direction ===
    'SELL'
  ) {

    if (
      Number.isFinite(
        analysis.cmo
      ) &&

      analysis.cmo >
        STRATEGY.sellCmoMax
    ) {

      return 'SELL_CMO_WEAK';
    }
  }


  return '';
}


// ============================================================
// HTTP
// ============================================================

const http =
  axios.create({

    timeout:
      12000,

    headers: {

      'User-Agent':
        'LOMY-Forex-Paper/1.2.3'
    }
  });


// ============================================================
// BIQUOTE OHLC
// ============================================================

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
    Array.isArray(
      body
    )

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


// ============================================================
// QUOTES
// ============================================================

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

      Number.isFinite(
        bid
      ) &&
      Number.isFinite(
        ask
      )

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

        const row =
          body[symbol] ||
          body[
            symbol.toLowerCase()
          ];


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


// ============================================================
// CONCURRENCY
// ============================================================

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


// ============================================================
// MONGODB SCHEMAS
// ============================================================

const accountSchema =
  new mongoose.Schema({

    accountKey: {
      type:
        String,

      unique:
        true,

      index:
        true
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

  }, {
    minimize:
      false
  });


const tradeSchema =
  new mongoose.Schema({

    version:
      String,

    accountKey: {
      type:
        String,

      index:
        true
    },

    symbol: {
      type:
        String,

      index:
        true
    },

    direction:
      String,

    status: {
      type:
        String,

      index:
        true
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

    analysis:
      mongoose.Schema.Types.Mixed

  }, {
    minimize:
      false
  });


const signalSchema =
  new mongoose.Schema({

    version:
      String,

    accountKey: {
      type:
        String,

      index:
        true
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

  }, {
    minimize:
      false
  });


// ============================================================
// MODELS
// ============================================================

const Account =
  mongoose.models.LomyForexPaperAccount ||

  mongoose.model(
    'LomyForexPaperAccount',
    accountSchema,
    'lomyforexpaperaccounts'
  );


const Trade =
  mongoose.models.LomyForexTrade ||

  mongoose.model(
    'LomyForexTrade',
    tradeSchema,
    'lomyforextrades'
  );


const Signal =
  mongoose.models.LomyForexSignal ||

  mongoose.model(
    'LomyForexSignal',
    signalSchema,
    'lomyforexsignals'
  );


let account =
  null;


// ============================================================
// MONGODB INIT
// ============================================================

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

    state.openTrades.set(
      trade.symbol,
      trade
    );
  }


  console.log(
    `✅ Restored ${openTrades.length} open PAPER trade(s)`
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


// ============================================================
// TELEGRAM
// ============================================================

let bot =
  null;


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
      safeError(
        error
      )
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
        `✅ ${VERSION}\nPAPER ONLY`
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

        `Pairs ready: ${pairReadyCount()}/${INSTRUMENTS.length}\n` +

        `Open trades: ${state.openTrades.size}\n` +

        `Scans: ${state.totalSignalScans}\n` +

        `Signals: ${state.rawSignals}\n` +

        `Executed: ${state.executedSignals}\n` +

        `Skipped: ${state.skippedSignals}\n` +

        `Protection rejects: ${state.protectionRejects}\n` +

        `BE moves: ${state.breakEvenMoves}\n` +

        `TP: ${STRATEGY.riskReward.toFixed(1)}R\n` +

        `Break-even: +${STRATEGY.breakEvenTriggerR.toFixed(1)}R`
      );
    }
  );


  bot.command(
    'balance',
    async ctx => {

      await ctx.reply(

        `💰 PAPER BALANCE\n` +

        `Balance: ${fmtMoney(account?.balance)}\n` +

        `Realized PnL: ${fmtMoney(account?.realizedPnl)}`
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

        `Break-even: ${account?.breakeven || 0}\n` +

        `Realized: ${fmtMoney(account?.realizedPnl || 0)}`
      );
    }
  );


  bot.command(
    'pairs',
    async ctx => {

      await ctx.reply(
        `📡 ${INSTRUMENTS.length} instruments\n${INSTRUMENTS.join(', ')}`
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


      const lines =
        positions.map(
          trade =>

            `${trade.symbol} ${trade.direction}\n` +

            `Entry ${fmtPrice(trade.entryPrice, trade.symbol)} | ` +

            `SL ${fmtPrice(trade.stopLoss, trade.symbol)} | ` +

            `TP ${fmtPrice(trade.takeProfit, trade.symbol)} | ` +

            `BE ${trade.breakEvenActive ? 'ON' : 'OFF'}`
        );


      await ctx.reply(
        lines.join(
          '\n\n'
        )
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
          .limit(
            10
          )
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

        rows.map(
          trade =>

            `${trade.symbol} ${trade.direction} ` +

            `${trade.exitReason} ` +

            `${n(trade.resultR, 0).toFixed(2)}R`
        ).join(
          '\n'
        )
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
          safeError(
            error
          )
        );
      }
    );
}


// ============================================================
// SIGNAL JOURNAL
// ============================================================

async function recordSignal(
  analysis,
  executed,
  skipReason = ''
) {

  try {

    await Signal.create({

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
    });

  } catch (
    error
  ) {

    console.error(
      'Signal journal:',
      safeError(
        error
      )
    );
  }
}


// ============================================================
// BUILD EXECUTION LEVELS
// ============================================================

function buildExecutionLevels(
  analysis,
  quote
) {

  const direction =
    analysis.direction;


  const atr =
    analysis.atr;


  if (
    !Number.isFinite(
      atr
    ) ||
    atr <=
      0
  ) {
    return null;
  }


  const entry =
    direction ===
    'BUY'

      ? quote.ask

      : quote.bid;


  let stopLoss;


  if (
    direction ===
    'BUY'
  ) {

    stopLoss =
      n(
        analysis.support
      ) -
      atr *
      STRATEGY.atrMargin;


    if (
      !(
        stopLoss <
        entry
      )
    ) {

      stopLoss =
        analysis.signalClose -
        atr;
    }


    if (
      !(
        stopLoss <
        entry
      )
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
      !(
        stopLoss >
        entry
      )
    ) {

      stopLoss =
        analysis.signalClose +
        atr;
    }


    if (
      !(
        stopLoss >
        entry
      )
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
    !(
      riskDistance >
      0
    )
  ) {
    return null;
  }


  const takeProfit =
    direction ===
    'BUY'

      ? entry +
        riskDistance *
        STRATEGY.riskReward

      : entry -
        riskDistance *
        STRATEGY.riskReward;


  const breakEvenTriggerPrice =
    direction ===
    'BUY'

      ? entry +
        riskDistance *
        STRATEGY.breakEvenTriggerR

      : entry -
        riskDistance *
        STRATEGY.breakEvenTriggerR;


  const valid =
    direction ===
    'BUY'

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


// ============================================================
// OPEN PAPER TRADE
// ============================================================

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
    direction ===
    'BUY'

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


  const quantity =
    riskAmount /
    levels.riskDistance;


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
        new Date(),

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


  await sendTelegram(

    `${direction === 'BUY' ? '🟢' : '🔴'} ${symbol} ${direction} — PAPER\n` +

    `Entry: ${fmtPrice(levels.entry, symbol)}\n` +

    `SL: ${fmtPrice(levels.stopLoss, symbol)}\n` +

    `TP: ${fmtPrice(levels.takeProfit, symbol)} (${STRATEGY.riskReward.toFixed(1)}R)\n` +

    `BE: ${fmtPrice(levels.breakEvenTriggerPrice, symbol)} (+${STRATEGY.breakEvenTriggerR.toFixed(1)}R)\n` +

    `Quality: ${analysis.quality} ${analysis.qualityScore}/12\n` +

    `CMO: ${analysis.cmo.toFixed(1)}\n` +

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


// ============================================================
// BREAK-EVEN
// ============================================================

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


  state.breakEvenMoves++;


  state.openTrades.set(
    symbol,
    trade
  );


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


  await sendTelegram(

    `🛡️ BREAK-EVEN ACTIVATED\n` +

    `${symbol} ${trade.direction}\n` +

    `SL moved to Entry: ${fmtPrice(trade.entryPrice, symbol)}`
  );
}


// ============================================================
// CLOSE PAPER TRADE
// ============================================================

async function closePaperTrade(
  symbol,
  trade,
  exitPrice,
  reason
) {

  if (
    !state.openTrades.has(
      symbol
    )
  ) {
    return;
  }


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


  await Trade.updateOne(
    {
      _id:
        trade._id
    },
    {
      $set: {

        status:
          'CLOSED',

        closedAt:
          new Date(),

        exitPrice,

        exitReason:
          reason,

        pnl,

        resultR,

        stopLoss:
          trade.stopLoss,

        breakEvenActive:
          trade.breakEvenActive
      }
    }
  );


  state.openTrades.delete(
    symbol
  );


  await sendTelegram(

    `${pnl >= 0 ? '✅' : '❌'} CLOSED ${symbol} ${trade.direction}\n` +

    `Reason: ${reason}\n` +

    `Exit: ${fmtPrice(exitPrice, symbol)}\n` +

    `Result: ${resultR.toFixed(2)}R\n` +

    `PnL: ${fmtMoney(pnl)}\n` +

    `Balance: ${fmtMoney(account.balance)}`
  );
}


// ============================================================
// CURRENT R
// ============================================================

function currentR(
  trade,
  quote
) {

  const price =
    trade.direction ===
    'BUY'

      ? quote.bid

      : quote.ask;


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


// ============================================================
// REVERSE SIGNAL
// ============================================================

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


  // Do not close losing trade just because opposite signal appeared.
  if (
    r <
      0 &&
    !trade.breakEvenActive
  ) {
    return;
  }


  const exitPrice =
    trade.direction ===
    'BUY'

      ? quote.bid

      : quote.ask;


  await closePaperTrade(
    analysis.symbol,
    trade,
    exitPrice,
    'REVERSE_SIGNAL'
  );
}


// ============================================================
// MANAGE OPEN TRADES
// ============================================================

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


      // Break-even activation
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


          continue;
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

  } finally {

    state.quoteRunning =
      false;
  }
}


// ============================================================
// SCAN SYMBOL
// ============================================================

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


    // ========================================================
    // V1.2.3 PROTECTION MUST HAPPEN BEFORE REVERSE/ENTRY
    // ========================================================

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

        `CMO=${analysis.cmo.toFixed(1)} | ` +

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

      `CMO=${analysis.cmo.toFixed(1)} | ` +

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


    await openPaperTrade(
      analysis,
      quote
    );


    return {
      symbol,
      status:
        'SIGNAL',
      direction:
        analysis.direction
    };

  } catch (
    error
  ) {

    memory.errors++;


    state.lastMarketError =
      `${symbol}: ${safeError(error)}`;


    return {
      symbol,
      status:
        'ERROR',
      error:
        safeError(
          error
        )
    };
  }
}


// ============================================================
// SIGNAL SCAN LOOP
// ============================================================

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

  } finally {

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
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>${VERSION}</title>
</head>
<body style="font-family:Arial;background:#111;color:#eee;padding:24px">

<h2>${VERSION}</h2>

<p>PAPER ONLY — LIVE TRADING OFF</p>

<p>
Market:
${state.marketReady ? 'READY' : 'WAIT'}
</p>

<p>
Pairs:
${pairReadyCount()}/${INSTRUMENTS.length}
</p>

<p>
Open trades:
${state.openTrades.size}
</p>

<p>
Balance:
${fmtMoney(account?.balance ?? PAPER.startingBalance)}
</p>

<p>
Protection:
BUY BULL-FVG blocked |
BUY CMO &gt;= 80 blocked |
SELL CMO &gt; -40 blocked
</p>

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

      breakEvenMoves:
        state.breakEvenMoves,

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
          state.openTrades.size
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

        signalScans:
          state.totalSignalScans,

        quotePolls:
          state.totalQuotePolls,

        breakEvenMoves:
          state.breakEvenMoves
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
    '🧠 Core: V5.1 + SMC Sniper + FVG + Liquidity Sweep + Retest + Fibonacci'
  );


  console.log(
    '🛡️ V1.2.3 Protection: BUY BULL-FVG BLOCK | BUY CMO>=80 BLOCK | SELL CMO>-40 BLOCK'
  );


  console.log(

    `🎯 R:R 1:${STRATEGY.riskReward.toFixed(1)} | ` +

    `BE at +${STRATEGY.breakEvenTriggerR.toFixed(1)}R | ` +

    `Risk ${PAPER.riskPctPerTrade}%`
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

  } catch (
    error
  ) {

    state.initializing =
      false;


    state.marketReady =
      false;


    state.lastMarketError =
      safeError(
        error
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

async function shutdown(
  signal
) {

  console.log(
    `\n🛑 ${signal} received — shutting down`
  );


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
      safeError(
        error
      )
    );
  }
);


process.on(
  'uncaughtException',
  error => {

    console.error(
      'Uncaught exception:',
      safeError(
        error
      )
    );
  }
);


boot().catch(
  error => {

    console.error(
      'FATAL:',
      safeError(
        error
      )
    );


    process.exit(
      1
    );
  }
);
