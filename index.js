'use strict';

const axios = require('axios');
const express = require('express');
const mongoose = require('mongoose');
const { Telegraf } = require('telegraf');

// ============================================================
// LOMY FOREX V1.2.0 — SMC + BREAK-EVEN 1:1.2
// PAPER ONLY — NO REAL TRADING
// ============================================================

const VERSION = 'LOMY FOREX V1.2.0';
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

  // ==========================================
  // NEW MONEY MANAGEMENT
  // ==========================================

  riskReward: 1.20,

  breakEvenTriggerR: 0.60,

  // Prevent entering after price already ran away
  maxEntryMoveAtr: 0.50,

  // ==========================================
  // RETEST + FIB
  // ==========================================

  retestLookback: 8,

  fibLookback: 40,

  fibToleranceAtr: 0.20,

  // ==========================================
  // SMC SNIPER
  // ==========================================

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

  accountKey:
    'lomy-forex-v120-smc-be-12r'
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


for (
  const symbol
  of INSTRUMENTS
) {

  state.pairState.set(
    symbol,
    {

      bars: [],

      lastClosedBarTime: null,

      lastSignal: 0,

      lastAnalysis: null,

      initialized: false,

      errors: 0
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
    error?.response?.data?.status ||
    error?.message ||
    String(error)
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

  const tfMs =
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
        time > 0 &&
        time + tfMs <=
          now + 5000
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
    !Array.isArray(values) ||
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
    !Array.isArray(values) ||
    values.length <
      length
  ) {

    return [];
  }


  const multiplier =
    2 /
    (
      length +
      1
    );


  const output =
    new Array(
      values.length
    ).fill(
      NaN
    );


  const seed =
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


  output[
    length -
    1
  ] =
    seed;


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

function atrSeries(
  bars,
  length
) {

  if (
    bars.length <
      length +
      1
  ) {

    return [];
  }


  const trueRange =
    new Array(
      bars.length
    ).fill(
      NaN
    );


  for (
    let i = 1;
    i <
      bars.length;
    i++
  ) {

    const high =
      bars[i].high;

    const low =
      bars[i].low;

    const previousClose =
      bars[
        i -
        1
      ].close;


    trueRange[i] =
      Math.max(

        high -
          low,

        Math.abs(
          high -
          previousClose
        ),

        Math.abs(
          low -
          previousClose
        )
      );
  }


  const output =
    new Array(
      bars.length
    ).fill(
      NaN
    );


  const seedValues =
    trueRange.slice(
      1,
      length +
        1
    );


  if (
    !seedValues.every(
      Number.isFinite
    )
  ) {

    return output;
  }


  output[length] =
    seedValues.reduce(
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
      length +
      1;
    i <
      bars.length;
    i++
  ) {

    output[i] =
      (
        output[
          i -
          1
        ] *
          (
            length -
            1
          ) +
        trueRange[i]
      ) /
      length;
  }


  return output;
}


function atrLast(
  bars,
  length
) {

  const series =
    atrSeries(
      bars,
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
// CMO
// ============================================================

function cmo(
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


  let up = 0;

  let down = 0;


  for (
    let i =
      values.length -
      length;
    i <
      values.length;
    i++
  ) {

    const change =
      values[i] -
      values[
        i -
        1
      ];


    if (
      change >
      0
    ) {

      up +=
        change;

    } else if (
      change <
      0
    ) {

      down +=
        -change;
    }
  }


  const denominator =
    up +
    down;


  if (
    denominator ===
    0
  ) {

    return 0;
  }


  return (
    100 *
    (
      up -
      down
    ) /
    denominator
  );
}


// ============================================================
// DMI / ADX
// ============================================================

function dmiAdx(
  bars,
  diLength = 14,
  adxSmooth = 14
) {

  if (
    bars.length <
      diLength +
      adxSmooth +
      2
  ) {

    return {

      plusDI: NaN,

      minusDI: NaN,

      adx: NaN
    };
  }


  const trueRanges = [];

  const plusDMs = [];

  const minusDMs = [];


  for (
    let i = 1;
    i <
      bars.length;
    i++
  ) {

    const upMove =
      bars[i].high -
      bars[
        i -
        1
      ].high;


    const downMove =
      bars[
        i -
        1
      ].low -
      bars[i].low;


    plusDMs.push(

      upMove >
        downMove &&
      upMove >
        0

        ? upMove
        : 0
    );


    minusDMs.push(

      downMove >
        upMove &&
      downMove >
        0

        ? downMove
        : 0
    );


    trueRanges.push(

      Math.max(

        bars[i].high -
          bars[i].low,

        Math.abs(
          bars[i].high -
          bars[
            i -
            1
          ].close
        ),

        Math.abs(
          bars[i].low -
          bars[
            i -
            1
          ].close
        )
      )
    );
  }


  let trSmoothed =
    trueRanges
      .slice(
        0,
        diLength
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


  let plusSmoothed =
    plusDMs
      .slice(
        0,
        diLength
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


  let minusSmoothed =
    minusDMs
      .slice(
        0,
        diLength
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


  const dxValues = [];


  let lastPlusDI =
    NaN;

  let lastMinusDI =
    NaN;


  for (
    let i =
      diLength;
    i <
      trueRanges.length;
    i++
  ) {

    if (
      i >
      diLength
    ) {

      trSmoothed =
        trSmoothed -
        trSmoothed /
          diLength +
        trueRanges[i];


      plusSmoothed =
        plusSmoothed -
        plusSmoothed /
          diLength +
        plusDMs[i];


      minusSmoothed =
        minusSmoothed -
        minusSmoothed /
          diLength +
        minusDMs[i];
    }


    lastPlusDI =
      trSmoothed >
      0

        ? 100 *
          plusSmoothed /
          trSmoothed

        : 0;


    lastMinusDI =
      trSmoothed >
      0

        ? 100 *
          minusSmoothed /
          trSmoothed

        : 0;


    const denominator =
      lastPlusDI +
      lastMinusDI;


    dxValues.push(

      denominator >
      0

        ? 100 *
          Math.abs(
            lastPlusDI -
            lastMinusDI
          ) /
          denominator

        : 0
    );
  }


  if (
    dxValues.length <
      adxSmooth
  ) {

    return {

      plusDI:
        lastPlusDI,

      minusDI:
        lastMinusDI,

      adx:
        NaN
    };
  }


  let adx =
    dxValues
      .slice(
        0,
        adxSmooth
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
    adxSmooth;


  for (
    let i =
      adxSmooth;
    i <
      dxValues.length;
    i++
  ) {

    adx =
      (
        adx *
          (
            adxSmooth -
            1
          ) +
        dxValues[i]
      ) /
      adxSmooth;
  }


  return {

    plusDI:
      lastPlusDI,

    minusDI:
      lastMinusDI,

    adx
  };
}


// ============================================================
// HIGH / LOW
// ============================================================

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


// ============================================================
// CONFIRMED PIVOTS
// ============================================================

function confirmedPivots(
  bars,
  left = 5,
  right = 5
) {

  const lows = [];

  const highs = [];


  for (
    let i =
      left;
    i <
      bars.length -
        right;
    i++
  ) {

    const pivot =
      bars[i];


    let lowOk =
      true;

    let highOk =
      true;


    for (
      let j =
        i -
        left;
      j <=
        i +
        right;
      j++
    ) {

      if (
        j ===
        i
      ) {

        continue;
      }


      if (
        bars[j].low <=
        pivot.low
      ) {

        lowOk =
          false;
      }


      if (
        bars[j].high >=
        pivot.high
      ) {

        highOk =
          false;
      }


      if (
        !lowOk &&
        !highOk
      ) {

        break;
      }
    }


    if (
      lowOk
    ) {

      lows.push({

        index: i,

        price:
          pivot.low,

        time:
          pivot.openTime
      });
    }


    if (
      highOk
    ) {

      highs.push({

        index: i,

        price:
          pivot.high,

        time:
          pivot.openTime
      });
    }
  }


  return {

    lastLow:
      lows.length
        ? lows[
            lows.length -
            1
          ]
        : null,

    lastHigh:
      highs.length
        ? highs[
            highs.length -
            1
          ]
        : null
  };
}


// ============================================================
// FIBONACCI CONTEXT
// ============================================================

function fibContext(
  priorBars,
  signalBar,
  atr,
  direction
) {

  const window =
    priorBars.slice(
      -STRATEGY.fibLookback
    );


  if (
    window.length <
      10 ||
    !Number.isFinite(
      atr
    ) ||
    atr <=
      0
  ) {

    return {

      score: 0,

      nearFib: false,

      goldenZone: false,

      levels: null
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
    range <=
    0
  ) {

    return {

      score: 0,

      nearFib: false,

      goldenZone: false,

      levels: null
    };
  }


  let levels;

  let goldenLow;

  let goldenHigh;


  if (
    direction ===
    'BUY'
  ) {

    levels = {

      f382:
        high -
        range *
          0.382,

      f500:
        high -
        range *
          0.500,

      f618:
        high -
        range *
          0.618,

      f786:
        high -
        range *
          0.786,

      ext1272:
        high +
        range *
          0.272,

      ext1618:
        high +
        range *
          0.618
    };

  } else {

    levels = {

      f382:
        low +
        range *
          0.382,

      f500:
        low +
        range *
          0.500,

      f618:
        low +
        range *
          0.618,

      f786:
        low +
        range *
          0.786,

      ext1272:
        low -
        range *
          0.272,

      ext1618:
        low -
        range *
          0.618
    };
  }


  goldenLow =
    Math.min(
      levels.f500,
      levels.f618
    );


  goldenHigh =
    Math.max(
      levels.f500,
      levels.f618
    );


  const tolerance =
    atr *
    STRATEGY.fibToleranceAtr;


  const nearFib =
    [
      levels.f382,
      levels.f500,
      levels.f618,
      levels.f786
    ].some(
      level =>
        Math.abs(
          signalBar.close -
          level
        ) <=
        tolerance
    );


  const goldenZone =
    signalBar.low <=
      goldenHigh +
        tolerance &&
    signalBar.high >=
      goldenLow -
        tolerance;


  return {

    score:
      (
        goldenZone
          ? 2
          : 0
      ) +
      (
        nearFib
          ? 1
          : 0
      ),

    nearFib,

    goldenZone,

    levels
  };
}


// ============================================================
// RETEST
// ============================================================

function retestContext(
  priorBars,
  signalBar,
  direction
) {

  const window =
    priorBars.slice(
      -STRATEGY.retestLookback
    );


  if (
    window.length <
      3
  ) {

    return {

      breakout: false,

      retest: false,

      level: NaN,

      score: 0
    };
  }


  if (
    direction ===
    'BUY'
  ) {

    const level =
      highestHigh(
        window
      );


    const breakout =
      signalBar.high >
        level &&
      signalBar.close >
        level;


    const retest =
      breakout &&
      signalBar.low <=
        level &&
      signalBar.close >
        level;


    return {

      breakout,

      retest,

      level,

      score:
        (
          breakout
            ? 1
            : 0
        ) +
        (
          retest
            ? 1
            : 0
        )
    };
  }


  const level =
    lowestLow(
      window
    );


  const breakout =
    signalBar.low <
      level &&
    signalBar.close <
      level;


  const retest =
    breakout &&
    signalBar.high >=
      level &&
    signalBar.close <
      level;


  return {

    breakout,

    retest,

    level,

    score:
      (
        breakout
          ? 1
          : 0
      ) +
      (
        retest
          ? 1
          : 0
      )
  };
}


// ============================================================
// SMC SNIPER ENGINE
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
    emaLast(
      closes,
      STRATEGY.smcSlowEmaLen
    );


  const {

    plusDI,

    minusDI,

    adx

  } =
    dmiAdx(

      bars,

      STRATEGY.adxLen,

      STRATEGY.adxSmooth
    );


  const pivots =
    confirmedPivots(

      bars,

      STRATEGY.pivotLeft,

      STRATEGY.pivotRight
    );


  const bullishTrend =
    Number.isFinite(
      ema50
    ) &&
    Number.isFinite(
      ema200
    ) &&
    ema50 >
      ema200;


  const bearishTrend =
    Number.isFinite(
      ema50
    ) &&
    Number.isFinite(
      ema200
    ) &&
    ema50 <
      ema200;


  const strongTrend =
    Number.isFinite(
      adx
    ) &&
    adx >
      STRATEGY.adxThreshold;


  // ==========================================
  // LIQUIDITY SWEEP
  // ==========================================

  const bullishSweep =
    Boolean(

      pivots.lastLow &&

      signal.low <
        pivots.lastLow.price &&

      signal.close >
        pivots.lastLow.price &&

      signal.close >
        signal.open
    );


  const bearishSweep =
    Boolean(

      pivots.lastHigh &&

      signal.high >
        pivots.lastHigh.price &&

      signal.close <
        pivots.lastHigh.price &&

      signal.close <
        signal.open
    );


  // ==========================================
  // FVG
  // ==========================================

  const bar2 =
    bars[
      bars.length -
      3
    ];


  const bar1 =
    bars[
      bars.length -
      2
    ];


  const bullishFvg =
    Boolean(

      bar2 &&
      bar1 &&

      signal.low >
        bar2.high &&

      bar1.close >
        bar1.open
    );


  const bearishFvg =
    Boolean(

      bar2 &&
      bar1 &&

      signal.high <
        bar2.low &&

      bar1.close <
        bar1.open
    );


  let score = 0;

  const reasons = [];


  if (
    direction ===
    'BUY'
  ) {

    if (
      bullishTrend
    ) {

      score +=
        1;

      reasons.push(
        'EMA50>EMA200'
      );
    }


    if (
      strongTrend &&
      plusDI >
        minusDI
    ) {

      score +=
        1;

      reasons.push(
        'ADX_TREND'
      );
    }


    if (
      bullishSweep
    ) {

      score +=
        3;

      reasons.push(
        'LIQUIDITY_SWEEP'
      );
    }


    if (
      bullishFvg
    ) {

      score +=
        1;

      reasons.push(
        'BULL_FVG'
      );
    }

  } else {

    if (
      bearishTrend
    ) {

      score +=
        1;

      reasons.push(
        'EMA50<EMA200'
      );
    }


    if (
      strongTrend &&
      minusDI >
        plusDI
    ) {

      score +=
        1;

      reasons.push(
        'ADX_TREND'
      );
    }


    if (
      bearishSweep
    ) {

      score +=
        3;

      reasons.push(
        'LIQUIDITY_SWEEP'
      );
    }


    if (
      bearishFvg
    ) {

      score +=
        1;

      reasons.push(
        'BEAR_FVG'
      );
    }
  }


  return {

    score,

    reasons,

    ema50,

    ema200,

    adx,

    plusDI,

    minusDI,

    bullishSweep,

    bearishSweep,

    bullishFvg,

    bearishFvg,

    lastPivotLow:
      pivots.lastLow?.price ??
      NaN,

    lastPivotHigh:
      pivots.lastHigh?.price ??
      NaN
  };
}


// ============================================================
// MAIN SIGNAL ANALYSIS
// ============================================================

function analyzeBars(
  symbol,
  bars,
  pairMemory
) {

  const minimumHistory =
    230;


  if (
    bars.length <
      minimumHistory
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


  const volumes =
    bars.map(
      bar =>
        bar.volume
    );


  // ==========================================
  // V5.1
  // ==========================================

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
    cmo(
      closes,
      STRATEGY.cmoLen
    );


  const volumeAverage =
    sma(

      volumes.slice(
        0,
        -1
      ),

      STRATEGY.volLen
    );


  const atr =
    atrLast(
      bars,
      STRATEGY.atrLen
    );


  const range =
    signal.high -
    signal.low;


  const body =
    Math.abs(
      signal.close -
      signal.open
    );


  const bodyRatio =
    range >
    0

      ? body /
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
    Number.isFinite(
      volumeAverage
    ) &&
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
    pairMemory.lastSignal !==
      1
  ) {

    direction =
      'BUY';
  }


  if (
    rawSell &&
    pairMemory.lastSignal !==
      -1
  ) {

    direction =
      'SELL';
  }


  if (
    direction ===
    'BUY'
  ) {

    pairMemory.lastSignal =
      1;
  }


  if (
    direction ===
    'SELL'
  ) {

    pairMemory.lastSignal =
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

    volume:
      signal.volume,

    volumeAvg:
      volumeAverage,

    volumeRatio:
      volumeAverage >
      0

        ? signal.volume /
          volumeAverage

        : NaN,

    bodyRatio,

    atr,

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


  // ==========================================
  // ORIGINAL SUPPORT / RESISTANCE
  // ==========================================

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


  // ==========================================
  // NEW CONFLUENCE
  // ==========================================

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


  let qualityScore =
    retest.score +
    fib.score +
    smc.score;


  qualityScore =
    clamp(
      qualityScore,
      0,
      12
    );


  let quality =
    'NEUTRAL';


  if (
    qualityScore >=
      7
  ) {

    quality =
      'STRONG';

  } else if (
    qualityScore >=
      4
  ) {

    quality =
      'GOOD';
  }


  return {

    ...common,

    direction,

    support,

    resistance,

    retest,

    fib,

    smc,

    qualityScore,

    quality
  };
}


// ============================================================
// HTTP CLIENT
// ============================================================

const http =
  axios.create({

    timeout: 12000,

    headers: {

      'User-Agent':
        'LOMY-Forex-Paper/1.2'
    }
  });


// ============================================================
// BIQUOTE OHLC
// ============================================================

async function fetchOhlc(
  symbol
) {

  const url =
    `${BIQUOTE_BASE}/api/${encodeURIComponent(symbol)}/ohlc`;


  const response =
    await http.get(
      url,
      {

        params: {

          interval:
            TIMEFRAME,

          limit:
            HISTORY_LIMIT
        }
      }
    );


  const raw =
    Array.isArray(
      response.data
    )

      ? response.data

      : (
          response.data?.data ||
          response.data?.ohlc ||
          response.data?.candles ||
          []
        );


  return closedBarsOnly(
    normalizeBars(
      raw
    )
  );
}


// ============================================================
// QUOTES
// ============================================================

function normalizeQuoteItem(
  item,
  fallbackSymbol = ''
) {

  if (
    !item ||
    typeof item !==
      'object'
  ) {

    return null;
  }


  const symbol =
    String(

      item.symbol ||
      item.instrument ||
      fallbackSymbol ||
      ''

    ).toUpperCase();


  const bid =
    n(
      item.bid
    );


  const ask =
    n(
      item.ask
    );


  const mid =
    n(

      item.mid,

      Number.isFinite(bid) &&
      Number.isFinite(ask)

        ? (
            bid +
            ask
          ) /
          2

        : NaN
    );


  const timestamp =
    item.timestamp ||
    item.datetime ||
    item.time ||
    null;


  if (
    !symbol ||
    !Number.isFinite(bid) ||
    !Number.isFinite(ask) ||
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

    timestamp
  };
}


function parseLatestResponse(
  data,
  requestedSymbols
) {

  const map =
    new Map();


  if (
    Array.isArray(
      data
    )
  ) {

    for (
      const item
      of data
    ) {

      const quote =
        normalizeQuoteItem(
          item
        );


      if (
        quote
      ) {

        map.set(
          quote.symbol,
          quote
        );
      }
    }


    return map;
  }


  const array =
    data?.data ||
    data?.quotes ||
    data?.results;


  if (
    Array.isArray(
      array
    )
  ) {

    for (
      const item
      of array
    ) {

      const quote =
        normalizeQuoteItem(
          item
        );


      if (
        quote
      ) {

        map.set(
          quote.symbol,
          quote
        );
      }
    }


    return map;
  }


  if (
    data &&
    typeof data ===
      'object'
  ) {

    for (
      const symbol
      of requestedSymbols
    ) {

      const item =
        data[symbol] ||
        data[
          symbol.toLowerCase()
        ] ||
        data?.data?.[symbol];


      const quote =
        normalizeQuoteItem(
          item,
          symbol
        );


      if (
        quote
      ) {

        map.set(
          symbol,
          quote
        );
      }
    }


    if (
      map.size ===
        0 &&
      requestedSymbols.length ===
        1
    ) {

      const quote =
        normalizeQuoteItem(

          data,

          requestedSymbols[0]
        );


      if (
        quote
      ) {

        map.set(
          requestedSymbols[0],
          quote
        );
      }
    }
  }


  return map;
}


async function fetchLatestQuotes(
  symbols
) {

  if (
    !symbols.length
  ) {

    return new Map();
  }


  const query =
    symbols
      .map(
        symbol =>
          `symbols=${encodeURIComponent(symbol)}`
      )
      .join(
        '&'
      );


  const url =
    `${BIQUOTE_BASE}/api/latest?${query}&allowStale=false`;


  const response =
    await http.get(
      url
    );


  return parseLatestResponse(

    response.data,

    symbols
  );
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


    return normalizeQuoteItem(

      response.data?.data ||
      response.data,

      symbol
    );

  } catch (
    error
  ) {

    const quotes =
      await fetchLatestQuotes(
        [
          symbol
        ]
      );


    return (
      quotes.get(
        symbol
      ) ||
      null
    );
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


  let cursor = 0;


  async function run() {

    while (
      true
    ) {

      const index =
        cursor++;


      if (
        index >=
          items.length
      ) {

        return;
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

  }, {

    minimize:
      false
  });


const tradeSchema =
  new mongoose.Schema({

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

    openedAt:
      Date,

    closedAt:
      Date,

    signalBarTime:
      String,

    signalPrice:
      Number,

    entryPrice:
      Number,

    originalStopLoss:
      Number,

    stopLoss:
      Number,

    takeProfit:
      Number,

    initialRiskDistance:
      Number,

    riskUsd:
      Number,

    units:
      Number,

    beTriggerPrice:
      Number,

    breakEvenActive:
      Boolean,

    breakEvenActivatedAt:
      Date,

    exitPrice:
      Number,

    exitReason:
      String,

    pnlUsd:
      Number,

    pnlR:
      Number,

    quality:
      String,

    qualityScore:
      Number,

    analysis:
      mongoose.Schema.Types.Mixed

  }, {

    minimize:
      false
  });


const signalSchema =
  new mongoose.Schema({

    accountKey: {

      type: String,

      index: true
    },

    createdAt:
      Date,

    symbol:
      String,

    direction:
      String,

    timeframe:
      String,

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
  mongoose.models.LomyForexAccountV120 ||
  mongoose.model(
    'LomyForexAccountV120',
    accountSchema
  );


const Trade =
  mongoose.models.LomyForexTradeV120 ||
  mongoose.model(
    'LomyForexTradeV120',
    tradeSchema
  );


const Signal =
  mongoose.models.LomyForexSignalV120 ||
  mongoose.model(
    'LomyForexSignalV120',
    signalSchema
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

    state.openTrades.set(

      trade.symbol,

      trade
    );
  }


  console.log(

    `♻️ Restored ${openTrades.length} open PAPER trade(s)`
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

      text,

      {

        disable_web_page_preview:
          true
      }
    );

  } catch (
    error
  ) {

    console.error(

      'Telegram send error:',

      safeError(
        error
      )
    );
  }
}


async function rememberChat(
  ctx
) {

  const id =
    String(
      ctx.chat?.id ||
      ''
    );


  if (
    !id ||
    !account
  ) {

    return;
  }


  if (
    account.telegramChatId !==
      id
  ) {

    account.telegramChatId =
      id;


    await saveAccount();
  }
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

      await rememberChat(
        ctx
      );


      await ctx.reply(

        `🤖 ${VERSION}\n` +

        `🧪 PAPER ONLY\n` +

        `⏱ Timeframe: ${TIMEFRAME}\n` +

        `🎯 TP: 1:${STRATEGY.riskReward.toFixed(1)}R\n` +

        `🛡 Break-even: +${STRATEGY.breakEvenTriggerR.toFixed(1)}R → SL to Entry\n` +

        `🧠 V5.1 + SMC + FVG + Liquidity Sweep + Retest + Fibonacci`
      );
    }
  );


  bot.command(
    'status',
    async ctx => {

      await rememberChat(
        ctx
      );


      await ctx.reply(

        `📡 ${VERSION}\n\n` +

        `Market: ${state.marketReady ? 'READY ✅' : 'WAIT ⏳'}\n` +

        `MongoDB: ${state.mongoReady ? 'OK ✅' : 'NO ❌'}\n` +

        `Telegram: ${state.telegramReady ? 'OK ✅' : 'NO ❌'}\n` +

        `Instruments: ${INSTRUMENTS.length}\n` +

        `Open: ${state.openTrades.size}\n` +

        `Scans: ${state.totalSignalScans}\n` +

        `Signals: ${state.rawSignals}\n` +

        `Executed: ${state.executedSignals}\n` +

        `BE moves: ${state.breakEvenMoves}`
      );
    }
  );


  bot.command(
    'balance',
    async ctx => {

      await rememberChat(
        ctx
      );


      await ctx.reply(

        `🧪 PAPER ACCOUNT\n\n` +

        `Starting: ${fmtMoney(account?.startingBalance ?? PAPER.startingBalance)}\n` +

        `Balance: ${fmtMoney(account?.balance ?? PAPER.startingBalance)}\n` +

        `Realized PnL: ${fmtMoney(account?.realizedPnl ?? 0)}\n` +

        `Open Trades: ${state.openTrades.size}\n\n` +

        `🔒 NO REAL MONEY USED`
      );
    }
  );


  bot.command(
    'stats',
    async ctx => {

      await rememberChat(
        ctx
      );


      const total =
        account?.totalTrades ??
        0;


      const wins =
        account?.wins ??
        0;


      const losses =
        account?.losses ??
        0;


      const breakeven =
        account?.breakeven ??
        0;


      const winRate =
        total >
        0

          ? 100 *
            wins /
            total

          : 0;


      await ctx.reply(

        `📊 ${VERSION} STATS\n\n` +

        `Timeframe: ${TIMEFRAME}\n` +

        `Closed: ${total}\n` +

        `Wins: ${wins}\n` +

        `Losses: ${losses}\n` +

        `Breakeven: ${breakeven}\n` +

        `Win Rate: ${winRate.toFixed(2)}%\n` +

        `Realized PnL: ${fmtMoney(account?.realizedPnl ?? 0)}\n` +

        `Balance: ${fmtMoney(account?.balance ?? PAPER.startingBalance)}`
      );
    }
  );


  bot.command(
    'positions',
    async ctx => {

      await rememberChat(
        ctx
      );


      if (
        !state.openTrades.size
      ) {

        return ctx.reply(
          '📭 No open PAPER trades.'
        );
      }


      const lines = [

        '📂 OPEN PAPER TRADES',

        ''
      ];


      for (
        const trade
        of state.openTrades.values()
      ) {

        lines.push(

          `${trade.direction === 'BUY' ? '🟢' : '🔴'} ${trade.symbol} ${trade.direction}`,

          `Entry ${fmtPrice(trade.entryPrice, trade.symbol)} | SL ${fmtPrice(trade.stopLoss, trade.symbol)} | TP ${fmtPrice(trade.takeProfit, trade.symbol)}`,

          `BE ${trade.breakEvenActive ? 'ACTIVE ✅' : `at +${STRATEGY.breakEvenTriggerR.toFixed(1)}R`}`,

          `Quality ${trade.quality || 'NEUTRAL'} (${trade.qualityScore ?? 0})`,

          ''
        );
      }


      await ctx.reply(

        lines
          .join(
            '\n'
          )
          .slice(
            0,
            3900
          )
      );
    }
  );


  bot.command(
    'trades',
    async ctx => {

      await rememberChat(
        ctx
      );


      const trades =
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
        !trades.length
      ) {

        return ctx.reply(
          '📭 No closed PAPER trades yet.'
        );
      }


      const lines = [

        '🧾 LAST CLOSED TRADES',

        ''
      ];


      for (
        const trade
        of trades
      ) {

        const icon =
          trade.pnlUsd >
            0

            ? '✅'

            : trade.pnlUsd <
                0

              ? '❌'

              : '➖';


        lines.push(

          `${icon} ${trade.symbol} ${trade.direction} | ${trade.exitReason}`,

          `PnL ${fmtMoney(trade.pnlUsd)} | ${n(trade.pnlR, 0).toFixed(2)}R`,

          ''
        );
      }


      await ctx.reply(
        lines.join(
          '\n'
        )
      );
    }
  );


  bot.command(
    'pairs',
    async ctx => {

      await rememberChat(
        ctx
      );


      await ctx.reply(

        `📋 ${INSTRUMENTS.length} instruments\n` +

        INSTRUMENTS.join(
          ', '
        )
      );
    }
  );


  const me =
    await bot.telegram.getMe();


  state.telegramReady =
    true;


  console.log(

    `✅ Telegram authenticated: @${me.username}`
  );


  bot.launch({

    dropPendingUpdates:
      true

  }).then(
    () => {

      console.log(
        '✅ Telegram polling started'
      );
    }
  ).catch(
    error => {

      state.telegramReady =
        false;


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

      accountKey:
        PAPER.accountKey,

      createdAt:
        new Date(),

      symbol:
        analysis.symbol,

      direction:
        analysis.direction,

      timeframe:
        TIMEFRAME,

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


  // BUY enters ASK
  // SELL enters BID

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
      analysis.support -
      atr *
        STRATEGY.atrMargin;


    if (
      !Number.isFinite(
        stopLoss
      ) ||
      stopLoss >=
        entry
    ) {

      stopLoss =
        analysis.signalClose -
        atr;
    }


    if (
      !Number.isFinite(
        stopLoss
      ) ||
      stopLoss >=
        entry
    ) {

      stopLoss =
        entry -
        atr;
    }

  } else {

    stopLoss =
      analysis.resistance +
      atr *
        STRATEGY.atrMargin;


    if (
      !Number.isFinite(
        stopLoss
      ) ||
      stopLoss <=
        entry
    ) {

      stopLoss =
        analysis.signalClose +
        atr;
    }


    if (
      !Number.isFinite(
        stopLoss
      ) ||
      stopLoss <=
        entry
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
    riskDistance <=
      0
  ) {

    return null;
  }


  // ==========================================
  // NEW TP = 1.2R
  // ==========================================

  const takeProfit =
    direction ===
    'BUY'

      ? entry +
        riskDistance *
          STRATEGY.riskReward

      : entry -
        riskDistance *
          STRATEGY.riskReward;


  // ==========================================
  // NEW BREAK-EVEN TRIGGER = +0.6R
  // ==========================================

  const beTriggerPrice =
    direction ===
    'BUY'

      ? entry +
        riskDistance *
          STRATEGY.breakEvenTriggerR

      : entry -
        riskDistance *
          STRATEGY.breakEvenTriggerR;


  const geometryOk =
    direction ===
    'BUY'

      ? (
          stopLoss <
            entry &&
          entry <
            takeProfit
        )

      : (
          takeProfit <
            entry &&
          entry <
            stopLoss
        );


  if (
    !geometryOk
  ) {

    return null;
  }


  return {

    entry,

    stopLoss,

    takeProfit,

    riskDistance,

    beTriggerPrice
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

      'ALREADY_OPEN'
    );


    return;
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


    return;
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


  // Anti-chase only
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

      'ANTI_CHASE'
    );


    return;
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


    return;
  }


  const balance =
    n(

      account?.balance,

      PAPER.startingBalance
    );


  const riskUsd =
    balance *
    (
      PAPER.riskPctPerTrade /
      100
    );


  const units =
    riskUsd /
    levels.riskDistance;


  const tradeDocument =
    await Trade.create({

      accountKey:
        PAPER.accountKey,

      symbol,

      direction,

      status:
        'OPEN',

      timeframe:
        TIMEFRAME,

      openedAt:
        new Date(),

      signalBarTime:
        String(
          analysis.signalBarTime ||
          ''
        ),

      signalPrice:
        analysis.signalClose,

      entryPrice:
        levels.entry,

      originalStopLoss:
        levels.stopLoss,

      stopLoss:
        levels.stopLoss,

      takeProfit:
        levels.takeProfit,

      initialRiskDistance:
        levels.riskDistance,

      riskUsd,

      units,

      beTriggerPrice:
        levels.beTriggerPrice,

      breakEvenActive:
        false,

      breakEvenActivatedAt:
        null,

      quality:
        analysis.quality,

      qualityScore:
        analysis.qualityScore,

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

    `BE trigger: ${fmtPrice(levels.beTriggerPrice, symbol)} (+${STRATEGY.breakEvenTriggerR.toFixed(1)}R)\n` +

    `Quality: ${analysis.quality} ${analysis.qualityScore}/12\n` +

    `SMC: ${analysis.smc?.reasons?.join(', ') || 'none'}\n` +

    `Retest: ${analysis.retest?.retest ? 'YES' : 'NO'} | ` +

    `Fib Golden: ${analysis.fib?.goldenZone ? 'YES' : 'NO'}\n\n` +

    `🔒 PAPER ONLY`
  );
}


// ============================================================
// ACTIVATE BREAK-EVEN
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


  trade.breakEvenActivatedAt =
    new Date();


  // ==========================================
  // MOVE STOP LOSS TO EXACT ENTRY
  // ==========================================

  trade.stopLoss =
    trade.entryPrice;


  state.openTrades.set(

    symbol,

    trade
  );


  state.breakEvenMoves++;


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

        breakEvenActivatedAt:
          trade.breakEvenActivatedAt,

        stopLoss:
          trade.entryPrice
      }
    }
  );


  await sendTelegram(

    `🛡 BREAK-EVEN ACTIVATED\n` +

    `${symbol} ${trade.direction}\n` +

    `SL moved to Entry: ${fmtPrice(trade.entryPrice, symbol)}\n` +

    `TP remains: ${fmtPrice(trade.takeProfit, symbol)}\n` +

    `Trigger: +${STRATEGY.breakEvenTriggerR.toFixed(1)}R`
  );
}


// ============================================================
// CLOSE PAPER TRADE
// ============================================================

async function closePaperTrade(
  symbol,
  exitPrice,
  reason
) {

  const trade =
    state.openTrades.get(
      symbol
    );


  if (
    !trade
  ) {

    return;
  }


  const directionSign =
    trade.direction ===
    'BUY'

      ? 1

      : -1;


  const pnlPerUnit =
    (
      exitPrice -
      trade.entryPrice
    ) *
    directionSign;


  const pnlUsd =
    pnlPerUnit *
    trade.units;


  const pnlR =
    trade.riskUsd >
    0

      ? pnlUsd /
        trade.riskUsd

      : 0;


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

        closedAt:
          new Date(),

        exitPrice,

        exitReason:
          reason,

        pnlUsd,

        pnlR,

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


  account.balance =
    n(
      account.balance,
      PAPER.startingBalance
    ) +
    pnlUsd;


  account.realizedPnl =
    n(
      account.realizedPnl,
      0
    ) +
    pnlUsd;


  account.totalTrades =
    n(
      account.totalTrades,
      0
    ) +
    1;


  // small tolerance around 0R
  const breakevenTolerance =
    Math.max(

      0.01,

      trade.riskUsd *
        0.03
    );


  if (
    pnlUsd >
      breakevenTolerance
  ) {

    account.wins =
      n(
        account.wins,
        0
      ) +
      1;

  } else if (
    pnlUsd <
      -breakevenTolerance
  ) {

    account.losses =
      n(
        account.losses,
        0
      ) +
      1;

  } else {

    account.breakeven =
      n(
        account.breakeven,
        0
      ) +
      1;
  }


  await saveAccount();


  const icon =
    pnlUsd >
      breakevenTolerance

      ? '✅'

      : pnlUsd <
          -breakevenTolerance

        ? '❌'

        : '➖';


  await sendTelegram(

    `${icon} ${symbol} ${trade.direction} | ${reason}\n` +

    `Exit: ${fmtPrice(exitPrice, symbol)}\n` +

    `PnL ${fmtMoney(pnlUsd)} | ${pnlR.toFixed(2)}R\n` +

    (
      trade.breakEvenActive

        ? '🛡 Break-even had been activated'

        : 'SL remained original'
    )
  );
}


// ============================================================
// CURRENT R
// ============================================================

function unrealizedR(
  trade,
  quote
) {

  const mark =
    trade.direction ===
    'BUY'

      ? quote.bid

      : quote.ask;


  const sign =
    trade.direction ===
    'BUY'

      ? 1

      : -1;


  return (

    (
      mark -
      trade.entryPrice
    ) *
    sign

  ) /
  trade.initialRiskDistance;
}


// ============================================================
// FAST EXIT + BREAK-EVEN MONITOR
// ============================================================

async function quotePollLoop() {

  if (
    state.quoteRunning ||
    state.initializing ||
    state.openTrades.size ===
      0
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
      ]
      of quotes
    ) {

      state.latestQuotes.set(

        symbol,

        quote
      );
    }


    for (
      const [
        symbol,
        originalTrade
      ]
      of [
        ...state.openTrades.entries()
      ]
    ) {

      const quote =
        state.latestQuotes.get(
          symbol
        );


      if (
        !quote
      ) {

        continue;
      }


      let trade =
        state.openTrades.get(
          symbol
        ) ||
        originalTrade;


      // ==========================================
      // BREAK-EVEN TRIGGER
      // ==========================================

      if (
        !trade.breakEvenActive
      ) {

        const hitBreakEven =
          trade.direction ===
          'BUY'

            ? quote.bid >=
              trade.beTriggerPrice

            : quote.ask <=
              trade.beTriggerPrice;


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


      // ==========================================
      // BUY
      // ==========================================

      if (
        trade.direction ===
        'BUY'
      ) {

        // BUY exits using BID

        if (
          quote.bid <=
            trade.stopLoss
        ) {

          await closePaperTrade(

            symbol,

            quote.bid,

            trade.breakEvenActive

              ? 'BREAK_EVEN'

              : 'STOP_LOSS'
          );

        } else if (
          quote.bid >=
            trade.takeProfit
        ) {

          await closePaperTrade(

            symbol,

            quote.bid,

            'TAKE_PROFIT'
          );
        }

      // ==========================================
      // SELL
      // ==========================================

      } else {

        // SELL exits using ASK

        if (
          quote.ask >=
            trade.stopLoss
        ) {

          await closePaperTrade(

            symbol,

            quote.ask,

            trade.breakEvenActive

              ? 'BREAK_EVEN'

              : 'STOP_LOSS'
          );

        } else if (
          quote.ask <=
            trade.takeProfit
        ) {

          await closePaperTrade(

            symbol,

            quote.ask,

            'TAKE_PROFIT'
          );
        }
      }
    }

  } catch (
    error
  ) {

    state.lastMarketError =
      `quote: ${safeError(error)}`;


    console.error(

      'Quote loop:',

      state.lastMarketError
    );

  } finally {

    state.quoteRunning =
      false;
  }
}


// ============================================================
// REVERSE SIGNAL
// ============================================================

async function handleReverseSignal(
  analysis
) {

  const trade =
    state.openTrades.get(
      analysis.symbol
    );


  if (
    !trade ||
    !analysis.direction ||
    trade.direction ===
      analysis.direction
  ) {

    return false;
  }


  const quote =
    state.latestQuotes.get(
      analysis.symbol
    ) ||
    await fetchSingleQuote(
      analysis.symbol
    ).catch(
      () =>
        null
    );


  if (
    !quote
  ) {

    return false;
  }


  const currentR =
    unrealizedR(

      trade,

      quote
    );


  // ==========================================
  // IMPORTANT FIX:
  // Reverse signal will NOT close a losing trade.
  //
  // Prevents old situation where REVERSE_SIGNAL
  // crystallized -0.2R / -0.6R losses.
  // ==========================================

  if (
    currentR >=
      0 ||
    trade.breakEvenActive
  ) {

    const exitPrice =
      trade.direction ===
      'BUY'

        ? quote.bid

        : quote.ask;


    await closePaperTrade(

      analysis.symbol,

      exitPrice,

      'REVERSE_SIGNAL'
    );


    return true;
  }


  return false;
}


// ============================================================
// SCAN SLOT
// ============================================================

function currentScanSlot() {

  const date =
    new Date();


  const minute =
    date.getUTCMinutes();


  const second =
    date.getUTCSeconds();


  if (
    minute %
      15 !==
      0 ||
    second <
      4 ||
    second >
      50
  ) {

    return null;
  }


  return (

    `${date.getUTCFullYear()}-` +

    `${date.getUTCMonth() + 1}-` +

    `${date.getUTCDate()}-` +

    `${date.getUTCHours()}-` +

    `${minute}`
  );
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


    if (
      bars.length <
        230
    ) {

      memory.errors++;


      return {

        symbol,

        status:
          'WARMUP',

        bars:
          bars.length
      };
    }


    memory.bars =
      bars;


    memory.initialized =
      true;


    memory.errors =
      0;


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


    // Handle old opposite position
    await handleReverseSignal(
      analysis
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

        'OPPOSITE_SIGNAL_BUT_TRADE_STILL_OPEN'
      );


      return {

        symbol,

        status:
          'SKIP_OPEN'
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
// 15M SIGNAL SCANNER
// ============================================================

async function signalScanLoop() {

  if (
    state.scanRunning ||
    state.initializing ||
    !state.marketReady
  ) {

    return;
  }


  const slot =
    currentScanSlot();


  if (
    !slot ||
    state.lastScanSlot ===
      slot
  ) {

    return;
  }


  state.lastScanSlot =
    slot;


  state.scanRunning =
    true;


  state.lastSignalScanAt =
    new Date();


  state.totalSignalScans++;


  const scanNumber =
    state.totalSignalScans;


  console.log(

    `🔎 Scan #${scanNumber} | ${INSTRUMENTS.length} instruments | ${TIMEFRAME}`
  );


  try {

    const beforeRaw =
      state.rawSignals;


    const beforeExecuted =
      state.executedSignals;


    const beforeSkipped =
      state.skippedSignals;


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


  console.log(

    `⏳ Initializing ${INSTRUMENTS.length} instruments on ${TIMEFRAME}...`
  );


  const results =
    await mapWithConcurrency(

      INSTRUMENTS,

      OHLC_CONCURRENCY,

      async symbol => {

        const bars =
          await fetchOhlc(
            symbol
          );


        const memory =
          state.pairState.get(
            symbol
          );


        memory.bars =
          bars;


        memory.initialized =
          bars.length >=
            230;


        memory.lastClosedBarTime =
          bars[
            bars.length -
            1
          ]?.openTime ||
          null;


        return memory.initialized;
      }
    );


  const ready =
    results.filter(
      result =>
        result ===
        true
    ).length;


  state.marketReady =
    ready >=
    Math.max(

      1,

      Math.floor(

        INSTRUMENTS.length *
        0.80
      )
    );


  state.initializing =
    false;


  console.log(

    `✅ Market initialized: ${ready}/${INSTRUMENTS.length}`
  );


  console.log(

    state.marketReady

      ? '✅ Market engine READY'

      : '⚠️ Market engine PARTIAL'
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


// ============================================================
// HOME
// ============================================================

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

<style>
body{
font-family:Arial,sans-serif;
background:#111;
color:#eee;
padding:24px
}

.card{
max-width:820px;
margin:auto;
background:#1b1b1b;
padding:22px;
border-radius:14px
}

.ok{
color:#61d17c
}

.warn{
color:#ffcc66
}

code{
color:#8dd7ff
}
</style>
</head>

<body>

<div class="card">

<h2>${VERSION}</h2>

<p>
<b>Mode:</b>
PAPER ONLY — LIVE TRADING OFF
</p>

<p>
<b>Market:</b>
<span class="${state.marketReady ? 'ok' : 'warn'}">
${state.marketReady ? 'READY' : 'WAIT'}
</span>
</p>

<p>
<b>Timeframe:</b>
${TIMEFRAME} CLOSED candles
</p>

<p>
<b>Instruments:</b>
${INSTRUMENTS.length}
</p>

<p>
<b>Open trades:</b>
${state.openTrades.size}
</p>

<p>
<b>Paper balance:</b>
${fmtMoney(account?.balance ?? PAPER.startingBalance)}
</p>

<p>
<b>Core:</b>
Ultra-Fast V5.1 + SMC/Liquidity/FVG + Retest + Fibonacci
</p>

<p>
<b>Risk:</b>
${PAPER.riskPctPerTrade}% per trade
</p>

<p>
<b>Target:</b>
1:${STRATEGY.riskReward.toFixed(1)}R
</p>

<p>
<b>Break-even:</b>
at +${STRATEGY.breakEvenTriggerR.toFixed(1)}R → SL = Entry
</p>

<p>
<b>Provider:</b>
BiQuote primary
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

    quotePollLoop,

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

  } catch (
    error
  ) {}


  try {

    await mongoose.connection.close();

  } catch (
    error
  ) {}


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
