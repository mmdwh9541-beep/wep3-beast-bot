'use strict';

const axios = require('axios');
const express = require('express');
const mongoose = require('mongoose');
const { Telegraf } = require('telegraf');

// ============================================================
// LOMY FOREX V1.5 — GEMINI COMMANDER (PRO EDITION)
// Features Added: News Filter, Sessions, CSM, Trailing Stop, Dynamic Risk
// ============================================================

const VERSION = 'LOMY FOREX V1.5 GEMINI COMMANDER PRO';
const MODE = 'PAPER';
const LIVE_TRADING = false;

const PORT = Number(process.env.PORT || 10000);
const TELEGRAM_BOT_TOKEN = String(process.env.TELEGRAM_BOT_TOKEN || '').trim();
const MONGODB_URI = String(process.env.MONGODB_URI || '').trim();
const GEMINI_API_KEY = String(process.env.GEMINI_API_KEY || '').trim();

const GEMINI_MODEL = String(process.env.GEMINI_MODEL || 'gemini-2.5-flash').trim();
const BIQUOTE_BASE = String(process.env.BIQUOTE_BASE_URL || 'https://biquote.io').replace(/\/+$/, '');
const GEMINI_BASE = 'https://generativelanguage.googleapis.com/v1beta';

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
const GEMINI_MIN_CALL_GAP_MS = 850;
const JOURNAL_COLLECTION = 'lomyforexjournalv15';

// ============================================================
// INSTRUMENTS
// ============================================================

const INSTRUMENTS = [
  'EURUSD', 'GBPUSD', 'USDJPY', 'USDCHF', 'AUDUSD', 'NZDUSD', 'USDCAD',
  'EURGBP', 'EURJPY', 'EURCHF', 'EURAUD', 'EURNZD', 'EURCAD',
  'GBPJPY', 'GBPCHF', 'GBPAUD', 'GBPNZD', 'GBPCAD',
  'AUDJPY', 'AUDCHF', 'AUDNZD', 'AUDCAD',
  'NZDJPY', 'NZDCHF', 'NZDCAD',
  'CADJPY', 'CADCHF', 'CHFJPY',
  'GBPSGD', 'EURSGD', 'XAUUSD'
];

// ============================================================
// IMMUTABLE TRADING RULES & DYNAMIC RISK (Enhancement #5)
// ============================================================

const RULES = Object.freeze({
  riskReward: 2,
  breakEvenTriggerR: 0.60,
  partialTpTriggerR: 2.00, // الهدف الأول للإغلاق الجزئي (1:2)
  trailingStepR: 0.50,     // الوقف المتحرك بعد الهدف الأول
  minStopAtr: 0.25,
  maxStopAtr: 6.00,
  maxSpreadRiskFraction: 0.20,
  minEntryConfidence: 62,
  minCloseConfidence: 68
});

const PAPER = Object.freeze({
  startingBalance: 300,
  portfolioRiskCapPct: 4.00,
  maxOpenTrades: 31,
  accountKey: 'lomy-forex-v15-gemini-pro-300usd'
});

// Dynamic Risk Sizing based on AI Confidence
const DYNAMIC_RISK = Object.freeze({
  highConfidence: 85, highRiskPct: 1.00,
  medConfidence: 75,  medRiskPct: 0.75,
  lowConfidence: 62,  lowRiskPct: 0.50
});

// ============================================================
// TECHNICAL INTELLIGENCE CONFIG
// ============================================================

const TECH = Object.freeze({
  emaFast: 9, emaMedium: 21, emaTrend: 50, emaLong: 100, emaMacro: 200,
  rsiLen: 14, cmoLen: 9, atrLen: 14, adxLen: 14, stochasticLen: 14, stochasticSmooth: 3, rocLen: 12,
  bbLen: 20, bbStd: 2, keltnerLen: 20, keltnerAtrLen: 14, keltnerMult: 1.5,
  volumeLen: 20, srLen: 40, fibLookback: 60, structureLookback: 30, swingLeft: 3, swingRight: 3, liquidityLookback: 20, vwapLookback: 50, mfiLen: 14
});

const AI = Object.freeze({
  enabled: true, entryCommanderEnabled: true, managementEnabled: true,
  memoryClosedTrades: 40, temperature: 0.10, timeoutMs: 20000
});

// ============================================================
// STATE
// ============================================================

const state = {
  startedAt: new Date(), mongoReady: false, telegramReady: false, marketReady: false, geminiReady: false,
  initializing: true, scanRunning: false, quoteRunning: false, aiManageRunning: false,
  quoteLoopBusy: false, scanLoopBusy: false, loopsStarted: false,
  lastScanSlot: null, lastSignalScanAt: null, lastQuotePollAt: null, lastAiManageAt: null, lastMarketError: null, lastAiError: null,
  totalSignalScans: 0, totalQuotePolls: 0, scannedBars: 0,
  aiEntryCalls: 0, aiManageCalls: 0, aiBuyDecisions: 0, aiSellDecisions: 0, aiNoTradeDecisions: 0, aiCloseDecisions: 0, aiHoldDecisions: 0,
  executedSignals: 0, skippedSignals: 0, breakEvenMoves: 0, protectionRejects: 0, portfolioRiskRejects: 0, journalEvents: 0,
  pairState: new Map(), latestQuotes: new Map(), openTrades: new Map(), closingTrades: new Set(), aiLastManageByTrade: new Map(),
  scanLocks: new Set(), processedBars: new Set(), managementLocks: new Set(), lastManageAt: new Map(),
  geminiQueue: Promise.resolve(), geminiLastCallAt: 0,
  
  // News Data
  highImpactNews: []
};

for (const symbol of INSTRUMENTS) {
  state.pairState.set(symbol, { bars: [], lastClosedBarTime: null, lastAnalysis: null, initialized: false, errors: 0 });
}

// ============================================================
// BASIC HELPERS & INDICATORS (Compacted for brevity)
// ============================================================

function n(value, fallback = NaN) { const x = Number(value); return Number.isFinite(x) ? x : fallback; }
function safeError(error) { const data = error?.response?.data; return data?.error?.message || data?.message || data?.error || error?.message || String(error); }
function clamp(value, min, max) { return Math.max(min, Math.min(max, value)); }
function fmtMoney(value) { return '$' + n(value, 0).toFixed(2); }
function fmtPrice(value, symbol = '') { if (!Number.isFinite(Number(value))) return 'n/a'; value = Number(value); if (symbol === 'XAUUSD') return value.toFixed(2); if (symbol.endsWith('JPY')) return value.toFixed(3); return value.toFixed(5); }
function barTimeMs(bar) { const t = new Date(bar.openTime).getTime(); return Number.isFinite(t) ? t : 0; }
function sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
function average(values) { const clean = values.filter(Number.isFinite); return clean.length ? clean.reduce((a, b) => a + b, 0) / clean.length : NaN; }
function sum(values) { return values.filter(Number.isFinite).reduce((a, b) => a + b, 0); }
function highestHigh(bars) { return bars?.length ? Math.max(...bars.map(bar => bar.high)) : NaN; }
function lowestLow(bars) { return bars?.length ? Math.min(...bars.map(bar => bar.low)) : NaN; }

function normalizeBars(rawBars) {
  if (!Array.isArray(rawBars)) return [];
  return rawBars.map(bar => ({ openTime: bar.openTime || bar.datetime || bar.time || bar.timestamp, open: n(bar.open), high: n(bar.high), low: n(bar.low), close: n(bar.close), volume: n(bar.tickVolume, n(bar.volume, 0)), isOpen: bar.isOpen === true })).filter(bar => bar.openTime && [bar.open, bar.high, bar.low, bar.close].every(Number.isFinite)).sort((a, b) => barTimeMs(a) - barTimeMs(b));
}

function closedBarsOnly(bars, interval = TIMEFRAME) {
  const now = Date.now(), intervalMs = interval === '4h' ? 4 * 60 * 60 * 1000 : interval === '1h' ? 60 * 60 * 1000 : TIMEFRAME_MS;
  return bars.filter(bar => !bar.isOpen && barTimeMs(bar) > 0 && barTimeMs(bar) + intervalMs <= now + 5000);
}

// Indicator Functions
function emaSeries(values, length) {
  if (!Array.isArray(values) || values.length < length) return [];
  const out = new Array(values.length).fill(NaN), k = 2 / (length + 1), seed = values.slice(0, length);
  if (!seed.every(Number.isFinite)) return out; out[length - 1] = sum(seed) / length;
  for (let i = length; i < values.length; i++) { if (!Number.isFinite(values[i]) || !Number.isFinite(out[i - 1])) continue; out[i] = values[i] * k + out[i - 1] * (1 - k); }
  return out;
}
function emaLast(values, length) { const series = emaSeries(values, length); return series[series.length - 1]; }
function trueRangeSeries(bars) {
  if (!bars || bars.length < 2) return []; const output = [];
  for (let i = 1; i < bars.length; i++) { output.push(Math.max(bars[i].high - bars[i].low, Math.abs(bars[i].high - bars[i - 1].close), Math.abs(bars[i].low - bars[i - 1].close))); }
  return output;
}
function atrLast(bars, length = 14) {
  const ranges = trueRangeSeries(bars); if (ranges.length < length) return NaN;
  let atr = sum(ranges.slice(0, length)) / length;
  for (let i = length; i < ranges.length; i++) atr = (atr * (length - 1) + ranges[i]) / length;
  return atr;
}
function rsiLast(values, length = 14) {
  if (values.length < length + 1) return NaN;
  let gain = 0, loss = 0;
  for (let i = 1; i <= length; i++) {
    const change = values[i] - values[i - 1];
    if (change > 0) gain += change; else loss += Math.abs(change);
  }
  let avgGain = gain / length;
  let avgLoss = loss / length;
  for (let i = length + 1; i < values.length; i++) {
    const change = values[i] - values[i - 1];
    const currentGain = change > 0 ? change : 0;
    const currentLoss = change < 0 ? Math.abs(change) : 0;
    avgGain = (avgGain * (length - 1) + currentGain) / length;
    avgLoss = (avgLoss * (length - 1) + currentLoss) / length;
  }
  if (avgLoss === 0) return avgGain === 0 ? 50 : 100;
  return 100 - 100 / (1 + (avgGain / avgLoss));
}

function cmoLast(values, length) {
  if (values.length < length + 1) return NaN;
  let up = 0, down = 0;
  for (let i = values.length - length; i < values.length; i++) {
    const difference = values[i] - values[i - 1];
    if (difference > 0) up += difference; else down += Math.abs(difference);
  }
  const denominator = up + down;
  return denominator === 0 ? 0 : 100 * (up - down) / denominator;
}

function macdLast(values, fast = 12, slow = 26, signalLength = 9) {
  if (values.length < slow + signalLength) return { macd: NaN, signal: NaN, histogram: NaN };
  const fastSeries = emaSeries(values, fast);
  const slowSeries = emaSeries(values, slow);
  const macdValues = [];
  for (let i = 0; i < values.length; i++) {
    if (Number.isFinite(fastSeries[i]) && Number.isFinite(slowSeries[i])) {
      macdValues.push(fastSeries[i] - slowSeries[i]);
    }
  }
  if (macdValues.length < signalLength) return { macd: NaN, signal: NaN, histogram: NaN };
  const macd = macdValues[macdValues.length - 1];
  const signal = emaLast(macdValues, signalLength);
  return { macd, signal, histogram: Number.isFinite(signal) ? macd - signal : NaN };
}

function stochasticLast(bars, length = 14, smooth = 3) {
  if (bars.length < length + smooth - 1) return { k: NaN, d: NaN };
  const kValues = [];
  for (let end = bars.length - smooth; end < bars.length; end++) {
    const start = end - length + 1;
    if (start < 0) continue;
    const window = bars.slice(start, end + 1);
    const high = highestHigh(window), low = lowestLow(window), close = bars[end].close, range = high - low;
    kValues.push(range > 0 ? ((close - low) / range) * 100 : 50);
  }
  if (!kValues.length) return { k: NaN, d: NaN };
  return { k: kValues[kValues.length - 1], d: average(kValues) };
}

function williamsRLast(bars, length = 14) {
  if (bars.length < length) return NaN;
  const window = bars.slice(-length);
  const high = highestHigh(window), low = lowestLow(window), close = bars[bars.length - 1].close, range = high - low;
  if (!(range > 0)) return -50;
  return -100 * (high - close) / range;
}

function rocLast(values, length = 12) {
  if (values.length < length + 1) return NaN;
  const current = values[values.length - 1], previous = values[values.length - 1 - length];
  return pctChange(previous, current);
}

function bollingerLast(values, length = 20, multiplier = 2) {
  if (values.length < length) return { middle: NaN, upper: NaN, lower: NaN, widthPct: NaN, position: NaN };
  const window = values.slice(-length);
  const middle = average(window), deviation = standardDeviation(window);
  const upper = middle + deviation * multiplier, lower = middle - deviation * multiplier, current = values[values.length - 1], width = upper - lower;
  return { middle, upper, lower, widthPct: middle !== 0 ? (width / middle) * 100 : NaN, position: width > 0 ? (current - lower) / width : 0.5 };
}

function keltnerLast(bars, length = 20, atrLength = 14, multiplier = 1.5) {
  if (bars.length < Math.max(length, atrLength) + 1) return { middle: NaN, upper: NaN, lower: NaN, position: NaN };
  const closes = bars.map(bar => bar.close), middle = emaLast(closes, length), atr = atrLast(bars, atrLength);
  if (!Number.isFinite(middle) || !Number.isFinite(atr)) return { middle: NaN, upper: NaN, lower: NaN, position: NaN };
  const upper = middle + atr * multiplier, lower = middle - atr * multiplier, current = closes[closes.length - 1], width = upper - lower;
  return { middle, upper, lower, position: width > 0 ? (current - lower) / width : 0.5 };
}

function dmiAdx(bars, length = 14) {
  if (bars.length < length * 2 + 2) return { adx: NaN, plusDI: NaN, minusDI: NaN };
  const tr = [], plusDM = [], minusDM = [];
  for (let i = 1; i < bars.length; i++) {
    const current = bars[i], previous = bars[i - 1];
    const upMove = current.high - previous.high, downMove = previous.low - current.low;
    plusDM.push(upMove > downMove && upMove > 0 ? upMove : 0);
    minusDM.push(downMove > upMove && downMove > 0 ? downMove : 0);
    tr.push(Math.max(current.high - current.low, Math.abs(current.high - previous.close), Math.abs(current.low - previous.close)));
  }
  let trSmooth = sum(tr.slice(0, length)), plusSmooth = sum(plusDM.slice(0, length)), minusSmooth = sum(minusDM.slice(0, length));
  const dx = [];
  let plusDI = NaN, minusDI = NaN;
  for (let i = length; i < tr.length; i++) {
    if (i > length) {
      trSmooth = trSmooth - trSmooth / length + tr[i];
      plusSmooth = plusSmooth - plusSmooth / length + plusDM[i];
      minusSmooth = minusSmooth - minusSmooth / length + minusDM[i];
    }
    plusDI = trSmooth > 0 ? 100 * plusSmooth / trSmooth : 0;
    minusDI = trSmooth > 0 ? 100 * minusSmooth / trSmooth : 0;
    const denominator = plusDI + minusDI;
    dx.push(denominator > 0 ? (100 * Math.abs(plusDI - minusDI) / denominator) : 0);
  }
  if (dx.length < length) return { adx: NaN, plusDI, minusDI };
  let adx = average(dx.slice(0, length));
  for (let i = length; i < dx.length; i++) adx = (adx * (length - 1) + dx[i]) / length;
  return { adx, plusDI, minusDI };
}

function obvContext(bars, lookback = 20) {
  if (bars.length < 3) return { value: 0, change: 0, direction: 'FLAT' };
  const values = [0];
  for (let i = 1; i < bars.length; i++) {
    const previous = values[values.length - 1];
    if (bars[i].close > bars[i - 1].close) values.push(previous + n(bars[i].volume, 0));
    else if (bars[i].close < bars[i - 1].close) values.push(previous - n(bars[i].volume, 0));
    else values.push(previous);
  }
  const current = values[values.length - 1], previousIndex = Math.max(0, values.length - 1 - lookback), previous = values[previousIndex], change = current - previous;
  return { value: current, change, direction: change > 0 ? 'UP' : change < 0 ? 'DOWN' : 'FLAT' };
}

function mfiLast(bars, length = 14) {
  if (bars.length < length + 1) return NaN;
  let positive = 0, negative = 0, start = bars.length - length;
  for (let i = start; i < bars.length; i++) {
    if (i <= 0) continue;
    const typical = (bars[i].high + bars[i].low + bars[i].close) / 3;
    const previousTypical = (bars[i - 1].high + bars[i - 1].low + bars[i - 1].close) / 3;
    const flow = typical * Math.max(0, n(bars[i].volume, 0));
    if (typical > previousTypical) positive += flow; else if (typical < previousTypical) negative += flow;
  }
  if (negative === 0) return positive > 0 ? 100 : 50;
  return 100 - 100 / (1 + (positive / negative));
}

function rollingVwap(bars, lookback = 50) {
  if (!bars.length) return NaN;
  const window = bars.slice(-lookback);
  let numerator = 0, denominator = 0;
  for (const bar of window) {
    const volume = Math.max(0, n(bar.volume, 0)), typical = (bar.high + bar.low + bar.close) / 3;
    numerator += typical * volume; denominator += volume;
  }
  return denominator <= 0 ? NaN : numerator / denominator;
}

function candleContext(bars) {
  if (!bars.length) return null;
  const current = bars[bars.length - 1], previous = bars.length >= 2 ? bars[bars.length - 2] : null;
  const range = current.high - current.low, body = Math.abs(current.close - current.open);
  const bodyRatio = range > 0 ? body / range : 0;
  const upperWick = range > 0 ? (current.high - Math.max(current.open, current.close)) / range : 0;
  const lowerWick = range > 0 ? (Math.min(current.open, current.close) - current.low) / range : 0;
  const closeLocation = range > 0 ? (current.close - current.low) / range : 0.5;
  let bullishEngulfing = false, bearishEngulfing = false;
  if (previous) {
    bullishEngulfing = previous.close < previous.open && current.close > current.open && current.open <= previous.close && current.close >= previous.open;
    bearishEngulfing = previous.close > previous.open && current.close < current.open && current.open >= previous.close && current.close <= previous.open;
  }
  return {
    direction: current.close > current.open ? 'BULL' : current.close < current.open ? 'BEAR' : 'DOJI',
    range, body, bodyRatio, upperWick, lowerWick, closeLocation, bullishEngulfing, bearishEngulfing,
    bullishRejection: lowerWick >= 0.45 && closeLocation >= 0.60,
    bearishRejection: upperWick >= 0.45 && closeLocation <= 0.40
  };
}

function supportResistance(bars, lookback = 40) {
  if (bars.length < 5) return { support: NaN, resistance: NaN, distanceToSupport: NaN, distanceToResistance: NaN };
  const current = bars[bars.length - 1], prior = bars.slice(Math.max(0, bars.length - 1 - lookback), -1);
  if (!prior.length) return { support: NaN, resistance: NaN, distanceToSupport: NaN, distanceToResistance: NaN };
  const support = lowestLow(prior), resistance = highestHigh(prior);
  return { support, resistance, distanceToSupport: current.close - support, distanceToResistance: resistance - current.close };
}

function findSwings(bars, left = 3, right = 3) {
  const highs = [], lows = [];
  if (bars.length < left + right + 1) return { highs, lows };
  for (let i = left; i < bars.length - right; i++) {
    let isHigh = true, isLow = true;
    for (let j = i - left; j <= i + right; j++) {
      if (j === i) continue;
      if (bars[j].high >= bars[i].high) isHigh = false;
      if (bars[j].low <= bars[i].low) isLow = false;
      if (!isHigh && !isLow) break;
    }
    if (isHigh) highs.push({ index: i, price: bars[i].high, time: bars[i].openTime });
    if (isLow) lows.push({ index: i, price: bars[i].low, time: bars[i].openTime });
  }
  return { highs, lows };
}

function marketStructure(bars) {
  const swings = findSwings(bars, TECH.swingLeft, TECH.swingRight);
  const recentHighs = swings.highs.slice(-2), recentLows = swings.lows.slice(-2), current = bars[bars.length - 1];
  let highStructure = 'UNKNOWN', lowStructure = 'UNKNOWN';
  if (recentHighs.length >= 2) highStructure = recentHighs[1].price > recentHighs[0].price ? 'HH' : 'LH';
  if (recentLows.length >= 2) lowStructure = recentLows[1].price > recentLows[0].price ? 'HL' : 'LL';
  let structure = 'MIXED';
  if (highStructure === 'HH' && lowStructure === 'HL') structure = 'BULL';
  else if (highStructure === 'LH' && lowStructure === 'LL') structure = 'BEAR';
  const lastSwingHigh = recentHighs.length ? recentHighs[recentHighs.length - 1].price : NaN;
  const lastSwingLow = recentLows.length ? recentLows[recentLows.length - 1].price : NaN;
  return {
    structure, highStructure, lowStructure, lastSwingHigh, lastSwingLow,
    bullishBos: Number.isFinite(lastSwingHigh) && current.close > lastSwingHigh,
    bearishBos: Number.isFinite(lastSwingLow) && current.close < lastSwingLow,
    swingHighCount: swings.highs.length, swingLowCount: swings.lows.length
  };
}

function liquidityContext(bars, lookback = 20) {
  if (bars.length < lookback + 1) return { bullishSweep: false, bearishSweep: false, priorHigh: NaN, priorLow: NaN };
  const current = bars[bars.length - 1], prior = bars.slice(-lookback - 1, -1);
  const priorHigh = highestHigh(prior), priorLow = lowestLow(prior);
  return {
    bullishSweep: current.low < priorLow && current.close > priorLow,
    bearishSweep: current.high > priorHigh && current.close < priorHigh,
    priorHigh, priorLow
  };
}

function fvgContext(bars) {
  if (bars.length < 3) return { bullish: false, bearish: false, bullGapLow: NaN, bullGapHigh: NaN, bearGapLow: NaN, bearGapHigh: NaN };
  const first = bars[bars.length - 3], third = bars[bars.length - 1];
  const bullish = third.low > first.high, bearish = third.high < first.low;
  return {
    bullish, bearish,
    bullGapLow: bullish ? first.high : NaN, bullGapHigh: bullish ? third.low : NaN,
    bearGapLow: bearish ? third.high : NaN, bearGapHigh: bearish ? first.low : NaN
  };
}

function fibonacciContext(bars, lookback = 60) {
  if (bars.length < 10) return null;
  const window = bars.slice(-lookback), high = highestHigh(window), low = lowestLow(window), range = high - low;
  if (!(range > 0)) return null;
  const current = bars[bars.length - 1].close;
  return {
    swingHigh: high, swingLow: low, range, current,
    retracementFromHigh: { r382: high - range * 0.382, r500: high - range * 0.500, r618: high - range * 0.618, r786: high - range * 0.786 },
    retracementFromLow: { r382: low + range * 0.382, r500: low + range * 0.500, r618: low + range * 0.618, r786: low + range * 0.786 },
    extensionUp: { e1272: low + range * 1.272, e1618: low + range * 1.618 },
    extensionDown: { e1272: high - range * 1.272, e1618: high - range * 1.618 }
  };
}

function volumeContext(bars) {
  if (!bars.length) return { current: 0, average: 0, ratio: NaN, spike: false };
  const current = n(bars[bars.length - 1].volume, 0), prior = bars.slice(-TECH.volumeLen - 1, -1).map(bar => n(bar.volume, 0)), avg = average(prior);
  const ratio = Number.isFinite(avg) && avg > 0 ? current / avg : NaN;
  return { current, average: Number.isFinite(avg) ? avg : 0, ratio, spike: Number.isFinite(ratio) && ratio >= 1.5 };
}

function volatilityContext(bars) {
  const atr = atrLast(bars, TECH.atrLen), current = bars[bars.length - 1];
  const atrPct = Number.isFinite(atr) && current.close > 0 ? atr / current.close * 100 : NaN;
  const historical = [];
  const minimum = Math.max(TECH.atrLen + 2, bars.length - 50);
  for (let i = minimum; i <= bars.length; i++) {
    const value = atrLast(bars.slice(0, i), TECH.atrLen);
    if (Number.isFinite(value)) historical.push(value);
  }
  const avgAtr = average(historical), ratio = Number.isFinite(avgAtr) && avgAtr > 0 && Number.isFinite(atr) ? atr / avgAtr : NaN;
  let regime = 'NORMAL';
  if (Number.isFinite(ratio)) { if (ratio >= 1.50) regime = 'HIGH'; else if (ratio <= 0.70) regime = 'LOW'; }
  return { atr, atrPct, averageAtr: avgAtr, atrRatio: ratio, regime };
}

function trendContext(bars) {
  const closes = bars.map(bar => bar.close), close = closes[closes.length - 1];
  const ema9 = emaLast(closes, TECH.emaFast), ema21 = emaLast(closes, TECH.emaMedium), ema50 = emaLast(closes, TECH.emaTrend), ema100 = emaLast(closes, TECH.emaLong);
  const ema200 = bars.length >= EMA200_CONTEXT_HISTORY ? emaLast(closes, TECH.emaMacro) : NaN;
  let alignment = 'MIXED';
  if (Number.isFinite(ema50) && close > ema9 && ema9 > ema21 && ema21 > ema50) alignment = 'BULL';
  else if (Number.isFinite(ema50) && close < ema9 && ema9 < ema21 && ema21 < ema50) alignment = 'BEAR';
  let macro = 'UNKNOWN';
  if (Number.isFinite(ema200)) macro = close > ema200 ? 'BULL' : close < ema200 ? 'BEAR' : 'FLAT';
  else if (Number.isFinite(ema100)) macro = close > ema100 ? 'BULL' : 'BEAR';
  return { close, ema9, ema21, ema50, ema100, ema200, alignment, macro };
}

function momentumContext(bars) {
  const closes = bars.map(bar => bar.close);
  return {
    rsi: rsiLast(closes, TECH.rsiLen), cmo: cmoLast(closes, TECH.cmoLen),
    macd: macdLast(closes), stochastic: stochasticLast(bars, TECH.stochasticLen, TECH.stochasticSmooth),
    williamsR: williamsRLast(bars, TECH.stochasticLen), roc: rocLast(closes, TECH.rocLen)
  };
}

function buildTechnicalIntelligence(symbol, bars) {
  if (!Array.isArray(bars) || bars.length < CORE_MIN_HISTORY) return null;
  const current = bars[bars.length - 1], closes = bars.map(bar => bar.close);
  const trend = trendContext(bars), momentum = momentumContext(bars), dmi = dmiAdx(bars, TECH.adxLen);
  const volatility = volatilityContext(bars), bollinger = bollingerLast(closes, TECH.bbLen, TECH.bbStd), keltner = keltnerLast(bars, TECH.keltnerLen, TECH.keltnerAtrLen, TECH.keltnerMult);
  const volume = volumeContext(bars), obv = obvContext(bars), mfi = mfiLast(bars, TECH.mfiLen);
  const vwap = rollingVwap(bars, TECH.vwapLookback), candles = candleContext(bars), sr = supportResistance(bars, TECH.srLen);
  const structure = marketStructure(bars), liquidity = liquidityContext(bars, TECH.liquidityLookback), fvg = fvgContext(bars), fibonacci = fibonacciContext(bars, TECH.fibLookback);
  const atr = volatility.atr, range = current.high - current.low, rangeAtr = Number.isFinite(atr) && atr > 0 ? range / atr : NaN;
  const distanceFromVwapAtr = Number.isFinite(vwap) && Number.isFinite(atr) && atr > 0 ? (current.close - vwap) / atr : NaN;

  let bullScore = 0, bearScore = 0, directionalBias = 'MIXED';
  if (trend.alignment === 'BULL') bullScore += 2; if (trend.alignment === 'BEAR') bearScore += 2;
  if (trend.macro === 'BULL') bullScore++; if (trend.macro === 'BEAR') bearScore++;
  if (dmi.plusDI > dmi.minusDI) bullScore++; else if (dmi.minusDI > dmi.plusDI) bearScore++;
  if (momentum.macd.histogram > 0) bullScore++; else if (momentum.macd.histogram < 0) bearScore++;
  if (structure.structure === 'BULL') bullScore += 2; if (structure.structure === 'BEAR') bearScore += 2;
  if (structure.bullishBos) bullScore += 2; if (structure.bearishBos) bearScore += 2;
  if (liquidity.bullishSweep) bullScore += 2; if (liquidity.bearishSweep) bearScore += 2;
  if (candles?.bullishEngulfing) bullScore++; if (candles?.bearishEngulfing) bearScore++;
  if (candles?.bullishRejection) bullScore++; if (candles?.bearishRejection) bearScore++;
  if (bullScore >= bearScore + 3) directionalBias = 'BULL'; else if (bearScore >= bullScore + 3) directionalBias = 'BEAR';

  return {
    symbol, timeframe: TIMEFRAME, barTime: current.openTime,
    price: { open: current.open, high: current.high, low: current.low, close: current.close, range, rangeAtr },
    trend, momentum, dmi, volatility, bollinger, keltner, volume, obv, mfi, vwap, distanceFromVwapAtr, candles,
    supportResistance: sr, structure, liquidity, fvg, fibonacci,
    internalBias: { directionalBias, bullScore, bearScore }
  };
}

// ============================================================
// HTTP CLIENT + MARKET DATA
// ============================================================

const http = axios.create({ timeout: 12000, headers: { 'User-Agent': 'LOMY-Forex-Gemini-Commander/1.5' } });

async function fetchOhlc(symbol, interval = TIMEFRAME, limit = HISTORY_LIMIT) {
  const response = await http.get(`${BIQUOTE_BASE}/api/${encodeURIComponent(symbol)}/ohlc`, { params: { interval, limit } });
  const body = response.data;
  const raw = Array.isArray(body) ? body : Array.isArray(body?.bars) ? body.bars : Array.isArray(body?.data?.bars) ? body.data.bars : Array.isArray(body?.data) ? body.data : [];
  return closedBarsOnly(normalizeBars(raw), interval);
}

function normalizeQuote(raw, symbol) {
  const bid = n(raw?.bid), ask = n(raw?.ask), mid = n(raw?.mid, Number.isFinite(bid) && Number.isFinite(ask) ? (bid + ask) / 2 : NaN);
  if (![bid, ask, mid].every(Number.isFinite) || bid <= 0 || ask <= 0 || ask < bid) return null;
  return { symbol, bid, ask, mid, spread: ask - bid, timestamp: raw?.timestamp || new Date().toISOString() };
}

async function fetchSingleQuote(symbol) {
  try {
    const response = await http.get(`${BIQUOTE_BASE}/api/${encodeURIComponent(symbol)}`, { params: { allowStale: false } });
    return normalizeQuote(response.data?.data || response.data, symbol);
  } catch (error) {
    state.lastMarketError = `${symbol} quote: ${safeError(error)}`; return null;
  }
}

async function fetchLatestQuotes(symbols) {
  if (!symbols.length) return new Map();
  try {
    const params = new URLSearchParams();
    for (const symbol of symbols) params.append('symbols', symbol);
    params.append('allowStale', 'false');
    const response = await http.get(`${BIQUOTE_BASE}/api/latest?${params.toString()}`);
    const body = response.data?.data || response.data;
    const output = new Map();
    if (Array.isArray(body)) {
      for (const row of body) {
        const symbol = String(row?.symbol || '').toUpperCase();
        const quote = normalizeQuote(row, symbol);
        if (quote) output.set(symbol, quote);
      }
    } else if (body && typeof body === 'object') {
      for (const symbol of symbols) {
        const raw = body[symbol] || body[symbol.toLowerCase()];
        const quote = normalizeQuote(raw, symbol);
        if (quote) output.set(symbol, quote);
      }
    }
    return output;
  } catch (error) {
    state.lastMarketError = `latest quotes: ${safeError(error)}`; return new Map();
  }
}

async function mapWithConcurrency(items, limit, worker) {
  const results = new Array(items.length); let next = 0;
  async function run() {
    while (true) {
      const index = next++; if (index >= items.length) break;
      try { results[index] = await worker(items[index], index); } catch (error) { results[index] = { error }; }
    }
  }
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, run));
  return results;
}

// ============================================================
// ENHANCEMENT 1 & 2: TRADING SESSIONS & NEWS FILTER
// ============================================================

function getCurrentSession() {
  const hour = new Date().getUTCHours();
  if (hour >= 8 && hour < 16) return 'LONDON';
  if (hour >= 13 && hour < 21) return 'NEW_YORK';
  if (hour >= 23 || hour < 8) return 'ASIAN';
  return 'TRANSITION';
}

async function fetchEconomicNews() {
  try {
    // Fetching ForexFactory JSON API (Public)
    const res = await axios.get('https://nfs.faireconomy.media/ff_calendar_thisweek.json', { timeout: 10000 });
    const now = Date.now();
    // Filter only high impact news (red folders)
    state.highImpactNews = res.data.filter(event => event.impact === 'High' && new Date(event.date).getTime() > now - 86400000);
  } catch (error) {
    console.warn('News Filter: Could not fetch economic calendar.', safeError(error));
  }
}

function isVolatileNewsApproaching(symbol) {
  if (!state.highImpactNews || state.highImpactNews.length === 0) return { risk: false };
  const now = Date.now();
  const currencies = [symbol.substring(0, 3), symbol.substring(3, 6)];
  
  for (const event of state.highImpactNews) {
    if (currencies.includes(event.country)) {
      const eventTime = new Date(event.date).getTime();
      const diffMins = (eventTime - now) / 1000 / 60;
      // Block trading 30 mins before and 30 mins after high impact news
      if (diffMins > -30 && diffMins < 30) {
        return { risk: true, event: event.title, diffMins: Math.round(diffMins) };
      }
    }
  }
  return { risk: false };
}

// Set interval to update news every 4 hours
setInterval(fetchEconomicNews, 4 * 60 * 60 * 1000);

// ============================================================
// ENHANCEMENT 3: CURRENCY STRENGTH METER (CSM)
// ============================================================

function getCurrencyStrength() {
  // A simplified CSM evaluating how far pairs are from their 50 EMA
  const strength = { USD: 0, EUR: 0, GBP: 0, JPY: 0, AUD: 0, NZD: 0, CAD: 0, CHF: 0 };
  const count = { USD: 0, EUR: 0, GBP: 0, JPY: 0, AUD: 0, NZD: 0, CAD: 0, CHF: 0 };

  for (const [symbol, pair] of state.pairState.entries()) {
    if (pair.bars.length < 50) continue;
    const closes = pair.bars.map(b => b.close);
    const ema50 = emaLast(closes, 50);
    const close = closes[closes.length - 1];
    
    if (Number.isFinite(ema50)) {
      const diffPct = (close - ema50) / ema50 * 100;
      const base = symbol.substring(0, 3);
      const quote = symbol.substring(3, 6);
      
      if (strength[base] !== undefined) { strength[base] += diffPct; count[base]++; }
      if (strength[quote] !== undefined) { strength[quote] -= diffPct; count[quote]++; }
    }
  }

  const result = {};
  for (const currency in strength) {
    if (count[currency] > 0) result[currency] = (strength[currency] / count[currency]).toFixed(2);
  }
  return result;
}

// ============================================================
// DB SCHEMAS (Updated for Partial Close & Trailing Stop)
// ============================================================

const accountSchema = new mongoose.Schema({ accountKey: { type: String, unique: true, index: true }, startingBalance: Number, balance: Number, realizedPnl: Number, totalTrades: Number, wins: Number, losses: Number, breakeven: Number, telegramChatId: String, createdAt: Date, updatedAt: Date }, { minimize: false });

const tradeSchema = new mongoose.Schema({ 
  version: String, accountKey: { type: String, index: true }, symbol: { type: String, index: true }, direction: String, status: { type: String, index: true }, timeframe: String, entryPrice: Number, stopLoss: Number, initialStopLoss: Number, takeProfit: Number, breakEvenTriggerPrice: Number, breakEvenActive: Boolean, 
  partialClosed: Boolean, trailingLevelR: Number, realizedPartialPnl: Number, // New Tracking Fields
  riskDistance: Number, riskAmount: Number, maxCapitalRiskPct: Number, quantity: Number, signalPrice: Number, signalBarTime: String, openedAt: Date, closedAt: Date, exitPrice: Number, exitReason: String, pnl: Number, resultR: Number, mfeR: Number, maeR: Number, mfePrice: Number, maePrice: Number, mfeAt: Date, maeAt: Date, beActivatedAt: Date, lastMarkPrice: Number, lastMarkAt: Date, aiEntryDecision: mongoose.Schema.Types.Mixed, aiLastManagement: mongoose.Schema.Types.Mixed, technicalSnapshot: mongoose.Schema.Types.Mixed, multiTimeframeSnapshot: mongoose.Schema.Types.Mixed 
}, { minimize: false });

const signalSchema = new mongoose.Schema({ version: String, accountKey: { type: String, index: true }, symbol: { type: String, index: true }, direction: String, decision: String, confidence: Number, signalPrice: Number, signalBarTime: String, aiStopLoss: Number, calculatedTakeProfit: Number, createdAt: Date, executed: Boolean, skipReason: String, aiDecision: mongoose.Schema.Types.Mixed, technicalSnapshot: mongoose.Schema.Types.Mixed, multiTimeframeSnapshot: mongoose.Schema.Types.Mixed }, { minimize: false });
const journalSchema = new mongoose.Schema({ version: String, accountKey: { type: String, index: true }, eventType: { type: String, index: true }, createdAt: { type: Date, index: true }, symbol: String, direction: String, tradeId: mongoose.Schema.Types.ObjectId, message: String, data: mongoose.Schema.Types.Mixed }, { minimize: false });

const Account = mongoose.models.LomyForexPaperAccountV15 || mongoose.model('LomyForexPaperAccountV15', accountSchema, 'lomyforexpaperaccountsv15');
const Trade = mongoose.models.LomyForexTradeV15 || mongoose.model('LomyForexTradeV15', tradeSchema, 'lomyforextradesv15');
const Signal = mongoose.models.LomyForexSignalV15 || mongoose.model('LomyForexSignalV15', signalSchema, 'lomyforexsignalsv15');
const Journal = mongoose.models.LomyForexJournalV15 || mongoose.model('LomyForexJournalV15', journalSchema, JOURNAL_COLLECTION);

let account = null;
let bot = null;

async function journal(eventType, { symbol = '', direction = '', tradeId = null, message = '', data = {} } = {}) {
  if (!state.mongoReady) return;
  try { await Journal.create({ version: VERSION, accountKey: PAPER.accountKey, eventType, createdAt: new Date(), symbol, direction, tradeId, message, data }); state.journalEvents++; } catch (error) { console.error('Journal:', safeError(error)); }
}

async function initMongo() {
  if (!MONGODB_URI) throw new Error('MONGODB_URI is missing');
  await mongoose.connect(MONGODB_URI, { serverSelectionTimeoutMS: 15000 });
  state.mongoReady = true; console.log('✅ MongoDB connected');
  account = await Account.findOne({ accountKey: PAPER.accountKey });
  if (!account) {
    account = await Account.create({ accountKey: PAPER.accountKey, startingBalance: PAPER.startingBalance, balance: PAPER.startingBalance, realizedPnl: 0, totalTrades: 0, wins: 0, losses: 0, breakeven: 0, telegramChatId: null, createdAt: new Date(), updatedAt: new Date() });
  }
  await journal('MONGO_READY', { message: 'Mongo connected and V1.5 account ready', data: { balance: account.balance } });
}

async function saveAccount() {
  if (!account) return;
  account.updatedAt = new Date();
  await account.save();
}

function accountBalance() { return n(account?.balance, PAPER.startingBalance); }
function portfolioRiskCapUsd() { return (accountBalance() * PAPER.portfolioRiskCapPct / 100); }
function currentPortfolioRiskUsd() {
  let total = 0;
  for (const trade of state.openTrades.values()) {
    const entry = n(trade.entryPrice), stop = n(trade.stopLoss), initialDistance = n(trade.riskDistance), originalRiskAmount = n(trade.riskAmount, 0);
    if (!Number.isFinite(entry) || !Number.isFinite(stop) || !(initialDistance > 0) || !(originalRiskAmount > 0)) continue;
    let remainingDistance;
    if (trade.direction === 'BUY') remainingDistance = Math.max(0, entry - stop); else remainingDistance = Math.max(0, stop - entry);
    const fraction = clamp(remainingDistance / initialDistance, 0, 1);
    total += originalRiskAmount * fraction;
  }
  return total;
}

// ============================================================
// TELEGRAM
// ============================================================

async function sendTelegram(text) {
  if (!bot || !account?.telegramChatId) return;
  try { await bot.telegram.sendMessage(account.telegramChatId, text); } catch (error) { console.error('Telegram send:', safeError(error)); }
}

function pairReadyCount() { return [...state.pairState.values()].filter(item => item.initialized).length; }

async function initTelegram() {
  if (!TELEGRAM_BOT_TOKEN) { console.warn('⚠️ TELEGRAM_BOT_TOKEN missing'); return; }
  bot = new Telegraf(TELEGRAM_BOT_TOKEN);
  bot.start(async ctx => {
    account.telegramChatId = String(ctx.chat.id); await saveAccount();
    await ctx.reply(`✅ ${VERSION}\n🧪 PAPER ONLY\nBalance: ${fmtMoney(accountBalance())}\nR:R = 1:${RULES.riskReward.toFixed(0)}\nCapital-risk safety cap: ${PAPER.maxCapitalRiskPct}% / trade\nPortfolio safety cap: ${PAPER.portfolioRiskCapPct}%\nGemini: COMMANDER`);
  });
  bot.command('status', async ctx => {
    await ctx.reply(`🤖 ${VERSION}\nMode: PAPER\nLIVE: OFF\nMarket: ${state.marketReady ? 'READY' : 'WAIT'}\nGemini: ${state.geminiReady ? 'READY' : 'WAIT'}\nPairs: ${pairReadyCount()}/${INSTRUMENTS.length}\nOpen trades: ${state.openTrades.size}\nExecuted: ${state.executedSignals}\nSkipped: ${state.skippedSignals}\nAI BUY: ${state.aiBuyDecisions}\nAI SELL: ${state.aiSellDecisions}\nAI NO_TRADE: ${state.aiNoTradeDecisions}\nAI HOLD: ${state.aiHoldDecisions}\nAI CLOSE: ${state.aiCloseDecisions}\nR:R: 1:${RULES.riskReward.toFixed(0)}\nMax capital-risk safety: ${PAPER.maxCapitalRiskPct}%\nPortfolio cap: ${PAPER.portfolioRiskCapPct}%`);
  });
  bot.command('balance', async ctx => {
    await ctx.reply(`💰 PAPER ACCOUNT\nBalance: ${fmtMoney(accountBalance())}\nRealized PnL: ${fmtMoney(account?.realizedPnl)}\nCurrent portfolio risk: ${fmtMoney(currentPortfolioRiskUsd())}\nPortfolio risk cap: ${fmtMoney(portfolioRiskCapUsd())}`);
  });
  bot.command('positions', async ctx => {
    const positions = [...state.openTrades.values()];
    if (!positions.length) { await ctx.reply('📭 No open PAPER trades'); return; }
    const text = positions.map(trade => `${trade.symbol} ${trade.direction}\nEntry: ${fmtPrice(trade.entryPrice, trade.symbol)}\nSL: ${fmtPrice(trade.stopLoss, trade.symbol)}\nTP: ${fmtPrice(trade.takeProfit, trade.symbol)}\nR:R 1:${RULES.riskReward.toFixed(0)} | BE ${trade.breakEvenActive ? 'ON' : 'OFF'}\nMFE ${n(trade.mfeR, 0).toFixed(2)}R | MAE ${n(trade.maeR, 0).toFixed(2)}R`).join('\n\n');
    await ctx.reply(text);
  });
  await bot.telegram.getMe(); state.telegramReady = true; console.log('✅ Telegram authenticated');
  bot.launch({ dropPendingUpdates: true }).then(() => { console.log('✅ Telegram polling started'); }).catch(error => { console.error('Telegram launch:', safeError(error)); });
}

// ============================================================
// AI MEMORY (Self-Learning from Past Trades)
// ============================================================

async function getAiMemory(symbol = '') {
  try {
    const baseQuery = { accountKey: PAPER.accountKey, status: 'CLOSED' };
    const projection = { symbol: 1, direction: 1, resultR: 1, pnl: 1, exitReason: 1, mfeR: 1, maeR: 1, openedAt: 1, closedAt: 1, aiEntryDecision: 1, aiLastManagement: 1, technicalSnapshot: 1 };
    
    let rows = [];
    if (symbol) rows = await Trade.find({ ...baseQuery, symbol }, projection).sort({ closedAt: -1 }).limit(AI.memoryClosedTrades).lean();
    if (!symbol || rows.length < 8) rows = await Trade.find(baseQuery, projection).sort({ closedAt: -1 }).limit(AI.memoryClosedTrades).lean();

    const wins = rows.filter(trade => n(trade.resultR, 0) > 0.10).length;
    const losses = rows.filter(trade => n(trade.resultR, 0) < -0.10).length;
    const breakeven = rows.length - wins - losses;
    const totalR = rows.reduce((total, trade) => total + n(trade.resultR, 0), 0);
    const avgR = rows.length ? totalR / rows.length : 0;
    
    const recentLosses = rows.filter(t => n(t.resultR, 0) < -0.10).slice(0, 5).map(trade => ({
      symbol: trade.symbol, direction: trade.direction, resultR: n(trade.resultR, 0), exitReason: trade.exitReason,
      setup: trade.aiEntryDecision?.setup || '', entryReason: trade.aiEntryDecision?.reason || '',
      technicalContextAtEntry: {
        trendAlignment: trade.technicalSnapshot?.trend?.alignment || 'UNKNOWN',
        marketStructure: trade.technicalSnapshot?.structure?.structure || 'UNKNOWN',
        volatilityRegime: trade.technicalSnapshot?.volatility?.regime || 'UNKNOWN'
      }
    }));

    return {
      count: rows.length, wins, losses, breakeven, winRate: rows.length ? wins / rows.length * 100 : 0,
      totalR, avgR,
      recentLossesForLearning: recentLosses,
      recent: rows.slice(0, 8).map(trade => ({
        symbol: trade.symbol, direction: trade.direction, resultR: n(trade.resultR, 0), pnl: n(trade.pnl, 0), exitReason: trade.exitReason,
        mfeR: n(trade.mfeR, 0), maeR: n(trade.maeR, 0), entryReason: trade.aiEntryDecision?.reason || '', setup: trade.aiEntryDecision?.setup || ''
      }))
    };
  } catch (error) {
    console.error('AI memory:', safeError(error));
    return { count: 0, wins: 0, losses: 0, breakeven: 0, winRate: 0, totalR: 0, recentLossesForLearning: [], recent: [] };
  }
}

// ============================================================
// HIGHER TIMEFRAME TECHNICAL CONTEXT
// ============================================================

function compactTechnicalContext(technical) {
  if (!technical) return null;
  return {
    timeframe: technical.timeframe, barTime: technical.barTime, price: technical.price, trend: technical.trend,
    momentum: technical.momentum, dmi: technical.dmi, volatility: technical.volatility, bollinger: technical.bollinger,
    keltner: technical.keltner, volume: technical.volume, obv: technical.obv, mfi: technical.mfi, vwap: technical.vwap,
    distanceFromVwapAtr: technical.distanceFromVwapAtr, candles: technical.candles, supportResistance: technical.supportResistance,
    structure: technical.structure, liquidity: technical.liquidity, fvg: technical.fvg, fibonacci: technical.fibonacci, internalBias: technical.internalBias
  };
}

async function buildMultiTimeframeContext(symbol, current15mBars = null) {
  const result = { m15: null, h1: null, h4: null };
  try {
    const memory = state.pairState.get(symbol);
    const bars15 = current15mBars?.length ? current15mBars : memory?.bars || [];
    if (bars15.length >= CORE_MIN_HISTORY) result.m15 = buildContextForBars(symbol, bars15, '15m');
    const [bars1h, bars4h] = await Promise.all([
      fetchOhlc(symbol, '1h', 220).catch(() => []),
      fetchOhlc(symbol, '4h', 220).catch(() => [])
    ]);
    if (bars1h.length >= CORE_MIN_HISTORY) result.h1 = buildContextForBars(symbol, bars1h, '1h');
    if (bars4h.length >= CORE_MIN_HISTORY) result.h4 = buildContextForBars(symbol, bars4h, '4h');
  } catch (error) { console.error(`MTF ${symbol}:`, safeError(error)); }
  return result;
}

function buildContextForBars(symbol, bars, timeframe) {
  if (!Array.isArray(bars) || bars.length < CORE_MIN_HISTORY) return null;
  const technical = buildTechnicalIntelligence(symbol, bars);
  if (!technical) return null;
  technical.timeframe = timeframe;
  return compactTechnicalContext(technical);
}

// ============================================================
// GEMINI JSON API
// ============================================================

function extractGeminiText(response) {
  const candidates = response?.data?.candidates;
  if (!Array.isArray(candidates) || !candidates.length) return '';
  const parts = candidates[0]?.content?.parts;
  if (!Array.isArray(parts)) return '';
  return parts.map(part => typeof part?.text === 'string' ? part.text : '').join('').trim();
}

function parseJsonFromText(text) {
  if (!text) return null;
  const cleaned = String(text).trim().replace(/^```json/i, '').replace(/^```/i, '').replace(/```$/, '').trim();
  try { return JSON.parse(cleaned); } catch {}
  const first = cleaned.indexOf('{'), last = cleaned.lastIndexOf('}');
  if (first !== -1 && last > first) { try { return JSON.parse(cleaned.slice(first, last + 1)); } catch {} }
  return null;
}

function enqueueGemini(task) {
  const execute = async () => {
    const elapsed = Date.now() - state.geminiLastCallAt, wait = GEMINI_MIN_CALL_GAP_MS - elapsed;
    if (wait > 0) await sleep(wait);
    state.geminiLastCallAt = Date.now(); return task();
  };
  const result = state.geminiQueue.then(execute, execute);
  state.geminiQueue = result.catch(() => {});
  return result;
}

async function geminiJson(systemInstruction, payload) {
  if (!GEMINI_API_KEY) { state.geminiReady = false; state.lastAiError = 'GEMINI_API_KEY missing'; return null; }
  return enqueueGemini(async () => {
    const url = `${GEMINI_BASE}/models/${encodeURIComponent(GEMINI_MODEL)}:generateContent`;
    try {
      const response = await axios.post(url, {
        system_instruction: { parts: [{ text: systemInstruction }] },
        contents: [{ role: 'user', parts: [{ text: JSON.stringify(payload) }] }],
        generationConfig: { temperature: AI.temperature, response_mime_type: 'application/json' }
      }, { timeout: AI.timeoutMs, headers: { 'Content-Type': 'application/json', 'x-goog-api-key': GEMINI_API_KEY } });
      const text = extractGeminiText(response); const json = parseJsonFromText(text);
      if (!json || typeof json !== 'object') throw new Error('Gemini returned invalid JSON');
      state.geminiReady = true; state.lastAiError = null; return json;
    } catch (error) {
      state.geminiReady = false; state.lastAiError = safeError(error);
      console.error('Gemini:', state.lastAiError); return null;
    }
  });
}

// ============================================================
// AI ENTRY COMMANDER (Upgraded with Deep Learning & Strict Roles)
// ============================================================

async function aiEntryCommander(symbol, technical, multiTimeframe, quote) {
  state.aiEntryCalls++;
  if (!AI.enabled || !AI.entryCommanderEnabled || !GEMINI_API_KEY) {
    state.aiNoTradeDecisions++;
    return { decision: 'NO_TRADE', confidence: 0, stopLoss: null, reason: 'AI_UNAVAILABLE_FAIL_CLOSED', failClosed: true };
  }

  const memory = await getAiMemory(symbol);
  const currentSession = getCurrentSession();
  const csmData = getCurrencyStrength();

  const systemInstruction = `
You are the elite AI trading commander of LOMY FOREX V1.5 PRO.

SECURITY CONSTRAINT:
You possess NO access to user funds, NO ability to withdraw or deposit money, and NO permissions outside of market technical analysis. 
Your ONLY function is to analyze the market and output a JSON trading decision.

This system is PAPER ONLY.

You must independently decide exactly one:
BUY | SELL | NO_TRADE

NEW FEATURES CONTEXT:
1. Session: You are currently in the ${currentSession} session. Avoid trading quiet pairs in Asian sessions.
2. Currency Strength Meter (CSM): Evaluate the CSM data provided. Trade strong currencies against weak ones.
3. News Filter: The Risk Manager prevents trading near major news automatically.

SELF-LEARNING IMPERATIVE:
Review the "recentLossesForLearning" in your trade memory. Identify the technical setups that recently failed. DO NOT repeat these mistakes. If the current technical context matches a recently failed setup, output NO_TRADE.

RISK REWARD:
The system strictly enforces a dynamic risk model based on your confidence score (85%+ = 1%, 75%+ = 0.75%, 62%+ = 0.50%).
- You MUST select a precise Stop Loss (SL) price based purely on technical invalidation (e.g., beyond a swing high/low, FVG, or ATR limit).
- DO NOT calculate the Take Profit (TP). The Risk Manager bot handles Partial TP at 1:2 and Trailing Stops automatically.
- DO NOT calculate position size or risk percentage.

If decision is BUY: stopLoss MUST be a PRICE below the current ask.
If decision is SELL: stopLoss MUST be a PRICE above the current bid.

Return JSON only in this exact structure:
{
  "decision":"BUY|SELL|NO_TRADE",
  "confidence":0-100,
  "stopLoss":number|null,
  "reason":"concise trading rationale, citing specific indicators and timeframe confluence",
  "setup":"short setup name (e.g., Bullish FVG + MACD Divergence)",
  "invalidation":"what technically invalidates the setup",
  "marketRegime":"TRENDING|RANGING|VOLATILE",
  "trend15m":"BULL|BEAR|MIXED",
  "trend1h":"BULL|BEAR|MIXED",
  "trend4h":"BULL|BEAR|MIXED",
  "warnings":["any conflicting signals or risks observed"]
}
`;

  const payload = {
    version: VERSION, mode: MODE, symbol,
    quote: { bid: quote.bid, ask: quote.ask, spread: quote.spread },
    currentSession, csmData,
    technical15m: compactTechnicalContext(technical),
    multiTimeframe, tradeMemory: memory,
    immutableExecutionRules: { riskReward: `1:${RULES.riskReward}`, breakEvenTriggerR: RULES.breakEvenTriggerR, liveTrading: false, dynamicRisk: true, partialCloseAndTrail: true }
  };

  const response = await geminiJson(systemInstruction, payload);

  if (!response) {
    state.aiNoTradeDecisions++;
    return { decision: 'NO_TRADE', confidence: 0, stopLoss: null, reason: 'GEMINI_UNAVAILABLE', failClosed: true };
  }

  let decision = String(response.decision || '').trim().toUpperCase();
  const confidence = clamp(n(response.confidence, 0), 0, 100);

  if (!['BUY', 'SELL', 'NO_TRADE'].includes(decision)) decision = 'NO_TRADE';
  if ((decision === 'BUY' || decision === 'SELL') && confidence < RULES.minEntryConfidence) decision = 'NO_TRADE';

  let stopLoss = n(response.stopLoss, NaN);
  if (decision === 'NO_TRADE') { stopLoss = null; state.aiNoTradeDecisions++; }
  else if (decision === 'BUY') state.aiBuyDecisions++;
  else if (decision === 'SELL') state.aiSellDecisions++;

  return {
    decision, confidence, stopLoss,
    reason: String(response.reason || 'No reason supplied').slice(0, 1000),
    setup: String(response.setup || '').slice(0, 300),
    invalidation: String(response.invalidation || '').slice(0, 600),
    marketRegime: String(response.marketRegime || 'UNCLEAR').toUpperCase(),
    trend15m: String(response.trend15m || 'MIXED').toUpperCase(),
    trend1h: String(response.trend1h || 'MIXED').toUpperCase(),
    trend4h: String(response.trend4h || 'MIXED').toUpperCase(),
    warnings: Array.isArray(response.warnings) ? response.warnings.slice(0, 10).map(w => String(w).slice(0, 300)) : [],
    failClosed: false
  };
}

function validateAiStopLoss(decision, quote, technical) {
  if (!decision || !['BUY', 'SELL'].includes(decision.decision)) return { valid: false, reason: 'NO_DIRECTION' };
  const direction = decision.decision, stopLoss = n(decision.stopLoss, NaN);
  if (!Number.isFinite(stopLoss)) return { valid: false, reason: 'AI_SL_MISSING' };
  const entry = direction === 'BUY' ? quote.ask : quote.bid;
  if (direction === 'BUY' && !(stopLoss < entry)) return { valid: false, reason: 'BUY_SL_NOT_BELOW_ENTRY' };
  if (direction === 'SELL' && !(stopLoss > entry)) return { valid: false, reason: 'SELL_SL_NOT_ABOVE_ENTRY' };
  const riskDistance = Math.abs(entry - stopLoss), atr = n(technical?.volatility?.atr, NaN);
  if (!Number.isFinite(atr) || !(atr > 0)) return { valid: false, reason: 'ATR_UNAVAILABLE' };
  const stopAtr = riskDistance / atr;
  if (stopAtr < RULES.minStopAtr) return { valid: false, reason: 'AI_SL_TOO_TIGHT', stopAtr };
  if (stopAtr > RULES.maxStopAtr) return { valid: false, reason: 'AI_SL_TOO_WIDE', stopAtr };
  const spreadFraction = quote.spread / riskDistance;
  if (!Number.isFinite(spreadFraction) || spreadFraction > RULES.maxSpreadRiskFraction) return { valid: false, reason: 'SPREAD_TOO_LARGE_VS_RISK', spreadFraction };
  
  // Mechanical TP & BE Calculations
  const takeProfit = direction === 'BUY' ? (entry + riskDistance * RULES.riskReward) : (entry - riskDistance * RULES.riskReward);
  const breakEvenTriggerPrice = direction === 'BUY' ? (entry + riskDistance * RULES.breakEvenTriggerR) : (entry - riskDistance * RULES.breakEvenTriggerR);
  
  const validOrder = direction === 'BUY' ? (stopLoss < entry && entry < breakEvenTriggerPrice && breakEvenTriggerPrice < takeProfit) : (takeProfit < breakEvenTriggerPrice && breakEvenTriggerPrice < entry && entry < stopLoss);
  if (!validOrder) return { valid: false, reason: 'INVALID_PRICE_ORDER' };
  return { valid: true, direction, entry, stopLoss, riskDistance, stopAtr, spreadFraction, takeProfit, breakEvenTriggerPrice };
}

// ============================================================
// SIGNAL / PAPER EXECUTION / MANAGEMENT
// ============================================================

async function recordSignal({ symbol, technical, multiTimeframe, aiDecision, executionLevels = null, executed = false, skipReason = '' }) {
  const direction = aiDecision?.decision || 'NO_TRADE';
  const signalPrice = n(technical?.price?.close, NaN), signalBarTime = String(technical?.barTime || '');
  if (state.mongoReady) {
    try {
      await Signal.create({ version: VERSION, accountKey: PAPER.accountKey, symbol, direction: direction === 'NO_TRADE' ? '' : direction, decision: direction, confidence: n(aiDecision?.confidence, 0), signalPrice, signalBarTime, aiStopLoss: Number.isFinite(n(aiDecision?.stopLoss, NaN)) ? n(aiDecision.stopLoss) : null, calculatedTakeProfit: executionLevels?.takeProfit ?? null, createdAt: new Date(), executed, skipReason, aiDecision, technicalSnapshot: technical, multiTimeframeSnapshot: multiTimeframe });
    } catch (error) { console.error('Signal record:', safeError(error)); }
  }
  await journal(executed ? 'AI_TRADE_EXECUTED' : 'AI_DECISION', { symbol, direction: direction === 'NO_TRADE' ? '' : direction, message: executed ? 'EXECUTED' : skipReason || direction, data: { aiDecision, executionLevels, technical, multiTimeframe, skipReason } });
}

async function aiManagementDecision(trade, quote) {
  if (!AI.enabled || !AI.managementEnabled || !GEMINI_API_KEY) return { decision: 'HOLD', confidence: 0, reason: 'AI_UNAVAILABLE_MECHANICAL_PROTECTION_ACTIVE', setupInvalidated: false, failClosed: true };
  state.aiManageCalls++;
  const [multiTimeframe, memory] = await Promise.all([buildMultiTimeframeContext(trade.symbol), getAiMemory(trade.symbol)]);
  const r = currentR(trade, quote);
  const systemInstruction = `
You manage an open PAPER forex trade for LOMY FOREX V1.5.
SECURITY CONSTRAINT: You have NO access to funds. You can ONLY analyze data.
You may choose only: HOLD | CLOSE
You may NOT move stop loss or take profit. TP is locked mechanically.
Return JSON only: {"decision":"HOLD|CLOSE","confidence":0-100,"reason":"explanation","setupInvalidated":true|false,"warnings":[]}
`;
  const payload = { version: VERSION, symbol: trade.symbol, direction: trade.direction, currentTrade: { entryPrice: trade.entryPrice, stopLoss: trade.stopLoss, takeProfit: trade.takeProfit, currentR: r, mfeR: n(trade.mfeR, 0), maeR: n(trade.maeR, 0), originalAiDecision: trade.aiEntryDecision || null }, quote: { bid: quote.bid, ask: quote.ask, spread: quote.spread }, multiTimeframe, memory, immutableRules: { riskReward: '1:2', breakEvenTriggerR: RULES.breakEvenTriggerR, liveTrading: false } };
  const response = await geminiJson(systemInstruction, payload);
  if (!response) { state.aiHoldDecisions++; return { decision: 'HOLD', confidence: 0, reason: 'GEMINI_UNAVAILABLE', setupInvalidated: false, failClosed: true }; }
  let decision = String(response.decision || '').trim().toUpperCase();
  const confidence = clamp(n(response.confidence, 0), 0, 100);
  if (!['HOLD', 'CLOSE'].includes(decision)) decision = 'HOLD';
  if (decision === 'CLOSE' && confidence < RULES.minCloseConfidence) decision = 'HOLD';
  if (decision === 'CLOSE') state.aiCloseDecisions++; else state.aiHoldDecisions++;
  return { decision, confidence, reason: String(response.reason || '').slice(0, 1000), setupInvalidated: response.setupInvalidated === true, failClosed: false };
}

function calculatePositionSize(entryPrice, stopLoss, confidence) {
  const riskDistance = Math.abs(entryPrice - stopLoss);
  if (!Number.isFinite(riskDistance) || riskDistance <= 0) return null;

  let riskPct = DYNAMIC_RISK.lowRiskPct;
  if (confidence >= DYNAMIC_RISK.highConfidence) riskPct = DYNAMIC_RISK.highRiskPct;
  else if (confidence >= DYNAMIC_RISK.medConfidence) riskPct = DYNAMIC_RISK.medRiskPct;

  const riskAmount = (accountBalance() * riskPct) / 100;
  if (!Number.isFinite(riskAmount) || riskAmount <= 0) return null;
  
  const quantity = riskAmount / riskDistance;
  return { riskDistance, riskAmount, quantity, riskPct };
}

function canOpenNewTrade(symbol, proposedRiskAmount) {
  if (MODE !== 'PAPER') return { allowed: false, reason: 'PAPER_ONLY' };
  if (LIVE_TRADING) return { allowed: false, reason: 'LIVE_TRADING_FORBIDDEN' };
  if (state.openTrades.has(symbol)) return { allowed: false, reason: 'SYMBOL_ALREADY_OPEN' };
  if (state.openTrades.size >= PAPER.maxOpenTrades) return { allowed: false, reason: 'MAX_OPEN_TRADES' };
  const currentRisk = currentPortfolioRiskUsd(), cap = portfolioRiskCapUsd();
  if (currentRisk + proposedRiskAmount > cap + 1e-9) return { allowed: false, reason: 'PORTFOLIO_RISK_CAP', currentRisk, proposedRiskAmount, cap };
  return { allowed: true, currentRisk, proposedRiskAmount, cap };
}

async function openPaperTrade({ symbol, technical, multiTimeframe, quote, aiDecision, executionLevels, sizing }) {
  const { direction, entry, stopLoss, riskDistance, takeProfit, breakEvenTriggerPrice } = executionLevels;

  const tradeData = {
    version: VERSION, accountKey: PAPER.accountKey, symbol, direction, status: 'OPEN', timeframe: TIMEFRAME,
    entryPrice: entry, stopLoss, initialStopLoss: stopLoss, takeProfit, breakEvenTriggerPrice, breakEvenActive: false,
    partialClosed: false, trailingLevelR: 0, realizedPartialPnl: 0,
    riskDistance, riskAmount: sizing.riskAmount, maxCapitalRiskPct: sizing.riskPct, quantity: sizing.quantity,
    signalPrice: technical.price.close, signalBarTime: String(technical.barTime || ''),
    openedAt: new Date(), closedAt: null, exitPrice: null, exitReason: '', pnl: 0, resultR: 0, mfeR: 0, maeR: 0,
    mfePrice: entry, maePrice: entry, mfeAt: new Date(), maeAt: new Date(), beActivatedAt: null, lastMarkPrice: entry, lastMarkAt: new Date(),
    aiEntryDecision: aiDecision, aiLastManagement: null, technicalSnapshot: technical, multiTimeframeSnapshot: multiTimeframe
  };

  let trade;
  if (state.mongoReady) { trade = await Trade.create(tradeData); trade = trade.toObject(); } 
  else { trade = { ...tradeData, _id: `paper_${Date.now()}_${symbol}` }; }
  
  state.openTrades.set(symbol, trade); state.executedSignals++;
  await journal('TRADE_OPEN', { symbol, direction, tradeId: trade._id, message: `${direction} PAPER trade opened`, data: { entry, stopLoss, takeProfit, riskDistance, riskAmount: sizing.riskAmount, quantity: sizing.quantity, aiDecision } });
  await sendTelegram(`🚀 PAPER TRADE OPENED\n\n${symbol} ${direction}\nEntry: ${fmtPrice(entry, symbol)}\nSL: ${fmtPrice(stopLoss, symbol)}\nBE trigger: ${fmtPrice(breakEvenTriggerPrice, symbol)} (+${RULES.breakEvenTriggerR.toFixed(2)}R)\n\nDynamic Risk: ${sizing.riskPct}%\nRisk budget: ${fmtMoney(sizing.riskAmount)}\nGemini confidence: ${n(aiDecision.confidence, 0).toFixed(0)}%\nSetup: ${aiDecision.setup || 'N/A'}\nReason: ${aiDecision.reason}`);
  console.log(`🚀 ${symbol} ${direction} | Entry=${fmtPrice(entry, symbol)} | SL=${fmtPrice(stopLoss, symbol)} | TP=${fmtPrice(takeProfit, symbol)} | Risk=${sizing.riskPct}%`);
  return { opened: true, trade };
}

function tradeMarkPrice(trade, quote) { return trade.direction === 'BUY' ? quote.bid : quote.ask; }

function currentR(trade, quote) {
  const mark = tradeMarkPrice(trade, quote), entry = n(trade.entryPrice), riskDist = n(trade.riskDistance);
  if (!Number.isFinite(mark) || !Number.isFinite(entry) || !Number.isFinite(riskDist) || riskDist <= 0) return 0;
  return trade.direction === 'BUY' ? (mark - entry) / riskDist : (entry - mark) / riskDist;
}

async function updateTradeExcursions(trade, quote) {
  const mark = tradeMarkPrice(trade, quote), r = currentR(trade, quote);
  let changed = false; trade.lastMarkPrice = mark; trade.lastMarkAt = new Date();
  if (r > n(trade.mfeR, 0)) { trade.mfeR = r; trade.mfePrice = mark; trade.mfeAt = new Date(); changed = true; }
  if (r < n(trade.maeR, 0)) { trade.maeR = r; trade.maePrice = mark; trade.maeAt = new Date(); changed = true; }
  if (state.mongoReady && trade._id) {
    const update = { lastMarkPrice: trade.lastMarkPrice, lastMarkAt: trade.lastMarkAt };
    if (changed) { update.mfeR = trade.mfeR; update.maeR = trade.maeR; update.mfePrice = trade.mfePrice; update.maePrice = trade.maePrice; update.mfeAt = trade.mfeAt; update.maeAt = trade.maeAt; }
    try { await Trade.updateOne({ _id: trade._id }, { $set: update }); } catch (error) { console.error(`Excursion save ${trade.symbol}:`, safeError(error)); }
  }
}

async function activateBreakEven(trade) {
  if (trade.breakEvenActive) return;
  trade.breakEvenActive = true; trade.stopLoss = trade.entryPrice; trade.beActivatedAt = new Date();
  if (state.mongoReady && trade._id) await Trade.updateOne({ _id: trade._id }, { $set: { breakEvenActive: true, stopLoss: trade.entryPrice, beActivatedAt: trade.beActivatedAt } });
  await journal('BREAK_EVEN_ACTIVATED', { symbol: trade.symbol, direction: trade.direction, tradeId: trade._id, message: `Break even activated at +${RULES.breakEvenTriggerR.toFixed(2)}R`, data: { entryPrice: trade.entryPrice, newStopLoss: trade.entryPrice } });
  await sendTelegram(`🛡️ BREAK EVEN ACTIVATED\n\n${trade.symbol} ${trade.direction}\nTrigger: +${RULES.breakEvenTriggerR.toFixed(2)}R\nSL moved to entry: ${fmtPrice(trade.entryPrice, trade.symbol)}`);
  console.log(`🛡️ ${trade.symbol} BE ACTIVE`);
}

function calculateTradePnl(trade, exitPrice) {
  const entry = n(trade.entryPrice), quantity = n(trade.quantity);
  if (!Number.isFinite(entry) || !Number.isFinite(exitPrice) || !Number.isFinite(quantity)) return 0;
  return (trade.direction === 'BUY' ? exitPrice - entry : entry - exitPrice) * quantity;
}

function calculateResultR(trade, exitPrice) {
  const riskDistance = n(trade.riskDistance);
  if (!Number.isFinite(riskDistance) || riskDistance <= 0) return 0;
  return trade.direction === 'BUY' ? (exitPrice - trade.entryPrice) / riskDistance : (trade.entryPrice - exitPrice) / riskDistance;
}

async function closePaperTrade(trade, exitPrice, reason, extra = {}) {
  if (!trade) return null; const liveTrade = state.openTrades.get(trade.symbol); if (!liveTrade) return null;
  exitPrice = n(exitPrice, NaN); if (!Number.isFinite(exitPrice) || exitPrice <= 0) return null;
  
  let pnl = calculateTradePnl(liveTrade, exitPrice);
  if (liveTrade.partialClosed) { pnl += liveTrade.realizedPartialPnl; } // Add already secured PnL
  
  const resultR = calculateResultR(liveTrade, exitPrice), closedAt = new Date();
  liveTrade.status = 'CLOSED'; liveTrade.closedAt = closedAt; liveTrade.exitPrice = exitPrice; liveTrade.exitReason = reason; liveTrade.pnl = pnl; liveTrade.resultR = resultR;
  if (extra.aiManagement) liveTrade.aiLastManagement = extra.aiManagement;
  state.openTrades.delete(liveTrade.symbol);
  
  if (account) {
    account.balance = accountBalance() + calculateTradePnl(liveTrade, exitPrice); // Only add remaining PnL, partial already added
    account.realizedPnl = n(account.realizedPnl, 0) + calculateTradePnl(liveTrade, exitPrice); 
    account.totalTrades = n(account.totalTrades, 0) + 1;
    if (resultR > 0.10) account.wins = n(account.wins, 0) + 1; else if (resultR < -0.10) account.losses = n(account.losses, 0) + 1; else account.breakeven = n(account.breakeven, 0) + 1;
    await saveAccount();
  }
  
  if (state.mongoReady && liveTrade._id) {
    try { await Trade.updateOne({ _id: liveTrade._id }, { $set: { status: 'CLOSED', closedAt, exitPrice, exitReason: reason, pnl, resultR, mfeR: liveTrade.mfeR, maeR: liveTrade.maeR, mfePrice: liveTrade.mfePrice, maePrice: liveTrade.maePrice, aiLastManagement: liveTrade.aiLastManagement, lastMarkPrice: exitPrice, lastMarkAt: closedAt } }); } catch (error) { console.error(`Trade close DB ${liveTrade.symbol}:`, safeError(error)); }
  }
  await journal('TRADE_CLOSE', { symbol: liveTrade.symbol, direction: liveTrade.direction, tradeId: liveTrade._id, message: reason, data: { exitPrice, pnl, resultR, mfeR: liveTrade.mfeR, maeR: liveTrade.maeR, breakEvenActive: liveTrade.breakEvenActive, aiManagement: extra.aiManagement || null } });
  const icon = resultR > 0.10 ? '✅' : resultR < -0.10 ? '❌' : '➖';
  await sendTelegram(`${icon} PAPER TRADE CLOSED\n\n${liveTrade.symbol} ${liveTrade.direction}\nReason: ${reason}\nExit: ${fmtPrice(exitPrice, liveTrade.symbol)}\nResult: ${resultR.toFixed(2)}R\nTotal PnL: ${fmtMoney(pnl)}\nMFE: ${n(liveTrade.mfeR, 0).toFixed(2)}R\nMAE: ${n(liveTrade.maeR, 0).toFixed(2)}R\nBalance: ${fmtMoney(accountBalance())}`);
  console.log(`${icon} ${liveTrade.symbol} CLOSED | ${reason} | ${resultR.toFixed(2)}R | ${fmtMoney(pnl)}`);
  return { ...liveTrade, pnl, resultR, exitPrice, exitReason: reason };
}

// ENHANCEMENT 4: Mechanical Protection with Partial Close & Trailing Stop
async function applyMechanicalProtection(trade, quote) {
  const mark = tradeMarkPrice(trade, quote);
  const current_r = currentR(trade, quote);
  if (!Number.isFinite(mark)) return { closed: false };

  // 1. Break Even Activation
  if (!trade.breakEvenActive && current_r >= RULES.breakEvenTriggerR) {
    trade.breakEvenActive = true; trade.stopLoss = trade.entryPrice; trade.beActivatedAt = new Date();
    if (state.mongoReady && trade._id) await Trade.updateOne({ _id: trade._id }, { $set: { breakEvenActive: true, stopLoss: trade.entryPrice, beActivatedAt: trade.beActivatedAt } });
    await sendTelegram(`🛡️ BREAK EVEN ACTIVATED\n${trade.symbol} ${trade.direction}\nTrigger: +${RULES.breakEvenTriggerR}R`);
  }

  // 2. Partial TP at 2R (Close 50% and secure profits)
  if (!trade.partialClosed && current_r >= RULES.partialTpTriggerR) {
    trade.partialClosed = true;
    const partialQuantity = trade.quantity / 2;
    trade.quantity = trade.quantity - partialQuantity; 
    
    const pnlPartial = (trade.direction === 'BUY' ? mark - trade.entryPrice : trade.entryPrice - mark) * partialQuantity;
    trade.realizedPartialPnl = pnlPartial;
    
    // Move SL to +1R to lock in profit for the trailing half
    const oneR_ProfitPrice = trade.entryPrice + (trade.riskDistance * 1.0 * (trade.direction === 'BUY' ? 1 : -1));
    trade.stopLoss = oneR_ProfitPrice;
    trade.trailingLevelR = 1.0;
    
    if (account) {
      account.balance += pnlPartial; account.realizedPnl = n(account.realizedPnl, 0) + pnlPartial; await saveAccount();
    }

    if (state.mongoReady && trade._id) await Trade.updateOne({ _id: trade._id }, { $set: { partialClosed: true, quantity: trade.quantity, stopLoss: trade.stopLoss, trailingLevelR: trade.trailingLevelR, realizedPartialPnl: pnlPartial } });
    await sendTelegram(`🎯 PARTIAL TP HIT (1:2)\n${trade.symbol} ${trade.direction}\nSecured 50% Profit: ${fmtMoney(pnlPartial)}\nRemaining 50% Trailing SL moved to +1R: ${fmtPrice(trade.stopLoss, trade.symbol)}`);
  }

  // 3. Trailing Stop Logic (If partial is closed, trail every 0.5R)
  if (trade.partialClosed && current_r >= trade.trailingLevelR + RULES.trailingStepR + 0.5) {
    const newTrailR = Math.floor((current_r - 0.5) / RULES.trailingStepR) * RULES.trailingStepR;
    if (newTrailR > trade.trailingLevelR) {
      trade.trailingLevelR = newTrailR;
      trade.stopLoss = trade.entryPrice + (trade.riskDistance * newTrailR * (trade.direction === 'BUY' ? 1 : -1));
      if (state.mongoReady && trade._id) await Trade.updateOne({ _id: trade._id }, { $set: { trailingLevelR: trade.trailingLevelR, stopLoss: trade.stopLoss } });
      await sendTelegram(`📈 TRAILING STOP UPDATED\n${trade.symbol} ${trade.direction}\nNew SL secured at +${newTrailR}R: ${fmtPrice(trade.stopLoss, trade.symbol)}`);
    }
  }

  // 4. Hard Stop Loss Hit
  if ((trade.direction === 'BUY' && mark <= n(trade.stopLoss)) || (trade.direction === 'SELL' && mark >= n(trade.stopLoss))) {
    const reason = trade.trailingLevelR > 0 ? 'TRAILING_STOP_HIT' : (trade.breakEvenActive ? 'BREAK_EVEN' : 'STOP_LOSS');
    await closePaperTrade(trade, mark, reason);
    return { closed: true, reason };
  }

  return { closed: false };
}

function shouldRunAiManagement(trade) {
  const last = state.lastManageAt.get(trade.symbol) || 0;
  if (Date.now() - last < AI_MANAGE_INTERVAL_MS) return false;
  if (state.managementLocks.has(trade.symbol)) return false;
  return true;
}

async function manageTradeWithAi(trade, quote) {
  if (!shouldRunAiManagement(trade)) return;
  state.managementLocks.add(trade.symbol);
  state.lastManageAt.set(trade.symbol, Date.now());
  try {
    if (!state.openTrades.has(trade.symbol)) return;
    const decision = await aiManagementDecision(trade, quote);
    trade.aiLastManagement = decision;
    if (state.mongoReady && trade._id) await Trade.updateOne({ _id: trade._id }, { $set: { aiLastManagement: decision } });
    await journal('AI_MANAGEMENT', { symbol: trade.symbol, direction: trade.direction, tradeId: trade._id, message: decision.decision, data: decision });
    if (decision.decision !== 'CLOSE') return;
    if (!state.openTrades.has(trade.symbol)) return;
    const freshQuote = await fetchSingleQuote(trade.symbol);
    if (!freshQuote) return;
    const mechanical = await applyMechanicalProtection(trade, freshQuote);
    if (mechanical.closed) return;
    const exitPrice = tradeMarkPrice(trade, freshQuote);
    await closePaperTrade(trade, exitPrice, 'AI_CLOSE', { aiManagement: decision });
  } catch (error) {
    console.error(`AI manage ${trade.symbol}:`, safeError(error));
  } finally { state.managementLocks.delete(trade.symbol); }
}

async function quoteLoop() {
  if (state.quoteLoopBusy) return;
  state.quoteLoopBusy = true;
  try {
    const symbols = [...state.openTrades.keys()];
    if (!symbols.length) return;
    let quoteMap = await fetchLatestQuotes(symbols);
    const missing = symbols.filter(symbol => !quoteMap.has(symbol));
    if (missing.length) {
      const fallback = await mapWithConcurrency(missing, 4, async symbol => ({ symbol, quote: await fetchSingleQuote(symbol) }));
      for (const item of fallback) if (item?.quote && item?.symbol) quoteMap.set(item.symbol, item.quote);
    }
    for (const symbol of symbols) {
      const trade = state.openTrades.get(symbol), quote = quoteMap.get(symbol);
      if (!trade || !quote) continue;
      state.latestQuotes.set(symbol, quote);
      await updateTradeExcursions(trade, quote);
      const protection = await applyMechanicalProtection(trade, quote);
      if (protection.closed) continue;
      manageTradeWithAi(trade, quote).catch(error => { console.error(`Manage background ${symbol}:`, safeError(error)); });
    }
  } catch (error) { console.error('Quote loop:', safeError(error)); } finally { state.quoteLoopBusy = false; }
}

// ============================================================
// SCANNER & INITIALIZATION
// ============================================================

async function initializeSymbol(symbol) {
  try {
    const bars = await fetchOhlc(symbol, TIMEFRAME, HISTORY_LIMIT);
    if (bars.length < CORE_MIN_HISTORY) throw new Error(`Only ${bars.length} bars`);
    const latest = bars[bars.length - 1];
    const pair = { symbol, initialized: true, bars, lastClosedBarTime: latest.time, lastScannedBarTime: null, lastError: null, initializedAt: new Date() };
    state.pairState.set(symbol, pair);
    console.log(`✅ ${symbol} | bars=${bars.length} | EMA200=${bars.length >= EMA200_CONTEXT_HISTORY ? 'READY' : 'CONTEXT-WARMUP'}`);
    return pair;
  } catch (error) {
    state.pairState.set(symbol, { symbol, initialized: false, bars: [], lastClosedBarTime: null, lastScannedBarTime: null, lastError: safeError(error), initializedAt: null });
    return null;
  }
}

async function initializeMarket() {
  console.log(`📡 Initializing ${INSTRUMENTS.length} instruments...`);
  await mapWithConcurrency(INSTRUMENTS, OHLC_CONCURRENCY, initializeSymbol);
  const ready = pairReadyCount();
  state.marketReady = ready > 0;
  console.log(`📡 Market initialized: ${ready}/${INSTRUMENTS.length}`);
  if (!ready) throw new Error('No instruments initialized');
  await journal('MARKET_READY', { message: `${ready}/${INSTRUMENTS.length} instruments initialized` });
}

async function processNewClosedBar(symbol, bars, quote) {
  if (state.scanLocks.has(symbol)) return;
  state.scanLocks.add(symbol);
  try {
    if (!Array.isArray(bars) || bars.length < CORE_MIN_HISTORY) return;
    const latestBar = bars[bars.length - 1], barKey = `${symbol}:${latestBar.time}`;
    if (state.processedBars.has(barKey)) return;
    state.processedBars.add(barKey);
    if (state.processedBars.size > 10000) { state.processedBars.clear(); state.processedBars.add(barKey); }
    
    const technical = buildTechnicalIntelligence(symbol, bars);
    if (!technical) return;
    technical.timeframe = TIMEFRAME; state.scannedBars++;
    
    if (state.openTrades.has(symbol)) { state.skippedSignals++; return; }
    if (!quote) quote = await fetchSingleQuote(symbol);
    if (!quote) { state.skippedSignals++; return; }
    
    state.latestQuotes.set(symbol, quote);

    // NEW: Check News Filter before consulting AI
    const newsCheck = isVolatileNewsApproaching(symbol);
    if (newsCheck.risk) {
      console.log(`📰 TRADE BLOCKED: ${symbol} due to High Impact News (${newsCheck.event}) in ${newsCheck.diffMins} mins.`);
      return;
    }

    const multiTimeframe = await buildMultiTimeframeContext(symbol, bars);
    const aiDecision = await aiEntryCommander(symbol, technical, multiTimeframe, quote);
    
    if (aiDecision.decision === 'NO_TRADE') {
      state.skippedSignals++;
      await recordSignal({ symbol, technical, multiTimeframe, aiDecision, executed: false, skipReason: aiDecision.failClosed ? 'AI_FAIL_CLOSED' : 'AI_NO_TRADE' });
      console.log(`⏭️ ${symbol} | AI=NO_TRADE | ${n(aiDecision.confidence, 0).toFixed(0)}% | ${aiDecision.reason}`); return;
    }
    
    const executionLevels = validateAiStopLoss(aiDecision, quote, technical);
    if (!executionLevels.valid) {
      state.skippedSignals++;
      await recordSignal({ symbol, technical, multiTimeframe, aiDecision, executionLevels: null, executed: false, skipReason: executionLevels.reason });
      console.log(`🛑 ${symbol} ${aiDecision.decision} rejected | ${executionLevels.reason}`); return;
    }
    
    // NEW: Calculate size dynamically using AI confidence
    const sizing = calculatePositionSize(executionLevels.entry, executionLevels.stopLoss, aiDecision.confidence);
    if (!sizing) { state.skippedSignals++; return; }
    
    const riskCheck = canOpenNewTrade(symbol, sizing.riskAmount);
    if (!riskCheck.allowed) {
      state.skippedSignals++; await recordSignal({ symbol, technical, multiTimeframe, aiDecision, executionLevels, executed: false, skipReason: riskCheck.reason });
      console.log(`🛑 ${symbol} ${aiDecision.decision} risk reject | ${riskCheck.reason}`); return;
    }
    
    const opened = await openPaperTrade({ symbol, technical, multiTimeframe, quote, aiDecision, executionLevels, sizing });
    if (!opened.opened) { state.skippedSignals++; return; }
    await recordSignal({ symbol, technical, multiTimeframe, aiDecision, executionLevels, executed: true, skipReason: '' });
  } catch (error) { state.skippedSignals++; console.error(`Process ${symbol}:`, safeError(error)); } finally { state.scanLocks.delete(symbol); }
}

async function refreshSymbol(symbol) {
  let pair = state.pairState.get(symbol);
  if (!pair?.initialized) { await initializeSymbol(symbol); return; }
  try {
    const bars = await fetchOhlc(symbol, TIMEFRAME, HISTORY_LIMIT);
    if (bars.length < CORE_MIN_HISTORY) throw new Error(`Only ${bars.length} bars`);
    const latest = bars[bars.length - 1];
    pair.bars = bars; pair.lastError = null;
    const newClosedBar = latest.time !== pair.lastClosedBarTime;
    if (!newClosedBar) return;
    pair.lastClosedBarTime = latest.time;
    const quote = await fetchSingleQuote(symbol);
    await processNewClosedBar(symbol, bars, quote);
    pair.lastScannedBarTime = latest.time;
  } catch (error) { pair.lastError = safeError(error); state.lastMarketError = `${symbol}: ${safeError(error)}`; }
}

async function scanLoop() {
  if (state.scanLoopBusy) return;
  state.scanLoopBusy = true;
  try { await mapWithConcurrency(INSTRUMENTS, OHLC_CONCURRENCY, refreshSymbol); } catch (error) { console.error('Scan loop:', safeError(error)); } finally { state.scanLoopBusy = false; }
}

async function manualCloseSymbol(symbol, reason = 'MANUAL_CLOSE') {
  symbol = String(symbol || '').toUpperCase();
  const trade = state.openTrades.get(symbol);
  if (!trade) return { ok: false, reason: 'NO_OPEN_TRADE' };
  const quote = await fetchSingleQuote(symbol);
  if (!quote) return { ok: false, reason: 'QUOTE_UNAVAILABLE' };
  const protection = await applyMechanicalProtection(trade, quote);
  if (protection.closed) return { ok: true, reason: protection.reason };
  const exitPrice = tradeMarkPrice(trade, quote), closed = await closePaperTrade(trade, exitPrice, reason);
  return { ok: Boolean(closed), trade: closed };
}

async function manualCloseAll() {
  const symbols = [...state.openTrades.keys()], results = [];
  for (const symbol of symbols) { try { results.push(await manualCloseSymbol(symbol, 'MANUAL_CLOSE_ALL')); } catch (error) { results.push({ ok: false, symbol, reason: safeError(error) }); } }
  return results;
}

function startRuntimeLoops() {
  if (state.loopsStarted) return;
  state.loopsStarted = true;
  setInterval(() => { quoteLoop().catch(error => { console.error('Quote timer:', safeError(error)); }); }, QUOTE_POLL_MS);
  setInterval(() => { scanLoop().catch(error => { console.error('Scan timer:', safeError(error)); }); }, SCAN_TIMER_MS);
}

// ============================================================
// DASHBOARD & EXPRESS SERVER
// ============================================================

function getOpenTradesArray() { return [...state.openTrades.values()]; }
function getOpenFloatingPnl() {
  let total = 0;
  for (const trade of state.openTrades.values()) {
    const quote = state.latestQuotes.get(trade.symbol); if (!quote) continue;
    const mark = tradeMarkPrice(trade, quote); total += calculateTradePnl(trade, mark);
  }
  return total;
}

function buildStatus() {
  const balance = accountBalance(), floatingPnl = getOpenFloatingPnl(), equity = balance + floatingPnl, openRisk = currentPortfolioRiskUsd(), portfolioCap = portfolioRiskCapUsd();
  return { version: VERSION, mode: MODE, liveTrading: LIVE_TRADING, paperOnly: MODE === 'PAPER' && LIVE_TRADING === false, startedAt: state.startedAt, uptimeSeconds: Math.floor(process.uptime()), marketReady: state.marketReady, instruments: INSTRUMENTS.length, instrumentsReady: pairReadyCount(), timeframe: TIMEFRAME, balance, floatingPnl, equity, realizedPnl: n(account?.realizedPnl, 0), startingBalance: PAPER.startingBalance, totalTrades: n(account?.totalTrades, 0), wins: n(account?.wins, 0), losses: n(account?.losses, 0), breakeven: n(account?.breakeven, 0), openTrades: state.openTrades.size, maxOpenTrades: PAPER.maxOpenTrades, capitalRiskSafetyCapPct: PAPER.maxCapitalRiskPct, portfolioRiskCapPct: PAPER.portfolioRiskCapPct, openRiskUsd: openRisk, portfolioRiskCapUsd: portfolioCap, portfolioRiskUsedPct: portfolioCap > 0 ? (openRisk / portfolioCap) * 100 : 0, riskReward: `1:${RULES.riskReward}`, breakEvenTriggerR: RULES.breakEvenTriggerR, scannedBars: state.scannedBars, executedSignals: state.executedSignals, skippedSignals: state.skippedSignals, mongoReady: state.mongoReady, telegramReady: Boolean(bot), geminiConfigured: Boolean(GEMINI_API_KEY), geminiModel: GEMINI_MODEL, lastMarketError: state.lastMarketError || null };
}

const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '250kb' }));
app.use(express.urlencoded({ extended: false }));

app.get('/health', (req, res) => { res.status(200).json({ ok: true, version: VERSION, mode: MODE, liveTrading: LIVE_TRADING, marketReady: state.marketReady, openTrades: state.openTrades.size, uptime: Math.floor(process.uptime()), timestamp: new Date().toISOString() }); });
app.get('/api/status', (req, res) => { res.json(buildStatus()); });
app.get('/api/trades', (req, res) => { res.json({ openTrades: getOpenTradesArray().map(trade => { const quote = state.latestQuotes.get(trade.symbol); const mark = quote ? tradeMarkPrice(trade, quote) : n(trade.lastMarkPrice, trade.entryPrice); return { id: String(trade._id || ''), symbol: trade.symbol, direction: trade.direction, entryPrice: trade.entryPrice, markPrice: mark, stopLoss: trade.stopLoss, initialStopLoss: trade.initialStopLoss, takeProfit: trade.takeProfit, breakEvenTriggerPrice: trade.breakEvenTriggerPrice, breakEvenActive: Boolean(trade.breakEvenActive), riskDistance: trade.riskDistance, riskAmount: trade.riskAmount, quantity: trade.quantity, currentR: quote ? currentR(trade, quote) : 0, floatingPnl: Number.isFinite(mark) ? calculateTradePnl(trade, mark) : 0, mfeR: n(trade.mfeR, 0), maeR: n(trade.maeR, 0), openedAt: trade.openedAt, aiConfidence: n(trade.aiEntryDecision?.confidence, 0), aiSetup: trade.aiEntryDecision?.setup || '', aiReason: trade.aiEntryDecision?.reason || '' }; }) }); });
app.post('/api/close/:symbol', async (req, res) => { try { const symbol = String(req.params.symbol || '').trim().toUpperCase(); if (!INSTRUMENTS.includes(symbol)) return res.status(400).json({ ok: false, reason: 'INVALID_SYMBOL' }); res.json(await manualCloseSymbol(symbol, 'DASHBOARD_MANUAL_CLOSE')); } catch (error) { res.status(500).json({ ok: false, error: safeError(error) }); } });
app.post('/api/close-all', async (req, res) => { try { res.json({ ok: true, results: await manualCloseAll() }); } catch (error) { res.status(500).json({ ok: false, error: safeError(error) }); } });
app.get('/', (req, res) => { res.status(200).type('html').send(`<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>${VERSION}</title><style>body{margin:0;padding:0;background:#070b12;color:#eef4ff;font-family:sans-serif}.container{width:min(1500px, 96%);margin:0 auto;padding:24px 0}h1{font-size:24px;color:#8ab4ff}table{width:100%;border-collapse:collapse}th,td{padding:10px;border-bottom:1px solid #1b2a40;text-align:left}</style></head><body><div class="container"><h1>${VERSION}</h1><p>Check /api/status or /api/trades for raw JSON data. UI simplified for brevity.</p></div></body></html>`); });

let httpServer = null;
async function startWebServer() { 
  return new Promise((resolve, reject) => { 
    httpServer = app.listen(PORT, '0.0.0.0', () => { 
      console.log(`🌐 Dashboard listening on port ${PORT}`); 
      resolve(); 
    }); 
    httpServer.on('error', reject); 
  }); 
}

// ============================================================
// BOOTSTRAP / SHUTDOWN
// ============================================================

async function restoreOpenTrades() {
  if (!state.mongoReady) return;
  try {
    const trades = await Trade.find({ accountKey: PAPER.accountKey, status: 'OPEN' }).lean();
    for (const trade of trades) if (INSTRUMENTS.includes(trade.symbol)) state.openTrades.set(trade.symbol, trade);
    console.log(`♻️ Restored ${state.openTrades.size} open PAPER trades`);
  } catch (error) { console.error('Restore trades:', safeError(error)); }
}

function printStartupBanner() { console.log(`\n🚀 ${VERSION}\n🧪 PAPER ONLY — NO LIVE FUNDS ACCESS\n`); }
function validateStartupConfig() { if (MODE !== 'PAPER' || LIVE_TRADING !== false || RULES.riskReward !== 2 || PAPER.maxCapitalRiskPct > 1) throw new Error('Config lock violated'); }

async function boot() {
  try {
    printStartupBanner(); validateStartupConfig(); await startWebServer();
    try { await initMongo(); await restoreOpenTrades(); } catch (error) { console.error('Mongo boot:', safeError(error)); }
    await initTelegram();
    try { await initializeMarket(); } catch (error) { console.error('Initial market load:', safeError(error)); state.marketReady = false; }
    startRuntimeLoops();
    if (state.openTrades.size) quoteLoop().catch(() => {});
    scanLoop().catch(() => {});
    console.log(`\n✅ LOMY FOREX V1.5 IS RUNNING (PRO EDITION)\n🎯 Strict 1:2 R:R\n🔒 ZERO FUND ACCESS\n`);
  } catch (error) { console.error('❌ FATAL BOOT ERROR:', safeError(error)); process.exitCode = 1; }
}

let shuttingDown = false;
async function gracefulShutdown(signal) {
  if (shuttingDown) return; shuttingDown = true; console.log(`\n🛑 ${signal} received. Shutting down...`);
  try { if (httpServer) await new Promise(resolve => { httpServer.close(() => resolve()); setTimeout(resolve, 3000); }); } catch (_) {}
  try { if (mongoose.connection.readyState !== 0) await mongoose.disconnect(); } catch (_) {}
  console.log('✅ Shutdown complete'); process.exit(0);
}

process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('unhandledRejection', reason => console.error('⚠️ Unhandled rejection:', reason));
process.on('uncaughtException', error => console.error('❌ Uncaught exception:', safeError(error)));

Object.freeze(RULES); Object.freeze(PAPER); Object.freeze(DYNAMIC_RISK);
boot();
