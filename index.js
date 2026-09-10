'use strict';

const axios = require('axios');
const express = require('express');
const mongoose = require('mongoose');
const { Telegraf } = require('telegraf');

// ═══════════════════════════════════════════════════════════
// LOMY FOREX V2.0 — GEMINI FALLBACK COMMANDER — PAPER ONLY
// ═══════════════════════════════════════════════════════════
const VERSION = 'LOMY FOREX V2.0 GEMINI FALLBACK';
const MODE = 'PAPER';
const LIVE_TRADING = false;

// ── ENV ─────────────────────────────────────────────────────
const PORT = Number(process.env.PORT || 10000);
const TELEGRAM_BOT_TOKEN = String(process.env.TELEGRAM_BOT_TOKEN || '').trim();
const TELEGRAM_CHAT_ID_ENV = String(process.env.TELEGRAM_CHAT_ID || '').trim();
const TWELVE_DATA_API_KEY = String(process.env.TWELVE_DATA_API_KEY || '').trim();
const MONGODB_URI = String(process.env.MONGODB_URI || '').trim();
const GEMINI_API_KEY = String(process.env.GEMINI_API_KEY || '').trim();

// ── GEMINI FALLBACK CHAIN ───────────────────────────────────
// الترتيب: الأعلى كوتا أولاً، ثم الأحدث جودة
const GEMINI_MODELS = [
  { name: 'gemini-3.1-flash-lite',   dailyCap: 500,  priority: 1 },
  { name: 'gemini-3.5-flash',        dailyCap: 100,  priority: 2 },
  { name: 'gemini-3-flash-preview',  dailyCap: 20,   priority: 3 },
  { name: 'gemini-2.5-flash-lite',   dailyCap: 20,   priority: 4 }
];
const GEMINI_TOTAL_DAILY_CAP = 500;
const GEMINI_ENTRY_DAILY_CAP = 400;
const GEMINI_MANAGE_DAILY_CAP = 100;
const GEMINI_ENTRY_MIN_GAP_MS = 30 * 60 * 1000;   // 30 دقيقة بين كل دخول ودخول
const GEMINI_CALL_GAP_MS = 4000;
const GEMINI_CIRCUIT_RESET_MS = 60 * 60 * 1000;   // ساعة لو كل الموديلات فشلت

// ── MARKET / QUOTA ──────────────────────────────────────────
const TIMEFRAME = '15m';
const TIMEFRAME_MS = 15 * 60 * 1000;
const CORE_MIN_HISTORY = 60;
const INITIAL_HISTORY = 1000;         // ← تم تقليله من 5000
const REFRESH_HISTORY = 24;
const TWELVE_MIN_GAP_MS = 8500;
const TWELVE_SCAN_SOFT_CAP = 650;
const TWELVE_HARD_LOCAL_CAP = 760;
const NEWS_REFRESH_MS = 12 * 60 * 60 * 1000;
const NEWS_BLOCK_MIN = 30;
const NEWS_CACHE_MAX_AGE_MS = 36 * 60 * 60 * 1000;

// ── INSTRUMENTS ─────────────────────────────────────────────
const ALL_INSTRUMENTS = [
  'EURUSD','GBPUSD','USDJPY','USDCHF','AUDUSD','NZDUSD','USDCAD',
  'EURGBP','EURJPY','EURCHF','EURAUD','EURNZD','EURCAD',
  'GBPJPY','GBPCHF','GBPAUD','GBPNZD','GBPCAD',
  'AUDJPY','AUDCHF','AUDNZD','AUDCAD',
  'NZDJPY','NZDCHF','NZDCAD',
  'CADJPY','CADCHF','CHFJPY',
  'GBPSGD','EURSGD','XAUUSD'
];
const DEFAULT_ACTIVE = ['EURUSD','GBPUSD','USDJPY','AUDUSD','USDCAD'];
const ACTIVE_SYMBOLS = (() => {
  const raw = String(process.env.ACTIVE_SYMBOLS || '').trim();
  if (!raw) return DEFAULT_ACTIVE;
  const x = [...new Set(raw.split(',').map(s => s.trim().toUpperCase()).filter(s => ALL_INSTRUMENTS.includes(s)))];
  return x.length ? x : DEFAULT_ACTIVE;
})();

// ── FROZEN RULES ────────────────────────────────────────────
const RULES = Object.freeze({
  riskReward: 2,
  breakEvenTriggerR: 0.6,
  partialTpTriggerR: 2,
  trailingStartStopR: 1,
  trailingStepR: 0.5,
  minStopAtr: 0.25,
  maxStopAtr: 6,
  maxSpreadRiskFraction: 0.2,
  minEntryConfidence: 62,
  minCloseConfidence: 68,
  // Confluence
  confluenceThreshold: 8,        // ← الجديد: عتبة الفرصة القوية
  // Dynamic ATR trailing
  atrTrailMult: 1.2,
  atrTrailStartR: 1.5
});

const PAPER = Object.freeze({
  startingBalance: 300,
  maxCapitalRiskPct: 1,
  portfolioRiskCapPct: 4,
  maxOpenTrades: 31,
  accountKey: 'lomy-forex-v2-gemini-fallback-300usd'
});

const DYNAMIC_RISK = Object.freeze({
  highConfidence: 85, highRiskPct: 1,
  medConfidence: 75,  medRiskPct: 0.75,
  lowConfidence: 62,  lowRiskPct: 0.5
});

// ── CAPITAL PROTECTION ──────────────────────────────────────
const PROTECTION = Object.freeze({
  dailyLossLimitPct: 3,
  weeklyLossLimitPct: 6,
  consecutiveLossReduce: 2,      // بعد خسارتين → نص الحجم
  cooldownAfterLossMs: 30 * 60 * 1000,
  correlationThreshold: 0.7,     // منع الصفقات المترابطة
  // الجلسات المسموحة (UTC)
  allowedSessions: ['LONDON','NEW_YORK'],
  blockFridayAfterUTC: 20,
  blockSundayBeforeUTC: 22
});

// ── TECHNICAL PARAMS ────────────────────────────────────────
const TECH = Object.freeze({
  emaFast: 9, emaMedium: 21, emaTrend: 50, emaLong: 100, emaMacro: 200,
  rsiLen: 14, cmoLen: 9, atrLen: 14, adxLen: 14,
  stochasticLen: 14, stochasticSmooth: 3,
  rocLen: 12, bbLen: 20, bbStd: 2,
  keltnerLen: 20, keltnerAtrLen: 14, keltnerMult: 1.5,
  volumeLen: 20, srLen: 40, fibLookback: 60,
  swingLeft: 3, swingRight: 3,
  liquidityLookback: 20, vwapLookback: 50, mfiLen: 14
});

const AI = Object.freeze({
  entryCommanderEnabled: true,
  managementEnabled: true,
  memoryClosedTrades: 40,
  temperature: 0.1,
  timeoutMs: 20000
});

// ── STATE ───────────────────────────────────────────────────
const state = {
  startedAt: new Date(),
  mongoReady: false,
  telegramReady: false,
  telegramPollingReady: false,
  marketReady: false,
  geminiReady: false,
  newsReady: false,
  scanBusy: false,
  loopsStarted: false,
  lastMarketError: null,
  lastAiError: null,
  lastNewsError: null,
  scannedBars: 0,
  aiEntryCalls: 0,
  aiManageCalls: 0,
  aiBuyDecisions: 0,
  aiSellDecisions: 0,
  aiNoTradeDecisions: 0,
  aiCloseDecisions: 0,
  aiHoldDecisions: 0,
  aiFallbackUsed: 0,
  executedSignals: 0,
  skippedSignals: 0,
  pairState: new Map(),
  openTrades: new Map(),
  tradeLocks: new Set(),
  twelveBlockedUntil: 0,
  geminiBlockedUntil: 0,
  geminiActiveModelIndex: 0,   // ← النموذج الحالي في الـ fallback
  geminiModelFailures: new Map(),
  nextScanAt: null,
  lastEntryAiAt: 0,
  dailyPnl: 0,
  weeklyPnl: 0,
  consecutiveLosses: 0,
  lastLossAt: 0,
  dayStartBalance: 0,
  weekStartBalance: 0
};

let account = null;
let telegramBot = null;
let telegramChatId = TELEGRAM_CHAT_ID_ENV;
let economicNews = [];
let twelveChain = Promise.resolve();
let geminiChain = Promise.resolve();
let accountSaveChain = Promise.resolve();
let lastTwelveAt = 0;
let lastGeminiAt = 0;
let telegramRetryTimer = null;

const http = axios.create({ timeout: 20000, headers: { 'User-Agent': 'LOMY-FOREX-V2.0' } });

// ═══════════════════════════════════════════════════════════
// UTILITIES
// ═══════════════════════════════════════════════════════════
function n(v, f = 0) { const x = Number(v); return Number.isFinite(x) ? x : f; }
function clamp(v, a, b) { return Math.max(a, Math.min(b, v)); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function sum(a) { return a.filter(Number.isFinite).reduce((x, y) => x + y, 0); }
function average(a) { const v = (a || []).filter(Number.isFinite); return v.length ? sum(v) / v.length : NaN; }
function standardDeviation(a) {
  const v = (a || []).filter(Number.isFinite);
  if (!v.length) return NaN;
  const m = average(v);
  return Math.sqrt(v.reduce((s, x) => s + (x - m) ** 2, 0) / v.length);
}
function last(a) { return Array.isArray(a) && a.length ? a[a.length - 1] : undefined; }
function pctChange(a, b) {
  a = n(a, NaN); b = n(b, NaN);
  return Number.isFinite(a) && Number.isFinite(b) && a !== 0 ? (b - a) / Math.abs(a) * 100 : NaN;
}
function safeError(e) {
  const d = e?.response?.data;
  return d?.error?.message || d?.message || d?.error || e?.message || String(e);
}
function fmtMoney(v) { return '$' + n(v).toFixed(2); }
function fmtPrice(v, s = '') {
  if (!Number.isFinite(Number(v))) return 'n/a';
  v = Number(v);
  if (s === 'XAUUSD') return v.toFixed(2);
  if (s.endsWith('JPY')) return v.toFixed(3);
  return v.toFixed(5);
}
function parseTime(v) {
  if (v instanceof Date) return v.getTime();
  if (typeof v === 'number') return v;
  let s = String(v || '').trim();
  if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(s)) s = s.replace(' ', 'T') + 'Z';
  const t = new Date(s).getTime();
  return Number.isFinite(t) ? t : 0;
}
function barTimeMs(b) { return parseTime(b?.openTime); }
function highestHigh(b) { return b?.length ? Math.max(...b.map(x => x.high)) : NaN; }
function lowestLow(b) { return b?.length ? Math.min(...b.map(x => x.low)) : NaN; }
function utcDayKey() { return new Date().toISOString().slice(0, 10); }
function pacificDayKey(date = new Date()) {
  const p = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/Los_Angeles',
    year: 'numeric', month: '2-digit', day: '2-digit'
  }).formatToParts(date);
  const o = {};
  for (const x of p) o[x.type] = x.value;
  return `${o.year}-${o.month}-${o.day}`;
}
function nextUtcMidnightMs() {
  const d = new Date();
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() + 1, 0, 0, 5);
}
function nextPacificDayMs() {
  const key = pacificDayKey();
  let t = Date.now() + 60000;
  for (let i = 0; i < 1800; i++, t += 60000)
    if (pacificDayKey(new Date(t)) !== key) return t + 5000;
  return Date.now() + 24 * 60 * 60 * 1000;
}
function marketSymbols() {
  return [...new Set([...ACTIVE_SYMBOLS, ...state.openTrades.keys()])];
}
function correlationKey(symbol) {
  // مفتاح مبسط للكشف عن الترابط: عملة أساسية مشتركة
  if (symbol === 'XAUUSD') return 'XAU';
  return symbol.slice(0, 3);
}

// ═══════════════════════════════════════════════════════════
// BAR NORMALIZATION / AGGREGATION
// ═══════════════════════════════════════════════════════════
function normalizeBars(raw) {
  if (!Array.isArray(raw)) return [];
  return raw.map(x => ({
    openTime: x.openTime || x.datetime || x.time || x.timestamp,
    open: n(x.open, NaN),
    high: n(x.high, NaN),
    low: n(x.low, NaN),
    close: n(x.close, NaN),
    volume: n(x.tickVolume, n(x.volume, 0)),
    isOpen: x.isOpen === true
  })).filter(x => x.openTime && [x.open, x.high, x.low, x.close].every(Number.isFinite))
    .sort((a, b) => barTimeMs(a) - barTimeMs(b));
}

function closed15Bars(b) {
  const now = Date.now();
  return (b || []).filter(x => !x.isOpen && barTimeMs(x) > 0 && barTimeMs(x) + TIMEFRAME_MS <= now + 5000);
}

function mergeBars(a, b, max = INITIAL_HISTORY) {
  const m = new Map();
  for (const x of [...(a || []), ...(b || [])]) {
    const t = barTimeMs(x);
    if (t) m.set(t, x);
  }
  return [...m.values()].sort((x, y) => barTimeMs(x) - barTimeMs(y)).slice(-max);
}

function aggregateBars(bars, minutes) {
  const ms = minutes * 60000;
  const needed = Math.max(1, Math.round(minutes / 15));
  const m = new Map();
  const now = Date.now();
  for (const b of bars || []) {
    const t = barTimeMs(b);
    if (!t) continue;
    const k = Math.floor(t / ms) * ms;
    let x = m.get(k);
    if (!x) {
      x = {
        openTime: new Date(k).toISOString(),
        open: b.open, high: b.high, low: b.low, close: b.close,
        volume: n(b.volume), isOpen: false, _count: 1
      };
      m.set(k, x);
    } else {
      x.high = Math.max(x.high, b.high);
      x.low = Math.min(x.low, b.low);
      x.close = b.close;
      x.volume += n(b.volume);
      x._count++;
    }
  }
  return [...m.values()]
    .filter(x => barTimeMs(x) + ms <= now + 5000 && x._count >= needed)
    .sort((a, b) => barTimeMs(a) - barTimeMs(b))
    .map(({ _count, ...x }) => x);
}

// ═══════════════════════════════════════════════════════════
// INDICATORS
// ═══════════════════════════════════════════════════════════
function emaSeries(v, l) {
  if (!Array.isArray(v) || v.length < l) return [];
  const o = new Array(v.length).fill(NaN);
  const k = 2 / (l + 1);
  const seed = v.slice(0, l);
  if (!seed.every(Number.isFinite)) return o;
  o[l - 1] = sum(seed) / l;
  for (let i = l; i < v.length; i++)
    if (Number.isFinite(v[i]) && Number.isFinite(o[i - 1]))
      o[i] = v[i] * k + o[i - 1] * (1 - k);
  return o;
}
function emaLast(v, l) { const s = emaSeries(v, l); return s[s.length - 1]; }

function trueRangeSeries(b) {
  const o = [];
  for (let i = 1; i < (b?.length || 0); i++)
    o.push(Math.max(
      b[i].high - b[i].low,
      Math.abs(b[i].high - b[i - 1].close),
      Math.abs(b[i].low - b[i - 1].close)
    ));
  return o;
}
function atrLast(b, l = 14) {
  const r = trueRangeSeries(b);
  if (r.length < l) return NaN;
  let a = sum(r.slice(0, l)) / l;
  for (let i = l; i < r.length; i++) a = (a * (l - 1) + r[i]) / l;
  return a;
}
function rsiLast(v, l = 14) {
  if (!v || v.length <= l) return NaN;
  let g = 0, loss = 0;
  for (let i = 1; i <= l; i++) {
    const c = v[i] - v[i - 1];
    c >= 0 ? g += c : loss += Math.abs(c);
  }
  let ag = g / l, al = loss / l;
  for (let i = l + 1; i < v.length; i++) {
    const c = v[i] - v[i - 1];
    ag = (ag * (l - 1) + Math.max(c, 0)) / l;
    al = (al * (l - 1) + Math.max(-c, 0)) / l;
  }
  return al === 0 ? 100 : 100 - 100 / (1 + ag / al);
}
function cmoLast(v, l = 9) {
  if (!v || v.length <= l) return NaN;
  let up = 0, dn = 0;
  for (let i = v.length - l; i < v.length; i++) {
    const c = v[i] - v[i - 1];
    if (c > 0) up += c; else if (c < 0) dn += -c;
  }
  return up + dn ? 100 * (up - dn) / (up + dn) : 0;
}
function macdLast(v) {
  if (!v || v.length < 35) return { macd: NaN, signal: NaN, histogram: NaN };
  const f = emaSeries(v, 12), s = emaSeries(v, 26), m = [];
  for (let i = 0; i < v.length; i++)
    if (Number.isFinite(f[i]) && Number.isFinite(s[i])) m.push(f[i] - s[i]);
  if (m.length < 9) return { macd: NaN, signal: NaN, histogram: NaN };
  const sg = emaSeries(m, 9), mv = last(m), sv = last(sg);
  return { macd: mv, signal: sv, histogram: mv - sv };
}
function stochasticLast(b, l = 14) {
  if (!b || b.length < l) return { k: NaN, d: NaN };
  const ks = [];
  for (let i = Math.max(l - 1, b.length - 5); i < b.length; i++) {
    const s = b.slice(i - l + 1, i + 1);
    const h = highestHigh(s), lo = lowestLow(s), r = h - lo;
    ks.push(r === 0 ? 50 : (b[i].close - lo) / r * 100);
  }
  return { k: last(ks), d: average(ks.slice(-TECH.stochasticSmooth)) };
}
function williamsRLast(b, l = 14) {
  if (!b || b.length < l) return NaN;
  const s = b.slice(-l), h = highestHigh(s), lo = lowestLow(s), c = last(s).close;
  return h === lo ? -50 : -100 * (h - c) / (h - lo);
}
function rocLast(v, l = 12) {
  return !v || v.length <= l ? NaN : pctChange(v[v.length - 1 - l], last(v));
}
function bollingerLast(v, l = 20, m = 2) {
  if (!v || v.length < l) return { middle: NaN, upper: NaN, lower: NaN, widthPct: NaN };
  const s = v.slice(-l), mid = average(s), d = standardDeviation(s), u = mid + m * d, lo = mid - m * d;
  return { middle: mid, upper: u, lower: lo, widthPct: mid ? (u - lo) / Math.abs(mid) * 100 : NaN };
}
function keltnerLast(b) {
  const c = (b || []).map(x => x.close);
  const mid = emaLast(c, TECH.keltnerLen);
  const a = atrLast(b, TECH.keltnerAtrLen);
  return { middle: mid, upper: mid + TECH.keltnerMult * a, lower: mid - TECH.keltnerMult * a };
}
function obvLast(b) {
  let o = 0;
  for (let i = 1; i < (b?.length || 0); i++) {
    const v = n(b[i].volume);
    if (b[i].close > b[i - 1].close) o += v;
    else if (b[i].close < b[i - 1].close) o -= v;
  }
  return o;
}
function mfiLast(b, l = 14) {
  if (!b || b.length <= l) return NaN;
  let p = 0, ng = 0;
  for (let i = b.length - l; i < b.length; i++) {
    const t = (b[i].high + b[i].low + b[i].close) / 3;
    const pt = (b[i - 1].high + b[i - 1].low + b[i - 1].close) / 3;
    const f = t * n(b[i].volume);
    if (t > pt) p += f; else if (t < pt) ng += f;
  }
  if (!p && !ng) return 50;
  if (!ng) return 100;
  return 100 - 100 / (1 + p / ng);
}
function vwapLast(b, l = 50) {
  const s = (b || []).slice(-l);
  let w = 0, v = 0;
  for (const x of s) {
    const t = (x.high + x.low + x.close) / 3;
    const q = n(x.volume);
    w += t * q; v += q;
  }
  return v > 0 ? w / v : average(s.map(x => (x.high + x.low + x.close) / 3));
}
function candleContext(b) {
  if (!b) return { direction: 'UNKNOWN', bodyRatio: 0, upperWickRatio: 0, lowerWickRatio: 0 };
  const r = Math.max(b.high - b.low, Number.EPSILON);
  const body = Math.abs(b.close - b.open);
  return {
    direction: b.close > b.open ? 'BULLISH' : b.close < b.open ? 'BEARISH' : 'DOJI',
    bodyRatio: body / r,
    upperWickRatio: (b.high - Math.max(b.open, b.close)) / r,
    lowerWickRatio: (Math.min(b.open, b.close) - b.low) / r
  };
}
function supportResistance(b, l = 40) {
  const s = (b || []).slice(-(l + 1), -1);
  return { support: lowestLow(s), resistance: highestHigh(s) };
}
function findSwings(b, left = 3, right = 3) {
  const highs = [], lows = [];
  if (!b || b.length < left + right + 1) return { highs, lows };
  for (let i = left; i < b.length - right; i++) {
    let sh = true, sl = true;
    for (let j = 1; j <= left; j++) {
      if (b[i].high <= b[i - j].high) sh = false;
      if (b[i].low >= b[i - j].low) sl = false;
    }
    for (let j = 1; j <= right; j++) {
      if (b[i].high <= b[i + j].high) sh = false;
      if (b[i].low >= b[i + j].low) sl = false;
    }
    if (sh) highs.push({ index: i, price: b[i].high, time: b[i].openTime });
    if (sl) lows.push({ index: i, price: b[i].low, time: b[i].openTime });
  }
  return { highs, lows };
}
function marketStructure(b) {
  const s = findSwings(b, TECH.swingLeft, TECH.swingRight);
  const h = s.highs.slice(-2), l = s.lows.slice(-2);
  let structure = 'NEUTRAL';
  if (h.length >= 2 && l.length >= 2) {
    if (h[1].price > h[0].price && l[1].price > l[0].price) structure = 'BULLISH';
    else if (h[1].price < h[0].price && l[1].price < l[0].price) structure = 'BEARISH';
  }
  const c = last(b)?.close;
  const lh = last(s.highs)?.price;
  const ll = last(s.lows)?.price;
  let bos = 'NONE';
  if (Number.isFinite(c) && Number.isFinite(lh) && c > lh) bos = 'BULLISH_BOS';
  else if (Number.isFinite(c) && Number.isFinite(ll) && c < ll) bos = 'BEARISH_BOS';
  return { structure, bos, lastSwingHigh: lh, lastSwingLow: ll, swingHighCount: s.highs.length, swingLowCount: s.lows.length };
}
function dmiAdx(b, l = 14) {
  if (!b || b.length < l * 2 + 2) return { adx: NaN, plusDI: NaN, minusDI: NaN };
  const tr = [], pdm = [], mdm = [];
  for (let i = 1; i < b.length; i++) {
    const u = b[i].high - b[i - 1].high;
    const d = b[i - 1].low - b[i].low;
    pdm.push(u > d && u > 0 ? u : 0);
    mdm.push(d > u && d > 0 ? d : 0);
    tr.push(Math.max(b[i].high - b[i].low, Math.abs(b[i].high - b[i - 1].close), Math.abs(b[i].low - b[i - 1].close)));
  }
  let st = sum(tr.slice(0, l)), sp = sum(pdm.slice(0, l)), sm = sum(mdm.slice(0, l));
  let p = st ? 100 * sp / st : 0, m = st ? 100 * sm / st : 0;
  const dx = [p + m ? 100 * Math.abs(p - m) / (p + m) : 0];
  for (let i = l; i < tr.length; i++) {
    st = st - st / l + tr[i];
    sp = sp - sp / l + pdm[i];
    sm = sm - sm / l + mdm[i];
    p = st ? 100 * sp / st : 0;
    m = st ? 100 * sm / st : 0;
    dx.push(p + m ? 100 * Math.abs(p - m) / (p + m) : 0);
  }
  if (dx.length < l) return { adx: NaN, plusDI: p, minusDI: m };
  let a = average(dx.slice(0, l));
  for (let i = l; i < dx.length; i++) a = (a * (l - 1) + dx[i]) / l;
  return { adx: a, plusDI: p, minusDI: m };
}
function volumeContext(b) {
  const cur = n(last(b)?.volume);
  const prior = (b || []).slice(-(TECH.volumeLen + 1), -1).map(x => n(x.volume));
  const a = average(prior);
  const r = Number.isFinite(a) && a > 0 ? cur / a : NaN;
  return { current: cur, average: n(a), ratio: r, spike: Number.isFinite(r) && r >= 1.5 };
}
function volatilityContext(b) {
  const a = atrLast(b, TECH.atrLen);
  const c = last(b);
  const hist = [];
  for (let i = Math.max(TECH.atrLen + 2, b.length - 50); i <= b.length; i++) {
    const x = atrLast(b.slice(0, i), TECH.atrLen);
    if (Number.isFinite(x)) hist.push(x);
  }
  const aa = average(hist);
  const r = Number.isFinite(a) && Number.isFinite(aa) && aa > 0 ? a / aa : NaN;
  return {
    atr: a,
    atrPct: c && c.close > 0 && Number.isFinite(a) ? a / c.close * 100 : NaN,
    averageAtr: aa,
    atrRatio: r,
    regime: Number.isFinite(r) ? (r >= 1.5 ? 'HIGH' : r <= 0.7 ? 'LOW' : 'NORMAL') : 'NORMAL'
  };
}
function liquidityContext(b, l = TECH.liquidityLookback) {
  if (!b || b.length < l + 1) return { bullishSweep: false, bearishSweep: false, priorHigh: NaN, priorLow: NaN };
  const c = last(b);
  const p = b.slice(-(l + 1), -1);
  const h = highestHigh(p), lo = lowestLow(p);
  return {
    bullishSweep: c.low < lo && c.close > lo,
    bearishSweep: c.high > h && c.close < h,
    priorHigh: h,
    priorLow: lo
  };
}
function fvgContext(b) {
  if (!b || b.length < 3) return { bullish: false, bearish: false, bullGapLow: NaN, bullGapHigh: NaN, bearGapLow: NaN, bearGapHigh: NaN };
  const a = b[b.length - 3], c = last(b);
  const bull = c.low > a.high;
  const bear = c.high < a.low;
  return {
    bullish: bull, bearish: bear,
    bullGapLow: bull ? a.high : NaN,
    bullGapHigh: bull ? c.low : NaN,
    bearGapLow: bear ? c.high : NaN,
    bearGapHigh: bear ? a.low : NaN
  };
}
function fibonacciContext(b, l = TECH.fibLookback) {
  const s = (b || []).slice(-l);
  const h = highestHigh(s), lo = lowestLow(s), r = h - lo;
  if (!Number.isFinite(r) || r <= 0) return null;
  return {
    swingHigh: h, swingLow: lo, current: last(b).close,
    retracementFromHigh: { r382: h - r * 0.382, r500: h - r * 0.5, r618: h - r * 0.618, r786: h - r * 0.786 },
    retracementFromLow: { r382: lo + r * 0.382, r500: lo + r * 0.5, r618: lo + r * 0.618, r786: lo + r * 0.786 },
    extensionUp: { e1272: lo + r * 1.272, e1618: lo + r * 1.618 },
    extensionDown: { e1272: h - r * 1.272, e1618: h - r * 1.618 }
  };
}
function supertrendContext(b, l = 10, m = 3) {
  if (!b || b.length < l + 5) return { direction: 'UNKNOWN', value: NaN };
  let fu = NaN, fl = NaN, st = NaN, prevFu = NaN, prevFl = NaN, prevSt = NaN;
  for (let i = l + 1; i < b.length; i++) {
    const a = atrLast(b.slice(0, i + 1), l);
    if (!Number.isFinite(a)) continue;
    const c = b[i], p = b[i - 1];
    const mid = (c.high + c.low) / 2;
    const bu = mid + m * a, bl = mid - m * a;
    prevFu = fu; prevFl = fl; prevSt = st;
    fu = !Number.isFinite(prevFu) || bu < prevFu || p.close > prevFu ? bu : prevFu;
    fl = !Number.isFinite(prevFl) || bl > prevFl || p.close < prevFl ? bl : prevFl;
    if (!Number.isFinite(prevSt)) st = c.close >= mid ? fl : fu;
    else if (prevSt === prevFu) st = c.close <= fu ? fu : fl;
    else st = c.close >= fl ? fl : fu;
  }
  const c = last(b);
  return { direction: Number.isFinite(st) ? (c.close > st ? 'BULL' : 'BEAR') : 'UNKNOWN', value: st };
}
function ichimokuContext(b) {
  if (!b || b.length < 52) return { tenkan: NaN, kijun: NaN, spanA: NaN, spanB: NaN, cloudTop: NaN, cloudBottom: NaN, bias: 'UNKNOWN' };
  const mid = l => (highestHigh(b.slice(-l)) + lowestLow(b.slice(-l))) / 2;
  const t = mid(9), k = mid(26), a = (t + k) / 2, s = mid(52);
  const top = Math.max(a, s), bot = Math.min(a, s);
  const c = last(b).close;
  return { tenkan: t, kijun: k, spanA: a, spanB: s, cloudTop: top, cloudBottom: bot, bias: c > top && t > k ? 'BULL' : c < bot && t < k ? 'BEAR' : 'MIXED' };
}
function chochContext(b) {
  const s = findSwings(b, TECH.swingLeft, TECH.swingRight);
  const h = s.highs.slice(-2), l = s.lows.slice(-2), c = last(b);
  if (!c || h.length < 2 || l.length < 2) return { bullish: false, bearish: false, direction: 'NONE' };
  const pb = h[1].price < h[0].price && l[1].price < l[0].price;
  const pu = h[1].price > h[0].price && l[1].price > l[0].price;
  const bull = pb && c.close > h[1].price;
  const bear = pu && c.close < l[1].price;
  return { bullish: bull, bearish: bear, direction: bull ? 'BULLISH_CHOCH' : bear ? 'BEARISH_CHOCH' : 'NONE' };
}
function trendContext(b) {
  const c = b.map(x => x.close);
  const cl = last(c);
  const e9 = emaLast(c, TECH.emaFast);
  const e21 = emaLast(c, TECH.emaMedium);
  const e50 = emaLast(c, TECH.emaTrend);
  const e100 = emaLast(c, TECH.emaLong);
  const e200 = c.length >= TECH.emaMacro ? emaLast(c, TECH.emaMacro) : NaN;
  const alignment =
    Number.isFinite(e50) && cl > e9 && e9 > e21 && e21 > e50 ? 'BULL' :
    Number.isFinite(e50) && cl < e9 && e9 < e21 && e21 < e50 ? 'BEAR' : 'MIXED';
  const macro = Number.isFinite(e200) ? (cl > e200 ? 'BULL' : cl < e200 ? 'BEAR' : 'FLAT') :
                Number.isFinite(e100) ? (cl > e100 ? 'BULL' : 'BEAR') : 'UNKNOWN';
  return { close: cl, ema9: e9, ema21: e21, ema50: e50, ema100: e100, ema200: e200, alignment, macro };
}
function momentumContext(b) {
  const c = b.map(x => x.close);
  return {
    rsi: rsiLast(c, TECH.rsiLen),
    cmo: cmoLast(c, TECH.cmoLen),
    macd: macdLast(c),
    stochastic: stochasticLast(b, TECH.stochasticLen),
    williamsR: williamsRLast(b, TECH.stochasticLen),
    roc: rocLast(c, TECH.rocLen)
  };
}

// ═══════════════════════════════════════════════════════════
// CANDLE PATTERNS (NEW)
// ═══════════════════════════════════════════════════════════
function body(b) { return Math.abs(b.close - b.open); }
function range(b) { return Math.max(b.high - b.low, Number.EPSILON); }
function upperWick(b) { return b.high - Math.max(b.open, b.close); }
function lowerWick(b) { return Math.min(b.open, b.close) - b.low; }
function isBull(b) { return b.close > b.open; }
function isBear(b) { return b.close < b.open; }

// Engulfing
function isBullishEngulfing(c, p) {
  return isBear(p) && isBull(c) &&
    c.open <= p.close && c.close >= p.open &&
    body(c) > body(p) * 1.1;
}
function isBearishEngulfing(c, p) {
  return isBull(p) && isBear(c) &&
    c.open >= p.close && c.close <= p.open &&
    body(c) > body(p) * 1.1;
}

// Pin Bar / Hammer / Shooting Star
function isHammer(b) {
  const r = range(b);
  return lowerWick(b) >= r * 0.6 && body(b) <= r * 0.3 && upperWick(b) <= r * 0.15;
}
function isShootingStar(b) {
  const r = range(b);
  return upperWick(b) >= r * 0.6 && body(b) <= r * 0.3 && lowerWick(b) <= r * 0.15;
}

// Inside / Outside Bar
function isInsideBar(c, p) { return c.high <= p.high && c.low >= p.low; }
function isOutsideBar(c, p) { return c.high > p.high && c.low < p.low; }

// Morning/Evening Star (3-bar)
function isMorningStar(a, b, c) {
  return isBear(a) && body(b) < body(a) * 0.5 && isBull(c) && c.close > (a.open + a.close) / 2;
}
function isEveningStar(a, b, c) {
  return isBull(a) && body(b) < body(a) * 0.5 && isBear(c) && c.close < (a.open + a.close) / 2;
}

// Three White Soldiers / Three Black Crows
function isThreeWhiteSoldiers(a, b, c) {
  return isBull(a) && isBull(b) && isBull(c) &&
    b.close > a.close && c.close > b.close &&
    body(b) > range(b) * 0.6 && body(c) > range(c) * 0.6;
}
function isThreeBlackCrows(a, b, c) {
  return isBear(a) && isBear(b) && isBear(c) &&
    b.close < a.close && c.close < b.close &&
    body(b) > range(b) * 0.6 && body(c) > range(c) * 0.6;
}

// Tweezer
function isTweezerBottom(c, p) {
  return isBear(p) && isBull(c) && Math.abs(p.low - c.low) <= range(p) * 0.05;
}
function isTweezerTop(c, p) {
  return isBull(p) && isBear(c) && Math.abs(p.high - c.high) <= range(p) * 0.05;
}

// Doji
function isDoji(b) { return body(b) <= range(b) * 0.1; }

// ── الأنماط المجمعة ─────────────────────────────────────────
function detectCandlePatterns(b) {
  if (!b || b.length < 3) return { patterns: [], bullScore: 0, bearScore: 0 };
  const a = b[b.length - 3], p = b[b.length - 2], c = b[b.length - 1];
  const patterns = [];
  let bullScore = 0, bearScore = 0;

  if (isBullishEngulfing(c, p)) { patterns.push('BULLISH_ENGULFING'); bullScore += 2; }
  if (isBearishEngulfing(c, p)) { patterns.push('BEARISH_ENGULFING'); bearScore += 2; }
  if (isHammer(c))              { patterns.push('HAMMER'); bullScore += 1.5; }
  if (isShootingStar(c))        { patterns.push('SHOOTING_STAR'); bearScore += 1.5; }
  if (isMorningStar(a, p, c))   { patterns.push('MORNING_STAR'); bullScore += 2.5; }
  if (isEveningStar(a, p, c))   { patterns.push('EVENING_STAR'); bearScore += 2.5; }
  if (isThreeWhiteSoldiers(a, p, c)) { patterns.push('THREE_WHITE_SOLDIERS'); bullScore += 2; }
  if (isThreeBlackCrows(a, p, c))    { patterns.push('THREE_BLACK_CROWS'); bearScore += 2; }
  if (isTweezerBottom(c, p))    { patterns.push('TWEEZER_BOTTOM'); bullScore += 1; }
  if (isTweezerTop(c, p))       { patterns.push('TWEEZER_TOP'); bearScore += 1; }
  if (isInsideBar(c, p))        { patterns.push('INSIDE_BAR'); }
  if (isOutsideBar(c, p))       { patterns.push('OUTSIDE_BAR'); }
  if (isDoji(c))                { patterns.push('DOJI'); }

  return { patterns, bullScore, bearScore };
}

// ── Order Blocks ────────────────────────────────────────────
function detectOrderBlocks(b, atr) {
  if (!b || b.length < 20 || !Number.isFinite(atr)) return { bullishOB: null, bearishOB: null };
  const lastBar = b[b.length - 1];
  let bullishOB = null, bearishOB = null;
  // آخر شمعة هابطة قبل حركة صعودية قوية
  for (let i = b.length - 3; i >= Math.max(0, b.length - 20); i--) {
    const x = b[i];
    const next = b[i + 1];
    if (!next) continue;
    if (isBear(x) && isBull(next) && (next.close - next.open) > atr * 1.5) {
      if (!bullishOB || b[i].openTime > bullishOB.time) {
        bullishOB = { low: x.low, high: x.high, time: x.openTime, price: (x.high + x.low) / 2 };
      }
    }
    if (isBull(x) && isBear(next) && (next.open - next.close) > atr * 1.5) {
      if (!bearishOB || b[i].openTime > bearishOB.time) {
        bearishOB = { low: x.low, high: x.high, time: x.openTime, price: (x.high + x.low) / 2 };
      }
    }
  }
  // هل السعر الحالي قريب من OB؟
  const nearBull = bullishOB && Math.abs(lastBar.close - bullishOB.price) < atr * 0.8;
  const nearBear = bearishOB && Math.abs(lastBar.close - bearishOB.price) < atr * 0.8;
  return {
    bullishOB: nearBull ? bullishOB : null,
    bearishOB: nearBear ? bearishOB : null,
    rawBullishOB: bullishOB,
    rawBearishOB: bearishOB
  };
}

// ── Supply/Demand Zones ─────────────────────────────────────
function detectSupplyDemand(b, atr, lookback = 30) {
  if (!b || b.length < lookback || !Number.isFinite(atr)) return { demandZone: null, supplyZone: null };
  const s = b.slice(-lookback);
  // Demand: أدنى low مع volume عالي
  let demandZone = null, supplyZone = null;
  const avgVol = average(s.map(x => n(x.volume)));
  for (let i = 1; i < s.length - 1; i++) {
    const x = s[i];
    const volRatio = avgVol > 0 ? n(x.volume) / avgVol : 1;
    if (volRatio >= 1.5) {
      if (isBull(x) && lowerWick(x) > range(x) * 0.5) {
        demandZone = { low: x.low, high: x.low + atr * 0.5, time: x.openTime };
      }
      if (isBear(x) && upperWick(x) > range(x) * 0.5) {
        supplyZone = { low: x.high - atr * 0.5, high: x.high, time: x.openTime };
      }
    }
  }
  return { demandZone, supplyZone };
}

// ═══════════════════════════════════════════════════════════
// CONFLUENCE SCORING (NEW)
// ═══════════════════════════════════════════════════════════
function buildTechnicalIntelligence(b) {
  if (!Array.isArray(b) || b.length < CORE_MIN_HISTORY) return null;
  const c = b.map(x => x.close);
  const bar = last(b);
  const trend = trendContext(b);
  const momentum = momentumContext(b);
  const volatility = volatilityContext(b);
  const dmi = dmiAdx(b, TECH.adxLen);
  const bollinger = bollingerLast(c, TECH.bbLen, TECH.bbStd);
  const keltner = keltnerLast(b);
  const volume = volumeContext(b);
  const structure = marketStructure(b);
  const liquidity = liquidityContext(b);
  const fvg = fvgContext(b);
  const fibonacci = fibonacciContext(b);
  const candle = candleContext(bar);
  const sr = supportResistance(b, TECH.srLen);
  const supertrend = supertrendContext(b);
  const ichimoku = ichimokuContext(b);
  const choch = chochContext(b);
  const vwap = vwapLast(b, TECH.vwapLookback);
  const obv = obvLast(b);
  const mfi = mfiLast(b, TECH.mfiLen);

  // ── الجديد: أنماط الشموع + OB + S/D ──
  const candlePatterns = detectCandlePatterns(b);
  const orderBlocks = detectOrderBlocks(b, volatility.atr);
  const supplyDemand = detectSupplyDemand(b, volatility.atr);

  // ── حساب bias أساسي ──
  let bull = 0, bear = 0;
  if (trend.alignment === 'BULL') bull += 2;
  if (trend.alignment === 'BEAR') bear += 2;
  if (trend.macro === 'BULL') bull++;
  if (trend.macro === 'BEAR') bear++;
  if (supertrend.direction === 'BULL') bull++;
  if (supertrend.direction === 'BEAR') bear++;
  if (ichimoku.bias === 'BULL') bull++;
  if (ichimoku.bias === 'BEAR') bear++;
  if (Number.isFinite(dmi.adx) && dmi.adx >= 20) {
    if (dmi.plusDI > dmi.minusDI) bull++;
    if (dmi.minusDI > dmi.plusDI) bear++;
  }
  if (Number.isFinite(momentum.rsi)) {
    if (momentum.rsi >= 52 && momentum.rsi <= 75) bull++;
    if (momentum.rsi <= 48 && momentum.rsi >= 25) bear++;
  }
  if (momentum.cmo > 0) bull++;
  if (momentum.cmo < 0) bear++;
  if (momentum.macd?.histogram > 0) bull++;
  if (momentum.macd?.histogram < 0) bear++;
  if (structure.structure === 'BULLISH') bull++;
  if (structure.structure === 'BEARISH') bear++;
  if (structure.bos === 'BULLISH_BOS') bull++;
  if (structure.bos === 'BEARISH_BOS') bear++;
  if (choch.bullish) bull += 2;
  if (choch.bearish) bear += 2;
  if (liquidity.bullishSweep) bull++;
  if (liquidity.bearishSweep) bear++;
  if (candle.direction === 'BULLISH' && candle.bodyRatio >= 0.5) bull++;
  if (candle.direction === 'BEARISH' && candle.bodyRatio >= 0.5) bear++;
  if (Number.isFinite(vwap)) {
    if (bar.close > vwap) bull++;
    if (bar.close < vwap) bear++;
  }

  return {
    barTime: bar.openTime,
    price: bar.close,
    bias: bull > bear ? 'BULL' : bear > bull ? 'BEAR' : 'NEUTRAL',
    score: { bullish: bull, bearish: bear },
    trend, momentum, volatility, dmi, bollinger, keltner, volume,
    structure, liquidity, fvg, fibonacci, candle, supportResistance: sr,
    supertrend, ichimoku, choch, vwap, obv, mfi,
    // الجديد
    candlePatterns, orderBlocks, supplyDemand
  };
}

// ── Confluence Score (نظام النقاط المرجح) ──────────────────
function computeConfluence(t, mtf) {
  if (!t) return { total: 0, bull: 0, bear: 0, breakdown: [] };
  const breakdown = [];
  let bull = 0, bear = 0;
  const add = (name, weight, side) => {
    if (side === 'BULL') { bull += weight; breakdown.push(`+${weight} ${name}`); }
    else if (side === 'BEAR') { bear += weight; breakdown.push(`-${weight} ${name}`); }
  };

  const dir = t.bias;
  const h1 = mtf?.h1?.bias;
  const h4 = mtf?.h4?.bias;

  // Multi-timeframe
  if (h4 === dir && dir !== 'NEUTRAL') add('4h_aligned', 3, dir);
  if (h1 === dir && dir !== 'NEUTRAL') add('1h_aligned', 2, dir);

  // Structure
  if (t.choch?.bullish && dir === 'BULL') add('choch_bull', 2, 'BULL');
  if (t.choch?.bearish && dir === 'BEAR') add('choch_bear', 2, 'BEAR');
  if (t.structure?.bos === 'BULLISH_BOS' && dir === 'BULL') add('bos_bull', 2, 'BULL');
  if (t.structure?.bos === 'BEARISH_BOS' && dir === 'BEAR') add('bos_bear', 2, 'BEAR');

  // Order Blocks
  if (t.orderBlocks?.bullishOB && dir === 'BULL') add('bull_ob', 2, 'BULL');
  if (t.orderBlocks?.bearishOB && dir === 'BEAR') add('bear_ob', 2, 'BEAR');

  // Candle patterns
  if (t.candlePatterns?.bullScore > 0 && dir === 'BULL') add('bull_candle', Math.min(t.candlePatterns.bullScore, 2.5), 'BULL');
  if (t.candlePatterns?.bearScore > 0 && dir === 'BEAR') add('bear_candle', Math.min(t.candlePatterns.bearScore, 2.5), 'BEAR');

  // Liquidity
  if (t.liquidity?.bullishSweep && dir === 'BULL') add('liq_sweep_bull', 1.5, 'BULL');
  if (t.liquidity?.bearishSweep && dir === 'BEAR') add('liq_sweep_bear', 1.5, 'BEAR');

  // FVG
  if (t.fvg?.bullish && dir === 'BULL') add('fvg_bull', 1.5, 'BULL');
  if (t.fvg?.bearish && dir === 'BEAR') add('fvg_bear', 1.5, 'BEAR');

  // EMA alignment
  if (t.trend?.alignment === 'BULL' && dir === 'BULL') add('ema_align_bull', 1.5, 'BULL');
  if (t.trend?.alignment === 'BEAR' && dir === 'BEAR') add('ema_align_bear', 1.5, 'BEAR');

  // MACD
  if (t.momentum?.macd?.histogram > 0 && dir === 'BULL') add('macd_bull', 1, 'BULL');
  if (t.momentum?.macd?.histogram < 0 && dir === 'BEAR') add('macd_bear', 1, 'BEAR');

  // Volume spike
  if (t.volume?.spike) add('volume_spike', 1, dir === 'BULL' ? 'BULL' : dir === 'BEAR' ? 'BEAR' : null);

  // S/R reaction
  if (Number.isFinite(t.supportResistance?.support) && Number.isFinite(t.price)) {
    const nearSupport = Math.abs(t.price - t.supportResistance.support) < t.volatility.atr * 0.5;
    const nearResistance = Math.abs(t.price - t.supportResistance.resistance) < t.volatility.atr * 0.5;
    if (nearSupport && dir === 'BULL') add('near_support', 1, 'BULL');
    if (nearResistance && dir === 'BEAR') add('near_resistance', 1, 'BEAR');
  }

  // Supply/Demand
  if (t.supplyDemand?.demandZone && dir === 'BULL') add('demand_zone', 1, 'BULL');
  if (t.supplyDemand?.supplyZone && dir === 'BEAR') add('supply_zone', 1, 'BEAR');

  const total = dir === 'BULL' ? bull : dir === 'BEAR' ? bear : Math.max(bull, bear);
  return { total, bull, bear, breakdown, direction: dir };
}

function buildMtf(pair) {
  return {
    m15: buildTechnicalIntelligence(pair.bars15m),
    h1: pair.bars1h.length >= CORE_MIN_HISTORY ? buildTechnicalIntelligence(pair.bars1h) : null,
    h4: pair.bars4h.length >= CORE_MIN_HISTORY ? buildTechnicalIntelligence(pair.bars4h) : null
  };
}

// ── المرشح المحلي (يعتمد على Confluence) ──────────────────
function localCandidate(symbol, mtf) {
  const t = mtf?.m15;
  if (!t || t.bias === 'NEUTRAL') return null;
  if (!Number.isFinite(t.volatility?.atr) || t.volatility.atr <= 0) return null;

  const conf = computeConfluence(t, mtf);

  // الشرط الجديد: لازم Confluence ≥ العتبة
  if (conf.total < RULES.confluenceThreshold) return null;

  const dir = t.bias;
  let rank = conf.total;
  if (mtf.h1?.bias === dir) rank += 1.5;
  if (mtf.h4?.bias === dir) rank += 2;
  if (n(t.dmi?.adx) >= 20) rank += 1;

  return { symbol, technical: t, mtf, edge: conf.total, rank, confluence: conf };
}

function compactTechnical(t) {
  if (!t) return null;
  return {
    barTime: t.barTime, price: t.price, bias: t.bias, score: t.score,
    trend: { alignment: t.trend?.alignment, macro: t.trend?.macro, ema9: t.trend?.ema9, ema21: t.trend?.ema21, ema50: t.trend?.ema50, ema200: t.trend?.ema200 },
    momentum: { rsi: t.momentum?.rsi, cmo: t.momentum?.cmo, macdHistogram: t.momentum?.macd?.histogram, stochastic: t.momentum?.stochastic, williamsR: t.momentum?.williamsR, roc: t.momentum?.roc },
    adx: t.dmi?.adx, plusDI: t.dmi?.plusDI, minusDI: t.dmi?.minusDI,
    atr: t.volatility?.atr, volatilityRegime: t.volatility?.regime,
    structure: t.structure, choch: t.choch, supertrend: t.supertrend,
    ichimoku: { bias: t.ichimoku?.bias, tenkan: t.ichimoku?.tenkan, kijun: t.ichimoku?.kijun },
    liquidity: t.liquidity, fvg: t.fvg, fibonacci: t.fibonacci,
    bollinger: t.bollinger, keltner: t.keltner,
    supportResistance: t.supportResistance, volume: t.volume,
    vwap: t.vwap, obv: t.obv, mfi: t.mfi,
    // الجديد
    candlePatterns: t.candlePatterns,
    orderBlocks: t.orderBlocks,
    supplyDemand: t.supplyDemand
  };
}

module.exports = {
  // Config
  VERSION, MODE, LIVE_TRADING, RULES, PAPER, PROTECTION, GEMINI_MODELS,
  // Utilities
  n, clamp, sleep, average, standardDeviation, last, pctChange, safeError,
  fmtMoney, fmtPrice, parseTime, barTimeMs, utcDayKey, pacificDayKey,
  // Bars
  normalizeBars, closed15Bars, mergeBars, aggregateBars,
  // Indicators
  emaSeries, emaLast, atrLast, rsiLast, macdLast, dmiAdx, vwapLast,
  // Technical
  buildTechnicalIntelligence, buildMtf, localCandidate, compactTechnical, computeConfluence,
  // Candle Patterns
  detectCandlePatterns, detectOrderBlocks, detectSupplyDemand,
  // Trade helpers
  tradePriceR, stopPriceAtR, stopFillFromBar, riskPctFromConfidence
 
};
// ═══════════════════════════════════════════════════════════
// MONGODB
// ═══════════════════════════════════════════════════════════
const accountSchema = new mongoose.Schema({
  accountKey: { type: String, unique: true, required: true },
  balance: { type: Number, required: true },
  startingBalance: { type: Number, required: true },
  version: String,
  mode: String,
  telegramChatId: String,
  twelveUsageDay: String,
  twelveCreditsUsed: { type: Number, default: 0 },
  geminiUsageDay: String,
  geminiCallsUsed: { type: Number, default: 0 },
  geminiEntryUsed: { type: Number, default: 0 },
  geminiManageUsed: { type: Number, default: 0 },
  appliedPnlKeys: { type: [String], default: [] },
  // إحصائيات الحماية
  dayStartBalance: { type: Number, default: 0 },
  weekStartBalance: { type: Number, default: 0 },
  dailyPnl: { type: Number, default: 0 },
  weeklyPnl: { type: Number, default: 0 },
  consecutiveLosses: { type: Number, default: 0 },
  lastLossAt: Date,
  lastDayKey: String,
  lastWeekKey: String
}, { timestamps: true });

const tradeSchema = new mongoose.Schema({
  tradeId: { type: String, unique: true, required: true },
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
  confluenceScore: Number,
  entryReason: String,
  managementReason: String,
  openedAt: Date,
  closedAt: Date,
  exitPrice: Number,
  realizedPartialPnl: { type: Number, default: 0 },
  totalPnl: { type: Number, default: 0 },
  resultR: Number,
  partialClosed: { type: Boolean, default: false },
  trailingLevelR: { type: Number, default: 0 },
  breakEvenActivated: { type: Boolean, default: false },
  atrTrailLevel: Number,
  maxFavorablePrice: Number,
  maxAdversePrice: Number,
  mfeR: Number,
  maeR: Number,
  lastManagedBarTime: Date,
  settlementVersion: String,
  finalRemainingPnl: Number,
  aiEntryDecision: mongoose.Schema.Types.Mixed,
  technicalSnapshot: mongoose.Schema.Types.Mixed,
  correlationKey: String
}, { timestamps: true });

const journalSchema = new mongoose.Schema({
  type: String,
  symbol: String,
  tradeId: String,
  message: String,
  data: mongoose.Schema.Types.Mixed,
  createdAt: { type: Date, default: Date.now }
}, { collection: 'lomyforexjournalv2' });

const newsCacheSchema = new mongoose.Schema({
  key: { type: String, unique: true },
  events: mongoose.Schema.Types.Mixed,
  fetchedAt: Date
}, { timestamps: true });

const Account = mongoose.models.LomyForexAccountV2 || mongoose.model('LomyForexAccountV2', accountSchema);
const Trade = mongoose.models.LomyForexTradeV2 || mongoose.model('LomyForexTradeV2', tradeSchema);
const Journal = mongoose.models.LomyForexJournalV2 || mongoose.model('LomyForexJournalV2', journalSchema);
const NewsCache = mongoose.models.LomyForexNewsCacheV2 || mongoose.model('LomyForexNewsCacheV2', newsCacheSchema);

async function saveAccount() {
  if (!account) return;
  const run = accountSaveChain.then(() => account.save());
  accountSaveChain = run.catch(() => {});
  return run;
}

async function journal(type, data = {}) {
  try {
    if (state.mongoReady)
      await Journal.create({
        type,
        symbol: data.symbol || null,
        tradeId: data.tradeId || null,
        message: data.message || '',
        data
      });
  } catch (e) {
    console.error('[JOURNAL]', safeError(e));
  }
}

async function ensureUsageDays() {
  if (!account) return;
  let changed = false;
  const td = utcDayKey();
  const gd = pacificDayKey();
  const wk = (() => {
    const d = new Date();
    const day = d.getUTCDay();
    const diff = d.getUTCDate() - day + (day === 0 ? -6 : 1);
    const monday = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), diff));
    return monday.toISOString().slice(0, 10);
  })();

  if (account.twelveUsageDay !== td) {
    account.twelveUsageDay = td;
    account.twelveCreditsUsed = 0;
    state.twelveBlockedUntil = 0;
    changed = true;
  }
  if (account.geminiUsageDay !== gd) {
    account.geminiUsageDay = gd;
    account.geminiCallsUsed = 0;
    account.geminiEntryUsed = 0;
    account.geminiManageUsed = 0;
    state.geminiBlockedUntil = 0;
    state.geminiActiveModelIndex = 0;
    state.geminiModelFailures.clear();
    changed = true;
  }
  // إعادة تعيين الحماية اليومية
  if (account.lastDayKey !== td) {
    account.lastDayKey = td;
    account.dayStartBalance = n(account.balance, PAPER.startingBalance);
    account.dailyPnl = 0;
    account.consecutiveLosses = 0;
    changed = true;
  }
  // إعادة تعيين الحماية الأسبوعية
  if (account.lastWeekKey !== wk) {
    account.lastWeekKey = wk;
    account.weekStartBalance = n(account.balance, PAPER.startingBalance);
    account.weeklyPnl = 0;
    changed = true;
  }
  if (changed) await saveAccount();
}

async function initMongo() {
  if (!MONGODB_URI) throw new Error('MONGODB_URI is required');
  await mongoose.connect(MONGODB_URI);
  state.mongoReady = true;
  account = await Account.findOne({ accountKey: PAPER.accountKey });
  if (!account) {
    account = await Account.create({
      accountKey: PAPER.accountKey,
      balance: PAPER.startingBalance,
      startingBalance: PAPER.startingBalance,
      version: VERSION,
      mode: MODE,
      telegramChatId: '',
      twelveUsageDay: utcDayKey(),
      twelveCreditsUsed: 0,
      geminiUsageDay: pacificDayKey(),
      geminiCallsUsed: 0,
      geminiEntryUsed: 0,
      geminiManageUsed: 0,
      dayStartBalance: PAPER.startingBalance,
      weekStartBalance: PAPER.startingBalance
    });
  }
  account.version = VERSION;
  account.mode = MODE;
  await ensureUsageDays();
  await saveAccount();
  if (!telegramChatId) telegramChatId = String(account.telegramChatId || '');
  console.log(`[MONGO] connected | balance ${fmtMoney(account.balance)}`);
}

async function restoreOpenTrades() {
  if (!state.mongoReady) return;
  const rows = await Trade.find({ status: 'OPEN' }).lean();
  state.openTrades.clear();
  for (const r of rows) {
    if (r.partialClosed === true && r.settlementVersion !== 'V2') {
      await markAccountKeyOnly(`${r.tradeId}:partial`);
    }
    const t = {
      ...r,
      initialQuantity: n(r.initialQuantity, n(r.quantity)),
      realizedPartialPnl: n(r.realizedPartialPnl),
      trailingLevelR: n(r.trailingLevelR),
      partialClosed: r.partialClosed === true,
      breakEvenActivated: r.breakEvenActivated === true,
      lastManagedBarTime: r.lastManagedBarTime || r.openedAt || null
    };
    state.openTrades.set(t.symbol, t);
  }
  console.log(`[TRADES] restored ${state.openTrades.size} open trades`);
}

async function saveTrade(t) {
  if (!t?.tradeId) return;
  await Trade.updateOne({ tradeId: t.tradeId }, {
    $set: {
      status: t.status,
      quantity: t.quantity,
      initialQuantity: t.initialQuantity,
      stopLoss: t.stopLoss,
      partialClosed: t.partialClosed,
      trailingLevelR: t.trailingLevelR,
      breakEvenActivated: t.breakEvenActivated,
      atrTrailLevel: t.atrTrailLevel,
      realizedPartialPnl: t.realizedPartialPnl,
      totalPnl: t.totalPnl,
      managementReason: t.managementReason,
      maxFavorablePrice: t.maxFavorablePrice,
      maxAdversePrice: t.maxAdversePrice,
      mfeR: t.mfeR,
      maeR: t.maeR,
      lastManagedBarTime: t.lastManagedBarTime,
      settlementVersion: t.settlementVersion,
      finalRemainingPnl: t.finalRemainingPnl,
      exitPrice: t.exitPrice,
      closedAt: t.closedAt,
      resultR: t.resultR
    }
  });
}

async function markAccountKeyOnly(key) {
  if (!account || !key) return;
  await accountSaveChain.catch(() => {});
  const updated = await Account.findOneAndUpdate(
    { accountKey: PAPER.accountKey },
    { $addToSet: { appliedPnlKeys: key } },
    { new: true }
  );
  if (updated) account.appliedPnlKeys = updated.appliedPnlKeys;
}

async function creditAccountOnce(key, amount) {
  amount = n(amount);
  if (!account || !key || amount === 0) return false;
  await accountSaveChain.catch(() => {});
  const updated = await Account.findOneAndUpdate(
    { accountKey: PAPER.accountKey, appliedPnlKeys: { $ne: key } },
    { $inc: { balance: amount }, $addToSet: { appliedPnlKeys: key } },
    { new: true }
  );
  if (updated) {
    account.balance = updated.balance;
    account.appliedPnlKeys = updated.appliedPnlKeys;
    return true;
  }
  return false;
}

async function reconcilePnl() {
  if (!state.mongoReady || !account) return;
  const rows = await Trade.find({
    settlementVersion: 'V2',
    $or: [{ status: 'OPEN', partialClosed: true }, { status: 'CLOSED' }]
  }).sort({ updatedAt: -1 }).limit(500).lean();

  for (const t of rows) {
    if (t.partialClosed && n(t.realizedPartialPnl) !== 0)
      await creditAccountOnce(`${t.tradeId}:partial`, n(t.realizedPartialPnl));
    if (t.status === 'CLOSED' && n(t.finalRemainingPnl) !== 0)
      await creditAccountOnce(`${t.tradeId}:final`, n(t.finalRemainingPnl));
  }
}

// ═══════════════════════════════════════════════════════════
// TWELVE DATA
// ═══════════════════════════════════════════════════════════
function toTwelveSymbol(s) {
  return s === 'XAUUSD' ? 'XAU/USD' :
    typeof s === 'string' && s.length === 6 ? s.slice(0, 3) + '/' + s.slice(3) : s;
}
function assertTwelve(data) {
  if (!data) throw new Error('Twelve Data returned empty response');
  if (data.status === 'error') throw new Error(data.message || data.code || 'Twelve Data API error');
}
function twelveBlockFromError(e) {
  const msg = safeError(e).toLowerCase();
  if (msg.includes('daily') || msg.includes('per day') || msg.includes('800 api') || msg.includes('credits for the day'))
    return nextUtcMidnightMs();
  if (msg.includes('minute') || e?.response?.status === 429)
    return Math.floor(Date.now() / 60000) * 60000 + 65000;
  return 0;
}

async function queueTwelve(task, kind = 'scan') {
  const run = twelveChain.then(async () => {
    await ensureUsageDays();
    if (Date.now() < state.twelveBlockedUntil) {
      const msg = `Twelve Data paused until ${new Date(state.twelveBlockedUntil).toISOString()}`;
      state.lastMarketError = msg;
      throw new Error(msg);
    }
    const used = n(account?.twelveCreditsUsed);
    const limit = kind === 'critical' ? TWELVE_HARD_LOCAL_CAP : TWELVE_SCAN_SOFT_CAP;
    if (used >= limit) {
      const msg = `Twelve Data local ${kind} cap reached (${used}/${limit})`;
      state.lastMarketError = msg;
      throw new Error(msg);
    }
    const wait = TWELVE_MIN_GAP_MS - (Date.now() - lastTwelveAt);
    if (wait > 0) await sleep(wait);
    lastTwelveAt = Date.now();
    if (account) {
      account.twelveCreditsUsed = used + 1;
      await saveAccount();
    }
    try {
      const out = await task();
      state.lastMarketError = null;
      return out;
    } catch (e) {
      state.lastMarketError = safeError(e);
      const until = twelveBlockFromError(e);
      if (until) state.twelveBlockedUntil = until;
      throw e;
    }
  });
  twelveChain = run.catch(() => {});
  return run;
}

async function fetchBars(symbol, outputSize = REFRESH_HISTORY, kind = 'scan') {
  return queueTwelve(async () => {
    const r = await http.get(`${TWELVE_BASE}/time_series`, {
      params: {
        symbol: toTwelveSymbol(symbol),
        interval: '15min',
        outputsize: outputSize,
        order: 'asc',
        timezone: 'UTC',
        apikey: TWELVE_DATA_API_KEY
      }
    });
    assertTwelve(r.data);
    if (!Array.isArray(r.data.values)) throw new Error(`No OHLC values for ${symbol}`);
    return normalizeBars(r.data.values.map(x => ({
      openTime: x.datetime, open: x.open, high: x.high, low: x.low,
      close: x.close, volume: x.volume, isOpen: false
    })));
  }, kind);
}

async function fetchCurrentPrice(symbol) {
  return queueTwelve(async () => {
    const r = await http.get(`${TWELVE_BASE}/price`, {
      params: { symbol: toTwelveSymbol(symbol), apikey: TWELVE_DATA_API_KEY }
    });
    assertTwelve(r.data);
    const p = n(r.data.price, NaN);
    if (!Number.isFinite(p) || p <= 0) throw new Error(`Invalid price for ${symbol}`);
    return p;
  }, 'critical');
}

async function fetchQuote(symbol) {
  return queueTwelve(async () => {
    const r = await http.get(`${TWELVE_BASE}/quote`, {
      params: { symbol: toTwelveSymbol(symbol), apikey: TWELVE_DATA_API_KEY }
    });
    assertTwelve(r.data);
    const close = n(r.data.close, NaN);
    const bid = n(r.data.bid, NaN);
    const ask = n(r.data.ask, NaN);
    const spreadKnown = Number.isFinite(bid) && Number.isFinite(ask) && bid > 0 && ask > 0 && ask >= bid;
    const marketPrice = Number.isFinite(close) && close > 0 ? close :
      spreadKnown ? (bid + ask) / 2 : NaN;
    if (!Number.isFinite(marketPrice) || marketPrice <= 0)
      throw new Error(`Invalid quote for ${symbol}`);
    return {
      symbol,
      marketPrice,
      bid: spreadKnown ? bid : null,
      ask: spreadKnown ? ask : null,
      mid: spreadKnown ? (bid + ask) / 2 : marketPrice,
      spread: spreadKnown ? ask - bid : null,
      spreadKnown,
      fetchedAt: new Date()
    };
  }, 'critical');
}

function entryExecutionPrice(direction, q) {
  if (q?.spreadKnown) return direction === 'BUY' ? q.ask : q.bid;
  return q?.marketPrice;
}
function exitExecutionPrice(direction, q) {
  if (q?.spreadKnown) return direction === 'BUY' ? q.bid : q.ask;
  return q?.marketPrice;
}

async function initializeSymbol(symbol) {
  const closed = closed15Bars(await fetchBars(symbol, INITIAL_HISTORY, 'scan'));
  if (closed.length < CORE_MIN_HISTORY)
    throw new Error(`${symbol}: insufficient 15m history`);
  const pair = {
    symbol,
    bars15m: closed.slice(-INITIAL_HISTORY),
    bars1h: aggregateBars(closed, 60),
    bars4h: aggregateBars(closed, 240),
    lastDataBarTime: last(closed).openTime,
    lastEvaluatedBarTime: last(closed).openTime,
    initializedAt: new Date(),
    lastRefreshAt: new Date(),
    lastError: null
  };
  state.pairState.set(symbol, pair);
  console.log(`[MARKET] ${symbol} ready | 15m=${pair.bars15m.length} 1h=${pair.bars1h.length} 4h=${pair.bars4h.length}`);
  return pair;
}

async function initializeMarket() {
  if (!TWELVE_DATA_API_KEY) {
    state.lastMarketError = 'TWELVE_DATA_API_KEY missing';
    return false;
  }
  const syms = marketSymbols();
  console.log(`[MARKET] initializing ${syms.length} symbols...`);
  let ok = 0;
  for (const s of syms) {
    try {
      await initializeSymbol(s);
      ok++;
    } catch (e) {
      state.lastMarketError = safeError(e);
      console.error(`[MARKET] ${s}:`, safeError(e));
      if (Date.now() < state.twelveBlockedUntil) break;
    }
  }
  state.marketReady = ok > 0;
  console.log(`[MARKET] ${ok}/${syms.length} ready`);
  return state.marketReady;
}

async function ensurePair(symbol) {
  if (state.pairState.has(symbol)) return state.pairState.get(symbol);
  return initializeSymbol(symbol);
}

async function refreshSymbol(symbol) {
  let pair;
  try {
    pair = await ensurePair(symbol);
  } catch (e) {
    state.lastMarketError = safeError(e);
    return null;
  }
  try {
    const closed = closed15Bars(await fetchBars(symbol, REFRESH_HISTORY, 'scan'));
    if (!closed.length) return null;
    const prev = barTimeMs({ openTime: pair.lastDataBarTime });
    pair.bars15m = mergeBars(pair.bars15m, closed);
    pair.bars1h = aggregateBars(pair.bars15m, 60);
    pair.bars4h = aggregateBars(pair.bars15m, 240);
    pair.lastRefreshAt = new Date();
    pair.lastError = null;
    const latest = last(pair.bars15m);
    const cur = barTimeMs(latest);
    if (cur > prev) {
      pair.lastDataBarTime = latest.openTime;
      return { pair, latest, newBar: true };
    }
    return { pair, latest, newBar: false };
  } catch (e) {
    pair.lastError = safeError(e);
    state.lastMarketError = safeError(e);
    console.error(`[REFRESH] ${symbol}:`, safeError(e));
    return null;
  }
}

// ═══════════════════════════════════════════════════════════
// NEWS / SESSION / STRENGTH / PROTECTION
// ═══════════════════════════════════════════════════════════
function getMarketSession(date = new Date()) {
  const h = date.getUTCHours();
  const sessions = [];
  if (h >= 0 && h < 9) sessions.push('ASIA');
  if (h >= 7 && h < 16) sessions.push('LONDON');
  if (h >= 12 && h < 21) sessions.push('NEW_YORK');
  if (!sessions.length) sessions.push('OFF_HOURS');
  return {
    utcHour: h,
    sessions,
    londonNewYorkOverlap: h >= 12 && h < 16
  };
}

function isSessionAllowed() {
  const s = getMarketSession();
  const d = new Date();
  const utcH = d.getUTCHours();
  const utcDay = d.getUTCDay();
  // منع الجمعة متأخر
  if (utcDay === 5 && utcH >= PROTECTION.blockFridayAfterUTC) return false;
  // منع الأحد بدري
  if (utcDay === 0 && utcH < PROTECTION.blockSundayBeforeUTC) return false;
  return s.sessions.some(x => PROTECTION.allowedSessions.includes(x));
}

function symbolCurrencies(s) {
  return s === 'XAUUSD' ? ['XAU', 'USD'] :
    typeof s === 'string' && s.length >= 6 ? [s.slice(0, 3), s.slice(3, 6)] : [];
}
function normalizeNewsImpact(v) {
  const x = String(v || '').toLowerCase();
  if (x.includes('high') || x.includes('red')) return 'HIGH';
  if (x.includes('medium') || x.includes('orange')) return 'MEDIUM';
  return 'LOW';
}

async function loadNewsCache() {
  try {
    const c = await NewsCache.findOne({ key: 'weekly' }).lean();
    if (!c?.events || !Array.isArray(c.events) || !c.fetchedAt) return false;
    if (Date.now() - new Date(c.fetchedAt).getTime() > NEWS_CACHE_MAX_AGE_MS) return false;
    economicNews = c.events.map(x => ({ ...x, time: new Date(x.time) }))
      .filter(x => Number.isFinite(x.time.getTime()));
    state.newsReady = true;
    return true;
  } catch (_) {
    return false;
  }
}

async function fetchEconomicNews() {
  try {
    const r = await http.get('https://nfs.faireconomy.media/ff_calendar_thisweek.json', {
      timeout: 15000,
      headers: { Accept: 'application/json' }
    });
    const rows = Array.isArray(r.data) ? r.data : [];
    economicNews = rows.map(x => ({
      title: String(x.title || x.event || ''),
      country: String(x.country || '').trim().toUpperCase(),
      impact: normalizeNewsImpact(x.impact),
      time: new Date(x.date || x.datetime || x.time || x.timestamp)
    })).filter(x => x.title && Number.isFinite(x.time.getTime()));
    state.newsReady = true;
    state.lastNewsError = null;
    if (state.mongoReady) {
      await NewsCache.findOneAndUpdate(
        { key: 'weekly' },
        { $set: { events: economicNews, fetchedAt: new Date() } },
        { upsert: true }
      );
    }
    console.log(`[NEWS] loaded ${economicNews.length} events`);
  } catch (e) {
    state.lastNewsError = safeError(e);
    const cached = state.mongoReady ? await loadNewsCache() : false;
    state.newsReady = cached;
    console.error(`[NEWS] fetch failed${cached ? ' - using cache' : ''}:`, safeError(e));
  }
}

function getNewsBlock(symbol, now = new Date()) {
  if (!state.newsReady) return { blocked: false, available: false, events: [] };
  const currencies = symbolCurrencies(symbol);
  const w = NEWS_BLOCK_MIN * 60000;
  const t = now.getTime();
  const events = economicNews
    .filter(x => x.impact === 'HIGH' && currencies.includes(x.country) && Math.abs(x.time.getTime() - t) <= w)
    .slice(0, 5);
  return { blocked: events.length > 0, available: true, events };
}

function calculateCurrencyStrength() {
  const values = new Map(), counts = new Map();
  for (const [s, p] of state.pairState) {
    if (s === 'XAUUSD' || !p?.bars15m?.length) continue;
    const b = p.bars15m;
    if (b.length < 13) continue;
    const ch = pctChange(b[b.length - 13].close, last(b).close);
    if (!Number.isFinite(ch)) continue;
    const base = s.slice(0, 3), quote = s.slice(3, 6);
    values.set(base, n(values.get(base)) + ch);
    counts.set(base, n(counts.get(base)) + 1);
    values.set(quote, n(values.get(quote)) - ch);
    counts.set(quote, n(counts.get(quote)) + 1);
  }
  const out = {};
  for (const [k, v] of values) out[k] = v / Math.max(1, n(counts.get(k), 1));
  return out;
}

function pairStrengthContext(s) {
  const x = calculateCurrencyStrength();
  if (s === 'XAUUSD') return { base: 'XAU', quote: 'USD', baseStrength: null, quoteStrength: n(x.USD), differential: null };
  const base = s.slice(0, 3), quote = s.slice(3, 6);
  const bs = n(x[base]), qs = n(x[quote]);
  return { base, quote, baseStrength: bs, quoteStrength: qs, differential: bs - qs };
}

// ── Capital Protection Checks ──────────────────────────────
function isDailyLossLimitHit() {
  if (!account) return false;
  const loss = n(account.dailyPnl);
  const start = n(account.dayStartBalance, PAPER.startingBalance);
  if (start <= 0) return false;
  return (loss / start) * 100 <= -PROTECTION.dailyLossLimitPct;
}

function isWeeklyLossLimitHit() {
  if (!account) return false;
  const loss = n(account.weeklyPnl);
  const start = n(account.weekStartBalance, PAPER.startingBalance);
  if (start <= 0) return false;
  return (loss / start) * 100 <= -PROTECTION.weeklyLossLimitPct;
}

function isCooldownActive() {
  const t = parseTime(account?.lastLossAt);
  if (!t) return false;
  return Date.now() - t < PROTECTION.cooldownAfterLossMs;
}

function isCorrelated(symbol) {
  const k = correlationKey(symbol);
  for (const t of state.openTrades.values()) {
    if (t.symbol === symbol) return true;
    if (correlationKey(t.symbol) === k) return true;
  }
  return false;
}

function isProtectionTriggered() {
  return isDailyLossLimitHit() || isWeeklyLossLimitHit() || isCooldownActive();
}

function protectionReason() {
  if (isDailyLossLimitHit()) return `Daily loss limit hit (${PROTECTION.dailyLossLimitPct}%)`;
  if (isWeeklyLossLimitHit()) return `Weekly loss limit hit (${PROTECTION.weeklyLossLimitPct}%)`;
  if (isCooldownActive()) return 'Cooldown after recent loss';
  return null;
}

// ═══════════════════════════════════════════════════════════
// TELEGRAM
// ═══════════════════════════════════════════════════════════
async function captureTelegramChat(ctx) {
  try {
    const id = String(ctx?.chat?.id || '');
    if (!id) return;
    if (id !== telegramChatId) {
      telegramChatId = id;
      if (account) {
        account.telegramChatId = id;
        await saveAccount();
      }
    }
  } catch (_) {}
}

async function sendTelegram(message) {
  if (!telegramBot || !state.telegramReady || !telegramChatId) return;
  try {
    await telegramBot.telegram.sendMessage(telegramChatId, message);
  } catch (e) {
    console.error('[TELEGRAM SEND]', safeError(e));
  }
}

function telegramStatusText() {
  const r = protectionReason();
  return [
    VERSION,
    `Mode: ${MODE}`,
    `Balance: ${account ? fmtMoney(account.balance) : 'n/a'}`,
    `Open trades: ${state.openTrades.size}`,
    `Active symbols: ${ACTIVE_SYMBOLS.join(', ')}`,
    `Market: ${state.marketReady ? 'READY' : 'NOT READY'}`,
    `Mongo: ${state.mongoReady ? 'READY' : 'NOT READY'}`,
    `Gemini: ${state.geminiReady ? 'READY' : 'NOT READY'}`,
    `Gemini model: ${GEMINI_MODELS[state.geminiActiveModelIndex]?.name || 'n/a'}`,
    `Telegram polling: ${state.telegramPollingReady ? 'READY' : 'NOT READY'}`,
    `News: ${state.newsReady ? 'READY' : 'NOT READY'}`,
    `Protection: ${r ? 'ACTIVE — ' + r : 'clear'}`,
    `Scanned bars: ${state.scannedBars}`,
    `Executed: ${state.executedSignals}`,
    `Skipped: ${state.skippedSignals}`
  ].join('\n');
}

function scheduleTelegramRetry() {
  if (telegramRetryTimer) return;
  telegramRetryTimer = setTimeout(() => {
    telegramRetryTimer = null;
    startTelegramPolling().catch(() => {});
  }, 60000);
}

async function startTelegramPolling() {
  if (!telegramBot) return;
  try {
    await telegramBot.telegram.getMe();
    state.telegramReady = true;
    const p = telegramBot.launch();
    state.telegramPollingReady = true;
    p.catch(e => {
      state.telegramPollingReady = false;
      console.error('[TELEGRAM POLLING]', safeError(e));
      scheduleTelegramRetry();
    });
  } catch (e) {
    state.telegramReady = false;
    state.telegramPollingReady = false;
    console.error('[TELEGRAM]', safeError(e));
    scheduleTelegramRetry();
  }
}

async function initTelegram() {
  if (!TELEGRAM_BOT_TOKEN) {
    console.log('[TELEGRAM] token missing - disabled');
    return;
  }
  telegramBot = new Telegraf(TELEGRAM_BOT_TOKEN);

  telegramBot.start(async ctx => {
    await captureTelegramChat(ctx);
    await ctx.reply(telegramStatusText());
  });
  telegramBot.command('status', async ctx => {
    await captureTelegramChat(ctx);
    await ctx.reply(telegramStatusText());
  });
  telegramBot.command('balance', async ctx => {
    await captureTelegramChat(ctx);
    await ctx.reply(account ? `Balance: ${fmtMoney(account.balance)}` : 'Account not ready');
  });
  telegramBot.command('positions', async ctx => {
    await captureTelegramChat(ctx);
    const x = [...state.openTrades.values()];
    await ctx.reply(x.length
      ? x.map(t => `${t.symbol} ${t.direction} | Entry ${fmtPrice(t.entryPrice, t.symbol)} | SL ${fmtPrice(t.stopLoss, t.symbol)} | Qty ${n(t.quantity).toFixed(4)}`).join('\n')
      : 'No open trades.');
  });
  telegramBot.command('protection', async ctx => {
    await captureTelegramChat(ctx);
    await ctx.reply([
      `Daily PnL: ${fmtMoney(account?.dailyPnl || 0)}`,
      `Weekly PnL: ${fmtMoney(account?.weeklyPnl || 0)}`,
      `Consecutive losses: ${n(account?.consecutiveLosses)}`,
      `Cooldown: ${isCooldownActive() ? 'ACTIVE' : 'clear'}`,
      `Reason: ${protectionReason() || 'none'}`
    ].join('\n'));
  });

  await startTelegramPolling();
  console.log('[TELEGRAM] API ready');
}

// ═══════════════════════════════════════════════════════════
// GEMINI FALLBACK CHAIN (NEW)
// ═══════════════════════════════════════════════════════════
function retryAfterMs(e) {
  const h = Number(e?.response?.headers?.['retry-after']);
  if (Number.isFinite(h) && h > 0) return h * 1000;
  const msg = safeError(e);
  const m = msg.match(/retry(?:\s+in|Delay)?[^0-9]*([0-9]+(?:\.[0-9]+)?)\s*s/i);
  return m ? Math.ceil(Number(m[1]) * 1000) : 0;
}

function isGeminiQuotaError(e) {
  const x = safeError(e).toLowerCase();
  return e?.response?.status === 429 ||
    x.includes('quota') ||
    x.includes('resource_exhausted') ||
    x.includes('rate limit');
}

function isGeminiModelError(e) {
  const x = safeError(e).toLowerCase();
  return e?.response?.status === 404 ||
    x.includes('not found') ||
    x.includes('not supported') ||
    x.includes('invalid model');
}

function currentGeminiModel() {
  return GEMINI_MODELS[state.geminiActiveModelIndex] || GEMINI_MODELS[0];
}

function advanceGeminiModel(reason) {
  const prev = currentGeminiModel();
  if (state.geminiActiveModelIndex < GEMINI_MODELS.length - 1) {
    state.geminiActiveModelIndex++;
    state.aiFallbackUsed++;
    console.warn(`[GEMINI FALLBACK] ${prev.name} → ${currentGeminiModel().name} (${reason})`);
    return true;
  }
  // كل الموديلات فشلت → Circuit Breaker
  state.geminiBlockedUntil = Date.now() + GEMINI_CIRCUIT_RESET_MS;
  state.geminiReady = false;
  state.lastAiError = `All Gemini models exhausted: ${reason}. Circuit open for 1h.`;
  console.error('[GEMINI CIRCUIT OPEN]', reason);
  return false;
}

function resetGeminiModelIfNeeded() {
  if (Date.now() >= state.geminiBlockedUntil && state.geminiBlockedUntil !== 0) {
    state.geminiActiveModelIndex = 0;
    state.geminiModelFailures.clear();
    state.geminiBlockedUntil = 0;
    console.log('[GEMINI] Circuit closed; reset to primary model');
  }
}

async function queueGemini(task, kind) {
  const run = geminiChain.then(async () => {
    await ensureUsageDays();
    resetGeminiModelIfNeeded();

    if (Date.now() < state.geminiBlockedUntil) {
      const msg = `Gemini paused until ${new Date(state.geminiBlockedUntil).toISOString()}`;
      state.lastAiError = msg;
      throw new Error(msg);
    }

    const total = n(account?.geminiCallsUsed);
    const usedKind = kind === 'entry' ? n(account?.geminiEntryUsed) : n(account?.geminiManageUsed);
    const kindCap = kind === 'entry' ? GEMINI_ENTRY_DAILY_CAP : GEMINI_MANAGE_DAILY_CAP;

    if (total >= GEMINI_TOTAL_DAILY_CAP || usedKind >= kindCap) {
      const msg = `Gemini local ${kind} cap reached`;
      state.lastAiError = msg;
      throw new Error(msg);
    }

    const wait = GEMINI_CALL_GAP_MS - (Date.now() - lastGeminiAt);
    if (wait > 0) await sleep(wait);
    lastGeminiAt = Date.now();

    try {
      const out = await task();
      // ← العداد يزيد فقط بعد النجاح
      if (account) {
        account.geminiCallsUsed = total + 1;
        if (kind === 'entry') account.geminiEntryUsed = usedKind + 1;
        else account.geminiManageUsed = usedKind + 1;
        await saveAccount();
      }
      return out;
    } catch (e) {
      // فشل → نتنقل للنموذج التالي بدون زيادة العداد
      if (isGeminiQuotaError(e) || isGeminiModelError(e)) {
        advanceGeminiModel(safeError(e));
      }
      throw e;
    }
  });
  geminiChain = run.catch(() => {});
  return run;
}

function extractJson(text) {
  let s = String(text || '').trim().replace(/^```json/i, '').replace(/^```/, '').replace(/```$/, '').trim();
  try {
    return JSON.parse(s);
  } catch (_) {
    const a = s.indexOf('{'), b = s.lastIndexOf('}');
    if (a < 0 || b <= a) throw new Error('No JSON object in Gemini response');
    return JSON.parse(s.slice(a, b + 1));
  }
}

async function callGemini(prompt, kind) {
  if (!GEMINI_API_KEY) {
    state.geminiReady = false;
    throw new Error('GEMINI_API_KEY is missing');
  }
  // محاولة على كل الموديلات المتاحة بالترتيب
  let lastErr = null;
  for (let attempt = 0; attempt < GEMINI_MODELS.length; attempt++) {
    const model = currentGeminiModel();
    try {
      return await queueGemini(async () => {
        const r = await http.post(
          `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model.name)}:generateContent`,
          {
            contents: [{ role: 'user', parts: [{ text: prompt }] }],
            generationConfig: {
              responseMimeType: 'application/json',
              maxOutputTokens: 500
            }
          },
          { headers: { 'x-goog-api-key': GEMINI_API_KEY }, timeout: AI.timeoutMs }
        );
        const text = r?.data?.candidates?.[0]?.content?.parts?.map(x => x.text || '').join('');
        if (!text) throw new Error('Gemini returned empty response');
        state.geminiReady = true;
        state.lastAiError = null;
        return extractJson(text);
      }, kind);
    } catch (e) {
      lastErr = e;
      state.lastAiError = safeError(e);
      // لو الخطأ مش quota/model → مفيش فايدة من المحاولة على نموذج تاني
      if (!isGeminiQuotaError(e) && !isGeminiModelError(e)) throw e;
      // لو وصلنا لآخر موديل، advanceGeminiModel هيفتح Circuit ويرمي
    }
  }
  // كل المحاولات فشلت
  if (lastErr) {
    state.geminiReady = false;
    throw lastErr;
  }
  throw new Error('Gemini unavailable');
}

async function getAiMemory(symbol) {
  if (!state.mongoReady) return [];
  const rows = await Trade.find({ status: 'CLOSED' }).sort({ closedAt: -1 }).limit(AI.memoryClosedTrades).lean();
  return rows.map(t => ({
    symbol: t.symbol,
    sameSymbol: t.symbol === symbol,
    direction: t.direction,
    confidence: t.confidence,
    confluence: n(t.confluenceScore),
    resultR: n(t.resultR),
    pnl: n(t.totalPnl),
    mfeR: n(t.mfeR),
    maeR: n(t.maeR),
    entryReason: String(t.entryReason || '').slice(0, 250),
    managementReason: String(t.managementReason || '').slice(0, 250)
  }));
}

async function askEntryCommander({ symbol, mtf, session, strength, news, memory, confluence }) {
  state.aiEntryCalls++;
  const prompt = `You are the entry commander for LOMY FOREX V2.0. PAPER TRADING ONLY.
Decide BUY, SELL, or NO_TRADE using only supplied market evidence. Do not invent data.
If BUY or SELL: confidence must be 62-100 and stopLoss must be a technical price based on structure, volatility, support/resistance, swing, liquidity, order block, or invalidation. Do not size the position.
If evidence is weak/conflicting, current news risk is unsafe, or there is no clean technical stop, return NO_TRADE.
Return JSON only: {"decision":"BUY|SELL|NO_TRADE","confidence":0,"stopLoss":null,"reason":"short precise reason"}
DATA:${JSON.stringify({
    symbol, session, strength, news,
    confluence: { score: confluence.total, breakdown: confluence.breakdown },
    mtf: {
      m15: compactTechnical(mtf.m15),
      h1: compactTechnical(mtf.h1),
      h4: compactTechnical(mtf.h4)
    },
    recentClosedTrades: memory
  })}`;

  const r = await callGemini(prompt, 'entry');
  const decision = String(r?.decision || 'NO_TRADE').toUpperCase();
  const confidence = clamp(n(r?.confidence), 0, 100);
  const stopLoss = n(r?.stopLoss, NaN);
  const reason = String(r?.reason || '').slice(0, 800);

  if (!['BUY', 'SELL', 'NO_TRADE'].includes(decision) || decision === 'NO_TRADE' ||
      confidence < RULES.minEntryConfidence || !Number.isFinite(stopLoss)) {
    state.aiNoTradeDecisions++;
    return { decision: 'NO_TRADE', confidence, stopLoss: NaN, reason: reason || 'Rejected Gemini entry' };
  }

  if (decision === 'BUY') state.aiBuyDecisions++;
  else state.aiSellDecisions++;

  return { decision, confidence, stopLoss, reason };
}

async function askTradeManager({ trade, technical, memory, currentPrice }) {
  state.aiManageCalls++;
  const prompt = `You manage an EXISTING PAPER forex trade. Mechanical risk management is controlled by the bot.
You may ONLY decide HOLD or CLOSE.
You may not change stop loss, take profit, trailing stop, position size, or partial-close rules.
CLOSE only when supplied evidence materially invalidates the trade thesis.
Return JSON only: {"decision":"HOLD|CLOSE","confidence":0,"reason":"short precise reason"}
DATA:${JSON.stringify({
    symbol: trade.symbol,
    direction: trade.direction,
    entryPrice: trade.entryPrice,
    currentPrice,
    stopLoss: trade.stopLoss,
    currentR: tradePriceR(trade, currentPrice),
    partialClosed: trade.partialClosed,
    trailingLevelR: trade.trailingLevelR,
    confidence: trade.confidence,
    entryReason: trade.entryReason,
    technical: compactTechnical(technical),
    recentMemory: memory
  })}`;

  const r = await callGemini(prompt, 'manage');
  const d = String(r?.decision || 'HOLD').toUpperCase();
  const c = clamp(n(r?.confidence), 0, 100);
  const reason = String(r?.reason || '').slice(0, 800);

  if (d === 'CLOSE' && c >= RULES.minCloseConfidence) {
    state.aiCloseDecisions++;
    return { decision: 'CLOSE', confidence: c, reason };
  }

  state.aiHoldDecisions++;
  return { decision: 'HOLD', confidence: c, reason };
}

// ═══════════════════════════════════════════════════════════
// RISK / POSITION SIZING
// ═══════════════════════════════════════════════════════════
function riskPctFromConfidence(c) {
  if (c >= DYNAMIC_RISK.highConfidence) return Math.min(DYNAMIC_RISK.highRiskPct, PAPER.maxCapitalRiskPct);
  if (c >= DYNAMIC_RISK.medConfidence) return Math.min(DYNAMIC_RISK.medRiskPct, PAPER.maxCapitalRiskPct);
  if (c >= DYNAMIC_RISK.lowConfidence) return Math.min(DYNAMIC_RISK.lowRiskPct, PAPER.maxCapitalRiskPct);
  return 0;
}

function quoteCurrency(s) { return s === 'XAUUSD' ? 'USD' : String(s).slice(3, 6).toUpperCase(); }
function cachedPairPrice(s) {
  const p = n(last(state.pairState.get(s)?.bars15m)?.close, NaN);
  return Number.isFinite(p) && p > 0 ? p : NaN;
}

async function currencyToUsdRate(currency) {
  currency = String(currency || '').toUpperCase();
  if (!currency || currency === 'USD') return 1;
  const direct = `${currency}USD`, inverse = `USD${currency}`;
  const dp = cachedPairPrice(direct), ip = cachedPairPrice(inverse);
  if (Number.isFinite(dp)) return dp;
  if (Number.isFinite(ip)) return 1 / ip;
  try {
    return await fetchCurrentPrice(direct);
  } catch (_) {
    const p = await fetchCurrentPrice(inverse);
    if (!Number.isFinite(p) || p <= 0) throw new Error(`Cannot convert ${currency} to USD`);
    return 1 / p;
  }
}

async function calculatePositionSize({ symbol, entryPrice, stopLoss, riskAmount }) {
  const distance = Math.abs(entryPrice - stopLoss);
  if (!Number.isFinite(distance) || distance <= 0) throw new Error('Invalid stop distance');
  const q = quoteCurrency(symbol);
  const rate = await currencyToUsdRate(q);
  if (!Number.isFinite(rate) || rate <= 0) throw new Error(`Invalid ${q}/USD conversion`);
  const riskPerUnitUsd = distance * rate;
  const quantity = riskAmount / riskPerUnitUsd;
  if (!Number.isFinite(quantity) || quantity <= 0) throw new Error('Invalid calculated quantity');
  return { quantity, quoteCurrency: q, quoteToUsd: rate, stopDistance: distance, riskPerUnitUsd };
}

async function calculatePnlUsd({ symbol, direction, entryPrice, exitPrice, quantity }) {
  const raw = (direction === 'BUY' ? exitPrice - entryPrice : entryPrice - exitPrice) * quantity;
  const rate = await currencyToUsdRate(quoteCurrency(symbol));
  return raw * rate;
}

function currentPortfolioRiskUsd() {
  let total = 0;
  for (const t of state.openTrades.values()) {
    const iq = Math.max(Number.EPSILON, n(t.initialQuantity, t.quantity));
    const rf = clamp(n(t.quantity) / iq, 0, 1);
    total += Math.max(0, n(t.riskAmount)) * rf;
  }
  return total;
}

function portfolioRiskCapUsd() {
  return n(account?.balance, PAPER.startingBalance) * PAPER.portfolioRiskCapPct / 100;
}

async function validateEntryRisk({ symbol, direction, confidence, technicalStop, quote, technical, confluence }) {
  if (!account) return { approved: false, reason: 'Paper account unavailable' };
  if (state.openTrades.size >= PAPER.maxOpenTrades) return { approved: false, reason: 'Maximum open trades reached' };
  if (state.openTrades.has(symbol)) return { approved: false, reason: 'Symbol already has an open trade' };
  if (isCorrelated(symbol)) return { approved: false, reason: 'Correlated pair already open' };
  if (isProtectionTriggered()) return { approved: false, reason: protectionReason() };
  if (!isSessionAllowed()) return { approved: false, reason: 'Outside allowed sessions' };

  const entryPrice = entryExecutionPrice(direction, quote);
  const stopLoss = n(technicalStop, NaN);
  const atr = n(technical?.volatility?.atr, NaN);

  if (!Number.isFinite(entryPrice) || !Number.isFinite(stopLoss))
    return { approved: false, reason: 'Invalid live entry or stop price' };
  if (direction === 'BUY' && stopLoss >= entryPrice)
    return { approved: false, reason: 'BUY stop must be below entry' };
  if (direction === 'SELL' && stopLoss <= entryPrice)
    return { approved: false, reason: 'SELL stop must be above entry' };
  if (!Number.isFinite(atr) || atr <= 0)
    return { approved: false, reason: 'ATR unavailable' };

  const stopDistance = Math.abs(entryPrice - stopLoss);
  const stopAtr = stopDistance / atr;
  if (stopAtr < RULES.minStopAtr)
    return { approved: false, reason: `Technical stop too tight (${stopAtr.toFixed(2)} ATR)` };
  if (stopAtr > RULES.maxStopAtr)
    return { approved: false, reason: `Technical stop too wide (${stopAtr.toFixed(2)} ATR)` };
  if (quote.spreadKnown && n(quote.spread) > stopDistance * RULES.maxSpreadRiskFraction)
    return { approved: false, reason: 'Spread too large relative to stop distance' };

  let riskPct = riskPctFromConfidence(confidence);
  if (riskPct <= 0 || riskPct > PAPER.maxCapitalRiskPct)
    return { approved: false, reason: 'Invalid risk percentage' };

  // تقليل الحجم بعد خسائر متتالية
  if (n(account.consecutiveLosses) >= PROTECTION.consecutiveLossReduce) {
    riskPct = riskPct * 0.5;
  }

  const riskAmount = n(account.balance) * riskPct / 100;
  if (currentPortfolioRiskUsd() + riskAmount > portfolioRiskCapUsd() + 1e-9)
    return { approved: false, reason: 'Portfolio risk cap would be exceeded' };

  let sizing;
  try {
    sizing = await calculatePositionSize({ symbol, entryPrice, stopLoss, riskAmount });
  } catch (e) {
    return { approved: false, reason: `Sizing failed: ${safeError(e)}` };
  }

  return {
    approved: true,
    entryPrice,
    stopLoss,
    riskDistance: stopDistance,
    riskPct,
    riskAmount,
    quantity: sizing.quantity,
    initialQuantity: sizing.quantity,
    partialTargetPrice: direction === 'BUY'
      ? entryPrice + stopDistance * RULES.partialTpTriggerR
      : entryPrice - stopDistance * RULES.partialTpTriggerR,
    rewardRisk: RULES.riskReward,
    spreadKnown: quote.spreadKnown,
    confluenceScore: confluence?.total || 0
  };
}

module.exports = {
  // MongoDB
  initMongo, restoreOpenTrades, saveTrade, journal, ensureUsageDays, creditAccountOnce,
  saveAccount, markAccountKeyOnly, reconcilePnl, Account, Trade, Journal, NewsCache,
  // Twelve
  fetchBars, fetchCurrentPrice, fetchQuote, initializeSymbol, initializeMarket,
  ensurePair, refreshSymbol, entryExecutionPrice, exitExecutionPrice,
  // News / Session
  getMarketSession, isSessionAllowed, fetchEconomicNews, getNewsBlock,
  calculateCurrencyStrength, pairStrengthContext,
  // Protection
  isDailyLossLimitHit, isWeeklyLossLimitHit, isCooldownActive, isCorrelated,
  isProtectionTriggered, protectionReason,
  // Telegram
  initTelegram, sendTelegram, telegramStatusText, captureTelegramChat,
  // Gemini
  callGemini, queueGemini, askEntryCommander, askTradeManager,
  getAiMemory, currentGeminiModel, advanceGeminiModel, resetGeminiModelIfNeeded,
  // Risk
  riskPctFromConfidence, calculatePositionSize, calculatePnlUsd,
  currentPortfolioRiskUsd, portfolioRiskCapUsd, validateEntryRisk, currencyToUsdRate
};
// ═══════════════════════════════════════════════════════════
// TRADE LIFECYCLE
// ═══════════════════════════════════════════════════════════
async function openPaperTrade({ symbol, direction, confidence, reason, aiDecision, technical, risk, confluence }) {
  if (MODE !== 'PAPER' || LIVE_TRADING !== false)
    throw new Error('Live trading is forbidden');
  if (!risk?.approved) throw new Error('Cannot open unapproved trade');

  const tradeData = {
    tradeId: `${symbol}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    symbol,
    direction,
    status: 'OPEN',
    entryPrice: risk.entryPrice,
    stopLoss: risk.stopLoss,
    initialStopLoss: risk.stopLoss,
    partialTargetPrice: risk.partialTargetPrice,
    quantity: risk.quantity,
    initialQuantity: risk.initialQuantity,
    riskAmount: risk.riskAmount,
    riskPct: risk.riskPct,
    confidence,
    confluenceScore: confluence?.total || 0,
    entryReason: reason,
    managementReason: '',
    openedAt: new Date(),
    realizedPartialPnl: 0,
    totalPnl: 0,
    partialClosed: false,
    trailingLevelR: 0,
    breakEvenActivated: false,
    atrTrailLevel: NaN,
    maxFavorablePrice: risk.entryPrice,
    maxAdversePrice: risk.entryPrice,
    mfeR: 0,
    maeR: 0,
    lastManagedBarTime: new Date(parseTime(technical.barTime) || Date.now()),
    settlementVersion: 'V2',
    finalRemainingPnl: 0,
    aiEntryDecision: aiDecision,
    technicalSnapshot: compactTechnical(technical),
    correlationKey: correlationKey(symbol)
  };

  const doc = await Trade.create(tradeData);
  const trade = doc.toObject();
  state.openTrades.set(symbol, trade);
  state.executedSignals++;

  await journal('TRADE_OPENED', {
    symbol, tradeId: trade.tradeId, direction, confidence,
    confluence: confluence?.total || 0,
    breakdown: confluence?.breakdown || [],
    entryPrice: risk.entryPrice,
    stopLoss: risk.stopLoss,
    riskPct: risk.riskPct,
    riskAmount: risk.riskAmount,
    quantity: risk.quantity,
    partialTargetPrice: risk.partialTargetPrice,
    spreadKnown: risk.spreadKnown,
    message: reason
  });

  await sendTelegram([
    'LOMY PAPER TRADE OPENED',
    `${symbol} ${direction}`,
    `Entry: ${fmtPrice(risk.entryPrice, symbol)}`,
    `SL: ${fmtPrice(risk.stopLoss, symbol)}`,
    `+2R: ${fmtPrice(risk.partialTargetPrice, symbol)}`,
    `Risk: ${risk.riskPct.toFixed(2)}% (${fmtMoney(risk.riskAmount)})`,
    `Confidence: ${confidence.toFixed(0)}%`,
    `Confluence: ${confluence?.total || 0}`,
    'Plan: BE +0.60R | close 50% +2R | trail remaining 50%'
  ].join('\n'));

  return trade;
}

function tradePriceR(t, p) {
  const d = Math.abs(t.entryPrice - t.initialStopLoss);
  if (!Number.isFinite(d) || d <= 0) return 0;
  return t.direction === 'BUY' ? (p - t.entryPrice) / d : (t.entryPrice - p) / d;
}

function stopPriceAtR(t, r) {
  const d = Math.abs(t.entryPrice - t.initialStopLoss);
  return t.direction === 'BUY' ? t.entryPrice + d * r : t.entryPrice - d * r;
}

function improveStop(t, c) {
  if (!Number.isFinite(c)) return false;
  if (t.direction === 'BUY' && c > t.stopLoss) { t.stopLoss = c; return true; }
  if (t.direction === 'SELL' && c < t.stopLoss) { t.stopLoss = c; return true; }
  return false;
}

function updateExcursionsFromBar(t, b) {
  if (t.direction === 'BUY') {
    t.maxFavorablePrice = Math.max(n(t.maxFavorablePrice, t.entryPrice), b.high);
    t.maxAdversePrice = Math.min(n(t.maxAdversePrice, t.entryPrice), b.low);
  } else {
    t.maxFavorablePrice = Math.min(n(t.maxFavorablePrice, t.entryPrice), b.low);
    t.maxAdversePrice = Math.max(n(t.maxAdversePrice, t.entryPrice), b.high);
  }
  t.mfeR = Math.max(n(t.mfeR), tradePriceR(t, t.maxFavorablePrice));
  t.maeR = Math.min(n(t.maeR), tradePriceR(t, t.maxAdversePrice));
}

function stopFillFromBar(t, b, stop = t.stopLoss) {
  if (t.direction === 'BUY' && b.low <= stop) return b.open < stop ? b.open : stop;
  if (t.direction === 'SELL' && b.high >= stop) return b.open > stop ? b.open : stop;
  return NaN;
}

// ── تحديث إحصائيات الحماية عند إغلاق صفقة ─────────────────
async function recordTradeResult(pnl) {
  if (!account) return;
  account.dailyPnl = n(account.dailyPnl) + pnl;
  account.weeklyPnl = n(account.weeklyPnl) + pnl;
  if (pnl < 0) {
    account.consecutiveLosses = n(account.consecutiveLosses) + 1;
    account.lastLossAt = new Date();
  } else if (pnl > 0) {
    account.consecutiveLosses = 0;
  }
  await saveAccount();
}

async function closeTradeAtPrice(t, price, reason) {
  if (!t || t.status !== 'OPEN') return null;
  const current = state.openTrades.get(t.symbol);
  if (!current || current.tradeId !== t.tradeId) return null;

  const qty = Math.max(0, n(t.quantity));
  const remainingPnl = qty
    ? await calculatePnlUsd({
        symbol: t.symbol, direction: t.direction,
        entryPrice: t.entryPrice, exitPrice: price, quantity: qty
      })
    : 0;

  const totalPnl = n(t.realizedPartialPnl) + remainingPnl;

  t.status = 'CLOSED';
  t.exitPrice = price;
  t.closedAt = new Date();
  t.quantity = 0;
  t.totalPnl = totalPnl;
  t.finalRemainingPnl = remainingPnl;
  t.settlementVersion = 'V2';
  t.managementReason = String(reason).slice(0, 800);
  t.resultR = totalPnl / Math.max(Number.EPSILON, n(t.riskAmount));

  await saveTrade(t);
  state.openTrades.delete(t.symbol);
  await creditAccountOnce(`${t.tradeId}:final`, remainingPnl);
  await recordTradeResult(totalPnl);

  await journal('TRADE_CLOSED', {
    symbol: t.symbol, tradeId: t.tradeId, direction: t.direction,
    exitPrice: price, remainingPnl, partialPnl: n(t.realizedPartialPnl),
    totalPnl, resultR: t.resultR, mfeR: t.mfeR, maeR: t.maeR,
    message: reason
  });

  await sendTelegram([
    'LOMY PAPER TRADE CLOSED',
    `${t.symbol} ${t.direction}`,
    `Exit: ${fmtPrice(price, t.symbol)}`,
    `Total PnL: ${fmtMoney(totalPnl)}`,
    `Result: ${t.resultR.toFixed(2)}R`,
    `Balance: ${fmtMoney(account.balance)}`,
    `Reason: ${reason}`
  ].join('\n'));

  return t;
}

async function partialCloseAtPrice(t, price) {
  const q = Math.min(n(t.initialQuantity) * 0.5, n(t.quantity));
  if (q <= 0) return false;

  const pnl = await calculatePnlUsd({
    symbol: t.symbol, direction: t.direction,
    entryPrice: t.entryPrice, exitPrice: price, quantity: q
  });

  t.quantity = Math.max(0, n(t.quantity) - q);
  t.realizedPartialPnl = n(t.realizedPartialPnl) + pnl;
  t.partialClosed = true;
  t.settlementVersion = 'V2';

  improveStop(t, stopPriceAtR(t, RULES.trailingStartStopR));
  t.trailingLevelR = RULES.trailingStartStopR;
  // تفعيل ATR trailing
  t.atrTrailLevel = stopPriceAtR(t, RULES.atrTrailStartR);

  await saveTrade(t);
  await creditAccountOnce(`${t.tradeId}:partial`, pnl);
  await recordTradeResult(pnl);

  await journal('PARTIAL_CLOSE', {
    symbol: t.symbol, tradeId: t.tradeId,
    closeQuantity: q, exitPrice: price, partialPnl: pnl,
    remainingQuantity: t.quantity, newStop: t.stopLoss,
    message: 'Closed 50% at +2R; remaining 50% protected at +1R from next bar'
  });

  await sendTelegram([
    'LOMY PARTIAL CLOSE',
    `${t.symbol} ${t.direction}`,
    'Reached +2R | Closed 50%',
    `Partial PnL: ${fmtMoney(pnl)}`,
    `Remaining: ${n(t.quantity).toFixed(4)}`,
    `New SL: ${fmtPrice(t.stopLoss, t.symbol)}`
  ].join('\n'));

  return true;
}

// ── ATR-based Dynamic Trailing ─────────────────────────────
function applyAtrTrailing(t, bar, atr) {
  if (!Number.isFinite(atr) || atr <= 0) return false;
  const r = tradePriceR(t, last([bar]).close);
  if (r < RULES.atrTrailStartR) return false;

  const desired = t.direction === 'BUY'
    ? bar.close - atr * RULES.atrTrailMult
    : bar.close + atr * RULES.atrTrailMult;

  if (improveStop(t, desired)) {
    t.atrTrailLevel = desired;
    return true;
  }
  return false;
}

async function manageMechanicalBar(t, b) {
  if (!t || t.status !== 'OPEN') return false;
  const bt = barTimeMs(b);
  const lastManaged = parseTime(t.lastManagedBarTime);
  if (!bt || bt <= lastManaged) return true;

  const stopAtBarStart = t.stopLoss;
  const fill = stopFillFromBar(t, b, stopAtBarStart);

  if (Number.isFinite(fill)) {
    t.lastManagedBarTime = new Date(bt);
    await closeTradeAtPrice(t, fill,
      t.partialClosed ? 'TRAILING_STOP_HIT'
      : t.breakEvenActivated ? 'BREAK_EVEN_STOP_HIT'
      : 'STOP_LOSS_HIT');
    return false;
  }

  updateExcursionsFromBar(t, b);

  const favorable = t.direction === 'BUY' ? b.high : b.low;
  const maxR = tradePriceR(t, favorable);

  if (!t.breakEvenActivated && maxR >= RULES.breakEvenTriggerR) {
    improveStop(t, t.entryPrice);
    t.breakEvenActivated = true;
    await journal('BREAK_EVEN', {
      symbol: t.symbol, tradeId: t.tradeId,
      currentR: maxR, newStop: t.stopLoss,
      message: 'Break-even armed; new stop applies from next 15m bar'
    });
  }

  if (!t.partialClosed && maxR >= RULES.partialTpTriggerR) {
    await partialCloseAtPrice(t, t.partialTargetPrice);
  }
  if (t.status !== 'OPEN') return false;

  // Trailing R-based
  if (t.partialClosed && maxR > RULES.partialTpTriggerR) {
    const steps = Math.floor((maxR - RULES.partialTpTriggerR) / RULES.trailingStepR);
    const desiredR = RULES.trailingStartStopR + steps * RULES.trailingStepR;
    if (desiredR > n(t.trailingLevelR) && improveStop(t, stopPriceAtR(t, desiredR))) {
      t.trailingLevelR = desiredR;
      await journal('TRAILING_STOP', {
        symbol: t.symbol, tradeId: t.tradeId,
        currentR: maxR, trailingLevelR: desiredR, newStop: t.stopLoss,
        message: `Trailing stop moved to +${desiredR.toFixed(2)}R; applies from next 15m bar`
      });
    }
  }

  // ATR trailing (إضافة جديدة)
  if (t.partialClosed && t.status === 'OPEN') {
    const atr = atrLast([b], TECH.atrLen);
    const prevStop = t.stopLoss;
    if (applyAtrTrailing(t, b, atr) && t.stopLoss !== prevStop) {
      await journal('ATR_TRAILING', {
        symbol: t.symbol, tradeId: t.tradeId,
        atr, newStop: t.stopLoss,
        message: `ATR trailing stop moved; applies from next 15m bar`
      });
    }
  }

  t.lastManagedBarTime = new Date(bt);
  await saveTrade(t);
  return true;
}

function shouldAskManager(t, tech) {
  if (!AI.managementEnabled || !tech) return false;
  const opposite = t.direction === 'BUY' ? tech.bias === 'BEAR' : tech.bias === 'BULL';
  const r = tradePriceR(t, tech.price);
  return opposite && r <= 0.25;
}

async function maybeAiManage(t, tech) {
  if (!shouldAskManager(t, tech)) return;
  await ensureUsageDays();
  if (n(account?.geminiManageUsed) >= GEMINI_MANAGE_DAILY_CAP ||
      n(account?.geminiCallsUsed) >= GEMINI_TOTAL_DAILY_CAP) return;

  try {
    const d = await askTradeManager({
      trade: t, technical: tech,
      memory: await getAiMemory(t.symbol),
      currentPrice: tech.price
    });

    if (d.decision === 'CLOSE') {
      let price = tech.price;
      try {
        const q = await fetchQuote(t.symbol);
        price = exitExecutionPrice(t.direction, q);
      } catch (e) {
        console.warn(`[AI CLOSE QUOTE] ${t.symbol}:`, safeError(e));
      }
      await closeTradeAtPrice(t, price,
        `AI_CLOSE ${d.confidence.toFixed(0)}%: ${d.reason}`);
    } else {
      t.managementReason = `AI_HOLD ${d.confidence.toFixed(0)}%: ${d.reason}`;
      await saveTrade(t);
    }
  } catch (e) {
    console.error(`[AI MANAGE] ${t.symbol}:`, safeError(e));
  }
}

// ═══════════════════════════════════════════════════════════
// ENTRY PROCESSING
// ═══════════════════════════════════════════════════════════
async function processCandidate(c) {
  const { symbol, technical, mtf, confluence } = c;
  const pair = state.pairState.get(symbol);
  if (!pair) return false;

  const barTime = technical.barTime;

  // حماية رأس المال
  if (isProtectionTriggered()) {
    state.skippedSignals++;
    pair.lastEvaluatedBarTime = barTime;
    await journal('PROTECTION_BLOCK', {
      symbol, message: protectionReason()
    });
    return false;
  }

  // الجلسة
  if (!isSessionAllowed()) {
    state.skippedSignals++;
    pair.lastEvaluatedBarTime = barTime;
    await journal('SESSION_BLOCK', {
      symbol, session: getMarketSession(),
      message: 'Outside allowed sessions'
    });
    return false;
  }

  const news = getNewsBlock(symbol);
  if (news.blocked) {
    state.skippedSignals++;
    pair.lastEvaluatedBarTime = barTime;
    await journal('NEWS_BLOCK', {
      symbol, events: news.events,
      message: 'High-impact news block'
    });
    return false;
  }

  if (!AI.entryCommanderEnabled || !GEMINI_API_KEY) {
    state.skippedSignals++;
    pair.lastEvaluatedBarTime = barTime;
    await journal('ENTRY_SKIPPED', {
      symbol, message: 'Gemini unavailable - fail closed'
    });
    return false;
  }

  // لو Circuit Breaker شغال، منحاولش أصلاً
  if (Date.now() < state.geminiBlockedUntil) {
    state.skippedSignals++;
    pair.lastEvaluatedBarTime = barTime;
    return false;
  }

  await ensureUsageDays();

  if (Date.now() - state.lastEntryAiAt < GEMINI_ENTRY_MIN_GAP_MS ||
      n(account?.geminiEntryUsed) >= GEMINI_ENTRY_DAILY_CAP ||
      n(account?.geminiCallsUsed) >= GEMINI_TOTAL_DAILY_CAP) {
    state.skippedSignals++;
    pair.lastEvaluatedBarTime = barTime;
    return false;
  }

  let decision;
  try {
    decision = await askEntryCommander({
      symbol, mtf,
      session: getMarketSession(),
      strength: pairStrengthContext(symbol),
      news,
      memory: await getAiMemory(symbol),
      confluence
    });
    state.lastEntryAiAt = Date.now();
  } catch (e) {
    state.skippedSignals++;
    await journal('AI_ENTRY_ERROR', { symbol, message: safeError(e) });
    console.error(`[AI ENTRY] ${symbol}:`, safeError(e));
    return false;
  }

  if (decision.decision === 'NO_TRADE') {
    state.skippedSignals++;
    pair.lastEvaluatedBarTime = barTime;
    await journal('NO_TRADE', {
      symbol, confidence: decision.confidence,
      confluence: confluence?.total, message: decision.reason
    });
    return false;
  }

  let quote;
  try {
    quote = await fetchQuote(symbol);
  } catch (e) {
    state.skippedSignals++;
    pair.lastEvaluatedBarTime = barTime;
    await journal('QUOTE_REJECT', {
      symbol, message: `Live quote unavailable: ${safeError(e)}`
    });
    console.error(`[ENTRY QUOTE] ${symbol}:`, safeError(e));
    return false;
  }

  let risk;
  try {
    risk = await validateEntryRisk({
      symbol, direction: decision.decision,
      confidence: decision.confidence,
      technicalStop: decision.stopLoss,
      quote, technical, confluence
    });
  } catch (e) {
    risk = { approved: false, reason: safeError(e) };
  }

  if (!risk.approved) {
    state.skippedSignals++;
    pair.lastEvaluatedBarTime = barTime;
    await journal('RISK_REJECT', {
      symbol, direction: decision.decision,
      confidence: decision.confidence,
      message: risk.reason
    });
    return false;
  }

  try {
    await openPaperTrade({
      symbol, direction: decision.decision,
      confidence: decision.confidence,
      reason: decision.reason,
      aiDecision: decision, technical, risk, confluence
    });
    pair.lastEvaluatedBarTime = barTime;
    return true;
  } catch (e) {
    state.skippedSignals++;
    pair.lastEvaluatedBarTime = barTime;
    await journal('OPEN_ERROR', { symbol, message: safeError(e) });
    console.error(`[OPEN] ${symbol}:`, safeError(e));
    return false;
  }
}

async function manageSymbolTrade(symbol, pair) {
  if (state.tradeLocks.has(symbol)) return;
  state.tradeLocks.add(symbol);
  try {
    const t = state.openTrades.get(symbol);
    if (!t) return;

    const start = parseTime(t.lastManagedBarTime || t.openedAt);
    const bars = pair.bars15m.filter(b => barTimeMs(b) > start);

    for (const b of bars) {
      const current = state.openTrades.get(symbol);
      if (!current || current.tradeId !== t.tradeId) break;
      const open = await manageMechanicalBar(current, b);
      if (!open) break;
    }

    const current = state.openTrades.get(symbol);
    if (current) {
      const tech = buildTechnicalIntelligence(pair.bars15m);
      await maybeAiManage(current, tech);
    }
  } finally {
    state.tradeLocks.delete(symbol);
  }
}

async function scanMarket() {
  if (state.scanBusy) return;
  state.scanBusy = true;

  try {
    const candidates = [];

    for (const symbol of marketSymbols()) {
      if (Date.now() < state.twelveBlockedUntil) break;

      const r = await refreshSymbol(symbol);
      if (!r) continue;
      const { pair, newBar } = r;

      if (state.openTrades.has(symbol))
        await manageSymbolTrade(symbol, pair);

      if (!newBar) continue;
      state.scannedBars++;

      if (state.openTrades.has(symbol)) {
        pair.lastEvaluatedBarTime = r.latest.openTime;
        continue;
      }

      if (state.openTrades.size >= PAPER.maxOpenTrades) {
        state.skippedSignals++;
        pair.lastEvaluatedBarTime = r.latest.openTime;
        continue;
      }

      // حماية رأس المال / الجلسة على مستوى الـ scan
      if (isProtectionTriggered() || !isSessionAllowed()) {
        state.skippedSignals++;
        pair.lastEvaluatedBarTime = r.latest.openTime;
        continue;
      }

      const mtf = buildMtf(pair);
      const c = localCandidate(symbol, mtf);

      if (c) candidates.push(c);
      else {
        state.skippedSignals++;
        pair.lastEvaluatedBarTime = r.latest.openTime;
      }
    }

    candidates.sort((a, b) => b.rank - a.rank);

    // نأخذ أفضل مرشح واحد فقط لكل scan (لتوفير الكوتا)
    if (candidates.length)
      await processCandidate(candidates[0]);

    for (const c of candidates.slice(1)) {
      const p = state.pairState.get(c.symbol);
      if (p) p.lastEvaluatedBarTime = c.technical.barTime;
      state.skippedSignals++;
    }

    state.marketReady = marketSymbols().some(s => state.pairState.has(s));
  } finally {
    state.scanBusy = false;
  }
}

function scheduleNextScan() {
  const now = Date.now();
  const next = Math.floor(now / TIMEFRAME_MS) * TIMEFRAME_MS + TIMEFRAME_MS + 20000;
  const delay = Math.max(5000, next - now);
  state.nextScanAt = new Date(now + delay);

  setTimeout(async () => {
    try {
      await scanMarket();
    } catch (e) {
      console.error('[SCAN]', safeError(e));
    }
    scheduleNextScan();
  }, delay);
}

function startLoops() {
  if (state.loopsStarted) return;
  state.loopsStarted = true;
  scheduleNextScan();
  setInterval(
    () => fetchEconomicNews().catch(e => console.error('[NEWS LOOP]', safeError(e))),
    NEWS_REFRESH_MS
  );
  console.log('[LOOPS] started');
}

// ═══════════════════════════════════════════════════════════
// STATUS / WEB
// ═══════════════════════════════════════════════════════════
function buildStatus() {
  const model = currentGeminiModel();
  return {
    version: VERSION,
    mode: MODE,
    liveTrading: LIVE_TRADING,
    uptimeSeconds: Math.floor((Date.now() - state.startedAt.getTime()) / 1000),
    activeSymbols: ACTIVE_SYMBOLS,
    ready: {
      mongo: state.mongoReady,
      telegram: state.telegramReady,
      telegramPolling: state.telegramPollingReady,
      market: state.marketReady,
      gemini: state.geminiReady,
      news: state.newsReady
    },
    account: {
      startingBalance: PAPER.startingBalance,
      balance: account ? n(account.balance) : null,
      maxTradeRiskPct: PAPER.maxCapitalRiskPct,
      portfolioRiskCapPct: PAPER.portfolioRiskCapPct,
      currentPortfolioRiskUsd: currentPortfolioRiskUsd(),
      portfolioRiskCapUsd: portfolioRiskCapUsd(),
      dailyPnl: n(account?.dailyPnl),
      weeklyPnl: n(account?.weeklyPnl),
      consecutiveLosses: n(account?.consecutiveLosses)
    },
    protection: {
      dailyLossLimitPct: PROTECTION.dailyLossLimitPct,
      weeklyLossLimitPct: PROTECTION.weeklyLossLimitPct,
      cooldownAfterLossMs: PROTECTION.cooldownAfterLossMs,
      allowedSessions: PROTECTION.allowedSessions,
      dailyLimitHit: isDailyLossLimitHit(),
      weeklyLimitHit: isWeeklyLossLimitHit(),
      cooldownActive: isCooldownActive(),
      reason: protectionReason()
    },
    usage: {
      twelveDay: account?.twelveUsageDay || null,
      twelveUsed: n(account?.twelveCreditsUsed),
      twelveScanSoftCap: TWELVE_SCAN_SOFT_CAP,
      twelveHardLocalCap: TWELVE_HARD_LOCAL_CAP,
      twelveBlockedUntil: state.twelveBlockedUntil ? new Date(state.twelveBlockedUntil) : null,
      geminiDay: account?.geminiUsageDay || null,
      geminiTotalUsed: n(account?.geminiCallsUsed),
      geminiTotalCap: GEMINI_TOTAL_DAILY_CAP,
      geminiEntryUsed: n(account?.geminiEntryUsed),
      geminiEntryCap: GEMINI_ENTRY_DAILY_CAP,
      geminiManageUsed: n(account?.geminiManageUsed),
      geminiManageCap: GEMINI_MANAGE_DAILY_CAP,
      geminiActiveModel: model.name,
      geminiModelIndex: state.geminiActiveModelIndex,
      geminiFallbackUsed: state.aiFallbackUsed,
      geminiBlockedUntil: state.geminiBlockedUntil ? new Date(state.geminiBlockedUntil) : null
    },
    trades: {
      open: state.openTrades.size,
      max: PAPER.maxOpenTrades,
      executed: state.executedSignals,
      skipped: state.skippedSignals
    },
    ai: {
      entryCalls: state.aiEntryCalls,
      manageCalls: state.aiManageCalls,
      buy: state.aiBuyDecisions,
      sell: state.aiSellDecisions,
      noTrade: state.aiNoTradeDecisions,
      close: state.aiCloseDecisions,
      hold: state.aiHoldDecisions,
      fallbackUsed: state.aiFallbackUsed,
      lastError: state.lastAiError
    },
    market: {
      initializedSymbols: state.pairState.size,
      totalActiveSymbols: ACTIVE_SYMBOLS.length,
      scannedBars: state.scannedBars,
      lastError: state.lastMarketError,
      nextScanAt: state.nextScanAt
    },
    news: {
      ready: state.newsReady,
      lastError: state.lastNewsError,
      eventsLoaded: economicNews.length
    },
    exits: {
      breakEvenAtR: RULES.breakEvenTriggerR,
      partialCloseAtR: RULES.partialTpTriggerR,
      partialClosePct: 50,
      remainingPct: 50,
      trailingStartStopR: RULES.trailingStartStopR,
      trailingStepR: RULES.trailingStepR,
      atrTrailStartR: RULES.atrTrailStartR,
      atrTrailMult: RULES.atrTrailMult
    },
    confluence: {
      threshold: RULES.confluenceThreshold
    }
  };
}

function startWebServer() {
  const app = express();
  app.use(express.json({ limit: '1mb' }));

  app.get('/', (req, res) => {
    res.json({ service: VERSION, mode: MODE, status: 'RUNNING' });
  });

  app.get('/health', (req, res) => {
    res.status(200).json({
      ok: true,
      version: VERSION,
      mode: MODE,
      mongoReady: state.mongoReady,
      marketReady: state.marketReady,
      geminiReady: state.geminiReady,
      telegramReady: state.telegramReady,
      telegramPollingReady: state.telegramPollingReady,
      newsReady: state.newsReady,
      openTrades: state.openTrades.size,
      geminiActiveModel: currentGeminiModel().name,
      protection: protectionReason()
    });
  });

  app.get('/api/status', (req, res) => res.json(buildStatus()));
  app.get('/api/trades', (req, res) => res.json([...state.openTrades.values()]));

  app.post('/api/trades/:symbol/close', async (req, res) => {
    const symbol = String(req.params.symbol || '').trim().toUpperCase();
    if (state.tradeLocks.has(symbol))
      return res.status(409).json({ ok: false, error: 'Trade is busy; retry shortly' });

    state.tradeLocks.add(symbol);
    try {
      const t = state.openTrades.get(symbol);
      if (!t) return res.status(404).json({ ok: false, error: 'No open trade for symbol' });
      const q = await fetchQuote(symbol);
      const price = exitExecutionPrice(t.direction, q);
      const closed = await closeTradeAtPrice(t, price, 'MANUAL_API_CLOSE');
      return res.json({ ok: true, trade: closed });
    } catch (e) {
      return res.status(500).json({ ok: false, error: safeError(e) });
    } finally {
      state.tradeLocks.delete(symbol);
    }
  });

  app.listen(PORT, '0.0.0.0', () => console.log(`[WEB] listening on ${PORT}`));
}

// ═══════════════════════════════════════════════════════════
// STARTUP / SHUTDOWN
// ═══════════════════════════════════════════════════════════
function validateStartupConfig() {
  if (MODE !== 'PAPER') throw new Error('MODE must remain PAPER');
  if (LIVE_TRADING !== false) throw new Error('LIVE_TRADING must remain false');
  if (PAPER.maxCapitalRiskPct > 1) throw new Error('Max capital risk cannot exceed 1%');
  if (PAPER.portfolioRiskCapPct > 4) throw new Error('Portfolio risk cap cannot exceed 4%');
  if (RULES.riskReward !== 2 || RULES.partialTpTriggerR !== 2 || RULES.breakEvenTriggerR !== 0.6)
    throw new Error('Frozen exit rules changed');
  if (!TWELVE_DATA_API_KEY) throw new Error('TWELVE_DATA_API_KEY is missing');
  if (!MONGODB_URI) throw new Error('MONGODB_URI is missing');
  if (!GEMINI_API_KEY)
    console.warn('[STARTUP] GEMINI_API_KEY missing. New entries will remain disabled.');
}

async function shutdown(signal) {
  console.log(`[SHUTDOWN] ${signal}`);
  if (telegramRetryTimer) clearTimeout(telegramRetryTimer);
  try { telegramBot?.stop(signal); } catch (_) {}
  try {
    if (mongoose.connection.readyState !== 0) await mongoose.disconnect();
  } catch (_) {}
  process.exit(0);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('unhandledRejection', e => console.error('[UNHANDLED REJECTION]', safeError(e)));
process.on('uncaughtException', e => console.error('[UNCAUGHT EXCEPTION]', safeError(e)));

// ═══════════════════════════════════════════════════════════
// BOOT
// ═══════════════════════════════════════════════════════════
async function boot() {
  console.log('========================================');
  console.log(VERSION);
  console.log(`MODE=${MODE}`);
  console.log(`LIVE_TRADING=${LIVE_TRADING}`);
  console.log(`ACTIVE_SYMBOLS=${ACTIVE_SYMBOLS.join(',')}`);
  console.log(`GEMINI_CHAIN=${GEMINI_MODELS.map(m => m.name).join(' → ')}`);
  console.log('========================================');

  validateStartupConfig();
  startWebServer();

  await initMongo();
  await restoreOpenTrades();
  await reconcilePnl();
  await initTelegram();
  await fetchEconomicNews();
  await initializeMarket();

  // فحص أولي لموديل Gemini (بدون استدعاء فعلي)
  state.geminiReady = !!GEMINI_API_KEY;

  startLoops();

  await journal('BOT_STARTED', {
    message: VERSION,
    mode: MODE,
    activeSymbols: ACTIVE_SYMBOLS,
    startingBalance: PAPER.startingBalance,
    maxTradeRiskPct: PAPER.maxCapitalRiskPct,
    portfolioRiskCapPct: PAPER.portfolioRiskCapPct,
    breakEvenR: RULES.breakEvenTriggerR,
    partialCloseR: RULES.partialTpTriggerR,
    partialClosePct: 50,
    remainingTrailingPct: 50,
    atrTrailStartR: RULES.atrTrailStartR,
    atrTrailMult: RULES.atrTrailMult,
    confluenceThreshold: RULES.confluenceThreshold,
    geminiChain: GEMINI_MODELS.map(m => m.name)
  });

  console.log('[BOOT] LOMY FOREX V2.0 READY');
}

if (require.main === module) {
  boot().catch(e => {
    console.error('[BOOT FATAL]', safeError(e));
    process.exit(1);
  });
}

module.exports = {
  // Bars
  normalizeBars, closed15Bars, mergeBars, aggregateBars,
  // Technical
  buildTechnicalIntelligence, buildMtf, localCandidate, computeConfluence,
  // Candle
  detectCandlePatterns, detectOrderBlocks, detectSupplyDemand,
  // Trade helpers
  tradePriceR, stopPriceAtR, stopFillFromBar, riskPctFromConfidence,
  // Time
  pacificDayKey, utcDayKey
};
