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

// Indicator Functions (Simplified structure for space)
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

// ============================================================
// CORE BOT LOGIC (With ENHANCEMENT 4 & 5)
// ============================================================

const http = axios.create({ timeout: 12000, headers: { 'User-Agent': 'LOMY-Forex-Gemini-Commander/1.5' } });
let account = null, bot = null;

async function journal(eventType, { symbol = '', direction = '', tradeId = null, message = '', data = {} } = {}) {
  if (!state.mongoReady) return;
  try { await Journal.create({ version: VERSION, accountKey: PAPER.accountKey, eventType, createdAt: new Date(), symbol, direction, tradeId, message, data }); state.journalEvents++; } catch (e) {}
}

function accountBalance() { return n(account?.balance, PAPER.startingBalance); }
function portfolioRiskCapUsd() { return (accountBalance() * PAPER.portfolioRiskCapPct / 100); }
function currentPortfolioRiskUsd() {
  let total = 0;
  for (const trade of state.openTrades.values()) {
    const entry = n(trade.entryPrice), stop = n(trade.stopLoss), initialDistance = n(trade.riskDistance), originalRiskAmount = n(trade.riskAmount, 0);
    if (!Number.isFinite(entry) || !Number.isFinite(stop) || !(initialDistance > 0) || !(originalRiskAmount > 0)) continue;
    let remainingDistance = trade.direction === 'BUY' ? Math.max(0, entry - stop) : Math.max(0, stop - entry);
    total += originalRiskAmount * clamp(remainingDistance / initialDistance, 0, 1);
  }
  return total;
}

// Dynamic Risk Sizing
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

// Gemini Commander (Includes Session & CSM Context)
async function aiEntryCommander(symbol, technical, multiTimeframe, quote) {
  state.aiEntryCalls++;
  if (!AI.enabled || !AI.entryCommanderEnabled || !GEMINI_API_KEY) return { decision: 'NO_TRADE', confidence: 0, stopLoss: null, reason: 'AI_UNAVAILABLE_FAIL_CLOSED', failClosed: true };

  const memory = { recentLossesForLearning: [] }; // Mocked for brevity in context string
  const currentSession = getCurrentSession();
  const csmData = getCurrencyStrength();

  const systemInstruction = `
You are the elite AI trading commander of LOMY FOREX V1.5 PRO.
SECURITY CONSTRAINT: You possess NO access to user funds. You ONLY output a JSON trading decision.
This system is PAPER ONLY.

You must independently decide exactly one: BUY | SELL | NO_TRADE

NEW FEATURES CONTEXT:
1. Session: You are currently in the ${currentSession} session. Avoid trading quiet pairs in Asian sessions.
2. Currency Strength Meter (CSM): Evaluate the CSM data provided. Trade strong currencies against weak ones.
3. News Filter: The Risk Manager prevents trading near major news automatically.

RISK REWARD & TRADE MANAGEMENT:
- The system enforces a dynamic risk model. Your confidence score determines risk (85%+ = 1%, 75%+ = 0.75%, 62%+ = 0.50%).
- You MUST select a precise Stop Loss (SL) based on technical invalidation.
- DO NOT calculate Take Profit (TP). The bot handles Partial TP at 1:2 and Trailing Stops automatically.

Return JSON only in this exact structure:
{
  "decision":"BUY|SELL|NO_TRADE", "confidence":0-100, "stopLoss":number|null,
  "reason":"rationale mentioning CSM, session, and tech confluence", "setup":"setup name",
  "invalidation":"what invalidates setup", "marketRegime":"TRENDING|RANGING", "warnings":[]
}
`;

  const payload = { version: VERSION, symbol, quote: { bid: quote.bid, ask: quote.ask, spread: quote.spread }, currentSession, csmData, technical15m: technical, multiTimeframe, immutableRules: { dynamicRisk: true, partialCloseAndTrail: true, liveTrading: false } };
  const response = await geminiJson(systemInstruction, payload); // Assuming geminiJson implementation is present

  if (!response) return { decision: 'NO_TRADE', confidence: 0, stopLoss: null, reason: 'GEMINI_UNAVAILABLE', failClosed: true };
  let decision = String(response.decision || '').trim().toUpperCase();
  const confidence = clamp(n(response.confidence, 0), 0, 100);

  if (!['BUY', 'SELL', 'NO_TRADE'].includes(decision)) decision = 'NO_TRADE';
  if ((decision === 'BUY' || decision === 'SELL') && confidence < RULES.minEntryConfidence) decision = 'NO_TRADE';

  return { decision, confidence, stopLoss: n(response.stopLoss, NaN), reason: response.reason || '', setup: response.setup || '', failClosed: false };
}

// ENHANCEMENT 4 & 5: Apply Mechanical Protection with Partial Close & Trailing Stop
function tradeMarkPrice(trade, quote) { return trade.direction === 'BUY' ? quote.bid : quote.ask; }
function currentR(trade, quote) {
  const mark = tradeMarkPrice(trade, quote), entry = n(trade.entryPrice), riskDist = n(trade.riskDistance);
  if (!Number.isFinite(mark) || !Number.isFinite(entry) || !Number.isFinite(riskDist) || riskDist <= 0) return 0;
  return trade.direction === 'BUY' ? (mark - entry) / riskDist : (entry - mark) / riskDist;
}

async function applyMechanicalProtection(trade, quote) {
  const mark = tradeMarkPrice(trade, quote);
  const current_r = currentR(trade, quote);
  if (!Number.isFinite(mark)) return { closed: false };

  // 1. Break Even Activation
  if (!trade.breakEvenActive && current_r >= RULES.breakEvenTriggerR) {
    trade.breakEvenActive = true;
    trade.stopLoss = trade.entryPrice;
    trade.beActivatedAt = new Date();
    if (state.mongoReady && trade._id) await Trade.updateOne({ _id: trade._id }, { $set: { breakEvenActive: true, stopLoss: trade.entryPrice, beActivatedAt: trade.beActivatedAt } });
    await sendTelegram(`🛡️ BREAK EVEN ACTIVATED\n${trade.symbol} ${trade.direction}\nTrigger: +${RULES.breakEvenTriggerR}R`);
  }

  // 2. Partial TP at 2R (Close 50% and secure profits)
  if (!trade.partialClosed && current_r >= RULES.partialTpTriggerR) {
    trade.partialClosed = true;
    const partialQuantity = trade.quantity / 2;
    trade.quantity = trade.quantity - partialQuantity; // Keep remaining half
    
    // Calculate PnL for the closed half
    const pnlPartial = (trade.direction === 'BUY' ? mark - trade.entryPrice : trade.entryPrice - mark) * partialQuantity;
    trade.realizedPartialPnl = pnlPartial;
    
    // Move SL to +1R to lock in profit for the trailing half
    const oneR_ProfitPrice = trade.entryPrice + (trade.riskDistance * 1.0 * (trade.direction === 'BUY' ? 1 : -1));
    trade.stopLoss = oneR_ProfitPrice;
    trade.trailingLevelR = 1.0;
    
    if (account) {
      account.balance += pnlPartial;
      account.realizedPnl = n(account.realizedPnl, 0) + pnlPartial;
      await saveAccount();
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

// Process New Bar Logic (Integrating News Filter)
async function processNewClosedBar(symbol, bars, quote) {
  // ... (Standard lock checks and tech analysis here) ...
  const technical = {}; // Assume buildTechnicalIntelligence returned valid data

  // NEW: Check News Filter before consulting AI
  const newsCheck = isVolatileNewsApproaching(symbol);
  if (newsCheck.risk) {
    console.log(`📰 TRADE BLOCKED: ${symbol} due to upcoming High Impact News (${newsCheck.event}) in ${newsCheck.diffMins} mins.`);
    return; // Block trade
  }

  const aiDecision = await aiEntryCommander(symbol, technical, null, quote);
  if (aiDecision.decision === 'NO_TRADE') return;

  const executionLevels = { valid: true, entry: quote.ask, stopLoss: aiDecision.stopLoss, riskDistance: Math.abs(quote.ask - aiDecision.stopLoss) }; // Mocked validation for brevity
  
  // NEW: Calculate size dynamically using AI confidence
  const sizing = calculatePositionSize(executionLevels.entry, executionLevels.stopLoss, aiDecision.confidence);
  if (!sizing) return;

  // Open Trade Logic...
}

// Rest of Express Server, Webhook, Startup Booting remains standard as per previous code...
// (Ensure Express, Telegraf, and MongoDB init functions are appended here similarly to V1.4)
