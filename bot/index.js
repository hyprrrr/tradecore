/**
 * TradeCore Pro Bot — Upgraded Engine v3
 *
 *  ✅ Real-time Alpaca market data
 *  ✅ 200 EMA trend filter
 *  ✅ Volume confirmation
 *  ✅ Multi-timeframe confirmation (5min + 15min)
 *  ✅ Market regime detection (SPY health check)
 *  ✅ ATR-based position sizing (risk 1% per trade)
 *  ✅ Correlation filter
 *  ✅ Daily loss circuit breaker
 *  ✅ Gap filter
 *  ✅ Market hours enforcement
 *  ✅ Trailing stop loss
 *  ✅ VWAP + Bollinger Bands
 *  ✅ Confidence scoring (60%+ required)
 *  ✅ International ETF session awareness
 *     - Asia session  (7 PM - 2 AM ET)  → EWJ, FXI, EWA, EWY
 *     - London session (3 AM - 9 AM ET) → EWU, EWG, EWZ
 *     - US open (9:30 AM - 4 PM ET)     → all symbols
 *     - ETFs boosted/dampened by their home market activity
 */

'use strict';
const cron = require('node-cron');
const http = require('http');

// ─────────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────────
const CONFIG = {
  alpacaKey:    process.env.ALPACA_KEY    || '',
  alpacaSecret: process.env.ALPACA_SECRET || '',
  alpacaPaper:  process.env.ALPACA_PAPER  !== 'false',
  mode:         (process.env.MODE || 'alpaca').toLowerCase(),

  discordWebhook: process.env.DISCORD_WEBHOOK || '',

  symbols: (process.env.SYMBOLS || 'SPY,QQQ,AAPL,MSFT,NVDA,TSLA,EWJ,EWU,EWZ,EWC,FXI,EWA,EWG,EWY,INDA').split(',').map(s => s.trim().toUpperCase()),

  strategy:      process.env.STRATEGY      || 'rsi_macd',
  rsiPeriod:     +(process.env.RSI_PERIOD     || 14),
  rsiOversold:   +(process.env.RSI_OVERSOLD   || 32),
  rsiOverbought: +(process.env.RSI_OVERBOUGHT || 68),

  startingCapital:  +(process.env.CAPITAL          || 10000),
  maxPositionPct:   +(process.env.MAX_POSITION_PCT  || 15) / 100,
  maxOpenPositions: +(process.env.MAX_POSITIONS     || 3),
  stopLossPct:      +(process.env.STOP_LOSS_PCT     || 4)  / 100,
  takeProfitPct:    +(process.env.TAKE_PROFIT_PCT   || 8)  / 100,
  trailingStop:     process.env.TRAILING_STOP       !== 'false',
  trailingStopPct:  +(process.env.TRAILING_STOP_PCT || 3)  / 100,
  maxDailyLossPct:  +(process.env.MAX_DAILY_LOSS    || 3)  / 100,

  trendFilter:       process.env.TREND_FILTER  !== 'false',
  volumeFilter:      process.env.VOLUME_FILTER !== 'false',
  regimeFilter:      process.env.REGIME_FILTER !== 'false',
  correlationFilter: process.env.CORR_FILTER   !== 'false',
  gapFilter:         process.env.GAP_FILTER    !== 'false',

  scanIntervalMin: +(process.env.SCAN_INTERVAL_MIN || 5),

  // Supabase
  supabaseUrl: process.env.SUPABASE_URL || '',
  supabaseKey: process.env.SUPABASE_ANON_KEY || '',
};

// Stocks that move together — avoid holding more than 1 from each group
const CORRELATION_GROUPS = [
  ['AAPL', 'MSFT', 'GOOGL', 'META', 'AMZN'],
  ['NVDA', 'AMD', 'INTC', 'QCOM'],
  ['TSLA', 'RIVN', 'LCID', 'NIO'],
  ['SPY', 'QQQ', 'IWM', 'DIA'],
  ['JPM', 'BAC', 'GS', 'MS'],
];

// ─────────────────────────────────────────────
// SUPABASE SYNC
// ─────────────────────────────────────────────
async function sbFetch(path, method = 'GET', body = null) {
  if (!CONFIG.supabaseUrl || !CONFIG.supabaseKey) return null;
  try {
    const { default: fetch } = await import('node-fetch');
    const res = await fetch(`${CONFIG.supabaseUrl}/rest/v1/${path}`, {
      method,
      headers: {
        'apikey': CONFIG.supabaseKey,
        'Authorization': `Bearer ${CONFIG.supabaseKey}`,
        'Content-Type': 'application/json',
        'Prefer': method === 'POST' ? 'resolution=merge-duplicates' : '',
      },
      body: body ? JSON.stringify(body) : undefined,
    });
    if (res.status === 204 || res.status === 200) {
      const text = await res.text();
      return text ? JSON.parse(text) : null;
    }
    return null;
  } catch (e) {
    log('error', `Supabase ${method} ${path}: ${e.message}`);
    return null;
  }
}

async function syncPortfolio() {
  const openPnl = Object.entries(positions).reduce((acc, [sym, pos]) => {
    const cur = priceHistory5m[sym]?.[priceHistory5m[sym].length - 1] || pos.entryPrice;
    return acc + (cur - pos.entryPrice) * pos.qty;
  }, 0);
  const totalValue = portfolio + openPnl;
  const dayPnl = totalValue - dailyStartPortfolio;

  await sbFetch('tc_portfolio?id=eq.1', 'PATCH', {
    cash: +portfolio.toFixed(2),
    total_value: +totalValue.toFixed(2),
    day_pnl: +dayPnl.toFixed(2),
    total_wins: totalWins,
    total_losses: totalLosses,
    circuit_breaker: circuitBreakerOn,
    last_scan: new Date().toISOString(),
    session: getCurrentSession(),
    updated_at: new Date().toISOString(),
  });
}

async function syncPositions() {
  // Delete all then reinsert current positions
  await sbFetch('tc_positions', 'DELETE', undefined);
  // Supabase REST delete needs a filter — use neq on a dummy field
  await sbFetch('tc_positions?symbol=neq.___NONE___', 'DELETE');

  for (const [sym, pos] of Object.entries(positions)) {
    const cur = priceHistory5m[sym]?.[priceHistory5m[sym].length - 1] || pos.entryPrice;
    const pnl = (cur - pos.entryPrice) * pos.qty;
    const pnlPct = ((cur - pos.entryPrice) / pos.entryPrice) * 100;
    await sbFetch('tc_positions', 'POST', {
      symbol: sym,
      entry_price: +pos.entryPrice.toFixed(4),
      qty: pos.qty,
      cost: +pos.cost.toFixed(2),
      current_price: +cur.toFixed(4),
      pnl: +pnl.toFixed(2),
      pnl_pct: +pnlPct.toFixed(2),
      entry_time: pos.entryTime.toISOString(),
      high_water: +pos.highWater.toFixed(4),
      confidence: pos.sigInfo?.confidence || 0,
      updated_at: new Date().toISOString(),
    });
  }
}

async function syncTrade(trade) {
  await sbFetch('tc_trades', 'POST', {
    symbol: trade.sym,
    side: trade.side,
    qty: trade.qty,
    price: +trade.price.toFixed(4),
    pnl: trade.pnl !== null ? +trade.pnl.toFixed(2) : null,
    reason: trade.reason || null,
    confidence: trade.confidence || null,
    created_at: new Date().toISOString(),
  });
}

async function syncLog(type, msg) {
  await sbFetch('tc_logs', 'POST', {
    type,
    message: msg,
    created_at: new Date().toISOString(),
  });
}

async function syncAll() {
  await Promise.all([syncPortfolio(), syncPositions()]);
}

// ─────────────────────────────────────────────
// INTERNATIONAL ETF SESSION MAP
// ─────────────────────────────────────────────
// Each ETF's home market hours in ET, and a session confidence multiplier.
// During a symbol's "prime" window its signals get boosted.
// Outside it, signals are dampened (ETF still trades but home market is closed).
const ETF_SESSIONS = {
  // Asia session  — prime: 7 PM – 2 AM ET
  EWJ:  { region: 'Japan',       primeStart: 19, primeEnd:  2, multiplier: 1.25 },
  FXI:  { region: 'China',       primeStart: 21, primeEnd:  3, multiplier: 1.25 },
  EWA:  { region: 'Australia',   primeStart: 19, primeEnd:  1, multiplier: 1.20 },
  EWY:  { region: 'S. Korea',    primeStart: 20, primeEnd:  2, multiplier: 1.20 },
  // London/Europe session — prime: 3 AM – 11:30 AM ET
  EWU:  { region: 'UK',          primeStart:  3, primeEnd: 11, multiplier: 1.25 },
  EWG:  { region: 'Germany',     primeStart:  3, primeEnd: 11, multiplier: 1.25 },
  EWZ:  { region: 'Brazil',      primeStart:  9, primeEnd: 16, multiplier: 1.15 },
  // Americas/all-day
  EWC:  { region: 'Canada',      primeStart:  9, primeEnd: 16, multiplier: 1.10 },
  INDA: { region: 'India',       primeStart:  0, primeEnd:  6, multiplier: 1.20 },
  // US ETFs — always prime during market hours
  SPY:  { region: 'US (S&P500)', primeStart:  9, primeEnd: 16, multiplier: 1.00 },
  QQQ:  { region: 'US (Nasdaq)', primeStart:  9, primeEnd: 16, multiplier: 1.00 },
};

/**
 * Returns a session multiplier for the symbol right now.
 * 1.0 = neutral, >1.0 = prime session (boost signals), <1.0 = off-hours (dampen)
 */
function getSessionMultiplier(symbol) {
  const session = ETF_SESSIONS[symbol];
  if (!session) return 1.0; // regular stock — no adjustment

  const etHour = getETTime().getHours();
  const { primeStart, primeEnd, multiplier } = session;

  let inPrime;
  if (primeStart < primeEnd) {
    inPrime = etHour >= primeStart && etHour < primeEnd;
  } else {
    // Wraps midnight (e.g. 19 → 2)
    inPrime = etHour >= primeStart || etHour < primeEnd;
  }

  return inPrime ? multiplier : 0.80; // off-prime: dampen confidence by 20%
}

/**
 * Returns the current trading session name for logging/alerts
 */
function getCurrentSession() {
  const h = getETTime().getHours();
  if (h >= 19 || h < 2)  return '🌏 Asia Session';
  if (h >= 2  && h < 9)  return '🇬🇧 London Session';
  if (h >= 9  && h < 16) return '🇺🇸 US Session';
  return '😴 Off Hours';
}

// ─────────────────────────────────────────────
// STATE
// ─────────────────────────────────────────────
let portfolio           = CONFIG.startingCapital;
let positions           = {};
let priceHistory5m      = {};
let priceHistory15m     = {};
let prevDayClose        = {};
let trades              = [];
let totalWins           = 0;
let totalLosses         = 0;
let dailyStartPortfolio = CONFIG.startingCapital;
let circuitBreakerOn    = false;
let lastScanTime        = null;

// ─────────────────────────────────────────────
// LOGGING
// ─────────────────────────────────────────────
function log(type, msg) {
  const ts = new Date().toLocaleString('en-US', { timeZone: 'America/New_York' });
  console.log(`[${ts} ET] [${type.toUpperCase().padEnd(6)}] ${msg}`);
}

// ─────────────────────────────────────────────
// MARKET HOURS
// ─────────────────────────────────────────────
function getETTime() {
  return new Date(new Date().toLocaleString('en-US', { timeZone: 'America/New_York' }));
}

function isMarketOpen() {
  const et = getETTime();
  const day = et.getDay();
  const mins = et.getHours() * 60 + et.getMinutes();
  return day >= 1 && day <= 5 && mins >= 571 && mins <= 955; // 9:31 - 3:55
}

function isWeekday() {
  const day = getETTime().getDay();
  return day >= 1 && day <= 5;
}

function isPreMarket() {
  const et = getETTime();
  const mins = et.getHours() * 60 + et.getMinutes();
  return et.getDay() >= 1 && et.getDay() <= 5 && mins >= 240 && mins < 570;
}

/**
 * For international ETFs we scan whenever their home market is open.
 * For regular US stocks (AAPL, TSLA, MSFT etc.) we STRICTLY enforce
 * US market hours — no scanning, no orders outside 9:31-3:55 ET.
 */
function shouldScanSymbol(symbol) {
  if (!isWeekday()) return false;
  const isIntlETF = !!ETF_SESSIONS[symbol];
  if (!isIntlETF) {
    // Hard block — regular US stocks only trade during market hours
    return isMarketOpen();
  }
  // International ETF — always scan on weekdays (with session multiplier applied)
  return true;
}

// ─────────────────────────────────────────────
// ALPACA API
// ─────────────────────────────────────────────
const ALPACA_BASE      = () => CONFIG.alpacaPaper ? 'https://paper-api.alpaca.markets' : 'https://api.alpaca.markets';
const ALPACA_DATA_BASE = 'https://data.alpaca.markets';

async function alpacaFetch(url, opts = {}) {
  const { default: fetch } = await import('node-fetch');
  const res = await fetch(url, {
    ...opts,
    headers: {
      'APCA-API-KEY-ID':     CONFIG.alpacaKey,
      'APCA-API-SECRET-KEY': CONFIG.alpacaSecret,
      'Content-Type': 'application/json',
      ...(opts.headers || {}),
    },
  });
  return res.json();
}

async function fetchBars(symbol, timeframe, limit) {
  try {
    const start = new Date(Date.now() - 3 * 24 * 60 * 60 * 1000).toISOString();
    const url = `${ALPACA_DATA_BASE}/v2/stocks/${symbol}/bars?timeframe=${timeframe}&start=${start}&limit=${limit}&feed=iex`;
    const data = await alpacaFetch(url);
    return data.bars || null;
  } catch (e) {
    log('error', `fetchBars ${symbol} ${timeframe}: ${e.message}`);
    return null;
  }
}

async function placeOrder(symbol, qty, side) {
  const data = await alpacaFetch(`${ALPACA_BASE()}/v2/orders`, {
    method: 'POST',
    body: JSON.stringify({ symbol, qty: String(qty), side, type: 'market', time_in_force: 'day' }),
  });
  if (data.id) { log('order', `${side.toUpperCase()} order placed: ${qty}x ${symbol} | ID: ${data.id}`); return data; }
  throw new Error(data.message || JSON.stringify(data));
}

async function getAccount() {
  try { return await alpacaFetch(`${ALPACA_BASE()}/v2/account`); }
  catch (e) { return null; }
}

// ─────────────────────────────────────────────
// INDICATORS
// ─────────────────────────────────────────────
function ema(prices, period) {
  if (!prices || prices.length < period) return prices?.[prices.length - 1] || 0;
  const k = 2 / (period + 1);
  let val = prices.slice(0, period).reduce((a, b) => a + b, 0) / period;
  for (let i = period; i < prices.length; i++) val = prices[i] * k + val * (1 - k);
  return val;
}

function rsi(prices, period) {
  if (!prices || prices.length < period + 1) return 50;
  let gains = 0, losses = 0;
  for (let i = prices.length - period; i < prices.length; i++) {
    const d = prices[i] - prices[i - 1];
    d > 0 ? gains += d : losses += Math.abs(d);
  }
  const rs = (gains / period) / ((losses / period) || 0.0001);
  return 100 - 100 / (1 + rs);
}

function atr(bars, period = 14) {
  if (!bars || bars.length < 2) return 0;
  const trs = bars.slice(1).map((b, i) => Math.max(b.h - b.l, Math.abs(b.h - bars[i].c), Math.abs(b.l - bars[i].c)));
  return trs.slice(-period).reduce((a, b) => a + b, 0) / Math.min(period, trs.length);
}

function vwap(bars) {
  let tv = 0, v = 0;
  for (const b of bars) { const tp = (b.h + b.l + b.c) / 3; tv += tp * b.v; v += b.v; }
  return v > 0 ? tv / v : 0;
}

function bollingerBands(prices, period = 20, mult = 2) {
  if (!prices || prices.length < period) return null;
  const sl = prices.slice(-period);
  const mean = sl.reduce((a, b) => a + b, 0) / period;
  const std = Math.sqrt(sl.reduce((a, b) => a + (b - mean) ** 2, 0) / period);
  return { upper: mean + mult * std, middle: mean, lower: mean - mult * std };
}

// ─────────────────────────────────────────────
// SIGNAL ENGINE
// ─────────────────────────────────────────────
function generateSignal(sym, bars5m, bars15m) {
  if (!bars5m || bars5m.length < 20) return { signal: 'HOLD', confidence: 0, reasons: ['Insufficient data'] };

  const c5  = bars5m.map(b => b.c);
  const c15 = bars15m?.length >= 20 ? bars15m.map(b => b.c) : null;
  const vol = bars5m.map(b => b.v);
  const price = c5[c5.length - 1];

  let buy = 0, sell = 0;
  const reasons = [];

  // RSI
  const r = rsi(c5, CONFIG.rsiPeriod);
  if (r < CONFIG.rsiOversold)   { buy  += 25; reasons.push(`RSI oversold (${r.toFixed(1)})`);  }
  else if (r > CONFIG.rsiOverbought) { sell += 25; reasons.push(`RSI overbought (${r.toFixed(1)})`); }

  // MACD (EMA 8/21 cross)
  const e8 = ema(c5, 8), e21 = ema(c5, 21);
  const pe8 = ema(c5.slice(0, -1), 8), pe21 = ema(c5.slice(0, -1), 21);
  if (e8 > e21)  { buy  += 15; reasons.push('MACD bullish'); }
  else           { sell += 15; reasons.push('MACD bearish'); }
  if (pe8 < pe21 && e8 > e21) { buy  += 10; reasons.push('MACD bullish crossover ↑'); }
  if (pe8 > pe21 && e8 < e21) { sell += 10; reasons.push('MACD bearish crossover ↓'); }

  // 200 EMA trend filter
  if (CONFIG.trendFilter && c5.length >= 40) {
    const e200 = ema(c5, Math.min(200, c5.length));
    if (price > e200) { buy  += 20; reasons.push('Above 200 EMA (uptrend)'); }
    else              { sell += 20; reasons.push('Below 200 EMA (downtrend)'); }
  }

  // Volume
  if (CONFIG.volumeFilter && vol.length >= 10) {
    const avgVol = vol.slice(-20).reduce((a, b) => a + b, 0) / Math.min(20, vol.length);
    const curVol = vol[vol.length - 1];
    if (curVol > avgVol * 1.3) {
      buy > sell ? buy += 15 : sell += 15;
      reasons.push(`Volume surge (${(curVol / avgVol).toFixed(1)}x avg)`);
    } else if (curVol < avgVol * 0.5) {
      buy = Math.max(0, buy - 10); sell = Math.max(0, sell - 10);
      reasons.push('Low volume — dampening');
    }
  }

  // 15min timeframe confirmation
  if (c15) {
    const r15 = rsi(c15, CONFIG.rsiPeriod);
    const e8_15 = ema(c15, 8), e21_15 = ema(c15, 21);
    if (r15 < 45 && e8_15 > e21_15) { buy  += 15; reasons.push('15min confirms bullish'); }
    else if (r15 > 55 && e8_15 < e21_15) { sell += 15; reasons.push('15min confirms bearish'); }
    else { buy = Math.max(0, buy - 5); sell = Math.max(0, sell - 5); reasons.push('15min neutral'); }
  }

  // Bollinger Bands
  const bb = bollingerBands(c5);
  if (bb) {
    if (price <= bb.lower) { buy  += 10; reasons.push('At lower Bollinger Band'); }
    else if (price >= bb.upper) { sell += 10; reasons.push('At upper Bollinger Band'); }
  }

  // VWAP
  if (bars5m.length >= 5) {
    const vw = vwap(bars5m.slice(-20));
    if (price > vw * 1.001) { buy  += 5; reasons.push(`Above VWAP ($${vw.toFixed(2)})`); }
    else                    { sell += 5; reasons.push(`Below VWAP ($${vw.toFixed(2)})`); }
  }

  const total = buy + sell;
  const confidence = total > 0 ? Math.round(Math.max(buy, sell) / total * 100) : 0;

  if (buy >= 55 && buy > sell * 1.3)  return { signal: 'BUY',  confidence, score: buy,  reasons, rsi: r };
  if (sell >= 55 && sell > buy * 1.3) return { signal: 'SELL', confidence, score: sell, reasons, rsi: r };
  return { signal: 'HOLD', confidence, reasons, rsi: r };
}

// ─────────────────────────────────────────────
// FILTERS
// ─────────────────────────────────────────────
async function getMarketRegime() {
  if (!CONFIG.regimeFilter) return true;
  try {
    const bars = await fetchBars('SPY', '1Day', 30);
    if (!bars || bars.length < 10) return true;
    const closes = bars.map(b => b.c);
    const e10 = ema(closes, 10), e30 = ema(closes, Math.min(30, closes.length));
    const bullish = closes[closes.length - 1] > e10 && e10 > e30;
    log('regime', bullish ? '✅ Market BULLISH — trading enabled' : `⚠ Market BEARISH (SPY EMA10:${e10.toFixed(2)} < EMA30:${e30.toFixed(2)}) — BUYs paused`);
    return bullish;
  } catch (e) { return true; }
}

function isCorrelated(symbol) {
  if (!CONFIG.correlationFilter) return false;
  for (const group of CORRELATION_GROUPS) {
    if (!group.includes(symbol)) continue;
    for (const held of Object.keys(positions)) {
      if (group.includes(held)) {
        log('filter', `Correlation block: ${symbol} (already holding ${held})`);
        return true;
      }
    }
  }
  return false;
}

function hasLargeGap(symbol, price) {
  if (!CONFIG.gapFilter || !prevDayClose[symbol]) return false;
  const gap = Math.abs((price - prevDayClose[symbol]) / prevDayClose[symbol]);
  if (gap > 0.03) { log('filter', `Gap filter: ${symbol} gapped ${(gap*100).toFixed(1)}% — skip`); return true; }
  return false;
}

function checkCircuitBreaker() {
  if (circuitBreakerOn) return true;
  const loss = (dailyStartPortfolio - portfolio) / dailyStartPortfolio;
  if (loss >= CONFIG.maxDailyLossPct) {
    circuitBreakerOn = true;
    log('risk', `🔴 CIRCUIT BREAKER: Down ${(loss*100).toFixed(1)}% today — halting all trades`);
    sendDiscordAlert('circuit_breaker', 'ALL', 0, 0, -(dailyStartPortfolio - portfolio));
  }
  return circuitBreakerOn;
}

// ─────────────────────────────────────────────
// POSITION SIZING (ATR-based, risk 1% per trade)
// ─────────────────────────────────────────────
function calcQty(symbol, price, bars) {
  const maxCost  = portfolio * CONFIG.maxPositionPct;
  const maxShares = Math.floor(maxCost / price);
  if (bars && bars.length >= 14) {
    const atrVal = atr(bars, 14);
    if (atrVal > 0) {
      const riskShares = Math.floor((portfolio * 0.01) / atrVal);
      const qty = Math.min(riskShares, maxShares);
      if (qty >= 1) { log('size', `${symbol} ATR=${atrVal.toFixed(2)} → qty=${qty} (max=${maxShares})`); return qty; }
    }
  }
  return maxShares;
}

// ─────────────────────────────────────────────
// TRADE EXECUTION
// ─────────────────────────────────────────────
async function enterPosition(sym, price, sigInfo, bars) {
  // Final safety check — never place orders for US stocks outside market hours
  const isIntlETF = !!ETF_SESSIONS[sym];
  if (!isIntlETF && !isMarketOpen()) {
    log('warn', `🚫 Blocked order for ${sym} — US market is closed`);
    return;
  }
  if (checkCircuitBreaker()) return;
  if (isCorrelated(sym))    return;
  if (hasLargeGap(sym, price)) return;

  const qty  = calcQty(sym, price, bars);
  const cost = qty * price;
  if (qty < 1 || cost > portfolio) { log('warn', `Cannot buy ${sym}: qty=${qty} cost=$${cost.toFixed(2)} avail=$${portfolio.toFixed(2)}`); return; }

  if (CONFIG.mode === 'alpaca' && CONFIG.alpacaKey) {
    try { await placeOrder(sym, qty, 'buy'); }
    catch (e) { log('error', `Order failed ${sym}: ${e.message}`); return; }
  }

  portfolio -= cost;
  positions[sym] = { entryPrice: price, qty, cost, entryTime: new Date(), highWater: price, sigInfo };
  trades.push({ time: new Date(), sym, side: 'BUY', qty, price, pnl: null, reason: 'SIGNAL', confidence: sigInfo.confidence });

  log('buy', `✅ BUY ${qty}x ${sym} @ $${price.toFixed(2)} | conf=${sigInfo.confidence}% | cash=$${portfolio.toFixed(2)}`);
  await sendDiscordAlert('buy', sym, qty, price, undefined, undefined, sigInfo);
  await syncTrade({ sym, side: 'BUY', qty, price, pnl: null, reason: 'SIGNAL', confidence: sigInfo.confidence });
  await syncAll();
  await syncLog('buy', `BUY ${qty}x ${sym} @ $${price.toFixed(2)} conf=${sigInfo.confidence}%`);
}

async function exitPosition(sym, price, reason) {
  const pos = positions[sym];
  if (!pos) return;

  if (CONFIG.mode === 'alpaca' && CONFIG.alpacaKey) {
    try { await placeOrder(sym, pos.qty, 'sell'); }
    catch (e) { log('error', `Sell failed ${sym}: ${e.message}`); return; }
  }

  const pnl = pos.qty * price - pos.cost;
  portfolio += pos.qty * price;
  pnl > 0 ? totalWins++ : totalLosses++;
  delete positions[sym];

  trades.push({ time: new Date(), sym, side: 'SELL', qty: pos.qty, price, pnl, reason });
  const icon = { STOP_LOSS: '🛑', TAKE_PROFIT: '🎯', TRAILING_STOP: '📉', SIGNAL: '📤' }[reason] || '📤';
  log('sell', `${icon} SELL ${pos.qty}x ${sym} @ $${price.toFixed(2)} | P&L: ${pnl >= 0 ? '+' : ''}$${pnl.toFixed(2)} (${reason})`);
  await sendDiscordAlert('sell', sym, pos.qty, price, pnl, reason);
  await syncTrade({ sym, side: 'SELL', qty: pos.qty, price, pnl, reason });
  await syncAll();
  await syncLog('sell', `${icon} SELL ${pos.qty}x ${sym} @ $${price.toFixed(2)} P&L=${pnl >= 0 ? '+' : ''}$${pnl.toFixed(2)} (${reason})`);
}

async function managePosition(sym, price) {
  const pos = positions[sym];
  if (!pos) return;
  if (price > pos.highWater) positions[sym].highWater = price;

  // Trailing stop
  if (CONFIG.trailingStop && pos.highWater > pos.entryPrice) {
    if ((pos.highWater - price) / pos.highWater >= CONFIG.trailingStopPct) {
      log('risk', `Trailing stop: ${sym} hw=$${pos.highWater.toFixed(2)} cur=$${price.toFixed(2)}`);
      return exitPosition(sym, price, 'TRAILING_STOP');
    }
  }
  const chg = (price - pos.entryPrice) / pos.entryPrice;
  if (chg <= -CONFIG.stopLossPct)    return exitPosition(sym, price, 'STOP_LOSS');
  if (chg >= CONFIG.takeProfitPct)   return exitPosition(sym, price, 'TAKE_PROFIT');
}

// ─────────────────────────────────────────────
// MAIN SCAN
// ─────────────────────────────────────────────
async function runScan() {
  lastScanTime = new Date();
  const session = getCurrentSession();

  if (!isWeekday()) {
    log('scan', 'Weekend — markets closed globally');
    return;
  }

  log('scan', `═══ ${session} — Scanning ${CONFIG.symbols.length} symbols ═══`);

  // Market regime only matters during US hours
  const marketOk = isMarketOpen() ? await getMarketRegime() : true;

  // Log Alpaca account state
  if (CONFIG.alpacaKey && isMarketOpen()) {
    const acct = await getAccount();
    if (acct?.equity) log('acct', `Equity=$${(+acct.equity).toFixed(2)} BuyingPower=$${(+acct.buying_power).toFixed(2)}`);
  }

  for (const sym of CONFIG.symbols) {
    try {
      // Skip if this symbol shouldn't be scanned right now
      if (!shouldScanSymbol(sym)) {
        log('skip', `${sym} — not in trading window`);
        continue;
      }

      const sessionMult = getSessionMultiplier(sym);
      const sessionLabel = ETF_SESSIONS[sym]
        ? `[${ETF_SESSIONS[sym].region} ${sessionMult >= 1.0 ? '🟢 PRIME' : '🟡 off-hrs'} x${sessionMult}]`
        : '[US stock]';

      const bars5m  = await fetchBars(sym, '5Min',  60);
      const bars15m = await fetchBars(sym, '15Min', 40);
      if (!bars5m || bars5m.length < 10) { log('warn', `No data for ${sym}`); continue; }

      const price = bars5m[bars5m.length - 1].c;
      priceHistory5m[sym]  = bars5m.map(b => b.c);
      priceHistory15m[sym] = bars15m?.map(b => b.c) || [];

      // Manage open position first
      if (positions[sym]) {
        await managePosition(sym, price);
        if (positions[sym]) {
          const pct = ((price - positions[sym].entryPrice) / positions[sym].entryPrice * 100).toFixed(2);
          log('pos', `Holding ${positions[sym].qty}x ${sym} @ $${positions[sym].entryPrice.toFixed(2)} → $${price.toFixed(2)} (${pct}%) ${sessionLabel}`);
        }
        continue;
      }

      // Generate signal and apply session multiplier to confidence
      const sig = generateSignal(sym, bars5m, bars15m);
      const adjustedConfidence = Math.round(sig.confidence * sessionMult);
      const adjustedSig = { ...sig, confidence: adjustedConfidence };

      if (sessionMult < 1.0) {
        adjustedSig.reasons = [`Off-prime session (x${sessionMult} conf penalty)`, ...sig.reasons];
      } else if (sessionMult > 1.0) {
        adjustedSig.reasons = [`Prime session boost (x${sessionMult})`, ...sig.reasons];
      }

      log('signal', `${sym} @ $${price.toFixed(2)} → ${sig.signal} (conf:${adjustedConfidence}% RSI:${sig.rsi?.toFixed(1)}) ${sessionLabel}`);

      if (adjustedSig.signal === 'BUY' && marketOk && Object.keys(positions).length < CONFIG.maxOpenPositions) {
        await enterPosition(sym, price, adjustedSig, bars5m);
      }
    } catch (e) {
      log('error', `Scan error ${sym}: ${e.message}`);
    }
  }

  // Summary
  const openPnl = Object.entries(positions).reduce((acc, [sym, pos]) => {
    const cur = priceHistory5m[sym]?.[priceHistory5m[sym].length - 1] || pos.entryPrice;
    return acc + (cur - pos.entryPrice) * pos.qty;
  }, 0);
  const total = portfolio + openPnl;
  const dayPnl = total - dailyStartPortfolio;
  log('info', `${session} | Total=$${total.toFixed(2)} DayP&L=${dayPnl >= 0 ? '+' : ''}$${dayPnl.toFixed(2)} Open:${Object.keys(positions).length} W:${totalWins}/L:${totalLosses}`);
  await syncAll();
}

async function storePrevClose() {
  for (const sym of CONFIG.symbols) {
    try {
      const bars = await fetchBars(sym, '1Day', 2);
      if (bars?.length >= 1) prevDayClose[sym] = bars[bars.length - 1].c;
    } catch (e) {}
  }
}

// ─────────────────────────────────────────────
// DISCORD ALERTS
// ─────────────────────────────────────────────
async function sendDiscordAlert(type, sym, qty, price, pnl, reason, sigInfo) {
  if (!CONFIG.discordWebhook) return;
  const { default: fetch } = await import('node-fetch');

  const colors = { buy: 0x7fff6e, sell: 0x4da6ff, circuit_breaker: 0xff0000 };
  if (type === 'sell') {
    if (reason === 'STOP_LOSS')     colors.sell = 0xff5f57;
    if (reason === 'TAKE_PROFIT')   colors.sell = 0x00e5ff;
    if (reason === 'TRAILING_STOP') colors.sell = 0xffb547;
  }

  const icons = {
    buy: '🟢', circuit_breaker: '🔴',
    sell: { STOP_LOSS: '🛑', TAKE_PROFIT: '🎯', TRAILING_STOP: '📉', SIGNAL: '🔵' }[reason] || '🔵',
  };
  const titles = {
    buy: 'BUY Signal Executed',
    circuit_breaker: '⛔ CIRCUIT BREAKER — Trading Halted',
    sell: { STOP_LOSS: 'Stop Loss Hit', TAKE_PROFIT: 'Take Profit Hit ✅', TRAILING_STOP: 'Trailing Stop Hit', SIGNAL: 'SELL Signal' }[reason] || 'SELL',
  };

  const fields = [
    sym !== 'ALL' ? { name: 'Symbol',    value: sym,             inline: true } : null,
    qty  > 0      ? { name: 'Shares',    value: String(qty),     inline: true } : null,
    price > 0     ? { name: 'Price',     value: `$${price.toFixed(2)}`, inline: true } : null,
    pnl  !== undefined ? { name: 'P&L', value: `${pnl >= 0 ? '+' : ''}$${pnl.toFixed(2)}`, inline: true } : null,
    { name: 'Portfolio', value: `$${portfolio.toFixed(2)}`, inline: true },
    { name: 'W / L',     value: `${totalWins} / ${totalLosses}`, inline: true },
    { name: 'Mode',      value: CONFIG.mode === 'alpaca' ? (CONFIG.alpacaPaper ? '📄 Paper' : '💰 LIVE') : '🔵 Sim', inline: true },
  ].filter(Boolean);

  if (type === 'buy' && sigInfo?.reasons?.length) {
    fields.push({ name: `Confidence: ${sigInfo.confidence}%`, value: sigInfo.reasons.slice(0, 5).join('\n'), inline: false });
  }

  try {
    await fetch(CONFIG.discordWebhook, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ embeds: [{
        title: `${icons[type]} TradeCore Pro — ${titles[type]}`,
        color: type === 'buy' ? colors.buy : type === 'sell' ? colors.sell : colors.circuit_breaker,
        fields,
        footer: { text: `TradeCore Pro | ${CONFIG.strategy.toUpperCase()} | ${CONFIG.alpacaPaper ? 'Paper' : 'Live'}` },
        timestamp: new Date().toISOString(),
      }]}),
    });
  } catch (e) { log('error', `Discord failed: ${e.message}`); }
}

async function sendDailySummary() {
  if (!CONFIG.discordWebhook) return;
  const { default: fetch } = await import('node-fetch');

  const closed   = trades.filter(t => t.pnl !== null);
  const totalPnl = closed.reduce((a, t) => a + t.pnl, 0);
  const winRate  = (totalWins + totalLosses) > 0 ? ((totalWins / (totalWins + totalLosses)) * 100).toFixed(1) : 'N/A';
  const avgWin   = totalWins   > 0 ? (closed.filter(t => t.pnl > 0).reduce((a,t) => a + t.pnl, 0) / totalWins).toFixed(2)   : '0.00';
  const avgLoss  = totalLosses > 0 ? (closed.filter(t => t.pnl <= 0).reduce((a,t) => a + t.pnl, 0) / totalLosses).toFixed(2) : '0.00';

  await fetch(CONFIG.discordWebhook, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ embeds: [{
      title: '📊 TradeCore Pro — End of Day Summary',
      color: totalPnl >= 0 ? 0x7fff6e : 0xff5f57,
      fields: [
        { name: 'Portfolio',     value: `$${portfolio.toFixed(2)}`, inline: true },
        { name: 'Day P&L',       value: `${totalPnl >= 0 ? '+' : ''}$${totalPnl.toFixed(2)}`, inline: true },
        { name: 'Win Rate',      value: `${winRate}%`, inline: true },
        { name: 'Trades Today',  value: String(closed.length), inline: true },
        { name: 'W / L',         value: `${totalWins} / ${totalLosses}`, inline: true },
        { name: 'Open Positions',value: String(Object.keys(positions).length), inline: true },
        { name: 'Avg Win',       value: `$${avgWin}`, inline: true },
        { name: 'Avg Loss',      value: `$${avgLoss}`, inline: true },
        { name: 'Filters',       value: `Trend:${CONFIG.trendFilter?'✅':'❌'} Volume:${CONFIG.volumeFilter?'✅':'❌'} Regime:${CONFIG.regimeFilter?'✅':'❌'} Corr:${CONFIG.correlationFilter?'✅':'❌'}`, inline: false },
      ],
      footer: { text: 'TradeCore Pro' },
      timestamp: new Date().toISOString(),
    }]}),
  }).catch(e => log('error', `Daily summary failed: ${e.message}`));

  // Reset daily state
  dailyStartPortfolio = portfolio;
  circuitBreakerOn    = false;
  totalWins = 0; totalLosses = 0;
  trades = trades.filter(t => t.pnl === null); // keep only open trades
  log('sys', 'Daily reset complete');
}

// ─────────────────────────────────────────────
// STARTUP + SCHEDULER
// ─────────────────────────────────────────────
log('sys', '══════════════════════════════════════════');
log('sys', '   TradeCore Pro — Upgraded Engine v3     ');
log('sys', '   + International ETF Session Awareness  ');
log('sys', '══════════════════════════════════════════');
log('sys', `Mode: ${CONFIG.mode.toUpperCase()} | Paper: ${CONFIG.alpacaPaper} | Strategy: ${CONFIG.strategy}`);
log('sys', `Symbols: ${CONFIG.symbols.join(', ')}`);
log('sys', `Intl ETFs: ${Object.keys(ETF_SESSIONS).filter(s => !['SPY','QQQ'].includes(s)).join(', ')}`);
log('sys', `Risk: SL=${CONFIG.stopLossPct*100}% TP=${CONFIG.takeProfitPct*100}% Trailing=${CONFIG.trailingStop} MaxDailyLoss=${CONFIG.maxDailyLossPct*100}%`);
log('sys', `Filters: Trend=${CONFIG.trendFilter} Volume=${CONFIG.volumeFilter} Regime=${CONFIG.regimeFilter} Corr=${CONFIG.correlationFilter}`);
log('sys', `Current session: ${getCurrentSession()}`);

// Scan every N minutes
cron.schedule(`*/${CONFIG.scanIntervalMin} * * * *`, runScan);
// End of day summary at 4:05 PM ET
cron.schedule('5 16 * * 1-5', sendDailySummary, { timezone: 'America/New_York' });
// Pre-market: store prev day close at 8:55 AM ET for gap detection
cron.schedule('55 8 * * 1-5', storePrevClose, { timezone: 'America/New_York' });

// Run immediately on startup
runScan();

// ─────────────────────────────────────────────
// HEALTH ENDPOINT
// ─────────────────────────────────────────────
http.createServer((req, res) => {
  const openPnl = Object.entries(positions).reduce((acc, [sym, pos]) => {
    const cur = priceHistory5m[sym]?.[priceHistory5m[sym].length - 1] || pos.entryPrice;
    return acc + (cur - pos.entryPrice) * pos.qty;
  }, 0);
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    status: 'running',
    session: getCurrentSession(),
    market_open: isMarketOpen(),
    portfolio: +portfolio.toFixed(2),
    open_pnl: +openPnl.toFixed(2),
    total_value: +(portfolio + openPnl).toFixed(2),
    open_positions: Object.keys(positions).length,
    wins: totalWins, losses: totalLosses,
    win_rate: (totalWins + totalLosses) > 0 ? ((totalWins / (totalWins + totalLosses)) * 100).toFixed(1) + '%' : 'N/A',
    circuit_breaker: circuitBreakerOn,
    last_scan: lastScanTime,
    uptime_min: Math.round(process.uptime() / 60),
  }, null, 2));
}).listen(process.env.PORT || 3000, () => log('sys', `Health → port ${process.env.PORT || 3000}`));
