/**
 * TradeCore Pro Bot — Upgraded Engine v4
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
 *  ✅ International ETF session awareness
 *  ✅ VWAP + Bollinger Bands
 *  ✅ Confidence scoring (60%+ required)
 *
 *  🆕 ADVANCED EXIT ALGORITHMS v4:
 *  ✅ ATR-based dynamic stop loss (adjusts to volatility)
 *  ✅ Break-even stop (moves SL to entry once +2%)
 *  ✅ 3-tier scaled take profit (33% at TP1, 33% at TP2, 34% at TP3)
 *  ✅ Trailing stop on remaining position after partial exits
 *  ✅ Time-based stop (exits flat trades after N hours)
 *  ✅ Volatility squeeze exit (exits if ATR collapses while in trade)
 *  ✅ Support/resistance exit (exits near key price levels)
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
  rsiOversold:   +(process.env.RSI_OVERSOLD   || 35),
  rsiOverbought: +(process.env.RSI_OVERBOUGHT || 65),

  startingCapital:  +(process.env.CAPITAL          || 10000),
  maxPositionPct:   +(process.env.MAX_POSITION_PCT  || 15) / 100,
  maxOpenPositions: +(process.env.MAX_POSITIONS     || 3),
  stopLossPct:      +(process.env.STOP_LOSS_PCT     || 4)  / 100,
  takeProfitPct:    +(process.env.TAKE_PROFIT_PCT   || 8)  / 100,
  trailingStop:     process.env.TRAILING_STOP       !== 'false',
  trailingStopPct:  +(process.env.TRAILING_STOP_PCT || 3)  / 100,
  maxDailyLossPct:  +(process.env.MAX_DAILY_LOSS    || 3)  / 100,

  // Advanced exit config
  breakEvenAt:       +(process.env.BREAK_EVEN_AT     || 2)  / 100, // move SL to entry once up X%
  tp1Pct:            +(process.env.TP1_PCT           || 4)  / 100, // sell 33% here
  tp2Pct:            +(process.env.TP2_PCT           || 8)  / 100, // sell 33% here
  tp3Pct:            +(process.env.TP3_PCT           || 14) / 100, // sell final 34% here
  timeStopHours:     +(process.env.TIME_STOP_HOURS   || 6),        // exit flat trade after N hours
  timeStopMinPct:    +(process.env.TIME_STOP_MIN_PCT || 0.5) / 100,// only exit if gain < X%
  atrStopMult:       +(process.env.ATR_STOP_MULT     || 2.0),      // SL = entry - (ATR * mult)
  volSqueezePct:     +(process.env.VOL_SQUEEZE_PCT   || 50) / 100, // exit if ATR drops X% vs entry ATR

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

// ─────────────────────────────────────────────
// DYNAMIC CONFIG — loaded from Supabase on startup + every scan
// Secrets (API keys, webhook) stay in Render env vars only
// Everything else can be changed from the dashboard
// ─────────────────────────────────────────────
async function loadRemoteConfig() {
  if (!CONFIG.supabaseUrl || !CONFIG.supabaseKey) return;
  try {
    const { default: fetch } = await import('node-fetch');
    const res = await fetch(`${CONFIG.supabaseUrl}/rest/v1/tc_settings?id=eq.1`, {
      headers: {
        'apikey': CONFIG.supabaseKey,
        'Authorization': `Bearer ${CONFIG.supabaseKey}`,
      }
    });
    if (!res.ok) return;
    const rows = await res.json();
    if (!rows || rows.length === 0) return;
    const s = rows[0];

    // Apply remote settings over CONFIG — secrets never overwritten
    if (s.symbols)          CONFIG.symbols          = s.symbols.split(',').map(x => x.trim().toUpperCase());
    if (s.strategy)         CONFIG.strategy         = s.strategy;
    if (s.rsi_period)       CONFIG.rsiPeriod        = +s.rsi_period;
    if (s.rsi_oversold)     CONFIG.rsiOversold      = +s.rsi_oversold;
    if (s.rsi_overbought)   CONFIG.rsiOverbought    = +s.rsi_overbought;
    if (s.scan_interval)    CONFIG.scanIntervalSec  = +s.scan_interval; // seconds
    if (s.stop_loss_pct)    CONFIG.stopLossPct      = +s.stop_loss_pct / 100;
    if (s.take_profit_pct)  CONFIG.takeProfitPct    = +s.take_profit_pct / 100;
    if (s.trailing_stop_pct) CONFIG.trailingStopPct = +s.trailing_stop_pct / 100;
    if (s.max_daily_loss)   CONFIG.maxDailyLossPct  = +s.max_daily_loss / 100;
    if (s.max_positions)    CONFIG.maxOpenPositions = +s.max_positions;
    if (s.max_position_pct) CONFIG.maxPositionPct   = +s.max_position_pct / 100;
    if (s.tp1_pct)          CONFIG.tp1Pct           = +s.tp1_pct / 100;
    if (s.tp2_pct)          CONFIG.tp2Pct           = +s.tp2_pct / 100;
    if (s.tp3_pct)          CONFIG.tp3Pct           = +s.tp3_pct / 100;
    if (s.break_even_at)    CONFIG.breakEvenAt      = +s.break_even_at / 100;
    if (s.discord_webhook)  CONFIG.discordWebhook   = s.discord_webhook;
    if (s.trend_filter      !== undefined) CONFIG.trendFilter       = !!s.trend_filter;
    if (s.volume_filter     !== undefined) CONFIG.volumeFilter      = !!s.volume_filter;
    if (s.regime_filter     !== undefined) CONFIG.regimeFilter      = !!s.regime_filter;
    if (s.correlation_filter !== undefined) CONFIG.correlationFilter = !!s.correlation_filter;

    log('sys', `Remote config loaded from Supabase — strategy:${CONFIG.strategy} symbols:${CONFIG.symbols.length} RSI:${CONFIG.rsiOversold}/${CONFIG.rsiOverbought}`);
  } catch(e) {
    log('warn', `Could not load remote config: ${e.message} — using defaults`);
  }
}
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
    const headers = {
      'apikey': CONFIG.supabaseKey,
      'Authorization': `Bearer ${CONFIG.supabaseKey}`,
      'Content-Type': 'application/json',
    };
    if (method === 'POST') headers['Prefer'] = 'resolution=merge-duplicates,return=minimal';
    if (method === 'PATCH') headers['Prefer'] = 'return=minimal';
    if (method === 'DELETE') headers['Prefer'] = 'return=minimal';

    const res = await fetch(`${CONFIG.supabaseUrl}/rest/v1/${path}`, {
      method,
      headers,
      body: body ? JSON.stringify(body) : undefined,
    });
    // 200, 201, 204 are all success for Supabase REST
    if (res.status >= 200 && res.status < 300) {
      const text = await res.text();
      return text ? JSON.parse(text) : { ok: true };
    }
    const errText = await res.text();
    log('error', `Supabase ${method} ${path} → ${res.status}: ${errText}`);
    return null;
  } catch (e) {
    log('error', `Supabase ${method} ${path}: ${e.message}`);
    return null;
  }
}

async function syncPortfolio() {
  // Get real values directly from Alpaca — most accurate source
  let cashValue = portfolio;
  let equityValue = portfolio;

  if (CONFIG.alpacaKey) {
    try {
      const acct = await getAccount();
      if (acct?.cash)         cashValue   = +parseFloat(acct.cash).toFixed(2);
      if (acct?.equity)       equityValue = +parseFloat(acct.equity).toFixed(2);
      if (acct?.last_equity)  CONFIG._lastEquity = +parseFloat(acct.last_equity).toFixed(2);
    } catch(e) {}
  }

  // Day P&L = today's equity vs yesterday's close (from Alpaca account)
  const dayPnl = CONFIG._lastEquity
    ? +(equityValue - CONFIG._lastEquity).toFixed(2)
    : trades.filter(t => t.side==='SELL' && t.pnl !== null && new Date(t.time).toDateString() === new Date().toDateString())
             .reduce((a,t) => a + t.pnl, 0);

  await sbFetch('tc_portfolio?id=eq.1', 'PATCH', {
    cash:            +cashValue.toFixed(2),
    total_value:     +equityValue.toFixed(2),
    day_pnl:         +dayPnl.toFixed(2),
    total_wins:      totalWins,
    total_losses:    totalLosses,
    circuit_breaker: circuitBreakerOn,
    last_scan:       new Date().toISOString(),
    session:         getCurrentSession(),
    updated_at:      new Date().toISOString(),
  });

  // Store equity snapshot for the chart (every sync)
  await sbFetch('tc_equity', 'POST', {
    value:      +equityValue.toFixed(2),
    created_at: new Date().toISOString(),
  });
}

async function syncPositions() {
  // Step 1: get all symbols currently tracked in Supabase
  const existing = await sbFetch('tc_positions', 'GET');
  const existingSyms = new Set((existing || []).map(p => p.symbol));

  // Step 2: delete any symbols no longer in positions{}
  const currentSyms = new Set(Object.keys(positions));
  for (const sym of existingSyms) {
    if (!currentSyms.has(sym)) {
      await sbFetch(`tc_positions?symbol=eq.${sym}`, 'DELETE');
    }
  }

  // Step 3: upsert each current position with latest price
  for (const [sym, pos] of Object.entries(positions)) {
    const cur     = priceHistory5m[sym]?.[priceHistory5m[sym].length - 1] || pos.entryPrice;
    const qty     = pos.qtyRemaining || pos.qty;
    const cost    = pos.entryPrice * qty;
    const pnl     = (cur - pos.entryPrice) * qty;
    const pnlPct  = ((cur - pos.entryPrice) / pos.entryPrice) * 100;

    await sbFetch('tc_positions?symbol=eq.' + sym, 'DELETE');
    await sbFetch('tc_positions', 'POST', {
      symbol:        sym,
      entry_price:   +pos.entryPrice.toFixed(4),
      qty,
      cost:          +cost.toFixed(2),
      current_price: +cur.toFixed(4),
      pnl:           +pnl.toFixed(2),
      pnl_pct:       +pnlPct.toFixed(2),
      entry_time:    new Date(pos.entryTime).toISOString(),
      high_water:    +pos.highWater.toFixed(4),
      confidence:    pos.sigInfo?.confidence || 0,
      updated_at:    new Date().toISOString(),
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

// Sync live prices every 60s using Alpaca's own position data
// Alpaca gives us current_price, unrealized_pl directly — most accurate source
async function syncPricesOnly() {
  if (!CONFIG.alpacaKey) return;
  if (Object.keys(positions).length === 0) return;

  try {
    const alpacaPos = await alpacaFetch(`${ALPACA_BASE()}/v2/positions`);
    if (!Array.isArray(alpacaPos)) return;

    for (const ap of alpacaPos) {
      const sym         = ap.symbol;
      const cur         = +ap.current_price;
      const qty         = +ap.qty;
      const entryPrice  = +ap.avg_entry_price;
      const pnl         = +ap.unrealized_pl;
      const pnlPct      = +ap.unrealized_plpc * 100;

      // Update in-memory price history
      if (!priceHistory5m[sym]) priceHistory5m[sym] = [];
      priceHistory5m[sym].push(cur);
      if (priceHistory5m[sym].length > 60) priceHistory5m[sym].shift();

      // Update high water mark
      if (positions[sym] && cur > positions[sym].highWater) {
        positions[sym].highWater = cur;
      }

      // Push live price to Supabase via PATCH
      const patchResult = await sbFetch(`tc_positions?symbol=eq.${sym}`, 'PATCH', {
        current_price: +cur.toFixed(4),
        pnl:           +pnl.toFixed(2),
        pnl_pct:       +pnlPct.toFixed(4),
        high_water:    positions[sym] ? +positions[sym].highWater.toFixed(4) : +cur.toFixed(4),
        updated_at:    new Date().toISOString(),
      });

      log('price', `${sym} live price: $${cur.toFixed(2)} | P&L: ${pnl>=0?'+':''}$${pnl.toFixed(2)} (${pnlPct.toFixed(2)}%)`);
    }

    // Also update portfolio cash in Supabase
    const acct = await getAccount();
    if (acct?.cash) {
      await sbFetch('tc_portfolio?id=eq.1', 'PATCH', {
        cash:        +parseFloat(acct.cash).toFixed(2),
        last_scan:   new Date().toISOString(),
        updated_at:  new Date().toISOString(),
      });
    }

  } catch (e) {
    log('error', `syncPricesOnly failed: ${e.message}`);
  }
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

// Set BYPASS_HOURS=true in Render to test the bot outside market hours
const BYPASS_HOURS = process.env.BYPASS_HOURS === 'true';

function isMarketOpen() {
  if (BYPASS_HOURS) return true; // testing mode
  const et = getETTime();
  const day = et.getDay();
  const mins = et.getHours() * 60 + et.getMinutes();
  return day >= 1 && day <= 5 && mins >= 571 && mins <= 955; // 9:31 - 3:55
}

function isWeekday() {
  if (BYPASS_HOURS) return true; // testing mode
  const day = getETTime().getDay();
  return day >= 1 && day <= 5;
}

function isPreMarket() {
  if (BYPASS_HOURS) return false;
  const et = getETTime();
  const mins = et.getHours() * 60 + et.getMinutes();
  return et.getDay() >= 1 && et.getDay() <= 5 && mins >= 240 && mins < 570;
}

/**
 * For international ETFs we scan whenever their home market is open.
 * For regular US stocks (AAPL, TSLA, MSFT etc.) we STRICTLY enforce
 * US market hours — no scanning, no orders outside 9:31-3:55 ET.
 * Set BYPASS_HOURS=true to override for testing.
 */
function shouldScanSymbol(symbol) {
  if (BYPASS_HOURS) return true; // testing mode — scan everything
  if (!isWeekday()) return false;
  const isIntlETF = !!ETF_SESSIONS[symbol];
  if (!isIntlETF) {
    return isMarketOpen();
  }
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
  // Try Alpaca first
  try {
    const start = new Date(Date.now() - 5 * 24 * 60 * 60 * 1000).toISOString();
    const url = `${ALPACA_DATA_BASE}/v2/stocks/${symbol}/bars?timeframe=${timeframe}&start=${start}&limit=${limit}&feed=iex`;
    const data = await alpacaFetch(url);
    if (data.bars && data.bars.length >= 5) return data.bars;
  } catch (e) {}

  // Fallback: Yahoo Finance (free, no key)
  try {
    const { default: fetch } = await import('node-fetch');
    const interval = timeframe === '5Min' ? '5m' : timeframe === '15Min' ? '15m' : '1d';
    const range    = timeframe === '1Day' ? '3mo' : '5d';
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${symbol}?interval=${interval}&range=${range}`;
    const res = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
    const data = await res.json();
    const result = data?.chart?.result?.[0];
    if (!result) return null;
    const timestamps = result.timestamp || [];
    const q = result.indicators?.quote?.[0] || {};
    const bars = timestamps.map((t, i) => ({
      t: new Date(t * 1000).toISOString(),
      o: q.open?.[i], h: q.high?.[i], l: q.low?.[i],
      c: q.close?.[i], v: q.volume?.[i] || 0,
    })).filter(b => b.c != null);
    if (bars.length >= 5) {
      log('data', `${symbol} using Yahoo Finance fallback (${bars.length} bars)`);
      return bars;
    }
  } catch (e) {}

  log('error', `fetchBars ${symbol} ${timeframe}: all sources failed`);
  return null;
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

// Stochastic RSI — %K and %D (more sensitive than plain RSI)
function stochRSI(prices, rsiPeriod = 14, stochPeriod = 14) {
  if (prices.length < rsiPeriod + stochPeriod) return { k: 50, d: 50 };
  // Calculate RSI series
  const rsiSeries = [];
  for (let i = rsiPeriod; i <= prices.length; i++) {
    rsiSeries.push(rsi(prices.slice(0, i), rsiPeriod));
  }
  const slice = rsiSeries.slice(-stochPeriod);
  const minRsi = Math.min(...slice);
  const maxRsi = Math.max(...slice);
  const k = maxRsi === minRsi ? 50 : ((rsiSeries[rsiSeries.length - 1] - minRsi) / (maxRsi - minRsi)) * 100;
  const d = rsiSeries.slice(-3).reduce((a, v, _, arr) => {
    const s = arr.slice(-3); const mn = Math.min(...s); const mx = Math.max(...s);
    return mx === mn ? 50 : ((v - mn) / (mx - mn)) * 100;
  }, 50);
  return { k, d };
}

// ADX — trend strength (>25 = trending, <20 = ranging)
function adx(bars, period = 14) {
  if (!bars || bars.length < period + 1) return { adx: 0, diPlus: 0, diMinus: 0 };
  let trSum = 0, dmPlus = 0, dmMinus = 0;
  for (let i = 1; i < bars.length; i++) {
    const h = bars[i].h, l = bars[i].l, ph = bars[i-1].h, pl = bars[i-1].l, pc = bars[i-1].c;
    const tr = Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc));
    const upMove = h - ph, downMove = pl - l;
    trSum += tr;
    if (upMove > downMove && upMove > 0)   dmPlus  += upMove;
    if (downMove > upMove && downMove > 0) dmMinus += downMove;
  }
  const diPlus  = trSum > 0 ? (dmPlus  / trSum) * 100 : 0;
  const diMinus = trSum > 0 ? (dmMinus / trSum) * 100 : 0;
  const dx = (diPlus + diMinus) > 0 ? Math.abs(diPlus - diMinus) / (diPlus + diMinus) * 100 : 0;
  return { adx: dx, diPlus, diMinus };
}

// ─────────────────────────────────────────────
// SIGNAL ENGINE
// ─────────────────────────────────────────────
function generateSignal(sym, bars5m, bars15m) {
  if (!bars5m || bars5m.length < 20) return { signal: 'HOLD', confidence: 0, reasons: ['Insufficient data'] };

  const c5   = bars5m.map(b => b.c);
  const c15  = bars15m?.length >= 20 ? bars15m.map(b => b.c) : null;
  const vol  = bars5m.map(b => b.v);
  const price = c5[c5.length - 1];

  let buy = 0, sell = 0;
  const reasons = [];

  // ── 1. RSI ──
  const r = rsi(c5, CONFIG.rsiPeriod);

  // Hard gate: RSI must be clearly oversold/overbought — no trading in neutral zone
  const rsiNeutral = r >= 40 && r <= 60;
  if (rsiNeutral) {
    return { signal: 'HOLD', confidence: 0, reasons: [`RSI neutral (${r.toFixed(1)}) — no edge, skipping`], rsi: r };
  }

  if (r < CONFIG.rsiOversold)        { buy  += 30; reasons.push(`RSI oversold (${r.toFixed(1)})`); }
  else if (r > CONFIG.rsiOverbought) { sell += 30; reasons.push(`RSI overbought (${r.toFixed(1)})`); }
  else if (r < 40)                   { buy  += 10; reasons.push(`RSI leaning oversold (${r.toFixed(1)})`); }
  else if (r > 60)                   { sell += 10; reasons.push(`RSI leaning overbought (${r.toFixed(1)})`); }

  // ── 2. MACD — only count crossovers, not just position ──
  const e8 = ema(c5, 8), e21 = ema(c5, 21);
  const pe8 = ema(c5.slice(0, -1), 8), pe21 = ema(c5.slice(0, -1), 21);
  const macdDiff = Math.abs(e8 - e21) / e21; // how far apart are the EMAs?

  // Only count MACD if EMAs have meaningful separation (not crossing in neutral)
  if (macdDiff > 0.001) {
    if (e8 > e21) { buy  += 15; reasons.push('MACD bullish'); }
    else          { sell += 15; reasons.push('MACD bearish'); }
  } else {
    reasons.push('MACD neutral (EMAs too close)');
  }

  // Crossover bonus — strong signal
  if (pe8 < pe21 && e8 > e21) { buy  += 15; reasons.push('MACD bullish crossover ↑'); }
  if (pe8 > pe21 && e8 < e21) { sell += 15; reasons.push('MACD bearish crossover ↓'); }

  // ── 3. 200 EMA trend filter — hard gate, not just points ──
  if (CONFIG.trendFilter && c5.length >= 40) {
    const e200 = ema(c5, Math.min(200, c5.length));
    if (price > e200) { buy  += 20; reasons.push('Above 200 EMA (uptrend)'); }
    else {
      // Below 200 EMA — cancel any buy signal entirely
      if (buy > 0) {
        reasons.push('Below 200 EMA — BUY cancelled');
        return { signal: 'HOLD', confidence: 0, reasons, rsi: r };
      }
      sell += 20; reasons.push('Below 200 EMA (downtrend)');
    }
  }

  // ── 4. Volume — must confirm direction ──
  if (CONFIG.volumeFilter && vol.length >= 10) {
    const avgVol = vol.slice(-20).reduce((a, b) => a + b, 0) / Math.min(20, vol.length);
    const curVol = vol[vol.length - 1];
    if (curVol > avgVol * 1.5) {
      buy > sell ? buy += 20 : sell += 20;
      reasons.push(`Strong volume (${(curVol/avgVol).toFixed(1)}x avg)`);
    } else if (curVol < avgVol * 0.7) {
      // Low volume = indecision, cancel signal
      reasons.push('Low volume — indecision, skipping');
      return { signal: 'HOLD', confidence: 0, reasons, rsi: r };
    } else {
      reasons.push(`Normal volume (${(curVol/avgVol).toFixed(1)}x avg)`);
    }
  }

  // ── 5. 15min timeframe — must agree or signal is cancelled ──
  if (c15) {
    const r15 = rsi(c15, CONFIG.rsiPeriod);
    const e8_15 = ema(c15, 8), e21_15 = ema(c15, 21);
    const tf15Bullish = r15 < 50 && e8_15 > e21_15;
    const tf15Bearish = r15 > 50 && e8_15 < e21_15;

    if (buy > sell && !tf15Bullish) {
      reasons.push('15min disagrees with BUY — cancelled');
      return { signal: 'HOLD', confidence: 0, reasons, rsi: r };
    }
    if (sell > buy && !tf15Bearish) {
      reasons.push('15min disagrees with SELL — cancelled');
      return { signal: 'HOLD', confidence: 0, reasons, rsi: r };
    }
    buy > sell
      ? (buy += 15, reasons.push('15min confirms bullish'))
      : (sell += 15, reasons.push('15min confirms bearish'));
  }

  // ── 6. Bollinger Bands ──
  const bb = bollingerBands(c5);
  if (bb) {
    if (price <= bb.lower)       { buy  += 15; reasons.push('At/below lower Bollinger Band'); }
    else if (price >= bb.upper)  { sell += 15; reasons.push('At/above upper Bollinger Band'); }
    else {
      // Price in middle of bands = indecision zone
      const midDist = Math.abs(price - bb.middle) / (bb.upper - bb.middle);
      if (midDist < 0.3) {
        reasons.push('Price in Bollinger Band middle — indecision');
        return { signal: 'HOLD', confidence: 0, reasons, rsi: r };
      }
    }
  }

  // ── 7. VWAP ──
  if (bars5m.length >= 5) {
    const vw = vwap(bars5m.slice(-20));
    if (price > vw * 1.002)       { buy  += 10; reasons.push(`Above VWAP ($${vw.toFixed(2)})`); }
    else if (price < vw * 0.998)  { sell += 10; reasons.push(`Below VWAP ($${vw.toFixed(2)})`); }
    else {
      reasons.push('Hugging VWAP — indecision');
      return { signal: 'HOLD', confidence: 0, reasons, rsi: r };
    }
  }

  // ── 8. Stochastic RSI — early entry signals ──
  const stoch = stochRSI(c5);
  if (stoch.k < 20 && stoch.d < 20)       { buy  += 15; reasons.push(`StochRSI oversold (K:${stoch.k.toFixed(0)} D:${stoch.d.toFixed(0)})`); }
  else if (stoch.k > 80 && stoch.d > 80)  { sell += 15; reasons.push(`StochRSI overbought (K:${stoch.k.toFixed(0)} D:${stoch.d.toFixed(0)})`); }
  // Stoch K crossing D = signal confirmation
  if (stoch.k > stoch.d && stoch.k < 50)  { buy  += 10; reasons.push('StochRSI bullish cross'); }
  if (stoch.k < stoch.d && stoch.k > 50)  { sell += 10; reasons.push('StochRSI bearish cross'); }

  // ── 9. ADX — only trade when trend is strong ──
  const adxData = adx(bars5m);
  if (adxData.adx < 20) {
    reasons.push(`ADX ${adxData.adx.toFixed(0)} — ranging market, no trend`);
    return { signal: 'HOLD', confidence: 0, reasons, rsi: r };
  }
  if (adxData.adx > 25) {
    if (adxData.diPlus > adxData.diMinus)  { buy  += 15; reasons.push(`ADX trending (${adxData.adx.toFixed(0)}) +DI>${adxData.diPlus.toFixed(0)}`); }
    else                                    { sell += 15; reasons.push(`ADX trending (${adxData.adx.toFixed(0)}) -DI>${adxData.diMinus.toFixed(0)}`); }
  }

  // ── 8. Momentum check — price must be moving in signal direction ──
  if (c5.length >= 5) {
    const recentChange = (c5[c5.length-1] - c5[c5.length-5]) / c5[c5.length-5];
    if (buy > sell && recentChange < -0.005) {
      reasons.push('Price falling despite bullish signal — skipping');
      return { signal: 'HOLD', confidence: 0, reasons, rsi: r };
    }
    if (sell > buy && recentChange > 0.005) {
      reasons.push('Price rising despite bearish signal — skipping');
      return { signal: 'HOLD', confidence: 0, reasons, rsi: r };
    }
  }

  // ── Final threshold ──
  const total = buy + sell;
  const confidence = total > 0 ? Math.round(Math.max(buy, sell) / total * 100) : 0;

  // Require strong score AND clear edge
  const minScore = +(process.env.MIN_SCORE || 60);
  const minEdge  = +(process.env.MIN_EDGE  || 1.5);

  if (buy >= minScore && buy > sell * minEdge)  return { signal: 'BUY',  confidence, score: buy,  reasons, rsi: r };
  if (sell >= minScore && sell > buy * minEdge) return { signal: 'SELL', confidence, score: sell, reasons, rsi: r };

  reasons.push(`Score insufficient (buy=${buy} sell=${sell} need ${minScore} with ${minEdge}x edge)`);
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
    const latest = closes[closes.length - 1];
    // Loosened: only block if BOTH price below EMA10 AND EMA10 below EMA30 by >1%
    const hardBearish = latest < e10 && (e10 - e30) / e30 > 0.01;
    if (hardBearish) {
      log('regime', `⚠ Market BEARISH — BUYs paused (SPY=$${latest.toFixed(2)} EMA10=$${e10.toFixed(2)} EMA30=$${e30.toFixed(2)})`);
    } else {
      log('regime', `✅ Market OK — trading enabled (SPY=$${latest.toFixed(2)})`);
    }
    return !hardBearish;
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

// Short positions tracked separately (sym → { entryPrice, qty, ... })
let shortPositions = {};

async function enterPosition(sym, price, sigInfo, bars, direction = 'long') {
  const isIntlETF = !!ETF_SESSIONS[sym];
  if (!isIntlETF && !isMarketOpen()) { log('warn', `🚫 Blocked ${sym} — market closed`); return; }
  if (checkCircuitBreaker()) return;
  if (hasLargeGap(sym, price)) return;

  // Don't enter if already in position in same direction
  if (direction === 'long'  && (positions[sym]      || alpacaPositions.has(sym)))      { log('warn', `${sym} already long`); return; }
  if (direction === 'short' && (shortPositions[sym]  || alpacaShorts.has(sym)))         { log('warn', `${sym} already short`); return; }
  if (direction === 'long'  && isCorrelated(sym)) return;

  const qty  = calcQty(sym, price, bars);
  const cost = qty * price;
  if (qty < 1) { log('warn', `Cannot enter ${sym}: qty too small`); return; }

  const atrVal    = bars && bars.length >= 14 ? atr(bars, 14) : price * CONFIG.stopLossPct;
  const srLevels  = calcSRLevels(bars || []);

  if (direction === 'long') {
    if (cost > portfolio) { log('warn', `Not enough cash for ${sym}`); return; }
    const atrStop  = price - (atrVal * CONFIG.atrStopMult);
    const pctStop  = price * (1 - CONFIG.stopLossPct);
    const stopPrice = Math.max(atrStop, pctStop);

    if (CONFIG.mode === 'alpaca' && CONFIG.alpacaKey) {
      try { await placeOrder(sym, qty, 'buy'); }
      catch (e) { log('error', `BUY failed ${sym}: ${e.message}`); return; }
    }
    portfolio -= cost;
    positions[sym] = {
      entryPrice: price, qty, qtyRemaining: qty, cost,
      entryTime: new Date(), highWater: price,
      atrAtEntry: atrVal, stopPrice,
      breakEvenSet: false, tp1Hit: false, tp2Hit: false,
      srLevels, sigInfo, direction: 'long',
    };
    alpacaPositions.add(sym);
    trades.push({ time: new Date(), sym, side: 'BUY', qty, price, pnl: null, reason: 'SIGNAL', confidence: sigInfo.confidence });
    const stopPct = ((price - stopPrice) / price * 100).toFixed(2);
    log('buy', `✅ LONG ${qty}x ${sym} @ $${price.toFixed(2)} | SL=$${stopPrice.toFixed(2)} (-${stopPct}%) | conf=${sigInfo.confidence}%`);
    await sendDiscordAlert('buy', sym, qty, price, undefined, undefined, sigInfo, { stopPrice, atrVal });
    await syncTrade({ sym, side: 'BUY', qty, price, pnl: null, reason: 'SIGNAL', confidence: sigInfo.confidence });

  } else {
    // SHORT — borrow shares to sell, profit if price falls
    // Stop loss ABOVE entry for shorts
    const atrStop   = price + (atrVal * CONFIG.atrStopMult);
    const pctStop   = price * (1 + CONFIG.stopLossPct);
    const stopPrice = Math.min(atrStop, pctStop);

    if (CONFIG.mode === 'alpaca' && CONFIG.alpacaKey) {
      try { await placeOrder(sym, qty, 'sell'); } // Alpaca handles shorting automatically
      catch (e) { log('error', `SHORT failed ${sym}: ${e.message}`); return; }
    }
    shortPositions[sym] = {
      entryPrice: price, qty, qtyRemaining: qty,
      entryTime: new Date(), lowWater: price, // track lowest price for trailing
      atrAtEntry: atrVal, stopPrice,
      breakEvenSet: false, tp1Hit: false, tp2Hit: false,
      srLevels, sigInfo, direction: 'short',
    };
    alpacaShorts.add(sym);
    trades.push({ time: new Date(), sym, side: 'SHORT', qty, price, pnl: null, reason: 'SIGNAL', confidence: sigInfo.confidence });
    const stopPct = ((stopPrice - price) / price * 100).toFixed(2);
    log('short', `🔴 SHORT ${qty}x ${sym} @ $${price.toFixed(2)} | SL=$${stopPrice.toFixed(2)} (+${stopPct}%) | conf=${sigInfo.confidence}%`);
    await sendDiscordAlert('short', sym, qty, price, undefined, undefined, sigInfo, { stopPrice, atrVal });
    await syncTrade({ sym, side: 'SHORT', qty, price, pnl: null, reason: 'SIGNAL', confidence: sigInfo.confidence });
  }

  await syncAll();
  await syncLog(direction === 'long' ? 'buy' : 'sell', `${direction === 'long' ? '✅ LONG' : '🔴 SHORT'} ${qty}x ${sym} @ $${price.toFixed(2)} conf=${sigInfo.confidence}%`);
}

// Cover a short position (buy back the borrowed shares)
async function coverShort(sym, price, reason) {
  const pos = shortPositions[sym];
  if (!pos) return;
  const qty = pos.qtyRemaining || pos.qty;

  if (CONFIG.mode === 'alpaca' && CONFIG.alpacaKey) {
    try { await placeOrder(sym, qty, 'buy'); } // buy back to cover
    catch (e) { log('error', `Cover failed ${sym}: ${e.message}`); return; }
  }

  // Short P&L = (entry - exit) × qty (profit when price drops)
  const pnl = (pos.entryPrice - price) * qty;
  portfolio += pnl; // add profit (or subtract loss)
  pnl > 0 ? totalWins++ : totalLosses++;
  delete shortPositions[sym];
  alpacaShorts.delete(sym);

  const icon = { STOP_LOSS:'🛑', TAKE_PROFIT:'🎯', TRAILING_STOP:'📉', SIGNAL:'📤', TIME_STOP:'⏰' }[reason] || '📤';
  log('short', `${icon} COVER ${qty}x ${sym} @ $${price.toFixed(2)} | P&L: ${pnl>=0?'+':''}$${pnl.toFixed(2)} (${reason})`);
  trades.push({ time: new Date(), sym, side: 'COVER', qty, price, pnl, reason });
  await sendDiscordAlert('cover', sym, qty, price, pnl, reason);
  await syncTrade({ sym, side: 'COVER', qty, price, pnl, reason });
  await syncAll();
  await syncLog('sell', `${icon} COVER ${qty}x ${sym} @ $${price.toFixed(2)} P&L=${pnl>=0?'+':''}$${pnl.toFixed(2)} (${reason})`);
}

// Manage short positions — mirror of managePosition but inverted
async function manageShort(sym, price, bars) {
  const pos = shortPositions[sym];
  if (!pos) return;

  const chg      = (pos.entryPrice - price) / pos.entryPrice; // positive = profitable (price dropped)
  const holdMins = (Date.now() - new Date(pos.entryTime).getTime()) / 60000;

  // Update low water mark (we want price to go DOWN)
  if (price < pos.lowWater) shortPositions[sym].lowWater = price;

  // Ratchet stop loss DOWN as price falls (lock in profits)
  if (bars && bars.length >= 14) {
    const curAtr = atr(bars, 14);
    const newStop = price + (curAtr * CONFIG.atrStopMult);
    if (newStop < shortPositions[sym].stopPrice) {
      shortPositions[sym].stopPrice = newStop;
      log('risk', `${sym} short stop ratcheted → $${newStop.toFixed(2)}`);
    }
  }

  // Stop loss — price went UP (bad for short)
  if (price >= pos.stopPrice) {
    const stopType = pos.breakEvenSet ? 'BREAK_EVEN_STOP' : 'STOP_LOSS';
    return coverShort(sym, price, stopType);
  }

  // Break-even: once down breakEvenAt%, move stop to entry
  if (!pos.breakEvenSet && chg >= CONFIG.breakEvenAt) {
    shortPositions[sym].stopPrice  = pos.entryPrice;
    shortPositions[sym].breakEvenSet = true;
    log('risk', `🔒 Short break-even: ${sym} SL → $${pos.entryPrice.toFixed(2)}`);
  }

  // TP1 — cover 33% at tp1Pct drop
  if (!pos.tp1Hit && chg >= CONFIG.tp1Pct) {
    const sell = Math.max(1, Math.floor(pos.qtyRemaining * 0.33));
    shortPositions[sym].tp1Hit = true;
    await coverPartialShort(sym, price, sell, 'TP1');
    return;
  }
  // TP2 — cover 50% of remainder at tp2Pct
  if (pos.tp1Hit && !pos.tp2Hit && chg >= CONFIG.tp2Pct) {
    const sell = Math.max(1, Math.floor((shortPositions[sym]?.qtyRemaining || 1) * 0.5));
    shortPositions[sym].tp2Hit = true;
    await coverPartialShort(sym, price, sell, 'TP2');
    return;
  }
  // TP3 — cover rest at tp3Pct
  if (pos.tp1Hit && pos.tp2Hit && chg >= CONFIG.tp3Pct) {
    return coverShort(sym, price, 'TAKE_PROFIT');
  }

  // Time stop
  if (holdMins >= CONFIG.timeStopHours * 60 && Math.abs(chg) < CONFIG.timeStopMinPct) {
    return coverShort(sym, price, 'TIME_STOP');
  }

  log('pos', `SHORT ${sym} ${chg>=0?'+':''}${(chg*100).toFixed(2)}% | SL=$${pos.stopPrice.toFixed(2)} | BE:${pos.breakEvenSet?'✅':'❌'}`);
}

async function coverPartialShort(sym, price, qty, reason) {
  const pos = shortPositions[sym];
  if (!pos || qty < 1) return;
  if (CONFIG.mode === 'alpaca' && CONFIG.alpacaKey) {
    try { await placeOrder(sym, qty, 'buy'); }
    catch(e) { log('error', `Partial cover failed: ${e.message}`); return; }
  }
  const pnl = (pos.entryPrice - price) * qty;
  portfolio += pnl;
  shortPositions[sym].qtyRemaining -= qty;
  pnl > 0 ? totalWins++ : totalLosses++;
  if (shortPositions[sym].qtyRemaining <= 0) delete shortPositions[sym];
  log('short', `🎯 PARTIAL COVER ${reason}: ${qty}x ${sym} @ $${price.toFixed(2)} P&L=${pnl>=0?'+':''}$${pnl.toFixed(2)}`);
  trades.push({ time: new Date(), sym, side: 'COVER', qty, price, pnl, reason });
  await syncTrade({ sym, side: 'COVER', qty, price, pnl, reason });
  await syncAll();
}

async function partialExit(sym, price, qtyToSell, reason) {
  const pos = positions[sym];
  if (!pos || qtyToSell < 1) return;
  if (CONFIG.mode === 'alpaca' && CONFIG.alpacaKey) {
    try { await placeOrder(sym, qtyToSell, 'sell'); }
    catch (e) { log('error', `Partial sell failed ${sym}: ${e.message}`); return; }
  }
  const avgCost = pos.cost / (pos.qtyRemaining || pos.qty);
  const pnl = qtyToSell * price - qtyToSell * avgCost;
  portfolio += qtyToSell * price;
  positions[sym].qtyRemaining -= qtyToSell;
  positions[sym].cost = positions[sym].qtyRemaining * avgCost;
  pnl > 0 ? totalWins++ : totalLosses++;

  const icons = { TP1: '🎯', TP2: '🎯🎯', TP3: '🎯🎯🎯' };
  const icon = icons[reason] || '🎯';
  log('sell', `${icon} PARTIAL ${reason}: ${qtyToSell}x ${sym} @ $${price.toFixed(2)} P&L=${pnl>=0?'+':''}$${pnl.toFixed(2)} | Remaining: ${positions[sym].qtyRemaining}`);
  trades.push({ time: new Date(), sym, side: 'SELL', qty: qtyToSell, price, pnl, reason });
  await sendDiscordAlert('partial', sym, qtyToSell, price, pnl, reason);
  await syncTrade({ sym, side: 'SELL', qty: qtyToSell, price, pnl, reason });
  await syncLog('sell', `${icon} PARTIAL ${reason} ${qtyToSell}x ${sym} @ $${price.toFixed(2)} P&L=${pnl>=0?'+':''}$${pnl.toFixed(2)}`);

  if (positions[sym].qtyRemaining <= 0) { delete positions[sym]; }
  await syncAll();
}

async function exitPosition(sym, price, reason) {
  const pos = positions[sym];
  if (!pos) return;
  const qtyToSell = pos.qtyRemaining || pos.qty;
  if (CONFIG.mode === 'alpaca' && CONFIG.alpacaKey) {
    try { await placeOrder(sym, qtyToSell, 'sell'); }
    catch (e) { log('error', `Sell failed ${sym}: ${e.message}`); return; }
  }
  const avgCost = pos.cost / qtyToSell;
  const pnl = qtyToSell * price - qtyToSell * avgCost;
  portfolio += qtyToSell * price;
  pnl > 0 ? totalWins++ : totalLosses++;
  delete positions[sym];
  alpacaPositions.delete(sym);

  trades.push({ time: new Date(), sym, side: 'SELL', qty: qtyToSell, price, pnl, reason });
  const icon = { STOP_LOSS:'🛑', BREAK_EVEN_STOP:'🔒', TAKE_PROFIT:'🎯', TRAILING_STOP:'📉', SIGNAL:'📤', TIME_STOP:'⏰', VOL_SQUEEZE:'📊', RESISTANCE_EXIT:'🧱' }[reason] || '📤';
  log('sell', `${icon} SELL ${qtyToSell}x ${sym} @ $${price.toFixed(2)} | P&L: ${pnl>=0?'+':''}$${pnl.toFixed(2)} (${reason})`);
  await sendDiscordAlert('sell', sym, qtyToSell, price, pnl, reason);
  await syncTrade({ sym, side: 'SELL', qty: qtyToSell, price, pnl, reason });
  await syncAll();
  await syncLog('sell', `${icon} SELL ${qtyToSell}x ${sym} @ $${price.toFixed(2)} P&L=${pnl>=0?'+':''}$${pnl.toFixed(2)} (${reason})`);
}

// ─────────────────────────────────────────────
// SUPPORT / RESISTANCE
// ─────────────────────────────────────────────
function calcSRLevels(bars) {
  if (!bars || bars.length < 10) return [];
  const levels = [];
  for (let i = 2; i < bars.length - 2; i++) {
    const b = bars[i];
    if (b.h > bars[i-1].h && b.h > bars[i-2].h && b.h > bars[i+1].h && b.h > bars[i+2].h)
      levels.push({ price: b.h, type: 'resistance' });
    if (b.l < bars[i-1].l && b.l < bars[i-2].l && b.l < bars[i+1].l && b.l < bars[i+2].l)
      levels.push({ price: b.l, type: 'support' });
  }
  return levels.slice(-10);
}

function nearResistance(price, srLevels) {
  if (!srLevels || !srLevels.length) return false;
  return srLevels.some(l => l.type === 'resistance' && l.price > price && Math.abs((l.price - price) / price) < 0.003);
}

// ─────────────────────────────────────────────
// ADVANCED POSITION MANAGEMENT
// ─────────────────────────────────────────────
async function managePosition(sym, price, bars) {
  const pos = positions[sym];
  if (!pos) return;

  const chg      = (price - pos.entryPrice) / pos.entryPrice;
  const holdMins = (Date.now() - new Date(pos.entryTime).getTime()) / 60000;

  // Update high water mark
  if (price > pos.highWater) positions[sym].highWater = price;

  // ── 1. Ratchet ATR stop upward as price rises ──
  if (bars && bars.length >= 14) {
    const curAtr = atr(bars, 14);
    const newStop = price - (curAtr * CONFIG.atrStopMult);
    if (newStop > positions[sym].stopPrice) {
      positions[sym].stopPrice = newStop;
      log('risk', `${sym} ATR stop ratcheted → $${newStop.toFixed(2)}`);
    }
  }

  // ── 2. Hard stop loss ──
  if (price <= pos.stopPrice) {
    const reason = pos.breakEvenSet ? 'BREAK_EVEN_STOP' : 'STOP_LOSS';
    log('risk', `${sym} stop hit @ $${price.toFixed(2)} (stop=$${pos.stopPrice.toFixed(2)}) → ${reason}`);
    return exitPosition(sym, price, reason);
  }

  // ── 3. Break-even: move SL to entry once up breakEvenAt% ──
  if (!pos.breakEvenSet && chg >= CONFIG.breakEvenAt) {
    positions[sym].stopPrice   = pos.entryPrice;
    positions[sym].breakEvenSet = true;
    log('risk', `🔒 Break-even set: ${sym} SL → $${pos.entryPrice.toFixed(2)} (can't lose now)`);
    await syncLog('sys', `🔒 Break-even locked for ${sym} @ $${pos.entryPrice.toFixed(2)}`);
    await sendDiscordAlert('breakeven', sym, pos.qtyRemaining, price, undefined, 'BREAK_EVEN_SET');
  }

  // ── 4. TP1 — sell 33% at tp1Pct ──
  if (!pos.tp1Hit && chg >= CONFIG.tp1Pct) {
    const sell = Math.max(1, Math.floor((pos.qtyRemaining) * 0.33));
    positions[sym].tp1Hit = true;
    log('sell', `🎯 TP1 +${(chg*100).toFixed(1)}%: selling ${sell}x ${sym} @ $${price.toFixed(2)}`);
    await partialExit(sym, price, sell, 'TP1');
    if (!positions[sym]) return;
    positions[sym].stopPrice = Math.max(pos.stopPrice, pos.entryPrice); // lock to BE after TP1
    return;
  }

  // ── 5. TP2 — sell 50% of remainder (≈33% of original) at tp2Pct ──
  if (pos.tp1Hit && !pos.tp2Hit && chg >= CONFIG.tp2Pct) {
    const sell = Math.max(1, Math.floor(positions[sym].qtyRemaining * 0.5));
    positions[sym].tp2Hit = true;
    log('sell', `🎯🎯 TP2 +${(chg*100).toFixed(1)}%: selling ${sell}x ${sym} @ $${price.toFixed(2)}`);
    await partialExit(sym, price, sell, 'TP2');
    if (!positions[sym]) return;
    positions[sym].stopPrice = Math.max(positions[sym].stopPrice, price * 0.98); // tight 2% trail on runner
    return;
  }

  // ── 6. TP3 — exit final runner at tp3Pct ──
  if (pos.tp1Hit && pos.tp2Hit && chg >= CONFIG.tp3Pct) {
    log('sell', `🎯🎯🎯 TP3 +${(chg*100).toFixed(1)}%: final exit ${sym} @ $${price.toFixed(2)}`);
    return exitPosition(sym, price, 'TAKE_PROFIT');
  }

  // ── 7. Trailing stop on runner after partial exits ──
  if (CONFIG.trailingStop && (pos.tp1Hit || pos.tp2Hit)) {
    const trailPct = pos.tp2Hit ? 0.02 : CONFIG.trailingStopPct;
    if ((pos.highWater - price) / pos.highWater >= trailPct) {
      log('risk', `📉 Trail stop on runner: ${sym} hw=$${pos.highWater.toFixed(2)} → $${price.toFixed(2)}`);
      return exitPosition(sym, price, 'TRAILING_STOP');
    }
  }

  // ── 8. Standard trailing stop (no TPs hit yet) ──
  if (CONFIG.trailingStop && !pos.tp1Hit && pos.highWater > pos.entryPrice * (1 + CONFIG.breakEvenAt)) {
    if ((pos.highWater - price) / pos.highWater >= CONFIG.trailingStopPct) {
      log('risk', `📉 Trail stop: ${sym} hw=$${pos.highWater.toFixed(2)} → $${price.toFixed(2)}`);
      return exitPosition(sym, price, 'TRAILING_STOP');
    }
  }

  // ── 9. Resistance exit ──
  if (nearResistance(price, pos.srLevels) && chg > 0) {
    log('sell', `🧱 Resistance exit: ${sym} approaching resistance @ $${price.toFixed(2)}`);
    return exitPosition(sym, price, 'RESISTANCE_EXIT');
  }

  // ── 10. Time stop — exit dead money ──
  if (holdMins >= CONFIG.timeStopHours * 60 && Math.abs(chg) < CONFIG.timeStopMinPct) {
    log('sell', `⏰ Time stop: ${sym} held ${(holdMins/60).toFixed(1)}h, only ${(chg*100).toFixed(2)}% move`);
    return exitPosition(sym, price, 'TIME_STOP');
  }

  // ── 11. Volatility squeeze — momentum dead ──
  if (bars && bars.length >= 14 && pos.atrAtEntry > 0) {
    const curAtr  = atr(bars, 14);
    const atrDrop = (pos.atrAtEntry - curAtr) / pos.atrAtEntry;
    if (atrDrop >= CONFIG.volSqueezePct && chg > 0.01) {
      log('sell', `📊 Vol squeeze: ${sym} ATR fell ${(atrDrop*100).toFixed(0)}% — locking profit`);
      return exitPosition(sym, price, 'VOL_SQUEEZE');
    }
  }

  // Log status
  const tpNext = !pos.tp1Hit ? `TP1@+${(CONFIG.tp1Pct*100).toFixed(0)}%` : !pos.tp2Hit ? `TP2@+${(CONFIG.tp2Pct*100).toFixed(0)}%` : `TP3@+${(CONFIG.tp3Pct*100).toFixed(0)}%`;
  const stopDist = ((price - pos.stopPrice) / price * 100).toFixed(2);
  log('pos', `${sym} ${chg>=0?'+':''}${(chg*100).toFixed(2)}% | SL=$${pos.stopPrice.toFixed(2)}(${stopDist}% away) | ${tpNext} | BE:${pos.breakEvenSet?'✅':'❌'} TP1:${pos.tp1Hit?'✅':'❌'} TP2:${pos.tp2Hit?'✅':'❌'}`);
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
  await syncLog('sys', `Scan started — ${session} — ${CONFIG.symbols.length} symbols`);

  // Reload settings from dashboard on every scan — no Render redeploy needed
  await loadRemoteConfig();

  // Always sync live Alpaca positions first — prevents duplicate buys after restarts
  await syncAlpacaPositions();

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

      // Manage open long position
      if (positions[sym]) {
        await managePosition(sym, price, bars5m);
        if (positions[sym]) {
          const pct = ((price - positions[sym].entryPrice) / positions[sym].entryPrice * 100).toFixed(2);
          log('pos', `LONG ${positions[sym].qtyRemaining||positions[sym].qty}x ${sym} @ $${positions[sym].entryPrice.toFixed(2)} → $${price.toFixed(2)} (${pct}%) ${sessionLabel}`);
        }
        continue;
      }

      // Manage open short position
      if (shortPositions[sym]) {
        await manageShort(sym, price, bars5m);
        if (shortPositions[sym]) {
          const chg = ((shortPositions[sym].entryPrice - price) / shortPositions[sym].entryPrice * 100).toFixed(2);
          log('pos', `SHORT ${shortPositions[sym].qtyRemaining}x ${sym} @ $${shortPositions[sym].entryPrice.toFixed(2)} → $${price.toFixed(2)} (${chg}%)`);
        }
        continue;
      }

      // Generate signal and apply session multiplier to confidence
      const sig = generateSignal(sym, bars5m, bars15m);
      const adjustedConfidence = Math.round(sig.confidence * sessionMult);
      const adjustedSig = { ...sig, confidence: adjustedConfidence };

      if (sessionMult < 1.0) adjustedSig.reasons = [`Off-prime (x${sessionMult})`, ...sig.reasons];
      else if (sessionMult > 1.0) adjustedSig.reasons = [`Prime boost (x${sessionMult})`, ...sig.reasons];

      log('signal', `${sym} @ $${price.toFixed(2)} → ${sig.signal} (conf:${adjustedConfidence}% RSI:${sig.rsi?.toFixed(1)}) ${sessionLabel}`);
      await syncLog('info', `${sym} @ $${price.toFixed(2)} → ${sig.signal} conf:${adjustedConfidence}% RSI:${sig.rsi?.toFixed(1)} ${sessionLabel}`);

      const totalOpen = Object.keys(positions).length + Object.keys(shortPositions).length;

      // BUY long
      if (adjustedSig.signal === 'BUY' && marketOk && totalOpen < CONFIG.maxOpenPositions) {
        if (!positions[sym] && !alpacaPositions.has(sym)) {
          await enterPosition(sym, price, adjustedSig, bars5m, 'long');
        }
      }

      // SHORT — only when market is bearish or stock is overbought+downtrending
      if (adjustedSig.signal === 'SELL' && SHORTING_ENABLED && totalOpen < CONFIG.maxOpenPositions) {
        if (!shortPositions[sym] && !alpacaShorts.has(sym)) {
          await enterPosition(sym, price, adjustedSig, bars5m, 'short');
        }
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
  await syncLog('sys', `Scan complete | Portfolio=$${total.toFixed(2)} DayP&L=${dayPnl >= 0 ? '+' : ''}$${dayPnl.toFixed(2)} | Open:${Object.keys(positions).length} W:${totalWins}/L:${totalLosses}`);
  await syncAll();
}

// Live Alpaca positions — refreshed every scan to prevent duplicate buys
let alpacaPositions = new Set();
let alpacaShorts    = new Set();
const SHORTING_ENABLED = process.env.ENABLE_SHORTS !== 'false'; // default on

async function syncAlpacaPositions() {
  if (!CONFIG.alpacaKey) return;
  try {
    const data = await alpacaFetch(`${ALPACA_BASE()}/v2/positions`);
    if (!Array.isArray(data)) return;

    const liveSymbols = new Set(data.map(p => p.symbol));

    // ── Detect manual closes ──
    // If we have a position in memory but it's gone from Alpaca → manually closed
    for (const sym of Object.keys(positions)) {
      if (!liveSymbols.has(sym)) {
        const pos = positions[sym];
        // Get the last known price
        const lastPrice = priceHistory5m[sym]?.[priceHistory5m[sym].length - 1] || pos.entryPrice;
        const qty       = pos.qtyRemaining || pos.qty;
        const pnl       = (lastPrice - pos.entryPrice) * qty;

        log('sys', `🖐 Manual close detected: ${sym} | P&L: ${pnl>=0?'+':''}$${pnl.toFixed(2)}`);

        // Update stats
        pnl > 0 ? totalWins++ : totalLosses++;
        portfolio += qty * lastPrice;

        // Record as trade
        trades.push({ time: new Date(), sym, side: 'SELL', qty, price: lastPrice, pnl, reason: 'MANUAL_CLOSE' });

        // Remove from memory and Supabase
        delete positions[sym];
        alpacaPositions.delete(sym);
        await sbFetch(`tc_positions?symbol=eq.${sym}`, 'DELETE');

        // Log and alert
        await syncTrade({ sym, side: 'SELL', qty, price: lastPrice, pnl, reason: 'MANUAL_CLOSE' });
        await syncLog('warn', `🖐 Manual close: ${sym} ${qty}x @ ~$${lastPrice.toFixed(2)} | P&L: ${pnl>=0?'+':''}$${pnl.toFixed(2)}`);
        await sendDiscordAlert('manual_close', sym, qty, lastPrice, pnl, 'MANUAL_CLOSE');
        await syncPortfolio();
      }
    }

    // ── Update live set ──
    alpacaPositions = liveSymbols;

    // ── Restore positions after restart ──
    for (const p of data) {
      if (!positions[p.symbol]) {
        positions[p.symbol] = {
          entryPrice:   +p.avg_entry_price,
          qty:          +p.qty,
          qtyRemaining: +p.qty,
          cost:         +p.avg_entry_price * +p.qty,
          entryTime:    new Date(),
          highWater:    +p.current_price,
          atrAtEntry:   0,
          stopPrice:    +p.avg_entry_price * (1 - CONFIG.stopLossPct),
          breakEvenSet: false,
          tp1Hit:       false,
          tp2Hit:       false,
          srLevels:     [],
          sigInfo:      { confidence: 0, reasons: ['Restored from Alpaca on restart'] },
        };
        log('sys', `Restored: ${p.symbol} ${p.qty}x @ $${p.avg_entry_price}`);
        await syncLog('sys', `Restored ${p.symbol} from Alpaca after restart`);
        // Make sure it exists in Supabase positions table
        await syncPositions();
      }
    }

    if (liveSymbols.size > 0) {
      log('acct', `Alpaca positions: ${[...liveSymbols].join(', ')}`);
    }
  } catch (e) {
    log('error', `syncAlpacaPositions: ${e.message}`);
  }
}

async function storePrevClose() {
  for (const sym of CONFIG.symbols) {
    try {
      const bars = await fetchBars(sym, '1Day', 2);
      if (bars?.length >= 1) prevDayClose[sym] = bars[bars.length - 1].c;
    } catch (e) {}
  }
}

// Sync live prices every 60s using Alpaca positions API
async function syncPricesOnly() {
  if (!CONFIG.alpacaKey) return;
  try {
    const alpacaPos = await alpacaFetch(`${ALPACA_BASE()}/v2/positions`);
    if (!Array.isArray(alpacaPos) || alpacaPos.length === 0) return;

    for (const ap of alpacaPos) {
      const sym    = ap.symbol;
      const cur    = +ap.current_price;
      const pnl    = +ap.unrealized_pl;
      const pnlPct = +ap.unrealized_plpc * 100;

      // Update in-memory
      if (!priceHistory5m[sym]) priceHistory5m[sym] = [];
      priceHistory5m[sym].push(cur);
      if (priceHistory5m[sym].length > 60) priceHistory5m[sym].shift();
      if (positions[sym] && cur > positions[sym].highWater) positions[sym].highWater = cur;

      // Push to Supabase
      await sbFetch(`tc_positions?symbol=eq.${sym}`, 'PATCH', {
        current_price: +cur.toFixed(4),
        pnl:           +pnl.toFixed(2),
        pnl_pct:       +pnlPct.toFixed(4),
        updated_at:    new Date().toISOString(),
      });
      log('price', `${sym} $${cur.toFixed(2)} | P&L: ${pnl>=0?'+':''}$${pnl.toFixed(2)} (${pnlPct.toFixed(2)}%)`);
    }

    // Update cash from Alpaca account
    const acct = await getAccount();
    if (acct?.cash) {
      await sbFetch('tc_portfolio?id=eq.1', 'PATCH', {
        cash:       +parseFloat(acct.cash).toFixed(2),
        updated_at: new Date().toISOString(),
      });
    }
  } catch (e) {
    log('error', `syncPricesOnly: ${e.message}`);
  }
}

// ─────────────────────────────────────────────
// DISCORD ALERTS
// ─────────────────────────────────────────────
async function sendDiscordAlert(type, sym, qty, price, pnl, reason, sigInfo, extra) {
  if (!CONFIG.discordWebhook) return;
  const { default: fetch } = await import('node-fetch');

  const colorMap = { buy:0x7fff6e, short:0xff5f57, cover:0xb47fff, partial:0x00e5ff, breakeven:0xffb547, circuit_breaker:0xff0000, sell:0x4da6ff, manual_close:0xb47fff };
  if (type==='sell'||type==='manual_close'){
    if (['STOP_LOSS','BREAK_EVEN_STOP'].includes(reason)) colorMap.sell=0xff5f57;
    else if (reason==='TAKE_PROFIT') colorMap.sell=0x00e5ff;
    else if (reason==='TRAILING_STOP') colorMap.sell=0xffb547;
  }

  const iconMap = {
    buy:'🟢', short:'🔴', cover:'🔵', breakeven:'🔒', circuit_breaker:'🔴', manual_close:'🖐',
    partial:{TP1:'🎯',TP2:'🎯🎯',TP3:'🎯🎯🎯'}[reason]||'🎯',
    sell:{STOP_LOSS:'🛑',BREAK_EVEN_STOP:'🔒',TAKE_PROFIT:'🎯',TRAILING_STOP:'📉',
          TIME_STOP:'⏰',VOL_SQUEEZE:'📊',RESISTANCE_EXIT:'🧱',SIGNAL:'🔵'}[reason]||'🔵',
  };
  const titleMap = {
    buy:'LONG Position Entered', short:'SHORT Position Entered 🔴',
    cover:'Short Covered', breakeven:'Break-Even Stop Set 🔒',
    circuit_breaker:'⛔ CIRCUIT BREAKER', partial:`Partial Exit — ${reason}`,
    manual_close:'🖐 Position Manually Closed',
    sell:{STOP_LOSS:'Stop Loss Hit',BREAK_EVEN_STOP:'Break-Even Stop Hit 🔒',
          TAKE_PROFIT:'Take Profit ✅',TRAILING_STOP:'Trailing Stop',
          TIME_STOP:'⏰ Time Stop',VOL_SQUEEZE:'📊 Vol Squeeze Exit',
          RESISTANCE_EXIT:'🧱 Resistance Exit',SIGNAL:'SELL Signal'}[reason]||'Exit',
  };

  const fields = [
    sym!=='ALL' ? {name:'Symbol',value:sym,inline:true} : null,
    qty>0       ? {name:'Shares',value:String(qty),inline:true} : null,
    price>0     ? {name:'Price',value:`$${price.toFixed(2)}`,inline:true} : null,
    pnl!==undefined ? {name:'P&L',value:`${pnl>=0?'+':''}$${pnl.toFixed(2)}`,inline:true} : null,
    extra?.stopPrice ? {name:'Stop Loss',value:`$${extra.stopPrice.toFixed(2)}`,inline:true} : null,
    extra?.atrVal    ? {name:'ATR',value:extra.atrVal.toFixed(2),inline:true} : null,
    {name:'Portfolio',value:`$${portfolio.toFixed(2)}`,inline:true},
    {name:'W / L',value:`${totalWins} / ${totalLosses}`,inline:true},
    {name:'Mode',value:CONFIG.alpacaPaper?'📄 Paper':'💰 LIVE',inline:true},
    type==='manual_close' ? {name:'Source',value:'Closed manually in Alpaca/TradingView',inline:false} : null,
  ].filter(Boolean);

  if (type==='buy' && sigInfo?.reasons?.length) {
    fields.push({name:`Confidence: ${sigInfo.confidence}%`,value:sigInfo.reasons.slice(0,5).join('\n'),inline:false});
  }

  try {
    await fetch(CONFIG.discordWebhook, {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({embeds:[{
        title:`${iconMap[type]||'📤'} TradeCore Pro — ${titleMap[type]||type}`,
        color: colorMap[type]||0x4da6ff,
        fields,
        footer:{text:`TradeCore Pro | ${CONFIG.strategy.toUpperCase()} | ${CONFIG.alpacaPaper?'Paper':'Live'}`},
        timestamp: new Date().toISOString(),
      }]}),
    });
  } catch(e){ log('error',`Discord failed: ${e.message}`); }
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
log('sys', '   TradeCore Pro — Upgraded Engine v4     ');
log('sys', '   + 10s scan + live dashboard refresh    ');
log('sys', '══════════════════════════════════════════');
log('sys', `Mode: ${CONFIG.mode.toUpperCase()} | Paper: ${CONFIG.alpacaPaper} | Strategy: ${CONFIG.strategy}`);
if (BYPASS_HOURS) log('sys', '⚠️  BYPASS_HOURS=true — trading outside market hours (TEST MODE)');
log('sys', `Symbols: ${CONFIG.symbols.join(', ')}`);
log('sys', `Risk: SL=${CONFIG.stopLossPct*100}% TP=${CONFIG.takeProfitPct*100}% Trailing=${CONFIG.trailingStop} MaxDailyLoss=${CONFIG.maxDailyLossPct*100}%`);
log('sys', `Filters: Trend=${CONFIG.trendFilter} Volume=${CONFIG.volumeFilter} Regime=${CONFIG.regimeFilter} Corr=${CONFIG.correlationFilter}`);
log('sys', `Current session: ${getCurrentSession()}`);

// ── Scan interval ──
// Use setInterval so we can do sub-minute scans (cron only supports 1min minimum)
// Rate limit guard: Alpaca free tier = ~200 req/min
// Each scan = ~2 requests per symbol (5min bars + 15min bars) + ~3 account calls
// 15 symbols × 2 = 30 req + 3 = 33 req per scan
// At 10s intervals = 6 scans/min = 198 req/min — right at the limit
// So we stagger: full signal scan every 30s, price-only update every 10s
let scanInProgress = false;
let lastFullScan = 0;
const FULL_SCAN_INTERVAL_MS = Math.max(
  +(process.env.SCAN_INTERVAL_SEC || 30) * 1000,
  10000 // never faster than 10s
);
const PRICE_SYNC_INTERVAL_MS = 10000; // price updates every 10s

log('sys', `Full scan every ${FULL_SCAN_INTERVAL_MS/1000}s | Price sync every ${PRICE_SYNC_INTERVAL_MS/1000}s`);

async function tick() {
  if (scanInProgress) return; // prevent overlapping scans
  scanInProgress = true;
  try {
    const now = Date.now();
    if (now - lastFullScan >= FULL_SCAN_INTERVAL_MS) {
      // Full signal scan
      lastFullScan = now;
      await runScan();
    } else {
      // Price-only update between full scans
      await syncPricesOnly();
    }
  } catch(e) {
    log('error', `Tick error: ${e.message}`);
  } finally {
    scanInProgress = false;
  }
}

// Main tick every 10 seconds
setInterval(tick, PRICE_SYNC_INTERVAL_MS);

// Daily tasks (cron still handles these fine)
cron.schedule('5 16 * * 1-5', sendDailySummary, { timezone: 'America/New_York' });
cron.schedule('55 8 * * 1-5', storePrevClose,   { timezone: 'America/New_York' });

// Startup
loadRemoteConfig().then(() => {
  runScan();
  lastFullScan = Date.now();
});
setTimeout(syncPricesOnly, 5000);

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
