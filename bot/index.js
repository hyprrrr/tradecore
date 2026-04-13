/**
 * TradeCore Bot Backend
 * Deploy to Railway.app or Render.com (free tier) for 24/7 operation
 * 
 * FREE DATA SOURCES:
 *  - Yahoo Finance (stocks, paper)
 *  - CoinGecko (crypto, paper)
 *  - Alpaca Markets API (stocks, paper + live, free account)
 *
 * SETUP:
 *  npm install node-fetch node-cron
 *  Set env vars: DISCORD_WEBHOOK, MODE (paper|alpaca), ALPACA_KEY, ALPACA_SECRET
 */

const cron = require('node-cron');

// ─────────────────────────────────────────────
// CONFIG (override via environment variables)
// ─────────────────────────────────────────────
const CONFIG = {
  mode: process.env.MODE || 'paper',           // 'paper' | 'alpaca'
  discordWebhook: process.env.DISCORD_WEBHOOK || '',
  alpacaKey: process.env.ALPACA_KEY || '',
  alpacaSecret: process.env.ALPACA_SECRET || '',
  alpacaPaper: process.env.ALPACA_PAPER !== 'false', // default true = paper
  
  strategy: process.env.STRATEGY || 'rsi_macd',
  symbols: (process.env.SYMBOLS || 'AAPL,TSLA,GOOGL,MSFT,SPY').split(','),
  cryptoSymbols: (process.env.CRYPTO_SYMBOLS || 'bitcoin,ethereum,solana').split(','),
  
  startingCapital: +(process.env.CAPITAL || 10000),
  maxPositionPct: +(process.env.MAX_POSITION_PCT || 10) / 100,
  maxOpenPositions: +(process.env.MAX_POSITIONS || 3),
  stopLossPct: +(process.env.STOP_LOSS_PCT || 5) / 100,
  takeProfitPct: +(process.env.TAKE_PROFIT_PCT || 10) / 100,
  
  rsiPeriod: +(process.env.RSI_PERIOD || 14),
  rsiOversold: +(process.env.RSI_OVERSOLD || 30),
  rsiOverbought: +(process.env.RSI_OVERBOUGHT || 70),
  
  scanIntervalMin: +(process.env.SCAN_INTERVAL_MIN || 5),
};

// ─────────────────────────────────────────────
// STATE
// ─────────────────────────────────────────────
let portfolio = CONFIG.startingCapital;
let positions = {};
let priceHistory = {};
let trades = [];
let totalWins = 0, totalLosses = 0;

// ─────────────────────────────────────────────
// LOGGING
// ─────────────────────────────────────────────
function log(type, msg) {
  const ts = new Date().toISOString();
  console.log(`[${ts}] [${type.toUpperCase()}] ${msg}`);
}

// ─────────────────────────────────────────────
// DATA FETCHING
// ─────────────────────────────────────────────

/**
 * Fetch stock prices from Yahoo Finance (free, no API key)
 * NOTE: Yahoo Finance is unofficial — use Alpaca's free data for production
 */
async function fetchStockPrice(symbol) {
  try {
    const { default: fetch } = await import('node-fetch');
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${symbol}?interval=5m&range=1d`;
    const res = await fetch(url, {
      headers: { 'User-Agent': 'Mozilla/5.0' }
    });
    const data = await res.json();
    const closes = data?.chart?.result?.[0]?.indicators?.quote?.[0]?.close;
    if (!closes) throw new Error('No data');
    const filtered = closes.filter(Boolean);
    return {
      symbol,
      price: filtered[filtered.length - 1],
      history: filtered.slice(-30),
    };
  } catch (e) {
    log('error', `Failed to fetch ${symbol}: ${e.message}`);
    return null;
  }
}

/**
 * Fetch crypto prices from CoinGecko (free, no API key, 30 req/min)
 */
async function fetchCryptoPrice(coinId) {
  try {
    const { default: fetch } = await import('node-fetch');
    const url = `https://api.coingecko.com/api/v3/coins/${coinId}/market_chart?vs_currency=usd&days=1&interval=5minute`;
    const res = await fetch(url);
    const data = await res.json();
    const prices = data.prices?.map(p => p[1]) || [];
    const symbolMap = { bitcoin: 'BTC-USD', ethereum: 'ETH-USD', solana: 'SOL-USD', binancecoin: 'BNB-USD' };
    return {
      symbol: symbolMap[coinId] || coinId.toUpperCase(),
      price: prices[prices.length - 1],
      history: prices.slice(-30),
    };
  } catch (e) {
    log('error', `Failed to fetch crypto ${coinId}: ${e.message}`);
    return null;
  }
}

/**
 * Alpaca — place real or paper orders (free API, commission-free)
 * Sign up at https://alpaca.markets (free)
 */
async function alpacaOrder(symbol, qty, side) {
  const { default: fetch } = await import('node-fetch');
  const base = CONFIG.alpacaPaper
    ? 'https://paper-api.alpaca.markets'
    : 'https://api.alpaca.markets';
  
  const res = await fetch(`${base}/v2/orders`, {
    method: 'POST',
    headers: {
      'APCA-API-KEY-ID': CONFIG.alpacaKey,
      'APCA-API-SECRET-KEY': CONFIG.alpacaSecret,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      symbol,
      qty,
      side,
      type: 'market',
      time_in_force: 'gtc',
    }),
  });
  const order = await res.json();
  if (order.id) {
    log('trade', `Alpaca ${side} order placed: ${qty}x ${symbol} | Order ID: ${order.id}`);
    return order;
  } else {
    throw new Error(order.message || JSON.stringify(order));
  }
}

// ─────────────────────────────────────────────
// INDICATORS
// ─────────────────────────────────────────────
function calcRSI(prices, period) {
  if (prices.length < period + 1) return 50;
  let gains = 0, losses = 0;
  for (let i = prices.length - period; i < prices.length; i++) {
    const d = prices[i] - prices[i - 1];
    if (d > 0) gains += d; else losses -= d;
  }
  const rs = gains / (losses || 0.0001);
  return 100 - 100 / (1 + rs);
}

function calcEMA(prices, period) {
  if (prices.length === 0) return 0;
  const k = 2 / (period + 1);
  let ema = prices[0];
  for (let i = 1; i < prices.length; i++) ema = prices[i] * k + ema * (1 - k);
  return ema;
}

function calcMACD(prices) {
  return {
    fast: calcEMA(prices, 8),
    slow: calcEMA(prices, 21),
    bullish: calcEMA(prices, 8) > calcEMA(prices, 21),
  };
}

function generateSignal(sym, prices) {
  if (prices.length < 15) return 'HOLD';
  
  const rsi = calcRSI(prices, CONFIG.rsiPeriod);
  const macd = calcMACD(prices);
  
  switch (CONFIG.strategy) {
    case 'rsi_macd':
      if (rsi < CONFIG.rsiOversold && macd.bullish) return 'BUY';
      if (rsi > CONFIG.rsiOverbought && !macd.bullish) return 'SELL';
      break;
    case 'ema_cross': {
      const ema8 = calcEMA(prices, 8);
      const ema21 = calcEMA(prices, 21);
      const prev8 = calcEMA(prices.slice(0, -1), 8);
      const prev21 = calcEMA(prices.slice(0, -1), 21);
      if (prev8 < prev21 && ema8 > ema21) return 'BUY';
      if (prev8 > prev21 && ema8 < ema21) return 'SELL';
      break;
    }
    case 'momentum': {
      const returns = prices.map((p, i) => i > 0 ? (p - prices[i-1]) / prices[i-1] : 0).slice(-10);
      const momentum = returns.reduce((a, b) => a + b, 0);
      if (momentum > 0.005 && rsi < 65) return 'BUY';
      if (momentum < -0.005 && rsi > 35) return 'SELL';
      break;
    }
    case 'mean_reversion': {
      const mean = prices.reduce((a, b) => a + b, 0) / prices.length;
      const latest = prices[prices.length - 1];
      const deviation = (latest - mean) / mean;
      if (deviation < -0.02 && rsi < 40) return 'BUY';
      if (deviation > 0.02 && rsi > 60) return 'SELL';
      break;
    }
  }
  return 'HOLD';
}

// ─────────────────────────────────────────────
// TRADE EXECUTION
// ─────────────────────────────────────────────
async function enterPosition(sym, price) {
  const posSize = portfolio * CONFIG.maxPositionPct;
  const qty = Math.floor(posSize / price);
  if (qty < 1) { log('warn', `Insufficient capital for ${sym}`); return; }
  const cost = qty * price;

  if (CONFIG.mode === 'alpaca') {
    try {
      await alpacaOrder(sym, qty, 'buy');
    } catch (e) {
      log('error', `Alpaca BUY failed for ${sym}: ${e.message}`);
      return;
    }
  }

  portfolio -= cost;
  positions[sym] = { entryPrice: price, qty, cost, entryTime: new Date() };
  
  const trade = { time: new Date(), sym, side: 'BUY', qty, price, pnl: null, reason: 'SIGNAL' };
  trades.push(trade);
  
  log('buy', `BUY ${qty}x ${sym} @ $${price.toFixed(2)} | Capital remaining: $${portfolio.toFixed(2)}`);
  await sendDiscordAlert('buy', sym, qty, price);
}

async function exitPosition(sym, price, reason) {
  const pos = positions[sym];
  if (!pos) return;
  
  if (CONFIG.mode === 'alpaca') {
    try {
      await alpacaOrder(sym, pos.qty, 'sell');
    } catch (e) {
      log('error', `Alpaca SELL failed for ${sym}: ${e.message}`);
      return;
    }
  }
  
  const proceeds = pos.qty * price;
  const pnl = proceeds - pos.cost;
  portfolio += proceeds;
  
  if (pnl > 0) totalWins++; else totalLosses++;
  delete positions[sym];
  
  const trade = { time: new Date(), sym, side: 'SELL', qty: pos.qty, price, pnl, reason };
  trades.push(trade);
  
  const icon = reason === 'STOP_LOSS' ? '🛑' : reason === 'TAKE_PROFIT' ? '🎯' : '📤';
  log('sell', `${icon} SELL ${pos.qty}x ${sym} @ $${price.toFixed(2)} | P&L: ${pnl >= 0 ? '+' : ''}$${pnl.toFixed(2)} (${reason})`);
  await sendDiscordAlert('sell', sym, pos.qty, price, pnl, reason);
}

// ─────────────────────────────────────────────
// MAIN SCAN
// ─────────────────────────────────────────────
async function runScan() {
  log('scan', `Starting scan — ${CONFIG.symbols.length} symbols`);

  const allSymbols = [...CONFIG.symbols];
  const priceData = await Promise.all(allSymbols.map(sym => fetchStockPrice(sym)));

  for (const data of priceData) {
    if (!data) continue;
    const { symbol, price, history } = data;
    
    if (!priceHistory[symbol]) priceHistory[symbol] = [];
    priceHistory[symbol] = [...priceHistory[symbol], ...history].slice(-50);
    
    const signal = generateSignal(symbol, priceHistory[symbol]);
    const openCount = Object.keys(positions).length;
    
    log('signal', `${symbol} @ $${price.toFixed(2)} → ${signal} (RSI: ${calcRSI(priceHistory[symbol], CONFIG.rsiPeriod).toFixed(1)})`);

    if (signal === 'BUY' && !positions[symbol] && openCount < CONFIG.maxOpenPositions) {
      await enterPosition(symbol, price);
    } else if (signal === 'SELL' && positions[symbol]) {
      await exitPosition(symbol, price, 'SIGNAL');
    }
    
    // SL / TP checks
    if (positions[symbol]) {
      const entry = positions[symbol].entryPrice;
      const change = (price - entry) / entry;
      if (change <= -CONFIG.stopLossPct) await exitPosition(symbol, price, 'STOP_LOSS');
      else if (change >= CONFIG.takeProfitPct) await exitPosition(symbol, price, 'TAKE_PROFIT');
    }
  }

  // Portfolio summary
  const openVal = Object.values(positions).reduce((a, p) => {
    const cur = priceHistory[p.sym || Object.keys(positions).find(k => positions[k] === p)]?.[priceHistory[Object.keys(positions).find(k => positions[k] === p)]?.length - 1] || p.entryPrice;
    return a + p.qty * cur;
  }, 0);
  log('info', `Portfolio: $${(portfolio + openVal).toFixed(2)} | Open: ${Object.keys(positions).length} | W:${totalWins}/L:${totalLosses}`);
}

// ─────────────────────────────────────────────
// DISCORD ALERTS
// ─────────────────────────────────────────────
async function sendDiscordAlert(type, sym, qty, price, pnl, reason) {
  if (!CONFIG.discordWebhook) return;
  const { default: fetch } = await import('node-fetch');

  const colors = { buy: 0x7fff6e, sell: 0xff5f57 };
  const icons = { buy: '🟢', sell: reason === 'STOP_LOSS' ? '🛑' : reason === 'TAKE_PROFIT' ? '🎯' : '🔵' };
  const titles = { buy: 'BUY Signal', sell: reason === 'STOP_LOSS' ? 'Stop Loss Hit' : reason === 'TAKE_PROFIT' ? 'Take Profit Hit' : 'SELL Signal' };

  const embed = {
    embeds: [{
      title: `${icons[type]} TradeCore — ${titles[type]}`,
      color: colors[type],
      fields: [
        { name: 'Symbol', value: sym, inline: true },
        { name: 'Quantity', value: String(qty), inline: true },
        { name: 'Price', value: `$${price.toFixed(2)}`, inline: true },
        ...(pnl !== undefined ? [{ name: 'P&L', value: `${pnl >= 0 ? '+' : ''}$${pnl.toFixed(2)}`, inline: true }] : []),
        { name: 'Portfolio', value: `$${portfolio.toFixed(2)}`, inline: true },
        { name: 'Mode', value: CONFIG.mode === 'alpaca' ? (CONFIG.alpacaPaper ? 'Alpaca Paper' : '⚠ LIVE') : 'Paper', inline: true },
      ],
      footer: { text: `TradeCore Bot | Strategy: ${CONFIG.strategy}` },
      timestamp: new Date().toISOString(),
    }]
  };

  try {
    await fetch(CONFIG.discordWebhook, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(embed),
    });
  } catch (e) {
    log('error', `Discord alert failed: ${e.message}`);
  }
}

async function sendDailySummary() {
  if (!CONFIG.discordWebhook) return;
  const { default: fetch } = await import('node-fetch');
  
  const totalTrades = trades.length;
  const totalPnl = trades.filter(t => t.pnl !== null).reduce((a, t) => a + t.pnl, 0);
  const winRate = (totalWins + totalLosses) > 0 ? ((totalWins / (totalWins + totalLosses)) * 100).toFixed(1) : 'N/A';
  
  await fetch(CONFIG.discordWebhook, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ embeds: [{
      title: '📊 TradeCore — Daily Summary',
      color: totalPnl >= 0 ? 0x7fff6e : 0xff5f57,
      fields: [
        { name: 'Portfolio Value', value: `$${portfolio.toFixed(2)}`, inline: true },
        { name: 'Total P&L', value: `${totalPnl >= 0 ? '+' : ''}$${totalPnl.toFixed(2)}`, inline: true },
        { name: 'Win Rate', value: `${winRate}%`, inline: true },
        { name: 'Total Trades', value: String(totalTrades), inline: true },
        { name: 'W / L', value: `${totalWins} / ${totalLosses}`, inline: true },
        { name: 'Open Positions', value: String(Object.keys(positions).length), inline: true },
      ],
      footer: { text: 'TradeCore Autonomous Bot' },
      timestamp: new Date().toISOString()
    }]}),
  }).catch(e => log('error', 'Daily summary Discord failed: ' + e.message));
}

// ─────────────────────────────────────────────
// SCHEDULER
// ─────────────────────────────────────────────
log('sys', `TradeCore starting | Mode: ${CONFIG.mode} | Strategy: ${CONFIG.strategy}`);
log('sys', `Symbols: ${CONFIG.symbols.join(', ')}`);
log('sys', `Scan interval: every ${CONFIG.scanIntervalMin} min`);

// Main scan cron
const scanCron = `*/${CONFIG.scanIntervalMin} * * * *`;
cron.schedule(scanCron, runScan);

// Daily summary at 9 PM
cron.schedule('0 21 * * *', sendDailySummary);

// Run immediately on startup
runScan();

// Health check endpoint (for Railway/Render)
const http = require('http');
http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    status: 'running',
    portfolio: portfolio.toFixed(2),
    openPositions: Object.keys(positions).length,
    wins: totalWins,
    losses: totalLosses,
    trades: trades.length,
    uptime: process.uptime(),
  }));
}).listen(process.env.PORT || 3000, () => {
  log('sys', `Health endpoint on port ${process.env.PORT || 3000}`);
});
