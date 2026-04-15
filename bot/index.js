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

// Cache node-fetch at startup — importing dynamically on every request adds ~5-20ms latency
let _fetch = null;
async function getFetch() {
  if (!_fetch) {
    const mod = await import('node-fetch');
    _fetch = mod.default;
  }
  return _fetch;
}

// ─────────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────────
const CONFIG = {
  alpacaKey:    process.env.ALPACA_KEY    || '',
  alpacaSecret: process.env.ALPACA_SECRET || '',
  alpacaPaper:  process.env.ALPACA_PAPER  !== 'false',
  // mode: 'alpaca' = real paper/live trading | 'sim' = simulation (no orders sent, instant fills)
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

  // ── Mode controls (overridden by dashboard via Supabase) ──
  swingEnabled:        process.env.SWING_ENABLED !== 'false', // default on
  scalpMode:           process.env.SCALP_MODE === 'true',
  scalpSymbols:        (process.env.SCALP_SYMBOLS || 'SPY,QQQ,AAPL,TSLA,NVDA').split(',').map(s => s.trim().toUpperCase()),
  scalpTpPct:          +(process.env.SCALP_TP_PCT        || 0.4)  / 100, // take profit at +0.4%
  scalpSlPct:          +(process.env.SCALP_SL_PCT        || 0.2)  / 100, // stop loss at -0.2%
  scalpMaxHoldMins:    +(process.env.SCALP_MAX_HOLD_MINS || 8),           // exit if held > 8 minutes
  scalpMaxPositions:   +(process.env.SCALP_MAX_POSITIONS || 2),           // max concurrent scalp positions
  scalpPositionPct:    +(process.env.SCALP_POSITION_PCT  || 20)  / 100,  // 20% of portfolio per scalp
  scalpMinScore:       +(process.env.SCALP_MIN_SCORE     || 70),          // higher confidence required
  scalpTrailingPct:    +(process.env.SCALP_TRAILING_PCT  || 0.15) / 100, // tight trailing stop 0.15%

  // Supabase
  supabaseUrl: process.env.SUPABASE_URL || '',
  supabaseKey: process.env.SUPABASE_ANON_KEY || '',

  // Discord slash command support
  discordAppId:    process.env.DISCORD_APP_ID    || '', // from Discord Developer Portal
  discordPublicKey:process.env.DISCORD_PUBLIC_KEY|| '', // from Discord Developer Portal
};

// ─────────────────────────────────────────────
// DYNAMIC CONFIG — loaded from Supabase on startup + every scan
// Secrets (API keys, webhook) stay in Render env vars only
// Everything else can be changed from the dashboard
// ─────────────────────────────────────────────
async function loadRemoteConfig() {
  if (!CONFIG.supabaseUrl || !CONFIG.supabaseKey) return;
  try {
    const fetch = await getFetch();
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

    // Mode flags — controlled from dashboard
    if (s.swing_enabled !== undefined) CONFIG.swingEnabled = !!s.swing_enabled;
    if (s.scalp_mode    !== undefined) CONFIG.scalpMode    = !!s.scalp_mode;
    if (s.sim_mode !== undefined) {
      const newMode = s.sim_mode ? 'sim' : (process.env.MODE || 'alpaca');
      if (newMode !== CONFIG.mode) {
        const entering = newMode === 'sim';
        CONFIG.mode = newMode;
        simState.loaded = false;

        if (!entering) {
          log('sim', '✅ Simulation mode DISABLED — restoring live account');
          await syncLog('sys', '✅ Exited sim mode — restoring live Alpaca account…');

          // 1. Clear all sim positions from memory
          Object.keys(positions).forEach(k => delete positions[k]);
          Object.keys(shortPositions).forEach(k => delete shortPositions[k]);
          Object.keys(scalpPositions).forEach(k => delete scalpPositions[k]);
          pendingSignals.clear();
          totalWins = 0; totalLosses = 0;

          // 2. Wipe sim positions from Supabase
          await sbFetch('tc_positions?symbol=neq.____NONE____', 'DELETE');

          // 3. Fetch real Alpaca account values RIGHT NOW
          let liveEquity = 0, liveCash = 0, liveLastEquity = 0;
          try {
            const acct = await getAccount();
            if (acct?.equity)      liveEquity      = +parseFloat(acct.equity).toFixed(2);
            if (acct?.cash)        liveCash        = +parseFloat(acct.cash).toFixed(2);
            if (acct?.last_equity) liveLastEquity  = +parseFloat(acct.last_equity).toFixed(2);
          } catch(e) { log('warn', `Could not fetch Alpaca account: ${e.message}`); }

          // Use live values if valid, otherwise keep startingCapital
          if (liveEquity > 0) {
            portfolio            = liveEquity;
            realEquity           = liveEquity;
            realDailyStartEquity = liveLastEquity > 0 ? liveLastEquity : liveEquity;
            const dayPnl         = liveLastEquity > 0 ? liveEquity - liveLastEquity : 0;

            // 4. Write live values directly to Supabase immediately
            await sbFetch('tc_portfolio?id=eq.1', 'PATCH', {
              cash:            +liveCash.toFixed(2),
              total_value:     +liveEquity.toFixed(2),
              day_pnl:         +dayPnl.toFixed(2),
              total_wins:      0,
              total_losses:    0,
              circuit_breaker: false,
              session:         getCurrentSession(),
              updated_at:      new Date().toISOString(),
            });
            log('sys', `✅ Live account restored: equity=$${liveEquity.toFixed(2)} dayPnl=${dayPnl>=0?'+':''}$${dayPnl.toFixed(2)}`);
          }

          // 5. Restore real open positions from Alpaca
          await syncAlpacaPositions();
          await syncPositions();

        } else {
          log('sim', '🎮 Simulation mode ENABLED — loading bar data on next scan');
        }
      }
    }

    // Scalp settings
    if (s.scalp_tp_pct)        CONFIG.scalpTpPct        = +s.scalp_tp_pct        / 100;
    if (s.scalp_sl_pct)        CONFIG.scalpSlPct        = +s.scalp_sl_pct        / 100;
    if (s.scalp_max_hold_mins) CONFIG.scalpMaxHoldMins  = +s.scalp_max_hold_mins;
    if (s.scalp_max_positions) CONFIG.scalpMaxPositions = +s.scalp_max_positions;
    if (s.scalp_position_pct)  CONFIG.scalpPositionPct  = +s.scalp_position_pct  / 100;
    if (s.scalp_min_score)     CONFIG.scalpMinScore     = +s.scalp_min_score;
    if (s.scalp_symbols)       CONFIG.scalpSymbols      = s.scalp_symbols.split(',').map(x => x.trim().toUpperCase());

    const modes = [];
    if (CONFIG.swingEnabled !== false) modes.push('Swing');
    if (CONFIG.scalpMode)              modes.push('Scalp⚡');
    log('sys', `Remote config loaded — Modes: ${modes.join('+')||'none'} | Symbols: ${CONFIG.symbols.length} | RSI: ${CONFIG.rsiOversold}/${CONFIG.rsiOverbought}`);
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
    const fetch = await getFetch();
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
  // In sim mode — use in-memory portfolio, no Alpaca account needed
  if (isSimMode()) {
    const openVal = Object.values(positions).reduce((a, p) => {
      const cur = priceHistory5m[p.sym || '']?.[priceHistory5m[p.sym||'']?.length - 1] || p.entryPrice;
      return a + (cur * (p.qtyRemaining || p.qty));
    }, 0);
    const equity  = portfolio + openVal;
    const dayPnl  = equity - (realDailyStartEquity || CONFIG.startingCapital);
    if (equity > 0) {
      realEquity = equity;
      if (!realDailyStartEquity) realDailyStartEquity = CONFIG.startingCapital;
    }
    await sbFetch('tc_portfolio?id=eq.1', 'PATCH', {
      cash:            +portfolio.toFixed(2),
      total_value:     +equity.toFixed(2),
      day_pnl:         +dayPnl.toFixed(2),
      total_wins:      totalWins,
      total_losses:    totalLosses,
      circuit_breaker: circuitBreakerOn,
      last_scan:       new Date().toISOString(),
      session:         getCurrentSession(),
      updated_at:      new Date().toISOString(),
    });
    if (equity > 0) await sbFetch('tc_equity', 'POST', { value: +equity.toFixed(2), created_at: new Date().toISOString() });
    return;
  }
  let cashValue   = 0;
  let equityValue = 0;
  let lastEquity  = 0;

  if (CONFIG.alpacaKey) {
    try {
      const acct = await getAccount();
      if (acct?.cash)        cashValue   = +parseFloat(acct.cash).toFixed(2);
      if (acct?.equity)      equityValue = +parseFloat(acct.equity).toFixed(2);
      if (acct?.last_equity) lastEquity  = +parseFloat(acct.last_equity).toFixed(2);
    } catch(e) { log('warn', `getAccount failed: ${e.message}`); }
  }

  // If Alpaca didn't return valid equity, skip this sync — don't write wrong values
  if (equityValue <= 0) {
    log('warn', 'syncPortfolio: no valid equity from Alpaca — skipping write');
    return;
  }

  // Sanity check: Alpaca's equity can lag after a new order fills —
  // during that window equity = just cash (position not yet valued).
  // If equity is suspiciously low (< cash + estimated position value), 
  // calculate it ourselves from cash + current market value of all positions.
  const positionMarketValue = Object.entries(positions).reduce((acc, [sym, pos]) => {
    const cur = priceHistory5m[sym]?.[priceHistory5m[sym].length - 1] || pos.entryPrice;
    return acc + cur * (pos.qtyRemaining || pos.qty);
  }, 0) + Object.entries(shortPositions).reduce((acc, [sym, pos]) => {
    // Short positions don't add market value (cash already received from short sale)
    return acc;
  }, 0) + Object.entries(scalpPositions).reduce((acc, [sym, pos]) => {
    const cur = priceHistory5m[sym]?.[priceHistory5m[sym].length - 1] || pos.entryPrice;
    return acc + (pos.direction === 'long' ? cur * pos.qty : 0);
  }, 0);

  const calculatedEquity = cashValue + positionMarketValue;

  // Use whichever is higher — Alpaca's number or our calculation.
  // This prevents the "position just opened" lag from showing as a loss.
  if (calculatedEquity > equityValue + 100) {
    log('sys', `Equity lag detected: Alpaca=$${equityValue.toFixed(2)} Calculated=$${calculatedEquity.toFixed(2)} — using calculated`);
    equityValue = +calculatedEquity.toFixed(2);
  }

  // Keep real equity updated for accurate circuit breaker
  if (equityValue > 0) {
    realEquity = equityValue;
    if (realDailyStartEquity === 0 && lastEquity > 0) {
      realDailyStartEquity = lastEquity;
      log('risk', `Circuit breaker baseline: $${lastEquity.toFixed(2)} (yesterday close)`);
    }
  }

  // Day P&L = today's equity vs yesterday's close
  // If lastEquity is 0 (paper account first day), fall back to realized trades
  const dayPnl = lastEquity > 0
    ? +(equityValue - lastEquity).toFixed(2)
    : trades
        .filter(t => ['SELL','COVER','SCALP_EXIT'].includes(t.side) && t.pnl !== null
          && new Date(t.time).toDateString() === new Date().toDateString())
        .reduce((a, t) => a + t.pnl, 0);

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

  // Equity snapshot for chart — only every 2 minutes to reduce Supabase writes
  const now = Date.now();
  if (equityValue > 0 && equityValue < CONFIG.startingCapital * 10 && (now - lastEquitySnapshot) > EQUITY_SNAPSHOT_INTERVAL_MS) {
    lastEquitySnapshot = now;
    await sbFetch('tc_equity', 'POST', {
      value:      +equityValue.toFixed(2),
      created_at: new Date().toISOString(),
    });
  }
}

async function syncPositions() {
  // Build the full list of all open positions across all types
  const allPositions = [];

  // Swing longs
  for (const [sym, pos] of Object.entries(positions)) {
    const cur    = priceHistory5m[sym]?.[priceHistory5m[sym].length - 1] || pos.entryPrice;
    const qty    = pos.qtyRemaining || pos.qty;
    const pnl    = (cur - pos.entryPrice) * qty;
    const pnlPct = pos.entryPrice > 0 ? ((cur - pos.entryPrice) / pos.entryPrice) * 100 : 0;
    allPositions.push({
      symbol:        sym,
      side:          'LONG',
      entry_price:   +pos.entryPrice.toFixed(4),
      qty,
      cost:          +(pos.entryPrice * qty).toFixed(2),
      current_price: +cur.toFixed(4),
      pnl:           +pnl.toFixed(2),
      pnl_pct:       +pnlPct.toFixed(2),
      entry_time:    new Date(pos.entryTime).toISOString(),
      high_water:    +(pos.highWater || pos.entryPrice).toFixed(4),
      confidence:    pos.sigInfo?.confidence || 0,
      updated_at:    new Date().toISOString(),
    });
  }

  // Swing shorts
  for (const [sym, pos] of Object.entries(shortPositions)) {
    const cur    = priceHistory5m[sym]?.[priceHistory5m[sym].length - 1] || pos.entryPrice;
    const qty    = pos.qtyRemaining || pos.qty;
    const pnl    = (pos.entryPrice - cur) * qty; // inverted for shorts
    const pnlPct = pos.entryPrice > 0 ? ((pos.entryPrice - cur) / pos.entryPrice) * 100 : 0;
    allPositions.push({
      symbol:        sym,
      side:          'SHORT',
      entry_price:   +pos.entryPrice.toFixed(4),
      qty:           -qty, // negative qty = short in dashboard
      cost:          +(pos.entryPrice * qty).toFixed(2),
      current_price: +cur.toFixed(4),
      pnl:           +pnl.toFixed(2),
      pnl_pct:       +pnlPct.toFixed(2),
      entry_time:    new Date(pos.entryTime).toISOString(),
      high_water:    +(pos.lowWater || pos.entryPrice).toFixed(4),
      confidence:    pos.sigInfo?.confidence || 0,
      updated_at:    new Date().toISOString(),
    });
  }

  // Scalp positions
  for (const [sym, pos] of Object.entries(scalpPositions)) {
    const cur    = priceHistory5m[sym]?.[priceHistory5m[sym].length - 1] || pos.entryPrice;
    const qty    = pos.qty;
    const pnl    = pos.direction === 'long'
      ? (cur - pos.entryPrice) * qty
      : (pos.entryPrice - cur) * qty;
    const pnlPct = pos.entryPrice > 0 ? (pnl / (pos.entryPrice * qty)) * 100 : 0;
    allPositions.push({
      symbol:        sym,
      side:          pos.direction === 'long' ? 'SCALP_LONG' : 'SCALP_SHORT',
      entry_price:   +pos.entryPrice.toFixed(4),
      qty:           pos.direction === 'long' ? qty : -qty,
      cost:          +(pos.entryPrice * qty).toFixed(2),
      current_price: +cur.toFixed(4),
      pnl:           +pnl.toFixed(2),
      pnl_pct:       +pnlPct.toFixed(2),
      entry_time:    new Date(pos.entryTime).toISOString(),
      high_water:    +(pos.highWater || pos.entryPrice).toFixed(4),
      confidence:    pos.sigInfo?.confidence || 0,
      updated_at:    new Date().toISOString(),
    });
  }

  // Wipe and rewrite all positions atomically
  // Use symbol=neq.ZZZZ as a "match all" filter since Supabase requires a filter for DELETE
  await sbFetch('tc_positions?symbol=neq.____NONE____', 'DELETE');
  if (allPositions.length > 0) {
    await sbFetch('tc_positions', 'POST', allPositions);
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
  // Run both in parallel — was sequential before (2× slower)
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

// Scalp-specific state
let scalpPositions      = {};   // { SYM: { entryPrice, qty, entryTime, stopPrice, tpPrice, highWater, direction } }
let scalpWins           = 0;
let scalpLosses         = 0;
let lastScalpScan       = 0;
const SCALP_SCAN_INTERVAL_MS = 5000; // scalp scans every 5 seconds

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

async function alpacaFetch(url, opts = {}, retries = 2) {
  const fetch = await getFetch();
  const res = await fetch(url, {
    ...opts,
    headers: {
      'APCA-API-KEY-ID':     CONFIG.alpacaKey,
      'APCA-API-SECRET-KEY': CONFIG.alpacaSecret,
      'Content-Type': 'application/json',
      ...(opts.headers || {}),
    },
  });

  // Rate limit hit — wait and retry
  if (res.status === 429 && retries > 0) {
    const retryAfter = +(res.headers.get('retry-after') || 2) * 1000;
    log('warn', `Alpaca rate limit hit — waiting ${retryAfter}ms then retrying`);
    await new Promise(r => setTimeout(r, retryAfter));
    return alpacaFetch(url, opts, retries - 1);
  }

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
    const fetch = await getFetch();
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

// ═══════════════════════════════════════════════════════════════════
// MARKET SCREENER
// ═══════════════════════════════════════════════════════════════════
//
// Two-stage pipeline:
//  Stage 1 (screener): 1 API call → snapshots for ~150 stocks
//                      Scores each by volume, momentum, range
//                      Returns top 25 candidates in ~500ms
//  Stage 2 (signal):   Full RSI/MACD/VWAP/ADX analysis on candidates
//
// Favorites (watchlist) are always included regardless of screener.

const UNIVERSE = [
  'AAPL','MSFT','GOOGL','AMZN','META','NVDA','TSLA','AMD','INTC','QCOM',
  'ORCL','CRM','ADBE','NFLX','PYPL','SHOP','SNOW','UBER','LYFT','COIN',
  'JPM','BAC','GS','MS','WFC','C','BLK','SCHW','V','MA','AXP',
  'JNJ','UNH','PFE','ABBV','MRK','LLY','BMY','GILD','AMGN',
  'WMT','TGT','COST','HD','LOW','NKE','MCD','SBUX','DKNG',
  'XOM','CVX','SLB','OXY','MPC','VLO',
  'BA','CAT','DE','MMM','HON','UPS','FDX',
  'SPY','QQQ','IWM','DIA','XLK','XLF','XLE','XLV','XLI',
  'GLD','SLV','TLT','HYG','XBI','ARKK','SQQQ','TQQQ',
  'PLTR','SOFI','RIVN','NIO','RBLX','HOOD','AFRM',
  'MARA','RIOT','CLSK','GME','AMC',
  'MU','AMAT','LRCX','KLAC','MRVL','TXN','AVGO',
  'PANW','CRWD','ZS','NET','DDOG','MDB','OKTA',
];

let screenerCandidates = [];
let lastScreenerRun    = 0;
const SCREENER_INTERVAL_MS = 3 * 60 * 1000; // re-screen every 3 minutes

function scoreSnapshot(sym, snap) {
  let score = 0;
  const reasons = [];
  const dailyBar = snap.dailyBar, prevDay = snap.prevDailyBar;
  const minuteBar = snap.minuteBar, latestTrade = snap.latestTrade;
  const latestQuote = snap.latestQuote;
  if (!dailyBar || !latestTrade) return null;

  const price    = latestTrade.p || dailyBar.c;
  const dayVol   = dailyBar.v || 0;
  const prevVol  = prevDay?.v || 0;
  const dayHigh  = dailyBar.h || price;
  const dayLow   = dailyBar.l || price;
  const prevClose= prevDay?.c || price;
  const dayOpen  = dailyBar.o || prevClose;

  if (price < 5 || price > 2000) return null;
  if (dayVol < 500000) return null;
  if (latestQuote?.ap && latestQuote?.bp) {
    if ((latestQuote.ap - latestQuote.bp) / price > 0.003) return null;
  }

  const volRatio = prevVol > 0 ? dayVol / prevVol : 1;
  if      (volRatio > 3.0) { score += 40; reasons.push(`Volume ${volRatio.toFixed(1)}x 🔥`); }
  else if (volRatio > 2.0) { score += 30; reasons.push(`Volume ${volRatio.toFixed(1)}x strong`); }
  else if (volRatio > 1.5) { score += 20; reasons.push(`Volume ${volRatio.toFixed(1)}x`); }
  else if (volRatio < 0.5) return null;

  const dayChangePct = (price - prevClose) / prevClose;
  if      (Math.abs(dayChangePct) > 0.05)  { score += 30; reasons.push(`Move ${(dayChangePct*100).toFixed(1)}% 🔥`); }
  else if (Math.abs(dayChangePct) > 0.03)  { score += 20; reasons.push(`Move ${(dayChangePct*100).toFixed(1)}%`); }
  else if (Math.abs(dayChangePct) > 0.015) { score += 10; reasons.push(`Move ${(dayChangePct*100).toFixed(1)}%`); }
  else if (Math.abs(dayChangePct) < 0.005) score -= 10;

  const dayRange = dayHigh - dayLow;
  const rangeRatio = dayRange / price;
  if      (rangeRatio > 0.04)  { score += 25; reasons.push(`Wide range ${(rangeRatio*100).toFixed(1)}%`); }
  else if (rangeRatio > 0.02)  { score += 15; reasons.push(`Range ${(rangeRatio*100).toFixed(1)}%`); }
  else if (rangeRatio < 0.005) return null;

  if (dayRange > 0) {
    const posInRange = (price - dayLow) / dayRange;
    if      (posInRange > 0.85) { score += 20; reasons.push('Near day high'); }
    else if (posInRange < 0.15) { score += 20; reasons.push('Near day low'); }
    else                         score +=  5;
  }

  const gapPct = (dayOpen - prevClose) / prevClose;
  if (Math.abs(gapPct) > 0.02) { score += 15; reasons.push(`Gap ${gapPct > 0 ? 'up' : 'dn'} ${(Math.abs(gapPct)*100).toFixed(1)}%`); }

  if (minuteBar) {
    const minChg = (minuteBar.c - minuteBar.o) / (minuteBar.o || 1);
    if (Math.abs(minChg) > 0.002) { score += 10; reasons.push(`Last min ${minChg > 0 ? '▲' : '▼'}${(Math.abs(minChg)*100).toFixed(2)}%`); }
  }

  const direction = dayChangePct > 0.001 ? 'bullish' : dayChangePct < -0.001 ? 'bearish' : 'neutral';
  return { symbol: sym, score, direction, reasons, price, volRatio, dayChangePct };
}

async function runMarketScreener() {
  if (!CONFIG.alpacaKey) return [];
  try {
    const symbolsParam = UNIVERSE.join(',');
    const url  = `${ALPACA_DATA_BASE}/v2/stocks/snapshots?symbols=${symbolsParam}&feed=iex`;
    const data = await alpacaFetch(url);
    if (!data || typeof data !== 'object') { log('screen', 'Screener: no data'); return []; }

    const candidates = [];
    for (const [sym, snap] of Object.entries(data)) {
      const scored = scoreSnapshot(sym, snap);
      if (scored && scored.score >= 30) candidates.push(scored);
    }

    candidates.sort((a, b) => b.score - a.score);
    const top = candidates.slice(0, 25);
    log('screen', `Screener: ${Object.keys(data).length} scanned → ${candidates.length} candidates → top ${top.length}: ${top.slice(0,6).map(c=>`${c.symbol}(${c.score})`).join(' ')}`);
    await syncScreenerResults(top);
    return top.map(c => c.symbol);
  } catch (e) {
    log('error', `Screener failed: ${e.message}`);
    return [];
  }
}

async function syncScreenerResults(candidates) {
  if (!candidates.length) return;
  try {
    await sbFetch('tc_portfolio?id=eq.1', 'PATCH', {
      screener_candidates: JSON.stringify(candidates.slice(0, 15).map(c => ({
        symbol: c.symbol, score: c.score, price: (+c.price).toFixed(2),
        change: (c.dayChangePct * 100).toFixed(2) + '%',
        volume: c.volRatio.toFixed(1) + 'x', direction: c.direction,
        reasons: c.reasons.slice(0, 2).join(', '),
      }))),
      updated_at: new Date().toISOString(),
    });
  } catch(e) {}
}

// Build the final scan list: favorites + screener candidates, deduplicated
function buildScanList() {
  const favorites  = CONFIG.symbols; // always included — from dashboard watchlist
  const discovered = screenerCandidates;
  const combined   = [...new Set([...favorites, ...discovered])];
  return combined.filter(s => /^[A-Z]{1,5}$/.test(s)); // strip invalid symbols
}

// ═══════════════════════════════════════════════════════════════════
// END MARKET SCREENER
// ═══════════════════════════════════════════════════════════════════

async function placeOrder(symbol, qty, side) {
  if (isSimMode()) return simOrder(symbol, qty, side);
  const data = await alpacaFetch(`${ALPACA_BASE()}/v2/orders`, {
    method: 'POST',
    body: JSON.stringify({ symbol, qty: String(qty), side, type: 'market', time_in_force: 'day' }),
  });
  if (data.id) { log('order', `${side.toUpperCase()} order placed: ${qty}x ${symbol} | ID: ${data.id}`); return data; }
  throw new Error(data.message || JSON.stringify(data));
}

// ─────────────────────────────────────────────
// SIMULATION / BAR REPLAY ENGINE
// ─────────────────────────────────────────────
//
// Problem: when markets are closed there's no live price.
// A simulated fill at a stale price hours old = meaningless.
//
// Solution: Bar Replay.
// We load the last N days of 5-minute bars from Yahoo Finance,
// then step through them one bar at a time at the scan interval.
// Every "scan" the bot sees exactly the bars it would have seen
// if it were running live at that moment in time.
//
// This gives you realistic, honest backtesting using real
// historical prices — anytime, any day of the week.
// ─────────────────────────────────────────────

function isSimMode() { return CONFIG.mode === 'sim'; }

// Bar replay state
const simState = {
  loaded:      false,          // has replay data been loaded?
  bars:        {},             // { SYM: [bar, bar, ...] } — full history
  cursor:      0,              // which bar index we're "at" right now
  totalBars:   0,              // total bars available
  startTime:   null,           // timestamp of first bar
  currentTime: null,           // timestamp of current bar
  speed:       1,              // replay speed multiplier (1 = real-time equivalent)
};

// Load historical 5-minute bars for all symbols to replay
async function loadSimBars(symbols) {
  log('sim', `🎮 Loading bar replay data for ${symbols.length} symbols in parallel…`);
  simState.bars    = {};
  simState.cursor  = 0;
  simState.loaded  = false;

  const fetch = await getFetch();

  // Fetch all symbols in parallel — was sequential before (~10s), now ~1-2s
  const results = await Promise.allSettled(symbols.map(async sym => {
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${sym}?interval=5m&range=5d`;
    const res  = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
    const data = await res.json();
    const result = data?.chart?.result?.[0];
    if (!result) return { sym, bars: null };

    const timestamps = result.timestamp || [];
    const q = result.indicators?.quote?.[0] || {};
    const bars = timestamps.map((t, i) => ({
      t: new Date(t * 1000).toISOString(),
      o: q.open?.[i]   || null,
      h: q.high?.[i]   || null,
      l: q.low?.[i]    || null,
      c: q.close?.[i]  || null,
      v: q.volume?.[i] || 0,
    })).filter(b => b.c != null && b.h != null && b.l != null);

    return { sym, bars: bars.length >= 20 ? bars : null };
  }));

  for (const r of results) {
    if (r.status === 'fulfilled' && r.value.bars) {
      simState.bars[r.value.sym] = r.value.bars;
    }
  }

  const loaded = Object.keys(simState.bars);
  log('sim', `🎮 Loaded ${loaded.length}/${symbols.length} symbols`);

  const lengths = Object.values(simState.bars).map(b => b.length);
  if (lengths.length === 0) {
    log('error', 'Sim: no bar data loaded — cannot start replay');
    return false;
  }

  simState.totalBars   = Math.min(...lengths);
  simState.cursor      = 0; // start from the beginning of the replay window
  simState.loaded      = true;
  simState.startTime   = simState.bars[Object.keys(simState.bars)[0]][0].t;
  simState.currentTime = simState.startTime;

  log('sim', `🎮 Bar replay ready: ${simState.totalBars} bars per symbol`);
  log('sim', `🎮 Replay starts at: ${simState.startTime}`);
  return true;
}

// Advance the replay cursor by one bar
// Returns { sym → bars[] } sliced up to current cursor, or null if replay is done
function simAdvanceCursor() {
  if (!simState.loaded) return null;

  if (simState.cursor >= simState.totalBars - 20) {
    // End of replay — loop back to start (or stop)
    log('sim', '🎮 Bar replay reached end — looping back to start');
    simState.cursor = 20; // start with enough history for indicators
  }

  simState.cursor++;
  const firstSym = Object.keys(simState.bars)[0];
  if (simState.bars[firstSym]?.[simState.cursor]) {
    simState.currentTime = simState.bars[firstSym][simState.cursor].t;
  }

  // Return sliced bars — bot only sees bars UP TO current cursor
  // This is the key: no lookahead, realistic simulation
  const snapshot = {};
  for (const [sym, bars] of Object.entries(simState.bars)) {
    snapshot[sym] = bars.slice(0, simState.cursor + 1);
  }
  return snapshot;
}

// Get sim price for a symbol at current cursor position
function simCurrentPrice(sym) {
  if (!simState.loaded || !simState.bars[sym]) return null;
  const bar = simState.bars[sym][simState.cursor];
  return bar?.c || null;
}

// Simulated order — fills at the OPEN of the NEXT bar (realistic)
// In real trading you submit at bar close, fill at next open
function simOrder(symbol, qty, side) {
  // Fill at next bar's open price (realistic — avoids lookahead bias)
  const nextIdx  = Math.min(simState.cursor + 1, (simState.bars[symbol]?.length || 1) - 1);
  const nextBar  = simState.bars[symbol]?.[nextIdx];
  const fillPrice = nextBar?.o || simCurrentPrice(symbol) || 100;

  const id = `SIM-${Date.now()}-${Math.random().toString(36).slice(2,6).toUpperCase()}`;
  const ts = simState.currentTime || new Date().toISOString();
  log('sim', `🎮 SIM ${side.toUpperCase()} ${qty}x ${symbol} @ $${fillPrice.toFixed(2)} [bar: ${ts.slice(11,16)}]`);
  return { id, status: 'filled', filled_avg_price: String(fillPrice), symbol, qty: String(qty), side, simPrice: fillPrice };
}

// Sim-mode runScan — uses replayed bars instead of live data
async function runSimScan() {
  if (!isSimMode()) return;

  // Load bars on first run
  if (!simState.loaded) {
    const symbols = buildScanList().slice(0, 20); // limit to 20 in sim for speed
    const ok = await loadSimBars(symbols);
    if (!ok) return;
    // Reset portfolio for fresh sim
    portfolio = CONFIG.startingCapital;
    Object.keys(positions).forEach(k => delete positions[k]);
    Object.keys(shortPositions).forEach(k => delete shortPositions[k]);
    totalWins = 0; totalLosses = 0;
    realDailyStartEquity = CONFIG.startingCapital;
    await sbFetch('tc_portfolio?id=eq.1', 'PATCH', {
      cash: portfolio, total_value: portfolio, day_pnl: 0,
      total_wins: 0, total_losses: 0, circuit_breaker: false,
      session: '🎮 SIM REPLAY', updated_at: new Date().toISOString(),
    });
    log('sim', `🎮 Sim portfolio reset to $${portfolio.toFixed(2)}`);
  }

  // Advance one bar
  const snapshot = simAdvanceCursor();
  if (!snapshot) return;

  const barTime = simState.currentTime;
  log('sim', `─── 🎮 SIM BAR [${barTime?.slice(11,16)||'?'}] cursor:${simState.cursor}/${simState.totalBars} ───`);

  // Manage existing positions using replayed prices
  for (const sym of Object.keys(positions)) {
    const bars5m = snapshot[sym];
    if (!bars5m || bars5m.length < 5) continue;
    const price = bars5m[bars5m.length - 1].c;
    priceHistory5m[sym] = bars5m.map(b => b.c);
    await managePosition(sym, price, bars5m);
  }
  for (const sym of Object.keys(shortPositions)) {
    const bars5m = snapshot[sym];
    if (!bars5m || bars5m.length < 5) continue;
    const price = bars5m[bars5m.length - 1].c;
    await manageShort(sym, price, bars5m);
  }

  // Generate signals using replayed bars
  const totalOpen = Object.keys(positions).length + Object.keys(shortPositions).length;
  for (const sym of Object.keys(snapshot)) {
    if (positions[sym] || shortPositions[sym]) continue;
    const bars5m  = snapshot[sym];
    const bars15m = simState.bars[sym]
      ? simState.bars[sym].slice(0, simState.cursor + 1).filter((_, i) => i % 3 === 0) // approximate 15min
      : null;
    if (!bars5m || bars5m.length < 20) continue;

    const price = bars5m[bars5m.length - 1].c;
    priceHistory5m[sym] = bars5m.map(b => b.c);

    let sig = generateSignal(sym, bars5m, bars15m);
    sig = applyDayBias(sig);

    if (sig.signal !== 'HOLD' && sig.score > 0) {
      log('sim', `  ${sym} $${price.toFixed(2)} → ${sig.signal} (score:${sig.score} conf:${sig.confidence}%)`);
    }

    const confirmed = confirmSignal(sym, sig);
    if (!confirmed) continue;

    if (sig.signal === 'BUY' && totalOpen < CONFIG.maxOpenPositions) {
      await enterPosition(sym, price, sig, bars5m, 'long');
    } else if (sig.signal === 'SELL' && SHORTING_ENABLED && totalOpen < CONFIG.maxOpenPositions) {
      await enterPosition(sym, price, sig, bars5m, 'short');
    }
  }

  // Sync simulated portfolio to Supabase so dashboard updates live
  const openPnl = Object.entries(positions).reduce((acc, [sym, pos]) => {
    const cur = priceHistory5m[sym]?.[priceHistory5m[sym].length-1] || pos.entryPrice;
    return acc + (cur - pos.entryPrice) * (pos.qtyRemaining || pos.qty);
  }, 0);
  const equity = portfolio + openPnl;
  const dayPnl = equity - (realDailyStartEquity || CONFIG.startingCapital);

  realEquity = equity;

  await sbFetch('tc_portfolio?id=eq.1', 'PATCH', {
    cash:            +portfolio.toFixed(2),
    total_value:     +equity.toFixed(2),
    day_pnl:         +dayPnl.toFixed(2),
    total_wins:      totalWins,
    total_losses:    totalLosses,
    circuit_breaker: circuitBreakerOn,
    last_scan:       new Date().toISOString(),
    session:         `🎮 SIM [${barTime?.slice(11,16)||'?'}]`,
    updated_at:      new Date().toISOString(),
  });
  await sbFetch('tc_equity', 'POST', { value: +equity.toFixed(2), created_at: new Date().toISOString() });
  await syncLog('sim', `🎮 SIM bar ${simState.cursor}/${simState.totalBars} | Equity=$${equity.toFixed(2)} P&L=${dayPnl>=0?'+':''}$${dayPnl.toFixed(2)} | Open:${Object.keys(positions).length} W:${totalWins}/L:${totalLosses}`);
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
// SIGNAL ENGINE — High Win Rate Edition
// ─────────────────────────────────────────────
//
// Philosophy: fewer trades, higher quality.
// Every indicator must AGREE — not just contribute points.
// A signal only fires when multiple independent systems
// all point the same direction simultaneously.
//
// Quality gates (hard blocks, not dampeners):
//   1. Daily trend must align — no fighting the daily chart
//   2. RSI must be in meaningful territory — not neutral
//   3. MACD must confirm with actual crossover or clear separation
//   4. Volume must be above average — confirms conviction
//   5. 15min timeframe must agree — no counter-trend 5min signals
//   6. ADX must show trending conditions — no ranging markets
//   7. Reward:Risk must be at least 2:1 before entering
// ─────────────────────────────────────────────
function generateSignal(sym, bars5m, bars15m) {
  if (!bars5m || bars5m.length < 30) return { signal: 'HOLD', confidence: 0, reasons: ['Need 30+ bars'] };

  const c5    = bars5m.map(b => b.c);
  const c15   = bars15m?.length >= 20 ? bars15m.map(b => b.c) : null;
  const vol   = bars5m.map(b => b.v);
  const highs = bars5m.map(b => b.h);
  const lows  = bars5m.map(b => b.l);
  const price = c5[c5.length - 1];

  const reasons = [];
  let passedGates = 0; // how many hard gates passed
  let direction   = null; // 'buy' or 'sell' — set by first strong signal, all others must agree

  // ── GATE 1: RSI must be in meaningful territory ──
  // Neutral RSI (40-60) = no edge. We only trade extremes.
  const r = rsi(c5, CONFIG.rsiPeriod);
  let rsiScore = 0;
  if      (r < CONFIG.rsiOversold)   { rsiScore = 2; direction = 'buy';  reasons.push(`RSI oversold ${r.toFixed(1)} ✅`); }
  else if (r > CONFIG.rsiOverbought) { rsiScore = 2; direction = 'sell'; reasons.push(`RSI overbought ${r.toFixed(1)} ✅`); }
  else if (r < 38)                   { rsiScore = 1; direction = 'buy';  reasons.push(`RSI leaning oversold ${r.toFixed(1)}`); }
  else if (r > 62)                   { rsiScore = 1; direction = 'sell'; reasons.push(`RSI leaning overbought ${r.toFixed(1)}`); }
  else {
    return { signal: 'HOLD', confidence: 0, score: 0, reasons: [`RSI neutral (${r.toFixed(1)}) — no edge`], rsi: r };
  }
  passedGates++;

  // ── GATE 2: MACD must confirm direction ──
  const e8  = ema(c5, 8),  e21 = ema(c5, 21);
  const pe8 = ema(c5.slice(0,-1), 8), pe21 = ema(c5.slice(0,-1), 21);
  const macdBull = e8 > e21;
  const macdCross = (pe8 < pe21 && e8 > e21) || (pe8 > pe21 && e8 < e21);

  if (direction === 'buy'  && !macdBull) {
    return { signal: 'HOLD', confidence: 0, score: 0, reasons: [...reasons, 'MACD bearish vs bullish RSI — no entry'], rsi: r };
  }
  if (direction === 'sell' && macdBull) {
    return { signal: 'HOLD', confidence: 0, score: 0, reasons: [...reasons, 'MACD bullish vs bearish RSI — no entry'], rsi: r };
  }
  const macdScore = macdCross ? 2 : 1;
  if (macdCross) reasons.push(`MACD crossover ${direction === 'buy' ? '↑' : '↓'} ✅`);
  else reasons.push(`MACD aligned ${direction === 'buy' ? 'bullish' : 'bearish'}`);
  passedGates++;

  // ── GATE 3: 200 EMA trend alignment ──
  // Never fight the primary trend. Long only above 200 EMA, short only below.
  if (c5.length >= 30) {
    const e200 = ema(c5, Math.min(200, c5.length));
    if      (direction === 'buy'  && price > e200) { reasons.push(`Above 200 EMA ✅`); passedGates++; }
    else if (direction === 'sell' && price < e200) { reasons.push(`Below 200 EMA ✅`); passedGates++; }
    else {
      return { signal: 'HOLD', confidence: 0, score: 0, reasons: [...reasons, `200 EMA against signal — skip`], rsi: r };
    }
  }

  // ── GATE 4: Volume confirmation ──
  // No volume = no conviction = no trade
  const avgVol = vol.slice(-20).reduce((a,b)=>a+b,0) / Math.min(20, vol.length);
  const curVol = vol[vol.length-1];
  const volRatio = avgVol > 0 ? curVol / avgVol : 1;
  if (volRatio < 0.8) {
    return { signal: 'HOLD', confidence: 0, score: 0, reasons: [...reasons, `Volume too low (${volRatio.toFixed(1)}x) — no conviction`], rsi: r };
  }
  const volScore = volRatio >= 1.5 ? 2 : 1;
  reasons.push(`Volume ${volRatio.toFixed(1)}x avg ${volRatio >= 1.5 ? '✅' : ''}`);
  passedGates++;

  // ── GATE 5: 15-minute timeframe must agree ──
  // 5-minute signals against the 15-min trend are low probability
  if (c15) {
    const r15    = rsi(c15, CONFIG.rsiPeriod);
    const e8_15  = ema(c15, 8), e21_15 = ema(c15, 21);
    const bull15 = e8_15 > e21_15 && r15 < 60;
    const bear15 = e8_15 < e21_15 && r15 > 40;
    if      (direction === 'buy'  && bull15) { reasons.push(`15min bullish ✅`); passedGates++; }
    else if (direction === 'sell' && bear15) { reasons.push(`15min bearish ✅`); passedGates++; }
    else {
      return { signal: 'HOLD', confidence: 0, score: 0, reasons: [...reasons, '15min disagrees — skip'], rsi: r };
    }
  }

  // ── GATE 6: ADX — only trade when market is trending ──
  const adxData = adx(bars5m);
  if (adxData.adx > 0) {
    if (adxData.adx < 18) {
      return { signal: 'HOLD', confidence: 0, score: 0, reasons: [...reasons, `ADX ${adxData.adx.toFixed(0)} — ranging market, skip`], rsi: r };
    }
    const adxAligned = direction === 'buy'
      ? adxData.diPlus > adxData.diMinus
      : adxData.diMinus > adxData.diPlus;
    if (adxAligned) { reasons.push(`ADX ${adxData.adx.toFixed(0)} trending ✅`); passedGates++; }
    else { reasons.push(`ADX direction mismatch — dampening`); }
  }

  // ── BONUS POINTS: Additional confluence adds confidence ──
  let bonus = 0;

  // VWAP — price position relative to intraday average
  const vw = vwap(bars5m.slice(-20));
  const vwapDist = (price - vw) / vw;
  if      (direction === 'buy'  && vwapDist >  0.002) { bonus++; reasons.push(`Above VWAP ✅`); }
  else if (direction === 'sell' && vwapDist < -0.002) { bonus++; reasons.push(`Below VWAP ✅`); }

  // Bollinger Bands — price at extremes
  const bb = bollingerBands(c5);
  if (bb) {
    if      (direction === 'buy'  && price <= bb.lower * 1.001) { bonus++; reasons.push(`At BB lower ✅`); }
    else if (direction === 'sell' && price >= bb.upper * 0.999) { bonus++; reasons.push(`At BB upper ✅`); }
  }

  // StochRSI extreme readings
  const stoch = stochRSI(c5);
  if      (direction === 'buy'  && stoch.k < 25 && stoch.d < 25) { bonus++; reasons.push(`StochRSI oversold ✅`); }
  else if (direction === 'sell' && stoch.k > 75 && stoch.d > 75) { bonus++; reasons.push(`StochRSI overbought ✅`); }

  // Price momentum matches direction
  const recentMove = (c5[c5.length-1] - c5[c5.length-4]) / c5[c5.length-4];
  if      (direction === 'buy'  && recentMove >  0.002) { bonus++; reasons.push(`Momentum up ✅`); }
  else if (direction === 'sell' && recentMove < -0.002) { bonus++; reasons.push(`Momentum down ✅`); }

  // Higher highs / lower lows structure
  if (direction === 'buy') {
    const hh = highs[highs.length-1] > highs[highs.length-3] && highs[highs.length-3] > highs[highs.length-5];
    if (hh) { bonus++; reasons.push(`Higher highs structure ✅`); }
  } else {
    const ll = lows[lows.length-1] < lows[lows.length-3] && lows[lows.length-3] < lows[lows.length-5];
    if (ll) { bonus++; reasons.push(`Lower lows structure ✅`); }
  }

  // ── Quality score ──
  // Gates passed (required) + bonus confluence (nice to have)
  // Need at least 5 gates AND at least 1 bonus for a trade
  const minGates = c15 ? 6 : 5; // stricter when 15min data available
  const score    = passedGates * 10 + bonus * 5 + rsiScore * 5 + macdScore * 5 + volScore * 3;
  const confidence = Math.min(99, Math.round((passedGates / (minGates + 2)) * 100));

  if (passedGates < minGates) {
    reasons.push(`Only ${passedGates}/${minGates} gates passed — need more confluence`);
    return { signal: 'HOLD', confidence, score, reasons, rsi: r };
  }
  if (bonus < 1) {
    reasons.push(`No bonus confirmations — waiting for better setup`);
    return { signal: 'HOLD', confidence, score, reasons, rsi: r };
  }

  // ── Reward:Risk check ──
  // Only take trades where the expected move justifies the risk
  const atrVal  = atr(bars5m, 14);
  const stopDist = atrVal * CONFIG.atrStopMult;
  const tpDist   = atrVal * CONFIG.atrStopMult * 2; // require minimum 2:1 R:R
  if (stopDist > 0 && tpDist / stopDist < 1.8) {
    reasons.push(`R:R too low (${(tpDist/stopDist).toFixed(1)}:1) — need 1.8:1 minimum`);
    return { signal: 'HOLD', confidence, score, reasons, rsi: r };
  }

  // ── Shorting requires extra conviction ──
  // Stocks have natural upward drift — short signals need more gates
  if (direction === 'sell' && bonus < 2) {
    reasons.push(`Short needs 2+ bonus confirmations (only ${bonus}) — skip`);
    return { signal: 'HOLD', confidence, score, reasons, rsi: r };
  }

  reasons.push(`✅ ${passedGates} gates + ${bonus} bonus — HIGH QUALITY SETUP`);
  return {
    signal:     direction === 'buy' ? 'BUY' : 'SELL',
    confidence: Math.min(99, confidence + bonus * 5),
    score,
    reasons,
    rsi: r,
    atr: atrVal,
  };
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

// Track real equity from Alpaca for circuit breaker (not just cash)
let realEquity = 0;
let realDailyStartEquity = 0;

function checkCircuitBreaker() {
  if (circuitBreakerOn) return true;

  // Use real Alpaca equity if available, otherwise skip the check
  // (avoids false triggers after restarts when in-memory state is wrong)
  const equityToCheck  = realEquity          > 0 ? realEquity          : null;
  const startToCheck   = realDailyStartEquity > 0 ? realDailyStartEquity : null;

  if (!equityToCheck || !startToCheck) {
    log('risk', 'Circuit breaker: waiting for real equity data from Alpaca…');
    return false; // don't trip until we have real data
  }

  const loss = (startToCheck - equityToCheck) / startToCheck;
  if (loss >= CONFIG.maxDailyLossPct) {
    circuitBreakerOn = true;
    log('risk', `🔴 CIRCUIT BREAKER: Down ${(loss*100).toFixed(1)}% today (equity $${equityToCheck.toFixed(2)} vs start $${startToCheck.toFixed(2)}) — halting all trades`);
    sendDiscordAlert('circuit_breaker', 'ALL', 0, 0, -(startToCheck - equityToCheck));
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

    if (isSimMode() || (CONFIG.mode === 'alpaca' && CONFIG.alpacaKey)) {
      try { await placeSmartOrder(sym, qty, 'buy', false); }
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
    // Seed price history immediately so P&L shows right away
    if (!priceHistory5m[sym] || priceHistory5m[sym].length === 0) {
      priceHistory5m[sym] = [price];
    }
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

    if (isSimMode() || (CONFIG.mode === 'alpaca' && CONFIG.alpacaKey)) {
      try { await placeSmartOrder(sym, qty, 'sell', false); } // sell to open short
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
    // Seed price history immediately
    if (!priceHistory5m[sym] || priceHistory5m[sym].length === 0) {
      priceHistory5m[sym] = [price];
    }
    trades.push({ time: new Date(), sym, side: 'SHORT', qty, price, pnl: null, reason: 'SIGNAL', confidence: sigInfo.confidence });
    const stopPct = ((stopPrice - price) / price * 100).toFixed(2);
    log('short', `🔴 SHORT ${qty}x ${sym} @ $${price.toFixed(2)} | SL=$${stopPrice.toFixed(2)} (+${stopPct}%) | conf=${sigInfo.confidence}%`);
    await sendDiscordAlert('short', sym, qty, price, undefined, undefined, sigInfo, { stopPrice, atrVal });
    await syncTrade({ sym, side: 'SHORT', qty, price, pnl: null, reason: 'SIGNAL', confidence: sigInfo.confidence });
  }

  await syncAll();
  await syncLog(direction === 'long' ? 'buy' : 'sell', `${direction === 'long' ? '✅ LONG' : '🔴 SHORT'} ${qty}x ${sym} @ $${price.toFixed(2)} conf=${sigInfo.confidence}%`);

  // Fetch live price immediately so dashboard shows P&L right away
  // Don't wait — fire and forget
  fetchLatestPrice(sym).then(livePrice => {
    if (livePrice && livePrice !== price) {
      priceHistory5m[sym] = [...(priceHistory5m[sym] || [price]), livePrice];
      syncPositions().catch(() => {});
    }
  }).catch(() => {});
}

// Cover a short position (buy back the borrowed shares)
async function coverShort(sym, price, reason) {
  const pos = shortPositions[sym];
  if (!pos) return;
  const qty = pos.qtyRemaining || pos.qty;

  if (isSimMode() || (CONFIG.mode === 'alpaca' && CONFIG.alpacaKey)) {
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
  if (isSimMode() || (CONFIG.mode === 'alpaca' && CONFIG.alpacaKey)) {
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
  if (isSimMode() || (CONFIG.mode === 'alpaca' && CONFIG.alpacaKey)) {
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
  if (isSimMode() || (CONFIG.mode === 'alpaca' && CONFIG.alpacaKey)) {
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

  // Reload settings from dashboard on every scan
  await loadRemoteConfig();

  // If swing mode is disabled, skip signal scanning (scalp still runs separately)
  if (CONFIG.swingEnabled === false) {
    log('scan', '📈 Swing trading DISABLED from dashboard — skipping swing scan');
    await syncAll();
    return;
  }

  // Build dynamic scan list: favorites + screener candidates
  const scanList = buildScanList();
  const symbolsToScan = scanList.filter(s => shouldScanSymbol(s));
  log('scan', `Scan list: ${symbolsToScan.length} symbols (${CONFIG.symbols.length} favorites + ${Math.max(0, symbolsToScan.length - CONFIG.symbols.length)} screener picks)`);
  log('scan', `═══ ${session} — Scanning ${symbolsToScan.length} symbols ═══`);
  await syncLog('sys', `Scan started — ${session} — ${symbolsToScan.length} symbols (${CONFIG.symbols.length} favs + screener)`);

  // Always sync live Alpaca positions first
  await syncAlpacaPositions();
  log('scan', `Prefetching bars for ${symbolsToScan.length} symbols in parallel…`);
  const prefetchStart = Date.now();
  const allBarData = await fetchAllBarsParallel(symbolsToScan);
  log('scan', `Bars ready in ${Date.now() - prefetchStart}ms`);

  // Build lookup maps from prefetch results
  const barData5m  = {};
  const barData15m = {};
  for (const { sym, bars5m, bars15m } of allBarData) {
    barData5m[sym]  = bars5m;
    barData15m[sym] = bars15m;
  }

  // Market regime only matters during US hours
  const marketOk = isMarketOpen() ? await getMarketRegime() : true;

  // Log Alpaca account state
  if (CONFIG.alpacaKey && isMarketOpen()) {
    const acct = await getAccount();
    if (acct?.equity) log('acct', `Equity=$${(+acct.equity).toFixed(2)} BuyingPower=$${(+acct.buying_power).toFixed(2)}`);
  }

  for (const sym of symbolsToScan) {
    try {

      const sessionMult = getSessionMultiplier(sym);
      const sessionLabel = ETF_SESSIONS[sym]
        ? `[${ETF_SESSIONS[sym].region} ${sessionMult >= 1.0 ? '🟢 PRIME' : '🟡 off-hrs'} x${sessionMult}]`
        : '[US stock]';

      // Use cached bars from parallel prefetch
      const bars5m  = barData5m[sym]  || await fetchBarsCached(sym, '5Min',  60);
      const bars15m = barData15m[sym] || await fetchBarsCached(sym, '15Min', 40);
      if (!bars5m || bars5m.length < 10) { log('warn', `No data for ${sym}`); continue; }

      // Capture opening range at 10:00 AM ET
      await captureOpeningRange(sym);

      // Use last bar close as price — accurate enough for swing entries
      // (fetchLatestPrice per symbol was burning too many API calls)
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

      // Generate signal
      let sig = generateSignal(sym, bars5m, bars15m);
      const adjustedConfidence = Math.round(sig.confidence * sessionMult);
      sig = { ...sig, confidence: adjustedConfidence };

      if (sessionMult < 1.0) sig.reasons = [`Off-prime (x${sessionMult})`, ...sig.reasons];
      else if (sessionMult > 1.0) sig.reasons = [`Prime boost (x${sessionMult})`, ...sig.reasons];

      // Apply day bias (aligns with institutional flow)
      sig = applyDayBias(sig);

      // Apply ORB score bonus
      if (sig.signal !== 'HOLD') {
        const orbBonus = orbScoreBonus(sym, price, sig.signal === 'BUY' ? 'buy' : 'sell');
        if (orbBonus !== 0) {
          sig = { ...sig, score: (sig.score||0) + orbBonus, reasons: [`ORB ${orbBonus > 0 ? 'breakout ↑' : 'against breakout ↓'} (${orbBonus > 0 ? '+' : ''}${orbBonus}pts)`, ...sig.reasons] };
        }
      }

      log('signal', `${sym} @ $${price.toFixed(2)} → ${sig.signal} (conf:${sig.confidence}% RSI:${sig.rsi?.toFixed(1)} score:${sig.score||0}) ${sessionLabel}`);
      // Only write to Supabase logs for non-HOLD signals (reduces writes by ~90%)
      if (sig.signal !== 'HOLD') {
        await syncLog('info', `${sym} @ $${price.toFixed(2)} → ${sig.signal} conf:${sig.confidence}% score:${sig.score||0} ${sessionLabel}`);
      }

      const totalOpen = Object.keys(positions).length + Object.keys(shortPositions).length;

      // Signal confirmation gate — must appear on 2 consecutive scans
      const confirmed = confirmSignal(sym, sig);
      if (!confirmed) continue; // waiting for confirmation

      // BUY long — confirmed signal
      if (sig.signal === 'BUY' && marketOk && totalOpen < CONFIG.maxOpenPositions) {
        if (!positions[sym] && !alpacaPositions.has(sym)) {
          // Spread check — don't enter into a wide spread
          const spreadOk = await spreadIsAcceptable(sym, price);
          if (spreadOk) await enterPosition(sym, price, sig, bars5m, 'long');
        }
      }

      // SHORT — confirmed signal
      if (sig.signal === 'SELL' && SHORTING_ENABLED && totalOpen < CONFIG.maxOpenPositions) {
        if (!shortPositions[sym] && !alpacaShorts.has(sym)) {
          const spreadOk = await spreadIsAcceptable(sym, price);
          if (spreadOk) await enterPosition(sym, price, sig, bars5m, 'short');
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
  if (isSimMode()) return; // sim tracks positions in memory — no Alpaca sync needed
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

async function syncPricesOnly() {
  // In sim mode — update prices from bar cache
  if (isSimMode()) {
    for (const [sym, pos] of Object.entries(positions)) {
      const cur = priceHistory5m[sym]?.[priceHistory5m[sym].length - 1];
      if (!cur) continue;
      if (cur > pos.highWater) positions[sym].highWater = cur;
    }
    await syncPositions();
    return;
  }

  if (!CONFIG.alpacaKey) return;

  try {
    // Fetch account AND positions in parallel for maximum speed
    const [alpacaPos, acct] = await Promise.all([
      alpacaFetch(`${ALPACA_BASE()}/v2/positions`),
      getAccount(),
    ]);

    // Update price history from live Alpaca positions
    if (Array.isArray(alpacaPos)) {
      for (const ap of alpacaPos) {
        const sym = ap.symbol;
        const cur = +ap.current_price;
        if (!cur || cur <= 0) continue;

        if (!priceHistory5m[sym]) priceHistory5m[sym] = [];
        priceHistory5m[sym].push(cur);
        if (priceHistory5m[sym].length > 60) priceHistory5m[sym].shift();

        if (positions[sym]     && cur > positions[sym].highWater)     positions[sym].highWater     = cur;
        if (shortPositions[sym] && cur < shortPositions[sym].lowWater) shortPositions[sym].lowWater = cur;
      }
    }

    // Sync all position data (prices + P&L) to Supabase
    await syncPositions();

    // Update portfolio with REAL Alpaca values every price sync
    if (acct) {
      const cash       = acct.cash       ? +parseFloat(acct.cash).toFixed(2)       : null;
      const equity     = acct.equity     ? +parseFloat(acct.equity).toFixed(2)     : null;
      const lastEquity = acct.last_equity? +parseFloat(acct.last_equity).toFixed(2): null;

      if (equity && equity > 0) {
        realEquity = equity;
        if (!realDailyStartEquity && lastEquity > 0) {
          realDailyStartEquity = lastEquity;
        }

        const dayPnl = lastEquity > 0 ? +(equity - lastEquity).toFixed(2) : 0;

        // Only write total_value and day_pnl — not equity snapshot (that's in syncPortfolio)
        await sbFetch('tc_portfolio?id=eq.1', 'PATCH', {
          cash:        cash ? +cash.toFixed(2) : undefined,
          total_value: +equity.toFixed(2),
          day_pnl:     +dayPnl.toFixed(2),
          updated_at:  new Date().toISOString(),
        });
      }
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
  const fetch = await getFetch();

  const colorMap = { buy:0x7fff6e, short:0xff5f57, cover:0xb47fff, partial:0x00e5ff, breakeven:0xffb547, circuit_breaker:0xff0000, sell:0x4da6ff, manual_close:0xb47fff, scalp_entry:0x00e5ff, scalp_exit:0xffb547 };
  if (type==='sell'||type==='manual_close'){
    if (['STOP_LOSS','BREAK_EVEN_STOP'].includes(reason)) colorMap.sell=0xff5f57;
    else if (reason==='TAKE_PROFIT') colorMap.sell=0x00e5ff;
    else if (reason==='TRAILING_STOP') colorMap.sell=0xffb547;
  }
  if (type==='scalp_exit'){
    if (reason==='SCALP_SL')    colorMap.scalp_exit=0xff5f57;
    if (reason==='SCALP_TP')    colorMap.scalp_exit=0x7fff6e;
    if (reason==='SCALP_TRAIL') colorMap.scalp_exit=0x00e5ff;
  }

  const iconMap = {
    buy:'🟢', short:'🔴', cover:'🔵', breakeven:'🔒', circuit_breaker:'🔴', manual_close:'🖐',
    scalp_entry: reason === 'long' ? '⚡🟢' : '⚡🔴',
    scalp_exit:  { SCALP_TP:'⚡🎯', SCALP_SL:'⚡🛑', SCALP_TRAIL:'⚡📉', SCALP_TIME:'⚡⏰', SCALP_REVERSE:'⚡↩️' }[reason] || '⚡📤',
    partial:{TP1:'🎯',TP2:'🎯🎯',TP3:'🎯🎯🎯'}[reason]||'🎯',
    sell:{STOP_LOSS:'🛑',BREAK_EVEN_STOP:'🔒',TAKE_PROFIT:'🎯',TRAILING_STOP:'📉',
          TIME_STOP:'⏰',VOL_SQUEEZE:'📊',RESISTANCE_EXIT:'🧱',SIGNAL:'🔵'}[reason]||'🔵',
  };
  const titleMap = {
    buy:'LONG Position Entered', short:'SHORT Position Entered 🔴',
    cover:'Short Covered', breakeven:'Break-Even Stop Set 🔒',
    circuit_breaker:'⛔ CIRCUIT BREAKER', partial:`Partial Exit — ${reason}`,
    manual_close:'🖐 Position Manually Closed',
    scalp_entry: `⚡ Scalp ${(reason||'long').toUpperCase()} Entry`,
    scalp_exit:  { SCALP_TP:'⚡🎯 Scalp Take Profit', SCALP_SL:'⚡🛑 Scalp Stop Loss', SCALP_TRAIL:'⚡📉 Scalp Trailing Stop', SCALP_TIME:'⚡⏰ Scalp Time Stop', SCALP_REVERSE:'⚡↩️ Scalp Signal Reversal' }[reason] || '⚡ Scalp Exit',
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
  const fetch = await getFetch();

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
  dailyStartPortfolio  = portfolio;
  realDailyStartEquity = realEquity; // reset to current equity for new day
  circuitBreakerOn     = false;
  totalWins = 0; totalLosses = 0;
  trades = trades.filter(t => t.pnl === null);
  log('sys', `Daily reset complete. New baseline: $${realDailyStartEquity.toFixed(2)}`);
}

// ═══════════════════════════════════════════════════════════════════
// SCALPING ENGINE
// ═══════════════════════════════════════════════════════════════════
//
// Scalping targets small, fast moves — 0.2-0.5% — in and out quickly.
// Uses 1-minute bars for precision, requires strong confluence of:
//   - Price action (candle structure)
//   - Momentum (VWAP, EMA9, EMA21)
//   - Volume (must be above average — confirms intent)
//   - Microstructure (bid/ask spread, candle wicks)
//   - Regime (only scalp when SPY is not in a sharp downtrend)
// Exits are managed every 5 seconds, never holds more than 8 minutes.
// ═══════════════════════════════════════════════════════════════════

/**
 * Fetch 1-minute bars for scalping precision
 */
async function fetchScalpBars(symbol, limit = 30) {
  try {
    const start = new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString(); // last 2 hours
    const url   = `${ALPACA_DATA_BASE}/v2/stocks/${symbol}/bars?timeframe=1Min&start=${start}&limit=${limit}&feed=iex`;
    const data  = await alpacaFetch(url);
    if (data.bars && data.bars.length >= 10) return data.bars;
  } catch(e) {}

  // Yahoo Finance fallback (1m bars, last 1 day)
  try {
    const fetch = await getFetch();
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${symbol}?interval=1m&range=1d`;
    const res  = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
    const data = await res.json();
    const result = data?.chart?.result?.[0];
    if (!result) return null;
    const timestamps = result.timestamp || [];
    const q = result.indicators?.quote?.[0] || {};
    const bars = timestamps.map((t, i) => ({
      t: new Date(t * 1000).toISOString(),
      o: q.open?.[i], h: q.high?.[i], l: q.low?.[i],
      c: q.close?.[i], v: q.volume?.[i] || 0,
    })).filter(b => b.c != null && b.h != null).slice(-limit);
    if (bars.length >= 10) return bars;
  } catch(e) {}

  return null;
}

/**
 * Fetch latest trade price (most current — better than bar close for scalping)
 */
async function fetchLatestTrade(symbol) {
  try {
    const data = await alpacaFetch(`${ALPACA_DATA_BASE}/v2/stocks/${symbol}/trades/latest?feed=iex`);
    return data?.trade?.p || null;
  } catch(e) { return null; }
}

/**
 * Scalp signal generator — uses 1-minute precision
 *
 * Scoring system (max ~130 points, need 70 to fire):
 *   Momentum  : EMA9 vs EMA21, price vs VWAP, candle body direction
 *   Pressure  : Volume spike, consecutive bullish/bearish candles
 *   Structure : Higher highs/lower lows, candle wicks (rejection)
 *   Volatility: ATR expanding (means a move is starting)
 */
function generateScalpSignal(sym, bars1m) {
  if (!bars1m || bars1m.length < 15) {
    return { signal: 'HOLD', confidence: 0, score: 0, reasons: ['Need 15+ 1m bars'] };
  }

  const closes  = bars1m.map(b => b.c);
  const highs   = bars1m.map(b => b.h);
  const lows    = bars1m.map(b => b.l);
  const opens   = bars1m.map(b => b.o);
  const volumes = bars1m.map(b => b.v);
  const price   = closes[closes.length - 1];
  const prev    = closes[closes.length - 2];
  const pprev   = closes[closes.length - 3];

  let buy = 0, sell = 0;
  const reasons = [];

  // ── 1. EMA 9 / 21 — primary trend on 1m ──
  const e9  = ema(closes, 9);
  const e21 = ema(closes, 21);
  const pe9  = ema(closes.slice(0, -1), 9);
  const pe21 = ema(closes.slice(0, -1), 21);

  if (e9 > e21)  { buy  += 20; reasons.push(`EMA9 > EMA21 (${e9.toFixed(2)} > ${e21.toFixed(2)})`); }
  else           { sell += 20; reasons.push(`EMA9 < EMA21 (${e9.toFixed(2)} < ${e21.toFixed(2)})`); }

  // EMA crossover on 1m = strong immediate signal
  if (pe9 <= pe21 && e9 > e21) { buy  += 20; reasons.push('🔀 EMA9/21 bullish cross on 1m'); }
  if (pe9 >= pe21 && e9 < e21) { sell += 20; reasons.push('🔀 EMA9/21 bearish cross on 1m'); }

  // ── 2. VWAP — most important intraday level ──
  const vw = vwap(bars1m);
  const vwapDist = (price - vw) / vw;

  if (vwapDist > 0.001) {
    buy  += 15;
    reasons.push(`Above VWAP by ${(vwapDist*100).toFixed(2)}% ($${vw.toFixed(2)})`);
  } else if (vwapDist < -0.001) {
    sell += 15;
    reasons.push(`Below VWAP by ${(Math.abs(vwapDist)*100).toFixed(2)}% ($${vw.toFixed(2)})`);
  } else {
    // Sitting exactly on VWAP — wait for a break
    reasons.push(`At VWAP ($${vw.toFixed(2)}) — waiting for break`);
    return { signal: 'HOLD', confidence: 0, score: 0, reasons };
  }

  // ── 3. Price momentum — last 3 candles ──
  const last3Bull = price > prev && prev > pprev;
  const last3Bear = price < prev && prev < pprev;

  if (last3Bull) { buy  += 15; reasons.push('3 consecutive bullish closes'); }
  if (last3Bear) { sell += 15; reasons.push('3 consecutive bearish closes'); }

  // ── 4. Current candle body direction and size ──
  const lastBar   = bars1m[bars1m.length - 1];
  const body      = Math.abs(lastBar.c - lastBar.o);
  const range     = lastBar.h - lastBar.l;
  const bodyRatio = range > 0 ? body / range : 0;

  // Strong body (>60% of range) = conviction
  if (bodyRatio > 0.6) {
    if (lastBar.c > lastBar.o) { buy  += 10; reasons.push(`Strong bull candle (body ${(bodyRatio*100).toFixed(0)}%)`); }
    else                       { sell += 10; reasons.push(`Strong bear candle (body ${(bodyRatio*100).toFixed(0)}%)`); }
  }

  // ── 5. Wick rejection — pin bars show reversal or continuation ──
  const upperWick = lastBar.h - Math.max(lastBar.c, lastBar.o);
  const lowerWick = Math.min(lastBar.c, lastBar.o) - lastBar.l;

  // Long lower wick (buyers rejected the lows) = bullish
  if (lowerWick > body * 1.5 && lowerWick > 0.01) {
    buy  += 10;
    reasons.push(`Lower wick rejection (${(lowerWick/price*100).toFixed(2)}%)`);
  }
  // Long upper wick (sellers rejected the highs) = bearish
  if (upperWick > body * 1.5 && upperWick > 0.01) {
    sell += 10;
    reasons.push(`Upper wick rejection (${(upperWick/price*100).toFixed(2)}%)`);
  }

  // ── 6. Volume confirmation — must be above 1.2x average ──
  const avgVol = volumes.slice(-20).reduce((a, b) => a + b, 0) / Math.min(20, volumes.length);
  const curVol = volumes[volumes.length - 1];
  const volRatio = avgVol > 0 ? curVol / avgVol : 1;

  if (volRatio >= 1.5) {
    // High volume — strongly confirms whichever direction
    buy > sell ? buy += 20 : sell += 20;
    reasons.push(`Volume ${volRatio.toFixed(1)}x avg — strong confirmation`);
  } else if (volRatio >= 1.2) {
    buy > sell ? buy += 10 : sell += 10;
    reasons.push(`Volume ${volRatio.toFixed(1)}x avg — moderate confirmation`);
  } else if (volRatio < 0.7) {
    // Low volume scalp = dangerous, likely to reverse
    reasons.push(`⚠ Low volume (${volRatio.toFixed(1)}x) — scalp rejected`);
    return { signal: 'HOLD', confidence: 0, score: 0, reasons };
  }

  // ── 7. ATR expansion — volatility must be alive for scalping ──
  const atrVal = atr(bars1m, 10);
  const atrPct = price > 0 ? (atrVal / price) * 100 : 0;

  if (atrPct < 0.05) {
    // Market is frozen — spread will eat any gain
    reasons.push(`⚠ ATR too low (${atrPct.toFixed(3)}%) — market frozen`);
    return { signal: 'HOLD', confidence: 0, score: 0, reasons };
  }
  if (atrPct > 0.08) {
    buy > sell ? buy += 10 : sell += 10;
    reasons.push(`ATR ${atrPct.toFixed(3)}% — good volatility`);
  }

  // ── 8. Higher highs / lower lows structure ──
  const recentHighs = highs.slice(-5);
  const recentLows  = lows.slice(-5);
  const higherHighs = recentHighs[4] > recentHighs[3] && recentHighs[3] > recentHighs[2];
  const lowerLows   = recentLows[4]  < recentLows[3]  && recentLows[3]  < recentLows[2];

  if (higherHighs && buy > sell)  { buy  += 10; reasons.push('Structure: higher highs forming'); }
  if (lowerLows   && sell > buy)  { sell += 10; reasons.push('Structure: lower lows forming'); }

  // ── 9. RSI on 1m — only for extreme readings ──
  const r1m = rsi(closes, 9); // shorter period for 1m scalping
  if (r1m < 25)      { buy  += 10; reasons.push(`1m RSI oversold (${r1m.toFixed(0)})`); }
  else if (r1m > 75) { sell += 10; reasons.push(`1m RSI overbought (${r1m.toFixed(0)})`); }

  // ── Final score ──
  const total      = buy + sell;
  const confidence = total > 0 ? Math.round(Math.max(buy, sell) / total * 100) : 0;
  const minScore   = CONFIG.scalpMinScore;

  if (buy >= minScore && buy > sell * 1.5)  {
    return { signal: 'BUY',  confidence, score: buy,  reasons, atr: atrVal, vwap: vw, rsi: r1m };
  }
  if (sell >= minScore && sell > buy * 1.5) {
    return { signal: 'SELL', confidence, score: sell, reasons, atr: atrVal, vwap: vw, rsi: r1m };
  }

  reasons.push(`Score insufficient (buy=${buy} sell=${sell} need ${minScore})`);
  return { signal: 'HOLD', confidence, score: Math.max(buy, sell), reasons, atr: atrVal, vwap: vw };
}

/**
 * Enter a scalp position (long or short)
 */
async function enterScalp(sym, price, sigInfo, direction = 'long') {
  if (checkCircuitBreaker()) return;
  if (!isMarketOpen()) { log('scalp', `Scalp blocked — market closed`); return; }
  if (scalpPositions[sym]) { log('scalp', `Already in scalp position for ${sym}`); return; }
  if (Object.keys(scalpPositions).length >= CONFIG.scalpMaxPositions) {
    log('scalp', `Max scalp positions (${CONFIG.scalpMaxPositions}) reached`);
    return;
  }

  // Position sizing — flat % of portfolio, no ATR sizing for scalps (moves too small)
  const budget = portfolio * CONFIG.scalpPositionPct;
  const qty    = Math.floor(budget / price);
  if (qty < 1) { log('scalp', `Scalp ${sym}: qty too small (budget=$${budget.toFixed(2)} price=$${price.toFixed(2)})`); return; }

  const cost = qty * price;
  if (cost > portfolio) { log('scalp', `Scalp ${sym}: insufficient cash`); return; }

  // Tight stops — scalping lives and dies by discipline
  const atrVal   = sigInfo.atr || price * 0.002;
  const slOffset = Math.max(atrVal * 0.5, price * CONFIG.scalpSlPct);
  const tpOffset = Math.max(atrVal * 1.0, price * CONFIG.scalpTpPct);

  const stopPrice = direction === 'long'  ? price - slOffset : price + slOffset;
  const tpPrice   = direction === 'long'  ? price + tpOffset : price - tpOffset;

  // Place order
  const side = direction === 'long' ? 'buy' : 'sell';
  if (isSimMode() || (CONFIG.alpacaKey && CONFIG.mode === 'alpaca')) {
    try { await placeOrder(sym, qty, side); }
    catch(e) { log('error', `Scalp order failed ${sym}: ${e.message}`); return; }
  }

  portfolio -= direction === 'long' ? cost : 0;

  scalpPositions[sym] = {
    entryPrice: price, qty, cost,
    direction,
    entryTime:  new Date(),
    highWater:  price,
    lowWater:   price,
    stopPrice,
    tpPrice,
    atrAtEntry: atrVal,
    trailingActive: false,
    sigInfo,
  };

  const slPct = ((Math.abs(price - stopPrice) / price) * 100).toFixed(2);
  const tpPct = ((Math.abs(tpPrice - price)   / price) * 100).toFixed(2);
  log('scalp', `⚡ ${direction.toUpperCase()} SCALP ${qty}x ${sym} @ $${price.toFixed(2)} | SL=$${stopPrice.toFixed(2)}(-${slPct}%) TP=$${tpPrice.toFixed(2)}(+${tpPct}%) | conf=${sigInfo.confidence}%`);

  trades.push({ time: new Date(), sym, side: direction === 'long' ? 'SCALP_BUY' : 'SCALP_SHORT', qty, price, pnl: null, reason: 'SCALP', confidence: sigInfo.confidence });
  await syncTrade({ sym, side: direction === 'long' ? 'SCALP_BUY' : 'SCALP_SHORT', qty, price, pnl: null, reason: 'SCALP', confidence: sigInfo.confidence });
  await sendDiscordAlert('scalp_entry', sym, qty, price, undefined, direction, sigInfo, { stopPrice, tpPrice, atrVal });
  await syncLog('buy', `⚡ SCALP ${direction.toUpperCase()} ${qty}x ${sym} @ $${price.toFixed(2)} SL=$${stopPrice.toFixed(2)} TP=$${tpPrice.toFixed(2)} conf=${sigInfo.confidence}%`);
}

/**
 * Exit a scalp position with reason
 */
async function exitScalp(sym, price, reason) {
  const pos = scalpPositions[sym];
  if (!pos) return;

  const { qty, entryPrice, direction } = pos;
  const side = direction === 'long' ? 'sell' : 'buy';

  if (isSimMode() || (CONFIG.alpacaKey && CONFIG.mode === 'alpaca')) {
    try { await placeOrder(sym, qty, side); }
    catch(e) { log('error', `Scalp exit failed ${sym}: ${e.message}`); return; }
  }

  const pnl = direction === 'long'
    ? (price - entryPrice) * qty
    : (entryPrice - price) * qty;

  portfolio += direction === 'long' ? qty * price : pnl;
  pnl > 0 ? (totalWins++, scalpWins++) : (totalLosses++, scalpLosses++);

  delete scalpPositions[sym];

  const holdSecs = Math.round((Date.now() - new Date(pos.entryTime).getTime()) / 1000);
  const icons = { SCALP_TP:'🎯', SCALP_SL:'🛑', SCALP_TRAIL:'📉', SCALP_TIME:'⏰', SCALP_REVERSE:'↩️', SCALP_MANUAL:'🖐' };
  const icon  = icons[reason] || '📤';
  log('scalp', `${icon} SCALP EXIT ${qty}x ${sym} @ $${price.toFixed(2)} | P&L: ${pnl>=0?'+':''}$${pnl.toFixed(2)} | Held: ${holdSecs}s | (${reason})`);

  trades.push({ time: new Date(), sym, side: 'SCALP_EXIT', qty, price, pnl, reason });
  await syncTrade({ sym, side: 'SCALP_EXIT', qty, price, pnl, reason });
  await sendDiscordAlert('scalp_exit', sym, qty, price, pnl, reason, pos.sigInfo);
  await syncLog('sell', `${icon} SCALP EXIT ${qty}x ${sym} @ $${price.toFixed(2)} P&L=${pnl>=0?'+':''}$${pnl.toFixed(2)} (${reason}) ${holdSecs}s`);
  await syncAll();
}

/**
 * Manage all open scalp positions — called every 5 seconds
 * Precision exit logic: TP, SL, trailing, time stop, signal reversal
 */
async function manageScalpPositions() {
  if (Object.keys(scalpPositions).length === 0) return;

  for (const [sym, pos] of Object.entries(scalpPositions)) {
    // Get latest price — use trade price for max precision
    let price = await fetchLatestTrade(sym);
    if (!price) {
      // Fallback to last bar close
      const bars = await fetchScalpBars(sym, 3);
      price = bars ? bars[bars.length - 1].c : null;
    }
    if (!price) continue;

    const { direction, entryPrice, stopPrice, tpPrice, highWater, lowWater } = pos;
    const holdMins = (Date.now() - new Date(pos.entryTime).getTime()) / 60000;

    // Update high/low water marks
    if (direction === 'long'  && price > pos.highWater) scalpPositions[sym].highWater = price;
    if (direction === 'short' && price < pos.lowWater)  scalpPositions[sym].lowWater  = price;

    // ── 1. Take profit ──
    const hitTP = direction === 'long' ? price >= tpPrice : price <= tpPrice;
    if (hitTP) { await exitScalp(sym, price, 'SCALP_TP'); continue; }

    // ── 2. Stop loss ──
    const hitSL = direction === 'long' ? price <= stopPrice : price >= stopPrice;
    if (hitSL) { await exitScalp(sym, price, 'SCALP_SL'); continue; }

    // ── 3. Trailing stop — activates once 50% of TP distance is covered ──
    const tpDist  = Math.abs(tpPrice - entryPrice);
    const moved   = direction === 'long' ? price - entryPrice : entryPrice - price;
    if (moved >= tpDist * 0.5) {
      if (!scalpPositions[sym].trailingActive) {
        scalpPositions[sym].trailingActive = true;
        log('scalp', `${sym} trailing stop activated`);
      }
      // Trail at CONFIG.scalpTrailingPct below/above the high/low water
      const trail = direction === 'long'
        ? scalpPositions[sym].highWater * (1 - CONFIG.scalpTrailingPct)
        : scalpPositions[sym].lowWater  * (1 + CONFIG.scalpTrailingPct);
      // Ratchet the stop — only moves in favor, never against
      if (direction === 'long'  && trail > scalpPositions[sym].stopPrice) scalpPositions[sym].stopPrice = trail;
      if (direction === 'short' && trail < scalpPositions[sym].stopPrice) scalpPositions[sym].stopPrice = trail;
    }

    // ── 4. Time stop — scalps must not sit idle ──
    if (holdMins >= CONFIG.scalpMaxHoldMins) {
      log('scalp', `⏰ Scalp time stop: ${sym} held ${holdMins.toFixed(1)}m`);
      await exitScalp(sym, price, 'SCALP_TIME');
      continue;
    }

    // ── 5. Signal reversal — exit if 1m signal flips strongly against us ──
    const bars1m = await fetchScalpBars(sym, 20);
    if (bars1m) {
      const sig = generateScalpSignal(sym, bars1m);
      const reversed = (direction === 'long'  && sig.signal === 'SELL' && sig.score >= CONFIG.scalpMinScore)
                    || (direction === 'short' && sig.signal === 'BUY'  && sig.score >= CONFIG.scalpMinScore);
      if (reversed) {
        log('scalp', `↩️ Signal reversal: ${sym} ${direction} → exiting`);
        await exitScalp(sym, price, 'SCALP_REVERSE');
        continue;
      }
    }

    // Log current scalp status
    const pnlNow = direction === 'long' ? (price - entryPrice) * pos.qty : (entryPrice - price) * pos.qty;
    log('scalp', `${sym} ${direction.toUpperCase()} ${pos.qty}x @ $${entryPrice.toFixed(2)} → $${price.toFixed(2)} | P&L: ${pnlNow>=0?'+':''}$${pnlNow.toFixed(2)} | ${holdMins.toFixed(1)}m | SL=$${pos.stopPrice.toFixed(2)} TP=$${tpPrice.toFixed(2)}`);
  }
}

/**
 * Scalp scan — runs every 5 seconds on SCALP_SYMBOLS only
 */
async function runScalpScan() {
  if (!CONFIG.scalpMode) return;
  if (!isMarketOpen()) return;
  if (checkCircuitBreaker()) return;

  // First manage existing positions (most important)
  await manageScalpPositions();

  // Check if we have room for new positions
  const openScalps = Object.keys(scalpPositions).length;
  if (openScalps >= CONFIG.scalpMaxPositions) return;

  // Scan scalp symbols for entries
  for (const sym of CONFIG.scalpSymbols) {
    if (scalpPositions[sym]) continue; // already in position
    if (positions[sym] || shortPositions[sym]) continue; // don't scalp what we're swinging

    try {
      const bars1m = await fetchScalpBars(sym, 30);
      if (!bars1m || bars1m.length < 15) continue;

      const sig = generateScalpSignal(sym, bars1m);
      const price = bars1m[bars1m.length - 1].c;

      if (sig.score > 0) {
        log('scalp', `${sym} @ $${price.toFixed(2)} → ${sig.signal} (score:${sig.score} conf:${sig.confidence}%)`);
      }

      if (sig.signal === 'BUY') {
        await enterScalp(sym, price, sig, 'long');
      } else if (sig.signal === 'SELL' && SHORTING_ENABLED) {
        await enterScalp(sym, price, sig, 'short');
      }
    } catch(e) {
      log('error', `Scalp scan error ${sym}: ${e.message}`);
    }
  }
}

// ═══════════════════════════════════════════════════════════════════
// END SCALPING ENGINE
// ═══════════════════════════════════════════════════════════════════
// ─────────────────────────────────────────────
log('sys', '══════════════════════════════════════════');
log('sys', '   TradeCore Pro — Upgraded Engine v4     ');
log('sys', '   + 10s scan + live dashboard refresh    ');
log('sys', '══════════════════════════════════════════');
log('sys', `Mode: ${CONFIG.mode.toUpperCase()} | Paper: ${CONFIG.alpacaPaper} | Strategy: ${CONFIG.strategy}`);
if (isSimMode()) log('sys', '🎮 SIMULATION MODE — orders are simulated, no real/paper trades placed');
if (BYPASS_HOURS) log('sys', '⚠️  BYPASS_HOURS=true — trading outside market hours (TEST MODE)');
if (CONFIG.scalpMode) log('sys', `⚡ SCALP MODE ACTIVE — symbols: ${CONFIG.scalpSymbols.join(',')} | TP:${CONFIG.scalpTpPct*100}% SL:${CONFIG.scalpSlPct*100}% MaxHold:${CONFIG.scalpMaxHoldMins}m`);
log('sys', `Symbols: ${CONFIG.symbols.join(', ')}`);
log('sys', `Risk: SL=${CONFIG.stopLossPct*100}% TP=${CONFIG.takeProfitPct*100}% Trailing=${CONFIG.trailingStop} MaxDailyLoss=${CONFIG.maxDailyLossPct*100}%`);
log('sys', `Filters: Trend=${CONFIG.trendFilter} Volume=${CONFIG.volumeFilter} Regime=${CONFIG.regimeFilter} Corr=${CONFIG.correlationFilter}`);
log('sys', `Current session: ${getCurrentSession()}`);

// ═══════════════════════════════════════════════════════════════════
// SPEED & ACCURACY LAYER
// All of these run before/during signal generation to make entries
// faster and more precise.
// ═══════════════════════════════════════════════════════════════════

// ── 1. Bar cache ──
// Avoid re-fetching the same bars multiple times per scan cycle.
// Bars change at most every minute, so an 8-second cache is safe.
const barCache = new Map(); // `SYM_TF` → { bars, ts }
const BAR_CACHE_TTL = 8000;

async function fetchBarsCached(symbol, timeframe, limit) {
  const key = `${symbol}_${timeframe}`;
  const hit  = barCache.get(key);
  if (hit && Date.now() - hit.ts < BAR_CACHE_TTL) return hit.bars;
  const bars = await fetchBars(symbol, timeframe, limit);
  if (bars) barCache.set(key, { bars, ts: Date.now() });
  return bars;
}

// ── 2. Parallel bar fetching ──
// Old: fetched symbols one-by-one (symbol 15 waited for symbols 1-14)
// New: fetch all symbols simultaneously → scan time drops ~10x
async function fetchAllBarsParallel(symbols) {
  const results = await Promise.allSettled(
    symbols.map(async sym => {
      const [bars5m, bars15m] = await Promise.all([
        fetchBarsCached(sym, '5Min',  60),
        fetchBarsCached(sym, '15Min', 40),
      ]);
      return { sym, bars5m, bars15m };
    })
  );
  return results
    .filter(r => r.status === 'fulfilled' && r.value.bars5m)
    .map(r => r.value);
}

// ── 3. Latest trade price (real-time, not bar close) ──
// Bar closes can be up to 5 minutes stale. For entry, we want
// the actual last traded price from the tape.
const latestPriceCache = new Map(); // sym → { price, ts }
async function fetchLatestPrice(symbol) {
  const cached = latestPriceCache.get(symbol);
  if (cached && Date.now() - cached.ts < 3000) return cached.price; // 3s cache

  try {
    const data = await alpacaFetch(`${ALPACA_DATA_BASE}/v2/stocks/${symbol}/trades/latest?feed=iex`);
    const price = data?.trade?.p;
    if (price) {
      latestPriceCache.set(symbol, { price: +price, ts: Date.now() });
      return +price;
    }
  } catch (e) {}
  return null;
}

// ── 4. Bid/ask spread check ──
// If the spread is wider than your TP target, you're losing money
// before the order even fills. Skip these.
async function spreadIsAcceptable(symbol, price) {
  try {
    const data  = await alpacaFetch(`${ALPACA_DATA_BASE}/v2/stocks/${symbol}/quotes/latest?feed=iex`);
    const quote = data?.quote;
    if (!quote?.ap || !quote?.bp) return true; // can't check — allow
    const spreadPct = (quote.ap - quote.bp) / price;
    const maxSpread = 0.0015; // 0.15% max spread — tighter than any TP target
    if (spreadPct > maxSpread) {
      log('filter', `${symbol} spread too wide: ${(spreadPct*100).toFixed(3)}% — skipping`);
      return false;
    }
    return true;
  } catch (e) { return true; }
}

// ── 5. Signal confirmation gate ──
// Requires a signal to appear on 2 CONSECUTIVE scans before acting.
// Eliminates single-candle spikes that look like signals but reverse
// immediately. The most effective false-positive filter possible.
const pendingSignals = new Map(); // sym → { signal, count, sigInfo }

function confirmSignal(sym, sig) {
  if (sig.signal === 'HOLD') {
    pendingSignals.delete(sym);
    return false;
  }
  const prev = pendingSignals.get(sym);
  if (prev && prev.signal === sig.signal) {
    prev.count++;
    prev.sigInfo = sig;
    pendingSignals.set(sym, prev);
    if (prev.count >= 3) {  // 3 consecutive scans = ~90 seconds of signal holding
      pendingSignals.delete(sym);
      log('signal', `✅ ${sym} signal CONFIRMED after ${prev.count} scans → ${sig.signal} (conf:${sig.confidence}%)`);
      return true;
    }
    log('signal', `⏳ ${sym} signal pending (${sig.signal} x${prev.count}/3)`);
    return false;
  }
  pendingSignals.set(sym, { signal: sig.signal, count: 1, sigInfo: sig });
  log('signal', `⏳ ${sym} new signal (${sig.signal} conf:${sig.confidence}%) — need 2 more confirmations`);
  return false;
}

// ── 6. Day bias ──
// SPY's gap at open tells us if institutional money is buying or selling.
// Align trades with the day's institutional flow for higher win rate.
let dayBias = 'neutral'; // 'bullish' | 'bearish' | 'neutral'
let dayBiasBonus = 0;

async function updateDayBias() {
  try {
    const bars = await fetchBars('SPY', '1Day', 3);
    if (!bars || bars.length < 2) return;
    const prevClose = bars[bars.length - 2].c;
    const todayOpen = bars[bars.length - 1].o || prevClose;
    const gapPct    = (todayOpen - prevClose) / prevClose;

    if      (gapPct >  0.003) { dayBias = 'bullish'; dayBiasBonus = Math.min(20, Math.round(gapPct * 3000)); }
    else if (gapPct < -0.003) { dayBias = 'bearish'; dayBiasBonus = Math.min(20, Math.round(Math.abs(gapPct) * 3000)); }
    else                      { dayBias = 'neutral';  dayBiasBonus = 0; }

    log('sys', `Day bias: ${dayBias.toUpperCase()} (SPY gap ${gapPct >= 0 ? '+' : ''}${(gapPct*100).toFixed(2)}%${dayBiasBonus ? `, bonus ±${dayBiasBonus}pts` : ''})`);
  } catch (e) {}
}

function applyDayBias(sig) {
  if (dayBias === 'neutral' || !dayBiasBonus) return sig;
  if (dayBias === 'bullish' && sig.signal === 'BUY') {
    return { ...sig, score: (sig.score||0) + dayBiasBonus, reasons: [`📈 Day bias bullish (+${dayBiasBonus}pts)`, ...(sig.reasons||[])] };
  }
  if (dayBias === 'bearish' && sig.signal === 'SELL') {
    return { ...sig, score: (sig.score||0) + dayBiasBonus, reasons: [`📉 Day bias bearish (+${dayBiasBonus}pts)`, ...(sig.reasons||[])] };
  }
  // Fighting the bias — dampen
  const dampened = Math.max(0, (sig.score||0) - dayBiasBonus);
  return { ...sig, score: dampened, confidence: Math.max(0, (sig.confidence||0) - 8), reasons: [`⚠ Against day bias (${dayBias}) -${dayBiasBonus}pts`, ...(sig.reasons||[])] };
}

// ── 7. Opening Range Breakout (ORB) ──
// The first 30 minutes of trading (9:30-10:00 AM ET) establishes
// the day's opening range. A break ABOVE the high = strong bull signal.
// A break BELOW the low = strong bear signal.
// Used as a score modifier on top of the main signal.
const openingRanges = {}; // sym → { high, low }

async function captureOpeningRange(symbol) {
  const et   = getETTime();
  const mins = et.getHours() * 60 + et.getMinutes();
  if (mins < 600 || mins > 606) return; // only capture at 10:00-10:06 AM ET
  if (openingRanges[symbol]) return;    // already captured today

  try {
    const bars = await fetchBars(symbol, '5Min', 10);
    if (!bars || bars.length < 6) return;
    const orbBars = bars.slice(0, 6); // first 6 × 5min = 30 minutes
    openingRanges[symbol] = {
      high: Math.max(...orbBars.map(b => b.h)),
      low:  Math.min(...orbBars.map(b => b.l)),
    };
    log('sys', `ORB ${symbol}: $${openingRanges[symbol].low.toFixed(2)} — $${openingRanges[symbol].high.toFixed(2)}`);
  } catch (e) {}
}

function orbScoreBonus(symbol, price, direction) {
  const orb = openingRanges[symbol];
  if (!orb) return 0;
  if (direction === 'buy'  && price > orb.high) return 20; // ORB breakout ↑
  if (direction === 'sell' && price < orb.low)  return 20; // ORB breakdown ↓
  if (direction === 'buy'  && price < orb.low)  return -15; // buying into breakdown
  if (direction === 'sell' && price > orb.high) return -15; // shorting into breakout
  return 0;
}

// ── 8. Smart limit order ──
// For swing trades: attempt a limit order just inside the spread
// for better fill price. Falls back to market after 4 seconds.
// For scalps: always market (speed > price improvement).
async function placeSmartOrder(symbol, qty, side, isScalp = false) {
  if (isSimMode()) return simOrder(symbol, qty, side);
  if (isScalp || !CONFIG.alpacaKey) return placeOrder(symbol, qty, side);

  try {
    const fetch = await getFetch();
    // Get current quote
    const qd    = await alpacaFetch(`${ALPACA_DATA_BASE}/v2/stocks/${symbol}/quotes/latest?feed=iex`);
    const quote = qd?.quote;

    if (!quote?.ap || !quote?.bp) return placeOrder(symbol, qty, side);

    // Limit price just inside the spread — virtually guaranteed fill on liquid stocks
    const limitPrice = side === 'buy'
      ? +(quote.ap + 0.01).toFixed(2) // 1 cent above ask
      : +(quote.bp - 0.01).toFixed(2); // 1 cent below bid

    const orderData = await alpacaFetch(`${ALPACA_BASE()}/v2/orders`, {
      method: 'POST',
      body: JSON.stringify({
        symbol, qty: String(qty), side,
        type: 'limit', limit_price: String(limitPrice),
        time_in_force: 'day',
      }),
    });

    if (!orderData.id) return placeOrder(symbol, qty, side); // placement failed → market

    log('order', `LIMIT ${side.toUpperCase()} ${qty}x ${symbol} @ $${limitPrice} | ID: ${orderData.id}`);

    // Wait up to 4 seconds for fill
    await new Promise(r => setTimeout(r, 4000));
    const status = await alpacaFetch(`${ALPACA_BASE()}/v2/orders/${orderData.id}`);

    if (status.status === 'filled') {
      const fillPrice = +status.filled_avg_price;
      log('order', `✅ Limit filled: ${symbol} @ $${fillPrice} (saved ~$${((Math.abs(fillPrice - limitPrice)) * qty).toFixed(2)})`);
      return status;
    }

    // Not filled in time — cancel and market
    log('order', `Limit not filled (${symbol}) — cancelling and placing market order`);
    try { await alpacaFetch(`${ALPACA_BASE()}/v2/orders/${orderData.id}`, { method: 'DELETE' }); } catch(e) {}
    return placeOrder(symbol, qty, side);

  } catch (e) {
    log('warn', `Smart order error ${symbol}: ${e.message} — falling back to market`);
    return placeOrder(symbol, qty, side);
  }
}

// ── 9. Data pre-warming ──
// Fetch all bar data on startup so the first scan fires immediately
// with full data instead of waiting for serial fetches.
async function prewarmData() {
  log('sys', `Pre-warming bar cache for ${CONFIG.symbols.length} symbols…`);
  const t = Date.now();
  await fetchAllBarsParallel(CONFIG.symbols);
  log('sys', `Bar cache ready in ${Date.now() - t}ms — first scan will fire immediately`);
}

// ═══════════════════════════════════════════════════════════════════
// END SPEED & ACCURACY LAYER
// ═══════════════════════════════════════════════════════════════════
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
  15000 // never faster than 15s
);
const PRICE_SYNC_INTERVAL_MS = 15000; // price updates every 15s (was 10s)
const EQUITY_SNAPSHOT_INTERVAL_MS = 2 * 60 * 1000; // equity curve point every 2 min
let lastEquitySnapshot = 0;

log('sys', `Full scan every ${FULL_SCAN_INTERVAL_MS/1000}s | Price sync every ${PRICE_SYNC_INTERVAL_MS/1000}s`);

async function tick() {
  if (scanInProgress) return;
  scanInProgress = true;
  try {
    const now = Date.now();

    // ── Simulation mode — use bar replay instead of live market ──
    if (isSimMode()) {
      if (now - lastFullScan >= FULL_SCAN_INTERVAL_MS) {
        lastFullScan = now;
        await runSimScan();
      }
      return; // skip all live market logic in sim mode
    }

    // ── Live / Paper mode ──

    // Screener every 3 minutes
    if (isMarketOpen() && (now - lastScreenerRun >= SCREENER_INTERVAL_MS)) {
      lastScreenerRun    = now;
      screenerCandidates = await runMarketScreener();
    }

    // Scalp scan every 5s
    if (CONFIG.scalpMode && (now - lastScalpScan >= SCALP_SCAN_INTERVAL_MS)) {
      lastScalpScan = now;
      await runScalpScan();
    }

    // Full swing scan
    if (now - lastFullScan >= FULL_SCAN_INTERVAL_MS) {
      lastFullScan = now;
      await runScan();
    } else {
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

// Daily tasks
cron.schedule('5 16 * * 1-5',  sendDailySummary, { timezone: 'America/New_York' });
cron.schedule('55 8 * * 1-5',  storePrevClose,   { timezone: 'America/New_York' });
cron.schedule('31 9 * * 1-5',  updateDayBias,    { timezone: 'America/New_York' }); // run at market open

// Startup — prewarm data then scan immediately
loadRemoteConfig().then(async () => {
  await updateDayBias();
  // Run screener first so first scan has candidates ready
  if (isMarketOpen()) {
    screenerCandidates = await runMarketScreener();
    lastScreenerRun    = Date.now();
  }
  await prewarmData();
  await runScan();
  lastFullScan = Date.now();
});
setTimeout(syncPricesOnly, 5000);

// ─────────────────────────────────────────────
// HTTP SERVER + DISCORD SLASH COMMANDS
// ─────────────────────────────────────────────
//
// Discord sends a POST to your Render URL whenever someone types
// a slash command in Discord. We verify the request signature,
// parse the command, execute it, and respond — all in <300ms.
//
// Slash commands supported:
//   /exit [symbol]     — exit a long or short position immediately
//   /status            — show all open positions + P&L
//   /pause             — engage circuit breaker (stops new trades)
//   /resume            — reset circuit breaker
//   /sim               — toggle simulation mode
//   /positions         — list all open positions
//

const nacl = require('tweetnacl');

// Verify Discord's Ed25519 signature — required or Discord rejects the endpoint
function verifyDiscordRequest(rawBody, signature, timestamp) {
  if (!CONFIG.discordPublicKey) {
    log('warn', 'Discord: DISCORD_PUBLIC_KEY not set — skipping verification (unsafe)');
    return true;
  }
  if (!signature || !timestamp) {
    log('warn', 'Discord: missing signature or timestamp headers');
    return false;
  }
  try {
    const key = Buffer.from(CONFIG.discordPublicKey.trim(), 'hex');
    const msg = Buffer.from(timestamp + rawBody, 'utf8');
    const sig = Buffer.from(signature.trim(), 'hex');

    if (key.length !== 32) {
      log('error', `Discord: public key wrong length (${key.length} bytes, expected 32) — check DISCORD_PUBLIC_KEY`);
      return false;
    }
    if (sig.length !== 64) {
      log('error', `Discord: signature wrong length (${sig.length} bytes, expected 64)`);
      return false;
    }

    const valid = nacl.sign.detached.verify(new Uint8Array(msg), new Uint8Array(sig), new Uint8Array(key));
    if (!valid) log('warn', 'Discord: signature verification failed');
    return valid;
  } catch(e) {
    log('error', `Discord: verification error — ${e.message}`);
    return false;
  }
}

// Format a position as a Discord embed field value
function formatPosition(sym, pos, isShort = false) {
  const cur    = priceHistory5m[sym]?.[priceHistory5m[sym].length - 1] || pos.entryPrice;
  const pnl    = isShort
    ? (pos.entryPrice - cur) * (pos.qtyRemaining || pos.qty)
    : (cur - pos.entryPrice) * (pos.qtyRemaining || pos.qty);
  const pct    = ((Math.abs(pnl) / pos.cost || 1) * 100).toFixed(2);
  const held   = Math.round((Date.now() - new Date(pos.entryTime).getTime()) / 60000);
  const dir    = isShort ? '🔴 SHORT' : '🟢 LONG';
  return `${dir} ${pos.qtyRemaining || pos.qty}x @ $${pos.entryPrice.toFixed(2)} → $${cur.toFixed(2)}\nP&L: ${pnl>=0?'+':''}$${pnl.toFixed(2)} (${pnl>=0?'+':''}${pct}%) | Held: ${held}m`;
}

// Execute a slash command — returns a Discord interaction response object
async function handleSlashCommand(commandName, options) {
  const opt = (name) => options?.find(o => o.name === name)?.value;

  // ── /exit [symbol] ──
  if (commandName === 'exit') {
    const sym = (opt('symbol') || '').toUpperCase().trim();
    if (!sym) return { content: '⚠ Usage: `/exit AAPL`' };

    // Check all position types
    if (positions[sym]) {
      const pos   = positions[sym];
      const price = priceHistory5m[sym]?.[priceHistory5m[sym].length - 1] || pos.entryPrice;
      await exitPosition(sym, price, 'MANUAL_DISCORD');
      const pnl = (price - pos.entryPrice) * (pos.qtyRemaining || pos.qty);
      return { content: `✅ **${sym}** long position exited @ $${price.toFixed(2)}\nP&L: ${pnl>=0?'**+':'**'}$${pnl.toFixed(2)}**\nReason: Manual Discord command` };
    }

    if (shortPositions[sym]) {
      const pos   = shortPositions[sym];
      const price = priceHistory5m[sym]?.[priceHistory5m[sym].length - 1] || pos.entryPrice;
      await coverShort(sym, price, 'MANUAL_DISCORD');
      const pnl = (pos.entryPrice - price) * (pos.qtyRemaining || pos.qty);
      return { content: `✅ **${sym}** short position covered @ $${price.toFixed(2)}\nP&L: ${pnl>=0?'**+':'**'}$${pnl.toFixed(2)}**\nReason: Manual Discord command` };
    }

    if (scalpPositions[sym]) {
      const pos   = scalpPositions[sym];
      const price = priceHistory5m[sym]?.[priceHistory5m[sym].length - 1] || pos.entryPrice;
      await exitScalp(sym, price, 'SCALP_MANUAL');
      const pnl = pos.direction === 'long'
        ? (price - pos.entryPrice) * pos.qty
        : (pos.entryPrice - price) * pos.qty;
      return { content: `✅ **${sym}** scalp exited @ $${price.toFixed(2)}\nP&L: ${pnl>=0?'**+':'**'}$${pnl.toFixed(2)}**` };
    }

    return { content: `⚠ No open position found for **${sym}**\nOpen positions: ${[...Object.keys(positions), ...Object.keys(shortPositions), ...Object.keys(scalpPositions)].join(', ') || 'none'}` };
  }

  // ── /status ──
  if (commandName === 'status') {
    const openPnl = Object.entries(positions).reduce((a, [s, p]) => {
      const cur = priceHistory5m[s]?.[priceHistory5m[s].length-1] || p.entryPrice;
      return a + (cur - p.entryPrice) * (p.qtyRemaining || p.qty);
    }, 0);
    const equity = realEquity || portfolio + openPnl;
    const dayPnl = equity - (realDailyStartEquity || CONFIG.startingCapital);
    const allPos = [
      ...Object.entries(positions).map(([s,p])      => `• **${s}** ${formatPosition(s,p,false)}`),
      ...Object.entries(shortPositions).map(([s,p]) => `• **${s}** ${formatPosition(s,p,true)}`),
      ...Object.entries(scalpPositions).map(([s,p]) => `• **${s}** ⚡SCALP ${p.direction} ${p.qty}x @ $${p.entryPrice.toFixed(2)}`),
    ];
    const modeStr = isSimMode() ? '🎮 SIM' : CONFIG.alpacaPaper ? '📄 PAPER' : '💰 LIVE';
    return { content: [
      `**TradeCore Status** | ${modeStr} | ${getCurrentSession()}`,
      `Portfolio: **$${equity.toFixed(2)}** | Day P&L: **${dayPnl>=0?'+':''}$${dayPnl.toFixed(2)}**`,
      `Wins: ${totalWins} | Losses: ${totalLosses} | Circuit: ${circuitBreakerOn?'🔴 ON':'🟢 OFF'}`,
      allPos.length ? `\n**Open Positions (${allPos.length}):**\n${allPos.join('\n')}` : '\nNo open positions.',
    ].join('\n') };
  }

  // ── /positions ──
  if (commandName === 'positions') {
    const all = [
      ...Object.entries(positions).map(([s,p])      => `🟢 **${s}** (LONG)\n${formatPosition(s,p,false)}`),
      ...Object.entries(shortPositions).map(([s,p]) => `🔴 **${s}** (SHORT)\n${formatPosition(s,p,true)}`),
      ...Object.entries(scalpPositions).map(([s,p]) => {
        const cur = priceHistory5m[s]?.[priceHistory5m[s].length-1] || p.entryPrice;
        const pnl = p.direction==='long' ? (cur-p.entryPrice)*p.qty : (p.entryPrice-cur)*p.qty;
        return `⚡ **${s}** (SCALP ${p.direction.toUpperCase()})\n${p.qty}x @ $${p.entryPrice.toFixed(2)} → $${cur.toFixed(2)} | P&L: ${pnl>=0?'+':''}$${pnl.toFixed(2)}`;
      }),
    ];
    if (!all.length) return { content: '📭 No open positions right now.' };
    return { content: `**Open Positions (${all.length}):**\n\n${all.join('\n\n')}` };
  }

  // ── /pause ──
  if (commandName === 'pause') {
    circuitBreakerOn = true;
    await syncLog('warn', '⏸ Bot paused via Discord command');
    return { content: '⏸ **Bot paused.** No new trades will be placed.\nUse `/resume` to restart trading.' };
  }

  // ── /resume ──
  if (commandName === 'resume') {
    circuitBreakerOn     = false;
    realDailyStartEquity = realEquity || CONFIG.startingCapital;
    await syncLog('sys', '▶️ Bot resumed via Discord command');
    return { content: '▶️ **Bot resumed.** Circuit breaker cleared — trading is active.' };
  }

  // ── /sim ──
  if (commandName === 'sim') {
    const wasSimMode = isSimMode();
    CONFIG.mode = wasSimMode ? (process.env.MODE || 'alpaca') : 'sim';
    if (!wasSimMode) simState.loaded = false; // reload bars on next scan
    await syncLog('sys', `🎮 Sim mode ${isSimMode()?'enabled':'disabled'} via Discord`);
    return { content: isSimMode()
      ? '🎮 **Simulation mode enabled.** Bot will replay historical bars. No real orders.'
      : '✅ **Simulation mode disabled.** Returning to live/paper trading.' };
  }

  return { content: `❓ Unknown command: \`/${commandName}\`` };
}

// Read request body
function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', chunk => data += chunk);
    req.on('end',  ()    => resolve(data));
    req.on('error', reject);
  });
}

http.createServer(async (req, res) => {
  const url = req.url.split('?')[0];

  // ── GET /setup-discord — register slash commands (visit once in browser) ──
  if (req.method === 'GET' && url === '/setup-discord') {
    if (!CONFIG.discordAppId || !process.env.DISCORD_TOKEN) {
      res.writeHead(400, { 'Content-Type': 'text/plain' });
      res.end('Missing DISCORD_APP_ID or DISCORD_TOKEN env vars in Render');
      return;
    }
    try {
      const fetch = await getFetch();
      const commands = [
        { name:'exit',      description:'Exit an open position immediately', options:[{name:'symbol',description:'Ticker (e.g. AAPL)',type:3,required:true}] },
        { name:'status',    description:'Show bot status and all open positions' },
        { name:'positions', description:'List all open positions with P&L' },
        { name:'pause',     description:'Pause the bot — no new trades' },
        { name:'resume',    description:'Resume the bot — clear circuit breaker' },
        { name:'sim',       description:'Toggle simulation mode on/off' },
      ];
      const r = await fetch(`https://discord.com/api/v10/applications/${CONFIG.discordAppId}/commands`, {
        method: 'PUT',
        headers: { 'Authorization': `Bot ${process.env.DISCORD_TOKEN}`, 'Content-Type': 'application/json' },
        body: JSON.stringify(commands),
      });
      const data = await r.json();
      if (r.ok) {
        const list = data.map(c => `✅ /${c.name}`).join('\n');
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end(`Commands registered successfully!\n\n${list}\n\nYou can close this page.`);
        log('sys', `Discord commands registered: ${data.map(c=>'/'+c.name).join(', ')}`);
      } else {
        res.writeHead(400, { 'Content-Type': 'text/plain' });
        res.end(`Failed: ${JSON.stringify(data)}`);
      }
    } catch(e) {
      res.writeHead(500, { 'Content-Type': 'text/plain' });
      res.end(`Error: ${e.message}`);
    }
    return;
  }
  if (req.method === 'GET' && url === '/reset-cb') {
    circuitBreakerOn     = false;
    realDailyStartEquity = realEquity;
    log('risk', '🟢 Circuit breaker reset via /reset-cb');
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, message: 'Circuit breaker reset' }));
    return;
  }

  // ── GET / — health check ──
  if (req.method === 'GET' && url === '/') {
    const openPnl = Object.entries(positions).reduce((acc, [sym, pos]) => {
      const cur = priceHistory5m[sym]?.[priceHistory5m[sym].length - 1] || pos.entryPrice;
      return acc + (cur - pos.entryPrice) * pos.qty;
    }, 0);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'running',
      mode: CONFIG.mode,
      session: getCurrentSession(),
      market_open: isMarketOpen(),
      portfolio: +portfolio.toFixed(2),
      equity: +(realEquity || portfolio + openPnl).toFixed(2),
      open_positions: Object.keys(positions).length,
      short_positions: Object.keys(shortPositions).length,
      scalp_positions: Object.keys(scalpPositions).length,
      scalp_mode: CONFIG.scalpMode,
      scalp_wins: scalpWins,
      scalp_losses: scalpLosses,
      wins: totalWins, losses: totalLosses,
      circuit_breaker: circuitBreakerOn,
      last_scan: lastScanTime,
      uptime_min: Math.round(process.uptime() / 60),
    }, null, 2));
    return;
  }

  // ── POST /discord — Discord Interactions endpoint ──
  if (req.method === 'POST' && url === '/discord') {
    let rawBody;
    try { rawBody = await readBody(req); } catch(e) {
      res.writeHead(400); res.end('Bad request'); return;
    }

    const signature = req.headers['x-signature-ed25519'];
    const timestamp = req.headers['x-signature-timestamp'];

    log('sys', `Discord POST received | sig:${signature?.slice(0,8)}… ts:${timestamp} body:${rawBody.slice(0,60)}…`);

    // Discord requires signature verification
    if (!verifyDiscordRequest(rawBody, signature, timestamp)) {
      log('warn', 'Discord: rejected invalid signature');
      res.writeHead(401, { 'Content-Type': 'text/plain' });
      res.end('Invalid request signature');
      return;
    }

    let interaction;
    try { interaction = JSON.parse(rawBody); } catch(e) {
      res.writeHead(400); res.end('Invalid JSON'); return;
    }

    log('sys', `Discord interaction type: ${interaction.type}`);

    // Discord PING — must respond with type 1 to verify endpoint
    if (interaction.type === 1) {
      log('sys', 'Discord PING received → responding with PONG');
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ type: 1 }));
      log('sys', 'Discord endpoint verified ✅');
      return;
    }

    // Slash command (type 2)
    if (interaction.type === 2) {
      const commandName = interaction.data?.name;
      const options     = interaction.data?.options || [];

      log('sys', `Discord command: /${commandName} from ${interaction.member?.user?.username || interaction.user?.username || 'unknown'}`);

      try {
        const result = await handleSlashCommand(commandName, options);

        // Respond immediately (Discord requires response within 3 seconds)
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          type: 4, // CHANNEL_MESSAGE_WITH_SOURCE
          data: {
            content: result.content,
            flags: 0, // 64 = ephemeral (only visible to user who ran command)
          },
        }));
      } catch(e) {
        log('error', `Discord command error: ${e.message}`);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
          type: 4,
          data: { content: `❌ Error: ${e.message}`, flags: 64 },
        }));
      }
      return;
    }

    res.writeHead(400); res.end('Unknown interaction type');
    return;
  }

  // 404 for anything else
  res.writeHead(404); res.end('Not found');

}).listen(process.env.PORT || 3000, () => {
  log('sys', `Server on port ${process.env.PORT || 3000}`);
  log('sys', `Discord endpoint: POST /discord`);
  if (CONFIG.discordPublicKey) log('sys', `Discord signature verification: ✅ enabled`);
  else log('warn', `Discord signature verification: ⚠ DISCORD_PUBLIC_KEY not set`);
});
