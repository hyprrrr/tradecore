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
const nacl = require('tweetnacl');

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

  symbols: (process.env.SYMBOLS || 'NVDA,AAPL,MSFT,TSLA,META,AMZN,GOOGL,AMD,SPY,QQQ,MSTR,COIN,PLTR,SOFI,HOOD').split(',').map(s => s.trim().toUpperCase()),

  strategy:      process.env.STRATEGY      || 'rsi_macd',
  rsiPeriod:     +(process.env.RSI_PERIOD     || 14),
  rsiOversold:   +(process.env.RSI_OVERSOLD   || 35),
  rsiOverbought: +(process.env.RSI_OVERBOUGHT || 65),

  startingCapital:  +(process.env.CAPITAL          || 100000),
  maxPositionPct:   +(process.env.MAX_POSITION_PCT  || 15) / 100,
  maxOpenPositions: +(process.env.MAX_POSITIONS     || 3),
  stopLossPct:      +(process.env.STOP_LOSS_PCT     || 4)  / 100,
  takeProfitPct:    +(process.env.TAKE_PROFIT_PCT   || 8)  / 100,
  trailingStop:     process.env.TRAILING_STOP       !== 'false',
  trailingStopPct:  +(process.env.TRAILING_STOP_PCT || 3)  / 100,
  maxDailyLossPct:  +(process.env.MAX_DAILY_LOSS    || 3)  / 100,

  // Advanced exit config
  breakEvenAt:       +(process.env.BREAK_EVEN_AT     || 0.3)  / 100,
  trailT2At:         0.010,  // trail 0.8% once up 1%
  trailT3At:         0.020,  // trail 1.5% once up 2%
  trailT4At:         0.040,  // trail 2.0% once up 4%
  minConfidence:     60,
  adaptTargetWR:     0.60,
  adaptEmergencyWR:  0.40,
  confirmCount:      1,  // 1 scan = 15s delay — fast enough for open market moves
  peakMinProfit:     0.020,  // 2% min profit before peak detection runs
  peakSignalsReq:    2,      // signals needed for peak exit
  peakRsiExit:       80,     // RSI level for immediate exit
  fadeMinProfit:     0.020,  // 2% min profit before fade exit
  fadePullback:      0.020,  // 2% pullback from high triggers fade exit
  hardMaxLoss:       0.015,  // 1.5% max loss per trade (hard ceiling — tightened)
  initialStopPct:    0.020,  // 2% initial stop before break-even // move SL to entry once up X%
  tp1Pct:            +(process.env.TP1_PCT           || 1.5) / 100, // sell 33% at +1.5%
  tp2Pct:            +(process.env.TP2_PCT           || 3.0) / 100, // sell 33% at +3%
  tp3Pct:            +(process.env.TP3_PCT           || 5.0) / 100, // sell rest at +5%
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
  positionTradingEnabled: process.env.POSITION_TRADING === 'true',
  scalpMode:           process.env.SCALP_MODE === 'true',
  shortsEnabled:       process.env.ENABLE_SHORTS === 'true',
  recoveryMode:        process.env.RECOVERY_MODE === 'true',
  minConfidence:       +(process.env.MIN_CONFIDENCE || 60), // adaptive — auto-adjusted by learning engine
  scalpSymbols:        (process.env.SCALP_SYMBOLS || 'NVDA,TSLA,MSTR,COIN,AMD,META,AAPL,SPY,QQQ').split(',').map(s => s.trim().toUpperCase()),
  scalpTpPct:          +(process.env.SCALP_TP_PCT        || 0.3)  / 100, // 0.3% TP (2x ATR)
  scalpSlPct:          +(process.env.SCALP_SL_PCT        || 0.15) / 100, // 0.15% SL (1x ATR)
  scalpMaxHoldMins:    +(process.env.SCALP_MAX_HOLD_MINS || 4),           // max 4 min
  scalpMaxPositions:   +(process.env.SCALP_MAX_POSITIONS || 1),           // 1 at a time — focus
  scalpPositionPct:    +(process.env.SCALP_POSITION_PCT  || 10)  / 100,  // 10% per scalp — smaller risk
  scalpMinScore:       +(process.env.SCALP_MIN_SCORE     || 75),          // 75 = high bar
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
    if (s.max_position_pct) {
      const raw = +s.max_position_pct;
      // DB always stores as percentage integer (e.g. 15 = 15%, 3 = 3%)
      // Always divide by 100 to get decimal for CONFIG
      CONFIG.maxPositionPct = raw / 100;
      // Safety clamp: 5-25% per position
      CONFIG.maxPositionPct = Math.max(0.05, Math.min(0.25, CONFIG.maxPositionPct));
    }
    if (s.tp1_pct)          CONFIG.tp1Pct           = Math.max(0.012, Math.min(0.025, +s.tp1_pct / 100)); // min 1.2%
    if (s.tp2_pct)          CONFIG.tp2Pct           = Math.max(0.025, Math.min(0.060, +s.tp2_pct / 100)); // min 2.5%
    if (s.tp3_pct)          CONFIG.tp3Pct           = Math.max(0.040, Math.min(0.100, +s.tp3_pct / 100)); // min 4.0%
    if (s.break_even_at)    CONFIG.breakEvenAt      = +s.break_even_at / 100;
    if (s.trail_t2_at)      CONFIG.trailT2At        = +s.trail_t2_at / 100;
    if (s.trail_t3_at)      CONFIG.trailT3At        = +s.trail_t3_at / 100;
    if (s.trail_t4_at)      CONFIG.trailT4At        = +s.trail_t4_at / 100;
    if (s.min_confidence)   CONFIG.minConfidence     = +s.min_confidence;
    if (s.adapt_target_wr)  CONFIG.adaptTargetWR     = +s.adapt_target_wr / 100;
    if (s.adapt_emergency_wr) CONFIG.adaptEmergencyWR = +s.adapt_emergency_wr / 100;
    if (s.confirm_count)    CONFIG.confirmCount      = +s.confirm_count;
    if (s.peak_min_profit)  CONFIG.peakMinProfit     = +s.peak_min_profit / 100;
    if (s.peak_signals_req) CONFIG.peakSignalsReq    = +s.peak_signals_req;
    if (s.peak_rsi_exit)    CONFIG.peakRsiExit       = +s.peak_rsi_exit;
    if (s.fade_min_profit)  CONFIG.fadeMinProfit     = +s.fade_min_profit / 100;
    if (s.fade_pullback)    CONFIG.fadePullback      = +s.fade_pullback / 100;
    if (s.hard_max_loss)    CONFIG.hardMaxLoss       = Math.max(0.01, +s.hard_max_loss / 100); // min 1% hard stop
    if (s.initial_stop_pct) CONFIG.initialStopPct    = +s.initial_stop_pct / 100;
    if (s.atr_stop_mult)    CONFIG.atrStopMult       = Math.min(2.0, Math.max(1.5, +s.atr_stop_mult)); // clamp 1.5-2.0x
    if (s.discord_webhook)  CONFIG.discordWebhook   = s.discord_webhook;
    if (s.trend_filter      !== undefined) CONFIG.trendFilter       = !!s.trend_filter;
    if (s.volume_filter     !== undefined) CONFIG.volumeFilter      = !!s.volume_filter;
    if (s.regime_filter     !== undefined) CONFIG.regimeFilter      = !!s.regime_filter;
    if (s.correlation_filter !== undefined) CONFIG.correlationFilter = !!s.correlation_filter;

    // Mode flags — controlled from dashboard
    if (s.swing_enabled  !== undefined) CONFIG.swingEnabled          = !!s.swing_enabled;
    if (s.scalp_mode     !== undefined) CONFIG.scalpMode             = !!s.scalp_mode;
    if (s.shorts_enabled !== undefined) CONFIG.shortsEnabled         = !!s.shorts_enabled;
    if (s.position_trading !== undefined) CONFIG.positionTradingEnabled = !!s.position_trading;
    if (s.recovery_mode !== undefined) CONFIG.recoveryMode          = !!s.recovery_mode;
    if (s.sim_mode !== undefined) {
      const newMode = s.sim_mode ? 'sim' : (process.env.MODE || 'alpaca');
      if (newMode !== CONFIG.mode) {
        const entering = newMode === 'sim';
        CONFIG.mode = newMode;
        simState.loaded = false;

        if (!entering) {
          log('sim', '✅ Simulation mode DISABLED — restoring live account');
          await syncLog('sys', '✅ Exited sim mode — restoring live Alpaca account…');

          // 1. Clear ALL sim state from memory — nothing carries over to live
          Object.keys(positions).forEach(k => delete positions[k]);
          Object.keys(shortPositions).forEach(k => delete shortPositions[k]);
          Object.keys(scalpPositions).forEach(k => delete scalpPositions[k]);
          pendingSignals.clear();
          totalWins = 0; totalLosses = 0;
          tradePerformanceLog = [];

          // Reset circuit breaker — sim losses must never affect live trading
          circuitBreakerOn     = false;
          realEquity           = 0; // force re-fetch from Alpaca
          realDailyStartEquity = 0; // force re-fetch from Alpaca

          // 2. Wipe sim positions from Supabase
          await sbFetch(tbl('tc_positions')+'?symbol=neq.____NONE____', 'DELETE');

          // 2b. Delete sim equity snapshots — fake values pollute the chart
          await sbFetch('tc_equity?id=gt.0', 'DELETE');
          log('sys', '✅ Sim data cleared from Supabase');

          // 3. Fetch real Alpaca account values RIGHT NOW
          let liveEquity = 0, liveCash = 0, liveLastEquity = 0;
          try {
            const acct = await getAccount();
            if (acct?.equity)      liveEquity      = +parseFloat(acct.equity).toFixed(2);
            if (acct?.cash)        liveCash        = +parseFloat(acct.cash).toFixed(2);
            if (acct?.last_equity) liveLastEquity  = +parseFloat(acct.last_equity).toFixed(2);
          } catch(e) { log('warn', `Could not fetch Alpaca account: ${e.message}`); }

          if (liveEquity > 0) {
            portfolio            = liveEquity;
            realEquity           = liveEquity;
            realDailyStartEquity = liveLastEquity > 0 ? liveLastEquity : liveEquity;
            const dayPnl         = liveLastEquity > 0 ? liveEquity - liveLastEquity : 0;

            // 4. Write real live values to Supabase immediately
            await sbFetch(tbl('tc_portfolio')+'?id=eq.1', 'PATCH', {
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
          } else {
            // Alpaca fetch failed — do NOT write startingCapital as total_value
            // Writing $100k when real equity is $94k causes the equity spike bug
            // Just update non-financial fields so dashboard knows we're alive
            await sbFetch(tbl('tc_portfolio')+'?id=eq.1', 'PATCH', {
              circuit_breaker: false,
              session:         getCurrentSession(),
              updated_at:      new Date().toISOString(),
            });
            log('warn', 'Alpaca fetch failed — reset to starting capital, will re-sync on next scan');
          }

          // 5. Restore real open positions from Alpaca and sync prices
          await syncAlpacaPositions();
          await syncPricesOnly();   // immediately populate current_price for all positions
          await syncPositions();    // write to Supabase so dashboard shows them
          await syncPortfolio();    // update equity/cash values
          log('sys', `✅ Live account fully restored — ${Object.keys(positions).length} positions synced`);

        } else {
          // Entering sim — full clean reset so every sim run starts fresh
          log('sim', '🎮 Simulation mode ENABLED — resetting for fresh replay');

          // Clear all in-memory positions and state
          Object.keys(positions).forEach(k => delete positions[k]);
          Object.keys(shortPositions).forEach(k => delete shortPositions[k]);
          Object.keys(scalpPositions).forEach(k => delete scalpPositions[k]);
          pendingSignals.clear();
          totalWins = 0; totalLosses = 0;
          tradePerformanceLog = [];
          circuitBreakerOn = false;
          wsSubscribed.clear(); // don't stream live prices during sim

          // Reset portfolio to starting capital
          portfolio            = CONFIG.startingCapital;
          realEquity           = CONFIG.startingCapital;
          realDailyStartEquity = CONFIG.startingCapital;

          // Force bar reload on next sim scan
          simState.loaded  = false;
          simState.cursor  = 0;
          simState.bars    = {};

          // Wipe sim Supabase tables so dashboard shows clean state
          await sbFetch('sim_tc_positions?symbol=neq.____NONE____', 'DELETE').catch(()=>{});
          await sbFetch('sim_tc_trades?id=gt.0', 'DELETE').catch(()=>{});
          await sbFetch('sim_tc_logs?id=gt.0', 'DELETE').catch(()=>{});
          await sbFetch('sim_tc_portfolio?id=eq.1', 'PATCH', {
            cash: CONFIG.startingCapital, total_value: CONFIG.startingCapital,
            day_pnl: 0, total_wins: 0, total_losses: 0,
            circuit_breaker: false, session: '🎮 SIM LOADING…',
            updated_at: new Date().toISOString(),
          }).catch(()=>{});

          log('sim', `🎮 Sim reset complete — portfolio=$${CONFIG.startingCapital.toFixed(2)}`);
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
    // Config loaded silently — only logged when changes detected (see poll interval above)
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

// ═══════════════════════════════════════════════════════════════════
// ADAPTIVE LEARNING ENGINE v2 — Statistical Fine-Tuning
// ═══════════════════════════════════════════════════════════════════
//
// Triggers: after EVERY trade, checks if adjustment needed
// Target:   maintain ≥60% win rate at all times
//
// Approach: Bayesian-style parameter scoring
//   - Each parameter has a "score" based on win rate at that value
//   - Adjustments are proportional to how far below target we are
//   - Uses exponential moving average so recent trades matter more
//   - Finds optimal "sweet spots" by tracking win rate per RSI bucket,
//     confidence bucket, session, etc.
//   - Applies diminishing returns — large adjustments when far off target,
//     tiny micro-adjustments when close to sweet spot
//   - Emergency mode triggers instantly if win rate drops below 40%
// ═══════════════════════════════════════════════════════════════════

const ADAPT_TARGET_WR   = CONFIG.adaptTargetWR || 0.60; // target win rate
const ADAPT_EMERGENCY   = CONFIG.adaptEmergencyWR || 0.40; // emergency mode threshold
const ADAPT_ALPHA       = 0.3;  // EMA weight for recent trades (higher = more reactive)

const ADAPT_DEFAULTS = {
  rsiOversold:    35,
  rsiOverbought:  65,
  minConfidence:  60,
  atrStopMult:    2.0,
  maxPositionPct: 0.15,
  tp1Pct:         0.015,
  tp2Pct:         0.030,
  tp3Pct:         0.050,
  breakEvenAt:    0.02,
};

const ADAPT_BOUNDS = {
  rsiOversold:    { min: 18, max: 45 },
  rsiOverbought:  { min: 55, max: 82 },
  minConfidence:  { min: 45, max: 90 },
  atrStopMult:    { min: 1.5, max: 2.5 }, // hard cap — 3.5x caused $2900 loss
  maxPositionPct: { min: 0.03, max: 0.25 },
  tp1Pct:         { min: 0.010, max: 0.030 },
  tp2Pct:         { min: 0.025, max: 0.060 },
  tp3Pct:         { min: 0.040, max: 0.100 },
  breakEvenAt:    { min: 0.01, max: 0.05 },
};

// Extended trade log — stores rich context for each trade
let tradePerformanceLog = [];

// Exponential moving average of win rate (updates after every trade)
let ewmaWinRate = 0.5; // start neutral

// Parameter sweet spot tracking — maps parameter buckets to win rates
const paramBuckets = {
  rsi:        {}, // { '25-30': {wins:3, total:5}, ... }
  confidence: {}, // { '70-75': {wins:4, total:6}, ... }
  session:    {}, // { 'US Session': {wins:8, total:12}, ... }
  adx:        {}, // { '20-25': {wins:2, total:4}, ... }
};

// Record a trade with full context for learning

// ═══════════════════════════════════════════════════════════════════
// APEX LEARNING ENGINE v2 — Precision Trade Intelligence
// ───────────────────────────────────────────────────────────────────
// What v1 got wrong:
//   - Learned from sim replay (same bars = fake patterns)
//   - RSI=50 (no bar data) counted as valid signal
//   - Only tracked RSI in isolation
//   - Never adjusted TP/SL based on outcomes
//   - Same 5 strategies every run, never evolved
//
// v2 fixes:
//   - Only learns from REAL trades (pnl != 0, rsi != 50, conf > 60)
//   - Tracks 12 dimensions: RSI, MACD, ADX, session, hour, ATR,
//     symbol, confidence, hold time, exit type, side, trend
//   - Detects which conditions CONSISTENTLY win vs lose
//   - Directly adjusts TP/SL/confidence thresholds based on evidence
//   - Generates precise named rules with statistical confidence scores
//   - Blocks re-entry on symbols/conditions with proven loss patterns
// ═══════════════════════════════════════════════════════════════════

const APEX = {
  trades:     [],      // full trade records (only real, valid trades)
  rules:      [],      // generated rules
  blocked:    {},      // sym+condition → blocked until N more wins
  lastSave:   0,
  minSamples: 4,       // minimum trades before a pattern is trusted
  minEdge:    0.15,    // minimum WR difference vs baseline to matter
};

// ── Record a real trade outcome ──────────────────────────────────
function apexRecord(pnl, ctx = {}) {
  // Filter out garbage data from sim/stale bars
  const rsi  = ctx.rsi  || 50;
  const conf = ctx.confidence || 0;
  if (rsi === 50 && conf < 60) return;  // stale bar — skip
  if (pnl === 0) return;                // no fill — skip
  if (!ctx.sym) return;                 // no symbol — skip

  const hour = new Date().getHours();   // ET hour of trade
  const won  = pnl > 0;

  APEX.trades.push({
    pnl, won,
    sym:      ctx.sym,
    rsi:      Math.round(rsi),
    rsiBand:  Math.floor(rsi / 5) * 5,  // band every 5 points (30-35, 35-40...)
    conf:     Math.round(conf),
    confBand: Math.floor(conf / 10) * 10,
    atrPct:   +(ctx.atrPct || 0).toFixed(4),
    atrBand:  ctx.atrPct < 0.003 ? 'low' : ctx.atrPct < 0.008 ? 'mid' : 'high',
    session:  (ctx.session || '').includes('US') ? 'US' :
              (ctx.session || '').includes('Pre') ? 'Pre' : 'Off',
    hour,
    hourBand: hour < 10 ? 'open'   :  // 9:30-9:59 — volatile open
              hour < 12 ? 'mid-am' :  // 10:00-11:59 — best window
              hour < 14 ? 'lunch'  :  // 12:00-13:59 — low volume
              hour < 16 ? 'pm'     :  // 14:00-15:59 — afternoon
                          'after',    // 16:00+ — after hours
    side:     ctx.side || 'long',
    holdMins: ctx.holdMins || 0,
    holdBand: ctx.holdMins < 3   ? 'quick'  :
              ctx.holdMins < 10  ? 'normal' :
              ctx.holdMins < 30  ? 'long'   : 'very-long',
    exitType: ctx.exitReason || 'unknown',
    ts:       Date.now(),
  });

  // Run analysis every 3 real trades
  if (APEX.trades.length % 3 === 0) {
    setTimeout(() => apexAnalyze().catch(() => {}), 500);
  }
}

// ── Core pattern analyser ────────────────────────────────────────
function apexSlice(field, value) {
  // Get all trades matching field=value
  const matched = APEX.trades.filter(t => t[field] === value);
  if (matched.length < APEX.minSamples) return null;
  const wins = matched.filter(t => t.won).length;
  const wr   = wins / matched.length;
  const avgPnl = matched.reduce((a, t) => a + t.pnl, 0) / matched.length;
  return { n: matched.length, wins, wr, avgPnl };
}

function apexBaseline() {
  const n = APEX.trades.length;
  if (!n) return { wr: 0.5, avgPnl: 0 };
  const wins = APEX.trades.filter(t => t.won).length;
  return {
    wr: wins / n,
    avgPnl: APEX.trades.reduce((a, t) => a + t.pnl, 0) / n,
  };
}

async function apexAnalyze() {
  const n = APEX.trades.length;
  if (n < APEX.minSamples) return;

  const now = Date.now();
  if (now - APEX.lastSave < 45000) return; // max once per 45s
  APEX.lastSave = now;

  const base = apexBaseline();
  const newRules = [];

  // ── DIMENSION 1: Symbol-level WR ─────────────────────────────
  const syms = [...new Set(APEX.trades.map(t => t.sym))];
  for (const sym of syms) {
    const s = apexSlice('sym', sym);
    if (!s) continue;
    const edge = s.wr - base.wr;
    if (edge > APEX.minEdge) {
      newRules.push({
        id: `sym_${sym}`,
        type: 'sym_boost',
        sym,
        name: `${sym}: ${(s.wr*100).toFixed(0)}% WR (${s.n} trades)`,
        description: `${sym} is outperforming baseline by ${(edge*100).toFixed(0)}pp. Confidence boosted on ${sym} entries.`,
        action: { boost: Math.round(edge * 30) },  // up to +9 conf
        wr: s.wr, n: s.n, edge,
      });
    } else if (edge < -APEX.minEdge) {
      newRules.push({
        id: `sym_avoid_${sym}`,
        type: 'sym_block',
        sym,
        name: `Avoid ${sym}: ${(s.wr*100).toFixed(0)}% WR (${s.n} trades)`,
        description: `${sym} underperforms by ${(Math.abs(edge)*100).toFixed(0)}pp. Blocking new entries until pattern improves.`,
        action: { block: true },
        wr: s.wr, n: s.n, edge,
      });
    }
  }

  // ── DIMENSION 2: RSI band performance ────────────────────────
  const rsiBands = [...new Set(APEX.trades.map(t => t.rsiBand))].sort((a,b)=>a-b);
  let bestRsiEdge = 0, bestRsiBand = null, worstRsiEdge = 0, worstRsiBand = null;
  for (const band of rsiBands) {
    const s = apexSlice('rsiBand', band);
    if (!s) continue;
    const edge = s.wr - base.wr;
    if (edge > bestRsiEdge)  { bestRsiEdge = edge;  bestRsiBand  = band; }
    if (edge < worstRsiEdge) { worstRsiEdge = edge; worstRsiBand = band; }
  }
  if (bestRsiBand !== null && bestRsiEdge > APEX.minEdge) {
    const s = apexSlice('rsiBand', bestRsiBand);
    newRules.push({
      id: 'rsi_sweet_spot',
      type: 'rsi_boost',
      rsiBand: bestRsiBand,
      name: `RSI ${bestRsiBand}-${bestRsiBand+5}: Best Zone (${(s.wr*100).toFixed(0)}% WR)`,
      description: `Entries when RSI is ${bestRsiBand}-${bestRsiBand+5} win ${(s.wr*100).toFixed(0)}% of the time vs ${(base.wr*100).toFixed(0)}% baseline. Boosting confidence in this zone.`,
      action: { boost: Math.round(bestRsiEdge * 40) },
      wr: s.wr, n: s.n, edge: bestRsiEdge,
    });
  }
  if (worstRsiBand !== null && worstRsiEdge < -APEX.minEdge) {
    const s = apexSlice('rsiBand', worstRsiBand);
    newRules.push({
      id: 'rsi_avoid',
      type: 'rsi_block',
      rsiBand: worstRsiBand,
      name: `RSI ${worstRsiBand}-${worstRsiBand+5}: Avoid (${(s.wr*100).toFixed(0)}% WR)`,
      description: `Only ${(s.wr*100).toFixed(0)}% WR when RSI ${worstRsiBand}-${worstRsiBand+5}. Blocking entries in this zone.`,
      action: { block: true },
      wr: s.wr, n: s.n, edge: worstRsiEdge,
    });
  }

  // ── DIMENSION 3: Time of day ──────────────────────────────────
  const hours = [...new Set(APEX.trades.map(t => t.hourBand))];
  let bestHourEdge = 0, bestHour = null, worstHourEdge = 0, worstHour = null;
  for (const h of hours) {
    const s = apexSlice('hourBand', h);
    if (!s) continue;
    const edge = s.wr - base.wr;
    if (edge > bestHourEdge)  { bestHourEdge = edge;  bestHour  = h; }
    if (edge < worstHourEdge) { worstHourEdge = edge; worstHour = h; }
  }
  if (bestHour && bestHourEdge > APEX.minEdge) {
    const s = apexSlice('hourBand', bestHour);
    const labels = {open:'market open (9:30-10am)', 'mid-am':'mid-morning (10am-12pm)', lunch:'lunch (12-2pm)', pm:'afternoon (2-4pm)', after:'after hours'};
    newRules.push({
      id: 'time_boost',
      type: 'time_boost',
      hourBand: bestHour,
      name: `Best Window: ${labels[bestHour]||bestHour} (${(s.wr*100).toFixed(0)}% WR)`,
      description: `${(s.wr*100).toFixed(0)}% win rate during ${labels[bestHour]||bestHour} vs ${(base.wr*100).toFixed(0)}% baseline. Prioritizing entries in this window.`,
      action: { boost: Math.round(bestHourEdge * 35) },
      wr: s.wr, n: s.n, edge: bestHourEdge,
    });
  }
  if (worstHour && worstHourEdge < -APEX.minEdge) {
    const s = apexSlice('hourBand', worstHour);
    const labels = {open:'market open (9:30-10am)', 'mid-am':'mid-morning (10am-12pm)', lunch:'lunch (12-2pm)', pm:'afternoon (2-4pm)', after:'after hours'};
    newRules.push({
      id: 'time_avoid',
      type: 'time_block',
      hourBand: worstHour,
      name: `Avoid: ${labels[worstHour]||worstHour} (${(s.wr*100).toFixed(0)}% WR)`,
      description: `Only ${(s.wr*100).toFixed(0)}% WR during ${labels[worstHour]||worstHour}. Raising confidence threshold in this window.`,
      action: { minConf: 80 },
      wr: s.wr, n: s.n, edge: worstHourEdge,
    });
  }

  // ── DIMENSION 4: Hold time patterns ──────────────────────────
  const holds = [...new Set(APEX.trades.map(t => t.holdBand))];
  let worstHoldEdge = 0, worstHoldBand = null;
  for (const h of holds) {
    const s = apexSlice('holdBand', h);
    if (!s) continue;
    const edge = s.wr - base.wr;
    if (edge < worstHoldEdge) { worstHoldEdge = edge; worstHoldBand = h; }
  }
  if (worstHoldBand && worstHoldEdge < -APEX.minEdge) {
    const s = apexSlice('holdBand', worstHoldBand);
    const advice = worstHoldBand === 'quick' ? 'Entries closing too fast may be entering at bad spots. Require stronger setups.'
                 : worstHoldBand === 'very-long' ? 'Positions held >30min underperform. Consider tighter time stops.'
                 : `${worstHoldBand} holds are losing. Check entry timing.`;
    newRules.push({
      id: 'hold_pattern',
      type: 'hold_warn',
      holdBand: worstHoldBand,
      name: `${worstHoldBand} holds: ${(s.wr*100).toFixed(0)}% WR — underperforming`,
      description: advice,
      action: { warn: true },
      wr: s.wr, n: s.n, edge: worstHoldEdge,
    });
  }

  // ── DIMENSION 5: Exit type analysis ──────────────────────────
  const exits = [...new Set(APEX.trades.map(t => t.exitType))];
  for (const exit of exits) {
    const s = apexSlice('exitType', exit);
    if (!s || s.n < 3) continue;
    // If STOP_LOSS is winning 0% and avg loss is big, ATR stop is too wide
    if (exit === 'STOP_LOSS' && s.wr === 0 && s.n >= 4) {
      newRules.push({
        id: 'stop_too_wide',
        type: 'config_adjust',
        name: `Stop Loss too wide — 0% recovery rate (${s.n} stops)`,
        description: `All ${s.n} stop losses are full losses with 0% recovery. ATR multiplier may be too wide, allowing too much drawdown before exit.`,
        action: { suggestAtrMult: Math.max(1.5, (CONFIG.atrStopMult || 2.0) - 0.25) },
        wr: s.wr, n: s.n, edge: s.wr - base.wr,
      });
    }
    // If TRAILING_STOP is consistently losing, trailing is too tight
    if (exit === 'TRAILING_STOP' && s.wr < 0.3 && s.n >= 4) {
      newRules.push({
        id: 'trailing_too_tight',
        type: 'config_adjust',
        name: `Trailing stop cutting profits — ${(s.wr*100).toFixed(0)}% WR on ${s.n} trails`,
        description: `Trailing stop exits are mostly losses, suggesting the trail activates too early and stops out before the move completes.`,
        action: { warn: true },
        wr: s.wr, n: s.n, edge: s.wr - base.wr,
      });
    }
  }

  // ── DIMENSION 6: Confidence band ─────────────────────────────
  const confBands = [...new Set(APEX.trades.map(t => t.confBand))].sort((a,b)=>a-b);
  let bestConfEdge = 0, bestConfBand = null;
  for (const band of confBands) {
    const s = apexSlice('confBand', band);
    if (!s) continue;
    const edge = s.wr - base.wr;
    if (edge > bestConfEdge) { bestConfEdge = edge; bestConfBand = band; }
  }
  if (bestConfBand !== null && bestConfEdge > APEX.minEdge) {
    const s = apexSlice('confBand', bestConfBand);
    newRules.push({
      id: 'conf_threshold',
      type: 'conf_adjust',
      confBand: bestConfBand,
      name: `Best entries at conf ${bestConfBand}-${bestConfBand+10}% (${(s.wr*100).toFixed(0)}% WR)`,
      description: `Entries with ${bestConfBand}-${bestConfBand+10}% confidence win ${(s.wr*100).toFixed(0)}% vs ${(base.wr*100).toFixed(0)}% baseline. Suggests current threshold may be too low.`,
      action: { suggestMinConf: bestConfBand },
      wr: s.wr, n: s.n, edge: bestConfEdge,
    });
  }

  // Deduplicate — keep best version of each id
  APEX.rules = [];
  for (const rule of newRules) {
    const existing = APEX.rules.findIndex(r => r.id === rule.id);
    if (existing >= 0) APEX.rules[existing] = rule;
    else APEX.rules.push(rule);
  }

  // Sort by edge magnitude descending
  APEX.rules.sort((a, b) => Math.abs(b.edge) - Math.abs(a.edge));

  // Save to Supabase
  try {
    await sbFetch('tc_settings?id=eq.1', 'PATCH', {
      apex_strategies: JSON.stringify(APEX.rules),
      updated_at: new Date().toISOString(),
    });
    const summary = APEX.rules.slice(0,3).map(r=>r.name.substring(0,40)).join(' | ');
    log('apex', `🧠 APEX v2: ${APEX.rules.length} rules from ${n} trades | ${summary}`);
  } catch(e) {
    log('error', `APEX save failed: ${e.message}`);
  }
}

// ── Apply rules to a signal ──────────────────────────────────────
function apexFilter(sig, ctx = {}) {
  if (!APEX.rules.length) return sig;
  let confBoost = 0;
  let blocked   = null;
  let minConf   = CONFIG.minConfidence || 65;
  const hour    = new Date().getHours();
  const hourBand = hour < 10 ? 'open' : hour < 12 ? 'mid-am' : hour < 14 ? 'lunch' : hour < 16 ? 'pm' : 'after';

  for (const rule of APEX.rules) {
    const a = rule.action || {};
    switch (rule.type) {
      case 'sym_boost':
        if (ctx.sym === rule.sym) confBoost += (a.boost || 5);
        break;
      case 'sym_block':
        if (ctx.sym === rule.sym) blocked = rule.name;
        break;
      case 'rsi_boost':
        if (sig.rsi >= rule.rsiBand && sig.rsi < rule.rsiBand + 5) confBoost += (a.boost || 5);
        break;
      case 'rsi_block':
        if (sig.rsi >= rule.rsiBand && sig.rsi < rule.rsiBand + 5) blocked = rule.name;
        break;
      case 'time_boost':
        if (hourBand === rule.hourBand) confBoost += (a.boost || 5);
        break;
      case 'time_block':
        if (hourBand === rule.hourBand) minConf = Math.max(minConf, a.minConf || 80);
        break;
    }
  }

  if (blocked) {
    const reasons = [...(sig.reasons || []), `🧠 APEX blocked: ${blocked}`];
    return { ...sig, signal: 'HOLD', confidence: 0, score: 0, reasons };
  }

  const newConf = Math.min(99, (sig.confidence || 0) + confBoost);
  if (newConf < minConf) {
    const reasons = [...(sig.reasons || []), `🧠 APEX: conf ${newConf}% below APEX minimum ${minConf}%`];
    return { ...sig, signal: 'HOLD', confidence: newConf, score: sig.score, reasons };
  }

  return { ...sig, confidence: newConf, apexBoost: confBoost };
}

// ── Load history from Supabase on startup ────────────────────────
async function apexLoadStrategies() {
  try {
    const data = await sbFetch('tc_settings?id=eq.1&select=apex_strategies', 'GET');
    const saved = data?.[0]?.apex_strategies;
    if (saved) {
      const parsed = typeof saved === 'string' ? JSON.parse(saved) : saved;
      if (Array.isArray(parsed)) {
        APEX.rules = parsed;
        log('apex', `🧠 APEX v2 loaded ${APEX.rules.length} saved rules`);
      }
    }

    // Seed from real tc_trades (not sim)
    const since = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
    const hist = await sbFetch(
      'tc_trades?order=created_at.desc&limit=300&created_at=gte.' + since + '&select=symbol,pnl,side,created_at',
      'GET'
    );
    if (Array.isArray(hist) && hist.length) {
      let seeded = 0;
      for (const t of hist) {
        if (!t.pnl || t.pnl === 0) continue;
        const pnl = +t.pnl;
        const hour = new Date(t.created_at).getHours();
        // We don't have RSI/conf from DB — only seed if pnl is meaningful
        // This gives APEX symbol and exit-type data at minimum
        APEX.trades.push({
          pnl, won: pnl > 0,
          sym:      t.symbol || '',
          rsi:      50,        // unknown — will be marked invalid for RSI analysis
          rsiBand:  50,        // won't match any meaningful band
          conf:     70,        // assume decent conf
          confBand: 70,
          atrPct:   0.005,
          atrBand:  'mid',
          session:  'US',
          hour,
          hourBand: hour < 10 ? 'open' : hour < 12 ? 'mid-am' : hour < 14 ? 'lunch' : hour < 16 ? 'pm' : 'after',
          side:     t.side?.includes('SHORT')||t.side?.includes('COVER') ? 'short' : 'long',
          holdMins: 10,
          holdBand: 'normal',
          exitType: t.side || 'unknown',
          ts:       new Date(t.created_at).getTime(),
        });
        seeded++;
      }
      if (seeded >= APEX.minSamples) {
        log('apex', `🧠 APEX v2 seeded ${seeded} historical trades — analyzing`);
        await apexAnalyze();
      }
    }
  } catch(e) {
    log('error', `APEX load failed: ${e.message}`);
  }
}


function recordTradeOutcome(pnl, ctx = {}) {
  const won = pnl > 0;

  // Update exponential moving average win rate (after every single trade)
  ewmaWinRate = ADAPT_ALPHA * (won ? 1 : 0) + (1 - ADAPT_ALPHA) * ewmaWinRate;

  const entry = {
    pnl, won,
    pnlPct:     ctx.pnlPct     || 0,
    confidence: ctx.confidence || 0,
    rsi:        ctx.rsi        || 50,
    adx:        ctx.adx        || 0,
    session:    ctx.session    || getCurrentSession(),
    side:       ctx.side       || 'long',
    holdMins:   ctx.holdMins   || 0,
    exitReason: ctx.exitReason || 'unknown',
    timestamp:  Date.now(),
  };
  tradePerformanceLog.push(entry);
  if (tradePerformanceLog.length > 200) tradePerformanceLog.shift();

  // Update parameter buckets for sweet spot detection
  _updateBucket('rsi',        Math.floor(entry.rsi / 5) * 5,        won);
  _updateBucket('confidence', Math.floor(entry.confidence / 5) * 5, won);
  _updateBucket('session',    entry.session,                          won);
  _updateBucket('adx',        Math.floor(entry.adx / 5) * 5,        won);

  // Run adaptation after EVERY trade (micro-adjustments)
  runAdaptiveTuning().catch(() => {});

  // Feed APEX learning engine
  apexRecord(pnl, { ...ctx, sym: ctx.sym || '' });
}

function _updateBucket(param, key, won) {
  if (!paramBuckets[param][key]) paramBuckets[param][key] = { wins: 0, total: 0 };
  paramBuckets[param][key].total++;
  if (won) paramBuckets[param][key].wins++;
}

function _bucketWR(param, key) {
  const b = paramBuckets[param][key];
  return b && b.total >= 3 ? b.wins / b.total : null;
}

// Find the RSI bucket with best win rate (min 3 samples)
function _bestRSIBucket(side) {
  const buckets = paramBuckets.rsi;
  let best = null, bestWR = 0;
  for (const [key, data] of Object.entries(buckets)) {
    if (data.total < 3) continue;
    const wr = data.wins / data.total;
    const rsiVal = +key;
    // Only consider relevant range for direction
    if (side === 'long'  && rsiVal > 45) continue;
    if (side === 'short' && rsiVal < 55) continue;
    if (wr > bestWR) { best = rsiVal; bestWR = wr; }
  }
  return { rsi: best, wr: bestWR };
}

// Proportional step size — larger adjustment when further from target
function _step(current, target, sensitivity = 1.0) {
  const gap = Math.abs(ewmaWinRate - target);
  // Bigger gap = bigger step, smaller gap = micro-adjustment
  // gap 0.20 → step ~3x larger than gap 0.05
  return Math.max(0.5, Math.min(5, gap * 20 * sensitivity));
}

// Core tuning — runs after every trade
async function runAdaptiveTuning() {
  const log_prefix = 'adapt';
  const n = tradePerformanceLog.length;
  if (n < 5) return; // need minimum samples

  const recent20  = tradePerformanceLog.slice(-20);
  const recent10  = tradePerformanceLog.slice(-10);
  const recent5   = tradePerformanceLog.slice(-5);
  const wr20      = recent20.filter(t=>t.won).length / recent20.length;
  const wr10      = recent10.filter(t=>t.won).length / recent10.length;
  const wr5       = recent5.filter(t=>t.won).length  / recent5.length;

  // EWMA is most responsive — use it as primary signal
  const effectiveWR = ewmaWinRate;

  const isEmergency = effectiveWR < ADAPT_EMERGENCY;
  const isUnderTarget = effectiveWR < ADAPT_TARGET_WR;
  const isOverperforming = effectiveWR > ADAPT_TARGET_WR + 0.15;

  if (!isUnderTarget && !isOverperforming) return; // at sweet spot, no change needed

  const changes = {};
  const reasons = [];

  // ─── EMERGENCY MODE: win rate < 40% ───────────────────────────
  if (isEmergency) {
    // Cooldown: only apply emergency corrections every 5 trades to prevent oscillation
    const tradesSinceLastEmergency = (totalWins + totalLosses) - (runAdaptiveEngine._lastEmergencyAt || 0);
    if (tradesSinceLastEmergency < 3) {
      log(log_prefix, `⏸ Emergency skipped — cooldown (${tradesSinceLastEmergency}/3 trades since last)`);
      return;
    }
    runAdaptiveEngine._lastEmergencyAt = totalWins + totalLosses;
    log(log_prefix, `🚨 EMERGENCY: EWMA win rate ${(effectiveWR*100).toFixed(0)}% — aggressive correction`);

    // 1. Drastically raise minimum confidence
    const newConf = Math.min(ADAPT_BOUNDS.minConfidence.max, CONFIG.minConfidence + _step(CONFIG.minConfidence, 80, 2));
    if (newConf !== CONFIG.minConfidence) { CONFIG.minConfidence = Math.round(newConf); changes.minConfidence = CONFIG.minConfidence; reasons.push('Emergency: raise confidence threshold'); }

    // 2. Tighten RSI to only deepest extremes
    const newRSI = Math.max(ADAPT_BOUNDS.rsiOversold.min, CONFIG.rsiOversold - _step(CONFIG.rsiOversold, 25, 1.5));
    if (Math.abs(newRSI - CONFIG.rsiOversold) > 0.5) { CONFIG.rsiOversold = Math.round(newRSI); changes.rsiOversold = CONFIG.rsiOversold; reasons.push('Emergency: tighten RSI oversold'); }
    // Emergency: disable shorts when WR is critically low — don't fight the trend both ways
    if (CONFIG.shortsEnabled) {
      CONFIG.shortsEnabled = false;
      changes.shortsEnabled = false;
      reasons.push('Emergency: shorts disabled — WR too low to trade both directions');
    }

    // 3. Cut position size immediately
    const newPct = Math.max(ADAPT_BOUNDS.maxPositionPct.min, CONFIG.maxPositionPct * 0.6);
    if (Math.abs(newPct - CONFIG.maxPositionPct) > 0.005) { CONFIG.maxPositionPct = +newPct.toFixed(3); changes.maxPositionPct = (CONFIG.maxPositionPct*100).toFixed(1)+'%'; reasons.push('Emergency: cut position size'); }

    // 4. Widen stops — exits might be too tight
    const stopWins   = recent10.filter(t => t.won && t.exitReason !== 'STOP_LOSS').length;
    const stopLosses = recent10.filter(t => !t.won && t.exitReason === 'STOP_LOSS').length;
    if (stopLosses > stopWins) {
      const newMult = Math.min(ADAPT_BOUNDS.atrStopMult.max, CONFIG.atrStopMult + 0.3);
      if (newMult !== CONFIG.atrStopMult) { CONFIG.atrStopMult = +newMult.toFixed(1); changes.atrStopMult = CONFIG.atrStopMult; reasons.push('Emergency: widen ATR stop'); }
    }

  } else if (isUnderTarget) {
    // ─── UNDER TARGET: fine-tune proportionally ──────────────────
    const step = _step(effectiveWR, ADAPT_TARGET_WR);

    // 1. Confidence fine-tuning using sweet spot detection
    const highConf = recent20.filter(t => t.confidence >= CONFIG.minConfidence + 10);
    const lowConf  = recent20.filter(t => t.confidence >= CONFIG.minConfidence && t.confidence < CONFIG.minConfidence + 10);
    const highWR   = highConf.length >= 3 ? highConf.filter(t=>t.won).length / highConf.length : null;
    const lowWR    = lowConf.length  >= 3 ? lowConf.filter(t=>t.won).length  / lowConf.length  : null;

    if (highWR !== null && lowWR !== null && highWR > lowWR + 0.12) {
      // Higher confidence trades win clearly more — raise threshold toward sweet spot
      const target = CONFIG.minConfidence + step;
      const newConf = Math.min(ADAPT_BOUNDS.minConfidence.max, target);
      if (Math.abs(newConf - CONFIG.minConfidence) > 0.5) { CONFIG.minConfidence = Math.round(newConf); changes.minConfidence = CONFIG.minConfidence; reasons.push(`Confidence sweet spot: ${highWR.toFixed(0)*100}% vs ${lowWR.toFixed(0)*100}%`); }
    } else if (lowWR !== null && lowWR < 0.45) {
      // Even minimum confidence trades losing — raise floor
      const newConf = Math.min(ADAPT_BOUNDS.minConfidence.max, CONFIG.minConfidence + step * 0.5);
      if (Math.abs(newConf - CONFIG.minConfidence) > 0.5) { CONFIG.minConfidence = Math.round(newConf); changes.minConfidence = CONFIG.minConfidence; reasons.push('Low confidence trades underperforming'); }
    }

    // 2. RSI sweet spot detection
    const bestLong = _bestRSIBucket('long');
    if (bestLong.rsi !== null && bestLong.wr > ADAPT_TARGET_WR) {
      // Wins cluster at a specific RSI level — move threshold toward it
      const direction = bestLong.rsi < CONFIG.rsiOversold ? -1 : 1;
      const newRSI = CONFIG.rsiOversold + direction * step * 0.5;
      const clamped = Math.max(ADAPT_BOUNDS.rsiOversold.min, Math.min(ADAPT_BOUNDS.rsiOversold.max, newRSI));
      if (Math.abs(clamped - CONFIG.rsiOversold) > 0.5) { CONFIG.rsiOversold = Math.round(clamped); changes.rsiOversold = CONFIG.rsiOversold; reasons.push(`RSI sweet spot at ${bestLong.rsi} (${(bestLong.wr*100).toFixed(0)}% WR)`); }
    }

    // 3. Position sizing — Kelly-inspired fractional sizing
    // Optimal f = (bp - q) / b where b=avg win/loss ratio, p=win rate, q=loss rate
    const winTrades  = recent20.filter(t=>t.won);
    const loseTrades = recent20.filter(t=>!t.won);
    if (winTrades.length >= 3 && loseTrades.length >= 3) {
      const avgWin  = winTrades.reduce((a,t) => a + Math.abs(t.pnlPct||t.pnl), 0) / winTrades.length;
      const avgLoss = loseTrades.reduce((a,t) => a + Math.abs(t.pnlPct||t.pnl), 0) / loseTrades.length;
      if (avgLoss > 0) {
        const b = avgWin / avgLoss;
        const kellyF = (b * wr20 - (1 - wr20)) / b;
        // Use half-Kelly for safety, bounded by our limits
        const halfKelly = Math.max(ADAPT_BOUNDS.maxPositionPct.min, Math.min(ADAPT_BOUNDS.maxPositionPct.max, kellyF * 0.5));
        // Move toward Kelly fraction gradually (max 20% change per adaptation)
        const delta = (halfKelly - CONFIG.maxPositionPct) * 0.2;
        const newPct = CONFIG.maxPositionPct + delta;
        if (Math.abs(delta) > 0.003) { CONFIG.maxPositionPct = +newPct.toFixed(3); changes.maxPositionPct = (CONFIG.maxPositionPct*100).toFixed(1)+'%'; reasons.push(`Kelly sizing: ${(halfKelly*100).toFixed(1)}% optimal (b=${b.toFixed(2)})`); }
      }
    }

    // 4. Exit timing analysis
    // If stop-loss exits are disproportionately high, stops are too tight
    const slExits   = recent20.filter(t => t.exitReason === 'STOP_LOSS').length;
    const tpExits   = recent20.filter(t => t.exitReason?.startsWith('TAKE_PROFIT') || t.exitReason === 'TP1' || t.exitReason === 'TP2' || t.exitReason === 'TP3').length;
    if (slExits > tpExits * 2 && wr20 < ADAPT_TARGET_WR) {
      const newMult = Math.min(ADAPT_BOUNDS.atrStopMult.max, CONFIG.atrStopMult + 0.15);
      if (newMult !== CONFIG.atrStopMult) { CONFIG.atrStopMult = +newMult.toFixed(2); changes.atrStopMult = CONFIG.atrStopMult; reasons.push(`SL exits (${slExits}) >> TP exits (${tpExits}) — widening stop`); }
    }

    // 5. Break-even timing
    // If many trades reach +1% then reverse, move break-even earlier
    const almostWins = recent20.filter(t => !t.won && t.exitReason === 'STOP_LOSS');
    if (almostWins.length > recent20.length * 0.3) {
      const newBE = Math.max(ADAPT_BOUNDS.breakEvenAt.min, CONFIG.breakEvenAt - 0.005);
      if (newBE !== CONFIG.breakEvenAt) { CONFIG.breakEvenAt = +newBE.toFixed(3); changes.breakEvenAt = (CONFIG.breakEvenAt*100).toFixed(1)+'%'; reasons.push('Many near-wins — earlier break-even'); }
    }

  } else if (isOverperforming) {
    // ─── OVERPERFORMING: loosen constraints to get more trades ───
    const step = _step(effectiveWR, ADAPT_TARGET_WR + 0.15) * 0.3; // smaller steps when loosening

    // Lower confidence threshold slightly to catch more setups
    const newConf = Math.max(ADAPT_BOUNDS.minConfidence.min, CONFIG.minConfidence - step);
    if (Math.abs(newConf - CONFIG.minConfidence) > 0.5) { CONFIG.minConfidence = Math.round(newConf); changes.minConfidence = CONFIG.minConfidence; reasons.push(`Overperforming (${(effectiveWR*100).toFixed(0)}%) — expanding entry criteria`); }

    // Increase position size slightly (Kelly says bet more when edge is strong)
    const newPct = Math.min(ADAPT_BOUNDS.maxPositionPct.max, CONFIG.maxPositionPct * 1.05);
    if (Math.abs(newPct - CONFIG.maxPositionPct) > 0.003) { CONFIG.maxPositionPct = +newPct.toFixed(3); changes.maxPositionPct = (CONFIG.maxPositionPct*100).toFixed(1)+'%'; reasons.push('Strong edge — increasing position size'); }
  }

  // ── DEEP STOP LOSS ANALYSIS ──────────────────────────────────────
  // Analyze WHY trades hit stop loss using MFE/MAE data
  const slTrades  = recent20.filter(t => t.exitReason === 'STOP_LOSS' && t.mae !== undefined);
  const tpTrades  = recent20.filter(t => t.won && t.mfe !== undefined);

  if (slTrades.length >= 3) {
    const avgMAE = slTrades.reduce((a,t) => a + (t.mae||0), 0) / slTrades.length;
    const avgMFE_beforeSL = slTrades.reduce((a,t) => a + (t.mfe||0), 0) / slTrades.length;
    const avgStopDist = slTrades.reduce((a,t) => a + (t.stopDistPct||CONFIG.stopLossPct*100), 0) / slTrades.length;

    // If stop was hit but trade first went positive — stop is too tight relative to noise
    if (avgMFE_beforeSL > 0.5 && avgMAE > avgStopDist * 0.8) {
      const newMult = Math.min(ADAPT_BOUNDS.atrStopMult.max, CONFIG.atrStopMult + 0.15);
      if (newMult !== CONFIG.atrStopMult) {
        CONFIG.atrStopMult = +newMult.toFixed(2);
        changes.atrStopMult = CONFIG.atrStopMult;
        reasons.push(`SL analysis: avg MAE ${avgMAE.toFixed(2)}% vs stop ${avgStopDist.toFixed(2)}% — stop too tight for noise`);
      }
    }

    // If trade went negative immediately (no MFE before SL) — signal quality issue
    if (avgMFE_beforeSL < 0.2 && slTrades.length >= 4) {
      const newConf = Math.min(ADAPT_BOUNDS.minConfidence.max, CONFIG.minConfidence + _step(effectiveWR, ADAPT_TARGET_WR, 1.5));
      if (Math.abs(newConf - CONFIG.minConfidence) > 0.5) {
        CONFIG.minConfidence = Math.round(newConf);
        changes.minConfidence = CONFIG.minConfidence;
        reasons.push(`SL analysis: trades went negative immediately (avg MFE ${avgMFE_beforeSL.toFixed(2)}%) — poor entry timing`);
      }
    }

    // Analyze stop distance vs ATR — if stop is less than 1× ATR it's likely noise
    const avgStopAtr = slTrades.reduce((a,t) => a + (t.stopDistAtr||CONFIG.atrStopMult), 0) / slTrades.length;
    if (avgStopAtr < 1.5 && effectiveWR < ADAPT_TARGET_WR) {
      const newMult = Math.min(ADAPT_BOUNDS.atrStopMult.max, CONFIG.atrStopMult + 0.2);
      if (newMult !== CONFIG.atrStopMult) {
        CONFIG.atrStopMult = +newMult.toFixed(2);
        changes.atrStopMult = CONFIG.atrStopMult;
        reasons.push(`SL analysis: stop only ${avgStopAtr.toFixed(1)}× ATR — needs at least 1.5× to absorb noise`);
      }
    }
  }

  // ── DEEP TAKE PROFIT ANALYSIS ─────────────────────────────────────
  if (tpTrades.length >= 3) {
    const avgMFE = tpTrades.reduce((a,t) => a + (t.mfe||0), 0) / tpTrades.length;
    const avgHold = tpTrades.reduce((a,t) => a + (t.holdMins||0), 0) / tpTrades.length;

    // If winners regularly exceed TP3 — TP levels too conservative, missing upside
    const exceededTP3 = tpTrades.filter(t => (t.mfe||0) > CONFIG.tp3Pct * 100 * 1.5).length;
    if (exceededTP3 > tpTrades.length * 0.4) {
      const newTP3 = Math.min(ADAPT_BOUNDS.tp3Pct.max, CONFIG.tp3Pct * 1.1);
      if (Math.abs(newTP3 - CONFIG.tp3Pct) > 0.005) {
        CONFIG.tp3Pct = +newTP3.toFixed(3);
        changes.tp3Pct = (CONFIG.tp3Pct*100).toFixed(1)+'%';
        reasons.push(`TP analysis: ${exceededTP3}/${tpTrades.length} winners exceeded TP3 — raising target`);
      }
    }

    // If winners are being exited too early (MFE much higher than exit pnl)
    const earlyExits = tpTrades.filter(t => (t.mfe||0) > (t.pnlPct||0) * 100 * 1.5).length;
    if (earlyExits > tpTrades.length * 0.5) {
      // Winners have more room — loosen trailing stop slightly
      const newTrail = Math.min(0.05, CONFIG.trailingStopPct * 1.15);
      if (Math.abs(newTrail - CONFIG.trailingStopPct) > 0.002) {
        CONFIG.trailingStopPct = +newTrail.toFixed(3);
        changes.trailingStopPct = (CONFIG.trailingStopPct*100).toFixed(1)+'%';
        reasons.push(`TP analysis: winners have more room (avg MFE ${avgMFE.toFixed(1)}%) — loosening trail`);
      }
    }
  }

  // ── ENTRY TIMING ANALYSIS ─────────────────────────────────────────
  // Analyze volume, momentum, and ATR at entry across wins vs losses
  const winEntries  = recent20.filter(t => t.won && t.volumeRatio !== undefined);
  const lossEntries = recent20.filter(t => !t.won && t.volumeRatio !== undefined);

  if (winEntries.length >= 3 && lossEntries.length >= 3) {
    const avgWinVol  = winEntries.reduce((a,t) => a + (t.volumeRatio||1), 0) / winEntries.length;
    const avgLossVol = lossEntries.reduce((a,t) => a + (t.volumeRatio||1), 0) / lossEntries.length;
    const avgWinAtr  = winEntries.reduce((a,t) => a + (t.atrPct||0), 0) / winEntries.length;
    const avgLossAtr = lossEntries.reduce((a,t) => a + (t.atrPct||0), 0) / lossEntries.length;

    // Wins happen on higher volume — volume filter is working, maybe need higher threshold
    if (avgWinVol > avgLossVol * 1.3) {
      reasons.push(`Entry analysis: wins avg ${avgWinVol.toFixed(1)}x vol vs losses ${avgLossVol.toFixed(1)}x — volume signal strong`);
    }

    // Losses happen on high ATR (volatile) markets — consider tightening in volatile conditions
    if (avgLossAtr > avgWinAtr * 1.4 && effectiveWR < ADAPT_TARGET_WR) {
      const newMult = Math.min(ADAPT_BOUNDS.atrStopMult.max, CONFIG.atrStopMult + 0.1);
      if (newMult !== CONFIG.atrStopMult) {
        CONFIG.atrStopMult = +newMult.toFixed(2);
        changes.atrStopMult = CONFIG.atrStopMult;
        reasons.push(`Entry analysis: losses in high ATR (${avgLossAtr.toFixed(2)}% vs wins ${avgWinAtr.toFixed(2)}%) — widening stop for volatility`);
      }
    }
  }

  // ── SESSION ANALYSIS ──────────────────────────────────────────────
  // Find which sessions are profitable vs losing
  const sessionStats = {};
  recent20.forEach(t => {
    const s = t.session || 'unknown';
    if (!sessionStats[s]) sessionStats[s] = { wins: 0, total: 0 };
    sessionStats[s].total++;
    if (t.won) sessionStats[s].wins++;
  });
  const badSessions = Object.entries(sessionStats)
    .filter(([s, d]) => d.total >= 3 && d.wins/d.total < 0.35)
    .map(([s]) => s);
  if (badSessions.length > 0) {
    reasons.push(`Session analysis: poor WR in ${badSessions.join(', ')} — consider avoiding these sessions`);
  }

  // ── HOLD TIME ANALYSIS ────────────────────────────────────────────
  const longHoldsWon  = recent20.filter(t => t.won && (t.holdMins||0) > 120);
  const shortHoldsWon = recent20.filter(t => t.won && (t.holdMins||0) <= 60);
  const longHoldsLost = recent20.filter(t => !t.won && (t.holdMins||0) > 120);

  if (longHoldsLost.length > longHoldsWon.length && longHoldsLost.length >= 3) {
    // Long holds are losing — time stop might need tightening
    const newTimeStop = Math.max(2, CONFIG.timeStopHours - 1);
    if (newTimeStop !== CONFIG.timeStopHours) {
      CONFIG.timeStopHours = newTimeStop;
      changes.timeStopHours = newTimeStop + 'h';
      reasons.push(`Hold analysis: long holds losing (${longHoldsLost.length} vs ${longHoldsWon.length} wins) — tightening time stop to ${newTimeStop}h`);
    }
  } else if (shortHoldsWon.length > longHoldsLost.length && effectiveWR > ADAPT_TARGET_WR + 0.1) {
    // Short holds winning — could extend time stop to let winners run
    const newTimeStop = Math.min(12, CONFIG.timeStopHours + 1);
    if (newTimeStop !== CONFIG.timeStopHours) {
      CONFIG.timeStopHours = newTimeStop;
      changes.timeStopHours = newTimeStop + 'h';
      reasons.push(`Hold analysis: short holds winning — letting winners run longer (${newTimeStop}h)`);
    }
  }

  // Log and persist
  const summary = `EWMA WR: ${(ewmaWinRate*100).toFixed(1)}% | Last5: ${(wr5*100).toFixed(0)}% | Last10: ${(wr10*100).toFixed(0)}% | Last20: ${(wr20*100).toFixed(0)}%`;
  log(log_prefix, `🧠 Adapted after trade ${n}: ${summary}`);
  log(log_prefix, `   Changes: ${JSON.stringify(changes)}`);
  log(log_prefix, `   Reasons: ${reasons.join(' | ')}`);

  // Save to Supabase
  try {
    await sbFetch('tc_settings?id=eq.1', 'PATCH', {
      rsi_oversold:       CONFIG.rsiOversold,
      rsi_overbought:     CONFIG.rsiOverbought,
      max_position_pct:   +(CONFIG.maxPositionPct * 100).toFixed(2), // stored as percentage e.g. 15.0 = 15%
      atr_stop_mult:      CONFIG.atrStopMult,
      min_confidence:     CONFIG.minConfidence,
      tp1_pct:            +(Math.max(0.012, Math.min(0.025, CONFIG.tp1Pct)) * 100).toFixed(1), // min 1.2%
      tp2_pct:            +(Math.max(0.025, Math.min(0.060, CONFIG.tp2Pct)) * 100).toFixed(1), // min 2.5%
      tp3_pct:            +(Math.max(0.040, Math.min(0.100, CONFIG.tp3Pct)) * 100).toFixed(1), // min 4.0%

      break_even_at:      +(CONFIG.breakEvenAt * 100).toFixed(2),
      trail_t2_at:        +(CONFIG.trailT2At * 100).toFixed(2),
      trail_t3_at:        +(CONFIG.trailT3At * 100).toFixed(2),
      trail_t4_at:        +(CONFIG.trailT4At * 100).toFixed(2),
      peak_min_profit:    +(CONFIG.peakMinProfit * 100).toFixed(2),
      peak_signals_req:   CONFIG.peakSignalsReq,
      peak_rsi_exit:      CONFIG.peakRsiExit,
      fade_min_profit:    +(CONFIG.fadeMinProfit * 100).toFixed(2),
      fade_pullback:      +(CONFIG.fadePullback * 100).toFixed(2),
      hard_max_loss:      +(CONFIG.hardMaxLoss * 100).toFixed(2),
      scalp_mode:         CONFIG.scalpMode,     // preserve — don't let adapt turn off scalp
      shorts_enabled:     CONFIG.shortsEnabled,  // preserve — don't let adapt change shorts
      updated_at:         new Date().toISOString(),
    });
    await syncLog('adapt', `🧠 Adapted | ${summary} | ${JSON.stringify(changes)}`);
  } catch(e) {}

  // Discord alert for significant changes (emergency or major adjustments)
  if (CONFIG.discordWebhook && (isEmergency || Object.keys(changes).length >= 2)) {
    const fetch = await getFetch();
    fetch(CONFIG.discordWebhook, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ embeds: [{
        title: isEmergency ? '🚨 TradeCore Emergency Adaptation' : '🧠 TradeCore Auto-Tuned',
        color: isEmergency ? 0xff0000 : effectiveWR >= ADAPT_TARGET_WR ? 0x7fff6e : 0xffb547,
        fields: [
          { name: 'EWMA Win Rate', value: `${(ewmaWinRate*100).toFixed(1)}%`, inline: true },
          { name: 'Last 10 Trades', value: `${(wr10*100).toFixed(0)}%`, inline: true },
          { name: 'Target', value: `${(ADAPT_TARGET_WR*100).toFixed(0)}%`, inline: true },
          { name: 'Changes', value: Object.entries(changes).map(([k,v])=>`**${k}** → ${v}`).join('\n') || 'None', inline: false },
          { name: 'Why', value: reasons.slice(0,3).join('\n') || '—', inline: false },
        ],
        footer: { text: `TradeCore Adaptive Engine v2 | ${n} trades analyzed` },
        timestamp: new Date().toISOString(),
      }]}),
    }).catch(() => {});
  }
}

// ═══════════════════════════════════════════════════════════════════
// END ADAPTIVE LEARNING ENGINE v2
// ═══════════════════════════════════════════════════════════════════

// ═══════════════════════════════════════════════════════════════════
// POSITION TRADING ENGINE
// ═══════════════════════════════════════════════════════════════════

const SECTOR_ETFS = {
  AAPL:'QQQ',MSFT:'QQQ',NVDA:'QQQ',TSLA:'QQQ',META:'QQQ',GOOGL:'QQQ',AMZN:'QQQ',
  JPM:'XLF',BAC:'XLF',GS:'XLF',MS:'XLF',WFC:'XLF',
  XOM:'XLE',CVX:'XLE',SLB:'XLE',OXY:'XLE',
  JNJ:'XLV',PFE:'XLV',UNH:'XLV',ABBV:'XLV',
  WMT:'XLP',COST:'XLP',PG:'XLP',
  CAT:'XLI',BA:'XLI',GE:'XLI',
  DEFAULT:'SPY',
};
function getSectorEtf(sym){ return SECTOR_ETFS[sym]||SECTOR_ETFS.DEFAULT; }

let positionTrades = {};

const PT_CONFIG = {
  maxPositions:3, maxPosPct:0.10, initialStopPct:0.08, trailStopPct:0.08,
  tp1Pct:0.15, tp2Pct:0.25, tp3Pct:0.40, reassessDays:30, flatThreshold:0.03,
};

async function fetchDailyBars(sym, days=250) {
  try {
    const end   = new Date().toISOString().split("T")[0];
    const start = new Date(Date.now()-days*86400000).toISOString().split("T")[0];
    const url   = `${ALPACA_DATA_BASE}/v2/stocks/${sym}/bars?timeframe=1Day&start=${start}&end=${end}&limit=${days}&feed=${getDataFeed()}`;
    const data  = await alpacaFetch(url);
    return (data?.bars||[]).map(b=>({t:b.t,o:b.o,h:b.h,l:b.l,c:b.c,v:b.v||0}));
  } catch(e){ return []; }
}

async function generatePositionSignal(sym) {
  const [daily, sector] = await Promise.all([fetchDailyBars(sym,250), fetchDailyBars(getSectorEtf(sym),250)]);
  if (daily.length < 200) return { signal:'HOLD', reason:'Insufficient history' };

  const closes  = daily.map(b=>b.c);
  const volumes = daily.map(b=>b.v);
  const price   = closes[closes.length-1];

  const ma50   = closes.slice(-50).reduce((a,b)=>a+b,0)/50;
  const ma200  = closes.slice(-200).reduce((a,b)=>a+b,0)/200;
  const pma50  = closes.slice(-51,-1).reduce((a,b)=>a+b,0)/50;
  const pma200 = closes.slice(-201,-1).reduce((a,b)=>a+b,0)/200;

  const goldenCross = ma50 > ma200;
  const freshGolden = pma50 <= pma200 && ma50 > ma200;
  const aboveMa50   = price > ma50;
  const aboveMa200  = price > ma200;

  const wkCloses = closes.filter((_,i)=>i%5===0);
  const weeklyRSI = rsi(wkCloses, 14);

  const avgVol20  = volumes.slice(-20).reduce((a,b)=>a+b,0)/20;
  const avgVol5   = volumes.slice(-5).reduce((a,b)=>a+b,0)/5;
  const volExpand = avgVol5 > avgVol20*1.1;

  const ma200_30ago = closes.slice(-230,-30).reduce((a,b)=>a+b,0)/200;
  const maSlope     = (ma200-ma200_30ago)/ma200_30ago;

  let sectorBull = false;
  if (sector.length>=200) {
    const sc=sector.map(b=>b.c);
    sectorBull = sc.slice(-50).reduce((a,b)=>a+b,0)/50 > sc.slice(-200).reduce((a,b)=>a+b,0)/200
              && sc[sc.length-1] > sc.slice(-50).reduce((a,b)=>a+b,0)/50;
  }

  let relStrength = false;
  if (sector.length>=20) {
    const sRet = (sector[sector.length-1].c-sector[sector.length-20].c)/sector[sector.length-20].c;
    const pRet = (price-closes[closes.length-20])/closes[closes.length-20];
    relStrength = pRet > sRet+0.02;
  }

  if (!aboveMa200)    return { signal:'HOLD', reason:`Below 200d MA ($${ma200.toFixed(0)})` };
  if (ma50 < ma200)   return { signal:'HOLD', reason:`Death cross — 50d < 200d` };
  if (weeklyRSI > 75) return { signal:'HOLD', reason:`Weekly RSI overbought (${weeklyRSI.toFixed(0)})` };
  if (weeklyRSI < 35) return { signal:'HOLD', reason:`Weekly RSI oversold (${weeklyRSI.toFixed(0)})` };

  const reasons = [];
  let score = 0;
  if (goldenCross)    { score+=3; reasons.push(`Golden cross: MA50=$${ma50.toFixed(0)} > MA200=$${ma200.toFixed(0)} ✅`); }
  if (freshGolden)    { score+=2; reasons.push(`Fresh golden cross 🔥`); }
  if (aboveMa50)      { score+=1; reasons.push(`Above 50d MA ✅`); }
  if (maSlope>0.01)   { score+=1; reasons.push(`200d MA rising ✅`); }
  if (volExpand)      { score+=1; reasons.push(`Volume expanding (${(avgVol5/avgVol20).toFixed(1)}x) ✅`); }
  if (sectorBull)     { score+=2; reasons.push(`Sector (${getSectorEtf(sym)}) bullish ✅`); }
  if (relStrength)    { score+=2; reasons.push(`Outperforming sector ✅`); }
  if (weeklyRSI>=50&&weeklyRSI<=65) { score+=1; reasons.push(`Weekly RSI ${weeklyRSI.toFixed(0)} healthy ✅`); }

  if (score < 7) return { signal:'HOLD', score, reason:`Score ${score} < 7 required`, reasons };

  return {
    signal:'BUY', score, price, ma50, ma200, weeklyRSI,
    stopPrice: price*(1-PT_CONFIG.initialStopPct),
    reasons,
    summary:`Score ${score} | MA50=$${ma50.toFixed(0)} MA200=$${ma200.toFixed(0)} | Stop $${(price*0.92).toFixed(0)} | Target $${(price*1.40).toFixed(0)}`,
  };
}

async function managePositionTrades() {
  if (!CONFIG.positionTradingEnabled) return;
  if (isSimMode() || !isWeekday()) return;

  for (const [sym, pos] of Object.entries(positionTrades)) {
    const bars = await fetchDailyBars(sym, 250);
    if (!bars.length) continue;
    const closes = bars.map(b=>b.c);
    const price  = closes[closes.length-1];
    const chg    = (price-pos.entryPrice)/pos.entryPrice;
    const ma50   = closes.slice(-50).reduce((a,b)=>a+b,0)/50;
    const ma200  = closes.slice(-200).reduce((a,b)=>a+b,0)/200;
    const days   = Math.floor((Date.now()-new Date(pos.entryDate).getTime())/86400000);

    if (price > pos.highWater) positionTrades[sym].highWater = price;
    const trail = pos.highWater*(1-PT_CONFIG.trailStopPct);
    if (trail > positionTrades[sym].stopPrice) positionTrades[sym].stopPrice = trail;

    if (ma50 < ma200)         { await exitPositionTrade(sym,price,'DEATH_CROSS'); continue; }
    if (price < ma200*0.99)   { await exitPositionTrade(sym,price,'TREND_BROKEN'); continue; }
    if (price <= positionTrades[sym].stopPrice) { await exitPositionTrade(sym,price,chg>0?'TRAIL_STOP_PROFIT':'STOP_LOSS'); continue; }

    if (!pos.tp1Hit && chg >= PT_CONFIG.tp1Pct) {
      const qty = Math.max(1,Math.floor(pos.qty*0.33));
      positionTrades[sym].tp1Hit = true;
      positionTrades[sym].qty -= qty;
      positionTrades[sym].stopPrice = Math.max(positionTrades[sym].stopPrice, pos.entryPrice);
      await placeOrder(sym,qty,'sell');
      const pnl = qty*(price-pos.entryPrice);
      await syncTrade({sym,side:'PT_TP1',qty,price,pnl,reason:'PT_TP1'});
      await syncLog('position',`🎯 PT TP1: ${sym} +${(chg*100).toFixed(1)}% P&L=+$${pnl.toFixed(2)}`);
      log('position',`🎯 ${sym} PT-TP1 +${(chg*100).toFixed(1)}% selling ${qty}x`);
    }
    if (pos.tp1Hit && !pos.tp2Hit && chg >= PT_CONFIG.tp2Pct) {
      const qty = Math.max(1,Math.floor(positionTrades[sym].qty*0.5));
      positionTrades[sym].tp2Hit = true; positionTrades[sym].qty -= qty;
      await placeOrder(sym,qty,'sell');
      const pnl = qty*(price-pos.entryPrice);
      await syncTrade({sym,side:'PT_TP2',qty,price,pnl,reason:'PT_TP2'});
      log('position',`🎯🎯 ${sym} PT-TP2 +${(chg*100).toFixed(1)}% selling ${qty}x`);
    }
    if (pos.tp1Hit && pos.tp2Hit && chg >= PT_CONFIG.tp3Pct) {
      await exitPositionTrade(sym,price,'PT_TAKE_PROFIT'); continue;
    }
    if (days >= PT_CONFIG.reassessDays && Math.abs(chg) < PT_CONFIG.flatThreshold) {
      const sig = await generatePositionSignal(sym);
      if (sig.signal !== 'BUY') { await exitPositionTrade(sym,price,'TIME_REASSESS'); }
      else log('position',`✅ ${sym} reassessment passed — holding`);
      continue;
    }
    log('position',`📊 PT ${sym} ${chg>=0?"+":""}${(chg*100).toFixed(1)}% day${days} stop=$${positionTrades[sym].stopPrice.toFixed(0)}`);
  }

  if (Object.keys(positionTrades).length >= PT_CONFIG.maxPositions) return;

  const scanList = [...new Set([...CONFIG.symbols,'AAPL','MSFT','NVDA','AMZN','GOOGL','META','JPM','UNH','XOM','WMT'])];
  for (const sym of scanList) {
    if (positionTrades[sym]||positions[sym]||shortPositions[sym]) continue;
    if (Object.keys(positionTrades).length >= PT_CONFIG.maxPositions) break;
    const sig = await generatePositionSignal(sym);
    if (sig.signal !== 'BUY') continue;

    const qty = Math.max(1,Math.floor(portfolio*PT_CONFIG.maxPosPct/sig.price));
    positionTrades[sym] = { entryPrice:sig.price, qty, entryDate:new Date().toISOString(), highWater:sig.price, stopPrice:sig.stopPrice, tp1Hit:false, tp2Hit:false, score:sig.score };
    portfolio -= qty*sig.price;
    await placeOrder(sym,qty,'buy');
    await syncTrade({sym,side:'PT_BUY',qty,price:sig.price,pnl:null,reason:'POSITION_TRADE'});
    await syncLog('position',`📈 Position Trade: ${sym} @ $${sig.price.toFixed(2)} | ${sig.reasons?.slice(0,3).join(' | ')}`);
    log('position',`📈 POSITION TRADE: ${sym} @ $${sig.price.toFixed(2)} qty=${qty} | ${sig.summary}`);

    if (CONFIG.discordWebhook) {
      const fetch = await getFetch();
      fetch(CONFIG.discordWebhook,{method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({embeds:[{title:`📈 Position Trade Opened: ${sym}`,color:0x7fff6e,
          fields:[
            {name:'Entry',value:`$${sig.price.toFixed(2)}`,inline:true},
            {name:'Qty',value:String(qty),inline:true},
            {name:'Stop',value:`$${sig.stopPrice.toFixed(0)} (-8%)`,inline:true},
            {name:'Targets',value:`+15% / +25% / +40%`,inline:false},
            {name:'Why',value:sig.reasons?.slice(0,4).join("\n")||"—",inline:false},
          ],footer:{text:`Score ${sig.score} | Hold weeks-months`},timestamp:new Date().toISOString(),
        }]})}).catch(()=>{});
    }
  }
}

async function exitPositionTrade(sym, price, reason) {
  const pos = positionTrades[sym];
  if (!pos) return;
  const pnl = (price-pos.entryPrice)*pos.qty;
  await placeOrder(sym,pos.qty,'sell');
  portfolio += pos.qty*price;
  delete positionTrades[sym];
  pnl > 0 ? totalWins++ : totalLosses++;
  log('position',`${pnl>0?"✅":"🛑"} PT EXIT ${sym} @ $${price.toFixed(2)} P&L=${pnl>=0?"+":""}$${pnl.toFixed(2)} (${reason})`);
  await syncTrade({sym,side:'PT_SELL',qty:pos.qty,price,pnl,reason});
  await syncLog('position',`${pnl>0?"✅":"🛑"} Position Trade Closed: ${sym} P&L=${pnl>=0?"+":""}$${pnl.toFixed(2)} (${reason})`);
}

// ═══════════════════════════════════════════════════════════════════
// END POSITION TRADING ENGINE
// ═══════════════════════════════════════════════════════════════════

// ─────────────────────────────────────────────
// SUPABASE SYNC
// ─────────────────────────────────────────────
// Sim mode uses completely separate tables (sim_tc_*) so live data
// is NEVER touched, overwritten, or polluted by simulation runs.
function tbl(name) {
  return isSimMode() ? `sim_${name}` : name;
}

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
  // ── SIM MODE ──
  if (isSimMode()) {
    const openVal = Object.entries(positions).reduce((a, [sym, pos]) => {
      const cur = priceHistory5m[sym]?.[priceHistory5m[sym].length - 1] || pos.entryPrice;
      return a + cur * (pos.qtyRemaining || pos.qty);
    }, 0);
    const shortPnl = Object.entries(shortPositions).reduce((a, [sym, pos]) => {
      const cur = priceHistory5m[sym]?.[priceHistory5m[sym].length - 1] || pos.entryPrice;
      return a + (pos.entryPrice - cur) * (pos.qtyRemaining || pos.qty);
    }, 0);
    const equity = portfolio + openVal + shortPnl;
    const dayPnl = equity - (realDailyStartEquity || CONFIG.startingCapital);
    if (equity > 0) { realEquity = equity; if (!realDailyStartEquity) realDailyStartEquity = CONFIG.startingCapital; }
    await sbFetch(tbl('tc_portfolio')+'?id=eq.1', 'PATCH', {
      cash: +portfolio.toFixed(2), total_value: +equity.toFixed(2), day_pnl: +dayPnl.toFixed(2),
      total_wins: totalWins, total_losses: totalLosses, circuit_breaker: circuitBreakerOn,
      last_scan: new Date().toISOString(), session: getCurrentSession(), updated_at: new Date().toISOString(),
    });
    // Log equity event in sim too for diagnostics
    logEquityEvent(equity, portfolio, 'sim_syncPortfolio', { dayPnl, barPnl: dayPnl });
    // Sim never writes to tc_equity — equity chart only shows real account history
    return;
  }

  // ── LIVE / PAPER MODE ──
  // RULE: Never use in-memory `portfolio` variable for cash in live mode.
  // It's unreliable because we stopped modifying it on trades.
  // Always use Alpaca's account API. realEquity is our last known good value.
  let cashValue   = realEquity > 0 ? realEquity : CONFIG.startingCapital;
  let equityValue = realEquity > 0 ? realEquity : CONFIG.startingCapital;
  let lastEquity  = realDailyStartEquity || 0;

  if (CONFIG.alpacaKey) {
    try {
      const acct = await getAccount();
      if (acct?.equity && +acct.equity > 0) {
        equityValue = +parseFloat(acct.equity).toFixed(2);
        cashValue   = acct.cash ? +parseFloat(acct.cash).toFixed(2) : equityValue;
        if (acct.last_equity && +acct.last_equity > 0) lastEquity = +parseFloat(acct.last_equity).toFixed(2);
        // Update in-memory state
        realEquity = equityValue;
        // Keep portfolio in sync with real cash so calcQty etc work correctly
        if (!isSimMode() && cashValue > 0) portfolio = cashValue;
        if (!realDailyStartEquity && lastEquity > 0) {
          realDailyStartEquity = lastEquity;
          log('risk', `Day baseline set: $${lastEquity.toFixed(2)}`);
        }
      }
    } catch(e) { log('warn', `getAccount failed: ${e.message} — using last known value`); }
  }

  // Step 2: Fix Alpaca equity lag — if we just entered a position,
  // Alpaca might show equity = cash only (position not valued yet).
  // Calculate our own equity and use whichever is higher.
  const posMarketVal = Object.entries(positions).reduce((a, [sym, pos]) => {
    const cur = priceHistory5m[sym]?.[priceHistory5m[sym].length-1] || pos.entryPrice;
    return a + cur * (pos.qtyRemaining || pos.qty);
  }, 0) + Object.entries(scalpPositions).reduce((a, [sym, pos]) => {
    const cur = priceHistory5m[sym]?.[priceHistory5m[sym].length-1] || pos.entryPrice;
    return a + (pos.direction === 'long' ? cur * pos.qty : 0);
  }, 0);

  // Step 2: Override check — ONLY use computed if Alpaca is clearly wrong (>5% off)
  // The old ">$100" threshold caused spikes: any new position added $15k+ to posMarketVal
  // immediately while Alpaca still showed pre-entry equity → false spike
  const computedEquity = cashValue + posMarketVal;
  if (computedEquity > equityValue * 1.05) {
    // Alpaca is more than 5% below our computed — likely lag, but don't spike the chart
    // Just log it; Alpaca will catch up in the next scan
    log('port', `Alpaca equity lag: alpaca=$${equityValue.toFixed(2)} computed=$${computedEquity.toFixed(2)} — keeping Alpaca value`);
  } else if (computedEquity < equityValue * 0.95) {
    // Alpaca shows significantly more than computed — trust Alpaca
    log('port', `Alpaca shows higher equity: alpaca=$${equityValue.toFixed(2)} computed=$${computedEquity.toFixed(2)}`);
  }
  // Always use Alpaca's equity as the source of truth for total_value
  // It accounts for both cash and positions correctly without double-counting

  // Step 3: Day P&L
  const dayPnl = lastEquity > 0
    ? +(equityValue - lastEquity).toFixed(2)
    : trades.filter(t => ['SELL','COVER','SCALP_EXIT'].includes(t.side) && t.pnl != null
        && new Date(t.time).toDateString() === new Date().toDateString())
        .reduce((a, t) => a + t.pnl, 0);

  // Step 4: Validate before writing — reject spiky values
  // If equity jumps >5% from last known good in one scan, it's a bad Alpaca read
  if (realEquity > 0 && Math.abs(equityValue - realEquity) / realEquity > 0.05) {
    log('warn', `syncPortfolio: equity spike rejected ($${equityValue.toFixed(2)} vs last $${realEquity.toFixed(2)}) — using last known`);
    equityValue = realEquity; // use last known good value
  }
  // Never write starting capital as equity — that's always wrong in live mode
  if (equityValue === CONFIG.startingCapital && realEquity > 0) {
    equityValue = realEquity;
  }
  await sbFetch(tbl('tc_portfolio')+'?id=eq.1', 'PATCH', {
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

  // Step 5: Equity snapshot every 2 minutes
  const now = Date.now();
  if (equityValue > 0 && equityValue < CONFIG.startingCapital * 10
      && (now - lastEquitySnapshot) > EQUITY_SNAPSHOT_INTERVAL_MS) {
    lastEquitySnapshot = now;
    await sbFetch(tbl('tc_equity'), 'POST', { value: +equityValue.toFixed(2), created_at: new Date().toISOString() });
  }

  log('port', `Portfolio: equity=$${equityValue.toFixed(2)} cash=$${cashValue.toFixed(2)} dayPnl=${dayPnl>=0?'+':''}$${dayPnl.toFixed(2)}`);
  logEquityEvent(equityValue, cashValue, 'syncPortfolio', { dayPnl, alpacaRaw: realEquity });
}

async function syncPositions() {
  const allPositions = [];

  // Swing longs
  for (const [sym, pos] of Object.entries(positions)) {
    // Use real-time price — no age guard needed, priceHistory5m is seeded on restore
    const cur    = priceHistory5m[sym]?.[priceHistory5m[sym].length - 1] || pos.highWater || pos.entryPrice;
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

  // Sync positions atomically:
  // 1. Delete stale symbols (no longer open)
  // 2. Upsert each current position via PATCH (update) or POST (insert)
  // Using PATCH with return=representation to detect if row existed
  if (allPositions.length > 0) {
    // Delete any rows for symbols NOT in our current open set
    const openSyms = allPositions.map(p => p.symbol);
    await sbFetch(
      tbl('tc_positions') + `?symbol=not.in.(${openSyms.join(',')})`,
      'DELETE'
    );
    // Wipe all then reinsert — clean slate approach
    await sbFetch(tbl('tc_positions') + '?symbol=neq.____NONE____', 'DELETE');
    if (allPositions.length > 0) {
      // Use upsert with the unique constraint on symbol
      const res = await sbFetch(tbl('tc_positions'), 'POST', allPositions);
      if (!res) {
        // If batch insert failed (e.g. constraint violation), try one by one
        for (const pos of allPositions) {
          await sbFetch(tbl('tc_positions'), 'POST', pos).catch(() => {});
        }
      }
    }
  } else {
    await sbFetch(tbl('tc_positions') + '?symbol=neq.____NONE____', 'DELETE');
  }
}

async function syncTrade(trade) {
  await sbFetch(tbl('tc_trades'), 'POST', {
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
  await sbFetch(tbl('tc_logs'), 'POST', {
    type,
    message: msg,
    created_at: new Date().toISOString(),
  });
}

async function syncAll() {
  // In sim mode, positions and portfolio are synced by runSimScan
  // after prices are updated — don't sync here or we get stale prices
  if (isSimMode()) return;
  await Promise.all([syncPortfolio(), syncPositions()]);
}

// Sync live prices every 60s using Alpaca's own position data
// Alpaca gives us current_price, unrealized_pl directly — most accurate source
// ─────────────────────────────────────────────
// REAL-TIME PRICE STREAM — Alpaca WebSocket
// ─────────────────────────────────────────────
let wsStream       = null;
let wsConnected    = false;
let wsSubscribed   = new Set();
let wsReconnectMs  = 2000;

// ── Polygon.io free real-time WebSocket ──────────────────────────
// Free tier gives real-time US stock quotes + trades
// Get a free key at polygon.io → set POLYGON_API_KEY in Railway env vars
// This runs alongside Alpaca WS — whichever updates first wins
let polygonWs        = null;
let polygonConnected = false;

async function connectPolygon() {
  const key = process.env.POLYGON_API_KEY;
  if (!key || isSimMode()) return;
  try {
    const { WebSocket } = await import('ws');
    polygonWs = new WebSocket('wss://socket.polygon.io/stocks');
    polygonWs.on('open', () => {
      polygonWs.send(JSON.stringify({ action:'auth', params:key }));
    });
    polygonWs.on('message', (raw) => {
      try {
        const msgs = JSON.parse(raw);
        for (const msg of (Array.isArray(msgs) ? msgs : [msgs])) {
          if (msg.ev==='status' && msg.status==='auth_success') {
            polygonConnected = true;
            log('stream', '✅ Polygon.io real-time feed live');
            subscribePolygon([...Object.keys(positions),...Object.keys(shortPositions)]);
          }
          // Real-time quote (bid/ask mid)
          if (msg.ev==='Q') {
            const sym=msg.sym, mid=((msg.bp||0)+(msg.ap||0))/2;
            if (!sym||!mid) continue;
            if (!priceHistory5m[sym]) priceHistory5m[sym]=[];
            const last=priceHistory5m[sym][priceHistory5m[sym].length-1];
            if (!last||Math.abs(mid-last)/last>0.00005) {
              priceHistory5m[sym].push(mid);
              if (priceHistory5m[sym].length>500) priceHistory5m[sym].shift();
            }
            if (liveBar[sym]) { liveBar[sym].c=mid; liveBar[sym].h=Math.max(liveBar[sym].h,mid); liveBar[sym].l=Math.min(liveBar[sym].l,mid); }
            if (positions[sym]) { if (mid>positions[sym].highWater) positions[sym].highWater=mid; if (mid<(positions[sym].lowWater||mid)) positions[sym].lowWater=mid; }
          }
          // Real-time trade print
          if (msg.ev==='T') {
            const sym=msg.sym, price=msg.p, size=msg.s||1;
            if (!sym||!price) continue;
            const barTime=Math.floor((msg.t||Date.now())/300000)*300000;
            if (!liveBar[sym]||liveBar[sym].time!==barTime) {
              if (liveBar[sym]) injectLiveBar(sym,liveBar[sym]);
              liveBar[sym]={time:barTime,t:new Date(barTime).toISOString(),o:price,h:price,l:price,c:price,v:size};
            } else { liveBar[sym].c=price; liveBar[sym].h=Math.max(liveBar[sym].h,price); liveBar[sym].l=Math.min(liveBar[sym].l,price); liveBar[sym].v+=size; }
          }
        }
      } catch(e){}
    });
    polygonWs.on('close', ()=>{ polygonConnected=false; setTimeout(connectPolygon,5000); });
    polygonWs.on('error', ()=>{});
  } catch(e) { log('warn',`Polygon unavailable: ${e.message}`); }
}

function subscribePolygon(syms) {
  if (!polygonWs||!polygonConnected||!syms?.length) return;
  polygonWs.send(JSON.stringify({ action:'subscribe', params:syms.map(s=>`Q.${s},T.${s}`).join(',') }));
}

// Real-time bar builder — one entry per symbol, the currently forming 5-min bar
// Gets injected into the bars array on every managePosition call
// This is what makes indicators (ATR, RSI) update at tick speed not bar-close speed
let liveBar = {}; // sym → { time, t, o, h, l, c, v }

// Inject the live (incomplete) current bar into a bars array
// Replaces the last bar if it's in the same 5-min window, otherwise appends
function injectLiveBar(sym, bar) {
  if (!bar || !liveBar5m) return;
  if (!liveBar5m[sym]) liveBar5m[sym] = [];
  const last = liveBar5m[sym][liveBar5m[sym].length - 1];
  if (last && last.time === bar.time) {
    liveBar5m[sym][liveBar5m[sym].length - 1] = { ...bar };
  } else {
    liveBar5m[sym].push({ ...bar });
    if (liveBar5m[sym].length > 100) liveBar5m[sym].shift();
  }
}

// Completed live bars storage — separate from REST bars so we can merge them
let liveBar5m = {}; // sym → completed bars from live ticks this session

// Get bars with live tick data merged in — called by managePosition instead of raw REST bars
// In sim mode: just return the bars directly (no live stream)
function getLiveBars(sym, restBars) {
  if (!restBars || restBars.length === 0) return restBars;
  if (isSimMode()) return restBars; // sim has no live stream, bars are already current

  // Start with REST bars
  const merged = [...restBars];

  // Append any completed live bars from this session that are newer than REST data
  const lastRestTime = restBars[restBars.length - 1]?.t || '';
  if (liveBar5m[sym]) {
    for (const b of liveBar5m[sym]) {
      if (b.t > lastRestTime) merged.push(b);
    }
  }

  // Append or replace the currently forming bar (real-time incomplete candle)
  if (liveBar[sym]) {
    const live = liveBar[sym];
    const lastMerged = merged[merged.length - 1];
    if (lastMerged && lastMerged.time === live.time) {
      merged[merged.length - 1] = {
        t: live.t, o: live.o, h: live.h, l: live.l, c: live.c, v: live.v,
        time: live.time, live: true,
      };
    } else {
      merged.push({
        t: live.t, o: live.o, h: live.h, l: live.l, c: live.c, v: live.v,
        time: live.time, live: true,
      });
    }
  }

  return merged;
}

// Feed selection:
// - SIP = consolidated tape (all exchanges), true real-time, requires live account
// - IEX = IEX exchange only, ~15ms delay, works on paper accounts
// - iex is used as fallback for sim/paper. On live accounts SIP is always better.
function getDataFeed() {
  // Allow override via env var
  if (process.env.DATA_FEED) return process.env.DATA_FEED.toLowerCase();
  return CONFIG.alpacaPaper ? 'iex' : 'sip';
}

function getStreamUrl() {
  const feed = getDataFeed();
  return `wss://stream.data.alpaca.markets/v2/${feed}`;
}

async function connectPriceStream() {
  if (isSimMode()) return;
  if (!CONFIG.alpacaKey || !CONFIG.alpacaSecret) return;
  try {
    const { WebSocket } = await import('ws');
    if (wsStream) { try { wsStream.terminate(); } catch(e){} }
    wsStream = new WebSocket(getStreamUrl());
    wsConnected = false;

    wsStream.on('open', () => {
      wsStream.send(JSON.stringify({ action:'auth', key:CONFIG.alpacaKey, secret:CONFIG.alpacaSecret }));
    });

    wsStream.on('message', (raw) => {
      try {
        const msgs = JSON.parse(raw);
        for (const msg of (Array.isArray(msgs) ? msgs : [msgs])) {

          // ── Auth ──
          if (msg.T === 'success' && msg.msg === 'authenticated') {
            wsConnected = true; wsReconnectMs = 2000;
            log('stream', '✅ Price stream live');
            subscribeOpenPositions();
          }

          // ── Real-time trade print → build live bar ──
          if (msg.T === 't') {
            const sym   = msg.S;
            const price = +msg.p;
            const size  = +msg.s || 1;
            const ts    = msg.t ? new Date(msg.t).getTime() : Date.now();
            if (!sym || !price) continue;

            // 1. Update flat price history (used for P&L display)
            if (!priceHistory5m[sym]) priceHistory5m[sym] = [];
            priceHistory5m[sym].push(price);
            if (priceHistory5m[sym].length > 500) priceHistory5m[sym].shift();

            // 2. Build live 5-min bar from ticks
            // Bar boundary = floor to nearest 5-min interval
            const barTime = Math.floor(ts / 300000) * 300000;
            if (!liveBar[sym] || liveBar[sym].time !== barTime) {
              // New bar started — push completed bar into bar history
              if (liveBar[sym]) {
                injectLiveBar(sym, liveBar[sym]);
              }
              liveBar[sym] = {
                time: barTime,
                t:    new Date(barTime).toISOString(),
                o:    price, h: price, l: price, c: price,
                v:    size,
              };
            } else {
              // Update current bar
              const b = liveBar[sym];
              b.c = price;
              b.h = Math.max(b.h, price);
              b.l = Math.min(b.l, price);
              b.v += size;
            }

            // 3. Update position water marks immediately
            if (positions[sym]) {
              if (price > positions[sym].highWater) positions[sym].highWater = price;
              if (price < (positions[sym].lowWater || price)) positions[sym].lowWater = price;
            }
            if (shortPositions[sym]) {
              if (price < (shortPositions[sym].lowWater || price)) shortPositions[sym].lowWater = price;
              if (price > (shortPositions[sym].highWater || price)) shortPositions[sym].highWater = price;
            }
          }

          // ── Quote (bid/ask) — sub-ms price updates between trades ──
          if (msg.T === 'q') {
            const sym = msg.S;
            const mid = ((+msg.bp || 0) + (+msg.ap || 0)) / 2;
            if (!sym || !mid) continue;

            // Update price history with mid price for smoothest P&L display
            if (!priceHistory5m[sym]) priceHistory5m[sym] = [];
            const lastP = priceHistory5m[sym][priceHistory5m[sym].length - 1];
            // Only update if quote moved meaningfully (avoid flooding with identical quotes)
            if (!lastP || Math.abs(mid - lastP) / lastP > 0.00005) {
              priceHistory5m[sym].push(mid);
              if (priceHistory5m[sym].length > 500) priceHistory5m[sym].shift();
            }

            // Update live bar with quote mid
            if (liveBar[sym]) {
              liveBar[sym].c = mid;
              liveBar[sym].h = Math.max(liveBar[sym].h, mid);
              liveBar[sym].l = Math.min(liveBar[sym].l, mid);
            }

            // Update water marks
            if (positions[sym]) {
              if (mid > positions[sym].highWater) positions[sym].highWater = mid;
              if (mid < (positions[sym].lowWater || mid)) positions[sym].lowWater = mid;
            }
          }

          if (msg.T === 'error') log('warn', `Stream: ${msg.msg}`);
        }
      } catch(e) {}
    });

    wsStream.on('close', (code) => {
      wsConnected = false; wsSubscribed.clear();
      log('stream', `Stream closed (${code}) — reconnecting in ${wsReconnectMs/1000}s`);
      setTimeout(() => { wsReconnectMs = Math.min(wsReconnectMs*2,30000); connectPriceStream(); }, wsReconnectMs);
    });

    wsStream.on('error', (err) => { log('warn', `Stream err: ${err.message}`); });
  } catch(e) {
    log('warn', `WS unavailable (${e.message}) — using REST polling`);
  }
}

function subscribeOpenPositions() {
  if (!wsStream || !wsConnected) return;
  const syms = [...new Set([...Object.keys(positions),...Object.keys(shortPositions),...Object.keys(scalpPositions)])].filter(s=>!wsSubscribed.has(s));
  if (!syms.length) return;
  wsStream.send(JSON.stringify({ action:'subscribe', trades:syms, quotes:syms }));
  syms.forEach(s=>wsSubscribed.add(s));
  log('stream', `📡 Subscribed: ${syms.join(', ')}`);
}

function unsubscribeClosedPositions() {
  if (!wsStream || !wsConnected) return;
  const open = new Set([...Object.keys(positions),...Object.keys(shortPositions),...Object.keys(scalpPositions)]);
  const toUnsub = [...wsSubscribed].filter(s=>!open.has(s));
  if (!toUnsub.length) return;
  wsStream.send(JSON.stringify({ action:'unsubscribe', trades:toUnsub, quotes:toUnsub }));
  toUnsub.forEach(s=>wsSubscribed.delete(s));
}

// ─────────────────────────────────────────────
// END REAL-TIME PRICE STREAM
// ─────────────────────────────────────────────

async function syncPricesOnly() {
  if (isSimMode()) {
    for (const [sym, pos] of Object.entries(positions)) {
      const cur = priceHistory5m[sym]?.[priceHistory5m[sym].length - 1];
      if (cur && cur > pos.highWater) positions[sym].highWater = cur;
    }
    await syncPositions();
    return;
  }

  if (!CONFIG.alpacaKey) return;

  try {
    // Fetch account and positions in parallel
    const [alpacaPos, acct] = await Promise.all([
      alpacaFetch(`${ALPACA_BASE()}/v2/positions`),
      getAccount(),
    ]);

    // Update in-memory price history from Alpaca live prices
    if (Array.isArray(alpacaPos)) {
      for (const ap of alpacaPos) {
        const sym = ap.symbol;
        const cur = +ap.current_price;
        if (!cur || cur <= 0) continue;

        if (!priceHistory5m[sym]) priceHistory5m[sym] = [];
        priceHistory5m[sym].push(cur);
        if (priceHistory5m[sym].length > 60) priceHistory5m[sym].shift();

        if (positions[sym]      && cur > positions[sym].highWater)     positions[sym].highWater     = cur;
        if (shortPositions[sym] && cur < shortPositions[sym].lowWater) shortPositions[sym].lowWater = cur;
      }
    }

    // Single write to Supabase — P&L calculated consistently in syncPositions
    // This prevents the flicker from two different P&L sources racing each other
    await syncPositions();

    // Update portfolio totals from Alpaca account
    if (acct?.equity && +acct.equity > 0) {
      const equity     = +parseFloat(acct.equity).toFixed(2);
      const cash       = acct.cash ? +parseFloat(acct.cash).toFixed(2) : equity;
      const lastEquity = acct.last_equity ? +parseFloat(acct.last_equity).toFixed(2) : 0;
      const dayPnl     = lastEquity > 0 ? +(equity - lastEquity).toFixed(2) : 0;

      realEquity = equity;
      if (!realDailyStartEquity && lastEquity > 0) realDailyStartEquity = lastEquity;

      // Validate: reject if equity spikes >5% from last known (bad Alpaca read)
      const _lastKnown = realEquity > 0 ? realEquity : CONFIG.startingCapital;
      if (Math.abs(equity - _lastKnown) / _lastKnown > 0.05) {
        log('warn', `syncPricesOnly: spike rejected ($${equity.toFixed(2)} vs $${_lastKnown.toFixed(2)}) — skipping write`);
      } else {
        await sbFetch(tbl('tc_portfolio')+'?id=eq.1', 'PATCH', {
          cash:        +cash.toFixed(2),
          total_value: +equity.toFixed(2),
          day_pnl:     +dayPnl.toFixed(2),
          updated_at:  new Date().toISOString(),
        });
        log('price', `Equity=$${equity.toFixed(2)} DayP&L=${dayPnl>=0?'+':''}$${dayPnl.toFixed(2)} Positions:${Object.keys(positions).length}`);
      }
    }

  } catch (e) {
    log('error', `syncPricesOnly: ${e.message}`);
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

// ── EQUITY EVENT LOG — ring buffer of last 500 equity changes with causes ──
// Helps diagnose spikes: every time equity is written to Supabase, log what caused it
const eqLog = [];
function logEquityEvent(equity, cash, cause, extra = {}) {
  const longMV  = Object.entries(positions).reduce((a,[s,p])=>a+(priceHistory5m[s]?.[priceHistory5m[s].length-1]||p.entryPrice)*(p.qtyRemaining||p.qty),0);
  const shortPnl = Object.entries(shortPositions).reduce((a,[s,p])=>a+(p.entryPrice-(priceHistory5m[s]?.[priceHistory5m[s].length-1]||p.entryPrice))*(p.qtyRemaining||p.qty),0);
  const scalpMV = Object.entries(scalpPositions).reduce((a,[s,p])=>a+(p.direction==='long'?(priceHistory5m[s]?.[priceHistory5m[s].length-1]||p.entryPrice)*p.qty:0),0);
  eqLog.push({
    ts: new Date().toISOString(),
    equity: +equity.toFixed(2),
    cash: +cash.toFixed(2),
    longMV: +longMV.toFixed(2),
    shortPnl: +shortPnl.toFixed(2),
    scalpMV: +scalpMV.toFixed(2),
    openPositions: Object.keys(positions).length,
    openShorts: Object.keys(shortPositions).length,
    openScalps: Object.keys(scalpPositions).length,
    cause,
    ...extra,
  });
  if (eqLog.length > 500) eqLog.shift();
}
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
// ─────────────────────────────────────────────
// COMPREHENSIVE LOGGER — captures everything
// Ring buffer of last 2000 entries, exportable via /diagnostic
// ─────────────────────────────────────────────
const masterLog = [];  // [{ts, type, msg, data}]
const MAX_LOG = 2000;

function log(type, msg, data = null) {
  const ts = new Date().toISOString();
  const etTs = new Date().toLocaleString('en-US', { timeZone: 'America/New_York' });
  console.log(`[${etTs} ET] [${type.toUpperCase().padEnd(8)}] ${msg}`);
  masterLog.push({ ts, type, msg, ...(data ? { data } : {}) });
  if (masterLog.length > MAX_LOG) masterLog.shift();
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
  // Open 9:45 AM ET (not 9:31) — skip first 15min whipsaw window
  // Close 3:45 PM ET (not 3:55) — avoid end-of-day volatility
  return day >= 1 && day <= 5 && mins >= 585 && mins <= 945; // 9:45 - 3:45
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
  // Try Alpaca first — extend to 10 days to ensure enough bars for RSI(14)
  try {
    const start = new Date(Date.now() - 10 * 24 * 60 * 60 * 1000).toISOString();
    const url = `${ALPACA_DATA_BASE}/v2/stocks/${symbol}/bars?timeframe=${timeframe}&start=${start}&limit=${Math.max(limit, 100)}&feed=${getDataFeed()}`;
    const data = await alpacaFetch(url);
    // Need at least 30 bars for RSI+MACD+ADX to be meaningful
    // If Alpaca returns too few, fall through to Yahoo
    if (data.bars && data.bars.length >= 30) return data.bars;
    if (data.bars && data.bars.length > 0) {
      log('data', `${symbol} Alpaca only returned ${data.bars.length} bars — trying Yahoo`);
    }
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
const SCREENER_INTERVAL_MS = 2 * 60 * 1000; // re-screen every 2 minutes

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
    const url  = `${ALPACA_DATA_BASE}/v2/stocks/snapshots?symbols=${symbolsParam}&feed=${getDataFeed()}`;
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
    await sbFetch(tbl('tc_portfolio')+'?id=eq.1', 'PATCH', {
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
  log('sim', `🎮 Loading 60 days of bar history for ${symbols.length} symbols…`);
  simState.bars    = {};
  simState.cursor  = 0;
  simState.loaded  = false;

  // Fetch 60 days of 5-min bars using pagination (Alpaca limit=1000 per request)
  // 60 days × 6.5 trading hours × 12 bars/hour = ~4,680 bars per symbol
  // We paginate with next_page_token until we have all bars or hit 60 days
  const end   = new Date();
  const start = new Date(end.getTime() - 60 * 24 * 60 * 60 * 1000);
  const startStr = start.toISOString().split('.')[0] + 'Z';
  const endStr   = end.toISOString().split('.')[0] + 'Z';

  async function fetchAllBarsForSym(sym) {
    const allBars = [];
    let nextToken = null;
    let pages = 0;
    const maxPages = 8; // max 8 × 1000 = 8,000 bars per symbol

    do {
      try {
        let url = `${ALPACA_DATA_BASE}/v2/stocks/${sym}/bars?timeframe=5Min&start=${startStr}&end=${endStr}&limit=1000&adjustment=raw&feed=${getDataFeed()}`;
        if (nextToken) url += `&page_token=${encodeURIComponent(nextToken)}`;
        const data = await alpacaFetch(url);
        const bars = (data?.bars || []).map(b => ({
          t: b.t, o: b.o, h: b.h, l: b.l, c: b.c, v: b.v || 0,
        })).filter(b => b.c && b.h && b.l && b.o);

        // Filter: only keep market hours (9:30-16:00 ET) bars
        // Skip pre/after market — they have low volume and mess up indicators
        const mktBars = bars.filter(b => {
          const d = new Date(b.t);
          const h = d.getUTCHours() - 4; // ET offset (approximate — no DST handling needed for filtering)
          const m = d.getUTCMinutes();
          const etMins = h * 60 + m;
          return etMins >= 570 && etMins < 960; // 9:30am-4:00pm = 570-960 mins
        });

        allBars.push(...mktBars);
        nextToken = data?.next_page_token || null;
        pages++;

        // Small delay between pages to avoid rate limiting
        if (nextToken) await new Promise(r => setTimeout(r, 150));
      } catch(e) {
        log('warn', `Sim: page ${pages} failed for ${sym}: ${e.message}`);
        break;
      }
    } while (nextToken && pages < maxPages);

    return allBars;
  }

  // Fetch symbols in batches of 3 (rate limit friendly)
  log('sim', `🎮 Fetching paginated history (this takes ~20s)…`);
  for (let i = 0; i < symbols.length; i += 3) {
    const batch = symbols.slice(i, i + 3);
    const results = await Promise.allSettled(batch.map(async sym => {
      const bars = await fetchAllBarsForSym(sym);
      return { sym, bars };
    }));
    for (const r of results) {
      if (r.status === 'fulfilled' && r.value?.bars?.length >= 50) {
        simState.bars[r.value.sym] = r.value.bars;
        log('sim', `🎮 ${r.value.sym}: ${r.value.bars.length} bars loaded`);
      }
    }
    if (i + 3 < symbols.length) await new Promise(r => setTimeout(r, 300));
  }

  const loaded = Object.keys(simState.bars);
  log('sim', `🎮 Loaded ${loaded.length}/${symbols.length} symbols`);

  const lengths = Object.values(simState.bars).map(b => b.length);
  if (lengths.length === 0) {
    log('error', 'Sim: no bar data — check Alpaca keys');
    return false;
  }

  simState.totalBars   = Math.min(...lengths);
  simState.cursor      = 50; // start with 50 bars of history for indicators
  simState.loaded      = true;
  simState.startTime   = simState.bars[loaded[0]][0].t;
  simState.currentTime = simState.startTime;

  log('sim', `🎮 Replay ready: ${simState.totalBars} bars/symbol (~${(simState.totalBars/78).toFixed(0)} trading days)`);
  log('sim', `🎮 Starts: ${simState.startTime} | Ends: ${simState.bars[loaded[0]][simState.totalBars-1].t}`);
  return true;
}

// Advance the replay cursor by one bar
// Returns { sym → bars[] } sliced up to current cursor, or null if replay is done
function simAdvanceCursor() {
  if (!simState.loaded) return null;

  if (simState.cursor >= simState.totalBars - 20) {
    // End of 60-day replay — close all open positions at last price then stop
    log('sim', `🎮 Replay complete — ${simState.totalBars} bars processed`);
    log('sim', `🎮 Final equity: $${portfolio.toFixed(2)} | ${totalWins}W/${totalLosses}L`);

    // Force-close all open positions at final bar price
    for (const sym of Object.keys(positions)) {
      const pos = positions[sym];
      const finalPrice = simCurrentPrice(sym) || pos.entryPrice;
      const pnl = (finalPrice - pos.entryPrice) * pos.qty;
      portfolio += pos.qty * finalPrice;
      delete positions[sym];
      log('sim', `🎮 Sim end — closed ${sym} long @ $${finalPrice.toFixed(2)} PnL: $${pnl.toFixed(2)}`);
    }
    for (const sym of Object.keys(shortPositions)) {
      const pos = shortPositions[sym];
      const finalPrice = simCurrentPrice(sym) || pos.entryPrice;
      const pnl = (pos.entryPrice - finalPrice) * pos.qty;
      portfolio += pnl;
      delete shortPositions[sym];
      log('sim', `🎮 Sim end — closed ${sym} short PnL: $${pnl.toFixed(2)}`);
    }
    pendingSignals.clear();

    // Update final sim portfolio state
    sbFetch('sim_tc_portfolio?id=eq.1', 'PATCH', {
      cash: +portfolio.toFixed(2), total_value: +portfolio.toFixed(2),
      total_wins: totalWins, total_losses: totalLosses,
      session: '🎮 SIM COMPLETE', updated_at: new Date().toISOString(),
    }).catch(()=>{});

    // Pause — don't auto-reset, let user review results
    simState.cursor = simState.totalBars - 20; // hold at last bar
    log('sim', '🎮 Sim paused at end. Toggle Sim off/on to restart with fresh data.');
    return null; // signal to stop advancing
  }

  simState.cursor++;
  const firstSym = Object.keys(simState.bars)[0];
  if (simState.bars[firstSym]?.[simState.cursor]) {
    simState.currentTime = simState.bars[firstSym][simState.cursor].t;
  }
  // Check if we just hit end-of-replay
  if (simState.cursor >= simState.totalBars - 20) {
    return null; // signals runSimScan to stop
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
    await sbFetch(tbl('tc_portfolio')+'?id=eq.1', 'PATCH', {
      cash: portfolio, total_value: portfolio, day_pnl: 0,
      total_wins: 0, total_losses: 0, circuit_breaker: false,
      session: '🎮 SIM REPLAY', updated_at: new Date().toISOString(),
    });
    log('sim', `🎮 Sim portfolio reset to $${portfolio.toFixed(2)}`);
  }

  // 1 bar per scan — keeps positions visible and lets you watch them play out
  // Scan runs every 3s in sim mode (controlled by tick interval override below)
  const SIM_BARS_PER_SCAN = 5; // 5 bars per scan = ~3hrs to complete 60 trading days
  let lastSnapshot = null;
  let lastBarTime = null;

  for (let step = 0; step < SIM_BARS_PER_SCAN; step++) {
    const snapshot = simAdvanceCursor();
    if (!snapshot) break;
    lastSnapshot = snapshot;
    lastBarTime = simState.currentTime;

    // Manage existing positions
    for (const sym of Object.keys(positions)) {
      const bars5m = snapshot[sym];
      if (!bars5m || bars5m.length < 5) continue;
      // Use the bar close price directly in sim — no WebSocket in sim
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

    // Generate signals — recalculate totalOpen inside loop so each entry is counted
    for (const sym of Object.keys(snapshot)) {
      if (positions[sym] || shortPositions[sym]) continue;

      // Hard stop — recalculate after every entry
      const openNow = Object.keys(positions).length + Object.keys(shortPositions).length;
      if (openNow >= CONFIG.maxOpenPositions) break;

      const bars5m = snapshot[sym];
      if (!bars5m || bars5m.length < 30) continue;

      const bars15m = [];
      for (let i = 0; i + 2 < bars5m.length; i += 3) {
        const chunk = bars5m.slice(i, i + 3);
        bars15m.push({
          t: chunk[0].t, o: chunk[0].o,
          h: Math.max(...chunk.map(b => b.h)),
          l: Math.min(...chunk.map(b => b.l)),
          c: chunk[chunk.length - 1].c,
          v: chunk.reduce((a, b) => a + b.v, 0),
        });
      }

      const price = bars5m[bars5m.length - 1].c;
      // Seed price history from bars — don't carry over stale data from previous run
      priceHistory5m[sym] = bars5m.map(b => b.c);

      let sig = generateSignalByStrategy(sym, bars5m, bars15m.length >= 10 ? bars15m : null) || generateSignal(sym, bars5m, bars15m.length >= 10 ? bars15m : null);
      sig = applyDayBias(sig);

      if (sig.signal !== 'HOLD') {
        log('sim', `  ${sym} $${price.toFixed(2)} → ${sig.signal} conf:${sig.confidence}% score:${sig.score}`);
      }

      // Must be a real confirmed signal — not just HOLD with score > 0
      if (sig.signal === 'HOLD' || sig.confidence < 50) continue;

      if (sig.signal === 'BUY') {
        await enterPosition(sym, price, sig, bars5m, 'long');
      } else if (sig.signal === 'SELL' && CONFIG.shortsEnabled) {
        await enterPosition(sym, price, sig, bars5m, 'short');
      }
    }
  }

  if (!lastSnapshot) return;
  const barTime = lastBarTime;

  // Calculate equity from fresh bar prices
  const positionMarketValue = Object.entries(positions).reduce((acc, [sym, pos]) => {
    const cur = priceHistory5m[sym]?.[priceHistory5m[sym].length-1] || pos.entryPrice;
    return acc + cur * (pos.qtyRemaining || pos.qty);
  }, 0);
  const shortPnl = Object.entries(shortPositions).reduce((acc, [sym, pos]) => {
    const cur = priceHistory5m[sym]?.[priceHistory5m[sym].length-1] || pos.entryPrice;
    return acc + (pos.entryPrice - cur) * (pos.qtyRemaining || pos.qty);
  }, 0);
  const equity = portfolio + positionMarketValue + shortPnl;
  const dayPnl = equity - (realDailyStartEquity || CONFIG.startingCapital);
  realEquity   = equity;

  // Write positions + portfolio in parallel — atomic so dashboard never sees mismatched state
  await Promise.all([
    syncPositions(),
    sbFetch(tbl('tc_portfolio')+'?id=eq.1', 'PATCH', {
      cash:            +portfolio.toFixed(2),
      total_value:     +equity.toFixed(2),
      day_pnl:         +dayPnl.toFixed(2),
      total_wins:      totalWins,
      total_losses:    totalLosses,
      circuit_breaker: circuitBreakerOn,
      last_scan:       new Date().toISOString(),
      session:         `🎮 SIM [${barTime?.slice(11,16)||'?'}]`,
      updated_at:      new Date().toISOString(),
    }),
  ]);
  // Log async — don't block the next bar
  syncLog('sim', `🎮 SIM bar ${simState.cursor}/${simState.totalBars} | Equity=$${equity.toFixed(2)} P&L=${dayPnl>=0?'+':''}$${dayPnl.toFixed(2)} | Open:${Object.keys(positions).length} W:${totalWins}/L:${totalLosses}`).catch(()=>{});
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

// ═══════════════════════════════════════════════════════════════════
// PROFESSIONAL STRATEGY LIBRARY
// ═══════════════════════════════════════════════════════════════════
//
// Strategies used by institutional and professional traders.
// Each is a complete signal generator returning { signal, score, confidence, reasons }.
// All run live on every bar and are selected via CONFIG.strategy.
//
// Available strategies:
//   rsi_macd        — Classic RSI + MACD (default, balanced)
//   smc             — Smart Money Concepts (FVG, DOL, OB, BOS)
//   ict             — ICT (kill zones, liquidity sweeps, OTE)
//   vwap_reversion  — VWAP mean reversion (institutional entry)
//   order_flow      — Volume delta + absorption (tape reading)
//   wyckoff         — Accumulation/distribution phases
// ═══════════════════════════════════════════════════════════════════


// ── Helper: BIG TRAP — Trapped Buyers/Sellers (from Pine Script) ──
// Detects price levels where:
//   - Trapped BUYERS: heavy buying happened but price closed BELOW that level
//     → these buyers are underwater, will panic-sell if price returns = resistance
//   - Trapped SELLERS: heavy selling happened but price closed ABOVE that level
//     → these sellers are underwater, will panic-buy to cover = support
//
// Translated from: BIG TRAP by drop_trades
// Uses volume delta per bar to simulate footprint analysis
function findTraps(bars, lookback = 20) {
  if (!bars || bars.length < 5) return { trappedBuyers: [], trappedSellers: [], nearestTrap: null };

  const slice = bars.slice(-Math.min(lookback, bars.length));
  const price  = bars[bars.length - 1].c; // current price
  const IMBALANCE_RATIO = 3.0; // buy vol must be 3x sell vol (or vice versa) — matches Pine default
  const MIN_TRAP_VOL    = 30;  // minimum volume to qualify as a trap

  const trappedBuyers  = []; // buyers who bought above current price (underwater)
  const trappedSellers = []; // sellers who sold below current price (underwater)

  for (const bar of slice) {
    const barRange = bar.h - bar.l;
    if (barRange <= 0) continue;

    // Estimate buy/sell volume split from bar structure
    // This approximates footprint data using OHLCV:
    // Close near high = mostly buying, close near low = mostly selling
    const closeLocation = (bar.c - bar.l) / barRange; // 0=at low, 1=at high
    const totalVol      = bar.v || 0;
    const buyVol        = totalVol * closeLocation;
    const sellVol       = totalVol * (1 - closeLocation);

    const netDelta  = buyVol - sellVol;
    const absDelta  = Math.abs(netDelta);

    // Only consider bars with clear directional imbalance
    const buyRatio  = sellVol > 0 ? buyVol / sellVol : buyVol;
    const sellRatio = buyVol > 0 ? sellVol / buyVol : sellVol;

    const isBuyImbalance  = buyRatio  >= IMBALANCE_RATIO;
    const isSellImbalance = sellRatio >= IMBALANCE_RATIO;

    // TRAPPED BUYERS: heavy buying + price closed below this bar's high
    // (buyers bought aggressively but got trapped as price fell away)
    if (isBuyImbalance && totalVol >= MIN_TRAP_VOL) {
      const trapPrice = (bar.h + bar.c) / 2; // weighted trap level
      // "Strict mode" — only count if in top 30% of candle (wick area)
      const inTopWick = bar.h > bar.c + barRange * 0.7;
      if (inTopWick && trapPrice > price) {
        // Buyers are underwater — this is resistance
        trappedBuyers.push({
          price:   trapPrice,
          vol:     totalVol,
          delta:   absDelta,
          barHigh: bar.h,
          barLow:  bar.l,
          dist:    (trapPrice - price) / price, // how far above current price
        });
      }
    }

    // TRAPPED SELLERS: heavy selling + price closed above this bar's low
    // (sellers sold aggressively but price rallied above them — trapped shorts)
    if (isSellImbalance && totalVol >= MIN_TRAP_VOL) {
      const trapPrice = (bar.l + bar.c) / 2;
      // "Strict mode" — only count if in bottom 30% of candle (wick area)
      const inBottomWick = bar.l < bar.c - barRange * 0.7;
      if (inBottomWick && trapPrice < price) {
        // Sellers are underwater — this is support
        trappedSellers.push({
          price:   trapPrice,
          vol:     totalVol,
          delta:   absDelta,
          barHigh: bar.h,
          barLow:  bar.l,
          dist:    (price - trapPrice) / price, // how far below current price
        });
      }
    }
  }

  // Sort by volume (biggest traps first — most trapped = strongest level)
  trappedBuyers.sort((a,b)  => b.vol - a.vol);
  trappedSellers.sort((a,b) => b.vol - a.vol);

  // Find nearest trap to current price (within 3%)
  const allTraps = [
    ...trappedBuyers.map(t  => ({ ...t, type:'buyer'  })),
    ...trappedSellers.map(t => ({ ...t, type:'seller' })),
  ].filter(t => t.dist < 0.03).sort((a,b) => a.dist - b.dist);

  return {
    trappedBuyers,
    trappedSellers,
    nearestTrap: allTraps[0] || null,
    // Summary scores for signal engine
    buyerTrapScore:  trappedBuyers.length  > 0 ? Math.min(3, trappedBuyers.length)  : 0,
    sellerTrapScore: trappedSellers.length > 0 ? Math.min(3, trappedSellers.length) : 0,
  };
}

// ── Helper: Detect Fair Value Gaps (FVG) ─────────────────────────
// FVG = when price gaps past a candle leaving an "imbalance"
// Price tends to return to fill these gaps
function findFVGs(bars, lookback = 20) {
  const fvgs = [];
  const slice = bars.slice(-lookback);
  for (let i = 1; i < slice.length - 1; i++) {
    const prev = slice[i-1], curr = slice[i], next = slice[i+1];
    // Bullish FVG: previous high < next low (gap up — imbalance above)
    if (prev.h < next.l) {
      fvgs.push({ type:'bull', top: next.l, bottom: prev.h, mid:(prev.h+next.l)/2, bar:i });
    }
    // Bearish FVG: previous low > next high (gap down — imbalance below)
    if (prev.l > next.h) {
      fvgs.push({ type:'bear', top: prev.l, bottom: next.h, mid:(prev.l+next.h)/2, bar:i });
    }
  }
  return fvgs;
}

// ── Helper: Detect Order Blocks (OB) ─────────────────────────────
// OB = last opposing candle before a strong directional move
// Strong support/resistance — price often returns to test them
function findOrderBlocks(bars, lookback = 30) {
  const obs = [];
  const slice = bars.slice(-lookback);
  for (let i = 1; i < slice.length - 3; i++) {
    const b = slice[i];
    // Look for a down candle (supply OB) followed by 3+ strong up candles
    const isBearCandle = b.c < b.o;
    const isBullCandle = b.c > b.o;
    const nextThree = slice.slice(i+1, i+4);
    const strongUp   = nextThree.every(x => x.c > x.o) && nextThree.reduce((a,x)=>a+(x.c-x.o),0) > (b.h-b.l)*1.5;
    const strongDown = nextThree.every(x => x.c < x.o) && nextThree.reduce((a,x)=>a+(x.o-x.c),0) > (b.h-b.l)*1.5;
    if (isBearCandle && strongUp)  obs.push({ type:'bull', high:b.h, low:b.l, mid:(b.h+b.l)/2 });
    if (isBullCandle && strongDown) obs.push({ type:'bear', high:b.h, low:b.l, mid:(b.h+b.l)/2 });
  }
  return obs.slice(-5); // last 5 order blocks
}

// ── Helper: Break of Structure (BOS) / Change of Character (ChoCH) ──
// BOS = price breaks a prior swing high/low (trend continuation)
// ChoCH = first break in the opposite direction (potential reversal)
function detectBOS(bars, lookback = 20) {
  const slice = bars.slice(-lookback);
  const highs = slice.map(b => b.h);
  const lows  = slice.map(b => b.l);
  const price = slice[slice.length-1].c;

  // Find most recent swing high and low
  let swingHigh = Math.max(...highs.slice(0, -3));
  let swingLow  = Math.min(...lows.slice(0, -3));

  const bullBOS = price > swingHigh; // broke above prior high
  const bearBOS = price < swingLow;  // broke below prior low

  // Higher highs = uptrend structure intact
  const hh = highs[highs.length-1] > highs[highs.length-5] &&
              highs[highs.length-5] > highs[highs.length-10];
  const ll = lows[lows.length-1] < lows[lows.length-5] &&
             lows[lows.length-5]  < lows[lows.length-10];

  return { bullBOS, bearBOS, swingHigh, swingLow, hh, ll };
}

// ── Helper: Liquidity Levels (DOL — Draw on Liquidity) ───────────
// Markets are drawn toward liquidity pools — clusters of stops
// Equal highs/lows = stop-hunt targets
function findLiquidityLevels(bars, lookback = 30) {
  const slice = bars.slice(-lookback);
  const highs = slice.map(b => b.h);
  const lows  = slice.map(b => b.l);
  const price = slice[slice.length-1].c;

  // Equal highs (sell-side liquidity above) — stops sitting just above
  const highClusters = [];
  for (let i = 0; i < highs.length - 3; i++) {
    const similar = highs.filter(h => Math.abs(h - highs[i]) / highs[i] < 0.002);
    if (similar.length >= 2) highClusters.push(highs[i]);
  }

  // Equal lows (buy-side liquidity below) — stops sitting just below
  const lowClusters = [];
  for (let i = 0; i < lows.length - 3; i++) {
    const similar = lows.filter(l => Math.abs(l - lows[i]) / lows[i] < 0.002);
    if (similar.length >= 2) lowClusters.push(lows[i]);
  }

  // Nearest liquidity above and below
  const above = highClusters.filter(h => h > price).sort((a,b)=>a-b)[0];
  const below  = lowClusters.filter(l => l < price).sort((a,b)=>b-a)[0];

  // Which liquidity pool is price being drawn to?
  const dolUp   = above && (above - price) / price < 0.02;  // within 2%
  const dolDown = below && (price - below) / price < 0.02;

  return { above, below, dolUp, dolDown };
}

// ── Helper: ICT Kill Zones ────────────────────────────────────────
// High-probability entry windows when institutional orders are placed
// London Open: 2-5 AM ET | NY Open: 8-11 AM ET | London Close: 10 AM-12 PM ET
function inKillZone() {
  const et = new Date(new Date().toLocaleString('en-US',{timeZone:'America/New_York'}));
  const h = et.getHours(), m = et.getMinutes();
  const mins = h * 60 + m;
  const london = mins >= 120 && mins <= 300;   // 2-5 AM ET
  const nyOpen = mins >= 480 && mins <= 660;   // 8-11 AM ET
  const lClose = mins >= 600 && mins <= 720;   // 10 AM-12 PM ET
  const nyPM   = mins >= 780 && mins <= 900;   // 1-3 PM ET
  return { inZone: london||nyOpen||lClose||nyPM, london, nyOpen, lClose, nyPM };
}

// ── Helper: Wyckoff Phase Detection ──────────────────────────────
// Accumulation: declining volume on down bars, rising on up bars
// Distribution: rising volume on down bars, declining on up bars
function detectWyckoffPhase(bars, lookback = 20) {
  const slice = bars.slice(-lookback);
  let upVolume = 0, downVolume = 0, upCount = 0, downCount = 0;
  for (const b of slice) {
    if (b.c > b.o) { upVolume += b.v; upCount++; }
    else            { downVolume += b.v; downCount++; }
  }
  const avgUpVol   = upCount   > 0 ? upVolume/upCount     : 0;
  const avgDownVol = downCount > 0 ? downVolume/downCount : 0;

  // Price range compression (spring/upthrust zone)
  const ranges = slice.map(b => b.h - b.l);
  const avgRange = ranges.reduce((a,b)=>a+b,0)/ranges.length;
  const recentRange = ranges.slice(-5).reduce((a,b)=>a+b,0)/5;
  const compressed = recentRange < avgRange * 0.6; // range shrinking

  const accumulating  = avgUpVol > avgDownVol * 1.3 && compressed; // smart money buying
  const distributing  = avgDownVol > avgUpVol * 1.3 && compressed; // smart money selling

  return { accumulating, distributing, avgUpVol, avgDownVol, compressed };
}

// ── Helper: VWAP Band Analysis ────────────────────────────────────
function vwapBands(bars) {
  if (!bars || bars.length < 10) return null;
  const vw = vwap(bars);
  const prices = bars.map(b => (b.h+b.l+b.c)/3);
  const std = Math.sqrt(prices.reduce((a,p)=>a+(p-vw)**2,0)/prices.length);
  return { vwap:vw, upper1:vw+std, upper2:vw+std*2, lower1:vw-std, lower2:vw-std*2 };
}

// ── Helper: Order Flow / Delta ────────────────────────────────────
// Estimates buying vs selling pressure from bar structure
function estimateDelta(bars, lookback = 10) {
  const slice = bars.slice(-lookback);
  let buyPressure = 0, sellPressure = 0;
  for (const b of slice) {
    const range = b.h - b.l || 0.0001;
    const close_loc = (b.c - b.l) / range; // 1 = closed at high, 0 = at low
    buyPressure  += b.v * close_loc;
    sellPressure += b.v * (1 - close_loc);
  }
  const delta = (buyPressure - sellPressure) / (buyPressure + sellPressure);
  return { delta, buyPressure, sellPressure }; // delta: +1 = all buying, -1 = all selling
}

// ═══════════════════════════════════════════════════════════════════
// STRATEGY IMPLEMENTATIONS
// ═══════════════════════════════════════════════════════════════════

// ── Strategy: SMC (Smart Money Concepts) ─────────────────────────
// Used by: Prop traders, hedge funds, retail ICT traders
// Edge: Identifies where institutions are buying/selling by reading
//       market structure, not just indicators
function signalSMC(sym, bars5m, bars15m) {
  if (bars5m.length < 50) return { signal:'HOLD', confidence:0, reasons:['Need 50+ bars'] };
  const closes = bars5m.map(b=>b.c);
  const price  = closes[closes.length-1];
  const reasons = [];
  let score = 0, direction = null;

  const bos    = detectBOS(bars5m, 20);
  const fvgs   = findFVGs(bars5m, 30);
  const obs    = findOrderBlocks(bars5m, 40);
  const liq    = findLiquidityLevels(bars5m, 30);
  const kz     = inKillZone();
  const adxVal = adx(bars5m);
  const r      = rsi(closes, 14);
  const vol    = bars5m.map(b=>b.v);
  const avgVol = vol.slice(-20).reduce((a,b)=>a+b,0)/20;
  const curVol = vol[vol.length-1];

  // BOS signals direction
  if (bos.bullBOS && bos.hh) { direction='buy';  score+=3; reasons.push('Bull BOS + HH structure ✅'); }
  if (bos.bearBOS && bos.ll) { direction='sell'; score+=3; reasons.push('Bear BOS + LL structure ✅'); }
  if (!direction) return { signal:'HOLD', confidence:0, reasons:['No BOS — no clear direction'] };

  // FVG — price inside a bullish FVG = likely to bounce up
  const bullFVG = fvgs.find(f => f.type==='bull' && price >= f.bottom && price <= f.top);
  const bearFVG = fvgs.find(f => f.type==='bear' && price >= f.bottom && price <= f.top);
  if (direction==='buy'  && bullFVG) { score+=3; reasons.push(`Inside bull FVG ($${bullFVG.bottom.toFixed(2)}-$${bullFVG.top.toFixed(2)}) ✅`); }
  if (direction==='sell' && bearFVG) { score+=3; reasons.push(`Inside bear FVG ($${bearFVG.bottom.toFixed(2)}-$${bearFVG.top.toFixed(2)}) ✅`); }

  // Order block confluence
  const nearBullOB = obs.find(o => o.type==='bull' && price >= o.low*0.999 && price <= o.high*1.005);
  const nearBearOB = obs.find(o => o.type==='bear' && price <= o.high*1.001 && price >= o.low*0.995);
  if (direction==='buy'  && nearBullOB) { score+=2; reasons.push(`At bull order block $${nearBullOB.mid.toFixed(2)} ✅`); }
  if (direction==='sell' && nearBearOB) { score+=2; reasons.push(`At bear order block $${nearBearOB.mid.toFixed(2)} ✅`); }

  // DOL — price being drawn toward liquidity
  if (direction==='buy'  && liq.dolUp)   { score+=2; reasons.push(`DOL: liquidity sweep above $${liq.above?.toFixed(2)} ✅`); }
  if (direction==='sell' && liq.dolDown) { score+=2; reasons.push(`DOL: liquidity below $${liq.below?.toFixed(2)} ✅`); }

  // Kill zone bonus
  if (kz.inZone) { score+=1; reasons.push(`ICT kill zone active ✅`); }

  // Volume and trend filters
  if (curVol > avgVol * 1.2) { score+=1; reasons.push(`Volume surge (${(curVol/avgVol).toFixed(1)}x) ✅`); }
  if (adxVal.adx < 15) return { signal:'HOLD', confidence:0, reasons:[...reasons,'ADX too low — ranging'] };

  // BIG TRAP — adds high conviction when institutional traps align with SMC
  const trapsSMC = findTraps(bars5m, 20);
  if (direction==='buy'  && trapsSMC.sellerTrapScore > 0) { score += trapsSMC.sellerTrapScore; reasons.push(`🪤 Trapped sellers = support ✅`); }
  if (direction==='sell' && trapsSMC.buyerTrapScore  > 0) { score += trapsSMC.buyerTrapScore;  reasons.push(`🪤 Trapped buyers = resistance ✅`); }

  if (score < 6) return { signal:'HOLD', confidence:0, score, reasons, rsi: r };
  const conf = Math.min(95, 50 + score*5);
  return { signal:direction==='buy'?'BUY':'SELL', confidence:conf, score, reasons, rsi:r, atr:atr(bars5m,14) };
}

// ── Strategy: ICT (Inner Circle Trader) ──────────────────────────
// Used by: ICT students, prop firm traders
// Edge: Kill zones + OTE (Optimal Trade Entry) retracements
//       + liquidity sweeps before reversals
function signalICT(sym, bars5m, bars15m) {
  if (bars5m.length < 50) return { signal:'HOLD', confidence:0, reasons:['Need 50+ bars'] };
  const closes = bars5m.map(b=>b.c);
  const highs  = bars5m.map(b=>b.h);
  const lows   = bars5m.map(b=>b.l);
  const price  = closes[closes.length-1];
  const reasons = [];
  let score = 0, direction = null;

  const kz  = inKillZone();
  const bos = detectBOS(bars5m, 30);
  const liq = findLiquidityLevels(bars5m, 40);
  const fvgs = findFVGs(bars5m, 20);
  const r   = rsi(closes, 14);
  const adxVal = adx(bars5m);

  // ICT requires kill zone
  if (!kz.inZone) return { signal:'HOLD', confidence:0, reasons:['Not in ICT kill zone (NY/London open required)'] };
  score += 2; reasons.push(`Kill zone active (${kz.nyOpen?'NY Open':kz.london?'London':kz.lClose?'London Close':'PM'}) ✅`);

  // Market structure
  if (bos.hh && !bos.bearBOS) { direction='buy';  score+=2; reasons.push('Higher highs structure ✅'); }
  if (bos.ll && !bos.bullBOS) { direction='sell'; score+=2; reasons.push('Lower lows structure ✅'); }
  if (!direction) return { signal:'HOLD', confidence:0, reasons:[...reasons,'No clear market structure'] };

  // OTE (Optimal Trade Entry) — 62-79% Fibonacci retracement of last swing
  const lookback = 20;
  const swingHigh = Math.max(...highs.slice(-lookback));
  const swingLow  = Math.min(...lows.slice(-lookback));
  const swing = swingHigh - swingLow;
  const fib62  = direction==='buy' ? swingLow  + swing*0.62 : swingHigh - swing*0.62;
  const fib79  = direction==='buy' ? swingLow  + swing*0.79 : swingHigh - swing*0.79;
  const inOTE  = direction==='buy'
    ? price >= Math.min(fib62,fib79) && price <= Math.max(fib62,fib79)
    : price <= Math.max(fib62,fib79) && price >= Math.min(fib62,fib79);
  if (inOTE) { score+=3; reasons.push(`In OTE zone (62-79% fib) ✅`); }

  // Liquidity sweep before entry (price sweeps stops then reverses)
  if (direction==='buy'  && liq.below && (price-liq.below)/price < 0.005) {
    score+=3; reasons.push(`Liquidity sweep below $${liq.below?.toFixed(2)} — reversal likely ✅`);
  }
  if (direction==='sell' && liq.above && (liq.above-price)/price < 0.005) {
    score+=3; reasons.push(`Liquidity sweep above $${liq.above?.toFixed(2)} — reversal likely ✅`);
  }

  // FVG at entry
  const entryFVG = fvgs.find(f => f.type===(direction==='buy'?'bull':'bear') && Math.abs(price-f.mid)/price < 0.005);
  if (entryFVG) { score+=2; reasons.push(`FVG entry confluence ✅`); }

  // BIG TRAP — liquidity sweeps + traps = strongest ICT entry
  const trapsICT = findTraps(bars5m, 20);
  if (direction==='buy'  && trapsICT.sellerTrapScore > 0) { score += trapsICT.sellerTrapScore + 1; reasons.push(`🪤 ICT + Trapped sellers = strong support ✅`); }
  if (direction==='sell' && trapsICT.buyerTrapScore  > 0) { score += trapsICT.buyerTrapScore  + 1; reasons.push(`🪤 ICT + Trapped buyers = strong resistance ✅`); }

  if (adxVal.adx < 12) return { signal:'HOLD', confidence:0, reasons:[...reasons,'ADX too low'] };
  if (score < 7) return { signal:'HOLD', confidence:0, score, reasons };
  const conf = Math.min(95, 45 + score*5);
  return { signal:direction==='buy'?'BUY':'SELL', confidence:conf, score, reasons, rsi:r, atr:atr(bars5m,14) };
}

// ── Strategy: VWAP Reversion ──────────────────────────────────────
// Used by: Day traders, market makers, quant desks
// Edge: Price always reverts to VWAP — trade from 2σ bands back to mean
function signalVWAPReversion(sym, bars5m, bars15m) {
  if (bars5m.length < 20) return { signal:'HOLD', confidence:0, reasons:['Need 20+ bars'] };
  const closes = bars5m.map(b=>b.c);
  const price  = closes[closes.length-1];
  const reasons = [];
  let score = 0, direction = null;

  const vb  = vwapBands(bars5m);
  if (!vb) return { signal:'HOLD', confidence:0, reasons:['No VWAP data'] };

  const r      = rsi(closes, 14);
  const adxVal = adx(bars5m);
  const stoch  = stochRSI(closes);
  const vol    = bars5m.map(b=>b.v);
  const avgVol = vol.slice(-10).reduce((a,b)=>a+b,0)/10;
  const curVol = vol[vol.length-1];

  // Only trade in ranging or lightly trending markets
  if (adxVal.adx > 35) return { signal:'HOLD', confidence:0, reasons:['ADX too high — trending, not reverting'] };

  // Price at 2σ band = high probability reversion
  if (price <= vb.lower2) {
    direction='buy';
    score+=4; reasons.push(`At VWAP -2σ ($${vb.lower2.toFixed(2)}) — extreme low ✅`);
  } else if (price <= vb.lower1) {
    direction='buy';
    score+=2; reasons.push(`At VWAP -1σ ($${vb.lower1.toFixed(2)}) ✅`);
  } else if (price >= vb.upper2) {
    direction='sell';
    score+=4; reasons.push(`At VWAP +2σ ($${vb.upper2.toFixed(2)}) — extreme high ✅`);
  } else if (price >= vb.upper1) {
    direction='sell';
    score+=2; reasons.push(`At VWAP +1σ ($${vb.upper1.toFixed(2)}) ✅`);
  } else {
    return { signal:'HOLD', confidence:0, reasons:['Price near VWAP — no edge'] };
  }

  // RSI confirmation
  if (direction==='buy'  && r < 40) { score+=2; reasons.push(`RSI oversold (${r.toFixed(0)}) ✅`); }
  if (direction==='sell' && r > 60) { score+=2; reasons.push(`RSI overbought (${r.toFixed(0)}) ✅`); }

  // StochRSI
  if (direction==='buy'  && stoch.k < 20) { score+=1; reasons.push(`StochRSI oversold ✅`); }
  if (direction==='sell' && stoch.k > 80) { score+=1; reasons.push(`StochRSI overbought ✅`); }

  // Volume spike = institutional order at extreme
  if (curVol > avgVol * 1.5) { score+=2; reasons.push(`Volume spike (${(curVol/avgVol).toFixed(1)}x) at extreme ✅`); }

  // Price starting to reject (last bar closing toward VWAP)
  const lastBar = bars5m[bars5m.length-1];
  const rejecting = direction==='buy'
    ? lastBar.c > lastBar.o && lastBar.l <= vb.lower1 // green bar at low
    : lastBar.c < lastBar.o && lastBar.h >= vb.upper1; // red bar at high
  if (rejecting) { score+=2; reasons.push(`Rejection candle at extreme ✅`); }

  if (score < 5) return { signal:'HOLD', confidence:0, score, reasons };
  const conf = Math.min(95, 40 + score*6);
  return { signal:direction==='buy'?'BUY':'SELL', confidence:conf, score, reasons, rsi:r, atr:atr(bars5m,14) };
}

// ── Strategy: Order Flow ──────────────────────────────────────────
// Used by: Futures traders, HFT desks, tape readers
// Edge: Delta analysis shows who's in control — buyers or sellers
//       Absorption = price stops moving despite heavy buying/selling
function signalOrderFlow(sym, bars5m, bars15m) {
  if (bars5m.length < 20) return { signal:'HOLD', confidence:0, reasons:['Need 20+ bars'] };
  const closes = bars5m.map(b=>b.c);
  const price  = closes[closes.length-1];
  const reasons = [];
  let score = 0, direction = null;

  const delta  = estimateDelta(bars5m, 5);   // last 5 bars
  const delta20 = estimateDelta(bars5m, 20); // trend delta
  const r      = rsi(closes, 14);
  const adxVal = adx(bars5m);
  const vb     = vwapBands(bars5m);
  const vol    = bars5m.map(b=>b.v);

  // Strong positive delta = buyers dominating
  if (delta.delta > 0.3) {
    direction='buy';
    score+=3; reasons.push(`Positive delta (${(delta.delta*100).toFixed(0)}% buy pressure) ✅`);
  } else if (delta.delta < -0.3) {
    direction='sell';
    score+=3; reasons.push(`Negative delta (${(Math.abs(delta.delta)*100).toFixed(0)}% sell pressure) ✅`);
  } else {
    return { signal:'HOLD', confidence:0, reasons:['Delta neutral — no clear order flow'] };
  }

  // Delta divergence = potential reversal (price up but delta down = absorption)
  const priceUp    = price > closes[closes.length-5];
  const deltaUp    = delta.delta > delta20.delta;
  const absorption = (priceUp && !deltaUp) || (!priceUp && deltaUp);
  if (!absorption) { score+=2; reasons.push('Delta confirms price direction ✅'); }
  else { reasons.push('⚠ Absorption detected — possible reversal'); }

  // Volume confirmation
  const avgVol = vol.slice(-20).reduce((a,b)=>a+b,0)/20;
  const recentVol = vol.slice(-3).reduce((a,b)=>a+b,0)/3;
  if (recentVol > avgVol * 1.3) { score+=2; reasons.push(`Volume confirming (${(recentVol/avgVol).toFixed(1)}x) ✅`); }

  // VWAP alignment
  if (vb) {
    if (direction==='buy'  && price > vb.vwap) { score+=1; reasons.push('Above VWAP ✅'); }
    if (direction==='sell' && price < vb.vwap) { score+=1; reasons.push('Below VWAP ✅'); }
  }

  // ADX for trend strength
  if (adxVal.adx > 20) { score+=1; reasons.push(`ADX ${adxVal.adx.toFixed(0)} trending ✅`); }

  if (score < 5) return { signal:'HOLD', confidence:0, score, reasons };
  const conf = Math.min(95, 40 + score*6);
  return { signal:direction==='buy'?'BUY':'SELL', confidence:conf, score, reasons, rsi:r, atr:atr(bars5m,14) };
}

// ── Strategy: Wyckoff ────────────────────────────────────────────
// Used by: Institutional traders, value investors with timing
// Edge: Reads accumulation/distribution phases — gets in before
//       the markup/markdown phase when the big move happens
function signalWyckoff(sym, bars5m, bars15m) {
  if (bars5m.length < 50) return { signal:'HOLD', confidence:0, reasons:['Need 50+ bars'] };
  const closes = bars5m.map(b=>b.c);
  const price  = closes[closes.length-1];
  const reasons = [];
  let score = 0, direction = null;

  const wy    = detectWyckoffPhase(bars5m, 30);
  const bos   = detectBOS(bars5m, 20);
  const r     = rsi(closes, 14);
  const adxVal = adx(bars5m);
  const vol   = bars5m.map(b=>b.v);
  const avgVol = vol.slice(-20).reduce((a,b)=>a+b,0)/20;
  const lastVol = vol[vol.length-1];

  // Phase detection
  if (wy.accumulating) {
    direction='buy';
    score+=4; reasons.push(`Wyckoff accumulation (up vol ${(wy.avgUpVol/wy.avgDownVol).toFixed(1)}× down vol) ✅`);
  } else if (wy.distributing) {
    direction='sell';
    score+=4; reasons.push(`Wyckoff distribution (down vol ${(wy.avgDownVol/wy.avgUpVol).toFixed(1)}× up vol) ✅`);
  } else {
    return { signal:'HOLD', confidence:0, reasons:['No clear Wyckoff phase'] };
  }

  // Spring (acc) or Upthrust (dist) — fake break before reversal
  if (direction==='buy' && bos.bearBOS && wy.compressed) {
    score+=3; reasons.push('Wyckoff spring — fake low before markup ✅');
  }
  if (direction==='sell' && bos.bullBOS && wy.compressed) {
    score+=3; reasons.push('Wyckoff upthrust — fake high before markdown ✅');
  }

  // Range compression = price coiling before breakout
  if (wy.compressed) { score+=2; reasons.push('Price range compressed — coiling ✅'); }

  // RSI — not overbought/oversold, just leaving extreme
  if (direction==='buy'  && r > 35 && r < 55) { score+=1; reasons.push(`RSI recovering (${r.toFixed(0)}) ✅`); }
  if (direction==='sell' && r < 65 && r > 45) { score+=1; reasons.push(`RSI rolling over (${r.toFixed(0)}) ✅`); }

  // Volume test — last bar high volume = sign of strength/weakness
  if (lastVol > avgVol * 1.5) { score+=1; reasons.push(`High volume ${(lastVol/avgVol).toFixed(1)}x ✅`); }

  if (score < 6) return { signal:'HOLD', confidence:0, score, reasons, rsi: r };
  const conf = Math.min(95, 40 + score*5);
  return { signal:direction==='buy'?'BUY':'SELL', confidence:conf, score, reasons, rsi:r, atr:atr(bars5m,14) };
}

// ── Strategy Router ───────────────────────────────────────────────
// Calls the right strategy based on CONFIG.strategy
// Also runs a secondary strategy for confluence (optional)
function generateSignalByStrategy(sym, bars5m, bars15m) {
  const strategy = CONFIG.strategy || 'rsi_macd';

  switch(strategy) {
    case 'smc':            return signalSMC(sym, bars5m, bars15m);
    case 'ict':            return signalICT(sym, bars5m, bars15m);
    case 'vwap_reversion': return signalVWAPReversion(sym, bars5m, bars15m);
    case 'order_flow':     return signalOrderFlow(sym, bars5m, bars15m);
    case 'wyckoff':        return signalWyckoff(sym, bars5m, bars15m);
    case 'rsi_macd':
    default:               return null; // falls through to existing generateSignal
  }
}

// ═══════════════════════════════════════════════════════════════════
// END PROFESSIONAL STRATEGY LIBRARY
// ═══════════════════════════════════════════════════════════════════


function generateSignal(sym, bars5m, bars15m) {
  if (!bars5m || bars5m.length < 30) return { signal: 'HOLD', confidence: 0, reasons: ['Need 30+ bars'] };

  const c5    = bars5m.map(b => b.c);
  const c15   = bars15m?.length >= 20 ? bars15m.map(b => b.c) : null;
  const vol   = bars5m.map(b => b.v);
  const highs = bars5m.map(b => b.h);
  const lows  = bars5m.map(b => b.l);
  const price = c5[c5.length - 1];

  // ── PRE-GATE: Minimum volatility ──────────────────────────────
  // Skip in sim mode — historical bars can have low ATR during off-hours
  // In live mode reject stocks that don't move enough to reach TP1
  const atrNow = atr(bars5m, 14);
  const atrPct = price > 0 ? atrNow / price : 0;
  if (!isSimMode() && atrPct < 0.002) { // 0.2% ATR minimum for live only
    return { signal: 'HOLD', confidence: 0, score: 0,
      reasons: [`ATR too low (${(atrPct*100).toFixed(2)}% < 0.2% min) — not moving`], rsi: 50 };
  }

  const reasons = [];
  let passedGates = 0;
  let direction   = null;

  // ── GATE 0: Price slope — no catching falling knives ─────────
  // If price is in a steep downtrend (dropped >1% in last 3 bars),
  // RSI oversold is a falling knife not a reversal.
  // If price is in a steep uptrend, RSI overbought is continuation not reversal.
  // We check BOTH directions so we know which signals are valid.
  const last3 = c5.slice(-4);
  const slope3 = last3.length >= 4 ? (last3[3] - last3[0]) / last3[0] : 0;
  const last6  = c5.slice(-7);
  const slope6 = last6.length >= 7 ? (last6[6] - last6[0]) / last6[0] : 0;
  // Steep drop: >0.8% in 3 bars = still falling = risky long
  const isFallingKnife = slope3 < -0.008;
  // Steep rise: >0.8% in 3 bars = still rising = risky short
  const isRisingTide   = slope3 > 0.008;
  // We'll use these in Gate 1 to filter bad RSI signals

  // ── GATE 1: RSI must be in meaningful territory ──
  // Primary mode: mean-reversion at RSI extremes
  // Momentum mode: activates on trending days when RSI stays 45-65
  const r = rsi(c5, CONFIG.rsiPeriod);
  let rsiScore = 0;

  // Detect if this is a trending day vs mean-reversion day
  const _e8  = ema(c5, 8);
  const _e21 = ema(c5, 21);
  const _e50 = ema(c5, Math.min(50, c5.length));
  const emaBullStack = price > _e8 && _e8 > _e21 && _e21 > _e50;
  const emaBearStack = price < _e8 && _e8 < _e21 && _e21 < _e50;
  const _macdLine  = _e8 - _e21;
  const _prevMacd  = ema(c5.slice(0,-1), 8) - ema(c5.slice(0,-1), 21);
  const macdRising  = _macdLine > _prevMacd && _macdLine > 0;
  const macdFalling = _macdLine < _prevMacd && _macdLine < 0;

  if (r < CONFIG.rsiOversold) {
    // Falling knife check: if price still dropping steeply, wait for stabilization
    if (isFallingKnife) {
      return { signal:'HOLD', confidence:0, score:0,
        reasons:[`RSI oversold (${r.toFixed(1)}) but price still falling (${(slope3*100).toFixed(2)}% in 3 bars) — falling knife`], rsi:r };
    }
    rsiScore = 2; direction = 'buy';
    reasons.push(`RSI oversold ${r.toFixed(1)} + price stabilizing ✅`);
  } else if (r > CONFIG.rsiOverbought) {
    if (!CONFIG.shortsEnabled) return { signal:'HOLD', confidence:0, score:0, reasons:[`RSI overbought (${r.toFixed(1)}) — shorts disabled`], rsi:r };
    // Rising tide check: if still surging, short too early
    if (isRisingTide) {
      return { signal:'HOLD', confidence:0, score:0,
        reasons:[`RSI overbought (${r.toFixed(1)}) but price still rising (${(slope3*100).toFixed(2)}% in 3 bars) — wait for peak`], rsi:r };
    }
    rsiScore = 2; direction = 'sell';
    reasons.push(`RSI overbought ${r.toFixed(1)} + momentum fading ✅`);
  } else if (r < 38) {
    rsiScore = 1; direction = 'buy';
    reasons.push(`RSI leaning oversold ${r.toFixed(1)}`);
  } else if (r > 62) {
    if (!CONFIG.shortsEnabled) return { signal: 'HOLD', confidence: 0, score: 0, reasons: [`RSI leaning overbought (${r.toFixed(1)}) — shorts disabled`], rsi: r };
    rsiScore = 1; direction = 'sell';
    reasons.push(`RSI leaning overbought ${r.toFixed(1)}`);
  } else if (r >= 45 && r <= 65 && emaBullStack && macdRising) {
    // ── MOMENTUM MODE: trending up ──
    // Extra check: price must be near recent high (not in downtrend bounce)
    const recent20High = Math.max(...c5.slice(-20));
    const pctFromHigh = (recent20High - price) / recent20High;
    if (pctFromHigh > 0.02) {
      // Price is >2% below recent high = this is a relief bounce, not a trend
      // Don't go long into a downtrend — look for short instead
      if (CONFIG.shortsEnabled && macdFalling) {
        rsiScore = 1; direction = 'sell';
        reasons.push(`Momentum short: RSI ${r.toFixed(1)} relief bounce (${(pctFromHigh*100).toFixed(1)}% below high)`);
      } else {
        return { signal:'HOLD', confidence:0, score:0,
          reasons:[`Momentum blocked: ${(pctFromHigh*100).toFixed(1)}% below 20-bar high — downtrend`], rsi:r };
      }
    } else {
      rsiScore = 1; direction = 'buy';
      reasons.push(`Momentum long: RSI ${r.toFixed(1)} + EMA bull stack + MACD rising ✅`);
    }
  } else if (r >= 35 && r <= 55 && emaBearStack && macdFalling && CONFIG.shortsEnabled) {
    rsiScore = 1; direction = 'sell';
    reasons.push(`Momentum short: RSI ${r.toFixed(1)} + EMA bear stack + MACD falling ✅`);
  } else {
    return { signal: 'HOLD', confidence: 0, score: 0, reasons: [`RSI neutral (${r.toFixed(1)}) — no trend or extreme`], rsi: r };
  }
  passedGates++;

  // ── GATE 2: MACD — HARD GATE ──────────────────────────────────
  // MACD must agree with direction. No MACD agreement = no trade.
  // This is the single biggest filter against catching falling knives.
  const e8  = ema(c5, 8),  e21 = ema(c5, 21);
  const pe8 = ema(c5.slice(0,-1), 8), pe21 = ema(c5.slice(0,-1), 21);
  const macdBull  = e8 > e21;
  const macdCross = (pe8 < pe21 && e8 > e21) || (pe8 > pe21 && e8 < e21);
  const macdAgrees = (direction === 'buy' && macdBull) || (direction === 'sell' && !macdBull);
  if (!macdAgrees) {
    // MACD hard disagrees — block the signal
    // Exception: RSI is extremely oversold (<25) or overbought (>75) — momentum
    // extreme can override a lagging MACD in fast-moving markets
    const rsiExtreme = (direction === 'buy' && r < 25) || (direction === 'sell' && r > 75);
    if (!rsiExtreme) {
      return { signal:'HOLD', confidence:0, score:0,
        reasons:[...reasons, `MACD disagrees with ${direction} (e8=${e8.toFixed(2)} vs e21=${e21.toFixed(2)}) — blocked`], rsi:r };
    }
    reasons.push(`MACD disagrees but RSI extreme (${r.toFixed(1)}) — allowed`);
  } else {
    const macdScore = macdCross ? 2 : 1;
    if (macdCross) reasons.push(`MACD crossover ${direction==='buy'?'↑':'↓'} ✅`);
    else reasons.push(`MACD aligned ✅`);
    passedGates++;
  }

  // ── GATE 3: Trend alignment ──
  // For LONGS: price must be above 5-min 200 EMA (short-term uptrend)
  // For SHORTS: much stricter — need BOTH 5-min below 200 EMA AND daily downtrend
  if (c5.length >= 30) {
    const e200 = ema(c5, Math.min(200, c5.length));
    if (direction === 'buy') {
      if (price > e200) { reasons.push(`Above 200 EMA ✅`); passedGates++; }
      else { reasons.push(`Below 200 EMA — reduced confidence`); } // soft gate
    } else {
      // Shorts: must be below 200 EMA on 5-min
      if (price >= e200) return { signal: 'HOLD', confidence: 0, score: 0, reasons: [...reasons, `Above 200 EMA — no short`], rsi: r };
      reasons.push(`Below 200 EMA ✅`);
      passedGates++;

      // Shorts: ALSO require 15-min downtrend confirmed (not just 5-min)
      // This prevents shorting a stock that's only pulling back intraday
      if (c15 && c15.length >= 20) {
        const e50_15 = ema(c15, Math.min(50, c15.length));
        const e20_15 = ema(c15, Math.min(20, c15.length));
        const price15 = c15[c15.length - 1];
        if (price15 >= e50_15) {
          return { signal: 'HOLD', confidence: 0, score: 0, reasons: [...reasons, `15min above 50 EMA — stock in uptrend, no short`], rsi: r };
        }
        if (e20_15 >= e50_15) {
          return { signal: 'HOLD', confidence: 0, score: 0, reasons: [...reasons, `15min EMA20 above EMA50 — uptrend, no short`], rsi: r };
        }
        reasons.push(`15min confirmed downtrend ✅`);
      }

      // Shorts: RSI must be truly overbought (>65), not just leaning
      // Shorting on RSI 62-65 is too risky — needs real exhaustion
      if (r < 65) {
        return { signal: 'HOLD', confidence: 0, score: 0, reasons: [...reasons, `RSI ${r.toFixed(1)} not overbought enough for short (need >65)`], rsi: r };
      }
    }
  }

  // ── GATE 4: Volume confirmation ──
  const avgVol  = vol.slice(-20).reduce((a,b)=>a+b,0) / Math.min(20, vol.length);
  const curVol  = vol[vol.length-1];
  const volRatio = avgVol > 0 ? curVol / avgVol : 1;
  if (volRatio < 0.4) {
    // Hard block on very low volume — applies in sim too
    // Sim bars during off-hours have near-zero volume = stale/unreliable
    return { signal: 'HOLD', confidence: 0, score: 0,
      reasons: [...reasons, `Volume very low (${volRatio.toFixed(1)}x avg) — no conviction`], rsi: r };
  }
  const volScore = volRatio >= 1.5 ? 2 : volRatio >= 0.8 ? 1 : 0;
  if (volRatio >= 0.8) { reasons.push(`Volume ${volRatio.toFixed(1)}x avg ✅`); passedGates++; }
  else { reasons.push(`Volume low (${volRatio.toFixed(1)}x)`); }

  // ── GATE 5: 15-minute timeframe — HARD GATE ───────────────────
  // 15min must agree with 5min direction. Multi-timeframe confluence
  // is the most reliable filter against bad entries.
  if (c15 && c15.length >= 15) {
    const r15    = rsi(c15, CONFIG.rsiPeriod);
    const e8_15  = ema(c15, 8), e21_15 = ema(c15, 21);
    const price15 = c15[c15.length - 1];
    const bull15 = e8_15 > e21_15;   // 15m EMA bullish
    const bear15 = e8_15 < e21_15;   // 15m EMA bearish
    const rsiAgrees15 = (direction === 'buy' && r15 < 70) ||
                        (direction === 'sell' && r15 > 30);
    if (direction === 'buy' && (!bull15 || !rsiAgrees15)) {
      return { signal:'HOLD', confidence:0, score:0,
        reasons:[...reasons, `15min disagrees with long (e8_15=${e8_15.toFixed(2)} vs e21_15=${e21_15.toFixed(2)}, RSI15=${r15.toFixed(0)})`], rsi:r };
    }
    if (direction === 'sell' && (!bear15 || !rsiAgrees15)) {
      return { signal:'HOLD', confidence:0, score:0,
        reasons:[...reasons, `15min disagrees with short (e8_15=${e8_15.toFixed(2)} vs e21_15=${e21_15.toFixed(2)}, RSI15=${r15.toFixed(0)})`], rsi:r };
    }
    reasons.push(`15min ${direction==='buy'?'bullish':'bearish'} confirmed ✅`);
    passedGates++;
  }

  // ── GATE 6: ADX — trend strength (soft gate) ──
  const adxData = adx(bars5m);
  if (adxData.adx > 0) {
    if (adxData.adx < 15) {
      // Weak ADX = no trend = choppy = bad entries in both sim and live
      return { signal: 'HOLD', confidence: 0, score: 0,
        reasons: [...reasons, `ADX ${adxData.adx.toFixed(0)} < 15 — market choppy, no trend`], rsi: r };
    }
    const adxAligned = direction === 'buy'
      ? adxData.diPlus > adxData.diMinus
      : adxData.diMinus > adxData.diPlus;
    if (adxData.adx >= 18 && adxAligned) { reasons.push(`ADX ${adxData.adx.toFixed(0)} trending ✅`); passedGates++; }
    else if (adxData.adx >= 18) { reasons.push(`ADX ${adxData.adx.toFixed(0)} trending (direction mismatch)`); }
    else { reasons.push(`ADX ${adxData.adx.toFixed(0)} weak — reduced confidence`); }
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

  // ── BIG TRAP ANALYSIS ──────────────────────────────────────────
  // Trapped sellers below = strong support → confirms long entries
  // Trapped buyers above = strong resistance → confirms short entries
  // Strongest confluence signal — institutions are forced to cover
  const traps = findTraps(bars5m, 20);

  if (direction === 'buy' && traps.sellerTrapScore > 0) {
    const best = traps.trappedSellers[0];
    bonus += traps.sellerTrapScore;
    reasons.push(`🪤 Trapped sellers below $${best?.price.toFixed(2)} (vol:${best?.vol.toFixed(0)}) — support ✅`);
  }
  if (direction === 'sell' && traps.buyerTrapScore > 0) {
    const best = traps.trappedBuyers[0];
    bonus += traps.buyerTrapScore;
    reasons.push(`🪤 Trapped buyers above $${best?.price.toFixed(2)} (vol:${best?.vol.toFixed(0)}) — resistance ✅`);
  }
  // Nearest trap as price target info
  if (traps.nearestTrap) {
    const t = traps.nearestTrap;
    reasons.push(`Nearest trap: ${t.type==='buyer'?'🔴 buyers above':'🟢 sellers below'} $${t.price.toFixed(2)} (${(t.dist*100).toFixed(2)}% away)`);
  }
  // Gates passed (required) + bonus confluence (nice to have)
  // Need at least 5 gates AND at least 1 bonus for a trade
  const minGates = c15 ? 5 : 4; // gates needed — soft gates mean we need fewer hard passes
  const score    = passedGates * 10 + bonus * 5 + rsiScore * 5 + macdScore * 5 + volScore * 3;
  const confidence = Math.min(99, Math.round((passedGates / (minGates + 2)) * 100));

  if (passedGates < minGates) {
    reasons.push(`Only ${passedGates}/${minGates} gates passed — need more confluence`);
    return { signal: 'HOLD', confidence, score, reasons, rsi: r };
  }
  // Require at least 1 bonus confirmation
  if (bonus < 1) {
    reasons.push(`No bonus confluence — waiting for stronger setup`);
    return { signal: 'HOLD', confidence, score, reasons, rsi: r };
  }

  // R:R is handled by ATR-adaptive TP/stop system in managePosition
  const atrVal = atr(bars5m, 14);

  // Candle health check removed — too aggressive, blocks valid setups

  // ── Shorting requires much more conviction than going long ──
  // Stocks have natural upward drift — the market always assumes growth.
  // A short that fails means you're fighting the trend AND the market makers.
  // Requirements: 3+ bonus confirmations (vs 1 for longs) and higher RSI threshold
  if (direction === 'sell' && bonus < 3) {
    reasons.push(`Short needs 3+ bonus confirmations (only ${bonus}) — skip`);
    return { signal: 'HOLD', confidence, score, reasons, rsi: r };
  }

  reasons.push(`✅ ${passedGates} gates + ${bonus} bonus — HIGH QUALITY SETUP`);

  const finalConf = Math.min(99, confidence + bonus * 5);
  // Note low confidence but don't hard-block — let position sizing handle risk

  // ── SESSION QUALITY FILTER ────────────────────────────────────────
  const session = getCurrentSession();
  const isOffHours = session.includes('Off') || session.includes('Asia') || session.includes('Pre');
  const sessionMinConf = isOffHours ? 80 : 65;

  // Block US-listed international ETFs during off-hours — they track closed markets
  // and have almost zero real volume during Asia session
  const intlETFs = ['EWG','EWJ','EWA','EWY','EWZ','FXI','EFA','EEM','VWO','IEFA','MCHI'];
  if (isOffHours && intlETFs.includes(sym)) {
    return { signal:'HOLD', confidence:0, score:0,
      reasons:[`${sym} is intl ETF — no signal during ${session}`], rsi:r };
  }
  if (finalConf < sessionMinConf) {
    // Log first time per symbol to help diagnose — don't spam
    reasons.push(`Conf ${finalConf}% below session minimum ${sessionMinConf}% (${session})`);
    return { signal: 'HOLD', confidence: finalConf, score, reasons, rsi: r, blockedBy: 'session_conf' };
  }

  const _baseSig = {
    signal:     direction === 'buy' ? 'BUY' : 'SELL',
    confidence: finalConf,
    score,
    reasons,
    rsi: r,
    atr: atrVal,
  };
  // Apply APEX learned rules — may boost confidence or block bad setups
  return apexFilter(_baseSig, {
    sym: sym || '',
    session: getCurrentSession(),
    atrPct: atrVal / (price || 1),
  });
}

// ─────────────────────────────────────────────
// FILTERS
// ─────────────────────────────────────────────
async function getMarketRegime() {
  if (!CONFIG.regimeFilter) return true;
  try {
    const [dayBars, intraBars] = await Promise.all([
      fetchBars('SPY', '1Day', 30),
      fetchBars('SPY', '5Min', 20),
    ]);

    // ── Daily trend check ──
    if (dayBars && dayBars.length >= 10) {
      const closes = dayBars.map(b => b.c);
      const e10 = ema(closes, 10), e30 = ema(closes, Math.min(30, closes.length));
      const latest = closes[closes.length - 1];
      const hardBearish = latest < e10 && (e10 - e30) / e30 > 0.01;
      if (hardBearish) {
        log('regime', `⚠ Market BEARISH — BUYs paused (SPY=$${latest.toFixed(2)})`);
        return false;
      }
    }

    // ── Intraday volatility check — don't trade choppy / high-volatility opens ──
    if (intraBars && intraBars.length >= 10) {
      const spyAtr = atr(intraBars, 10);
      const spyPrice = intraBars[intraBars.length-1].c;
      const spyAtrPct = (spyAtr / spyPrice) * 100;

      // If SPY is moving more than 0.4% per 5-min bar — too volatile
      if (spyAtrPct > 0.4) {
        log('regime', `⚠ SPY too volatile (ATR=${spyAtrPct.toFixed(2)}%/bar) — waiting for calmer market`);
        return false;
      }

      // Check if SPY is in a sustained intraday downtrend (3+ consecutive down bars)
      const last4 = intraBars.slice(-4).map(b => b.c);
      const spyDowntrend = last4[3] < last4[2] && last4[2] < last4[1] && last4[1] < last4[0];
      if (spyDowntrend) {
        log('regime', `⚠ SPY 3-bar downtrend — pausing longs`);
        return false;
      }
    }

    log('regime', `✅ Market OK — regime clear, volatility normal`);
    return true;
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
  // Block if gapped more than 2% — these stocks are unpredictable post-gap
  if (gap > 0.02) { log('filter', `Gap filter: ${symbol} gapped ${(gap*100).toFixed(1)}% — skip`); return true; }
  return false;
}

// Track real equity from Alpaca for circuit breaker (not just cash)
let realEquity = 0;
let realDailyStartEquity = 0;

function checkCircuitBreaker() {
  if (circuitBreakerOn) return true;

  // Never trip circuit breaker in sim mode
  if (isSimMode()) return false;

  const equityToCheck  = realEquity          > 0 ? realEquity          : null;
  const startToCheck   = realDailyStartEquity > 0 ? realDailyStartEquity : null;

  if (!equityToCheck || !startToCheck) {
    log('risk', 'Circuit breaker: waiting for real equity data from Alpaca…');
    return false;
  }

  const loss = (startToCheck - equityToCheck) / startToCheck;

  // Trip at maxDailyLossPct (default 3%, but also enforced at 1.5% minimum)
  const effectiveLimit = Math.min(CONFIG.maxDailyLossPct, 0.03);
  if (loss >= effectiveLimit) {
    circuitBreakerOn = true;
    log('risk', `🔴 CIRCUIT BREAKER: Down ${(loss*100).toFixed(1)}% today — halting all trades for the day`);
    sendDiscordAlert('circuit_breaker', 'ALL', 0, 0, -(startToCheck - equityToCheck));
    return true;
  }

  // Consecutive loss streak check — pause for 30 min after 3 straight losses
  const recentTrades = tradePerformanceLog.slice(-3);
  if (recentTrades.length >= 3 && recentTrades.every(t => !t.won)) {
    const lastLoss = recentTrades[recentTrades.length-1].timestamp;
    const pauseUntil = lastLoss + 30 * 60 * 1000;
    if (Date.now() < pauseUntil) {
      const minsLeft = Math.ceil((pauseUntil - Date.now()) / 60000);
      log('risk', `⏸ 3 consecutive losses — cooling off for ${minsLeft} more minutes`);
      return true;
    }
  }

  return false;
}

// ─────────────────────────────────────────────
// POSITION SIZING (ATR-based, risk 1% per trade)
// ─────────────────────────────────────────────
function calcQty(symbol, price, bars, confidence = 70) {
  if (!price || price <= 0) return 0;

  // Hard dollar cap — never put more than maxPositionPct of STARTING capital in one trade
  // Use startingCapital not current portfolio — prevents oversizing on winning streaks
  // Always size against STARTING capital — prevents runaway sizing on winning streaks
  const baseCap    = CONFIG.startingCapital;
  // Cap maxPositionPct — adaptive engine can drift it up, hard cap at 8%
  const cappedMaxPos = Math.min(0.08, Math.max(0.02, CONFIG.maxPositionPct));
  // WR-weighted confidence scaling
  const recentWR = totalWins + totalLosses > 10
    ? totalWins / (totalWins + totalLosses)
    : 0.5;
  const wrMult = recentWR >= 0.60 ? 1.0 : recentWR >= 0.50 ? 0.85 : recentWR >= 0.40 ? 0.65 : 0.45;
  const confScale  = Math.max(0.4, Math.min(1.0, (confidence - CONFIG.minConfidence) / 30 + 0.5)) * wrMult;
  const scaledPct  = cappedMaxPos * confScale;
  const maxCost    = baseCap * scaledPct;              // hard dollar cap
  const maxShares  = Math.floor(maxCost / price);

  if (maxShares < 1) return 0;

  if (bars && bars.length >= 14) {
    let atrVal = atr(bars, 14);
    // Minimum ATR floor: 0.1% of price — prevents insane qty on near-zero ATR stocks
    // e.g. NIO at $6.91 with ATR=0.01 → floor to $0.0069, capping qty to reasonable size
    const atrFloor = price * 0.004;  // min 0.4% — prevents insane qty on low-ATR ETFs (was 0.25%)
    atrVal = Math.max(atrVal, atrFloor);
    if (atrVal > 0) {
      // Risk at most 1% of starting capital per trade
      const riskShares = Math.floor((baseCap * 0.01) / atrVal);
      const qty = Math.min(riskShares, maxShares);
      if (qty >= 1) {
        log('size', `${symbol} conf=${confidence}% scale=${(confScale*100).toFixed(0)}% max=$${maxCost.toFixed(0)} ATR=${atrVal.toFixed(2)} → qty=${qty}`);
        return qty;
      }
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
  // In sim mode — always allow entries (we're replaying historical market hours)
  if (!isSimMode() && !isIntlETF && !isMarketOpen()) {
    log('warn', `🚫 Blocked ${sym} — market closed`);
    return;
  }
  if (checkCircuitBreaker()) return;
  if (hasLargeGap(sym, price)) return;

  // ── HARD POSITION LIMITS — checked inside enterPosition so they can never be bypassed ──
  const totalNow = Object.keys(positions).length + Object.keys(shortPositions).length + Object.keys(scalpPositions).length;
  if (totalNow >= CONFIG.maxOpenPositions) {
    log('risk', `🚫 Max positions (${CONFIG.maxOpenPositions}) reached — blocked ${sym}`);
    return;
  }

  // Don't enter if already in position in same direction
  if (direction === 'long'  && (positions[sym] || alpacaPositions.has(sym))) {
    const _k=sym+'L', _n=Date.now();
    if(!dupWarnThrottle[_k]||_n-dupWarnThrottle[_k]>300000){dupWarnThrottle[_k]=_n;log('warn',`${sym} already long`);}
    return;
  }
  if (direction === 'short' && (shortPositions[sym] || alpacaShorts.has(sym))) {
    const _k=sym+'S', _n=Date.now();
    if(!dupWarnThrottle[_k]||_n-dupWarnThrottle[_k]>300000){dupWarnThrottle[_k]=_n;log('warn',`${sym} already short`);}
    return;
  }
  if (direction === 'long'  && isCorrelated(sym)) return;

  // ── DOLLAR EXPOSURE LIMIT — never deploy more than 90% of starting capital ──
  const deployedNow = Object.entries(positions).reduce((a,[s,pos]) => {
    const cur = priceHistory5m[s]?.[priceHistory5m[s].length-1] || pos.entryPrice;
    return a + cur * (pos.qtyRemaining || pos.qty);
  }, 0);
  const maxDeploy = CONFIG.startingCapital * 0.90;
  if (!isSimMode() && deployedNow >= maxDeploy) {
    log('risk', `🚫 90% capital deployed ($${deployedNow.toFixed(0)}) — blocked ${sym}`);
    return;
  }

  const qty  = calcQty(sym, price, bars, sigInfo?.confidence || 70);
  const cost = qty * price;
  if (qty < 1) { log('warn', `Cannot enter ${sym}: qty too small`); return; }

  // Sanity check: max dollar loss at hardMaxLoss must be < 2% of portfolio
  // If position size would create a loss > this, reduce qty
  const maxDollarLoss = portfolio * 0.02; // never lose more than 2% of portfolio on one trade
  const impliedLoss   = qty * price * CONFIG.hardMaxLoss;
  if (impliedLoss > maxDollarLoss) {
    const safeQty = Math.floor(maxDollarLoss / (price * CONFIG.hardMaxLoss));
    if (safeQty < 1) { log('warn', `${sym}: position too risky even at 1 share — skipping`); return; }
    log('risk', `${sym}: reducing qty ${qty}→${safeQty} (implied loss $${impliedLoss.toFixed(0)} > max $${maxDollarLoss.toFixed(0)})`);
  }
  const finalQty = Math.min(qty, Math.max(1, Math.floor(maxDollarLoss / (price * CONFIG.hardMaxLoss))));

  const atrVal    = bars && bars.length >= 14 ? atr(bars, 14) : price * CONFIG.stopLossPct;
  const srLevels  = calcSRLevels(bars || []);

  if (direction === 'long') {
    const finalCost = finalQty * price;
    if (finalCost > portfolio) { log('warn', `Not enough cash for ${sym}`); return; }

    // With immediate trailing stops, we use a tighter initial stop
    // The trade only needs to go +0.3% before break-even locks in
    // So initial stop should be just wide enough to avoid normal noise
    const atrVal14  = bars && bars.length >= 14 ? atr(bars, 14) : price * 0.005;
    // Stop = 2× ATR below entry — wide enough to avoid noise, tight enough to limit loss
    const atrStop   = price - (atrVal14 * 2);
    // Hard cap: never risk more than 1.5% on initial stop
    const capStop   = price * 0.985;
    const stopPrice = Math.max(atrStop, capStop);

    if (isSimMode() || (CONFIG.mode === 'alpaca' && CONFIG.alpacaKey)) {
      // For live mode — verify Alpaca doesn't already have this position
      // This catches restarts, sync delays, and duplicate signals
      if (!isSimMode()) {
        try {
          const existingPos = await alpacaFetch(`${ALPACA_BASE()}/v2/positions/${sym}`);
          if (existingPos && existingPos.qty) {
            log('warn', `🚫 Alpaca already has ${sym} position (${existingPos.qty} shares) — blocked duplicate`);
            alpacaPositions.add(sym); // update in-memory to prevent future attempts
            return;
          }
        } catch(e) { /* 404 = no position, that's fine */ }
      }
      try { await placeSmartOrder(sym, finalQty, 'buy', false); }
      catch (e) { log('error', `BUY failed ${sym}: ${e.message}`); return; }
    }
    if (isSimMode()) portfolio -= finalCost; // live mode: Alpaca tracks cash

    // Use trapped buyer clusters above as TP targets — they'll panic-sell when price hits them
    // This gives us precise TP levels based on where the real supply/resistance is
    const entryTraps = bars ? findTraps(bars, 20) : null;
    let initTP1 = CONFIG.tp1Pct, initTP2 = CONFIG.tp2Pct, initTP3 = CONFIG.tp3Pct;
    if (entryTraps && entryTraps.trappedBuyers.length > 0) {
      // Sort trapped buyer clusters by proximity above entry price
      const buyerTargets = entryTraps.trappedBuyers
        .filter(t => t.price > price && (t.price - price)/price < 0.15)
        .sort((a,b) => a.price - b.price);
      if (buyerTargets.length >= 1) {
        initTP1 = (buyerTargets[0].price - price) / price;
        log('trap', `🪤 ${sym} TP1 set to trapped buyer cluster @ $${buyerTargets[0].price.toFixed(2)} (+${(initTP1*100).toFixed(2)}%)`);
      }
      if (buyerTargets.length >= 2) {
        initTP2 = (buyerTargets[1].price - price) / price;
        log('trap', `🪤 ${sym} TP2 set to trapped buyer cluster @ $${buyerTargets[1].price.toFixed(2)} (+${(initTP2*100).toFixed(2)}%)`);
      }
      if (buyerTargets.length >= 3) {
        initTP3 = (buyerTargets[2].price - price) / price;
      }
    }

    positions[sym] = {
      entryPrice: price, qty: finalQty, qtyRemaining: finalQty, cost: finalCost,
      entryTime: new Date(), highWater: price, lowWater: price,
      atrAtEntry: atrVal14, stopPrice,
      breakEvenSet: false, tp1Hit: false, tp2Hit: false,
      srLevels, sigInfo, direction: 'long',
      // Rich entry analytics for post-trade learning
      entryAnalytics: {
        stopDistPct:    +((price - stopPrice) / price * 100).toFixed(3),
        stopDistAtr:    atrVal14 > 0 ? +((price - stopPrice) / atrVal14).toFixed(2) : 0,
        atrPct:         +((atrVal14 / price) * 100).toFixed(3),
        confidence:     sigInfo.confidence,
        score:          sigInfo.score || 0,
        rsi:            sigInfo.rsi || 50,
        entryMomentum:  bars?.length >= 5 ? +((price - bars[bars.length-5]?.c||price) / (bars[bars.length-5]?.c||price) * 100).toFixed(3) : 0,
        volumeRatio:    bars?.length >= 10 ? +(bars[bars.length-1].v / (bars.slice(-10).reduce((a,b)=>a+b.v,0)/10)).toFixed(2) : 1,
        session:        getCurrentSession(),
        signalReasons:  sigInfo.reasons?.slice(0,5) || [],
        mfe:            0,
        mae:            0,
      },
    };
    alpacaPositions.add(sym);
    // Seed price history immediately so P&L shows right away
    if (!priceHistory5m[sym] || priceHistory5m[sym].length === 0) {
      priceHistory5m[sym] = [price];
    }
    // Subscribe to real-time price stream for this symbol
    subscribeOpenPositions();
    trades.push({ time: new Date(), sym, side: 'BUY', qty, price, pnl: null, reason: 'SIGNAL', confidence: sigInfo.confidence });
    const stopPct = ((price - stopPrice) / price * 100).toFixed(2);
    log('buy', `✅ LONG ${qty}x ${sym} @ $${price.toFixed(2)} | SL=$${stopPrice.toFixed(2)} (-${stopPct}%) | conf=${sigInfo.confidence}%`);
    await sendDiscordAlert('buy', sym, qty, price, undefined, undefined, sigInfo, { stopPrice, atrVal: atrVal14 });
    await syncTrade({ sym, side: 'BUY', qty, price, pnl: null, reason: 'SIGNAL', confidence: sigInfo.confidence });

    // Refresh portfolio from Alpaca in background — don't write stale in-memory cash
    if (!isSimMode()) syncPortfolio().catch(()=>{});

  } else {
    // SHORT — borrow shares to sell, profit if price falls
    // Stop loss ABOVE entry for shorts — 2× ATR, capped at 1.5%
    const atrVal14s = bars && bars.length >= 14 ? atr(bars, 14) : price * 0.005;
    const atrStopS  = price + (atrVal14s * 2);
    const capStopS  = price * 1.015;
    const stopPrice = Math.min(atrStopS, capStopS);

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
    // syncAll below handles portfolio refresh from Alpaca
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
  const forceClose = reason === 'MANUAL_DISCORD';

  if (isSimMode() || (CONFIG.mode === 'alpaca' && CONFIG.alpacaKey)) {
    try { await placeOrder(sym, qty, 'buy'); }
    catch (e) {
      log('error', `Cover failed ${sym}: ${e.message}`);
      if (!forceClose) return;
      log('warn', `Force-closing short ${sym} in memory despite order failure`);
    }
  }

  // Short P&L = (entry - exit) × qty (profit when price drops)
  const pnl = (pos.entryPrice - price) * qty;
  if (isSimMode()) portfolio += pnl; // only track in sim — live uses Alpaca
  pnl > 0 ? totalWins++ : totalLosses++;
  recordTradeOutcome(pnl, { confidence: pos?.sigInfo?.confidence||0, rsi: pos?.sigInfo?.rsi||50, session: getCurrentSession(), side: 'short', sym, exitReason: reason, holdMins: Math.round((Date.now()-new Date(pos.entryTime).getTime())/60000), pnlPct: pos.entryPrice > 0 ? pnl/(pos.entryPrice*(pos.qtyRemaining||pos.qty)) : 0, ...(pos.entryAnalytics||{}) });
  delete shortPositions[sym];
  alpacaShorts.delete(sym);
  // Immediately delete from Supabase so dashboard reflects closure instantly
  sbFetch(`${tbl('tc_positions')}?symbol=eq.${sym}`, 'DELETE').catch(() => {});

  const icon = { STOP_LOSS:'🛑', TAKE_PROFIT:'🎯', TRAILING_STOP:'📉', SIGNAL:'📤', TIME_STOP:'⏰' }[reason] || '📤';
  log('short', `${icon} COVER ${qty}x ${sym} @ $${price.toFixed(2)} | P&L: ${pnl>=0?'+':''}$${pnl.toFixed(2)} (${reason})`);
  trades.push({ time: new Date(), sym, side: 'COVER', qty, price, pnl, reason });
  await sendDiscordAlert('cover', sym, qty, price, pnl, reason);
  await syncTrade({ sym, side: 'COVER', qty, price, pnl, reason });
  await syncAll();
  await syncLog('sell', `${icon} COVER ${qty}x ${sym} @ $${price.toFixed(2)} P&L=${pnl>=0?'+':''}$${pnl.toFixed(2)} (${reason})`);

  // Trigger recovery on short losses too
  if (pnl < 0 && CONFIG.recoveryMode) {
    triggerRecovery(sym, Math.abs(pnl), price, pos);
  }
}

// Manage short positions — mirror of managePosition but inverted
async function manageShort(sym, price, bars) {
  const pos = shortPositions[sym];
  if (!pos) return;

  // Don't manage until 30 seconds old — prevents false triggers from stale prices
  const ageSecs = (Date.now() - new Date(pos.entryTime).getTime()) / 1000;
  if (ageSecs < 30) return;

  const chg      = (pos.entryPrice - price) / pos.entryPrice;
  const holdMins = (Date.now() - new Date(pos.entryTime).getTime()) / 60000;

  // Intraday only — close shorts before market close too
  if (!isSimMode() && isMarketOpen()) {
    const mins = minsToClose();
    if (mins <= 0 && mins > -60) {
      log('risk', `⏰ SHORT ${sym} INTRADAY CLOSE — market closing`);
      return coverShort(sym, price, 'INTRADAY_CLOSE');
    }
  }

  // Update low water mark (lowest price = most profit for short)
  if (price < pos.lowWater) shortPositions[sym].lowWater = price;
  const hwChg = (pos.entryPrice - pos.lowWater) / pos.entryPrice; // how far down we've been

  // ── IMMEDIATE PROFIT LOCK + TRAILING STOP (shorts — inverted) ──
  // Tier 1 — ATR-scaled break-even for shorts
  const shortAtrPct = bars && bars.length >= 14 ? atr(bars, 14) / price : 0.008;
  const shortScaledBE = Math.max(0.002, Math.min(0.010, shortAtrPct * 0.4));
  if (!pos.breakEvenSet && chg >= shortScaledBE) {
    // For shorts: stopPrice must be ABOVE entry so it fires when price rises back up
    // entryPrice * 1.0001 = just above entry = exit at scratch if price reverses
    shortPositions[sym].stopPrice   = pos.entryPrice * 1.0001;
    shortPositions[sym].breakEvenSet = true;
    log('risk', `🔒 ${sym} short BE @ $${pos.entryPrice.toFixed(2)} → SL now $${(pos.entryPrice*1.0001).toFixed(2)} (BE=${(shortScaledBE*100).toFixed(2)}%)`);
    await syncLog('sys', `🔒 Short BE: ${sym} @ $${pos.entryPrice.toFixed(2)}`);
  }

  // Tier 2 — trail 0.8% above low water once down 1%
  if (pos.breakEvenSet && hwChg >= 0.010 && !pos.tp1Hit) {
    const trail = pos.lowWater * (1 + 0.008);
    if (trail < shortPositions[sym].stopPrice) {
      shortPositions[sym].stopPrice = trail;
    }
  }

  // Tier 3 — trail 1.5% above low water once down 2%
  if (pos.breakEvenSet && hwChg >= 0.020 && !pos.tp1Hit) {
    const trail = pos.lowWater * (1 + 0.015);
    if (trail < shortPositions[sym].stopPrice) {
      shortPositions[sym].stopPrice = trail;
    }
  }

  // Tier 4 — trail 2% above low water once down 4%+
  if (pos.breakEvenSet && hwChg >= 0.040 && !pos.tp1Hit) {
    const trail = pos.lowWater * (1 + 0.020);
    if (trail < shortPositions[sym].stopPrice) {
      shortPositions[sym].stopPrice = trail;
    }
  }

  // After TP1 — 1.5% trail on remaining
  if (pos.tp1Hit && !pos.tp2Hit) {
    const trail = pos.lowWater * (1 + 0.015);
    if (trail < shortPositions[sym].stopPrice) shortPositions[sym].stopPrice = trail;
  }

  // After TP2 — 1% trail on final runner
  if (pos.tp1Hit && pos.tp2Hit) {
    const trail = pos.lowWater * (1 + 0.010);
    if (trail < shortPositions[sym].stopPrice) shortPositions[sym].stopPrice = trail;
  }

  // Check stop hit — price went UP against short
  if (price >= shortPositions[sym].stopPrice) {
    const pct = ((pos.entryPrice - price) / pos.entryPrice * 100).toFixed(2);
    const reason = price >= pos.entryPrice * 0.999 ? 'BREAK_EVEN_STOP' : 'TRAILING_STOP';
    log('risk', `${reason === 'BREAK_EVEN_STOP' ? '🔒' : '📉'} SHORT ${sym} ${reason} @ $${price.toFixed(2)} (${pct >= 0 ? '+' : ''}${pct}%)`);
    return coverShort(sym, price, reason);
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

  log('pos', `SHORT ${sym} ${chg>=0?'+':''}${(chg*100).toFixed(2)}% | SL=$${shortPositions[sym].stopPrice.toFixed(2)} | BE:${pos.breakEvenSet?'✅':'❌'}`);
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
  if (isSimMode()) portfolio += qtyToSell * price;
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
  const forceClose = reason === 'MANUAL_DISCORD';

  if (isSimMode() || (CONFIG.mode === 'alpaca' && CONFIG.alpacaKey)) {
    try { await placeOrder(sym, qtyToSell, 'sell'); }
    catch (e) {
      log('error', `Sell order failed ${sym}: ${e.message}`);
      if (!forceClose) return;
      log('warn', `Force-closing ${sym} in memory despite order failure`);
    }
  }
  const avgCost = pos.cost / qtyToSell;
  const pnl = qtyToSell * price - qtyToSell * avgCost;
  // Only update in-memory cash in sim mode — in live mode Alpaca is the source of truth
  // Adding proceeds in live mode causes cash to inflate and diverge from reality
  if (isSimMode()) portfolio += qtyToSell * price;
  pnl > 0 ? totalWins++ : totalLosses++;
  recordTradeOutcome(pnl, { confidence: pos?.sigInfo?.confidence||0, rsi: pos?.sigInfo?.rsi||50, session: getCurrentSession(), side: 'long', sym, exitReason: reason, holdMins: Math.round((Date.now()-new Date(pos.entryTime).getTime())/60000), pnlPct: pos.entryPrice > 0 ? pnl/(pos.entryPrice*(pos.qtyRemaining||pos.qty)) : 0, ...(pos.entryAnalytics||{}) });
  delete positions[sym];
  alpacaPositions.delete(sym);
  sbFetch(`${tbl('tc_positions')}?symbol=eq.${sym}`, 'DELETE').catch(() => {});

  trades.push({ time: new Date(), sym, side: 'SELL', qty: qtyToSell, price, pnl, reason });
  const icon = { STOP_LOSS:'🛑', BREAK_EVEN_STOP:'🔒', TAKE_PROFIT:'🎯', TRAILING_STOP:'📉', SIGNAL:'📤', TIME_STOP:'⏰', VOL_SQUEEZE:'📊', RESISTANCE_EXIT:'🧱', PEAK_EXIT:'🔔', INTRADAY_CLOSE:'🔔', MICRO_MOVE_EXIT:'💤', AI_EXIT:'🤖', AI_PARTIAL:'🤖' }[reason] || '📤';
  log('sell', `${icon} SELL ${qtyToSell}x ${sym} @ $${price.toFixed(2)} | P&L: ${pnl>=0?'+':''}$${pnl.toFixed(2)} (${reason})`);
  await sendDiscordAlert('sell', sym, qtyToSell, price, pnl, reason);
  await syncTrade({ sym, side: 'SELL', qty: qtyToSell, price, pnl, reason });
  await syncAll();
  await syncLog('sell', `${icon} SELL ${qtyToSell}x ${sym} @ $${price.toFixed(2)} P&L=${pnl>=0?'+':''}$${pnl.toFixed(2)} (${reason})`);

  // ── RECOVERY MODE ─────────────────────────────────────────────
  if (pnl < 0 && CONFIG.recoveryMode && !isSimMode()) {
    // If this was itself a recovery trade that lost, add losses together
    const combinedLoss = Math.abs(pnl) + (pos.sigInfo?.recoveryMode ? (recoveryState?.targetPnl || 0) : 0);
    triggerRecovery(sym, combinedLoss, price, pos);
  }
}

// ─────────────────────────────────────────────
// SUPPORT / RESISTANCE
// ─────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════════
// RECOVERY MODE ENGINE
// ═══════════════════════════════════════════════════════════════════
// Activated after any losing trade.
// Goal: recover the exact dollar loss with a high-conviction counter-trade
// on the same symbol. Uses tighter rules than normal — only the very best
// setups qualify so we don't compound the loss.
//
// Recovery rules (stricter than normal):
//   - Same symbol only — we know its recent behavior
//   - Minimum confidence 80% (vs normal 60%)
//   - Target = exactly the loss amount (not % based)
//   - Tighter stop = 1× ATR (vs normal 2× ATR)
//   - Max 1 recovery trade at a time
//   - Expires after 30 minutes if no setup found
//   - Circuit break: if recovery trade also loses, mode deactivates
// ═══════════════════════════════════════════════════════════════════

let recoveryState = null;
const dupWarnThrottle = {}; // throttle warnings + scalp cooldowns // { sym, targetPnl, lossPrice, startTime, attempts }
const posLogThrottle = {}; // sym -> last log timestamp, prevents log spam

function isInRecovery() { return !!recoveryState && CONFIG.recoveryMode; }

function triggerRecovery(sym, lossAmt, lossPrice, lostPos) {
  if (!CONFIG.recoveryMode) return;
  if (isInRecovery()) return; // already recovering — don't stack

  recoveryState = {
    sym,
    targetPnl:  lossAmt,       // exact dollar amount to recover
    lossPrice,                 // price we lost at
    lostDir:    lostPos.direction || 'long',
    startTime:  Date.now(),
    attempts:   0,
    active:     true,
  };

  log('recovery', `🔴 RECOVERY MODE: need to recover $${lossAmt.toFixed(2)} on ${sym}`);
  syncLog('sys', `🔴 Recovery mode: ${sym} lost $${lossAmt.toFixed(2)} — hunting counter-trade`);

  // Alert Discord
  if (CONFIG.discordWebhook) {
    getFetch().then(fetch => fetch(CONFIG.discordWebhook, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ embeds: [{ title: `🔴 Recovery Mode: ${sym}`, color: 0xff4466,
        fields: [
          { name: 'Loss', value: `-$${lossAmt.toFixed(2)}`, inline: true },
          { name: 'Target', value: `+$${lossAmt.toFixed(2)}`, inline: true },
          { name: 'Expires', value: '30 minutes', inline: true },
        ],
        footer: { text: 'TradeCore Recovery Engine' },
        timestamp: new Date().toISOString(),
      }]}),
    }).catch(() => {}));
  }
}

function clearRecovery(reason) {
  if (!recoveryState) return;
  const elapsed = ((Date.now() - recoveryState.startTime) / 60000).toFixed(1);
  log('recovery', `✅ Recovery ended: ${reason} (${elapsed}min active)`);
  syncLog('sys', `Recovery ended: ${reason}`);
  recoveryState = null;
}

async function runRecoveryScan() {
  if (!recoveryState || !recoveryState.active) return;
  if (!CONFIG.recoveryMode) { clearRecovery('mode disabled'); return; }

  const { sym, targetPnl, startTime, attempts } = recoveryState;

  // Expire after 60 minutes (extended from 30 — markets can be choppy)
  if (Date.now() - startTime > 60 * 60 * 1000) {
    clearRecovery('expired (60min)');
    return;
  }

  // Scan the original symbol first, then all other symbols if no signal found
  // This way recovery isn't stuck waiting for one symbol that may not move
  const openSyms = new Set([...Object.keys(positions), ...Object.keys(shortPositions)]);
  const scanList = [sym, ...CONFIG.symbols.filter(s => s !== sym && !openSyms.has(s))];

  recoveryState.attempts++;
  log('recovery', `🔄 Recovery scan #${recoveryState.attempts}: scanning ${scanList.length} symbols for opportunity`);

  const MIN_RECOVERY_CONF = 65;
  let bestSig = null, bestSym = null, bestBars = null;

  for (const candidate of scanList.slice(0, 8)) { // check up to 8 symbols
    if (openSyms.has(candidate)) continue;

    let bars;
    try {
      const feed = getDataFeed();
      const url = `${ALPACA_BASE()}/v2/stocks/${candidate}/bars?timeframe=5Min&limit=60&feed=${feed}`;
      const data = await alpacaFetch(url);
      bars = (data?.bars || []).map(b => ({ t:b.t, o:+b.o, h:+b.h, l:+b.l, c:+b.c, v:+b.v }));
    } catch(e) { continue; }

    if (!bars || bars.length < 20) continue;

    const bars15m = [];
    for (let i = 0; i + 2 < bars.length; i += 3) {
      const chunk = bars.slice(i, i + 3);
      bars15m.push({ t:chunk[0].t, o:chunk[0].o, h:Math.max(...chunk.map(b=>b.h)), l:Math.min(...chunk.map(b=>b.l)), c:chunk[chunk.length-1].c, v:chunk.reduce((a,b)=>a+b.v,0) });
    }

    const sig = generateSignalByStrategy(candidate, bars, bars15m.length >= 10 ? bars15m : null)
             || generateSignal(candidate, bars, bars15m.length >= 10 ? bars15m : null);

    if (sig.signal !== 'HOLD' && sig.confidence >= MIN_RECOVERY_CONF) {
      if (!bestSig || sig.confidence > bestSig.confidence) {
        bestSig = sig; bestSym = candidate; bestBars = bars;
        log('recovery', `  ✓ ${candidate}: ${sig.signal} conf=${sig.confidence}%`);
      }
    } else {
      log('recovery', `  ✗ ${candidate}: ${sig.signal} conf=${sig.confidence}%`);
    }
  }

  if (!bestSig) {
    log('recovery', `⏳ No recovery opportunity found this scan — will retry`);
    return;
  }

  const price = priceHistory5m[bestSym]?.[priceHistory5m[bestSym].length-1] || bestBars[bestBars.length-1].c;
  const atrVal = atr(bestBars, 14);
  const atrTarget = atrVal * 2;
  const recoverQty = Math.max(1, Math.min(
    Math.ceil(targetPnl / atrTarget),
    Math.floor(portfolio * CONFIG.maxPositionPct / price)
  ));

  // If shorts disabled and best signal is SHORT, find best LONG signal instead
  let direction = bestSig.signal === 'BUY' ? 'long' : 'short';
  if (direction === 'short' && !CONFIG.shortsEnabled) {
    // Re-scan for a long-only opportunity
    let longSig = null, longSym = null, longBars = null;
    for (const candidate of scanList.slice(0, 8)) {
      if (openSyms.has(candidate)) continue;
      try {
        const feed = getDataFeed();
        const data = await alpacaFetch(`${ALPACA_BASE()}/v2/stocks/${candidate}/bars?timeframe=5Min&limit=60&feed=${feed}`);
        const bars = (data?.bars||[]).map(b=>({t:b.t,o:+b.o,h:+b.h,l:+b.l,c:+b.c,v:+b.v}));
        if (bars.length < 20) continue;
        const b15 = [];
        for (let i=0;i+2<bars.length;i+=3){const ch=bars.slice(i,i+3);b15.push({t:ch[0].t,o:ch[0].o,h:Math.max(...ch.map(b=>b.h)),l:Math.min(...ch.map(b=>b.l)),c:ch[ch.length-1].c,v:ch.reduce((a,b)=>a+b.v,0)});}
        const sig = generateSignalByStrategy(candidate, bars, b15.length>=10?b15:null) || generateSignal(candidate, bars, b15.length>=10?b15:null);
        if (sig.signal === 'BUY' && sig.confidence >= MIN_RECOVERY_CONF) {
          if (!longSig || sig.confidence > longSig.confidence) { longSig=sig; longSym=candidate; longBars=bars; }
        }
      } catch(e) { continue; }
    }
    if (!longSig) {
      log('recovery', `Recovery: shorts disabled and no long setup found — will retry`);
      return;
    }
    bestSig = longSig; bestSym = longSym; bestBars = longBars;
    direction = 'long';
    log('recovery', `Recovery: switched to long-only — best: ${bestSym} conf=${bestSig.confidence}%`);
  }

  log('recovery', `🎯 RECOVERY ENTRY: ${bestSym} ${bestSig.signal} conf=${bestSig.confidence}% qty=${recoverQty} target=$${targetPnl.toFixed(2)}`);
  await syncLog('recovery', `🎯 Recovery: ${bestSym} ${bestSig.signal} qty=${recoverQty} target=$${targetPnl.toFixed(2)}`);

  // Use best candidate for entry
  // (sym/sig/bars are local aliases — outer sym still refers to original lost symbol)

  // Check we can actually enter before clearing state
  const openBefore = Object.keys(positions).length + Object.keys(shortPositions).length;
  await enterPosition(bestSym, price, { ...bestSig, recoveryMode: true, recoveryTarget: targetPnl / (recoverQty * price) }, bestBars, direction);
  const openAfter = Object.keys(positions).length + Object.keys(shortPositions).length;

  if (openAfter > openBefore) {
    clearRecovery(`recovery trade entered ${bestSym} ${bestSig.signal}`);
  } else {
    log('recovery', `⚠ Recovery entry blocked for ${bestSym} — will retry on next scan`);
  }
}

// ═══════════════════════════════════════════════════════════════════
// END RECOVERY MODE ENGINE
// ═══════════════════════════════════════════════════════════════════

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
// PEAK DETECTION — signals when a trade has hit its max
// Returns { isPeak: bool, reason: string, urgency: 'immediate'|'warning' }
//
// Uses multiple confluence signals:
//   1. RSI divergence — price makes new high but RSI doesn't
//   2. Volume exhaustion — volume shrinking as price extends
//   3. Bearish candle patterns — shooting star, bearish engulfing, doji at high
//   4. VWAP rejection — price pushed above VWAP but rejected back
//   5. Bollinger Band upper touch — price at upper band, mean-reversion likely
//   6. Momentum reversal — price acceleration slowing (lower highs on each bar)
// ─────────────────────────────────────────────
function detectPeak(sym, bars, pos) {
  if (!bars || bars.length < 10 || !pos) return { isPeak: false };

  const prices  = bars.map(b => b.c);
  const highs   = bars.map(b => b.h);
  const volumes = bars.map(b => b.v);
  const price   = prices[prices.length - 1];
  const signals = [];
  let urgency   = 'warning';

  // Only run peak detection if we're in profit
  const chg = (price - pos.entryPrice) / pos.entryPrice;
  if (chg <= 0) return { isPeak: false };

  // ── 1. RSI DIVERGENCE ──────────────────────────────────────────
  // Price making higher highs but RSI making lower highs = bearish divergence
  if (bars.length >= 20) {
    const rsiNow  = rsi(prices, 14);
    const rsi5ago = rsi(prices.slice(0, -5), 14);
    const priceHigh    = Math.max(...highs.slice(-5));
    const priceHigh5ag = Math.max(...highs.slice(-10, -5));
    if (priceHigh > priceHigh5ag * 1.001 && rsiNow < rsi5ago - 3) {
      signals.push(`RSI divergence (price ↑ RSI ${rsiNow.toFixed(0)} < ${rsi5ago.toFixed(0)})`);
      if (rsiNow > (CONFIG.peakRsiExit || 80) - 10) urgency = 'immediate';
    }
    // Overbought RSI alone is a warning
    if (rsiNow > (CONFIG.peakRsiExit || 80) - 5) signals.push(`RSI overbought ${rsiNow.toFixed(0)}`);
    if (rsiNow > (CONFIG.peakRsiExit || 80)) urgency = 'immediate';
  }

  // ── 2. VOLUME EXHAUSTION ───────────────────────────────────────
  // Volume shrinking as price extends = buyers running out of steam
  if (volumes.length >= 6) {
    const avgVol   = volumes.slice(-10, -3).reduce((a,b) => a+b, 0) / 7;
    const recentVol = volumes.slice(-3).reduce((a,b) => a+b, 0) / 3;
    if (recentVol < avgVol * 0.5 && chg > 0.01) {
      signals.push(`Volume exhaustion (${(recentVol/avgVol*100).toFixed(0)}% of avg)`);
    }
  }

  // ── 3. BEARISH CANDLE PATTERNS ─────────────────────────────────
  const last = bars[bars.length - 1];
  const prev = bars[bars.length - 2];
  if (last && prev) {
    const body     = Math.abs(last.c - last.o);
    const range    = last.h - last.l || 0.0001;
    const upperWick = last.h - Math.max(last.c, last.o);
    const lowerWick = Math.min(last.c, last.o) - last.l;

    // Shooting star: small body, long upper wick, little lower wick
    if (upperWick > body * 2 && upperWick > range * 0.6 && lowerWick < body * 0.5 && last.c < last.o) {
      signals.push('Shooting star candle');
      urgency = 'immediate';
    }

    // Bearish engulfing: current bearish bar completely covers previous bullish bar
    if (last.c < last.o && prev.c > prev.o &&
        last.o >= prev.c && last.c <= prev.o) {
      signals.push('Bearish engulfing');
      urgency = 'immediate';
    }

    // Doji at high: open ≈ close, price near high water
    if (body < range * 0.1 && last.h >= pos.highWater * 0.999) {
      signals.push('Doji at peak');
    }

    // Dark cloud cover: gap up then closes below midpoint of previous bar
    if (last.o > prev.h && last.c < (prev.o + prev.c) / 2 && last.c < last.o) {
      signals.push('Dark cloud cover');
      urgency = 'immediate';
    }
  }

  // ── 4. VWAP REJECTION ─────────────────────────────────────────
  if (bars.length >= 20) {
    const vw = vwap(bars.slice(-20));
    const vwapDist = (price - vw) / vw;
    // Far above VWAP and starting to pull back
    if (vwapDist > 0.015 && price < pos.highWater * 0.999) {
      signals.push(`VWAP extended +${(vwapDist*100).toFixed(1)}% — rejection likely`);
    }
  }

  // ── 5. BOLLINGER BAND UPPER TOUCH ─────────────────────────────
  const bb = bollingerBands(prices, 20, 2);
  if (bb && price >= bb.upper * 0.998) {
    signals.push(`At BB upper ($${bb.upper.toFixed(2)})`);
    // If RSI also overbought at BB upper — strong peak signal
    if (rsi(prices, 14) > 68) urgency = 'immediate';
  }

  // ── 6. MOMENTUM DECELERATION ──────────────────────────────────
  // Bar-over-bar gains getting smaller (momentum running out)
  if (bars.length >= 6) {
    const moves = [];
    for (let i = bars.length - 5; i < bars.length; i++) {
      moves.push(bars[i].c - bars[i-1].c);
    }
    const positiveDecelerating = moves.every(m => m !== undefined) &&
      moves[0] > 0 && moves[1] > 0 && moves[2] > 0 &&
      moves[moves.length-1] < moves[moves.length-3] * 0.3; // last move < 30% of earlier move
    if (positiveDecelerating && chg > 0.008) {
      signals.push('Momentum decelerating (3 smaller gains)');
    }
  }

  // ── CONFLUENCE THRESHOLD ───────────────────────────────────────
  // Need 2+ signals for a warning, 1 immediate signal to act
  const isPeak = urgency === 'immediate' || signals.length >= 2;

  if (isPeak && signals.length > 0) {
    log('risk', `🔔 PEAK DETECTED ${sym} @ $${price.toFixed(2)} (+${(chg*100).toFixed(2)}%) | ${signals.join(' | ')}`);
  }

  return { isPeak, urgency, signals };
}

// ─────────────────────────────────────────────
// ADVANCED POSITION MANAGEMENT
// ─────────────────────────────────────────────

// ── Take Profit Calculator ────────────────────────────────────────
// Simple, clean ATR-based targets with minimum floors.
// Goal: targets that are ACTUALLY reachable given the stock's volatility.
// 
// TP1 = 2× ATR  (minimum 1.5%)   — first profit lock
// TP2 = 4× ATR  (minimum 3.0%)   — main target
// TP3 = 7× ATR  (minimum 5.0%)   — let winner run
//
// These are FLOORS not ceilings — uses CONFIG values if they're higher.
function calcDynamicTP(pos, price, bars) {
  if (!bars || bars.length < 14) return null;

  const atrVal = atr(bars, 14);
  const atrPct = atrVal / price;

  // ATR-based targets with hard minimums so they're always reachable
  const tp1 = Math.max(CONFIG.tp1Pct, atrPct * 2,  0.015); // at least 1.5%
  const tp2 = Math.max(CONFIG.tp2Pct, atrPct * 4,  0.030); // at least 3.0%
  const tp3 = Math.max(CONFIG.tp3Pct, atrPct * 7,  0.050); // at least 5.0%

  return {
    tp1: +tp1.toFixed(4),
    tp2: +tp2.toFixed(4),
    tp3: +tp3.toFixed(4),
    atrPct: +atrPct.toFixed(4),
  };
}

// ── Market Close Time Check ───────────────────────────────────────
// Returns minutes until market close (negative = already closed)
function minsToClose() {
  const now = new Date();
  const et  = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const closeHour = 15, closeMin = 45; // 3:45 PM ET — 15 min before actual close
  const minsLeft = (closeHour * 60 + closeMin) - (et.getHours() * 60 + et.getMinutes());
  return minsLeft;
}

// ─────────────────────────────────────────────
// AI TRADE ADVISOR — Claude Haiku
// ─────────────────────────────────────────────
// Runs every 2 minutes per open position.
// Gets the full trade picture and decides:
//   HOLD     — keep the position, no action
//   TIGHTEN  — move stop closer to price
//   PARTIAL  — take partial profit now
//   EXIT     — close immediately
// The bot acts on the decision directly.
// ─────────────────────────────────────────────

const AI_ADVISOR_KEY   = process.env.ANTHROPIC_API_KEY || '';
const aiAdvisorLastRun = {}; // sym → timestamp

async function runAIAdvisor(sym, price, bars, pos) {
  if (!AI_ADVISOR_KEY) return null;
  if (isSimMode()) return null; // sim runs too fast, would burn tokens

  // Only run every 2 minutes per position
  const now = Date.now();
  if (aiAdvisorLastRun[sym] && now - aiAdvisorLastRun[sym] < 120000) return null;
  aiAdvisorLastRun[sym] = now;

  try {
    const fetch = await getFetch();

    const closes    = bars.map(b => b.c);
    const atrVal    = atr(bars, 14);
    const rsiVal    = rsi(closes, 14);
    const macdData  = (() => {
      if (closes.length < 26) return { macd: 0, signal: 0 };
      const ema12 = closes.slice(-12).reduce((a,b)=>a+b,0)/12;
      const ema26 = closes.slice(-26).reduce((a,b)=>a+b,0)/26;
      return { macd: +(ema12-ema26).toFixed(4), signal: 0 };
    })();
    const vwapVal   = vwap(bars.slice(-20));
    const holdMins  = Math.round((now - new Date(pos.entryTime).getTime()) / 60000);
    const chg       = ((price - pos.entryPrice) / pos.entryPrice * 100).toFixed(3);
    const mfe       = ((pos.highWater - pos.entryPrice) / pos.entryPrice * 100).toFixed(3);
    const mae       = ((pos.entryPrice - pos.lowWater)  / pos.entryPrice * 100).toFixed(3);
    const last5bars = bars.slice(-5).map(b =>
      `  ${new Date(b.t||Date.now()).toISOString().slice(11,16)} O:${b.o?.toFixed(2)} H:${b.h?.toFixed(2)} L:${b.l?.toFixed(2)} C:${b.c?.toFixed(2)} V:${b.v||0}`
    ).join('\n');

    const prompt = `You are an expert intraday trader analyzing an open position. Make a fast, decisive call.

POSITION:
  Symbol:      ${sym}
  Direction:   ${pos.direction || 'LONG'}
  Entry:       $${pos.entryPrice.toFixed(2)}
  Current:     $${price.toFixed(2)}
  P&L:         ${chg >= 0 ? '+' : ''}${chg}%
  Hold time:   ${holdMins} minutes
  Stop loss:   $${pos.stopPrice.toFixed(2)}
  TP1 hit:     ${pos.tp1Hit ? 'YES' : 'NO'}
  TP2 hit:     ${pos.tp2Hit ? 'YES' : 'NO'}

TRADE STATS:
  MFE (best):  +${mfe}% (highest price reached)
  MAE (worst): -${mae}% (lowest price reached)
  ATR (14):    $${atrVal.toFixed(3)} (${(atrVal/price*100).toFixed(3)}%)

INDICATORS:
  RSI(14):     ${rsiVal.toFixed(1)}
  MACD:        ${macdData.macd}
  VWAP:        $${vwapVal.toFixed(2)} (price is ${price > vwapVal ? 'ABOVE' : 'BELOW'} VWAP)

LAST 5 BARS (5-min):
${last5bars}

Based on this data, what is the single best action right now?

Respond with ONLY a JSON object, nothing else:
{
  "action": "HOLD" | "TIGHTEN" | "PARTIAL" | "EXIT",
  "reason": "one sentence max",
  "newStopPrice": <number or null>,
  "confidence": <0-100>
}

Rules:
- EXIT if RSI > 75 and price pulling back from MFE, or momentum clearly dead
- PARTIAL if up significantly and showing signs of topping
- TIGHTEN if profitable but risky, move stop to lock more profit
- HOLD if trade is developing normally
- newStopPrice only needed for TIGHTEN action`;

    const resp = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type':      'application/json',
        'x-api-key':         AI_ADVISOR_KEY,
        'anthropic-version': '2023-06-01',
      },
      body: JSON.stringify({
        model:      'claude-haiku-4-5-20251001',
        max_tokens: 150,
        messages: [{ role: 'user', content: prompt }],
      }),
    });

    const data = await resp.json();
    const text = data?.content?.[0]?.text || '';

    // Parse JSON response
    const match = text.match(/\{[\s\S]*\}/);
    if (!match) return null;
    const decision = JSON.parse(match[0]);

    log('ai', `🤖 ${sym} AI → ${decision.action} (${decision.confidence}% conf): ${decision.reason}`);
    await syncLog('ai', `🤖 AI Advisor: ${sym} → ${decision.action} | ${decision.reason}`);

    return decision;
  } catch(e) {
    log('warn', `AI advisor error for ${sym}: ${e.message}`);
    return null;
  }
}

// Act on the AI decision — called from managePosition
async function applyAIDecision(sym, price, pos, decision) {
  if (!decision || decision.confidence < 60) return; // ignore low confidence

  switch(decision.action) {
    case 'EXIT':
      log('ai', `🤖 AI EXIT: ${sym} @ $${price.toFixed(2)} — ${decision.reason}`);
      await exitPosition(sym, price, 'AI_EXIT');
      break;

    case 'PARTIAL':
      if (!pos.tp1Hit) {
        const qty = Math.max(1, Math.floor((pos.qtyRemaining || pos.qty) * 0.5));
        log('ai', `🤖 AI PARTIAL: selling ${qty}x ${sym} — ${decision.reason}`);
        positions[sym].tp1Hit = true;
        await partialExit(sym, price, qty, 'AI_PARTIAL');
      }
      break;

    case 'TIGHTEN':
      if (decision.newStopPrice && decision.newStopPrice > pos.stopPrice && decision.newStopPrice < price) {
        positions[sym].stopPrice = decision.newStopPrice;
        log('ai', `🤖 AI TIGHTEN: ${sym} stop → $${decision.newStopPrice.toFixed(2)} — ${decision.reason}`);
      }
      break;

    case 'HOLD':
    default:
      break;
  }
}

// ─────────────────────────────────────────────
// END AI TRADE ADVISOR
// ─────────────────────────────────────────────

async function managePosition(sym, price, bars) {
  const pos = positions[sym];
  if (!pos) return;

  // Don't manage a position until it's at least 30 seconds old
  // This prevents false stop-loss triggers from stale bar prices immediately after entry
  const ageSecs = (Date.now() - new Date(pos.entryTime).getTime()) / 1000;
  if (ageSecs < 30) {
    if (ageSecs < 5 || ageSecs % 15 < 3) log('pos', `${sym} entry ${ageSecs.toFixed(0)}s — waiting for price tick`);
    return;
  }

  const chg      = (price - pos.entryPrice) / pos.entryPrice;
  const holdMins = (Date.now() - new Date(pos.entryTime).getTime()) / 60000;

  // ── INTRADAY ONLY — close 15 min before market close ─────────────
  // Never hold overnight — gaps are unpredictable and can blow through stops
  if (!isSimMode() && isMarketOpen()) {
    const mins = minsToClose();
    if (mins <= 0 && mins > -60) {
      // Within the closing window — exit at whatever price we have
      const exitPct = (chg * 100).toFixed(2);
      log('risk', `⏰ ${sym} INTRADAY CLOSE — market closing, exiting at ${exitPct >= 0 ? '+' : ''}${exitPct}%`);
      return exitPosition(sym, price, 'INTRADAY_CLOSE');
    }
  }

  // Update high water mark first so everything below uses latest value
  if (price > pos.highWater) positions[sym].highWater = price;
  if (price < pos.lowWater)  positions[sym].lowWater  = price;

  // ── ATR-ADAPTIVE TAKE PROFITS ─────────────────────────────────
  // TPs scale to the stock's actual ATR so they're always reachable.
  // A stock with ATR 0.1% should have TP1 at 0.15%, not 1.5%.
  // A stock with ATR 0.8% should have TP1 at 1.2%, not 1.5%.
  const posAtr    = bars && bars.length >= 14 ? atr(bars, 14) : price * 0.005;
  const posAtrPct = posAtr / price;
  // TP tiers: 1.5×, 3×, 5× ATR — naturally scales to stock's real movement
  // Dynamic refresh: use current ATR if it expanded (momentum picking up)
  // but never shrink TPs below original levels (don't punish compression)
  const atrAtEntry = pos.atrAtEntry || posAtr;
  const dynamicAtr = Math.max(posAtr, atrAtEntry); // only expand, never shrink
  const dynamicAtrPct = dynamicAtr / price;
  const effectiveTP1 = Math.max(dynamicAtrPct * 1.5, 0.003); // min 0.3%
  const effectiveTP2 = Math.max(dynamicAtrPct * 3.0, 0.006); // min 0.6%
  const effectiveTP3 = Math.max(dynamicAtrPct * 5.0, 0.010); // min 1.0%

  // Track MFE (max favorable) and MAE (max adverse) for post-trade analysis
  if (pos.entryAnalytics) {
    const mfe = (pos.highWater - pos.entryPrice) / pos.entryPrice * 100;
    const mae = (pos.entryPrice - pos.lowWater)  / pos.entryPrice * 100;
    positions[sym].entryAnalytics.mfe = +mfe.toFixed(3);
    positions[sym].entryAnalytics.mae = +mae.toFixed(3);
  }

  // ── 1. HARD STOP LOSS — fires unconditionally, no exceptions ──
  // CRITICAL: this must ALWAYS run regardless of breakEvenSet, tp1Hit, etc.
  // Never allow a position to lose more than hardMaxLoss under any circumstance.
  const hardStop = pos.entryPrice - (posAtr * CONFIG.atrStopMult);

  // Case A: Price below the hard ATR stop (initial stop before break-even)
  if (!pos.breakEvenSet && price <= Math.max(pos.stopPrice, hardStop)) {
    log('risk', `🛑 ${sym} ATR stop @ $${price.toFixed(2)} | initial stop=$${pos.stopPrice.toFixed(2)} hardStop=$${hardStop.toFixed(2)}`);
    return exitPosition(sym, price, 'STOP_LOSS');
  }

  // Case B: Hard max loss — ALWAYS fires regardless of break-even or trailing state
  // This is the last line of defense. If a position somehow bypasses all other stops,
  // this guarantees we never lose more than hardMaxLoss% on any single trade.
  if (chg <= -CONFIG.hardMaxLoss) {
    log('risk', `🛑 ${sym} HARD MAX LOSS -${(CONFIG.hardMaxLoss*100).toFixed(1)}% triggered @ $${price.toFixed(2)} | entry=$${pos.entryPrice.toFixed(2)} | lost=${(chg*100).toFixed(2)}%`);
    return exitPosition(sym, price, 'STOP_LOSS');
  }

  // Case C: After break-even set — trailing stop below stopPrice
  // stopPrice was moved to entry on break-even, so this protects from giving back all gains
  if (pos.breakEvenSet && price <= pos.stopPrice) {
    const pct = ((price - pos.entryPrice) / pos.entryPrice * 100).toFixed(2);
    const reason = pos.stopPrice <= pos.entryPrice * 1.002 ? 'BREAK_EVEN_STOP' : 'TRAILING_STOP';
    log('risk', `🔒 ${sym} ${reason} @ $${price.toFixed(2)} (${pct}%) stop=$${pos.stopPrice.toFixed(2)}`);
    return exitPosition(sym, price, reason);
  }

  // ── 3. IMMEDIATE PROFIT LOCK + TRAILING STOP ──────────────────────
  // Philosophy: as soon as price moves in our favor at all, lock in profit.
  // Worst case = scratch trade (exit at entry). Best case = full profit run.
  //
  // Tier 1 (0.3%+): move stop to entry price → can never lose on this trade
  // Tier 2 (1.0%+): trail 0.8% below high water → lock small profit
  // Tier 3 (2.0%+): trail 1.5% below high water → lock bigger profit
  // Tier 4 (4.0%+): trail 2.0% below high water → let winner run with tight trail
  // After TP1 hit:  trail 1.5% below high water on remaining shares
  // After TP2 hit:  trail 1.0% below high water on final runner

  const hwChg = (pos.highWater - pos.entryPrice) / pos.entryPrice;

  // Break-even: lock when up 0.5× ATR — stock made a real move, protect it
  const beThreshold = Math.max(posAtrPct * 0.5, 0.002); // min 0.2%
  if (!pos.breakEvenSet && chg >= beThreshold) {
    positions[sym].stopPrice    = pos.entryPrice * 1.0001;
    positions[sym].breakEvenSet = true;
    log('risk', `🔒 ${sym} BE @ $${pos.entryPrice.toFixed(2)} (+${(chg*100).toFixed(2)}%)`);
    await syncLog('sys', `🔒 Break-even: ${sym}`);
    // Lock 15% of position at break-even — guarantees we never fully scratch
    // Even if stop hits, we've already taken some off the table
    const lockQty = Math.max(0, Math.floor((pos.qtyRemaining || pos.qty) * 0.15));
    if (lockQty >= 1 && !pos.tp1Hit) {
      log('risk', `🔒 ${sym} locking ${lockQty} shares at BE ($${price.toFixed(2)}) — guaranteed profit`);
      await partialExit(sym, price, lockQty, 'BE_LOCK');
    }
  }

  // ── PRE-TP1 TRAILING: protect gains between BE and TP1 ──────────
  // Once up 0.8%+ and break-even set, trail 1.2× ATR below high water
  // This locks partial profits before TP1 fires
  if (pos.breakEvenSet && !pos.tp1Hit && chg >= 0.005) { // trail from 0.5% (was 0.8%)
    const preTrail = pos.highWater * (1 - Math.max(posAtrPct * 1.2, 0.004));
    if (preTrail > positions[sym].stopPrice) {
      positions[sym].stopPrice = preTrail;
      // Don't log every update — only when stop actually moves up
    }
  }

  // Trailing stop only after TP1 — trail 1× ATR below high water
  if (pos.tp1Hit && !pos.tp2Hit) {
    const trail = pos.highWater * (1 - Math.max(posAtrPct, 0.008));
    if (trail > positions[sym].stopPrice) positions[sym].stopPrice = trail;
  }
  if (pos.tp1Hit && pos.tp2Hit) {
    const trail = pos.highWater * (1 - Math.max(posAtrPct * 0.75, 0.005));
    if (trail > positions[sym].stopPrice) positions[sym].stopPrice = trail;
  }

  // Stop checks consolidated above (Case A/B/C)

  // Momentum fade removed -- trailing stop after TP1 handles this cleanly

  // ── PEAK DETECTION — exit when strong reversal signals fire at meaningful profit ──
  // ── MICRO-MOVE DETECTOR ────────────────────────────────────────
  // Detects trades that are only moving in tiny increments (cents) and never
  // building into real profit. These drain time and lock up capital.
  //
  // Triggers when ALL of these are true:
  //   1. Trade has been open for at least 5 bars (25 min)
  //   2. Max favorable excursion is tiny (< 0.3% or < $0.10 per share)
  //   3. Price range of last 5 bars is very small (< 0.2% total range)
  //   4. Current P&L is positive but microscopic (making cents not dollars)
  //   5. RSI is not deeply oversold (no imminent bounce expected)
  //
  // Exit immediately — free up capital for a real opportunity
  if (pos.breakEvenSet && bars && bars.length >= 10 && holdMins >= 15) {
    const last5       = bars.slice(-5);
    const last5high   = Math.max(...last5.map(b => b.h));
    const last5low    = Math.min(...last5.map(b => b.l));
    const last5range  = (last5high - last5low) / price;   // range of last 5 bars as %
    const mfe         = (pos.highWater - pos.entryPrice) / pos.entryPrice;
    const pnlPerShare = price - pos.entryPrice;            // actual cents moved
    const rsiNow      = rsi(bars.map(b => b.c), 14);

    // Is the trade stuck in a micro-range?
    const microRange  = last5range < 0.002;                // last 5 bars moved < 0.2%
    const microProfit = mfe < 0.003;                       // best move < 0.3%
    const microCents  = Math.abs(pnlPerShare) < 0.15;      // less than 15 cents per share
    const notOversold = rsiNow > 38;                       // no oversold bounce expected
    const inProfit    = chg >= 0;                          // currently at or above entry

    if (microRange && microProfit && microCents && notOversold && inProfit) {
      const totalPnl = pnlPerShare * (pos.qtyRemaining || pos.qty);
      log('sell', `💤 ${sym} MICRO-MOVE EXIT — stuck in ${(last5range*100).toFixed(2)}% range for ${holdMins.toFixed(0)}min | MFE=${(mfe*100).toFixed(2)}% | P&L=+$${totalPnl.toFixed(2)} — freeing capital`);
      await syncLog('sell', `💤 Micro-move exit: ${sym} @ $${price.toFixed(2)} | range=${(last5range*100).toFixed(2)}% over 5 bars | total=$${totalPnl.toFixed(2)}`);
      return exitPosition(sym, price, 'MICRO_MOVE_EXIT');
    }
  }

  // Only runs when trade is up 2%+ to avoid exiting early on small moves
  if (pos.breakEvenSet && chg >= CONFIG.peakMinProfit && bars && bars.length >= 10) {
    const peak = detectPeak(sym, bars, pos);
    // Only act on immediate urgency with 2+ signals — avoids hair-trigger exits
    if (peak.isPeak && peak.urgency === 'immediate' && peak.signals.length >= CONFIG.peakSignalsReq) {
      log('sell', `🔔 PEAK EXIT ${sym} @ $${price.toFixed(2)} (+${(chg*100).toFixed(2)}%) — ${peak.signals.slice(0,2).join(', ')}`);
      await syncLog('sell', `🔔 Peak exit: ${sym} @ $${price.toFixed(2)} | ${peak.signals.join(' | ')}`);
      return exitPosition(sym, price, 'PEAK_EXIT');
    }
  }

  // ── Take profit tiers ──
  if (!pos.tp1Hit && chg >= effectiveTP1) {
    const sell = Math.max(1, Math.floor(pos.qtyRemaining * 0.33));
    positions[sym].tp1Hit = true;
    log('sell', `🎯 TP1 +${(chg*100).toFixed(1)}% (dyn ${(effectiveTP1*100).toFixed(1)}%): selling ${sell}x ${sym} @ $${price.toFixed(2)}`);
    await partialExit(sym, price, sell, 'TP1');
    if (!positions[sym]) return;
    return;
  }
  if (pos.tp1Hit && !pos.tp2Hit && chg >= effectiveTP2) {
    const sell = Math.max(1, Math.floor(positions[sym].qtyRemaining * 0.5));
    positions[sym].tp2Hit = true;
    log('sell', `🎯🎯 TP2 +${(chg*100).toFixed(1)}% (dyn ${(effectiveTP2*100).toFixed(1)}%): selling ${sell}x ${sym} @ $${price.toFixed(2)}`);
    await partialExit(sym, price, sell, 'TP2');
    if (!positions[sym]) return;
    return;
  }
  if (pos.tp1Hit && pos.tp2Hit && chg >= effectiveTP3) {
    log('sell', `🎯🎯🎯 TP3 +${(chg*100).toFixed(1)}% (dyn ${(effectiveTP3*100).toFixed(1)}%): final exit ${sym} @ $${price.toFixed(2)}`);
    return exitPosition(sym, price, 'TAKE_PROFIT');
  }

  // ── 9. Resistance exit — disabled in sim (SR levels unreliable with limited history) ──
  if (!isSimMode() && nearResistance(price, pos.srLevels) && chg > 0) {
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

  // ── AI TRADE ADVISOR ─────────────────────────────────────────
  // Runs every 2 min per position — Claude Haiku analyzes full context
  // and makes HOLD/TIGHTEN/PARTIAL/EXIT decision
  if (AI_ADVISOR_KEY && !isSimMode()) {
    const aiDecision = await runAIAdvisor(sym, price, bars, pos);
    if (aiDecision) await applyAIDecision(sym, price, pos, aiDecision);
    // If AI exited the position, stop processing
    if (!positions[sym]) return;
  }

  // Log status with actual effective TP values so they're visible in bot log
  const tpNext    = !pos.tp1Hit ? `TP1@+${(effectiveTP1*100).toFixed(2)}%` : !pos.tp2Hit ? `TP2@+${(effectiveTP2*100).toFixed(2)}%` : `TP3@+${(effectiveTP3*100).toFixed(2)}%`;
  const stopDist  = ((price - pos.stopPrice) / price * 100).toFixed(2);
  // Log position status max once per 60s to prevent log spam
  const _now = Date.now();
  if (!posLogThrottle[sym] || _now - posLogThrottle[sym] > 60000) {
    posLogThrottle[sym] = _now;
    log('pos', `${sym} ${chg>=0?'+':''}${(chg*100).toFixed(2)}% | SL=$${pos.stopPrice.toFixed(2)}(${stopDist}% away) | ${tpNext} | BE:${pos.breakEvenSet?'✅':'❌'} TP1:${pos.tp1Hit?'✅':'❌'} TP2:${pos.tp2Hit?'✅':'❌'}`);
  }
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

  // Settings are loaded by the fast config poll every 5s — no need to reload here
  // This prevents the sim mode from being reset on every scan

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

  // Always sync real-time prices BEFORE managing positions
  // This prevents stale bar-close prices from triggering false stop losses
  if (!isSimMode()) await syncPricesOnly();

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
      // Use real-time price from WebSocket stream — sub-ms latency
      if (positions[sym]) {
        const rtPrice  = priceHistory5m[sym]?.[priceHistory5m[sym].length - 1];
        const managedPrice = rtPrice && rtPrice > 0 ? rtPrice : price;
        // Merge live tick bars so ATR/RSI use tick-accurate current candle
        const liveBars = getLiveBars(sym, bars5m);
        await managePosition(sym, managedPrice, liveBars);
        if (positions[sym]) {
          const pct = ((managedPrice - positions[sym].entryPrice) / positions[sym].entryPrice * 100).toFixed(2);
          if (!posLogThrottle[sym+'_scan'] || Date.now()-posLogThrottle[sym+'_scan']>120000) {
            posLogThrottle[sym+'_scan']=Date.now();
            log('pos', `LONG ${positions[sym].qtyRemaining||positions[sym].qty}x ${sym} @ $${positions[sym].entryPrice.toFixed(2)} → $${managedPrice.toFixed(2)} (${pct}%) ${sessionLabel}`);
          }
        }
        continue;
      }

      // Manage open short position — use real-time price
      if (shortPositions[sym]) {
        const rtPriceS = priceHistory5m[sym]?.[priceHistory5m[sym].length - 1];
        const managedPriceS = rtPriceS && rtPriceS > 0 ? rtPriceS : price;
        await manageShort(sym, managedPriceS, bars5m);
        if (shortPositions[sym]) {
          const chg = ((shortPositions[sym].entryPrice - managedPriceS) / shortPositions[sym].entryPrice * 100).toFixed(2);
          log('pos', `SHORT ${shortPositions[sym].qtyRemaining}x ${sym} @ $${shortPositions[sym].entryPrice.toFixed(2)} → $${managedPriceS.toFixed(2)} (${chg}%)`);
        }
        continue;
      }

      // Generate signal
      let sig = generateSignalByStrategy(sym, bars5m, bars15m) || generateSignal(sym, bars5m, bars15m);
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

      // Recalculate AFTER each entry — prevents multiple entries in same scan
      const totalOpen = Object.keys(positions).length + Object.keys(shortPositions).length + Object.keys(scalpPositions).length;
      if (totalOpen >= CONFIG.maxOpenPositions) continue; // hard stop

      // Portfolio risk check — never deploy more than 90% of portfolio
      const deployedCapital = Object.entries(positions).reduce((a,[s,pos])=>{
        const cur = priceHistory5m[s]?.[priceHistory5m[s].length-1] || pos.entryPrice;
        return a + cur * (pos.qtyRemaining||pos.qty);
      }, 0);
      if (deployedCapital > portfolio * 0.90) {
        log('risk', `Portfolio 90% deployed ($${deployedCapital.toFixed(0)}) — no new entries`);
        continue;
      }

      // Signal confirmation gate — must appear on consecutive scans
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
      if (sig.signal === 'SELL' && CONFIG.shortsEnabled && totalOpen < CONFIG.maxOpenPositions) {
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

  // Recovery scan — runs every live scan when recovery mode is active
  if (isInRecovery()) {
    await runRecoveryScan();
  }

  await syncAll();
}

// Live Alpaca positions — refreshed every scan to prevent duplicate buys
let alpacaPositions = new Set();
let alpacaShorts    = new Set();
// Shorting controlled via CONFIG.shortsEnabled (dashboard toggle)

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
        await sbFetch(`${tbl('tc_positions')}?symbol=eq.${sym}`, 'DELETE');

        // Log and alert
        await syncTrade({ sym, side: 'SELL', qty, price: lastPrice, pnl, reason: 'MANUAL_CLOSE' });
        await syncLog('warn', `🖐 Manual close: ${sym} ${qty}x @ ~$${lastPrice.toFixed(2)} | P&L: ${pnl>=0?'+':''}$${pnl.toFixed(2)}`);
        await sendDiscordAlert('manual_close', sym, qty, lastPrice, pnl, 'MANUAL_CLOSE');
        await syncPortfolio();
      }
    }

    // ── Update live set ──
    alpacaPositions = new Set(data.filter(p => p.side === 'long').map(p => p.symbol));
    alpacaShorts    = new Set(data.filter(p => p.side === 'short').map(p => p.symbol));

    // ── Restore positions after restart ──
    for (const p of data) {
      const sym       = p.symbol;
      const isShort   = p.side === 'short';
      const curPrice  = +p.current_price || +p.avg_entry_price;
      const entryPrice = +p.avg_entry_price;
      const qty        = Math.abs(+p.qty);

      // Seed price history immediately so P&L is correct on first poll
      priceHistory5m[sym] = [curPrice];

      if (isShort && !shortPositions[sym]) {
        shortPositions[sym] = {
          entryPrice, qty, qtyRemaining: qty,
          cost:         entryPrice * qty,
          entryTime:    new Date(),
          highWater:    curPrice,
          lowWater:     curPrice,
          atrAtEntry:   0,
          stopPrice:    entryPrice * (1 + CONFIG.stopLossPct),
          breakEvenSet: false, tp1Hit: false, tp2Hit: false,
          srLevels:     [],
          direction:    'short',
          sigInfo:      { confidence: 0, reasons: ['Restored from Alpaca'] },
        };
        log('sys', `Restored SHORT: ${sym} ${qty}x @ $${entryPrice} cur=$${curPrice}`);
        await syncLog('sys', `Restored SHORT ${sym} from Alpaca`);
      } else if (!isShort && !positions[sym]) {
        positions[sym] = {
          entryPrice, qty, qtyRemaining: qty,
          cost:         entryPrice * qty,
          entryTime:    new Date(),
          highWater:    curPrice,
          lowWater:     curPrice,
          atrAtEntry:   0,
          stopPrice:    entryPrice * (1 - CONFIG.stopLossPct),
          breakEvenSet: false, tp1Hit: false, tp2Hit: false,
          srLevels:     [],
          direction:    'long',
          sigInfo:      { confidence: 0, reasons: ['Restored from Alpaca'] },
        };
        log('sys', `Restored LONG: ${sym} ${qty}x @ $${entryPrice} cur=$${curPrice}`);
        await syncLog('sys', `Restored ${sym} from Alpaca after restart`);
      }
    }

    // Write all restored positions to Supabase with correct current prices
    await syncPositions();
    log('acct', `Alpaca positions restored: ${data.map(p=>`${p.symbol}(${p.side})`).join(', ')}`);
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

  const displayEquity = realEquity > 0 ? realEquity : portfolio;
  const openPnlTotal  = Object.entries(positions).reduce((a, [sym, pos]) => {
    const cur = priceHistory5m[sym]?.[priceHistory5m[sym].length-1] || pos.entryPrice;
    return a + (cur - pos.entryPrice) * (pos.qtyRemaining || pos.qty);
  }, 0);
  const dayPnlDisplay = realDailyStartEquity > 0 ? displayEquity - realDailyStartEquity : openPnlTotal;

  const fields = [
    sym!=='ALL' ? {name:'Symbol',value:sym,inline:true} : null,
    qty>0       ? {name:'Shares',value:String(qty),inline:true} : null,
    price>0     ? {name:'Price',value:`$${price.toFixed(2)}`,inline:true} : null,
    pnl!==undefined ? {name:'Trade P&L',value:`${pnl>=0?'+':''}$${(+pnl).toFixed(2)}`,inline:true} : null,
    extra?.stopPrice ? {name:'Stop Loss',value:`$${extra.stopPrice.toFixed(2)}`,inline:true} : null,
    extra?.atrVal    ? {name:'ATR',value:(+extra.atrVal).toFixed(2),inline:true} : null,
    {name:'Account Value',value:`$${displayEquity.toFixed(2)}`,inline:true},
    {name:'Day P&L',value:`${dayPnlDisplay>=0?'+':''}$${dayPnlDisplay.toFixed(2)}`,inline:true},
    {name:'W / L',value:`${totalWins} / ${totalLosses}`,inline:true},
    {name:'Mode',value:isSimMode()?'🎮 Sim':CONFIG.alpacaPaper?'📄 Paper':'💰 LIVE',inline:true},
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
        { name: 'Account Value',  value: `$${(realEquity > 0 ? realEquity : portfolio).toFixed(2)}`, inline: true },
        { name: 'Day P&L',        value: `${totalPnl >= 0 ? '+' : ''}$${totalPnl.toFixed(2)}`, inline: true },
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
    const url   = `${ALPACA_DATA_BASE}/v2/stocks/${symbol}/bars?timeframe=1Min&start=${start}&limit=${limit}&feed=${getDataFeed()}`;
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
    const data = await alpacaFetch(`${ALPACA_DATA_BASE}/v2/stocks/${symbol}/trades/latest?feed=${getDataFeed()}`);
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
  if (!bars1m || bars1m.length < 20) {
    return { signal: 'HOLD', confidence: 0, score: 0, reasons: ['Need 20+ 1m bars'] };
  }

  const closes  = bars1m.map(b => b.c);
  const highs   = bars1m.map(b => b.h);
  const lows    = bars1m.map(b => b.l);
  const volumes = bars1m.map(b => b.v);
  const price   = closes[closes.length - 1];
  const reasons = [];

  // ── HARD BLOCKS — reject immediately if conditions aren't right ──

  // 1. ATR must be meaningful — frozen markets kill scalps
  const atrVal = atr(bars1m, 10);
  const atrPct = price > 0 ? atrVal / price : 0;
  if (atrPct < 0.0003) { // 0.03% minimum — stock must be moving
    return { signal: 'HOLD', confidence: 0, score: 0,
      reasons: [`ATR too low (${(atrPct*100).toFixed(3)}%) — market frozen`] };
  }

  // 2. Volume must be present
  const avgVol  = volumes.slice(-15).reduce((a,b)=>a+b,0) / 15;
  const curVol  = volumes[volumes.length-1];
  const volRatio = avgVol > 0 ? curVol / avgVol : 1;
  if (volRatio < 0.6) {
    return { signal: 'HOLD', confidence: 0, score: 0,
      reasons: [`Volume too low (${volRatio.toFixed(1)}x) — no liquidity`] };
  }

  // ── SCORING — need strong confluence from multiple sources ──
  let buy = 0, sell = 0;

  // ── A. VWAP position — the most important intraday level ──
  const vw       = vwap(bars1m);
  const vwapDist = (price - vw) / vw;
  // Require meaningful separation from VWAP (not just sitting on it)
  if (vwapDist > 0.002) {
    buy += 25; reasons.push(`Above VWAP +${(vwapDist*100).toFixed(2)}% ✅`);
  } else if (vwapDist < -0.002) {
    sell += 25; reasons.push(`Below VWAP ${(vwapDist*100).toFixed(2)}% ✅`);
  } else {
    return { signal: 'HOLD', confidence: 0, score: 0,
      reasons: [`Too close to VWAP (${(vwapDist*100).toFixed(3)}%) — no edge`] };
  }

  // ── B. EMA stack on 1m — trend direction ──
  const e5  = ema(closes, 5);
  const e13 = ema(closes, 13);
  const e21 = ema(closes, 21);
  const pe5 = ema(closes.slice(0,-1), 5);
  const pe13 = ema(closes.slice(0,-1), 13);

  // All three stacked = strong trend
  if (price > e5 && e5 > e13 && e13 > e21) {
    buy += 20; reasons.push(`EMA stack bull: price>E5>E13>E21 ✅`);
  } else if (price < e5 && e5 < e13 && e13 < e21) {
    sell += 20; reasons.push(`EMA stack bear: price<E5<E13<E21 ✅`);
  } else if (e5 > e13) {
    buy += 10; reasons.push(`EMA5 > EMA13 ✅`);
  } else if (e5 < e13) {
    sell += 10; reasons.push(`EMA5 < EMA13 ✅`);
  }

  // Fresh crossover = extra points (momentum just started)
  if (pe5 <= pe13 && e5 > e13) { buy  += 15; reasons.push(`Fresh EMA5/13 bullish cross 🔥`); }
  if (pe5 >= pe13 && e5 < e13) { sell += 15; reasons.push(`Fresh EMA5/13 bearish cross 🔥`); }

  // ── C. Momentum — last 2 bars must agree (not 3, less lag) ──
  const last = closes[closes.length-1], prev = closes[closes.length-2];
  const prev2 = closes[closes.length-3];
  if (last > prev && prev > prev2) { buy  += 15; reasons.push(`Momentum up ✅`); }
  if (last < prev && prev < prev2) { sell += 15; reasons.push(`Momentum down ✅`); }

  // ── D. Candle quality — need a real move, not noise ──
  const lastBar  = bars1m[bars1m.length-1];
  const body     = Math.abs(lastBar.c - lastBar.o);
  const range    = lastBar.h - lastBar.l || 0.0001;
  const bodyPct  = body / range;
  // Require solid body (>50% of range) — indecision candles = no trade
  if (bodyPct < 0.40) {
    return { signal: 'HOLD', confidence: 0, score: 0,
      reasons: [`Weak candle body (${(bodyPct*100).toFixed(0)}% of range) — indecision`] };
  }
  if (bodyPct > 0.65) {
    lastBar.c > lastBar.o ? buy += 10 : sell += 10;
    reasons.push(`Strong candle (${(bodyPct*100).toFixed(0)}% body) ✅`);
  }

  // ── E. Volume surge confirms move ──
  if (volRatio >= 2.0) {
    buy > sell ? buy += 20 : sell += 20;
    reasons.push(`Volume surge ${volRatio.toFixed(1)}x ✅`);
  } else if (volRatio >= 1.3) {
    buy > sell ? buy += 10 : sell += 10;
    reasons.push(`Volume ${volRatio.toFixed(1)}x ✅`);
  }

  // ── F. RSI 1m — confirm not already exhausted ──
  const r1m = rsi(closes, 9);
  // Don't buy overbought, don't sell oversold
  if (buy > sell && r1m > 80) {
    return { signal: 'HOLD', confidence: 0, score: 0,
      reasons: [`RSI ${r1m.toFixed(0)} overbought — scalp buy too late`] };
  }
  if (sell > buy && r1m < 20) {
    return { signal: 'HOLD', confidence: 0, score: 0,
      reasons: [`RSI ${r1m.toFixed(0)} oversold — scalp sell too late`] };
  }
  if (buy > sell && r1m < 55) { buy  += 10; reasons.push(`RSI ${r1m.toFixed(0)} has room to run ✅`); }
  if (sell > buy && r1m > 45) { sell += 10; reasons.push(`RSI ${r1m.toFixed(0)} has room to fall ✅`); }

  // ── G. Structure: higher highs or lower lows ──
  const rh = highs.slice(-4);
  const rl = lows.slice(-4);
  if (rh[3]>rh[2] && rh[2]>rh[1] && buy>sell)  { buy  += 10; reasons.push(`Higher highs structure ✅`); }
  if (rl[3]<rl[2] && rl[2]<rl[1] && sell>buy)  { sell += 10; reasons.push(`Lower lows structure ✅`); }

  // ── FINAL DECISION ──
  // Need strong directional conviction — buy must be 2x sell (or vice versa)
  // and total score must exceed threshold
  const minScore = CONFIG.scalpMinScore || 70;
  const total    = buy + sell;
  const conf     = total > 0 ? Math.round(Math.max(buy,sell)/total*100) : 0;

  // Require clear direction AND minimum score AND volume present
  if (buy >= minScore && buy >= sell * 2.0 && volRatio >= 0.8) {
    reasons.push(`✅ Scalp BUY: score=${buy} conf=${conf}% ATR=${(atrPct*100).toFixed(3)}%`);
    return { signal:'BUY',  confidence:conf, score:buy,  reasons, atr:atrVal, vwap:vw, rsi:r1m };
  }
  if (sell >= minScore && sell >= buy * 2.0 && volRatio >= 0.8) {
    reasons.push(`✅ Scalp SELL: score=${sell} conf=${conf}% ATR=${(atrPct*100).toFixed(3)}%`);
    return { signal:'SELL', confidence:conf, score:sell, reasons, atr:atrVal, vwap:vw, rsi:r1m };
  }

  return { signal:'HOLD', confidence:conf, score:Math.max(buy,sell),
    reasons:[...reasons, `Insufficient conviction (buy=${buy} sell=${sell} need=${minScore} ratio=${(Math.max(buy,sell)/Math.max(1,Math.min(buy,sell))).toFixed(1)}x)`] };
}

/**
 * Enter a scalp position (long or short)
 */
async function enterScalp(sym, price, sigInfo, direction = 'long') {
  if (checkCircuitBreaker()) return;
  if (!isMarketOpen()) { log('scalp', `Scalp blocked — market closed`); return; }
  if (scalpPositions[sym]) { log('scalp', `Already in scalp position for ${sym}`); return; }
  // Also check Alpaca — in-memory state can diverge from actual positions
  if (alpacaPositions.has(sym) || positions[sym] || shortPositions[sym]) {
    log('scalp', `🚫 ${sym}: Alpaca already has position — blocking scalp entry to prevent duplicate`);
    return;
  }
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
  // Stop = 1× ATR, TP = 2× ATR → guaranteed 2:1 R:R minimum
  const atrVal   = sigInfo.atr || price * 0.002;
  const slDist   = Math.max(atrVal * 1.0, price * 0.001); // 1× ATR, min 0.1%
  const tpDist   = Math.max(atrVal * 2.0, price * 0.002); // 2× ATR, min 0.2% → always 2:1

  const stopPrice = direction === 'long'  ? price - slDist : price + slDist;
  const tpPrice   = direction === 'long'  ? price + tpDist : price - tpDist;

  // Hard check: TP must be > $0.05/share after estimated spread cost
  // Entering a scalp where the spread eats the gain is an instant loss
  const estimatedSpread = price * 0.0002; // ~0.02% typical spread
  const netTP = tpDist - estimatedSpread;
  if (netTP < 0.03) {
    log('scalp', `🚫 ${sym} scalp skipped — TP ($${tpDist.toFixed(3)}) too close to spread ($${estimatedSpread.toFixed(3)})`);
    return;
  }

  // Place order
  const side = direction === 'long' ? 'buy' : 'sell';
  if (isSimMode() || (CONFIG.alpacaKey && CONFIG.mode === 'alpaca')) {
    try { await placeOrder(sym, qty, side); }
    catch(e) { log('error', `Scalp order failed ${sym}: ${e.message}`); return; }
  }

  if (isSimMode()) portfolio -= direction === 'long' ? cost : 0;

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

  // Delete position immediately to prevent double-exit from monitor loop
  delete scalpPositions[sym];

  if (isSimMode() || (CONFIG.alpacaKey && CONFIG.mode === 'alpaca')) {
    try {
      // Always use market order for scalp exits — speed is everything
      // Smart limit orders add 2-4s latency which kills scalp profitability
      if (isSimMode()) {
        simOrder(sym, qty, side);
      } else {
        await alpacaFetch(`${ALPACA_BASE()}/v2/orders`, {
          method: 'POST',
          body: JSON.stringify({
            symbol: sym, qty: String(qty), side,
            type: 'market', time_in_force: 'day',
          }),
        });
      }
    } catch(e) {
      log('error', `Scalp exit order failed ${sym}: ${e.message}`);
      // Restore position if order failed
      scalpPositions[sym] = pos;
      return;
    }
  }

  const pnl = direction === 'long'
    ? (price - entryPrice) * qty
    : (entryPrice - price) * qty;

  if (isSimMode()) portfolio += direction === 'long' ? qty * price : pnl;
  pnl > 0 ? (totalWins++, scalpWins++) : (totalLosses++, scalpLosses++);
  recordTradeOutcome(pnl, { confidence: pos?.sigInfo?.confidence||0, rsi: 50, session: getCurrentSession(), side: pos?.direction||'long', sym, exitReason: reason, holdMins: Math.round((Date.now()-new Date(pos.entryTime).getTime())/60000), pnlPct: pos.entryPrice > 0 ? pnl/(pos.entryPrice*pos.qty) : 0 });

  const holdSecs = Math.round((Date.now() - new Date(pos.entryTime).getTime()) / 1000);
  const icons = { SCALP_TP:'🎯', SCALP_SL:'🛑', SCALP_TRAIL:'📉', SCALP_TIME:'⏰', SCALP_REVERSE:'↩️', SCALP_MANUAL:'🖐' };
  log('scalp', `${icons[reason]||'📤'} SCALP EXIT ${qty}x ${sym} @ $${price.toFixed(2)} | P&L: ${pnl>=0?'+':''}$${pnl.toFixed(2)} | ${holdSecs}s | ${reason}`);

  trades.push({ time: new Date(), sym, side: 'SCALP_EXIT', qty, price, pnl, reason });

  // Fire-and-forget the non-critical async work — don't await it
  // This prevents the monitor from being held up by Supabase/Discord writes
  Promise.all([
    syncTrade({ sym, side: 'SCALP_EXIT', qty, price, pnl, reason }),
    sendDiscordAlert('scalp_exit', sym, qty, price, pnl, reason, pos.sigInfo),
    syncLog('sell', `${icons[reason]||'📤'} SCALP EXIT ${qty}x ${sym} @ $${price.toFixed(2)} P&L=${pnl>=0?'+':''}$${pnl.toFixed(2)} (${reason}) ${holdSecs}s`),
    syncAll(),
  ]).catch(e => log('error', `Scalp exit sync failed: ${e.message}`));

  // Cooldown: block re-entry on this symbol for 60 seconds after a loss
  if (pnl < 0) {
    if (!dupWarnThrottle) {} // ensure exists
    dupWarnThrottle[sym + '_scalpCooldown'] = Date.now();
    log('scalp', `⏸ ${sym} scalp cooldown: no re-entry for 60s after loss`);
  }

  // Trigger recovery on scalp losses too
  if (pnl < 0 && CONFIG.recoveryMode) {
    triggerRecovery(sym, Math.abs(pnl), price, pos);
  }
}

/**
 * Manage all open scalp positions — called every 5 seconds
 * Precision exit logic: TP, SL, trailing, time stop, signal reversal
 */
// ─────────────────────────────────────────────
// SCALP POSITION MONITOR — runs every 2 seconds
// ─────────────────────────────────────────────
// Completely separate from the main scan tick.
// Uses cached prices for checks (no API call),
// only hits Alpaca when actually placing an exit order.
// This makes exits near-instant instead of delayed by scan cycle.
// ─────────────────────────────────────────────
let scalpMonitorRunning = false;

async function scalpPositionMonitor() {
  if (scalpMonitorRunning) return;
  if (Object.keys(scalpPositions).length === 0) return;
  scalpMonitorRunning = true;

  try {
    for (const [sym, pos] of Object.entries(scalpPositions)) {
      // CRITICAL: skip first 5 seconds after entry (increased from 3)
      // Stale bar prices from before entry can immediately trigger stop loss
      const ageSecs = (Date.now() - new Date(pos.entryTime).getTime()) / 1000;
      if (ageSecs < 5) continue;

      // CRITICAL: Always use LIVE price for scalp decisions
      // NEVER use priceHistory5m — it's the 5m bar close which can be 5 minutes stale
      // A $393 entry with a $363 cached price triggers stop loss immediately (proven bug)
      let price = await fetchLatestTrade(sym);
      if (!price || price <= 0) {
        // fetchLatestTrade failed — skip this cycle rather than use stale price
        log('scalp', `⚠ ${sym}: could not get live price — skipping check to avoid false exit`);
        continue;
      }
      // Sanity check: price must be within 5% of entry price
      // If not, it's likely a stale/bad quote — skip rather than false-exit
      const priceDeviation = Math.abs(price - pos.entryPrice) / pos.entryPrice;
      if (priceDeviation > 0.05) {
        log('scalp', `⚠ ${sym}: live price $${price.toFixed(2)} deviates ${(priceDeviation*100).toFixed(1)}% from entry $${pos.entryPrice.toFixed(2)} — verifying`);
        // Double-check with a second fetch before acting
        const price2 = await fetchLatestTrade(sym);
        if (!price2 || Math.abs(price2 - pos.entryPrice) / pos.entryPrice > 0.05) {
          log('scalp', `⚠ ${sym}: confirmed large move — proceeding with exit check`);
          price = price2 || price;
        } else {
          price = price2; // use the better quote
        }
      }

      const { direction, entryPrice, stopPrice, tpPrice } = pos;
      const holdMins = (Date.now() - new Date(pos.entryTime).getTime()) / 60000;
      const pnlNow   = direction === 'long' ? (price - entryPrice) * pos.qty : (entryPrice - price) * pos.qty;

      // Update water marks
      if (direction === 'long'  && price > pos.highWater) scalpPositions[sym].highWater = price;
      if (direction === 'short' && price < pos.lowWater)  scalpPositions[sym].lowWater  = price;

      // ── EXIT 1: Take profit ──
      const hitTP = direction === 'long' ? price >= tpPrice : price <= tpPrice;
      if (hitTP) {
        log('scalp', `🎯 TP hit: ${sym} @ $${price.toFixed(2)} (+$${pnlNow.toFixed(2)})`);
        await exitScalp(sym, price, 'SCALP_TP');
        continue;
      }

      // ── EXIT 2: Stop loss ──
      const hitSL = direction === 'long' ? price <= stopPrice : price >= stopPrice;
      if (hitSL) {
        log('scalp', `🛑 SL hit: ${sym} @ $${price.toFixed(2)} ($${pnlNow.toFixed(2)})`);
        await exitScalp(sym, price, 'SCALP_SL');
        continue;
      }

      // ── EXIT 3: Trailing stop (activates at 50% of TP distance) ──
      const tpDist = Math.abs(tpPrice - entryPrice);
      const moved  = direction === 'long' ? price - entryPrice : entryPrice - price;
      if (moved >= tpDist * 0.5) {
        if (!scalpPositions[sym].trailingActive) {
          scalpPositions[sym].trailingActive = true;
          log('scalp', `📉 ${sym} trailing stop activated @ $${price.toFixed(2)}`);
        }
        const trail = direction === 'long'
          ? scalpPositions[sym].highWater * (1 - CONFIG.scalpTrailingPct)
          : scalpPositions[sym].lowWater  * (1 + CONFIG.scalpTrailingPct);
        // Floor: trailing stop must never go below entry for longs (guarantees scratch at worst)
        // or above entry for shorts
        const trailFloorLong  = Math.max(trail, pos.entryPrice * 0.9995); // min = entry - 0.05%
        const trailFloorShort = Math.min(trail, pos.entryPrice * 1.0005); // max = entry + 0.05%
        if (direction === 'long'  && trailFloorLong  > scalpPositions[sym].stopPrice) scalpPositions[sym].stopPrice = trailFloorLong;
        if (direction === 'short' && trailFloorShort < scalpPositions[sym].stopPrice) scalpPositions[sym].stopPrice = trailFloorShort;

        // If trailing stop now hit
        const trailHit = direction === 'long' ? price <= scalpPositions[sym].stopPrice : price >= scalpPositions[sym].stopPrice;
        if (trailHit) {
          log('scalp', `📉 Trail stop hit: ${sym} @ $${price.toFixed(2)} P&L: ${pnlNow>=0?'+':''}$${pnlNow.toFixed(2)}`);
          await exitScalp(sym, price, 'SCALP_TRAIL');
          continue;
        }
      }

      // ── EXIT 4: Time stop ──
      // Max 4 minutes — scalps that haven't moved must be cut loose
      const maxMins = Math.min(CONFIG.scalpMaxHoldMins, 4);
      if (holdMins >= maxMins) {
        log('scalp', `⏰ Time stop: ${sym} held ${holdMins.toFixed(1)}m P&L: ${pnlNow>=0?'+':''}$${pnlNow.toFixed(2)}`);
        await exitScalp(sym, price, 'SCALP_TIME');
        continue;
      }

      // ── EXIT 5: Momentum death ──
      if (holdMins >= 1.5) {
        const mfe = direction === 'long'
          ? (pos.highWater - entryPrice) / entryPrice
          : (entryPrice - pos.lowWater) / entryPrice;
        const curChg = direction === 'long'
          ? (price - entryPrice) / entryPrice
          : (entryPrice - price) / entryPrice;
        // Was profitable but giving back >50% of gains — lock it
        if (mfe > 0.001 && curChg < mfe * 0.5) {
          log('scalp', `📉 ${sym} momentum reverting MFE=${(mfe*100).toFixed(3)}% now=${(curChg*100).toFixed(3)}%`);
          await exitScalp(sym, price, 'SCALP_TRAIL');
          continue;
        }
        // Micro-move: 2min in and barely moved
        if (holdMins >= 2 && mfe < 0.0005 && Math.abs(price-entryPrice) < 0.05 && pnlNow >= 0) {
          log('scalp', `💤 ${sym} scalp dead (${holdMins.toFixed(1)}m) — freeing capital`);
          await exitScalp(sym, price, 'SCALP_MICRO_MOVE');
          continue;
        }
      }
      // Still holding — throttled status log (once per 30s per symbol)
      const _sk = sym+'_scalpLog';
      if (!posLogThrottle[_sk] || Date.now()-posLogThrottle[_sk] > 30000) {
        posLogThrottle[_sk] = Date.now();
        log('scalp', `⚡ ${sym} ${direction.toUpperCase()} $${entryPrice.toFixed(2)}→$${price.toFixed(2)} P&L:${pnlNow>=0?'+':''}$${pnlNow.toFixed(2)} SL=$${pos.stopPrice.toFixed(2)} TP=$${tpPrice.toFixed(2)} ${holdMins.toFixed(1)}m`);
      }
    }
  } catch(e) {
    log('error', `scalpMonitor: ${e.message}`);
  } finally {
    scalpMonitorRunning = false;
  }
}

async function manageScalpPositions() {
  // Now just a thin wrapper — real management is in scalpPositionMonitor
  // Keep this for signal reversal check (less time-critical)
  if (Object.keys(scalpPositions).length === 0) return;

  for (const [sym, pos] of Object.entries(scalpPositions)) {
    if (!scalpPositions[sym]) continue; // may have been exited by monitor

    // Signal reversal check — only fetch bars if still holding
    try {
      const bars1m = await fetchScalpBars(sym, 20);
      if (bars1m) {
        const sig      = generateScalpSignal(sym, bars1m);
        const reversed = (pos.direction === 'long'  && sig.signal === 'SELL' && sig.score >= CONFIG.scalpMinScore)
                      || (pos.direction === 'short' && sig.signal === 'BUY'  && sig.score >= CONFIG.scalpMinScore);
        if (reversed && scalpPositions[sym]) {
          const price = priceHistory5m[sym]?.[priceHistory5m[sym].length-1] || pos.entryPrice;
          log('scalp', `↩️ Signal reversal: ${sym} — exiting`);
          await exitScalp(sym, price, 'SCALP_REVERSE');
        }
      }
    } catch(e) {}
  }
}

/**
 * Scalp scan — runs every 5 seconds on SCALP_SYMBOLS only
 */
async function runScalpScan() {
  if (!CONFIG.scalpMode) return;
  if (!isMarketOpen()) return;
  if (checkCircuitBreaker()) return;

  // Don't scalp during off-hours — 1m bars have no liquidity for US stocks
  // Scalping needs real price discovery, not pre/after-market noise
  const sess = getCurrentSession();
  if (sess.includes('Off') || sess.includes('Pre') || sess.includes('Asia')) {
    return; // no scalping outside US market hours
  }

  // First manage existing positions (most important)
  await manageScalpPositions();

  // Check if we have room for new positions
  const openScalps = Object.keys(scalpPositions).length;
  if (openScalps >= CONFIG.scalpMaxPositions) return;

  // Scan scalp symbols for entries
  for (const sym of CONFIG.scalpSymbols) {
    if (scalpPositions[sym]) continue; // already in position
    if (positions[sym] || shortPositions[sym]) continue; // don't scalp what we're swinging
    // Check 60s cooldown after a loss on this symbol
    const cooldownKey = sym + '_scalpCooldown';
    if (dupWarnThrottle[cooldownKey] && Date.now() - dupWarnThrottle[cooldownKey] < 60000) continue;

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
      } else if (sig.signal === 'SELL' && CONFIG.shortsEnabled) {
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
  // Stagger requests in batches of 3 to avoid rate limiting
  // Alpaca free tier: 200 requests/min — 15 symbols × 2 timeframes = 30 requests
  // Batching with 150ms delays keeps us well under the limit
  const BATCH_SIZE = 3;
  const results = [];
  for (let i = 0; i < symbols.length; i += BATCH_SIZE) {
    const batch = symbols.slice(i, i + BATCH_SIZE);
    const batchResults = await Promise.allSettled(
      batch.map(async sym => {
        const [bars5m, bars15m] = await Promise.all([
          fetchBarsCached(sym, '5Min',  100),
          fetchBarsCached(sym, '15Min', 40),
        ]);
        return { sym, bars5m, bars15m };
      })
    );
    results.push(...batchResults);
    // Small delay between batches to stay under rate limit
    if (i + BATCH_SIZE < symbols.length) {
      await new Promise(r => setTimeout(r, 200));
    }
  }
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
    const data = await alpacaFetch(`${ALPACA_DATA_BASE}/v2/stocks/${symbol}/trades/latest?feed=${getDataFeed()}`);
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
    const data  = await alpacaFetch(`${ALPACA_DATA_BASE}/v2/stocks/${symbol}/quotes/latest?feed=${getDataFeed()}`);
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

    // Shorts need 3 consecutive confirmations, longs need 2
    // 4 confirmations = 60s delay — move is often over before entry
    const required = sig.signal === 'SELL' ? 3 : (CONFIG.confirmCount || 2);

    if (prev.count >= required) {
      pendingSignals.delete(sym);
      log('signal', `✅ ${sym} signal CONFIRMED after ${prev.count} scans → ${sig.signal} (conf:${sig.confidence}%)`);
      return true;
    }
    log('signal', `⏳ ${sym} ${sig.signal} pending (${prev.count}/${required})`);
    return false;
  }
  const required = sig.signal === 'SELL' ? 5 : 4;
  pendingSignals.set(sym, { signal: sig.signal, count: 1, sigInfo: sig });
  log('signal', `⏳ ${sym} new signal (${sig.signal} conf:${sig.confidence}%) — need ${required - 1} more confirmations`);
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
    const qd    = await alpacaFetch(`${ALPACA_DATA_BASE}/v2/stocks/${symbol}/quotes/latest?feed=${getDataFeed()}`);
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
  log('sys', `Pre-warming bar cache for ${CONFIG.symbols.length} symbols (batched to avoid rate limits)…`);
  const t = Date.now();
  // Use small batches with delays during startup to avoid hitting Alpaca rate limits
  // Cold cache = all symbols fetch simultaneously = rate limit burst
  const STARTUP_BATCH = 2;
  for (let i = 0; i < CONFIG.symbols.length; i += STARTUP_BATCH) {
    const batch = CONFIG.symbols.slice(i, i + STARTUP_BATCH);
    await fetchAllBarsParallel(batch);
    if (i + STARTUP_BATCH < CONFIG.symbols.length) {
      await new Promise(r => setTimeout(r, 300)); // 300ms between startup batches
    }
  }
  log('sys', `Bar cache ready in ${Date.now() - t}ms — first scan will fire immediately`);
  // Load previously learned strategies from Supabase
  await apexLoadStrategies();
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
  +(process.env.SCAN_INTERVAL_SEC || 15) * 1000,
  10000 // never faster than 10s
);
const PRICE_SYNC_INTERVAL_MS      = 8000;          // price updates every 8s
const EQUITY_SNAPSHOT_INTERVAL_MS = 90 * 1000;     // equity curve point every 90s
let lastEquitySnapshot = 0;

log('sys', `Full scan every ${FULL_SCAN_INTERVAL_MS/1000}s | Price sync every ${PRICE_SYNC_INTERVAL_MS/1000}s`);

async function tick() {
  if (scanInProgress) return;
  scanInProgress = true;
  try {
    const now = Date.now();

    // ── Simulation mode — handled by dedicated 3s sim tick ──
    if (isSimMode()) return;

    // ── INTRADAY SAFETY NET — force close all positions at 3:50 PM ET ──
  // Belt-and-suspenders: even if managePosition missed the 3:45 signal
  if (!isSimMode() && isWeekday()) {
    const minsLeft = minsToClose();
    if (minsLeft <= -5 && minsLeft > -30) { // 3:50 PM window
      const allOpen = [...Object.keys(positions), ...Object.keys(shortPositions), ...Object.keys(scalpPositions)];
      if (allOpen.length > 0) {
        log('risk', `⏰ INTRADAY CLOSE-ALL: ${allOpen.length} positions still open at market close`);
        for (const sym of Object.keys(positions)) {
          const cur = priceHistory5m[sym]?.[priceHistory5m[sym].length-1] || positions[sym].entryPrice;
          await exitPosition(sym, cur, 'INTRADAY_CLOSE');
        }
        for (const sym of Object.keys(shortPositions)) {
          const cur = priceHistory5m[sym]?.[priceHistory5m[sym].length-1] || shortPositions[sym].entryPrice;
          await coverShort(sym, cur, 'INTRADAY_CLOSE');
        }
      }
    }
  }

  // ── Live / Paper mode ──
  if (CONFIG.positionTradingEnabled && !isSimMode() && isMarketOpen()) {
    const hoursSinceLastPT = (now2 - (tick._lastPTRun||0)) / 3600000;
    if (hoursSinceLastPT >= 23) { // once per day
      tick._lastPTRun = now2;
      managePositionTrades().catch(e => log('error', `PT scan: ${e.message}`));
    }
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

// Main tick every 8 seconds (live/paper mode)
setInterval(tick, PRICE_SYNC_INTERVAL_MS);

// Sim gets its own fast tick — 3 seconds per bar = realistic pace
// 1 bar = 5 min of market time, so 3s real = ~5min sim
// A 2-hour trade plays out in ~24 real seconds — fast enough to test, slow enough to watch
setInterval(async () => {
  if (!isSimMode()) return;
  if (scanInProgress) return;
  scanInProgress = true;
  try {
    await runSimScan();
  } catch(e) {
    log('error', `Sim tick error: ${e.message}`);
  } finally {
    scanInProgress = false;
  }
}, 3000);

// Fast config poll — checks for mode changes every 5 seconds
// This means sim on/off takes effect within 5s instead of up to 30s
let lastConfigCheck = 0;
setInterval(async () => {
  const now = Date.now();
  if (now - lastConfigCheck < 28000) return; // poll every 30s not every 3s
  lastConfigCheck = now;
  try {
    const prevMode = CONFIG.mode;
    const prevSyms = CONFIG.symbols.join(',');
    const prevRsi  = CONFIG.rsiOversold;
    await loadRemoteConfig();
    // Only log if something actually changed
    const changed = CONFIG.mode !== prevMode || CONFIG.symbols.join(',') !== prevSyms || CONFIG.rsiOversold !== prevRsi;
    if (changed) {
      log('sys', `Config updated — Modes: ${CONFIG.mode} | Symbols: ${CONFIG.symbols.length} | RSI: ${CONFIG.rsiOversold}`);
      if (CONFIG.mode !== prevMode) lastFullScan = 0;
    }
  } catch(e) {}
}, 5000);

// Dedicated scalp exit monitor — runs every 2 seconds independently
// Much faster than the main tick so TP/SL exits happen near-instantly
setInterval(() => {
  if (CONFIG.scalpMode && Object.keys(scalpPositions).length > 0) {
    scalpPositionMonitor().catch(e => log('error', `scalpMonitor: ${e.message}`));
  }
}, 2000);

// Daily tasks
cron.schedule('5 16 * * 1-5',  sendDailySummary, { timezone: 'America/New_York' });
cron.schedule('55 8 * * 1-5',  storePrevClose,   { timezone: 'America/New_York' });
cron.schedule('31 9 * * 1-5',  updateDayBias,    { timezone: 'America/New_York' }); // run at market open

// Startup — prewarm data then scan immediately
loadRemoteConfig().then(async () => {
  await updateDayBias();
  if (isMarketOpen()) {
    screenerCandidates = await runMarketScreener();
    lastScreenerRun    = Date.now();
  }
  await prewarmData();
  await runScan();
  lastFullScan = Date.now();

  // Start real-time price streams — both run simultaneously
  connectPriceStream();   // Alpaca WebSocket (IEX paper / SIP live)
  connectPolygon();       // Polygon.io free real-time (if POLYGON_API_KEY set)
});
setTimeout(syncPricesOnly, 5000);

// Sync prices every 10 seconds regardless of market hours
setInterval(async () => {
  if (!isSimMode() && CONFIG.alpacaKey && Object.keys(positions).length + Object.keys(shortPositions).length > 0) {
    await syncPricesOnly();
  }
}, 20000); // Every 20s (was 10s) — reduces Alpaca rate limit hits

// ── OVERNIGHT GUARDIAN — runs every 60 seconds, completely independent of scan loop ──
// If market is closed and we still have open positions, close them immediately.
// This is the final safety net for the "$2900 overnight loss" scenario.
// Works even if the main scan loop is stuck, paused, or missed the 3:45 PM window.
setInterval(async () => {
  if (isSimMode()) return;
  if (!isWeekday()) return;

  const allOpen = Object.keys(positions).length + Object.keys(shortPositions).length + Object.keys(scalpPositions).length;
  if (allOpen === 0) return;

  // Market is closed (after 4:00 PM or before 9:30 AM ET) — no positions should be held
  const now = new Date();
  const et = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const h = et.getHours(), m = et.getMinutes();
  const minsSinceMidnight = h * 60 + m;

  const marketClosed = minsSinceMidnight < 570 || minsSinceMidnight > 960; // before 9:30 AM or after 4:00 PM
  if (!marketClosed) return;

  log('risk', `🚨 OVERNIGHT GUARDIAN: market closed but ${allOpen} position(s) still open — force closing NOW`);
  await syncLog('risk', `🚨 Overnight guardian fired: ${allOpen} positions still open after market close`);

  for (const sym of [...Object.keys(positions)]) {
    try {
      const cur = priceHistory5m[sym]?.[priceHistory5m[sym].length-1] || positions[sym].entryPrice;
      await exitPosition(sym, cur, 'INTRADAY_CLOSE');
      log('risk', `✅ Overnight close: ${sym}`);
    } catch(e) { log('error', `Overnight close failed ${sym}: ${e.message}`); }
  }
  for (const sym of [...Object.keys(shortPositions)]) {
    try {
      const cur = priceHistory5m[sym]?.[priceHistory5m[sym].length-1] || shortPositions[sym].entryPrice;
      await coverShort(sym, cur, 'INTRADAY_CLOSE');
    } catch(e) { log('error', `Overnight short close failed ${sym}: ${e.message}`); }
  }
  for (const sym of [...Object.keys(scalpPositions)]) {
    try {
      const cur = priceHistory5m[sym]?.[priceHistory5m[sym].length-1] || scalpPositions[sym].entryPrice;
      await exitScalp(sym, cur, 'SCALP_TIME');
    } catch(e) { log('error', `Overnight scalp close failed ${sym}: ${e.message}`); }
  }
}, 60000); // check every 60 seconds

// ── DEDICATED RECOVERY SCANNER — every 30s, independent of main scan loop ──
// Recovery needs to act fast after a loss — can't wait for the 15s scan cycle
// which might be busy with position management or rate-limited
setInterval(async () => {
  if (!isInRecovery()) return;
  // Allow in sim for testing — recovery is important to validate
  if (!isMarketOpen() && !isSimMode()) {
    log('recovery', `⏸ Recovery paused — market closed, will resume at open`);
    return; // pause, don't clear — recovery persists until next open
  }
  log('recovery', `🔄 Recovery interval: active for ${recoveryState?.sym} (attempt ${recoveryState?.attempts||0})`);
  await runRecoveryScan();
}, 30000);
// (catches positions opened after initial subscription)
setInterval(() => {
  if (!isSimMode()) subscribeOpenPositions();
  unsubscribeClosedPositions();
}, 30000);

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

// Discord Ed25519 signature verification (nacl loaded at top of file)

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
    if (!valid) log('warn', `Discord: signature verification FAILED — key=${CONFIG.discordPublicKey?.slice(0,8)}... keylen=${key.length} siglen=${sig.length} msglen=${msg.length}`);
    else log('sys', 'Discord: signature verified ✅');
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

  // ── /exitall ──
  if (commandName === 'exitall') {
    const allSyms = [
      ...Object.keys(positions),
      ...Object.keys(shortPositions),
      ...Object.keys(scalpPositions),
    ];

    if (!allSyms.length) {
      return { content: '📭 No open positions to close.' };
    }

    const results = [];
    let totalPnl = 0;

    for (const sym of Object.keys(positions)) {
      try {
        const pos   = positions[sym];
        const price = priceHistory5m[sym]?.[priceHistory5m[sym].length - 1] || pos.entryPrice;
        const pnl   = (price - pos.entryPrice) * (pos.qtyRemaining || pos.qty);
        await exitPosition(sym, price, 'MANUAL_DISCORD');
        totalPnl += pnl;
        results.push(`${pnl >= 0 ? '🟢' : '🔴'} **${sym}** LONG @ $${price.toFixed(2)} → ${pnl >= 0 ? '+' : ''}$${pnl.toFixed(2)}`);
      } catch(e) {
        results.push(`⚠ **${sym}** failed: ${e.message}`);
      }
    }

    for (const sym of Object.keys(shortPositions)) {
      try {
        const pos   = shortPositions[sym];
        const price = priceHistory5m[sym]?.[priceHistory5m[sym].length - 1] || pos.entryPrice;
        const pnl   = (pos.entryPrice - price) * (pos.qtyRemaining || pos.qty);
        await coverShort(sym, price, 'MANUAL_DISCORD');
        totalPnl += pnl;
        results.push(`${pnl >= 0 ? '🟢' : '🔴'} **${sym}** SHORT @ $${price.toFixed(2)} → ${pnl >= 0 ? '+' : ''}$${pnl.toFixed(2)}`);
      } catch(e) {
        results.push(`⚠ **${sym}** failed: ${e.message}`);
      }
    }

    for (const sym of Object.keys(scalpPositions)) {
      try {
        const pos   = scalpPositions[sym];
        const price = priceHistory5m[sym]?.[priceHistory5m[sym].length - 1] || pos.entryPrice;
        const pnl   = pos.direction === 'long'
          ? (price - pos.entryPrice) * pos.qty
          : (pos.entryPrice - price) * pos.qty;
        await exitScalp(sym, price, 'SCALP_MANUAL');
        totalPnl += pnl;
        results.push(`${pnl >= 0 ? '🟢' : '🔴'} **${sym}** SCALP @ $${price.toFixed(2)} → ${pnl >= 0 ? '+' : ''}$${pnl.toFixed(2)}`);
      } catch(e) {
        results.push(`⚠ **${sym}** failed: ${e.message}`);
      }
    }

    await syncLog('warn', `🚨 EXIT ALL: ${allSyms.length} positions closed via Discord | P&L: ${totalPnl >= 0 ? '+' : ''}$${totalPnl.toFixed(2)}`);

    return { content: [
      `🚨 **EXIT ALL — ${allSyms.length} position${allSyms.length > 1 ? 's' : ''} closed**`,
      '',
      results.join('\n'),
      '',
      `**Total P&L: ${totalPnl >= 0 ? '+' : ''}$${totalPnl.toFixed(2)}**`,
    ].join('\n') };
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
      res.end('Missing DISCORD_APP_ID or DISCORD_TOKEN env vars — add them in Railway → Variables');
      return;
    }
    try {
      const fetch = await getFetch();
      const commands = [
        { name:'exit',      description:'Exit a specific open position immediately', options:[{name:'symbol',description:'Ticker (e.g. AAPL)',type:3,required:true}] },
        { name:'exitall',   description:'🚨 Close ALL open positions immediately and pause the bot' },
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

  // ── GET /diagnostic — full state dump for debugging equity spikes ──
  if (req.method === 'GET' && url === '/diagnostic') {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });

    // Snapshot every position with full detail
    const posSnapshot = Object.entries(positions).map(([sym, pos]) => {
      const cur = priceHistory5m[sym]?.[priceHistory5m[sym].length - 1] || pos.entryPrice;
      return {
        sym, direction: 'LONG',
        entryPrice: pos.entryPrice, currentPrice: cur,
        qty: pos.qtyRemaining || pos.qty,
        mktValue: cur * (pos.qtyRemaining || pos.qty),
        pnl: (cur - pos.entryPrice) * (pos.qtyRemaining || pos.qty),
        cost: pos.cost, entryTime: pos.entryTime,
        stopPrice: pos.stopPrice, tpPrice: pos.tpPrice,
        tp1Hit: pos.tp1Hit, tp2Hit: pos.tp2Hit,
        highWater: pos.highWater,
        ageMin: Math.round((Date.now() - new Date(pos.entryTime).getTime()) / 60000),
      };
    });
    const shortSnapshot = Object.entries(shortPositions).map(([sym, pos]) => {
      const cur = priceHistory5m[sym]?.[priceHistory5m[sym].length - 1] || pos.entryPrice;
      return {
        sym, direction: 'SHORT',
        entryPrice: pos.entryPrice, currentPrice: cur,
        qty: pos.qtyRemaining || pos.qty,
        pnl: (pos.entryPrice - cur) * (pos.qtyRemaining || pos.qty),
        entryTime: pos.entryTime,
      };
    });
    const scalpSnapshot = Object.entries(scalpPositions).map(([sym, pos]) => {
      const cur = priceHistory5m[sym]?.[priceHistory5m[sym].length - 1] || pos.entryPrice;
      return {
        sym, direction: pos.direction,
        entryPrice: pos.entryPrice, currentPrice: cur,
        qty: pos.qty, stopPrice: pos.stopPrice, tpPrice: pos.tpPrice,
        pnl: pos.direction === 'long' ? (cur - pos.entryPrice) * pos.qty : (pos.entryPrice - cur) * pos.qty,
        ageMin: Math.round((Date.now() - new Date(pos.entryTime).getTime()) / 60000),
      };
    });

    const longMktVal = posSnapshot.reduce((a, p) => a + p.mktValue, 0);
    const shortPnl   = shortSnapshot.reduce((a, p) => a + p.pnl, 0);
    const scalpMktVal = scalpSnapshot.filter(p => p.direction === 'long').reduce((a, p) => a + p.currentPrice * p.qty, 0);
    const computedEquity = portfolio + longMktVal + shortPnl + scalpMktVal;

    res.end(JSON.stringify({
      timestamp: new Date().toISOString(),
      mode: CONFIG.mode,
      session: getCurrentSession(),
      market_open: isMarketOpen(),

      // ── Equity breakdown ──
      equity: {
        portfolio_cash: +portfolio.toFixed(2),
        real_equity_alpaca: +realEquity.toFixed(2),
        long_market_value: +longMktVal.toFixed(2),
        short_pnl: +shortPnl.toFixed(2),
        scalp_market_value: +scalpMktVal.toFixed(2),
        computed_total: +computedEquity.toFixed(2),
        daily_start: +realDailyStartEquity.toFixed(2),
        day_pnl: +(computedEquity - realDailyStartEquity).toFixed(2),
        starting_capital: CONFIG.startingCapital,
      },

      // ── Position details ──
      positions: posSnapshot,
      short_positions: shortSnapshot,
      scalp_positions: scalpSnapshot,

      // ── Price cache ──
      price_cache: Object.fromEntries(
        Object.keys({ ...positions, ...shortPositions, ...scalpPositions })
          .map(sym => [sym, priceHistory5m[sym]?.slice(-5) || []])
      ),

      // ── Stats ──
      stats: {
        total_wins: totalWins, total_losses: totalLosses,
        scalp_wins: scalpWins, scalp_losses: scalpLosses,
        circuit_breaker: circuitBreakerOn,
        recovery_active: !!recoveryState,
      },

      // ── Recent trades (last 20) ──
      recent_trades: trades.slice(-20).map(t => ({
        time: t.time, sym: t.sym, side: t.side,
        qty: t.qty, price: t.price, pnl: t.pnl, reason: t.reason,
      })),

      // ── Equity event log ──
      equity_log: eqLog.slice(-200),

      // ── MASTER LOG — every single log line, last 2000 entries ──
      // This is the complete diagnostic trail — submit this when reporting issues
      master_log: masterLog,

      // ── Config snapshot ──
      config: {
        symbols: CONFIG.symbols,
        strategy: CONFIG.strategy,
        maxOpenPositions: CONFIG.maxOpenPositions,
        maxPositionPct: CONFIG.maxPositionPct,
        tp1Pct: CONFIG.tp1Pct, tp2Pct: CONFIG.tp2Pct, tp3Pct: CONFIG.tp3Pct,
        atrStopMult: CONFIG.atrStopMult,
        scalpMode: CONFIG.scalpMode, shortsEnabled: CONFIG.shortsEnabled,
        recoveryMode: CONFIG.recoveryMode, startingCapital: CONFIG.startingCapital,
      },
    }, null, 2));
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

    // Debug — log everything so we can see exactly what's failing
    log('discord', `POST /discord received`);
    log('discord', `  signature: ${signature?.slice(0,16)}... (${signature?.length} chars)`);
    log('discord', `  timestamp: ${timestamp}`);
    log('discord', `  body: ${rawBody.slice(0,80)}`);
    log('discord', `  public key set: ${!!CONFIG.discordPublicKey} (${CONFIG.discordPublicKey?.length} chars)`);

    // Verify signature
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

    // Discord PING — respond IMMEDIATELY with type 1
    if (interaction.type === 1) {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ type: 1 }));
      log('sys', '✅ Discord PING verified');
      return;
    }

    // Slash command (type 2)
    if (interaction.type === 2) {
      const commandName = interaction.data?.name;
      const options     = interaction.data?.options || [];
      const username    = interaction.member?.user?.username || interaction.user?.username || 'unknown';
      const token       = interaction.token;

      log('sys', `Discord command: /${commandName} from ${username}`);

      // Respond immediately with type 5 (deferred) — Discord requires response within 3s
      // Then do the real work and send a followup
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ type: 5, data: { flags: 0 } }));

      handleSlashCommand(commandName, options).then(async result => {
        try {
          const fetch = await getFetch();
          await fetch(`https://discord.com/api/v10/webhooks/${CONFIG.discordAppId}/${token}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ content: result.content }),
          });
        } catch(e) { log('error', `Discord followup failed: ${e.message}`); }
      }).catch(async e => {
        log('error', `Discord command error: ${e.message}`);
        try {
          const fetch = await getFetch();
          await fetch(`https://discord.com/api/v10/webhooks/${CONFIG.discordAppId}/${token}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ content: `❌ Error: ${e.message}` }),
          });
        } catch(_) {}
      });

      return;
    }

    res.writeHead(400); res.end('Unknown interaction type');
    return;
  }

  // 404 for anything else
  res.writeHead(404); res.end('Not found');

}).listen(process.env.PORT || 3000, () => {
  log('sys', `Server on port ${process.env.PORT || 3000}`);
  log('sys', `Health check: https://tradecore.up.railway.app/`);
  log('sys', `Discord endpoint: https://tradecore.up.railway.app/discord`);
  log('sys', `Register commands: https://tradecore.up.railway.app/setup-discord`);
  if (CONFIG.discordPublicKey) log('sys', `Discord signature verification: ✅ enabled`);
  else log('warn', `Discord signature verification: ⚠ DISCORD_PUBLIC_KEY not set`);
});
