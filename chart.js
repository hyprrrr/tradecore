/**
 * AlphaCore Chart Engine
 * Uses LightweightCharts for the equity curve + stock charts
 * Loads from CDN, falls back gracefully
 */

// ─── Load LightweightCharts from CDN ──────────────────────────────────────
(function() {
  if (window.LightweightCharts) return;
  const s = document.createElement('script');
  s.src = 'https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js';
  s.onload = () => { if (window._lwcReady) window._lwcReady(); };
  document.head.appendChild(s);
})();

// ─── Equity Curve Chart ───────────────────────────────────────────────────
class EquityChart {
  constructor(containerId) {
    this.id        = containerId;
    this.container = document.getElementById(containerId);
    this.chart     = null;
    this.series    = null;
    this.ema       = null;
    this._pending  = null;

    if (window.LightweightCharts) {
      this._init();
    } else {
      window._lwcReady = () => this._init();
    }
  }

  _init() {
    const el = this.container;
    if (!el) return;

    // Make container fill its space
    el.style.cssText += ';width:100%;height:100%;min-height:300px;display:block;';

    const w = el.offsetWidth  || el.parentElement?.offsetWidth  || 900;
    const h = el.offsetHeight || el.parentElement?.offsetHeight || 420;

    this.chart = LightweightCharts.createChart(el, {
      width:  w,
      height: Math.max(h, 300),
      layout: {
        background: { type: 'solid', color: 'transparent' },
        textColor:  'rgba(255,255,255,0.4)',
        fontSize:   11,
      },
      grid: {
        vertLines: { color: 'rgba(255,255,255,0.04)' },
        horzLines: { color: 'rgba(255,255,255,0.04)' },
      },
      crosshair: {
        mode: LightweightCharts.CrosshairMode.Normal,
        vertLine: { color: 'rgba(255,255,255,0.25)', labelBackgroundColor: '#1e2130' },
        horzLine: { color: 'rgba(255,255,255,0.25)', labelBackgroundColor: '#1e2130' },
      },
      rightPriceScale: {
        borderColor:  'rgba(255,255,255,0.07)',
        scaleMargins: { top: 0.08, bottom: 0.08 },
      },
      timeScale: {
        borderColor:    'rgba(255,255,255,0.07)',
        timeVisible:    true,
        secondsVisible: false,
        fixLeftEdge:    true,
        fixRightEdge:   true,
      },
      handleScroll: true,
      handleScale:  true,
    });

    this.series = this.chart.addCandlestickSeries({
      upColor:         '#2962ff',
      downColor:       '#131722',
      borderUpColor:   '#2962ff',
      borderDownColor: '#2962ff',
      wickUpColor:     '#2962ff',
      wickDownColor:   '#2962ff',
    });

    this.ema = this.chart.addLineSeries({
      color:            'rgba(245,197,24,0.8)',
      lineWidth:        1.5,
      priceLineVisible: false,
      lastValueVisible: false,
    });

    // Auto-resize
    const ro = new ResizeObserver(() => {
      const nw = el.offsetWidth  || el.parentElement?.offsetWidth  || 900;
      const nh = el.offsetHeight || el.parentElement?.offsetHeight || 420;
      if (nw > 10 && nh > 10) this.chart.resize(nw, Math.max(nh, 300));
    });
    ro.observe(el);
    if (el.parentElement) ro.observe(el.parentElement);

    // Flush pending data
    if (this._pending) { this.setData(this._pending); this._pending = null; }
  }

  setData(candles) {
    if (!candles?.length) return;
    if (!this.series) { this._pending = candles; return; }

    const seen = new Set();
    const data = candles
      .map(c => ({
        time:  Math.floor((c.time instanceof Date ? c.time.getTime() : +c.time) / 1000),
        open:  +c.open, high: +c.high, low: +c.low, close: +c.close,
      }))
      .filter(c => isFinite(c.close) && c.time > 0 && !seen.has(c.time) && seen.add(c.time))
      .sort((a,b) => a.time - b.time);

    if (!data.length) return;
    this.series.setData(data);

    // EMA(20)
    const k = 2/21;
    let e = data[0].close;
    const emaData = data.map((d,i) => {
      e = i === 0 ? d.close : d.close*k + e*(1-k);
      return { time: d.time, value: e };
    });
    this.ema.setData(emaData);
    this.chart.timeScale().scrollToRealTime();
  }

  update(c) {
    if (!this.series) return;
    try {
      const t = Math.floor((c.time instanceof Date ? c.time.getTime() : +c.time) / 1000);
      this.series.update({ time:t, open:+c.open, high:+c.high, low:+c.low, close:+c.close });
    } catch(e) {}
  }
}

// ─── Stock Chart (for position click) ────────────────────────────────────
class StockChart {
  constructor(containerId) {
    this.id        = containerId;
    this.container = document.getElementById(containerId);
    this.chart     = null;
    this.series    = null;
    this.sym       = null;

    if (window.LightweightCharts) this._init();
    else window._lwcReady = () => this._init();
  }

  _init() {
    const el = this.container;
    if (!el) return;
    el.style.cssText += ';width:100%;height:100%;display:block;';

    this.chart = LightweightCharts.createChart(el, {
      layout: {
        background: { type: 'solid', color: 'transparent' },
        textColor:  'rgba(255,255,255,0.4)',
        fontSize:   11,
      },
      grid: {
        vertLines: { color: 'rgba(255,255,255,0.04)' },
        horzLines: { color: 'rgba(255,255,255,0.04)' },
      },
      crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
      rightPriceScale: { borderColor: 'rgba(255,255,255,0.07)' },
      timeScale: { borderColor: 'rgba(255,255,255,0.07)', timeVisible: true, secondsVisible: false },
      handleScroll: true,
      handleScale:  true,
    });

    this.series = this.chart.addCandlestickSeries({
      upColor: '#2962ff', downColor: '#131722',
      borderUpColor: '#2962ff', borderDownColor: '#2962ff',
      wickUpColor: '#2962ff', wickDownColor: '#2962ff',
    });

    const ro = new ResizeObserver(() => {
      const nw = el.offsetWidth || 600, nh = el.offsetHeight || 400;
      if (nw > 10 && nh > 10) this.chart.resize(nw, nh);
    });
    ro.observe(el);
  }

  async loadSymbol(sym) {
    if (!this.series || !sym) return;
    this.sym = sym;
    try {
      const res  = await fetch(`https://query2.finance.yahoo.com/v8/finance/chart/${sym}?interval=5m&range=1d`, {
        headers: { 'User-Agent': 'Mozilla/5.0' }
      });
      const json = await res.json();
      const r    = json?.chart?.result?.[0];
      if (!r) return;
      const ts = r.timestamp || [];
      const q  = r.indicators?.quote?.[0] || {};
      const data = ts.map((t,i) => ({
        time:  t,
        open:  q.open?.[i], high: q.high?.[i],
        low:   q.low?.[i],  close: q.close?.[i],
      })).filter(d => isFinite(d.close) && isFinite(d.open));

      if (!data.length) return;
      this.series.setData(data);
      this.chart.timeScale().scrollToRealTime();
    } catch(e) { console.warn('StockChart load failed:', sym, e); }
  }
}

// ─── Exports ──────────────────────────────────────────────────────────────
window.EquityChart = EquityChart;
window.StockChart  = StockChart;
