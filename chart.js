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

    // Equity curve: area chart — clean, professional, shows full history
    this.series = this.chart.addAreaSeries({
      lineColor:              '#2962ff',
      lineWidth:              2,
      topColor:               'rgba(41,98,255,0.25)',
      bottomColor:            'rgba(41,98,255,0.02)',
      priceLineColor:         '#2962ff',
      priceLineWidth:         1,
      priceLineStyle:         1, // dashed
      lastValueVisible:       true,
      crosshairMarkerVisible: true,
      crosshairMarkerRadius:  5,
      crosshairMarkerBackgroundColor: '#2962ff',
      priceFormat: { type: 'price', precision: 2, minMove: 0.01 },
    });

    this.ema = this.chart.addLineSeries({
      color:            'rgba(245,197,24,0.7)',
      lineWidth:        1,
      priceLineVisible: false,
      lastValueVisible: false,
      lineStyle:        1, // 1 = Dashed
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
        time:  typeof c.time === 'number' ? c.time
               : Math.floor((c.time instanceof Date ? c.time.getTime() : +c.time) / 1000),
        value: +c.close || +c.value || 0,
      }))
      .filter(c => isFinite(c.value) && c.value > 0 && c.time > 0 && !seen.has(c.time) && seen.add(c.time))
      .sort((a,b) => a.time - b.time);

    if (!data.length) return;
    this.series.setData(data);

    // EMA(20)
    const k = 2/21;
    let e = data[0].value;
    const emaData = data.map((d,i) => {
      e = i === 0 ? d.value : d.value*k + e*(1-k);
      return { time: d.time, value: e };
    });
    this.ema.setData(emaData);
    this.chart.timeScale().fitContent();
  }

  update(c) {
    if (!this.series) return;
    try {
      const t = typeof c.time === 'number' ? c.time
                : Math.floor((c.time instanceof Date ? c.time.getTime() : +c.time) / 1000);
      const v = +c.close || +c.value || 0;
      if (!isFinite(v) || v <= 0) return;
      this.series.update({ time: t, value: v });
    } catch(e) {}
  }

  /** Force chart to show all data */
  fitAll() {
    try { this.chart?.timeScale().fitContent(); } catch(e) {}
  }
}

// ─── Stock Chart (for position click) ────────────────────────────────────
class StockChart {
  constructor(containerId) {
    this.id          = containerId;
    this.container   = document.getElementById(containerId);
    this.chart       = null;
    this.series      = null;
    this.sym         = null;
    this._entryPrice = null;
    // Don't init yet — container is inside a hidden modal (dimensions = 0)
    // _init() will be called by loadSymbol once modal is visible
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
    if (!sym) return;
    this.sym = sym;

    // If chart not ready yet, wait for it (modal was hidden during init)
    if (!this.series) {
      await new Promise(resolve => {
        let attempts = 0;
        const wait = setInterval(() => {
          if (!this.chart && window.LightweightCharts) this._init();
          if (this.series || ++attempts > 30) { clearInterval(wait); resolve(); }
        }, 100);
      });
    }
    if (!this.series) return;

    // Clear previous data and price lines before loading new symbol
    this.series.setData([]);
    if (this._priceLines) {
      this._priceLines.forEach(pl => { try { this.series.removePriceLine(pl); } catch(e){} });
    }
    this._priceLines = [];

    // Resize now that container is visible
    const el = this.container;
    if (el) {
      const w = el.offsetWidth || 800, h = el.offsetHeight || 400;
      if (w > 10 && h > 10) this.chart.resize(w, h);
    }

    try {
      // Always use absolute Render URL — never relative
      const _rawBotUrl = window.ALPHACORE_BOT_URL || '';
      const botUrl = _rawBotUrl.startsWith('http')
        ? _rawBotUrl.replace(/\/+$/, '')
        : 'https://tradecore-cyvw.onrender.com';

      const yUrl = `https://query2.finance.yahoo.com/v8/finance/chart/${sym}?interval=5m&range=1d`;
      let json   = null;

      // 1. Bot proxy (own server, most reliable once deployed)
      try {
        const u = `${botUrl}/chart/${sym}`;
        if (u.startsWith('https://')) { // safety: never call relative URL
          const r1 = await fetch(u, {signal: AbortSignal.timeout(8000)});
          if (r1.ok) { const d = await r1.json(); if (d?.chart?.result?.[0]) json = d; }
        }
      } catch(e) {}

      // 2. allorigins (most reliable public CORS proxy)
      if (!json) {
        try {
          const r2 = await fetch(
            `https://api.allorigins.win/get?url=${encodeURIComponent(yUrl)}`,
            {signal: AbortSignal.timeout(10000)}
          );
          if (r2.ok) {
            const w = await r2.json();
            const d = w?.contents ? JSON.parse(w.contents) : w;
            if (d?.chart?.result?.[0]) json = d;
          }
        } catch(e) {}
      }

      // 3. cors-anywhere on herokuapp (requires no-cors header trick)
      if (!json) {
        try {
          const r3 = await fetch(
            `https://cors-anywhere.herokuapp.com/${yUrl}`,
            {headers: {'X-Requested-With': 'XMLHttpRequest'}, signal: AbortSignal.timeout(10000)}
          );
          if (r3.ok) { const d = await r3.json(); if (d?.chart?.result?.[0]) json = d; }
        } catch(e) {}
      }

      if (!json) throw new Error('Chart data unavailable — proxies blocked');
      const r = json?.chart?.result?.[0];
      if (!r) return;
      const ts = r.timestamp || [];
      const q  = r.indicators?.quote?.[0] || {};
      const data = ts.map((t,i) => ({
        time:  t,
        open:  +(q.open?.[i]  || 0),
        high:  +(q.high?.[i]  || 0),
        low:   +(q.low?.[i]   || 0),
        close: +(q.close?.[i] || 0),
      })).filter(d => d.close > 0 && d.open > 0 && d.high > 0 && d.low > 0);

      if (!data.length) return;
      this.series.setData(data);
      this.chart.timeScale().fitContent();

      // Live price updates — update last candle every 5s
      if (this._liveInterval) clearInterval(this._liveInterval);
      this._liveData = data;
      this._liveSym  = sym;
      this._liveInterval = setInterval(async () => {
        if (this._liveSym !== sym) { clearInterval(this._liveInterval); return; }
        try {
          const botUrl = (window.ALPHACORE_BOT_URL || 'https://tradecore-cyvw.onrender.com').replace(/\/+$/, '');
          // Use /prices endpoint for current price
          const pr = await fetch(`${botUrl}/prices`, {signal: AbortSignal.timeout(3000)});
          if (!pr.ok) return;
          const prData = await pr.json();
          const livePrice = prData?.prices?.[sym];
          if (!livePrice || !this._liveData?.length) return;
          const last = { ...this._liveData[this._liveData.length - 1] };
          last.close = livePrice;
          if (livePrice > last.high) last.high = livePrice;
          if (livePrice < last.low)  last.low  = livePrice;
          try { this.series.update(last); } catch(e) {}
        } catch(e) {}
      }, 5000);

      // Add entry price line (tracked so it can be removed on next load)
      if (this._entryPrice) {
        try {
          const pl = this.series.createPriceLine({
            price:            this._entryPrice,
            color:            '#05d890',
            lineWidth:        1,
            lineStyle:        LightweightCharts.LineStyle.Dashed,
            axisLabelVisible: true,
            title:            'Entry',
          });
          if (!this._priceLines) this._priceLines = [];
          this._priceLines.push(pl);
        } catch(e) {}
      }
    } catch(e) { console.warn('StockChart load failed:', sym, e); }
  }

  setEntryPrice(price) {
    this._entryPrice = price;
  }

  stop() {
    if (this._liveInterval) { clearInterval(this._liveInterval); this._liveInterval = null; }
    this._liveSym = null;
  }
}

// ─── Exports ──────────────────────────────────────────────────────────────
window.EquityChart = EquityChart;
window.StockChart  = StockChart;
