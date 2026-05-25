/* AlphaCore — Equity Chart (candlestick) + Stock Chart */

// ── EquityChart ──────────────────────────────────────────────────────────────
class EquityChart {
  constructor(containerId) {
    this.container = document.getElementById(containerId);
    if (!this.container || !window.LightweightCharts) return;
    this._init();
  }

  _init() {
    this.chart = LightweightCharts.createChart(this.container, {
      layout:     { background: { color: 'transparent' }, textColor: '#9b9ea4' },
      grid:       { vertLines: { color: '#1e2130' }, horzLines: { color: '#1e2130' } },
      crosshair:  { mode: LightweightCharts.CrosshairMode.Normal },
      rightPriceScale: { borderColor: '#2a2e39', scaleMargins: { top: 0.1, bottom: 0.1 } },
      timeScale:  { borderColor: '#2a2e39', timeVisible: true, secondsVisible: false },
      handleScale: { axisPressedMouseMove: true },
      handleScroll: true,
    });

    this.series = this.chart.addCandlestickSeries({
      upColor:         '#2962ff',
      downColor:       '#131722',
      borderUpColor:   '#2962ff',
      borderDownColor: '#ef5350',
      wickUpColor:     '#2962ff',
      wickDownColor:   '#ef5350',
      priceFormat:     { type: 'price', precision: 2, minMove: 0.01 },
    });

    this.ema = this.chart.addLineSeries({
      color:            'rgba(245,197,24,0.7)',
      lineWidth:        1,
      lineStyle:        1,
      priceLineVisible: false,
      lastValueVisible: false,
    });

    const ro = new ResizeObserver(() => {
      if (this.container) this.chart.resize(this.container.clientWidth, this.container.clientHeight);
    });
    ro.observe(this.container);
  }

  setData(candles) {
    if (!this.series || !candles?.length) return;
    const seen = new Set();
    const data = candles.map(c => {
      const t = typeof c.time === 'number' ? c.time
              : Math.floor((c.time instanceof Date ? c.time.getTime() : +c.time) / 1000);
      const close = +c.close || +c.value || 0;
      const open  = +c.open  || close;
      const high  = +c.high  || Math.max(open, close);
      const low   = +c.low   || Math.min(open, close);
      return {
        time:  t,
        open,
        high:  high === low ? high * 1.00005 : high,
        low:   high === low ? low  * 0.99995 : low,
        close,
      };
    })
    .filter(c => isFinite(c.close) && c.close > 0 && c.time > 0 && !seen.has(c.time) && seen.add(c.time))
    .sort((a, b) => a.time - b.time);

    if (!data.length) return;
    try { this.series.setData(data); } catch(e) { console.warn('setData:', e); }

    try {
      const k = 2 / 21;
      let e = data[0].close;
      this.ema.setData(data.map((d, i) => {
        e = i === 0 ? d.close : d.close * k + e * (1 - k);
        return { time: d.time, value: e };
      }));
    } catch(e) {}

    this.chart.timeScale().fitContent();
  }

  update(c) {
    if (!this.series) return;
    try {
      const t = typeof c.time === 'number' ? c.time
              : Math.floor((c.time instanceof Date ? c.time.getTime() : +c.time) / 1000);
      const close = +c.close || +c.value || 0;
      const open  = +c.open  || close;
      const high  = +c.high  || Math.max(open, close);
      const low   = +c.low   || Math.min(open, close);
      if (!isFinite(close) || close <= 0) return;
      this.series.update({
        time:  t,
        open,
        high:  high === low ? high * 1.00005 : high,
        low:   high === low ? low  * 0.99995 : low,
        close,
      });
    } catch(e) {}
  }

  fitAll() {
    try { this.chart?.timeScale().fitContent(); } catch(e) {}
  }
}

// ── StockChart ───────────────────────────────────────────────────────────────
class StockChart {
  constructor(containerId) {
    this.container = document.getElementById(containerId);
    this.chart  = null;
    this.series = null;
    if (this.container && window.LightweightCharts) this._init();
  }

  _init() {
    this.chart = LightweightCharts.createChart(this.container, {
      layout:     { background: { color: 'transparent' }, textColor: '#9b9ea4' },
      grid:       { vertLines: { color: '#1e2130' }, horzLines: { color: '#1e2130' } },
      crosshair:  { mode: LightweightCharts.CrosshairMode.Normal },
      rightPriceScale: { borderColor: '#2a2e39' },
      timeScale:  { borderColor: '#2a2e39', timeVisible: true },
      handleScale: true,
      handleScroll: true,
    });
    this.series = this.chart.addCandlestickSeries({
      upColor: '#26a69a', downColor: '#ef5350',
      borderUpColor: '#26a69a', borderDownColor: '#ef5350',
      wickUpColor: '#26a69a', wickDownColor: '#ef5350',
    });
    const ro = new ResizeObserver(() => {
      if (this.container) this.chart.resize(this.container.clientWidth, this.container.clientHeight);
    });
    ro.observe(this.container);
  }

  async load(sym, botUrl) {
    if (!this.series) return;
    try {
      const r = await fetch(`${botUrl}/chart/${sym}`);
      const bars = await r.json();
      if (!bars?.length) return;
      const seen = new Set();
      const data = bars.map(b => {
        const t = typeof b.t === 'string' ? Math.floor(new Date(b.t).getTime() / 1000) : +b.t;
        return { time: t, open: +b.o, high: +b.h, low: +b.l, close: +b.c };
      })
      .filter(b => isFinite(b.close) && b.close > 0 && b.time > 0 && !seen.has(b.time) && seen.add(b.time))
      .sort((a, b) => a.time - b.time);
      this.series.setData(data);
      this.chart.timeScale().fitContent();
    } catch(e) { console.warn('StockChart load:', e); }
  }
}
