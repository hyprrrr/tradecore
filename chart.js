/**
 * AlphaCore Chart Engine v2
 * Hardware-accelerated canvas chart — TV style
 * Usage: new AlphaCoreChart('containerId')
 */
(function(global) {
'use strict';

const C = {
  grid:       'rgba(255,255,255,0.05)',
  text:       'rgba(255,255,255,0.35)',
  bullBody:   '#2962ff',
  bullWick:   '#2962ff',
  bearBody:   '#0d1117',
  bearBorder: '#2962ff',
  bearWick:   '#2962ff',
  ema:        '#f5c518',
  vwap:       '#9b59b6',
  cross:      'rgba(255,255,255,0.3)',
  gradTop:    'rgba(41,98,255,0.15)',
  gradBot:    'rgba(41,98,255,0.00)',
};

class AlphaCoreChart {
  constructor(containerId) {
    this.container = document.getElementById(containerId);
    if (!this.container) { console.error('AlphaCoreChart: no container', containerId); return; }

    this.container.style.cssText += ';position:relative;overflow:hidden;';

    // Main canvas
    this.canvas = document.createElement('canvas');
    this.canvas.style.cssText = 'position:absolute;inset:0;width:100%;height:100%;display:block;cursor:crosshair;';
    this.container.appendChild(this.canvas);
    this.ctx = this.canvas.getContext('2d');

    // Overlay canvas for crosshair (separate layer = no full redraw on mouse move)
    this.ov = document.createElement('canvas');
    this.ov.style.cssText = 'position:absolute;inset:0;width:100%;height:100%;pointer-events:none;';
    this.container.appendChild(this.ov);
    this.octx = this.ov.getContext('2d');

    this.candles   = [];
    this.viewStart = 0;
    this.viewCount = 80;
    this.pad       = {t:10, r:75, b:28, l:0};
    this._dirty    = false;

    this._resize();
    this._bindResize();
    this._bindEvents();
  }

  // ─── Public ───────────────────────────────────────────────────────────────

  setData(candles) {
    this.candles = (candles || []).filter(c => isFinite(+c.close));
    const n = this.candles.length;
    this.viewCount = Math.min(Math.max(10, this.viewCount), n);
    this.viewStart = Math.max(0, n - this.viewCount);
    this._draw();
  }

  update(candle) {
    if (!this.candles.length) { this.setData([candle]); return; }
    const last = this.candles[this.candles.length - 1];
    const t1 = Math.floor(+(candle.time instanceof Date ? candle.time : new Date(candle.time)) / 60000);
    const t2 = Math.floor(+(last.time instanceof Date ? last.time : new Date(last.time)) / 60000);
    const atEdge = this.viewStart + this.viewCount >= this.candles.length;
    if (t1 === t2) {
      last.close = +candle.close;
      if (+candle.close > last.high) last.high = +candle.close;
      if (+candle.close < last.low)  last.low  = +candle.close;
    } else {
      this.candles.push({...candle, open:+candle.open, high:+candle.high, low:+candle.low, close:+candle.close});
      if (atEdge) this.viewStart = Math.max(0, this.candles.length - this.viewCount);
    }
    this._draw();
  }

  // ─── Drawing ──────────────────────────────────────────────────────────────

  _draw() {
    if (this._dirty) return;
    this._dirty = true;
    requestAnimationFrame(() => { this._dirty = false; this._render(); });
  }

  _render() {
    const {ctx, ov, canvas, pad} = this;
    const W = canvas.width, H = canvas.height;
    if (!W || !H) return;

    const cw = W - pad.l - pad.r;
    const ch = H - pad.t - pad.b;

    ctx.clearRect(0, 0, W, H);

    const vis = this._vis();
    if (!vis.length) return;

    const {lo, hi} = this._range(vis);
    const yRng = hi - lo || 1;
    const dpr  = window.devicePixelRatio || 1;

    // Coordinate helpers
    const xOf = i  => pad.l + (i + 0.5) * (cw / vis.length);
    const yOf = p  => pad.t + ch - ((p - lo) / yRng) * ch;

    this._grid(ctx, W, H, cw, ch, lo, hi, yRng, dpr);
    this._gradient(ctx, vis, cw, ch, xOf, yOf, pad);
    this._ema(ctx, vis, xOf, yOf);
    this._candles(ctx, vis, cw, xOf, yOf, dpr);
    this._priceLabel(ctx, vis, W, H, cw, ch, lo, yRng, xOf, yOf, pad, dpr);
    this._timeAxis(ctx, vis, W, H, cw, pad, dpr);
  }

  _vis() {
    if (!this.candles.length) return [];
    const s = Math.max(0, Math.min(this.viewStart, this.candles.length - 1));
    const n = Math.max(1, Math.min(this.viewCount, this.candles.length - s));
    return this.candles.slice(s, s + n);
  }

  _range(vis) {
    let lo = Infinity, hi = -Infinity;
    vis.forEach(d => {
      if (d.low  < lo) lo = d.low;
      if (d.high > hi) hi = d.high;
    });
    if (!isFinite(lo) || !isFinite(hi)) return {lo: 0, hi: 1};
    const rng = Math.max(hi - lo, hi * 0.003);
    const mid = (lo + hi) / 2;
    return {lo: mid - rng * 0.72, hi: mid + rng * 0.72};
  }

  _grid(ctx, W, H, cw, ch, lo, hi, yRng, dpr) {
    ctx.save();
    const steps = 6;
    ctx.font = `${10 * dpr}px "DM Mono","DM Sans",monospace`;
    ctx.textAlign = 'left';
    ctx.lineWidth = dpr;
    for (let i = 0; i <= steps; i++) {
      const y = this.pad.t + (ch / steps) * i;
      const price = hi - (i / steps) * yRng;
      ctx.strokeStyle = C.grid;
      ctx.setLineDash([]);
      ctx.beginPath(); ctx.moveTo(this.pad.l, y); ctx.lineTo(W - this.pad.r, y); ctx.stroke();
      ctx.fillStyle = C.text;
      ctx.fillText('$' + this._fmt(price), W - this.pad.r + 4 * dpr, y + 3.5 * dpr);
    }
    ctx.restore();
  }

  _gradient(ctx, vis, cw, ch, xOf, yOf, pad) {
    if (vis.length < 2) return;
    const grad = ctx.createLinearGradient(0, pad.t, 0, pad.t + ch);
    grad.addColorStop(0, C.gradTop);
    grad.addColorStop(1, C.gradBot);
    ctx.save();
    ctx.fillStyle = grad;
    ctx.beginPath();
    ctx.moveTo(xOf(0), yOf(vis[0].close));
    vis.forEach((d, i) => { if (i) ctx.lineTo(xOf(i), yOf(d.close)); });
    ctx.lineTo(xOf(vis.length - 1), pad.t + ch);
    ctx.lineTo(xOf(0), pad.t + ch);
    ctx.closePath();
    ctx.fill();
    ctx.restore();
  }

  _ema(ctx, vis, xOf, yOf) {
    if (vis.length < 5) return;
    const k = 2 / 21;
    let e = vis[0].close;
    ctx.save();
    ctx.strokeStyle = C.ema;
    ctx.lineWidth   = 1.5;
    ctx.setLineDash([]);
    ctx.beginPath();
    vis.forEach((d, i) => {
      e = i === 0 ? d.close : d.close * k + e * (1 - k);
      i === 0 ? ctx.moveTo(xOf(i), yOf(e)) : ctx.lineTo(xOf(i), yOf(e));
    });
    ctx.stroke();
    ctx.restore();
  }

  _candles(ctx, vis, cw, xOf, yOf, dpr) {
    const slotW = cw / Math.max(vis.length, 1);
    const bw    = Math.max(1.5 * dpr, Math.min(20 * dpr, slotW * 0.6));
    const half  = bw / 2;

    ctx.save();
    vis.forEach((d, i) => {
      const x   = xOf(i);
      const up  = d.close >= d.open;
      const yH  = yOf(d.high);
      const yL  = yOf(d.low);
      const yO  = yOf(d.open);
      const yC  = yOf(d.close);
      const top = Math.min(yO, yC);
      const bh  = Math.max(Math.abs(yC - yO), 1.5 * dpr);

      // Wick
      ctx.strokeStyle = C.bullWick;
      ctx.lineWidth   = Math.max(dpr, 1);
      ctx.beginPath(); ctx.moveTo(x, yH); ctx.lineTo(x, yL); ctx.stroke();

      // Body
      if (up) {
        ctx.fillStyle = C.bullBody;
        ctx.fillRect(x - half, top, bw, bh);
      } else {
        ctx.fillStyle = C.bearBody;
        ctx.fillRect(x - half, top, bw, bh);
        ctx.strokeStyle = C.bearBorder;
        ctx.lineWidth = dpr;
        ctx.strokeRect(x - half, top, bw, bh);
      }
    });
    ctx.restore();
  }

  _priceLabel(ctx, vis, W, H, cw, ch, lo, yRng, xOf, yOf, pad, dpr) {
    const last = vis[vis.length - 1];
    if (!last) return;
    const up  = last.close >= last.open;
    const y   = Math.max(pad.t + 8 * dpr, Math.min(pad.t + ch - 8 * dpr, yOf(last.close)));

    // Dashed reference line
    ctx.save();
    ctx.strokeStyle = up ? C.bullBody : '#ef5350';
    ctx.lineWidth   = dpr;
    ctx.setLineDash([4 * dpr, 3 * dpr]);
    ctx.beginPath(); ctx.moveTo(pad.l, y); ctx.lineTo(W - pad.r, y); ctx.stroke();
    ctx.setLineDash([]);

    // Price box
    const lbl  = this._fmt(last.close);
    const lbW  = 70 * dpr;
    const lbH  = 18 * dpr;
    const lx   = W - pad.r + 2 * dpr;
    ctx.fillStyle = up ? C.bullBody : '#ef5350';
    ctx.beginPath();
    ctx.roundRect ? ctx.roundRect(lx, y - lbH / 2, lbW, lbH, 4 * dpr)
                  : ctx.rect(lx, y - lbH / 2, lbW, lbH);
    ctx.fill();
    ctx.fillStyle = '#fff';
    ctx.font      = `bold ${10 * dpr}px "DM Mono","DM Sans",monospace`;
    ctx.textAlign = 'center';
    ctx.fillText(lbl, lx + lbW / 2, y + 3.5 * dpr);
    ctx.restore();
  }

  _timeAxis(ctx, vis, W, H, cw, pad, dpr) {
    if (vis.length < 2) return;
    const step   = Math.ceil(vis.length / 6);
    const slotW  = cw / vis.length;
    ctx.save();
    ctx.fillStyle = C.text;
    ctx.font      = `${9.5 * dpr}px "DM Sans",system-ui,sans-serif`;
    ctx.textAlign = 'center';
    vis.forEach((d, i) => {
      if (i % step !== 0) return;
      const t   = d.time instanceof Date ? d.time : new Date(+d.time);
      const lbl = t.toLocaleTimeString([], {hour:'2-digit', minute:'2-digit'});
      ctx.fillText(lbl, pad.l + (i + 0.5) * slotW, H - 6 * dpr);
    });
    ctx.restore();
  }

  // ─── Crosshair ────────────────────────────────────────────────────────────

  _crosshair(ex, ey) {
    const {ov, octx, canvas, pad} = this;
    const W = canvas.width, H = canvas.height;
    const cw = W - pad.l - pad.r;
    const ch = H - pad.t - pad.b;
    const dpr = window.devicePixelRatio || 1;
    // Convert event coords to canvas coords
    const rect = canvas.getBoundingClientRect();
    const sx = canvas.width  / rect.width;
    const sy = canvas.height / rect.height;
    const mx = (ex - rect.left) * sx;
    const my = (ey - rect.top)  * sy;

    octx.clearRect(0, 0, W, H);
    if (mx < pad.l || mx > W - pad.r || my < pad.t || my > pad.t + ch) return;

    const vis = this._vis();
    if (!vis.length) return;
    const {lo, hi} = this._range(vis);
    const yRng = hi - lo || 1;
    const slotW = cw / vis.length;
    const ci    = Math.max(0, Math.min(vis.length - 1, Math.floor((mx - pad.l) / slotW)));
    const d     = vis[ci];
    const cx    = pad.l + (ci + 0.5) * slotW;

    // Lines
    octx.save();
    octx.strokeStyle = C.cross;
    octx.lineWidth   = dpr;
    octx.setLineDash([4 * dpr, 4 * dpr]);
    octx.beginPath(); octx.moveTo(cx, pad.t); octx.lineTo(cx, pad.t + ch); octx.stroke();
    octx.beginPath(); octx.moveTo(pad.l, my);  octx.lineTo(W - pad.r, my);  octx.stroke();
    octx.setLineDash([]);
    octx.restore();

    // Tooltip
    const chg  = d.close - d.open;
    const up   = chg >= 0;
    const t    = d.time instanceof Date ? d.time : new Date(+d.time);
    const rows = [
      t.toLocaleString([], {month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}),
      `O: ${this._fmt(d.open)}   H: ${this._fmt(d.high)}`,
      `L: ${this._fmt(d.low)}    C: ${this._fmt(d.close)}`,
      `${chg >= 0 ? '+' : ''}${this._fmt(chg)}`,
    ];
    const tw = 175 * dpr, th = 68 * dpr;
    let tx = cx + 10 * dpr;
    let ty = my - 10 * dpr - th;
    if (tx + tw > W - pad.r) tx = cx - tw - 10 * dpr;
    if (ty < pad.t) ty = my + 10 * dpr;

    octx.save();
    octx.fillStyle = 'rgba(8,8,18,0.92)';
    octx.shadowColor = 'rgba(0,0,0,0.5)';
    octx.shadowBlur = 10 * dpr;
    octx.beginPath();
    if (octx.roundRect) octx.roundRect(tx, ty, tw, th, 6 * dpr);
    else octx.rect(tx, ty, tw, th);
    octx.fill();
    octx.shadowBlur = 0;
    octx.strokeStyle = 'rgba(255,255,255,0.1)';
    octx.lineWidth = dpr;
    octx.beginPath();
    if (octx.roundRect) octx.roundRect(tx, ty, tw, th, 6 * dpr);
    else octx.rect(tx, ty, tw, th);
    octx.stroke();
    octx.font = `${10 * dpr}px "DM Mono","DM Sans",monospace`;
    octx.textAlign = 'left';
    const colors = ['rgba(255,255,255,0.5)', 'rgba(255,255,255,0.8)', 'rgba(255,255,255,0.8)', up ? '#05d890' : '#ff4060'];
    rows.forEach((r, ri) => {
      octx.fillStyle = colors[ri];
      octx.fillText(r, tx + 8 * dpr, ty + 16 * dpr + ri * 16 * dpr);
    });
    octx.restore();
  }

  _fmt(p) {
    const v = +p;
    if (!isFinite(v)) return '--';
    return v.toLocaleString('en-US', {minimumFractionDigits: v >= 1000 ? 2 : 2, maximumFractionDigits: v >= 1000 ? 2 : 2});
  }

  // ─── Resize ───────────────────────────────────────────────────────────────

  _resize() {
    const dpr = window.devicePixelRatio || 1;
    // Get PARENT dimensions — container may report 0 if flex
    const src  = this.container;
    const w    = src.offsetWidth  || src.parentElement?.offsetWidth  || 900;
    const h    = src.offsetHeight || src.parentElement?.offsetHeight || 420;
    [this.canvas, this.ov].forEach(c => {
      c.width  = Math.round(w * dpr);
      c.height = Math.round(h * dpr);
      // Reset scale after resize (getContext shares state)
    });
    // Re-apply DPR scale
    [this.ctx, this.octx].forEach(c => { c.setTransform(1,0,0,1,0,0); c.scale(dpr, dpr); });
    this._draw();
  }

  _bindResize() {
    const ro = new ResizeObserver(() => this._resize());
    ro.observe(this.container);
    if (this.container.parentElement) ro.observe(this.container.parentElement);
  }

  // ─── Events ───────────────────────────────────────────────────────────────

  _bindEvents() {
    const el   = this.canvas;
    let drag   = false, dragX = 0, dragView = 0;

    // Crosshair
    el.addEventListener('mousemove', e => {
      this._crosshair(e.clientX, e.clientY);
      if (drag) {
        const rect   = el.getBoundingClientRect();
        const slotW  = (rect.width - this.pad.l / (window.devicePixelRatio||1) - this.pad.r / (window.devicePixelRatio||1)) / this.viewCount;
        const delta  = Math.round((dragX - e.clientX) / slotW);
        this.viewStart = Math.max(0, Math.min(this.candles.length - this.viewCount, dragView + delta));
        this._draw();
      }
    });

    el.addEventListener('mouseleave', () => {
      this.octx.clearRect(0, 0, this.ov.width, this.ov.height);
      drag = false;
    });

    // Drag
    el.addEventListener('mousedown', e => {
      drag = true; dragX = e.clientX; dragView = this.viewStart;
      el.style.cursor = 'grabbing';
      e.preventDefault();
    });
    window.addEventListener('mouseup', () => {
      drag = false;
      if (el) el.style.cursor = 'crosshair';
    });

    // Scroll zoom — zoom toward cursor position
    el.addEventListener('wheel', e => {
      e.preventDefault();
      const rect    = el.getBoundingClientRect();
      const ratio   = (e.clientX - rect.left) / rect.width;
      const zoomIn  = e.deltaY < 0;
      const factor  = zoomIn ? 0.82 : 1.22;
      const prev    = this.viewCount;
      this.viewCount = Math.max(5, Math.min(this.candles.length, Math.round(prev * factor)));
      // Anchor zoom to cursor position
      const anchor  = Math.round(this.viewStart + prev * ratio);
      this.viewStart = Math.max(0, Math.min(
        this.candles.length - this.viewCount,
        Math.round(anchor - this.viewCount * ratio)
      ));
      this._draw();
    }, {passive: false});

    // Pinch zoom
    let pinch = 0;
    el.addEventListener('touchstart', e => {
      if (e.touches.length === 2) {
        pinch = Math.hypot(e.touches[0].clientX - e.touches[1].clientX, e.touches[0].clientY - e.touches[1].clientY);
      } else {
        drag = true;
        dragX = e.touches[0].clientX;
        dragView = this.viewStart;
      }
    }, {passive: true});

    el.addEventListener('touchmove', e => {
      if (e.touches.length === 2 && pinch) {
        const d2   = Math.hypot(e.touches[0].clientX - e.touches[1].clientX, e.touches[0].clientY - e.touches[1].clientY);
        const f    = pinch / d2;
        pinch      = d2;
        this.viewCount = Math.max(5, Math.min(this.candles.length, Math.round(this.viewCount * f)));
        this.viewStart = Math.max(0, Math.min(this.candles.length - this.viewCount, this.viewStart));
        this._draw();
      } else if (drag && e.touches.length === 1) {
        const rect  = el.getBoundingClientRect();
        const slotW = rect.width / this.viewCount;
        const delta = Math.round((dragX - e.touches[0].clientX) / slotW);
        this.viewStart = Math.max(0, Math.min(this.candles.length - this.viewCount, dragView + delta));
        this._draw();
      }
    }, {passive: true});

    el.addEventListener('touchend', () => { drag = false; pinch = 0; });

    // Double-click reset
    el.addEventListener('dblclick', () => {
      this.viewCount = 80;
      this.viewStart = Math.max(0, this.candles.length - 80);
      this._draw();
    });

    el.style.cursor = 'crosshair';
  }
}

global.AlphaCoreChart = AlphaCoreChart;
})(window);
