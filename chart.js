/**
 * AlphaCore Chart Engine
 * TV-style canvas candlestick chart — no dependencies
 * Loads via <script src="chart.js"> in index.html
 */

(function(global) {
'use strict';

// ─── Palette (matches AlphaCore glass theme) ───────────────────────────────
const C = {
  bg:          'transparent',
  grid:        'rgba(255,255,255,0.05)',
  gridBright:  'rgba(255,255,255,0.10)',
  text:        'rgba(255,255,255,0.35)',
  textBright:  'rgba(255,255,255,0.7)',
  bullBody:    '#2962ff',
  bullWick:    '#2962ff',
  bearBody:    '#131722',
  bearBorder:  '#2962ff',
  bearWick:    '#2962ff',
  ema:         '#f5c518',
  vwap:        '#9b59b6',
  crosshair:   'rgba(255,255,255,0.25)',
  labelBg:     '#2962ff',
  labelText:   '#ffffff',
  gradTop:     'rgba(41,98,255,0.18)',
  gradBot:     'rgba(41,98,255,0.00)',
};

// ─── Chart class ──────────────────────────────────────────────────────────
class AlphaCoreChart {
  constructor(containerId) {
    this.container = document.getElementById(containerId);
    if (!this.container) throw new Error('Container not found: ' + containerId);

    // Canvas setup — two layers: base (grid+candles) + overlay (crosshair)
    this.canvas     = this._mkCanvas();
    this.overlay    = this._mkCanvas();
    this.overlay.style.position  = 'absolute';
    this.overlay.style.inset     = '0';
    this.overlay.style.pointerEvents = 'none';
    this.container.style.position = 'relative';
    this.container.style.overflow = 'hidden';
    this.container.appendChild(this.canvas);
    this.container.appendChild(this.overlay);

    this.ctx  = this.canvas.getContext('2d');
    this.octx = this.overlay.getContext('2d');

    // View state
    this.candles    = [];       // {time, open, high, low, close, volume}
    this.viewStart  = 0;        // index of leftmost visible candle
    this.viewCount  = 80;       // how many candles visible
    this.pad        = {t:20, r:72, b:30, l:4};
    this.showEma    = true;
    this.showVwap   = true;
    this._raf       = null;
    this._dirty     = false;

    // Interaction state
    this._drag      = null;
    this._pinchDist = null;

    this._bindEvents();
    this._bindResize();
    this._resize();
  }

  // ── Public API ────────────────────────────────────────────────────────────

  /** Set full candle dataset and re-render */
  setData(candles) {
    this.candles   = candles.filter(c => isFinite(c.close) && isFinite(c.open));
    this.viewCount = Math.min(80, this.candles.length);
    this.viewStart = Math.max(0, this.candles.length - this.viewCount);
    this._scheduleRender();
  }

  /** Update or append the latest candle (for live feed) */
  update(candle) {
    if (!this.candles.length) { this.setData([candle]); return; }
    const last = this.candles[this.candles.length - 1];
    const sameTs = Math.floor(+candle.time / 1000) === Math.floor(+last.time / 1000);
    const atEdge = this.viewStart + this.viewCount >= this.candles.length;
    if (sameTs) {
      last.close  = candle.close;
      last.high   = Math.max(last.high,  candle.close);
      last.low    = Math.min(last.low,   candle.close);
      last.volume = (last.volume||0) + (candle.volume||0);
    } else {
      this.candles.push({ ...candle });
      if (atEdge) this.viewStart = Math.max(0, this.candles.length - this.viewCount);
    }
    this._scheduleRender();
  }

  // ── Rendering ─────────────────────────────────────────────────────────────

  _scheduleRender() {
    if (this._dirty) return;
    this._dirty = true;
    this._raf = requestAnimationFrame(() => { this._dirty = false; this._render(); });
  }

  _render() {
    const {ctx, canvas, pad} = this;
    const W = canvas.width, H = canvas.height;
    const cw = W - pad.l - pad.r;
    const ch = H - pad.t - pad.b;

    ctx.clearRect(0, 0, W, H);

    const visible = this._visible();
    if (!visible.length) return;

    const {yMin, yMax} = this._yRange(visible);
    const yRng = yMax - yMin || 1;
    const xOf = i => pad.l + (i + 0.5) * (cw / visible.length);
    const yOf = p => pad.t + ch - ((p - yMin) / yRng) * ch;

    this._drawGrid(ctx, W, H, cw, ch, yMin, yMax, yRng);
    this._drawAreaGradient(ctx, visible, cw, ch, xOf, yOf, W, H, pad);
    this._drawEma(ctx, visible, xOf, yOf);
    this._drawCandles(ctx, visible, cw, xOf, yOf);
    this._drawPriceLabel(ctx, visible, W, H, cw, ch, yOf, pad);
    this._drawTimeAxis(ctx, visible, W, H, cw, pad);
  }

  _visible() {
    const start = Math.max(0, Math.min(this.viewStart, this.candles.length - 1));
    const count = Math.max(1, Math.min(this.viewCount, this.candles.length - start));
    return this.candles.slice(start, start + count);
  }

  _yRange(visible) {
    const prices = visible.flatMap(d => [d.high, d.low]).filter(isFinite);
    if (!prices.length) return { yMin: 0, yMax: 1 };
    const lo = Math.min(...prices), hi = Math.max(...prices);
    const rng = Math.max(hi - lo, hi * 0.004);
    const mid = (lo + hi) / 2;
    return { yMin: mid - rng * 0.7, yMax: mid + rng * 0.7 };
  }

  _drawGrid(ctx, W, H, cw, ch, yMin, yMax, yRng) {
    // Horizontal price lines
    const steps = 6;
    ctx.font = '10px "DM Sans", system-ui, sans-serif';
    for (let i = 0; i <= steps; i++) {
      const y = this.pad.t + (ch / steps) * i;
      const price = yMax - (i / steps) * yRng;
      ctx.strokeStyle = i === 0 || i === steps ? C.gridBright : C.grid;
      ctx.lineWidth = 1;
      ctx.setLineDash([]);
      ctx.beginPath(); ctx.moveTo(this.pad.l, y); ctx.lineTo(W - this.pad.r, y); ctx.stroke();
      ctx.fillStyle = C.text;
      ctx.textAlign = 'left';
      ctx.fillText(this._fmtPrice(price), W - this.pad.r + 4, y + 3.5);
    }
  }

  _drawAreaGradient(ctx, visible, cw, ch, xOf, yOf, W, H, pad) {
    if (visible.length < 2) return;
    const grad = ctx.createLinearGradient(0, pad.t, 0, pad.t + ch);
    grad.addColorStop(0, C.gradTop);
    grad.addColorStop(1, C.gradBot);
    ctx.fillStyle = grad;
    ctx.beginPath();
    ctx.moveTo(xOf(0), yOf(visible[0].close));
    visible.forEach((d, i) => { if (i > 0) ctx.lineTo(xOf(i), yOf(d.close)); });
    ctx.lineTo(xOf(visible.length - 1), pad.t + ch);
    ctx.lineTo(xOf(0), pad.t + ch);
    ctx.closePath();
    ctx.fill();
  }

  _drawEma(ctx, visible, xOf, yOf) {
    if (!this.showEma || visible.length < 5) return;
    const k = 2 / 21;
    let ema = visible[0].close;
    const pts = visible.map((d, i) => {
      ema = i === 0 ? d.close : d.close * k + ema * (1 - k);
      return { x: xOf(i), y: yOf(ema) };
    });
    ctx.strokeStyle = C.ema;
    ctx.lineWidth = 1.5;
    ctx.setLineDash([]);
    ctx.beginPath();
    pts.forEach((p, i) => i === 0 ? ctx.moveTo(p.x, p.y) : ctx.lineTo(p.x, p.y));
    ctx.stroke();
  }

  _drawCandles(ctx, visible, cw, xOf, yOf) {
    const slotW   = cw / Math.max(visible.length, 1);
    const bodyW   = Math.max(1, Math.min(20, slotW * 0.6));
    const halfW   = bodyW / 2;
    const dpr     = window.devicePixelRatio || 1;

    visible.forEach((d, i) => {
      const x   = xOf(i);
      const up  = d.close >= d.open;
      const yH  = yOf(d.high);
      const yL  = yOf(d.low);
      const yO  = yOf(d.open);
      const yC  = yOf(d.close);
      const top = Math.min(yO, yC);
      const bH  = Math.max(Math.abs(yO - yC), 1.5);

      // Wick
      ctx.strokeStyle = C.bullWick;
      ctx.lineWidth   = Math.max(1, 1 * dpr);
      ctx.setLineDash([]);
      ctx.beginPath();
      ctx.moveTo(x, yH);
      ctx.lineTo(x, yL);
      ctx.stroke();

      // Body
      if (up) {
        ctx.fillStyle = C.bullBody;
        ctx.fillRect(x - halfW, top, bodyW, bH);
      } else {
        ctx.fillStyle = C.bearBody;
        ctx.fillRect(x - halfW, top, bodyW, bH);
        ctx.strokeStyle = C.bearBorder;
        ctx.lineWidth   = 1;
        ctx.strokeRect(x - halfW, top, bodyW, bH);
      }
    });
  }

  _drawPriceLabel(ctx, visible, W, H, cw, ch, yOf, pad) {
    const last = visible[visible.length - 1];
    if (!last) return;
    const y   = Math.max(pad.t + 10, Math.min(pad.t + ch - 10, yOf(last.close)));
    const up  = last.close >= last.open;

    // Dashed line to price
    ctx.strokeStyle = C.labelBg;
    ctx.lineWidth   = 0.8;
    ctx.setLineDash([4, 3]);
    ctx.beginPath();
    ctx.moveTo(pad.l, y);
    ctx.lineTo(W - pad.r, y);
    ctx.stroke();
    ctx.setLineDash([]);

    // Label box
    const lbl  = this._fmtPrice(last.close);
    const lblW = 66;
    const lblH = 18;
    const lx   = W - pad.r + 2;
    ctx.fillStyle    = up ? '#2962ff' : '#ef5350';
    this._roundRect(ctx, lx, y - lblH / 2, lblW, lblH, 4);
    ctx.fill();
    ctx.fillStyle    = '#fff';
    ctx.font         = 'bold 10px "DM Mono", "DM Sans", monospace';
    ctx.textAlign    = 'center';
    ctx.fillText(lbl, lx + lblW / 2, y + 3.5);
  }

  _drawTimeAxis(ctx, visible, W, H, cw, pad) {
    if (visible.length < 2) return;
    const step = Math.ceil(visible.length / 6);
    ctx.fillStyle  = C.text;
    ctx.font       = '9.5px "DM Sans", system-ui, sans-serif';
    ctx.textAlign  = 'center';
    const slotW    = cw / visible.length;
    const xOf      = i => pad.l + (i + 0.5) * slotW;

    visible.forEach((d, i) => {
      if (i % step !== 0) return;
      const t = d.time instanceof Date ? d.time : new Date(+d.time);
      const label = t.toLocaleTimeString([], {hour:'2-digit', minute:'2-digit'});
      ctx.fillText(label, xOf(i), H - pad.b + 14);
    });
  }

  // ── Crosshair overlay ─────────────────────────────────────────────────────

  _renderCrosshair(mx, my) {
    const {octx, overlay, pad, canvas} = this;
    const W = canvas.width, H = canvas.height;
    const cw = W - pad.l - pad.r;
    const ch = H - pad.t - pad.b;
    octx.clearRect(0, 0, W, H);

    if (mx < pad.l || mx > W - pad.r || my < pad.t || my > pad.t + ch) return;

    const visible = this._visible();
    if (!visible.length) return;

    const slotW = cw / visible.length;
    const i     = Math.max(0, Math.min(visible.length - 1, Math.floor((mx - pad.l) / slotW)));
    const d     = visible[i];
    const cx    = pad.l + (i + 0.5) * slotW;

    octx.strokeStyle = C.crosshair;
    octx.lineWidth   = 1;
    octx.setLineDash([4, 4]);
    octx.beginPath(); octx.moveTo(cx, pad.t); octx.lineTo(cx, pad.t + ch); octx.stroke();
    octx.beginPath(); octx.moveTo(pad.l, my);  octx.lineTo(W - pad.r, my);  octx.stroke();
    octx.setLineDash([]);

    // Tooltip
    const {yMin, yMax} = this._yRange(visible);
    const yRng = yMax - yMin || 1;
    const price = yMax - ((my - pad.t) / ch) * yRng;
    const chg   = d.close - d.open;
    const up    = chg >= 0;

    const tw = 160, th = 120;
    let tx = cx + 12;
    let ty = my - 10;
    if (tx + tw > W - pad.r) tx = cx - tw - 12;
    if (ty + th > H - pad.b) ty = H - pad.b - th;
    if (ty < pad.t) ty = pad.t;

    const t = d.time instanceof Date ? d.time : new Date(+d.time);
    const rows = [
      ['Time',   t.toLocaleString([], {month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'})],
      ['Open',   this._fmtPrice(d.open)],
      ['High',   this._fmtPrice(d.high)],
      ['Low',    this._fmtPrice(d.low)],
      ['Close',  this._fmtPrice(d.close)],
      ['Change', (chg >= 0 ? '+' : '') + this._fmtPrice(chg)],
    ];

    // Box
    octx.fillStyle = 'rgba(8,8,18,0.92)';
    this._roundRect(octx, tx, ty, tw, th, 8);
    octx.fill();
    octx.strokeStyle = 'rgba(255,255,255,0.12)';
    octx.lineWidth = 1;
    this._roundRect(octx, tx, ty, tw, th, 8);
    octx.stroke();

    // Rows
    octx.font      = '10.5px "DM Sans", system-ui, sans-serif';
    octx.textAlign = 'left';
    rows.forEach(([label, val], ri) => {
      const ry = ty + 16 + ri * 17;
      octx.fillStyle = 'rgba(255,255,255,0.38)';
      octx.fillText(label, tx + 10, ry);
      octx.fillStyle = ri === 5 ? (up ? '#05d890' : '#ff4060') : 'rgba(255,255,255,0.85)';
      octx.textAlign = 'right';
      octx.fillText(val, tx + tw - 10, ry);
      octx.textAlign = 'left';
    });
  }

  // ── Helpers ───────────────────────────────────────────────────────────────

  _fmtPrice(p) {
    const v = +p;
    if (!isFinite(v)) return '--';
    const dec = v >= 1000 ? 2 : v >= 100 ? 2 : v >= 10 ? 3 : 4;
    return v.toLocaleString('en-US', {minimumFractionDigits: dec, maximumFractionDigits: dec});
  }

  _roundRect(ctx, x, y, w, h, r) {
    ctx.beginPath();
    ctx.moveTo(x + r, y);
    ctx.lineTo(x + w - r, y);
    ctx.quadraticCurveTo(x + w, y, x + w, y + r);
    ctx.lineTo(x + w, y + h - r);
    ctx.quadraticCurveTo(x + w, y + h, x + w - r, y + h);
    ctx.lineTo(x + r, y + h);
    ctx.quadraticCurveTo(x, y + h, x, y + h - r);
    ctx.lineTo(x, y + r);
    ctx.quadraticCurveTo(x, y, x + r, y);
    ctx.closePath();
  }

  _mkCanvas() {
    const c = document.createElement('canvas');
    c.style.width  = '100%';
    c.style.height = '100%';
    c.style.display = 'block';
    return c;
  }

  // ── Resize ────────────────────────────────────────────────────────────────

  _resize() {
    const dpr = window.devicePixelRatio || 1;
    const w   = this.container.clientWidth  || 900;
    const h   = this.container.clientHeight || 400;
    [this.canvas, this.overlay].forEach(c => {
      c.width  = w * dpr;
      c.height = h * dpr;
      c.getContext('2d').scale(dpr, dpr);
    });
    this._scheduleRender();
  }

  _bindResize() {
    const ro = new ResizeObserver(() => this._resize());
    ro.observe(this.container);
  }

  // ── Events ────────────────────────────────────────────────────────────────

  _bindEvents() {
    const el = this.canvas;
    const dpr = () => window.devicePixelRatio || 1;
    const px  = e => e.clientX - el.getBoundingClientRect().left;
    const py  = e => e.clientY - el.getBoundingClientRect().top;

    // Mouse move — crosshair
    el.addEventListener('mousemove', e => this._renderCrosshair(px(e), py(e)));
    el.addEventListener('mouseleave', () => this.octx.clearRect(0, 0, this.overlay.width, this.overlay.height));

    // Wheel zoom
    el.addEventListener('wheel', e => {
      e.preventDefault();
      const zoomIn = e.deltaY < 0;
      const factor = zoomIn ? 0.85 : 1.18;
      const ratio  = px(e) / (el.clientWidth || 900);
      const prev   = this.viewCount;
      this.viewCount = Math.max(5, Math.min(this.candles.length, Math.round(prev * factor)));
      this.viewStart = Math.max(0, Math.min(
        this.candles.length - this.viewCount,
        Math.round(this.viewStart - (this.viewCount - prev) * ratio)
      ));
      this._scheduleRender();
    }, {passive: false});

    // Drag pan
    let dragStartX = 0, dragStartView = 0, dragging = false;
    el.addEventListener('mousedown', e => {
      dragging = true; dragStartX = px(e); dragStartView = this.viewStart;
      el.style.cursor = 'grabbing';
    });
    window.addEventListener('mousemove', e => {
      if (!dragging) return;
      const slotW   = (el.clientWidth - this.pad.l - this.pad.r) / this.viewCount;
      this.viewStart = Math.max(0, Math.min(
        this.candles.length - this.viewCount,
        Math.round(dragStartView - (px(e) - dragStartX) / slotW)
      ));
      this._scheduleRender();
    });
    window.addEventListener('mouseup', () => { dragging = false; el.style.cursor = 'crosshair'; });

    // Touch pinch+pan
    el.addEventListener('touchstart', e => {
      if (e.touches.length === 2) {
        const dx = e.touches[0].clientX - e.touches[1].clientX;
        const dy = e.touches[0].clientY - e.touches[1].clientY;
        this._pinchDist = Math.hypot(dx, dy);
      } else {
        dragging = true;
        dragStartX = e.touches[0].clientX;
        dragStartView = this.viewStart;
      }
    }, {passive:true});

    el.addEventListener('touchmove', e => {
      if (e.touches.length === 2 && this._pinchDist) {
        const dx   = e.touches[0].clientX - e.touches[1].clientX;
        const dy   = e.touches[0].clientY - e.touches[1].clientY;
        const dist = Math.hypot(dx, dy);
        const factor = this._pinchDist / dist;
        this._pinchDist = dist;
        this.viewCount = Math.max(5, Math.min(this.candles.length, Math.round(this.viewCount * factor)));
        this.viewStart = Math.max(0, Math.min(this.candles.length - this.viewCount, this.viewStart));
        this._scheduleRender();
      } else if (dragging && e.touches.length === 1) {
        const slotW = (el.clientWidth - this.pad.l - this.pad.r) / this.viewCount;
        this.viewStart = Math.max(0, Math.min(
          this.candles.length - this.viewCount,
          Math.round(dragStartView - (e.touches[0].clientX - dragStartX) / slotW)
        ));
        this._scheduleRender();
      }
    }, {passive:true});

    el.addEventListener('touchend', () => {
      dragging = false; this._pinchDist = null;
    });

    // Double-click to reset view
    el.addEventListener('dblclick', () => {
      this.viewCount = 80;
      this.viewStart = Math.max(0, this.candles.length - 80);
      this._scheduleRender();
    });

    el.style.cursor = 'crosshair';
  }
}

// ─── Export ────────────────────────────────────────────────────────────────
global.AlphaCoreChart = AlphaCoreChart;

})(window);
