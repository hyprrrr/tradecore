# TradeCore — Autonomous Trading Bot

A fully free trading bot with a Vercel-hosted dashboard, Discord alerts, and paper/live trading support.

---

## 🏗 Project Structure

```
/
├── index.html          ← Dashboard (deploy to Vercel)
└── bot/
    ├── index.js        ← Bot engine (deploy to Railway or Render)
    └── package.json
```

---

## 🚀 Deploy Dashboard to Vercel (Free)

1. Push this repo to GitHub
2. Go to [vercel.com](https://vercel.com) → New Project → Import repo
3. Vercel auto-detects it as a static site (no build needed)
4. Done! Your dashboard is live at `https://your-project.vercel.app`

---

## 🤖 Deploy Bot to Railway.app (Free)

1. Go to [railway.app](https://railway.app) → New Project → Deploy from GitHub
2. Select the `bot/` folder as the root directory  
   *(Or set the root dir to `bot` in Railway settings)*
3. Add these environment variables in Railway:

| Variable | Description | Default |
|---|---|---|
| `DISCORD_WEBHOOK` | Your Discord webhook URL | (none) |
| `MODE` | `paper` or `alpaca` | `paper` |
| `SYMBOLS` | Comma-separated stock symbols | `AAPL,TSLA,GOOGL,MSFT,SPY` |
| `CAPITAL` | Starting capital in USD | `10000` |
| `MAX_POSITION_PCT` | Max % per position | `10` |
| `MAX_POSITIONS` | Max open positions | `3` |
| `STOP_LOSS_PCT` | Stop loss % | `5` |
| `TAKE_PROFIT_PCT` | Take profit % | `10` |
| `RSI_PERIOD` | RSI period | `14` |
| `RSI_OVERSOLD` | RSI oversold level | `30` |
| `RSI_OVERBOUGHT` | RSI overbought level | `70` |
| `SCAN_INTERVAL_MIN` | Scan frequency in minutes | `5` |
| `STRATEGY` | `rsi_macd`, `ema_cross`, `momentum`, `mean_reversion` | `rsi_macd` |

---

## 📈 For Live Stock Trading (Alpaca — Free)

1. Sign up at [alpaca.markets](https://alpaca.markets) (free, commission-free)
2. Get your API Key & Secret from the dashboard
3. Set in Railway: `MODE=alpaca`, `ALPACA_KEY=...`, `ALPACA_SECRET=...`
4. Set `ALPACA_PAPER=true` to stay in paper mode via Alpaca's system
5. Set `ALPACA_PAPER=false` ONLY when you're ready for real money

---

## ₿ Free Crypto Data

The bot uses **CoinGecko API** (free, no API key needed) for crypto price data.  
Paper trades only — crypto execution requires a paid exchange API.

---

## 📨 Discord Alerts Setup

1. Open your Discord server
2. Go to **Server Settings → Integrations → Webhooks**
3. Click **New Webhook** → Copy URL
4. Paste in the dashboard **Discord Alerts** section
5. Click **Test Webhook** to confirm

Alerts sent for: BUY signals, SELL signals, Stop Loss hits, Take Profit hits, Daily 9PM summary.

---

## ⚠️ Disclaimer

This bot is for educational and paper trading purposes. Past performance of algorithmic strategies does not guarantee future results. Never trade with money you can't afford to lose.
