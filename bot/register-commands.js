/**
 * TradeCore — Discord Slash Command Registration
 *
 * Run this ONCE to register slash commands with Discord.
 * After running, commands appear in Discord within a few minutes.
 *
 * Usage:
 *   DISCORD_TOKEN=your_bot_token DISCORD_APP_ID=your_app_id node register-commands.js
 *
 * Or set the env vars in your shell first:
 *   export DISCORD_TOKEN=...
 *   export DISCORD_APP_ID=...
 *   node register-commands.js
 *
 * Find these in Discord Developer Portal → Your App:
 *   - Application ID = DISCORD_APP_ID
 *   - Bot Token = DISCORD_TOKEN (Bot → Reset Token)
 */

'use strict';

const TOKEN  = process.env.DISCORD_TOKEN;
const APP_ID = process.env.DISCORD_APP_ID;

if (!TOKEN || !APP_ID) {
  console.error('❌ Missing DISCORD_TOKEN or DISCORD_APP_ID');
  console.error('   Run: DISCORD_TOKEN=xxx DISCORD_APP_ID=xxx node register-commands.js');
  process.exit(1);
}

const commands = [
  {
    name: 'exit',
    description: 'Exit an open position immediately at market price',
    options: [{
      name: 'symbol',
      description: 'Stock ticker (e.g. AAPL, TSLA)',
      type: 3, // STRING
      required: true,
    }],
  },
  {
    name: 'status',
    description: 'Show bot status, equity, and all open positions',
  },
  {
    name: 'positions',
    description: 'List all open positions with P&L',
  },
  {
    name: 'pause',
    description: 'Pause the bot — engages circuit breaker, no new trades',
  },
  {
    name: 'resume',
    description: 'Resume the bot — clears circuit breaker, trading restarts',
  },
  {
    name: 'sim',
    description: 'Toggle simulation mode on/off (replays historical bars)',
  },
];

async function register() {
  const { default: fetch } = await import('node-fetch');
  const url = `https://discord.com/api/v10/applications/${APP_ID}/commands`;

  console.log(`Registering ${commands.length} slash commands for app ${APP_ID}…`);

  const res = await fetch(url, {
    method: 'PUT', // PUT replaces ALL global commands atomically
    headers: {
      'Authorization': `Bot ${TOKEN}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(commands),
  });

  if (res.ok) {
    const data = await res.json();
    console.log(`✅ Registered ${data.length} commands:`);
    data.forEach(c => console.log(`   /${c.name} — ${c.description}`));
    console.log('\nCommands will appear in Discord within 1-2 minutes.');
    console.log('\nNext step: set your bot\'s Interactions Endpoint URL in Discord Developer Portal:');
    console.log('   https://your-render-url.onrender.com/discord');
  } else {
    const err = await res.text();
    console.error(`❌ Registration failed (${res.status}):`, err);
  }
}

register().catch(console.error);
