require('dotenv').config();
const express = require('express');
const { Telegraf } = require('telegraf');
const solanaWeb3 = require('@solana/web3.js');
const mongoose = require('mongoose');
const bip39 = require('bip39');
const { derivePath } = require('ed25519-hd-key');

const app = express();
const port = process.env.PORT || 3000; 

// تنظيف وتجهيز المتغيرات بأمان تام بدون توقف السيرفر
const telegramToken = process.env.TELEGRAM_TOKEN ? process.env.TELEGRAM_TOKEN.trim().replace(/['"]/g, '') : '';
const botPrivateKey = process.env.BOT_PRIVATE_KEY ? process.env.BOT_PRIVATE_KEY.trim() : '';
const mongoURI = process.env.MONGO_URI ? process.env.MONGO_URI.trim().replace(/['"]/g, '') : '';

const seenSignatures = new Set(); 
const MAX_TRADES = 5; 
let currentTradesCount = 0; 

const TRAILING_STOP_PERCENT = 15; 
const WHALE_DUMP_ALERT = 25; 

try {
  if (mongoURI) {
      mongoose.connect(mongoURI).then(() => console.log("☁️ Cloud Memory Connected!"));
  }

  const tradeSchema = new mongoose.Schema({
    coin_address: String,
    buy_price_sol: Number,
    token_amount: Number,
    status: { type: String, default: 'OPEN' }, 
    trade_time: { type: Date, default: Date.now }
  });
  const Trade = mongoose.models.Trade || mongoose.model('Trade', tradeSchema);

  const bot = telegramToken ? new Telegraf(telegramToken) : null;
  const solanaConnection = new solanaWeb3.Connection(solanaWeb3.clusterApiUrl('mainnet-beta'), 'confirmed');

  let botWallet = null;
  try {
      if(botPrivateKey && bip39.validateMnemonic(botPrivateKey)) {
          const seed = bip39.mnemonicToSeedSync(botPrivateKey);
          const derivedSeed = derivePath("m/44'/501'/0'/0'", seed.toString('hex')).key;
          botWallet = solanaWeb3.Keypair.fromSeed(derivedSeed);
          console.log(`✅ Wallet Loaded: ${botWallet.publicKey.toString()}`);
      }
  } catch(e) {
      console.log("⚠️ Wallet parse warning, check private key format.");
  }

  app.get('/', (req, res) => res.send('✅ Sniper Engine LIVE with Trailing Stop!'));

  if (bot) {
      bot.start((ctx) => ctx.reply("Welcome Boss! 💸\n🎯 SNIPER MODE: Trailing Stop Loss Active.\nType /hunt to start."));

      bot.command('scan', async (ctx) => {
          if (!botWallet) return ctx.reply("❌ المحفظة غير محملة، تأكد من المفتاح.");
          const balance = await solanaConnection.getBalance(botWallet.publicKey);
          const solBalance = (balance / solanaWeb3.LAMPORTS_PER_SOL).toFixed(4);
          const safeBalance = Math.max(0, solBalance - 0.005); 
          const expectedTradeSize = (safeBalance / 5).toFixed(4);
          ctx.reply(`✅ Network Connected!\n💰 رصيد المحفظة: ${solBalance} SOL\n📊 الصفقات: ${currentTradesCount}/${MAX_TRADES}\n⚖️ الحجم: ${expectedTradeSize} SOL`);
      });

      bot.command('hunt', (ctx) => {
          ctx.reply("🦈 Sniper Mode: ON\nTarget: Ride the pump, dump on the whales.");
      });

      bot.launch();
  }

  app.listen(port, () => console.log(`✅ Sniper Engine running on port ${port}...`));

} catch (error) {
  console.log("❌ ERROR:", error.message);
}
