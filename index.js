require('dotenv').config();
const express = require('express');
const { Telegraf } = require('telegraf');
const solanaWeb3 = require('@solana/web3.js');
const mongoose = require('mongoose');
const bip39 = require('bip39');
const { derivePath } = require('ed25519-hd-key');

const app = express();
const port = process.env.PORT || 10000; 

// حماية شاملة من الانهيار المفاجئ
process.on('uncaughtException', (err) => {
    console.error('🔥 Uncaught Exception:', err.message);
});
process.on('unhandledRejection', (reason) => {
    console.error('🔥 Unhandled Rejection:', reason);
});

const telegramToken = process.env.TELEGRAM_TOKEN ? process.env.TELEGRAM_TOKEN.trim().replace(/['"]/g, '') : '';
const botPrivateKey = process.env.BOT_PRIVATE_KEY ? process.env.BOT_PRIVATE_KEY.trim() : '';
const mongoURI = process.env.MONGO_URI ? process.env.MONGO_URI.trim().replace(/['"]/g, '') : '';

const seenSignatures = new Set(); 
const MAX_TRADES = 5; 
let currentTradesCount = 0; 

app.get('/', (req, res) => res.send('✅ Web3 Beast Sniper Engine is LIVE!'));

try {
  if (mongoURI) {
      mongoose.connect(mongoURI)
        .then(() => console.log("☁️ Cloud Memory Connected!"))
        .catch(err => console.log("❌ DB Error:", err.message));
  }

  const tradeSchema = new mongoose.Schema({
    coin_address: String,
    buy_price_sol: Number,
    token_amount: Number,
    status: { type: String, default: 'OPEN' }, 
    trade_time: { type: Date, default: Date.now }
  });
  const Trade = mongoose.models.Trade || mongoose.model('Trade', tradeSchema);

  const solanaConnection = new solanaWeb3.Connection(solanaWeb3.clusterApiUrl('mainnet-beta'), 'confirmed');

  let botWallet = null;
  try {
      if(botPrivateKey && bip39.validateMnemonic(botPrivateKey)) {
          const seed = bip39.mnemonicToSeedSync(botPrivateKey);
          const derivedSeed = derivePath("m/44'/501'/0'/0'", seed.toString('hex')).key;
          botWallet = solanaWeb3.Keypair.fromSeed(derivedSeed);
          console.log(`✅ Wallet Loaded: ${botWallet.publicKey.toString()}`);
      } else {
          console.log("⚠️ Warning: Bot Private Key is invalid or missing.");
      }
  } catch(e) {
      console.log("⚠️ Wallet derivation error:", e.message);
  }

  if (telegramToken) {
      const bot = new Telegraf(telegramToken);

      bot.start((ctx) => ctx.reply("Welcome Boss! 💸\n🎯 SNIPER & Trailing Stop Engine Active.\nType /scan or /hunt."));

      bot.command('scan', async (ctx) => {
          if (!botWallet) return ctx.reply("❌ المحفظة غير محملة، تأكد من الـ 24 كلمة في Render.");
          try {
              const balance = await solanaConnection.getBalance(botWallet.publicKey);
              const solBalance = (balance / solanaWeb3.LAMPORTS_PER_SOL).toFixed(4);
              const safeBalance = Math.max(0, solBalance - 0.005); 
              const expectedTradeSize = (safeBalance / 5).toFixed(4);
              ctx.reply(`✅ Network Connected!\n💰 رصيد المحفظة: ${solBalance} SOL\n📊 الصفقات المفتوحة: ${currentTradesCount}/${MAX_TRADES}\n⚖️ الحجم المتوقع للصفقة: ${expectedTradeSize} SOL`);
          } catch(err) {
              ctx.reply("❌ خطأ أثناء فحص الشبكة والمحفظة.");
          }
      });

      bot.command('hunt', (ctx) => {
          ctx.reply("🦈 Sniper Radar is ON!\nTracking Raydium pools & Whale activity...");
      });

      bot.launch();
      console.log("✅ Telegram Bot launched successfully!");
  }

  app.listen(port, () => console.log(`✅ Beast Sniper running on port ${port}...`));

} catch (error) {
  console.log("❌ CRITICAL STARTUP ERROR:", error.message);
}
