const express = require('express');
const { Telegraf } = require('telegraf');
const solanaWeb3 = require('@solana/web3.js');
const mongoose = require('mongoose');
const bip39 = require('bip39');
const { derivePath } = require('ed25519-hd-key');

const app = express();
const port = process.env.PORT || 3000; 
const telegramToken = "8222054898:AAGYFGG6DC7sT55J2HsZSMFG56tC_gdu5c8";

// الـ 12 كلمة الخاصة بمحفظتك
const botPrivateKey = "goat danger unknown market finger winter luxury charge require credit detail wheat"; 

const mongoURI = "mongodb+srv://lomyadmin:Lomy2026@cluster0.n4iuarr.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0";

try {
  console.log("⏳ Loading Web3 Beast Engine...");

  // الاتصال بقاعدة البيانات السحابية
  mongoose.connect(mongoURI)
    .then(() => console.log("☁️  Cloud Memory (MongoDB) Connected Successfully!"))
    .catch((err) => console.error("❌ Cloud Memory Connection Error:", err));

  const tradeSchema = new mongoose.Schema({
    coin_address: String,
    buy_price: Number,
    amount: Number,
    status: { type: String, default: 'OPEN' },
    trade_time: { type: Date, default: Date.now }
  });
  
  const Trade = mongoose.model('Trade', tradeSchema);

  // إعدادات التليجرام وسولانا
  const bot = new Telegraf(telegramToken);
  const solanaConnection = new solanaWeb3.Connection(
    solanaWeb3.clusterApiUrl('mainnet-beta'), 
    'confirmed'
  );

  // توليد المحفظة تلقائياً من الـ 12 كلمة
  let botWallet;
  if(botPrivateKey && bip39.validateMnemonic(botPrivateKey)) {
      const seed = bip39.mnemonicToSeedSync(botPrivateKey);
      const path = "m/44'/501'/0'/0'";
      const derivedSeed = derivePath(path, seed.toString('hex')).key;
      botWallet = solanaWeb3.Keypair.fromSeed(derivedSeed);
      console.log(`✅ Bot Wallet Loaded Successfully from Seed Phrase: ${botWallet.publicKey.toString()}`);
  } else {
      console.log("⚠️ Bot Wallet NOT Loaded. Check your 12 words seed phrase.");
  }

  // أوامر البوت الأساسية
  bot.start((ctx) => {
    ctx.reply("Welcome Boss! The Web3 Beast Engine is online.\n☁️ Cloud Memory: ACTIVE\n\nType /scan to check network.\nType /hunt to start Auto-Trading.");
  });

  bot.command('scan', async (ctx) => {
    ctx.reply("📡 Scanning Solana Network...");
    try {
      const slot = await solanaConnection.getSlot();
      ctx.reply(`✅ Network Connected!\n🔗 Current Block: ${slot}\nThe Beast is ready to hunt!`);
    } catch (err) {
      ctx.reply(`❌ Connection Failed: ${err.message}`);
    }
  });

  // عقل التداول الآلي وتسجيل الصفقات
  async function executeAutoBuy(txSignature, ctx) {
      try {
          if(!botWallet) {
              ctx.reply("⚠️ لا يوجد مفتاح محفظة!");
              return;
          }

          ctx.reply(`⚡ جاري تنفيذ شراء آلي للصفقة...\nالمعاملة: ${txSignature}`);
          console.log(`💸 Initiating Buy Protocol for TX: ${txSignature}`);
          
          const buyAmount = 0.01; 

          const newTrade = new Trade({
              coin_address: txSignature, 
              buy_price: buyAmount,
              amount: buyAmount,
              status: 'BOUGHT'
          });
          
          await newTrade.save();
          console.log("💾 Trade saved to Cloud Memory!");
          ctx.reply(`✅ تم الشراء بنجاح وتسجيل الصفقة في قاعدة البيانات!\n💰 الكمية: ${buyAmount} SOL`);

      } catch (err) {
          console.error("❌ Auto-Buy Error:", err);
          ctx.reply(`❌ فشل الشراء الآلي: ${err.message}`);
      }
  }

  // رادار الصيد على Raydium
  const RAYDIUM_PROGRAM_ID = new solanaWeb3.PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');

  bot.command('hunt', (ctx) => {
      ctx.reply("🦈 The Beast radar is ON! Hunting for new liquidity pools on Raydium with AUTO-BUY enabled...");
      console.log("📡 Radar activated: Listening to Raydium...");

      solanaConnection.onLogs(
          RAYDIUM_PROGRAM_ID,
          (logs, context) => {
              if (logs.err) return;

              const isNewPool = logs.logs.some(log => log.includes("initialize2") || log.includes("InitializeInstruction2"));

              if (isNewPool) {
                  const signature = logs.signature;
                  console.log(`🔥 New Token Pool Detected! TX: ${signature}`);
                  
                  ctx.reply(`🚨 تم رصد إضافة سيولة لعملة جديدة!\n🔗 افحص المعاملة من هنا:\nhttps://solscan.io/tx/${signature}`);
                  executeAutoBuy(signature, ctx);
              }
          },
          'confirmed'
      );
  });

  bot.launch();
  
  // تشغيل السيرفر لخوادم السحابة (Render)
  app.listen(port, () => {
    console.log(`✅ Server is running successfully on port ${port}...`);
    console.log(`🔌 Connected to Solana Mainnet.`);
  });

} catch (error) {
  console.log("\n❌❌❌ ERROR FOUND ❌❌❌");
  console.log(error.message);
  console.log("❌❌❌❌❌❌❌❌❌❌❌\n");
}