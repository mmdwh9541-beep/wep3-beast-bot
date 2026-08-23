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

// --- فلتر منع التكرار ---
const seenSignatures = new Set(); 

try {
  console.log("⏳ Loading Web3 Beast Engine...");

  // الاتصال بقاعدة البيانات
  mongoose.connect(mongoURI)
    .then(() => console.log("☁️  Cloud Memory (MongoDB) Connected Successfully!"))
    .catch((err) => console.error("❌ Cloud Memory Connection Error:", err));

  // تحديث قاعدة البيانات عشان تقبل سعر البيع
  const tradeSchema = new mongoose.Schema({
    coin_address: String,
    buy_price: Number,
    sell_price: Number,
    amount: Number,
    status: { type: String, default: 'OPEN' }, // OPEN = لسه شغال | SOLD = تم البيع
    trade_time: { type: Date, default: Date.now }
  });
  
  const Trade = mongoose.model('Trade', tradeSchema);

  const bot = new Telegraf(telegramToken);
  const solanaConnection = new solanaWeb3.Connection(
    solanaWeb3.clusterApiUrl('mainnet-beta'), 
    'confirmed'
  );

  let botWallet;
  if(botPrivateKey && bip39.validateMnemonic(botPrivateKey)) {
      const seed = bip39.mnemonicToSeedSync(botPrivateKey);
      const path = "m/44'/501'/0'/0'";
      const derivedSeed = derivePath(path, seed.toString('hex')).key;
      botWallet = solanaWeb3.Keypair.fromSeed(derivedSeed);
      console.log(`✅ Bot Wallet Loaded: ${botWallet.publicKey.toString()}`);
  } else {
      console.log("⚠️ Bot Wallet NOT Loaded.");
  }

  // --- واجهة UptimeRobot عشان ينور أخضر ---
  app.get('/', (req, res) => {
    res.send('✅ Web3 Beast Bot is ALIVE and RUNNING on the Cloud!');
  });

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

  // --- دالة البيع الآلي (تجريبي) ---
  async function executeAutoSell(tradeId, txSignature, buyAmount, ctx) {
    try {
      ctx.reply(`⏳ جاري تحليل السوق ومحاولة بيع الصفقة:\n${txSignature.substring(0, 15)}...`);
      
      // محاكاة نسبة ربح أو خسارة (من -10% إلى +30%) للتجربة
      const pnlPercentage = (Math.random() * 40 - 10).toFixed(2); 
      const isProfit = pnlPercentage > 0;
      const sellAmount = buyAmount + (buyAmount * (pnlPercentage / 100));

      // تحديث حالة الصفقة في قاعدة البيانات
      await Trade.findByIdAndUpdate(tradeId, {
          status: 'SOLD',
          sell_price: sellAmount
      });

      const emoji = isProfit ? '🟢' : '🔴';
      const resultText = isProfit ? 'ربح ممتاز 🚀' : 'خسارة طفيفة 📉';

      ctx.reply(`🛒 **تم البيع بنجاح!**\n\n${emoji} النتيجة: ${pnlPercentage}% (${resultText})\n💰 الكمية بعد البيع: ${sellAmount.toFixed(4)} SOL\n💾 تم تحديث الصفقة في قاعدة البيانات.`);
      console.log(`💰 Trade SOLD. PNL: ${pnlPercentage}%`);

    } catch (err) {
      console.error("❌ Auto-Sell Error:", err);
      ctx.reply("❌ حدث خطأ أثناء محاولة البيع.");
    }
  }

  // --- دالة الشراء الآلي ---
  async function executeAutoBuy(txSignature, ctx) {
      try {
          if(!botWallet) return;

          // فلتر التكرار: لو الصفقة اتنفذت قبل كده، تجاهلها فوراً
          if (seenSignatures.has(txSignature)) {
            return; 
          }
          seenSignatures.add(txSignature);
          // تنظيف الذاكرة بعد ساعة عشان السيرفر ميهنجش
          setTimeout(() => seenSignatures.delete(txSignature), 3600000); 

          const buyAmount = 0.01; 
          ctx.reply(`⚡ جاري تنفيذ شراء آلي للصفقة...\nالمعاملة: ${txSignature}`);
          
          const newTrade = new Trade({
              coin_address: txSignature, 
              buy_price: buyAmount,
              amount: buyAmount,
              status: 'BOUGHT'
          });
          
          const savedTrade = await newTrade.save();
          ctx.reply(`✅ تم الشراء بنجاح (مرة واحدة فقط)!\n💰 الكمية: ${buyAmount} SOL\n⏱️ سيتم محاكاة البيع بعد دقيقة واحدة لمعرفة الأرباح...`);

          // أمر ببيع الصفقة دي بعد دقيقة واحدة (60000 ملي ثانية)
          setTimeout(() => {
            executeAutoSell(savedTrade._id, txSignature, buyAmount, ctx);
          }, 60000);

      } catch (err) {
          console.error("❌ Auto-Buy Error:", err);
      }
  }

  const RAYDIUM_PROGRAM_ID = new solanaWeb3.PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');

  bot.command('hunt', (ctx) => {
      ctx.reply("🦈 The Beast radar is ON! Anti-Spam Filter Enabled. Hunting for new pools...");

      solanaConnection.onLogs(
          RAYDIUM_PROGRAM_ID,
          (logs, context) => {
              if (logs.err) return;
              const isNewPool = logs.logs.some(log => log.includes("initialize2") || log.includes("InitializeInstruction2"));

              if (isNewPool) {
                  const signature = logs.signature;
                  executeAutoBuy(signature, ctx);
              }
          },
          'confirmed'
      );
  });

  bot.launch();
  
  app.listen(port, () => {
    console.log(`✅ Server is running on port ${port}...`);
  });

} catch (error) {
  console.log("\n❌❌❌ ERROR FOUND ❌❌❌\n", error.message);
}
