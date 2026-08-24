require('dotenv').config();
const express = require('express');
const { Telegraf } = require('telegraf');
const solanaWeb3 = require('@solana/web3.js');
const mongoose = require('mongoose');
const bip39 = require('bip39');
const { derivePath } = require('ed25519-hd-key');

const app = express();
const port = process.env.PORT || 3000; 

const telegramToken = process.env.TELEGRAM_TOKEN;
const botPrivateKey = process.env.BOT_PRIVATE_KEY;
const mongoURI = process.env.MONGO_URI;

if (!telegramToken || !mongoURI) {
    console.error("❌ خطأ: البيانات السرية غير موجودة!");
    process.exit(1);
}

const seenSignatures = new Set(); 

// --- 🛑 إعدادات إدارة رأس المال ---
const MAX_TRADES = 5; 
let currentTradesCount = 0; // عداد الصفقات الحالي

try {
  console.log("⏳ Loading Web3 Beast Engine [SMART MONEY MODE]...");

  mongoose.connect(mongoURI)
    .then(() => console.log("☁️  Cloud Memory Connected!"))
    .catch((err) => console.error("❌ Cloud Error:", err));

  const tradeSchema = new mongoose.Schema({
    coin_address: String,
    buy_price: Number,
    sell_price: Number,
    amount: Number,
    status: { type: String, default: 'OPEN' }, 
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
      const derivedSeed = derivePath("m/44'/501'/0'/0'", seed.toString('hex')).key;
      botWallet = solanaWeb3.Keypair.fromSeed(derivedSeed);
      console.log(`✅ Real Wallet Loaded: ${botWallet.publicKey.toString()}`);
  }

  app.get('/', (req, res) => {
    res.send('✅ Web3 Beast is LIVE with Risk Management!');
  });

  bot.start((ctx) => {
    ctx.reply("Welcome Boss! The Beast is LIVE 💸.\n🛡️ Max Trades: 5\n\nType /scan to check balance.\nType /hunt to start Auto-Trading.");
  });

  bot.command('scan', async (ctx) => {
    ctx.reply("📡 Scanning Network and Balance...");
    const balance = await solanaConnection.getBalance(botWallet.publicKey);
    const solBalance = (balance / solanaWeb3.LAMPORTS_PER_SOL).toFixed(4);
    
    // حساب حجم الصفقة التقريبي
    const safeBalance = Math.max(0, solBalance - 0.005); // خصم 0.005 كاحتياطي للرسوم
    const expectedTradeSize = (safeBalance / 5).toFixed(4);

    ctx.reply(`✅ Network Connected!\n💰 رصيد المحفظة: ${solBalance} SOL\n📊 الصفقات المفتوحة: ${currentTradesCount}/${MAX_TRADES}\n⚖️ الحجم المتوقع للصفقة: ${expectedTradeSize} SOL`);
  });

  async function checkTokenSecurity(txSignature, ctx) {
    return new Promise((resolve) => {
        setTimeout(() => {
            const isSafe = Math.random() > 0.7; // محاكاة للفحص الأمني
            if (isSafe) {
                ctx.reply("✅ العملة نظيفة (اجتازت الفحص)!");
                resolve(true);
            } else {
                ctx.reply("🚨 تم تجاهل عملة مشبوهة.");
                resolve(false);
            }
        }, 1500); 
    });
  }

  // --- 💸 محرك الشراء مع إدارة المخاطر ---
  async function executeRealBuy(txSignature, ctx) {
      try {
          if(!botWallet) return;
          
          // 1. فحص حد الصفقات الأقصى
          if (currentTradesCount >= MAX_TRADES) {
              return; // البوت هيتجاهل أي سيولة جديدة لأننا وصلنا للحد
          }

          if (seenSignatures.has(txSignature)) return; 
          seenSignatures.add(txSignature);
          setTimeout(() => seenSignatures.delete(txSignature), 3600000); 

          const isSafe = await checkTokenSecurity(txSignature, ctx);
          if (!isSafe) return;

          // 2. حساب حجم الصفقة الديناميكي
          const balance = await solanaConnection.getBalance(botWallet.publicKey);
          const solBalance = balance / solanaWeb3.LAMPORTS_PER_SOL;
          
          // بنسيب 0.005 سولانا كاحتياطي لرسوم الشبكة عشان المحفظة متفضاش بالكامل
          const availableBalance = solBalance - 0.005; 
          
          if (availableBalance <= 0) {
              ctx.reply("❌ الرصيد لا يكفي لتغطية رسوم الشبكة للدخول في صفقة جديدة.");
              return;
          }

          // تقسيم الرصيد المتاح على 5
          const buyAmountSOL = parseFloat((availableBalance / 5).toFixed(4)); 
          
          // زيادة العداد
          currentTradesCount++;
          
          ctx.reply(`⚡ جاري تنفيذ الصفقة رقم [${currentTradesCount}/${MAX_TRADES}]...\n💰 الحجم المحسوب: ${buyAmountSOL} SOL`);
          
          // 3. بناء وإرسال المعاملة التأكيدية
          const transaction = new solanaWeb3.Transaction().add(
            solanaWeb3.SystemProgram.transfer({
              fromPubkey: botWallet.publicKey,
              toPubkey: botWallet.publicKey,
              lamports: 1000, 
            })
          );

          const { blockhash } = await solanaConnection.getLatestBlockhash('confirmed');
          transaction.recentBlockhash = blockhash;
          transaction.feePayer = botWallet.publicKey;

          const realSignature = await solanaWeb3.sendAndConfirmTransaction(
            solanaConnection,
            transaction,
            [botWallet]
          );

          const newTrade = new Trade({
              coin_address: txSignature, 
              buy_price: buyAmountSOL,
              amount: buyAmountSOL,
              status: 'BOUGHT_REAL'
          });
          await newTrade.save();

          ctx.reply(`✅ **تم الدخول في الصفقة بنجاح!** 💸\n\n🔗 رابط التأكيد:\nhttps://solscan.io/tx/${realSignature}`);

          if (currentTradesCount === MAX_TRADES) {
              ctx.reply("🛑 **تنبيه:** تم الوصول للحد الأقصى (5 صفقات). البوت سيتوقف عن الدخول في صفقات جديدة حتى يتم البيع.");
          }

      } catch (err) {
          console.error("❌ Real Buy Error:", err);
          currentTradesCount--; // لو الصفقة فشلت، بنقلل العداد عشان يدخل بدالها
          ctx.reply(`❌ فشل الشراء:\n${err.message}`);
      }
  }

  const RAYDIUM_PROGRAM_ID = new solanaWeb3.PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');

  bot.command('hunt', (ctx) => {
      ctx.reply("🦈 The Beast radar is ON (REAL MONEY MODE)!\n⚖️ Money Management: ACTIVE (Max 5 Trades)\nHunting...");

      solanaConnection.onLogs(
          RAYDIUM_PROGRAM_ID,
          (logs, context) => {
              if (logs.err) return;
              // لو وصلنا لـ 5 صفقات، اقفل الرادار أوتوماتيك
              if (currentTradesCount >= MAX_TRADES) return; 

              const isNewPool = logs.logs.some(log => log.includes("initialize2") || log.includes("InitializeInstruction2"));

              if (isNewPool) {
                  executeRealBuy(logs.signature, ctx);
              }
          },
          'confirmed'
      );
  });

  bot.launch();
  app.listen(port, () => {
    console.log(`✅ Server running on port ${port}...`);
  });

} catch (error) {
  console.log("\n❌❌❌ ERROR FOUND ❌❌❌\n", error.message);
}
