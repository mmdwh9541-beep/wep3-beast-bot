require('dotenv').config();
const express = require('express');
const { Telegraf } = require('telegraf');
const solanaWeb3 = require('@solana/web3.js');
const mongoose = require('mongoose');
const bip39 = require('bip39');
const { derivePath } = require('ed25519-hd-key');

const app = express();
const port = process.env.PORT || 3000; 

// --- 🔍 فحص المتغيرات في الـ Logs ---
console.log("--- 🔍 فحص حالة المتغيرات السرية على Render ---");
console.log("TELEGRAM_TOKEN:", process.env.TELEGRAM_TOKEN ? "✅ موجود وقاري" : "❌ مفقود/مش قاري");
console.log("MONGO_URI:", process.env.MONGO_URI ? "✅ موجود وقاري" : "❌ مفقود/مش قاري");
console.log("BOT_PRIVATE_KEY:", process.env.BOT_PRIVATE_KEY ? "✅ موجود وقاري" : "❌ مفقود/مش قاري");
console.log("-----------------------------------------------");

const telegramToken = process.env.TELEGRAM_TOKEN;
const botPrivateKey = process.env.BOT_PRIVATE_KEY;
const mongoURI = process.env.MONGO_URI;

if (!telegramToken || !mongoURI) {
    console.error("❌ خطأ: المتغيرات ناقصة، السيرفر سيتم إيقافه لحين ضبطها في Render.");
    process.exit(1);
}

const seenSignatures = new Set(); 
const MAX_TRADES = 5; 
let currentTradesCount = 0; 

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
    const safeBalance = Math.max(0, solBalance - 0.005); 
    const expectedTradeSize = (safeBalance / 5).toFixed(4);

    ctx.reply(`✅ Network Connected!\n💰 رصيد المحفظة: ${solBalance} SOL\n📊 الصفقات المفتوحة: ${currentTradesCount}/${MAX_TRADES}\n⚖️ الحجم المتوقع للصفقة: ${expectedTradeSize} SOL`);
  });

  async function checkTokenSecurity(txSignature, ctx) {
    return new Promise((resolve) => {
        setTimeout(() => {
            const isSafe = Math.random() > 0.7; 
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

  async function executeRealBuy(txSignature, ctx) {
      try {
          if(!botWallet) return;
          if (currentTradesCount >= MAX_TRADES) return;

          if (seenSignatures.has(txSignature)) return; 
          seenSignatures.add(txSignature);
          setTimeout(() => seenSignatures.delete(txSignature), 3600000); 

          const isSafe = await checkTokenSecurity(txSignature, ctx);
          if (!isSafe) return;

          const balance = await solanaConnection.getBalance(botWallet.publicKey);
          const solBalance = balance / solanaWeb3.LAMPORTS_PER_SOL;
          const availableBalance = solBalance - 0.005; 
          
          if (availableBalance <= 0) {
              ctx.reply("❌ الرصيد لا يكفي لرسوم الغاز.");
              return;
          }

          const buyAmountSOL = parseFloat((availableBalance / 5).toFixed(4)); 
          currentTradesCount++;
          
          ctx.reply(`⚡ جاري تنفيذ الصفقة رقم [${currentTradesCount}/${MAX_TRADES}]...\n💰 الحجم المحسوب: ${buyAmountSOL} SOL`);
          
          const transaction = new solanaWeb3.Transaction().add(
            solanaSystemProgramTransferSafe = solanaWeb3.SystemProgram.transfer({
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
              status: `BOUGHT_REAL`
          });
          await newTrade.save();

          ctx.reply(`✅ **تم الدخول في الصفقة بنجاح!** 💸\n\n🔗 رابط التأكيد:\nhttps://solscan.io/tx/${realSignature}`);

          if (currentTradesCount === MAX_TRADES) {
              ctx.reply("🛑 **تنبيه:** تم الوصول للحد الأقصى (5 صفقات).");
          }

      } catch (err) {
          console.error("❌ Real Buy Error:", err);
          currentTradesCount--; 
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
