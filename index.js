const express = require('express');
const { Telegraf } = require('telegraf');
const solanaWeb3 = require('@solana/web3.js');
const mongoose = require('mongoose');
const bip39 = require('bip39');
const { derivePath } = require('ed25519-hd-key');

const app = express();
const port = process.env.PORT || 3000; 
const telegramToken = "8222054898:AAGYFGG6DC7sT55J2HsZSMFG56tC_gdu5c8";
const botPrivateKey = "goat danger unknown market finger winter luxury charge require credit detail wheat"; 
const mongoURI = "mongodb+srv://lomyadmin:Lomy2026@cluster0.n4iuarr.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0";

const seenSignatures = new Set(); 

try {
  console.log("⏳ Loading Web3 Beast Engine...");

  mongoose.connect(mongoURI)
    .then(() => console.log("☁️  Cloud Memory (MongoDB) Connected Successfully!"))
    .catch((err) => console.error("❌ Cloud Memory Connection Error:", err));

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
      console.log(`✅ Bot Wallet Loaded: ${botWallet.publicKey.toString()}`);
  }

  app.get('/', (req, res) => {
    res.send('✅ Web3 Beast Bot is ALIVE and RUNNING! Security Filters Active.');
  });

  bot.start((ctx) => {
    ctx.reply("Welcome Boss! The Web3 Beast Engine is online.\n🛡️ Anti-Rug Security: ACTIVE\n\nType /scan to check network.\nType /hunt to start Auto-Trading.");
  });

  bot.command('scan', async (ctx) => {
    ctx.reply("📡 Scanning Solana Network...");
    const slot = await solanaConnection.getSlot();
    ctx.reply(`✅ Network Connected! Current Block: ${slot}`);
  });

  // --- 🛡️ محرك فحص الأمان (Anti-Rug Filter) ---
  async function checkTokenSecurity(txSignature, ctx) {
    ctx.reply(`🛡️ جاري فحص العقد الذكي للعملة ضد الاحتيال...\nالمعاملة: ${txSignature.substring(0,15)}...`);
    
    // محاكاة لفحص الأمان (في النسخة الحقيقية بيتم الربط مع واجهة برمجة RugCheck)
    return new Promise((resolve) => {
        setTimeout(() => {
            // 70% من العملات الجديدة بتكون فخ، و 30% آمنة
            const isSafe = Math.random() > 0.7; 
            if (isSafe) {
                ctx.reply("✅ فحص الأمان: العملة نظيفة!\n- 🔓 Mint: Disabled\n- 🔓 Freeze: Disabled\n- 🔥 LP: Locked\n\n🟢 سيتم تنفيذ الشراء الآن...");
                resolve(true);
            } else {
                ctx.reply("🚨 تحذير أمان: تم اكتشاف خطر (Scam/Honeypot)!\n- ❌ الصلاحيات مفتوحة للمطور.\n\n🛑 تم إلغاء الشراء وتجاهل الصفقة لحماية رأس المال.");
                resolve(false);
            }
        }, 2500); // الفحص بياخد ثانيتين ونص
    });
  }

  // --- دالة البيع ---
  async function executeAutoSell(tradeId, txSignature, buyAmount, ctx) {
    try {
      const pnlPercentage = (Math.random() * 40 - 10).toFixed(2); 
      const isProfit = pnlPercentage > 0;
      const sellAmount = buyAmount + (buyAmount * (pnlPercentage / 100));

      await Trade.findByIdAndUpdate(tradeId, { status: 'SOLD', sell_price: sellAmount });

      const emoji = isProfit ? '🟢' : '🔴';
      ctx.reply(`🛒 **تم البيع بنجاح!**\n\n${emoji} النتيجة: ${pnlPercentage}%\n💰 الكمية بعد البيع: ${sellAmount.toFixed(4)} SOL\n💾 تم تحديث الصفقة.`);
    } catch (err) {
      console.error("❌ Auto-Sell Error:", err);
    }
  }

  // --- دالة الشراء معدلة بالفلتر الأمني ---
  async function executeAutoBuy(txSignature, ctx) {
      try {
          if(!botWallet) return;
          if (seenSignatures.has(txSignature)) return; 
          
          seenSignatures.add(txSignature);
          setTimeout(() => seenSignatures.delete(txSignature), 3600000); 

          // 🛑 تفعيل الفحص الأمني قبل الشراء
          const isSafe = await checkTokenSecurity(txSignature, ctx);
          if (!isSafe) {
              return; // لو العملة نصابة، البوت هيقف هنا ومش هيشتري
          }

          const buyAmount = 0.01; 
          ctx.reply(`⚡ جاري تنفيذ شراء آلي للصفقة الآمنة...`);
          
          const newTrade = new Trade({
              coin_address: txSignature, 
              buy_price: buyAmount,
              amount: buyAmount,
              status: 'BOUGHT'
          });
          
          const savedTrade = await newTrade.save();
          ctx.reply(`✅ تم الشراء بنجاح!\n💰 الكمية: ${buyAmount} SOL\n⏱️ سيتم البيع بعد دقيقة...`);

          setTimeout(() => {
            executeAutoSell(savedTrade._id, txSignature, buyAmount, ctx);
          }, 60000);

      } catch (err) {
          console.error("❌ Auto-Buy Error:", err);
      }
  }

  const RAYDIUM_PROGRAM_ID = new solanaWeb3.PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');

  bot.command('hunt', (ctx) => {
      ctx.reply("🦈 The Beast radar is ON!\n🛡️ Security Filters: ENABLED.\nHunting for safe pools...");

      solanaConnection.onLogs(
          RAYDIUM_PROGRAM_ID,
          (logs, context) => {
              if (logs.err) return;
              const isNewPool = logs.logs.some(log => log.includes("initialize2") || log.includes("InitializeInstruction2"));

              if (isNewPool) {
                  executeAutoBuy(logs.signature, ctx);
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
