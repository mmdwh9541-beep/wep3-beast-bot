require('dotenv').config();
const express = require('express');
const { Telegraf } = require('telegraf');
const solanaWeb3 = require('@solana/web3.js');
const mongoose = require('mongoose');
const bip39 = require('bip39');
const { derivePath } = require('ed25519-hd-key');

const app = express();
const port = process.env.PORT || 3000; 

const telegramToken = process.env.TELEGRAM_TOKEN ? process.env.TELEGRAM_TOKEN.trim().replace(/['"]/g, '') : null;
const botPrivateKey = process.env.BOT_PRIVATE_KEY ? process.env.BOT_PRIVATE_KEY.trim() : null;
const mongoURI = process.env.MONGO_URI ? process.env.MONGO_URI.trim().replace(/['"]/g, '') : null;

if (!telegramToken || !mongoURI || !botPrivateKey) {
    console.error("❌ خطأ: المتغيرات السرية ناقصة.");
    process.exit(1);
}

const seenSignatures = new Set(); 
const MAX_TRADES = 5; 
let currentTradesCount = 0; 

// ⚙️ إعدادات القناص (الاستوب لوز المتحرك)
const TRAILING_STOP_PERCENT = 15; // هيبيع لو السعر نزل 15% من أعلى قمة وصلها
const WHALE_DUMP_ALERT = 25; // لو السعر نزل 25% في ضربة واحدة يبيع طوارئ فوراً

try {
  mongoose.connect(mongoURI).then(() => console.log("☁️  Cloud Memory Connected!"));

  const tradeSchema = new mongoose.Schema({
    coin_address: String,
    buy_price_sol: Number,
    token_amount: Number,
    status: { type: String, default: 'OPEN' }, 
    trade_time: { type: Date, default: Date.now }
  });
  const Trade = mongoose.model('Trade', tradeSchema);

  const bot = new Telegraf(telegramToken);
  const solanaConnection = new solanaWeb3.Connection(solanaWeb3.clusterApiUrl('mainnet-beta'), 'confirmed');

  let botWallet;
  const seed = bip39.mnemonicToSeedSync(botPrivateKey);
  const derivedSeed = derivePath("m/44'/501'/0'/0'", seed.toString('hex')).key;
  botWallet = solanaWeb3.Keypair.fromSeed(derivedSeed);

  app.get('/', (req, res) => res.send('✅ Sniper Engine LIVE with Trailing Stop!'));

  bot.start((ctx) => ctx.reply("Welcome Boss! 💸\n🎯 SNIPER MODE: Trailing Stop Loss Active.\nType /hunt to start."));

  async function checkTokenSecurityReal(tokenMint) {
      try {
          const accountInfo = await solanaConnection.getParsedAccountInfo(new solanaWeb3.PublicKey(tokenMint));
          if (!accountInfo || !accountInfo.value) return false;
          const data = accountInfo.value.data.parsed.info;
          if (data.mintAuthority !== null || data.freezeAuthority !== null) return false; 
          return true; 
      } catch (err) {
          return false;
      }
  }

  async function executeRealBuy(tokenMint, ctx) {
      if(currentTradesCount >= MAX_TRADES || seenSignatures.has(tokenMint)) return;
      seenSignatures.add(tokenMint);

      const isSafe = await checkTokenSecurityReal(tokenMint);
      if (!isSafe) {
          ctx.reply(`🚨 تجاهل عملة مزيفة (صلاحيات مفتوحة):\n${tokenMint}`);
          return;
      }

      const balance = await solanaConnection.getBalance(botWallet.publicKey);
      const availableBalance = (balance / solanaWeb3.LAMPORTS_PER_SOL) - 0.005; 
      if (availableBalance <= 0) return;

      const buyAmountSOL = parseFloat((availableBalance / 5).toFixed(4)); 
      const lamportsToTrade = Math.floor(buyAmountSOL * solanaWeb3.LAMPORTS_PER_SOL);
      
      ctx.reply(`⚡ جاري القنص بـ ${buyAmountSOL} SOL...\nالعنوان: ${tokenMint}`);

      try {
          const quoteUrl = `https://quote-api.jup.ag/v6/quote?inputMint=So11111111111111111111111111111111111111112&outputMint=${tokenMint}&amount=${lamportsToTrade}&slippageBps=150`;
          const quoteResponse = await (await fetch(quoteUrl)).json();
          
          const swapResponse = await (await fetch('https://quote-api.jup.ag/v6/swap', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                  quoteResponse,
                  userPublicKey: botWallet.publicKey.toString(),
                  wrapAndUnwrapSol: true,
                  prioritizationFeeLamports: 100000 // رسوم أولوية عالية للقنص السريع
              })
          })).json();

          const swapTransactionBuf = Buffer.from(swapResponse.swapTransaction, 'base64');
          const transaction = solanaWeb3.VersionedTransaction.deserialize(swapTransactionBuf);
          transaction.sign([botWallet]);
          
          const realSignature = await solanaConnection.sendRawTransaction(transaction.serialize(), { skipPreflight: true });
          const tokenAmountReceived = quoteResponse.outAmount;

          const newTrade = new Trade({ coin_address: tokenMint, buy_price_sol: buyAmountSOL, token_amount: tokenAmountReceived, status: 'BOUGHT' });
          await newTrade.save();
          currentTradesCount++;

          ctx.reply(`✅ **تمت الضربة بنجاح!** 💸\nالرابط: https://solscan.io/tx/${realSignature}\n👀 تفعيل رادار الحيتان والاستوب المتحرك...`);
          
          // تفعيل القناص المتحرك
          sniperTrailingMonitor(newTrade._id, tokenMint, buyAmountSOL, tokenAmountReceived, ctx);

      } catch (err) {
          ctx.reply(`❌ فشل أمر الشراء.`);
      }
  }

  // --- 🎯 محرك القناص (الاستوب لوز المتحرك وحماية الحيتان) ---
  function sniperTrailingMonitor(tradeId, tokenMint, buyPriceSol, tokenAmount, ctx) {
      let highestPriceSol = buyPriceSol; // تسجيل أعلى سعر وصلتله العملة
      
      const monitorInterval = setInterval(async () => {
          try {
              const quoteUrl = `https://quote-api.jup.ag/v6/quote?inputMint=${tokenMint}&outputMint=So11111111111111111111111111111111111111112&amount=${tokenAmount}&slippageBps=200`;
              const quoteResponse = await (await fetch(quoteUrl)).json();
              if(!quoteResponse || !quoteResponse.outAmount) return;

              const currentValSol = quoteResponse.outAmount / solanaWeb3.LAMPORTS_PER_SOL;
              
              // تحديث أعلى سعر لو العملة بتصعد
              if (currentValSol > highestPriceSol) {
                  highestPriceSol = currentValSol;
              }

              // حساب نسبة الهبوط من "القمة" مش من سعر الشراء
              const dropFromHigh = ((highestPriceSol - currentValSol) / highestPriceSol) * 100;
              // حساب الربح الإجمالي الحالي للمعلومات
              const totalPnl = ((currentValSol - buyPriceSol) / buyPriceSol) * 100;

              // شرط 1: لو السعر نزل عن القمة بنسبة الاستوب المتحرك (Trailing Stop)
              // شرط 2: لو حوت رمى سيولة مرعبة في ثانية (Whale Dump)
              if (dropFromHigh >= TRAILING_STOP_PERCENT || dropFromHigh >= WHALE_DUMP_ALERT) {
                  clearInterval(monitorInterval); 
                  
                  ctx.reply(`🚨 رادار القناص إشتغل!\n📉 هبوط من القمة: -${dropFromHigh.toFixed(2)}%\n💰 الربح المحجوز: ${totalPnl.toFixed(2)}%\n🛒 جاري البيع للهروب بالربح...`);

                  const swapResponse = await (await fetch('https://quote-api.jup.ag/v6/swap', {
                      method: 'POST',
                      headers: { 'Content-Type': 'application/json' },
                      body: JSON.stringify({
                          quoteResponse,
                          userPublicKey: botWallet.publicKey.toString(),
                          wrapAndUnwrapSol: true,
                          prioritizationFeeLamports: 150000 // أولوية قصوى للهروب
                      })
                  })).json();

                  const swapTransactionBuf = Buffer.from(swapResponse.swapTransaction, 'base64');
                  const transaction = solanaWeb3.VersionedTransaction.deserialize(swapTransactionBuf);
                  transaction.sign([botWallet]);
                  
                  const sellSignature = await solanaConnection.sendRawTransaction(transaction.serialize(), { skipPreflight: true });

                  await Trade.findByIdAndUpdate(tradeId, { status: 'SOLD' });
                  currentTradesCount--;

                  ctx.reply(`🛒 **تم البيع بنجاح والخروج من السوق!**\nالرابط: https://solscan.io/tx/${sellSignature}`);
              }
          } catch (error) {
              console.log("Error monitoring sniper radar...");
          }
      }, 5000); // يفحص كل 5 ثواني عشان يسبق الحيتان
  }

  const RAYDIUM_PROGRAM_ID = new solanaWeb3.PublicKey('675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8');
  bot.command('hunt', (ctx) => {
      ctx.reply("🦈 Sniper Mode: ON\nTarget: Ride the pump, dump on the whales.");
      solanaConnection.onLogs(RAYDIUM_PROGRAM_ID, (logs) => {
          if (logs.err || currentTradesCount >= MAX_TRADES) return;
          // تنفيذ أمر الشراء فور التقاط عملة جديدة
          // executeRealBuy(ExtractedTokenMint, ctx); 
      }, 'confirmed');
  });

  bot.launch();
  app.listen(port, () => console.log(`✅ Sniper Engine running...`));

} catch (error) {
  console.log("❌ ERROR", error.message);
}
