// 1. اصطياد أي أخطاء مخفية ومنع التطبيق من الإغلاق الصامت
process.on('uncaughtException', (err) => {
    console.error('🔥 خطأ فادح مخفي:', err);
});
process.on('unhandledRejection', (reason, promise) => {
    console.error('⚠️ خطأ في الوعود (Promise):', reason);
});

require('dotenv').config();
const express = require('express');
const { Telegraf } = require('telegraf');
const mongoose = require('mongoose');
const { Connection, Keypair } = require('@solana/web3.js');
const bip39 = require('bip39');
const { derivePath } = require('ed25519-hd-key');
const bs58 = require('bs58'); 

const app = express();
app.use(express.json());

const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const BOT_PRIVATE_KEY = process.env.BOT_PRIVATE_KEY;
const MONGODB_URI = process.env.MONGODB_URI;
const RPC_URL = process.env.RPC_URL || 'https://api.mainnet-beta.solana.com';
const PORT = process.env.PORT || 10000;

console.log('🚀 بدء تشغيل البوت...');

async function connectToDatabase() {
    try {
        if (!MONGODB_URI) throw new Error('رابط قاعدة البيانات غير موجود');
        await mongoose.connect(MONGODB_URI);
        console.log('✅ تم الاتصال بقاعدة البيانات!');
    } catch (error) {
        console.log('❌ خطأ في الاتصال بقاعدة البيانات:', error.message);
    }
}

let wallet = null;
async function loadWallet() {
    try {
        if (!BOT_PRIVATE_KEY) throw new Error('المفتاح الخاص غير موجود');
        const trimmedKey = BOT_PRIVATE_KEY.trim();
        
        if (trimmedKey.includes(' ')) {
            const seed = bip39.mnemonicToSeedSync(trimmedKey);
            const derivedSeed = derivePath("m/44'/501'/0'/0'", seed.toString('hex')).key;
            wallet = Keypair.fromSeed(derivedSeed);
        } else if (trimmedKey.startsWith('[')) {
            const secretKey = JSON.parse(trimmedKey);
            wallet = Keypair.fromSecretKey(Buffer.from(secretKey));
        } else {
            wallet = Keypair.fromSecretKey(bs58.decode(trimmedKey));
        }
        console.log('✅ تم تحميل المحفظة:', wallet.publicKey.toString());
    } catch (error) {
        console.log('❌ خطأ في تحميل المحفظة:', error.message);
    }
}

let bot;
try {
    bot = new Telegraf(TELEGRAM_TOKEN);
    
    bot.start((ctx) => ctx.reply('🤖 **مرحباً بك في بوت التداول!**'));
    bot.command('status', (ctx) => ctx.reply('✅ البوت يعمل'));
    bot.command('balance', async (ctx) => {
        if (!wallet) return ctx.reply('❌ المحفظة غير مهيأة');
        try {
            const connection = new Connection(RPC_URL);
            const balance = await connection.getBalance(wallet.publicKey);
            ctx.reply(`💰 الرصيد: ${(balance / 1e9).toFixed(4)} SOL`);
        } catch (error) {
            ctx.reply('❌ خطأ في جلب الرصيد');
        }
    });
} catch (error) {
    console.error('❌ خطأ في إعداد التليجرام:', error.message);
}

// === التغيير الجذري هنا ===
async function main() {
    // 1. فتح المنفذ فوراً لكي لا تغلق منصة Render التطبيق
    app.listen(PORT, '0.0.0.0', () => {
        console.log(`🌐 السيرفر يعمل على المنفذ ${PORT} - Render راضٍ الآن!`);
    });

    app.get('/', (req, res) => res.send('✅ البوت يعمل بنجاح!'));
    
    // 2. الاتصال بباقي الخدمات بعد فتح المنفذ
    await connectToDatabase();
    await loadWallet();
    
    if (bot) {
        bot.launch().then(() => {
            console.log('✅ تم تشغيل بوت التليجرام بنجاح!');
        }).catch(err => {
            console.error('❌ فشل تشغيل بوت التليجرام:', err.message);
        });
    }
}

main().catch(error => {
    console.error('❌ خطأ قاتل في التشغيل:', error);
});
