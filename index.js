require('dotenv').config();
const express = require('express');
const { Telegraf } = require('telegraf');
const mongoose = require('mongoose');
const { Connection, Keypair, PublicKey } = require('@solana/web3.js');
const bip39 = require('bip39');
const { derivePath } = require('ed25519-hd-key');

const app = express();
app.use(express.json());

const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const BOT_PRIVATE_KEY = process.env.BOT_PRIVATE_KEY;
const MONGODB_URI = process.env.MONGODB_URI;
const RPC_URL = process.env.RPC_URL || 'https://api.mainnet-beta.solana.com';
const PORT = process.env.PORT || 10000;

console.log('🚀 بدء تشغيل البوت...');
console.log('📊 التحقق من الإعدادات:');
console.log('✅ تليجرام:', TELEGRAM_TOKEN ? 'موجود' : 'مفقود!');
console.log('✅ الكلمات السرية:', BOT_PRIVATE_KEY ? 'موجودة' : 'مفقودة!');
console.log('✅ قاعدة البيانات:', MONGODB_URI ? 'موجودة' : 'مفقودة!');

async function connectToDatabase() {
    try {
        await mongoose.connect(MONGODB_URI);
        console.log('✅ تم الاتصال بقاعدة البيانات!');
    } catch (error) {
        console.log('❌ خطأ في الاتصال بقاعدة البيانات:', error.message);
    }
}

let wallet = null;
async function loadWallet() {
    try {
        // التحقق من نوع المفتاح
        const trimmedKey = BOT_PRIVATE_KEY.trim();
        
        if (trimmedKey.includes(' ')) {
            // المفتاح عبارة عن 24 كلمة (Mnemonic)
            console.log('📝 تم اكتشاف 24 كلمة سرية...');
            const seed = bip39.mnemonicToSeedSync(trimmedKey);
            const derivedSeed = derivePath("m/44'/501'/0'/0'", seed.toString('hex')).key;
            wallet = Keypair.fromSeed(derivedSeed);
        } else if (trimmedKey.startsWith('[')) {
            // المفتاح عبارة عن مصفوفة أرقام
            console.log('🔢 تم اكتشاف مصفوفة أرقام...');
            const secretKey = JSON.parse(trimmedKey);
            wallet = Keypair.fromSecretKey(Buffer.from(secretKey));
        } else {
            // محاولة تحويل من Base58
            console.log('🔤 تم اكتشاف مفتاح Base58...');
            wallet = Keypair.fromSecretKey(Buffer.from(trimmedKey, 'base58'));
        }
        
        console.log('✅ تم تحميل المحفظة:', wallet.publicKey.toString());
    } catch (error) {
        console.log('❌ خطأ في تحميل المحفظة:', error.message);
        console.log('💡 تأكد من صحة الكلمات السرية أو المفتاح الخاص');
    }
}

const bot = new Telegraf(TELEGRAM_TOKEN);

bot.start((ctx) => {
    ctx.reply(
        '🤖 **مرحباً بك في بوت التداول!**\n\n' +
        '📊 الأوامر المتاحة:\n' +
        '/status - حالة البوت\n' +
        '/balance - رصيد المحفظة\n' +
        '/help - المساعدة'
    );
});

bot.command('status', (ctx) => {
    ctx.reply(
        '📊 **حالة البوت**\n\n' +
        '✅ البوت يعمل\n' +
        '🔗 الشبكة: Solana Mainnet\n' +
        '💾 قاعدة البيانات: متصلة'
    );
});

bot.command('balance', async (ctx) => {
    if (!wallet) {
        return ctx.reply('❌ المحفظة غير مهيأة');
    }
    
    try {
        const connection = new Connection(RPC_URL);
        const balance = await connection.getBalance(wallet.publicKey);
        const solBalance = balance / 1e9;
        
        ctx.reply(
            `💰 **رصيد المحفظة**\n\n` +
            `💎 الرصيد: ${solBalance.toFixed(4)} SOL\n` +
            `📮 العنوان: ${wallet.publicKey.toString().slice(0, 20)}...`
        );
    } catch (error) {
        ctx.reply('❌ خطأ في جلب الرصيد');
    }
});

bot.command('help', (ctx) => {
    ctx.reply(
        '📚 **المساعدة**\n\n' +
        '/start - بدء البوت\n' +
