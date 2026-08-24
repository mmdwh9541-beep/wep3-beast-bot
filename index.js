require('dotenv').config();
const express = require('express');
const { Telegraf } = require('telegraf');

const app = express();
const port = process.env.PORT || 10000; 

// التقاط أي خطأ عام عشان السيرفر ما يقفلش لوحده أبداً
process.on('uncaughtException', (err) => {
    console.error('🔥 Uncaught Exception:', err.message);
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('🔥 Unhandled Rejection at:', promise, 'reason:', reason);
});

app.get('/', (req, res) => {
    res.send('✅ Web3 Beast is LIVE and Bulletproof!');
});

try {
    const telegramToken = process.env.TELEGRAM_TOKEN ? process.env.TELEGRAM_TOKEN.trim().replace(/['"]/g, '') : '';
    
    if (telegramToken) {
        const bot = new Telegraf(telegramToken);
        bot.start((ctx) => ctx.reply("Welcome Boss! The Beast is online 🚀"));
        bot.launch();
        console.log("✅ Telegram Bot launched successfully!");
    } else {
        console.log("⚠️ Telegram Token is missing, running web server only.");
    }
} catch (e) {
    console.log("⚠️ Bot startup warning:", e.message);
}

app.listen(port, () => {
    console.log(`✅ Bulletproof Server running smoothly on port ${port}...`);
});
