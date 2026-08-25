// ======================================================
// LOMY SOLANA HUNTER
// Render-Safe Core V1
// MODE: PAPER / NO LIVE TRADING
// ======================================================

require('dotenv').config();

const express = require('express');
const { Telegraf } = require('telegraf');
const mongoose = require('mongoose');
const { Connection, Keypair, LAMPORTS_PER_SOL } = require('@solana/web3.js');
const bip39 = require('bip39');
const { derivePath } = require('ed25519-hd-key');
const bs58 = require('bs58');

// ======================================================
// APP
// ======================================================

const app = express();
app.use(express.json());

// ======================================================
// ENVIRONMENT
// ======================================================

const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const BOT_PRIVATE_KEY = process.env.BOT_PRIVATE_KEY;
const MONGODB_URI = process.env.MONGODB_URI;

const RPC_URL =
    process.env.RPC_URL ||
    'https://api.mainnet-beta.solana.com';

const PORT = Number(process.env.PORT) || 10000;

// ======================================================
// IMPORTANT SAFETY LOCK
// ======================================================

// التداول الحقيقي مقفول تماماً في هذه النسخة.
// حتى لو تم إضافة TRADING_MODE=LIVE في Render
// فلن يتم تنفيذ أي تداول.

const TRADING_MODE = 'PAPER';
const LIVE_TRADING_ENABLED = false;

// ======================================================
// SYSTEM STATE
// ======================================================

const systemState = {
    server: 'starting',
    database: 'disconnected',
    wallet: 'not_loaded',
    solana: 'disconnected',
    telegram: 'stopped',
    trading: 'disabled',
    mode: TRADING_MODE,
    ready: false,
    startedAt: new Date().toISOString(),
    lastError: null
};

// ======================================================
// GLOBAL VARIABLES
// ======================================================

let wallet = null;
let bot = null;
let httpServer = null;
let telegramStarted = false;
let shuttingDown = false;

// Connection واحدة فقط طول عمر البرنامج
const connection = new Connection(
    RPC_URL,
    'confirmed'
);

// ======================================================
// LOGGING
// ======================================================

function logInfo(message) {
    console.log(
        `[${new Date().toISOString()}] ℹ️ ${message}`
    );
}

function logSuccess(message) {
    console.log(
        `[${new Date().toISOString()}] ✅ ${message}`
    );
}

function logWarning(message) {
    console.warn(
        `[${new Date().toISOString()}] ⚠️ ${message}`
    );
}

function logError(message, error = null) {

    if (error) {

        console.error(
            `[${new Date().toISOString()}] ❌ ${message}`,
            error
        );

        systemState.lastError =
            error?.message || String(error);

    } else {

        console.error(
            `[${new Date().toISOString()}] ❌ ${message}`
        );

        systemState.lastError = message;
    }
}

// ======================================================
// UPDATE READY STATE
// ======================================================

function updateReadyState() {

    const coreReady =
        systemState.server === 'online' &&
        systemState.database === 'connected' &&
        systemState.wallet === 'loaded' &&
        systemState.solana === 'connected';

    systemState.ready = coreReady;

    // التداول يظل مقفول
    systemState.trading = 'disabled';
}

// ======================================================
// HTTP ROUTES
// ======================================================

app.get('/', (req, res) => {

    res.status(200).send(
        '✅ LOMY Solana Hunter is running - PAPER MODE'
    );
});

// ======================================================
// HEALTH CHECK
// ======================================================

app.get('/health', async (req, res) => {

    updateReadyState();

    const statusCode =
        systemState.server === 'online'
            ? 200
            : 503;

    res.status(statusCode).json({

        service: 'LOMY Solana Hunter',

        server: systemState.server,

        database: systemState.database,

        wallet: systemState.wallet,

        solana: systemState.solana,

        telegram: systemState.telegram,

        trading: systemState.trading,

        tradingMode: TRADING_MODE,

        liveTradingEnabled: LIVE_TRADING_ENABLED,

        ready: systemState.ready,

        uptimeSeconds:
            Math.floor(process.uptime()),

        startedAt:
            systemState.startedAt,

        lastError:
            systemState.lastError
    });
});

// ======================================================
// BOT STATUS
// ======================================================

app.get('/api/status', (req, res) => {

    updateReadyState();

    res.json({
        success: true,
        mode: TRADING_MODE,
        liveTrading: LIVE_TRADING_ENABLED,
        ready: systemState.ready,
        services: {
            database: systemState.database,
            wallet: systemState.wallet,
            solana: systemState.solana,
            telegram: systemState.telegram
        }
    });
});

// ======================================================
// START HTTP SERVER FIRST
// ======================================================

function startHttpServer() {

    return new Promise((resolve, reject) => {

        httpServer = app.listen(
            PORT,
            '0.0.0.0',
            () => {

                systemState.server = 'online';

                logSuccess(
                    `Render HTTP Server يعمل على PORT ${PORT}`
                );

                updateReadyState();

                resolve();
            }
        );

        httpServer.on('error', (error) => {

            systemState.server = 'error';

            logError(
                'فشل تشغيل HTTP Server',
                error
            );

            reject(error);
        });
    });
}

// ======================================================
// MONGODB EVENTS
// ======================================================

mongoose.connection.on('connected', () => {

    systemState.database = 'connected';

    logSuccess(
        'MongoDB connected'
    );

    updateReadyState();
});

mongoose.connection.on('disconnected', () => {

    systemState.database = 'disconnected';

    logWarning(
        'MongoDB disconnected'
    );

    updateReadyState();
});

mongoose.connection.on('reconnected', () => {

    systemState.database = 'connected';

    logSuccess(
        'MongoDB reconnected'
    );

    updateReadyState();
});

mongoose.connection.on('error', (error) => {

    systemState.database = 'error';

    logError(
        'MongoDB connection error',
        error
    );

    updateReadyState();
});

// ======================================================
// CONNECT DATABASE
// ======================================================

async function connectToDatabase() {

    if (!MONGODB_URI) {

        systemState.database = 'missing_config';

        throw new Error(
            'MONGODB_URI غير موجود في Render'
        );
    }

    try {

        systemState.database = 'connecting';

        logInfo(
            'جاري الاتصال بـ MongoDB...'
        );

        await mongoose.connect(
            MONGODB_URI,
            {
                serverSelectionTimeoutMS: 10000
            }
        );

        systemState.database = 'connected';

        logSuccess(
            'تم الاتصال بقاعدة البيانات'
        );

    } catch (error) {

        systemState.database = 'error';

        logError(
            'فشل الاتصال بقاعدة البيانات',
            error
        );

        // لا نغلق Render
        // ولكن التداول سيظل ممنوعاً
    }

    updateReadyState();
}

// ======================================================
// LOAD WALLET
// ======================================================

async function loadWallet() {

    try {

        if (!BOT_PRIVATE_KEY) {

            throw new Error(
                'BOT_PRIVATE_KEY غير موجود في Render'
            );
        }

        const trimmedKey =
            BOT_PRIVATE_KEY.trim();

        // ==============================================
        // Mnemonic Seed Phrase
        // ==============================================

        if (
            trimmedKey.includes(' ') &&
            bip39.validateMnemonic(trimmedKey)
        ) {

            const seed =
                bip39.mnemonicToSeedSync(
                    trimmedKey
                );

            const derivedSeed =
                derivePath(
                    "m/44'/501'/0'/0'",
                    seed.toString('hex')
                ).key;

            wallet =
                Keypair.fromSeed(
                    derivedSeed
                );
        }

        // ==============================================
        // JSON Secret Key
        // ==============================================

        else if (
            trimmedKey.startsWith('[')
        ) {

            const secretKey =
                JSON.parse(
                    trimmedKey
                );

            wallet =
                Keypair.fromSecretKey(
                    Uint8Array.from(
                        secretKey
                    )
                );
        }

        // ==============================================
        // Base58 Secret Key
        // ==============================================

        else {

            const decoded =
                bs58.decode(
                    trimmedKey
                );

            wallet =
                Keypair.fromSecretKey(
                    decoded
                );
        }

        systemState.wallet = 'loaded';

        logSuccess(
            `المحفظة تم تحميلها: ${wallet.publicKey.toString()}`
        );

    } catch (error) {

        wallet = null;

        systemState.wallet = 'error';

        logError(
            'فشل تحميل المحفظة',
            error
        );
    }

    updateReadyState();
}

// ======================================================
// TEST SOLANA RPC
// ======================================================

async function testSolanaConnection() {

    if (!wallet) {

        systemState.solana =
            'wallet_unavailable';

        logWarning(
            'لن يتم اختبار Solana لأن المحفظة غير متاحة'
        );

        updateReadyState();

        return;
    }

    try {

        logInfo(
            'جاري اختبار Solana RPC...'
        );

        const balance =
            await connection.getBalance(
                wallet.publicKey
            );

        const solBalance =
            balance / LAMPORTS_PER_SOL;

        systemState.solana =
            'connected';

        logSuccess(
            `Solana RPC متصل`
        );

        logSuccess(
            `رصيد المحفظة: ${solBalance.toFixed(6)} SOL`
        );

    } catch (error) {

        systemState.solana = 'error';

        logError(
            'فشل الاتصال بـ Solana RPC',
            error
        );
    }

    updateReadyState();
}

// ======================================================
// TELEGRAM
// ======================================================

async function startTelegram() {

    if (!TELEGRAM_TOKEN) {

        systemState.telegram =
            'missing_config';

        logWarning(
            'TELEGRAM_TOKEN غير موجود'
        );

        return;
    }

    try {

        bot =
            new Telegraf(
                TELEGRAM_TOKEN
            );

        // ==============================================
        // START
        // ==============================================

        bot.start(async (ctx) => {

            await ctx.reply(
                '🤖 LOMY Solana Hunter\n\n' +
                '🧪 الوضع الحالي: PAPER MODE\n' +
                '🔒 التداول الحقيقي: مقفول'
            );
        });

        // ==============================================
        // STATUS
        // ==============================================

        bot.command(
            'status',
            async (ctx) => {

                updateReadyState();

                await ctx.reply(

                    `🤖 LOMY STATUS\n\n` +

                    `🌐 Server: ${systemState.server}\n` +

                    `🗄 Database: ${systemState.database}\n` +

                    `👛 Wallet: ${systemState.wallet}\n` +

                    `⚡ Solana: ${systemState.solana}\n` +

                    `📡 Telegram: ${systemState.telegram}\n\n` +

                    `🧪 Mode: ${TRADING_MODE}\n` +

                    `🔒 Live Trading: OFF`
                );
            }
        );

        // ==============================================
        // BALANCE
        // ==============================================

        bot.command(
            'balance',
            async (ctx) => {

                if (!wallet) {

                    return ctx.reply(
                        '❌ المحفظة غير مهيأة'
                    );
                }

                try {

                    const balance =
                        await connection.getBalance(
                            wallet.publicKey
                        );

                    const sol =
                        balance /
                        LAMPORTS_PER_SOL;

                    await ctx.reply(
                        `💰 الرصيد: ${sol.toFixed(6)} SOL`
                    );

                } catch (error) {

                    logError(
                        'خطأ في أمر balance',
                        error
                    );

                    await ctx.reply(
                        '❌ تعذر قراءة الرصيد'
                    );
                }
            }
        );

        // ==============================================
        // MODE
        // ==============================================

        bot.command(
            'mode',
            async (ctx) => {

                await ctx.reply(
                    '🧪 PAPER MODE\n\n' +
                    '❌ الشراء الحقيقي: OFF\n' +
                    '❌ البيع الحقيقي: OFF\n' +
                    '✅ التحليل والاختبار فقط'
                );
            }
        );

        // ==============================================
        // TELEGRAM ERROR HANDLER
        // ==============================================

        bot.catch(
            (error, ctx) => {

                logError(
                    `Telegram error (${ctx.updateType})`,
                    error
                );
            }
        );

        systemState.telegram =
            'starting';

        await bot.launch();

        telegramStarted = true;

        systemState.telegram =
            'online';

        logSuccess(
            'Telegram Bot يعمل'
        );

    } catch (error) {

        telegramStarted = false;

        systemState.telegram =
            'error';

        logError(
            'فشل تشغيل Telegram',
            error
        );
    }

    updateReadyState();
}

// ======================================================
// GRACEFUL SHUTDOWN
// ======================================================

async function shutdown(signal) {

    if (shuttingDown) {
        return;
    }

    shuttingDown = true;

    logWarning(
        `استلام ${signal} - جاري الإغلاق الآمن`
    );

    systemState.ready = false;

    // التداول غير موجود أصلاً في V1
    systemState.trading = 'disabled';

    try {

        if (
            bot &&
            telegramStarted
        ) {

            bot.stop(signal);

            telegramStarted = false;

            systemState.telegram =
                'stopped';

            logSuccess(
                'Telegram stopped'
            );
        }

    } catch (error) {

        logError(
            'خطأ أثناء إغلاق Telegram',
            error
        );
    }

    try {

        if (
            mongoose.connection.readyState !== 0
        ) {

            await mongoose.disconnect();

            logSuccess(
                'MongoDB disconnected safely'
            );
        }

    } catch (error) {

        logError(
            'خطأ أثناء إغلاق MongoDB',
            error
        );
    }

    try {

        if (httpServer) {

            httpServer.close(
                () => {

                    logSuccess(
                        'HTTP Server stopped'
                    );

                    process.exit(0);
                }
            );

            // حماية لو السيرفر لم يغلق
            setTimeout(
                () => process.exit(0),
                5000
            );

        } else {

            process.exit(0);
        }

    } catch (error) {

        logError(
            'خطأ أثناء إغلاق HTTP Server',
            error
        );

        process.exit(1);
    }
}

// ======================================================
// PROCESS ERRORS
// ======================================================

process.on(
    'unhandledRejection',
    (reason) => {

        logError(
            'Unhandled Promise Rejection',
            reason instanceof Error
                ? reason
                : new Error(String(reason))
        );
    }
);

process.on(
    'uncaughtException',
    async (error) => {

        logError(
            'Uncaught Exception',
            error
        );

        // الخطأ غير المتوقع قد يترك البرنامج
        // في حالة غير آمنة.
        // Render سيعيد تشغيله بعد الخروج.

        await shutdown(
            'uncaughtException'
        );
    }
);

process.once(
    'SIGTERM',
    () => shutdown('SIGTERM')
);

process.once(
    'SIGINT',
    () => shutdown('SIGINT')
);

// ======================================================
// MAIN
// ======================================================

async function main() {

    console.log('');
    console.log('========================================');
    console.log('🚀 LOMY SOLANA HUNTER');
    console.log('🧪 PAPER MODE');
    console.log('🔒 LIVE TRADING DISABLED');
    console.log('========================================');
    console.log('');

    // ==============================================
    // أهم خطوة لـ Render
    // السيرفر يفتح أولاً
    // ==============================================

    await startHttpServer();

    // ==============================================
    // بعد فتح PORT نبدأ الخدمات
    // ==============================================

    await connectToDatabase();

    await loadWallet();

    await testSolanaConnection();

    await startTelegram();

    updateReadyState();

    console.log('');
    console.log('========================================');

    if (systemState.ready) {

        logSuccess(
            'CORE SYSTEM READY'
        );

    } else {

        logWarning(
            'CORE SYSTEM يعمل لكن بعض الخدمات غير جاهزة'
        );
    }

    console.log(
        '🔒 التداول الحقيقي مازال مقفولاً'
    );

    console.log('========================================');
    console.log('');
}

// ======================================================
// START
// ======================================================

main().catch(
    async (error) => {

        logError(
            'خطأ في MAIN',
            error
        );

        // HTTP Server قد يكون شغال
        // فلا نسمح بسقوط صامت.

        updateReadyState();
    }
);
