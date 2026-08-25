// ======================================================
// LOMY SOLANA HUNTER V2
// Render-Safe Core + Fresh Token Hunter V1
// MODE: PAPER / ABSOLUTELY NO LIVE TRADING
// ======================================================

require('dotenv').config();

const express = require('express');
const { Telegraf } = require('telegraf');
const mongoose = require('mongoose');

const {
    Connection,
    Keypair,
    PublicKey,
    LAMPORTS_PER_SOL
} = require('@solana/web3.js');

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
// HARD SAFETY LOCK
// ======================================================

// مهم:
// لا توجد أي دالة BUY أو SELL في هذا الكود.
// التداول الحقيقي مستحيل في هذه النسخة.

const TRADING_MODE = 'PAPER';
const LIVE_TRADING_ENABLED = false;

// ======================================================
// TOKEN PROGRAMS
// ======================================================

const TOKEN_PROGRAM_ID = new PublicKey(
    'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
);

const TOKEN_2022_PROGRAM_ID = new PublicKey(
    'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb'
);

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

    startedAt:
        new Date().toISOString(),

    lastError: null
};

// ======================================================
// HUNTER STATE
// ======================================================

const hunterState = {

    status: 'stopped',

    detected: 0,

    saved: 0,

    duplicates: 0,

    errors: 0,

    startedAt: null,

    lastDetectedAt: null,

    lastMint: null,

    lastSignature: null
};

// ======================================================
// GLOBALS
// ======================================================

let wallet = null;

let bot = null;

let httpServer = null;

let telegramStarted = false;

let shuttingDown = false;

let tokenProgramSubscription = null;

let token2022Subscription = null;

// لمنع معالجة نفس transaction أكثر من مرة
const processingSignatures = new Set();

// ======================================================
// SOLANA CONNECTION
// ======================================================

const connection = new Connection(
    RPC_URL,
    {
        commitment: 'confirmed'
    }
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
// READY STATE
// ======================================================

function updateReadyState() {

    systemState.ready =

        systemState.server === 'online' &&

        systemState.database === 'connected' &&

        systemState.wallet === 'loaded' &&

        systemState.solana === 'connected';

    // التداول يظل مغلقاً مهما حدث
    systemState.trading = 'disabled';
}

// ======================================================
// MONGODB MODEL
// ======================================================

const freshTokenSchema =
    new mongoose.Schema(
        {

            mint: {
                type: String,
                required: true,
                unique: true,
                index: true
            },

            signature: {
                type: String,
                index: true
            },

            slot: {
                type: Number
            },

            tokenProgram: {
                type: String
            },

            detectedAt: {
                type: Date,
                default: Date.now,
                index: true
            },

            status: {
                type: String,
                default: 'NEW'
            },

            securityChecked: {
                type: Boolean,
                default: false
            },

            paperOnly: {
                type: Boolean,
                default: true
            }

        },
        {
            timestamps: true
        }
    );

const FreshToken =
    mongoose.models.FreshToken ||
    mongoose.model(
        'FreshToken',
        freshTokenSchema
    );

// ======================================================
// HTTP ROUTES
// ======================================================

app.get('/', (req, res) => {

    res.status(200).send(
        '✅ LOMY Solana Hunter V2 - PAPER MODE'
    );
});

// ======================================================
// HEALTH
// ======================================================

app.get('/health', (req, res) => {

    updateReadyState();

    res.status(200).json({

        service:
            'LOMY Solana Hunter V2',

        server:
            systemState.server,

        database:
            systemState.database,

        wallet:
            systemState.wallet,

        solana:
            systemState.solana,

        telegram:
            systemState.telegram,

        hunter:
            hunterState.status,

        hunterDetected:
            hunterState.detected,

        hunterSaved:
            hunterState.saved,

        trading:
            systemState.trading,

        tradingMode:
            TRADING_MODE,

        liveTradingEnabled:
            LIVE_TRADING_ENABLED,

        ready:
            systemState.ready,

        uptimeSeconds:
            Math.floor(
                process.uptime()
            ),

        startedAt:
            systemState.startedAt,

        lastError:
            systemState.lastError
    });
});

// ======================================================
// API STATUS
// ======================================================

app.get('/api/status', (req, res) => {

    updateReadyState();

    res.json({

        success: true,

        mode:
            TRADING_MODE,

        liveTrading:
            LIVE_TRADING_ENABLED,

        ready:
            systemState.ready,

        services: {

            database:
                systemState.database,

            wallet:
                systemState.wallet,

            solana:
                systemState.solana,

            telegram:
                systemState.telegram,

            hunter:
                hunterState.status
        },

        hunter: {
            detected:
                hunterState.detected,

            saved:
                hunterState.saved,

            duplicates:
                hunterState.duplicates,

            errors:
                hunterState.errors,

            lastMint:
                hunterState.lastMint
        }
    });
});

// ======================================================
// HUNTER API
// ======================================================

app.get('/api/hunter', async (req, res) => {

    let databaseCount = null;

    try {

        if (
            mongoose.connection.readyState === 1
        ) {

            databaseCount =
                await FreshToken.countDocuments();
        }

    } catch (error) {

        logError(
            'خطأ في عد Tokens',
            error
        );
    }

    res.json({

        success: true,

        status:
            hunterState.status,

        detected:
            hunterState.detected,

        saved:
            hunterState.saved,

        duplicates:
            hunterState.duplicates,

        errors:
            hunterState.errors,

        databaseTokens:
            databaseCount,

        startedAt:
            hunterState.startedAt,

        lastDetectedAt:
            hunterState.lastDetectedAt,

        lastMint:
            hunterState.lastMint,

        lastSignature:
            hunterState.lastSignature,

        trading:
            'DISABLED',

        mode:
            'PAPER'
    });
});

// ======================================================
// START HTTP SERVER
// ======================================================

function startHttpServer() {

    return new Promise(
        (resolve, reject) => {

            httpServer =
                app.listen(
                    PORT,
                    '0.0.0.0',
                    () => {

                        systemState.server =
                            'online';

                        logSuccess(
                            `Render HTTP Server يعمل على PORT ${PORT}`
                        );

                        updateReadyState();

                        resolve();
                    }
                );

            httpServer.on(
                'error',
                (error) => {

                    systemState.server =
                        'error';

                    logError(
                        'فشل تشغيل HTTP Server',
                        error
                    );

                    reject(error);
                }
            );
        }
    );
}

// ======================================================
// MONGODB EVENTS
// ======================================================

mongoose.connection.on(
    'connected',
    () => {

        systemState.database =
            'connected';

        logSuccess(
            'MongoDB connected'
        );

        updateReadyState();
    }
);

mongoose.connection.on(
    'disconnected',
    () => {

        systemState.database =
            'disconnected';

        logWarning(
            'MongoDB disconnected'
        );

        updateReadyState();
    }
);

mongoose.connection.on(
    'reconnected',
    () => {

        systemState.database =
            'connected';

        logSuccess(
            'MongoDB reconnected'
        );

        updateReadyState();
    }
);

mongoose.connection.on(
    'error',
    (error) => {

        systemState.database =
            'error';

        logError(
            'MongoDB connection error',
            error
        );

        updateReadyState();
    }
);

// ======================================================
// CONNECT DATABASE
// ======================================================

async function connectToDatabase() {

    if (!MONGODB_URI) {

        systemState.database =
            'missing_config';

        logError(
            'MONGODB_URI غير موجود في Render'
        );

        updateReadyState();

        return;
    }

    try {

        systemState.database =
            'connecting';

        logInfo(
            'جاري الاتصال بـ MongoDB...'
        );

        await mongoose.connect(
            MONGODB_URI,
            {
                serverSelectionTimeoutMS:
                    10000
            }
        );

        systemState.database =
            'connected';

        logSuccess(
            'تم الاتصال بقاعدة البيانات'
        );

    } catch (error) {

        systemState.database =
            'error';

        logError(
            'فشل الاتصال بقاعدة البيانات',
            error
        );
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

        // Seed Phrase
        if (
            trimmedKey.includes(' ') &&
            bip39.validateMnemonic(
                trimmedKey
            )
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

        // JSON Key
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

        // Base58
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

        systemState.wallet =
            'loaded';

        logSuccess(
            `المحفظة تم تحميلها: ${wallet.publicKey.toString()}`
        );

    } catch (error) {

        wallet = null;

        systemState.wallet =
            'error';

        logError(
            'فشل تحميل المحفظة',
            error
        );
    }

    updateReadyState();
}

// ======================================================
// TEST SOLANA
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
            balance /
            LAMPORTS_PER_SOL;

        systemState.solana =
            'connected';

        logSuccess(
            'Solana RPC متصل'
        );

        logSuccess(
            `رصيد المحفظة: ${solBalance.toFixed(6)} SOL`
        );

    } catch (error) {

        systemState.solana =
            'error';

        logError(
            'فشل الاتصال بـ Solana RPC',
            error
        );
    }

    updateReadyState();
}

// ======================================================
// FIND INITIALIZE MINT
// ======================================================

function findMintInInstructions(
    instructions
) {

    if (!Array.isArray(instructions)) {
        return null;
    }

    for (
        const instruction
        of instructions
    ) {

        if (!instruction) {
            continue;
        }

        if (
            instruction.parsed &&
            (
                instruction.parsed.type ===
                    'initializeMint' ||

                instruction.parsed.type ===
                    'initializeMint2'
            )
        ) {

            const mint =
                instruction.parsed.info
                    ?.mint;

            if (mint) {
                return mint;
            }
        }
    }

    return null;
}

// ======================================================
// PROCESS POSSIBLE MINT
// ======================================================

async function processPossibleNewMint(
    logEvent,
    programName
) {

    if (!logEvent) {
        return;
    }

    if (logEvent.err) {
        return;
    }

    const logs =
        logEvent.logs || [];

    const hasInitializeMint =
        logs.some(
            (line) =>
                typeof line ===
                    'string' &&
                (
                    line.includes(
                        'Instruction: InitializeMint'
                    ) ||
                    line.includes(
                        'Instruction: InitializeMint2'
                    )
                )
        );

    if (!hasInitializeMint) {
        return;
    }

    const signature =
        logEvent.signature;

    if (!signature) {
        return;
    }

    if (
        processingSignatures.has(
            signature
        )
    ) {
        return;
    }

    processingSignatures.add(
        signature
    );

    try {

        // أحياناً الـ log يصل قبل أن تصبح
        // transaction متاحة للقراءة.
        // لذلك نحاول عدة مرات.

        let transaction = null;

        for (
            let attempt = 1;
            attempt <= 4;
            attempt++
        ) {

            try {

                transaction =
                    await connection
                        .getParsedTransaction(
                            signature,
                            {
                                commitment:
                                    'confirmed',

                                maxSupportedTransactionVersion:
                                    0
                            }
                        );

            } catch (error) {

                if (attempt === 4) {
                    throw error;
                }
            }

            if (transaction) {
                break;
            }

            await new Promise(
                (resolve) =>
                    setTimeout(
                        resolve,
                        attempt * 600
                    )
            );
        }

        if (!transaction) {

            hunterState.errors++;

            logWarning(
                `لم نستطع قراءة Transaction: ${signature}`
            );

            return;
        }

        const outerInstructions =
            transaction
                .transaction
                .message
                .instructions || [];

        let mintAddress =
            findMintInInstructions(
                outerInstructions
            );

        // بعض عمليات إنشاء Mint قد تكون
        // داخل Inner Instructions.

        if (
            !mintAddress &&
            transaction.meta
                ?.innerInstructions
        ) {

            for (
                const group
                of transaction.meta
                    .innerInstructions
            ) {

                mintAddress =
                    findMintInInstructions(
                        group.instructions
                    );

                if (mintAddress) {
                    break;
                }
            }
        }

        if (!mintAddress) {

            logWarning(
                `InitializeMint ظهر لكن Mint Address لم يتم استخراجه: ${signature}`
            );

            return;
        }

        hunterState.detected++;

        hunterState.lastDetectedAt =
            new Date().toISOString();

        hunterState.lastMint =
            mintAddress;

        hunterState.lastSignature =
            signature;

        logSuccess(
            `🆕 Fresh Token detected: ${mintAddress}`
        );

        // MongoDB لازم يكون متصل
        // لكي نحفظ الاكتشاف.

        if (
            mongoose.connection
                .readyState !== 1
        ) {

            hunterState.errors++;

            logWarning(
                'تم اكتشاف Token ولكن MongoDB غير متصل'
            );

            return;
        }

        try {

            await FreshToken.create({

                mint:
                    mintAddress,

                signature,

                slot:
                    transaction.slot,

                tokenProgram:
                    programName,

                detectedAt:
                    new Date(),

                status:
                    'NEW',

                securityChecked:
                    false,

                paperOnly:
                    true
            });

            hunterState.saved++;

            logSuccess(
                `💾 Token saved: ${mintAddress}`
            );

        } catch (error) {

            // Duplicate Mint
            if (
                error &&
                error.code === 11000
            ) {

                hunterState.duplicates++;

                logInfo(
                    `Token موجود بالفعل: ${mintAddress}`
                );

                return;
            }

            throw error;
        }

    } catch (error) {

        hunterState.errors++;

        logError(
            'Hunter transaction processing error',
            error
        );

    } finally {

        processingSignatures.delete(
            signature
        );
    }
}

// ======================================================
// START FRESH TOKEN HUNTER
// ======================================================

async function startFreshTokenHunter() {

    if (
        hunterState.status ===
            'running' ||

        hunterState.status ===
            'starting'
    ) {

        return;
    }

    if (
        systemState.solana !==
        'connected'
    ) {

        hunterState.status =
            'waiting_for_solana';

        logWarning(
            'Hunter لم يبدأ لأن Solana غير متصل'
        );

        return;
    }

    try {

        hunterState.status =
            'starting';

        logInfo(
            'بدء Fresh Token Hunter...'
        );

        // SPL Token Program
        tokenProgramSubscription =
            connection.onLogs(

                TOKEN_PROGRAM_ID,

                (logs) => {

                    processPossibleNewMint(
                        logs,
                        'SPL_TOKEN'
                    ).catch(
                        (error) => {

                            hunterState.errors++;

                            logError(
                                'SPL Hunter callback error',
                                error
                            );
                        }
                    );
                },

                'confirmed'
            );

        // Token-2022
        token2022Subscription =
            connection.onLogs(

                TOKEN_2022_PROGRAM_ID,

                (logs) => {

                    processPossibleNewMint(
                        logs,
                        'TOKEN_2022'
                    ).catch(
                        (error) => {

                            hunterState.errors++;

                            logError(
                                'Token-2022 Hunter callback error',
                                error
                            );
                        }
                    );
                },

                'confirmed'
            );

        hunterState.status =
            'running';

        hunterState.startedAt =
            new Date().toISOString();

        logSuccess(
            '🔎 Fresh Token Hunter يعمل'
        );

        logSuccess(
            '🧪 Observation Only - NO BUY / NO SELL'
        );

    } catch (error) {

        hunterState.status =
            'error';

        hunterState.errors++;

        logError(
            'فشل تشغيل Fresh Token Hunter',
            error
        );
    }
}

// ======================================================
// STOP HUNTER
// ======================================================

async function stopFreshTokenHunter() {

    try {

        if (
            hunterState.status ===
            'stopped'
        ) {
            return;
        }

        hunterState.status =
            'stopping';

        if (
            tokenProgramSubscription !==
            null
        ) {

            await connection
                .removeOnLogsListener(
                    tokenProgramSubscription
                );

            tokenProgramSubscription =
                null;
        }

        if (
            token2022Subscription !==
            null
        ) {

            await connection
                .removeOnLogsListener(
                    token2022Subscription
                );

            token2022Subscription =
                null;
        }

        hunterState.status =
            'stopped';

        logSuccess(
            'Fresh Token Hunter stopped'
        );

    } catch (error) {

        hunterState.status =
            'error';

        logError(
            'خطأ أثناء إغلاق Hunter',
            error
        );
    }
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

        // START
        bot.start(
            async (ctx) => {

                await ctx.reply(
                    '🤖 LOMY Solana Hunter V2\n\n' +
                    '🧪 PAPER MODE\n' +
                    '🔎 Fresh Token Hunter\n' +
                    '🔒 التداول الحقيقي مقفول'
                );
            }
        );

        // STATUS
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

                    `📡 Telegram: ${systemState.telegram}\n` +

                    `🔎 Hunter: ${hunterState.status}\n\n` +

                    `🧪 Mode: PAPER\n` +

                    `🔒 Live Trading: OFF`
                );
            }
        );

        // BALANCE
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
                        await connection
                            .getBalance(
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

        // MODE
        bot.command(
            'mode',
            async (ctx) => {

                await ctx.reply(

                    '🧪 PAPER MODE\n\n' +

                    '❌ الشراء الحقيقي: OFF\n' +

                    '❌ البيع الحقيقي: OFF\n' +

                    '🔎 Fresh Token Hunter: Observation Only\n' +

                    '✅ التحليل والتسجيل فقط'
                );
            }
        );

        // HUNTER
        bot.command(
            'hunter',
            async (ctx) => {

                let databaseCount =
                    'N/A';

                try {

                    if (
                        mongoose.connection
                            .readyState === 1
                    ) {

                        databaseCount =
                            await FreshToken
                                .countDocuments();
                    }

                } catch (error) {

                    logError(
                        'خطأ في Hunter count',
                        error
                    );
                }

                await ctx.reply(

                    `🔎 FRESH TOKEN HUNTER\n\n` +

                    `Status: ${hunterState.status}\n` +

                    `Detected: ${hunterState.detected}\n` +

                    `Saved: ${hunterState.saved}\n` +

                    `Database: ${databaseCount}\n` +

                    `Duplicates: ${hunterState.duplicates}\n` +

                    `Errors: ${hunterState.errors}\n\n` +

                    `Last Mint:\n${hunterState.lastMint || 'None'}\n\n` +

                    `🧪 PAPER MODE\n` +

                    `🔒 NO BUY / NO SELL`
                );
            }
        );

        // LAST TOKEN
        bot.command(
            'lasttoken',
            async (ctx) => {

                try {

                    if (
                        mongoose.connection
                            .readyState !== 1
                    ) {

                        return ctx.reply(
                            '❌ MongoDB غير متصل'
                        );
                    }

                    const token =
                        await FreshToken
                            .findOne()
                            .sort({
                                detectedAt: -1
                            })
                            .lean();

                    if (!token) {

                        return ctx.reply(
                            '🔎 لم يتم تسجيل Tokens حتى الآن'
                        );
                    }

                    await ctx.reply(

                        `🆕 LAST TOKEN\n\n` +

                        `Mint:\n${token.mint}\n\n` +

                        `Program: ${token.tokenProgram}\n` +

                        `Status: ${token.status}\n` +

                        `Security Checked: ${token.securityChecked ? 'YES' : 'NO'}\n` +

                        `🧪 Paper Only: YES`
                    );

                } catch (error) {

                    logError(
                        'خطأ في lasttoken',
                        error
                    );

                    await ctx.reply(
                        '❌ تعذر قراءة آخر Token'
                    );
                }
            }
        );

        // Telegram Errors
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

        /*
         * لا نستخدم await هنا.
         * bot.launch يبدأ long polling ويستمر.
         */

        bot.launch()
            .then(() => {

                logInfo(
                    'Telegram polling انتهى'
                );

            })
            .catch((error) => {

                telegramStarted =
                    false;

                systemState.telegram =
                    'error';

                logError(
                    'Telegram polling error',
                    error
                );
            });

        telegramStarted =
            true;

        systemState.telegram =
            'online';

        logSuccess(
            'Telegram Bot يعمل'
        );

    } catch (error) {

        telegramStarted =
            false;

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

    systemState.trading =
        'disabled';

    // STOP HUNTER
    try {

        await stopFreshTokenHunter();

    } catch (error) {

        logError(
            'خطأ أثناء إغلاق Hunter',
            error
        );
    }

    // STOP TELEGRAM
    try {

        if (
            bot &&
            telegramStarted
        ) {

            bot.stop(signal);

            telegramStarted =
                false;

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

    // STOP MONGODB
    try {

        if (
            mongoose.connection
                .readyState !== 0
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

    // STOP HTTP
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
                : new Error(
                    String(reason)
                )
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
    console.log(
        '========================================'
    );
    console.log(
        '🚀 LOMY SOLANA HUNTER V2'
    );
    console.log(
        '🔎 FRESH TOKEN HUNTER V1'
    );
    console.log(
        '🧪 PAPER MODE'
    );
    console.log(
        '🔒 LIVE TRADING DISABLED'
    );
    console.log(
        '========================================'
    );
    console.log('');

    // Render أولاً
    await startHttpServer();

    // MongoDB
    await connectToDatabase();

    // Wallet
    await loadWallet();

    // Solana
    await testSolanaConnection();

    // Hunter
    if (
        systemState.solana ===
        'connected'
    ) {

        await startFreshTokenHunter();
    }

    // Telegram
    await startTelegram();

    updateReadyState();

    console.log('');
    console.log(
        '========================================'
    );

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
        `🔎 Hunter: ${hunterState.status}`
    );

    console.log(
        '🔒 التداول الحقيقي مازال مقفولاً'
    );

    console.log(
        '========================================'
    );
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

        updateReadyState();
    }
);
