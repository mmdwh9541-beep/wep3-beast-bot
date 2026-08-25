// ======================================================
// LOMY SOLANA HUNTER V3
// Render Safe + Fresh Token Hunter + Security Engine V1
// PAPER MODE ONLY - NO BUY / NO SELL
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
// APP / ENV
// ======================================================

const app = express();
app.use(express.json());

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

const TRADING_MODE = 'PAPER';
const LIVE_TRADING_ENABLED = false;

// لا توجد أي دوال BUY / SELL في هذه النسخة.

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
// STATE
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

const hunterState = {
    status: 'stopped',
    detected: 0,
    saved: 0,
    duplicates: 0,
    errors: 0,
    startedAt: null,
    lastDetectedAt: null,
    lastMint: null
};

const securityState = {
    status: 'idle',
    scanned: 0,
    passed: 0,
    review: 0,
    rejected: 0,
    errors: 0,
    lastMint: null,
    lastScore: null,
    lastDecision: null
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

const processingSignatures = new Set();
const securityQueue = [];
const securityQueuedMints = new Set();

let securityWorkerRunning = false;

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

function sleep(ms) {
    return new Promise(
        resolve => setTimeout(resolve, ms)
    );
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

    systemState.trading = 'disabled';
}

// ======================================================
// DATABASE MODEL
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

            slot: Number,

            tokenProgram: String,

            detectedAt: {
                type: Date,
                default: Date.now,
                index: true
            },

            status: {
                type: String,
                default: 'NEW'
            },

            paperOnly: {
                type: Boolean,
                default: true
            },

            securityChecked: {
                type: Boolean,
                default: false,
                index: true
            },

            securityCheckedAt: Date,

            securityScore: {
                type: Number,
                default: null,
                index: true
            },

            securityDecision: {
                type: String,
                default: 'PENDING',
                index: true
            },

            securityRisk: {
                type: String,
                default: 'UNKNOWN'
            },

            mintAuthority: {
                type: String,
                default: null
            },

            mintAuthorityRevoked: {
                type: Boolean,
                default: false
            },

            freezeAuthority: {
                type: String,
                default: null
            },

            freezeAuthorityRevoked: {
                type: Boolean,
                default: false
            },

            supplyRaw: {
                type: String,
                default: null
            },

            decimals: {
                type: Number,
                default: null
            },

            isInitialized: {
                type: Boolean,
                default: null
            },

            actualProgramOwner: {
                type: String,
                default: null
            },

            programOwnerValid: {
                type: Boolean,
                default: false
            },

            token2022: {
                type: Boolean,
                default: false
            },

            riskFlags: {
                type: [String],
                default: []
            },

            positiveFlags: {
                type: [String],
                default: []
            },

            securityError: {
                type: String,
                default: null
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
// HTTP
// ======================================================

app.get('/', (req, res) => {

    res.status(200).send(
        '✅ LOMY Solana Hunter V3 - PAPER MODE'
    );
});

app.get('/health', (req, res) => {

    updateReadyState();

    res.status(200).json({

        service: 'LOMY Solana Hunter V3',

        server: systemState.server,
        database: systemState.database,
        wallet: systemState.wallet,
        solana: systemState.solana,
        telegram: systemState.telegram,

        hunter: hunterState.status,

        security: securityState.status,

        securityQueue:
            securityQueue.length,

        trading: systemState.trading,

        tradingMode: TRADING_MODE,

        liveTradingEnabled:
            LIVE_TRADING_ENABLED,

        ready: systemState.ready,

        uptimeSeconds:
            Math.floor(process.uptime()),

        startedAt:
            systemState.startedAt,

        lastError:
            systemState.lastError
    });
});

app.get('/api/status', (req, res) => {

    updateReadyState();

    res.json({

        success: true,

        mode: TRADING_MODE,

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
                hunterState.status,

            security:
                securityState.status
        },

        hunter: hunterState,

        security: {
            ...securityState,
            queue:
                securityQueue.length
        }
    });
});

// ======================================================
// HTTP SERVER FIRST FOR RENDER
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
                error => {

                    systemState.server =
                        'error';

                    logError(
                        'HTTP Server error',
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
    error => {

        systemState.database =
            'error';

        logError(
            'MongoDB error',
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
            'MONGODB_URI غير موجود'
        );

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
            'فشل MongoDB',
            error
        );
    }

    updateReadyState();
}

// ======================================================
// WALLET
// ======================================================

async function loadWallet() {

    try {

        if (!BOT_PRIVATE_KEY) {

            throw new Error(
                'BOT_PRIVATE_KEY غير موجود'
            );
        }

        const key =
            BOT_PRIVATE_KEY.trim();

        if (
            key.includes(' ') &&
            bip39.validateMnemonic(key)
        ) {

            const seed =
                bip39.mnemonicToSeedSync(
                    key
                );

            const derived =
                derivePath(
                    "m/44'/501'/0'/0'",
                    seed.toString('hex')
                ).key;

            wallet =
                Keypair.fromSeed(
                    derived
                );

        } else if (
            key.startsWith('[')
        ) {

            wallet =
                Keypair.fromSecretKey(
                    Uint8Array.from(
                        JSON.parse(key)
                    )
                );

        } else {

            wallet =
                Keypair.fromSecretKey(
                    bs58.decode(key)
                );
        }

        systemState.wallet =
            'loaded';

        logSuccess(
            `المحفظة: ${wallet.publicKey.toString()}`
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
// SOLANA TEST
// ======================================================

async function testSolanaConnection() {

    if (!wallet) {

        systemState.solana =
            'wallet_unavailable';

        return;
    }

    try {

        const balance =
            await connection.getBalance(
                wallet.publicKey
            );

        const sol =
            balance /
            LAMPORTS_PER_SOL;

        systemState.solana =
            'connected';

        logSuccess(
            'Solana RPC متصل'
        );

        logSuccess(
            `الرصيد: ${sol.toFixed(6)} SOL`
        );

    } catch (error) {

        systemState.solana =
            'error';

        logError(
            'Solana RPC error',
            error
        );
    }

    updateReadyState();
}

// ======================================================
// SECURITY ENGINE V1
// ======================================================

function calculateSecurityScore(data) {

    let score = 50;

    const riskFlags = [];
    const positiveFlags = [];

    // ==============================================
    // MINT AUTHORITY
    // ==============================================

    if (
        data.mintAuthority === null
    ) {

        score += 20;

        positiveFlags.push(
            'MINT_AUTHORITY_REVOKED'
        );

    } else {

        score -= 20;

        riskFlags.push(
            'MINT_AUTHORITY_ACTIVE'
        );
    }

    // ==============================================
    // FREEZE AUTHORITY
    // ==============================================

    if (
        data.freezeAuthority === null
    ) {

        score += 20;

        positiveFlags.push(
            'FREEZE_AUTHORITY_REVOKED'
        );

    } else {

        score -= 25;

        riskFlags.push(
            'FREEZE_AUTHORITY_ACTIVE'
        );
    }

    // ==============================================
    // INITIALIZED
    // ==============================================

    if (
        data.isInitialized === true
    ) {

        score += 5;

        positiveFlags.push(
            'MINT_INITIALIZED'
        );

    } else {

        score -= 30;

        riskFlags.push(
            'MINT_NOT_INITIALIZED'
        );
    }

    // ==============================================
    // PROGRAM OWNER
    // ==============================================

    if (
        data.programOwnerValid
    ) {

        score += 5;

        positiveFlags.push(
            'VALID_TOKEN_PROGRAM'
        );

    } else {

        score -= 40;

        riskFlags.push(
            'INVALID_PROGRAM_OWNER'
        );
    }

    // ==============================================
    // SUPPLY
    // ==============================================

    try {

        const supply =
            BigInt(
                data.supplyRaw || '0'
            );

        if (supply <= 0n) {

            score -= 10;

            riskFlags.push(
                'ZERO_SUPPLY'
            );

        } else {

            positiveFlags.push(
                'POSITIVE_SUPPLY'
            );
        }

    } catch {

        score -= 5;

        riskFlags.push(
            'INVALID_SUPPLY'
        );
    }

    // ==============================================
    // DECIMALS
    // ==============================================

    if (
        typeof data.decimals ===
            'number' &&
        data.decimals >= 0 &&
        data.decimals <= 18
    ) {

        positiveFlags.push(
            'NORMAL_DECIMALS_RANGE'
        );

    } else {

        score -= 10;

        riskFlags.push(
            'UNUSUAL_DECIMALS'
        );
    }

    // Token-2022 مش نصب تلقائياً.
    // لكن يحتاج Extensions Scan لاحقاً.

    if (data.token2022) {

        riskFlags.push(
            'TOKEN_2022_REQUIRES_EXTENSION_SCAN'
        );
    }

    score =
        Math.max(
            0,
            Math.min(
                100,
                score
            )
        );

    let decision;
    let risk;

    if (score >= 80) {

        decision = 'PASS';
        risk = 'LOW';

    } else if (score >= 55) {

        decision = 'REVIEW';
        risk = 'MEDIUM';

    } else {

        decision = 'REJECT';
        risk = 'HIGH';
    }

    return {
        score,
        decision,
        risk,
        riskFlags,
        positiveFlags
    };
}

// ======================================================
// SCAN TOKEN SECURITY
// ======================================================

async function scanTokenSecurity(
    mintAddress
) {

    securityState.status =
        'scanning';

    try {

        const mintPublicKey =
            new PublicKey(
                mintAddress
            );

        const accountInfo =
            await connection
                .getParsedAccountInfo(
                    mintPublicKey,
                    'confirmed'
                );

        if (
            !accountInfo ||
            !accountInfo.value
        ) {

            throw new Error(
                'Mint account not found'
            );
        }

        const value =
            accountInfo.value;

        const owner =
            value.owner.toString();

        const validOwner =
            owner ===
                TOKEN_PROGRAM_ID.toString() ||
            owner ===
                TOKEN_2022_PROGRAM_ID.toString();

        const parsed =
            value.data?.parsed;

        if (
            !parsed ||
            parsed.type !== 'mint'
        ) {

            throw new Error(
                'Account is not a parsed Mint'
            );
        }

        const info =
            parsed.info || {};

        const mintAuthority =
            info.mintAuthority ?? null;

        const freezeAuthority =
            info.freezeAuthority ?? null;

        const data = {

            mintAuthority,

            freezeAuthority,

            supplyRaw:
                String(
                    info.supply ?? '0'
                ),

            decimals:
                Number(
                    info.decimals ?? 0
                ),

            isInitialized:
                info.isInitialized === true,

            actualProgramOwner:
                owner,

            programOwnerValid:
                validOwner,

            token2022:
                owner ===
                TOKEN_2022_PROGRAM_ID
                    .toString()
        };

        const result =
            calculateSecurityScore(
                data
            );

        await FreshToken.updateOne(
            {
                mint:
                    mintAddress
            },
            {
                $set: {

                    securityChecked:
                        true,

                    securityCheckedAt:
                        new Date(),

                    securityScore:
                        result.score,

                    securityDecision:
                        result.decision,

                    securityRisk:
                        result.risk,

                    mintAuthority:
                        data.mintAuthority,

                    mintAuthorityRevoked:
                        data.mintAuthority ===
                        null,

                    freezeAuthority:
                        data.freezeAuthority,

                    freezeAuthorityRevoked:
                        data.freezeAuthority ===
                        null,

                    supplyRaw:
                        data.supplyRaw,

                    decimals:
                        data.decimals,

                    isInitialized:
                        data.isInitialized,

                    actualProgramOwner:
                        data.actualProgramOwner,

                    programOwnerValid:
                        data.programOwnerValid,

                    token2022:
                        data.token2022,

                    riskFlags:
                        result.riskFlags,

                    positiveFlags:
                        result.positiveFlags,

                    securityError:
                        null,

                    status:
                        result.decision
                }
            }
        );

        securityState.scanned++;

        securityState.lastMint =
            mintAddress;

        securityState.lastScore =
            result.score;

        securityState.lastDecision =
            result.decision;

        if (
            result.decision ===
            'PASS'
        ) {

            securityState.passed++;

        } else if (
            result.decision ===
            'REVIEW'
        ) {

            securityState.review++;

        } else {

            securityState.rejected++;
        }

        logSuccess(
            `🛡️ Security ${mintAddress} | ${result.score}/100 | ${result.decision}`
        );

        return result;

    } catch (error) {

        securityState.errors++;

        logError(
            `Security Scan failed: ${mintAddress}`,
            error
        );

        try {

            await FreshToken.updateOne(
                {
                    mint:
                        mintAddress
                },
                {
                    $set: {

                        securityChecked:
                            false,

                        securityDecision:
                            'ERROR',

                        securityRisk:
                            'UNKNOWN',

                        securityError:
                            error.message
                    }
                }
            );

        } catch {}

        return null;

    } finally {

        securityState.status =
            securityQueue.length > 0
                ? 'scanning'
                : 'idle';
    }
}

// ======================================================
// SECURITY QUEUE
// ======================================================

function queueSecurityScan(
    mintAddress
) {

    if (
        !mintAddress ||
        securityQueuedMints.has(
            mintAddress
        )
    ) {

        return;
    }

    securityQueuedMints.add(
        mintAddress
    );

    securityQueue.push(
        mintAddress
    );

    runSecurityWorker()
        .catch(
            error => {

                logError(
                    'Security Worker error',
                    error
                );
            }
        );
}

async function runSecurityWorker() {

    if (securityWorkerRunning) {
        return;
    }

    securityWorkerRunning = true;

    try {

        while (
            securityQueue.length > 0 &&
            !shuttingDown
        ) {

            const mint =
                securityQueue.shift();

            try {

                await scanTokenSecurity(
                    mint
                );

            } finally {

                securityQueuedMints.delete(
                    mint
                );
            }

            // حماية الـRPC من الضغط
            await sleep(250);
        }

    } finally {

        securityWorkerRunning =
            false;

        securityState.status =
            'idle';
    }
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

        if (
            instruction?.parsed &&
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
// PROCESS NEW MINT
// ======================================================

async function processPossibleNewMint(
    event,
    programName
) {

    if (!event || event.err) {
        return;
    }

    const logs =
        event.logs || [];

    const found =
        logs.some(
            line =>
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

    if (!found) {
        return;
    }

    const signature =
        event.signature;

    if (
        !signature ||
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

            await sleep(
                attempt * 600
            );
        }

        if (!transaction) {

            hunterState.errors++;

            return;
        }

        let mint =
            findMintInInstructions(
                transaction.transaction
                    .message.instructions
            );

        if (
            !mint &&
            transaction.meta
                ?.innerInstructions
        ) {

            for (
                const group
                of transaction.meta
                    .innerInstructions
            ) {

                mint =
                    findMintInInstructions(
                        group.instructions
                    );

                if (mint) {
                    break;
                }
            }
        }

        if (!mint) {
            return;
        }

        hunterState.detected++;

        hunterState.lastMint =
            mint;

        hunterState.lastDetectedAt =
            new Date().toISOString();

        if (
            mongoose.connection
                .readyState !== 1
        ) {

            hunterState.errors++;

            return;
        }

        let tokenDocument = null;

        try {

            tokenDocument =
                await FreshToken.create(
                    {
                        mint,

                        signature,

                        slot:
                            transaction.slot,

                        tokenProgram:
                            programName,

                        detectedAt:
                            new Date(),

                        status:
                            'NEW',

                        paperOnly:
                            true,

                        securityChecked:
                            false,

                        securityDecision:
                            'PENDING'
                    }
                );

            hunterState.saved++;

            logSuccess(
                `🆕 Token: ${mint}`
            );

        } catch (error) {

            if (
                error?.code === 11000
            ) {

                hunterState.duplicates++;

                tokenDocument =
                    await FreshToken
                        .findOne({
                            mint
                        });

            } else {

                throw error;
            }
        }

        // لو جديد أو لم يتم فحصه سابقاً
        if (
            tokenDocument &&
            !tokenDocument
                .securityChecked
        ) {

            queueSecurityScan(
                mint
            );
        }

    } catch (error) {

        hunterState.errors++;

        logError(
            'Hunter processing error',
            error
        );

    } finally {

        processingSignatures.delete(
            signature
        );
    }
}

// ======================================================
// HUNTER START
// ======================================================

async function startFreshTokenHunter() {

    if (
        hunterState.status ===
            'running'
    ) {

        return;
    }

    if (
        systemState.solana !==
        'connected'
    ) {

        hunterState.status =
            'waiting_for_solana';

        return;
    }

    try {

        hunterState.status =
            'starting';

        tokenProgramSubscription =
            connection.onLogs(

                TOKEN_PROGRAM_ID,

                event => {

                    processPossibleNewMint(
                        event,
                        'SPL_TOKEN'
                    ).catch(
                        error =>
                            logError(
                                'SPL Hunter error',
                                error
                            )
                    );
                },

                'confirmed'
            );

        token2022Subscription =
            connection.onLogs(

                TOKEN_2022_PROGRAM_ID,

                event => {

                    processPossibleNewMint(
                        event,
                        'TOKEN_2022'
                    ).catch(
                        error =>
                            logError(
                                'Token2022 Hunter error',
                                error
                            )
                    );
                },

                'confirmed'
            );

        hunterState.status =
            'running';

        hunterState.startedAt =
            new Date().toISOString();

        logSuccess(
            '🔎 Fresh Token Hunter RUNNING'
        );

    } catch (error) {

        hunterState.status =
            'error';

        hunterState.errors++;

        logError(
            'Hunter start failed',
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

    } catch (error) {

        logError(
            'Hunter stop error',
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

        return;
    }

    try {

        bot =
            new Telegraf(
                TELEGRAM_TOKEN
            );

        bot.start(
            ctx =>
                ctx.reply(
                    '🤖 LOMY SOLANA HUNTER V3\n\n' +
                    '🔎 Fresh Token Hunter: ON\n' +
                    '🛡️ Security Engine: ON\n' +
                    '🧪 PAPER MODE\n' +
                    '🔒 LIVE TRADING: OFF'
                )
        );

        // ==============================================
        // STATUS
        // ==============================================

        bot.command(
            'status',
            async ctx => {

                updateReadyState();

                await ctx.reply(

                    `🤖 LOMY STATUS\n\n` +

                    `🌐 Server: ${systemState.server}\n` +

                    `🗄 Database: ${systemState.database}\n` +

                    `👛 Wallet: ${systemState.wallet}\n` +

                    `⚡ Solana: ${systemState.solana}\n` +

                    `📡 Telegram: ${systemState.telegram}\n` +

                    `🔎 Hunter: ${hunterState.status}\n` +

                    `🛡️ Security: ${securityState.status}\n\n` +

                    `🧪 Mode: PAPER\n` +

                    `🔒 Live Trading: OFF`
                );
            }
        );

        // ==============================================
        // BALANCE
        // ==============================================

        bot.command(
            'balance',
            async ctx => {

                if (!wallet) {

                    return ctx.reply(
                        '❌ المحفظة غير جاهزة'
                    );
                }

                try {

                    const balance =
                        await connection
                            .getBalance(
                                wallet.publicKey
                            );

                    await ctx.reply(
                        `💰 الرصيد: ${(balance / LAMPORTS_PER_SOL).toFixed(6)} SOL`
                    );

                } catch {

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
            ctx =>
                ctx.reply(
                    '🧪 PAPER MODE\n\n' +
                    '❌ BUY: OFF\n' +
                    '❌ SELL: OFF\n' +
                    '🔎 Detection: ON\n' +
                    '🛡️ Security Analysis: ON'
                )
        );

        // ==============================================
        // HUNTER
        // ==============================================

        bot.command(
            'hunter',
            async ctx => {

                const count =
                    mongoose.connection
                        .readyState === 1
                        ?
                        await FreshToken
                            .countDocuments()
                        :
                        'N/A';

                await ctx.reply(

                    `🔎 FRESH TOKEN HUNTER\n\n` +

                    `Status: ${hunterState.status}\n` +

                    `Detected: ${hunterState.detected}\n` +

                    `Saved: ${hunterState.saved}\n` +

                    `Database: ${count}\n` +

                    `Duplicates: ${hunterState.duplicates}\n` +

                    `Errors: ${hunterState.errors}\n\n` +

                    `Last Mint:\n${hunterState.lastMint || 'None'}\n\n` +

                    `🔒 NO BUY / NO SELL`
                );
            }
        );

        // ==============================================
        // SECURITY
        // ==============================================

        bot.command(
            'security',
            async ctx => {

                let last = null;

                try {

                    last =
                        await FreshToken
                            .findOne({
                                securityChecked:
                                    true
                            })
                            .sort({
                                securityCheckedAt:
                                    -1
                            })
                            .lean();

                } catch {}

                let message =

                    `🛡️ SECURITY ENGINE\n\n` +

                    `Status: ${securityState.status}\n` +

                    `Queue: ${securityQueue.length}\n` +

                    `Scanned: ${securityState.scanned}\n` +

                    `PASS: ${securityState.passed} ✅\n` +

                    `REVIEW: ${securityState.review} ⚠️\n` +

                    `REJECT: ${securityState.rejected} ❌\n` +

                    `Errors: ${securityState.errors}\n`;

                if (last) {

                    message +=

                        `\n──────────────\n` +

                        `LAST SCAN\n\n` +

                        `Mint:\n${last.mint}\n\n` +

                        `Score: ${last.securityScore}/100\n` +

                        `Risk: ${last.securityRisk}\n` +

                        `Decision: ${last.securityDecision}\n\n` +

                        `Mint Authority: ${
                            last.mintAuthorityRevoked
                                ? 'REVOKED ✅'
                                : 'ACTIVE ⚠️'
                        }\n` +

                        `Freeze Authority: ${
                            last.freezeAuthorityRevoked
                                ? 'REVOKED ✅'
                                : 'ACTIVE ⚠️'
                        }\n` +

                        `Program: ${last.tokenProgram}\n` +

                        `Decimals: ${last.decimals}\n\n` +

                        `🔒 PAPER ONLY`;
                }

                await ctx.reply(
                    message
                );
            }
        );

        // ==============================================
        // STATS
        // ==============================================

        bot.command(
            'stats',
            async ctx => {

                try {

                    const [
                        total,
                        passed,
                        review,
                        rejected,
                        pending
                    ] =
                    await Promise.all([

                        FreshToken
                            .countDocuments(),

                        FreshToken
                            .countDocuments({
                                securityDecision:
                                    'PASS'
                            }),

                        FreshToken
                            .countDocuments({
                                securityDecision:
                                    'REVIEW'
                            }),

                        FreshToken
                            .countDocuments({
                                securityDecision:
                                    'REJECT'
                            }),

                        FreshToken
                            .countDocuments({
                                securityChecked:
                                    false
                            })
                    ]);

                    await ctx.reply(

                        `📊 DATABASE STATS\n\n` +

                        `Total Tokens: ${total}\n\n` +

                        `PASS: ${passed} ✅\n` +

                        `REVIEW: ${review} ⚠️\n` +

                        `REJECT: ${rejected} ❌\n` +

                        `Pending: ${pending} ⏳\n\n` +

                        `🧪 PAPER MODE`
                    );

                } catch {

                    await ctx.reply(
                        '❌ تعذر قراءة الإحصائيات'
                    );
                }
            }
        );

        // ==============================================
        // LAST TOKEN
        // ==============================================

        bot.command(
            'lasttoken',
            async ctx => {

                try {

                    const token =
                        await FreshToken
                            .findOne()
                            .sort({
                                detectedAt: -1
                            })
                            .lean();

                    if (!token) {

                        return ctx.reply(
                            'لا توجد Tokens'
                        );
                    }

                    await ctx.reply(

                        `🆕 LAST TOKEN\n\n` +

                        `Mint:\n${token.mint}\n\n` +

                        `Program: ${token.tokenProgram}\n` +

                        `Security: ${token.securityDecision}\n` +

                        `Score: ${token.securityScore ?? 'PENDING'}\n\n` +

                        `🧪 PAPER ONLY`
                    );

                } catch {

                    await ctx.reply(
                        '❌ خطأ'
                    );
                }
            }
        );

        bot.catch(
            (error, ctx) => {

                logError(
                    `Telegram ${ctx.updateType}`,
                    error
                );
            }
        );

        systemState.telegram =
            'starting';

        bot.launch()
            .catch(
                error => {

                    telegramStarted =
                        false;

                    systemState.telegram =
                        'error';

                    logError(
                        'Telegram polling error',
                        error
                    );
                }
            );

        telegramStarted =
            true;

        systemState.telegram =
            'online';

        logSuccess(
            'Telegram Bot ONLINE'
        );

    } catch (error) {

        systemState.telegram =
            'error';

        logError(
            'Telegram start error',
            error
        );
    }

    updateReadyState();
}

// ======================================================
// RECOVER PENDING TOKENS
// ======================================================

async function recoverPendingSecurityScans() {

    if (
        mongoose.connection
            .readyState !== 1
    ) {

        return;
    }

    try {

        // بعد Restart على Render
        // نكمل آخر Tokens غير المفحوصة.

        const pending =
            await FreshToken
                .find({
                    securityChecked:
                        false,

                    securityDecision: {
                        $in: [
                            'PENDING',
                            'ERROR'
                        ]
                    }
                })
                .sort({
                    detectedAt: -1
                })
                .limit(100)
                .select({
                    mint: 1
                })
                .lean();

        for (
            const token
            of pending
        ) {

            queueSecurityScan(
                token.mint
            );
        }

        logInfo(
            `Security Recovery queued: ${pending.length}`
        );

    } catch (error) {

        logError(
            'Security Recovery error',
            error
        );
    }
}

// ======================================================
// SHUTDOWN
// ======================================================

async function shutdown(signal) {

    if (shuttingDown) {
        return;
    }

    shuttingDown = true;

    logWarning(
        `${signal} - Safe shutdown`
    );

    systemState.ready =
        false;

    systemState.trading =
        'disabled';

    try {

        await stopFreshTokenHunter();

    } catch {}

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
        }

    } catch {}

    try {

        if (
            mongoose.connection
                .readyState !== 0
        ) {

            await mongoose.disconnect();
        }

    } catch {}

    if (httpServer) {

        httpServer.close(
            () =>
                process.exit(0)
        );

        setTimeout(
            () => process.exit(0),
            5000
        );

    } else {

        process.exit(0);
    }
}

// ======================================================
// ERRORS
// ======================================================

process.on(
    'unhandledRejection',
    reason => {

        logError(
            'Unhandled Rejection',

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
    async error => {

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
    () =>
        shutdown('SIGTERM')
);

process.once(
    'SIGINT',
    () =>
        shutdown('SIGINT')
);

// ======================================================
// MAIN
// ======================================================

async function main() {

    console.log('');
    console.log(
        '======================================'
    );
    console.log(
        '🚀 LOMY SOLANA HUNTER V3'
    );
    console.log(
        '🔎 FRESH TOKEN HUNTER'
    );
    console.log(
        '🛡️ SECURITY ENGINE V1'
    );
    console.log(
        '🧪 PAPER MODE'
    );
    console.log(
        '🔒 LIVE TRADING DISABLED'
    );
    console.log(
        '======================================'
    );
    console.log('');

    // Render لازم يفتح PORT أولاً
    await startHttpServer();

    await connectToDatabase();

    await loadWallet();

    await testSolanaConnection();

    // نكمل الفحوصات القديمة
    await recoverPendingSecurityScans();

    // نبدأ اكتشاف الجديد
    if (
        systemState.solana ===
        'connected'
    ) {

        await startFreshTokenHunter();
    }

    await startTelegram();

    updateReadyState();

    console.log('');
    console.log(
        '======================================'
    );

    logSuccess(
        'LOMY V3 STARTED'
    );

    console.log(
        `🔎 Hunter: ${hunterState.status}`
    );

    console.log(
        `🛡️ Security: ${securityState.status}`
    );

    console.log(
        '🔒 NO LIVE TRADING'
    );

    console.log(
        '======================================'
    );
}

// ======================================================
// START
// ======================================================

main().catch(
    error => {

        logError(
            'MAIN ERROR',
            error
        );

        updateReadyState();
    }
);
