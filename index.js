// ======================================================
// LOMY SOLANA HUNTER V4
// Render Safe
// Fresh Token Hunter
// Security Engine V2
// DEX / Liquidity Engine V1
// PAPER MODE ONLY
// ABSOLUTELY NO BUY / NO SELL
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

// لا توجد أي دالة شراء أو بيع في V4.

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
// V4 FILTER SETTINGS
// ======================================================

// هذه حدود مراقبة وليست قواعد تداول.

const MIN_LIQUIDITY_USD = 3000;
const STRONG_LIQUIDITY_USD = 10000;

const MIN_VOLUME_M5 = 250;
const MIN_TXNS_M5 = 5;

const DEX_RECHECK_DELAY_MS = 30000;
const DEX_MAX_RECHECKS = 5;

// ======================================================
// CONNECTION
// ======================================================

const connection = new Connection(
    RPC_URL,
    {
        commitment: 'confirmed'
    }
);

// ======================================================
// STATES
// ======================================================

const systemState = {
    server: 'starting',
    database: 'disconnected',
    wallet: 'not_loaded',
    solana: 'disconnected',
    telegram: 'stopped',
    ready: false,
    trading: 'disabled',
    mode: 'PAPER',
    startedAt: new Date().toISOString(),
    lastError: null
};

const hunterState = {
    status: 'stopped',
    detected: 0,
    saved: 0,
    duplicates: 0,
    errors: 0,
    lastMint: null
};

const securityState = {
    status: 'idle',
    scanned: 0,
    pass: 0,
    review: 0,
    reject: 0,
    errors: 0,
    queue: 0,
    lastMint: null,
    lastScore: null,
    lastDecision: null
};

const dexState = {
    status: 'idle',
    scanned: 0,
    listed: 0,
    noPool: 0,
    pass: 0,
    watch: 0,
    reject: 0,
    errors: 0,
    queue: 0,
    lastMint: null,
    lastDex: null,
    lastLiquidity: null
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
const securityQueued = new Set();

const dexQueue = [];
const dexQueued = new Set();

let securityWorkerRunning = false;
let dexWorkerRunning = false;

// ======================================================
// HELPERS
// ======================================================

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function now() {
    return new Date().toISOString();
}

function safeNumber(value, fallback = 0) {

    const n = Number(value);

    return Number.isFinite(n)
        ? n
        : fallback;
}

function logInfo(message) {
    console.log(`[${now()}] ℹ️ ${message}`);
}

function logSuccess(message) {
    console.log(`[${now()}] ✅ ${message}`);
}

function logWarning(message) {
    console.warn(`[${now()}] ⚠️ ${message}`);
}

function logError(message, error = null) {

    if (error) {

        console.error(
            `[${now()}] ❌ ${message}`,
            error
        );

        systemState.lastError =
            error?.message || String(error);

    } else {

        console.error(
            `[${now()}] ❌ ${message}`
        );

        systemState.lastError = message;
    }
}

function updateReadyState() {

    systemState.ready =
        systemState.server === 'online' &&
        systemState.database === 'connected' &&
        systemState.wallet === 'loaded' &&
        systemState.solana === 'connected';

    systemState.trading = 'disabled';

    securityState.queue =
        securityQueue.length;

    dexState.queue =
        dexQueue.length;
}

// ======================================================
// DATABASE
// ======================================================

const tokenSchema = new mongoose.Schema({

    mint: {
        type: String,
        required: true,
        unique: true,
        index: true
    },

    signature: String,

    slot: Number,

    tokenProgram: String,

    detectedAt: {
        type: Date,
        default: Date.now,
        index: true
    },

    paperOnly: {
        type: Boolean,
        default: true
    },

    // ---------------- SECURITY ----------------

    securityChecked: {
        type: Boolean,
        default: false,
        index: true
    },

    securityCheckedAt: Date,

    securityScore: {
        type: Number,
        default: null
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

    mintAuthority: String,

    mintAuthorityRevoked: {
        type: Boolean,
        default: false
    },

    freezeAuthority: String,

    freezeAuthorityRevoked: {
        type: Boolean,
        default: false
    },

    supplyRaw: String,

    decimals: Number,

    token2022: {
        type: Boolean,
        default: false
    },

    extensions: {
        type: [String],
        default: []
    },

    dangerousExtensions: {
        type: [String],
        default: []
    },

    securityFlags: {
        type: [String],
        default: []
    },

    securityError: String,

    // ---------------- DEX ----------------

    dexChecked: {
        type: Boolean,
        default: false,
        index: true
    },

    dexCheckedAt: Date,

    dexListed: {
        type: Boolean,
        default: false
    },

    dexId: String,

    pairAddress: String,

    pairCreatedAt: Date,

    pairAgeMinutes: Number,

    priceUsd: Number,

    liquidityUsd: Number,

    volumeM5: Number,

    volumeH1: Number,

    volumeH24: Number,

    buysM5: Number,

    sellsM5: Number,

    txnsM5: Number,

    fdv: Number,

    marketCap: Number,

    dexScore: Number,

    dexDecision: {
        type: String,
        default: 'PENDING',
        index: true
    },

    dexFlags: {
        type: [String],
        default: []
    },

    dexCheckAttempts: {
        type: Number,
        default: 0
    },

    dexError: String,

    // ---------------- FINAL ----------------

    finalScore: {
        type: Number,
        default: null,
        index: true
    },

    finalDecision: {
        type: String,
        default: 'PENDING',
        index: true
    }

}, {
    timestamps: true
});

const FreshToken =
    mongoose.models.FreshToken ||
    mongoose.model(
        'FreshToken',
        tokenSchema
    );

// ======================================================
// HTTP
// ======================================================

app.get('/', (req, res) => {

    res.status(200).send(
        '✅ LOMY SOLANA HUNTER V4 - PAPER MODE'
    );
});

app.get('/health', (req, res) => {

    updateReadyState();

    res.status(200).json({

        service: 'LOMY V4',

        server: systemState.server,
        database: systemState.database,
        wallet: systemState.wallet,
        solana: systemState.solana,
        telegram: systemState.telegram,

        hunter: hunterState.status,

        security: securityState.status,

        dex: dexState.status,

        ready: systemState.ready,

        mode: 'PAPER',

        liveTrading: false,

        uptimeSeconds:
            Math.floor(process.uptime()),

        lastError:
            systemState.lastError
    });
});

app.get('/api/status', (req, res) => {

    updateReadyState();

    res.json({

        success: true,

        system: systemState,

        hunter: hunterState,

        security: {
            ...securityState,
            queue: securityQueue.length
        },

        dex: {
            ...dexState,
            queue: dexQueue.length
        }
    });
});

// ======================================================
// HTTP SERVER - RENDER FIRST
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
                            `Render Server PORT ${PORT}`
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

                    reject(error);
                }
            );
        }
    );
}

// ======================================================
// MONGODB
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
    }
);

async function connectDatabase() {

    try {

        if (!MONGODB_URI) {

            throw new Error(
                'MONGODB_URI missing'
            );
        }

        systemState.database =
            'connecting';

        await mongoose.connect(
            MONGODB_URI,
            {
                serverSelectionTimeoutMS:
                    10000
            }
        );

    } catch (error) {

        systemState.database =
            'error';

        logError(
            'Database connection failed',
            error
        );
    }
}

// ======================================================
// WALLET
// ======================================================

async function loadWallet() {

    try {

        if (!BOT_PRIVATE_KEY) {

            throw new Error(
                'BOT_PRIVATE_KEY missing'
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
            `Wallet: ${wallet.publicKey.toString()}`
        );

    } catch (error) {

        wallet = null;

        systemState.wallet =
            'error';

        logError(
            'Wallet error',
            error
        );
    }
}

// ======================================================
// SOLANA
// ======================================================

async function testSolana() {

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

        systemState.solana =
            'connected';

        logSuccess(
            `Solana connected | ${(balance / LAMPORTS_PER_SOL).toFixed(6)} SOL`
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
// TOKEN-2022 EXTENSION EXTRACTION
// ======================================================

function extractExtensions(parsedInfo) {

    const result = [];

    function walk(value) {

        if (!value) return;

        if (Array.isArray(value)) {

            for (const item of value) {

                if (
                    item &&
                    typeof item === 'object'
                ) {

                    if (
                        typeof item.extension ===
                        'string'
                    ) {

                        result.push(
                            item.extension
                        );
                    }

                    if (
                        typeof item.extensionType ===
                        'string'
                    ) {

                        result.push(
                            item.extensionType
                        );
                    }
                }

                walk(item);
            }

            return;
        }

        if (typeof value === 'object') {

            for (
                const [key, val]
                of Object.entries(value)
            ) {

                const lower =
                    key.toLowerCase();

                if (
                    lower.includes(
                        'permanentdelegate'
                    )
                ) {

                    result.push(
                        'PermanentDelegate'
                    );
                }

                if (
                    lower.includes(
                        'transferfee'
                    )
                ) {

                    result.push(
                        'TransferFeeConfig'
                    );
                }

                if (
                    lower.includes(
                        'nontransferable'
                    )
                ) {

                    result.push(
                        'NonTransferable'
                    );
                }

                if (
                    lower.includes(
                        'transferhook'
                    )
                ) {

                    result.push(
                        'TransferHook'
                    );
                }

                if (
                    lower.includes(
                        'defaultaccountstate'
                    )
                ) {

                    result.push(
                        'DefaultAccountState'
                    );
                }

                if (
                    lower.includes(
                        'confidential'
                    )
                ) {

                    result.push(
                        'ConfidentialTransfer'
                    );
                }

                walk(val);
            }
        }
    }

    walk(parsedInfo);

    return [
        ...new Set(result)
    ];
}

// ======================================================
// SECURITY V2
// ======================================================

function calculateSecurity(data) {

    let score = 50;

    const flags = [];

    if (
        data.mintAuthority === null
    ) {

        score += 20;

        flags.push(
            'MINT_REVOKED'
        );

    } else {

        score -= 20;

        flags.push(
            'MINT_AUTHORITY_ACTIVE'
        );
    }

    if (
        data.freezeAuthority === null
    ) {

        score += 20;

        flags.push(
            'FREEZE_REVOKED'
        );

    } else {

        score -= 25;

        flags.push(
            'FREEZE_AUTHORITY_ACTIVE'
        );
    }

    if (data.supply > 0n) {

        score += 5;

    } else {

        score -= 15;

        flags.push(
            'ZERO_SUPPLY'
        );
    }

    if (
        data.decimals >= 0 &&
        data.decimals <= 18
    ) {

        score += 5;

    } else {

        score -= 10;

        flags.push(
            'UNUSUAL_DECIMALS'
        );
    }

    const dangerous = [];

    for (
        const extension
        of data.extensions
    ) {

        const e =
            extension.toLowerCase();

        if (
            e.includes(
                'permanentdelegate'
            )
        ) {

            score -= 35;

            dangerous.push(
                'PermanentDelegate'
            );
        }

        if (
            e.includes(
                'nontransferable'
            )
        ) {

            score -= 50;

            dangerous.push(
                'NonTransferable'
            );
        }

        if (
            e.includes(
                'transferhook'
            )
        ) {

            score -= 20;

            dangerous.push(
                'TransferHook'
            );
        }

        if (
            e.includes(
                'transferfee'
            )
        ) {

            score -= 10;

            dangerous.push(
                'TransferFeeConfig'
            );
        }

        if (
            e.includes(
                'defaultaccountstate'
            )
        ) {

            score -= 15;

            dangerous.push(
                'DefaultAccountState'
            );
        }

        if (
            e.includes(
                'confidential'
            )
        ) {

            score -= 15;

            dangerous.push(
                extension
            );
        }
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
        flags,
        dangerous
    };
}

async function scanSecurity(
    mintAddress
) {

    securityState.status =
        'scanning';

    try {

        const pubkey =
            new PublicKey(
                mintAddress
            );

        const response =
            await connection
                .getParsedAccountInfo(
                    pubkey,
                    'confirmed'
                );

        if (!response.value) {

            throw new Error(
                'Mint not found'
            );
        }

        const account =
            response.value;

        const owner =
            account.owner.toString();

        const token2022 =
            owner ===
            TOKEN_2022_PROGRAM_ID.toString();

        const parsed =
            account.data?.parsed;

        if (
            !parsed ||
            parsed.type !== 'mint'
        ) {

            throw new Error(
                'Not parsed mint'
            );
        }

        const info =
            parsed.info || {};

        const extensions =
            token2022
                ? extractExtensions(
                    account.data
                )
                : [];

        const data = {

            mintAuthority:
                info.mintAuthority ??
                null,

            freezeAuthority:
                info.freezeAuthority ??
                null,

            supply:
                BigInt(
                    String(
                        info.supply || '0'
                    )
                ),

            decimals:
                safeNumber(
                    info.decimals
                ),

            extensions
        };

        const result =
            calculateSecurity(
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
                        data.supply.toString(),

                    decimals:
                        data.decimals,

                    token2022,

                    extensions,

                    dangerousExtensions:
                        result.dangerous,

                    securityFlags:
                        result.flags,

                    securityError:
                        null
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

            securityState.pass++;

        } else if (
            result.decision ===
            'REVIEW'
        ) {

            securityState.review++;

        } else {

            securityState.reject++;
        }

        logSuccess(
            `🛡️ ${result.score}/100 ${result.decision} ${mintAddress}`
        );

        // حتى PASS وREVIEW ندخلهم DEX.
        // REJECT لا نضيّع API عليه إلا لاحقاً.

        if (
            result.decision !==
            'REJECT'
        ) {

            queueDex(
                mintAddress
            );
        }

        return result;

    } catch (error) {

        securityState.errors++;

        logError(
            `Security error ${mintAddress}`,
            error
        );

        await FreshToken.updateOne(
            {
                mint:
                    mintAddress
            },
            {
                $set: {

                    securityDecision:
                        'ERROR',

                    securityError:
                        error.message
                }
            }
        ).catch(() => {});

        return null;

    } finally {

        securityState.status =
            securityQueue.length
                ? 'scanning'
                : 'idle';
    }
}

// ======================================================
// SECURITY QUEUE
// ======================================================

function queueSecurity(mint) {

    if (
        !mint ||
        securityQueued.has(mint)
    ) {

        return;
    }

    securityQueued.add(mint);

    securityQueue.push(mint);

    runSecurityWorker()
        .catch(
            error =>
                logError(
                    'Security Worker',
                    error
                )
        );
}
