require('dotenv').config();
const express = require('express');
const { Telegraf } = require('telegraf');
const mongoose = require('mongoose');
const { Connection, PublicKey, Keypair } = require('@solana/web3.js');
const { Token, TOKEN_PROGRAM_ID } = require('@solana/spl-token');

const app = express();
app.use(express.json());

// ============ CONFIGURATION ============
const config = {
    telegramToken: process.env.TELEGRAM_TOKEN?.trim(),
    botPrivateKey: process.env.BOT_PRIVATE_KEY?.trim(),
    mongoURI: process.env.MONGODB_URI?.trim(),
    rpcUrl: process.env.RPC_URL || 'https://api.mainnet-beta.solana.com',
    port: process.env.PORT || 10000,
    
    // Trading Configuration
    MAX_TRADES: 5,
    MIN_BALANCE: 0.01,
    GAS_RESERVE: 0.005,
    STOP_LOSS: 0.10,
    TAKE_PROFIT: 0.20,
    TRAILING_STOP: 0.08,
};

// ============ DATABASE SCHEMAS ============
const tradeSchema = new mongoose.Schema({
    tradeId: { type: String, unique: true },
    tokenAddress: String,
    entryPrice: Number,
    currentPrice: Number,
    amount: Number,
    fees: Number,
    status: { type: String, default: 'OPEN' },
    profitLoss: Number,
    trailingStop: Number,
    highestPrice: Number,
    lowestPrice: Number,
    entryTime: { type: Date, default: Date.now },
    exitTime: Date,
    stopReason: String,
});

const walletSchema = new mongoose.Schema({
    address: { type: String, unique: true },
    balance: Number,
    gasReserve: Number,
    availableBalance: Number,
    lastUpdated: { type: Date, default: Date.now },
});

const whaleAlertSchema = new mongoose.Schema({
    alertId: { type: String, unique: true },
    tokenAddress: String,
    amount: Number,
    from: String,
    to: String,
    type: String,
    timestamp: { type: Date, default: Date.now },
});

const Trade = mongoose.models.Trade || mongoose.model('Trade', tradeSchema);
const Wallet = mongoose.models.Wallet || mongoose.model('Wallet', walletSchema);
const WhaleAlert = mongoose.models.WhaleAlert || mongoose.model('WhaleAlert', whaleAlertSchema);

// ============ CAPITAL MANAGEMENT SYSTEM ============
class CapitalManager {
    constructor(connection, wallet) {
        this.connection = connection;
        this.wallet = wallet;
        this.config = {
            NUM_TRADES: 5,
            GAS_RESERVE_PERCENT: 0.02,
            MIN_GAS_RESERVE: 0.005,
            MAX_GAS_PER_TRADE: 0.01,
        };
    }
    
    async getWalletBalance() {
        const balance = await this.connection.getBalance(this.wallet.publicKey);
        return balance / 1e9; // Convert to SOL
    }
    
    async calculateFees() {
        try {
            const { feeCalculator } = await this.connection.getRecentBlockhash();
            const baseFee = feeCalculator.lamportsPerSignature;
            const priorityFee = 5000; // Default priority fee
            const signatures = 3; // Average signatures per swap
            const computeUnits = 200000;
            const computePrice = 5000;
            
            const totalFees = (baseFee * signatures + priorityFee * signatures + computeUnits * computePrice) / 1e9;
            
            return {
                totalFees,
                baseFee: baseFee / 1e9,
                priorityFee: priorityFee * signatures / 1e9,
                computeFees: computeUnits * computePrice / 1e9,
            };
        } catch (error) {
            return {
                totalFees: 0.00017,
                baseFee: 0.000005,
                priorityFee: 0.0001,
                computeFees: 0.00005,
            };
        }
    }
    
    async calculateAllocation() {
        const totalBalance = await this.getWalletBalance();
        const fees = await this.calculateFees();
        const gasReserve = Math.max(fees.totalFees * 5, this.config.MIN_GAS_RESERVE);
        const availableBalance = Math.max(0, totalBalance - gasReserve);
        const perTrade = availableBalance / this.config.NUM_TRADES;
        const actualPerTrade = Math.max(0, perTrade - fees.totalFees);
        
        return {
            totalBalance,
            gasReserve,
            availableBalance,
            perTrade,
            actualPerTrade,
            fees,
            numTrades: this.config.NUM_TRADES,
        };
    }
}

// ============ TRAILING STOP SYSTEM ============
class TrailingStopSystem {
    constructor() {
        this.activeStops = new Map();
        this.config = {
            INITIAL_STOP: 0.10,
            TRAILING_START: 0.15,
            TRAILING_DISTANCE: 0.08,
            CHECK_INTERVAL: 5000, // 5 seconds
        };
    }
    
    async startTrailing(tradeId, entryPrice) {
        const stop = {
            tradeId,
            entryPrice,
            highestPrice: entryPrice,
            stopPrice: entryPrice * (1 - this.config.INITIAL_STOP),
            isTrailing: false,
            startTime: Date.now(),
        };
        
        this.activeStops.set(tradeId, stop);
        
        // Start monitoring
        this.monitor(tradeId);
        
        return stop;
    }
    
    async monitor(tradeId) {
        const interval = setInterval(async () => {
            const stop = this.activeStops.get(tradeId);
            if (!stop) {
                clearInterval(interval);
                return;
            }
            
            // Update prices
            const currentPrice = await this.getPrice(stop.tradeId);
            stop.currentPrice = currentPrice;
            
            if (currentPrice > stop.highestPrice) {
                stop.highestPrice = currentPrice;
            }
            
            // Check if trailing should start
            const profitPercent = (currentPrice - stop.entryPrice) / stop.entryPrice;
            if (profitPercent >= this.config.TRAILING_START) {
                stop.isTrailing = true;
                stop.stopPrice = stop.highestPrice * (1 - this.config.TRAILING_DISTANCE);
            }
            
            // Check stop loss
            if (currentPrice <= stop.stopPrice) {
                await this.triggerStopLoss(tradeId);
                clearInterval(interval);
            }
        }, this.config.CHECK_INTERVAL);
    }
    
    async getPrice(tokenAddress) {
        // Implement price fetching from Jupiter or other DEX
        return 0; // Placeholder
    }
    
    async triggerStopLoss(tradeId) {
        const stop = this.activeStops.get(tradeId);
        if (stop) {
            console.log(`🛑 Stop loss triggered for ${tradeId}`);
            this.activeStops.delete(tradeId);
            // Execute sell order here
        }
    }
}

// ============ MAIN BOT CLASS ============
class TradingBot {
    constructor() {
        this.connection = null;
        this.wallet = null;
        this.telegram = null;
        this.capitalManager = null;
        this.trailingStop = new TrailingStopSystem();
        this.isRunning = false;
    }
    
    async initialize() {
        console.log('🚀 Initializing Trading Bot...');
        
        // Initialize Solana connection
        this.connection = new Connection(config.rpcUrl, 'confirmed');
        
        // Load wallet
        await this.loadWallet();
        
        // Connect to MongoDB
        await this.connectDatabase();
        
        // Initialize capital manager
        this.capitalManager = new CapitalManager(this.connection, this.wallet);
        
        // Initialize Telegram
        await this.initializeTelegram();
        
        this.isRunning = true;
        console.log('✅ Trading Bot initialized successfully!');
    }
    
    async loadWallet() {
        try {
            if (config.botPrivateKey) {
                let secretKey;
                try {
                    secretKey = JSON.parse(config.botPrivateKey);
                } catch {
                    secretKey = config.botPrivateKey.split(',').map(n => parseInt(n.trim()));
                }
                
                if (Array.isArray(secretKey)) {
                    this.wallet = Keypair.fromSecretKey(Buffer.from(secretKey));
                    console.log(`✅ Wallet loaded: ${this.wallet.publicKey.toString()}`);
                }
            }
        } catch (error) {
            console.error('❌ Error loading wallet:', error.message);
        }
    }
    
    async connectDatabase() {
        if (config.mongoURI) {
            try {
                await mongoose.connect(config.mongoURI, {
                    useNewUrlParser: true,
                    useUnifiedTopology: true,
                    serverSelectionTimeoutMS: 5000,
                });
                console.log('✅ MongoDB connected successfully!');
            } catch (error) {
                console.error('❌ MongoDB connection error:', error.message);
            }
        }
    }
    
    async initializeTelegram() {
        if (config.telegramToken) {
            try {
                this.telegram = new Telegraf(config.telegramToken);
                this.setupTelegramCommands();
                await this.telegram.launch();
                console.log('✅ Telegram bot launched!');
            } catch (error) {
                console.error('❌ Telegram launch error:', error.message);
            }
        }
    }
    
    setupTelegramCommands() {
        this.telegram.start(async (ctx) => {
            await ctx.reply(
                '🤖 **ULTRA TRADING BOT** 🤖\n\n' +
                'أهلاً بك في أقوى بوت تداول!\n\n' +
                '📊 **الأوامر:**\n' +
                '/status - حالة البوت\n' +
                '/balance - رصيد المحفظة\n' +
                '/trades - الصفقات النشطة\n' +
                '/capital - توزيع رأس المال\n' +
                '/help - المساعدة',
                { parse_mode: 'Markdown' }
            );
        });
        
        this.telegram.command('status', async (ctx) => {
            const uptime = process.uptime();
            const memory = process.memoryUsage();
            
            await ctx.reply(
                `📊 **حالة البوت**\n\n` +
                `✅ الحالة: يعمل\n` +
                `⏱️ وقت التشغيل: ${Math.floor(uptime / 60)} دقيقة\n` +
                `💾 الذاكرة: ${(memory.heapUsed / 1024 / 1024).toFixed(2)} MB\n` +
                `🔗 الشبكة: Solana Mainnet\n` +
                `📈 الصفقات النشطة: ${this.trailingStop.activeStops.size}`,
                { parse_mode: 'Markdown' }
            );
        });
        
        this.telegram.command('balance', async (ctx) => {
            if (!this.wallet || !this.capitalManager) {
                return ctx.reply('❌ المحفظة غير مهيأة');
            }
            
            const allocation = await this.capitalManager.calculateAllocation();
            
            await ctx.reply(
                `💰 **رصيد المحفظة**\n\n` +
                `💎 الرصيد الكلي: ${allocation.totalBalance.toFixed(4)} SOL\n` +
                `⛽ احتياطي الغاز: ${allocation.gasReserve.toFixed(4)} SOL\n` +
                `📊 المتاح للتداول: ${allocation.availableBalance.toFixed(4)} SOL\n` +
                `💵 نصيب كل صفقة: ${allocation.actualPerTrade.toFixed(4)} SOL\n` +
                `🔢 عدد الصفقات: ${allocation.numTrades}`,
                { parse_mode: 'Markdown' }
            );
        });
        
        this.telegram.command('capital', async (ctx) => {
            if (!this.capitalManager) {
                return ctx.reply('❌ نظام إدارة رأس المال غير مهيأ');
            }
            
            const allocation = await this.capitalManager.calculateAllocation();
            
            const message = 
                `💰 **توزيع رأس المال**\n\n` +
                `💎 الرصيد: ${allocation.totalBalance.toFixed(4)} SOL\n` +
                `⛽ الغاز: ${allocation.gasReserve.toFixed(4)} SOL\n` +
                `📊 المتاح: ${allocation.availableBalance.toFixed(4)} SOL\n` +
                `💵 لكل صفقة: ${allocation.actualPerTrade.toFixed(4)} SOL\n\n` +
                `📋 **تفاصيل الرسوم:**\n` +
                `• أساسية: ${allocation.fees.baseFee.toFixed(6)} SOL\n` +
                `• أولوية: ${allocation.fees.priorityFee.toFixed(6)} SOL\n` +
                `• Compute: ${allocation.fees.computeFees.toFixed(6)} SOL\n` +
                `• الإجمالي: ${allocation.fees.totalFees.toFixed(6)} SOL`;
            
            await ctx.reply(message, { parse_mode: 'Markdown' });
        });
        
        this.telegram.command('trades', async (ctx) => {
            const trades = await Trade.find({ status: 'OPEN' }).limit(10);
            
            if (trades.length === 0) {
                return ctx.reply('📭 لا توجد صفقات نشطة');
            }
            
            let message = '📈 **الصفقات النشطة:**\n\n';
            for (const trade of trades) {
                message += `🔖 ${trade.tradeId.slice(0, 8)}...\n`;
                message += `💰 ${trade.tokenAddress.slice(0, 8)}...\n`;
                message += `📊 الدخول: ${trade.entryPrice}\n`;
                message += `💵 الحالي: ${trade.currentPrice}\n`;
                message += `📈 الربح: ${(trade.profitLoss * 100).toFixed(2)}%\n\n`;
            }
            
            await ctx.reply(message, { parse_mode: 'Markdown' });
        });
    }
    
    async start() {
        await this.initialize();
        
        // Start Express server
        app.get('/', (req, res) => {
            res.json({
                status: 'running',
                uptime: process.uptime(),
                timestamp: Date.now(),
            });
        });
        
        app.get('/health', (req, res) => {
            res.json({
                status: 'healthy',
                wallet: this.wallet?.publicKey.toString(),
                database: mongoose.connection.readyState === 1 ? 'connected' : 'disconnected',
                telegram: this.telegram ? 'active' : 'inactive',
            });
        });
        
        app.listen(config.port, () => {
            console.log(`🌐 Server running on port ${config.port}`);
            console.log('🎯 ULTRA TRADING BOT IS FULLY OPERATIONAL!');
        });
    }
}

// ============ START THE BOT ============
const bot = new TradingBot();
bot.start().catch(error => {
    console.error('❌ Fatal error:', error);
    process.exit(1);
});
