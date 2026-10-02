import fastify from 'fastify';
import fastifyWebsocket from '@fastify/websocket';
import pg from 'pg';
import Decimal from 'decimal.js';
import crypto from 'crypto';
import { WebSocket } from 'ws';

// ==========================================
// 1. HASSASİYET VE AMM MATEMATİK MOTORU
// ==========================================
Decimal.set({ precision: 50, rounding: Decimal.ROUND_DOWN });

class MoneyMath {
    static roundDown(val, scale = 6) {
        return new Decimal(val).toDecimalPlaces(scale, Decimal.ROUND_DOWN);
    }
    static roundUp(val, scale = 6) {
        return new Decimal(val).toDecimalPlaces(scale, Decimal.ROUND_UP);
    }
}

class AMMEngine {
    static calculateBuy(state, mGross, feeRate) {
        const fee = MoneyMath.roundUp(mGross.mul(feeRate), 6);
        const mNet = mGross.minus(fee);
        if (mNet.lte(0)) throw new Error('INVALID_AMOUNT: Net tutar 0 veya negatif olamaz');

        const k = state.yesReserve.mul(state.noReserve);
        const newNoReserve = state.noReserve.plus(mNet);
        const newYesReserve = k.div(newNoReserve);
        const deltaY = state.yesReserve.minus(newYesReserve);
        const sharesOut = MoneyMath.roundDown(mNet.plus(deltaY), 6);

        return { mNet, fee, newYesReserve, newNoReserve, sharesOut };
    }
}

// ==========================================
// 2. VERİTABANI BAĞLANTISI VE OTOMATİK MİGRASYON
// ==========================================
pg.types.setTypeParser(1700, (val) => val);
const pool = new pg.Pool({
    connectionString: process.env.DATABASE_URL || 'postgresql://postgres:oyver_secure@localhost:5432/oyver',
    max: 20
});

async function runInTransaction(callback) {
    const client = await pool.connect();
    try {
        await client.query('BEGIN;');
        const result = await callback(client);
        await client.query('COMMIT;');
        return result;
    } catch (error) {
        await client.query('ROLLBACK;');
        throw error;
    } finally {
        client.release();
    }
}

async function initDatabase() {
    const client = await pool.connect();
    try {
        console.info('[DATABASE] Şema ve tablolar doğrulanıyor...');
        
        // 1. Tabloları oluştur
        await client.query(`
            CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

            CREATE TABLE IF NOT EXISTS users (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                email VARCHAR(255) UNIQUE NOT NULL,
                username VARCHAR(64) UNIQUE NOT NULL,
                balance_kor NUMERIC(24,6) NOT NULL DEFAULT 14500.000000 CHECK (balance_kor >= 0),
                streak INT NOT NULL DEFAULT 5,
                quests_today INT NOT NULL DEFAULT 0,
                quest_rewarded BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS markets (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                slug VARCHAR(255) UNIQUE NOT NULL,
                category VARCHAR(64) NOT NULL DEFAULT 'GÜNDEM',
                question TEXT NOT NULL,
                source_url TEXT,
                source_name VARCHAR(128),
                is_sponsored BOOLEAN NOT NULL DEFAULT FALSE,
                sponsored_by VARCHAR(128),
                status VARCHAR(32) NOT NULL DEFAULT 'TRADING',
                closing_date VARCHAR(64) NOT NULL DEFAULT '31 Aralık 2026',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS amm_state (
                market_id UUID PRIMARY KEY REFERENCES markets(id) ON DELETE CASCADE,
                yes_reserve NUMERIC(30,12) NOT NULL CHECK (yes_reserve > 0),
                no_reserve NUMERIC(30,12) NOT NULL CHECK (no_reserve > 0),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS positions (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                market_id UUID NOT NULL REFERENCES markets(id) ON DELETE CASCADE,
                user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                outcome VARCHAR(8) NOT NULL,
                shares NUMERIC(30,12) NOT NULL DEFAULT 0 CHECK (shares >= 0),
                total_invested NUMERIC(24,6) NOT NULL DEFAULT 0,
                realized_pnl NUMERIC(24,6) NOT NULL DEFAULT 0,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT uq_user_market_outcome UNIQUE (user_id, market_id, outcome)
            );

            CREATE TABLE IF NOT EXISTS ledger_entries (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                entry_type VARCHAR(64) NOT NULL,
                reference_id UUID,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS sessions (
                token CHAR(64) PRIMARY KEY,
                user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);

        // 2. OTOMATİK MİGRASYON: Eski veritabanı tablolarındaki eksik sütunları ekle
        console.info('[DATABASE] Sütun migrasyonları uygulanıyor...');
        await client.query(`
            ALTER TABLE users ADD COLUMN IF NOT EXISTS balance_kor NUMERIC(24,6) NOT NULL DEFAULT 14500.000000;
            ALTER TABLE users ADD COLUMN IF NOT EXISTS streak INT NOT NULL DEFAULT 5;
            ALTER TABLE users ADD COLUMN IF NOT EXISTS quests_today INT NOT NULL DEFAULT 0;
            ALTER TABLE users ADD COLUMN IF NOT EXISTS quest_rewarded BOOLEAN NOT NULL DEFAULT FALSE;

            ALTER TABLE markets ADD COLUMN IF NOT EXISTS is_sponsored BOOLEAN NOT NULL DEFAULT FALSE;
            ALTER TABLE markets ADD COLUMN IF NOT EXISTS sponsored_by VARCHAR(128);
            ALTER TABLE markets ADD COLUMN IF NOT EXISTS closing_date VARCHAR(64) NOT NULL DEFAULT '31 Aralık 2026';
            ALTER TABLE markets ADD COLUMN IF NOT EXISTS source_url TEXT;
            ALTER TABLE markets ADD COLUMN IF NOT EXISTS source_name VARCHAR(128);

            ALTER TABLE positions ADD COLUMN IF NOT EXISTS total_invested NUMERIC(24,6) NOT NULL DEFAULT 0;
            ALTER TABLE positions ADD COLUMN IF NOT EXISTS realized_pnl NUMERIC(24,6) NOT NULL DEFAULT 0;
        `);

        // 3. Demo Kullanıcıları ve Top 100 Kahin Başlangıç Verisi
        const checkUsers = await client.query(`SELECT count(*) FROM users`);
        if (parseInt(checkUsers.rows[0].count, 10) === 0) {
            console.info('[DATABASE] Kullanıcı tohumları yükleniyor...');
            await client.query(`
                INSERT INTO users (id, email, username, balance_kor, streak, quests_today) VALUES
                ('11111111-1111-1111-1111-111111111111', 'demo@oyver.pro', 'LeisanB', 14500, 5, 1),
                ('22222222-2222-2222-2222-222222222222', 'ahmet@oyver.pro', 'Ahmet_Kahin', 420000, 12, 3),
                ('33333333-3333-3333-3333-333333333333', 'ece@oyver.pro', 'Ece_Analist', 315000, 8, 3),
                ('44444444-4444-4444-4444-444444444444', 'quant@oyver.pro', 'QuantTraderTR', 280000, 6, 2),
                ('55555555-5555-5555-5555-555555555555', 'zeki@oyver.pro', 'Zeki_Forecaster', 195000, 4, 1)
                ON CONFLICT (email) DO NOTHING;
            `);
        }

        // 4. Başlangıç Pazarları
        const checkMarket = await client.query(`SELECT id FROM markets LIMIT 1`);
        if (checkMarket.rows.length === 0) {
            console.info('[DATABASE] Canlı pazarlar tohumlanıyor...');
            const initialMarkets = [
                {
                    slug: 'asgari-ucret-2026',
                    cat: 'SİYASET',
                    q: '2026 Yılı Asgari Ücreti 30.000 TL Üzerinde Açıklanır mı?',
                    srcName: 'Resmi Gazete',
                    srcUrl: 'https://www.resmigazete.gov.tr',
                    spons: false,
                    by: null,
                    closing: '31 Aralık 2026',
                    yesR: 12000,
                    noR: 6500
                },
                {
                    slug: 'togg-t10f-teslimat',
                    cat: 'SPONSORLU',
                    q: 'TOGG T10F Sedan Modeli 2026 Q3 Öncesi Teslimata Başlar mı?',
                    srcName: 'TOGG Basın Bülteni',
                    srcUrl: 'https://togg.com.tr',
                    spons: true,
                    by: 'TOGG',
                    closing: '30 Eylül 2026',
                    yesR: 15000,
                    noR: 3500
                },
                {
                    slug: 'faiz-indirimi-2026',
                    cat: 'EKONOMİ',
                    q: 'TCMB Politika Faizini 2026 Yıl Sonuna Kadar %30 Altına İndirir mi?',
                    srcName: 'TCMB Kararı',
                    srcUrl: 'https://tcmb.gov.tr',
                    spons: false,
                    by: null,
                    closing: '24 Aralık 2026',
                    yesR: 7000,
                    noR: 10500
                },
                {
                    slug: 'dunya-kupasi-elemeleri',
                    cat: 'SPOR',
                    q: 'A Milli Takım 2026 Dünya Kupası Elemelerinde Grubunu Lider Bitirir mi?',
                    srcName: 'TFF',
                    srcUrl: 'https://tff.org',
                    spons: false,
                    by: null,
                    closing: '15 Kasım 2026',
                    yesR: 8500,
                    noR: 8500
                }
            ];

            for (const item of initialMarkets) {
                const mRes = await client.query(`
                    INSERT INTO markets (slug, category, question, source_name, source_url, is_sponsored, sponsored_by, closing_date)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id
                `, [item.slug, item.cat, item.q, item.srcName, item.srcUrl, item.spons, item.by, item.closing]);
                const mId = mRes.rows[0].id;
                await client.query(`INSERT INTO amm_state (market_id, yes_reserve, no_reserve) VALUES ($1, $2, $3)`, [mId, item.yesR, item.noR]);
            }
        }
        console.info('[DATABASE] Sistem başarıyla hazırlandı.');
    } finally {
        client.release();
    }
}

// ==========================================
// 3. SUNUCU, REST VE WEBSOCKET
// ==========================================
const app = fastify({ logger: false });
await app.register(fastifyWebsocket);

const wsClients = new Set();
function broadcast(type, payload) {
    const msg = JSON.stringify({ type, ...payload });
    for (const ws of wsClients) {
        if (ws.readyState === WebSocket.OPEN) ws.send(msg);
    }
}

app.addHook('onRequest', async (req) => {
    let token = null;
    const cookie = req.headers.cookie;
    if (cookie) {
        const m = cookie.split(';').find(c => c.trim().startsWith('oyver_token='));
        if (m) token = m.split('=')[1].trim();
    }
    if (token) {
        const r = await pool.query(`SELECT user_id FROM sessions WHERE token = $1`, [token]);
        if (r.rows.length > 0) req.userId = r.rows[0].user_id;
    }
    if (!req.userId) req.userId = '11111111-1111-1111-1111-111111111111';
});

app.get('/ws', { websocket: true }, (connection) => {
    const ws = connection.socket;
    wsClients.add(ws);
    ws.on('close', () => wsClients.delete(ws));
});

app.get('/health', async () => ({ status: 'UP', time: new Date().toISOString() }));

app.get('/api/me', async (req) => {
    const r = await pool.query(`
        SELECT id, username, email, balance_kor, streak, quests_today, quest_rewarded 
        FROM users WHERE id = $1
    `, [req.userId]);
    return r.rows[0] || {};
});

app.post('/api/auth/login-mock', async (req, rep) => {
    const { username } = req.body || {};
    const name = username || 'LeisanB';
    const email = `${name.toLowerCase()}@oyver.pro`;

    const ur = await pool.query(`
        INSERT INTO users (email, username, balance_kor, streak)
        VALUES ($1, $2, 14500, 5)
        ON CONFLICT (email) DO UPDATE SET username = EXCLUDED.username
        RETURNING id, username, balance_kor, streak
    `, [email, name]);

    const user = ur.rows[0];
    const token = crypto.randomBytes(32).toString('hex');
    await pool.query(`INSERT INTO sessions (token, user_id) VALUES ($1, $2)`, [token, user.id]);

    rep.header('Set-Cookie', `oyver_token=${token}; Path=/; HttpOnly; SameSite=Lax; Max-Age=2592000`);
    return { success: true, user };
});

app.get('/api/markets', async () => {
    const r = await pool.query(`
        SELECT m.*, a.yes_reserve, a.no_reserve 
        FROM markets m 
        JOIN amm_state a ON m.id = a.market_id 
        ORDER BY m.created_at DESC
    `);
    const formatted = r.rows.map(m => {
        const y = new Decimal(m.yes_reserve);
        const n = new Decimal(m.no_reserve);
        const probYes = Math.round(n.div(y.plus(n)).mul(100).toNumber());
        const poolTotal = Math.round(y.plus(n).toNumber());
        return {
            ...m,
            probYes,
            probNo: 100 - probYes,
            poolTotal: poolTotal.toLocaleString('tr-TR')
        };
    });
    return { markets: formatted };
});

app.get('/api/leaderboard', async () => {
    const r = await pool.query(`
        SELECT u.username, u.balance_kor, u.streak,
               COALESCE(SUM(p.realized_pnl), 0) + (u.balance_kor - 14500) as net_pnl
        FROM users u
        LEFT JOIN positions p ON u.id = p.user_id
        GROUP BY u.id, u.username, u.balance_kor, u.streak
        ORDER BY net_pnl DESC
        LIMIT 100
    `);

    const top100 = r.rows.map((row, idx) => {
        const pnlNum = parseFloat(row.net_pnl);
        const sign = pnlNum >= 0 ? '+' : '';
        return {
            rank: idx + 1,
            name: row.username,
            pnl: `${sign}${Math.round(pnlNum).toLocaleString('tr-TR')} KOR`,
            streak: row.streak,
            winRate: `%${75 - (idx * 2)}`
        };
    });
    return { top100 };
});

// TAHMİN YAPMA (AMM & DEFTER)
app.post('/api/predict', async (req, rep) => {
    const { marketId, outcome, amountKor } = req.body || {};
    const amt = new Decimal(amountKor || 0);
    if (!marketId || !outcome || amt.lte(0)) {
        return rep.status(400).send({ error: 'Geçersiz tutar veya parametre' });
    }

    try {
        const result = await runInTransaction(async (c) => {
            const ur = await c.query(
                `UPDATE users SET balance_kor = balance_kor - $1 WHERE id = $2 AND balance_kor >= $1 RETURNING balance_kor, quests_today, quest_rewarded`,
                [amt.toFixed(6), req.userId]
            );
            if (ur.rows.length === 0) throw new Error('Yetersiz KOR bakiyesi');

            const mr = await c.query(
                `SELECT yes_reserve, no_reserve FROM amm_state WHERE market_id = $1 FOR UPDATE`,
                [marketId]
            );
            if (mr.rows.length === 0) throw new Error('Pazar bulunamadı');

            const isYes = outcome === 'YES';
            const amm = {
                yesReserve: new Decimal(mr.rows[0].yes_reserve),
                noReserve: new Decimal(mr.rows[0].no_reserve)
            };

            const calc = AMMEngine.calculateBuy(
                isYes ? amm : { yesReserve: amm.noReserve, noReserve: amm.yesReserve },
                amt,
                new Decimal(0.02)
            );

            const ny = isYes ? calc.newYesReserve : calc.newNoReserve;
            const nn = isYes ? calc.newNoReserve : calc.newYesReserve;
            await c.query(`UPDATE amm_state SET yes_reserve = $1, no_reserve = $2, updated_at = NOW() WHERE market_id = $3`, [ny.toFixed(12), nn.toFixed(12), marketId]);

            await c.query(`INSERT INTO ledger_entries (entry_type, reference_id) VALUES ('TRADE_BUY', $1)`, [marketId]);
            await c.query(`
                INSERT INTO positions (market_id, user_id, outcome, shares, total_invested)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (user_id, market_id, outcome) DO UPDATE 
                SET shares = positions.shares + $4, total_invested = positions.total_invested + $5, updated_at = NOW()
            `, [marketId, req.userId, outcome, calc.sharesOut.toFixed(6), amt.toFixed(6)]);

            let quests = ur.rows[0].quests_today + 1;
            let rewarded = ur.rows[0].quest_rewarded;
            let currentBal = new Decimal(ur.rows[0].balance_kor);
            let bonusAdded = false;

            if (quests >= 3 && !rewarded) {
                currentBal = currentBal.plus(150);
                rewarded = true;
                bonusAdded = true;
                await c.query(`INSERT INTO ledger_entries (entry_type, reference_id) VALUES ('QUEST_REWARD', $1)`, [req.userId]);
            }

            await c.query(`
                UPDATE users SET quests_today = $1, quest_rewarded = $2, balance_kor = $3 WHERE id = $4
            `, [quests, rewarded, currentBal.toFixed(6), req.userId]);

            return {
                newBalance: MoneyMath.roundDown(currentBal, 0).toNumber(),
                sharesOut: calc.sharesOut.toFixed(2),
                questsToday: quests,
                bonusAdded,
                newYesR: ny,
                newNoR: nn
            };
        });

        const y = new Decimal(result.newYesR), n = new Decimal(result.newNoR);
        const probYes = Math.round(n.div(y.plus(n)).mul(100).toNumber());
        broadcast('MARKET_UPDATE', {
            marketId,
            yesReserve: y.toFixed(2),
            noReserve: n.toFixed(2),
            probYes,
            probNo: 100 - probYes,
            poolTotal: Math.round(y.plus(n).toNumber()).toLocaleString('tr-TR')
        });

        return rep.send({
            success: true,
            balanceKor: result.newBalance,
            sharesOut: result.sharesOut,
            questsToday: result.questsToday,
            bonusAdded: result.bonusAdded
        });
    } catch (e) {
        return rep.status(400).send({ error: e.message });
    }
});

// UGC (ANKET AÇMA) & 10.000 KOR TEMİNAT
app.post('/api/markets/create', async (req, rep) => {
    const { question, category, sourceName, sourceUrl, closingDate } = req.body || {};
    if (!question || !category || !sourceName) {
        return rep.status(400).send({ error: 'Lütfen tüm alanları doldurun.' });
    }

    try {
        const result = await runInTransaction(async (c) => {
            const stakeAmt = new Decimal(10000);
            const ur = await c.query(
                `UPDATE users SET balance_kor = balance_kor - $1 WHERE id = $2 AND balance_kor >= $1 RETURNING balance_kor`,
                [stakeAmt.toFixed(6), req.userId]
            );
            if (ur.rows.length === 0) throw new Error('Anket açmak için en az 10.000 KOR teminata ihtiyacınız var.');

            const slug = question.toLowerCase().replace(/[^a-z0-9]/g, '-').slice(0, 40) + '-' + Date.now().toString().slice(-4);
            const mRes = await c.query(`
                INSERT INTO markets (slug, category, question, source_name, source_url, closing_date)
                VALUES ($1, $2, $3, $4, $5, $6) RETURNING id
            `, [slug, category, question, sourceName, sourceUrl || 'https://resmigazete.gov.tr', closingDate || '31 Aralık 2026']);

            const mId = mRes.rows[0].id;
            await c.query(`INSERT INTO amm_state (market_id, yes_reserve, no_reserve) VALUES ($1, 5000, 5000)`, [mId]);
            await c.query(`INSERT INTO ledger_entries (entry_type, reference_id) VALUES ('UGC_STAKE', $1)`, [mId]);

            return {
                marketId: mId,
                newBalance: MoneyMath.roundDown(ur.rows[0].balance_kor, 0).toNumber()
            };
        });

        broadcast('NEW_MARKET', { marketId: result.marketId });
        return rep.send({ success: true, ...result });
    } catch (e) {
        return rep.status(400).send({ error: e.message });
    }
});

// PRO KOR MAĞAZASI
app.post('/api/shop/buy-kor', async (req, rep) => {
    const { amountKor, packageTitle } = req.body || {};
    const amt = new Decimal(amountKor || 0);
    if (amt.lte(0)) return rep.status(400).send({ error: 'Geçersiz KOR tutarı' });

    try {
        const ur = await pool.query(
            `UPDATE users SET balance_kor = balance_kor + $1 WHERE id = $2 RETURNING balance_kor`,
            [amt.toFixed(6), req.userId]
        );
        await pool.query(`INSERT INTO ledger_entries (entry_type, reference_id) VALUES ('SHOP_PURCHASE', $1)`, [req.userId]);

        const newBal = MoneyMath.roundDown(ur.rows[0].balance_kor, 0).toNumber();
        return rep.send({ success: true, newBalance: newBal, packageTitle });
    } catch (e) {
        return rep.status(500).send({ error: e.message });
    }
});

// ==========================================
// 4. FRONTEND ARAYÜZÜ (TAM ETKİLEŞİMLİ)
// ==========================================
app.get('/', async (req, reply) => {
    return reply.type('text/html').send(`<!DOCTYPE html>
<html lang="tr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>OYVER PRO - Canlı Sosyal Tahmin ve Karar Motoru</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
    <style>
        body { font-family: 'Inter', sans-serif; }
        .glass-blur { backdrop-filter: blur(12px); -webkit-backdrop-filter: blur(12px); }
        ::-webkit-scrollbar { width: 6px; }
        ::-webkit-scrollbar-track { background: #0f172a; }
        ::-webkit-scrollbar-thumb { background: #334155; border-radius: 3px; }
    </style>
</head>
<body class="bg-slate-900 text-slate-100 min-h-screen flex flex-col antialiased">

    <!-- TOAST BİLDİRİM KONTEYNERİ -->
    <div id="toast-container" class="fixed bottom-5 right-5 z-50 flex flex-col gap-2 pointer-events-none"></div>

    <!-- HEADER -->
    <header class="sticky top-0 z-40 bg-slate-900/90 backdrop-blur-md border-b border-slate-800">
        <div class="container mx-auto px-4 h-16 flex items-center justify-between gap-4">
            <div class="flex items-center space-x-6">
                <a href="#" class="text-2xl font-black tracking-tight text-transparent bg-clip-text bg-gradient-to-r from-purple-400 via-pink-400 to-orange-400">
                    OYVER<span class="text-xs ml-1 px-1.5 py-0.5 rounded bg-purple-500/20 text-purple-300 border border-purple-500/30">PRO</span>
                </a>
                <nav class="hidden md:flex items-center space-x-6 text-sm font-semibold">
                    <a href="#markets" class="text-white hover:text-purple-400 transition">Pazarlar</a>
                    <button id="btn-open-leaderboard" class="text-slate-400 hover:text-white transition flex items-center gap-1.5">
                        <i class="fas fa-trophy text-amber-400 text-xs"></i> Top 100 Kahin
                    </button>
                    <button id="btn-nav-ugc" class="text-slate-400 hover:text-white transition flex items-center gap-1.5">
                        <i class="fas fa-plus-circle text-purple-400 text-xs"></i> Anket Başlat
                    </button>
                    <button id="btn-open-shop-nav" class="px-2 py-0.5 rounded-full text-xs font-bold bg-amber-500/10 text-amber-400 border border-amber-500/20 hover:bg-amber-500/20 transition">
                        👑 KOR Mağazası
                    </button>
                </nav>
            </div>

            <!-- Kullanıcı Paneli -->
            <div class="flex items-center space-x-3">
                <div id="user-badge" class="flex items-center bg-slate-800 border border-slate-700 rounded-lg p-1 pr-3 space-x-3 cursor-pointer" onclick="promptSwitchUser()" title="Kullanıcı değiştirmek için tıkla">
                    <div class="flex items-center gap-1.5 bg-slate-900 px-2.5 py-1 rounded-md border border-slate-700/60">
                        <i class="fas fa-fire text-orange-500 text-xs"></i>
                        <span id="user-streak" class="text-xs font-black text-orange-400">5 Gün</span>
                    </div>
                    <div class="flex items-center gap-1.5">
                        <i class="fas fa-coins text-amber-400 text-xs"></i>
                        <span id="user-balance" class="text-xs font-bold text-amber-200">-- KOR</span>
                    </div>
                    <div id="user-avatar" class="w-6 h-6 rounded-full bg-gradient-to-tr from-purple-500 to-pink-500 flex items-center justify-center text-xs font-black">L</div>
                </div>
            </div>
        </div>
    </header>

    <!-- QUESTS BAR -->
    <section class="bg-slate-800/60 border-b border-slate-800 py-2.5 px-4 text-xs font-medium">
        <div class="container mx-auto flex flex-wrap items-center justify-between gap-3">
            <div class="flex items-center gap-2 text-slate-300">
                <span class="px-2 py-0.5 rounded bg-purple-500/20 text-purple-300 font-bold uppercase text-[10px]">Günlük Görev</span>
                <span>Gündem Oylamalarına Katıl</span>
                <span id="quest-label" class="text-purple-400 font-bold">(0/3 Tamamlandı)</span>
            </div>
            <div class="flex items-center gap-4 text-slate-400">
                <div class="w-32 bg-slate-700 rounded-full h-1.5 overflow-hidden">
                    <div id="quest-progress-bar" class="bg-gradient-to-r from-purple-500 to-pink-500 h-full w-0 transition-all duration-500"></div>
                </div>
                <span id="quest-reward-text" class="text-amber-400 font-semibold">+150 KOR Ödül</span>
            </div>
        </div>
    </section>

    <!-- HERO SECTION -->
    <section id="hero" class="relative overflow-hidden py-12 md:py-16 border-b border-slate-800">
        <div class="relative z-10 container mx-auto px-4 text-center max-w-4xl">
            <div class="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-slate-800/80 border border-slate-700 text-xs font-semibold text-purple-300 mb-4">
                <span class="w-2 h-2 rounded-full bg-emerald-400 animate-ping"></span>
                PostgreSQL & AMM Destekli Karar Piyasası
            </div>
            <h1 class="text-3xl sm:text-5xl font-extrabold text-transparent bg-clip-text bg-gradient-to-r from-purple-400 via-pink-400 to-orange-400 tracking-tight mb-4">
                Yarının Nabzını Bugün Tutun.
            </h1>
            <p class="text-sm sm:text-base text-slate-400 max-w-2xl mx-auto mb-6">
                Kolektif zekaya katıl, KOR puanınla fikrini savun, Top 100 Kahin ligine adını yazdır.
            </p>
            <div class="flex justify-center gap-3">
                <button id="btn-hero-ugc" class="bg-gradient-to-r from-purple-600 to-pink-600 hover:from-purple-500 hover:to-pink-500 text-white font-bold py-2.5 px-6 rounded-xl text-xs flex items-center gap-2 shadow-lg transition">
                    <i class="fas fa-plus"></i> Kendi Anketini Aç (10.000 KOR)
                </button>
            </div>
        </div>
    </section>

    <!-- KATEGORİ FİLTRELERİ -->
    <section class="container mx-auto px-4 pt-8 pb-2">
        <div class="flex items-center space-x-2 overflow-x-auto pb-2">
            <button class="cat-btn active bg-purple-600 text-white px-4 py-1.5 rounded-lg text-xs font-bold transition" data-cat="ALL">Tümü</button>
            <button class="cat-btn bg-slate-800 hover:bg-slate-700 text-slate-300 px-4 py-1.5 rounded-lg text-xs font-bold transition" data-cat="SİYASET">Siyaset</button>
            <button class="cat-btn bg-slate-800 hover:bg-slate-700 text-slate-300 px-4 py-1.5 rounded-lg text-xs font-bold transition" data-cat="EKONOMİ">Ekonomi</button>
            <button class="cat-btn bg-slate-800 hover:bg-slate-700 text-slate-300 px-4 py-1.5 rounded-lg text-xs font-bold transition" data-cat="SPOR">Spor</button>
            <button class="cat-btn bg-slate-800 hover:bg-slate-700 text-slate-300 px-4 py-1.5 rounded-lg text-xs font-bold transition" data-cat="SPONSORLU">📢 Sponsorlu</button>
        </div>
    </section>

    <!-- MARKET GRID -->
    <section id="markets" class="container mx-auto px-4 py-6">
        <div id="market-grid" class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6"></div>
    </section>

    <!-- SAĞDAN AÇILAN TAHMİN ÇEKMECESİ -->
    <div id="drawer-backdrop" class="fixed inset-0 bg-slate-950/70 z-50 backdrop-blur-sm hidden transition-opacity duration-300 opacity-0"></div>
    <aside id="trade-drawer" class="fixed right-0 top-0 bottom-0 w-full max-w-md bg-slate-900 border-l border-slate-800 z-50 transform translate-x-full transition-transform duration-300 overflow-y-auto flex flex-col">
        <div class="p-5 border-b border-slate-800 flex items-center justify-between bg-slate-900/90 sticky top-0 z-10">
            <span id="drawer-category" class="px-2 py-0.5 rounded text-[10px] font-black uppercase tracking-wider bg-purple-500/20 text-purple-300">KATEGORİ</span>
            <button id="btn-close-drawer" class="text-slate-400 hover:text-white p-1 text-lg"><i class="fas fa-times"></i></button>
        </div>

        <div class="p-6 space-y-6 flex-grow">
            <div>
                <h3 id="drawer-title" class="text-lg font-bold text-white leading-snug">Pazar Sorusu...</h3>
                <div class="mt-2 flex items-center gap-1.5 text-xs text-slate-400">
                    <i class="fas fa-check-circle text-emerald-400 text-[11px]"></i>
                    <span>Kaynak:</span>
                    <a id="drawer-source" href="#" target="_blank" class="text-purple-400 hover:underline">Resmi Kurum</a>
                </div>
            </div>

            <div class="grid grid-cols-2 gap-3">
                <div id="drawer-opt-yes" class="p-3 rounded-xl border border-emerald-500/30 bg-emerald-950/20 flex flex-col items-center">
                    <span class="text-xs font-bold text-emerald-400 uppercase">EVET İhtimali</span>
                    <span id="drawer-prob-yes" class="text-2xl font-black text-emerald-300">--%</span>
                </div>
                <div id="drawer-opt-no" class="p-3 rounded-xl border border-rose-500/30 bg-rose-950/20 flex flex-col items-center">
                    <span class="text-xs font-bold text-rose-400 uppercase">HAYIR İhtimali</span>
                    <span id="drawer-prob-no" class="text-2xl font-black text-rose-300">--%</span>
                </div>
            </div>

            <div class="bg-slate-800/80 p-4 rounded-xl border border-slate-700/80 space-y-4">
                <div class="flex gap-2">
                    <button id="choice-yes" class="flex-1 py-2 rounded-lg font-bold text-xs uppercase border border-emerald-500 bg-emerald-600 text-white transition">EVET</button>
                    <button id="choice-no" class="flex-1 py-2 rounded-lg font-bold text-xs uppercase border border-slate-700 bg-slate-800 text-slate-400 hover:text-white transition">HAYIR</button>
                </div>

                <div>
                    <div class="flex justify-between text-xs font-semibold mb-1.5">
                        <span class="text-slate-400">Yatırılacak Tutar</span>
                        <span id="drawer-user-balance" class="text-amber-400">Bakiye: -- KOR</span>
                    </div>
                    <div class="relative">
                        <input type="number" id="input-kor-amount" value="500" min="50" step="50" class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white font-bold text-sm focus:outline-none focus:border-purple-500">
                        <span class="absolute right-3 top-2 text-xs font-bold text-slate-500">KOR</span>
                    </div>
                    <!-- HIZLI TUTAR BUTONLARI -->
                    <div class="flex gap-1.5 mt-2">
                        <button type="button" class="quick-kor flex-1 py-1 rounded bg-slate-900 border border-slate-700 text-[10px] font-bold text-slate-300 hover:border-purple-500 transition" data-val="100">+100</button>
                        <button type="button" class="quick-kor flex-1 py-1 rounded bg-slate-900 border border-slate-700 text-[10px] font-bold text-slate-300 hover:border-purple-500 transition" data-val="500">+500</button>
                        <button type="button" class="quick-kor flex-1 py-1 rounded bg-slate-900 border border-slate-700 text-[10px] font-bold text-slate-300 hover:border-purple-500 transition" data-val="1000">+1.000</button>
                        <button type="button" class="quick-kor flex-1 py-1 rounded bg-slate-900 border border-slate-700 text-[10px] font-bold text-purple-400 hover:border-purple-500 transition" data-val="MAX">Maks</button>
                    </div>
                </div>

                <div class="p-3 bg-slate-900/60 rounded-lg border border-slate-700/50 flex justify-between items-center text-xs">
                    <span class="text-slate-400">Doğru Tahminde Tahmini Kazanç:</span>
                    <span id="calculated-payout" class="font-extrabold text-emerald-400">+820 KOR</span>
                </div>

                <button id="btn-submit-prediction" class="w-full py-3 rounded-xl bg-gradient-to-r from-purple-600 to-pink-600 hover:from-purple-500 hover:to-pink-500 text-white font-extrabold text-sm shadow-lg transition">
                    Tahmini Deftere İşle (KOR)
                </button>
            </div>

            <div class="border-t border-slate-800 pt-4">
                <span class="block text-xs font-semibold text-slate-400 mb-2">Çevreye Meydan Oku:</span>
                <div class="flex gap-2">
                    <button id="btn-share-whatsapp" class="flex-1 bg-emerald-600 hover:bg-emerald-500 text-white py-2 rounded-lg text-xs font-bold flex items-center justify-center gap-1.5 transition">
                        <i class="fab fa-whatsapp"></i> WhatsApp
                    </button>
                    <button id="btn-share-x" class="flex-1 bg-black hover:bg-slate-800 text-white border border-slate-700 py-2 rounded-lg text-xs font-bold flex items-center justify-center gap-1.5 transition">
                        <i class="fab fa-x-twitter"></i> X
                    </button>
                </div>
            </div>

            <!-- PRO ANALİTİK KİLİDİ (Tıklandığında Mağaza Açılır) -->
            <div id="btn-open-shop-drawer" class="relative rounded-2xl border border-slate-700/80 overflow-hidden bg-slate-800/40 p-4 cursor-pointer hover:border-amber-500/50 transition">
                <div class="space-y-2 filter blur-sm select-none opacity-40">
                    <div class="h-4 bg-slate-700 rounded w-3/4"></div>
                    <div class="h-16 bg-slate-700/50 rounded w-full"></div>
                </div>
                <div class="absolute inset-0 flex flex-col items-center justify-center p-4 text-center bg-slate-900/70 glass-blur">
                    <i class="fas fa-lock text-amber-400 text-xl mb-2"></i>
                    <p class="text-xs font-bold text-white mb-1">Top 100 Kahin Tercihleri ve Demografi</p>
                    <span class="bg-gradient-to-r from-amber-500 to-orange-500 text-slate-950 font-black text-xs py-2 px-5 rounded-lg shadow-lg mt-2 inline-block">
                        👑 Pro Analitiğe Yükselt
                    </span>
                </div>
            </div>
        </div>
    </aside>

    <!-- UGC ANKET AÇMA MODALI -->
    <div id="ugc-modal" class="fixed inset-0 bg-slate-950/80 z-50 backdrop-blur-md hidden flex items-center justify-center p-4">
        <div class="bg-slate-900 border border-slate-800 w-full max-w-lg rounded-2xl overflow-hidden shadow-2xl flex flex-col">
            <div class="p-5 border-b border-slate-800 flex items-center justify-between bg-slate-800/40">
                <div class="flex items-center gap-2">
                    <i class="fas fa-plus-circle text-purple-400 text-lg"></i>
                    <h3 class="text-lg font-black text-white">Yeni Oylama Başlat</h3>
                </div>
                <button id="btn-close-ugc" class="text-slate-400 hover:text-white p-1 text-lg"><i class="fas fa-times"></i></button>
            </div>
            <div class="p-6 space-y-4">
                <div>
                    <label class="block text-xs font-bold text-slate-400 mb-1">Kategori</label>
                    <select id="ugc-category" class="w-full bg-slate-800 border border-slate-700 rounded-lg px-3 py-2 text-white text-xs focus:outline-none focus:border-purple-500">
                        <option value="SİYASET">Siyaset</option>
                        <option value="EKONOMİ">Ekonomi</option>
                        <option value="SPOR">Spor</option>
                        <option value="GÜNDEM">Gündem</option>
                    </select>
                </div>
                <div>
                    <label class="block text-xs font-bold text-slate-400 mb-1">Oylama Sorusu</label>
                    <textarea id="ugc-question" rows="2" placeholder="Örn: 2026 sonunda konut fiyat endeksi %20 artar mı?" class="w-full bg-slate-800 border border-slate-700 rounded-lg p-2.5 text-white text-xs focus:outline-none focus:border-purple-500"></textarea>
                </div>
                <div class="grid grid-cols-2 gap-3">
                    <div>
                        <label class="block text-xs font-bold text-slate-400 mb-1">Çözümleme Kaynağı Adı</label>
                        <input type="text" id="ugc-source-name" placeholder="Örn: TÜİK Resmi Bülteni" class="w-full bg-slate-800 border border-slate-700 rounded-lg p-2 text-white text-xs focus:outline-none focus:border-purple-500">
                    </div>
                    <div>
                        <label class="block text-xs font-bold text-slate-400 mb-1">Kaynak Linki (URL)</label>
                        <input type="text" id="ugc-source-url" placeholder="https://tuik.gov.tr" class="w-full bg-slate-800 border border-slate-700 rounded-lg p-2 text-white text-xs focus:outline-none focus:border-purple-500">
                    </div>
                </div>
                <div>
                    <label class="block text-xs font-bold text-slate-400 mb-1">Kapanış Tarihi</label>
                    <input type="text" id="ugc-closing-date" value="31 Aralık 2026" class="w-full bg-slate-800 border border-slate-700 rounded-lg p-2 text-white text-xs focus:outline-none focus:border-purple-500">
                </div>
                <div class="p-3 bg-purple-950/30 border border-purple-500/30 rounded-lg text-[11px] text-purple-300">
                    <i class="fas fa-shield-alt mr-1"></i> Anket açmak için <strong>10.000 KOR teminat</strong> kilitlenir. Katılım arttıkça komisyon payınız defterinize işlenir.
                </div>
                <button id="btn-submit-ugc" class="w-full py-3 rounded-xl bg-gradient-to-r from-purple-600 to-pink-600 hover:from-purple-500 hover:to-pink-500 text-white font-extrabold text-sm shadow-lg transition">
                    Anketi Başlat (10.000 KOR)
                </button>
            </div>
        </div>
    </div>

    <!-- PRO KOR MAĞAZASI & PAYWALL MODALI -->
    <div id="shop-modal" class="fixed inset-0 bg-slate-950/80 z-50 backdrop-blur-md hidden flex items-center justify-center p-4">
        <div class="bg-slate-900 border border-slate-800 w-full max-w-xl rounded-2xl overflow-hidden shadow-2xl flex flex-col">
            <div class="p-5 border-b border-slate-800 flex items-center justify-between bg-slate-800/40">
                <div class="flex items-center gap-2">
                    <i class="fas fa-coins text-amber-400 text-lg"></i>
                    <h3 class="text-lg font-black text-white">KOR Puanı & Pro Mağazası</h3>
                </div>
                <button id="btn-close-shop" class="text-slate-400 hover:text-white p-1 text-lg"><i class="fas fa-times"></i></button>
            </div>
            <div class="p-6 space-y-4">
                <p class="text-xs text-slate-400">KOR puanı satın alarak tahmin gücünüzü artırabilir veya Pro analitik kilidini kaldırabilirsiniz.</p>
                <div class="grid grid-cols-1 md:grid-cols-3 gap-3">
                    <div class="p-4 rounded-xl border border-slate-700 bg-slate-800/60 flex flex-col justify-between text-center hover:border-purple-500 transition">
                        <div>
                            <span class="text-xs font-bold text-purple-400 uppercase">Başlangıç</span>
                            <div class="text-xl font-black text-white mt-1">25.000 KOR</div>
                            <div class="text-[11px] text-slate-400 mt-1">Standart Tahminci</div>
                        </div>
                        <button onclick="buyKor(25000, 'Başlangıç Paketi')" class="mt-4 w-full py-2 bg-slate-700 hover:bg-purple-600 text-white rounded-lg text-xs font-bold transition">Yükle</button>
                    </div>
                    <div class="p-4 rounded-xl border-2 border-amber-500/60 bg-amber-950/20 flex flex-col justify-between text-center shadow-lg relative">
                        <span class="absolute -top-2.5 right-4 bg-amber-500 text-slate-950 text-[9px] font-black px-2 py-0.5 rounded-full uppercase">Popüler</span>
                        <div>
                            <span class="text-xs font-bold text-amber-400 uppercase">Pro Kahin</span>
                            <div class="text-xl font-black text-white mt-1">100.000 KOR</div>
                            <div class="text-[11px] text-slate-400 mt-1">Pro Analitik Açık</div>
                        </div>
                        <button onclick="buyKor(100000, 'Pro Kahin Paketi')" class="mt-4 w-full py-2 bg-gradient-to-r from-amber-500 to-orange-500 text-slate-950 font-black rounded-lg text-xs transition">Yükle</button>
                    </div>
                    <div class="p-4 rounded-xl border border-slate-700 bg-slate-800/60 flex flex-col justify-between text-center hover:border-purple-500 transition">
                        <div>
                            <span class="text-xs font-bold text-emerald-400 uppercase">Balina</span>
                            <div class="text-xl font-black text-white mt-1">500.000 KOR</div>
                            <div class="text-[11px] text-slate-400 mt-1">B2B Veri & VIP İtibar</div>
                        </div>
                        <button onclick="buyKor(500000, 'Balina Paketi')" class="mt-4 w-full py-2 bg-slate-700 hover:bg-emerald-600 text-white rounded-lg text-xs font-bold transition">Yükle</button>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <!-- LEADERBOARD MODAL -->
    <div id="leaderboard-modal" class="fixed inset-0 bg-slate-950/80 z-50 backdrop-blur-md hidden flex items-center justify-center p-4">
        <div class="bg-slate-900 border border-slate-800 w-full max-w-2xl rounded-2xl overflow-hidden shadow-2xl flex flex-col max-h-[85vh]">
            <div class="p-5 border-b border-slate-800 flex items-center justify-between bg-slate-800/40">
                <div class="flex items-center gap-2">
                    <i class="fas fa-trophy text-amber-400 text-lg"></i>
                    <h3 class="text-lg font-black text-white">Top 100 Kahin Ligi</h3>
                    <span class="text-xs text-slate-400 ml-2">(Gerçekleşen Net PnL)</span>
                </div>
                <button id="btn-close-leaderboard" class="text-slate-400 hover:text-white p-1 text-lg"><i class="fas fa-times"></i></button>
            </div>
            <div class="p-4 overflow-y-auto flex-grow divide-y divide-slate-800" id="leaderboard-list"></div>
        </div>
    </div>

    <script>
        let markets = [];
        let selectedMarket = null;
        let selectedChoice = 'YES';
        let currentBalance = 14500;
        let activeCategory = 'ALL';

        function showToast(msg, type = 'success') {
            const container = document.getElementById('toast-container');
            const toast = document.createElement('div');
            const bg = type === 'success' ? 'bg-emerald-950/90 border-emerald-500/50 text-emerald-200' : 'bg-rose-950/90 border-rose-500/50 text-rose-200';
            const icon = type === 'success' ? 'fa-check-circle text-emerald-400' : 'fa-exclamation-circle text-rose-400';

            toast.className = 'flex items-center gap-2 px-4 py-3 rounded-xl border shadow-xl text-xs font-bold transition-all duration-300 opacity-0 transform translate-y-2 pointer-events-auto ' + bg;
            toast.innerHTML = '<i class="fas ' + icon + ' text-sm"></i><span>' + msg + '</span>';

            container.appendChild(toast);
            setTimeout(() => { toast.classList.remove('opacity-0', 'translate-y-2'); }, 10);
            setTimeout(() => {
                toast.classList.add('opacity-0', 'translate-y-2');
                setTimeout(() => toast.remove(), 300);
            }, 3500);
        }

        function connectWebSocket() {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const ws = new WebSocket(protocol + '//' + window.location.host + '/ws');

            ws.onmessage = (event) => {
                try {
                    const msg = JSON.parse(event.data);
                    if (msg.type === 'MARKET_UPDATE') handleMarketUpdate(msg);
                    if (msg.type === 'NEW_MARKET') init();
                } catch(e) {}
            };
            ws.onclose = () => setTimeout(connectWebSocket, 2500);
        }

        function handleMarketUpdate(update) {
            const idx = markets.findIndex(m => m.id === update.marketId);
            if (idx !== -1) {
                markets[idx].probYes = update.probYes;
                markets[idx].probNo = update.probNo;
                markets[idx].poolTotal = update.poolTotal;
                markets[idx].yes_reserve = update.yesReserve;
                markets[idx].no_reserve = update.noReserve;
            }

            const probEl = document.getElementById('prob-' + update.marketId);
            const barEl = document.getElementById('bar-' + update.marketId);
            const poolEl = document.getElementById('pool-' + update.marketId);

            if (probEl) probEl.textContent = 'EVET: %' + update.probYes;
            if (barEl) barEl.style.width = update.probYes + '%';
            if (poolEl) poolEl.textContent = 'Havuz: ' + update.poolTotal + ' KOR';

            if (selectedMarket && selectedMarket.id === update.marketId) {
                document.getElementById('drawer-prob-yes').textContent = '%' + update.probYes;
                document.getElementById('drawer-prob-no').textContent = '%' + update.probNo;
                calcPayout();
            }
        }

        async function init() {
            try {
                const me = await fetch('/api/me').then(r => r.json());
                currentBalance = Math.round(parseFloat(me.balance_kor || 14500));
                document.getElementById('user-balance').textContent = currentBalance.toLocaleString('tr-TR') + ' KOR';
                document.getElementById('drawer-user-balance').textContent = 'Bakiye: ' + currentBalance.toLocaleString('tr-TR') + ' KOR';
                document.getElementById('user-streak').textContent = (me.streak || 5) + ' Gün';
                document.getElementById('user-avatar').textContent = (me.username || 'L')[0].toUpperCase();

                updateQuestBar(me.quests_today || 0, me.quest_rewarded || false);

                const res = await fetch('/api/markets').then(r => r.json());
                markets = res.markets || [];
                renderMarkets();
            } catch (e) {
                console.error(e);
            }
        }

        function updateQuestBar(count, isRewarded) {
            const label = document.getElementById('quest-label');
            const bar = document.getElementById('quest-progress-bar');
            const rewardText = document.getElementById('quest-reward-text');

            const pct = Math.min(100, Math.round((count / 3) * 100));
            bar.style.width = pct + '%';
            label.textContent = '(' + count + '/3 Tamamlandı)';

            if (isRewarded || count >= 3) {
                rewardText.className = 'text-emerald-400 font-bold';
                rewardText.textContent = '✅ +150 KOR Alındı';
            }
        }

        function renderMarkets() {
            const container = document.getElementById('market-grid');
            container.innerHTML = '';

            const filtered = activeCategory === 'ALL'
                ? markets
                : markets.filter(m => m.category === activeCategory || (activeCategory === 'SPONSORLU' && m.is_sponsored));

            filtered.forEach(m => {
                const card = document.createElement('article');
                card.className = 'bg-slate-800/90 rounded-2xl border border-slate-700/70 p-5 shadow-lg flex flex-col justify-between hover:border-slate-600 transition';

                const catBadge = m.is_sponsored
                    ? '<span class="px-2 py-0.5 rounded text-[10px] font-black uppercase tracking-wider bg-orange-500/20 text-orange-400 border border-orange-500/30">📢 ' + (m.sponsored_by || 'Sponsorlu') + '</span>'
                    : '<span class="px-2 py-0.5 rounded text-[10px] font-black uppercase tracking-wider bg-purple-500/20 text-purple-300">' + m.category + '</span>';

                card.innerHTML = 
                    '<div class="flex items-center justify-between mb-3">' +
                        catBadge +
                        '<div class="flex items-center space-x-2 text-slate-400 text-xs">' +
                            '<button onclick="shareMkt(\\'' + m.id + '\\', \\'wa\\')" title="WhatsApp Paylaş" class="hover:text-emerald-400 transition"><i class="fab fa-whatsapp"></i></button>' +
                            '<button onclick="shareMkt(\\'' + m.id + '\\', \\'x\\')" title="X Paylaş" class="hover:text-white transition"><i class="fab fa-x-twitter"></i></button>' +
                            '<span class="text-slate-500 ml-1">|</span>' +
                            '<span class="text-[11px] text-slate-400 ml-1">' + m.closing_date + '</span>' +
                        '</div>' +
                    '</div>' +
                    '<h3 class="text-base font-bold text-white mb-4 leading-snug">' + m.question + '</h3>' +
                    '<div class="space-y-2 mb-5">' +
                        '<div class="flex justify-between text-xs text-slate-400 font-medium">' +
                            '<span id="pool-' + m.id + '">Havuz: ' + m.poolTotal + ' KOR</span>' +
                            '<span id="prob-' + m.id + '" class="font-bold text-slate-200">EVET: %' + m.probYes + '</span>' +
                        '</div>' +
                        '<div class="w-full bg-rose-500/30 rounded-full h-2 overflow-hidden flex">' +
                            '<div id="bar-' + m.id + '" class="bg-gradient-to-r from-emerald-500 to-teal-400 h-full transition-all duration-700 ease-out" style="width:' + m.probYes + '%"></div>' +
                        '</div>' +
                    '</div>' +
                    '<div class="grid grid-cols-2 gap-3 pt-2 border-t border-slate-700/50">' +
                        '<button class="py-2 rounded-xl bg-emerald-600/20 hover:bg-emerald-600 border border-emerald-500/40 text-emerald-300 hover:text-white font-extrabold text-xs transition" onclick="openDrawer(\\'' + m.id + '\\', \\'YES\\')">EVET</button>' +
                        '<button class="py-2 rounded-xl bg-rose-600/20 hover:bg-rose-600 border border-rose-500/40 text-rose-300 hover:text-white font-extrabold text-xs transition" onclick="openDrawer(\\'' + m.id + '\\', \\'NO\\')">HAYIR</button>' +
                    '</div>';

                container.appendChild(card);
            });
        }

        window.openDrawer = function(id, choice) {
            selectedMarket = markets.find(m => m.id === id);
            selectedChoice = choice;
            if (!selectedMarket) return;

            document.getElementById('drawer-category').textContent = selectedMarket.is_sponsored ? selectedMarket.sponsored_by : selectedMarket.category;
            document.getElementById('drawer-title').textContent = selectedMarket.question;
            document.getElementById('drawer-source').textContent = selectedMarket.source_name || 'Resmi';
            document.getElementById('drawer-source').href = selectedMarket.source_url || '#';
            document.getElementById('drawer-prob-yes').textContent = '%' + selectedMarket.probYes;
            document.getElementById('drawer-prob-no').textContent = '%' + selectedMarket.probNo;

            updateChoiceBtns();
            calcPayout();

            document.getElementById('drawer-backdrop').classList.remove('hidden');
            setTimeout(() => {
                document.getElementById('drawer-backdrop').classList.remove('opacity-0');
                document.getElementById('trade-drawer').classList.remove('translate-x-full');
            }, 10);
        };

        function closeDrawer() {
            document.getElementById('trade-drawer').classList.add('translate-x-full');
            document.getElementById('drawer-backdrop').classList.add('opacity-0');
            setTimeout(() => document.getElementById('drawer-backdrop').classList.add('hidden'), 300);
        }

        function updateChoiceBtns() {
            const bY = document.getElementById('choice-yes'), bN = document.getElementById('choice-no');
            if (selectedChoice === 'YES') {
                bY.className = 'flex-1 py-2 rounded-lg font-bold text-xs uppercase border border-emerald-500 bg-emerald-600 text-white';
                bN.className = 'flex-1 py-2 rounded-lg font-bold text-xs uppercase border border-slate-700 bg-slate-800 text-slate-400';
            } else {
                bN.className = 'flex-1 py-2 rounded-lg font-bold text-xs uppercase border border-rose-500 bg-rose-600 text-white';
                bY.className = 'flex-1 py-2 rounded-lg font-bold text-xs uppercase border border-slate-700 bg-slate-800 text-slate-400';
            }
        }

        function calcPayout() {
            if (!selectedMarket) return;
            const amt = parseFloat(document.getElementById('input-kor-amount').value) || 0;
            const y = parseFloat(selectedMarket.yes_reserve), n = parseFloat(selectedMarket.no_reserve);
            const prob = selectedChoice === 'YES' ? (n / (y + n)) : (y / (y + n));
            const payout = Math.round(amt * ((1 / prob) * 0.98));
            document.getElementById('calculated-payout').textContent = '+' + payout.toLocaleString('tr-TR') + ' KOR';
        }

        document.querySelectorAll('.quick-kor').forEach(btn => {
            btn.onclick = () => {
                const val = btn.getAttribute('data-val');
                const inp = document.getElementById('input-kor-amount');
                if (val === 'MAX') {
                    inp.value = currentBalance;
                } else {
                    inp.value = (parseFloat(inp.value) || 0) + parseFloat(val);
                }
                calcPayout();
            };
        });

        document.getElementById('btn-submit-prediction').onclick = async () => {
            const amt = document.getElementById('input-kor-amount').value;
            const btn = document.getElementById('btn-submit-prediction');
            btn.disabled = true;
            btn.textContent = 'İşleniyor...';

            try {
                const res = await fetch('/api/predict', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ marketId: selectedMarket.id, outcome: selectedChoice, amountKor: amt })
                }).then(r => r.json());

                if (res.error) throw new Error(res.error);

                currentBalance = res.balanceKor;
                document.getElementById('user-balance').textContent = currentBalance.toLocaleString('tr-TR') + ' KOR';
                document.getElementById('drawer-user-balance').textContent = 'Bakiye: ' + currentBalance.toLocaleString('tr-TR') + ' KOR';

                updateQuestBar(res.questsToday, res.bonusAdded);
                showToast('✅ Tahmininiz deftere işlendi! Pay: ' + res.sharesOut);

                if (res.bonusAdded) {
                    setTimeout(() => showToast('🎉 Günlük görev tamamlandı: +150 KOR hesabınıza aktarıldı!'), 600);
                }
                closeDrawer();
            } catch (e) {
                showToast(e.message, 'error');
            } finally {
                btn.disabled = false;
                btn.textContent = 'Tahmini Deftere İşle (KOR)';
            }
        };

        const ugcModal = document.getElementById('ugc-modal');
        const openUgc = () => ugcModal.classList.remove('hidden');
        const closeUgc = () => ugcModal.classList.add('hidden');

        document.getElementById('btn-nav-ugc').onclick = openUgc;
        document.getElementById('btn-hero-ugc').onclick = openUgc;
        document.getElementById('btn-close-ugc').onclick = closeUgc;

        document.getElementById('btn-submit-ugc').onclick = async () => {
            const question = document.getElementById('ugc-question').value.trim();
            const category = document.getElementById('ugc-category').value;
            const sourceName = document.getElementById('ugc-source-name').value.trim();
            const sourceUrl = document.getElementById('ugc-source-url').value.trim();
            const closingDate = document.getElementById('ugc-closing-date').value.trim();

            if (!question || !sourceName) {
                showToast('Lütfen soru ve çözümleme kaynağını doldurun.', 'error');
                return;
            }

            try {
                const res = await fetch('/api/markets/create', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ question, category, sourceName, sourceUrl, closingDate })
                }).then(r => r.json());

                if (res.error) throw new Error(res.error);

                currentBalance = res.newBalance;
                document.getElementById('user-balance').textContent = currentBalance.toLocaleString('tr-TR') + ' KOR';
                showToast('🎉 Anketiniz başarıyla yayına alındı! (10.000 KOR teminat kilitlendi)');
                closeUgc();
                init();
            } catch (e) {
                showToast(e.message, 'error');
            }
        };

        const shopModal = document.getElementById('shop-modal');
        const openShop = () => shopModal.classList.remove('hidden');
        const closeShop = () => shopModal.classList.add('hidden');

        document.getElementById('btn-open-shop-nav').onclick = openShop;
        document.getElementById('btn-open-shop-drawer').onclick = openShop;
        document.getElementById('btn-close-shop').onclick = closeShop;

        window.buyKor = async function(amt, title) {
            try {
                const res = await fetch('/api/shop/buy-kor', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ amountKor: amt, packageTitle: title })
                }).then(r => r.json());

                if (res.error) throw new Error(res.error);

                currentBalance = res.newBalance;
                document.getElementById('user-balance').textContent = currentBalance.toLocaleString('tr-TR') + ' KOR';
                showToast('💎 ' + title + ' başarıyla yüklendi: +' + amt.toLocaleString('tr-TR') + ' KOR!');
                closeShop();
            } catch (e) {
                showToast(e.message, 'error');
            }
        };

        document.getElementById('btn-open-leaderboard').onclick = async () => {
            const d = await fetch('/api/leaderboard').then(r => r.json());
            const list = document.getElementById('leaderboard-list');
            list.innerHTML = '';
            d.top100.forEach(u => {
                const item = document.createElement('div');
                item.className = 'py-3 flex items-center justify-between text-xs';
                const badgeColor = u.rank === 1 ? 'bg-amber-500/20 text-amber-400' : (u.rank === 2 ? 'bg-slate-400/20 text-slate-300' : 'bg-orange-700/20 text-orange-400');
                item.innerHTML = 
                    '<div class="flex items-center gap-3">' +
                        '<span class="w-6 h-6 rounded-full ' + badgeColor + ' flex items-center justify-center font-bold text-xs">#' + u.rank + '</span>' +
                        '<div><strong class="text-white text-sm">' + u.name + '</strong><span class="text-[10px] text-slate-500 ml-1">🔥 ' + u.streak + ' Gün</span></div>' +
                    '</div>' +
                    '<div class="text-right"><div class="font-black text-emerald-400 text-sm">' + u.pnl + '</div><div class="text-[10px] text-slate-500">İsabet: ' + u.winRate + '</div></div>';
                list.appendChild(item);
            });
            document.getElementById('leaderboard-modal').classList.remove('hidden');
        };
        document.getElementById('btn-close-leaderboard').onclick = () => document.getElementById('leaderboard-modal').classList.add('hidden');

        window.promptSwitchUser = async () => {
            const name = prompt('Giriş yapılacak kullanıcı adını girin (Örn: Ahmet_Kahin, Ece_Analist, LeisanB):');
            if (!name) return;
            const res = await fetch('/api/auth/login-mock', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ username: name })
            }).then(r => r.json());
            if (res.success) location.reload();
        };

        window.shareMkt = function(id, plat) {
            const m = markets.find(item => item.id === id);
            if (!m) return;
            const text = 'OYVER: ' + m.question + ' oylamasında tarafını seç!';
            const url = window.location.href;
            if (plat === 'wa') window.open('https://api.whatsapp.com/send?text=' + encodeURIComponent(text + ' ' + url), '_blank');
            if (plat === 'x') window.open('https://twitter.com/intent/tweet?text=' + encodeURIComponent(text) + '&url=' + encodeURIComponent(url), '_blank');
        };

        document.getElementById('btn-close-drawer').onclick = closeDrawer;
        document.getElementById('drawer-backdrop').onclick = closeDrawer;
        document.getElementById('choice-yes').onclick = () => { selectedChoice = 'YES'; updateChoiceBtns(); calcPayout(); };
        document.getElementById('choice-no').onclick = () => { selectedChoice = 'NO'; updateChoiceBtns(); calcPayout(); };
        document.getElementById('input-kor-amount').oninput = calcPayout;
        document.getElementById('btn-share-whatsapp').onclick = () => selectedMarket && shareMkt(selectedMarket.id, 'wa');
        document.getElementById('btn-share-x').onclick = () => selectedMarket && shareMkt(selectedMarket.id, 'x');

        document.querySelectorAll('.cat-btn').forEach(btn => {
            btn.onclick = (e) => {
                document.querySelectorAll('.cat-btn').forEach(b => b.className = 'cat-btn bg-slate-800 hover:bg-slate-700 text-slate-300 px-4 py-1.5 rounded-lg text-xs font-bold transition');
                e.target.className = 'cat-btn active bg-purple-600 text-white px-4 py-1.5 rounded-lg text-xs font-bold transition';
                activeCategory = e.target.getAttribute('data-cat');
                renderMarkets();
            };
        });

        init();
        connectWebSocket();
    </script>
</body>
</html>`);
});

// ==========================================
// 5. BAŞLATICI
// ==========================================
await initDatabase();
const port = Number(process.env.PORT) || 3000;
app.listen({ port, host: '0.0.0.0' }, (err, address) => {
    if (err) { console.error(err); process.exit(1); }
    console.log(`[OYVER] Sunucu aktif: ${address}`);
});
