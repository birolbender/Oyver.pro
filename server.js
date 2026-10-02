import fastify from 'fastify';
import fastifyWebsocket from '@fastify/websocket';
import pg from 'pg';
import Decimal from 'decimal.js';
import crypto from 'crypto';
import { WebSocket } from 'ws';

// ==========================================
// 1. HASSASİYET VE SAYISAL SÖZLEŞME
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

// ==========================================
// 2. CITARDAUQ AMM MATEMATİK MOTORU
// ==========================================
class AMMEngine {
    static calculateBuy(state, mGross, feeRate) {
        const fee = MoneyMath.roundUp(mGross.mul(feeRate), 6);
        const mNet = mGross.minus(fee);
        if (mNet.lte(0)) throw new Error('INVALID_AMOUNT: Net tutar 0 veya negatif olamaz');

        const k = state.yesReserve.mul(state.noReserve);
        const newNoReserve = state.noReserve.plus(mNet);
        const newYesReserve = k.div(newNoReserve);
        const deltaY = state.yesReserve.minus(newYesReserve);
        const sharesOut = MoneyMath.roundDown(mNet.plus(deltaY), 12);

        return { mNet, fee, newYesReserve, newNoReserve, sharesOut };
    }

    static calculateSell(state, sharesIn, feeRate) {
        if (sharesIn.lte(0)) throw new Error('INVALID_AMOUNT: Satis payi sifirdan buyuk olmalidir');
        const Y = state.yesReserve;
        const N = state.noReserve;
        const S = sharesIn;

        const B = Y.plus(N).plus(S);
        const C = S.mul(N);
        const discriminant = B.pow(2).minus(C.mul(4));
        if (discriminant.lt(0)) throw new Error('MATH_ERROR: Negatif diskriminant');

        const sqrtDisc = discriminant.sqrt();
        const grossPayout = C.mul(2).div(B.plus(sqrtDisc));

        if (grossPayout.gte(N)) throw new Error('SOLVENCY_VIOLATION: Odeme rezervi asamaz');

        const fee = MoneyMath.roundUp(grossPayout.mul(feeRate), 6);
        const netPayout = MoneyMath.roundDown(grossPayout.minus(fee), 6);

        return {
            grossPayout: MoneyMath.roundDown(grossPayout, 6),
            fee,
            netPayout,
            newYesReserve: Y.plus(S).minus(grossPayout),
            newNoReserve: N.minus(grossPayout)
        };
    }
}

// ==========================================
// 3. VERİTABANI BAĞLANTISI VE TABLOLAR
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
        console.info('[DATABASE] Tablolar ve finansal kisitlar dogrulaniyor...');
        await client.query(`
            CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

            CREATE TABLE IF NOT EXISTS users (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                email VARCHAR(255) UNIQUE NOT NULL,
                username VARCHAR(64) UNIQUE NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                balance_kor NUMERIC(24,6) NOT NULL DEFAULT 1000.000000 CHECK (balance_kor >= 0),
                streak INT NOT NULL DEFAULT 1,
                status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS markets (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                slug VARCHAR(255) UNIQUE NOT NULL,
                category VARCHAR(64) NOT NULL DEFAULT 'GÜNDEM',
                question TEXT NOT NULL,
                description TEXT,
                source_url TEXT,
                source_name VARCHAR(128),
                is_sponsored BOOLEAN NOT NULL DEFAULT FALSE,
                sponsored_by VARCHAR(128),
                status VARCHAR(32) NOT NULL DEFAULT 'TRADING',
                resolution VARCHAR(16),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                closed_at TIMESTAMPTZ
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
                avg_price NUMERIC(30,18) NOT NULL DEFAULT 0,
                realized_pnl NUMERIC(24,6) NOT NULL DEFAULT 0,
                status VARCHAR(32) NOT NULL DEFAULT 'OPEN',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT uq_user_market_outcome UNIQUE (user_id, market_id, outcome)
            );

            CREATE TABLE IF NOT EXISTS accounts (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                code VARCHAR(32) NOT NULL,
                owner_user_id UUID REFERENCES users(id) ON DELETE CASCADE,
                market_id UUID REFERENCES markets(id) ON DELETE CASCADE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS ledger_entries (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                entry_type VARCHAR(64) NOT NULL,
                reference_id UUID,
                idempotency_key VARCHAR(64),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS ledger_lines (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                entry_id UUID NOT NULL REFERENCES ledger_entries(id) ON DELETE CASCADE,
                account_id UUID NOT NULL REFERENCES accounts(id),
                debit NUMERIC(24,6) NOT NULL DEFAULT 0 CHECK (debit >= 0),
                credit NUMERIC(24,6) NOT NULL DEFAULT 0 CHECK (credit >= 0),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS sessions (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                token_hash CHAR(64) UNIQUE NOT NULL,
                expires_at TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);

        // Başlangıç Tohum Pazarları
        const checkMarket = await client.query(`SELECT id FROM markets LIMIT 1`);
        if (checkMarket.rows.length === 0) {
            console.info('[GENESIS] İlk pazarlar tohumlanıyor...');
            for (const code of ['1000', '3000', '4000', '5000']) {
                await client.query(`INSERT INTO accounts (code) VALUES ($1) ON CONFLICT DO NOTHING`, [code]);
            }

            const initialMarkets = [
                {
                    slug: 'asgari-ucret-2026',
                    cat: 'SİYASET',
                    q: '2026 Yılı Asgari Ücreti 30.000 TL Üzerinde Açıklanır mı?',
                    srcName: 'Resmi Gazete',
                    srcUrl: 'https://www.resmigazete.gov.tr',
                    spons: false,
                    by: null,
                    yesR: 10000,
                    noR: 5500
                },
                {
                    slug: 'togg-t10f-teslimat',
                    cat: 'SPONSORLU',
                    q: 'TOGG T10F Sedan Modeli 2026 Q3 Öncesi Teslimata Başlar mı?',
                    srcName: 'TOGG Basın Bülteni',
                    srcUrl: 'https://togg.com.tr',
                    spons: true,
                    by: 'TOGG',
                    yesR: 12000,
                    noR: 3000
                },
                {
                    slug: 'faiz-indirimi-2026',
                    cat: 'EKONOMİ',
                    q: 'TCMB Politika Faizini 2026 Yıl Sonuna Kadar %30 Altına İndirir mi?',
                    srcName: 'TCMB Kararı',
                    srcUrl: 'https://tcmb.gov.tr',
                    spons: false,
                    by: null,
                    yesR: 7000,
                    noR: 9000
                },
                {
                    slug: 'dunya-kupasi-elemeleri',
                    cat: 'SPOR',
                    q: 'A Milli Takım 2026 Dünya Kupası Elemelerinde Grubunu Lider Bitirir mi?',
                    srcName: 'TFF',
                    srcUrl: 'https://tff.org',
                    spons: false,
                    by: null,
                    yesR: 8000,
                    noR: 8000
                }
            ];

            for (const item of initialMarkets) {
                const mRes = await client.query(`
                    INSERT INTO markets (slug, category, question, source_name, source_url, is_sponsored, sponsored_by)
                    VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id
                `, [item.slug, item.cat, item.q, item.srcName, item.srcUrl, item.spons, item.by]);
                const mId = mRes.rows[0].id;
                await client.query(`INSERT INTO accounts (code, market_id) VALUES ('2100', $1)`, [mId]);
                await client.query(`INSERT INTO amm_state (market_id, yes_reserve, no_reserve) VALUES ($1, $2, $3)`, [mId, item.yesR, item.noR]);
            }
        }
        console.info('[DATABASE] Sistem hazır.');
    } finally {
        client.release();
    }
}

// ==========================================
// 4. ÇİFT TARAFLI DEFTER MOTORU
// ==========================================
class LedgerEngine {
    static async recordEntry(client, entryType, refId, idempKey, lines) {
        let totalD = new Decimal(0), totalC = new Decimal(0);
        for (const l of lines) { totalD = totalD.plus(l.debit); totalC = totalC.plus(l.credit); }
        if (!totalD.eq(totalC) || totalD.lte(0)) throw new Error('LEDGER_UNBALANCED');

        const er = await client.query(
            `INSERT INTO ledger_entries (entry_type, reference_id, idempotency_key) VALUES ($1, $2, $3) RETURNING id`,
            [entryType, refId, idempKey]
        );
        const entryId = er.rows[0].id;

        for (const l of lines) {
            const ar = await client.query(
                `SELECT id FROM accounts WHERE code = $1 AND (owner_user_id = $2 OR ($2 IS NULL AND owner_user_id IS NULL)) AND (market_id = $3 OR ($3 IS NULL AND market_id IS NULL))`,
                [l.accountCode, l.userId || null, l.marketId || null]
            );
            let accId = ar.rows.length > 0 ? ar.rows[0].id : (await client.query(
                `INSERT INTO accounts (code, owner_user_id, market_id) VALUES ($1, $2, $3) RETURNING id`,
                [l.accountCode, l.userId || null, l.marketId || null]
            )).rows[0].id;

            await client.query(
                `INSERT INTO ledger_lines (entry_id, account_id, debit, credit) VALUES ($1, $2, $3, $4)`,
                [entryId, accId, l.debit.toFixed(6), l.credit.toFixed(6)]
            );
        }
        return entryId;
    }
}

// ==========================================
// 5. SUNUCU, REST VE WEBSOCKET
// ==========================================
const app = fastify({ logger: false });
await app.register(fastifyWebsocket);

const wsClients = new Set();
function broadcast(channel, data) {
    const msg = JSON.stringify({ channel, data });
    for (const ws of wsClients) {
        if (ws.readyState === WebSocket.OPEN) ws.send(msg);
    }
}

app.get('/ws', { websocket: true }, (connection) => {
    const ws = connection.socket;
    wsClients.add(ws);
    ws.on('close', () => wsClients.delete(ws));
});

app.get('/health', async () => ({ status: 'UP', time: new Date().toISOString() }));

app.get('/api/markets', async () => {
    const r = await pool.query(`
        SELECT m.*, a.yes_reserve, a.no_reserve 
        FROM markets m 
        JOIN amm_state a ON m.id = a.market_id 
        ORDER BY m.created_at ASC
    `);
    return { markets: r.rows };
});

app.get('/api/leaderboard', async () => {
    return {
        top100: [
            { rank: 1, name: "Ahmet_Kahin", pnl: "+540.200 KOR", winRate: "%78", streak: 12 },
            { rank: 2, name: "Ece_Analist", pnl: "+420.900 KOR", winRate: "%74", streak: 8 },
            { rank: 3, name: "QuantTraderTR", pnl: "+380.150 KOR", winRate: "%71", streak: 6 },
            { rank: 4, name: "Zeki_Forecaster", pnl: "+290.400 KOR", winRate: "%68", streak: 4 },
            { rank: 5, name: "Marmara_Data", pnl: "+215.000 KOR", winRate: "%65", streak: 5 }
        ]
    };
});

app.post('/api/predict', async (req, rep) => {
    const { marketId, outcome, amountKor } = req.body || {};
    if (!marketId || !outcome || !amountKor) return rep.status(400).send({ error: 'Eksik parametre' });

    try {
        const result = await runInTransaction(async (c) => {
            const mr = await c.query(
                `SELECT a.yes_reserve, a.no_reserve FROM amm_state a WHERE a.market_id = $1 FOR UPDATE`,
                [marketId]
            );
            if (mr.rows.length === 0) throw new Error('MARKET_NOT_FOUND');

            const isYes = outcome === 'YES';
            const amm = {
                yesReserve: new Decimal(mr.rows[0].yes_reserve),
                noReserve: new Decimal(mr.rows[0].no_reserve)
            };
            const calc = AMMEngine.calculateBuy(
                isYes ? amm : { yesReserve: amm.noReserve, noReserve: amm.yesReserve },
                new Decimal(amountKor),
                new Decimal(0.02)
            );

            const ny = isYes ? calc.newYesReserve : calc.newNoReserve;
            const nn = isYes ? calc.newNoReserve : calc.newYesReserve;
            await c.query(`UPDATE amm_state SET yes_reserve = $1, no_reserve = $2, updated_at = NOW() WHERE market_id = $3`, [ny.toFixed(12), nn.toFixed(12), marketId]);

            return { sharesOut: calc.sharesOut.toFixed(2), fee: calc.fee.toFixed(2) };
        });

        broadcast(`market:${marketId}`, { type: 'TRADE_UPDATE' });
        return rep.send({ success: true, ...result });
    } catch (e) {
        return rep.status(400).send({ error: e.message });
    }
});

// ==========================================
// 6. ANA ÖN YÜZ (TAM ENTEGRE BUSINESS ARAYÜZ)
// ==========================================
app.get('/', async (req, reply) => {
    return reply.type('text/html').send(`<!DOCTYPE html>
<html lang="tr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>OYVER - Sosyal Tahmin, İtibar Ligi ve Kolektif Zeka Platformu</title>
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

    <!-- HEADER -->
    <header class="sticky top-0 z-40 bg-slate-900/90 backdrop-blur-md border-b border-slate-800">
        <div class="container mx-auto px-4 h-16 flex items-center justify-between gap-4">
            <div class="flex items-center space-x-8">
                <a href="#" class="text-2xl font-black tracking-tight text-transparent bg-clip-text bg-gradient-to-r from-purple-400 via-pink-400 to-orange-400">
                    OYVER<span class="text-xs ml-1 px-1.5 py-0.5 rounded bg-purple-500/20 text-purple-300 border border-purple-500/30">PRO</span>
                </a>
                <nav class="hidden md:flex items-center space-x-6 text-sm font-semibold">
                    <a href="#markets" class="text-white hover:text-purple-400 transition">Pazarlar</a>
                    <button id="btn-open-leaderboard" class="text-slate-400 hover:text-white transition flex items-center gap-1.5">
                        <i class="fas fa-trophy text-amber-400 text-xs"></i> Top 100 Kahin
                    </button>
                    <span class="px-2 py-0.5 rounded-full text-xs font-bold bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">B2B Veri API</span>
                </nav>
            </div>

            <div class="flex items-center space-x-3">
                <div id="auth-container" class="flex items-center space-x-2">
                    <button id="btn-login-trigger" class="flex items-center gap-2 bg-gradient-to-r from-purple-600 to-pink-600 hover:from-purple-500 hover:to-pink-500 text-white text-xs font-bold py-2 px-4 rounded-lg shadow-lg transition">
                        <i class="fab fa-google"></i>
                        <span>Google ile Giriş</span>
                    </button>
                    <div id="user-profile-badge" class="hidden items-center bg-slate-800 border border-slate-700 rounded-lg p-1 pr-3 space-x-3">
                        <div class="flex items-center gap-1.5 bg-slate-900 px-2.5 py-1 rounded-md border border-slate-700/60">
                            <i class="fas fa-fire text-orange-500 text-xs"></i>
                            <span class="text-xs font-black text-orange-400">5 Gün</span>
                        </div>
                        <div class="flex items-center gap-1.5">
                            <i class="fas fa-coins text-amber-400 text-xs"></i>
                            <span id="user-kor" class="text-xs font-bold text-amber-200">14.500 KOR</span>
                        </div>
                        <div class="w-6 h-6 rounded-full bg-gradient-to-tr from-purple-500 to-pink-500 flex items-center justify-center text-xs font-black">L</div>
                    </div>
                </div>
            </div>
        </div>
    </header>

    <!-- QUESTS BAR -->
    <section class="bg-slate-800/60 border-b border-slate-800 py-2.5 px-4 text-xs font-medium">
        <div class="container mx-auto flex flex-wrap items-center justify-between gap-3">
            <div class="flex items-center gap-2 text-slate-300">
                <span class="px-2 py-0.5 rounded bg-purple-500/20 text-purple-300 font-bold uppercase text-[10px]">Günlük Görev</span>
                <span>3 Farklı Ekonomi Oylamasına Katıl</span>
                <span class="text-purple-400 font-bold">(1/3 Tamamlandı)</span>
            </div>
            <div class="flex items-center gap-4 text-slate-400">
                <div class="w-32 bg-slate-700 rounded-full h-1.5 overflow-hidden">
                    <div class="bg-gradient-to-r from-purple-500 to-pink-500 h-full w-1/3"></div>
                </div>
                <span class="text-amber-400 font-semibold">+150 KOR Ödül</span>
            </div>
        </div>
    </section>

    <!-- HERO SECTION -->
    <section id="hero" class="relative overflow-hidden py-16 md:py-20 border-b border-slate-800">
        <div class="relative z-10 container mx-auto px-4 text-center max-w-4xl">
            <div class="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-slate-800/80 border border-slate-700 text-xs font-semibold text-purple-300 mb-6">
                <span class="w-2 h-2 rounded-full bg-emerald-400 animate-ping"></span>
                Canlı Oylamalar ve Karar Motoru
            </div>
            <h1 class="text-4xl sm:text-6xl font-extrabold text-transparent bg-clip-text bg-gradient-to-r from-purple-400 via-pink-400 to-orange-400 tracking-tight mb-6">
                Yarının Nabzını Bugün Tutun.
            </h1>
            <p class="text-base sm:text-lg text-slate-400 mb-8 max-w-2xl mx-auto">
                Kolektif zekaya katıl, KOR puanınla fikrini savun, Top 100 Kahin arasına adını yazdır.
            </p>
        </div>
    </section>

    <!-- KATEGORİ FİLTRELERİ -->
    <section class="container mx-auto px-4 pt-8 pb-4">
        <div class="flex items-center justify-between border-b border-slate-800 pb-4 overflow-x-auto gap-4">
            <div id="category-filters" class="flex space-x-2 shrink-0">
                <button class="cat-btn active bg-purple-600 text-white px-4 py-1.5 rounded-lg text-xs font-bold transition" data-cat="ALL">Tümü</button>
                <button class="cat-btn bg-slate-800 hover:bg-slate-700 text-slate-300 px-4 py-1.5 rounded-lg text-xs font-bold transition" data-cat="SİYASET">Siyaset</button>
                <button class="cat-btn bg-slate-800 hover:bg-slate-700 text-slate-300 px-4 py-1.5 rounded-lg text-xs font-bold transition" data-cat="EKONOMİ">Ekonomi</button>
                <button class="cat-btn bg-slate-800 hover:bg-slate-700 text-slate-300 px-4 py-1.5 rounded-lg text-xs font-bold transition" data-cat="SPOR">Spor</button>
                <button class="cat-btn bg-slate-800 hover:bg-slate-700 text-slate-300 px-4 py-1.5 rounded-lg text-xs font-bold transition" data-cat="SPONSORLU">📢 Sponsorlu</button>
            </div>
        </div>
    </section>

    <!-- MARKET GRID -->
    <section id="markets" class="container mx-auto px-4 py-8">
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
                        <span class="text-amber-400">Bakiye: 14.500 KOR</span>
                    </div>
                    <div class="relative">
                        <input type="number" id="input-kor-amount" value="500" min="50" step="50" class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white font-bold text-sm focus:outline-none focus:border-purple-500">
                        <span class="absolute right-3 top-2 text-xs font-bold text-slate-500">KOR</span>
                    </div>
                </div>

                <div class="p-3 bg-slate-900/60 rounded-lg border border-slate-700/50 flex justify-between items-center text-xs">
                    <span class="text-slate-400">Doğru Tahminde Kazanç:</span>
                    <span id="calculated-payout" class="font-extrabold text-emerald-400">+820 KOR</span>
                </div>

                <button id="btn-submit-prediction" class="w-full py-3 rounded-xl bg-gradient-to-r from-purple-600 to-pink-600 hover:from-purple-500 hover:to-pink-500 text-white font-extrabold text-sm shadow-lg transition">
                    Tahmini Onayla (KOR)
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

            <!-- PRO PAYWALL -->
            <div class="relative rounded-2xl border border-slate-700/80 overflow-hidden bg-slate-800/40 p-4">
                <div class="space-y-2 filter blur-sm select-none opacity-40">
                    <div class="h-4 bg-slate-700 rounded w-3/4"></div>
                    <div class="h-16 bg-slate-700/50 rounded w-full"></div>
                </div>
                <div class="absolute inset-0 flex flex-col items-center justify-center p-4 text-center bg-slate-900/70 glass-blur">
                    <i class="fas fa-lock text-amber-400 text-xl mb-2"></i>
                    <p class="text-xs font-bold text-white mb-1">Top 100 Kahin Tercihleri ve Demografi</p>
                    <button class="bg-gradient-to-r from-amber-500 to-orange-500 text-slate-950 font-black text-xs py-2 px-5 rounded-lg shadow-lg mt-2">
                        👑 Pro Analitiğe Yükselt
                    </button>
                </div>
            </div>
        </div>
    </aside>

    <!-- LEADERBOARD MODAL -->
    <div id="leaderboard-modal" class="fixed inset-0 bg-slate-950/80 z-50 backdrop-blur-md hidden flex items-center justify-center p-4">
        <div class="bg-slate-900 border border-slate-800 w-full max-w-2xl rounded-2xl overflow-hidden shadow-2xl flex flex-col max-h-[85vh]">
            <div class="p-5 border-b border-slate-800 flex items-center justify-between bg-slate-800/40">
                <div class="flex items-center gap-2">
                    <i class="fas fa-trophy text-amber-400 text-lg"></i>
                    <h3 class="text-lg font-black text-white">Top 100 Kahin Ligi</h3>
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
        let activeCat = 'ALL';

        async function init() {
            try {
                const res = await fetch('/api/markets').then(r => r.json());
                markets = res.markets || [];
                renderMarkets();
            } catch (e) {
                console.error('Pazarlar yüklenemedi:', e);
            }
        }

        function renderMarkets() {
            const container = document.getElementById('market-grid');
            container.innerHTML = '';
            const filtered = activeCat === 'ALL' ? markets : markets.filter(m => m.category === activeCat);

            filtered.forEach(m => {
                const y = parseFloat(m.yes_reserve), n = parseFloat(m.no_reserve);
                const probYes = Math.round((n / (y + n)) * 100);
                const poolTotal = Math.round(y + n);

                const card = document.createElement('article');
                card.className = 'bg-slate-800/90 rounded-2xl border border-slate-700/70 p-5 shadow-lg flex flex-col justify-between';

                card.innerHTML = 
                    '<div class="flex items-center justify-between mb-3">' +
                        '<span class="px-2 py-0.5 rounded text-[10px] font-black uppercase tracking-wider bg-purple-500/20 text-purple-300">' + m.category + '</span>' +
                        '<div class="flex space-x-2 text-slate-400 text-sm">' +
                            '<button onclick="shareMkt(\\'' + m.id + '\\', \\'wa\\')"><i class="fab fa-whatsapp hover:text-emerald-400"></i></button>' +
                            '<button onclick="shareMkt(\\'' + m.id + '\\', \\'x\\')"><i class="fab fa-x-twitter hover:text-white"></i></button>' +
                        '</div>' +
                    '</div>' +
                    '<h3 class="text-base font-bold text-white mb-4 leading-snug">' + m.question + '</h3>' +
                    '<div class="space-y-2 mb-5">' +
                        '<div class="flex justify-between text-xs text-slate-400 font-medium">' +
                            '<span>Havuz: ' + poolTotal.toLocaleString('tr-TR') + ' KOR</span>' +
                            '<span class="font-bold text-slate-200">EVET: %' + probYes + '</span>' +
                        '</div>' +
                        '<div class="w-full bg-rose-500/30 rounded-full h-2 overflow-hidden flex">' +
                            '<div class="bg-gradient-to-r from-emerald-500 to-teal-400 h-full" style="width:' + probYes + '%"></div>' +
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

            const y = parseFloat(selectedMarket.yes_reserve), n = parseFloat(selectedMarket.no_reserve);
            const probYes = Math.round((n / (y + n)) * 100);

            document.getElementById('drawer-category').textContent = selectedMarket.category;
            document.getElementById('drawer-title').textContent = selectedMarket.question;
            document.getElementById('drawer-source').textContent = selectedMarket.source_name || 'Resmi';
            document.getElementById('drawer-source').href = selectedMarket.source_url || '#';
            document.getElementById('drawer-prob-yes').textContent = '%' + probYes;
            document.getElementById('drawer-prob-no').textContent = '%' + (100 - probYes);

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

        document.getElementById('btn-submit-prediction').onclick = async () => {
            const amt = document.getElementById('input-kor-amount').value;
            try {
                const res = await fetch('/api/predict', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ marketId: selectedMarket.id, outcome: selectedChoice, amountKor: amt })
                }).then(r => r.json());

                if (res.error) throw new Error(res.error);
                alert('Tahmininiz başarıyla iletildi: ' + res.sharesOut + ' Pay alındı.');
                closeDrawer();
                init();
            } catch (e) {
                alert('İşlem Hatası: ' + e.message);
            }
        };

        // Liderlik Modalı
        document.getElementById('btn-open-leaderboard').onclick = async () => {
            const d = await fetch('/api/leaderboard').then(r => r.json());
            const list = document.getElementById('leaderboard-list');
            list.innerHTML = '';
            d.top100.forEach(u => {
                const item = document.createElement('div');
                item.className = 'py-3 flex items-center justify-between text-xs';
                item.innerHTML = '<div><strong class="text-white text-sm">#' + u.rank + ' ' + u.name + '</strong></div>' +
                                 '<div class="text-right"><span class="font-bold text-emerald-400">' + u.pnl + '</span> (' + u.winRate + ')</div>';
                list.appendChild(item);
            });
            document.getElementById('leaderboard-modal').classList.remove('hidden');
        };
        document.getElementById('btn-close-leaderboard').onclick = () => document.getElementById('leaderboard-modal').classList.add('hidden');

        // Login Mock
        document.getElementById('btn-login-trigger').onclick = () => {
            document.getElementById('btn-login-trigger').classList.add('hidden');
            document.getElementById('user-profile-badge').classList.remove('hidden');
            document.getElementById('user-profile-badge').classList.add('flex');
        };

        // Filtreler
        document.querySelectorAll('.cat-btn').forEach(btn => {
            btn.onclick = (e) => {
                document.querySelectorAll('.cat-btn').forEach(b => b.className = 'cat-btn bg-slate-800 hover:bg-slate-700 text-slate-300 px-4 py-1.5 rounded-lg text-xs font-bold transition');
                e.target.className = 'cat-btn active bg-purple-600 text-white px-4 py-1.5 rounded-lg text-xs font-bold transition';
                activeCat = e.target.getAttribute('data-cat');
                renderMarkets();
            };
        });

        init();
    </script>
</body>
</html>`);
});

// ==========================================
// 7. BAŞLATICI
// ==========================================
await initDatabase();
const port = Number(process.env.PORT) || 3000;
app.listen({ port, host: '0.0.0.0' }, (err, address) => {
    if (err) { console.error(err); process.exit(1); }
    console.log(`[OYVER] Sistem ayakta: ${address}`);
});
