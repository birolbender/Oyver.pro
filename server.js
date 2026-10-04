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
        if (mNet.lte(0)) throw new Error('INVALID_AMOUNT: Net katılım puanı 0 veya negatif olamaz.');

        const k = state.yesReserve.mul(state.noReserve);
        const newNoReserve = state.noReserve.plus(mNet);
        const newYesReserve = k.div(newNoReserve);
        const deltaY = state.yesReserve.minus(newYesReserve);
        const sharesOut = MoneyMath.roundDown(mNet.plus(deltaY), 6);

        const currentProb = state.noReserve.div(state.yesReserve.plus(state.noReserve));
        const newProb = newNoReserve.div(newYesReserve.plus(newNoReserve));
        const priceImpact = newProb.minus(currentProb).abs().mul(100).toNumber();

        return { 
            mNet, 
            fee, 
            newYesReserve, 
            newNoReserve, 
            sharesOut,
            avgPrice: mGross.div(sharesOut).toDecimalPlaces(4, Decimal.ROUND_DOWN).toNumber(),
            priceImpact: Math.min(99.9, Math.round(priceImpact * 10) / 10)
        };
    }

    static calculateSell(state, sharesIn, feeRate) {
        const S = new Decimal(sharesIn);
        if (S.lte(0)) throw new Error('INVALID_AMOUNT: Satılacak pay 0 veya negatif olamaz.');

        const Y = state.yesReserve;
        const N = state.noReserve;
        const B = Y.plus(N).plus(S);
        const C = S.mul(N);

        const discriminant = B.pow(2).minus(C.mul(4));
        if (discriminant.lt(0)) throw new Error('MATH_ERROR: Diskriminant negatif olamaz.');

        const grossPayout = C.mul(2).div(B.plus(discriminant.sqrt()));
        if (grossPayout.gte(N)) throw new Error('SOLVENCY_VIOLATION: Ödeme rezervi aşamaz.');

        const fee = MoneyMath.roundUp(grossPayout.mul(feeRate), 6);
        const netPayout = MoneyMath.roundDown(grossPayout.minus(fee), 6);

        const currentProb = N.div(Y.plus(N));
        const newYesReserve = Y.plus(S).minus(grossPayout);
        const newNoReserve = N.minus(grossPayout);
        const newProb = newNoReserve.div(newYesReserve.plus(newNoReserve));
        const priceImpact = currentProb.minus(newProb).abs().mul(100).toNumber();

        return {
            grossPayout: MoneyMath.roundDown(grossPayout, 6),
            fee,
            netPayout,
            newYesReserve,
            newNoReserve,
            avgPrice: netPayout.div(S).toDecimalPlaces(4, Decimal.ROUND_DOWN).toNumber(),
            priceImpact: Math.min(99.9, Math.round(priceImpact * 10) / 10)
        };
    }
}

// ==========================================
// 2. VERİTABANI BAĞLANTISI VE DEFTER
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

class LedgerEngine {
    static async recordEntry(client, entryType, refId, idempKey, lines) {
        let totalD = new Decimal(0), totalC = new Decimal(0);
        for (const l of lines) { totalD = totalD.plus(l.debit); totalC = totalC.plus(l.credit); }
        if (!totalD.eq(totalC) || totalD.lte(0)) throw new Error('LEDGER_UNBALANCED: Borç ve alacak eşit olmalıdır.');

        const er = await client.query(
            `INSERT INTO ledger_entries (entry_type, reference_id, idempotency_key) VALUES ($1, $2, $3) RETURNING id`,
            [entryType, refId, idempKey || crypto.randomUUID()]
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

async function initDatabase() {
    const client = await pool.connect();
    try {
        console.info('[DATABASE] Şema, iletişim ve düello tabloları tescilleniyor...');
        await client.query(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`);

        await client.query(`
            CREATE TABLE IF NOT EXISTS users (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                email VARCHAR(255) UNIQUE NOT NULL,
                username VARCHAR(64) UNIQUE NOT NULL,
                password_hash VARCHAR(255) DEFAULT 'OAUTH_MOCK',
                role VARCHAR(16) NOT NULL DEFAULT 'USER',
                balance_kor NUMERIC(24,6) NOT NULL DEFAULT 14500.000000,
                streak INT NOT NULL DEFAULT 1,
                quests_today INT NOT NULL DEFAULT 0,
                quest_rewarded BOOLEAN NOT NULL DEFAULT FALSE,
                tier VARCHAR(16) NOT NULL DEFAULT 'ANALYST',
                birth_year INT,
                education_level VARCHAR(32),
                employment_status VARCHAR(32),
                industry VARCHAR(64),
                city VARCHAR(32),
                onboarding_completed BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS markets (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                slug VARCHAR(255) UNIQUE,
                question TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS amm_state (
                market_id UUID PRIMARY KEY REFERENCES markets(id) ON DELETE CASCADE,
                yes_reserve NUMERIC(30,12) NOT NULL CHECK (yes_reserve > 0),
                no_reserve NUMERIC(30,12) NOT NULL CHECK (no_reserve > 0),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS market_price_history (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                market_id UUID NOT NULL REFERENCES markets(id) ON DELETE CASCADE,
                prob_yes INT NOT NULL,
                pool_total NUMERIC(24,6) NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS positions (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                market_id UUID NOT NULL REFERENCES markets(id) ON DELETE CASCADE,
                user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                outcome VARCHAR(8) NOT NULL,
                shares NUMERIC(30,12) NOT NULL DEFAULT 0,
                total_invested NUMERIC(24,6) NOT NULL DEFAULT 0,
                entry_prob NUMERIC(6,2) NOT NULL DEFAULT 50.00,
                realized_pnl NUMERIC(24,6) NOT NULL DEFAULT 0,
                is_settled BOOLEAN NOT NULL DEFAULT FALSE,
                settlement_payout NUMERIC(24,6) NOT NULL DEFAULT 0,
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
                debit NUMERIC(24,6) NOT NULL DEFAULT 0,
                credit NUMERIC(24,6) NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS comments (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                market_id UUID NOT NULL REFERENCES markets(id) ON DELETE CASCADE,
                user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                content TEXT NOT NULL,
                outcome_at_time VARCHAR(8),
                shares_at_time NUMERIC(24,2) DEFAULT 0,
                upvotes INT NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS comment_votes (
                comment_id UUID NOT NULL REFERENCES comments(id) ON DELETE CASCADE,
                user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (comment_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS hero_duels (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                title TEXT NOT NULL,
                category VARCHAR(64) NOT NULL,
                option_a_name TEXT NOT NULL,
                option_a_img TEXT,
                option_b_name TEXT NOT NULL,
                option_b_img TEXT,
                votes_a INT NOT NULL DEFAULT 0,
                votes_b INT NOT NULL DEFAULT 0,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS duel_votes (
                duel_id UUID NOT NULL REFERENCES hero_duels(id) ON DELETE CASCADE,
                user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                choice VARCHAR(4) NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (duel_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS contact_leads (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                type VARCHAR(32) NOT NULL DEFAULT 'GENERAL', -- 'GENERAL', 'B2B_SALES'
                name VARCHAR(128) NOT NULL,
                email VARCHAR(255) NOT NULL,
                company VARCHAR(128),
                message TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS zarla_polls (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                question TEXT NOT NULL,
                option_a TEXT NOT NULL,
                option_b TEXT NOT NULL,
                votes_a INT NOT NULL DEFAULT 0,
                votes_b INT NOT NULL DEFAULT 0,
                category VARCHAR(64) NOT NULL DEFAULT 'GÜNDEM',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS sessions (
                token CHAR(64) PRIMARY KEY,
                user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);

        // Sütun Güvenceleri
        await client.query(`
            ALTER TABLE markets ADD COLUMN IF NOT EXISTS slug VARCHAR(255);
            ALTER TABLE markets ADD COLUMN IF NOT EXISTS category VARCHAR(64) NOT NULL DEFAULT 'EKONOMİ';
            ALTER TABLE markets ADD COLUMN IF NOT EXISTS description TEXT DEFAULT 'Bu oylama resmi bülten verisiyle sonuçlandırılacaktır.';
            ALTER TABLE markets ADD COLUMN IF NOT EXISTS source_url TEXT;
            ALTER TABLE markets ADD COLUMN IF NOT EXISTS source_name VARCHAR(128);
            ALTER TABLE markets ADD COLUMN IF NOT EXISTS status VARCHAR(32) NOT NULL DEFAULT 'TRADING';
            ALTER TABLE markets ADD COLUMN IF NOT EXISTS resolved_outcome VARCHAR(8);
            ALTER TABLE markets ADD COLUMN IF NOT EXISTS resolution_proof TEXT;
            ALTER TABLE markets ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ;
            ALTER TABLE markets ADD COLUMN IF NOT EXISTS closing_date VARCHAR(64) NOT NULL DEFAULT '31 Aralık 2026';

            ALTER TABLE users ADD COLUMN IF NOT EXISTS role VARCHAR(16) NOT NULL DEFAULT 'USER';
            ALTER TABLE users ADD COLUMN IF NOT EXISTS balance_kor NUMERIC(24,6) NOT NULL DEFAULT 14500.000000;
            ALTER TABLE users ADD COLUMN IF NOT EXISTS streak INT NOT NULL DEFAULT 1;

            CREATE UNIQUE INDEX IF NOT EXISTS uq_markets_slug ON markets (slug);
        `);

        for (const code of ['1000', '3000', '4000', '5000']) {
            await client.query(`INSERT INTO accounts (code) VALUES ($1) ON CONFLICT DO NOTHING`, [code]);
        }

        // Demo Kullanıcıları (Yalnızca ilk kurulumda)
        const checkUsers = await client.query(`SELECT count(*) FROM users`);
        if (parseInt(checkUsers.rows[0].count, 10) === 0) {
            await client.query(`
                INSERT INTO users (id, email, username, password_hash, role, balance_kor, streak, tier, onboarding_completed, birth_year, education_level, industry, city) VALUES
                ('11111111-1111-1111-1111-111111111111', 'demo@oyver.pro', 'LeisanB', 'OAUTH_MOCK', 'ADMIN', 14500, 5, 'ANALYST', true, 1989, 'MASTER_PHD', 'TECH', 'İstanbul'),
                ('22222222-2222-2222-2222-222222222222', 'ahmet@oyver.pro', 'Ahmet_Kahin', 'OAUTH_MOCK', 'USER', 420000, 14, 'PRO', true, 1984, 'BACHELOR', 'FINANCE', 'İstanbul'),
                ('33333333-3333-3333-3333-333333333333', 'ece@oyver.pro', 'Ece_Analist', 'OAUTH_MOCK', 'USER', 315000, 9, 'PRO', true, 1992, 'MASTER_PHD', 'FINANCE', 'Ankara')
                ON CONFLICT (email) DO NOTHING;
            `);
        }

        // Hero Düellolar (Yalnızca ilk kurulumda)
        const checkDuels = await client.query(`SELECT count(*) FROM hero_duels`);
        if (parseInt(checkDuels.rows[0].count, 10) === 0) {
            await client.query(`
                INSERT INTO hero_duels (title, category, option_a_name, option_a_img, option_b_name, option_b_img, votes_a, votes_b, is_active) VALUES
                ('2028 Cumhurbaşkanlığı Seçim Tercihiniz Hangisi?', 'SİYASET DÜELLOSU', 'Recep Tayyip Erdoğan', 'https://images.unsplash.com/photo-1544005313-94ddf0286df2?auto=format&fit=crop&w=120&h=120&q=80', 'Kemal Kılıçdaroğlu', 'https://images.unsplash.com/photo-1507003211169-0a1dd7228f2d?auto=format&fit=crop&w=120&h=120&q=80', 5420, 4580, true),
                ('Süper Lig Hafta Sonu Derbisini Kim Kazanır?', 'DERBİ DÜELLOSU', 'Galatasaray', 'https://images.unsplash.com/photo-1508098682722-e99c43a406b2?auto=format&fit=crop&w=120&h=120&q=80', 'Fenerbahçe', 'https://images.unsplash.com/photo-1579952363873-27f3bade9f55?auto=format&fit=crop&w=120&h=120&q=80', 5120, 4880, true)
            `);
        }

        // 5 Tescilli Pazar (Yalnızca ilk kurulumda eklenir, asla üzerine yazılmaz!)
        const checkMarkets = await client.query(`SELECT count(*) FROM markets`);
        if (parseInt(checkMarkets.rows[0].count, 10) === 0) {
            const officialMarkets = [
                { slug: 'asgari-ucret-2027', cat: 'EKONOMİ', q: '2027 Yılı Net Asgari Ücreti 35.000 TL Üzerinde Açıklanır mı?', desc: 'Asgari Ücret Tespit Komisyonu nihai kararının Resmi Gazete yayımlanan tutarı esas alınacaktır.', srcName: 'Çalışma Bakanlığı / Resmi Gazete', srcUrl: 'https://resmigazete.gov.tr', closing: '31 Aralık 2026', yesR: 8000, noR: 12000 },
                { slug: 'tcmb-faiz-2026', cat: 'EKONOMİ', q: 'TCMB Politika Faizini 2026 Yıl Sonuna Kadar %30 veya Altına İndirir mi?', desc: 'TCMB PPK Aralık 2026 toplantı kararında açıklanan 1 haftalık repo faizi esas alınır.', srcName: 'TCMB PPK Karar Metni', srcUrl: 'https://tcmb.gov.tr', closing: '24 Aralık 2026', yesR: 11000, noR: 9000 },
                { slug: 'bist-100-2026', cat: 'FİNANS', q: 'BIST 100 Endeksi 2026 Son Çeyreğini 12.000 Puan Üzerinde Kapatır mı?', desc: 'Borsa İstanbul 31 Aralık 2026 seans kapanışındaki resmi BIST 100 kapanış endeksi dikkate alınır.', srcName: 'Borsa İstanbul Bülteni', srcUrl: 'https://borsaistanbul.com', closing: '31 Aralık 2026', yesR: 10000, noR: 10000 },
                { slug: 'turksat-6a-ticari', cat: 'TEKNOLOJİ', q: 'TÜRKSAT 6A Uydusu 2026 Yılında Tam Kapasite Ticari Hizmete Başlar mı?', desc: 'Ulaştırma Bakanlığı resmi bülteninde uydunun ticari hizmete girdiği teyit edilmelidir.', srcName: 'Ulaştırma Bakanlığı', srcUrl: 'https://uab.gov.tr', closing: '15 Kasım 2026', yesR: 6000, noR: 14000 },
                { slug: 'turizm-ziyaretci-2026', cat: 'KÜLTÜR & YAŞAM', q: '2026 Yılında Türkiye\'ye Gelen Yabancı Ziyaretçi Sayısı 60 Milyonu Aşar mı?', desc: 'TÜİK tarafından Ocak 2027\'de yayımlanacak 2026 dördüncü çeyrek turizm bülteni esas alınır.', srcName: 'TÜİK Turizm İstatistikleri', srcUrl: 'https://tuik.gov.tr', closing: '31 Ocak 2027', yesR: 7000, noR: 13000 }
            ];

            for (const item of officialMarkets) {
                const mRes = await client.query(`
                    INSERT INTO markets (slug, category, question, description, source_name, source_url, closing_date)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    RETURNING id
                `, [item.slug, item.cat, item.q, item.desc, item.srcName, item.srcUrl, item.closing]);

                const mId = mRes.rows[0].id;
                await client.query(`INSERT INTO accounts (code, market_id) VALUES ('2100', $1) ON CONFLICT DO NOTHING`, [mId]);
                await client.query(`INSERT INTO amm_state (market_id, yes_reserve, no_reserve) VALUES ($1, $2, $3)`, [mId, item.yesR, item.noR]);

                const now = Date.now();
                for (let i = 0; i < 7; i++) {
                    const daysAgo = (6 - i) * 86400000;
                    await client.query(`
                        INSERT INTO market_price_history (market_id, prob_yes, pool_total, created_at)
                        VALUES ($1, $2, 20000, to_timestamp($3))
                    `, [mId, Math.round((item.noR / (item.yesR + item.noR)) * 100), (now - daysAgo) / 1000]);
                }
            }
        }

        console.info('[DATABASE] Sistem Hazır.');
    } finally {
        client.release();
    }
}

// ==========================================
// 3. SUNUCU, REST VE WEBSOCKET MERKEZİ
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

// GÜVENLİ AUTH KANCASI (Oturumsuzlar KESİNLİKLE 'USER' rolündedir!)
app.addHook('onRequest', async (req) => {
    let token = null;
    const cookie = req.headers.cookie;
    if (cookie) {
        const m = cookie.split(';').find(c => c.trim().startsWith('oyver_token='));
        if (m) token = m.split('=')[1].trim();
    }
    if (token) {
        const r = await pool.query(`SELECT u.id, u.role, u.username FROM sessions s JOIN users u ON s.user_id = u.id WHERE s.token = $1`, [token]);
        if (r.rows.length > 0) {
            req.userId = r.rows[0].id;
            req.userRole = r.rows[0].role;
            req.username = r.rows[0].username;
        }
    }
    if (!req.userId) {
        req.userId = '11111111-1111-1111-1111-111111111111';
        req.userRole = 'USER'; // ASLA 'ADMIN' DEĞİL!
        req.username = 'Misafir';
    }
});

app.get('/ws', { websocket: true }, (connection) => {
    const ws = connection.socket;
    wsClients.add(ws);
    ws.on('close', () => wsClients.delete(ws));
});

app.get('/health', async () => ({ status: 'UP', version: 'v1.1-PROD', timestamp: new Date().toISOString() }));

app.get('/api/me', async (req) => {
    const r = await pool.query(`
        SELECT id, username, email, role, balance_kor, streak, quests_today, quest_rewarded,
               tier, birth_year, education_level, industry, city, onboarding_completed
        FROM users WHERE id = $1
    `, [req.userId]);
    return r.rows[0] || {};
});

// HERO DÜELLOLAR
app.get('/api/duels', async () => {
    const r = await pool.query(`SELECT * FROM hero_duels WHERE is_active = true ORDER BY created_at ASC`);
    const duels = r.rows.map(d => {
        const tot = d.votes_a + d.votes_b;
        const pctA = tot > 0 ? Math.round((d.votes_a / tot) * 100) : 50;
        return { ...d, pctA, pctB: 100 - pctA, totalVotes: tot.toLocaleString('tr-TR') };
    });
    return { duels };
});

app.post('/api/duels/:id/vote', async (req, rep) => {
    const { id } = req.params;
    const { choice } = req.body || {};
    if (choice !== 'A' && choice !== 'B') return rep.status(400).send({ error: 'Geçersiz tercih' });

    try {
        const result = await runInTransaction(async (c) => {
            const vCheck = await c.query(`SELECT 1 FROM duel_votes WHERE duel_id = $1 AND user_id = $2`, [id, req.userId]);
            if (vCheck.rows.length > 0) throw new Error('Bu düelloda zaten oy kullandınız.');

            await c.query(`INSERT INTO duel_votes (duel_id, user_id, choice) VALUES ($1, $2, $3)`, [id, req.userId, choice]);
            const col = choice === 'A' ? 'votes_a' : 'votes_b';
            const r = await c.query(`UPDATE hero_duels SET ${col} = ${col} + 1 WHERE id = $1 RETURNING votes_a, votes_b`, [id]);

            const va = r.rows[0].votes_a, vb = r.rows[0].votes_b;
            const tot = va + vb;
            return { pctA: Math.round((va / tot) * 100), pctB: 100 - Math.round((va / tot) * 100), totalVotes: tot.toLocaleString('tr-TR') };
        });

        broadcast('DUEL_VOTE', { duelId: id, ...result });
        return rep.send({ success: true, ...result });
    } catch(e) {
        return rep.status(400).send({ error: e.message });
    }
});

// PAZARLAR & DETAY
app.get('/api/markets', async () => {
    const r = await pool.query(`
        SELECT m.*, a.yes_reserve, a.no_reserve 
        FROM markets m JOIN amm_state a ON m.id = a.market_id 
        ORDER BY m.created_at ASC
    `);
    const formatted = r.rows.map(m => {
        const y = new Decimal(m.yes_reserve), n = new Decimal(m.no_reserve);
        const probYes = Math.round(n.div(y.plus(n)).mul(100).toNumber());
        return { ...m, probYes, probNo: 100 - probYes, poolTotal: Math.round(y.plus(n).toNumber()).toLocaleString('tr-TR') };
    });
    return { markets: formatted };
});

app.get('/api/markets/:slug', async (req, rep) => {
    const { slug } = req.params;
    const r = await pool.query(`
        SELECT m.*, a.yes_reserve, a.no_reserve 
        FROM markets m JOIN amm_state a ON m.id = a.market_id 
        WHERE m.slug = $1
    `, [slug]);

    if (r.rows.length === 0) return rep.status(404).send({ error: 'Pazar bulunamadı.' });
    const m = r.rows[0];
    const y = new Decimal(m.yes_reserve), n = new Decimal(m.no_reserve);
    const probYes = Math.round(n.div(y.plus(n)).mul(100).toNumber());

    const hr = await pool.query(`
        SELECT prob_yes, pool_total, to_char(created_at, 'DD Mon HH24:MI') as time_label 
        FROM market_price_history WHERE market_id = $1 ORDER BY created_at ASC
    `, [m.id]);

    return {
        market: { ...m, probYes, probNo: 100 - probYes, poolTotal: Math.round(y.plus(n).toNumber()).toLocaleString('tr-TR') },
        history: hr.rows
    };
});

// QUOTE MOTORU
app.post('/api/trade/quote', async (req, rep) => {
    const { marketId, outcome, amountKor, action, sharesIn } = req.body || {};
    const mr = await pool.query(`SELECT yes_reserve, no_reserve FROM amm_state WHERE market_id = $1`, [marketId]);
    if (mr.rows.length === 0) return rep.status(404).send({ error: 'Pazar bulunamadı' });

    const isYes = outcome === 'YES';
    const amm = { yesReserve: new Decimal(mr.rows[0].yes_reserve), noReserve: new Decimal(mr.rows[0].no_reserve) };

    if (action === 'SELL') {
        const sIn = new Decimal(sharesIn || 0);
        if (sIn.lte(0)) return rep.status(400).send({ error: 'Geçersiz pay' });
        const calc = AMMEngine.calculateSell(isYes ? amm : { yesReserve: amm.noReserve, noReserve: amm.yesReserve }, sIn, new Decimal(0.02));
        return { action: 'SELL', netPayout: Math.round(calc.netPayout.toNumber()).toLocaleString('tr-TR') + ' KOR', avgPrice: calc.avgPrice, priceImpact: calc.priceImpact };
    } else {
        const amt = new Decimal(amountKor || 0);
        if (amt.lte(0)) return rep.status(400).send({ error: 'Geçersiz tutar' });
        const calc = AMMEngine.calculateBuy(isYes ? amm : { yesReserve: amm.noReserve, noReserve: amm.yesReserve }, amt, new Decimal(0.02));
        return { action: 'BUY', sharesOut: calc.sharesOut.toFixed(2), avgPrice: calc.avgPrice, priceImpact: calc.priceImpact, targetPayout: Math.round(calc.sharesOut.toNumber()).toLocaleString('tr-TR') + ' KOR' };
    }
});

// TAHMİN ALIM & SATIM
app.post('/api/trade/predict', async (req, rep) => {
    const { marketId, outcome, amountKor } = req.body || {};
    const amt = new Decimal(amountKor || 0);
    if (!marketId || !outcome || amt.lte(0)) return rep.status(400).send({ error: 'Geçersiz tutar.' });

    try {
        const result = await runInTransaction(async (c) => {
            const mCheck = await c.query(`SELECT status FROM markets WHERE id = $1`, [marketId]);
            if (mCheck.rows.length === 0 || mCheck.rows[0].status !== 'TRADING') throw new Error('Bu oylama kapalıdır.');

            const ur = await c.query(`UPDATE users SET balance_kor = balance_kor - $1 WHERE id = $2 AND balance_kor >= $1 RETURNING balance_kor`, [amt.toFixed(6), req.userId]);
            if (ur.rows.length === 0) throw new Error('Yetersiz KOR bakiyesi.');

            const mr = await c.query(`SELECT yes_reserve, no_reserve FROM amm_state WHERE market_id = $1 FOR UPDATE`, [marketId]);
            const isYes = outcome === 'YES';
            const amm = { yesReserve: new Decimal(mr.rows[0].yes_reserve), noReserve: new Decimal(mr.rows[0].no_reserve) };
            const calc = AMMEngine.calculateBuy(isYes ? amm : { yesReserve: amm.noReserve, noReserve: amm.yesReserve }, amt, new Decimal(0.02));

            const ny = isYes ? calc.newYesReserve : calc.newNoReserve;
            const nn = isYes ? calc.newNoReserve : calc.newYesReserve;
            await c.query(`UPDATE amm_state SET yes_reserve = $1, no_reserve = $2, updated_at = NOW() WHERE market_id = $3`, [ny.toFixed(12), nn.toFixed(12), marketId]);

            const feeHalf = MoneyMath.roundDown(calc.fee.div(2), 6);
            await LedgerEngine.recordEntry(c, 'PREDICTION_BUY', marketId, null, [
                { accountCode: '2000', userId: req.userId, debit: amt, credit: new Decimal(0) },
                { accountCode: '2100', marketId, debit: new Decimal(0), credit: calc.mNet },
                { accountCode: '4000', debit: new Decimal(0), credit: feeHalf },
                { accountCode: '5000', debit: new Decimal(0), credit: calc.fee.minus(feeHalf) }
            ]);

            const currentEntryProb = Math.round(new Decimal(mr.rows[0].no_reserve).div(new Decimal(mr.rows[0].yes_reserve).plus(new Decimal(mr.rows[0].no_reserve))).mul(100).toNumber());
            const newProb = Math.round(nn.div(ny.plus(nn)).mul(100).toNumber());
            await c.query(`INSERT INTO market_price_history (market_id, prob_yes, pool_total) VALUES ($1, $2, $3)`, [marketId, newProb, ny.plus(nn).toFixed(2)]);

            await c.query(`
                INSERT INTO positions (market_id, user_id, outcome, shares, total_invested, entry_prob)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (user_id, market_id, outcome) DO UPDATE 
                SET shares = positions.shares + $4, total_invested = positions.total_invested + $5, updated_at = NOW()
            `, [marketId, req.userId, outcome, calc.sharesOut.toFixed(6), amt.toFixed(6), currentEntryProb]);

            return { newBalance: MoneyMath.roundDown(ur.rows[0].balance_kor, 0).toNumber(), sharesOut: calc.sharesOut.toFixed(2), ny, nn };
        });

        const y = new Decimal(result.ny), n = new Decimal(result.nn);
        const probYes = Math.round(n.div(y.plus(n)).mul(100).toNumber());
        broadcast('MARKET_UPDATE', { marketId, probYes, probNo: 100 - probYes, poolTotal: Math.round(y.plus(n).toNumber()).toLocaleString('tr-TR') });
        return rep.send({ success: true, balanceKor: result.newBalance, sharesOut: result.sharesOut });
    } catch(e) {
        return rep.status(400).send({ error: e.message });
    }
});

app.post('/api/trade/sell', async (req, rep) => {
    const { marketId, outcome, sharesToSell } = req.body || {};
    const sIn = new Decimal(sharesToSell || 0);
    if (!marketId || !outcome || sIn.lte(0)) return rep.status(400).send({ error: 'Geçersiz pay.' });

    try {
        const result = await runInTransaction(async (c) => {
            const pr = await c.query(`SELECT shares, total_invested FROM positions WHERE market_id = $1 AND user_id = $2 AND outcome = $3 FOR UPDATE`, [marketId, req.userId, outcome]);
            if (pr.rows.length === 0 || new Decimal(pr.rows[0].shares).lt(sIn)) throw new Error('Yetersiz pay.');

            const mr = await c.query(`SELECT yes_reserve, no_reserve FROM amm_state WHERE market_id = $1 FOR UPDATE`, [marketId]);
            const isYes = outcome === 'YES';
            const amm = { yesReserve: new Decimal(mr.rows[0].yes_reserve), noReserve: new Decimal(mr.rows[0].no_reserve) };
            const calc = AMMEngine.calculateSell(isYes ? amm : { yesReserve: amm.noReserve, noReserve: amm.yesReserve }, sIn, new Decimal(0.02));

            const ny = isYes ? calc.newYesReserve : calc.newNoReserve;
            const nn = isYes ? calc.newNoReserve : calc.newYesReserve;
            await c.query(`UPDATE amm_state SET yes_reserve = $1, no_reserve = $2, updated_at = NOW() WHERE market_id = $3`, [ny.toFixed(12), nn.toFixed(12), marketId]);

            let remainingShares = new Decimal(pr.rows[0].shares).minus(sIn);
            if (remainingShares.lt(0.001)) remainingShares = new Decimal(0);

            await c.query(`
                UPDATE positions SET shares = $1, realized_pnl = realized_pnl + $2, updated_at = NOW()
                WHERE market_id = $3 AND user_id = $4 AND outcome = $5
            `, [remainingShares.toFixed(6), calc.netPayout.toFixed(6), marketId, req.userId, outcome]);

            const ur = await c.query(`UPDATE users SET balance_kor = balance_kor + $1 WHERE id = $2 RETURNING balance_kor`, [calc.netPayout.toFixed(6), req.userId]);
            return { newBalance: MoneyMath.roundDown(ur.rows[0].balance_kor, 0).toNumber(), payoutKor: calc.netPayout.toFixed(2), ny, nn };
        });

        const y = new Decimal(result.ny), n = new Decimal(result.nn);
        const probYes = Math.round(n.div(y.plus(n)).mul(100).toNumber());
        broadcast('MARKET_UPDATE', { marketId, probYes, probNo: 100 - probYes, poolTotal: Math.round(y.plus(n).toNumber()).toLocaleString('tr-TR') });
        return rep.send({ success: true, ...result });
    } catch(e) {
        return rep.status(400).send({ error: e.message });
    }
});

// PORTFÖY
app.get('/api/portfolio', async (req) => {
    const r = await pool.query(`
        SELECT p.*, m.question, m.slug, m.category, m.status as market_status, m.resolved_outcome, a.yes_reserve, a.no_reserve 
        FROM positions p JOIN markets m ON p.market_id = m.id JOIN amm_state a ON m.id = a.market_id
        WHERE p.user_id = $1 AND (p.shares > 0.001 OR p.is_settled = true) ORDER BY p.updated_at DESC
    `, [req.userId]);

    const active = [], settled = [];
    r.rows.forEach(row => {
        if (row.is_settled) {
            settled.push({ id: row.id, question: row.question, slug: row.slug, outcome: row.outcome, payout: Math.round(parseFloat(row.settlement_payout)), resolvedOutcome: row.resolved_outcome, won: row.settlement_payout > 0 });
        } else if (parseFloat(row.shares) > 0.001) {
            let currentSellVal = 0;
            try {
                const isYes = row.outcome === 'YES';
                const amm = { yesReserve: new Decimal(row.yes_reserve), noReserve: new Decimal(row.no_reserve) };
                const calc = AMMEngine.calculateSell(isYes ? amm : { yesReserve: amm.noReserve, noReserve: amm.yesReserve }, new Decimal(row.shares), new Decimal(0.02));
                currentSellVal = Math.round(calc.netPayout.toNumber());
            } catch(e) {}
            active.push({ id: row.id, marketId: row.market_id, question: row.question, slug: row.slug, outcome: row.outcome, shares: parseFloat(row.shares).toFixed(2), invested: Math.round(parseFloat(row.total_invested)), currentSellValue: currentSellVal, category: row.category });
        }
    });
    return { active, settled };
});

// BİZE ULAŞIN VE B2B TALEP FORMU
app.post('/api/contact', async (req, rep) => {
    const { name, email, company, message, type } = req.body || {};
    if (!name || !email || !message) return rep.status(400).send({ error: 'Lütfen zorunlu alanları doldurun.' });

    await pool.query(`
        INSERT INTO contact_leads (type, name, email, company, message)
        VALUES ($1, $2, $3, $4, $5)
    `, [type || 'GENERAL', name, email, company || null, message]);

    return { success: true, message: 'Mesajınız başarıyla iletildi. Ekibimiz en kısa sürede dönüş yapacaktır.' };
});

// YORUMLAR & KOHORT
app.get('/api/markets/:slug/comments', async (req, rep) => {
    const { slug } = req.params;
    const mRes = await pool.query(`SELECT id FROM markets WHERE slug = $1`, [slug]);
    if (mRes.rows.length === 0) return rep.status(404).send({ error: 'Pazar bulunamadı' });
    const r = await pool.query(`SELECT c.*, u.username, u.tier, to_char(c.created_at, 'DD Mon HH24:MI') as time_formatted FROM comments c JOIN users u ON c.user_id = u.id WHERE c.market_id = $1 ORDER BY c.upvotes DESC, c.created_at DESC`, [mRes.rows[0].id]);
    return { comments: r.rows };
});

app.post('/api/markets/:slug/comments', async (req, rep) => {
    const { slug } = req.params;
    const { content } = req.body || {};
    if (!content || content.trim().length < 5) return rep.status(400).send({ error: 'En az 5 karakter yazın.' });
    const mRes = await pool.query(`SELECT id FROM markets WHERE slug = $1`, [slug]);
    const posRes = await pool.query(`SELECT outcome, shares FROM positions WHERE market_id = $1 AND user_id = $2 AND shares > 0.001 LIMIT 1`, [mRes.rows[0].id, req.userId]);
    const outcome = posRes.rows.length > 0 ? posRes.rows[0].outcome : null;
    const shares = posRes.rows.length > 0 ? Math.round(parseFloat(posRes.rows[0].shares)) : 0;
    await pool.query(`INSERT INTO comments (market_id, user_id, content, outcome_at_time, shares_at_time) VALUES ($1, $2, $3, $4, $5)`, [mRes.rows[0].id, req.userId, content.trim(), outcome, shares]);
    return { success: true };
});

app.get('/api/markets/:slug/cohorts', async (req, rep) => {
    const { slug } = req.params;
    const mRes = await pool.query(`SELECT id FROM markets WHERE slug = $1`, [slug]);
    const eduRes = await pool.query(`SELECT u.education_level, p.outcome, COUNT(p.id) as vote_count FROM positions p JOIN users u ON p.user_id = u.id WHERE p.market_id = $1 AND p.shares > 0.001 GROUP BY u.education_level, p.outcome`, [mRes.rows[0].id]);
    const ageRes = await pool.query(`SELECT CASE WHEN (2026 - u.birth_year) < 30 THEN 'Genç (18-29)' WHEN (2026 - u.birth_year) BETWEEN 30 AND 44 THEN 'Orta Yaş (30-44)' ELSE 'Deneyimli (45+)' END as age_group, p.outcome, COUNT(p.id) as vote_count FROM positions p JOIN users u ON p.user_id = u.id WHERE p.market_id = $1 AND p.shares > 0.001 AND u.birth_year IS NOT NULL GROUP BY age_group, p.outcome`, [mRes.rows[0].id]);
    return { educationCohorts: eduRes.rows, ageCohorts: ageRes.rows };
});

// TOP 100 LİDERLİK
app.get('/api/leaderboard', async () => {
    const usersRes = await pool.query(`SELECT id, username, streak, tier, balance_kor FROM users`);
    const posRes = await pool.query(`SELECT p.*, m.resolved_outcome FROM positions p JOIN markets m ON p.market_id = m.id WHERE p.is_settled = true`);
    const leaderboard = usersRes.rows.map(u => {
        const uPositions = posRes.rows.filter(p => p.user_id === u.id);
        let wonCount = 0, netPnl = 0;
        for (const p of uPositions) {
            if (p.settlement_payout > 0) wonCount++;
            netPnl += parseFloat(p.realized_pnl);
        }
        const winRate = uPositions.length > 0 ? Math.round((wonCount / uPositions.length) * 100) : 0;
        const frsScore = Math.max(0, Math.round((netPnl * 0.35) + (Math.log10(uPositions.length + 1) * 1000 * 0.15) + (u.streak * 50 * 0.10)));
        return { id: u.id, name: u.username, tier: u.tier || 'ANALYST', streak: u.streak, frsScore, winRate, totalSettled: uPositions.length };
    });
    leaderboard.sort((a, b) => b.frsScore - a.frsScore);
    return { top100: leaderboard.slice(0, 100).map((item, idx) => ({ rank: idx + 1, ...item })) };
});

// MOCK GİRİŞ
app.post('/api/auth/login-mock', async (req, rep) => {
    const { username } = req.body || {};
    const name = username || 'LeisanB';
    const email = `${name.toLowerCase()}@oyver.pro`;
    const role = name === 'AdminLeisan' ? 'ADMIN' : 'USER'; // Yalnızca özel isimle Admin olunabilir

    const ur = await pool.query(`
        INSERT INTO users (email, username, password_hash, role, balance_kor, streak, tier)
        VALUES ($1, $2, 'OAUTH_MOCK', $3, 14500, 5, 'ANALYST')
        ON CONFLICT (email) DO UPDATE SET username = EXCLUDED.username
        RETURNING id, username, role, balance_kor, streak, tier
    `, [email, name, role]);

    const user = ur.rows[0];
    const token = crypto.randomBytes(32).toString('hex');
    await pool.query(`INSERT INTO sessions (token, user_id) VALUES ($1, $2)`, [token, user.id]);
    rep.header('Set-Cookie', `oyver_token=${token}; Path=/; HttpOnly; SameSite=Lax; Max-Age=2592000`);
    return { success: true, user };
});

// ZARLA
app.get('/api/zarla', async () => {
    const r = await pool.query(`SELECT * FROM zarla_polls ORDER BY created_at ASC`);
    const polls = r.rows.map(p => {
        const tot = p.votes_a + p.votes_b;
        const pctA = tot > 0 ? Math.round((p.votes_a / tot) * 100) : 50;
        return { ...p, totalVotes: tot.toLocaleString('tr-TR'), pctA, pctB: 100 - pctA };
    });
    return { polls };
});

app.post('/api/zarla/vote', async (req, rep) => {
    const { pollId, choice } = req.body || {};
    const col = choice === 'A' ? 'votes_a' : 'votes_b';
    const r = await pool.query(`UPDATE zarla_polls SET ${col} = ${col} + 1 WHERE id = $1 RETURNING votes_a, votes_b`, [pollId]);
    const va = r.rows[0].votes_a, vb = r.rows[0].votes_b, tot = va + vb;
    return { success: true, pctA: Math.round((va / tot) * 100), pctB: 100 - Math.round((va / tot) * 100), totalVotes: tot.toLocaleString('tr-TR') };
});

// ==========================================
// 4. FRONTEND ARAYÜZÜ (RESPONSİVE + MOBİL BAR + DRAWERS)
// ==========================================
function renderIndexHtml() {
    return `<!DOCTYPE html>
<html lang="tr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>OYVER PRO - Tahmin & Kolektif Zeka Terminali</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap" rel="stylesheet">
    <style>
        body { font-family: 'Inter', sans-serif; }
        .glass-panel { background: rgba(15, 23, 42, 0.85); backdrop-filter: blur(12px); -webkit-backdrop-filter: blur(12px); }
        .duel-card { transition: transform 0.2s ease, border-color 0.2s ease; }
        .duel-card:hover { transform: translateY(-2px); border-color: rgba(168, 85, 247, 0.4); }
        ::-webkit-scrollbar { width: 4px; }
        ::-webkit-scrollbar-track { background: #020617; }
        ::-webkit-scrollbar-thumb { background: #334155; border-radius: 2px; }
    </style>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen flex flex-col antialiased selection:bg-purple-600 selection:text-white">

    <div id="toast-container" class="fixed top-5 right-5 z-50 flex flex-col gap-2 pointer-events-none"></div>

    <!-- MASAÜSTÜ & TABLET HEADER -->
    <header class="sticky top-0 z-40 bg-slate-950/90 backdrop-blur-md border-b border-slate-800">
        <div class="container mx-auto px-4 h-16 flex items-center justify-between gap-4">
            <div class="flex items-center space-x-6">
                <a href="/" onclick="navigateToHome(event)" class="text-2xl font-black tracking-tight text-transparent bg-clip-text bg-gradient-to-r from-purple-400 via-pink-400 to-orange-400 flex items-center gap-1">
                    OYVER<span class="text-xs px-1.5 py-0.5 rounded bg-purple-500/20 text-purple-300 border border-purple-500/30">PRO</span>
                </a>
                <nav class="hidden sm:flex items-center space-x-2 text-xs font-bold">
                    <button id="nav-tab-zarla" class="px-3 py-1.5 rounded-xl text-slate-300 hover:text-white hover:bg-slate-900 transition flex items-center gap-1.5">
                        <i class="fas fa-dice text-pink-400"></i> ZARLA (Günün Nabzı)
                    </button>
                    <button id="nav-tab-portfolio" class="px-3 py-1.5 rounded-xl text-slate-300 hover:text-white hover:bg-slate-900 transition flex items-center gap-1.5">
                        <i class="fas fa-briefcase text-emerald-400"></i> Portföyüm
                    </button>
                </nav>
            </div>

            <div class="flex items-center space-x-3">
                <div id="user-badge" class="flex items-center bg-slate-900 border border-slate-800 rounded-2xl p-1 pr-3 space-x-2 cursor-pointer hover:border-slate-700 transition" onclick="toggleUserDropdown()">
                    <div class="flex items-center gap-1 bg-slate-950 px-2 py-1 rounded-xl border border-slate-800">
                        <i class="fas fa-fire text-orange-500 text-xs"></i>
                        <span id="user-streak" class="text-xs font-black text-orange-400">5g</span>
                    </div>
                    <div class="text-xs font-bold text-amber-300" id="user-balance">-- KOR</div>
                    <i class="fas fa-chevron-down text-[10px] text-slate-500"></i>
                </div>

                <!-- Masaüstü Açılır Menü -->
                <div id="user-dropdown" class="hidden absolute right-4 top-14 w-56 bg-slate-900 border border-slate-800 rounded-2xl shadow-2xl p-2 z-50 space-y-1">
                    <button onclick="openLeaderboard()" class="w-full text-left px-3 py-2 text-xs font-bold text-slate-200 hover:bg-slate-800 rounded-xl transition flex items-center gap-2">
                        <i class="fas fa-trophy text-amber-400"></i> Top 100 Kahin Ligi
                    </button>
                    <button onclick="openDrawer('about')" class="w-full text-left px-3 py-2 text-xs font-bold text-slate-200 hover:bg-slate-800 rounded-xl transition flex items-center gap-2">
                        <i class="fas fa-info-circle text-purple-400"></i> Biz Kimiz?
                    </button>
                    <button onclick="openDrawer('b2b')" class="w-full text-left px-3 py-2 text-xs font-bold text-slate-200 hover:bg-slate-800 rounded-xl transition flex items-center gap-2">
                        <i class="fas fa-chart-pie text-emerald-400"></i> Kurumsal Satış & B2B
                    </button>
                    <button onclick="openDrawer('contact')" class="w-full text-left px-3 py-2 text-xs font-bold text-slate-200 hover:bg-slate-800 rounded-xl transition flex items-center gap-2">
                        <i class="fas fa-envelope text-pink-400"></i> Bize Ulaşın
                    </button>
                    <div class="border-t border-slate-800 my-1"></div>
                    <button onclick="promptSwitchUser()" class="w-full text-left px-3 py-2 text-xs font-bold text-slate-400 hover:text-white hover:bg-slate-800 rounded-xl transition flex items-center gap-2">
                        <i class="fas fa-user-circle"></i> Kullanıcı Değiştir
                    </button>
                </div>
            </div>
        </div>
    </header>

    <!-- SAYFA GÖVDESİ (Mobilde alt barın arkasında kalmaması için pb-24 eklendi) -->
    <div class="flex-grow pb-24 sm:pb-12">
        <!-- 1. BÖLÜM: ANA PAZARLAR & HERO DÜELLO EKRANI -->
        <main id="section-markets">
            <section class="py-8 border-b border-slate-900 bg-gradient-to-b from-slate-900/30 to-transparent">
                <div class="container mx-auto px-4 max-w-5xl">
                    <div class="text-center mb-6">
                        <span class="inline-flex items-center gap-1.5 px-3 py-1 rounded-full bg-purple-500/10 border border-purple-500/20 text-xs font-bold text-purple-300 mb-2">
                            <span class="w-2 h-2 rounded-full bg-emerald-400 animate-ping"></span> Canlı Kamuoyu Düelloları
                        </span>
                        <h1 class="text-2xl sm:text-3xl font-black text-white tracking-tight">Yarının Nabzını Bugün Tutun.</h1>
                    </div>
                    <div id="hero-duels-container" class="grid grid-cols-1 sm:grid-cols-2 gap-4"></div>
                </div>
            </section>

            <!-- KATEGORİ FİLTRELERİ -->
            <section class="container mx-auto px-4 pt-6 pb-2 max-w-5xl">
                <div class="flex items-center space-x-2 overflow-x-auto pb-2">
                    <button class="cat-btn active bg-purple-600 text-white px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap" data-cat="ALL">Tümü</button>
                    <button class="cat-btn bg-slate-900 hover:bg-slate-800 text-slate-300 px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap" data-cat="EKONOMİ">Ekonomi</button>
                    <button class="cat-btn bg-slate-900 hover:bg-slate-800 text-slate-300 px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap" data-cat="FİNANS">Finans</button>
                    <button class="cat-btn bg-slate-900 hover:bg-slate-800 text-slate-300 px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap" data-cat="TEKNOLOJİ">Teknoloji</button>
                    <button class="cat-btn bg-slate-900 hover:bg-slate-800 text-slate-300 px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap" data-cat="KÜLTÜR & YAŞAM">Kültür & Yaşam</button>
                </div>
            </section>

            <!-- PAZAR KARTLARI -->
            <section class="container mx-auto px-4 py-4 max-w-5xl">
                <div id="market-grid" class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-5"></div>
            </section>
        </main>

        <!-- 2. BÖLÜM: PAZAR DETAY TERMİNALİ (/market/:slug) -->
        <main id="section-market-detail" class="hidden container mx-auto px-4 py-6 max-w-5xl">
            <button onclick="navigateToHome(event)" class="text-xs text-purple-400 hover:text-purple-300 font-bold flex items-center gap-1.5 mb-4">
                <i class="fas fa-arrow-left"></i> Tüm Pazarlara Dön
            </button>
            <div class="flex items-center justify-between gap-3 mb-2">
                <span id="dt-category" class="px-2.5 py-0.5 rounded text-[10px] font-black uppercase tracking-wider bg-purple-500/20 text-purple-300 border border-purple-500/30">KATEGORİ</span>
                <span class="text-xs text-slate-400"><i class="far fa-clock mr-1"></i><span id="dt-closing-date">--</span></span>
            </div>
            <h1 id="dt-title" class="text-xl sm:text-2xl font-black text-white mb-6 leading-snug">Pazar Başlığı</h1>

            <div class="flex border-b border-slate-800 mb-6 gap-6 text-xs font-bold">
                <button id="dt-tab-main" onclick="switchDetailTab('main')" class="pb-2.5 border-b-2 border-purple-500 text-white flex items-center gap-1.5">
                    <i class="fas fa-chart-line text-purple-400"></i> Grafik & İşlem
                </button>
                <button id="dt-tab-community" onclick="switchDetailTab('community')" class="pb-2.5 text-slate-400 hover:text-white flex items-center gap-1.5">
                    <i class="fas fa-comments text-pink-400"></i> Analizler
                </button>
                <button id="dt-tab-cohorts" onclick="switchDetailTab('cohorts')" class="pb-2.5 text-slate-400 hover:text-white flex items-center gap-1.5">
                    <i class="fas fa-layer-group text-amber-400"></i> Kohort Analitiği
                </button>
            </div>

            <!-- GRAFİK & TAHMİN KONSOLU -->
            <div id="dt-view-main" class="grid grid-cols-1 lg:grid-cols-3 gap-6">
                <div class="lg:col-span-2 space-y-6">
                    <div class="bg-slate-900 border border-slate-800 rounded-3xl p-5 shadow-xl">
                        <div class="flex items-center justify-between mb-4">
                            <div>
                                <div class="text-xs font-bold text-slate-400 uppercase">EVET Olasılık Eğrisi</div>
                                <div class="text-3xl font-black text-emerald-400 mt-0.5" id="dt-current-prob">--%</div>
                            </div>
                        </div>
                        <div class="h-60 w-full relative"><canvas id="marketChart"></canvas></div>
                    </div>
                    <div class="bg-slate-900/60 border border-slate-800 rounded-3xl p-5 space-y-2">
                        <h3 class="text-xs font-bold text-white flex items-center gap-1.5"><i class="fas fa-shield-alt text-purple-400"></i> Resmi Çözümleme Kaynağı</h3>
                        <p id="dt-desc" class="text-xs text-slate-400 leading-relaxed">Açıklama...</p>
                        <a id="dt-source-url" href="#" target="_blank" class="font-bold text-purple-400 hover:underline text-xs flex items-center gap-1">
                            <span id="dt-source-name">Kaynak</span> <i class="fas fa-external-link-alt text-[10px]"></i>
                        </a>
                    </div>
                </div>

                <div class="bg-slate-900 border border-slate-800 rounded-3xl p-5 shadow-xl space-y-4 h-fit">
                    <div class="flex justify-between items-center pb-2 border-b border-slate-800">
                        <span class="text-xs font-bold text-slate-400 uppercase">Tahmin Konsolu</span>
                        <span id="dt-user-balance" class="text-xs font-black text-amber-300">-- KOR</span>
                    </div>
                    <div class="grid grid-cols-2 gap-2">
                        <button id="dt-choice-yes" class="py-2.5 rounded-xl font-black text-xs uppercase border border-emerald-500 bg-emerald-600 text-white">EVET</button>
                        <button id="dt-choice-no" class="py-2.5 rounded-xl font-black text-xs uppercase border border-slate-800 bg-slate-950 text-slate-400">HAYIR</button>
                    </div>
                    <div>
                        <label class="block text-xs font-bold text-slate-400 mb-1">Kullanılacak Puan</label>
                        <input type="number" id="dt-input-amount" value="500" min="50" step="50" class="w-full bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-white font-bold text-sm focus:outline-none focus:border-purple-500">
                    </div>
                    <div class="p-3 bg-slate-950/80 rounded-xl border border-slate-800 space-y-1 text-xs">
                        <div class="flex justify-between text-slate-400"><span>Hedef Pay:</span><span id="dt-quote-shares" class="font-bold text-white">-- Pay</span></div>
                        <div class="flex justify-between text-slate-400"><span>Ortalama Fiyat:</span><span id="dt-quote-avg" class="font-bold text-slate-300">-- Puan</span></div>
                        <div class="flex justify-between pt-1 border-t border-slate-800 font-bold"><span class="text-slate-300">Sonuçlanınca:</span><span id="dt-quote-payout" class="font-black text-emerald-400">-- KOR</span></div>
                    </div>
                    <button id="dt-btn-predict" class="w-full py-3 rounded-xl bg-gradient-to-r from-purple-600 to-pink-600 hover:from-purple-500 hover:to-pink-500 text-white font-extrabold text-xs shadow-lg transition">
                        Tahmini Onayla (KOR)
                    </button>
                </div>
            </div>

            <!-- ANALİZLER SEKMESİ -->
            <div id="dt-view-community" class="hidden space-y-5 max-w-3xl">
                <div class="bg-slate-900 border border-slate-800 rounded-3xl p-5 space-y-3">
                    <textarea id="comm-input-content" rows="2" placeholder="Gerekçeli analizinizi paylaşın..." class="w-full bg-slate-950 border border-slate-800 rounded-xl p-3 text-xs text-white placeholder-slate-500 focus:outline-none focus:border-purple-500"></textarea>
                    <button id="btn-submit-comment" class="px-5 py-2 bg-purple-600 hover:bg-purple-500 rounded-xl text-xs font-bold text-white transition">Analizi Yayınla</button>
                </div>
                <div id="dt-comments-list" class="space-y-3"></div>
            </div>

            <!-- KOHORT SEKMESİ -->
            <div id="dt-view-cohorts" class="hidden space-y-5">
                <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
                    <div class="bg-slate-900 border border-slate-800 rounded-3xl p-5 space-y-3">
                        <h4 class="text-xs font-bold text-slate-300 uppercase">Yaş Dağılımı</h4>
                        <div id="cohort-age-list" class="space-y-2 text-xs"></div>
                    </div>
                    <div class="bg-slate-900 border border-slate-800 rounded-3xl p-5 space-y-3">
                        <h4 class="text-xs font-bold text-slate-300 uppercase">Eğitim Dağılımı</h4>
                        <div id="cohort-edu-list" class="space-y-2 text-xs"></div>
                    </div>
                </div>
            </div>
        </main>

        <!-- 3. BÖLÜM: ZARLA -->
        <main id="section-zarla" class="hidden container mx-auto px-4 py-8 max-w-2xl">
            <div class="text-center mb-6">
                <span class="px-3 py-1 rounded-full bg-pink-500/10 border border-pink-500/20 text-xs font-bold text-pink-300">0 Puan, 0 Risk, Saf Kamuoyu Nabzı</span>
                <h2 class="text-2xl font-black text-white mt-2">Günün Kritik Meseleleri</h2>
            </div>
            <div id="zarla-list" class="space-y-4"></div>
        </main>

        <!-- 4. BÖLÜM: PORTFÖYÜM -->
        <main id="section-portfolio" class="hidden container mx-auto px-4 py-6 max-w-3xl space-y-6">
            <div>
                <div class="flex items-center justify-between mb-3 pb-2 border-b border-slate-800">
                    <h2 class="text-lg font-black text-white">Açık Paylarım</h2>
                    <button onclick="loadPortfolio()" class="text-xs text-purple-400 hover:underline"><i class="fas fa-sync-alt mr-1"></i> Yenile</button>
                </div>
                <div id="portfolio-active-list" class="space-y-3"></div>
            </div>
            <div>
                <h2 class="text-lg font-black text-emerald-400 mb-3 pb-2 border-b border-slate-800">Sonuçlanan Tahminlerim</h2>
                <div id="portfolio-settled-list" class="space-y-3"></div>
            </div>
        </main>
    </div>

    <!-- MASAÜSTÜ & TABLET FOOTER -->
    <footer class="hidden sm:block bg-slate-950 border-t border-slate-900 py-10 mt-auto text-xs text-slate-400">
        <div class="container mx-auto px-4 max-w-5xl">
            <div class="grid grid-cols-1 md:grid-cols-4 gap-8 mb-8">
                <div class="space-y-2 md:col-span-2">
                    <a href="/" class="text-xl font-black tracking-tight text-white flex items-center gap-1">
                        OYVER<span class="text-[10px] px-1 py-0.5 rounded bg-purple-500/20 text-purple-300 border border-purple-500/30">PRO</span>
                    </a>
                    <p class="text-slate-400 text-xs leading-relaxed max-w-md">
                        Türkiye odaklı tescilli kolektif zeka ve tahmin terminali. Kamuoyu verisini şeffaf, ölçülebilir ve manipülasyonsuz modellerle buluşturur.
                    </p>
                </div>
                <div>
                    <h4 class="text-white font-bold mb-3 uppercase tracking-wider text-[11px]">Platform</h4>
                    <ul class="space-y-2">
                        <li><button onclick="openDrawer('about')" class="hover:text-purple-400 transition">Biz Kimiz?</button></li>
                        <li><button onclick="openLeaderboard()" class="hover:text-purple-400 transition">Top 100 Kahin</button></li>
                        <li><button onclick="switchTab('zarla')" class="hover:text-purple-400 transition">ZARLA Nabız</button></li>
                    </ul>
                </div>
                <div>
                    <h4 class="text-white font-bold mb-3 uppercase tracking-wider text-[11px]">Kurumsal</h4>
                    <ul class="space-y-2">
                        <li><button onclick="openDrawer('b2b')" class="hover:text-purple-400 transition text-emerald-400">Kurumsal Satış (B2B)</button></li>
                        <li><button onclick="openDrawer('contact')" class="hover:text-purple-400 transition">Bize Ulaşın</button></li>
                        <li><button onclick="openDrawer('rules')" class="hover:text-purple-400 transition">Nasıl Çalışır & Kurallar</button></li>
                    </ul>
                </div>
            </div>
            <div class="pt-6 border-t border-slate-900 flex flex-col sm:flex-row items-center justify-between gap-4 text-[11px] text-slate-500">
                <p>© 2026 OYVER PRO. Tüm hakları saklıdır.</p>
                <p class="max-w-xl text-center sm:text-right">
                    <strong>Yasal Uyarı:</strong> OYVER PRO kapalı devre sanal itibar puanı (KOR) ile çalışır. Bahis veya kumar değildir; puanların nakit karşılığı ve çekimi yoktur.
                </p>
            </div>
        </div>
    </footer>

    <!-- MOBİL İÇİN ALT SABİT NAVİGASYON BARI (640px Altı) -->
    <nav class="sm:hidden fixed bottom-0 left-0 right-0 z-40 bg-slate-950/95 backdrop-blur-lg border-t border-slate-800 px-2 py-2 flex items-center justify-around shadow-2xl">
        <button onclick="navigateToHome(event)" class="mob-nav-btn flex flex-col items-center gap-1 text-slate-400 hover:text-purple-400 transition" id="mob-btn-home">
            <i class="fas fa-compass text-base"></i>
            <span class="text-[10px] font-bold">Pazarlar</span>
        </button>
        <button onclick="switchTab('zarla')" class="mob-nav-btn flex flex-col items-center gap-1 text-slate-400 hover:text-pink-400 transition" id="mob-btn-zarla">
            <i class="fas fa-dice text-base"></i>
            <span class="text-[10px] font-bold">ZARLA</span>
        </button>
        <button onclick="switchTab('portfolio')" class="mob-nav-btn flex flex-col items-center gap-1 text-slate-400 hover:text-emerald-400 transition" id="mob-btn-portfolio">
            <i class="fas fa-briefcase text-base"></i>
            <span class="text-[10px] font-bold">Portföy</span>
        </button>
        <button onclick="openMobileMenuDrawer()" class="mob-nav-btn flex flex-col items-center gap-1 text-slate-400 hover:text-purple-400 transition">
            <i class="fas fa-bars text-base"></i>
            <span class="text-[10px] font-bold">Menü</span>
        </button>
    </nav>

    <!-- MOBİL "DAHA FAZLA / MENÜ" ALT ÇEKMECESİ -->
    <div id="mobile-menu-drawer-backdrop" class="fixed inset-0 bg-slate-950/80 z-50 backdrop-blur-sm hidden" onclick="closeMobileMenuDrawer()"></div>
    <div id="mobile-menu-drawer" class="fixed bottom-0 left-0 right-0 z-50 bg-slate-900 border-t border-slate-800 rounded-t-3xl p-5 shadow-2xl transform translate-y-full transition-transform duration-300 max-h-[85vh] overflow-y-auto space-y-4">
        <div class="w-10 h-1 bg-slate-700 rounded-full mx-auto mb-2"></div>
        <div class="flex justify-between items-center pb-3 border-b border-slate-800">
            <span class="text-sm font-black text-white">Menü & Kurumsal</span>
            <button onclick="closeMobileMenuDrawer()" class="text-slate-400"><i class="fas fa-times"></i></button>
        </div>
        <div class="grid grid-cols-2 gap-2 text-xs font-bold">
            <button onclick="openLeaderboard(); closeMobileMenuDrawer();" class="p-3 bg-slate-950 border border-slate-800 rounded-2xl flex flex-col items-center gap-2 hover:border-amber-500 transition">
                <i class="fas fa-trophy text-amber-400 text-lg"></i><span>Top 100 Kahin</span>
            </button>
            <button onclick="openDrawer('b2b'); closeMobileMenuDrawer();" class="p-3 bg-slate-950 border border-slate-800 rounded-2xl flex flex-col items-center gap-2 hover:border-emerald-500 transition">
                <i class="fas fa-chart-pie text-emerald-400 text-lg"></i><span>Kurumsal B2B</span>
            </button>
            <button onclick="openDrawer('about'); closeMobileMenuDrawer();" class="p-3 bg-slate-950 border border-slate-800 rounded-2xl flex flex-col items-center gap-2 hover:border-purple-500 transition">
                <i class="fas fa-info-circle text-purple-400 text-lg"></i><span>Biz Kimiz?</span>
            </button>
            <button onclick="openDrawer('contact'); closeMobileMenuDrawer();" class="p-3 bg-slate-950 border border-slate-800 rounded-2xl flex flex-col items-center gap-2 hover:border-pink-500 transition">
                <i class="fas fa-envelope text-pink-400 text-lg"></i><span>Bize Ulaşın</span>
            </button>
        </div>
        <button onclick="promptSwitchUser(); closeMobileMenuDrawer();" class="w-full py-3 bg-slate-950 border border-slate-800 rounded-2xl text-xs font-bold text-slate-300 flex items-center justify-center gap-2">
            <i class="fas fa-user-circle"></i> Kullanıcı Değiştir
        </button>
    </div>

    <!-- KURUMSAL ÇEKMECE / MODALLAR (BİZ KİMİZ, B2B, İLETİŞİM, KURALLAR) -->
    <div id="info-modal" class="fixed inset-0 bg-slate-950/85 z-50 backdrop-blur-md hidden flex items-center justify-center p-4">
        <div class="bg-slate-900 border border-slate-800 w-full max-w-lg rounded-3xl overflow-hidden shadow-2xl p-6 max-h-[85vh] overflow-y-auto space-y-4">
            <div class="flex justify-between items-center pb-3 border-b border-slate-800">
                <h3 class="text-base font-black text-white" id="info-modal-title">Başlık</h3>
                <button onclick="closeInfoModal()" class="text-slate-400 hover:text-white"><i class="fas fa-times"></i></button>
            </div>
            <div id="info-modal-content" class="text-xs text-slate-300 leading-relaxed space-y-3"></div>
        </div>
    </div>

    <!-- TOP 100 MODAL -->
    <div id="leaderboard-modal" class="fixed inset-0 bg-slate-950/80 z-50 backdrop-blur-md hidden flex items-center justify-center p-4">
        <div class="bg-slate-900 border border-slate-800 w-full max-w-xl rounded-3xl overflow-hidden shadow-2xl p-5 max-h-[85vh] flex flex-col">
            <div class="flex justify-between items-center pb-3 border-b border-slate-800 mb-3">
                <div class="flex items-center gap-2"><i class="fas fa-trophy text-amber-400"></i><h3 class="text-base font-black text-white">Top 100 Kahin Ligi</h3></div>
                <button onclick="closeLeaderboard()" class="text-slate-400"><i class="fas fa-times"></i></button>
            </div>
            <div class="overflow-y-auto flex-grow divide-y divide-slate-800/80" id="leaderboard-list"></div>
        </div>
    </div>

    <script>
        let markets = [];
        let heroDuels = [];
        let currentBalance = 14500;
        let currentUsername = 'LeisanB';
        let currentUserRole = 'USER';
        let activeCategory = 'ALL';
        let activeDetailMarket = null;
        let activeDetailChoice = 'YES';
        let chartInstance = null;

        function escapeHtml(str) {
            if (!str) return '';
            return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#039;');
        }

        function showToast(msg, type = 'success') {
            const container = document.getElementById('toast-container');
            const toast = document.createElement('div');
            const bg = type === 'success' ? 'bg-emerald-950 border-emerald-500/50 text-emerald-200' : 'bg-rose-950 border-rose-500/50 text-rose-200';
            toast.className = 'flex items-center gap-2 px-4 py-3 rounded-2xl border shadow-xl text-xs font-bold transition-all duration-300 opacity-0 transform -translate-y-2 pointer-events-auto ' + bg;
            toast.innerHTML = '<i class="fas fa-info-circle"></i><span>' + escapeHtml(msg) + '</span>';
            container.appendChild(toast);
            setTimeout(() => toast.classList.remove('opacity-0', '-translate-y-2'), 10);
            setTimeout(() => { toast.classList.add('opacity-0', '-translate-y-2'); setTimeout(() => toast.remove(), 300); }, 3500);
        }

        function toggleUserDropdown() {
            document.getElementById('user-dropdown').classList.toggle('hidden');
        }

        window.onclick = function(e) {
            if (!e.target.closest('#user-badge') && !e.target.closest('#user-dropdown')) {
                document.getElementById('user-dropdown')?.classList.add('hidden');
            }
        };

        const secMarkets = document.getElementById('section-markets');
        const secMarketDetail = document.getElementById('section-market-detail');
        const secZarla = document.getElementById('section-zarla');
        const secPortfolio = document.getElementById('section-portfolio');

        function switchTab(target) {
            [secMarkets, secMarketDetail, secZarla, secPortfolio].forEach(s => s.classList.add('hidden'));
            if (target === 'markets') secMarkets.classList.remove('hidden');
            if (target === 'zarla') { secZarla.classList.remove('hidden'); loadZarla(); }
            if (target === 'portfolio') { secPortfolio.classList.remove('hidden'); loadPortfolio(); }
        }

        document.getElementById('nav-tab-zarla').onclick = () => switchTab('zarla');
        document.getElementById('nav-tab-portfolio').onclick = () => switchTab('portfolio');

        window.navigateToHome = function(e) {
            if (e) e.preventDefault();
            history.pushState({}, '', '/');
            switchTab('markets');
        };

        // HERO DÜELLOLAR
        async function loadHeroDuels() {
            const res = await fetch('/api/duels').then(r => r.json());
            heroDuels = res.duels || [];
            const container = document.getElementById('hero-duels-container');
            container.innerHTML = '';

            heroDuels.forEach(d => {
                const votedChoice = localStorage.getItem('voted_duel_' + d.id);
                const card = document.createElement('div');
                card.className = 'duel-card bg-slate-900 border border-slate-800 rounded-3xl p-4 shadow-xl flex flex-col justify-between';

                let actionsHtml = '';
                if (votedChoice) {
                    actionsHtml = 
                        '<div class="space-y-1.5 mt-3">' +
                            '<div class="w-full bg-slate-950 rounded-full h-2.5 overflow-hidden flex p-0.5 border border-slate-800">' +
                                '<div class="bg-purple-500 h-full rounded-full transition-all duration-700" style="width:' + d.pctA + '%"></div>' +
                                '<div class="bg-pink-500 h-full rounded-full transition-all duration-700" style="width:' + d.pctB + '%"></div>' +
                            '</div>' +
                            '<div class="flex justify-between items-center text-[11px] font-black">' +
                                '<span class="' + (votedChoice === 'A' ? 'text-purple-300' : 'text-slate-400') + '">' + escapeHtml(d.option_a_name) + ' %' + d.pctA + '</span>' +
                                '<span class="' + (votedChoice === 'B' ? 'text-pink-300' : 'text-slate-400') + '">' + escapeHtml(d.option_b_name) + ' %' + d.pctB + '</span>' +
                            '</div>' +
                        '</div>';
                } else {
                    actionsHtml = 
                        '<div class="grid grid-cols-2 gap-2 mt-3">' +
                            '<button onclick="voteDuel(\\'' + d.id + '\\', \\'A\\')" class="py-2 px-2 bg-slate-950 hover:bg-purple-600/30 border border-slate-800 hover:border-purple-500 rounded-xl text-[11px] font-bold text-slate-200 transition">' +
                                'Oy Ver: ' + escapeHtml(d.option_a_name) +
                            '</button>' +
                            '<button onclick="voteDuel(\\'' + d.id + '\\', \\'B\\')" class="py-2 px-2 bg-slate-950 hover:bg-pink-600/30 border border-slate-800 hover:border-pink-500 rounded-xl text-[11px] font-bold text-slate-200 transition">' +
                                'Oy Ver: ' + escapeHtml(d.option_b_name) +
                            '</button>' +
                        '</div>';
                }

                card.innerHTML = 
                    '<div>' +
                        '<div class="flex justify-between items-center mb-2">' +
                            '<span class="px-2 py-0.5 rounded text-[9px] font-black uppercase tracking-wider bg-purple-500/20 text-purple-300 border border-purple-500/30">' + escapeHtml(d.category) + '</span>' +
                            '<span class="text-[10px] text-slate-400 font-semibold">' + d.totalVotes + ' Oy</span>' +
                        '</div>' +
                        '<h3 class="text-xs sm:text-sm font-extrabold text-white leading-snug mb-3">' + escapeHtml(d.title) + '</h3>' +
                        '<div class="flex items-center justify-between px-3 py-1.5 bg-slate-950/70 rounded-2xl border border-slate-800/80 text-xs font-bold">' +
                            '<span>' + escapeHtml(d.option_a_name) + '</span>' +
                            '<span class="text-slate-500 text-[10px] italic">VS</span>' +
                            '<span>' + escapeHtml(d.option_b_name) + '</span>' +
                        '</div>' +
                    '</div>' + actionsHtml;

                container.appendChild(card);
            });
        }

        window.voteDuel = async function(id, choice) {
            try {
                const res = await fetch('/api/duels/' + id + '/vote', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ choice })
                }).then(r => r.json());

                if (res.error) throw new Error(res.error);
                localStorage.setItem('voted_duel_' + id, choice);
                showToast('🎉 Oyunuz kaydedildi!');
                loadHeroDuels();
            } catch(e) {
                showToast(e.message, 'error');
            }
        };

        // KURUMSAL ÇEKMECE / MODAL YÖNETİMİ
        window.openDrawer = function(type) {
            const modal = document.getElementById('info-modal');
            const title = document.getElementById('info-modal-title');
            const content = document.getElementById('info-modal-content');
            modal.classList.remove('hidden');

            if (type === 'about') {
                title.textContent = 'Biz Kimiz?';
                content.innerHTML = 
                    '<p><strong>OYVER PRO</strong>, Türkiye odaklı kolektif zeka, tahmin ve kamuoyu öngörü terminalidir.</p>' +
                    '<p>Geleneksel anket manipülasyonlarını engellemek amacıyla Automated Market Maker (AMM) matematiksel modelini kapalı devre <strong>KOR puanı</strong> ile birleştirir.</p>' +
                    '<p class="p-3 bg-slate-950 rounded-2xl border border-slate-800"><strong>Temel İlke:</strong> OYVER PRO bir bahis, iddaa veya şans oyunu değildir. Puanlar gerçek para ile satın alınamaz ve nakde dönüştürülemez. Amaç, halkın gerçek beklentisini ölçülebilir bir liyakat zeminine oturtmaktır.</p>';
            } else if (type === 'b2b') {
                title.textContent = 'Kurumsal Satış & B2B Veri Çözümleri';
                content.innerHTML = 
                    '<p>Araştırma şirketleri, markalar ve finans kurumları için filtrelenmiş demografik kamuoyu içgörüleri sunuyoruz.</p>' +
                    '<div class="space-y-2 mt-3">' +
                        '<input type="text" id="b2b-company" placeholder="Kurum / Şirket Adı" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white">' +
                        '<input type="email" id="b2b-email" placeholder="Kurumsal E-posta Adresi" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white">' +
                        '<textarea id="b2b-msg" rows="3" placeholder="İhtiyacınız (Özel Anket Açma, API Erişimi, Kohort Analizi)..." class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white"></textarea>' +
                        '<button onclick="submitContactForm(\\'B2B_SALES\\')" class="w-full py-2.5 bg-emerald-600 hover:bg-emerald-500 rounded-xl font-bold text-white transition">Kurumsal İletişim Talebi Gönder</button>' +
                    '</div>';
            } else if (type === 'contact') {
                title.textContent = 'Bize Ulaşın';
                content.innerHTML = 
                    '<p>Teknik destek, iş birliği veya pazar önerileriniz için ekibimize doğrudan mesaj gönderebilirsiniz.</p>' +
                    '<div class="space-y-2 mt-3">' +
                        '<input type="text" id="cnt-name" placeholder="Adınız Soyadınız" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white">' +
                        '<input type="email" id="cnt-email" placeholder="E-posta Adresiniz" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white">' +
                        '<textarea id="cnt-msg" rows="3" placeholder="Mesajınız..." class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white"></textarea>' +
                        '<button onclick="submitContactForm(\\'GENERAL\\')" class="w-full py-2.5 bg-purple-600 hover:bg-purple-500 rounded-xl font-bold text-white transition">Mesajı İlet</button>' +
                    '</div>';
            } else if (type === 'rules') {
                title.textContent = 'Nasıl Çalışır & Kurallar';
                content.innerHTML = 
                    '<p>1. <strong>Olasılık Eğrisi:</strong> Pazarlardaki yüzdeler kullanıcıların KOR puanlarıyla yaptıkları tercihler neticesinde AMM motoru tarafından otomatik belirlenir.</p>' +
                    '<p>2. <strong>Erken Satış (Sell):</strong> Pazar kapanmadan önce pozisyonunuzu anlık fiyattan satıp puanınızı serbest bırakabilirsiniz.</p>' +
                    '<p>3. <strong>Çözümleme:</strong> Pazar tarihi geldiğinde Resmi Gazete, TCMB veya TÜİK bülteniyle sonuçlandırılır ve kazananlara 1 Pay = 1 KOR tasfiye ödenir.</p>';
            }
        };

        window.closeInfoModal = () => document.getElementById('info-modal').classList.add('hidden');

        window.submitContactForm = async function(type) {
            const isB2B = type === 'B2B_SALES';
            const name = isB2B ? document.getElementById('b2b-company').value : document.getElementById('cnt-name').value;
            const email = isB2B ? document.getElementById('b2b-email').value : document.getElementById('cnt-email').value;
            const message = isB2B ? document.getElementById('b2b-msg').value : document.getElementById('cnt-msg').value;

            try {
                const res = await fetch('/api/contact', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ type, name, email, company: isB2B ? name : null, message })
                }).then(r => r.json());

                if (res.error) throw new Error(res.error);
                showToast(res.message);
                closeInfoModal();
            } catch(e) {
                showToast(e.message, 'error');
            }
        };

        // MOBİL MENÜ ÇEKMECESİ
        window.openMobileMenuDrawer = function() {
            document.getElementById('mobile-menu-drawer-backdrop').classList.remove('hidden');
            document.getElementById('mobile-menu-drawer').classList.remove('translate-y-full');
        };
        window.closeMobileMenuDrawer = function() {
            document.getElementById('mobile-menu-drawer').classList.add('translate-y-full');
            document.getElementById('mobile-menu-drawer-backdrop').classList.add('hidden');
        };

        // PAZARLARI LİSTELEME
        function renderMarkets() {
            const container = document.getElementById('market-grid');
            container.innerHTML = '';

            const filtered = activeCategory === 'ALL' ? markets : markets.filter(m => m.category === activeCategory);
            filtered.forEach(m => {
                const card = document.createElement('article');
                card.className = 'bg-slate-900 border border-slate-800 rounded-3xl p-4 shadow-lg flex flex-col justify-between hover:border-slate-700 transition';

                card.innerHTML = 
                    '<div>' +
                        '<div class="flex items-center justify-between mb-2">' +
                            '<span class="px-2 py-0.5 rounded text-[9px] font-black uppercase tracking-wider bg-purple-500/20 text-purple-300 border border-purple-500/30">' + escapeHtml(m.category) + '</span>' +
                            '<span class="text-[10px] text-slate-400"><i class="far fa-clock mr-1"></i>' + escapeHtml(m.closing_date) + '</span>' +
                        '</div>' +
                        '<h3 class="text-xs sm:text-sm font-bold text-white mb-2 leading-snug cursor-pointer hover:text-purple-300 transition" onclick="openMarketDetail(\\'' + m.slug + '\\')">' + escapeHtml(m.question) + '</h3>' +
                        '<div class="text-[10px] text-slate-400 mb-3 flex items-center gap-1"><i class="fas fa-landmark text-slate-500"></i><span>' + escapeHtml(m.source_name) + '</span></div>' +
                        '<div class="space-y-1.5 mb-4 cursor-pointer" onclick="openMarketDetail(\\'' + m.slug + '\\')">' +
                            '<div class="flex justify-between text-[11px] text-slate-400 font-semibold">' +
                                '<span>Havuz: ' + m.poolTotal + ' Puan</span><span class="text-emerald-400 font-bold">EVET: %' + m.probYes + '</span>' +
                            '</div>' +
                            '<div class="w-full bg-slate-950 rounded-full h-2 overflow-hidden flex border border-slate-800">' +
                                '<div class="bg-gradient-to-r from-emerald-500 to-teal-400 h-full transition-all duration-700" style="width:' + m.probYes + '%"></div>' +
                            '</div>' +
                        '</div>' +
                    '</div>' +
                    '<button onclick="openMarketDetail(\\'' + m.slug + '\\')" class="w-full py-2 bg-slate-950 hover:bg-slate-800 border border-slate-800 rounded-xl text-xs font-bold text-purple-300 hover:text-white transition flex justify-center items-center gap-1.5">' +
                        '<i class="fas fa-chart-line text-[10px]"></i> Terminal & Tahmin' +
                    '</button>';

                container.appendChild(card);
            });
        }

        window.openMarketDetail = async function(slug) {
            history.pushState({}, '', '/market/' + slug);
            [secMarkets, secZarla, secPortfolio].forEach(s => s.classList.add('hidden'));
            secMarketDetail.classList.remove('hidden');

            try {
                const res = await fetch('/api/markets/' + slug).then(r => r.json());
                activeDetailMarket = res.market;

                document.getElementById('dt-title').textContent = res.market.question;
                document.getElementById('dt-category').textContent = res.market.category;
                document.getElementById('dt-closing-date').textContent = res.market.closing_date;
                document.getElementById('dt-desc').textContent = res.market.description || 'Resmi açıklama dikkate alınacaktır.';
                document.getElementById('dt-source-name').textContent = res.market.source_name;
                document.getElementById('dt-source-url').href = res.market.source_url || '#';
                document.getElementById('dt-current-prob').textContent = '%' + res.market.probYes;
                document.getElementById('dt-user-balance').textContent = currentBalance.toLocaleString('tr-TR') + ' KOR';

                switchDetailTab('main');
                updateDetailChoiceBtns();
                fetchDetailQuote();
                renderChart(res.history);
            } catch (e) {
                showToast(e.message, 'error');
                navigateToHome();
            }
        };

        window.switchDetailTab = function(tab) {
            ['main', 'community', 'cohorts'].forEach(t => {
                document.getElementById('dt-tab-' + t).className = 'pb-2.5 text-slate-400 hover:text-white flex items-center gap-1.5';
                document.getElementById('dt-view-' + t).classList.add('hidden');
            });
            document.getElementById('dt-tab-' + tab).className = 'pb-2.5 border-b-2 border-purple-500 text-white flex items-center gap-1.5';
            document.getElementById('dt-view-' + tab).classList.remove('hidden');

            if (tab === 'community') loadComments();
            if (tab === 'cohorts') loadCohorts();
        };

        function renderChart(history) {
            const ctx = document.getElementById('marketChart').getContext('2d');
            if (chartInstance) chartInstance.destroy();

            chartInstance = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: history.map(h => h.time_label),
                    datasets: [{
                        label: 'EVET (%)',
                        data: history.map(h => h.prob_yes),
                        borderColor: '#10b981',
                        backgroundColor: 'rgba(16, 185, 129, 0.1)',
                        borderWidth: 2.5,
                        fill: true,
                        tension: 0.35,
                        pointRadius: 3
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        y: { min: 0, max: 100, grid: { color: 'rgba(51, 65, 85, 0.2)' }, ticks: { color: '#94a3b8', font: { size: 10 } } },
                        x: { grid: { display: false }, ticks: { color: '#94a3b8', font: { size: 10 } } }
                    },
                    plugins: { legend: { display: false } }
                }
            });
        }

        function updateDetailChoiceBtns() {
            if (!activeDetailMarket) return;
            const bY = document.getElementById('dt-choice-yes');
            const bN = document.getElementById('dt-choice-no');

            bY.textContent = 'EVET (%' + activeDetailMarket.probYes + ')';
            bN.textContent = 'HAYIR (%' + activeDetailMarket.probNo + ')';

            if (activeDetailChoice === 'YES') {
                bY.className = 'py-2.5 rounded-xl font-black text-xs uppercase border border-emerald-500 bg-emerald-600 text-white';
                bN.className = 'py-2.5 rounded-xl font-black text-xs uppercase border border-slate-800 bg-slate-950 text-slate-400';
            } else {
                bN.className = 'py-2.5 rounded-xl font-black text-xs uppercase border border-rose-500 bg-rose-600 text-white';
                bY.className = 'py-2.5 rounded-xl font-black text-xs uppercase border border-slate-800 bg-slate-950 text-slate-400';
            }
        }

        async function fetchDetailQuote() {
            if (!activeDetailMarket) return;
            const amt = parseFloat(document.getElementById('dt-input-amount').value) || 0;
            if (amt <= 0) return;

            try {
                const res = await fetch('/api/trade/quote', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ marketId: activeDetailMarket.id, outcome: activeDetailChoice, amountKor: amt, action: 'BUY' })
                }).then(r => r.json());

                if (res.sharesOut) {
                    document.getElementById('dt-quote-shares').textContent = res.sharesOut + ' Pay';
                    document.getElementById('dt-quote-avg').textContent = res.avgPrice + ' Puan (Etki: %' + res.priceImpact + ')';
                    document.getElementById('dt-quote-payout').textContent = '+' + res.targetPayout;
                }
            } catch(e) {}
        }

        document.getElementById('dt-choice-yes').onclick = () => { activeDetailChoice = 'YES'; updateDetailChoiceBtns(); fetchDetailQuote(); };
        document.getElementById('dt-choice-no').onclick = () => { activeDetailChoice = 'NO'; updateDetailChoiceBtns(); fetchDetailQuote(); };
        document.getElementById('dt-input-amount').oninput = fetchDetailQuote;

        document.getElementById('dt-btn-predict').onclick = async () => {
            const amt = document.getElementById('dt-input-amount').value;
            const btn = document.getElementById('dt-btn-predict');
            btn.disabled = true;

            try {
                const res = await fetch('/api/trade/predict', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ marketId: activeDetailMarket.id, outcome: activeDetailChoice, amountKor: amt })
                }).then(r => r.json());

                if (res.error) throw new Error(res.error);
                currentBalance = res.balanceKor;
                document.getElementById('user-balance').textContent = currentBalance.toLocaleString('tr-TR') + ' KOR';
                document.getElementById('dt-user-balance').textContent = currentBalance.toLocaleString('tr-TR') + ' KOR';
                showToast('✅ Tercihiniz işlendi! Alınan Pay: ' + res.sharesOut);
                openMarketDetail(activeDetailMarket.slug);
            } catch (e) {
                showToast(e.message, 'error');
            } finally {
                btn.disabled = false;
            }
        };

        // YORUMLAR & KOHORT
        async function loadComments() {
            if (!activeDetailMarket) return;
            const res = await fetch('/api/markets/' + activeDetailMarket.slug + '/comments').then(r => r.json());
            const list = document.getElementById('dt-comments-list');
            list.innerHTML = '';
            if (res.comments.length === 0) {
                list.innerHTML = '<div class="text-center py-4 text-slate-500 text-xs">Henüz analiz paylaşılmadı.</div>';
                return;
            }
            res.comments.forEach(c => {
                const card = document.createElement('div');
                card.className = 'bg-slate-900 border border-slate-800 rounded-2xl p-3 space-y-1.5 text-xs';
                card.innerHTML = 
                    '<div class="flex justify-between items-center">' +
                        '<div class="flex items-center gap-2"><strong class="text-white">' + escapeHtml(c.username) + '</strong><span class="text-[9px] text-purple-400 font-bold">[' + escapeHtml(c.tier) + ']</span></div>' +
                        '<span class="text-[10px] text-slate-500">' + escapeHtml(c.time_formatted) + '</span>' +
                    '</div>' +
                    '<p class="text-slate-300">' + escapeHtml(c.content) + '</p>';
                list.appendChild(card);
            });
        }

        document.getElementById('btn-submit-comment').onclick = async () => {
            const content = document.getElementById('comm-input-content').value;
            if (!content || content.trim().length < 5) return showToast('Lütfen en az 5 karakter yazın.', 'error');
            try {
                const res = await fetch('/api/markets/' + activeDetailMarket.slug + '/comments', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ content })
                }).then(r => r.json());
                if (res.error) throw new Error(res.error);
                document.getElementById('comm-input-content').value = '';
                showToast('✅ Analiziniz yayınlandı!');
                loadComments();
            } catch(e) {
                showToast(e.message, 'error');
            }
        };

        async function loadCohorts() {
            if (!activeDetailMarket) return;
            const res = await fetch('/api/markets/' + activeDetailMarket.slug + '/cohorts').then(r => r.json());
            const ageContainer = document.getElementById('cohort-age-list');
            const eduContainer = document.getElementById('cohort-edu-list');
            ageContainer.innerHTML = '';
            eduContainer.innerHTML = '';

            res.ageCohorts.forEach(c => {
                const row = document.createElement('div');
                row.className = 'flex justify-between p-2 bg-slate-950 rounded-xl border border-slate-800 font-bold';
                row.innerHTML = '<span>' + escapeHtml(c.age_group) + '</span><span class="' + (c.outcome === 'YES' ? 'text-emerald-400' : 'text-rose-400') + '">' + c.outcome + ' (' + c.vote_count + ')</span>';
                ageContainer.appendChild(row);
            });
            res.educationCohorts.forEach(c => {
                const row = document.createElement('div');
                row.className = 'flex justify-between p-2 bg-slate-950 rounded-xl border border-slate-800 font-bold';
                row.innerHTML = '<span>' + escapeHtml(c.education_level) + '</span><span class="' + (c.outcome === 'YES' ? 'text-emerald-400' : 'text-rose-400') + '">' + c.outcome + ' (' + c.vote_count + ')</span>';
                eduContainer.appendChild(row);
            });
        }

        // PORTFÖY
        async function loadPortfolio() {
            const res = await fetch('/api/portfolio').then(r => r.json());
            const actList = document.getElementById('portfolio-active-list');
            const setList = document.getElementById('portfolio-settled-list');
            actList.innerHTML = '';
            setList.innerHTML = '';

            if (res.active.length === 0) {
                actList.innerHTML = '<div class="text-center py-4 text-slate-500 text-xs">Açık tahmininiz yok.</div>';
            } else {
                res.active.forEach(item => {
                    const card = document.createElement('div');
                    card.className = 'bg-slate-900 border border-slate-800 rounded-2xl p-4 flex items-center justify-between gap-4 text-xs';
                    card.innerHTML = 
                        '<div class="space-y-1">' +
                            '<h4 class="font-bold text-white cursor-pointer hover:text-purple-300" onclick="openMarketDetail(\\'' + item.slug + '\\')">' + escapeHtml(item.question) + '</h4>' +
                            '<div class="text-slate-400 text-[11px]">Tercih: <strong class="' + (item.outcome === 'YES' ? 'text-emerald-400' : 'text-rose-400') + '">' + item.outcome + '</strong> | Pay: ' + item.shares + '</div>' +
                        '</div>' +
                        '<div class="flex items-center gap-3">' +
                            '<div class="text-right"><div class="text-[10px] text-slate-500">Değer</div><div class="font-black text-amber-300">' + item.currentSellValue + ' KOR</div></div>' +
                            '<button onclick="sellPosition(\\'' + item.marketId + '\\', \\'' + item.outcome + '\\', \\'' + item.shares + '\\')" class="bg-rose-600/20 hover:bg-rose-600 text-rose-300 hover:text-white px-3 py-1.5 rounded-xl font-bold transition">Sat</button>' +
                        '</div>';
                    actList.appendChild(card);
                });
            }

            if (res.settled.length === 0) {
                setList.innerHTML = '<div class="text-center py-4 text-slate-500 text-xs">Sonuçlanan tahmininiz yok.</div>';
            } else {
                res.settled.forEach(item => {
                    const card = document.createElement('div');
                    card.className = 'bg-slate-900/60 border border-slate-800 rounded-2xl p-3 flex items-center justify-between text-xs';
                    card.innerHTML = '<div>' + escapeHtml(item.question) + '</div><div class="font-bold text-emerald-400">+' + item.payout + ' KOR</div>';
                    setList.appendChild(card);
                });
            }
        }

        window.sellPosition = async function(marketId, outcome, shares) {
            if (!confirm(shares + ' payınızı satmak istiyor musunuz?')) return;
            try {
                const res = await fetch('/api/trade/sell', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ marketId, outcome, sharesToSell: shares })
                }).then(r => r.json());

                if (res.error) throw new Error(res.error);
                currentBalance = res.newBalance;
                document.getElementById('user-balance').textContent = currentBalance.toLocaleString('tr-TR') + ' KOR';
                showToast('💰 Pozisyon satıldı: +' + res.payoutKor + ' KOR');
                loadPortfolio();
            } catch (e) {
                showToast(e.message, 'error');
            }
        };

        // ZARLA
        async function loadZarla() {
            const res = await fetch('/api/zarla').then(r => r.json());
            const list = document.getElementById('zarla-list');
            list.innerHTML = '';

            res.polls.forEach(p => {
                const card = document.createElement('div');
                card.className = 'bg-slate-900 border border-slate-800 rounded-3xl p-4 shadow-lg space-y-3';
                card.innerHTML = 
                    '<div class="flex justify-between items-center text-xs">' +
                        '<span class="px-2 py-0.5 rounded text-[9px] font-black uppercase bg-purple-500/20 text-purple-300">' + escapeHtml(p.category) + '</span>' +
                        '<span class="text-slate-500">' + p.totalVotes + ' Oy</span>' +
                    '</div>' +
                    '<h3 class="text-xs sm:text-sm font-bold text-white">' + escapeHtml(p.question) + '</h3>' +
                    '<div class="w-full bg-slate-950 rounded-full h-2 overflow-hidden flex border border-slate-800">' +
                        '<div class="bg-purple-500 h-full" style="width:' + p.pctA + '%"></div>' +
                        '<div class="bg-pink-500 h-full" style="width:' + p.pctB + '%"></div>' +
                    '</div>' +
                    '<div class="grid grid-cols-2 gap-2 text-xs font-bold">' +
                        '<button onclick="voteZarla(\\'' + p.id + '\\', \\'A\\')" class="py-2 bg-slate-950 hover:bg-slate-800 border border-slate-800 rounded-xl text-slate-200 transition">' + escapeHtml(p.option_a) + ' (%' + p.pctA + ')</button>' +
                        '<button onclick="voteZarla(\\'' + p.id + '\\', \\'B\\')" class="py-2 bg-slate-950 hover:bg-slate-800 border border-slate-800 rounded-xl text-slate-200 transition">' + escapeHtml(p.option_b) + ' (%' + p.pctB + ')</button>' +
                    '</div>';
                list.appendChild(card);
            });
        }

        window.voteZarla = async function(pollId, choice) {
            try {
                const res = await fetch('/api/zarla/vote', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ pollId, choice })
                }).then(r => r.json());
                showToast('🗳️ Oyunuz kaydedildi!');
                loadZarla();
            } catch (e) {
                showToast(e.message, 'error');
            }
        };

        // LİDERLİK TABLOSU
        window.openLeaderboard = async function() {
            const d = await fetch('/api/leaderboard').then(r => r.json());
            const list = document.getElementById('leaderboard-list');
            list.innerHTML = '';

            d.top100.forEach(u => {
                const item = document.createElement('div');
                item.className = 'py-3 flex items-center justify-between text-xs px-2';
                item.innerHTML = 
                    '<div class="flex items-center gap-2.5">' +
                        '<span class="w-6 h-6 rounded-full bg-slate-800 flex items-center justify-center font-black text-[10px]">#' + u.rank + '</span>' +
                        '<div><strong class="text-white">' + escapeHtml(u.name) + '</strong><span class="text-[9px] text-purple-400 font-bold ml-1.5">[' + escapeHtml(u.tier) + ']</span></div>' +
                    '</div>' +
                    '<div class="text-right font-black text-purple-400">' + u.frsScore.toLocaleString('tr-TR') + ' FRS</div>';
                list.appendChild(item);
            });
            document.getElementById('leaderboard-modal').classList.remove('hidden');
        };
        window.closeLeaderboard = () => document.getElementById('leaderboard-modal').classList.add('hidden');

        window.promptSwitchUser = async () => {
            const name = prompt('Giriş yapılacak kullanıcı adını girin (Örn: LeisanB, Ahmet_Kahin):');
            if (!name) return;
            const res = await fetch('/api/auth/login-mock', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ username: name })
            }).then(r => r.json());
            if (res.success) location.reload();
        };

        document.querySelectorAll('.cat-btn').forEach(btn => {
            btn.onclick = (e) => {
                document.querySelectorAll('.cat-btn').forEach(b => b.className = 'cat-btn bg-slate-900 hover:bg-slate-800 text-slate-300 px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap');
                e.target.className = 'cat-btn active bg-purple-600 text-white px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap';
                activeCategory = e.target.getAttribute('data-cat');
                renderMarkets();
            };
        });

        // WEBSOCKET
        function connectWebSocket() {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const ws = new WebSocket(protocol + '//' + window.location.host + '/ws');
            ws.onmessage = (event) => {
                try {
                    const msg = JSON.parse(event.data);
                    if (msg.type === 'MARKET_UPDATE') {
                        const idx = markets.findIndex(m => m.id === msg.marketId);
                        if (idx !== -1) {
                            markets[idx].probYes = msg.probYes;
                            markets[idx].probNo = msg.probNo;
                            markets[idx].poolTotal = msg.poolTotal;
                        }
                        renderMarkets();
                    }
                    if (msg.type === 'DUEL_VOTE') loadHeroDuels();
                } catch(e) {}
            };
            ws.onclose = () => setTimeout(connectWebSocket, 2500);
        }

        async function init() {
            try {
                const me = await fetch('/api/me').then(r => r.json());
                currentBalance = Math.round(parseFloat(me.balance_kor || 14500));
                currentUsername = me.username || 'LeisanB';
                currentUserRole = me.role || 'USER';

                document.getElementById('user-balance').textContent = currentBalance.toLocaleString('tr-TR') + ' KOR';
                document.getElementById('user-streak').textContent = (me.streak || 1) + 'g';

                loadHeroDuels();

                const res = await fetch('/api/markets').then(r => r.json());
                markets = res.markets || [];
                renderMarkets();

                const path = window.location.pathname;
                if (path.startsWith('/market/')) {
                    const slug = path.split('/')[2];
                    openMarketDetail(slug);
                }
            } catch (e) {
                console.error(e);
            }
        }

        init();
        connectWebSocket();
    </script>
</body>
</html>`;
}

// ROTALAR
app.get('/', async (req, reply) => reply.type('text/html').send(renderIndexHtml()));
app.get('/market/:slug', async (req, reply) => reply.type('text/html').send(renderIndexHtml()));

// ==========================================
// 5. BAŞLATICI
// ==========================================
await initDatabase();
const port = Number(process.env.PORT) || 3000;
app.listen({ port, host: '0.0.0.0' }, (err, address) => {
    if (err) { console.error(err); process.exit(1); }
    console.log(`[OYVER PRO] v1.1-PROD AKTİF: ${address}`);
});
