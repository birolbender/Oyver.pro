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
        console.info('[DATABASE] Şema, P2P Düello ve Kademeli Profilleme tabloları doğrulanıyor...');
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
                tier VARCHAR(32) NOT NULL DEFAULT 'Araştırmacı',
                
                -- Kademeli Profilleme & Psikometrik Alanlar
                kvkk_accepted BOOLEAN NOT NULL DEFAULT FALSE,
                birth_year INT,
                city VARCHAR(32),
                industry VARCHAR(64),
                fav_team VARCHAR(64),
                mobility_type VARCHAR(64),
                risk_choice VARCHAR(8),
                music_taste VARCHAR(64),
                coffee_habit VARCHAR(64),
                contrarian_choice VARCHAR(8),
                personality_archetype VARCHAR(64) DEFAULT 'Belirlenmedi',
                profile_step INT NOT NULL DEFAULT 1,
                
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS markets (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                slug VARCHAR(255) UNIQUE NOT NULL,
                category VARCHAR(64) NOT NULL DEFAULT 'EKONOMİ',
                sub_category VARCHAR(64),
                question TEXT NOT NULL,
                description TEXT DEFAULT 'Bu oylama resmi bülten verisiyle sonuçlandırılacaktır.',
                source_url TEXT,
                source_name VARCHAR(128),
                status VARCHAR(32) NOT NULL DEFAULT 'TRADING',
                resolved_outcome VARCHAR(8),
                closing_date VARCHAR(64) NOT NULL DEFAULT '31 Aralık 2026',
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

            -- P2P BİREBİR DÜELLO TABLOLARI
            CREATE TABLE IF NOT EXISTS p2p_duels (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                challenger_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                challenged_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                stake_kor NUMERIC(24,6) NOT NULL DEFAULT 5000.000000,
                status VARCHAR(32) NOT NULL DEFAULT 'PENDING', -- 'PENDING', 'CHALLENGER_DONE', 'COMPLETED', 'EXPIRED'
                challenger_score INT DEFAULT 0,
                challenged_score INT DEFAULT 0,
                challenger_final_guess NUMERIC(10,2),
                challenged_final_guess NUMERIC(10,2),
                winner_id UUID REFERENCES users(id),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS hero_duels (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                title TEXT NOT NULL,
                category VARCHAR(64) NOT NULL,
                option_a_name TEXT NOT NULL,
                option_b_name TEXT NOT NULL,
                votes_a INT NOT NULL DEFAULT 1,
                votes_b INT NOT NULL DEFAULT 1,
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
                type VARCHAR(32) NOT NULL DEFAULT 'GENERAL',
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

        // Sütun Migrasyonları
        await client.query(`
            ALTER TABLE users ADD COLUMN IF NOT EXISTS kvkk_accepted BOOLEAN NOT NULL DEFAULT FALSE;
            ALTER TABLE users ADD COLUMN IF NOT EXISTS fav_team VARCHAR(64);
            ALTER TABLE users ADD COLUMN IF NOT EXISTS mobility_type VARCHAR(64);
            ALTER TABLE users ADD COLUMN IF NOT EXISTS risk_choice VARCHAR(8);
            ALTER TABLE users ADD COLUMN IF NOT EXISTS music_taste VARCHAR(64);
            ALTER TABLE users ADD COLUMN IF NOT EXISTS coffee_habit VARCHAR(64);
            ALTER TABLE users ADD COLUMN IF NOT EXISTS contrarian_choice VARCHAR(8);
            ALTER TABLE users ADD COLUMN IF NOT EXISTS personality_archetype VARCHAR(64) DEFAULT 'Belirlenmedi';
            ALTER TABLE users ADD COLUMN IF NOT EXISTS profile_step INT NOT NULL DEFAULT 1;
        `);

        for (const code of ['1000', '3000', '4000', '5000']) {
            await client.query(`INSERT INTO accounts (code) VALUES ($1) ON CONFLICT DO NOTHING`, [code]);
        }

        // Demo Kullanıcılar
        const checkUsers = await client.query(`SELECT count(*) FROM users`);
        if (parseInt(checkUsers.rows[0].count, 10) === 0) {
            await client.query(`
                INSERT INTO users (id, email, username, password_hash, role, balance_kor, streak, tier, personality_archetype) VALUES
                ('11111111-1111-1111-1111-111111111111', 'demo@oyver.pro', 'LeisanB', 'OAUTH_MOCK', 'USER', 14500, 5, 'Doçent', 'Stratejist'),
                ('22222222-2222-2222-2222-222222222222', 'ahmet@oyver.pro', 'Ahmet_Analist', 'OAUTH_MOCK', 'USER', 420000, 14, 'Ordinaryüs', 'Öncü'),
                ('33333333-3333-3333-3333-333333333333', 'ece@oyver.pro', 'Ece_Hoca', 'OAUTH_MOCK', 'USER', 315000, 9, 'Profesör', 'Sağlamcı')
                ON CONFLICT (email) DO NOTHING;
            `);
        }

        // Hero Düello Başlangıcı
        const checkDuels = await client.query(`SELECT count(*) FROM hero_duels`);
        if (parseInt(checkDuels.rows[0].count, 10) === 0) {
            await client.query(`
                INSERT INTO hero_duels (title, category, option_a_name, option_b_name, votes_a, votes_b, is_active) VALUES
                ('2028 Seçim Tercihiniz Hangisi Olur?', 'SİYASET DÜELLOSU', 'Cumhur İttifakı', 'Muhalefet Bloğu', 54, 46, true),
                ('Hafta Sonu Süper Lig Derbisini Kim Kazanır?', 'SPOR DÜELLOSU', 'Galatasaray', 'Fenerbahçe', 52, 48, true)
            `);
        }

        // Pazarlar
        const checkMarkets = await client.query(`SELECT count(*) FROM markets`);
        if (parseInt(checkMarkets.rows[0].count, 10) === 0) {
            const richMarkets = [
                { slug: 'asgari-ucret-2027', cat: 'EKONOMİ', sub: 'Maaş & Gelir', q: '2027 Yılı Net Asgari Ücreti 35.000 TL Üzerinde Açıklanır mı?', srcName: 'Çalışma Bakanlığı', closing: '31 Aralık 2026', yesR: 8000, noR: 12000 },
                { slug: 'tcmb-faiz-2026', cat: 'EKONOMİ', sub: 'Merkez Bankası', q: 'TCMB Politika Faizini 2026 Yıl Sonuna Kadar %30 veya Altına İndirir mi?', srcName: 'TCMB PPK', closing: '24 Aralık 2026', yesR: 11000, noR: 9000 },
                { slug: 'bist-100-2026', cat: 'BORSA & FİNANS', sub: 'BIST 100', q: 'BIST 100 Endeksi 2026 Son Çeyreğini 12.000 Puan Üzerinde Kapatır mı?', srcName: 'Borsa İstanbul', closing: '31 Aralık 2026', yesR: 10000, noR: 10000 },
                { slug: 'super-lig-90-puan', cat: 'SPOR', sub: 'Futbol', q: '2026-2027 Süper Lig Şampiyonu 90 Puan Barajını Aşar mı?', srcName: 'TFF Resmi Puan Durumu', closing: '30 Mayıs 2027', yesR: 9000, noR: 11000 },
                { slug: 'efes-final-four', cat: 'SPOR', sub: 'Basketbol', q: 'Anadolu Efes 2026-2027 EuroLeague Sezonunda Final Four\'a Kalır mı?', srcName: 'EuroLeague Basketball', closing: '15 Mayıs 2027', yesR: 12000, noR: 8000 },
                { slug: 'f1-ferrari-sampiyon', cat: 'SPOR', sub: 'Formula 1', q: '2026 Formula 1 Takımlar Şampiyonluğunu Ferrari Kazanır mı?', srcName: 'FIA Resmi Sonuçları', closing: '28 Kasım 2026', yesR: 13000, noR: 7000 },
                { slug: 'turksat-6a-ticari', cat: 'TEKNOLOJİ', sub: 'Uzay & Uydu', q: 'TÜRKSAT 6A Uydusu 2026 Yılında Tam Kapasite Ticari Hizmete Başlar mı?', srcName: 'Ulaştırma Bakanlığı', closing: '15 Kasım 2026', yesR: 6000, noR: 14000 },
                { slug: 'ist-moda-haftasi', cat: 'MODA & STİL', sub: 'Trendler', q: 'İstanbul Moda Haftası 2027 Resmi Takviminde 20+ Yabancı Tasarımcı Yer Alır mı?', srcName: 'İHKİB Basın Bülteni', closing: '20 Mart 2027', yesR: 10000, noR: 10000 }
            ];

            for (const item of richMarkets) {
                const mRes = await client.query(`
                    INSERT INTO markets (slug, category, sub_category, question, source_name, closing_date)
                    VALUES ($1, $2, $3, $4, $5, $6) RETURNING id
                `, [item.slug, item.cat, item.sub, item.q, item.srcName, item.closing]);

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

app.addHook('onRequest', async (req) => {
    let token = null;
    const cookie = req.headers.cookie;
    if (cookie) {
        const m = cookie.split(';').find(c => c.trim().startsWith('oyver_token='));
        if (m) token = m.split('=')[1].trim();
    }
    if (token) {
        const r = await pool.query(`SELECT id, role, username, tier, balance_kor FROM users WHERE id = (SELECT user_id FROM sessions WHERE token = $1)`, [token]);
        if (r.rows.length > 0) {
            req.userId = r.rows[0].id;
            req.userRole = r.rows[0].role;
            req.username = r.rows[0].username;
            req.userTier = r.rows[0].tier;
        }
    }
    if (!req.userId) {
        req.userId = '11111111-1111-1111-1111-111111111111';
        req.userRole = 'USER';
        req.username = 'LeisanB';
        req.userTier = 'Doçent';
    }
});

app.get('/ws', { websocket: true }, (connection) => {
    const ws = connection.socket;
    wsClients.add(ws);
    ws.on('close', () => wsClients.delete(ws));
});

app.get('/health', async () => ({ status: 'UP', version: 'v1.3-P2P-ARENA', timestamp: new Date().toISOString() }));

app.get('/api/me', async (req) => {
    const r = await pool.query(`SELECT * FROM users WHERE id = $1`, [req.userId]);
    return r.rows[0] || {};
});

// ==========================================
// KADEMELİ PROFİLLEME VE KİŞİLİK ANALİZİ APILERI
// ==========================================
app.post('/api/profile/step1', async (req, rep) => {
    const { kvkkAccepted, birthYear, city, industry } = req.body || {};
    if (!kvkkAccepted || !birthYear || !city || !industry) {
        return rep.status(400).send({ error: 'Lütfen zorunlu alanları doldurun ve KVKK metnini onaylayın.' });
    }

    const updated = await pool.query(`
        UPDATE users 
        SET kvkk_accepted = true, birth_year = $1, city = $2, industry = $3, 
            profile_step = GREATEST(profile_step, 2), balance_kor = balance_kor + 1000
        WHERE id = $4
        RETURNING balance_kor, profile_step
    `, [birthYear, city, industry, req.userId]);

    return { success: true, balanceKor: Math.round(parseFloat(updated.rows[0].balance_kor)), step: updated.rows[0].profile_step };
});

app.post('/api/profile/step2', async (req, rep) => {
    const { favTeam, mobilityType, riskChoice } = req.body || {};
    if (!favTeam || !mobilityType || !riskChoice) {
        return rep.status(400).send({ error: 'Lütfen tüm soruları yanıtlayın.' });
    }

    const updated = await pool.query(`
        UPDATE users 
        SET fav_team = $1, mobility_type = $2, risk_choice = $3,
            profile_step = GREATEST(profile_step, 3), balance_kor = balance_kor + 1500
        WHERE id = $4
        RETURNING balance_kor, profile_step
    `, [favTeam, mobilityType, riskChoice, req.userId]);

    return { success: true, balanceKor: Math.round(parseFloat(updated.rows[0].balance_kor)), step: updated.rows[0].profile_step };
});

app.post('/api/profile/step3', async (req, rep) => {
    const { musicTaste, coffeeHabit, contrarianChoice } = req.body || {};
    if (!musicTaste || !coffeeHabit || !contrarianChoice) {
        return rep.status(400).send({ error: 'Lütfen tüm soruları yanıtlayın.' });
    }

    const u = (await pool.query(`SELECT risk_choice FROM users WHERE id = $1`, [req.userId])).rows[0];
    const risk = u?.risk_choice || 'A'; // A: Garantici, B: Risk Arayan
    const contrarian = contrarianChoice; // A: Konsensüs, B: Aykırı

    let archetype = 'Sağlamcı (Dengeli Tüketici)';
    if (risk === 'A' && contrarian === 'B') archetype = 'Stratejist (Value Investor)';
    else if (risk === 'B' && contrarian === 'B') archetype = 'Öncü (Venture Capitalist)';
    else if (risk === 'B' && contrarian === 'A') archetype = 'Trend Takipçisi (Momentum)';

    const updated = await pool.query(`
        UPDATE users 
        SET music_taste = $1, coffee_habit = $2, contrarian_choice = $3,
            personality_archetype = $4, profile_step = 4, balance_kor = balance_kor + 1000
        WHERE id = $5
        RETURNING balance_kor, personality_archetype
    `, [musicTaste, coffeeHabit, contrarianChoice, archetype, req.userId]);

    return { success: true, balanceKor: Math.round(parseFloat(updated.rows[0].balance_kor)), archetype: updated.rows[0].personality_archetype };
});

// ==========================================
// P2P BİREBİR ANALİST DÜELLO APILERI
// ==========================================
const P2P_QUESTIONS = [
    // 1. Etap (10 Soru - Market Instinct & Fermi Estimation)
    { id: 1, stage: 1, q: "Piyasa değeri bakımından hangisi diğerinden daha büyüktür?", a: "Spotify (SPOT)", b: "Türk Hava Yolları (THYAO)", correct: "A" },
    { id: 2, stage: 1, q: "İstanbul'da günde ortalama kaç milyon kişi toplu taşıma kullanır?", a: "3 - 5 Milyon", b: "7 - 9 Milyon", correct: "B" },
    { id: 3, stage: 1, q: "2026 yılı itibarıyla Türkiye'nin yıllık ihracat hedefi hangisine daha yakındır?", a: "270 Milyar $", b: "390 Milyar $", correct: "A" },
    { id: 4, stage: 1, q: "BIST 100 işlem hacminde tarihsel olarak hangi sektör genelde liderdir?", a: "Bankacılık & Finans", b: "Madencilik", correct: "A" },
    { id: 5, stage: 1, q: "TCMB brüt döviz rezervleri hangi bandın üzerindedir?", a: "140 Milyar $ Üstü", b: "90 Milyar $ Altı", correct: "A" },
    { id: 6, stage: 1, q: "Global akıllı telefon pazarında teslimat payında hangisi öndedir?", a: "Apple", b: "Samsung", correct: "B" },
    { id: 7, stage: 1, q: "Hangisi Türkiye'nin en büyük organize sanayi bölgesidir?", a: "Bursa OSB", b: "Gaziantep OSB", correct: "B" },
    { id: 8, stage: 1, q: "Türkiye'nin nüfus ortanca yaşı hangisine daha yakındır?", a: "34 Yaş", b: "41 Yaş", correct: "A" },
    { id: 9, stage: 1, q: "Petrol fiyatlarında referans alınan Brent Petrol hangi bölgeden çıkarılır?", a: "Kuzey Denizi", b: "Basra Körfezi", correct: "A" },
    { id: 10, stage: 1, q: "Ethereum blokzincirinin konsensüs mekanizması nedir?", a: "Proof of Stake (PoS)", b: "Proof of Work (PoW)", correct: "A" },

    // 2. Etap (5 Soru - Logic Dilemma)
    { id: 11, stage: 2, q: "Bir kutuda 3 kırmızı, 2 mavi top var. Geri koymadan çekilen 2 topun aynı renk olma olasılığı %50'den büyük müdür?", a: "Hayır (%40)", b: "Evet (%60)", correct: "A" },
    { id: 12, stage: 2, q: "Bir hisse %50 düşüp ardından %50 yükselirse ilk maliyetine ulaşır mı?", a: "Ulaşır", b: "Ulaşamaz (%25 zarardadır)", correct: "B" },
    { id: 13, stage: 2, q: "İki zar atıldığında toplamın 7 gelme olasılığı 8 gelme olasılığından yüksek midir?", a: "Evet (6/36 vs 5/36)", b: "Hayır, eşittir", correct: "A" },
    { id: 14, stage: 2, q: "Piyasa faizleri yükseldiğinde mevcut tahvillerin piyasa fiyatı ne yönde hareket eder?", a: "Düşer", b: "Yükselir", correct: "A" },
    { id: 15, stage: 2, q: "Bir test %99 güvenilirdir. Toplumda hastalığın görülme sıklığı 1/10.000 ise, pozitif çıkan birinin gerçekten hasta olma ihtimali %50'den büyük müdür?", a: "Büyüktür", b: "Küçüktür (Bayes Yanılgısı)", correct: "B" },

    // Büyük Final (1 Soru - Golden Shot)
    { id: 16, stage: 3, q: "TÜİK resmi verilerine göre Türkiye'deki ortalama hanehalkı büyüklüğü kaçtır? (Ondalık yazın, örn: 3.14)", actualValue: 3.14 }
];

app.get('/api/p2p/questions', async () => {
    return { questions: P2P_QUESTIONS };
});

app.post('/api/p2p/challenge', async (req, rep) => {
    const { targetUsername } = req.body || {};
    const tUser = (await pool.query(`SELECT id, balance_kor FROM users WHERE username ILIKE $1`, [targetUsername])).rows[0];
    if (!tUser) return rep.status(404).send({ error: 'Meydan okunacak kullanıcı bulunamadı.' });
    if (tUser.id === req.userId) return rep.status(400).send({ error: 'Kendinize meydan okuyamazsınız.' });

    try {
        const result = await runInTransaction(async (c) => {
            const ur = await c.query(`UPDATE users SET balance_kor = balance_kor - 5000 WHERE id = $1 AND balance_kor >= 5000 RETURNING balance_kor`, [req.userId]);
            if (ur.rows.length === 0) throw new Error('Meydan okumak için en az 5.000 KOR teminatınız olmalıdır.');

            const duelRes = await c.query(`
                INSERT INTO p2p_duels (challenger_id, challenged_id, stake_kor, status)
                VALUES ($1, $2, 5000, 'PENDING')
                RETURNING id
            `, [req.userId, tUser.id]);

            return { duelId: duelRes.rows[0].id, newBalance: MoneyMath.roundDown(ur.rows[0].balance_kor, 0).toNumber() };
        });

        return rep.send({ success: true, ...result });
    } catch(e) {
        return rep.status(400).send({ error: e.message });
    }
});

app.post('/api/p2p/submit-turn', async (req, rep) => {
    const { duelId, answers, finalGuess } = req.body || {};
    const duel = (await pool.query(`SELECT * FROM p2p_duels WHERE id = $1`, [duelId])).rows[0];
    if (!duel) return rep.status(404).send({ error: 'Düello bulunamadı.' });

    const isChallenger = duel.challenger_id === req.userId;
    const isChallenged = duel.challenged_id === req.userId;
    if (!isChallenger && !isChallenged) return rep.status(403).send({ error: 'Bu düelloda taraf değilsiniz.' });

    // Puan hesaplama
    let score = 0;
    for (let i = 0; i < 15; i++) {
        const q = P2P_QUESTIONS[i];
        if (answers[q.id] === q.correct) score++;
    }

    try {
        if (isChallenger) {
            await pool.query(`
                UPDATE p2p_duels 
                SET challenger_score = $1, challenger_final_guess = $2, status = 'CHALLENGER_DONE'
                WHERE id = $3
            `, [score, parseFloat(finalGuess) || 0, duelId]);

            return rep.send({ success: true, message: 'İlk turunuz tamamlandı! Rakibiniz bekleniyor.', score });
        } else {
            // Rakip oynadı, maç sonuçlandırılıyor
            const result = await runInTransaction(async (c) => {
                const ur = await c.query(`UPDATE users SET balance_kor = balance_kor - 5000 WHERE id = $1 AND balance_kor >= 5000 RETURNING balance_kor`, [req.userId]);
                if (ur.rows.length === 0) throw new Error('Düelloyu kabul etmek için 5.000 KOR bakiyeniz olmalıdır.');

                const chScore = duel.challenger_score;
                const cdScore = score;
                let winnerId = null;

                if (chScore > cdScore) winnerId = duel.challenger_id;
                else if (cdScore > chScore) winnerId = duel.challenged_id;
                else {
                    // Beraberlikte Final Tahmini (Golden Shot) Resmi Veriye Yakınlık
                    const target = 3.14;
                    const diffA = Math.abs(parseFloat(duel.challenger_final_guess) - target);
                    const diffB = Math.abs(parseFloat(finalGuess) - target);
                    winnerId = diffA <= diffB ? duel.challenger_id : duel.challenged_id;
                }

                // Kazanan tarafa 9.600 KOR aktarılır (400 KOR sistem yakım/enflasyon payı kesilir)
                await c.query(`UPDATE users SET balance_kor = balance_kor + 9600 WHERE id = $1`, [winnerId]);
                await c.query(`
                    UPDATE p2p_duels 
                    SET challenged_score = $1, challenged_final_guess = $2, winner_id = $3, status = 'COMPLETED'
                    WHERE id = $4
                `, [score, parseFloat(finalGuess) || 0, winnerId, duelId]);

                return { winnerId, challengerScore: chScore, challengedScore: cdScore };
            });

            return rep.send({ success: true, ...result });
        }
    } catch(e) {
        return rep.status(400).send({ error: e.message });
    }
});

// MEVCUT PAZARLAR & QUOTE & AL-SAT
app.get('/api/markets', async () => {
    const r = await pool.query(`SELECT m.*, a.yes_reserve, a.no_reserve FROM markets m JOIN amm_state a ON m.id = a.market_id ORDER BY m.created_at ASC`);
    return {
        markets: r.rows.map(m => {
            const y = new Decimal(m.yes_reserve), n = new Decimal(m.no_reserve);
            const probYes = Math.round(n.div(y.plus(n)).mul(100).toNumber());
            return { ...m, probYes, probNo: 100 - probYes, poolTotal: Math.round(y.plus(n).toNumber()).toLocaleString('tr-TR') };
        })
    };
});

app.get('/api/markets/:slug', async (req, rep) => {
    const r = await pool.query(`SELECT m.*, a.yes_reserve, a.no_reserve FROM markets m JOIN amm_state a ON m.id = a.market_id WHERE m.slug = $1`, [req.params.slug]);
    if (r.rows.length === 0) return rep.status(404).send({ error: 'Pazar bulunamadı.' });
    const m = r.rows[0];
    const y = new Decimal(m.yes_reserve), n = new Decimal(m.no_reserve);
    const probYes = Math.round(n.div(y.plus(n)).mul(100).toNumber());
    const hr = await pool.query(`SELECT prob_yes, pool_total, to_char(created_at, 'DD Mon HH24:MI') as time_label FROM market_price_history WHERE market_id = $1 ORDER BY created_at ASC`, [m.id]);
    return {
        market: { ...m, probYes, probNo: 100 - probYes, poolTotal: Math.round(y.plus(n).toNumber()).toLocaleString('tr-TR') },
        history: hr.rows
    };
});

app.post('/api/trade/quote', async (req, rep) => {
    const { marketId, outcome, amountKor, action, sharesIn } = req.body || {};
    const mr = await pool.query(`SELECT yes_reserve, no_reserve FROM amm_state WHERE market_id = $1`, [marketId]);
    if (mr.rows.length === 0) return rep.status(404).send({ error: 'Pazar bulunamadı' });

    const isYes = outcome === 'YES';
    const amm = { yesReserve: new Decimal(mr.rows[0].yes_reserve), noReserve: new Decimal(mr.rows[0].no_reserve) };

    if (action === 'SELL') {
        const calc = AMMEngine.calculateSell(isYes ? amm : { yesReserve: amm.noReserve, noReserve: amm.yesReserve }, new Decimal(sharesIn || 0), new Decimal(0.02));
        return { action: 'SELL', netPayout: Math.round(calc.netPayout.toNumber()).toLocaleString('tr-TR') + ' KOR', avgPrice: calc.avgPrice, priceImpact: calc.priceImpact };
    } else {
        const calc = AMMEngine.calculateBuy(isYes ? amm : { yesReserve: amm.noReserve, noReserve: amm.yesReserve }, new Decimal(amountKor || 0), new Decimal(0.02));
        return { action: 'BUY', sharesOut: calc.sharesOut.toFixed(2), avgPrice: calc.avgPrice, priceImpact: calc.priceImpact, targetPayout: Math.round(calc.sharesOut.toNumber()).toLocaleString('tr-TR') + ' KOR' };
    }
});

app.post('/api/trade/predict', async (req, rep) => {
    const { marketId, outcome, amountKor } = req.body || {};
    const amt = new Decimal(amountKor || 0);
    try {
        const result = await runInTransaction(async (c) => {
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

            await c.query(`
                INSERT INTO positions (market_id, user_id, outcome, shares, total_invested)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (user_id, market_id, outcome) DO UPDATE 
                SET shares = positions.shares + $4, total_invested = positions.total_invested + $5, updated_at = NOW()
            `, [marketId, req.userId, outcome, calc.sharesOut.toFixed(6), amt.toFixed(6)]);

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
    try {
        const result = await runInTransaction(async (c) => {
            const pr = await c.query(`SELECT shares FROM positions WHERE market_id = $1 AND user_id = $2 AND outcome = $3 FOR UPDATE`, [marketId, req.userId, outcome]);
            if (pr.rows.length === 0 || new Decimal(pr.rows[0].shares).lt(sIn)) throw new Error('Yetersiz pay.');

            const mr = await c.query(`SELECT yes_reserve, no_reserve FROM amm_state WHERE market_id = $1 FOR UPDATE`, [marketId]);
            const isYes = outcome === 'YES';
            const amm = { yesReserve: new Decimal(mr.rows[0].yes_reserve), noReserve: new Decimal(mr.rows[0].no_reserve) };
            const calc = AMMEngine.calculateSell(isYes ? amm : { yesReserve: amm.noReserve, noReserve: amm.yesReserve }, sIn, new Decimal(0.02));

            const ny = isYes ? calc.newYesReserve : calc.newNoReserve;
            const nn = isYes ? calc.newNoReserve : calc.newYesReserve;
            await c.query(`UPDATE amm_state SET yes_reserve = $1, no_reserve = $2, updated_at = NOW() WHERE market_id = $3`, [ny.toFixed(12), nn.toFixed(12), marketId]);

            let rem = new Decimal(pr.rows[0].shares).minus(sIn);
            if (rem.lt(0.001)) rem = new Decimal(0);

            await c.query(`UPDATE positions SET shares = $1, realized_pnl = realized_pnl + $2, updated_at = NOW() WHERE market_id = $3 AND user_id = $4 AND outcome = $5`, [rem.toFixed(6), calc.netPayout.toFixed(6), marketId, req.userId, outcome]);
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
        SELECT p.*, m.question, m.slug, m.category, a.yes_reserve, a.no_reserve 
        FROM positions p JOIN markets m ON p.market_id = m.id JOIN amm_state a ON m.id = a.market_id
        WHERE p.user_id = $1 AND (p.shares > 0.001 OR p.is_settled = true) ORDER BY p.updated_at DESC
    `, [req.userId]);

    const active = [], settled = [];
    r.rows.forEach(row => {
        if (row.is_settled) {
            settled.push({ id: row.id, question: row.question, outcome: row.outcome, payout: Math.round(parseFloat(row.settlement_payout)), won: row.settlement_payout > 0 });
        } else if (parseFloat(row.shares) > 0.001) {
            const isYes = row.outcome === 'YES';
            const calc = AMMEngine.calculateSell(isYes ? { yesReserve: new Decimal(row.yes_reserve), noReserve: new Decimal(row.no_reserve) } : { yesReserve: new Decimal(row.no_reserve), noReserve: new Decimal(row.yes_reserve) }, new Decimal(row.shares), new Decimal(0.02));
            active.push({ id: row.id, marketId: row.market_id, question: row.question, slug: row.slug, outcome: row.outcome, shares: parseFloat(row.shares).toFixed(2), currentSellValue: Math.round(calc.netPayout.toNumber()) });
        }
    });
    return { active, settled };
});

// LİDERLİK LİGİ
app.get('/api/leaderboard', async () => {
    const usersRes = await pool.query(`SELECT id, username, streak, tier, balance_kor, personality_archetype FROM users`);
    const posRes = await pool.query(`SELECT p.* FROM positions p WHERE p.is_settled = true`);
    const leaderboard = usersRes.rows.map(u => {
        const uPositions = posRes.rows.filter(p => p.user_id === u.id);
        let wonCount = 0, netPnl = 0;
        for (const p of uPositions) {
            if (p.settlement_payout > 0) wonCount++;
            netPnl += parseFloat(p.realized_pnl);
        }
        const frsScore = Math.max(0, Math.round((netPnl * 0.35) + (Math.log10(uPositions.length + 1) * 1000 * 0.15) + (u.streak * 50 * 0.10)));
        return { id: u.id, name: u.username, tier: u.tier, archetype: u.personality_archetype || 'Stratejist', frsScore, winRate: uPositions.length > 0 ? Math.round((wonCount / uPositions.length) * 100) : 0 };
    });
    leaderboard.sort((a, b) => b.frsScore - a.frsScore);
    return { top100: leaderboard.slice(0, 100).map((item, idx) => ({ rank: idx + 1, ...item })) };
});

// ZARLA & DÜELLOLAR
app.get('/api/duels', async () => {
    const r = await pool.query(`SELECT * FROM hero_duels WHERE is_active = true ORDER BY created_at ASC`);
    return { duels: r.rows.map(d => ({ ...d, pctA: Math.round((d.votes_a / (d.votes_a + d.votes_b)) * 100), pctB: 100 - Math.round((d.votes_a / (d.votes_a + d.votes_b)) * 100), totalVotes: (d.votes_a + d.votes_b).toLocaleString('tr-TR') })) };
});

app.post('/api/duels/:id/vote', async (req, rep) => {
    const col = req.body?.choice === 'A' ? 'votes_a' : 'votes_b';
    const r = await pool.query(`UPDATE hero_duels SET ${col} = ${col} + 1 WHERE id = $1 RETURNING votes_a, votes_b`, [req.params.id]);
    const tot = r.rows[0].votes_a + r.rows[0].votes_b;
    return { success: true, pctA: Math.round((r.rows[0].votes_a / tot) * 100), pctB: 100 - Math.round((r.rows[0].votes_a / tot) * 100), totalVotes: tot.toLocaleString('tr-TR') };
});

app.get('/api/zarla', async () => {
    const r = await pool.query(`SELECT * FROM zarla_polls ORDER BY created_at ASC`);
    return { polls: r.rows.map(p => ({ ...p, pctA: Math.round((p.votes_a / (p.votes_a + p.votes_b || 1)) * 100), pctB: 100 - Math.round((p.votes_a / (p.votes_a + p.votes_b || 1)) * 100), totalVotes: (p.votes_a + p.votes_b).toLocaleString('tr-TR') })) };
});

app.post('/api/zarla/vote', async (req, rep) => {
    const col = req.body?.choice === 'A' ? 'votes_a' : 'votes_b';
    await pool.query(`UPDATE zarla_polls SET ${col} = ${col} + 1 WHERE id = $1`, [req.body?.pollId]);
    return { success: true };
});

app.post('/api/contact', async (req, rep) => {
    await pool.query(`INSERT INTO contact_leads (type, name, email, company, message) VALUES ($1, $2, $3, $4, $5)`, [req.body?.type || 'GENERAL', req.body?.name, req.body?.email, req.body?.company, req.body?.message]);
    return { success: true, message: 'Talebiniz kaydedildi.' };
});

app.post('/api/auth/login-mock', async (req, rep) => {
    const name = req.body?.username || 'LeisanB';
    const ur = await pool.query(`INSERT INTO users (email, username, tier) VALUES ($1, $2, 'Doçent') ON CONFLICT (email) DO UPDATE SET username = EXCLUDED.username RETURNING id, username, balance_kor`, [`${name.toLowerCase()}@oyver.pro`, name]);
    const token = crypto.randomBytes(32).toString('hex');
    await pool.query(`INSERT INTO sessions (token, user_id) VALUES ($1, $2)`, [token, ur.rows[0].id]);
    rep.header('Set-Cookie', `oyver_token=${token}; Path=/; HttpOnly; SameSite=Lax; Max-Age=2592000`);
    return { success: true };
});

// ==========================================
// 4. FRONTEND ARAYÜZÜ (OBSIDIAN DARK + P2P ARENA)
// ==========================================
function renderIndexHtml() {
    return `<!DOCTYPE html>
<html lang="tr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>OYVER PRO - Liyakat Tabanlı Kolektif Öngörü Terminali</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap" rel="stylesheet">
    <style>
        body { font-family: 'Inter', sans-serif; background-color: #080c14; }
        .glass-card { background: #0d131f; border: 1px solid #1e293b; }
        .glass-card:hover { border-color: #334155; }
        .duel-box { background: linear-gradient(180deg, #0f172a 0%, #090d16 100%); border: 1px solid #1e293b; }
        ::-webkit-scrollbar { width: 4px; }
        ::-webkit-scrollbar-track { background: #080c14; }
        ::-webkit-scrollbar-thumb { background: #1e293b; border-radius: 2px; }
    </style>
</head>
<body class="text-slate-100 min-h-screen flex flex-col antialiased selection:bg-indigo-600 selection:text-white">

    <div id="toast-container" class="fixed top-5 right-5 z-50 flex flex-col gap-2 pointer-events-none"></div>

    <!-- HEADER -->
    <header class="sticky top-0 z-40 bg-[#080c14]/90 backdrop-blur-md border-b border-slate-800/80">
        <div class="container mx-auto px-4 h-16 flex items-center justify-between gap-4">
            <div class="flex items-center space-x-6">
                <a href="/" onclick="navigateToHome(event)" class="text-2xl font-black tracking-tight text-white flex items-center gap-1.5">
                    OYVER<span class="text-[11px] px-1.5 py-0.5 rounded bg-indigo-500/20 text-indigo-400 border border-indigo-500/30">PRO</span>
                </a>
                <nav class="hidden sm:flex items-center space-x-2 text-xs font-bold">
                    <button id="nav-tab-zarla" class="px-3.5 py-2 rounded-xl text-slate-300 hover:text-white hover:bg-slate-900 transition flex items-center gap-2">
                        <i class="fas fa-poll text-indigo-400"></i> OYLA (Günün Nabzı)
                    </button>
                    <button onclick="openP2PModal()" class="px-3 py-1.5 rounded-xl bg-purple-600/20 text-purple-300 border border-purple-500/30 hover:bg-purple-600 hover:text-white transition flex items-center gap-1.5">
                        <i class="fas fa-swords"></i> ⚔️ P2P Meydan Oku
                    </button>
                </nav>
            </div>

            <!-- Portföy & Profil Kümesi -->
            <div class="flex items-center space-x-2.5">
                <button id="nav-tab-portfolio" class="px-3 py-1.5 rounded-xl bg-slate-900 border border-slate-800 text-xs font-bold text-slate-300 hover:text-white hover:border-slate-700 transition flex items-center gap-1.5">
                    <i class="fas fa-briefcase text-emerald-400"></i> <span class="hidden sm:inline">Portföyüm</span>
                </button>

                <div class="relative">
                    <div id="user-badge" class="flex items-center bg-slate-900 border border-slate-800 rounded-xl p-1 pr-3 space-x-2 cursor-pointer hover:border-slate-700 transition" onclick="toggleUserDropdown()">
                        <div class="flex items-center gap-1 bg-slate-950 px-2 py-1 rounded-lg border border-slate-800">
                            <i class="fas fa-fire text-amber-500 text-xs"></i>
                            <span id="user-streak" class="text-xs font-black text-amber-400">5g</span>
                        </div>
                        <div class="text-xs font-bold text-slate-200" id="user-balance">-- KOR</div>
                        <span id="user-tier-badge" class="px-1.5 py-0.5 rounded text-[10px] font-black uppercase bg-indigo-500/20 text-indigo-300 border border-indigo-500/30">Doçent</span>
                        <i class="fas fa-chevron-down text-[10px] text-slate-500"></i>
                    </div>

                    <div id="user-dropdown" class="hidden absolute right-0 top-14 w-64 bg-[#0d131f] border border-slate-800 rounded-2xl shadow-2xl p-2 z-50 space-y-1">
                        <div class="px-3 py-2 border-b border-slate-800/80 mb-1">
                            <div class="text-xs font-bold text-white flex justify-between">
                                <span id="dd-username">LeisanB</span>
                                <span class="text-[10px] text-emerald-400" id="dd-archetype">Stratejist</span>
                            </div>
                            <div class="text-[10px] text-indigo-400 font-bold" id="dd-tier">Kademesi: Doçent</div>
                        </div>
                        <button onclick="openProfileStepModal()" class="w-full text-left px-3 py-2 text-xs font-bold text-slate-200 hover:bg-slate-800/60 rounded-xl transition flex items-center gap-2">
                            <i class="fas fa-user-check text-purple-400"></i> Profilini Tamamla (+3.500 KOR)
                        </button>
                        <button onclick="openLeaderboard()" class="w-full text-left px-3 py-2 text-xs font-bold text-slate-200 hover:bg-slate-800/60 rounded-xl transition flex items-center gap-2">
                            <i class="fas fa-graduation-cap text-amber-400"></i> Akademik Liyakat Ligi
                        </button>
                        <button onclick="openDrawer('about')" class="w-full text-left px-3 py-2 text-xs font-bold text-slate-200 hover:bg-slate-800/60 rounded-xl transition flex items-center gap-2">
                            <i class="fas fa-info-circle text-indigo-400"></i> Biz Kimiz?
                        </button>
                        <button onclick="openDrawer('b2b')" class="w-full text-left px-3 py-2 text-xs font-bold text-slate-200 hover:bg-slate-800/60 rounded-xl transition flex items-center gap-2">
                            <i class="fas fa-chart-pie text-emerald-400"></i> B2B & Güven Endeksi
                        </button>
                        <button onclick="openDrawer('contact')" class="w-full text-left px-3 py-2 text-xs font-bold text-slate-200 hover:bg-slate-800/60 rounded-xl transition flex items-center gap-2">
                            <i class="fas fa-envelope text-slate-400"></i> Bize Ulaşın
                        </button>
                        <div class="border-t border-slate-800 my-1"></div>
                        <button onclick="promptSwitchUser()" class="w-full text-left px-3 py-2 text-xs font-bold text-slate-400 hover:text-white hover:bg-slate-800/60 rounded-xl transition flex items-center gap-2">
                            <i class="fas fa-user-circle"></i> Kullanıcı Değiştir
                        </button>
                    </div>
                </div>
            </div>
        </div>
    </header>

    <!-- SAYFA GÖVDESİ -->
    <div class="flex-grow pb-24 sm:pb-12">
        <main id="section-markets">
            <!-- REKABETÇİ VE NEŞELİ BANNER -->
            <section class="py-5 border-b border-slate-900 bg-[#0a0f1d]">
                <div class="container mx-auto px-4 max-w-5xl">
                    <div class="flex flex-col sm:flex-row items-center justify-between gap-4 p-4 rounded-2xl bg-[#0f172a]/60 border border-slate-800">
                        <div class="space-y-1 text-center sm:text-left">
                            <h2 class="text-sm font-black text-white flex items-center justify-center sm:justify-start gap-1.5">
                                <span>🎯 Geleceği Kokla, Ordinaryüs Kademesine Tırman!</span>
                            </h2>
                            <p class="text-xs text-slate-400">Cebinden 1 TL çıkmaz. 14.500 KOR puanınla tahmin yap, Brier skorunla itibar kazan.</p>
                        </div>
                        <button onclick="openP2PModal()" class="px-4 py-2 bg-gradient-to-r from-purple-600 to-indigo-600 rounded-xl text-xs font-black text-white shadow-lg hover:opacity-90 transition whitespace-nowrap">
                            ⚔️ Bir Analiste Meydan Oku
                        </button>
                    </div>
                </div>
            </section>

            <!-- HERO DÜELLOLAR -->
            <section class="py-6 border-b border-slate-900">
                <div class="container mx-auto px-4 max-w-5xl">
                    <h3 class="text-xs font-black uppercase tracking-wider text-slate-400 mb-3 flex items-center gap-2">
                        <span class="w-2 h-2 rounded-full bg-emerald-400 animate-pulse"></span> Canlı Kamuoyu Düelloları
                    </h3>
                    <div id="hero-duels-container" class="grid grid-cols-1 sm:grid-cols-2 gap-4"></div>
                </div>
            </section>

            <!-- KATEGORİ VE ALT KATEGORİ FİLTRELERİ -->
            <section class="container mx-auto px-4 pt-6 pb-2 max-w-5xl">
                <div class="flex items-center space-x-2 overflow-x-auto pb-2">
                    <button class="cat-btn active bg-indigo-600 text-white px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap" data-cat="ALL">Tümü</button>
                    <button class="cat-btn bg-slate-900 hover:bg-slate-800 text-slate-300 px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap" data-cat="BORSA & FİNANS">Borsa & Finans</button>
                    <button class="cat-btn bg-slate-900 hover:bg-slate-800 text-slate-300 px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap" data-cat="SPOR">Spor</button>
                    <button class="cat-btn bg-slate-900 hover:bg-slate-800 text-slate-300 px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap" data-cat="EKONOMİ">Ekonomi</button>
                    <button class="cat-btn bg-slate-900 hover:bg-slate-800 text-slate-300 px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap" data-cat="TEKNOLOJİ">Teknoloji</button>
                    <button class="cat-btn bg-slate-900 hover:bg-slate-800 text-slate-300 px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap" data-cat="MODA & STİL">Moda & Stil</button>
                </div>
                <div id="sub-cat-container" class="hidden flex items-center space-x-1.5 overflow-x-auto pt-2 pb-1 border-t border-slate-900 mt-2"></div>
            </section>

            <!-- PAZARLAR IZGARASI (HIZLI BUTONLARLA) -->
            <section class="container mx-auto px-4 py-4 max-w-5xl">
                <div id="market-grid" class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-5"></div>
            </section>
        </main>

        <!-- PAZAR DETAY TERMİNALİ -->
        <main id="section-market-detail" class="hidden container mx-auto px-4 py-6 max-w-5xl">
            <button onclick="navigateToHome(event)" class="text-xs text-indigo-400 hover:text-indigo-300 font-bold flex items-center gap-1.5 mb-4">
                <i class="fas fa-arrow-left"></i> Tüm Pazarlara Dön
            </button>
            <div class="flex items-center justify-between gap-3 mb-2">
                <span id="dt-category" class="px-2.5 py-0.5 rounded text-[10px] font-black uppercase tracking-wider bg-indigo-500/20 text-indigo-300 border border-indigo-500/30">KATEGORİ</span>
                <span class="text-xs text-slate-400"><i class="far fa-clock mr-1"></i><span id="dt-closing-date">--</span></span>
            </div>
            <h1 id="dt-title" class="text-xl sm:text-2xl font-black text-white mb-6 leading-snug">Pazar Başlığı</h1>

            <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
                <div class="lg:col-span-2 space-y-6">
                    <div class="glass-card rounded-2xl p-5 shadow-xl">
                        <div class="flex items-center justify-between mb-4">
                            <div>
                                <div class="text-xs font-bold text-slate-400 uppercase">EVET Olasılık Eğrisi</div>
                                <div class="text-3xl font-black text-emerald-400 mt-0.5" id="dt-current-prob">--%</div>
                            </div>
                        </div>
                        <div class="h-60 w-full relative"><canvas id="marketChart"></canvas></div>
                    </div>
                </div>

                <div class="glass-card rounded-2xl p-5 shadow-xl space-y-4 h-fit">
                    <div class="flex justify-between items-center pb-2 border-b border-slate-800">
                        <span class="text-xs font-bold text-slate-400 uppercase">Tahmin Konsolu</span>
                        <span id="dt-user-balance" class="text-xs font-black text-amber-300">-- KOR</span>
                    </div>
                    <div class="grid grid-cols-2 gap-2">
                        <button id="dt-choice-yes" class="py-2.5 rounded-xl font-black text-xs uppercase border border-emerald-500 bg-emerald-600 text-white">EVET</button>
                        <button id="dt-choice-no" class="py-2.5 rounded-xl font-black text-xs uppercase border border-slate-800 bg-slate-950 text-slate-400">HAYIR</button>
                    </div>
                    <div>
                        <label class="block text-xs font-bold text-slate-400 mb-1">Puan Tutarı</label>
                        <input type="number" id="dt-input-amount" value="500" min="50" step="50" class="w-full bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-white font-bold text-sm focus:outline-none focus:border-indigo-500">
                    </div>
                    <button id="dt-btn-predict" class="w-full py-3 rounded-xl bg-indigo-600 hover:bg-indigo-500 text-white font-extrabold text-xs shadow-lg transition">
                        Tahmini Onayla (KOR)
                    </button>
                </div>
            </div>
        </main>

        <!-- OYLA (ZARLA) -->
        <main id="section-zarla" class="hidden container mx-auto px-4 py-8 max-w-2xl">
            <div class="text-center mb-6">
                <span class="px-3 py-1 rounded-full bg-indigo-500/10 border border-indigo-500/20 text-xs font-bold text-indigo-400">0 Puan, 0 Risk, Saf Kamuoyu Nabzı</span>
                <h2 class="text-2xl font-black text-white mt-2">Günün Kritik Meseleleri</h2>
            </div>
            <div id="zarla-list" class="space-y-4"></div>
        </main>

        <!-- PORTFÖYÜM -->
        <main id="section-portfolio" class="hidden container mx-auto px-4 py-6 max-w-3xl space-y-6">
            <div>
                <div class="flex items-center justify-between mb-3 pb-2 border-b border-slate-800">
                    <h2 class="text-lg font-black text-white">Açık Paylarım</h2>
                    <button onclick="loadPortfolio()" class="text-xs text-indigo-400 hover:underline"><i class="fas fa-sync-alt mr-1"></i> Yenile</button>
                </div>
                <div id="portfolio-active-list" class="space-y-3"></div>
            </div>
            <div>
                <h2 class="text-lg font-black text-emerald-400 mb-3 pb-2 border-b border-slate-800">Sonuçlanan Tahminlerim</h2>
                <div id="portfolio-settled-list" class="space-y-3"></div>
            </div>
        </main>
    </div>

    <!-- HIZLI OY VERME ÇEKMECESİ (INLINE SLIDE-OVER) -->
    <div id="quick-vote-drawer-backdrop" class="fixed inset-0 bg-slate-950/80 z-50 backdrop-blur-sm hidden" onclick="closeQuickVoteDrawer()"></div>
    <div id="quick-vote-drawer" class="fixed bottom-0 sm:bottom-auto sm:right-0 sm:top-0 sm:h-full w-full sm:max-w-md bg-[#0d131f] border-t sm:border-t-0 sm:border-l border-slate-800 p-5 rounded-t-3xl sm:rounded-none z-50 transform translate-y-full sm:translate-y-0 sm:translate-x-full transition-transform duration-300 space-y-4">
        <div class="flex justify-between items-center pb-2 border-b border-slate-800">
            <span class="text-xs font-black uppercase text-indigo-400" id="qv-category">KATEGORİ</span>
            <button onclick="closeQuickVoteDrawer()" class="text-slate-400 hover:text-white"><i class="fas fa-times"></i></button>
        </div>
        <h3 class="text-sm font-bold text-white leading-snug" id="qv-title">Pazar Sorusu...</h3>
        <div class="p-3 rounded-xl bg-slate-950 border border-slate-800 flex justify-between items-center text-xs">
            <span class="text-slate-400">Seçilen Yön:</span>
            <span class="font-black text-sm" id="qv-outcome-badge">EVET</span>
        </div>
        <div>
            <label class="block text-xs font-bold text-slate-400 mb-1.5">Puan Miktarı</label>
            <div class="grid grid-cols-4 gap-2 mb-2">
                <button type="button" onclick="setQuickAmount(100)" class="py-1.5 rounded-lg bg-slate-900 border border-slate-800 text-xs font-bold hover:border-indigo-500">100</button>
                <button type="button" onclick="setQuickAmount(250)" class="py-1.5 rounded-lg bg-slate-900 border border-slate-800 text-xs font-bold hover:border-indigo-500">250</button>
                <button type="button" onclick="setQuickAmount(500)" class="py-1.5 rounded-lg bg-slate-900 border border-slate-800 text-xs font-bold hover:border-indigo-500">500</button>
                <button type="button" onclick="setQuickAmount(1000)" class="py-1.5 rounded-lg bg-slate-900 border border-slate-800 text-xs font-bold hover:border-indigo-500">1.000</button>
            </div>
            <input type="number" id="qv-amount" value="250" min="50" step="50" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white font-bold text-sm focus:outline-none focus:border-indigo-500">
        </div>
        <button id="qv-submit-btn" onclick="executeQuickVote()" class="w-full py-3 bg-indigo-600 hover:bg-indigo-500 rounded-xl font-bold text-white text-xs shadow-lg transition">
            Tahmini Deftere İşle (Sayfadan Ayrılmadan)
        </button>
    </div>

    <!-- P2P ANALİST DÜELLO ARENASI MODALI (10 -> 5 -> 1 SORU) -->
    <div id="p2p-modal" class="fixed inset-0 bg-slate-950/90 z-50 backdrop-blur-md hidden flex items-center justify-center p-4">
        <div class="bg-[#0d131f] border border-slate-800 w-full max-w-xl rounded-3xl p-6 shadow-2xl space-y-4">
            <div class="flex justify-between items-center pb-3 border-b border-slate-800">
                <div class="flex items-center gap-2">
                    <span class="text-lg">⚔️</span>
                    <h3 class="text-base font-black text-white">Birebir Analist Meydan Okuması (5.000 KOR)</h3>
                </div>
                <button onclick="closeP2PModal()" class="text-slate-400 hover:text-white"><i class="fas fa-times"></i></button>
            </div>

            <!-- Meydan Okuma Başlatma Ekranı -->
            <div id="p2p-init-view" class="space-y-4 text-xs">
                <p class="text-slate-300">İnternette hazır cevabı olmayan <strong>10 Eleme, 5 Yarı Final ve 1 Altın Final</strong> sorusunu rakibinden daha isabetli çöz, 9.600 KOR ödülü cüzdanına ekle!</p>
                <div class="p-3 bg-slate-950 rounded-xl border border-slate-800 space-y-1">
                    <span class="font-bold text-indigo-400">🛡️ 12 Saniye Kalkanı:</span>
                    <p class="text-slate-400">Her soru için süreniz tam 12 saniyedir. Süre bitince sonraki soruya geçer.</p>
                </div>
                <div>
                    <label class="block font-bold text-slate-300 mb-1">Meydan Okunacak Kullanıcı Adı</label>
                    <input type="text" id="p2p-target-user" placeholder="Örn: Ahmet_Analist, Ece_Hoca" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white">
                </div>
                <button onclick="startChallenge()" class="w-full py-3 bg-gradient-to-r from-purple-600 to-indigo-600 rounded-xl font-bold text-white text-xs shadow-lg transition">
                    5.000 KOR Teminatla Meydan Oku & Tura Başla
                </button>
            </div>

            <!-- Soru Çözüm Ekranı (12 Saniyelik Sayaçla) -->
            <div id="p2p-quiz-view" class="hidden space-y-4 text-xs">
                <div class="flex justify-between items-center bg-slate-950 p-3 rounded-xl border border-slate-800">
                    <span class="font-bold text-indigo-400" id="quiz-progress">Soru 1 / 16 (1. Etap: Eleme)</span>
                    <div class="flex items-center gap-1 text-amber-400 font-black text-sm">
                        <i class="far fa-clock"></i> <span id="quiz-timer">12</span>s
                    </div>
                </div>
                <div class="p-4 bg-slate-950 rounded-2xl border border-slate-800 min-h-[90px] flex items-center">
                    <p class="text-sm font-bold text-white leading-relaxed" id="quiz-question">Soru metni yükleniyor...</p>
                </div>
                <div class="grid grid-cols-2 gap-3" id="quiz-options-box">
                    <button onclick="answerP2P('A')" id="btn-opt-a" class="py-3 px-3 bg-slate-900 hover:bg-indigo-600/40 border border-slate-800 hover:border-indigo-500 rounded-xl text-xs font-bold text-slate-200 transition">Seçenek A</button>
                    <button onclick="answerP2P('B')" id="btn-opt-b" class="py-3 px-3 bg-slate-900 hover:bg-rose-600/40 border border-slate-800 hover:border-rose-500 rounded-xl text-xs font-bold text-slate-200 transition">Seçenek B</button>
                </div>
                <div id="quiz-golden-box" class="hidden space-y-2">
                    <input type="number" step="0.01" id="quiz-golden-input" placeholder="Nokta atışı sayıyı yazın (örn: 3.14)" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-3 text-white font-bold text-sm">
                    <button onclick="submitGoldenShot()" class="w-full py-2.5 bg-amber-600 hover:bg-amber-500 rounded-xl font-bold text-white transition">Altın Tahmini Gönder</button>
                </div>
            </div>
        </div>
    </div>

    <!-- KADEMELİ PROFİLLEME VE KİŞİLİK ANALİZİ MODALI -->
    <div id="profile-step-modal" class="fixed inset-0 bg-slate-950/90 z-50 backdrop-blur-md hidden flex items-center justify-center p-4">
        <div class="bg-[#0d131f] border border-slate-800 w-full max-w-lg rounded-3xl p-6 shadow-2xl space-y-4">
            <div class="flex justify-between items-center pb-2 border-b border-slate-800">
                <h3 class="text-sm font-black text-white flex items-center gap-2">
                    <i class="fas fa-brain text-purple-400"></i> Kademeli Profilleme & Kişilik Analizi
                </h3>
                <button onclick="closeProfileStepModal()" class="text-slate-400 hover:text-white"><i class="fas fa-times"></i></button>
            </div>

            <!-- Adım 1: Giriş & Yasal KVKK -->
            <div id="prof-step-1" class="space-y-3 text-xs">
                <div class="p-3 bg-slate-950 rounded-xl border border-slate-800 text-[11px] text-slate-400 leading-relaxed">
                    <label class="flex items-start gap-2 cursor-pointer">
                        <input type="checkbox" id="kvkk-check" class="mt-0.5 rounded border-slate-700 bg-slate-900 text-indigo-600">
                        <span><strong>KVKK Açık Rıza:</strong> Tahmin ve demografi tercihlerimin anonimleştirilerek istatistiki araştırmalarda ve B2B kohort analizlerinde işlenmesine onay veriyorum.</span>
                    </label>
                </div>
                <div>
                    <label class="block font-bold text-slate-300 mb-1">Doğum Yılınız (18+)</label>
                    <input type="number" id="p-birth" value="1995" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white">
                </div>
                <div>
                    <label class="block font-bold text-slate-300 mb-1">Şehir</label>
                    <input type="text" id="p-city" value="İstanbul" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white">
                </div>
                <div>
                    <label class="block font-bold text-slate-300 mb-1">Sektör / Meslek</label>
                    <input type="text" id="p-ind" value="Finans & Teknoloji" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white">
                </div>
                <button onclick="submitStep1()" class="w-full py-2.5 bg-indigo-600 rounded-xl font-bold text-white transition">+1.000 KOR Kazan & Kaydet</button>
            </div>

            <!-- Adım 2: Aidiyet & Risk İştahı -->
            <div id="prof-step-2" class="hidden space-y-3 text-xs">
                <div>
                    <label class="block font-bold text-slate-300 mb-1">Tuttuğunuz Takım</label>
                    <select id="p-team" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white">
                        <option value="Galatasaray">Galatasaray</option><option value="Fenerbahçe">Fenerbahçe</option><option value="Beşiktaş">Beşiktaş</option><option value="Trabzonspor">Trabzonspor</option><option value="Diğer">Diğer / İlgilenmiyorum</option>
                    </select>
                </div>
                <div>
                    <label class="block font-bold text-slate-300 mb-1">Ulaşım Tercihiniz</label>
                    <select id="p-mob" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white">
                        <option value="Şahsi Otomobil">Şahsi Otomobil</option><option value="Toplu Taşıma">Toplu Taşıma</option><option value="Yaya / Mikromobilite">Yaya / Mikromobilite</option>
                    </select>
                </div>
                <div class="p-3 bg-purple-950/20 border border-purple-500/30 rounded-xl space-y-2">
                    <span class="font-bold text-purple-300">🧠 Karar Alma Sorusu 1:</span>
                    <p class="text-slate-300">Bir yarışmada son aşamaya geldiniz. Hangisini seçersiniz?</p>
                    <div class="space-y-1">
                        <label class="flex items-center gap-2 cursor-pointer"><input type="radio" name="r-risk" value="A" checked> <span>Kesin 15.000 TL alıp çekilmek (Garantici)</span></label>
                        <label class="flex items-center gap-2 cursor-pointer"><input type="radio" name="r-risk" value="B"> <span>%50 şansla 45.000 TL kazanmak veya 0 TL (Risk Arayan)</span></label>
                    </div>
                </div>
                <button onclick="submitStep2()" class="w-full py-2.5 bg-indigo-600 rounded-xl font-bold text-white transition">+1.500 KOR Kazan & İlerle</button>
            </div>

            <!-- Adım 3: Zevkler & Aykırı Zeka -->
            <div id="prof-step-3" class="hidden space-y-3 text-xs">
                <div>
                    <label class="block font-bold text-slate-300 mb-1">Müzik Zevkiniz</label>
                    <select id="p-music" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white">
                        <option value="Rock / Metal">Rock / Metal</option><option value="Rap / Hip-Hop">Rap / Hip-Hop</option><option value="Türkçe Pop">Türkçe Pop</option><option value="Elektronik">Elektronik</option>
                    </select>
                </div>
                <div>
                    <label class="block font-bold text-slate-300 mb-1">Dışarıda Tüketim</label>
                    <select id="p-coffee" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white">
                        <option value="Zincir Kahveci">Her gün zincir kahvecideyim</option><option value="Evde Demleme">Kendi kahvemi demlerim</option><option value="Çaycı">Çay / Geleneksel</option>
                    </select>
                </div>
                <div class="p-3 bg-purple-950/20 border border-purple-500/30 rounded-xl space-y-2">
                    <span class="font-bold text-purple-300">🧠 Karar Alma Sorusu 2:</span>
                    <p class="text-slate-300">Piyasada veya toplumda herkes sizinle aynı fikirdeyse ne hissedersiniz?</p>
                    <div class="space-y-1">
                        <label class="flex items-center gap-2 cursor-pointer"><input type="radio" name="r-contra" value="A" checked> <span>Rahatlar ve doğru yolda olduğumu düşünürüm (Konsensüs)</span></label>
                        <label class="flex items-center gap-2 cursor-pointer"><input type="radio" name="r-contra" value="B"> <span>Şüphelenir, sürüde nerede hata olduğunu ararım (Aykırı Zeka)</span></label>
                    </div>
                </div>
                <button onclick="submitStep3()" class="w-full py-2.5 bg-emerald-600 rounded-xl font-bold text-white transition">+1.000 KOR Kazan & Arketipini Çıkar</button>
            </div>
        </div>
    </div>

    <!-- KURUMSAL ÇEKMECE (%93.2 GÜVEN ENDEKSİ DAHİL) -->
    <div id="info-modal" class="fixed inset-0 bg-slate-950/85 z-50 backdrop-blur-md hidden flex items-center justify-center p-4">
        <div class="bg-[#0d131f] border border-slate-800 w-full max-w-lg rounded-3xl p-6 max-h-[85vh] overflow-y-auto space-y-4">
            <div class="flex justify-between items-center pb-3 border-b border-slate-800">
                <h3 class="text-base font-black text-white" id="info-modal-title">Başlık</h3>
                <button onclick="closeInfoModal()" class="text-slate-400 hover:text-white"><i class="fas fa-times"></i></button>
            </div>
            <div id="info-modal-content" class="text-xs text-slate-300 leading-relaxed space-y-3"></div>
        </div>
    </div>

    <!-- LİYAKAT LİGİ MODALI -->
    <div id="leaderboard-modal" class="fixed inset-0 bg-slate-950/85 z-50 backdrop-blur-md hidden flex items-center justify-center p-4">
        <div class="bg-[#0d131f] border border-slate-800 w-full max-w-xl rounded-3xl p-5 max-h-[85vh] flex flex-col">
            <div class="flex justify-between items-center pb-3 border-b border-slate-800 mb-3">
                <div class="flex items-center gap-2"><i class="fas fa-graduation-cap text-amber-400"></i><h3 class="text-base font-black text-white">Akademik Liyakat Ligi</h3></div>
                <button onclick="closeLeaderboard()" class="text-slate-400"><i class="fas fa-times"></i></button>
            </div>
            <div class="overflow-y-auto flex-grow divide-y divide-slate-800/80" id="leaderboard-list"></div>
        </div>
    </div>

    <script>
        let markets = [];
        let heroDuels = [];
        let currentBalance = 14500;
        let activeCategory = 'ALL';
        let activeSubCategory = 'ALL';
        let quickTargetMarket = null;
        let quickTargetOutcome = 'YES';
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

        // KART ÜZERİNDE HIZLI OY VERME (SAYFA GEÇİŞSİZ)
        window.openQuickVote = function(marketId, outcome) {
            quickTargetMarket = markets.find(m => m.id === marketId);
            quickTargetOutcome = outcome;
            if (!quickTargetMarket) return;

            document.getElementById('qv-category').textContent = quickTargetMarket.category;
            document.getElementById('qv-title').textContent = quickTargetMarket.question;
            const badge = document.getElementById('qv-outcome-badge');
            badge.textContent = outcome === 'YES' ? 'EVET' : 'HAYIR';
            badge.className = outcome === 'YES' ? 'font-black text-sm text-emerald-400' : 'font-black text-sm text-rose-400';

            updateQuickQuote();
            document.getElementById('quick-vote-drawer-backdrop').classList.remove('hidden');
            document.getElementById('quick-vote-drawer').classList.remove('translate-y-full', 'sm:translate-x-full');
        };

        window.closeQuickVoteDrawer = function() {
            document.getElementById('quick-vote-drawer').classList.add('translate-y-full', 'sm:translate-x-full');
            document.getElementById('quick-vote-drawer-backdrop').classList.add('hidden');
        };

        window.setQuickAmount = function(val) {
            document.getElementById('qv-amount').value = val;
            updateQuickQuote();
        };

        async function updateQuickQuote() {
            if (!quickTargetMarket) return;
            const amt = parseFloat(document.getElementById('qv-amount').value) || 0;
            if (amt <= 0) return;
            try {
                const res = await fetch('/api/trade/quote', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ marketId: quickTargetMarket.id, outcome: quickTargetOutcome, amountKor: amt, action: 'BUY' })
                }).then(r => r.json());
                if (res.sharesOut) {
                    document.getElementById('qv-shares').textContent = res.sharesOut + ' Pay';
                    document.getElementById('qv-payout').textContent = '+' + res.targetPayout;
                }
            } catch(e) {}
        }

        window.executeQuickVote = async function() {
            const amt = document.getElementById('qv-amount').value;
            try {
                const res = await fetch('/api/trade/predict', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ marketId: quickTargetMarket.id, outcome: quickTargetOutcome, amountKor: amt })
                }).then(r => r.json());

                if (res.error) throw new Error(res.error);
                currentBalance = res.balanceKor;
                document.getElementById('user-balance').textContent = currentBalance.toLocaleString('tr-TR') + ' KOR';
                showToast('✅ Tercihiniz deftere işlendi! Alınan: ' + res.sharesOut + ' Pay');
                closeQuickVoteDrawer();
            } catch(e) {
                showToast(e.message, 'error');
            }
        };

        // PAZARLAR LİSTESİ
        function renderMarkets() {
            const container = document.getElementById('market-grid');
            container.innerHTML = '';

            let filtered = activeCategory === 'ALL' ? markets : markets.filter(m => m.category === activeCategory);
            if (activeSubCategory !== 'ALL') filtered = filtered.filter(m => m.sub_category === activeSubCategory);

            filtered.forEach(m => {
                const card = document.createElement('article');
                card.className = 'glass-card rounded-2xl p-4 shadow-lg flex flex-col justify-between transition';

                card.innerHTML = 
                    '<div>' +
                        '<div class="flex items-center justify-between mb-2">' +
                            '<span class="px-2 py-0.5 rounded text-[9px] font-black uppercase tracking-wider bg-indigo-500/20 text-indigo-300 border border-indigo-500/30">' + escapeHtml(m.category) + (m.sub_category ? ' • ' + escapeHtml(m.sub_category) : '') + '</span>' +
                            '<span class="text-[10px] text-slate-400"><i class="far fa-clock mr-1"></i>' + escapeHtml(m.closing_date) + '</span>' +
                        '</div>' +
                        '<h3 class="text-xs sm:text-sm font-bold text-white mb-2 leading-snug cursor-pointer hover:text-indigo-400 transition" onclick="openMarketDetail(\\'' + m.slug + '\\')">' + escapeHtml(m.question) + '</h3>' +
                        '<div class="space-y-1 mb-4">' +
                            '<div class="flex justify-between text-[11px] font-semibold">' +
                                '<span class="text-slate-400">Havuz: ' + m.poolTotal + '</span><span class="text-emerald-400 font-bold">EVET: %' + m.probYes + '</span>' +
                            '</div>' +
                            '<div class="w-full bg-slate-950 rounded-full h-1.5 overflow-hidden flex border border-slate-800">' +
                                '<div class="bg-emerald-500 h-full transition-all duration-700" style="width:' + m.probYes + '%"></div>' +
                            '</div>' +
                        '</div>' +
                    '</div>' +
                    '<div class="space-y-2 pt-2 border-t border-slate-800/80">' +
                        '<div class="grid grid-cols-2 gap-2">' +
                            '<button onclick="openQuickVote(\\'' + m.id + '\\', \\'YES\\')" class="py-2 bg-emerald-950/40 hover:bg-emerald-600 border border-emerald-500/30 text-emerald-300 hover:text-white rounded-xl text-xs font-bold transition">EVET (%' + m.probYes + ')</button>' +
                            '<button onclick="openQuickVote(\\'' + m.id + '\\', \\'NO\\')" class="py-2 bg-rose-950/40 hover:bg-rose-600 border border-rose-500/30 text-rose-300 hover:text-white rounded-xl text-xs font-bold transition">HAYIR (%' + m.probNo + ')</button>' +
                        '</div>' +
                        '<button onclick="openMarketDetail(\\'' + m.slug + '\\')" class="w-full py-1.5 text-center text-[11px] font-bold text-slate-400 hover:text-white transition flex items-center justify-center gap-1">' +
                            '<span>Derin Analiz & Terminal</span> <i class="fas fa-chevron-right text-[9px]"></i>' +
                        '</button>' +
                    '</div>';

                container.appendChild(card);
            });
        }

        window.openMarketDetail = async function(slug) {
            history.pushState({}, '', '/market/' + slug);
            [secMarkets, secZarla, secPortfolio].forEach(s => s.classList.add('hidden'));
            secMarketDetail.classList.remove('hidden');

            const res = await fetch('/api/markets/' + slug).then(r => r.json());
            document.getElementById('dt-title').textContent = res.market.question;
            document.getElementById('dt-category').textContent = res.market.category;
            document.getElementById('dt-closing-date').textContent = res.market.closing_date;
            document.getElementById('dt-current-prob').textContent = '%' + res.market.probYes;
            document.getElementById('dt-user-balance').textContent = currentBalance.toLocaleString('tr-TR') + ' KOR';
            renderChart(res.history);
        };

        function renderChart(history) {
            const ctx = document.getElementById('marketChart').getContext('2d');
            if (chartInstance) chartInstance.destroy();
            chartInstance = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: history.map(h => h.time_label),
                    datasets: [{ label: 'EVET (%)', data: history.map(h => h.prob_yes), borderColor: '#10b981', backgroundColor: 'rgba(16, 185, 129, 0.08)', borderWidth: 2, fill: true, tension: 0.35, pointRadius: 2.5 }]
                },
                options: {
                    responsive: true, maintainAspectRatio: false,
                    scales: { y: { min: 0, max: 100, grid: { color: 'rgba(30, 41, 59, 0.5)' }, ticks: { color: '#64748b' } }, x: { grid: { display: false }, ticks: { color: '#64748b' } } },
                    plugins: { legend: { display: false } }
                }
            });
        }

        // P2P DÜELLO ÇÖZÜM MOTORU (12 Saniyelik Sayaç)
        let p2pActiveDuelId = null;
        let p2pCurrentQIdx = 0;
        let p2pQuestions = [];
        let p2pAnswers = {};
        let p2pTimerInterval = null;

        window.openP2PModal = async () => {
            const res = await fetch('/api/p2p/questions').then(r => r.json());
            p2pQuestions = res.questions;
            document.getElementById('p2p-modal').classList.remove('hidden');
            document.getElementById('p2p-init-view').classList.remove('hidden');
            document.getElementById('p2p-quiz-view').classList.add('hidden');
        };
        window.closeP2PModal = () => {
            clearInterval(p2pTimerInterval);
            document.getElementById('p2p-modal').classList.add('hidden');
        };

        window.startChallenge = async () => {
            const target = document.getElementById('p2p-target-user').value;
            if (!target) return showToast('Lütfen rakip kullanıcı adı girin.', 'error');
            try {
                const res = await fetch('/api/p2p/challenge', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ targetUsername: target })
                }).then(r => r.json());

                if (res.error) throw new Error(res.error);
                p2pActiveDuelId = res.duelId;
                currentBalance = res.newBalance;
                document.getElementById('user-balance').textContent = currentBalance.toLocaleString('tr-TR') + ' KOR';

                document.getElementById('p2p-init-view').classList.add('hidden');
                document.getElementById('p2p-quiz-view').classList.remove('hidden');
                p2pCurrentQIdx = 0;
                p2pAnswers = {};
                loadNextP2PQuestion();
            } catch(e) {
                showToast(e.message, 'error');
            }
        };

        function loadNextP2PQuestion() {
            clearInterval(p2pTimerInterval);
            if (p2pCurrentQIdx >= 15) {
                // Büyük Final (Golden Shot)
                document.getElementById('quiz-progress').textContent = 'BÜYÜK FİNAL: NOKTA ATIŞI TAHMİN';
                document.getElementById('quiz-question').textContent = p2pQuestions[15].q;
                document.getElementById('quiz-options-box').classList.add('hidden');
                document.getElementById('quiz-golden-box').classList.remove('hidden');
                document.getElementById('quiz-timer').textContent = '30';
                startP2PTimer(30, () => submitGoldenShot());
                return;
            }

            const q = p2pQuestions[p2pCurrentQIdx];
            const stageText = q.stage === 1 ? '1. Etap: Eleme Sezgisi' : '2. Etap: Mantık İkilemi';
            document.getElementById('quiz-progress').textContent = 'Soru ' + (p2pCurrentQIdx + 1) + ' / 16 (' + stageText + ')';
            document.getElementById('quiz-question').textContent = q.q;
            document.getElementById('btn-opt-a').textContent = q.a;
            document.getElementById('btn-opt-b').textContent = q.b;

            document.getElementById('quiz-timer').textContent = '12';
            startP2PTimer(12, () => answerP2P('TIMEOUT'));
        }

        function startP2PTimer(seconds, onExpire) {
            let left = seconds;
            p2pTimerInterval = setInterval(() => {
                left--;
                document.getElementById('quiz-timer').textContent = left;
                if (left <= 0) {
                    clearInterval(p2pTimerInterval);
                    onExpire();
                }
            }, 1000);
        }

        window.answerP2P = function(choice) {
            const q = p2pQuestions[p2pCurrentQIdx];
            p2pAnswers[q.id] = choice;
            p2pCurrentQIdx++;
            loadNextP2PQuestion();
        };

        window.submitGoldenShot = async function() {
            clearInterval(p2pTimerInterval);
            const val = document.getElementById('quiz-golden-input').value || 3.0;
            try {
                const res = await fetch('/api/p2p/submit-turn', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ duelId: p2pActiveDuelId, answers: p2pAnswers, finalGuess: val })
                }).then(r => r.json());

                if (res.error) throw new Error(res.error);
                showToast('🎉 Turlarınız tamamlandı! 15 soruda ' + res.score + ' isabet sağladınız.');
                closeP2PModal();
            } catch(e) {
                showToast(e.message, 'error');
            }
        };

        // KADEMELİ PROFİLLEME MODALI
        window.openProfileStepModal = () => document.getElementById('profile-step-modal').classList.remove('hidden');
        window.closeProfileStepModal = () => document.getElementById('profile-step-modal').classList.add('hidden');

        window.submitStep1 = async () => {
            const check = document.getElementById('kvkk-check').checked;
            if (!check) return showToast('Lütfen KVKK açık rıza onayını işaretleyin.', 'error');
            const res = await fetch('/api/profile/step1', {
                method: 'POST', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ kvkkAccepted: true, birthYear: document.getElementById('p-birth').value, city: document.getElementById('p-city').value, industry: document.getElementById('p-ind').value })
            }).then(r => r.json());
            if (res.error) return showToast(res.error, 'error');
            showToast('🎉 +1.000 KOR Eklendi! 2. Aşamaya Geçtiniz.');
            currentBalance = res.balanceKor;
            document.getElementById('user-balance').textContent = currentBalance.toLocaleString('tr-TR') + ' KOR';
            document.getElementById('prof-step-1').classList.add('hidden');
            document.getElementById('prof-step-2').classList.remove('hidden');
        };

        window.submitStep2 = async () => {
            const risk = document.querySelector('input[name="r-risk"]:checked').value;
            const res = await fetch('/api/profile/step2', {
                method: 'POST', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ favTeam: document.getElementById('p-team').value, mobilityType: document.getElementById('p-mob').value, riskChoice: risk })
            }).then(r => r.json());
            if (res.error) return showToast(res.error, 'error');
            showToast('🎉 +1.500 KOR Eklendi! Son Aşamaya Geçtiniz.');
            currentBalance = res.balanceKor;
            document.getElementById('user-balance').textContent = currentBalance.toLocaleString('tr-TR') + ' KOR';
            document.getElementById('prof-step-2').classList.add('hidden');
            document.getElementById('prof-step-3').classList.remove('hidden');
        };

        window.submitStep3 = async () => {
            const contra = document.querySelector('input[name="r-contra"]:checked').value;
            const res = await fetch('/api/profile/step3', {
                method: 'POST', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ musicTaste: document.getElementById('p-music').value, coffeeHabit: document.getElementById('p-coffee').value, contrarianChoice: contra })
            }).then(r => r.json());
            if (res.error) return showToast(res.error, 'error');
            showToast('🏆 Profiliniz tamamlandı! Arketipiniz: ' + res.archetype);
            currentBalance = res.balanceKor;
            document.getElementById('user-balance').textContent = currentBalance.toLocaleString('tr-TR') + ' KOR';
            document.getElementById('dd-archetype').textContent = res.archetype;
            closeProfileStepModal();
        };

        // KURUMSAL VE %93.2 GÜVEN ENDEKSİ ÇEKMECESİ
        window.openDrawer = function(type) {
            const modal = document.getElementById('info-modal');
            const title = document.getElementById('info-modal-title');
            const content = document.getElementById('info-modal-content');
            modal.classList.remove('hidden');

            if (type === 'about') {
                title.textContent = 'Biz Kimiz & Vizyonumuz';
                content.innerHTML = 
                    '<p><strong>OYVER PRO</strong>, manipülasyondan uzak, ölçülebilir ve liyakat tabanlı bir kolektif kamuoyu tahmin terminalidir.</p>' +
                    '<p>Katılımcılar cebinden 1 TL dahi çıkmadan dağıtılan sanal KOR puanlarıyla öngörülerini bildirir. AMM fiyatlama motoru, toplumun gerçek beklenti eğrisini anlık hesaplar.</p>' +
                    '<div class="p-3 rounded-xl bg-slate-950 border border-slate-800 text-xs"><strong>Önemli Kural:</strong> Platformda bahis veya kumar unsuru bulunmaz; puanların nakit karşılığı ve para çekimi yoktur. En büyük ödül, isabetli kararlarla <strong>Ordinaryüs</strong> kademesine yükselmektir.</div>';
            } else if (type === 'b2b') {
                title.textContent = 'B2B Veri Terminali & %93.2 Güven Endeksi';
                content.innerHTML = 
                    '<div class="p-4 bg-emerald-950/20 border border-emerald-500/40 rounded-2xl space-y-2 mb-3">' +
                        '<div class="flex justify-between items-center">' +
                            '<span class="font-black text-emerald-400 text-sm">OYVER Güven Endeksi</span>' +
                            '<span class="text-base font-black text-emerald-300">%93.2 İsabet</span>' +
                        '</div>' +
                        '<div class="w-full bg-slate-950 rounded-full h-2.5 overflow-hidden flex border border-slate-800">' +
                            '<div class="bg-gradient-to-r from-emerald-500 to-teal-400 h-full" style="width: 93.2%"></div>' +
                        '</div>' +
                        '<p class="text-[11px] text-slate-300 leading-relaxed">OYVER kullanıcılarının geçmişte %70 olasılık verdiği 100 olayın 71\'i resmi bültenlerle doğrulanmıştır. Sapma payımız uluslararası araştırma normlarının altındadır.</p>' +
                    '</div>' +
                    '<p>Kurumunuz için özel pazar açma ve kohort verisi entegrasyonu için formu doldurabilirsiniz:</p>' +
                    '<div class="space-y-2 mt-3">' +
                        '<input type="text" id="b2b-company" placeholder="Kurum / Şirket Adı" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white">' +
                        '<input type="email" id="b2b-email" placeholder="Kurumsal E-posta" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white">' +
                        '<textarea id="b2b-msg" rows="2" placeholder="Talebiniz..." class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white"></textarea>' +
                        '<button onclick="submitContactForm(\\'B2B_SALES\\')" class="w-full py-2.5 bg-indigo-600 rounded-xl font-bold text-white transition">Kurumsal İletişim Başlat</button>' +
                    '</div>';
            } else if (type === 'contact') {
                title.textContent = 'Bize Ulaşın & Destek';
                content.innerHTML = 
                    '<p>Ekibimize doğrudan mesaj gönderebilirsiniz:</p>' +
                    '<div class="space-y-2 mt-3">' +
                        '<input type="text" id="cnt-name" placeholder="Adınız" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white">' +
                        '<input type="email" id="cnt-email" placeholder="E-posta" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white">' +
                        '<textarea id="cnt-msg" rows="3" placeholder="Mesajınız..." class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white"></textarea>' +
                        '<button onclick="submitContactForm(\\'GENERAL\\')" class="w-full py-2.5 bg-indigo-600 rounded-xl font-bold text-white transition">Mesajı İlet</button>' +
                    '</div>';
            } else if (type === 'rules') {
                title.textContent = 'Liyakat ve Derece Kuralları';
                content.innerHTML = 
                    '<p>1. <strong>Araştırmacı:</strong> Platforma yeni katılan analist adayı.</p>' +
                    '<p>2. <strong>Uzman Analist:</strong> 10+ sonuçlanmış pazarda isabet sağlayanlar.</p>' +
                    '<p>3. <strong>Doçent:</strong> FRS puanı 3.000\'i aşan kalibre öngörücüler.</p>' +
                    '<p>4. <strong>Profesör:</strong> Brier sapması düşük, uzun süreli istikrar yakalayanlar.</p>' +
                    '<p>5. <strong>Ordinaryüs:</strong> Türkiye genelinde ilk 10\'a giren en isabetli beyinler.</p>';
            }
        };

        window.closeInfoModal = () => document.getElementById('info-modal').classList.add('hidden');

        window.submitContactForm = async function(type) {
            const isB2B = type === 'B2B_SALES';
            const name = isB2B ? document.getElementById('b2b-company').value : document.getElementById('cnt-name').value;
            const email = isB2B ? document.getElementById('b2b-email').value : document.getElementById('cnt-email').value;
            const message = isB2B ? document.getElementById('b2b-msg').value : document.getElementById('cnt-msg').value;

            const res = await fetch('/api/contact', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ type, name, email, company: isB2B ? name : null, message })
            }).then(r => r.json());

            showToast(res.message || 'Talebiniz iletildi.');
            closeInfoModal();
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
                        '<span class="w-6 h-6 rounded-full bg-slate-900 border border-slate-800 flex items-center justify-center font-black text-[10px] text-amber-400">#' + u.rank + '</span>' +
                        '<div>' +
                            '<strong class="text-white">' + escapeHtml(u.name) + '</strong>' +
                            '<span class="text-[9px] text-indigo-400 font-bold ml-1.5 px-1.5 py-0.5 rounded bg-indigo-500/10 border border-indigo-500/20">' + escapeHtml(u.tier) + '</span>' +
                            '<span class="text-[9px] text-emerald-400 ml-1">[' + escapeHtml(u.archetype) + ']</span>' +
                        '</div>' +
                    '</div>' +
                    '<div class="text-right font-black text-indigo-400">' + u.frsScore.toLocaleString('tr-TR') + ' FRS</div>';
                list.appendChild(item);
            });
            document.getElementById('leaderboard-modal').classList.remove('hidden');
        };
        window.closeLeaderboard = () => document.getElementById('leaderboard-modal').classList.add('hidden');

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
                    card.className = 'glass-card rounded-2xl p-4 flex items-center justify-between gap-4 text-xs';
                    card.innerHTML = 
                        '<div><h4 class="font-bold text-white">' + escapeHtml(item.question) + '</h4><div class="text-slate-400 text-[11px]">Tercih: ' + item.outcome + ' | Değer: <strong class="text-amber-300">' + item.currentSellValue + ' KOR</strong></div></div>' +
                        '<button onclick="sellPosition(\\'' + item.marketId + '\\', \\'' + item.outcome + '\\', \\'' + item.shares + '\\')" class="bg-rose-950/40 hover:bg-rose-600 border border-rose-500/30 text-rose-300 hover:text-white px-3 py-1.5 rounded-xl font-bold transition">Sat</button>';
                    actList.appendChild(card);
                });
            }
        }

        window.sellPosition = async function(marketId, outcome, shares) {
            const res = await fetch('/api/trade/sell', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ marketId, outcome, sharesToSell: shares })
            }).then(r => r.json());
            currentBalance = res.newBalance;
            document.getElementById('user-balance').textContent = currentBalance.toLocaleString('tr-TR') + ' KOR';
            showToast('💰 Pozisyon satıldı: +' + res.payoutKor + ' KOR');
            loadPortfolio();
        };

        // OYLA (ZARLA)
        async function loadZarla() {
            const res = await fetch('/api/zarla').then(r => r.json());
            const list = document.getElementById('zarla-list');
            list.innerHTML = '';
            res.polls.forEach(p => {
                const card = document.createElement('div');
                card.className = 'glass-card rounded-2xl p-4 shadow-lg space-y-3';
                card.innerHTML = 
                    '<div class="flex justify-between items-center text-xs"><span class="px-2 py-0.5 rounded text-[9px] font-black uppercase bg-indigo-500/20 text-indigo-300">' + escapeHtml(p.category) + '</span><span class="text-slate-500">' + p.totalVotes + ' Oy</span></div>' +
                    '<h3 class="text-xs sm:text-sm font-bold text-white">' + escapeHtml(p.question) + '</h3>' +
                    '<div class="grid grid-cols-2 gap-2 text-xs font-bold">' +
                        '<button onclick="voteZarla(\\'' + p.id + '\\', \\'A\\')" class="py-2 bg-slate-950 hover:bg-slate-800 border border-slate-800 rounded-xl text-slate-200 transition">' + escapeHtml(p.option_a) + ' (%' + p.pctA + ')</button>' +
                        '<button onclick="voteZarla(\\'' + p.id + '\\', \\'B\\')" class="py-2 bg-slate-950 hover:bg-slate-800 border border-slate-800 rounded-xl text-slate-200 transition">' + escapeHtml(p.option_b) + ' (%' + p.pctB + ')</button>' +
                    '</div>';
                list.appendChild(card);
            });
        }

        window.voteZarla = async function(pollId, choice) {
            await fetch('/api/zarla/vote', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ pollId, choice }) });
            showToast('🗳️ Oyunuz kaydedildi!');
            loadZarla();
        };

        // KATEGORİ DEĞİŞİMİ
        document.querySelectorAll('.cat-btn').forEach(btn => {
            btn.onclick = (e) => {
                document.querySelectorAll('.cat-btn').forEach(b => b.className = 'cat-btn bg-slate-900 hover:bg-slate-800 text-slate-300 px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap');
                e.target.className = 'cat-btn active bg-indigo-600 text-white px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap';
                activeCategory = e.target.getAttribute('data-cat');
                activeSubCategory = 'ALL';

                const subBox = document.getElementById('sub-cat-container');
                if (activeCategory === 'SPOR') {
                    subBox.classList.remove('hidden');
                    subBox.innerHTML = 
                        '<span class="text-[10px] text-slate-500 font-bold uppercase mr-1">Dallar:</span>' +
                        '<button onclick="filterSubCat(\\'ALL\\')" class="sub-btn active px-2.5 py-1 rounded-lg bg-slate-800 text-white text-[11px] font-bold">Tümü</button>' +
                        '<button onclick="filterSubCat(\\'Futbol\\')" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">⚽ Futbol</button>' +
                        '<button onclick="filterSubCat(\\'Basketbol\\')" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">🏀 Basketbol</button>' +
                        '<button onclick="filterSubCat(\\'Formula 1\\')" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">🏎️ Formula 1</button>';
                } else {
                    subBox.classList.add('hidden');
                }
                renderMarkets();
            };
        });

        window.filterSubCat = function(sub) {
            activeSubCategory = sub;
            document.querySelectorAll('.sub-btn').forEach(b => b.className = 'sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold');
            event.target.className = 'sub-btn active px-2.5 py-1 rounded-lg bg-slate-800 text-white text-[11px] font-bold';
            renderMarkets();
        };

        window.promptSwitchUser = async () => {
            const name = prompt('Giriş yapılacak kullanıcı adını girin (Örn: Ahmet_Analist, Ece_Hoca, LeisanB):');
            if (!name) return;
            const res = await fetch('/api/auth/login-mock', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ username: name }) }).then(r => r.json());
            if (res.success) location.reload();
        };

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
                document.getElementById('user-balance').textContent = currentBalance.toLocaleString('tr-TR') + ' KOR';
                document.getElementById('user-streak').textContent = (me.streak || 1) + 'g';
                document.getElementById('user-tier-badge').textContent = me.tier || 'Doçent';
                document.getElementById('dd-username').textContent = me.username || 'LeisanB';
                document.getElementById('dd-tier').textContent = 'Kademesi: ' + (me.tier || 'Doçent');
                document.getElementById('dd-archetype').textContent = me.personality_archetype || 'Stratejist';

                // Profil Adımı Kontrolü
                if (!me.kvkk_accepted) {
                    setTimeout(() => openProfileStepModal(), 600);
                }

                loadHeroDuels();
                const res = await fetch('/api/markets').then(r => r.json());
                markets = res.markets || [];
                renderMarkets();
            } catch(e) {
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
    console.log(`[OYVER PRO] v1.3 P2P Arena & Profiling Aktif: ${address}`);
});
