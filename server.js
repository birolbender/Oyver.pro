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
// 2. VERİTABANI BAĞLANTISI VE ÇİFT TARAFLI DEFTER
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
        console.info('[DATABASE] Şema, zaman serisi, topluluk ve kohort tabloları doğrulanıyor...');
        
        await client.query(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`);

        // 1. ÖNCE TABLOLAR OLUŞTURULUR
        await client.query(`
            CREATE TABLE IF NOT EXISTS users (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                email VARCHAR(255) UNIQUE NOT NULL,
                username VARCHAR(64) UNIQUE NOT NULL,
                password_hash VARCHAR(255) DEFAULT 'OAUTH_MOCK',
                balance_kor NUMERIC(24,6) NOT NULL DEFAULT 14500.000000,
                streak INT NOT NULL DEFAULT 1,
                quests_today INT NOT NULL DEFAULT 0,
                quest_rewarded BOOLEAN NOT NULL DEFAULT FALSE,
                tier VARCHAR(16) NOT NULL DEFAULT 'ANALYST',
                monthly_kor_purchased NUMERIC(24,6) NOT NULL DEFAULT 0,
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
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
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

            CREATE TABLE IF NOT EXISTS zarla_polls (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                question TEXT NOT NULL,
                option_a TEXT NOT NULL,
                option_b TEXT NOT NULL,
                votes_a INT NOT NULL DEFAULT 0,
                votes_b INT NOT NULL DEFAULT 0,
                category VARCHAR(64) NOT NULL DEFAULT 'GÜNDEM',
                is_sponsored BOOLEAN NOT NULL DEFAULT FALSE,
                sponsor_brand VARCHAR(64),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS sessions (
                token CHAR(64) PRIMARY KEY,
                user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);

        // 2. KRİTİK MİGRASYON KALKANI (Eski tablolarda eksik olabilecek TÜM kolonları zorla ekler)
        console.info('[DATABASE] Sütun migrasyonları zorlanıyor...');

        // MARKETS tablosu onarımı
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
            ALTER TABLE markets ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
        `);

        // USERS tablosu onarımı
        await client.query(`
            DO $$ 
            BEGIN 
                IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='password_hash') THEN 
                    ALTER TABLE users ALTER COLUMN password_hash DROP NOT NULL;
                    ALTER TABLE users ALTER COLUMN password_hash SET DEFAULT 'OAUTH_MOCK';
                END IF;
            END $$;

            ALTER TABLE users ADD COLUMN IF NOT EXISTS balance_kor NUMERIC(24,6) NOT NULL DEFAULT 14500.000000;
            ALTER TABLE users ADD COLUMN IF NOT EXISTS streak INT NOT NULL DEFAULT 1;
            ALTER TABLE users ADD COLUMN IF NOT EXISTS quests_today INT NOT NULL DEFAULT 0;
            ALTER TABLE users ADD COLUMN IF NOT EXISTS quest_rewarded BOOLEAN NOT NULL DEFAULT FALSE;
            ALTER TABLE users ADD COLUMN IF NOT EXISTS tier VARCHAR(16) NOT NULL DEFAULT 'ANALYST';
            ALTER TABLE users ADD COLUMN IF NOT EXISTS monthly_kor_purchased NUMERIC(24,6) NOT NULL DEFAULT 0;
            ALTER TABLE users ADD COLUMN IF NOT EXISTS birth_year INT;
            ALTER TABLE users ADD COLUMN IF NOT EXISTS education_level VARCHAR(32);
            ALTER TABLE users ADD COLUMN IF NOT EXISTS employment_status VARCHAR(32);
            ALTER TABLE users ADD COLUMN IF NOT EXISTS industry VARCHAR(64);
            ALTER TABLE users ADD COLUMN IF NOT EXISTS city VARCHAR(32);
            ALTER TABLE users ADD COLUMN IF NOT EXISTS onboarding_completed BOOLEAN NOT NULL DEFAULT FALSE;
        `);

        // POSITIONS tablosu onarımı
        await client.query(`
            ALTER TABLE positions ADD COLUMN IF NOT EXISTS shares NUMERIC(30,12) NOT NULL DEFAULT 0;
            ALTER TABLE positions ADD COLUMN IF NOT EXISTS total_invested NUMERIC(24,6) NOT NULL DEFAULT 0;
            ALTER TABLE positions ADD COLUMN IF NOT EXISTS entry_prob NUMERIC(6,2) NOT NULL DEFAULT 50.00;
            ALTER TABLE positions ADD COLUMN IF NOT EXISTS realized_pnl NUMERIC(24,6) NOT NULL DEFAULT 0;
            ALTER TABLE positions ADD COLUMN IF NOT EXISTS is_settled BOOLEAN NOT NULL DEFAULT FALSE;
            ALTER TABLE positions ADD COLUMN IF NOT EXISTS settlement_payout NUMERIC(24,6) NOT NULL DEFAULT 0;
            ALTER TABLE positions ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
        `);

        // Tekillik (Unique) İndeksleri
        await client.query(`
            CREATE UNIQUE INDEX IF NOT EXISTS uq_markets_slug ON markets (slug);
            CREATE UNIQUE INDEX IF NOT EXISTS uq_positions_user_market_outcome ON positions (user_id, market_id, outcome);
        `);

        for (const code of ['1000', '3000', '4000', '5000']) {
            await client.query(`INSERT INTO accounts (code) VALUES ($1) ON CONFLICT DO NOTHING`, [code]);
        }

        // Demo Kullanıcıları
        const checkUsers = await client.query(`SELECT count(*) FROM users`);
        if (parseInt(checkUsers.rows[0].count, 10) === 0) {
            console.info('[DATABASE] Kullanıcı tohumları yükleniyor...');
            await client.query(`
                INSERT INTO users (id, email, username, password_hash, balance_kor, streak, quests_today, tier, onboarding_completed, birth_year, education_level, industry, city) VALUES
                ('11111111-1111-1111-1111-111111111111', 'demo@oyver.pro', 'LeisanB', 'OAUTH_MOCK', 14500, 5, 1, 'ANALYST', true, 1989, 'MASTER_PHD', 'TECH', 'İstanbul'),
                ('22222222-2222-2222-2222-222222222222', 'ahmet@oyver.pro', 'Ahmet_Kahin', 'OAUTH_MOCK', 420000, 14, 3, 'PRO', true, 1984, 'BACHELOR', 'FINANCE', 'İstanbul'),
                ('33333333-3333-3333-3333-333333333333', 'ece@oyver.pro', 'Ece_Analist', 'OAUTH_MOCK', 315000, 9, 3, 'PRO', true, 1992, 'MASTER_PHD', 'FINANCE', 'Ankara'),
                ('44444444-4444-4444-4444-444444444444', 'quant@oyver.pro', 'QuantTraderTR', 'OAUTH_MOCK', 280000, 7, 2, 'ANALYST', true, 1990, 'BACHELOR', 'TECH', 'İzmir'),
                ('55555555-5555-5555-5555-555555555555', 'zeki@oyver.pro', 'Zeki_Forecaster', 'OAUTH_MOCK', 195000, 4, 1, 'OBSERVER', true, 1996, 'ASSOCIATE', 'RETAIL', 'Bursa')
                ON CONFLICT (email) DO NOTHING;
            `);
        }

        // 5 Tescilli Resmi Pazar Tohumlaması
        const officialMarkets = [
            {
                slug: 'asgari-ucret-2027',
                cat: 'EKONOMİ',
                q: '2027 Yılı Net Asgari Ücreti 35.000 TL Üzerinde Açıklanır mı?',
                desc: 'Asgari Ücret Tespit Komisyonu nihai kararının Resmi Gazete\'de yayımlanan tutarı esas alınacaktır.',
                srcName: 'Çalışma Bakanlığı / Resmi Gazete',
                srcUrl: 'https://resmigazete.gov.tr',
                closing: '31 Aralık 2026',
                yesR: 12000,
                noR: 8000,
                history: [52, 54, 53, 56, 58, 60, 60]
            },
            {
                slug: 'tcmb-faiz-2026',
                cat: 'EKONOMİ',
                q: 'TCMB Politika Faizini 2026 Yıl Sonuna Kadar %30 veya Altına İndirir mi?',
                desc: 'TCMB Para Politikası Kurulu (PPK) Aralık 2026 toplantı kararında açıklanan 1 haftalık repo faizi esas alınır.',
                srcName: 'TCMB PPK Karar Metni',
                srcUrl: 'https://tcmb.gov.tr',
                closing: '24 Aralık 2026',
                yesR: 9000,
                noR: 11000,
                history: [35, 38, 42, 40, 44, 46, 45]
            },
            {
                slug: 'bist-100-2026',
                cat: 'FİNANS',
                q: 'BIST 100 Endeksi 2026 Son Çeyreğini 12.000 Puan Üzerinde Kapatır mı?',
                desc: 'Borsa İstanbul 31 Aralık 2026 seans kapanışındaki resmi BIST 100 kapanış endeksi dikkate alınır.',
                srcName: 'Borsa İstanbul Resmi Bülteni',
                srcUrl: 'https://borsaistanbul.com',
                closing: '31 Aralık 2026',
                yesR: 10000,
                noR: 10000,
                history: [48, 50, 49, 52, 50, 51, 50]
            },
            {
                slug: 'turksat-6a-ticari',
                cat: 'TEKNOLOJİ',
                q: 'TÜRKSAT 6A Uydusu 2026 Yılında Tam Kapasite Ticari Hizmete Başlar mı?',
                desc: 'Ulaştırma ve Altyapı Bakanlığı resmi basın bülteninde uydunun ticari hizmete girdiği teyit edilmelidir.',
                srcName: 'Ulaştırma Bakanlığı Basın Açıklaması',
                srcUrl: 'https://uab.gov.tr',
                closing: '15 Kasım 2026',
                yesR: 14000,
                noR: 6000,
                history: [62, 65, 68, 67, 70, 72, 70]
            },
            {
                slug: 'turizm-ziyaretci-2026',
                cat: 'KÜLTÜR & YAŞAM',
                q: '2026 Yılında Türkiye\'ye Gelen Yabancı Ziyaretçi Sayısı 60 Milyonu Aşar mı?',
                desc: 'TÜİK tarafından Ocak 2027\'de yayımlanacak 2026 yılı dördüncü çeyrek turizm istatistikleri bülteni esas alınır.',
                srcName: 'TÜİK Turizm İstatistikleri',
                srcUrl: 'https://tuik.gov.tr',
                closing: '31 Ocak 2027',
                yesR: 13000,
                noR: 7000,
                history: [58, 60, 62, 61, 64, 66, 65]
            }
        ];

        for (const item of officialMarkets) {
            const mRes = await client.query(`
                INSERT INTO markets (slug, category, question, description, source_name, source_url, closing_date)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (slug) DO UPDATE 
                SET question = EXCLUDED.question, category = EXCLUDED.category, description = EXCLUDED.description, source_name = EXCLUDED.source_name
                RETURNING id
            `, [item.slug, item.cat, item.q, item.desc, item.srcName, item.srcUrl, item.closing]);

            const mId = mRes.rows[0].id;
            await client.query(`INSERT INTO accounts (code, market_id) VALUES ('2100', $1) ON CONFLICT DO NOTHING`, [mId]);
            await client.query(`
                INSERT INTO amm_state (market_id, yes_reserve, no_reserve)
                VALUES ($1, $2, $3)
                ON CONFLICT (market_id) DO NOTHING
            `, [mId, item.yesR, item.noR]);

            const hCheck = await client.query(`SELECT count(*) FROM market_price_history WHERE market_id = $1`, [mId]);
            if (parseInt(hCheck.rows[0].count, 10) === 0) {
                const now = Date.now();
                for (let i = 0; i < item.history.length; i++) {
                    const daysAgo = (6 - i) * 86400000;
                    await client.query(`
                        INSERT INTO market_price_history (market_id, prob_yes, pool_total, created_at)
                        VALUES ($1, $2, 20000, to_timestamp($3))
                    `, [mId, item.history[i], (now - daysAgo) / 1000]);
                }
            }
        }

        // Liyakat Ligi İçin Tarihsel Sonuçlanmış Pozisyonlar
        const posCheck = await client.query(`SELECT count(*) FROM positions WHERE is_settled = true`);
        if (parseInt(posCheck.rows[0].count, 10) === 0) {
            console.info('[DATABASE] Kahin Ligi için tarihsel sonuçlanmış pozisyonlar tohumlanıyor...');
            const mFirst = (await client.query(`SELECT id FROM markets LIMIT 1`)).rows[0].id;
            await client.query(`
                INSERT INTO positions (market_id, user_id, outcome, shares, total_invested, entry_prob, realized_pnl, is_settled, settlement_payout) VALUES
                ($1, '22222222-2222-2222-2222-222222222222', 'YES', 15000, 9000, 60.00, 6000, true, 15000),
                ($1, '33333333-3333-3333-3333-333333333333', 'YES', 12000, 7800, 65.00, 4200, true, 12000),
                ($1, '44444444-4444-4444-4444-444444444444', 'NO', 8000, 5000, 40.00, -5000, true, 0),
                ($1, '55555555-5555-5555-5555-555555555555', 'YES', 6000, 3900, 65.00, 2100, true, 6000)
                ON CONFLICT DO NOTHING;
            `, [mFirst]);
        }

        // Topluluk Yorumları
        const commCheck = await client.query(`SELECT count(*) FROM comments`);
        if (parseInt(commCheck.rows[0].count, 10) === 0) {
            const mAsgari = (await client.query(`SELECT id FROM markets WHERE slug = 'asgari-ucret-2027'`)).rows[0]?.id;
            if (mAsgari) {
                await client.query(`
                    INSERT INTO comments (market_id, user_id, content, outcome_at_time, shares_at_time, upvotes) VALUES
                    ($1, '22222222-2222-2222-2222-222222222222', 'Enflasyon beklentisi ve refah payı düzenlemesi dikkate alındığında 35.000 TL psikolojik sınırının aşılması kuvvetle muhtemel.', 'YES', 1500, 18),
                    ($1, '33333333-3333-3333-3333-333333333333', 'İşveren maliyeti tarafındaki vergi dilimi ayarlamalarına bakılırsa bu rakam 33.500 TL civarında kalabilir.', 'NO', 800, 12)
                `, [mAsgari]);
            }
        }

        // ZARLA Günün Nabzı
        const checkZarla = await client.query(`SELECT count(*) FROM zarla_polls`);
        if (parseInt(checkZarla.rows[0].count, 10) === 0) {
            await client.query(`
                INSERT INTO zarla_polls (question, option_a, option_b, votes_a, votes_b, category, is_sponsored, sponsor_brand) VALUES
                ('Kahvaltıda çikolata kreması tercihiniz hangisi?', 'Nutella', 'Sarelle / Banada', 1240, 890, 'TÜKETİCİ', true, 'Nutella vs Yerli'),
                ('2028 Cumhurbaşkanlığı seçiminde Erdoğan aday olmalı mı?', 'Aday Olmalı', 'Aday Olmamalı', 3420, 3180, 'SİYASET', false, null),
                ('Bu pazar seçim olsa tercihiniz hangi blok olurdu?', 'Cumhur İttifakı', 'Muhalefet Bloğu', 2900, 3110, 'SİYASET', false, null),
                ('Kredi kartı limitlerine yeni yasal sınırlama getirilmeli mi?', 'Evet, Sınır Getirilsin', 'Hayır, Müdahale Edilmesin', 1840, 920, 'EKONOMİ', false, null),
                ('Yapay zeka dizi ve film senaryoları yazmalı mı?', 'Evet, Destekliyorum', 'Hayır, Sanat İnsana Aittir', 780, 1640, 'KÜLTÜR', false, null)
            `);
        }

        console.info('[DATABASE] Final Ready Sürümü Başarıyla Hazırlandı.');
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

app.get('/health', async () => ({ status: 'UP', version: 'v1.0-FINAL', timestamp: new Date().toISOString() }));

app.get('/api/me', async (req) => {
    const r = await pool.query(`
        SELECT id, username, email, balance_kor, streak, quests_today, quest_rewarded,
               tier, monthly_kor_purchased, onboarding_completed, birth_year, education_level, industry, city
        FROM users WHERE id = $1
    `, [req.userId]);
    return r.rows[0] || {};
});

// ONBOARDING
app.post('/api/onboarding', async (req, rep) => {
    const { birthYear, educationLevel, employmentStatus, industry, city } = req.body || {};
    if (!birthYear || !educationLevel || !city) return rep.status(400).send({ error: 'Eksik bilgi' });

    try {
        const result = await runInTransaction(async (c) => {
            const ur = await c.query(`SELECT onboarding_completed, balance_kor FROM users WHERE id = $1 FOR UPDATE`, [req.userId]);
            let rewardBonus = new Decimal(0);
            if (!ur.rows[0].onboarding_completed) {
                rewardBonus = new Decimal(1500);
                await LedgerEngine.recordEntry(c, 'ONBOARDING_BONUS', req.userId, null, [
                    { accountCode: '1000', debit: rewardBonus, credit: new Decimal(0) },
                    { accountCode: '2000', userId: req.userId, debit: new Decimal(0), credit: rewardBonus }
                ]);
            }

            const updated = await c.query(`
                UPDATE users 
                SET birth_year = $1, education_level = $2, employment_status = $3, industry = $4, city = $5,
                    onboarding_completed = true, balance_kor = balance_kor + $6
                WHERE id = $7
                RETURNING balance_kor, onboarding_completed, tier
            `, [birthYear, educationLevel, employmentStatus || 'PRIVATE_SECTOR', industry || 'OTHER', city, rewardBonus.toFixed(6), req.userId]);

            return {
                newBalance: MoneyMath.roundDown(updated.rows[0].balance_kor, 0).toNumber(),
                bonusAdded: rewardBonus.gt(0)
            };
        });
        return rep.send({ success: true, ...result });
    } catch (e) {
        return rep.status(400).send({ error: e.message });
    }
});

// PAZARLAR
app.get('/api/markets', async () => {
    const r = await pool.query(`
        SELECT m.*, a.yes_reserve, a.no_reserve 
        FROM markets m 
        JOIN amm_state a ON m.id = a.market_id 
        ORDER BY m.created_at ASC
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

// TEKİL PAZAR DETAYI & ZAMAN SERİSİ
app.get('/api/markets/:slug', async (req, rep) => {
    const { slug } = req.params;
    const r = await pool.query(`
        SELECT m.*, a.yes_reserve, a.no_reserve 
        FROM markets m 
        JOIN amm_state a ON m.id = a.market_id 
        WHERE m.slug = $1
    `, [slug]);

    if (r.rows.length === 0) return rep.status(404).send({ error: 'Pazar bulunamadı.' });

    const m = r.rows[0];
    const y = new Decimal(m.yes_reserve);
    const n = new Decimal(m.no_reserve);
    const probYes = Math.round(n.div(y.plus(n)).mul(100).toNumber());
    const poolTotal = Math.round(y.plus(n).toNumber());

    const hr = await pool.query(`
        SELECT prob_yes, pool_total, to_char(created_at, 'DD Mon HH24:MI') as time_label 
        FROM market_price_history 
        WHERE market_id = $1 
        ORDER BY created_at ASC
    `, [m.id]);

    return {
        market: {
            ...m,
            probYes,
            probNo: 100 - probYes,
            poolTotal: poolTotal.toLocaleString('tr-TR')
        },
        history: hr.rows
    };
});

// B2B KOHORT ANALİTİĞİ
app.get('/api/markets/:slug/cohorts', async (req, rep) => {
    const { slug } = req.params;
    const mRes = await pool.query(`SELECT id FROM markets WHERE slug = $1`, [slug]);
    if (mRes.rows.length === 0) return rep.status(404).send({ error: 'Pazar bulunamadı' });
    const marketId = mRes.rows[0].id;

    const eduRes = await pool.query(`
        SELECT u.education_level, p.outcome, COUNT(p.id) as vote_count, SUM(p.shares) as total_shares
        FROM positions p
        JOIN users u ON p.user_id = u.id
        WHERE p.market_id = $1 AND p.shares > 0
        GROUP BY u.education_level, p.outcome
    `, [marketId]);

    const ageRes = await pool.query(`
        SELECT 
            CASE 
                WHEN (2026 - u.birth_year) < 30 THEN 'Genç (18-29)'
                WHEN (2026 - u.birth_year) BETWEEN 30 AND 44 THEN 'Orta Yaş (30-44)'
                ELSE 'Deneyimli (45+)'
            END as age_group,
            p.outcome,
            COUNT(p.id) as vote_count
        FROM positions p
        JOIN users u ON p.user_id = u.id
        WHERE p.market_id = $1 AND p.shares > 0 AND u.birth_year IS NOT NULL
        GROUP BY age_group, p.outcome
    `, [marketId]);

    return {
        educationCohorts: eduRes.rows,
        ageCohorts: ageRes.rows
    };
});

// GEREKÇELİ YORUMLAR (INSIGHTS)
app.get('/api/markets/:slug/comments', async (req, rep) => {
    const { slug } = req.params;
    const mRes = await pool.query(`SELECT id FROM markets WHERE slug = $1`, [slug]);
    if (mRes.rows.length === 0) return rep.status(404).send({ error: 'Pazar bulunamadı' });

    const r = await pool.query(`
        SELECT c.*, u.username, u.tier, u.streak,
               to_char(c.created_at, 'DD Mon YYYY, HH24:MI') as time_formatted
        FROM comments c
        JOIN users u ON c.user_id = u.id
        WHERE c.market_id = $1
        ORDER BY c.upvotes DESC, c.created_at DESC
    `, [mRes.rows[0].id]);

    return { comments: r.rows };
});

app.post('/api/markets/:slug/comments', async (req, rep) => {
    const { slug } = req.params;
    const { content } = req.body || {};
    if (!content || content.trim().length < 5) {
        return rep.status(400).send({ error: 'Analiziniz en az 5 karakter olmalıdır.' });
    }

    const mRes = await pool.query(`SELECT id FROM markets WHERE slug = $1`, [slug]);
    if (mRes.rows.length === 0) return rep.status(404).send({ error: 'Pazar bulunamadı' });
    const marketId = mRes.rows[0].id;

    const posRes = await pool.query(`
        SELECT outcome, shares FROM positions WHERE market_id = $1 AND user_id = $2 AND shares > 0 ORDER BY updated_at DESC LIMIT 1
    `, [marketId, req.userId]);

    const outcome = posRes.rows.length > 0 ? posRes.rows[0].outcome : null;
    const shares = posRes.rows.length > 0 ? Math.round(parseFloat(posRes.rows[0].shares)) : 0;

    const ins = await pool.query(`
        INSERT INTO comments (market_id, user_id, content, outcome_at_time, shares_at_time)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id, created_at
    `, [marketId, req.userId, content.trim(), outcome, shares]);

    return { success: true, commentId: ins.rows[0].id };
});

app.post('/api/comments/:id/upvote', async (req, rep) => {
    const { id } = req.params;
    try {
        await pool.query(`INSERT INTO comment_votes (comment_id, user_id) VALUES ($1, $2)`, [id, req.userId]);
        await pool.query(`UPDATE comments SET upvotes = upvotes + 1 WHERE id = $1`, [id]);
        return { success: true };
    } catch (e) {
        return rep.status(400).send({ error: 'Bu analizi zaten oyladınız.' });
    }
});

// CSV DIŞA AKTARIMI
app.get('/api/markets/:slug/export', async (req, rep) => {
    const { slug } = req.params;
    const mRes = await pool.query(`SELECT * FROM markets WHERE slug = $1`, [slug]);
    if (mRes.rows.length === 0) return rep.status(404).send({ error: 'Pazar bulunamadı' });
    const m = mRes.rows[0];

    const pr = await pool.query(`
        SELECT p.outcome, p.shares, p.total_invested, u.birth_year, u.education_level, u.industry, u.city
        FROM positions p
        JOIN users u ON p.user_id = u.id
        WHERE p.market_id = $1 AND p.shares > 0
    `, [m.id]);

    let csv = 'Oylama,Tercih,Pay,Yatırılan_KOR,Dogum_Yili,Egitim,Sektor,Sehir\n';
    pr.rows.forEach(r => {
        csv += `"${m.question}","${r.outcome}",${r.shares},${r.total_invested},"${r.birth_year || ''}","${r.education_level || ''}","${r.industry || ''}","${r.city || ''}"\n`;
    });

    rep.header('Content-Type', 'text/csv');
    rep.header('Content-Disposition', `attachment; filename="oyver_data_${slug}.csv"`);
    return csv;
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
        return {
            action: 'SELL',
            netPayout: Math.round(calc.netPayout.toNumber()).toLocaleString('tr-TR') + ' KOR',
            avgPrice: calc.avgPrice,
            priceImpact: calc.priceImpact
        };
    } else {
        const amt = new Decimal(amountKor || 0);
        if (amt.lte(0)) return rep.status(400).send({ error: 'Geçersiz tutar' });

        const calc = AMMEngine.calculateBuy(isYes ? amm : { yesReserve: amm.noReserve, noReserve: amm.yesReserve }, amt, new Decimal(0.02));
        return {
            action: 'BUY',
            sharesOut: calc.sharesOut.toFixed(2),
            avgPrice: calc.avgPrice,
            priceImpact: calc.priceImpact,
            targetPayout: Math.round(calc.sharesOut.toNumber()).toLocaleString('tr-TR') + ' KOR'
        };
    }
});

// TAHMİN ALIM (BUY)
app.post('/api/trade/predict', async (req, rep) => {
    const { marketId, outcome, amountKor } = req.body || {};
    const amt = new Decimal(amountKor || 0);
    if (!marketId || !outcome || amt.lte(0)) return rep.status(400).send({ error: 'Geçersiz parametre.' });

    try {
        const result = await runInTransaction(async (c) => {
            const mCheck = await c.query(`SELECT status FROM markets WHERE id = $1`, [marketId]);
            if (mCheck.rows.length === 0 || mCheck.rows[0].status !== 'TRADING') throw new Error('Bu oylama işleme kapalıdır.');

            const ur = await c.query(
                `UPDATE users SET balance_kor = balance_kor - $1 WHERE id = $2 AND balance_kor >= $1 RETURNING balance_kor, quests_today, quest_rewarded`,
                [amt.toFixed(6), req.userId]
            );
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

            let quests = ur.rows[0].quests_today + 1;
            let rewarded = ur.rows[0].quest_rewarded;
            let currentBal = new Decimal(ur.rows[0].balance_kor);
            let bonusAdded = false;

            if (quests >= 3 && !rewarded) {
                currentBal = currentBal.plus(150);
                rewarded = true;
                bonusAdded = true;
                await LedgerEngine.recordEntry(c, 'DAILY_QUEST_REWARD', req.userId, null, [
                    { accountCode: '1000', debit: new Decimal(150), credit: new Decimal(0) },
                    { accountCode: '2000', userId: req.userId, debit: new Decimal(0), credit: new Decimal(150) }
                ]);
            }

            await c.query(`UPDATE users SET quests_today = $1, quest_rewarded = $2, balance_kor = $3 WHERE id = $4`, [quests, rewarded, currentBal.toFixed(6), req.userId]);

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

        return rep.send({ success: true, ...result });
    } catch (e) {
        return rep.status(400).send({ error: e.message });
    }
});

// ERKEN ÇIKIŞ (SELL)
app.post('/api/trade/sell', async (req, rep) => {
    const { marketId, outcome, sharesToSell } = req.body || {};
    const sIn = new Decimal(sharesToSell || 0);
    if (!marketId || !outcome || sIn.lte(0)) return rep.status(400).send({ error: 'Geçersiz pay.' });

    try {
        const result = await runInTransaction(async (c) => {
            const mCheck = await c.query(`SELECT status FROM markets WHERE id = $1`, [marketId]);
            if (mCheck.rows.length === 0 || mCheck.rows[0].status !== 'TRADING') throw new Error('İşleme kapalı.');

            const pr = await c.query(
                `SELECT shares, total_invested FROM positions WHERE market_id = $1 AND user_id = $2 AND outcome = $3 FOR UPDATE`,
                [marketId, req.userId, outcome]
            );
            if (pr.rows.length === 0 || new Decimal(pr.rows[0].shares).lt(sIn)) throw new Error('Yetersiz pay.');

            const mr = await c.query(`SELECT yes_reserve, no_reserve FROM amm_state WHERE market_id = $1 FOR UPDATE`, [marketId]);
            const isYes = outcome === 'YES';
            const amm = { yesReserve: new Decimal(mr.rows[0].yes_reserve), noReserve: new Decimal(mr.rows[0].no_reserve) };

            const calc = AMMEngine.calculateSell(isYes ? amm : { yesReserve: amm.noReserve, noReserve: amm.yesReserve }, sIn, new Decimal(0.02));
            const ny = isYes ? calc.newYesReserve : calc.newNoReserve;
            const nn = isYes ? calc.newNoReserve : calc.newYesReserve;
            await c.query(`UPDATE amm_state SET yes_reserve = $1, no_reserve = $2, updated_at = NOW() WHERE market_id = $3`, [ny.toFixed(12), nn.toFixed(12), marketId]);

            const remainingShares = new Decimal(pr.rows[0].shares).minus(sIn);
            await c.query(`
                UPDATE positions SET shares = $1, realized_pnl = realized_pnl + $2, updated_at = NOW()
                WHERE market_id = $3 AND user_id = $4 AND outcome = $5
            `, [remainingShares.toFixed(6), calc.netPayout.toFixed(6), marketId, req.userId, outcome]);

            const ur = await c.query(`UPDATE users SET balance_kor = balance_kor + $1 WHERE id = $2 RETURNING balance_kor`, [calc.netPayout.toFixed(6), req.userId]);

            const feeHalf = MoneyMath.roundDown(calc.fee.div(2), 6);
            await LedgerEngine.recordEntry(c, 'PREDICTION_SELL', marketId, null, [
                { accountCode: '2100', marketId, debit: calc.grossPayout, credit: new Decimal(0) },
                { accountCode: '2000', userId: req.userId, debit: new Decimal(0), credit: calc.netPayout },
                { accountCode: '4000', debit: new Decimal(0), credit: feeHalf },
                { accountCode: '5000', debit: new Decimal(0), credit: calc.fee.minus(feeHalf) }
            ]);

            const newProb = Math.round(nn.div(ny.plus(nn)).mul(100).toNumber());
            await c.query(`INSERT INTO market_price_history (market_id, prob_yes, pool_total) VALUES ($1, $2, $3)`, [marketId, newProb, ny.plus(nn).toFixed(2)]);

            return {
                newBalance: MoneyMath.roundDown(ur.rows[0].balance_kor, 0).toNumber(),
                payoutKor: calc.netPayout.toFixed(2),
                remainingShares: remainingShares.toFixed(2),
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

        return rep.send({ success: true, ...result });
    } catch (e) {
        return rep.status(400).send({ error: e.message });
    }
});

// ÇÖZÜMLEME
app.post('/api/admin/markets/:id/resolve', async (req, rep) => {
    const { id } = req.params;
    const { outcome, proof } = req.body || {};
    if (!['YES', 'NO', 'VOID'].includes(outcome)) return rep.status(400).send({ error: 'Geçersiz sonuç.' });

    try {
        const result = await runInTransaction(async (c) => {
            const mr = await c.query(`SELECT status FROM markets WHERE id = $1 FOR UPDATE`, [id]);
            if (mr.rows.length === 0) throw new Error('Pazar bulunamadı.');
            if (mr.rows[0].status === 'SETTLED') throw new Error('Zaten sonuçlandı.');

            await c.query(`
                UPDATE markets SET status = 'SETTLED', resolved_outcome = $1, resolution_proof = $2, resolved_at = NOW() WHERE id = $3
            `, [outcome, proof || 'Resmi Doğrulama', id]);

            const pr = await c.query(`SELECT id, user_id, outcome, shares, total_invested FROM positions WHERE market_id = $1 AND shares > 0 AND is_settled = false FOR UPDATE`, [id]);
            let totalPayoutToUsers = new Decimal(0);

            for (const pos of pr.rows) {
                const s = new Decimal(pos.shares);
                const invested = new Decimal(pos.total_invested);
                let payout = outcome === 'VOID' ? invested : (pos.outcome === outcome ? s : new Decimal(0));
                let pnlDelta = payout.minus(invested);

                if (payout.gt(0)) {
                    totalPayoutToUsers = totalPayoutToUsers.plus(payout);
                    await c.query(`UPDATE users SET balance_kor = balance_kor + $1 WHERE id = $2`, [payout.toFixed(6), pos.user_id]);
                    await LedgerEngine.recordEntry(c, 'SETTLEMENT_PAYOUT', id, null, [
                        { accountCode: '2100', marketId: id, debit: payout, credit: new Decimal(0) },
                        { accountCode: '2000', userId: pos.user_id, debit: new Decimal(0), credit: payout }
                    ]);
                }

                await c.query(`
                    UPDATE positions SET is_settled = true, settlement_payout = $1, realized_pnl = realized_pnl + $2, updated_at = NOW() WHERE id = $3
                `, [payout.toFixed(6), pnlDelta.toFixed(6), pos.id]);
            }

            return { settledCount: pr.rows.length, totalPayout: totalPayoutToUsers.toFixed(2), resolvedOutcome: outcome };
        });

        broadcast('MARKET_RESOLVED', { marketId: id, outcome });
        return rep.send({ success: true, ...result });
    } catch (e) {
        return rep.status(400).send({ error: e.message });
    }
});

// FRS VE LİYAKAT MOTORU
class MeritEngine {
    static calculateScorecard(settledPositions, user) {
        const validForBrier = settledPositions.filter(p => p.resolved_outcome === 'YES' || p.resolved_outcome === 'NO');
        const totalSettled = settledPositions.length;

        if (totalSettled === 0) {
            return {
                frsScore: 0,
                brierScore: 0.25,
                brierGrade: 'Başlangıç',
                winRate: 0,
                wonCount: 0,
                totalSettled: 0,
                netPnl: 0,
                badges: ['Yeni Başlayan']
            };
        }

        let totalSquaredError = 0;
        let wonCount = 0;
        let netPnl = 0;
        const categoryMap = {};

        for (const p of validForBrier) {
            const prob = (p.outcome === 'YES' ? parseFloat(p.entry_prob) : (100 - parseFloat(p.entry_prob))) / 100.0;
            totalSquaredError += Math.pow(prob - (p.outcome === p.resolved_outcome ? 1.0 : 0.0), 2);
        }

        for (const p of settledPositions) {
            if (p.settlement_payout > 0) wonCount++;
            netPnl += parseFloat(p.realized_pnl);
            categoryMap[p.category] = (categoryMap[p.category] || 0) + (p.settlement_payout > 0 ? 1 : 0);
        }

        const brierScore = validForBrier.length > 0 ? Math.round((totalSquaredError / validForBrier.length) * 1000) / 1000 : 0.25;
        const winRate = Math.round((wonCount / totalSettled) * 100);

        const partPnl = netPnl * 0.35;
        const partBrier = (1.0 - Math.min(1.0, brierScore)) * 10000 * 0.40;
        const partVolume = Math.log10(totalSettled + 1) * 1000 * 0.15;
        const partStreak = (user.streak || 1) * 50 * 0.10;

        const frsScore = Math.max(0, Math.round(partPnl + partBrier + partVolume + partStreak));

        const badges = [];
        if (brierScore <= 0.18) badges.push('Keskin Kalibrasyon');
        if (user.streak >= 5) badges.push('Seri Kahin 🔥');
        if (categoryMap['EKONOMİ'] && categoryMap['EKONOMİ'] >= 2) badges.push('Makro Analist 📊');
        if (badges.length === 0) badges.push('Topluluk Üyesi');

        let brierGrade = 'Ortalama';
        if (brierScore <= 0.15) brierGrade = 'Kusursuz Öngörü';
        else if (brierScore <= 0.22) brierGrade = 'Yüksek Kalibrasyon';

        return { frsScore, brierScore, brierGrade, winRate, wonCount, totalSettled, netPnl: Math.round(netPnl), badges };
    }
}

// LİDERLİK TABLOSU
app.get('/api/leaderboard', async () => {
    const usersRes = await pool.query(`SELECT id, username, streak, tier, balance_kor FROM users`);
    const posRes = await pool.query(`
        SELECT p.*, m.resolved_outcome, m.category 
        FROM positions p
        JOIN markets m ON p.market_id = m.id
        WHERE p.is_settled = true
    `);

    const leaderboard = usersRes.rows.map(u => {
        const uPositions = posRes.rows.filter(p => p.user_id === u.id);
        const sc = MeritEngine.calculateScorecard(uPositions, u);
        return {
            id: u.id,
            name: u.username,
            tier: u.tier || 'ANALYST',
            streak: u.streak,
            frsScore: sc.frsScore,
            brierScore: sc.brierScore,
            brierGrade: sc.brierGrade,
            winRate: sc.winRate,
            totalSettled: sc.totalSettled,
            badges: sc.badges
        };
    });

    leaderboard.sort((a, b) => b.frsScore - a.frsScore);
    const top100 = leaderboard.slice(0, 100).map((item, idx) => ({ rank: idx + 1, ...item }));
    return { top100 };
});

// PROFİL İTİBAR KARNESİ
app.get('/api/users/:username', async (req, rep) => {
    const { username } = req.params;
    const ur = await pool.query(`
        SELECT id, username, streak, tier, city, industry, education_level, created_at 
        FROM users WHERE username ILIKE $1
    `, [username]);

    if (ur.rows.length === 0) return rep.status(404).send({ error: 'Kullanıcı bulunamadı.' });
    const user = ur.rows[0];

    const pr = await pool.query(`
        SELECT p.*, m.question, m.slug, m.category, m.resolved_outcome 
        FROM positions p
        JOIN markets m ON p.market_id = m.id
        WHERE p.user_id = $1 AND p.is_settled = true
        ORDER BY p.settlement_payout DESC
    `, [user.id]);

    const sc = MeritEngine.calculateScorecard(pr.rows, user);
    const bestCalls = pr.rows.filter(p => p.settlement_payout > 0).slice(0, 3).map(p => ({
        question: p.question,
        slug: p.slug,
        outcome: p.outcome,
        payout: Math.round(parseFloat(p.settlement_payout)),
        invested: Math.round(parseFloat(p.total_invested))
    }));

    return {
        user: {
            username: user.username,
            tier: user.tier,
            streak: user.streak,
            city: user.city || 'Belirtilmedi',
            industry: user.industry || 'Genel',
            memberSince: new Date(user.created_at).toLocaleDateString('tr-TR', { month: 'short', year: 'numeric' })
        },
        scorecard: sc,
        bestCalls
    };
});

// PORTFÖY
app.get('/api/portfolio', async (req) => {
    const r = await pool.query(`
        SELECT p.*, m.question, m.slug, m.category, m.status as market_status, m.resolved_outcome, a.yes_reserve, a.no_reserve 
        FROM positions p
        JOIN markets m ON p.market_id = m.id
        JOIN amm_state a ON m.id = a.market_id
        WHERE p.user_id = $1 AND (p.shares > 0 OR p.is_settled = true)
        ORDER BY p.updated_at DESC
    `, [req.userId]);

    const active = [], settled = [];
    r.rows.forEach(row => {
        if (row.is_settled) {
            settled.push({
                id: row.id,
                marketId: row.market_id,
                question: row.question,
                slug: row.slug,
                outcome: row.outcome,
                invested: Math.round(parseFloat(row.total_invested)),
                payout: Math.round(parseFloat(row.settlement_payout)),
                resolvedOutcome: row.resolved_outcome,
                won: row.settlement_payout > 0
            });
        } else {
            let currentSellVal = 0;
            try {
                const calc = AMMEngine.calculateSell(
                    row.outcome === 'YES' ? { yesReserve: new Decimal(row.yes_reserve), noReserve: new Decimal(row.no_reserve) } : { yesReserve: new Decimal(row.no_reserve), noReserve: new Decimal(row.yes_reserve) },
                    new Decimal(row.shares),
                    new Decimal(0.02)
                );
                currentSellVal = Math.round(calc.netPayout.toNumber());
            } catch (e) {}

            active.push({
                id: row.id,
                marketId: row.market_id,
                question: row.question,
                slug: row.slug,
                outcome: row.outcome,
                shares: parseFloat(row.shares).toFixed(2),
                invested: Math.round(parseFloat(row.total_invested)),
                currentSellValue: currentSellVal,
                category: row.category
            });
        }
    });

    return { active, settled };
});

// ZARLA
app.get('/api/zarla', async () => {
    const r = await pool.query(`SELECT * FROM zarla_polls ORDER BY is_sponsored DESC, created_at ASC`);
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

// MOCK AUTH
app.post('/api/auth/login-mock', async (req, rep) => {
    const { username } = req.body || {};
    const name = username || 'LeisanB';
    const email = `${name.toLowerCase()}@oyver.pro`;
    const ur = await pool.query(`
        INSERT INTO users (email, username, password_hash, balance_kor, streak, tier)
        VALUES ($1, $2, 'OAUTH_MOCK', 14500, 5, 'ANALYST')
        ON CONFLICT (email) DO UPDATE SET username = EXCLUDED.username
        RETURNING id, username, balance_kor, streak, tier
    `, [email, name]);

    const user = ur.rows[0];
    const token = crypto.randomBytes(32).toString('hex');
    await pool.query(`INSERT INTO sessions (token, user_id) VALUES ($1, $2)`, [token, user.id]);
    rep.header('Set-Cookie', `oyver_token=${token}; Path=/; HttpOnly; SameSite=Lax; Max-Age=2592000`);
    return { success: true, user };
});

// ==========================================
// 4. FRONTEND ARAYÜZÜ (FINAL READY)
// ==========================================
function renderIndexHtml() {
    return `<!DOCTYPE html>
<html lang="tr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>OYVER PRO - Tahmin & Sosyal Zeka Terminali</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
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

    <div id="toast-container" class="fixed bottom-5 right-5 z-50 flex flex-col gap-2 pointer-events-none"></div>

    <!-- HEADER -->
    <header class="sticky top-0 z-40 bg-slate-900/90 backdrop-blur-md border-b border-slate-800">
        <div class="container mx-auto px-4 h-16 flex items-center justify-between gap-4">
            <div class="flex items-center space-x-6">
                <a href="/" onclick="navigateToHome(event)" class="text-2xl font-black tracking-tight text-transparent bg-clip-text bg-gradient-to-r from-purple-400 via-pink-400 to-orange-400">
                    OYVER<span class="text-xs ml-1 px-1.5 py-0.5 rounded bg-purple-500/20 text-purple-300 border border-purple-500/30">PRO</span>
                </a>
                <nav class="hidden md:flex items-center space-x-4 text-sm font-semibold">
                    <button id="nav-tab-markets" class="nav-main-tab active text-white border-b-2 border-purple-500 pb-1">Pazarlar</button>
                    <button id="nav-tab-zarla" class="nav-main-tab text-slate-400 hover:text-white pb-1 flex items-center gap-1.5">
                        <i class="fas fa-dice text-pink-400"></i> ZARLA (Günün Nabzı)
                    </button>
                    <button id="nav-tab-portfolio" class="nav-main-tab text-slate-400 hover:text-white pb-1 flex items-center gap-1.5">
                        <i class="fas fa-briefcase text-emerald-400"></i> Portföyüm
                    </button>
                    <button id="btn-open-leaderboard" class="text-slate-400 hover:text-white pb-1 flex items-center gap-1.5">
                        <i class="fas fa-trophy text-amber-400"></i> Top 100 Kahin
                    </button>
                </nav>
            </div>

            <div class="flex items-center space-x-3">
                <div id="user-badge" class="flex items-center bg-slate-800 border border-slate-700 rounded-lg p-1 pr-3 space-x-3 cursor-pointer" onclick="promptSwitchUser()" title="Kullanıcı değiştir">
                    <div class="flex items-center gap-1.5 bg-slate-900 px-2.5 py-1 rounded-md border border-slate-700/60">
                        <i class="fas fa-fire text-orange-500 text-xs"></i>
                        <span id="user-streak" class="text-xs font-black text-orange-400">1 Gün</span>
                    </div>
                    <div class="flex items-center gap-1.5">
                        <i class="fas fa-coins text-amber-400 text-xs"></i>
                        <span id="user-balance" class="text-xs font-bold text-amber-200">-- KOR</span>
                    </div>
                    <div id="user-tier-badge" class="px-1.5 py-0.5 rounded text-[10px] font-black uppercase bg-purple-500/20 text-purple-300 border border-purple-500/30">Analist</div>
                </div>
            </div>
        </div>
    </header>

    <!-- 1. BÖLÜM: ANA PAZARLAR LİSTESİ -->
    <main id="section-markets" class="flex-grow">
        <section class="py-8 md:py-10 border-b border-slate-800 text-center container mx-auto px-4">
            <div class="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-slate-800/80 border border-slate-700 text-xs font-semibold text-purple-300 mb-3">
                <span class="w-2 h-2 rounded-full bg-emerald-400 animate-ping"></span>
                Tescilli Kurumsal Veri & Liyakat Ligi
            </div>
            <h1 class="text-3xl sm:text-4xl font-extrabold text-transparent bg-clip-text bg-gradient-to-r from-purple-400 via-pink-400 to-orange-400 tracking-tight mb-2">
                Yarının Nabzını Bugün Tutun.
            </h1>
            <p class="text-xs sm:text-sm text-slate-400 max-w-xl mx-auto">
                Kolektif akla katılın, gerekçeli analizleri inceleyin, B2B kohort verilerini keşfedin.
            </p>
        </section>

        <section class="container mx-auto px-4 pt-6 pb-2">
            <div class="flex items-center space-x-2 overflow-x-auto pb-2">
                <button class="cat-btn active bg-purple-600 text-white px-4 py-1.5 rounded-lg text-xs font-bold transition" data-cat="ALL">Tümü</button>
                <button class="cat-btn bg-slate-800 hover:bg-slate-700 text-slate-300 px-4 py-1.5 rounded-lg text-xs font-bold transition" data-cat="EKONOMİ">Ekonomi</button>
                <button class="cat-btn bg-slate-800 hover:bg-slate-700 text-slate-300 px-4 py-1.5 rounded-lg text-xs font-bold transition" data-cat="FİNANS">Finans</button>
                <button class="cat-btn bg-slate-800 hover:bg-slate-700 text-slate-300 px-4 py-1.5 rounded-lg text-xs font-bold transition" data-cat="TEKNOLOJİ">Teknoloji</button>
                <button class="cat-btn bg-slate-800 hover:bg-slate-700 text-slate-300 px-4 py-1.5 rounded-lg text-xs font-bold transition" data-cat="KÜLTÜR & YAŞAM">Kültür & Yaşam</button>
            </div>
        </section>

        <section class="container mx-auto px-4 py-6">
            <div id="market-grid" class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6"></div>
        </section>
    </main>

    <!-- 2. BÖLÜM: PAZAR DETAY TERMİNALİ (/market/:slug) -->
    <main id="section-market-detail" class="hidden flex-grow container mx-auto px-4 py-6 max-w-6xl">
        <div class="mb-4">
            <button onclick="navigateToHome(event)" class="text-xs text-purple-400 hover:text-purple-300 font-bold flex items-center gap-1.5 mb-2">
                <i class="fas fa-arrow-left"></i> Tüm Pazarlara Dön
            </button>
            <div class="flex flex-wrap items-center justify-between gap-3">
                <div class="flex items-center gap-2">
                    <span id="dt-category" class="px-2.5 py-0.5 rounded text-[11px] font-black uppercase tracking-wider bg-purple-500/20 text-purple-300">KATEGORİ</span>
                    <span id="dt-status-badge" class="px-2.5 py-0.5 rounded text-[11px] font-black uppercase tracking-wider bg-emerald-500/20 text-emerald-300">İŞLEME AÇIK</span>
                </div>
                <div class="flex items-center gap-2 text-xs text-slate-400">
                    <i class="far fa-clock"></i> <span id="dt-closing-date">--</span>
                </div>
            </div>
            <h1 id="dt-title" class="text-2xl sm:text-3xl font-extrabold text-white mt-2 leading-snug">Pazar Başlığı</h1>
        </div>

        <div class="flex border-b border-slate-800 mb-6 gap-6 text-xs font-bold">
            <button id="dt-tab-main" onclick="switchDetailTab('main')" class="pb-2.5 border-b-2 border-purple-500 text-white flex items-center gap-1.5">
                <i class="fas fa-chart-line text-purple-400"></i> Grafik & İşlem
            </button>
            <button id="dt-tab-community" onclick="switchDetailTab('community')" class="pb-2.5 text-slate-400 hover:text-white flex items-center gap-1.5">
                <i class="fas fa-comments text-pink-400"></i> Topluluk Analizleri <span id="dt-comm-count" class="px-1.5 py-0.2 rounded bg-slate-800 text-[10px]">0</span>
            </button>
            <button id="dt-tab-cohorts" onclick="switchDetailTab('cohorts')" class="pb-2.5 text-slate-400 hover:text-white flex items-center gap-1.5">
                <i class="fas fa-layer-group text-amber-400"></i> B2B Kohort Analitiği <span class="px-1.5 py-0.2 rounded bg-amber-500/20 text-amber-300 text-[10px]">Pro</span>
            </button>
        </div>

        <!-- PANEL 1: GRAFİK & İŞLEM -->
        <div id="dt-view-main" class="grid grid-cols-1 lg:grid-cols-3 gap-6">
            <div class="lg:col-span-2 space-y-6">
                <div class="bg-slate-800/80 border border-slate-700/80 rounded-2xl p-5 shadow-xl">
                    <div class="flex items-center justify-between mb-4">
                        <div>
                            <div class="text-xs font-bold text-slate-400 uppercase">EVET Olasılık Eğrisi</div>
                            <div class="text-3xl font-black text-emerald-400 mt-0.5" id="dt-current-prob">--%</div>
                        </div>
                    </div>
                    <div class="h-64 w-full relative">
                        <canvas id="marketChart"></canvas>
                    </div>
                </div>

                <div class="bg-slate-800/60 border border-slate-700/60 rounded-2xl p-5 space-y-3">
                    <h3 class="text-sm font-bold text-white flex items-center gap-2">
                        <i class="fas fa-shield-alt text-purple-400"></i> Çözümleme Kriterleri & Resmi Kaynak
                    </h3>
                    <p id="dt-desc" class="text-xs text-slate-300 leading-relaxed">Pazar açıklaması...</p>
                    <div class="p-3 bg-slate-900/60 rounded-xl border border-slate-700/40 text-xs space-y-1">
                        <div class="text-slate-400">Tescilli Doğrulama Kaynağı:</div>
                        <a id="dt-source-url" href="#" target="_blank" class="font-bold text-purple-400 hover:underline flex items-center gap-1">
                            <span id="dt-source-name">Resmi Kurum</span> <i class="fas fa-external-link-alt text-[10px]"></i>
                        </a>
                    </div>
                </div>
            </div>

            <div class="space-y-6">
                <div class="bg-slate-800/90 border border-slate-700/80 rounded-2xl p-5 shadow-xl space-y-4">
                    <div class="flex justify-between items-center pb-2 border-b border-slate-700">
                        <span class="text-xs font-bold text-slate-400 uppercase">Tahmin Konsolu</span>
                        <span id="dt-user-balance" class="text-xs font-black text-amber-300">-- KOR</span>
                    </div>

                    <div class="grid grid-cols-2 gap-2">
                        <button id="dt-choice-yes" class="py-2.5 rounded-xl font-black text-xs uppercase border border-emerald-500 bg-emerald-600 text-white">EVET (%--)</button>
                        <button id="dt-choice-no" class="py-2.5 rounded-xl font-black text-xs uppercase border border-slate-700 bg-slate-800 text-slate-400">HAYIR (%--)</button>
                    </div>

                    <div>
                        <label class="block text-xs font-bold text-slate-400 mb-1">Kullanılacak Puan</label>
                        <div class="relative">
                            <input type="number" id="dt-input-amount" value="500" min="50" step="50" class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-white font-bold text-sm focus:outline-none focus:border-purple-500">
                            <span class="absolute right-3 top-2 text-xs font-bold text-slate-500">KOR</span>
                        </div>
                    </div>

                    <div class="p-3 bg-slate-900/80 rounded-xl border border-slate-700/60 space-y-1 text-xs">
                        <div class="flex justify-between text-slate-400">
                            <span>Alınacak Pay:</span>
                            <span id="dt-quote-shares" class="font-bold text-white">-- Pay</span>
                        </div>
                        <div class="flex justify-between text-slate-400">
                            <span>Ortalama Fiyat:</span>
                            <span id="dt-quote-avg" class="font-bold text-slate-300">-- Puan</span>
                        </div>
                        <div class="flex justify-between pt-1 border-t border-slate-800 font-bold">
                            <span class="text-slate-300">Sonuçlanınca Hedef Puan:</span>
                            <span id="dt-quote-payout" class="font-black text-emerald-400">-- KOR</span>
                        </div>
                    </div>

                    <button id="dt-btn-predict" class="w-full py-3 rounded-xl bg-gradient-to-r from-purple-600 to-pink-600 hover:from-purple-500 hover:to-pink-500 text-white font-extrabold text-sm shadow-lg transition">
                        Tahmini Onayla (KOR)
                    </button>
                </div>
            </div>
        </div>

        <!-- PANEL 2: TOPLULUK GEREKÇELİ ANALİZLERİ -->
        <div id="dt-view-community" class="hidden space-y-6 max-w-4xl">
            <div class="bg-slate-800/80 border border-slate-700/80 rounded-2xl p-5 shadow-xl space-y-3">
                <h3 class="text-sm font-bold text-white flex items-center gap-2">
                    <i class="fas fa-pen-nib text-pink-400"></i> Neden Bu Kararı Aldınız? (Gerekçeli Analiz Paylaşın)
                </h3>
                <textarea id="comm-input-content" rows="3" placeholder="Öngörünüzü destekleyen ekonomik veri, resmi bülten veya argümanınızı yazın..." class="w-full bg-slate-900 border border-slate-700 rounded-xl p-3 text-xs text-white placeholder-slate-500 focus:outline-none focus:border-purple-500"></textarea>
                <div class="flex justify-between items-center pt-1">
                    <span class="text-[11px] text-slate-400">Tercihiniz ve payınız otomatik olarak analizinizde rozetlenecektir.</span>
                    <button id="btn-submit-comment" class="px-5 py-2 bg-gradient-to-r from-purple-600 to-pink-600 hover:from-purple-500 hover:to-pink-500 rounded-xl text-xs font-bold text-white transition">
                        Analizi Yayınla
                    </button>
                </div>
            </div>

            <div id="dt-comments-list" class="space-y-3"></div>
        </div>

        <!-- PANEL 3: B2B KOHORT ANALİTİĞİ -->
        <div id="dt-view-cohorts" class="hidden space-y-6">
            <div class="flex flex-wrap items-center justify-between gap-3 bg-slate-800/50 p-4 rounded-2xl border border-slate-700/60">
                <div>
                    <h3 class="text-sm font-black text-white flex items-center gap-2">
                        <i class="fas fa-layer-group text-amber-400"></i> Kamuoyu Demografi & Tüketici Kohort Kırılımı
                    </h3>
                    <p class="text-xs text-slate-400 mt-0.5">Katılımcıların yaş, eğitim ve sektörel tercihlerinin çapraz analizi.</p>
                </div>
                <button onclick="downloadCohortCsv()" class="px-4 py-2 bg-emerald-600/20 hover:bg-emerald-600 border border-emerald-500/40 text-emerald-300 hover:text-white rounded-xl text-xs font-bold transition flex items-center gap-2">
                    <i class="fas fa-file-csv"></i> Ham Veriyi İndir (CSV)
                </button>
            </div>

            <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
                <div class="bg-slate-800/80 border border-slate-700/80 rounded-2xl p-5 space-y-4">
                    <h4 class="text-xs font-bold text-slate-300 uppercase flex items-center gap-1.5">
                        <i class="fas fa-users text-purple-400"></i> Yaş Kohortuna Göre Dağılım
                    </h4>
                    <div id="cohort-age-list" class="space-y-3 text-xs"></div>
                </div>

                <div class="bg-slate-800/80 border border-slate-700/80 rounded-2xl p-5 space-y-4">
                    <h4 class="text-xs font-bold text-slate-300 uppercase flex items-center gap-1.5">
                        <i class="fas fa-graduation-cap text-indigo-400"></i> Eğitim Düzeyine Göre Dağılım
                    </h4>
                    <div id="cohort-edu-list" class="space-y-3 text-xs"></div>
                </div>
            </div>
        </div>
    </main>

    <!-- 3. BÖLÜM: ZARLA -->
    <main id="section-zarla" class="hidden flex-grow container mx-auto px-4 py-8 max-w-3xl">
        <div class="text-center mb-8">
            <div class="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-pink-500/20 border border-pink-500/30 text-xs font-bold text-pink-300 mb-2">
                <i class="fas fa-dice"></i> 0 Puan, 0 Risk, Saf Kamuoyu Nabzı
            </div>
            <h2 class="text-3xl font-black text-white">Günün 10 Kritik Meselesi</h2>
            <p class="text-xs text-slate-400 mt-1">Gündemin en sıcak tartışmaları ve marka düelloları. Tek tıkla oy ver, anlık dağılımı gör.</p>
        </div>
        <div id="zarla-list" class="space-y-4"></div>
    </main>

    <!-- 4. BÖLÜM: PORTFÖYÜM -->
    <main id="section-portfolio" class="hidden flex-grow container mx-auto px-4 py-8 max-w-4xl space-y-8">
        <div>
            <div class="flex items-center justify-between mb-4 pb-3 border-b border-slate-800">
                <div>
                    <h2 class="text-2xl font-black text-white">Açık Paylarım (İşlemde)</h2>
                    <p class="text-xs text-slate-400">Pazar kapanmadan önce erken satıp KOR puanınızı serbest bırakabilirsiniz.</p>
                </div>
                <button onclick="loadPortfolio()" class="px-3 py-1.5 rounded-lg bg-slate-800 hover:bg-slate-700 text-xs font-bold transition">
                    <i class="fas fa-sync-alt mr-1"></i> Yenile
                </button>
            </div>
            <div id="portfolio-active-list" class="space-y-3"></div>
        </div>

        <div>
            <div class="mb-4 pb-3 border-b border-slate-800">
                <h2 class="text-xl font-black text-emerald-400">Sonuçlanan Tahminlerim (Tasfiye Edildi)</h2>
                <p class="text-xs text-slate-400">Kazanan pay başına 1.00 KOR doğrudan cüzdanınıza aktarılır.</p>
            </div>
            <div id="portfolio-settled-list" class="space-y-3"></div>
        </div>
    </main>

    <!-- KAHİN İTİBAR KARNESİ MODALI -->
    <div id="profile-modal" class="fixed inset-0 bg-slate-950/85 z-50 backdrop-blur-md hidden flex items-center justify-center p-4">
        <div class="bg-slate-900 border border-slate-800 w-full max-w-2xl rounded-2xl overflow-hidden shadow-2xl flex flex-col max-h-[90vh]">
            <div class="p-5 border-b border-slate-800 flex items-center justify-between bg-slate-800/40">
                <div class="flex items-center gap-3">
                    <div class="w-10 h-10 rounded-full bg-gradient-to-tr from-purple-500 to-pink-500 flex items-center justify-center text-lg font-black text-white" id="pf-avatar">A</div>
                    <div>
                        <div class="flex items-center gap-2">
                            <h3 class="text-lg font-black text-white" id="pf-username">Ahmet_Kahin</h3>
                            <span class="px-2 py-0.5 rounded text-[10px] font-black uppercase bg-purple-500/20 text-purple-300 border border-purple-500/30" id="pf-tier">PRO</span>
                        </div>
                        <div class="text-xs text-slate-400" id="pf-meta">İstanbul | Finans | Üye: Eki 2026</div>
                    </div>
                </div>
                <button onclick="closeProfileModal()" class="text-slate-400 hover:text-white p-1 text-lg"><i class="fas fa-times"></i></button>
            </div>

            <div class="p-6 space-y-6 overflow-y-auto">
                <div class="grid grid-cols-1 sm:grid-cols-3 gap-3">
                    <div class="p-4 rounded-xl bg-slate-800/80 border border-purple-500/30 text-center">
                        <div class="text-xs font-bold text-purple-400 uppercase">İtibar Skoru (FRS)</div>
                        <div class="text-2xl font-black text-white mt-1" id="pf-frs">-- Puan</div>
                        <div class="text-[10px] text-slate-400 mt-1">Liyakat Sıralama Puanı</div>
                    </div>
                    <div class="p-4 rounded-xl bg-slate-800/80 border border-emerald-500/30 text-center">
                        <div class="text-xs font-bold text-emerald-400 uppercase">Brier Skoru</div>
                        <div class="text-2xl font-black text-emerald-300 mt-1" id="pf-brier">0.140</div>
                        <div class="text-[10px] text-emerald-400 font-bold mt-1" id="pf-brier-grade">Kusursuz Öngörü</div>
                    </div>
                    <div class="p-4 rounded-xl bg-slate-800/80 border border-amber-500/30 text-center">
                        <div class="text-xs font-bold text-amber-400 uppercase">Doğruluk Oranı</div>
                        <div class="text-2xl font-black text-amber-300 mt-1" id="pf-winrate">%85</div>
                        <div class="text-[10px] text-slate-400 mt-1" id="pf-counts">-- / -- Pazar</div>
                    </div>
                </div>

                <div>
                    <h4 class="text-xs font-bold text-slate-400 uppercase mb-2">Kazanılan Liyakat Rozetleri</h4>
                    <div class="flex flex-wrap gap-2" id="pf-badges"></div>
                </div>

                <div>
                    <h4 class="text-xs font-bold text-slate-400 uppercase mb-2">En Çok Kazandıran Tahminleri (Best Calls)</h4>
                    <div class="space-y-2" id="pf-best-calls"></div>
                </div>
            </div>
        </div>
    </div>

    <!-- TOP 100 LİDERLİK TABLOSU -->
    <div id="leaderboard-modal" class="fixed inset-0 bg-slate-950/80 z-50 backdrop-blur-md hidden flex items-center justify-center p-4">
        <div class="bg-slate-900 border border-slate-800 w-full max-w-3xl rounded-2xl overflow-hidden shadow-2xl flex flex-col max-h-[85vh]">
            <div class="p-5 border-b border-slate-800 flex items-center justify-between bg-slate-800/40">
                <div>
                    <div class="flex items-center gap-2">
                        <i class="fas fa-trophy text-amber-400 text-lg"></i>
                        <h3 class="text-lg font-black text-white">Top 100 Kahin Liyakat Ligi</h3>
                    </div>
                    <p class="text-[11px] text-slate-400 mt-0.5">FRS Modeli: Brier kalibrasyon sapması, net kâr, pazar hacmi ve istikrarla hesaplanır.</p>
                </div>
                <button id="btn-close-leaderboard" class="text-slate-400 hover:text-white p-1 text-lg"><i class="fas fa-times"></i></button>
            </div>
            <div class="p-4 overflow-y-auto flex-grow divide-y divide-slate-800" id="leaderboard-list"></div>
        </div>
    </div>

    <!-- ONBOARDING MODAL -->
    <div id="onboarding-modal" class="fixed inset-0 bg-slate-950/85 z-50 backdrop-blur-md hidden flex items-center justify-center p-4">
        <div class="bg-slate-900 border border-slate-800 w-full max-w-lg rounded-2xl overflow-hidden shadow-2xl flex flex-col">
            <div class="p-5 border-b border-slate-800 flex items-center justify-between bg-slate-800/40">
                <div class="flex items-center gap-2">
                    <i class="fas fa-user-check text-purple-400 text-lg"></i>
                    <h3 class="text-base font-black text-white">Profilini Tamamla (+1.500 KOR Kazan)</h3>
                </div>
            </div>
            <div class="p-6 space-y-4 text-xs">
                <p class="text-slate-400">OYVER araştırmalarının demografik doğruluğu için anonim temel bilgilerini seç:</p>
                <div>
                    <label class="block font-bold text-slate-300 mb-1">Doğum Yılınız (18+ Zorunludur)</label>
                    <input type="number" id="ob-birth-year" value="1995" min="1940" max="2008" class="w-full bg-slate-800 border border-slate-700 rounded-lg p-2.5 text-white font-bold">
                </div>
                <div>
                    <label class="block font-bold text-slate-300 mb-1">Eğitim Düzeyi</label>
                    <select id="ob-education" class="w-full bg-slate-800 border border-slate-700 rounded-lg p-2.5 text-white font-bold">
                        <option value="BACHELOR">Lisans (Üniversite)</option>
                        <option value="MASTER_PHD">Yüksek Lisans / Doktora</option>
                        <option value="ASSOCIATE">Ön Lisans (MYO)</option>
                        <option value="HIGH_SCHOOL">Lise / Dengi</option>
                    </select>
                </div>
                <div>
                    <label class="block font-bold text-slate-300 mb-1">Sektör / Çalışma Durumu</label>
                    <select id="ob-industry" class="w-full bg-slate-800 border border-slate-700 rounded-lg p-2.5 text-white font-bold">
                        <option value="FINANCE">Finans & Bankacılık</option>
                        <option value="TECH">Teknoloji & Yazılım</option>
                        <option value="PUBLIC">Kamu / Memur</option>
                        <option value="RETAIL">Perakende & Ticaret</option>
                        <option value="STUDENT">Öğrenci</option>
                        <option value="OTHER">Diğer</option>
                    </select>
                </div>
                <div>
                    <label class="block font-bold text-slate-300 mb-1">Şehir</label>
                    <select id="ob-city" class="w-full bg-slate-800 border border-slate-700 rounded-lg p-2.5 text-white font-bold">
                        <option value="İstanbul">İstanbul</option>
                        <option value="Ankara">Ankara</option>
                        <option value="İzmir">İzmir</option>
                        <option value="Bursa">Bursa</option>
                        <option value="Antalya">Antalya</option>
                        <option value="Diğer">Diğer İl</option>
                    </select>
                </div>
                <button id="btn-save-onboarding" class="w-full py-3 rounded-xl bg-gradient-to-r from-purple-600 to-pink-600 hover:from-purple-500 hover:to-pink-500 text-white font-extrabold text-sm shadow-lg transition">
                    Kaydet ve +1.500 KOR Bonusu Al
                </button>
            </div>
        </div>
    </div>

    <script>
        let markets = [];
        let currentBalance = 14500;
        let activeCategory = 'ALL';
        let activeDetailMarket = null;
        let activeDetailChoice = 'YES';
        let chartInstance = null;

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

        const tabMarkets = document.getElementById('nav-tab-markets');
        const tabZarla = document.getElementById('nav-tab-zarla');
        const tabPortfolio = document.getElementById('nav-tab-portfolio');
        const secMarkets = document.getElementById('section-markets');
        const secMarketDetail = document.getElementById('section-market-detail');
        const secZarla = document.getElementById('section-zarla');
        const secPortfolio = document.getElementById('section-portfolio');

        function switchTab(target) {
            [tabMarkets, tabZarla, tabPortfolio].forEach(t => t.className = 'nav-main-tab text-slate-400 hover:text-white pb-1 flex items-center gap-1.5');
            [secMarkets, secMarketDetail, secZarla, secPortfolio].forEach(s => s.classList.add('hidden'));

            if (target === 'markets') {
                tabMarkets.className = 'nav-main-tab active text-white border-b-2 border-purple-500 pb-1';
                secMarkets.classList.remove('hidden');
            } else if (target === 'zarla') {
                tabZarla.className = 'nav-main-tab active text-white border-b-2 border-pink-500 pb-1 flex items-center gap-1.5';
                secZarla.classList.remove('hidden');
                loadZarla();
            } else if (target === 'portfolio') {
                tabPortfolio.className = 'nav-main-tab active text-white border-b-2 border-emerald-500 pb-1 flex items-center gap-1.5';
                secPortfolio.classList.remove('hidden');
                loadPortfolio();
            }
        }

        tabMarkets.onclick = () => { history.pushState({}, '', '/'); switchTab('markets'); };
        tabZarla.onclick = () => switchTab('zarla');
        tabPortfolio.onclick = () => switchTab('portfolio');

        window.navigateToHome = function(e) {
            if (e) e.preventDefault();
            history.pushState({}, '', '/');
            switchTab('markets');
        };

        window.switchDetailTab = function(tab) {
            const tabs = ['main', 'community', 'cohorts'];
            tabs.forEach(t => {
                document.getElementById('dt-tab-' + t).className = 'pb-2.5 text-slate-400 hover:text-white flex items-center gap-1.5';
                document.getElementById('dt-view-' + t).classList.add('hidden');
            });

            document.getElementById('dt-tab-' + tab).className = 'pb-2.5 border-b-2 border-purple-500 text-white flex items-center gap-1.5';
            document.getElementById('dt-view-' + tab).classList.remove('hidden');

            if (tab === 'community') loadComments();
            if (tab === 'cohorts') loadCohorts();
        };

        window.openMarketDetail = async function(slug) {
            history.pushState({}, '', '/market/' + slug);
            [secMarkets, secZarla, secPortfolio].forEach(s => s.classList.add('hidden'));
            secMarketDetail.classList.remove('hidden');

            try {
                const res = await fetch('/api/markets/' + slug).then(r => r.json());
                if (res.error) throw new Error(res.error);

                activeDetailMarket = res.market;
                document.getElementById('dt-title').textContent = res.market.question;
                document.getElementById('dt-category').textContent = res.market.category;
                document.getElementById('dt-closing-date').textContent = res.market.closing_date;
                document.getElementById('dt-desc').textContent = res.market.description || 'Resmi açıklama dikkate alınacaktır.';
                document.getElementById('dt-source-name').textContent = res.market.source_name;
                document.getElementById('dt-source-url').href = res.market.source_url || '#';
                document.getElementById('dt-current-prob').textContent = '%' + res.market.probYes;
                document.getElementById('dt-user-balance').textContent = 'Bakiye: ' + currentBalance.toLocaleString('tr-TR') + ' KOR';

                const statusBadge = document.getElementById('dt-status-badge');
                if (res.market.status === 'SETTLED') {
                    statusBadge.className = 'px-2.5 py-0.5 rounded text-[11px] font-black uppercase tracking-wider bg-purple-500/20 text-purple-300';
                    statusBadge.textContent = 'SONUÇLANDI (' + res.market.resolved_outcome + ')';
                } else {
                    statusBadge.className = 'px-2.5 py-0.5 rounded text-[11px] font-black uppercase tracking-wider bg-emerald-500/20 text-emerald-300';
                    statusBadge.textContent = 'İŞLEME AÇIK';
                }

                switchDetailTab('main');
                updateDetailChoiceBtns();
                fetchDetailQuote();
                renderChart(res.history);
            } catch (e) {
                showToast(e.message, 'error');
                navigateToHome();
            }
        };

        async function loadComments() {
            if (!activeDetailMarket) return;
            const res = await fetch('/api/markets/' + activeDetailMarket.slug + '/comments').then(r => r.json());
            const list = document.getElementById('dt-comments-list');
            list.innerHTML = '';
            document.getElementById('dt-comm-count').textContent = res.comments.length;

            if (res.comments.length === 0) {
                list.innerHTML = '<div class="text-center py-8 text-slate-500 text-xs">Bu pazar için henüz gerekçeli analiz paylaşılmadı. İlk görüşü siz yazın!</div>';
                return;
            }

            res.comments.forEach(c => {
                const card = document.createElement('div');
                card.className = 'bg-slate-800/80 border border-slate-700/80 rounded-2xl p-4 space-y-3';

                let stanceBadge = '';
                if (c.outcome_at_time) {
                    const isYes = c.outcome_at_time === 'YES';
                    const color = isYes ? 'bg-emerald-500/20 text-emerald-300 border-emerald-500/30' : 'bg-rose-500/20 text-rose-300 border-rose-500/30';
                    stanceBadge = '<span class="px-2 py-0.5 rounded text-[10px] font-black border ' + color + '">' + (isYes ? '🟢 EVET' : '🔴 HAYIR') + ' (' + c.shares_at_time + ' Pay Sahibi)</span>';
                }

                card.innerHTML = 
                    '<div class="flex justify-between items-center">' +
                        '<div class="flex items-center gap-2">' +
                            '<strong class="text-white text-xs cursor-pointer hover:text-purple-300" onclick="openProfile(\\'' + c.username + '\\')">' + c.username + '</strong>' +
                            '<span class="text-[10px] text-purple-400 font-bold">[' + c.tier + ']</span>' +
                            stanceBadge +
                        '</div>' +
                        '<span class="text-[10px] text-slate-500">' + c.time_formatted + '</span>' +
                    '</div>' +
                    '<p class="text-xs text-slate-300 leading-relaxed">' + c.content + '</p>' +
                    '<div class="flex justify-between items-center pt-2 border-t border-slate-700/40">' +
                        '<button onclick="upvoteComment(\\'' + c.id + '\\')" class="text-xs text-slate-400 hover:text-purple-400 transition flex items-center gap-1.5">' +
                            '<i class="far fa-thumbs-up"></i> <span>' + c.upvotes + ' Katılıyorum</span>' +
                        '</button>' +
                    '</div>';

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
                showToast('✅ Gerekçeli analiziniz yayınlandı!');
                loadComments();
            } catch (e) {
                showToast(e.message, 'error');
            }
        };

        window.upvoteComment = async function(id) {
            try {
                const res = await fetch('/api/comments/' + id + '/upvote', { method: 'POST' }).then(r => r.json());
                if (res.error) throw new Error(res.error);
                showToast('👍 Oyunuz kaydedildi!');
                loadComments();
            } catch (e) {
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

            if (res.ageCohorts.length === 0) {
                ageContainer.innerHTML = '<div class="text-slate-500 py-4 text-center">Yeterli veri toplanmadı.</div>';
            } else {
                res.ageCohorts.forEach(c => {
                    const row = document.createElement('div');
                    row.className = 'space-y-1 bg-slate-900/60 p-2.5 rounded-xl border border-slate-700/50';
                    row.innerHTML = 
                        '<div class="flex justify-between font-bold">' +
                            '<span>' + c.age_group + '</span>' +
                            '<span class="' + (c.outcome === 'YES' ? 'text-emerald-400' : 'text-rose-400') + '">' + c.outcome + ' (' + c.vote_count + ' Katılım)</span>' +
                        '</div>';
                    ageContainer.appendChild(row);
                });
            }

            if (res.educationCohorts.length === 0) {
                eduContainer.innerHTML = '<div class="text-slate-500 py-4 text-center">Yeterli veri toplanmadı.</div>';
            } else {
                res.educationCohorts.forEach(c => {
                    const row = document.createElement('div');
                    row.className = 'space-y-1 bg-slate-900/60 p-2.5 rounded-xl border border-slate-700/50';
                    row.innerHTML = 
                        '<div class="flex justify-between font-bold">' +
                            '<span>' + c.education_level + '</span>' +
                            '<span class="' + (c.outcome === 'YES' ? 'text-emerald-400' : 'text-rose-400') + '">' + c.outcome + ' (' + Math.round(c.total_shares) + ' Pay)</span>' +
                        '</div>';
                    eduContainer.appendChild(row);
                });
            }
        }

        window.downloadCohortCsv = function() {
            if (!activeDetailMarket) return;
            window.location.href = '/api/markets/' + activeDetailMarket.slug + '/export';
        };

        function renderChart(history) {
            const ctx = document.getElementById('marketChart').getContext('2d');
            const labels = history.map(h => h.time_label);
            const data = history.map(h => h.prob_yes);

            if (chartInstance) chartInstance.destroy();

            chartInstance = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: labels,
                    datasets: [{
                        label: 'EVET Olasılığı (%)',
                        data: data,
                        borderColor: '#10b981',
                        backgroundColor: 'rgba(16, 185, 129, 0.1)',
                        borderWidth: 3,
                        fill: true,
                        tension: 0.35,
                        pointBackgroundColor: '#10b981',
                        pointRadius: 4
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        y: { min: 0, max: 100, grid: { color: 'rgba(51, 65, 85, 0.4)' }, ticks: { color: '#94a3b8' } },
                        x: { grid: { display: false }, ticks: { color: '#94a3b8' } }
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
                bN.className = 'py-2.5 rounded-xl font-black text-xs uppercase border border-slate-700 bg-slate-800 text-slate-400';
            } else {
                bN.className = 'py-2.5 rounded-xl font-black text-xs uppercase border border-rose-500 bg-rose-600 text-white';
                bY.className = 'py-2.5 rounded-xl font-black text-xs uppercase border border-slate-700 bg-slate-800 text-slate-400';
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
                document.getElementById('dt-user-balance').textContent = 'Bakiye: ' + currentBalance.toLocaleString('tr-TR') + ' KOR';

                showToast('✅ Tercihiniz işlendi! Alınan Pay: ' + res.sharesOut);
                openMarketDetail(activeDetailMarket.slug);
            } catch (e) {
                showToast(e.message, 'error');
            } finally {
                btn.disabled = false;
            }
        };

        window.openProfile = async function(username) {
            try {
                const res = await fetch('/api/users/' + username).then(r => r.json());
                if (res.error) throw new Error(res.error);

                document.getElementById('pf-username').textContent = res.user.username;
                document.getElementById('pf-avatar').textContent = res.user.username[0].toUpperCase();
                document.getElementById('pf-tier').textContent = res.user.tier;
                document.getElementById('pf-meta').textContent = res.user.city + ' | ' + res.user.industry + ' | Üye: ' + res.user.memberSince;

                document.getElementById('pf-frs').textContent = res.scorecard.frsScore.toLocaleString('tr-TR') + ' Puan';
                document.getElementById('pf-brier').textContent = res.scorecard.brierScore.toFixed(3);
                document.getElementById('pf-brier-grade').textContent = res.scorecard.brierGrade;
                document.getElementById('pf-winrate').textContent = '%' + res.scorecard.winRate;
                document.getElementById('pf-counts').textContent = res.scorecard.wonCount + ' / ' + res.scorecard.totalSettled + ' Pazar';

                const bContainer = document.getElementById('pf-badges');
                bContainer.innerHTML = '';
                res.scorecard.badges.forEach(b => {
                    const badge = document.createElement('span');
                    badge.className = 'px-3 py-1 rounded-lg text-xs font-bold bg-purple-500/20 text-purple-300 border border-purple-500/30';
                    badge.textContent = b;
                    bContainer.appendChild(badge);
                });

                const cContainer = document.getElementById('pf-best-calls');
                cContainer.innerHTML = '';
                if (res.bestCalls.length === 0) {
                    cContainer.innerHTML = '<div class="text-slate-500 text-xs py-2">Henüz sonuçlanan kazançlı tahmini bulunmuyor.</div>';
                } else {
                    res.bestCalls.forEach(c => {
                        const callItem = document.createElement('div');
                        callItem.className = 'p-3 rounded-xl bg-slate-800/60 border border-slate-700/50 flex justify-between items-center text-xs';
                        callItem.innerHTML = 
                            '<div>' +
                                '<div class="font-bold text-slate-200">' + c.question + '</div>' +
                                '<div class="text-slate-400 text-[11px]">Tercih: <strong class="text-emerald-400">' + c.outcome + '</strong> | Katılım: ' + c.invested + ' KOR</div>' +
                            '</div>' +
                            '<div class="font-black text-emerald-400 text-sm">+' + c.payout + ' KOR</div>';
                        cContainer.appendChild(callItem);
                    });
                }

                document.getElementById('profile-modal').classList.remove('hidden');
            } catch (e) {
                showToast(e.message, 'error');
            }
        };

        window.closeProfileModal = () => document.getElementById('profile-modal').classList.add('hidden');

        document.getElementById('btn-open-leaderboard').onclick = async () => {
            const d = await fetch('/api/leaderboard').then(r => r.json());
            const list = document.getElementById('leaderboard-list');
            list.innerHTML = '';

            d.top100.forEach(u => {
                const item = document.createElement('div');
                item.className = 'py-3.5 flex items-center justify-between text-xs hover:bg-slate-800/40 px-2 rounded-xl transition cursor-pointer';
                item.onclick = () => openProfile(u.name);

                const badgeColor = u.rank === 1 ? 'bg-amber-500/20 text-amber-400 border border-amber-500/40' : (u.rank === 2 ? 'bg-slate-400/20 text-slate-300' : 'bg-orange-700/20 text-orange-400');
                item.innerHTML = 
                    '<div class="flex items-center gap-3">' +
                        '<span class="w-7 h-7 rounded-full ' + badgeColor + ' flex items-center justify-center font-black text-xs">#' + u.rank + '</span>' +
                        '<div>' +
                            '<div class="flex items-center gap-1.5">' +
                                '<strong class="text-white text-sm hover:text-purple-300">' + u.name + '</strong>' +
                                '<span class="text-[10px] text-purple-400 font-bold">[' + u.tier + ']</span>' +
                                '<span class="text-[10px] text-slate-400">🔥 ' + u.streak + 'g</span>' +
                            '</div>' +
                            '<div class="text-[11px] text-slate-400 flex items-center gap-2 mt-0.5">' +
                                '<span>Brier: <strong class="text-emerald-400">' + u.brierScore.toFixed(3) + '</strong></span> | ' +
                                '<span>İsabet: <strong>%' + u.winRate + '</strong> (' + u.totalSettled + ' Pazar)</span>' +
                            '</div>' +
                        '</div>' +
                    '</div>' +
                    '<div class="text-right">' +
                        '<div class="font-black text-purple-400 text-sm">' + u.frsScore.toLocaleString('tr-TR') + ' FRS</div>' +
                        '<div class="text-[10px] text-slate-500">Liyakat Skoru</div>' +
                    '</div>';

                list.appendChild(item);
            });
            document.getElementById('leaderboard-modal').classList.remove('hidden');
        };
        document.getElementById('btn-close-leaderboard').onclick = () => document.getElementById('leaderboard-modal').classList.add('hidden');

        function renderMarkets() {
            const container = document.getElementById('market-grid');
            container.innerHTML = '';

            const filtered = activeCategory === 'ALL'
                ? markets
                : markets.filter(m => m.category === activeCategory);

            filtered.forEach(m => {
                const card = document.createElement('article');
                card.className = 'bg-slate-800/90 rounded-2xl border border-slate-700/70 p-5 shadow-lg flex flex-col justify-between hover:border-slate-600 transition relative';

                const isSettled = m.status === 'SETTLED';
                let actionArea = '';

                if (isSettled) {
                    const outcomeText = m.resolved_outcome === 'YES' ? 'EVET KAZANDI' : (m.resolved_outcome === 'NO' ? 'HAYIR KAZANDI' : 'İPTAL EDİLDİ');
                    const badgeColor = m.resolved_outcome === 'YES' ? 'bg-emerald-500/20 text-emerald-300 border-emerald-500/40' : 'bg-rose-500/20 text-rose-300 border-rose-500/40';
                    actionArea = 
                        '<div class="pt-3 border-t border-slate-700/50 text-center">' +
                            '<div class="py-2.5 px-3 rounded-xl border text-xs font-black uppercase tracking-wider ' + badgeColor + '">' +
                                '<i class="fas fa-check-circle mr-1"></i> Sonuçlandı: ' + outcomeText + ' (1 Pay = 1 KOR)' +
                            '</div>' +
                        '</div>';
                } else {
                    actionArea = 
                        '<div class="grid grid-cols-2 gap-3 pt-2 border-t border-slate-700/50">' +
                            '<button class="py-2.5 rounded-xl bg-emerald-600/20 hover:bg-emerald-600 border border-emerald-500/40 text-emerald-300 hover:text-white font-extrabold text-xs transition flex justify-center items-center gap-1.5 btn-terminal-yes" data-slug="' + m.slug + '"><i class="fas fa-chart-line"></i> Terminal & Tahmin (%' + m.probYes + ')</button>' +
                            '<button class="py-2.5 rounded-xl bg-slate-700/40 hover:bg-slate-700 text-slate-300 hover:text-white font-extrabold text-xs transition flex justify-center items-center gap-1.5 btn-terminal-detail" data-slug="' + m.slug + '">Detayı Gör <i class="fas fa-arrow-right text-[10px]"></i></button>' +
                        '</div>';
                }

                card.innerHTML = 
                    '<div class="flex items-center justify-between mb-3">' +
                        '<span class="px-2 py-0.5 rounded text-[10px] font-black uppercase tracking-wider bg-purple-500/20 text-purple-300">' + m.category + '</span>' +
                        '<span class="text-[11px] text-slate-400"><i class="far fa-clock mr-1"></i>' + m.closing_date + '</span>' +
                    '</div>' +
                    '<h3 class="text-base font-bold text-white mb-2 leading-snug cursor-pointer hover:text-purple-300 transition market-click-title" data-slug="' + m.slug + '">' + m.question + '</h3>' +
                    '<div class="text-[11px] text-slate-400 mb-4 flex items-center gap-1">' +
                        '<i class="fas fa-landmark text-slate-500 text-[10px]"></i>' +
                        '<span>' + m.source_name + '</span>' +
                    '</div>' +
                    '<div class="space-y-2 mb-5 cursor-pointer market-click-bar" data-slug="' + m.slug + '">' +
                        '<div class="flex justify-between text-xs text-slate-400 font-medium">' +
                            '<span id="pool-' + m.id + '">Katılım: ' + m.poolTotal + ' Puan</span>' +
                            '<span id="prob-' + m.id + '" class="font-bold text-slate-200">EVET: %' + m.probYes + '</span>' +
                        '</div>' +
                        '<div class="w-full bg-rose-500/30 rounded-full h-2 overflow-hidden flex">' +
                            '<div id="bar-' + m.id + '" class="bg-gradient-to-r from-emerald-500 to-teal-400 h-full transition-all duration-700 ease-out" style="width:' + m.probYes + '%"></div>' +
                        '</div>' +
                    '</div>' +
                    actionArea;

                container.appendChild(card);
            });

            container.querySelectorAll('.market-click-title, .market-click-bar, .btn-terminal-yes, .btn-terminal-detail').forEach(el => {
                el.onclick = () => openMarketDetail(el.getAttribute('data-slug'));
            });
        }

        async function loadZarla() {
            const res = await fetch('/api/zarla').then(r => r.json());
            const list = document.getElementById('zarla-list');
            list.innerHTML = '';

            res.polls.forEach(p => {
                const card = document.createElement('div');
                card.className = 'bg-slate-800/80 border border-slate-700/80 rounded-2xl p-5 shadow-lg space-y-4';
                const badge = p.is_sponsored 
                    ? '<span class="px-2 py-0.5 rounded text-[10px] font-black bg-pink-500/20 text-pink-300 border border-pink-500/30">📢 SPONSORLU DÜELLO (' + p.sponsor_brand + ')</span>'
                    : '<span class="px-2 py-0.5 rounded text-[10px] font-black bg-slate-700 text-slate-300">' + p.category + '</span>';

                card.innerHTML = 
                    '<div class="flex justify-between items-center">' + badge + '<span class="text-[11px] text-slate-400 font-semibold">' + p.totalVotes + ' Oy</span></div>' +
                    '<h3 class="text-base font-bold text-white">' + p.question + '</h3>' +
                    '<div class="space-y-2">' +
                        '<div class="w-full bg-slate-700/60 rounded-full h-3 overflow-hidden flex">' +
                            '<div id="z-bar-a-' + p.id + '" class="bg-gradient-to-r from-purple-500 to-indigo-500 h-full transition-all duration-500" style="width:' + p.pctA + '%"></div>' +
                            '<div id="z-bar-b-' + p.id + '" class="bg-gradient-to-r from-pink-500 to-rose-500 h-full transition-all duration-500" style="width:' + p.pctB + '%"></div>' +
                        '</div>' +
                        '<div class="flex justify-between text-xs font-black text-slate-300">' +
                            '<span id="z-pct-a-' + p.id + '">%' + p.pctA + ' ' + p.option_a + '</span>' +
                            '<span id="z-pct-b-' + p.id + '">' + p.option_b + ' %' + p.pctB + '</span>' +
                        '</div>' +
                    '</div>' +
                    '<div class="grid grid-cols-2 gap-3 pt-2 border-t border-slate-700/50">' +
                        '<button onclick="voteZarla(\\'' + p.id + '\\', \\'A\\')" class="py-2.5 bg-slate-900 hover:bg-purple-600 border border-slate-700 rounded-xl text-xs font-bold text-slate-200 hover:text-white transition">' + p.option_a + '</button>' +
                        '<button onclick="voteZarla(\\'' + p.id + '\\', \\'B\\')" class="py-2.5 bg-slate-900 hover:bg-pink-600 border border-slate-700 rounded-xl text-xs font-bold text-slate-200 hover:text-white transition">' + p.option_b + '</button>' +
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

                if (res.success) {
                    document.getElementById('z-bar-a-' + pollId).style.width = res.pctA + '%';
                    document.getElementById('z-bar-b-' + pollId).style.width = res.pctB + '%';
                    document.getElementById('z-pct-a-' + pollId).textContent = '%' + res.pctA + ' ' + (document.getElementById('z-pct-a-' + pollId).textContent.split(' ')[1] || '');
                    document.getElementById('z-pct-b-' + pollId).textContent = (document.getElementById('z-pct-b-' + pollId).textContent.split(' ')[0] || '') + ' %' + res.pctB;
                    showToast('🗳️ Oyunuz kaydedildi! Anlık Nabız: %' + (choice === 'A' ? res.pctA : res.pctB));
                }
            } catch (e) {
                showToast(e.message, 'error');
            }
        };

        async function loadPortfolio() {
            const res = await fetch('/api/portfolio').then(r => r.json());
            const actList = document.getElementById('portfolio-active-list');
            const setList = document.getElementById('portfolio-settled-list');
            actList.innerHTML = '';
            setList.innerHTML = '';

            if (res.active.length === 0) {
                actList.innerHTML = '<div class="text-center py-6 text-slate-500 text-xs">Açık bir tahmininiz bulunmuyor.</div>';
            } else {
                res.active.forEach(item => {
                    const card = document.createElement('div');
                    card.className = 'bg-slate-800/80 border border-slate-700/80 rounded-xl p-4 flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4';
                    card.innerHTML = 
                        '<div class="space-y-1">' +
                            '<span class="px-2 py-0.5 rounded text-[10px] font-black uppercase bg-purple-500/20 text-purple-300">' + item.category + '</span>' +
                            '<h4 class="text-sm font-bold text-white cursor-pointer hover:text-purple-300 pf-open-slug" data-slug="' + item.slug + '">' + item.question + '</h4>' +
                            '<div class="text-xs text-slate-400">' +
                                '<span>Tercih: <strong class="' + (item.outcome === 'YES' ? 'text-emerald-400' : 'text-rose-400') + '">' + item.outcome + '</strong></span> | ' +
                                '<span>Pay: <strong class="text-white">' + item.shares + ' Pay</strong></span> | ' +
                                '<span>Yatırılan: <strong class="text-white">' + item.invested + ' KOR</strong></span>' +
                            '</div>' +
                        '</div>' +
                        '<div class="flex items-center gap-3 w-full sm:w-auto justify-between sm:justify-end border-t sm:border-t-0 pt-2 sm:pt-0 border-slate-700">' +
                            '<div class="text-right">' +
                                '<div class="text-[10px] text-slate-400 uppercase font-semibold">Anlık Satış Değeri</div>' +
                                '<div class="text-base font-black text-amber-300">' + item.currentSellValue + ' KOR</div>' +
                            '</div>' +
                            '<button class="bg-rose-600/20 hover:bg-rose-600 border border-rose-500/40 text-rose-300 hover:text-white font-extrabold text-xs px-3.5 py-2 rounded-xl transition btn-sell-pos" data-m="' + item.marketId + '" data-o="' + item.outcome + '" data-s="' + item.shares + '">' +
                                '<i class="fas fa-hand-holding-usd mr-1"></i> Erken Sat' +
                            '</button>' +
                        '</div>';
                    actList.appendChild(card);
                });

                actList.querySelectorAll('.pf-open-slug').forEach(el => el.onclick = () => openMarketDetail(el.getAttribute('data-slug')));
                actList.querySelectorAll('.btn-sell-pos').forEach(el => el.onclick = () => sellPosition(el.getAttribute('data-m'), el.getAttribute('data-o'), el.getAttribute('data-s')));
            }

            if (res.settled.length === 0) {
                setList.innerHTML = '<div class="text-center py-6 text-slate-500 text-xs">Henüz sonuçlanan bir tahmininiz yok.</div>';
            } else {
                res.settled.forEach(item => {
                    const card = document.createElement('div');
                    card.className = 'bg-slate-800/50 border border-slate-700/50 rounded-xl p-4 flex items-center justify-between gap-4';
                    const winBadge = item.won 
                        ? '<span class="text-emerald-400 font-black text-sm">+' + item.payout + ' KOR Kazanç (1 Pay = 1 KOR)</span>'
                        : '<span class="text-rose-400 font-bold text-xs">0 KOR (Tahmin Gerçekleşmedi)</span>';

                    card.innerHTML = 
                        '<div class="space-y-1">' +
                            '<h4 class="text-xs font-bold text-slate-300 cursor-pointer hover:text-purple-300 pf-open-slug" data-slug="' + item.slug + '">' + item.question + '</h4>' +
                            '<div class="text-[11px] text-slate-400">' +
                                '<span>Senin Tercihin: <strong>' + item.outcome + '</strong></span> | ' +
                                '<span>Resmi Sonuç: <strong class="text-white">' + item.resolvedOutcome + '</strong></span>' +
                            '</div>' +
                        '</div>' +
                        '<div class="text-right">' + winBadge + '</div>';
                    setList.appendChild(card);
                });
                setList.querySelectorAll('.pf-open-slug').forEach(el => el.onclick = () => openMarketDetail(el.getAttribute('data-slug')));
            }
        }

        window.sellPosition = async function(marketId, outcome, shares) {
            if (!confirm(shares + ' payınızı anlık fiyattan satıp KOR bakiyenize aktarmak istiyor musunuz?')) return;

            try {
                const res = await fetch('/api/trade/sell', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ marketId, outcome, sharesToSell: shares })
                }).then(r => r.json());

                if (res.error) throw new Error(res.error);

                currentBalance = res.newBalance;
                document.getElementById('user-balance').textContent = currentBalance.toLocaleString('tr-TR') + ' KOR';
                showToast('💰 Pozisyon bozduruldu: +' + res.payoutKor + ' KOR hesabınıza aktarıldı!');
                loadPortfolio();
            } catch (e) {
                showToast(e.message, 'error');
            }
        };

        // ONBOARDING
        document.getElementById('btn-save-onboarding').onclick = async () => {
            const birthYear = document.getElementById('ob-birth-year').value;
            const educationLevel = document.getElementById('ob-education').value;
            const industry = document.getElementById('ob-industry').value;
            const city = document.getElementById('ob-city').value;

            try {
                const res = await fetch('/api/onboarding', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ birthYear, educationLevel, industry, city })
                }).then(r => r.json());

                if (res.error) throw new Error(res.error);

                currentBalance = res.newBalance;
                document.getElementById('user-balance').textContent = currentBalance.toLocaleString('tr-TR') + ' KOR';
                document.getElementById('onboarding-modal').classList.add('hidden');
                showToast('🎉 Profiliniz onaylandı: +1.500 KOR hoş geldin ödülü eklendi!');
            } catch (e) {
                showToast(e.message, 'error');
            }
        };

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

        document.querySelectorAll('.cat-btn').forEach(btn => {
            btn.onclick = (e) => {
                document.querySelectorAll('.cat-btn').forEach(b => b.className = 'cat-btn bg-slate-800 hover:bg-slate-700 text-slate-300 px-4 py-1.5 rounded-lg text-xs font-bold transition');
                e.target.className = 'cat-btn active bg-purple-600 text-white px-4 py-1.5 rounded-lg text-xs font-bold transition';
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
                        const probEl = document.getElementById('prob-' + msg.marketId);
                        const barEl = document.getElementById('bar-' + msg.marketId);
                        const poolEl = document.getElementById('pool-' + msg.marketId);
                        if (probEl) probEl.textContent = 'EVET: %' + msg.probYes;
                        if (barEl) barEl.style.width = msg.probYes + '%';
                        if (poolEl) poolEl.textContent = 'Katılım: ' + msg.poolTotal + ' Puan';

                        if (activeDetailMarket && activeDetailMarket.id === msg.marketId) {
                            activeDetailMarket.probYes = msg.probYes;
                            activeDetailMarket.probNo = msg.probNo;
                            document.getElementById('dt-current-prob').textContent = '%' + msg.probYes;
                            updateDetailChoiceBtns();
                        }
                    }
                    if (msg.type === 'MARKET_RESOLVED') {
                        showToast('📢 Bir pazar sonuçlandırıldı: Kazançlar dağıtıldı!');
                        init();
                    }
                } catch(e) {}
            };
            ws.onclose = () => setTimeout(connectWebSocket, 2500);
        }

        async function init() {
            try {
                const me = await fetch('/api/me').then(r => r.json());
                currentBalance = Math.round(parseFloat(me.balance_kor || 14500));
                document.getElementById('user-balance').textContent = currentBalance.toLocaleString('tr-TR') + ' KOR';
                document.getElementById('user-streak').textContent = (me.streak || 1) + ' Gün';
                document.getElementById('user-tier-badge').textContent = me.tier || 'Analist';

                if (!me.onboarding_completed) {
                    document.getElementById('onboarding-modal').classList.remove('hidden');
                }

                const res = await fetch('/api/markets').then(r => r.json());
                markets = res.markets || [];
                renderMarkets();

                const path = window.location.pathname;
                if (path.startsWith('/market/')) {
                    const slug = path.split('/')[2];
                    openMarketDetail(slug);
                } else if (path.startsWith('/profile/')) {
                    const uname = path.split('/')[2];
                    openProfile(uname);
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

// ROUTE HANDLERS
app.get('/', async (req, reply) => reply.type('text/html').send(renderIndexHtml()));
app.get('/market/:slug', async (req, reply) => reply.type('text/html').send(renderIndexHtml()));
app.get('/profile/:username', async (req, reply) => reply.type('text/html').send(renderIndexHtml()));

// ==========================================
// 5. BAŞLATICI
// ==========================================
await initDatabase();
const port = Number(process.env.PORT) || 3000;
app.listen({ port, host: '0.0.0.0' }, (err, address) => {
    if (err) { console.error(err); process.exit(1); }
    console.log(`[OYVER] FINAL READY PRODUCTION ENGINE AKTİF: ${address}`);
});
