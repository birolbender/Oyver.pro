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
        console.info('[DATABASE] Şema, Profilleme ve 16 Pazar doğrulanıyor...');
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
                personality_archetype VARCHAR(64) DEFAULT 'Stratejist',
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

            CREATE TABLE IF NOT EXISTS p2p_duels (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                challenger_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                challenged_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                stake_kor NUMERIC(24,6) NOT NULL DEFAULT 5000.000000,
                status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
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
                votes_a INT NOT NULL DEFAULT 5420,
                votes_b INT NOT NULL DEFAULT 4580,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
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

        // Sütun Güvenceleri (Migration Kalkanı)
        await client.query(`
            ALTER TABLE markets ADD COLUMN IF NOT EXISTS sub_category VARCHAR(64);
            ALTER TABLE users ADD COLUMN IF NOT EXISTS tier VARCHAR(32) NOT NULL DEFAULT 'Araştırmacı';
            ALTER TABLE users ADD COLUMN IF NOT EXISTS kvkk_accepted BOOLEAN NOT NULL DEFAULT FALSE;
            ALTER TABLE users ADD COLUMN IF NOT EXISTS personality_archetype VARCHAR(64) DEFAULT 'Stratejist';
            ALTER TABLE users ADD COLUMN IF NOT EXISTS profile_step INT NOT NULL DEFAULT 1;
        `);

        for (const code of ['1000', '3000', '4000', '5000']) {
            await client.query(`INSERT INTO accounts (code) VALUES ($1) ON CONFLICT DO NOTHING`, [code]);
        }

        // Kullanıcı Tohumlama (Yalnızca ilk DB kurulumunda)
        await client.query(`
            INSERT INTO users (id, email, username, password_hash, role, balance_kor, streak, tier, personality_archetype) VALUES
            ('11111111-1111-1111-1111-111111111111', 'demo@oyver.pro', 'LeisanB', 'OAUTH_MOCK', 'USER', 14500, 5, 'Doçent', 'Stratejist'),
            ('22222222-2222-2222-2222-222222222222', 'ahmet@oyver.pro', 'Ahmet_Analist', 'OAUTH_MOCK', 'USER', 420000, 14, 'Ordinaryüs', 'Öncü'),
            ('33333333-3333-3333-3333-333333333333', 'ece@oyver.pro', 'Ece_Hoca', 'OAUTH_MOCK', 'USER', 315000, 9, 'Profesör', 'Sağlamcı')
            ON CONFLICT (email) DO NOTHING;
        `);

        // Hero Düellolar
        await client.query(`
            INSERT INTO hero_duels (title, category, option_a_name, option_b_name, votes_a, votes_b, is_active) VALUES
            ('2028 Seçim Tercihiniz Hangisi Olur?', 'SİYASET DÜELLOSU', 'Cumhur İttifakı', 'Muhalefet Bloğu', 5420, 4580, true),
            ('Hafta Sonu Süper Lig Derbisini Kim Kazanır?', 'SPOR DÜELLOSU', 'Galatasaray', 'Fenerbahçe', 5120, 4880, true)
            ON CONFLICT DO NOTHING;
        `);

        // 16 ZENGİN PAZAR (GÜVENLİ UPSERT - ESKİ HAVUZLARI ASLA EZMEZ)
        const rich16Markets = [
            // BORSA & FİNANS
            { slug: 'bist-100-2026', cat: 'BORSA & FİNANS', sub: 'BIST 100', q: 'BIST 100 Endeksi 2026 Son Çeyreğini 12.000 Puan Üzerinde Kapatır mı?', srcName: 'Borsa İstanbul Bülteni', closing: '31 Aralık 2026', yesR: 10000, noR: 10000 },
            { slug: 'bist-halka-arz-50', cat: 'BORSA & FİNANS', sub: 'Halka Arz', q: '2026 Yılında Borsa İstanbul\'da Halka Arz Edilen Şirket Sayısı 50\'yi Aşar mı?', srcName: 'SPK Bültenleri', closing: '31 Aralık 2026', yesR: 12000, noR: 8000 },
            { slug: 'ons-altin-3000', cat: 'BORSA & FİNANS', sub: 'Altın & Emtia', q: 'Ons Altın Fiyatı 2026 Sonuna Kadar 3.000 Dolar Seviyesini Görür mü?', srcName: 'Londra Külçe Piyasası (LBMA)', closing: '31 Aralık 2026', yesR: 9000, noR: 11000 },

            // SPOR
            { slug: 'super-lig-90-puan', cat: 'SPOR', sub: 'Futbol', q: '2026-2027 Süper Lig Şampiyonu 90 Puan Barajını Aşar mı?', srcName: 'TFF Resmi Puan Durumu', closing: '30 Mayıs 2027', yesR: 8000, noR: 12000 },
            { slug: 'derbi-kirmizi-kart', cat: 'SPOR', sub: 'Futbol', q: 'İlk Galatasaray - Fenerbahçe Derbisinde Kırmızı Kart Çıkar mı?', srcName: 'TFF Maç Raporu', closing: '22 Kasım 2026', yesR: 11000, noR: 9000 },
            { slug: 'efes-final-four', cat: 'SPOR', sub: 'Basketbol', q: 'Anadolu Efes 2026-2027 EuroLeague Sezonunda Final Four\'a Kalır mı?', srcName: 'EuroLeague Basketball', closing: '15 Mayıs 2027', yesR: 12000, noR: 8000 },
            { slug: 'f1-ferrari-sampiyon', cat: 'SPOR', sub: 'Formula 1', q: '2026 Formula 1 Takımlar Şampiyonluğunu Ferrari Kazanır mı?', srcName: 'FIA Resmi Sonuçları', closing: '28 Kasım 2026', yesR: 13000, noR: 7000 },

            // EKONOMİ & YAŞAM
            { slug: 'asgari-ucret-2027', cat: 'EKONOMİ', sub: 'Asgari Ücret', q: '2027 Yılı Net Asgari Ücreti 35.000 TL Üzerinde Açıklanır mı?', srcName: 'Çalışma Bakanlığı / Resmi Gazete', closing: '31 Aralık 2026', yesR: 8000, noR: 12000 },
            { slug: 'tcmb-faiz-2026', cat: 'EKONOMİ', sub: 'Faiz', q: 'TCMB Politika Faizini 2026 Yıl Sonuna Kadar %30 veya Altına İndirir mi?', srcName: 'TCMB PPK Karar Metni', closing: '24 Aralık 2026', yesR: 11000, noR: 9000 },
            { slug: 'enflasyon-tek-hane', cat: 'EKONOMİ', sub: 'Enflasyon', q: 'TÜİK Yıllık Tüketici Enflasyonu 2026 Yılında %20 Altına İner mi?', srcName: 'TÜİK TÜFE Bülteni', closing: '3 Ocak 2027', yesR: 14000, noR: 6000 },

            // TEKNOLOJİ
            { slug: 'turksat-6a-ticari', cat: 'TEKNOLOJİ', sub: 'Uzay & Uydu', q: 'TÜRKSAT 6A Uydusu 2026 Yılında Tam Kapasite Ticari Hizmete Başlar mı?', srcName: 'Ulaştırma Bakanlığı', closing: '15 Kasım 2026', yesR: 6000, noR: 14000 },
            { slug: 'yapay-zeka-kanunu', cat: 'TEKNOLOJİ', sub: 'Yapay Zeka', q: 'TBMM 2026 Yılında Kapsamlı Ulusal Yapay Zeka Yasasını Kabul Eder mi?', srcName: 'Resmi Gazete / TBMM', closing: '31 Aralık 2026', yesR: 10000, noR: 10000 },
            { slug: 'yerli-unicorn-2026', cat: 'TEKNOLOJİ', sub: 'Yapay Zeka', q: '2026 Yılında Türkiye\'den Yeni Bir Unicorn (1 Milyar $ Değerleme) Girişim Çıkar mı?', srcName: 'Sanayi Bakanlığı', closing: '31 Aralık 2026', yesR: 9000, noR: 11000 },

            // MODA & STİL
            { slug: 'ist-moda-haftasi', cat: 'MODA & STİL', sub: 'Trendler', q: 'İstanbul Moda Haftası 2027 Resmi Takviminde 20+ Yabancı Tasarımcı Yer Alır mı?', srcName: 'İHKİB Basın Bülteni', closing: '20 Mart 2027', yesR: 10000, noR: 10000 },
            { slug: 'surdurulebilir-tekstil', cat: 'MODA & STİL', sub: 'Lüks Tüketim', q: 'Türkiye\'nin 2026 Tekstil İhracatında Geri Dönüştürülmüş Ürün Payı %15\'i Aşar mı?', srcName: 'TİM İhracat Raporu', closing: '15 Ocak 2027', yesR: 12000, noR: 8000 },

            // KÜLTÜR & MEDYA
            { slug: 'turizm-ziyaretci-2026', cat: 'KÜLTÜR & YAŞAM', sub: 'Turizm & Yaşam', q: '2026 Yılında Türkiye\'ye Gelen Yabancı Ziyaretçi Sayısı 60 Milyonu Aşar mı?', srcName: 'TÜİK Turizm İstatistikleri', closing: '31 Ocak 2027', yesR: 7000, noR: 13000 }
        ];

        for (const item of rich16Markets) {
            const mRes = await client.query(`
                INSERT INTO markets (slug, category, sub_category, question, source_name, closing_date)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (slug) DO UPDATE SET 
                    category = EXCLUDED.category,
                    sub_category = EXCLUDED.sub_category,
                    question = EXCLUDED.question,
                    source_name = EXCLUDED.source_name,
                    closing_date = EXCLUDED.closing_date
                RETURNING id
            `, [item.slug, item.cat, item.sub, item.q, item.srcName, item.closing]);

            const mId = mRes.rows[0].id;
            await client.query(`INSERT INTO accounts (code, market_id) VALUES ('2100', $1) ON CONFLICT DO NOTHING`, [mId]);
            await client.query(`INSERT INTO amm_state (market_id, yes_reserve, no_reserve) VALUES ($1, $2, $3) ON CONFLICT (market_id) DO NOTHING`, [mId, item.yesR, item.noR]);

            const hCheck = await client.query(`SELECT count(*) FROM market_price_history WHERE market_id = $1`, [mId]);
            if (parseInt(hCheck.rows[0].count, 10) === 0) {
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
        console.info('[DATABASE] 16 Pazar ve Rezervler UPSERT Edildi.');
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

// GÜVENLİ AUTH KANCASI (GUEST İZOLASYONU)
app.addHook('onRequest', async (req) => {
    let token = null;
    const cookie = req.headers.cookie;
    if (cookie) {
        const m = cookie.split(';').find(c => c.trim().startsWith('oyver_token='));
        if (m) token = m.split('=')[1].trim();
    }
    if (token) {
        const r = await pool.query(`SELECT id, role, username, tier FROM users WHERE id = (SELECT user_id FROM sessions WHERE token = $1)`, [token]);
        if (r.rows.length > 0) {
            req.userId = r.rows[0].id;
            req.userRole = r.rows[0].role;
            req.username = r.rows[0].username;
            req.userTier = r.rows[0].tier;
            return;
        }
    }
    // Token yoksa MİSAFİR (İşlem yapamaz, 401 alır)
    req.userId = null;
    req.userRole = 'GUEST';
    req.username = 'Misafir';
    req.userTier = 'Gözlemci';
});

app.get('/ws', { websocket: true }, (connection) => {
    const ws = connection.socket;
    wsClients.add(ws);
    ws.on('close', () => wsClients.delete(ws));
});

app.get('/health', async () => ({ status: 'UP', version: 'v1.4-RECOVERY-PRODUCTION', timestamp: new Date().toISOString() }));

app.get('/api/me', async (req) => {
    if (!req.userId) return { username: 'Misafir', role: 'GUEST', balance_kor: 0, streak: 0, tier: 'Gözlemci', kvkk_accepted: false };
    const r = await pool.query(`SELECT id, username, email, role, balance_kor, streak, tier, kvkk_accepted, profile_step, personality_archetype FROM users WHERE id = $1`, [req.userId]);
    return r.rows[0] || {};
});

// ==========================================
// KADEMELİ PROFİLLEME (3 ADIMLI AKIŞ)
// ==========================================
app.post('/api/profile/step1', async (req, rep) => {
    if (!req.userId) return rep.status(401).send({ error: 'Lütfen giriş yapın.' });
    const { kvkkAccepted, birthYear, city, industry } = req.body || {};
    if (!kvkkAccepted || !birthYear || !city || !industry) return rep.status(400).send({ error: 'Zorunlu alanları doldurun ve KVKK onayını verin.' });

    const u = await pool.query(`
        UPDATE users SET kvkk_accepted = true, birth_year = $1, city = $2, industry = $3, profile_step = GREATEST(profile_step, 2), balance_kor = balance_kor + 1000
        WHERE id = $4 RETURNING balance_kor, profile_step
    `, [birthYear, city, industry, req.userId]);
    return { success: true, balanceKor: Math.round(parseFloat(u.rows[0].balance_kor)), step: u.rows[0].profile_step };
});

app.post('/api/profile/step2', async (req, rep) => {
    if (!req.userId) return rep.status(401).send({ error: 'Lütfen giriş yapın.' });
    const { favTeam, mobilityType, riskChoice } = req.body || {};
    if (!favTeam || !mobilityType || !riskChoice) return rep.status(400).send({ error: 'Eksik bilgi' });

    const u = await pool.query(`
        UPDATE users SET fav_team = $1, mobility_type = $2, risk_choice = $3, profile_step = GREATEST(profile_step, 3), balance_kor = balance_kor + 1500
        WHERE id = $4 RETURNING balance_kor, profile_step
    `, [favTeam, mobilityType, riskChoice, req.userId]);
    return { success: true, balanceKor: Math.round(parseFloat(u.rows[0].balance_kor)), step: u.rows[0].profile_step };
});

app.post('/api/profile/step3', async (req, rep) => {
    if (!req.userId) return rep.status(401).send({ error: 'Lütfen giriş yapın.' });
    const { musicTaste, coffeeHabit, contrarianChoice } = req.body || {};
    if (!musicTaste || !coffeeHabit || !contrarianChoice) return rep.status(400).send({ error: 'Eksik bilgi' });

    const uData = (await pool.query(`SELECT risk_choice FROM users WHERE id = $1`, [req.userId])).rows[0];
    const risk = uData?.risk_choice || 'A';
    let archetype = 'Sağlamcı (Dengeli Tüketici)';
    if (risk === 'A' && contrarianChoice === 'B') archetype = 'Stratejist (Value Investor)';
    else if (risk === 'B' && contrarianChoice === 'B') archetype = 'Öncü (Venture Capitalist)';
    else if (risk === 'B' && contrarianChoice === 'A') archetype = 'Trend Takipçisi (Momentum)';

    const u = await pool.query(`
        UPDATE users SET music_taste = $1, coffee_habit = $2, contrarian_choice = $3, personality_archetype = $4, profile_step = 4, balance_kor = balance_kor + 1000
        WHERE id = $5 RETURNING balance_kor, personality_archetype
    `, [musicTaste, coffeeHabit, contrarianChoice, archetype, req.userId]);
    return { success: true, balanceKor: Math.round(parseFloat(u.rows[0].balance_kor)), archetype: u.rows[0].personality_archetype };
});

// ==========================================
// P2P DÜELLO ARENASI (12 SN SAYAÇLI 16 SORU)
// ==========================================
const P2P_QUESTIONS = [
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
    { id: 11, stage: 2, q: "Bir kutuda 3 kırmızı, 2 mavi top var. Geri koymadan çekilen 2 topun aynı renk olma olasılığı %50'den büyük müdür?", a: "Hayır (%40)", b: "Evet (%60)", correct: "A" },
    { id: 12, stage: 2, q: "Bir hisse %50 düşüp ardından %50 yükselirse ilk maliyetine ulaşır mı?", a: "Ulaşır", b: "Ulaşamaz (%25 zarardadır)", correct: "B" },
    { id: 13, stage: 2, q: "İki zar atıldığında toplamın 7 gelme olasılığı 8 gelme olasılığından yüksek midir?", a: "Evet (6/36 vs 5/36)", b: "Hayır, eşittir", correct: "A" },
    { id: 14, stage: 2, q: "Piyasa faizleri yükseldiğinde mevcut tahvillerin piyasa fiyatı ne yönde hareket eder?", a: "Düşer", b: "Yükselir", correct: "A" },
    { id: 15, stage: 2, q: "Bir test %99 güvenilirdir. Hastalık sıklığı 1/10.000 ise, pozitif çıkan birinin gerçekten hasta olma ihtimali %50'den büyük müdür?", a: "Büyüktür", b: "Küçüktür (Bayes)", correct: "B" },
    { id: 16, stage: 3, q: "TÜİK verilerine göre Türkiye'deki ortalama hanehalkı büyüklüğü kaçtır? (Ondalık yazın, örn: 3.14)", actualValue: 3.14 }
];

app.get('/api/p2p/questions', async () => ({ questions: P2P_QUESTIONS }));

app.post('/api/p2p/challenge', async (req, rep) => {
    if (!req.userId) return rep.status(401).send({ error: 'Meydan okumak için giriş yapmalısınız.' });
    const { targetUsername } = req.body || {};
    const tUser = (await pool.query(`SELECT id FROM users WHERE username ILIKE $1`, [targetUsername])).rows[0];
    if (!tUser) return rep.status(404).send({ error: 'Kullanıcı bulunamadı.' });

    try {
        const result = await runInTransaction(async (c) => {
            const ur = await c.query(`UPDATE users SET balance_kor = balance_kor - 5000 WHERE id = $1 AND balance_kor >= 5000 RETURNING balance_kor`, [req.userId]);
            if (ur.rows.length === 0) throw new Error('En az 5.000 KOR teminatınız olmalıdır.');

            const duelRes = await c.query(`INSERT INTO p2p_duels (challenger_id, challenged_id) VALUES ($1, $2) RETURNING id`, [req.userId, tUser.id]);
            return { duelId: duelRes.rows[0].id, newBalance: MoneyMath.roundDown(ur.rows[0].balance_kor, 0).toNumber() };
        });
        return rep.send({ success: true, ...result });
    } catch(e) {
        return rep.status(400).send({ error: e.message });
    }
});

app.post('/api/p2p/submit-turn', async (req, rep) => {
    if (!req.userId) return rep.status(401).send({ error: 'Giriş yapmalısınız.' });
    const { duelId, answers, finalGuess } = req.body || {};
    const duel = (await pool.query(`SELECT * FROM p2p_duels WHERE id = $1`, [duelId])).rows[0];
    if (!duel) return rep.status(404).send({ error: 'Düello bulunamadı.' });

    let score = 0;
    for (let i = 0; i < 15; i++) {
        if (answers[P2P_QUESTIONS[i].id] === P2P_QUESTIONS[i].correct) score++;
    }

    if (duel.challenger_id === req.userId) {
        await pool.query(`UPDATE p2p_duels SET challenger_score = $1, challenger_final_guess = $2, status = 'CHALLENGER_DONE' WHERE id = $3`, [score, parseFloat(finalGuess) || 0, duelId]);
        return rep.send({ success: true, role: 'CHALLENGER', score });
    } else if (duel.challenged_id === req.userId) {
        const result = await runInTransaction(async (c) => {
            const ur = await c.query(`UPDATE users SET balance_kor = balance_kor - 5000 WHERE id = $1 AND balance_kor >= 5000 RETURNING balance_kor`, [req.userId]);
            if (ur.rows.length === 0) throw new Error('Düelloyu kabul etmek için 5.000 KOR teminatınız olmalıdır.');

            let winnerId = duel.challenger_id;
            if (score > duel.challenger_score) winnerId = duel.challenged_id;
            else if (score === duel.challenger_score) {
                const diffA = Math.abs(parseFloat(duel.challenger_final_guess) - 3.14);
                const diffB = Math.abs((parseFloat(finalGuess) || 0) - 3.14);
                winnerId = diffB < diffA ? duel.challenged_id : duel.challenger_id;
            }

            await c.query(`UPDATE users SET balance_kor = balance_kor + 9600 WHERE id = $1`, [winnerId]);
            await c.query(`UPDATE p2p_duels SET challenged_score = $1, challenged_final_guess = $2, status = 'COMPLETED', winner_id = $3 WHERE id = $4`, [score, parseFloat(finalGuess) || 0, winnerId, duelId]);
            return { winnerId, myScore: score, oppScore: duel.challenger_score };
        });
        return rep.send({ success: true, role: 'CHALLENGED', ...result });
    }
    return rep.status(403).send({ error: 'Yetkisiz.' });
});

app.get('/api/p2p/pending-challenges', async (req) => {
    if (!req.userId) return { pending: [] };
    const r = await pool.query(`
        SELECT d.id, u.username as challenger_name 
        FROM p2p_duels d JOIN users u ON d.challenger_id = u.id 
        WHERE d.challenged_id = $1 AND d.status = 'CHALLENGER_DONE' ORDER BY d.created_at DESC
    `, [req.userId]);
    return { pending: r.rows };
});

// ==========================================
// PAZARLAR & QUOTE & AL-SAT MOTORU
// ==========================================
app.get('/api/markets', async () => {
    const r = await pool.query(`SELECT m.*, COALESCE(a.yes_reserve, 10000) as yes_reserve, COALESCE(a.no_reserve, 10000) as no_reserve FROM markets m LEFT JOIN amm_state a ON m.id = a.market_id ORDER BY m.created_at ASC`);
    return {
        markets: r.rows.map(m => {
            const y = new Decimal(m.yes_reserve), n = new Decimal(m.no_reserve);
            const probYes = Math.round(n.div(y.plus(n)).mul(100).toNumber());
            return { ...m, probYes, probNo: 100 - probYes, poolTotal: Math.round(y.plus(n).toNumber()).toLocaleString('tr-TR') };
        })
    };
});

app.get('/api/markets/:slug', async (req, rep) => {
    const r = await pool.query(`SELECT m.*, COALESCE(a.yes_reserve, 10000) as yes_reserve, COALESCE(a.no_reserve, 10000) as no_reserve FROM markets m LEFT JOIN amm_state a ON m.id = a.market_id WHERE m.slug = $1`, [req.params.slug]);
    if (r.rows.length === 0) return rep.status(404).send({ error: 'Pazar bulunamadı.' });
    const m = r.rows[0];
    const y = new Decimal(m.yes_reserve), n = new Decimal(m.no_reserve);
    const probYes = Math.round(n.div(y.plus(n)).mul(100).toNumber());

    let hr = await pool.query(`SELECT prob_yes, pool_total, to_char(created_at, 'DD Mon HH24:MI') as time_label FROM market_price_history WHERE market_id = $1 ORDER BY created_at ASC`, [m.id]);
    if (hr.rows.length === 0) hr = { rows: [{ prob_yes: 50, time_label: '28 Eki' }, { prob_yes: probYes, time_label: 'Bugün' }] };
    return { market: { ...m, probYes, probNo: 100 - probYes, poolTotal: Math.round(y.plus(n).toNumber()).toLocaleString('tr-TR') }, history: hr.rows };
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
    if (!req.userId) return rep.status(401).send({ error: 'Tahmin girmek için giriş yapmalısınız.' });
    const { marketId, outcome, amountKor } = req.body || {};
    const amt = new Decimal(amountKor || 0);
    if (!marketId || !outcome || amt.lte(0)) return rep.status(400).send({ error: 'Geçersiz parametre.' });

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
    if (!req.userId) return rep.status(401).send({ error: 'İşlem için giriş yapmalısınız.' });
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
    if (!req.userId) return { active: [], settled: [] };
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

// YORUMLAR
app.get('/api/markets/:slug/comments', async (req, rep) => {
    const mRes = await pool.query(`SELECT id FROM markets WHERE slug = $1`, [req.params.slug]);
    if (mRes.rows.length === 0) return rep.status(404).send({ error: 'Pazar bulunamadı' });
    const r = await pool.query(`SELECT c.*, u.username, u.tier, to_char(c.created_at, 'DD Mon YYYY, HH24:MI') as time_formatted FROM comments c JOIN users u ON c.user_id = u.id WHERE c.market_id = $1 ORDER BY c.upvotes DESC, c.created_at DESC`, [mRes.rows[0].id]);
    return { comments: r.rows };
});

app.post('/api/markets/:slug/comments', async (req, rep) => {
    if (!req.userId) return rep.status(401).send({ error: 'Giriş yapmalısınız.' });
    const { content } = req.body || {};
    if (!content || content.trim().length < 5) return rep.status(400).send({ error: 'Analiz en az 5 karakter olmalıdır.' });

    const mRes = await pool.query(`SELECT id FROM markets WHERE slug = $1`, [req.params.slug]);
    const posRes = await pool.query(`SELECT outcome, shares FROM positions WHERE market_id = $1 AND user_id = $2 AND shares > 0.001 LIMIT 1`, [mRes.rows[0].id, req.userId]);
    const outcome = posRes.rows.length > 0 ? posRes.rows[0].outcome : null;
    const shares = posRes.rows.length > 0 ? Math.round(parseFloat(posRes.rows[0].shares)) : 0;

    await pool.query(`INSERT INTO comments (market_id, user_id, content, outcome_at_time, shares_at_time) VALUES ($1, $2, $3, $4, $5)`, [mRes.rows[0].id, req.userId, content.trim(), outcome, shares]);
    return { success: true };
});

// B2B KOHORT & CSV EXPORT
app.get('/api/markets/:slug/cohorts', async (req, rep) => {
    const mRes = await pool.query(`SELECT id FROM markets WHERE slug = $1`, [req.params.slug]);
    if (mRes.rows.length === 0) return rep.status(404).send({ error: 'Pazar bulunamadı' });

    const eduRes = await pool.query(`SELECT u.education_level, p.outcome, COUNT(p.id) as vote_count FROM positions p JOIN users u ON p.user_id = u.id WHERE p.market_id = $1 AND p.shares > 0.001 GROUP BY u.education_level, p.outcome`, [mRes.rows[0].id]);
    const ageRes = await pool.query(`SELECT CASE WHEN (2026 - u.birth_year) < 30 THEN 'Genç (18-29)' WHEN (2026 - u.birth_year) BETWEEN 30 AND 44 THEN 'Orta Yaş (30-44)' ELSE 'Deneyimli (45+)' END as age_group, p.outcome, COUNT(p.id) as vote_count FROM positions p JOIN users u ON p.user_id = u.id WHERE p.market_id = $1 AND p.shares > 0.001 AND u.birth_year IS NOT NULL GROUP BY age_group, p.outcome`, [mRes.rows[0].id]);

    return { educationCohorts: eduRes.rows, ageCohorts: ageRes.rows };
});

app.get('/api/markets/:slug/export', async (req, rep) => {
    const mRes = await pool.query(`SELECT id, question FROM markets WHERE slug = $1`, [req.params.slug]);
    if (mRes.rows.length === 0) return rep.status(404).send('Not found');

    const pRes = await pool.query(`
        SELECT p.outcome, p.shares, p.total_invested, u.city, u.industry, u.birth_year, u.personality_archetype
        FROM positions p JOIN users u ON p.user_id = u.id WHERE p.market_id = $1 AND p.shares > 0.001
    `, [mRes.rows[0].id]);

    let csv = 'Pazar,Yön,Pay,Yatirim_KOR,Sehir,Sektor,DogumYili,Arketip\n';
    pRes.rows.forEach(r => {
        csv += `"${mRes.rows[0].question}","${r.outcome}",${r.shares},${r.total_invested},"${r.city || ''}","${r.industry || ''}","${r.birth_year || ''}","${r.personality_archetype || ''}"\n`;
    });

    rep.header('Content-Type', 'text/csv; charset=utf-8');
    rep.header('Content-Disposition', `attachment; filename="oyver_cohort_${req.params.slug}.csv"`);
    return csv;
});

// LİDERLİK TABLOSU
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

// DÜELLOLAR & OYLA
app.get('/api/duels', async () => {
    const r = await pool.query(`SELECT * FROM hero_duels WHERE is_active = true ORDER BY created_at ASC`);
    return {
        duels: r.rows.map(d => ({
            ...d, pctA: Math.round((d.votes_a / (d.votes_a + d.votes_b)) * 100), pctB: 100 - Math.round((d.votes_a / (d.votes_a + d.votes_b)) * 100), totalVotes: (d.votes_a + d.votes_b).toLocaleString('tr-TR')
        }))
    };
});

app.post('/api/duels/:id/vote', async (req, rep) => {
    const col = req.body?.choice === 'A' ? 'votes_a' : 'votes_b';
    const r = await pool.query(`UPDATE hero_duels SET ${col} = ${col} + 1 WHERE id = $1 RETURNING votes_a, votes_b`, [req.params.id]);
    const tot = r.rows[0].votes_a + r.rows[0].votes_b;
    return { success: true, pctA: Math.round((r.rows[0].votes_a / tot) * 100), pctB: 100 - Math.round((r.rows[0].votes_a / tot) * 100), totalVotes: tot.toLocaleString('tr-TR') };
});

app.get('/api/zarla', async () => {
    const r = await pool.query(`SELECT * FROM zarla_polls ORDER BY created_at ASC`);
    return {
        polls: r.rows.map(p => ({
            ...p, pctA: Math.round((p.votes_a / (p.votes_a + p.votes_b || 1)) * 100), pctB: 100 - Math.round((p.votes_a / (p.votes_a + p.votes_b || 1)) * 100), totalVotes: (p.votes_a + p.votes_b).toLocaleString('tr-TR')
        }))
    };
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
    const email = `${name.toLowerCase()}@oyver.pro`;
    const ur = await pool.query(`
        INSERT INTO users (email, username, password_hash, role, balance_kor, streak, tier)
        VALUES ($1, $2, 'OAUTH_MOCK', 'USER', 14500, 5, 'Doçent')
        ON CONFLICT (email) DO UPDATE SET username = EXCLUDED.username
        RETURNING id, username, role, balance_kor, streak, tier
    `, [email, name]);

    const user = ur.rows[0];
    const token = crypto.randomBytes(32).toString('hex');
    await pool.query(`INSERT INTO sessions (token, user_id) VALUES ($1, $2)`, [token, user.id]);
    rep.header('Set-Cookie', `oyver_token=${token}; Path=/; HttpOnly; SameSite=Lax; Max-Age=2592000`);
    return { success: true, user };
});

// ==========================================
// 4. FRONTEND ARAYÜZÜ (DATA-ACTION DELEGATION İLE YENİDEN İNŞA EDİLDİ)
// ==========================================
function renderIndexHtml() {
    return `<!DOCTYPE html>
<html lang="tr" class="theme-obsidian">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>OYVER PRO - Liyakat Tabanlı Kolektif Öngörü Terminali</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap" rel="stylesheet">
    <style>
        body { font-family: 'Inter', sans-serif; transition: background-color 0.3s ease, color 0.3s ease; }
        .theme-obsidian body { background-color: #080c14; color: #f8fafc; }
        .theme-obsidian .card-bg { background-color: #0d131f; border-color: #1e293b; }
        .theme-bloomberg body { background-color: #0c0d0e; color: #fef3c7; }
        .theme-bloomberg .card-bg { background-color: #14171a; border-color: #292d32; }
        ::-webkit-scrollbar { width: 4px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: #334155; border-radius: 2px; }
    </style>
</head>
<body class="min-h-screen flex flex-col antialiased selection:bg-indigo-600 selection:text-white">

    <div id="toast-container" class="fixed top-5 right-5 z-50 flex flex-col gap-2 pointer-events-none"></div>

    <!-- HEADER (PORTFÖY VE PROFİL YAN YANA) -->
    <header class="sticky top-0 z-40 bg-[#080c14]/90 backdrop-blur-md border-b border-slate-800/80">
        <div class="container mx-auto px-4 h-16 flex items-center justify-between gap-4 max-w-5xl">
            <div class="flex items-center space-x-6">
                <a href="/" data-action="nav-home" class="text-2xl font-black tracking-tight text-white flex items-center gap-1.5">
                    OYVER<span class="text-[11px] px-1.5 py-0.5 rounded bg-indigo-500/20 text-indigo-400 border border-indigo-500/30">PRO</span>
                </a>
                <nav class="hidden sm:flex items-center space-x-2 text-xs font-bold">
                    <button data-action="nav-zarla" class="px-3.5 py-2 rounded-xl text-slate-300 hover:text-white hover:bg-slate-900 transition flex items-center gap-2">
                        <i class="fas fa-poll text-indigo-400"></i> OYLA (Günün Nabzı)
                    </button>
                    <button data-action="open-explainer" class="px-3 py-1.5 rounded-xl text-indigo-300 hover:text-white hover:bg-indigo-950/40 border border-indigo-500/20 transition flex items-center gap-1.5">
                        <i class="fas fa-sparkles text-amber-400"></i> 30s Rehber
                    </button>
                    <button data-action="open-p2p" class="px-3 py-1.5 rounded-xl bg-purple-600/20 text-purple-300 border border-purple-500/30 hover:bg-purple-600 hover:text-white transition flex items-center gap-1.5">
                        <i class="fas fa-swords"></i> ⚔️ P2P Meydan Oku
                    </button>
                </nav>
            </div>

            <div class="flex items-center space-x-2.5">
                <button data-action="toggle-theme" class="px-2.5 py-1.5 rounded-xl bg-slate-900 border border-slate-800 text-xs font-bold text-slate-300 hover:text-white transition flex items-center gap-1.5" title="Tema Değiştir">
                    <i class="fas fa-palette text-amber-400"></i> <span class="hidden md:inline" id="theme-label">Obsidian</span>
                </button>

                <button data-action="nav-portfolio" class="px-3 py-1.5 rounded-xl bg-slate-900 border border-slate-800 text-xs font-bold text-slate-300 hover:text-white hover:border-slate-700 transition flex items-center gap-1.5">
                    <i class="fas fa-briefcase text-emerald-400"></i> <span class="hidden sm:inline">Portföyüm</span>
                </button>

                <div class="relative">
                    <div id="user-badge" class="flex items-center bg-slate-900 border border-slate-800 rounded-xl p-1 pr-3 space-x-2 cursor-pointer hover:border-slate-700 transition" data-action="toggle-user-dropdown">
                        <div class="flex items-center gap-1 bg-slate-950 px-2 py-1 rounded-lg border border-slate-800">
                            <i class="fas fa-fire text-amber-500 text-xs"></i>
                            <span id="user-streak" class="text-xs font-black text-amber-400">5g</span>
                        </div>
                        <div class="text-xs font-bold text-slate-200" id="user-balance">-- KOR</div>
                        <span id="user-tier-badge" class="px-1.5 py-0.5 rounded text-[10px] font-black uppercase bg-indigo-500/20 text-indigo-300 border border-indigo-500/30">Misafir</span>
                        <i class="fas fa-chevron-down text-[10px] text-slate-500"></i>
                    </div>

                    <div id="user-dropdown" class="hidden absolute right-0 top-14 w-64 bg-[#0d131f] border border-slate-800 rounded-2xl shadow-2xl p-2 z-50 space-y-1">
                        <div class="px-3 py-2 border-b border-slate-800/80 mb-1">
                            <div class="text-xs font-bold text-white flex justify-between">
                                <span id="dd-username">Misafir</span>
                                <span class="text-[10px] text-emerald-400 font-bold" id="dd-archetype">-</span>
                            </div>
                            <div class="text-[10px] text-indigo-400 font-bold" id="dd-tier">Gözlemci</div>
                        </div>
                        <button data-action="open-profile-step" class="w-full text-left px-3 py-2 text-xs font-bold text-slate-200 hover:bg-slate-800/60 rounded-xl transition flex items-center gap-2">
                            <i class="fas fa-user-check text-purple-400"></i> Profilini Tamamla (+3.500 KOR)
                        </button>
                        <button data-action="open-inbox" class="w-full text-left px-3 py-2 text-xs font-bold text-amber-300 hover:bg-slate-800/60 rounded-xl transition flex items-center justify-between">
                            <span class="flex items-center gap-2"><i class="fas fa-inbox text-amber-400"></i> Gelen Meydan Okumalar</span>
                            <span id="inbox-badge" class="hidden px-1.5 py-0.2 bg-amber-500 text-slate-950 font-black rounded-full text-[10px]">0</span>
                        </button>
                        <button data-action="open-leaderboard" class="w-full text-left px-3 py-2 text-xs font-bold text-slate-200 hover:bg-slate-800/60 rounded-xl transition flex items-center gap-2">
                            <i class="fas fa-graduation-cap text-amber-400"></i> Akademik Liyakat Ligi
                        </button>
                        <button data-action="open-drawer" data-drawer="about" class="w-full text-left px-3 py-2 text-xs font-bold text-slate-200 hover:bg-slate-800/60 rounded-xl transition flex items-center gap-2">
                            <i class="fas fa-info-circle text-indigo-400"></i> Biz Kimiz?
                        </button>
                        <button data-action="open-drawer" data-drawer="b2b" class="w-full text-left px-3 py-2 text-xs font-bold text-slate-200 hover:bg-slate-800/60 rounded-xl transition flex items-center gap-2">
                            <i class="fas fa-chart-pie text-emerald-400"></i> B2B & Güven Endeksi
                        </button>
                        <div class="border-t border-slate-800 my-1"></div>
                        <button data-action="prompt-login" class="w-full text-left px-3 py-2 text-xs font-bold text-slate-400 hover:text-white hover:bg-slate-800/60 rounded-xl transition flex items-center gap-2">
                            <i class="fas fa-sign-in-alt"></i> Giriş Yap (Mock)
                        </button>
                    </div>
                </div>
            </div>
        </div>
    </header>

    <div class="flex-grow pb-24 sm:pb-12">
        <main id="section-markets">
            <!-- CANLI AKIŞ BANNERI (TICKER) -->
            <section class="py-4 border-b border-slate-900 bg-[#0a0f1d]/70">
                <div class="container mx-auto px-4 max-w-5xl">
                    <div class="flex flex-col sm:flex-row items-center justify-between gap-3 p-3.5 rounded-2xl bg-[#0f172a]/60 border border-slate-800">
                        <div class="space-y-0.5 text-center sm:text-left">
                            <h2 class="text-xs sm:text-sm font-black text-white flex items-center justify-center sm:justify-start gap-1.5">
                                <span>🎯 Geleceği Kokla, Ordinaryüs Kademesine Tırman!</span>
                            </h2>
                            <p class="text-[11px] text-slate-400">Cebinden 1 TL çıkmaz. Liyakat puanınla tahmin yap, itibar kazan.</p>
                        </div>
                        <div class="flex items-center gap-2 text-[11px] font-bold bg-slate-950 px-3 py-1.5 rounded-xl border border-slate-800 text-slate-300">
                            <span class="w-2 h-2 rounded-full bg-emerald-400 animate-ping"></span>
                            <span id="live-ticker-text">Ahmet_Analist Asgari Ücret pazarına 500 KOR ile EVET bildirdi.</span>
                        </div>
                    </div>
                </div>
            </section>

            <!-- HERO DÜELLOLAR -->
            <section class="py-6 border-b border-slate-900">
                <div class="container mx-auto px-4 max-w-5xl">
                    <div class="flex items-center justify-between mb-3">
                        <h3 class="text-xs font-black uppercase tracking-wider text-slate-400 flex items-center gap-2">
                            <span class="w-2 h-2 rounded-full bg-emerald-400 animate-pulse"></span> Canlı Kamuoyu Düelloları
                        </h3>
                    </div>
                    <div id="hero-duels-container" class="grid grid-cols-1 sm:grid-cols-2 gap-4"></div>
                </div>
            </section>

            <!-- AÇILIR OKLU (▾) KATEGORİ SEÇİCİ -->
            <section class="container mx-auto px-4 pt-6 pb-2 max-w-5xl">
                <div class="flex items-center space-x-2 overflow-x-auto pb-2">
                    <button class="cat-btn active bg-indigo-600 text-white px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap" data-action="cat-filter" data-cat="ALL">Tümü</button>
                    <button class="cat-btn bg-slate-900 hover:bg-slate-800 text-slate-300 px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap flex items-center gap-1" data-action="cat-filter" data-cat="BORSA & FİNANS">
                        <span>Borsa & Finans</span> <i class="fas fa-chevron-down text-[10px] text-slate-500"></i>
                    </button>
                    <button class="cat-btn bg-slate-900 hover:bg-slate-800 text-slate-300 px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap flex items-center gap-1" data-action="cat-filter" data-cat="SPOR">
                        <span>Spor</span> <i class="fas fa-chevron-down text-[10px] text-slate-500"></i>
                    </button>
                    <button class="cat-btn bg-slate-900 hover:bg-slate-800 text-slate-300 px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap flex items-center gap-1" data-action="cat-filter" data-cat="EKONOMİ">
                        <span>Ekonomi</span> <i class="fas fa-chevron-down text-[10px] text-slate-500"></i>
                    </button>
                    <button class="cat-btn bg-slate-900 hover:bg-slate-800 text-slate-300 px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap flex items-center gap-1" data-action="cat-filter" data-cat="TEKNOLOJİ">
                        <span>Teknoloji</span> <i class="fas fa-chevron-down text-[10px] text-slate-500"></i>
                    </button>
                    <button class="cat-btn bg-slate-900 hover:bg-slate-800 text-slate-300 px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap flex items-center gap-1" data-action="cat-filter" data-cat="MODA & STİL">
                        <span>Moda & Stil</span> <i class="fas fa-chevron-down text-[10px] text-slate-500"></i>
                    </button>
                </div>
                <div id="sub-cat-container" class="hidden flex items-center space-x-1.5 overflow-x-auto pt-2 pb-1 border-t border-slate-900 mt-2"></div>
            </section>

            <!-- 16 ZENGİN PAZAR KARTI -->
            <section class="container mx-auto px-4 py-4 max-w-5xl">
                <div id="market-grid" class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-5"></div>
            </section>
        </main>

        <!-- PAZAR DETAY TERMİNALİ (/market/:slug) -->
        <main id="section-market-detail" class="hidden container mx-auto px-4 py-6 max-w-5xl">
            <button data-action="nav-home" class="text-xs text-indigo-400 hover:text-indigo-300 font-bold flex items-center gap-1.5 mb-4">
                <i class="fas fa-arrow-left"></i> Tüm Pazarlara Dön
            </button>
            <div class="flex items-center justify-between gap-3 mb-2">
                <span id="dt-category" class="px-2.5 py-0.5 rounded text-[10px] font-black uppercase tracking-wider bg-indigo-500/20 text-indigo-300 border border-indigo-500/30">KATEGORİ</span>
                <span class="text-xs text-slate-400"><i class="far fa-clock mr-1"></i><span id="dt-closing-date">--</span></span>
            </div>
            <h1 id="dt-title" class="text-xl sm:text-2xl font-black text-white mb-6 leading-snug">Pazar Başlığı</h1>

            <div class="flex border-b border-slate-800 mb-6 gap-6 text-xs font-bold">
                <button data-action="dt-switch-tab" data-tab="main" id="dt-tab-main" class="pb-2.5 border-b-2 border-indigo-500 text-white flex items-center gap-1.5">
                    <i class="fas fa-chart-line text-indigo-400"></i> Grafik & İşlem
                </button>
                <button data-action="dt-switch-tab" data-tab="community" id="dt-tab-community" class="pb-2.5 text-slate-400 hover:text-white flex items-center gap-1.5">
                    <i class="fas fa-comments text-pink-400"></i> Topluluk Analizleri <span id="dt-comm-count" class="px-1.5 py-0.2 rounded bg-slate-800 text-[10px]">0</span>
                </button>
                <button data-action="dt-switch-tab" data-tab="cohorts" id="dt-tab-cohorts" class="pb-2.5 text-slate-400 hover:text-white flex items-center gap-1.5">
                    <i class="fas fa-layer-group text-amber-400"></i> B2B Kohort Analitiği <span class="px-1.5 py-0.2 rounded bg-amber-500/20 text-amber-300 text-[10px]">Pro</span>
                </button>
            </div>

            <!-- PANEL 1: GRAFİK & İŞLEM KONSOLU -->
            <div id="dt-view-main" class="grid grid-cols-1 lg:grid-cols-3 gap-6">
                <div class="lg:col-span-2 space-y-6">
                    <div class="card-bg border rounded-2xl p-5 shadow-xl">
                        <div class="flex items-center justify-between mb-4">
                            <div>
                                <div class="text-xs font-bold text-slate-400 uppercase">EVET Olasılık Eğrisi</div>
                                <div class="text-3xl font-black text-emerald-400 mt-0.5" id="dt-current-prob">--%</div>
                            </div>
                        </div>
                        <div class="h-60 w-full relative">
                            <canvas id="marketChart"></canvas>
                        </div>
                    </div>

                    <div class="card-bg border rounded-2xl p-5 space-y-3">
                        <h3 class="text-sm font-bold text-white flex items-center gap-2">
                            <i class="fas fa-shield-alt text-indigo-400"></i> Çözümleme Kriterleri & Resmi Kaynak
                        </h3>
                        <p id="dt-desc" class="text-xs text-slate-400 leading-relaxed">Pazar açıklaması...</p>
                        <div class="p-3 bg-slate-950 rounded-xl border border-slate-800 text-xs space-y-1">
                            <div class="text-slate-400">Tescilli Doğrulama Kaynağı:</div>
                            <a id="dt-source-url" href="#" target="_blank" class="font-bold text-indigo-400 hover:underline flex items-center gap-1">
                                <span id="dt-source-name">Resmi Kurum</span> <i class="fas fa-external-link-alt text-[10px]"></i>
                            </a>
                        </div>
                    </div>
                </div>

                <div class="card-bg border rounded-2xl p-5 shadow-xl space-y-4 h-fit">
                    <div class="flex justify-between items-center pb-2 border-b border-slate-800">
                        <span class="text-xs font-bold text-slate-400 uppercase">Tahmin Konsolu</span>
                        <span id="dt-user-balance" class="text-xs font-black text-amber-300">-- KOR</span>
                    </div>
                    <div class="grid grid-cols-2 gap-2">
                        <button data-action="dt-set-choice" data-choice="YES" id="dt-choice-yes" class="py-2.5 rounded-xl font-black text-xs uppercase border border-emerald-500 bg-emerald-600 text-white">EVET</button>
                        <button data-action="dt-set-choice" data-choice="NO" id="dt-choice-no" class="py-2.5 rounded-xl font-black text-xs uppercase border border-slate-800 bg-slate-950 text-slate-400">HAYIR</button>
                    </div>
                    <div>
                        <label class="block text-xs font-bold text-slate-400 mb-1">Puan Tutarı</label>
                        <input type="number" id="dt-input-amount" value="500" min="50" step="50" class="w-full bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-white font-bold text-sm focus:outline-none focus:border-indigo-500">
                    </div>
                    <button data-action="dt-predict-submit" id="dt-btn-predict" class="w-full py-3 rounded-xl bg-indigo-600 hover:bg-indigo-500 text-white font-extrabold text-xs shadow-lg transition">
                        Tahmini Onayla (KOR)
                    </button>
                </div>
            </div>

            <!-- PANEL 2: TOPLULUK ANALİZLERİ & YORUMLAR -->
            <div id="dt-view-community" class="hidden space-y-5 max-w-3xl">
                <div class="card-bg border rounded-2xl p-5 space-y-3">
                    <h3 class="text-xs font-bold text-white flex items-center gap-2">
                        <i class="fas fa-pen-nib text-indigo-400"></i> Neden Bu Kararı Aldınız? (Gerekçeli Analiz Paylaşın)
                    </h3>
                    <textarea id="comm-input-content" rows="3" placeholder="Öngörünüzü destekleyen resmi veri veya argümanınızı yazın..." class="w-full bg-slate-950 border border-slate-800 rounded-xl p-3 text-xs text-white placeholder-slate-500 focus:outline-none focus:border-indigo-500"></textarea>
                    <div class="flex justify-between items-center pt-1">
                        <span class="text-[11px] text-slate-500">Tercihiniz ve payınız otomatik olarak analizinizde rozetlenir.</span>
                        <button data-action="submit-comment" id="btn-submit-comment" class="px-5 py-2 bg-indigo-600 hover:bg-indigo-500 rounded-xl text-xs font-bold text-white transition">
                            Analizi Yayınla
                        </button>
                    </div>
                </div>
                <div id="dt-comments-list" class="space-y-3"></div>
            </div>

            <!-- PANEL 3: B2B KOHORT DÖKÜMÜ & CSV İNDİRME -->
            <div id="dt-view-cohorts" class="hidden space-y-5">
                <div class="flex justify-between items-center bg-slate-900/60 p-4 rounded-2xl border border-slate-800">
                    <div>
                        <h4 class="text-xs font-bold text-white uppercase">Kamuoyu Demografik Kırılımı</h4>
                        <p class="text-[11px] text-slate-400">Katılımcıların yaş ve eğitim düzeyi dağılımı.</p>
                    </div>
                    <button data-action="download-cohort-csv" class="px-3.5 py-2 bg-emerald-600/20 hover:bg-emerald-600 border border-emerald-500/40 text-emerald-300 hover:text-white rounded-xl text-xs font-bold transition flex items-center gap-1.5">
                        <i class="fas fa-file-csv"></i> Ham Veriyi İndir (CSV)
                    </button>
                </div>
                <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
                    <div class="card-bg border rounded-2xl p-5 space-y-3">
                        <h4 class="text-xs font-bold text-slate-300 uppercase flex items-center gap-1.5">
                            <i class="fas fa-users text-indigo-400"></i> Yaş Kohortuna Göre Dağılım
                        </h4>
                        <div id="cohort-age-list" class="space-y-2 text-xs"></div>
                    </div>
                    <div class="card-bg border rounded-2xl p-5 space-y-3">
                        <h4 class="text-xs font-bold text-slate-300 uppercase flex items-center gap-1.5">
                            <i class="fas fa-graduation-cap text-indigo-400"></i> Eğitim Düzeyine Göre Dağılım
                        </h4>
                        <div id="cohort-edu-list" class="space-y-2 text-xs"></div>
                    </div>
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
                    <button data-action="nav-portfolio" class="text-xs text-indigo-400 hover:underline"><i class="fas fa-sync-alt mr-1"></i> Yenile</button>
                </div>
                <div id="portfolio-active-list" class="space-y-3"></div>
            </div>
            <div>
                <h2 class="text-lg font-black text-emerald-400 mb-3 pb-2 border-b border-slate-800">Sonuçlanan Tahminlerim</h2>
                <div id="portfolio-settled-list" class="space-y-3"></div>
            </div>
        </main>
    </div>

    <!-- 30 SANİYEDE OYVER REHBERİ -->
    <div id="explainer-modal" class="fixed inset-0 bg-slate-950/85 z-50 backdrop-blur-md hidden flex items-center justify-center p-4">
        <div class="card-bg border w-full max-w-lg rounded-3xl p-6 shadow-2xl space-y-4">
            <div class="flex justify-between items-center pb-2 border-b border-slate-800">
                <h3 class="text-sm font-black text-white flex items-center gap-2">
                    <i class="fas fa-graduation-cap text-indigo-400"></i> 30 Saniyede OYVER PRO Rehberi
                </h3>
                <button data-action="close-explainer" class="text-slate-400 hover:text-white"><i class="fas fa-times"></i></button>
            </div>
            <div class="space-y-3 text-xs leading-relaxed text-slate-300">
                <div class="p-3 bg-slate-950 rounded-2xl border border-slate-800 space-y-1">
                    <span class="font-black text-amber-400">💰 1. KOR Nedir? Cebimden Para Çıkar mı?</span>
                    <p class="text-slate-400">Kesinlikle hayır! 14.500 KOR liyakat puanı ücretsiz verilir. Kumar değildir; gerçek bilgi ve öngörü yeteneğini ölçer.</p>
                </div>
                <div class="p-3 bg-slate-950 rounded-2xl border border-slate-800 space-y-1">
                    <span class="font-black text-pink-400">🗳️ 2. OYLA (Günün Nabzı) Nedir?</span>
                    <p class="text-slate-400">Puan riski olmadan, gündemin en sıcak konularına tek tıkla oy verip halkın anlık yüzdesini gördüğün kamuoyu alanıdır.</p>
                </div>
                <div class="p-3 bg-slate-950 rounded-2xl border border-slate-800 space-y-1">
                    <span class="font-black text-indigo-400">🎓 3. Ordinaryüs Kademesi Nasıl Kazanılır?</span>
                    <p class="text-slate-400">Tahminlerin resmi bültenlerle doğrulandıkça Brier kalibrasyon puanın artar. Sırasıyla Doçent, Profesör ve zirvede Ordinaryüs olursun!</p>
                </div>
            </div>
            <button data-action="close-explainer" class="w-full py-2.5 bg-indigo-600 hover:bg-indigo-500 rounded-xl font-bold text-white text-xs transition">
                Anladım, Öngörüye Başla!
            </button>
        </div>
    </div>

    <!-- P2P DÜELLO ARENASI & WHATSAPP PAYLAŞIM MODALI -->
    <div id="p2p-modal" class="fixed inset-0 bg-slate-950/90 z-50 backdrop-blur-md hidden flex items-center justify-center p-4">
        <div class="card-bg border w-full max-w-xl rounded-3xl p-6 shadow-2xl space-y-4">
            <div class="flex justify-between items-center pb-3 border-b border-slate-800">
                <div class="flex items-center gap-2">
                    <span class="text-lg">⚔️</span>
                    <h3 class="text-base font-black text-white">Birebir Analist Meydan Okuması (5.000 KOR)</h3>
                </div>
                <button data-action="close-p2p" class="text-slate-400 hover:text-white"><i class="fas fa-times"></i></button>
            </div>
            
            <div id="p2p-init-view" class="space-y-4 text-xs">
                <p class="text-slate-300">10 Eleme, 5 Yarı Final ve 1 Altın Final sorusunu çöz, rakibini mağlup et ve 9.600 KOR ödülü kazan!</p>
                <div class="p-3 bg-slate-950 rounded-xl border border-slate-800 space-y-1">
                    <span class="font-bold text-indigo-400">🛡️ 12 Saniye Hile Kalkanı:</span>
                    <p class="text-slate-400">Her soru için süreniz tam 12 saniyedir.</p>
                </div>
                <div>
                    <label class="block font-bold text-slate-300 mb-1">Meydan Okunacak Kullanıcı Adı</label>
                    <input type="text" id="p2p-target-user" placeholder="Örn: Ece_Hoca, LeisanB" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white">
                </div>
                <button data-action="p2p-start-challenge" class="w-full py-3 bg-indigo-600 hover:bg-indigo-500 rounded-xl font-bold text-white text-xs shadow-lg transition">
                    5.000 KOR Teminatla Meydan Oku & Tura Başla
                </button>
            </div>

            <div id="p2p-quiz-view" class="hidden space-y-4 text-xs">
                <div class="flex justify-between items-center bg-slate-950 p-3 rounded-xl border border-slate-800">
                    <span class="font-bold text-indigo-400" id="quiz-progress">Soru 1 / 16</span>
                    <div class="flex items-center gap-1 text-amber-400 font-black text-sm">
                        <i class="far fa-clock"></i> <span id="quiz-timer">12</span>s
                    </div>
                </div>
                <div class="p-4 bg-slate-950 rounded-2xl border border-slate-800 min-h-[90px] flex items-center">
                    <p class="text-sm font-bold text-white leading-relaxed" id="quiz-question">Soru yükleniyor...</p>
                </div>
                <div class="grid grid-cols-2 gap-3" id="quiz-options-box">
                    <button data-action="p2p-answer" data-choice="A" id="btn-opt-a" class="py-3 px-3 bg-slate-900 border border-slate-800 rounded-xl text-xs font-bold text-slate-200 transition">Seçenek A</button>
                    <button data-action="p2p-answer" data-choice="B" id="btn-opt-b" class="py-3 px-3 bg-slate-900 border border-slate-800 rounded-xl text-xs font-bold text-slate-200 transition">Seçenek B</button>
                </div>
                <div id="quiz-golden-box" class="hidden space-y-2">
                    <input type="number" step="0.01" id="quiz-golden-input" placeholder="Nokta atışı sayıyı yazın (Örn: 3.14)" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-3 text-white font-bold text-sm">
                    <button data-action="p2p-submit-golden" class="w-full py-2.5 bg-amber-600 hover:bg-amber-500 rounded-xl font-bold text-white transition">Altın Tahmini Gönder</button>
                </div>
            </div>

            <div id="p2p-done-view" class="hidden space-y-4 text-xs text-center">
                <div class="w-12 h-12 bg-emerald-500/20 text-emerald-400 rounded-full flex items-center justify-center text-xl mx-auto">
                    <i class="fas fa-check"></i>
                </div>
                <h4 class="text-sm font-black text-white">Soruları Tamamladınız!</h4>
                <p class="text-slate-400">Skorunuz kaydedildi. Şimdi rakibinize WhatsApp üzerinden davet gönderin:</p>
                <div class="flex gap-2">
                    <a id="btn-whatsapp-share" href="#" target="_blank" class="flex-1 py-3 bg-emerald-600 hover:bg-emerald-500 rounded-xl font-bold text-white flex items-center justify-center gap-2 transition">
                        <i class="fab fa-whatsapp text-base"></i> WhatsApp ile Davet Et
                    </a>
                </div>
            </div>
        </div>
    </div>

    <!-- GELEN MEYDAN OKUMALAR (INBOX) MODALI -->
    <div id="inbox-modal" class="fixed inset-0 bg-slate-950/90 z-50 backdrop-blur-md hidden flex items-center justify-center p-4">
        <div class="card-bg border w-full max-w-lg rounded-3xl p-6 shadow-2xl space-y-4">
            <div class="flex justify-between items-center pb-2 border-b border-slate-800">
                <h3 class="text-sm font-black text-white flex items-center gap-2">
                    <i class="fas fa-inbox text-amber-400"></i> Bekleyen Meydan Okumalar
                </h3>
                <button data-action="close-inbox" class="text-slate-400 hover:text-white"><i class="fas fa-times"></i></button>
            </div>
            <div id="inbox-list" class="space-y-3 text-xs"></div>
        </div>
    </div>

    <!-- KADEMELİ PROFİLLEME VE 4 KİŞİLİK ARKETİPİ MODALI -->
    <div id="profile-step-modal" class="fixed inset-0 bg-slate-950/90 z-50 backdrop-blur-md hidden flex items-center justify-center p-4">
        <div class="card-bg border w-full max-w-lg rounded-3xl p-6 shadow-2xl space-y-4">
            <div class="flex justify-between items-center pb-2 border-b border-slate-800">
                <h3 class="text-sm font-black text-white flex items-center gap-2">
                    <i class="fas fa-brain text-purple-400"></i> Kademeli Profilleme & Kişilik Analizi
                </h3>
                <button data-action="close-profile-step" class="text-slate-400 hover:text-white"><i class="fas fa-times"></i></button>
            </div>

            <!-- Adım 1 -->
            <div id="prof-step-1" class="space-y-3 text-xs">
                <div class="p-3 bg-slate-950 rounded-xl border border-slate-800 text-[11px] text-slate-400 leading-relaxed">
                    <label class="flex items-start gap-2 cursor-pointer">
                        <input type="checkbox" id="kvkk-check" class="mt-0.5 rounded border-slate-700 bg-slate-900 text-indigo-600">
                        <span><strong>KVKK Açık Rıza:</strong> Tercihlerimin anonimleştirilerek istatistiki araştırmalarda ve B2B kohort analizlerinde işlenmesine onay veriyorum.</span>
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
                <button data-action="submit-step-1" class="w-full py-2.5 bg-indigo-600 hover:bg-indigo-500 rounded-xl font-bold text-white transition">+1.000 KOR Kazan & 2. Adıma Geç</button>
            </div>

            <!-- Adım 2 -->
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
                    <span class="font-bold text-purple-300">🧠 Karar Sorusu 1 (Risk Algısı):</span>
                    <p class="text-slate-300">Bir yarışmada son aşamaya geldiniz. Hangisini seçersiniz?</p>
                    <div class="space-y-1">
                        <label class="flex items-center gap-2 cursor-pointer"><input type="radio" name="r-risk" value="A" checked> <span>Kesin 15.000 TL alıp çekilmek (Garantici)</span></label>
                        <label class="flex items-center gap-2 cursor-pointer"><input type="radio" name="r-risk" value="B"> <span>%50 şansla 45.000 TL kazanmak veya 0 TL (Risk Arayan)</span></label>
                    </div>
                </div>
                <button data-action="submit-step-2" class="w-full py-2.5 bg-indigo-600 hover:bg-indigo-500 rounded-xl font-bold text-white transition">+1.500 KOR Kazan & Son Adıma Geç</button>
            </div>

            <!-- Adım 3 -->
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
                    <span class="font-bold text-purple-300">🧠 Karar Sorusu 2 (Aykırı Düşünce):</span>
                    <p class="text-slate-300">Piyasada veya toplumda herkes sizinle aynı fikirdeyse ne hissedersiniz?</p>
                    <div class="space-y-1">
                        <label class="flex items-center gap-2 cursor-pointer"><input type="radio" name="r-contra" value="A" checked> <span>Rahatlar ve doğru yolda olduğumu düşünürüm (Konsensüs)</span></label>
                        <label class="flex items-center gap-2 cursor-pointer"><input type="radio" name="r-contra" value="B"> <span>Şüphelenir, sürüde nerede hata olduğunu ararım (Aykırı Zeka)</span></label>
                    </div>
                </div>
                <button data-action="submit-step-3" class="w-full py-2.5 bg-emerald-600 hover:bg-emerald-500 rounded-xl font-bold text-white transition">+1.000 KOR Kazan & Arketipini Gör</button>
            </div>
        </div>
    </div>

    <!-- KURUMSAL VE BİLGİ MODALI (%93.2 GÜVEN ENDEKSİ DAHİL) -->
    <div id="info-modal" class="fixed inset-0 bg-slate-950/85 z-50 backdrop-blur-md hidden flex items-center justify-center p-4">
        <div class="card-bg border w-full max-w-lg rounded-3xl p-6 max-h-[85vh] overflow-y-auto space-y-4">
            <div class="flex justify-between items-center pb-3 border-b border-slate-800">
                <h3 class="text-base font-black text-white" id="info-modal-title">Başlık</h3>
                <button data-action="close-info-modal" class="text-slate-400 hover:text-white"><i class="fas fa-times"></i></button>
            </div>
            <div id="info-modal-content" class="text-xs text-slate-300 leading-relaxed space-y-3"></div>
        </div>
    </div>

    <!-- AKADEMİK LİYAKAT LİDERLİK TABLOSU -->
    <div id="leaderboard-modal" class="fixed inset-0 bg-slate-950/85 z-50 backdrop-blur-md hidden flex items-center justify-center p-4">
        <div class="card-bg border w-full max-w-xl rounded-3xl p-5 max-h-[85vh] flex flex-col">
            <div class="flex justify-between items-center pb-3 border-b border-slate-800 mb-3">
                <div class="flex items-center gap-2"><i class="fas fa-graduation-cap text-amber-400"></i><h3 class="text-base font-black text-white">Akademik Liyakat Ligi</h3></div>
                <button data-action="close-leaderboard" class="text-slate-400"><i class="fas fa-times"></i></button>
            </div>
            <div class="overflow-y-auto flex-grow divide-y divide-slate-800/80" id="leaderboard-list"></div>
        </div>
    </div>

    <!-- MASAÜSTÜ FOOTER -->
    <footer class="hidden sm:block bg-[#05080f] border-t border-slate-900 py-10 mt-auto text-xs text-slate-400">
        <div class="container mx-auto px-4 max-w-5xl">
            <div class="grid grid-cols-1 md:grid-cols-4 gap-8 mb-8">
                <div class="space-y-2 md:col-span-2">
                    <a href="/" data-action="nav-home" class="text-xl font-black tracking-tight text-white flex items-center gap-1.5">
                        OYVER<span class="text-[10px] px-1.5 py-0.5 rounded bg-indigo-500/20 text-indigo-400 border border-indigo-500/30">PRO</span>
                    </a>
                    <p class="text-slate-400 text-xs leading-relaxed max-w-md">
                        Türkiye'nin liyakat tabanlı ilk kolektif öngörü ve kamuoyu araştırma terminali.
                    </p>
                </div>
                <div>
                    <h4 class="text-white font-bold mb-3 uppercase tracking-wider text-[11px]">Platform</h4>
                    <ul class="space-y-2">
                        <li><button data-action="open-drawer" data-drawer="about" class="hover:text-indigo-400 transition">Biz Kimiz?</button></li>
                        <li><button data-action="open-leaderboard" class="hover:text-indigo-400 transition">Akademik Sıralama</button></li>
                        <li><button data-action="nav-zarla" class="hover:text-indigo-400 transition">OYLA Nabız</button></li>
                    </ul>
                </div>
                <div>
                    <h4 class="text-white font-bold mb-3 uppercase tracking-wider text-[11px]">Kurumsal</h4>
                    <ul class="space-y-2">
                        <li><button data-action="open-drawer" data-drawer="b2b" class="hover:text-indigo-400 transition text-emerald-400">B2B Veri & Güven Endeksi</button></li>
                        <li><button data-action="open-drawer" data-drawer="contact" class="hover:text-indigo-400 transition">İletişim & Destek</button></li>
                        <li><button data-action="open-drawer" data-drawer="rules" class="hover:text-indigo-400 transition">Liyakat Kuralları</button></li>
                    </ul>
                </div>
            </div>
            <div class="pt-6 border-t border-slate-900 flex flex-col sm:flex-row items-center justify-between gap-4 text-[11px] text-slate-500">
                <p>© 2026 OYVER PRO. Tüm hakları saklıdır.</p>
                <p>Sanal itibar puanı (KOR) kapalı devredir, nakit karşılığı ve çekimi yoktur.</p>
            </div>
        </div>
    </footer>

    <!-- MOBİL ALT NAVİGASYON BARI -->
    <nav class="sm:hidden fixed bottom-0 left-0 right-0 z-40 bg-[#080c14]/95 backdrop-blur-lg border-t border-slate-800/80 px-2 py-2 flex items-center justify-around shadow-2xl">
        <button data-action="nav-home" class="flex flex-col items-center gap-1 text-slate-400 hover:text-indigo-400 transition">
            <i class="fas fa-compass text-base"></i><span class="text-[10px] font-bold">Pazarlar</span>
        </button>
        <button data-action="nav-zarla" class="flex flex-col items-center gap-1 text-slate-400 hover:text-indigo-400 transition">
            <i class="fas fa-poll text-base"></i><span class="text-[10px] font-bold">OYLA</span>
        </button>
        <button data-action="nav-portfolio" class="flex flex-col items-center gap-1 text-slate-400 hover:text-emerald-400 transition">
            <i class="fas fa-briefcase text-base"></i><span class="text-[10px] font-bold">Portföy</span>
        </button>
        <button data-action="open-leaderboard" class="flex flex-col items-center gap-1 text-slate-400 hover:text-amber-400 transition">
            <i class="fas fa-graduation-cap text-base"></i><span class="text-[10px] font-bold">Liyakat</span>
        </button>
    </nav>

    <!-- MERKEZİ JAVASCRIPT MOTORU -->
    <script>
        var markets = [];
        var heroDuels = [];
        var currentBalance = 0;
        var activeCategory = 'ALL';
        var activeSubCategory = 'ALL';
        var activeDetailMarket = null;
        var activeDetailChoice = 'YES';
        var chartInstance = null;

        var p2pActiveDuelId = null;
        var p2pCurrentQIdx = 0;
        var p2pQuestions = [];
        var p2pAnswers = {};
        var p2pTimerInterval = null;

        function escapeHtml(str) {
            if (!str) return '';
            return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#039;');
        }

        function showToast(msg, type) {
            var container = document.getElementById('toast-container');
            var toast = document.createElement('div');
            var bg = (type === 'error') ? 'bg-rose-950 border-rose-500/50 text-rose-200' : 'bg-emerald-950 border-emerald-500/50 text-emerald-200';
            toast.className = 'flex items-center gap-2 px-4 py-3 rounded-2xl border shadow-xl text-xs font-bold transition-all duration-300 opacity-0 transform -translate-y-2 pointer-events-auto ' + bg;
            toast.innerHTML = '<i class="fas fa-info-circle"></i><span>' + escapeHtml(msg) + '</span>';
            container.appendChild(toast);
            setTimeout(function() { toast.classList.remove('opacity-0', '-translate-y-2'); }, 10);
            setTimeout(function() {
                toast.classList.add('opacity-0', '-translate-y-2');
                setTimeout(function() { toast.remove(); }, 300);
            }, 3500);
        }

        function toggleTheme() {
            var html = document.documentElement;
            var label = document.getElementById('theme-label');
            if (html.classList.contains('theme-obsidian')) {
                html.classList.remove('theme-obsidian');
                html.classList.add('theme-bloomberg');
                if (label) label.textContent = 'Bloomberg';
                try { localStorage.setItem('oyver_theme', 'bloomberg'); } catch(e){}
            } else {
                html.classList.remove('theme-bloomberg');
                html.classList.add('theme-obsidian');
                if (label) label.textContent = 'Obsidian';
                try { localStorage.setItem('oyver_theme', 'obsidian'); } catch(e){}
            }
        }

        try {
            if (localStorage.getItem('oyver_theme') === 'bloomberg') {
                document.documentElement.classList.remove('theme-obsidian');
                document.documentElement.classList.add('theme-bloomberg');
            }
        } catch(e){}

        function switchTab(target) {
            var secMarkets = document.getElementById('section-markets');
            var secMarketDetail = document.getElementById('section-market-detail');
            var secZarla = document.getElementById('section-zarla');
            var secPortfolio = document.getElementById('section-portfolio');

            secMarkets.classList.add('hidden');
            secMarketDetail.classList.add('hidden');
            secZarla.classList.add('hidden');
            secPortfolio.classList.add('hidden');

            if (target === 'markets') secMarkets.classList.remove('hidden');
            if (target === 'zarla') { secZarla.classList.remove('hidden'); loadZarla(); }
            if (target === 'portfolio') { secPortfolio.classList.remove('hidden'); loadPortfolio(); }
        }

        function navigateToHome() {
            history.pushState({}, '', '/');
            switchTab('markets');
        }

        // KART İÇİ HIZLI OY KUTUSU (INLINE POPOVER)
        function toggleQuickVoteBox(marketId, outcome) {
            var box = document.getElementById('quick-box-' + marketId);
            if (!box) return;

            document.querySelectorAll('[id^="quick-box-"]').forEach(function(b) {
                if (b.id !== 'quick-box-' + marketId) b.classList.add('hidden');
            });

            box.classList.toggle('hidden');
            if (!box.classList.contains('hidden')) {
                box.setAttribute('data-outcome', outcome);
                var badge = document.getElementById('qb-outcome-' + marketId);
                if (badge) {
                    badge.textContent = (outcome === 'YES') ? 'EVET' : 'HAYIR';
                    badge.className = (outcome === 'YES') ? 'text-emerald-400 font-black' : 'text-rose-400 font-black';
                }
                updateQuickBoxCalculation(marketId);
            }
        }

        function setQuickAmount(marketId, val) {
            var input = document.getElementById('qb-input-' + marketId);
            if (input) {
                input.value = val;
                updateQuickBoxCalculation(marketId);
            }
        }

        async function updateQuickBoxCalculation(marketId) {
            var m = markets.find(function(x) { return x.id === marketId; });
            var box = document.getElementById('quick-box-' + marketId);
            var input = document.getElementById('qb-input-' + marketId);
            var payoutSpan = document.getElementById('qb-payout-' + marketId);
            if (!m || !box || !input || !payoutSpan) return;

            var outcome = box.getAttribute('data-outcome') || 'YES';
            var amt = parseFloat(input.value) || 0;
            if (amt <= 0) return;

            try {
                var res = await fetch('/api/trade/quote', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ marketId: m.id, outcome: outcome, amountKor: amt, action: 'BUY' })
                }).then(function(r){ return r.json(); });

                if (res.sharesOut) {
                    payoutSpan.textContent = '+' + res.targetPayout;
                }
            } catch(e){}
        }

        async function submitQuickVote(marketId) {
            var m = markets.find(function(x) { return x.id === marketId; });
            var box = document.getElementById('quick-box-' + marketId);
            var input = document.getElementById('qb-input-' + marketId);
            if (!m || !box || !input) return;

            var outcome = box.getAttribute('data-outcome') || 'YES';
            var amt = input.value;

            try {
                var res = await fetch('/api/trade/predict', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ marketId: m.id, outcome: outcome, amountKor: amt })
                }).then(function(r){ return r.json(); });

                if (res.error) throw new Error(res.error);
                currentBalance = res.balanceKor;
                document.getElementById('user-balance').textContent = currentBalance.toLocaleString('tr-TR') + ' KOR';
                showToast('✅ Tercih deftere işlendi! Alınan Pay: ' + res.sharesOut);
                box.classList.add('hidden');
            } catch(e) {
                showToast(e.message, 'error');
            }
        }

        // 16 PAZAR KARTINI BAS
        function renderMarkets() {
            var container = document.getElementById('market-grid');
            if (!container) return;
            container.innerHTML = '';

            var filtered = (activeCategory === 'ALL') ? markets : markets.filter(function(m){ return m.category === activeCategory; });
            if (activeSubCategory !== 'ALL') {
                filtered = filtered.filter(function(m){ return m.sub_category === activeSubCategory; });
            }

            filtered.forEach(function(m) {
                var card = document.createElement('article');
                card.className = 'card-bg border rounded-2xl p-4 shadow-lg flex flex-col justify-between transition';

                card.innerHTML = 
                    '<div>' +
                        '<div class="flex items-center justify-between mb-2">' +
                            '<span class="px-2 py-0.5 rounded text-[9px] font-black uppercase tracking-wider bg-indigo-500/20 text-indigo-300 border border-indigo-500/30">' + escapeHtml(m.category) + (m.sub_category ? ' • ' + escapeHtml(m.sub_category) : '') + '</span>' +
                            '<span class="text-[10px] text-slate-400"><i class="far fa-clock mr-1"></i>' + escapeHtml(m.closing_date) + '</span>' +
                        '</div>' +
                        '<h3 class="text-xs sm:text-sm font-bold text-white mb-2 leading-snug cursor-pointer hover:text-indigo-400 transition" data-action="open-detail" data-slug="' + m.slug + '">' + escapeHtml(m.question) + '</h3>' +
                        '<div class="text-[10px] text-slate-500 mb-3 flex items-center gap-1"><i class="fas fa-landmark"></i><span>' + escapeHtml(m.source_name) + '</span></div>' +
                        '<div class="space-y-1 mb-4 cursor-pointer" data-action="open-detail" data-slug="' + m.slug + '">' +
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
                            '<button type="button" data-action="quick-vote-open" data-id="' + m.id + '" data-outcome="YES" class="py-2.5 px-3 bg-emerald-950/60 hover:bg-emerald-600 border border-emerald-500/40 text-emerald-300 hover:text-white rounded-xl text-xs font-black transition flex items-center justify-center gap-1">' +
                                'EVET (%' + m.probYes + ')' +
                            '</button>' +
                            '<button type="button" data-action="quick-vote-open" data-id="' + m.id + '" data-outcome="NO" class="py-2.5 px-3 bg-rose-950/60 hover:bg-rose-600 border border-rose-500/40 text-rose-300 hover:text-white rounded-xl text-xs font-black transition flex items-center justify-center gap-1">' +
                                'HAYIR (%' + m.probNo + ')' +
                            '</button>' +
                        '</div>' +
                        
                        '<div id="quick-box-' + m.id + '" class="hidden p-3 bg-slate-950 rounded-xl border border-indigo-500/40 space-y-2 text-xs mb-1">' +
                            '<div class="flex justify-between items-center">' +
                                '<span class="text-[11px] font-bold text-slate-300">Yön: <span id="qb-outcome-' + m.id + '">EVET</span></span>' +
                                '<span class="text-[10px] text-slate-500">Tutar Seç:</span>' +
                            '</div>' +
                            '<div class="grid grid-cols-4 gap-1.5">' +
                                '<button type="button" data-action="quick-amount-select" data-id="' + m.id + '" data-amount="100" class="py-1 bg-slate-900 border border-slate-700 rounded-lg text-[10px] font-bold hover:border-indigo-400">100</button>' +
                                '<button type="button" data-action="quick-amount-select" data-id="' + m.id + '" data-amount="250" class="py-1 bg-slate-900 border border-slate-700 rounded-lg text-[10px] font-bold hover:border-indigo-400">250</button>' +
                                '<button type="button" data-action="quick-amount-select" data-id="' + m.id + '" data-amount="500" class="py-1 bg-slate-900 border border-slate-700 rounded-lg text-[10px] font-bold hover:border-indigo-400">500</button>' +
                                '<button type="button" data-action="quick-amount-select" data-id="' + m.id + '" data-amount="1000" class="py-1 bg-slate-900 border border-slate-700 rounded-lg text-[10px] font-bold hover:border-indigo-400">1.000</button>' +
                            '</div>' +
                            '<input type="number" id="qb-input-' + m.id + '" value="250" class="w-full bg-slate-900 border border-slate-700 rounded-xl p-2 text-white font-bold text-xs">' +
                            '<div class="flex justify-between text-[11px] text-slate-400 font-semibold">' +
                                '<span>Hedef:</span><span id="qb-payout-' + m.id + '" class="text-emerald-400 font-black">+-- KOR</span>' +
                            '</div>' +
                            '<button type="button" data-action="quick-submit" data-id="' + m.id + '" class="w-full py-2 bg-indigo-600 hover:bg-indigo-500 rounded-xl font-bold text-white text-xs transition">' +
                                'Deftere İşle' +
                            '</button>' +
                        '</div>' +

                        '<button type="button" data-action="open-detail" data-slug="' + m.slug + '" class="w-full py-1.5 text-center text-[11px] font-bold text-slate-400 hover:text-white transition flex items-center justify-center gap-1">' +
                            '<span>Terminal & Derin Analiz</span> <i class="fas fa-chevron-right text-[9px]"></i>' +
                        '</button>' +
                    '</div>';

                container.appendChild(card);
            });
        }

        // HERO DÜELLOLAR
        async function loadHeroDuels() {
            var res = await fetch('/api/duels').then(function(r){ return r.json(); });
            heroDuels = res.duels || [];
            var container = document.getElementById('hero-duels-container');
            if (!container) return;
            container.innerHTML = '';

            heroDuels.forEach(function(d) {
                var card = document.createElement('div');
                card.className = 'card-bg border rounded-2xl p-4 shadow-xl flex flex-col justify-between';

                card.innerHTML = 
                    '<div>' +
                        '<div class="flex justify-between items-center mb-2">' +
                            '<span class="px-2 py-0.5 rounded text-[9px] font-black uppercase tracking-wider bg-indigo-500/20 text-indigo-300 border border-indigo-500/30">' + escapeHtml(d.category) + '</span>' +
                            '<span class="text-[10px] text-slate-400 font-semibold">' + d.totalVotes + ' Oy</span>' +
                        '</div>' +
                        '<h3 class="text-xs sm:text-sm font-extrabold text-white leading-snug mb-2">' + escapeHtml(d.title) + '</h3>' +
                        '<div class="space-y-1.5 mt-2">' +
                            '<div class="w-full bg-slate-950 rounded-full h-2.5 overflow-hidden flex border border-slate-800">' +
                                '<div class="bg-indigo-500 h-full transition-all duration-700" style="width:' + d.pctA + '%"></div>' +
                                '<div class="bg-rose-500 h-full transition-all duration-700" style="width:' + d.pctB + '%"></div>' +
                            '</div>' +
                            '<div class="flex justify-between items-center text-[11px] font-black">' +
                                '<span class="text-indigo-400">' + escapeHtml(d.option_a_name) + ' %' + d.pctA + '</span>' +
                                '<span class="text-rose-400">' + escapeHtml(d.option_b_name) + ' %' + d.pctB + '</span>' +
                            '</div>' +
                        '</div>' +
                    '</div>' +
                    '<div class="grid grid-cols-2 gap-2 mt-3 pt-2 border-t border-slate-800">' +
                        '<button type="button" data-action="duel-vote" data-id="' + d.id + '" data-choice="A" class="py-2 px-2 bg-slate-950 hover:bg-indigo-600 border border-slate-800 hover:border-indigo-500 rounded-xl text-[11px] font-bold text-slate-200 hover:text-white transition">' +
                            'Oy Ver: ' + escapeHtml(d.option_a_name) +
                        '</button>' +
                        '<button type="button" data-action="duel-vote" data-id="' + d.id + '" data-choice="B" class="py-2 px-2 bg-slate-950 hover:bg-rose-600 border border-slate-800 hover:border-rose-500 rounded-xl text-[11px] font-bold text-slate-200 hover:text-white transition">' +
                            'Oy Ver: ' + escapeHtml(d.option_b_name) +
                        '</button>' +
                    '</div>';

                container.appendChild(card);
            });
        }

        async function voteDuel(id, choice) {
            try {
                var res = await fetch('/api/duels/' + id + '/vote', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ choice: choice })
                }).then(function(r){ return r.json(); });

                if (res.error) throw new Error(res.error);
                showToast('🎉 Oyunuz kaydedildi!');
                loadHeroDuels();
            } catch(e) {
                showToast(e.message, 'error');
            }
        }

        // DETAY SAYFASI
        async function openMarketDetail(slug) {
            history.pushState({}, '', '/market/' + slug);
            switchTab('detail');
            document.getElementById('section-market-detail').classList.remove('hidden');

            try {
                var res = await fetch('/api/markets/' + slug).then(function(r){ return r.json(); });
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
                renderChart(res.history);
            } catch(e) {
                showToast(e.message, 'error');
                navigateToHome();
            }
        }

        function switchDetailTab(tab) {
            ['main', 'community', 'cohorts'].forEach(function(t) {
                var btn = document.getElementById('dt-tab-' + t);
                var view = document.getElementById('dt-view-' + t);
                if (btn) btn.className = 'pb-2.5 text-slate-400 hover:text-white flex items-center gap-1.5';
                if (view) view.classList.add('hidden');
            });
            var activeBtn = document.getElementById('dt-tab-' + tab);
            var activeView = document.getElementById('dt-view-' + tab);
            if (activeBtn) activeBtn.className = 'pb-2.5 border-b-2 border-indigo-500 text-white flex items-center gap-1.5';
            if (activeView) activeView.classList.remove('hidden');

            if (tab === 'community') loadComments();
            if (tab === 'cohorts') loadCohorts();
        }

        function renderChart(history) {
            var ctx = document.getElementById('marketChart').getContext('2d');
            if (chartInstance) chartInstance.destroy();

            chartInstance = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: history.map(function(h){ return h.time_label; }),
                    datasets: [{
                        label: 'EVET (%)',
                        data: history.map(function(h){ return h.prob_yes; }),
                        borderColor: '#10b981',
                        backgroundColor: 'rgba(16, 185, 129, 0.08)',
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
                        y: { min: 0, max: 100, grid: { color: 'rgba(30, 41, 59, 0.5)' }, ticks: { color: '#64748b' } },
                        x: { grid: { display: false }, ticks: { color: '#64748b' } }
                    },
                    plugins: { legend: { display: false } }
                }
            });
        }

        function updateDetailChoiceBtns() {
            if (!activeDetailMarket) return;
            var bY = document.getElementById('dt-choice-yes');
            var bN = document.getElementById('dt-choice-no');
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

        async function submitPredict() {
            var amt = document.getElementById('dt-input-amount').value;
            var btn = document.getElementById('dt-btn-predict');
            btn.disabled = true;

            try {
                var res = await fetch('/api/trade/predict', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ marketId: activeDetailMarket.id, outcome: activeDetailChoice, amountKor: amt })
                }).then(function(r){ return r.json(); });

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
        }

        async function loadComments() {
            if (!activeDetailMarket) return;
            var res = await fetch('/api/markets/' + activeDetailMarket.slug + '/comments').then(function(r){ return r.json(); });
            var list = document.getElementById('dt-comments-list');
            list.innerHTML = '';
            document.getElementById('dt-comm-count').textContent = res.comments.length;

            if (res.comments.length === 0) {
                list.innerHTML = '<div class="text-center py-6 text-slate-500 text-xs">Henüz analiz paylaşılmadı. İlk görüşü siz yazın!</div>';
                return;
            }

            res.comments.forEach(function(c) {
                var card = document.createElement('div');
                card.className = 'card-bg rounded-2xl p-4 space-y-2 text-xs border';
                var stanceBadge = '';
                if (c.outcome_at_time) {
                    var isYes = (c.outcome_at_time === 'YES');
                    stanceBadge = '<span class="px-2 py-0.5 rounded text-[10px] font-black border ' + (isYes ? 'bg-emerald-500/20 text-emerald-300 border-emerald-500/30' : 'bg-rose-500/20 text-rose-300 border-rose-500/30') + '">' + (isYes ? '🟢 EVET' : '🔴 HAYIR') + ' (' + c.shares_at_time + ' Pay)</span>';
                }

                card.innerHTML = 
                    '<div class="flex justify-between items-center">' +
                        '<div class="flex items-center gap-2">' +
                            '<strong class="text-white">' + escapeHtml(c.username) + '</strong>' +
                            '<span class="text-[9px] text-indigo-400 font-bold">[' + escapeHtml(c.tier) + ']</span>' +
                            stanceBadge +
                        '</div>' +
                        '<span class="text-[10px] text-slate-500">' + escapeHtml(c.time_formatted) + '</span>' +
                    '</div>' +
                    '<p class="text-slate-300 leading-relaxed">' + escapeHtml(c.content) + '</p>';
                list.appendChild(card);
            });
        }

        async function submitComment() {
            var content = document.getElementById('comm-input-content').value;
            if (!content || content.trim().length < 5) return showToast('Lütfen en az 5 karakter yazın.', 'error');

            try {
                var res = await fetch('/api/markets/' + activeDetailMarket.slug + '/comments', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ content: content })
                }).then(function(r){ return r.json(); });

                if (res.error) throw new Error(res.error);
                document.getElementById('comm-input-content').value = '';
                showToast('✅ Analiziniz yayınlandı!');
                loadComments();
            } catch(e) {
                showToast(e.message, 'error');
            }
        }

        async function loadCohorts() {
            if (!activeDetailMarket) return;
            var res = await fetch('/api/markets/' + activeDetailMarket.slug + '/cohorts').then(function(r){ return r.json(); });
            var ageContainer = document.getElementById('cohort-age-list');
            var eduContainer = document.getElementById('cohort-edu-list');
            ageContainer.innerHTML = '';
            eduContainer.innerHTML = '';

            res.ageCohorts.forEach(function(c) {
                var row = document.createElement('div');
                row.className = 'flex justify-between p-2.5 bg-slate-950 rounded-xl border border-slate-800 font-bold text-xs';
                row.innerHTML = '<span>' + escapeHtml(c.age_group) + '</span><span class="' + (c.outcome === 'YES' ? 'text-emerald-400' : 'text-rose-400') + '">' + c.outcome + ' (' + c.vote_count + ')</span>';
                ageContainer.appendChild(row);
            });
            res.educationCohorts.forEach(function(c) {
                var row = document.createElement('div');
                row.className = 'flex justify-between p-2.5 bg-slate-950 rounded-xl border border-slate-800 font-bold text-xs';
                row.innerHTML = '<span>' + escapeHtml(c.education_level) + '</span><span class="' + (c.outcome === 'YES' ? 'text-emerald-400' : 'text-rose-400') + '">' + c.outcome + ' (' + c.vote_count + ')</span>';
                eduContainer.appendChild(row);
            });
        }

        // P2P ARENA
        async function openP2PModal() {
            var res = await fetch('/api/p2p/questions').then(function(r){ return r.json(); });
            p2pQuestions = res.questions;
            document.getElementById('p2p-modal').classList.remove('hidden');
            document.getElementById('p2p-init-view').classList.remove('hidden');
            document.getElementById('p2p-quiz-view').classList.add('hidden');
            document.getElementById('p2p-done-view').classList.add('hidden');
        }

        function closeP2PModal() {
            clearInterval(p2pTimerInterval);
            document.getElementById('p2p-modal').classList.add('hidden');
        }

        async function startP2PChallenge() {
            var target = document.getElementById('p2p-target-user').value;
            if (!target) return showToast('Lütfen rakip kullanıcı adı girin.', 'error');

            try {
                var res = await fetch('/api/p2p/challenge', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ targetUsername: target })
                }).then(function(r){ return r.json(); });

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
        }

        function loadNextP2PQuestion() {
            clearInterval(p2pTimerInterval);
            if (p2pCurrentQIdx >= 15) {
                document.getElementById('quiz-progress').textContent = 'BÜYÜK FİNAL: NOKTA ATIŞI TAHMİN';
                document.getElementById('quiz-question').textContent = p2pQuestions[15].q;
                document.getElementById('quiz-options-box').classList.add('hidden');
                document.getElementById('quiz-golden-box').classList.remove('hidden');
                document.getElementById('quiz-timer').textContent = '20';

                var left = 20;
                p2pTimerInterval = setInterval(function() {
                    left--;
                    document.getElementById('quiz-timer').textContent = left;
                    if (left <= 0) {
                        clearInterval(p2pTimerInterval);
                        submitGoldenShot();
                    }
                }, 1000);
                return;
            }

            var q = p2pQuestions[p2pCurrentQIdx];
            document.getElementById('quiz-progress').textContent = 'Soru ' + (p2pCurrentQIdx + 1) + ' / 16';
            document.getElementById('quiz-question').textContent = q.q;
            document.getElementById('btn-opt-a').textContent = q.a;
            document.getElementById('btn-opt-b').textContent = q.b;

            document.getElementById('quiz-timer').textContent = '12';
            var timeLeft = 12;
            p2pTimerInterval = setInterval(function() {
                timeLeft--;
                document.getElementById('quiz-timer').textContent = timeLeft;
                if (timeLeft <= 0) {
                    clearInterval(p2pTimerInterval);
                    answerP2P('TIMEOUT');
                }
            }, 1000);
        }

        function answerP2P(choice) {
            var q = p2pQuestions[p2pCurrentQIdx];
            p2pAnswers[q.id] = choice;
            p2pCurrentQIdx++;
            loadNextP2PQuestion();
        }

        async function submitGoldenShot() {
            clearInterval(p2pTimerInterval);
            var val = document.getElementById('quiz-golden-input').value || 3.0;

            try {
                var res = await fetch('/api/p2p/submit-turn', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ duelId: p2pActiveDuelId, answers: p2pAnswers, finalGuess: val })
                }).then(function(r){ return r.json(); });

                if (res.error) throw new Error(res.error);

                document.getElementById('p2p-quiz-view').classList.add('hidden');
                document.getElementById('p2p-done-view').classList.remove('hidden');

                var shareUrl = window.location.origin + '?p2p=' + p2pActiveDuelId;
                var waText = encodeURIComponent('OYVER PRO üzerinde sana 5.000 KOR ödüllü bir meydan okuma başlattım! Bilgine güveniyorsan hemen katıl: ' + shareUrl);
                document.getElementById('btn-whatsapp-share').href = 'https://wa.me/?text=' + waText;

                showToast('🎉 Turlarınız tamamlandı!');
            } catch(e) {
                showToast(e.message, 'error');
            }
        }

        async function checkPendingInbox() {
            var res = await fetch('/api/p2p/pending-challenges').then(function(r){ return r.json(); });
            var badge = document.getElementById('inbox-badge');
            if (res.pending && res.pending.length > 0) {
                badge.textContent = res.pending.length;
                badge.classList.remove('hidden');
            } else {
                badge.classList.add('hidden');
            }
        }

        async function openInboxModal() {
            var res = await fetch('/api/p2p/pending-challenges').then(function(r){ return r.json(); });
            var list = document.getElementById('inbox-list');
            list.innerHTML = '';
            document.getElementById('inbox-modal').classList.remove('hidden');

            if (!res.pending || res.pending.length === 0) {
                list.innerHTML = '<div class="text-center py-4 text-slate-500">Bekleyen meydan okuma bulunmuyor.</div>';
                return;
            }

            res.pending.forEach(function(item) {
                var div = document.createElement('div');
                div.className = 'p-3 bg-slate-950 border border-slate-800 rounded-2xl flex items-center justify-between';
                div.innerHTML = '<div><strong class="text-white">' + escapeHtml(item.challenger_name) + '</strong> sana 5.000 KOR meydan okudu!</div>' +
                    '<button data-action="accept-challenge" data-id="' + item.id + '" class="px-3 py-1.5 bg-emerald-600 hover:bg-emerald-500 text-white rounded-xl font-bold">Kabul Et</button>';
                list.appendChild(div);
            });
        }

        // KADEMELİ PROFİLLEME
        function openProfileStepModal() {
            document.getElementById('profile-step-modal').classList.remove('hidden');
        }

        async function submitStep1() {
            var check = document.getElementById('kvkk-check').checked;
            if (!check) return showToast('Lütfen KVKK açık rızasını onaylayın.', 'error');

            var res = await fetch('/api/profile/step1', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    kvkkAccepted: true,
                    birthYear: document.getElementById('p-birth').value,
                    city: document.getElementById('p-city').value,
                    industry: document.getElementById('p-ind').value
                })
            }).then(function(r){ return r.json(); });

            if (res.error) return showToast(res.error, 'error');
            showToast('🎉 +1.000 KOR Eklendi! 2. Adıma geçtiniz.');
            currentBalance = res.balanceKor;
            document.getElementById('user-balance').textContent = currentBalance.toLocaleString('tr-TR') + ' KOR';
            document.getElementById('prof-step-1').classList.add('hidden');
            document.getElementById('prof-step-2').classList.remove('hidden');
        }

        async function submitStep2() {
            var risk = document.querySelector('input[name="r-risk"]:checked').value;
            var res = await fetch('/api/profile/step2', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    favTeam: document.getElementById('p-team').value,
                    mobilityType: document.getElementById('p-mob').value,
                    riskChoice: risk
                })
            }).then(function(r){ return r.json(); });

            if (res.error) return showToast(res.error, 'error');
            showToast('🎉 +1.500 KOR Eklendi! Son adıma geçtiniz.');
            currentBalance = res.balanceKor;
            document.getElementById('user-balance').textContent = currentBalance.toLocaleString('tr-TR') + ' KOR';
            document.getElementById('prof-step-2').classList.add('hidden');
            document.getElementById('prof-step-3').classList.remove('hidden');
        }

        async function submitStep3() {
            var contra = document.querySelector('input[name="r-contra"]:checked').value;
            var res = await fetch('/api/profile/step3', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    musicTaste: document.getElementById('p-music').value,
                    coffeeHabit: document.getElementById('p-coffee').value,
                    contrarianChoice: contra
                })
            }).then(function(r){ return r.json(); });

            if (res.error) return showToast(res.error, 'error');
            showToast('🏆 Profil tamamlandı! Arketipiniz: ' + res.archetype);
            currentBalance = res.balanceKor;
            document.getElementById('user-balance').textContent = currentBalance.toLocaleString('tr-TR') + ' KOR';
            document.getElementById('dd-archetype').textContent = res.archetype;
            document.getElementById('profile-step-modal').classList.add('hidden');
        }

        // LİDERLİK TABLOSU
        async function openLeaderboard() {
            var d = await fetch('/api/leaderboard').then(function(r){ return r.json(); });
            var list = document.getElementById('leaderboard-list');
            list.innerHTML = '';

            d.top100.forEach(function(u) {
                var item = document.createElement('div');
                item.className = 'py-3 flex items-center justify-between text-xs px-2';
                item.innerHTML = 
                    '<div class="flex items-center gap-2.5">' +
                        '<span class="w-6 h-6 rounded-full bg-slate-900 border border-slate-800 flex items-center justify-center font-black text-[10px] text-amber-400">#' + u.rank + '</span>' +
                        '<div>' +
                            '<strong class="text-white">' + escapeHtml(u.name) + '</strong>' +
                            '<span class="text-[9px] text-indigo-400 font-bold ml-1.5 px-1.5 py-0.5 rounded bg-indigo-500/10 border border-indigo-500/20">' + escapeHtml(u.tier) + '</span>' +
                        '</div>' +
                    '</div>' +
                    '<div class="text-right font-black text-indigo-400">' + u.frsScore.toLocaleString('tr-TR') + ' FRS</div>';
                list.appendChild(item);
            });
            document.getElementById('leaderboard-modal').classList.remove('hidden');
        }

        // PORTFÖY
        async function loadPortfolio() {
            var res = await fetch('/api/portfolio').then(function(r){ return r.json(); });
            var actList = document.getElementById('portfolio-active-list');
            var setList = document.getElementById('portfolio-settled-list');
            actList.innerHTML = '';
            setList.innerHTML = '';

            if (res.active.length === 0) {
                actList.innerHTML = '<div class="text-center py-4 text-slate-500 text-xs">Açık tahmininiz yok.</div>';
            } else {
                res.active.forEach(function(item) {
                    var card = document.createElement('div');
                    card.className = 'card-bg border rounded-2xl p-4 flex items-center justify-between gap-4 text-xs';
                    card.innerHTML = 
                        '<div><h4 class="font-bold text-white cursor-pointer hover:text-indigo-400" data-action="open-detail" data-slug="' + item.slug + '">' + escapeHtml(item.question) + '</h4><div class="text-slate-400 text-[11px]">Tercih: ' + item.outcome + ' | Değer: <strong class="text-amber-300">' + item.currentSellValue + ' KOR</strong></div></div>' +
                        '<button data-action="sell-position" data-id="' + item.marketId + '" data-outcome="' + item.outcome + '" data-shares="' + item.shares + '" class="bg-rose-950/40 hover:bg-rose-600 border border-rose-500/30 text-rose-300 hover:text-white px-3 py-1.5 rounded-xl font-bold transition">Sat</button>';
                    actList.appendChild(card);
                });
            }

            if (res.settled.length === 0) {
                setList.innerHTML = '<div class="text-center py-4 text-slate-500 text-xs">Sonuçlanan tahmininiz yok.</div>';
            } else {
                res.settled.forEach(function(item) {
                    var card = document.createElement('div');
                    card.className = 'card-bg border rounded-2xl p-3 flex items-center justify-between text-xs';
                    card.innerHTML = '<div>' + escapeHtml(item.question) + '</div><div class="font-bold text-emerald-400">+' + item.payout + ' KOR</div>';
                    setList.appendChild(card);
                });
            }
        }

        async function sellPosition(marketId, outcome, shares) {
            var res = await fetch('/api/trade/sell', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ marketId: marketId, outcome: outcome, sharesToSell: shares })
            }).then(function(r){ return r.json(); });

            if (res.error) return showToast(res.error, 'error');
            currentBalance = res.newBalance;
            document.getElementById('user-balance').textContent = currentBalance.toLocaleString('tr-TR') + ' KOR';
            showToast('💰 Pozisyon satıldı: +' + res.payoutKor + ' KOR');
            loadPortfolio();
        }

        // ZARLA
        async function loadZarla() {
            var res = await fetch('/api/zarla').then(function(r){ return r.json(); });
            var list = document.getElementById('zarla-list');
            list.innerHTML = '';
            res.polls.forEach(function(p) {
                var card = document.createElement('div');
                card.className = 'card-bg border rounded-2xl p-4 shadow-lg space-y-3';
                card.innerHTML = 
                    '<div class="flex justify-between items-center text-xs"><span class="px-2 py-0.5 rounded text-[9px] font-black uppercase bg-indigo-500/20 text-indigo-300">' + escapeHtml(p.category) + '</span><span class="text-slate-500">' + p.totalVotes + ' Oy</span></div>' +
                    '<h3 class="text-xs sm:text-sm font-bold text-white">' + escapeHtml(p.question) + '</h3>' +
                    '<div class="grid grid-cols-2 gap-2 text-xs font-bold">' +
                        '<button data-action="vote-zarla" data-id="' + p.id + '" data-choice="A" class="py-2 bg-slate-950 hover:bg-slate-800 border border-slate-800 rounded-xl text-slate-200 transition">' + escapeHtml(p.option_a) + ' (%' + p.pctA + ')</button>' +
                        '<button data-action="vote-zarla" data-id="' + p.id + '" data-choice="B" class="py-2 bg-slate-950 hover:bg-slate-800 border border-slate-800 rounded-xl text-slate-200 transition">' + escapeHtml(p.option_b) + ' (%' + p.pctB + ')</button>' +
                    '</div>';
                list.appendChild(card);
            });
        }

        async function voteZarla(pollId, choice) {
            await fetch('/api/zarla/vote', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ pollId: pollId, choice: choice }) });
            showToast('🗳️ Oyunuz kaydedildi!');
            loadZarla();
        }

        // KURUMSAL ÇEKMECELER
        function openDrawer(type) {
            var modal = document.getElementById('info-modal');
            var title = document.getElementById('info-modal-title');
            var content = document.getElementById('info-modal-content');
            modal.classList.remove('hidden');

            if (type === 'about') {
                title.textContent = 'Biz Kimiz & Vizyonumuz';
                content.innerHTML = 
                    '<p><strong>OYVER PRO</strong>, manipülasyondan uzak, ölçülebilir ve liyakat tabanlı bir kolektif kamuoyu tahmin terminalidir.</p>' +
                    '<p>Katılımcılar sanal KOR puanlarıyla öngörülerini bildirir. AMM motoru toplumun gerçek beklenti eğrisini anlık hesaplar.</p>' +
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
                        '<p class="text-[11px] text-slate-300 leading-relaxed">OYVER kullanıcılarının geçmişte %70 olasılık verdiği 100 olayın 71\'i resmi bültenlerle doğrulanmıştır.</p>' +
                    '</div>' +
                    '<p>Kurumunuz için özel pazar açma ve kohort verisi entegrasyonu için formu doldurabilirsiniz:</p>' +
                    '<div class="space-y-2 mt-3">' +
                        '<input type="text" id="b2b-company" placeholder="Kurum / Şirket Adı" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white">' +
                        '<input type="email" id="b2b-email" placeholder="Kurumsal E-posta" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white">' +
                        '<textarea id="b2b-msg" rows="2" placeholder="Talebiniz..." class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white"></textarea>' +
                        '<button data-action="submit-b2b-form" class="w-full py-2.5 bg-indigo-600 rounded-xl font-bold text-white transition">Kurumsal İletişim Başlat</button>' +
                    '</div>';
            } else if (type === 'contact') {
                title.textContent = 'Bize Ulaşın & Destek';
                content.innerHTML = 
                    '<p>Ekibimize doğrudan mesaj gönderebilirsiniz:</p>' +
                    '<div class="space-y-2 mt-3">' +
                        '<input type="text" id="cnt-name" placeholder="Adınız" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white">' +
                        '<input type="email" id="cnt-email" placeholder="E-posta" class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white">' +
                        '<textarea id="cnt-msg" rows="3" placeholder="Mesajınız..." class="w-full bg-slate-950 border border-slate-800 rounded-xl p-2.5 text-white"></textarea>' +
                        '<button data-action="submit-contact-form" class="w-full py-2.5 bg-indigo-600 rounded-xl font-bold text-white transition">Mesajı İlet</button>' +
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
        }

        // ==========================================
        // TEK MERKEZİ EVENT DELEGATION DİNLİYİCİSİ
        // ==========================================
        document.addEventListener('click', function(e) {
            var btn = e.target.closest('[data-action]');
            if (!btn) return;
            var act = btn.getAttribute('data-action');

            if (act === 'nav-home') {
                e.preventDefault();
                navigateToHome();
            } else if (act === 'nav-zarla') {
                switchTab('zarla');
            } else if (act === 'nav-portfolio') {
                switchTab('portfolio');
            } else if (act === 'toggle-theme') {
                toggleTheme();
            } else if (act === 'toggle-user-dropdown') {
                toggleUserDropdown();
            } else if (act === 'open-explainer') {
                document.getElementById('explainer-modal').classList.remove('hidden');
            } else if (act === 'close-explainer') {
                document.getElementById('explainer-modal').classList.add('hidden');
            } else if (act === 'open-p2p') {
                openP2PModal();
            } else if (act === 'close-p2p') {
                closeP2PModal();
            } else if (act === 'p2p-start-challenge') {
                startP2PChallenge();
            } else if (act === 'p2p-answer') {
                answerP2P(btn.getAttribute('data-choice'));
            } else if (act === 'p2p-submit-golden') {
                submitGoldenShot();
            } else if (act === 'copy-p2p-link') {
                var url = window.location.origin + '?p2p=' + p2pActiveDuelId;
                navigator.clipboard.writeText(url);
                showToast('📋 Link panoya kopyalandı!');
            } else if (act === 'open-inbox') {
                openInboxModal();
            } else if (act === 'close-inbox') {
                document.getElementById('inbox-modal').classList.add('hidden');
            } else if (act === 'accept-challenge') {
                p2pActiveDuelId = btn.getAttribute('data-id');
                document.getElementById('inbox-modal').classList.add('hidden');
                openP2PModal();
                document.getElementById('p2p-init-view').classList.add('hidden');
                document.getElementById('p2p-quiz-view').classList.remove('hidden');
                p2pCurrentQIdx = 0;
                p2pAnswers = {};
                loadNextP2PQuestion();
            } else if (act === 'open-profile-step') {
                openProfileStepModal();
            } else if (act === 'close-profile-step') {
                document.getElementById('profile-step-modal').classList.add('hidden');
            } else if (act === 'submit-step-1') {
                submitStep1();
            } else if (act === 'submit-step-2') {
                submitStep2();
            } else if (act === 'submit-step-3') {
                submitStep3();
            } else if (act === 'open-leaderboard') {
                openLeaderboard();
            } else if (act === 'close-leaderboard') {
                document.getElementById('leaderboard-modal').classList.add('hidden');
            } else if (act === 'open-drawer') {
                openDrawer(btn.getAttribute('data-drawer'));
            } else if (act === 'close-info-modal') {
                document.getElementById('info-modal').classList.add('hidden');
            } else if (act === 'prompt-login') {
                var name = prompt('Giriş yapılacak kullanıcı adını girin (Örn: LeisanB, Ahmet_Analist):');
                if (name) {
                    fetch('/api/auth/login-mock', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ username: name })
                    }).then(function(r){ return r.json(); }).then(function(res){
                        if (res.success) location.reload();
                    });
                }
            } else if (act === 'cat-filter') {
                document.querySelectorAll('.cat-btn').forEach(function(b){
                    b.className = 'cat-btn bg-slate-900 hover:bg-slate-800 text-slate-300 px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap flex items-center gap-1';
                });
                btn.className = 'cat-btn active bg-indigo-600 text-white px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap flex items-center gap-1';
                activeCategory = btn.getAttribute('data-cat');
                activeSubCategory = 'ALL';

                var subBox = document.getElementById('sub-cat-container');
                var pillsHtml = '';

                if (activeCategory === 'SPOR') {
                    pillsHtml = '<span class="text-[10px] text-slate-500 font-bold uppercase mr-1">Dallar:</span>' +
                        '<button data-action="subcat-filter" data-sub="ALL" class="sub-btn active px-2.5 py-1 rounded-lg bg-slate-800 text-white text-[11px] font-bold">Tümü</button>' +
                        '<button data-action="subcat-filter" data-sub="Futbol" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">⚽ Futbol</button>' +
                        '<button data-action="subcat-filter" data-sub="Basketbol" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">🏀 Basketbol</button>' +
                        '<button data-action="subcat-filter" data-sub="Formula 1" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">🏎 Formula 1</button>';
                } else if (activeCategory === 'BORSA & FİNANS') {
                    pillsHtml = '<span class="text-[10px] text-slate-500 font-bold uppercase mr-1">Piyasalar:</span>' +
                        '<button data-action="subcat-filter" data-sub="ALL" class="sub-btn active px-2.5 py-1 rounded-lg bg-slate-800 text-white text-[11px] font-bold">Tümü</button>' +
                        '<button data-action="subcat-filter" data-sub="BIST 100" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">BIST 100</button>' +
                        '<button data-action="subcat-filter" data-sub="Halka Arz" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">Halka Arz</button>' +
                        '<button data-action="subcat-filter" data-sub="Altın & Emtia" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">Altın & Emtia</button>';
                } else if (activeCategory === 'EKONOMİ') {
                    pillsHtml = '<span class="text-[10px] text-slate-500 font-bold uppercase mr-1">Odak:</span>' +
                        '<button data-action="subcat-filter" data-sub="ALL" class="sub-btn active px-2.5 py-1 rounded-lg bg-slate-800 text-white text-[11px] font-bold">Tümü</button>' +
                        '<button data-action="subcat-filter" data-sub="Asgari Ücret" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">Asgari Ücret</button>' +
                        '<button data-action="subcat-filter" data-sub="Faiz" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">TCMB Faiz</button>';
                } else if (activeCategory === 'TEKNOLOJİ') {
                    pillsHtml = '<span class="text-[10px] text-slate-500 font-bold uppercase mr-1">Alanlar:</span>' +
                        '<button data-action="subcat-filter" data-sub="ALL" class="sub-btn active px-2.5 py-1 rounded-lg bg-slate-800 text-white text-[11px] font-bold">Tümü</button>' +
                        '<button data-action="subcat-filter" data-sub="Yapay Zeka" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">Yapay Zeka</button>' +
                        '<button data-action="subcat-filter" data-sub="Uzay & Uydu" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">Uzay & Havacılık</button>';
                } else if (activeCategory === 'MODA & STİL') {
                    pillsHtml = '<span class="text-[10px] text-slate-500 font-bold uppercase mr-1">Stil:</span>' +
                        '<button data-action="subcat-filter" data-sub="ALL" class="sub-btn active px-2.5 py-1 rounded-lg bg-slate-800 text-white text-[11px] font-bold">Tümü</button>' +
                        '<button data-action="subcat-filter" data-sub="Trendler" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">Moda Haftası</button>' +
                        '<button data-action="subcat-filter" data-sub="Lüks Tüketim" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">Tekstil & Tasarım</button>';
                }

                if (pillsHtml) {
                    subBox.classList.remove('hidden');
                    subBox.innerHTML = pillsHtml;
                } else {
                    subBox.classList.add('hidden');
                }
                renderMarkets();
            } else if (act === 'subcat-filter') {
                activeSubCategory = btn.getAttribute('data-sub');
                document.querySelectorAll('.sub-btn').forEach(function(b){
                    b.className = 'sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold';
                });
                btn.className = 'sub-btn active px-2.5 py-1 rounded-lg bg-slate-800 text-white text-[11px] font-bold';
                renderMarkets();
            } else if (act === 'quick-vote-open') {
                toggleQuickVoteBox(btn.getAttribute('data-id'), btn.getAttribute('data-outcome'));
            } else if (act === 'quick-amount-select') {
                setQuickAmount(btn.getAttribute('data-id'), btn.getAttribute('data-amount'));
            } else if (act === 'quick-submit') {
                submitQuickVote(btn.getAttribute('data-id'));
            } else if (act === 'open-detail') {
                openMarketDetail(btn.getAttribute('data-slug'));
            } else if (act === 'duel-vote') {
                voteDuel(btn.getAttribute('data-id'), btn.getAttribute('data-choice'));
            } else if (act === 'dt-switch-tab') {
                switchDetailTab(btn.getAttribute('data-tab'));
            } else if (act === 'dt-set-choice') {
                activeDetailChoice = btn.getAttribute('data-choice');
                updateDetailChoiceBtns();
            } else if (act === 'dt-predict-submit') {
                submitPredict();
            } else if (act === 'submit-comment') {
                submitComment();
            } else if (act === 'download-cohort-csv') {
                window.location.href = '/api/markets/' + activeDetailMarket.slug + '/export';
            } else if (act === 'sell-position') {
                sellPosition(btn.getAttribute('data-id'), btn.getAttribute('data-outcome'), btn.getAttribute('data-shares'));
            } else if (act === 'vote-zarla') {
                voteZarla(btn.getAttribute('data-id'), btn.getAttribute('data-choice'));
            } else if (act === 'load-portfolio') {
                loadPortfolio();
            } else if (act === 'submit-b2b-form') {
                submitContactForm('B2B_SALES');
            } else if (act === 'submit-contact-form') {
                submitContactForm('GENERAL');
            }
        });

        // CANLI TICKER
        var TICKER_MESSAGES = [
            "Ahmet_Analist Asgari Ücret pazarına 500 KOR ile EVET bildirdi.",
            "Ece_Hoca Formula 1 Ferrari Şampiyonluğuna 1.000 KOR yatırdı.",
            "Liyakat Ligi Zirvesi: Ahmet_Analist Ordinaryüs kademesinde lider!",
            "TCMB Faiz kararında EVET olasılığı %45 seviyesinde dengelendi."
        ];
        var tickerIdx = 0;
        setInterval(function() {
            tickerIdx = (tickerIdx + 1) % TICKER_MESSAGES.length;
            var el = document.getElementById('live-ticker-text');
            if (el) el.textContent = TICKER_MESSAGES[tickerIdx];
        }, 5000);

        // WEBSOCKET
        function connectWebSocket() {
            var protocol = (window.location.protocol === 'https:') ? 'wss:' : 'ws:';
            var ws = new WebSocket(protocol + '//' + window.location.host + '/ws');
            ws.onmessage = function(event) {
                try {
                    var msg = JSON.parse(event.data);
                    if (msg.type === 'MARKET_UPDATE') {
                        var idx = markets.findIndex(function(m){ return m.id === msg.marketId; });
                        if (idx !== -1) {
                            markets[idx].probYes = msg.probYes;
                            markets[idx].probNo = msg.probNo;
                            markets[idx].poolTotal = msg.poolTotal;
                            renderMarkets();
                        }
                    }
                    if (msg.type === 'DUEL_VOTE') loadHeroDuels();
                } catch(e){}
            };
            ws.onclose = function() { setTimeout(connectWebSocket, 2500); };
        }

        // BAŞLATICI
        async function init() {
            try {
                var me = await fetch('/api/me').then(function(r){ return r.json(); });
                currentBalance = Math.round(parseFloat(me.balance_kor || 0));
                document.getElementById('user-balance').textContent = currentBalance.toLocaleString('tr-TR') + ' KOR';
                document.getElementById('user-streak').textContent = (me.streak || 0) + 'g';
                document.getElementById('user-tier-badge').textContent = me.tier || 'Doçent';
                document.getElementById('dd-username').textContent = me.username || 'Misafir';
                document.getElementById('dd-tier').textContent = 'Kademesi: ' + (me.tier || 'Gözlemci');
                document.getElementById('dd-archetype').textContent = me.personality_archetype || '-';

                await loadHeroDuels();
                var res = await fetch('/api/markets').then(function(r){ return r.json(); });
                markets = res.markets || [];
                renderMarkets();

                if (me.role !== 'GUEST') {
                    checkPendingInbox();
                }

                var path = window.location.pathname;
                if (path.startsWith('/market/')) {
                    var slug = path.split('/')[2];
                    openMarketDetail(slug);
                }
            } catch(e) {
                console.error('[OYVER INIT HATASI]', e);
            }
        }

        window.onload = function() {
            init();
            connectWebSocket();
        };
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
    console.log(`[OYVER PRO] v1.4-RECOVERY-PRODUCTION Aktif: ${address}`);
});
