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
        console.info('[DATABASE] v1.4-PRE-RELEASE Şema ve 16 Pazar doğrulanıyor...');
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

        await client.query(`
            ALTER TABLE markets ADD COLUMN IF NOT EXISTS sub_category VARCHAR(64);
            ALTER TABLE users ADD COLUMN IF NOT EXISTS tier VARCHAR(32) NOT NULL DEFAULT 'Araştırmacı';
        `);

        for (const code of ['1000', '3000', '4000', '5000']) {
            await client.query(`INSERT INTO accounts (code) VALUES ($1) ON CONFLICT DO NOTHING`, [code]);
        }

        // Demo Kullanıcıları
        const checkUsers = await client.query(`SELECT count(*) FROM users`);
        if (parseInt(checkUsers.rows[0].count, 10) === 0) {
            await client.query(`
                INSERT INTO users (id, email, username, password_hash, role, balance_kor, streak, tier) VALUES
                ('11111111-1111-1111-1111-111111111111', 'demo@oyver.pro', 'LeisanB', 'OAUTH_MOCK', 'USER', 14500, 5, 'Doçent'),
                ('22222222-2222-2222-2222-222222222222', 'ahmet@oyver.pro', 'Ahmet_Analist', 'OAUTH_MOCK', 'USER', 420000, 14, 'Ordinaryüs'),
                ('33333333-3333-3333-3333-333333333333', 'ece@oyver.pro', 'Ece_Hoca', 'OAUTH_MOCK', 'USER', 315000, 9, 'Profesör')
                ON CONFLICT (email) DO NOTHING;
            `);
        }

        // Hero Düellolar
        const checkDuels = await client.query(`SELECT count(*) FROM hero_duels`);
        if (parseInt(checkDuels.rows[0].count, 10) === 0) {
            await client.query(`
                INSERT INTO hero_duels (title, category, option_a_name, option_b_name, votes_a, votes_b, is_active) VALUES
                ('2028 Seçim Tercihiniz Hangisi Olur?', 'SİYASET DÜELLOSU', 'Cumhur İttifakı', 'Muhalefet Bloğu', 54, 46, true),
                ('Hafta Sonu Süper Lig Derbisini Kim Kazanır?', 'SPOR DÜELLOSU', 'Galatasaray', 'Fenerbahçe', 52, 48, true)
            `);
        }

        // 16 ZENGİN PAZAR TOHUMLAMASI (Spor, Borsa, Moda, Teknoloji, Ekonomi, Kültür)
        const checkMarkets = await client.query(`SELECT count(*) FROM markets`);
        if (parseInt(checkMarkets.rows[0].count, 10) === 0) {
            const rich16Markets = [
                // BORSA & FİNANS
                { slug: 'bist-100-2026', cat: 'BORSA & FİNANS', sub: 'BIST 100', q: 'BIST 100 Endeksi 2026 Son Çeyreğini 12.000 Puan Üzerinde Kapatır mı?', srcName: 'Borsa İstanbul Bülteni', closing: '31 Aralık 2026', yesR: 10000, noR: 10000 },
                { slug: 'bist-halka-arz-50', cat: 'BORSA & FİNANS', sub: 'Halka Arz', q: '2026 Yılında Borsa İstanbul\'da Halka Arz Edilen Şirket Sayısı 50\'yi Aşar mı?', srcName: 'SPK Bültenleri', closing: '31 Aralık 2026', yesR: 12000, noR: 8000 },
                { slug: 'ons-altin-3000', cat: 'BORSA & FİNANS', sub: 'Emtia & Değerli Maden', q: 'Ons Altın Fiyatı 2026 Sonuna Kadar 3.000 Dolar Seviyesini Görür mü?', srcName: 'Londra Külçe Piyasası (LBMA)', closing: '31 Aralık 2026', yesR: 9000, noR: 11000 },

                // SPOR
                { slug: 'super-lig-90-puan', cat: 'SPOR', sub: 'Futbol', q: '2026-2027 Süper Lig Şampiyonu 90 Puan Barajını Aşar mı?', srcName: 'TFF Resmi Puan Durumu', closing: '30 Mayıs 2027', yesR: 8000, noR: 12000 },
                { slug: 'derbi-kirmizi-kart', cat: 'SPOR', sub: 'Futbol', q: 'İlk Galatasaray - Fenerbahçe Derbisinde Kırmızı Kart Çıkar mı?', srcName: 'TFF Maç Raporu', closing: '22 Kasım 2026', yesR: 11000, noR: 9000 },
                { slug: 'efes-final-four', cat: 'SPOR', sub: 'Basketbol', q: 'Anadolu Efes 2026-2027 EuroLeague Sezonunda Final Four\'a Kalır mı?', srcName: 'EuroLeague Basketball', closing: '15 Mayıs 2027', yesR: 12000, noR: 8000 },
                { slug: 'f1-ferrari-sampiyon', cat: 'SPOR', sub: 'Formula 1', q: '2026 Formula 1 Takımlar Şampiyonluğunu Ferrari Kazanır mı?', srcName: 'FIA Resmi Sonuçları', closing: '28 Kasım 2026', yesR: 13000, noR: 7000 },

                // EKONOMİ & YAŞAM
                { slug: 'asgari-ucret-2027', cat: 'EKONOMİ', sub: 'Maaş & Gelir', q: '2027 Yılı Net Asgari Ücreti 35.000 TL Üzerinde Açıklanır mı?', srcName: 'Çalışma Bakanlığı / Resmi Gazete', closing: '31 Aralık 2026', yesR: 8000, noR: 12000 },
                { slug: 'tcmb-faiz-2026', cat: 'EKONOMİ', sub: 'Merkez Bankası', q: 'TCMB Politika Faizini 2026 Yıl Sonuna Kadar %30 veya Altına İndirir mi?', srcName: 'TCMB PPK Karar Metni', closing: '24 Aralık 2026', yesR: 11000, noR: 9000 },
                { slug: 'enflasyon-tek-hane', cat: 'EKONOMİ', sub: 'Enflasyon', q: 'TÜİK Yıllık Tüketici Enflasyonu 2026 Yılında %20 Altına İner mi?', srcName: 'TÜİK TÜFE Bülteni', closing: '3 Ocak 2027', yesR: 14000, noR: 6000 },

                // TEKNOLOJİ
                { slug: 'turksat-6a-ticari', cat: 'TEKNOLOJİ', sub: 'Uzay & Uydu', q: 'TÜRKSAT 6A Uydusu 2026 Yılında Tam Kapasite Ticari Hizmete Başlar mı?', srcName: 'Ulaştırma Bakanlığı', closing: '15 Kasım 2026', yesR: 6000, noR: 14000 },
                { slug: 'yapay-zeka-kanunu', cat: 'TEKNOLOJİ', sub: 'Yapay Zeka', q: 'TBMM 2026 Yılında Kapsamlı Ulusal Yapay Zeka Yasasını Kabul Eder mi?', srcName: 'Resmi Gazete / TBMM Tutanakları', closing: '31 Aralık 2026', yesR: 10000, noR: 10000 },
                { slug: 'yerli-unicorn-2026', cat: 'TEKNOLOJİ', sub: 'Girişimcilik', q: '2026 Yılında Türkiye\'den Yeni Bir Unicorn (1 Milyar $ Değerleme) Girişim Çıkar mı?', srcName: 'Sanayi ve Teknoloji Bakanlığı', closing: '31 Aralık 2026', yesR: 9000, noR: 11000 },

                // MODA & STİL
                { slug: 'ist-moda-haftasi', cat: 'MODA & STİL', sub: 'Trendler', q: 'İstanbul Moda Haftası 2027 Resmi Takviminde 20+ Yabancı Tasarımcı Yer Alır mı?', srcName: 'İHKİB Basın Bülteni', closing: '20 Mart 2027', yesR: 10000, noR: 10000 },
                { slug: 'surdurulebilir-tekstil', cat: 'MODA & STİL', sub: 'Lüks & Tasarım', q: 'Türkiye\'nin 2026 Tekstil İhracatında Geri Dönüştürülmüş Ürün Payı %15\'i Aşar mı?', srcName: 'TİM İhracat Raporu', closing: '15 Ocak 2027', yesR: 12000, noR: 8000 },

                // KÜLTÜR & MEDYA
                { slug: 'turizm-ziyaretci-2026', cat: 'KÜLTÜR & MEDYA', sub: 'Turizm & Yaşam', q: '2026 Yılında Türkiye\'ye Gelen Yabancı Ziyaretçi Sayısı 60 Milyonu Aşar mı?', srcName: 'TÜİK Turizm İstatistikleri', closing: '31 Ocak 2027', yesR: 7000, noR: 13000 }
            ];

            for (const item of rich16Markets) {
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
        console.info('[DATABASE] v1.4-PRE-RELEASE Başarıyla Hazırlandı.');
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

app.get('/health', async () => ({ status: 'UP', version: 'v1.4-PRE-RELEASE', timestamp: new Date().toISOString() }));

app.get('/api/me', async (req) => {
    const r = await pool.query(`SELECT id, username, email, role, balance_kor, streak, tier FROM users WHERE id = $1`, [req.userId]);
    return r.rows[0] || {};
});

// PAZARLAR & DETAY
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

// TAHMİN ALIM (BUY)
app.post('/api/trade/predict', async (req, rep) => {
    const { marketId, outcome, amountKor } = req.body || {};
    const amt = new Decimal(amountKor || 0);
    if (!marketId || !outcome || amt.lte(0)) return rep.status(400).send({ error: 'Geçersiz parametre.' });

    try {
        const result = await runInTransaction(async (c) => {
            const mCheck = await c.query(`SELECT status FROM markets WHERE id = $1`, [marketId]);
            if (mCheck.rows.length === 0 || mCheck.rows[0].status !== 'TRADING') throw new Error('Bu oylama işleme kapalıdır.');

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

// ERKEN SATIŞ (SELL)
app.post('/api/trade/sell', async (req, rep) => {
    const { marketId, outcome, sharesToSell } = req.body || {};
    const sIn = new Decimal(sharesToSell || 0);
    if (!marketId || !outcome || sIn.lte(0)) return rep.status(400).send({ error: 'Geçersiz pay.' });

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

// AKADEMİK LİYAKAT LİGİ
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

        let academicTier = 'Araştırmacı';
        if (frsScore > 10000 && uPositions.length >= 20) academicTier = 'Ordinaryüs';
        else if (frsScore > 6000 && uPositions.length >= 10) academicTier = 'Profesör';
        else if (frsScore > 3000) academicTier = 'Doçent';
        else if (frsScore > 1000) academicTier = 'Uzman Analist';

        return { id: u.id, name: u.username, tier: academicTier, streak: u.streak, frsScore, winRate, totalSettled: uPositions.length };
    });
    leaderboard.sort((a, b) => b.frsScore - a.frsScore);
    return { top100: leaderboard.slice(0, 100).map((item, idx) => ({ rank: idx + 1, ...item })) };
});

// HERO DÜELLOLAR & OYLA
app.get('/api/duels', async () => {
    const r = await pool.query(`SELECT * FROM hero_duels WHERE is_active = true ORDER BY created_at ASC`);
    return {
        duels: r.rows.map(d => ({
            ...d,
            pctA: Math.round((d.votes_a / (d.votes_a + d.votes_b)) * 100),
            pctB: 100 - Math.round((d.votes_a / (d.votes_a + d.votes_b)) * 100),
            totalVotes: (d.votes_a + d.votes_b).toLocaleString('tr-TR')
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
            ...p,
            pctA: Math.round((p.votes_a / (p.votes_a + p.votes_b || 1)) * 100),
            pctB: 100 - Math.round((p.votes_a / (p.votes_a + p.votes_b || 1)) * 100),
            totalVotes: (p.votes_a + p.votes_b).toLocaleString('tr-TR')
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
    return { success: true, message: 'Talebiniz kaydedildi. Ekibimiz en kısa sürede iletişime geçecektir.' };
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
// 4. FRONTEND ARAYÜZÜ (OBSIDIAN & BLOOMBERG TEMALI)
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
        
        /* 1. TEMA: OBSIDIAN TITANIUM (VARSAYILAN) */
        .theme-obsidian body { background-color: #080c14; color: #f8fafc; }
        .theme-obsidian .card-bg { background-color: #0d131f; border-color: #1e293b; }
        .theme-obsidian .accent-text { color: #818cf8; }
        .theme-obsidian .accent-bg { background-color: #6366f1; }
        .theme-obsidian .accent-border { border-color: #6366f1; }
        
        /* 2. TEMA: BLOOMBERG TERMINAL (FİNANS KEHRİBARI) */
        .theme-bloomberg body { background-color: #0c0d0e; color: #fef3c7; }
        .theme-bloomberg .card-bg { background-color: #14171a; border-color: #292d32; }
        .theme-bloomberg .accent-text { color: #f59e0b; }
        .theme-bloomberg .accent-bg { background-color: #d97706; }
        .theme-bloomberg .accent-border { border-color: #f59e0b; }

        .popover-bubble { animation: popIn 0.2s cubic-bezier(0.16, 1, 0.3, 1); }
        @keyframes popIn {
            0% { opacity: 0; transform: scale(0.95) translateY(4px); }
            100% { opacity: 1; transform: scale(1) translateY(0); }
        }
        ::-webkit-scrollbar { width: 4px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: #334155; border-radius: 2px; }
    </style>
</head>
<body class="min-h-screen flex flex-col antialiased selection:bg-indigo-600 selection:text-white">

    <div id="toast-container" class="fixed top-5 right-5 z-50 flex flex-col gap-2 pointer-events-none"></div>

    <!-- HEADER (PORTFÖY VE PROFİL YAN YANA) -->
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
                    <button onclick="openExplainerModal()" class="px-3 py-1.5 rounded-xl text-indigo-300 hover:text-white hover:bg-indigo-950/40 border border-indigo-500/20 transition flex items-center gap-1.5">
                        <i class="fas fa-sparkles text-amber-400"></i> 30s Rehber
                    </button>
                </nav>
            </div>

            <!-- Sağ Küme: Tema Butonu, Portföy ve Profil Rozeti -->
            <div class="flex items-center space-x-2.5">
                <button onclick="toggleTheme()" class="px-2.5 py-1.5 rounded-xl bg-slate-900 border border-slate-800 text-xs font-bold text-slate-300 hover:text-white transition flex items-center gap-1.5" title="Tema Değiştir">
                    <i class="fas fa-palette text-amber-400"></i> <span class="hidden md:inline" id="theme-label">Obsidian</span>
                </button>

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

                    <!-- Profil Menüsü -->
                    <div id="user-dropdown" class="hidden absolute right-0 top-14 w-60 bg-[#0d131f] border border-slate-800 rounded-2xl shadow-2xl p-2 z-50 space-y-1">
                        <div class="px-3 py-2 border-b border-slate-800/80 mb-1">
                            <div class="text-xs font-bold text-white" id="dd-username">LeisanB</div>
                            <div class="text-[10px] text-indigo-400 font-bold" id="dd-tier">Kademesi: Doçent</div>
                        </div>
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
            <!-- REKABETÇİ & NEŞELİ BANNER (30s REHBERİ) -->
            <section class="py-5 border-b border-slate-900 bg-[#0a0f1d]/70">
                <div class="container mx-auto px-4 max-w-5xl">
                    <div class="flex flex-col sm:flex-row items-center justify-between gap-4 p-4 rounded-2xl bg-[#0f172a]/60 border border-slate-800">
                        <div class="space-y-1 text-center sm:text-left">
                            <h2 class="text-sm font-black text-white flex items-center justify-center sm:justify-start gap-1.5">
                                <span>🎯 Geleceği Kokla, Ordinaryüs Kademesine Tırman!</span>
                            </h2>
                            <p class="text-xs text-slate-400">Cebinden 1 TL çıkmaz. 14.500 KOR puanınla tahmin yap, Brier skorunla itibar kazan.</p>
                        </div>
                        <button onclick="openExplainerModal()" class="px-4 py-2 bg-indigo-600/20 hover:bg-indigo-600 border border-indigo-500/30 text-indigo-300 hover:text-white rounded-xl text-xs font-bold transition whitespace-nowrap">
                            Nasıl Çalışır? (3 Adım)
                        </button>
                    </div>
                </div>
            </section>

            <!-- HERO DÜELLOLAR (HERO ALANINDA DA HIZLI OY BUTONLARI) -->
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

            <!-- AÇILIR OKLU (▾) ANA KATEGORİLER VE HAP (PILL) ALT KATEGORİLER -->
            <section class="container mx-auto px-4 pt-6 pb-2 max-w-5xl">
                <!-- Ana Kategoriler (Açılır Ok Göstergeli) -->
                <div class="flex items-center space-x-2 overflow-x-auto pb-2">
                    <button class="cat-btn active bg-indigo-600 text-white px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap" data-cat="ALL">Tümü</button>
                    <button class="cat-btn bg-slate-900 hover:bg-slate-800 text-slate-300 px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap flex items-center gap-1" data-cat="BORSA & FİNANS">
                        <span>Borsa & Finans</span> <i class="fas fa-chevron-down text-[10px] text-slate-500"></i>
                    </button>
                    <button class="cat-btn bg-slate-900 hover:bg-slate-800 text-slate-300 px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap flex items-center gap-1" data-cat="SPOR">
                        <span>Spor</span> <i class="fas fa-chevron-down text-[10px] text-slate-500"></i>
                    </button>
                    <button class="cat-btn bg-slate-900 hover:bg-slate-800 text-slate-300 px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap flex items-center gap-1" data-cat="EKONOMİ">
                        <span>Ekonomi</span> <i class="fas fa-chevron-down text-[10px] text-slate-500"></i>
                    </button>
                    <button class="cat-btn bg-slate-900 hover:bg-slate-800 text-slate-300 px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap flex items-center gap-1" data-cat="TEKNOLOJİ">
                        <span>Teknoloji</span> <i class="fas fa-chevron-down text-[10px] text-slate-500"></i>
                    </button>
                    <button class="cat-btn bg-slate-900 hover:bg-slate-800 text-slate-300 px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap flex items-center gap-1" data-cat="MODA & STİL">
                        <span>Moda & Stil</span> <i class="fas fa-chevron-down text-[10px] text-slate-500"></i>
                    </button>
                </div>
                <!-- Alt Kategori Hapları (Dinamik Açılan Pills) -->
                <div id="sub-cat-container" class="hidden flex items-center space-x-1.5 overflow-x-auto pt-2 pb-1 border-t border-slate-900 mt-2"></div>
            </section>

            <!-- 16 ZENGİN PAZAR KARTI (KART ÜZERİNDE MİKRO-BALONLU HIZLI OY) -->
            <section class="container mx-auto px-4 py-4 max-w-5xl">
                <div id="market-grid" class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-5"></div>
            </section>
        </main>

        <!-- 2. BÖLÜM: PAZAR DETAY TERMİNALİ (/market/:slug) -->
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
                    <div class="card-bg rounded-2xl p-5 shadow-xl border">
                        <div class="flex items-center justify-between mb-4">
                            <div>
                                <div class="text-xs font-bold text-slate-400 uppercase">EVET Olasılık Eğrisi</div>
                                <div class="text-3xl font-black text-emerald-400 mt-0.5" id="dt-current-prob">--%</div>
                            </div>
                        </div>
                        <div class="h-60 w-full relative"><canvas id="marketChart"></canvas></div>
                    </div>
                </div>

                <div class="card-bg rounded-2xl p-5 shadow-xl space-y-4 h-fit border">
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

        <!-- 3. BÖLÜM: OYLA (ZARLA) -->
        <main id="section-zarla" class="hidden container mx-auto px-4 py-8 max-w-2xl">
            <div class="text-center mb-6">
                <span class="px-3 py-1 rounded-full bg-indigo-500/10 border border-indigo-500/20 text-xs font-bold text-indigo-400">0 Puan, 0 Risk, Saf Kamuoyu Nabzı</span>
                <h2 class="text-2xl font-black text-white mt-2">Günün Kritik Meseleleri</h2>
            </div>
            <div id="zarla-list" class="space-y-4"></div>
        </main>

        <!-- 4. BÖLÜM: PORTFÖYÜM -->
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

    <!-- 30 SANİYEDE OYVER REHBERİ (NEŞELİ & AÇIKLAYICI ONBOARDING) -->
    <div id="explainer-modal" class="fixed inset-0 bg-slate-950/85 z-50 backdrop-blur-md hidden flex items-center justify-center p-4">
        <div class="card-bg border w-full max-w-lg rounded-3xl p-6 shadow-2xl space-y-4">
            <div class="flex justify-between items-center pb-2 border-b border-slate-800">
                <h3 class="text-sm font-black text-white flex items-center gap-2">
                    <i class="fas fa-graduation-cap text-indigo-400"></i> 30 Saniyede OYVER PRO Rehberi
                </h3>
                <button onclick="closeExplainerModal()" class="text-slate-400 hover:text-white"><i class="fas fa-times"></i></button>
            </div>
            <div class="space-y-3 text-xs leading-relaxed text-slate-300">
                <div class="p-3 bg-slate-950 rounded-2xl border border-slate-800 space-y-1">
                    <span class="font-black text-amber-400">💰 1. KOR Nedir? Cebimden Para Çıkar mı?</span>
                    <p class="text-slate-400">Kesinlikle hayır! 14.500 KOR liyakat puanı platform tarafından ücretsiz verilir. Amaç kumar değil; gerçek bilgi ve öngörü yeteneğini ölçmektir.</p>
                </div>
                <div class="p-3 bg-slate-950 rounded-2xl border border-slate-800 space-y-1">
                    <span class="font-black text-pink-400">🗳️ 2. OYLA (Günün Nabzı) Nedir?</span>
                    <p class="text-slate-400">Puan riski olmadan, gündemin en sıcak 10 konusuna tek tıkla oy verip halkın anlık yüzdesini gördüğün kamuoyu alanıdır.</p>
                </div>
                <div class="p-3 bg-slate-950 rounded-2xl border border-slate-800 space-y-1">
                    <span class="font-black text-indigo-400">🎓 3. Ordinaryüs / Akademik Kademe Nasıl Kazanılır?</span>
                    <p class="text-slate-400">Tahminlerin resmi bültenlerle (TCMB, TÜİK, Resmi Gazete) doğrulandıkça Brier kalibrasyon puanın yükselir. Araştırmacı olarak başlar, sırasıyla Doçent, Profesör ve zirvede Ordinaryüs olursun!</p>
                </div>
            </div>
            <button onclick="closeExplainerModal()" class="w-full py-2.5 bg-indigo-600 hover:bg-indigo-500 rounded-xl font-bold text-white text-xs transition">
                Anladım, Öngörüye Başla!
            </button>
        </div>
    </div>

    <!-- KURUMSAL ÇEKMECE (%93.2 GÜVEN ENDEKSİ DAHİL) -->
    <div id="info-modal" class="fixed inset-0 bg-slate-950/85 z-50 backdrop-blur-md hidden flex items-center justify-center p-4">
        <div class="card-bg border w-full max-w-lg rounded-3xl p-6 max-h-[85vh] overflow-y-auto space-y-4">
            <div class="flex justify-between items-center pb-3 border-b border-slate-800">
                <h3 class="text-base font-black text-white" id="info-modal-title">Başlık</h3>
                <button onclick="closeInfoModal()" class="text-slate-400 hover:text-white"><i class="fas fa-times"></i></button>
            </div>
            <div id="info-modal-content" class="text-xs text-slate-300 leading-relaxed space-y-3"></div>
        </div>
    </div>

    <!-- LİYAKAT LİGİ MODALI -->
    <div id="leaderboard-modal" class="fixed inset-0 bg-slate-950/85 z-50 backdrop-blur-md hidden flex items-center justify-center p-4">
        <div class="card-bg border w-full max-w-xl rounded-3xl p-5 max-h-[85vh] flex flex-col">
            <div class="flex justify-between items-center pb-3 border-b border-slate-800 mb-3">
                <div class="flex items-center gap-2"><i class="fas fa-graduation-cap text-amber-400"></i><h3 class="text-base font-black text-white">Akademik Liyakat Ligi</h3></div>
                <button onclick="closeLeaderboard()" class="text-slate-400"><i class="fas fa-times"></i></button>
            </div>
            <div class="overflow-y-auto flex-grow divide-y divide-slate-800/80" id="leaderboard-list"></div>
        </div>
    </div>

    <!-- MASAÜSTÜ FOOTER -->
    <footer class="hidden sm:block bg-[#05080f] border-t border-slate-900 py-10 mt-auto text-xs text-slate-400">
        <div class="container mx-auto px-4 max-w-5xl">
            <div class="grid grid-cols-1 md:grid-cols-4 gap-8 mb-8">
                <div class="space-y-2 md:col-span-2">
                    <a href="/" class="text-xl font-black tracking-tight text-white flex items-center gap-1.5">
                        OYVER<span class="text-[10px] px-1.5 py-0.5 rounded bg-indigo-500/20 text-indigo-400 border border-indigo-500/30">PRO</span>
                    </a>
                    <p class="text-slate-400 text-xs leading-relaxed max-w-md">
                        Türkiye'nin liyakat tabanlı ilk kolektif öngörü ve kamuoyu araştırma terminali.
                    </p>
                </div>
                <div>
                    <h4 class="text-white font-bold mb-3 uppercase tracking-wider text-[11px]">Platform</h4>
                    <ul class="space-y-2">
                        <li><button onclick="openDrawer('about')" class="hover:text-indigo-400 transition">Biz Kimiz?</button></li>
                        <li><button onclick="openLeaderboard()" class="hover:text-indigo-400 transition">Akademik Sıralama</button></li>
                        <li><button onclick="switchTab('zarla')" class="hover:text-indigo-400 transition">OYLA Nabız</button></li>
                    </ul>
                </div>
                <div>
                    <h4 class="text-white font-bold mb-3 uppercase tracking-wider text-[11px]">Kurumsal</h4>
                    <ul class="space-y-2">
                        <li><button onclick="openDrawer('b2b')" class="hover:text-indigo-400 transition text-emerald-400">B2B Veri & Güven Endeksi</button></li>
                        <li><button onclick="openDrawer('contact')" class="hover:text-indigo-400 transition">İletişim & Destek</button></li>
                        <li><button onclick="openDrawer('rules')" class="hover:text-indigo-400 transition">Liyakat Kuralları</button></li>
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
        <button onclick="navigateToHome(event)" class="flex flex-col items-center gap-1 text-slate-400 hover:text-indigo-400 transition">
            <i class="fas fa-compass text-base"></i><span class="text-[10px] font-bold">Pazarlar</span>
        </button>
        <button onclick="switchTab('zarla')" class="flex flex-col items-center gap-1 text-slate-400 hover:text-indigo-400 transition">
            <i class="fas fa-poll text-base"></i><span class="text-[10px] font-bold">OYLA</span>
        </button>
        <button onclick="switchTab('portfolio')" class="flex flex-col items-center gap-1 text-slate-400 hover:text-emerald-400 transition">
            <i class="fas fa-briefcase text-base"></i><span class="text-[10px] font-bold">Portföy</span>
        </button>
        <button onclick="openLeaderboard()" class="flex flex-col items-center gap-1 text-slate-400 hover:text-amber-400 transition">
            <i class="fas fa-graduation-cap text-base"></i><span class="text-[10px] font-bold">Liyakat</span>
        </button>
    </nav>

    <script>
        let markets = [];
        let heroDuels = [];
        let currentBalance = 14500;
        let activeCategory = 'ALL';
        let activeSubCategory = 'ALL';
        let activePopoverCardId = null;
        let activePopoverOutcome = 'YES';
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

        // TEMA YÖNETİCİSİ (OBSIDIAN vs BLOOMBERG)
        function toggleTheme() {
            const html = document.documentElement;
            const label = document.getElementById('theme-label');
            if (html.classList.contains('theme-obsidian')) {
                html.classList.remove('theme-obsidian');
                html.classList.add('theme-bloomberg');
                if (label) label.textContent = 'Bloomberg';
                localStorage.setItem('oyver_theme', 'bloomberg');
            } else {
                html.classList.remove('theme-bloomberg');
                html.classList.add('theme-obsidian');
                if (label) label.textContent = 'Obsidian';
                localStorage.setItem('oyver_theme', 'obsidian');
            }
        }

        // Kayıtlı tema kontrolü
        if (localStorage.getItem('oyver_theme') === 'bloomberg') {
            document.documentElement.classList.remove('theme-obsidian');
            document.documentElement.classList.add('theme-bloomberg');
        }

        function toggleUserDropdown() {
            document.getElementById('user-dropdown').classList.toggle('hidden');
        }

        window.onclick = function(e) {
            if (!e.target.closest('#user-badge') && !e.target.closest('#user-dropdown')) {
                document.getElementById('user-dropdown')?.classList.add('hidden');
            }
            if (!e.target.closest('.popover-bubble') && !e.target.closest('.btn-quick-vote')) {
                closeAllCardPopovers();
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

        // KART ÜZERİNDE MİKRO AÇILIR BALON (INLINE POPOVER)
        window.toggleCardPopover = function(marketId, outcome, e) {
            if (e) e.stopPropagation();
            closeAllCardPopovers();

            activePopoverCardId = marketId;
            activePopoverOutcome = outcome;
            const popEl = document.getElementById('popover-' + marketId);
            if (!popEl) return;

            const m = markets.find(x => x.id === marketId);
            if (!m) return;

            const badge = document.getElementById('pop-badge-' + marketId);
            badge.textContent = outcome === 'YES' ? 'EVET' : 'HAYIR';
            badge.className = outcome === 'YES' ? 'text-emerald-400 font-black' : 'text-rose-400 font-black';

            popEl.classList.remove('hidden');
            updatePopoverCalculation(marketId);
        };

        window.closeAllCardPopovers = function() {
            document.querySelectorAll('.popover-bubble').forEach(p => p.classList.add('hidden'));
            activePopoverCardId = null;
        };

        window.setPopoverAmount = function(marketId, val) {
            const input = document.getElementById('pop-input-' + marketId);
            if (input) {
                input.value = val;
                updatePopoverCalculation(marketId);
            }
        };

        window.updatePopoverCalculation = async function(marketId) {
            const m = markets.find(x => x.id === marketId);
            const input = document.getElementById('pop-input-' + marketId);
            const payoutSpan = document.getElementById('pop-payout-' + marketId);
            if (!m || !input || !payoutSpan) return;

            const amt = parseFloat(input.value) || 0;
            if (amt <= 0) return;

            try {
                const res = await fetch('/api/trade/quote', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ marketId: m.id, outcome: activePopoverOutcome, amountKor: amt, action: 'BUY' })
                }).then(r => r.json());

                if (res.sharesOut) {
                    payoutSpan.textContent = '+' + res.targetPayout;
                }
            } catch(e) {}
        };

        window.submitInlineVote = async function(marketId) {
            const m = markets.find(x => x.id === marketId);
            const input = document.getElementById('pop-input-' + marketId);
            if (!m || !input) return;

            const amt = input.value;
            try {
                const res = await fetch('/api/trade/predict', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ marketId: m.id, outcome: activePopoverOutcome, amountKor: amt })
                }).then(r => r.json());

                if (res.error) throw new Error(res.error);
                currentBalance = res.balanceKor;
                document.getElementById('user-balance').textContent = currentBalance.toLocaleString('tr-TR') + ' KOR';
                showToast('✅ Tercih işlendi! Alınan Pay: ' + res.sharesOut);
                closeAllCardPopovers();
            } catch(e) {
                showToast(e.message, 'error');
            }
        };

        // 16 ZENGİN PAZAR KARTI RENDERI
        function renderMarkets() {
            const container = document.getElementById('market-grid');
            container.innerHTML = '';

            let filtered = activeCategory === 'ALL' ? markets : markets.filter(m => m.category === activeCategory);
            if (activeSubCategory !== 'ALL') filtered = filtered.filter(m => m.sub_category === activeSubCategory);

            filtered.forEach(m => {
                const card = document.createElement('article');
                card.className = 'card-bg border rounded-2xl p-4 shadow-lg flex flex-col justify-between transition relative overflow-visible';

                card.innerHTML = 
                    '<div>' +
                        '<div class="flex items-center justify-between mb-2">' +
                            '<span class="px-2 py-0.5 rounded text-[9px] font-black uppercase tracking-wider bg-indigo-500/20 text-indigo-300 border border-indigo-500/30">' + escapeHtml(m.category) + (m.sub_category ? ' • ' + escapeHtml(m.sub_category) : '') + '</span>' +
                            '<span class="text-[10px] text-slate-400"><i class="far fa-clock mr-1"></i>' + escapeHtml(m.closing_date) + '</span>' +
                        '</div>' +
                        '<h3 class="text-xs sm:text-sm font-bold text-white mb-2 leading-snug cursor-pointer hover:text-indigo-400 transition" onclick="openMarketDetail(\\'' + m.slug + '\\')">' + escapeHtml(m.question) + '</h3>' +
                        '<div class="text-[10px] text-slate-500 mb-3 flex items-center gap-1"><i class="fas fa-landmark"></i><span>' + escapeHtml(m.source_name) + '</span></div>' +
                        '<div class="space-y-1 mb-4">' +
                            '<div class="flex justify-between text-[11px] font-semibold">' +
                                '<span class="text-slate-400">Havuz: ' + m.poolTotal + '</span><span class="text-emerald-400 font-bold">EVET: %' + m.probYes + '</span>' +
                            '</div>' +
                            '<div class="w-full bg-slate-950 rounded-full h-1.5 overflow-hidden flex border border-slate-800">' +
                                '<div class="bg-emerald-500 h-full transition-all duration-700" style="width:' + m.probYes + '%"></div>' +
                            '</div>' +
                        '</div>' +
                    '</div>' +
                    '<div class="space-y-2 pt-2 border-t border-slate-800/80 relative">' +
                        '<!-- KART ÜZERİNDE HIZLI BUTONLAR -->' +
                        '<div class="grid grid-cols-2 gap-2">' +
                            '<button onclick="toggleCardPopover(\\'' + m.id + '\\', \\'YES\\', event)" class="btn-quick-vote py-2 bg-emerald-950/40 hover:bg-emerald-600 border border-emerald-500/30 text-emerald-300 hover:text-white rounded-xl text-xs font-bold transition flex items-center justify-center gap-1">' +
                                'EVET (%' + m.probYes + ')' +
                            '</button>' +
                            '<button onclick="toggleCardPopover(\\'' + m.id + '\\', \\'NO\\', event)" class="btn-quick-vote py-2 bg-rose-950/40 hover:bg-rose-600 border border-rose-500/30 text-rose-300 hover:text-white rounded-xl text-xs font-bold transition flex items-center justify-center gap-1">' +
                                'HAYIR (%' + m.probNo + ')' +
                            '</button>' +
                        '</div>' +
                        '<button onclick="openMarketDetail(\\'' + m.slug + '\\')" class="w-full py-1.5 text-center text-[11px] font-bold text-slate-400 hover:text-white transition flex items-center justify-center gap-1">' +
                            '<span>Terminal & Analiz</span> <i class="fas fa-chevron-right text-[9px]"></i>' +
                        '</button>' +
                        
                        '<!-- MİKRO AÇILIR BALON (POPOVER) -->' +
                        '<div id="popover-' + m.id + '" class="popover-bubble hidden absolute bottom-12 left-0 right-0 bg-[#0f172a] border border-indigo-500/40 rounded-2xl p-3 shadow-2xl z-30 space-y-2.5 text-xs">' +
                            '<div class="flex justify-between items-center">' +
                                '<span class="text-[11px] font-bold text-slate-300">Yön: <span id="pop-badge-' + m.id + '">EVET</span></span>' +
                                '<button onclick="closeAllCardPopovers()" class="text-slate-400 hover:text-white"><i class="fas fa-times"></i></button>' +
                            '</div>' +
                            '<div class="grid grid-cols-4 gap-1.5">' +
                                '<button onclick="setPopoverAmount(\\'' + m.id + '\\', 100)" class="py-1 bg-slate-900 border border-slate-700 rounded-lg text-[10px] font-bold hover:border-indigo-400">100</button>' +
                                '<button onclick="setPopoverAmount(\\'' + m.id + '\\', 250)" class="py-1 bg-slate-900 border border-slate-700 rounded-lg text-[10px] font-bold hover:border-indigo-400">250</button>' +
                                '<button onclick="setPopoverAmount(\\'' + m.id + '\\', 500)" class="py-1 bg-slate-900 border border-slate-700 rounded-lg text-[10px] font-bold hover:border-indigo-400">500</button>' +
                                '<button onclick="setPopoverAmount(\\'' + m.id + '\\', 1000)" class="py-1 bg-slate-900 border border-slate-700 rounded-lg text-[10px] font-bold hover:border-indigo-400">1.000</button>' +
                            '</div>' +
                            '<input type="number" id="pop-input-' + m.id + '" value="250" oninput="updatePopoverCalculation(\\'' + m.id + '\\')" class="w-full bg-slate-950 border border-slate-700 rounded-xl p-2 text-white font-bold text-xs">' +
                            '<div class="flex justify-between text-[11px] text-slate-400 font-semibold">' +
                                '<span>Hedef:</span><span id="pop-payout-' + m.id + '" class="text-emerald-400 font-black">+-- KOR</span>' +
                            '</div>' +
                            '<button onclick="submitInlineVote(\\'' + m.id + '\\')" class="w-full py-2 bg-indigo-600 hover:bg-indigo-500 rounded-xl font-bold text-white text-xs transition">' +
                                'Onayla & İşle' +
                            '</button>' +
                        '</div>' +
                    '</div>';

                container.appendChild(card);
            });
        }

        // HERO DÜELLOLAR
        async function loadHeroDuels() {
            const res = await fetch('/api/duels').then(r => r.json());
            heroDuels = res.duels || [];
            const container = document.getElementById('hero-duels-container');
            container.innerHTML = '';

            heroDuels.forEach(d => {
                const votedChoice = localStorage.getItem('voted_duel_' + d.id);
                const card = document.createElement('div');
                card.className = 'card-bg border rounded-2xl p-4 shadow-xl flex flex-col justify-between';

                let actionsHtml = '';
                if (votedChoice) {
                    actionsHtml = 
                        '<div class="space-y-1.5 mt-3">' +
                            '<div class="w-full bg-slate-950 rounded-full h-2.5 overflow-hidden flex border border-slate-800">' +
                                '<div class="bg-indigo-500 h-full rounded-full transition-all duration-700" style="width:' + d.pctA + '%"></div>' +
                                '<div class="bg-rose-500 h-full rounded-full transition-all duration-700" style="width:' + d.pctB + '%"></div>' +
                            '</div>' +
                            '<div class="flex justify-between items-center text-[11px] font-black">' +
                                '<span class="' + (votedChoice === 'A' ? 'text-indigo-400 font-bold' : 'text-slate-400') + '">' + escapeHtml(d.option_a_name) + ' %' + d.pctA + '</span>' +
                                '<span class="' + (votedChoice === 'B' ? 'text-rose-400 font-bold' : 'text-slate-400') + '">' + escapeHtml(d.option_b_name) + ' %' + d.pctB + '</span>' +
                            '</div>' +
                        '</div>';
                } else {
                    actionsHtml = 
                        '<div class="grid grid-cols-2 gap-2 mt-3">' +
                            '<button onclick="voteDuel(\\'' + d.id + '\\', \\'A\\')" class="py-2 px-2 bg-slate-900 hover:bg-indigo-600 border border-slate-800 hover:border-indigo-500 rounded-xl text-[11px] font-bold text-slate-200 hover:text-white transition">' +
                                escapeHtml(d.option_a_name) +
                            '</button>' +
                            '<button onclick="voteDuel(\\'' + d.id + '\\', \\'B\\')" class="py-2 px-2 bg-slate-900 hover:bg-rose-600 border border-slate-800 hover:border-rose-500 rounded-xl text-[11px] font-bold text-slate-200 hover:text-white transition">' +
                                escapeHtml(d.option_b_name) +
                            '</button>' +
                        '</div>';
                }

                card.innerHTML = 
                    '<div>' +
                        '<div class="flex justify-between items-center mb-2">' +
                            '<span class="px-2 py-0.5 rounded text-[9px] font-black uppercase tracking-wider bg-indigo-500/20 text-indigo-300 border border-indigo-500/30">' + escapeHtml(d.category) + '</span>' +
                            '<span class="text-[10px] text-slate-400 font-semibold">' + d.totalVotes + ' Oy</span>' +
                        '</div>' +
                        '<h3 class="text-xs sm:text-sm font-extrabold text-white leading-snug mb-2">' + escapeHtml(d.title) + '</h3>' +
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

        // KATEGORİ VE HAP (PILL) DALLANMASI
        document.querySelectorAll('.cat-btn').forEach(btn => {
            btn.onclick = (e) => {
                document.querySelectorAll('.cat-btn').forEach(b => {
                    b.className = 'cat-btn bg-slate-900 hover:bg-slate-800 text-slate-300 px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap flex items-center gap-1';
                });
                btn.className = 'cat-btn active bg-indigo-600 text-white px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap flex items-center gap-1';
                activeCategory = btn.getAttribute('data-cat');
                activeSubCategory = 'ALL';

                const subBox = document.getElementById('sub-cat-container');
                let pillsHtml = '';

                if (activeCategory === 'SPOR') {
                    pillsHtml = '<span class="text-[10px] text-slate-500 font-bold uppercase mr-1">Dallar:</span>' +
                        '<button onclick="filterSubCat(\\'ALL\\')" class="sub-btn active px-2.5 py-1 rounded-lg bg-slate-800 text-white text-[11px] font-bold">Tümü</button>' +
                        '<button onclick="filterSubCat(\\'Futbol\\')" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">⚽ Futbol</button>' +
                        '<button onclick="filterSubCat(\\'Basketbol\\')" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">🏀 Basketbol</button>' +
                        '<button onclick="filterSubCat(\\'Formula 1\\')" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">🏎️ Formula 1</button>';
                } else if (activeCategory === 'BORSA & FİNANS') {
                    pillsHtml = '<span class="text-[10px] text-slate-500 font-bold uppercase mr-1">Piyasalar:</span>' +
                        '<button onclick="filterSubCat(\\'ALL\\')" class="sub-btn active px-2.5 py-1 rounded-lg bg-slate-800 text-white text-[11px] font-bold">Tümü</button>' +
                        '<button onclick="filterSubCat(\\'BIST 100\\')" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">BIST 100</button>' +
                        '<button onclick="filterSubCat(\\'Halka Arz\\')" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">Halka Arz</button>' +
                        '<button onclick="filterSubCat(\\'Emtia & Değerli Maden\\')" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">Altın & Emtia</button>';
                } else if (activeCategory === 'EKONOMİ') {
                    pillsHtml = '<span class="text-[10px] text-slate-500 font-bold uppercase mr-1">Odak:</span>' +
                        '<button onclick="filterSubCat(\\'ALL\\')" class="sub-btn active px-2.5 py-1 rounded-lg bg-slate-800 text-white text-[11px] font-bold">Tümü</button>' +
                        '<button onclick="filterSubCat(\\'Maaş & Gelir\\')" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">Asgari Ücret & Maaş</button>' +
                        '<button onclick="filterSubCat(\\'Merkez Bankası\\')" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">TCMB Faiz</button>' +
                        '<button onclick="filterSubCat(\\'Enflasyon\\')" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">TÜİK Enflasyon</button>';
                } else if (activeCategory === 'TEKNOLOJİ') {
                    pillsHtml = '<span class="text-[10px] text-slate-500 font-bold uppercase mr-1">Alanlar:</span>' +
                        '<button onclick="filterSubCat(\\'ALL\\')" class="sub-btn active px-2.5 py-1 rounded-lg bg-slate-800 text-white text-[11px] font-bold">Tümü</button>' +
                        '<button onclick="filterSubCat(\\'Yapay Zeka\\')" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">Yapay Zeka</button>' +
                        '<button onclick="filterSubCat(\\'Uzay & Uydu\\')" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">Uzay & Havacılık</button>' +
                        '<button onclick="filterSubCat(\\'Girişimcilik\\')" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">Unicorn Girişimler</button>';
                } else if (activeCategory === 'MODA & STİL') {
                    pillsHtml = '<span class="text-[10px] text-slate-500 font-bold uppercase mr-1">Stil:</span>' +
                        '<button onclick="filterSubCat(\\'ALL\\')" class="sub-btn active px-2.5 py-1 rounded-lg bg-slate-800 text-white text-[11px] font-bold">Tümü</button>' +
                        '<button onclick="filterSubCat(\\'Trendler\\')" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">Moda Haftası</button>' +
                        '<button onclick="filterSubCat(\\'Lüks & Tasarım\\')" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">Tekstil & Tasarım</button>';
                }

                if (pillsHtml) {
                    subBox.classList.remove('hidden');
                    subBox.innerHTML = pillsHtml;
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

        // REHBER & KURUMSAL MODALLAR
        window.openExplainerModal = () => document.getElementById('explainer-modal').classList.remove('hidden');
        window.closeExplainerModal = () => document.getElementById('explainer-modal').classList.add('hidden');

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

        // LİDERLİK LİGİ
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
                    card.className = 'card-bg border rounded-2xl p-4 flex items-center justify-between gap-4 text-xs';
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
                card.className = 'card-bg border rounded-2xl p-4 shadow-lg space-y-3';
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

        window.promptSwitchUser = async () => {
            const name = prompt('Giriş yapılacak kullanıcı adını girin (Örn: LeisanB, Ahmet_Analist):');
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

                loadHeroDuels();
                const res = await fetch('/api/markets').then(r => r.json());
                markets = res.markets || [];
                renderMarkets();

                const path = window.location.pathname;
                if (path.startsWith('/market/')) {
                    const slug = path.split('/')[2];
                    openMarketDetail(slug);
                }
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
    console.log(`[OYVER PRO] v1.4-PRE-RELEASE Obsidian & Bloomberg Aktif: ${address}`);
});
1
