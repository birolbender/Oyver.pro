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
        console.info('[DATABASE] Şema, Efsaneler Meydanı ve Pazar Doğrulaması...');
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
                resolution_proof TEXT,
                resolved_at TIMESTAMPTZ,
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
                slug VARCHAR(64) UNIQUE,
                title TEXT NOT NULL,
                category VARCHAR(64) NOT NULL,
                option_a_name TEXT NOT NULL,
                option_b_name TEXT NOT NULL,
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

            -- FAZ 2: EFSANELER MEYDANI VERİTABANI TABLOLARI
            CREATE TABLE IF NOT EXISTS vs_polls (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                slug VARCHAR(64) UNIQUE NOT NULL,
                title TEXT NOT NULL,
                option_a TEXT NOT NULL,
                option_b TEXT NOT NULL,
                votes_a INT NOT NULL DEFAULT 0,
                votes_b INT NOT NULL DEFAULT 0,
                category VARCHAR(64) NOT NULL DEFAULT 'EFSANELER',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS vs_votes (
                poll_id UUID NOT NULL REFERENCES vs_polls(id) ON DELETE CASCADE,
                user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                choice VARCHAR(4) NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (poll_id, user_id)
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

            CREATE TABLE IF NOT EXISTS sessions (
                token CHAR(64) PRIMARY KEY,
                user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);

        // Sütun ve İndeks Güvenceleri
        await client.query(`
            ALTER TABLE markets ADD COLUMN IF NOT EXISTS sub_category VARCHAR(64);
            ALTER TABLE markets ADD COLUMN IF NOT EXISTS status VARCHAR(32) NOT NULL DEFAULT 'TRADING';
            ALTER TABLE markets ADD COLUMN IF NOT EXISTS resolved_outcome VARCHAR(8);
            ALTER TABLE markets ADD COLUMN IF NOT EXISTS resolution_proof TEXT;
            ALTER TABLE markets ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ;

            ALTER TABLE users ADD COLUMN IF NOT EXISTS tier VARCHAR(32) NOT NULL DEFAULT 'Araştırmacı';
            ALTER TABLE users ADD COLUMN IF NOT EXISTS kvkk_accepted BOOLEAN NOT NULL DEFAULT FALSE;
            ALTER TABLE users ADD COLUMN IF NOT EXISTS personality_archetype VARCHAR(64) DEFAULT 'Stratejist';
            ALTER TABLE users ADD COLUMN IF NOT EXISTS profile_step INT NOT NULL DEFAULT 1;

            ALTER TABLE hero_duels ADD COLUMN IF NOT EXISTS slug VARCHAR(64);
            CREATE UNIQUE INDEX IF NOT EXISTS uq_hero_duels_slug ON hero_duels (slug);
        `);

        // Mükerrer Hero Düelloları Temizleme
        await client.query(`
            DELETE FROM hero_duels a USING hero_duels b 
            WHERE a.ctid < b.ctid AND (a.slug = b.slug OR a.title = b.title);
        `);

        for (const code of ['1000', '2000', '2100', '3000', '4000', '5000']) {
            await client.query(`INSERT INTO accounts (code) VALUES ($1) ON CONFLICT DO NOTHING`, [code]);
        }

        // Demo Kullanıcıları
        await client.query(`
            INSERT INTO users (id, email, username, password_hash, role, balance_kor, streak, tier, personality_archetype) VALUES
            ('11111111-1111-1111-1111-111111111111', 'demo@oyver.pro', 'LeisanB', 'OAUTH_MOCK', 'USER', 14500, 5, 'Doçent', 'Stratejist'),
            ('22222222-2222-2222-2222-222222222222', 'ahmet@oyver.pro', 'Ahmet_Analist', 'OAUTH_MOCK', 'USER', 420000, 14, 'Ordinaryüs', 'Öncü'),
            ('33333333-3333-3333-3333-333333333333', 'ece@oyver.pro', 'Ece_Hoca', 'OAUTH_MOCK', 'USER', 315000, 9, 'Profesör', 'Sağlamcı')
            ON CONFLICT (email) DO NOTHING;
        `);

        // 2 Temel Hero Düello
        const initialDuels = [
            { slug: 'secim-2028-blok', title: '2028 Cumhurbaşkanlığı Seçim Tercihiniz Hangi Blok Olur?', cat: 'GÜNDEM', optA: 'Cumhur İttifakı', optB: 'Muhalefet Bloğu' },
            { slug: 'derbi-galip-nabiz', title: 'Hafta Sonu Süper Lig Derbisini Hangi Takım Kazanır?', cat: 'DERBİ NABZI', optA: 'Galatasaray', optB: 'Fenerbahçe' }
        ];

        for (const d of initialDuels) {
            await client.query(`
                INSERT INTO hero_duels (slug, title, category, option_a_name, option_b_name, votes_a, votes_b, is_active)
                VALUES ($1, $2, $3, $4, $5, 0, 0, true)
                ON CONFLICT (slug) DO UPDATE SET title = EXCLUDED.title, category = EXCLUDED.category
            `, [d.slug, d.title, d.cat, d.optA, d.optB]);
        }

        // FAZ 2: 10 EFSANELER MEYDANI ANKETİNİN VERİTABANINA TESCİLLENMESİ
        const legendPolls = [
            { slug: 'vs-messi-ronaldo', title: 'Tüm Zamanların En İyisi Kim?', a: 'Lionel Messi', b: 'Cristiano Ronaldo' },
            { slug: 'vs-tarkan-sezen', title: 'Türk Pop Müziğinin Zirvesi?', a: 'Tarkan', b: 'Sezen Aksu' },
            { slug: 'vs-vadi-ezel', title: 'Türk Dizi Tarihinin Başyapıtı?', a: 'Kurtlar Vadisi (İlk 97)', b: 'Ezel' },
            { slug: 'vs-baklava-kunefe', title: 'Geleneksel Tatlı Tercihiniz?', a: 'Gaziantep Baklavası', b: 'Hatay Künefesi' },
            { slug: 'vs-lahmacun-pide', title: 'Sokak Lezzetlerinde Hangisi?', a: 'Lahmacun', b: 'Karadeniz Pidesi' },
            { slug: 'vs-istanbul-izmir', title: 'Hangisinde Yaşamak İsterdiniz?', a: 'İstanbul', b: 'İzmir' },
            { slug: 'vs-ios-android', title: 'Mobil İşletim Sistemi Tercihiniz?', a: 'iOS (Apple)', b: 'Android' },
            { slug: 'vs-cay-kahve', title: 'Güne Başlama İçeceğiniz?', a: 'Demli Türk Çayı', b: 'Taze Filtre Kahve' },
            { slug: 'vs-maradona-pele', title: 'Futbol Tarihinin En Büyük Efsanesi?', a: 'Diego Maradona', b: 'Pelé' },
            { slug: 'vs-gs-fb', title: 'Ezeli Rekabette Kalbiniz Kiminle?', a: 'Galatasaray', b: 'Fenerbahçe' }
        ];

        for (const p of legendPolls) {
            await client.query(`
                INSERT INTO vs_polls (slug, title, option_a, option_b, votes_a, votes_b)
                VALUES ($1, $2, $3, $4, 0, 0)
                ON CONFLICT (slug) DO UPDATE SET title = EXCLUDED.title, option_a = EXCLUDED.option_a, option_b = EXCLUDED.option_b
            `, [p.slug, p.title, p.a, p.b]);
        }

        // 16 ZENGİN PAZAR (BORSA VE FİNANS AYRI)
        const rich16Markets = [
            { slug: 'bist-100-2026', cat: 'BORSA', sub: 'BIST 100', q: 'BIST 100 Endeksi 2026 Yıl Sonunu 12.000 Puanın Üzerinde Kapatır mı?', srcName: 'Borsa İstanbul Bülteni', closing: '31 Aralık 2026', yesR: 10000, noR: 10000 },
            { slug: 'bist-halka-arz-50', cat: 'BORSA', sub: 'Halka Arz', q: '2026 Yılında Borsa İstanbul’da Halka Arz Edilen Şirket Sayısı 50’yi Aşar mı?', srcName: 'SPK Bültenleri', closing: '31 Aralık 2026', yesR: 12000, noR: 8000 },
            { slug: 'bist-banka-rekor', cat: 'BORSA', sub: 'Sektör Endeksi', q: 'BIST Bankacılık Endeksi (XBANK) 2026 Son Çeyreğinde Tarihi Zirve Görür mü?', srcName: 'Borsa İstanbul Verileri', closing: '31 Aralık 2026', yesR: 11000, noR: 9000 },
            { slug: 'ons-altin-3200', cat: 'FİNANS', sub: 'Kıymetli Maden', q: 'Ons Altın Fiyatı 2026 Sonuna Kadar 3.200 Dolar Seviyesini Aşar mı?', srcName: 'Londra Külçe Piyasası (LBMA)', closing: '31 Aralık 2026', yesR: 9000, noR: 11000 },
            { slug: 'gumus-ons-40', cat: 'FİNANS', sub: 'Kıymetli Maden', q: 'Ons Gümüş Fiyatı 2026 Yılı İçerisinde 40 Dolar Eşiğini Aşar mı?', srcName: 'Comex Kapanış Fiyatları', closing: '31 Aralık 2026', yesR: 10000, noR: 10000 },
            { slug: 'tcmb-faiz-2026', cat: 'FİNANS', sub: 'Para Politikası', q: 'TCMB Politika Faizini 2026 Yıl Sonuna Kadar %30 veya Altına İndirir mi?', srcName: 'TCMB PPK Karar Metni', closing: '24 Aralık 2026', yesR: 11000, noR: 9000 },
            { slug: 'asgari-ucret-2027', cat: 'EKONOMİ', sub: 'Gelir & Ücret', q: '2027 Yılı Net Asgari Ücreti 35.000 TL Üzerinde Açıklanır mı?', srcName: 'Çalışma Bakanlığı / Resmi Gazete', closing: '31 Aralık 2026', yesR: 8000, noR: 12000 },
            { slug: 'enflasyon-2026', cat: 'EKONOMİ', sub: 'Fiyat İstikrarı', q: 'TÜİK Yıllık Tüketici Enflasyonu (TÜFE) 2026 Yılında %20 Altına İner mi?', srcName: 'TÜİK TÜFE Bülteni', closing: '3 Ocak 2027', yesR: 14000, noR: 6000 },
            { slug: 'turkiye-buyume-2026', cat: 'EKONOMİ', sub: 'Milli Gelir', q: 'Türkiye Ekonomisi 2026 Yıllık GSYH Büyümesi %4 Üzerinde Gerçekleşir mi?', srcName: 'TÜİK Dönemsel GSYH', closing: '15 Mart 2027', yesR: 10000, noR: 10000 },
            { slug: 'turksat-6a-ticari', cat: 'TEKNOLOJİ', sub: 'Uzay & Uydu', q: 'TÜRKSAT 6A Uydusu 2026 Yılında Tam Kapasite Ticari Hizmete Başlar mı?', srcName: 'Ulaştırma Bakanlığı', closing: '15 Kasım 2026', yesR: 6000, noR: 14000 },
            { slug: 'yapay-zeka-kanunu', cat: 'TEKNOLOJİ', sub: 'Regülasyon', q: 'TBMM 2026 Yılında Kapsamlı Ulusal Yapay Zeka Yasasını Kabul Eder mi?', srcName: 'Resmi Gazete / TBMM Tutanakları', closing: '31 Aralık 2026', yesR: 10000, noR: 10000 },
            { slug: 'yerli-unicorn-2026', cat: 'TEKNOLOJİ', sub: 'Girişimcilik', q: '2026 Yılında Türkiye’den Yeni Bir Unicorn (1 Milyar $ Değerleme) Girişim Çıkar mı?', srcName: 'Sanayi Bakanlığı Bülteni', closing: '31 Aralık 2026', yesR: 9000, noR: 11000 },
            { slug: 'dizi-reyting-rekor', cat: 'DİZİ & MEDYA', sub: 'Televizyon', q: '2026-2027 Dizi Sezonunda Total Reytingde 15 Puan Barajını Aşan Dizi Çıkar mı?', srcName: 'TİAK Resmi Reyting Ölçümleri', closing: '31 Mayıs 2027', yesR: 10000, noR: 10000 },
            { slug: 'turk-sinema-gise', cat: 'DİZİ & MEDYA', sub: 'Sinema', q: '2026 Yılında Türkiye Gişesinde 4 Milyon Seyirciyi Aşan Yerli Film Olur mu?', srcName: 'Box Office Türkiye', closing: '31 Aralık 2026', yesR: 12000, noR: 8000 },
            { slug: 'togg-sedan-teslimat', cat: 'OTOMOTİV', sub: 'Elektrikli Araç', q: 'TOGG T10F Sedan Modelinin İlk Ticari Müşteri Teslimatları 2026 İçinde Başlar mı?', srcName: 'TOGG Resmi Basın Bülteni', closing: '31 Aralık 2026', yesR: 7000, noR: 13000 },
            { slug: 'olimpiyat-madalya-2028', cat: 'SPOR', sub: 'Olimpiyat', q: 'Milli Sporcularımız 2028 Los Angeles Olimpiyatlarında 10 ve Üzeri Madalya Kazanır mı?', srcName: 'TMOK Resmi Kayıtları', closing: '31 Ağustos 2028', yesR: 10000, noR: 10000 }
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
        console.info('[DATABASE] Faz 2: Hero ve Efsaneler Meydanı Tabloları Hazırlandı.');
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

// GÜVENLİ AUTH KANCASI (MİSAFİR İZOLASYONU)
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

app.get('/health', async () => ({ status: 'UP', version: 'v1.2-PHASE2', timestamp: new Date().toISOString() }));

app.get('/api/me', async (req) => {
    if (!req.userId) return { username: 'Misafir', role: 'GUEST', balance_kor: 0, streak: 0, tier: 'Gözlemci', kvkk_accepted: false };
    const r = await pool.query(`SELECT id, username, email, role, balance_kor, streak, tier, kvkk_accepted, profile_step, personality_archetype FROM users WHERE id = $1`, [req.userId]);
    return r.rows[0] || {};
});

// HERO DÜELLOLAR (FAZ 2: KÖR OYLAMA / BANDWAGON KALKANI)
app.get('/api/duels', async (req) => {
    const r = await pool.query(`SELECT * FROM hero_duels WHERE is_active = true ORDER BY created_at ASC`);
    const userVotes = req.userId ? (await pool.query(`SELECT duel_id, choice FROM duel_votes WHERE user_id = $1`, [req.userId])).rows : [];
    
    return {
        duels: r.rows.map(d => {
            const tot = d.votes_a + d.votes_b;
            const pctA = tot > 0 ? Math.round((d.votes_a / tot) * 100) : 50;
            const myVote = userVotes.find(v => v.duel_id === d.id);
            const hasVoted = !!myVote;

            return {
                ...d,
                hasVoted,
                userChoice: myVote ? myVote.choice : null,
                totalVotes: tot.toLocaleString('tr-TR'),
                // Kör Oylama: Oy kullanmayan istemciye sunucu oranları göndermez
                pctA: hasVoted ? pctA : null,
                pctB: hasVoted ? (100 - pctA) : null
            };
        })
    };
});

app.post('/api/duels/:id/vote', async (req, rep) => {
    if (!req.userId) return rep.status(401).send({ error: 'Oy kullanmak için giriş yapmalısınız.' });
    const { choice } = req.body || {};
    if (choice !== 'A' && choice !== 'B') return rep.status(400).send({ error: 'Geçersiz tercih' });

    try {
        const result = await runInTransaction(async (c) => {
            const check = await c.query(`SELECT 1 FROM duel_votes WHERE duel_id = $1 AND user_id = $2`, [req.params.id, req.userId]);
            if (check.rows.length > 0) throw new Error('Bu düelloda zaten oy kullandınız.');

            await c.query(`INSERT INTO duel_votes (duel_id, user_id, choice) VALUES ($1, $2, $3)`, [req.params.id, req.userId, choice]);
            const col = choice === 'A' ? 'votes_a' : 'votes_b';
            const r = await c.query(`UPDATE hero_duels SET ${col} = ${col} + 1 WHERE id = $1 RETURNING votes_a, votes_b`, [req.params.id]);
            const tot = r.rows[0].votes_a + r.rows[0].votes_b;
            return { pctA: Math.round((r.rows[0].votes_a / tot) * 100), pctB: 100 - Math.round((r.rows[0].votes_a / tot) * 100), totalVotes: tot.toLocaleString('tr-TR') };
        });

        broadcast('DUEL_VOTE', { duelId: req.params.id, ...result });
        return rep.send({ success: true, ...result });
    } catch(e) {
        return rep.status(400).send({ error: e.message });
    }
});

// FAZ 2: EFSANELER MEYDANI API (CANLI VERİTABANI & KÖR OYLAMA)
app.get('/api/vs', async (req) => {
    const r = await pool.query(`SELECT * FROM vs_polls ORDER BY created_at ASC`);
    const userVotes = req.userId ? (await pool.query(`SELECT poll_id, choice FROM vs_votes WHERE user_id = $1`, [req.userId])).rows : [];

    return {
        polls: r.rows.map(p => {
            const tot = p.votes_a + p.votes_b;
            const pctA = tot > 0 ? Math.round((p.votes_a / tot) * 100) : 50;
            const myVote = userVotes.find(v => v.poll_id === p.id);
            const hasVoted = !!myVote;

            return {
                ...p,
                hasVoted,
                userChoice: myVote ? myVote.choice : null,
                totalVotes: tot.toLocaleString('tr-TR'),
                // Kör Oylama
                pctA: hasVoted ? pctA : null,
                pctB: hasVoted ? (100 - pctA) : null
            };
        })
    };
});

app.post('/api/vs/:id/vote', async (req, rep) => {
    if (!req.userId) return rep.status(401).send({ error: 'VS Arenasında oy kullanmak için lütfen giriş yapın.' });
    const { choice } = req.body || {};
    if (choice !== 'A' && choice !== 'B') return rep.status(400).send({ error: 'Geçersiz tercih' });

    try {
        const result = await runInTransaction(async (c) => {
            const check = await c.query(`SELECT 1 FROM vs_votes WHERE poll_id = $1 AND user_id = $2`, [req.params.id, req.userId]);
            if (check.rows.length > 0) throw new Error('Bu kapışmada zaten oy kullandınız.');

            await c.query(`INSERT INTO vs_votes (poll_id, user_id, choice) VALUES ($1, $2, $3)`, [req.params.id, req.userId, choice]);
            const col = choice === 'A' ? 'votes_a' : 'votes_b';
            const r = await c.query(`UPDATE vs_polls SET ${col} = ${col} + 1 WHERE id = $1 RETURNING votes_a, votes_b`, [req.params.id]);
            const tot = r.rows[0].votes_a + r.rows[0].votes_b;
            return { pctA: Math.round((r.rows[0].votes_a / tot) * 100), pctB: 100 - Math.round((r.rows[0].votes_a / tot) * 100), totalVotes: tot.toLocaleString('tr-TR') };
        });

        return rep.send({ success: true, ...result });
    } catch(e) {
        return rep.status(400).send({ error: e.message });
    }
});

// PAZARLAR & DETAY
app.get('/api/markets', async () => {
    const r = await pool.query(`
        SELECT m.*, COALESCE(a.yes_reserve, 10000) as yes_reserve, COALESCE(a.no_reserve, 10000) as no_reserve 
        FROM markets m LEFT JOIN amm_state a ON m.id = a.market_id ORDER BY m.created_at ASC
    `);
    return {
        markets: r.rows.map(m => {
            const y = new Decimal(m.yes_reserve), n = new Decimal(m.no_reserve);
            const probYes = Math.round(n.div(y.plus(n)).mul(100).toNumber());
            return { ...m, probYes, probNo: 100 - probYes, poolTotal: Math.round(y.plus(n).toNumber()).toLocaleString('tr-TR') };
        })
    };
});

app.get('/api/markets/:slug', async (req, rep) => {
    const r = await pool.query(`
        SELECT m.*, COALESCE(a.yes_reserve, 10000) as yes_reserve, COALESCE(a.no_reserve, 10000) as no_reserve 
        FROM markets m LEFT JOIN amm_state a ON m.id = a.market_id WHERE m.slug = $1
    `, [req.params.slug]);

    if (r.rows.length === 0) return rep.status(404).send({ error: 'Pazar bulunamadı.' });
    const m = r.rows[0];
    const y = new Decimal(m.yes_reserve), n = new Decimal(m.no_reserve);
    const probYes = Math.round(n.div(y.plus(n)).mul(100).toNumber());

    let hr = await pool.query(`SELECT prob_yes, pool_total, to_char(created_at, 'DD Mon HH24:MI') as time_label FROM market_price_history WHERE market_id = $1 ORDER BY created_at ASC`, [m.id]);
    if (hr.rows.length === 0) hr = { rows: [{ prob_yes: 50, time_label: '28 Eki' }, { prob_yes: probYes, time_label: 'Bugün' }] };
    return { market: { ...m, probYes, probNo: 100 - probYes, poolTotal: Math.round(y.plus(n).toNumber()).toLocaleString('tr-TR') }, history: hr.rows };
});

// QUOTE MOTORU
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

// TAHMİN ALIM (BUY)
app.post('/api/trade/predict', async (req, rep) => {
    if (!req.userId) return rep.status(401).send({ error: 'Tahmin bildirmek için giriş yapmalısınız.' });
    const { marketId, outcome, amountKor } = req.body || {};
    const amt = new Decimal(amountKor || 0);
    if (!marketId || !outcome || amt.lte(0)) return rep.status(400).send({ error: 'Geçersiz parametre.' });

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

// ERKEN SATIŞ (SELL)
app.post('/api/trade/sell', async (req, rep) => {
    if (!req.userId) return rep.status(401).send({ error: 'İşlem için giriş yapmalısınız.' });
    const { marketId, outcome, sharesToSell } = req.body || {};
    const sIn = new Decimal(sharesToSell || 0);
    if (!marketId || !outcome || sIn.lte(0)) return rep.status(400).send({ error: 'Geçersiz pay miktarı.' });

    try {
        const result = await runInTransaction(async (c) => {
            const mCheck = await c.query(`SELECT status FROM markets WHERE id = $1`, [marketId]);
            if (mCheck.rows.length === 0 || mCheck.rows[0].status !== 'TRADING') throw new Error('Bu oylama kapalıdır.');

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

            const feeHalf = MoneyMath.roundDown(calc.fee.div(2), 6);
            await LedgerEngine.recordEntry(c, 'PREDICTION_SELL', marketId, null, [
                { accountCode: '2100', marketId, debit: calc.grossPayout, credit: new Decimal(0) },
                { accountCode: '2000', userId: req.userId, debit: new Decimal(0), credit: calc.netPayout },
                { accountCode: '4000', debit: new Decimal(0), credit: feeHalf },
                { accountCode: '5000', debit: new Decimal(0), credit: calc.fee.minus(feeHalf) }
            ]);

            const newProb = Math.round(nn.div(ny.plus(nn)).mul(100).toNumber());
            await c.query(`INSERT INTO market_price_history (market_id, prob_yes, pool_total) VALUES ($1, $2, $3)`, [marketId, newProb, ny.plus(nn).toFixed(2)]);

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

// PAZAR SONUÇLANDIRMA (RESOLVER)
app.post('/api/admin/markets/:id/resolve', async (req, rep) => {
    if (!req.userId || req.userRole !== 'ADMIN' && req.username !== 'LeisanB') {
        return rep.status(403).send({ error: 'Yönetici yetkisi gereklidir.' });
    }
    const { outcome, proofUrl } = req.body || {};
    if (outcome !== 'YES' && outcome !== 'NO') return rep.status(400).send({ error: 'Sonuç YES veya NO olmalıdır.' });

    try {
        const summary = await runInTransaction(async (c) => {
            const m = await c.query(`SELECT id, status, question FROM markets WHERE id = $1 FOR UPDATE`, [req.params.id]);
            if (m.rows.length === 0) throw new Error('Pazar bulunamadı.');
            if (m.rows[0].status === 'RESOLVED') throw new Error('Bu pazar zaten sonuçlandırılmıştır.');

            await c.query(`
                UPDATE markets SET status = 'RESOLVED', resolved_outcome = $1, resolution_proof = $2, resolved_at = NOW() 
                WHERE id = $3
            `, [outcome, proofUrl || 'Resmi Bülten Teyidi', req.params.id]);

            const pos = await c.query(`SELECT id, user_id, outcome, shares FROM positions WHERE market_id = $1 AND is_settled = false FOR UPDATE`, [req.params.id]);
            let totalDistributed = new Decimal(0);
            let winnerCount = 0;

            for (const p of pos.rows) {
                const s = new Decimal(p.shares);
                let payout = new Decimal(0);
                if (p.outcome === outcome && s.gt(0.001)) {
                    payout = MoneyMath.roundDown(s, 6);
                    await c.query(`UPDATE users SET balance_kor = balance_kor + $1 WHERE id = $2`, [payout.toFixed(6), p.user_id]);
                    totalDistributed = totalDistributed.plus(payout);
                    winnerCount++;

                    await LedgerEngine.recordEntry(c, 'MARKET_SETTLEMENT_PAYOUT', req.params.id, null, [
                        { accountCode: '2100', marketId: req.params.id, debit: payout, credit: new Decimal(0) },
                        { accountCode: '2000', userId: p.user_id, debit: new Decimal(0), credit: payout }
                    ]);
                }
                await c.query(`UPDATE positions SET is_settled = true, settlement_payout = $1, updated_at = NOW() WHERE id = $2`, [payout.toFixed(6), p.id]);
            }

            return { marketTitle: m.rows[0].question, outcome, winnerCount, totalDistributed: totalDistributed.toFixed(2) };
        });

        broadcast('MARKET_RESOLVED', { marketId: req.params.id, ...summary });
        return rep.send({ success: true, ...summary });
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

// YORUMLAR & KOHORT & CSV
app.get('/api/markets/:slug/comments', async (req, rep) => {
    const mRes = await pool.query(`SELECT id FROM markets WHERE slug = $1`, [req.params.slug]);
    if (mRes.rows.length === 0) return rep.status(404).send({ error: 'Pazar bulunamadı' });

    const r = await pool.query(`
        SELECT c.*, u.username, u.tier, to_char(c.created_at, 'DD Mon YYYY, HH24:MI') as time_formatted
        FROM comments c JOIN users u ON c.user_id = u.id
        WHERE c.market_id = $1 ORDER BY c.upvotes DESC, c.created_at DESC
    `, [mRes.rows[0].id]);
    return { comments: r.rows };
});

app.post('/api/markets/:slug/comments', async (req, rep) => {
    if (!req.userId) return rep.status(401).send({ error: 'Analiz paylaşmak için giriş yapmalısınız.' });
    const { content } = req.body || {};
    if (!content || content.trim().length < 5) return rep.status(400).send({ error: 'Analiz en az 5 karakter olmalıdır.' });

    const mRes = await pool.query(`SELECT id FROM markets WHERE slug = $1`, [req.params.slug]);
    if (mRes.rows.length === 0) return rep.status(404).send({ error: 'Pazar bulunamadı' });

    const posRes = await pool.query(`SELECT outcome, shares FROM positions WHERE market_id = $1 AND user_id = $2 AND shares > 0.001 LIMIT 1`, [mRes.rows[0].id, req.userId]);
    const outcome = posRes.rows.length > 0 ? posRes.rows[0].outcome : null;
    const shares = posRes.rows.length > 0 ? Math.round(parseFloat(posRes.rows[0].shares)) : 0;

    await pool.query(`INSERT INTO comments (market_id, user_id, content, outcome_at_time, shares_at_time) VALUES ($1, $2, $3, $4, $5)`, [mRes.rows[0].id, req.userId, content.trim(), outcome, shares]);
    return { success: true };
});

app.get('/api/markets/:slug/cohorts', async (req, rep) => {
    const mRes = await pool.query(`SELECT id FROM markets WHERE slug = $1`, [req.params.slug]);
    if (mRes.rows.length === 0) return rep.status(404).send({ error: 'Pazar bulunamadı' });

    const eduRes = await pool.query(`SELECT u.industry, p.outcome, COUNT(p.id) as vote_count FROM positions p JOIN users u ON p.user_id = u.id WHERE p.market_id = $1 AND p.shares > 0.001 GROUP BY u.industry, p.outcome`, [mRes.rows[0].id]);
    const ageRes = await pool.query(`SELECT CASE WHEN (2026 - u.birth_year) < 30 THEN 'Genç (18-29)' WHEN (2026 - u.birth_year) BETWEEN 30 AND 44 THEN 'Orta Yaş (30-44)' ELSE 'Deneyimli (45+)' END as age_group, p.outcome, COUNT(p.id) as vote_count FROM positions p JOIN users u ON p.user_id = u.id WHERE p.market_id = $1 AND p.shares > 0.001 AND u.birth_year IS NOT NULL GROUP BY age_group, p.outcome`, [mRes.rows[0].id]);

    return { industryCohorts: eduRes.rows, ageCohorts: ageRes.rows };
});

app.get('/api/markets/:slug/export', async (req, rep) => {
    const mRes = await pool.query(`SELECT id, question FROM markets WHERE slug = $1`, [req.params.slug]);
    if (mRes.rows.length === 0) return rep.status(404).send('Not found');

    const pRes = await pool.query(`SELECT p.outcome, p.shares, p.total_invested, u.city, u.industry, u.birth_year, u.personality_archetype FROM positions p JOIN users u ON p.user_id = u.id WHERE p.market_id = $1 AND p.shares > 0.001`, [mRes.rows[0].id]);
    let csv = 'Pazar,Tercih,Pay,Yatirim_KOR,Sehir,Sektor,Dogum_Yili,Arketip\n';
    pRes.rows.forEach(r => {
        csv += `"${mRes.rows[0].question}","${r.outcome}",${r.shares},${r.total_invested},"${r.city || ''}","${r.industry || ''}","${r.birth_year || ''}","${r.personality_archetype || ''}"\n`;
    });

    rep.header('Content-Type', 'text/csv; charset=utf-8');
    rep.header('Content-Disposition', `attachment; filename="oyver_${req.params.slug}.csv"`);
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

app.post('/api/contact', async (req, rep) => {
    await pool.query(`INSERT INTO contact_leads (type, name, email, company, message) VALUES ($1, $2, $3, $4, $5)`, [req.body?.type || 'GENERAL', req.body?.name, req.body?.email, req.body?.company, req.body?.message]);
    return { success: true, message: 'Mesajınız iletildi.' };
});

app.post('/api/auth/login-mock', async (req, rep) => {
    const name = req.body?.username || 'LeisanB';
    const email = `${name.toLowerCase()}@oyver.pro`;
    const role = name === 'AdminLeisan' ? 'ADMIN' : 'USER';

    const ur = await pool.query(`INSERT INTO users (email, username, role, tier) VALUES ($1, $2, $3, 'Doçent') ON CONFLICT (email) DO UPDATE SET username = EXCLUDED.username, role = EXCLUDED.role RETURNING id, username, balance_kor`, [email, name, role]);
    const token = crypto.randomBytes(32).toString('hex');
    await pool.query(`INSERT INTO sessions (token, user_id) VALUES ($1, $2)`, [token, user.id]);
    rep.header('Set-Cookie', `oyver_token=${token}; Path=/; HttpOnly; SameSite=Lax; Max-Age=2592000`);
    return { success: true };
});

// ==========================================
// 4. FRONTEND ARAYÜZÜ (G3 SÖZLEŞMESİ & OBSIDIAN TEMA)
// ==========================================
function renderIndexHtml() {
    return `<!DOCTYPE html>
<html lang="tr" class="theme-obsidian">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
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

    <!-- HEADER (G3 TEK SATIR HİZALAMASI) -->
    <header class="sticky top-0 z-40 bg-[#080c14]/90 backdrop-blur-md border-b border-slate-800/80">
        <div class="container mx-auto px-4 h-16 flex items-center justify-between gap-4 max-w-5xl">
            <div class="flex items-center space-x-6">
                <a href="/" data-action="nav-home" class="text-2xl font-black tracking-tight text-white flex items-center gap-1.5">
                    OYVER<span class="text-[11px] px-1.5 py-0.5 rounded bg-indigo-500/20 text-indigo-400 border border-indigo-500/30">PRO</span>
                </a>
                <nav class="hidden sm:flex items-center space-x-2 text-xs font-bold">
                    <button data-action="nav-vs" class="px-3.5 py-2 rounded-xl text-slate-300 hover:text-white hover:bg-slate-900 transition flex items-center gap-2">
                        <i class="fas fa-bolt text-amber-400"></i> Efsaneler Meydanı
                    </button>
                    <button data-action="nav-portfolio" class="px-3 py-1.5 rounded-xl text-slate-300 hover:text-white hover:bg-slate-900 transition flex items-center gap-1.5">
                        <i class="fas fa-briefcase text-emerald-400"></i> Portföyüm
                    </button>
                </nav>
            </div>

            <!-- G3 SAĞ KÜME (TEK HAP: SERİ + KOR + PROFİL MENÜSÜ) -->
            <div class="flex items-center space-x-2.5">
                <div class="relative">
                    <div id="user-badge" class="flex items-center bg-slate-900 border border-slate-800 rounded-xl p-1 pr-3 space-x-2 cursor-pointer hover:border-slate-700 transition" data-action="toggle-user-dropdown">
                        <div class="flex items-center gap-1 bg-slate-950 px-2 py-1 rounded-lg border border-slate-800">
                            <i class="fas fa-fire text-amber-500 text-xs"></i>
                            <span id="user-streak" class="text-xs font-black text-amber-400">5g</span>
                        </div>
                        <div class="text-xs font-bold text-slate-200" id="user-balance">-- KOR</div>
                        <i class="fas fa-chevron-down text-[10px] text-slate-500"></i>
                    </div>

                    <div id="user-dropdown" class="hidden absolute right-0 top-14 w-60 bg-[#0d131f] border border-slate-800 rounded-2xl shadow-2xl p-2 z-50 space-y-1">
                        <div class="px-3 py-2 border-b border-slate-800/80 mb-1">
                            <div class="text-xs font-bold text-white flex justify-between">
                                <span id="dd-username">Misafir</span>
                                <span class="text-[10px] text-emerald-400 font-bold" id="dd-archetype">-</span>
                            </div>
                            <div class="text-[10px] text-indigo-400 font-bold" id="dd-tier">Gözlemci</div>
                        </div>
                        <button data-action="open-explainer" class="w-full text-left px-3 py-2 text-xs font-bold text-slate-200 hover:bg-slate-800/60 rounded-xl transition flex items-center gap-2">
                            <i class="fas fa-sparkles text-amber-400"></i> 30s Rehber
                        </button>
                        <button data-action="open-leaderboard" class="w-full text-left px-3 py-2 text-xs font-bold text-slate-200 hover:bg-slate-800/60 rounded-xl transition flex items-center gap-2">
                            <i class="fas fa-graduation-cap text-indigo-400"></i> Liyakat Ligi
                        </button>
                        <button data-action="toggle-theme" class="w-full text-left px-3 py-2 text-xs font-bold text-slate-200 hover:bg-slate-800/60 rounded-xl transition flex items-center justify-between">
                            <span class="flex items-center gap-2"><i class="fas fa-palette text-pink-400"></i> Tema Değiştir</span>
                            <span class="text-[10px] text-slate-400" id="theme-label">Obsidian</span>
                        </button>
                        <button data-action="open-drawer" data-drawer="about" class="w-full text-left px-3 py-2 text-xs font-bold text-slate-200 hover:bg-slate-800/60 rounded-xl transition flex items-center gap-2">
                            <i class="fas fa-info-circle text-slate-400"></i> Biz Kimiz?
                        </button>
                        <button data-action="open-drawer" data-drawer="b2b" class="w-full text-left px-3 py-2 text-xs font-bold text-slate-200 hover:bg-slate-800/60 rounded-xl transition flex items-center gap-2">
                            <i class="fas fa-chart-pie text-emerald-400"></i> B2B Güven Endeksi
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
            <!-- G3 HERO ALANI (FAZ 2: 2 KART KURALI + ARŞİV BUTONU) -->
            <section class="py-8 border-b border-slate-900 bg-gradient-to-b from-slate-900/30 to-transparent">
                <div class="container mx-auto px-4 max-w-5xl">
                    <div class="text-center mb-6">
                        <span class="inline-flex items-center gap-1.5 px-3 py-1 rounded-full bg-purple-500/10 border border-purple-500/20 text-xs font-bold text-purple-300 mb-2">
                            <span class="w-2 h-2 rounded-full bg-emerald-400 animate-ping"></span> Canlı Kamuoyu Düelloları
                        </span>
                        <h1 class="text-2xl sm:text-3xl font-black text-white tracking-tight">Yarının Nabzını Bugün Tutun.</h1>
                    </div>
                    <div id="hero-duels-container" class="grid grid-cols-1 sm:grid-cols-2 gap-4"></div>
                    <div class="mt-4 text-center">
                        <button data-action="open-duel-archive" class="px-4 py-2 bg-slate-900/80 hover:bg-slate-800 border border-slate-800 rounded-xl text-xs font-bold text-purple-300 hover:text-white transition inline-flex items-center gap-2">
                            <i class="fas fa-folder-open"></i> 📂 Diğer Düelloları Gör (Arşiv)
                        </button>
                    </div>
                </div>
            </section>

            <!-- AÇILIR OKLU (▾) KATEGORİ BANDI (BORSA VE FİNANS AYRI) -->
            <section class="container mx-auto px-4 pt-6 pb-2 max-w-5xl">
                <div class="flex items-center space-x-2 overflow-x-auto pb-2">
                    <button class="cat-btn active bg-indigo-600 text-white px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap" data-action="cat-filter" data-cat="ALL">Tümü</button>
                    <button class="cat-btn bg-slate-900 hover:bg-slate-800 text-slate-300 px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap flex items-center gap-1" data-action="cat-filter" data-cat="BORSA">
                        <span>Borsa</span> <i class="fas fa-chevron-down text-[10px] text-slate-500"></i>
                    </button>
                    <button class="cat-btn bg-slate-900 hover:bg-slate-800 text-slate-300 px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap flex items-center gap-1" data-action="cat-filter" data-cat="FİNANS">
                        <span>Finans</span> <i class="fas fa-chevron-down text-[10px] text-slate-500"></i>
                    </button>
                    <button class="cat-btn bg-slate-900 hover:bg-slate-800 text-slate-300 px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap flex items-center gap-1" data-action="cat-filter" data-cat="EKONOMİ">
                        <span>Ekonomi</span> <i class="fas fa-chevron-down text-[10px] text-slate-500"></i>
                    </button>
                    <button class="cat-btn bg-slate-900 hover:bg-slate-800 text-slate-300 px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap flex items-center gap-1" data-action="cat-filter" data-cat="TEKNOLOJİ">
                        <span>Teknoloji</span> <i class="fas fa-chevron-down text-[10px] text-slate-500"></i>
                    </button>
                    <button class="cat-btn bg-slate-900 hover:bg-slate-800 text-slate-300 px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap flex items-center gap-1" data-action="cat-filter" data-cat="DİZİ & MEDYA">
                        <span>Dizi & Film</span> <i class="fas fa-chevron-down text-[10px] text-slate-500"></i>
                    </button>
                    <button class="cat-btn bg-slate-900 hover:bg-slate-800 text-slate-300 px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap flex items-center gap-1" data-action="cat-filter" data-cat="OTOMOTİV">
                        <span>Otomotiv</span> <i class="fas fa-chevron-down text-[10px] text-slate-500"></i>
                    </button>
                </div>
                <div id="sub-cat-container" class="hidden flex items-center space-x-1.5 overflow-x-auto pt-2 pb-1 border-t border-slate-900 mt-2"></div>
            </section>

            <!-- 16 ZENGİN PAZAR KARTI -->
            <section class="container mx-auto px-4 py-4 max-w-5xl">
                <div id="market-grid" class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-5"></div>
            </section>
        </main>

        <!-- PAZAR DETAY SAYFASI (/market/:slug) -->
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
                    <i class="fas fa-chart-line text-indigo-400"></i> Grafik & Teklif Paneli
                </button>
                <button data-action="dt-switch-tab" data-tab="community" id="dt-tab-community" class="pb-2.5 text-slate-400 hover:text-white flex items-center gap-1.5">
                    <i class="fas fa-comments text-pink-400"></i> Topluluk Analizleri <span id="dt-comm-count" class="px-1.5 py-0.2 rounded bg-slate-800 text-[10px]">0</span>
                </button>
                <button data-action="dt-switch-tab" data-tab="cohorts" id="dt-tab-cohorts" class="pb-2.5 text-slate-400 hover:text-white flex items-center gap-1.5">
                    <i class="fas fa-layer-group text-amber-400"></i> B2B Kohort Dağılımı
                </button>
            </div>

            <!-- GRAFİK & TEKLİF PANELİ -->
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
                            <i class="fas fa-shield-alt text-indigo-400"></i> Çözümleme Kriteri & Tescilli Kaynak
                        </h3>
                        <p id="dt-desc" class="text-xs text-slate-400 leading-relaxed">Pazar açıklaması...</p>
                        <div class="p-3 bg-slate-950 rounded-xl border border-slate-800 text-xs space-y-1">
                            <div class="text-slate-400">Resmi Doğrulama Kaynağı:</div>
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
                        <label class="block text-xs font-bold text-slate-400 mb-1">Puan Miktarı</label>
                        <input type="number" id="dt-input-amount" value="500" min="50" step="50" class="w-full bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-white font-bold text-sm focus:outline-none focus:border-indigo-500">
                    </div>
                    <div class="p-3 bg-slate-950/80 rounded-xl border border-slate-800 space-y-1 text-xs">
                        <div class="flex justify-between text-slate-400"><span>Alınacak Pay:</span><span id="dt-quote-shares" class="font-bold text-white">-- Pay</span></div>
                        <div class="flex justify-between text-slate-400"><span>Ortalama Fiyat:</span><span id="dt-quote-avg" class="font-bold text-slate-300">-- Puan</span></div>
                        <div class="flex justify-between pt-1 border-t border-slate-800 font-bold"><span class="text-slate-300">Doğrulanırsa:</span><span id="dt-quote-payout" class="font-black text-emerald-400">-- KOR</span></div>
                    </div>
                    <button data-action="dt-predict-submit" id="dt-btn-predict" class="w-full py-3 rounded-xl bg-indigo-600 hover:bg-indigo-500 text-white font-extrabold text-xs shadow-lg transition">
                        Tahmini Onayla (KOR)
                    </button>
                </div>
            </div>

            <!-- TOPLULUK ANALİZLERİ -->
            <div id="dt-view-community" class="hidden space-y-5 max-w-3xl">
                <div class="card-bg border rounded-2xl p-5 space-y-3">
                    <h3 class="text-xs font-bold text-white flex items-center gap-2">
                        <i class="fas fa-pen-nib text-indigo-400"></i> Gerekçeli Analizinizi Paylaşın
                    </h3>
                    <textarea id="comm-input-content" rows="3" placeholder="Görüşünüzü destekleyen veri ve argümanı yazın..." class="w-full bg-slate-950 border border-slate-800 rounded-xl p-3 text-xs text-white placeholder-slate-500 focus:outline-none focus:border-indigo-500"></textarea>
                    <button data-action="submit-comment" class="px-5 py-2 bg-indigo-600 hover:bg-indigo-500 rounded-xl text-xs font-bold text-white transition">
                        Analizi Yayınla
                    </button>
                </div>
                <div id="dt-comments-list" class="space-y-3"></div>
            </div>

            <!-- B2B KOHORT DAĞILIMI & CSV -->
            <div id="dt-view-cohorts" class="hidden space-y-5">
                <div class="flex justify-between items-center bg-slate-900/60 p-4 rounded-2xl border border-slate-800">
                    <div>
                        <h4 class="text-xs font-bold text-white uppercase">Kamuoyu Demografik Kırılımı</h4>
                        <p class="text-[11px] text-slate-400">Katılımcıların sektör ve yaş dağılımı.</p>
                    </div>
                    <button data-action="download-cohort-csv" class="px-3.5 py-2 bg-emerald-600/20 hover:bg-emerald-600 border border-emerald-500/40 text-emerald-300 hover:text-white rounded-xl text-xs font-bold transition flex items-center gap-1.5">
                        <i class="fas fa-file-csv"></i> Ham Veriyi İndir (CSV)
                    </button>
                </div>
                <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
                    <div class="card-bg border rounded-2xl p-5 space-y-3">
                        <h4 class="text-xs font-bold text-slate-300 uppercase">Yaş Dağılımı</h4>
                        <div id="cohort-age-list" class="space-y-2 text-xs"></div>
                    </div>
                    <div class="card-bg border rounded-2xl p-5 space-y-3">
                        <h4 class="text-xs font-bold text-slate-300 uppercase">Sektörel Dağılım</h4>
                        <div id="cohort-ind-list" class="space-y-2 text-xs"></div>
                    </div>
                </div>
            </div>
        </main>

        <!-- FAZ 2: EFSANELER MEYDANI (ALL-TIME VS - POPÜLER KÜLTÜR ARENASI) -->
        <main id="section-vs" class="hidden container mx-auto px-4 py-8 max-w-4xl">
            <div class="text-center mb-8">
                <span class="px-3 py-1 rounded-full bg-amber-500/10 border border-amber-500/20 text-xs font-bold text-amber-400">Puansız, Risksiz, Saf Kamuoyu Nabzı</span>
                <h2 class="text-2xl sm:text-3xl font-black text-white mt-2 tracking-tight">Efsaneler Meydanı</h2>
                <p class="text-xs text-slate-400 mt-1 max-w-lg mx-auto">Tüm zamanların en büyük ikilemleri ve popüler kültür rekabetleri. Tercihinizi bildirin, kamuoyunun yönünü açın!</p>
            </div>
            <div id="vs-polls-list" class="grid grid-cols-1 sm:grid-cols-2 gap-4"></div>
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
                    <i class="fas fa-graduation-cap text-indigo-400"></i> 30 Saniyede OYVER PRO
                </h3>
                <button data-action="close-explainer" class="text-slate-400 hover:text-white"><i class="fas fa-times"></i></button>
            </div>
            <div class="space-y-3 text-xs leading-relaxed text-slate-300">
                <div class="p-3 bg-slate-950 rounded-2xl border border-slate-800 space-y-1">
                    <span class="font-black text-amber-400">💰 1. KOR Nedir? Cebimden Para Çıkar mı?</span>
                    <p class="text-slate-400">Kesinlikle hayır! 14.500 KOR liyakat puanı ücretsiz verilir. Puanların nakit karşılığı ve çekimi yoktur.</p>
                </div>
                <div class="p-3 bg-slate-950 rounded-2xl border border-slate-800 space-y-1">
                    <span class="font-black text-pink-400">⚡ 2. Efsaneler Meydanı Nedir?</span>
                    <p class="text-slate-400">Puan riski olmadan, gündemin en popüler ikilemlerine tek tıkla oy verip toplumun anlık eğilimini gördüğünüz alandır.</p>
                </div>
                <div class="p-3 bg-slate-950 rounded-2xl border border-slate-800 space-y-1">
                    <span class="font-black text-indigo-400">🎓 3. Ordinaryüs Kademesi Nasıl Kazanılır?</span>
                    <p class="text-slate-400">Tahminleriniz resmi bültenlerle doğrulandıkça Brier kalibrasyon puanınız artar. Sırasıyla Doçent, Profesör ve zirvede Ordinaryüs olursunuz!</p>
                </div>
            </div>
            <button data-action="close-explainer" class="w-full py-2.5 bg-indigo-600 hover:bg-indigo-500 rounded-xl font-bold text-white text-xs transition">
                Anladım, Öngörüye Başla!
            </button>
        </div>
    </div>

    <!-- FAZ 2: DÜELLO ARŞİV MODALI -->
    <div id="duel-archive-modal" class="fixed inset-0 bg-slate-950/85 z-50 backdrop-blur-md hidden flex items-center justify-center p-4">
        <div class="card-bg border w-full max-w-2xl rounded-3xl p-6 max-h-[85vh] overflow-y-auto space-y-4">
            <div class="flex justify-between items-center pb-3 border-b border-slate-800">
                <h3 class="text-base font-black text-white flex items-center gap-2">
                    <i class="fas fa-folder-open text-purple-400"></i> Tüm Kamuoyu Düelloları Arşivi
                </h3>
                <button data-action="close-duel-archive" class="text-slate-400 hover:text-white"><i class="fas fa-times"></i></button>
            </div>
            <div id="duel-archive-list" class="space-y-3"></div>
        </div>
    </div>

    <!-- KURUMSAL VE BİLGİ MODALI -->
    <div id="info-modal" class="fixed inset-0 bg-slate-950/85 z-50 backdrop-blur-md hidden flex items-center justify-center p-4">
        <div class="card-bg border w-full max-w-lg rounded-3xl p-6 max-h-[85vh] overflow-y-auto space-y-4">
            <div class="flex justify-between items-center pb-3 border-b border-slate-800">
                <h3 class="text-base font-black text-white" id="info-modal-title">Başlık</h3>
                <button data-action="close-info-modal" class="text-slate-400 hover:text-white"><i class="fas fa-times"></i></button>
            </div>
            <div id="info-modal-content" class="text-xs text-slate-300 leading-relaxed space-y-3"></div>
        </div>
    </div>

    <!-- LİDERLİK TABLOSU -->
    <div id="leaderboard-modal" class="fixed inset-0 bg-slate-950/85 z-50 backdrop-blur-md hidden flex items-center justify-center p-4">
        <div class="card-bg border w-full max-w-xl rounded-3xl p-5 max-h-[85vh] flex flex-col">
            <div class="flex justify-between items-center pb-3 border-b border-slate-800 mb-3">
                <div class="flex items-center gap-2"><i class="fas fa-graduation-cap text-amber-400"></i><h3 class="text-base font-black text-white">Liyakat Sıralaması</h3></div>
                <button data-action="close-leaderboard" class="text-slate-400"><i class="fas fa-times"></i></button>
            </div>
            <div class="overflow-y-auto flex-grow divide-y divide-slate-800/80" id="leaderboard-list"></div>
        </div>
    </div>

    <!-- FOOTER -->
    <footer class="bg-[#05080f] border-t border-slate-900 py-10 mt-auto text-xs text-slate-400">
        <div class="container mx-auto px-4 max-w-5xl">
            <div class="grid grid-cols-1 md:grid-cols-4 gap-8 mb-8">
                <div class="space-y-2 md:col-span-2">
                    <a href="/" data-action="nav-home" class="text-xl font-black tracking-tight text-white flex items-center gap-1.5">
                        OYVER<span class="text-[10px] px-1.5 py-0.5 rounded bg-indigo-500/20 text-indigo-400 border border-indigo-500/30">PRO</span>
                    </a>
                    <p class="text-slate-400 text-xs leading-relaxed max-w-md">
                        Türkiye’nin liyakat tabanlı ilk kolektif öngörü ve kamuoyu araştırma terminali.
                    </p>
                </div>
                <div>
                    <h4 class="text-white font-bold mb-3 uppercase tracking-wider text-[11px]">Platform</h4>
                    <ul class="space-y-2">
                        <li><button data-action="open-drawer" data-drawer="about" class="hover:text-indigo-400 transition">Biz Kimiz?</button></li>
                        <li><button data-action="open-leaderboard" class="hover:text-indigo-400 transition">Liyakat Sıralaması</button></li>
                        <li><button data-action="nav-vs" class="hover:text-indigo-400 transition">Efsaneler Meydanı</button></li>
                    </ul>
                </div>
                <div>
                    <h4 class="text-white font-bold mb-3 uppercase tracking-wider text-[11px]">Kurumsal</h4>
                    <ul class="space-y-2">
                        <li><button data-action="open-drawer" data-drawer="b2b" class="hover:text-indigo-400 transition text-emerald-400">B2B Güven Endeksi</button></li>
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
        <button data-action="nav-vs" class="flex flex-col items-center gap-1 text-slate-400 hover:text-amber-400 transition">
            <i class="fas fa-bolt text-base"></i><span class="text-[10px] font-bold">Meydan</span>
        </button>
        <button data-action="nav-portfolio" class="flex flex-col items-center gap-1 text-slate-400 hover:text-emerald-400 transition">
            <i class="fas fa-briefcase text-base"></i><span class="text-[10px] font-bold">Portföy</span>
        </button>
        <button data-action="open-leaderboard" class="flex flex-col items-center gap-1 text-slate-400 hover:text-indigo-400 transition">
            <i class="fas fa-graduation-cap text-base"></i><span class="text-[10px] font-bold">Liyakat</span>
        </button>
    </nav>

    <!-- MERKEZİ JAVASCRIPT MOTORU -->
    <script>
        var markets = [];
        var heroDuels = [];
        var vsPolls = [];
        var currentBalance = 0;
        var currentUserRole = 'GUEST';
        var activeCategory = 'ALL';
        var activeSubCategory = 'ALL';
        var activeDetailMarket = null;
        var activeDetailChoice = 'YES';
        var chartInstance = null;

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
            var secVs = document.getElementById('section-vs');
            var secPortfolio = document.getElementById('section-portfolio');

            secMarkets.classList.add('hidden');
            secMarketDetail.classList.add('hidden');
            secVs.classList.add('hidden');
            secPortfolio.classList.add('hidden');

            if (target === 'markets') secMarkets.classList.remove('hidden');
            if (target === 'vs') { secVs.classList.remove('hidden'); loadVsPolls(); }
            if (target === 'portfolio') { secPortfolio.classList.remove('hidden'); loadPortfolio(); }
        }

        function navigateToHome() {
            history.pushState({}, '', '/');
            switchTab('markets');
        }

        function toggleUserDropdown() {
            document.getElementById('user-dropdown').classList.toggle('hidden');
        }

        window.onclick = function(e) {
            if (!e.target.closest('#user-badge') && !e.target.closest('#user-dropdown')) {
                document.getElementById('user-dropdown')?.classList.add('hidden');
            }
        };

        // KART İÇİ HIZLI OY KUTUSU (INLINE POPOVER)
        function toggleQuickVoteBox(marketId, outcome) {
            if (currentUserRole === 'GUEST') {
                showToast('Tahmin yapabilmek için lütfen giriş yapın.', 'error');
                promptLogin();
                return;
            }

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

        // 16 PAZAR KARTI
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
                                '<span class="text-slate-400">Havuz: ' + m.poolTotal + ' Puan</span><span class="text-emerald-400 font-bold">EVET: %' + m.probYes + '</span>' +
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

                        '<button type="button" data-action="open-detail" data-slug="' + m.slug + '" class="w-full py-2 bg-slate-950 hover:bg-slate-800 border border-slate-800 rounded-xl text-xs font-bold text-purple-300 hover:text-white transition flex justify-center items-center gap-1.5">' +
                            '<i class="fas fa-chart-line text-[10px]"></i> Terminal & Tahmin' +
                        '</button>' +
                    '</div>';

                container.appendChild(card);
            });
        }

        // FAZ 2: HERO DÜELLOLAR (2 KART KURALI & KÖR OYLAMA)
        async function loadHeroDuels() {
            var res = await fetch('/api/duels').then(function(r){ return r.json(); });
            heroDuels = res.duels || [];
            var container = document.getElementById('hero-duels-container');
            if (!container) return;
            container.innerHTML = '';

            // 2 KART KURALI: Yalnızca ilk 2 kart Hero vitrinine basılır!
            var mainDuels = heroDuels.slice(0, 2);

            mainDuels.forEach(function(d) {
                var card = document.createElement('div');
                card.className = 'card-bg border rounded-2xl p-4 shadow-xl flex flex-col justify-between';

                var actionArea = '';
                if (d.hasVoted) {
                    actionArea = 
                        '<div class="space-y-1.5 mt-3">' +
                            '<div class="w-full bg-slate-950 rounded-full h-2.5 overflow-hidden flex border border-slate-800">' +
                                '<div class="bg-indigo-500 h-full transition-all duration-700" style="width:' + d.pctA + '%"></div>' +
                                '<div class="bg-rose-500 h-full transition-all duration-700" style="width:' + d.pctB + '%"></div>' +
                            '</div>' +
                            '<div class="flex justify-between items-center text-[11px] font-black">' +
                                '<span class="' + (d.userChoice === 'A' ? 'text-indigo-400 font-bold' : 'text-slate-400') + '">' + escapeHtml(d.option_a_name) + ' %' + d.pctA + '</span>' +
                                '<span class="' + (d.userChoice === 'B' ? 'text-rose-400 font-bold' : 'text-slate-400') + '">' + escapeHtml(d.option_b_name) + ' %' + d.pctB + '</span>' +
                            '</div>' +
                        '</div>';
                } else {
                    // Kör Oylama: Oy kullanmadan önce oranlar gizlidir (? % vs ? %)
                    actionArea = 
                        '<div class="grid grid-cols-2 gap-2 mt-3 pt-2 border-t border-slate-800">' +
                            '<button type="button" data-action="duel-vote" data-id="' + d.id + '" data-choice="A" class="py-2.5 px-2 bg-slate-950 hover:bg-indigo-600 border border-slate-800 hover:border-indigo-500 rounded-xl text-[11px] font-bold text-slate-200 hover:text-white transition">' +
                                'Oy Ver: ' + escapeHtml(d.option_a_name) +
                            '</button>' +
                            '<button type="button" data-action="duel-vote" data-id="' + d.id + '" data-choice="B" class="py-2.5 px-2 bg-slate-950 hover:bg-rose-600 border border-slate-800 hover:border-rose-500 rounded-xl text-[11px] font-bold text-slate-200 hover:text-white transition">' +
                                'Oy Ver: ' + escapeHtml(d.option_b_name) +
                            '</button>' +
                        '</div>';
                }

                card.innerHTML = 
                    '<div>' +
                        '<div class="flex justify-between items-center mb-2">' +
                            '<span class="px-2 py-0.5 rounded text-[9px] font-black uppercase tracking-wider bg-indigo-500/20 text-indigo-300 border border-indigo-500/30">' + escapeHtml(d.category) + '</span>' +
                            '<span class="text-[10px] text-slate-400 font-semibold">' + (d.hasVoted ? d.totalVotes + ' Oy' : '<i class="fas fa-lock text-[9px] mr-1"></i>Oranlar Gizli') + '</span>' +
                        '</div>' +
                        '<h3 class="text-xs sm:text-sm font-extrabold text-white leading-snug mb-3">' + escapeHtml(d.title) + '</h3>' +
                        '<div class="flex items-center justify-between px-3 py-1.5 bg-slate-950/70 rounded-xl border border-slate-800/80 text-xs font-bold">' +
                            '<span>' + escapeHtml(d.option_a_name) + '</span>' +
                            '<span class="text-slate-500 text-[10px] italic">VS</span>' +
                            '<span>' + escapeHtml(d.option_b_name) + '</span>' +
                        '</div>' +
                    '</div>' + actionArea;

                container.appendChild(card);
            });
        }

        async function voteDuel(id, choice) {
            if (currentUserRole === 'GUEST') {
                showToast('Düelloda oy kullanmak için lütfen giriş yapın.', 'error');
                promptLogin();
                return;
            }

            try {
                var res = await fetch('/api/duels/' + id + '/vote', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ choice: choice })
                }).then(function(r){ return r.json(); });

                if (res.error) throw new Error(res.error);
                showToast('🎉 Oyunuz tescillendi ve kamuoyu oranı açıldı!');
                loadHeroDuels();
            } catch(e) {
                showToast(e.message, 'error');
            }
        }

        function openDuelArchive() {
            var list = document.getElementById('duel-archive-list');
            list.innerHTML = '';
            document.getElementById('duel-archive-modal').classList.remove('hidden');

            heroDuels.forEach(function(d) {
                var item = document.createElement('div');
                item.className = 'p-4 bg-slate-950 border border-slate-800 rounded-2xl space-y-2';
                item.innerHTML = 
                    '<div class="flex justify-between items-center text-[10px] text-slate-400">' +
                        '<span class="font-bold text-indigo-400">' + escapeHtml(d.category) + '</span>' +
                        '<span>' + d.totalVotes + ' Toplam Oy</span>' +
                    '</div>' +
                    '<h4 class="text-xs font-bold text-white">' + escapeHtml(d.title) + '</h4>' +
                    '<div class="flex justify-between items-center text-xs font-black pt-1">' +
                        '<span>' + escapeHtml(d.option_a_name) + ' (' + (d.hasVoted ? '%' + d.pctA : '?') + ')</span>' +
                        '<span>' + escapeHtml(d.option_b_name) + ' (' + (d.hasVoted ? '%' + d.pctB : '?') + ')</span>' +
                    '</div>';
                list.appendChild(item);
            });
        }

        // FAZ 2: EFSANELER MEYDANI RENDER & OYLAMA (CANLI VERİTABANINDAN ÇEKİLEN)
        async function loadVsPolls() {
            var res = await fetch('/api/vs').then(function(r){ return r.json(); });
            vsPolls = res.polls || [];
            var container = document.getElementById('vs-polls-list');
            if (!container) return;
            container.innerHTML = '';

            vsPolls.forEach(function(p) {
                var card = document.createElement('div');
                card.className = 'card-bg border rounded-2xl p-5 shadow-lg space-y-3';

                var actionHtml = '';
                if (p.hasVoted) {
                    actionHtml = 
                        '<div class="space-y-1.5 mt-3">' +
                            '<div class="w-full bg-slate-950 rounded-full h-2.5 overflow-hidden flex border border-slate-800">' +
                                '<div class="bg-amber-500 h-full transition-all duration-700" style="width:' + p.pctA + '%"></div>' +
                                '<div class="bg-indigo-500 h-full transition-all duration-700" style="width:' + p.pctB + '%"></div>' +
                            '</div>' +
                            '<div class="flex justify-between items-center text-xs font-black">' +
                                '<span class="' + (p.userChoice === 'A' ? 'text-amber-400' : 'text-slate-400') + '">' + escapeHtml(p.option_a) + ' %' + p.pctA + '</span>' +
                                '<span class="' + (p.userChoice === 'B' ? 'text-indigo-400' : 'text-slate-400') + '">' + escapeHtml(p.option_b) + ' %' + p.pctB + '</span>' +
                            '</div>' +
                        '</div>';
                } else {
                    // Kör Oylama
                    actionHtml = 
                        '<div class="grid grid-cols-2 gap-2 text-xs font-bold pt-2">' +
                            '<button data-action="vote-vs-card" data-id="' + p.id + '" data-choice="A" class="py-2.5 bg-slate-950 hover:bg-amber-600/30 border border-slate-800 hover:border-amber-500 rounded-xl text-slate-200 transition">' + escapeHtml(p.option_a) + '</button>' +
                            '<button data-action="vote-vs-card" data-id="' + p.id + '" data-choice="B" class="py-2.5 bg-slate-950 hover:bg-indigo-600/30 border border-slate-800 hover:border-indigo-500 rounded-xl text-slate-200 transition">' + escapeHtml(p.option_b) + '</button>' +
                        '</div>';
                }

                card.innerHTML = 
                    '<div class="flex justify-between items-center text-[10px] text-slate-400">' +
                        '<span class="px-2 py-0.5 rounded bg-amber-500/10 text-amber-300 border border-amber-500/20 font-bold uppercase">BÜYÜK KAPIŞMA</span>' +
                        '<span>' + (p.hasVoted ? p.totalVotes + ' Oy' : '<i class="fas fa-lock text-[9px] mr-1"></i>Oranlar Gizli') + '</span>' +
                    '</div>' +
                    '<h3 class="text-sm font-extrabold text-white leading-snug">' + escapeHtml(p.title) + '</h3>' + actionHtml;

                container.appendChild(card);
            });
        }

        async function voteVsPoll(id, choice) {
            if (currentUserRole === 'GUEST') {
                showToast('Efsaneler Meydanında oy kullanmak için lütfen giriş yapın.', 'error');
                promptLogin();
                return;
            }

            try {
                var res = await fetch('/api/vs/' + id + '/vote', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ choice: choice })
                }).then(function(r){ return r.json(); });

                if (res.error) throw new Error(res.error);
                showToast('🎉 Oyunuz tescillendi ve kamuoyu oranı açıldı!');
                loadVsPolls();
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
                fetchDetailQuote();
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

        async function fetchDetailQuote() {
            if (!activeDetailMarket) return;
            var amt = parseFloat(document.getElementById('dt-input-amount').value) || 0;
            if (amt <= 0) return;

            try {
                var res = await fetch('/api/trade/quote', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ marketId: activeDetailMarket.id, outcome: activeDetailChoice, amountKor: amt, action: 'BUY' })
                }).then(function(r){ return r.json(); });

                if (res.sharesOut) {
                    document.getElementById('dt-quote-shares').textContent = res.sharesOut + ' Pay';
                    document.getElementById('dt-quote-avg').textContent = res.avgPrice + ' Puan (Etki: %' + res.priceImpact + ')';
                    document.getElementById('dt-quote-payout').textContent = '+' + res.targetPayout;
                }
            } catch(e){}
        }

        document.getElementById('dt-input-amount').oninput = fetchDetailQuote;

        async function submitPredict() {
            if (currentUserRole === 'GUEST') {
                showToast('Tahmin yapabilmek için lütfen giriş yapın.', 'error');
                promptLogin();
                return;
            }

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
            var indContainer = document.getElementById('cohort-ind-list');
            ageContainer.innerHTML = '';
            indContainer.innerHTML = '';

            res.ageCohorts.forEach(function(c) {
                var row = document.createElement('div');
                row.className = 'flex justify-between p-2.5 bg-slate-950 rounded-xl border border-slate-800 font-bold text-xs';
                row.innerHTML = '<span>' + escapeHtml(c.age_group) + '</span><span class="' + (c.outcome === 'YES' ? 'text-emerald-400' : 'text-rose-400') + '">' + c.outcome + ' (' + c.vote_count + ')</span>';
                ageContainer.appendChild(row);
            });
            res.industryCohorts.forEach(function(c) {
                var row = document.createElement('div');
                row.className = 'flex justify-between p-2.5 bg-slate-950 rounded-xl border border-slate-800 font-bold text-xs';
                row.innerHTML = '<span>' + escapeHtml(c.industry || 'Belirtilmedi') + '</span><span class="' + (c.outcome === 'YES' ? 'text-emerald-400' : 'text-rose-400') + '">' + c.outcome + ' (' + c.vote_count + ')</span>';
                indContainer.appendChild(row);
            });
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
                title.textContent = 'B2B Güven Endeksi & Doğrulama Metodolojisi';
                content.innerHTML = 
                    '<div class="p-4 bg-emerald-950/20 border border-emerald-500/40 rounded-2xl space-y-2 mb-3">' +
                        '<div class="flex justify-between items-center">' +
                            '<span class="font-black text-emerald-400 text-sm">OYVER Doğrulama Güvencesi</span>' +
                            '<span class="text-base font-black text-emerald-300">Tescilli Konsensüs</span>' +
                        '</div>' +
                        '<p class="text-[11px] text-slate-300 leading-relaxed">Platform üzerindeki pazarlar yalnızca TCMB, TÜİK, SPK ve Resmi Gazete bültenleriyle tescillenir. Kurumunuz için kohort ve API veri akışı talep edebilirsiniz:</p>' +
                    '</div>' +
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
                    '<p>2. <strong>Uzman Analist:</strong> 10 ve üzeri sonuçlanmış pazarda isabet sağlayanlar.</p>' +
                    '<p>3. <strong>Doçent:</strong> FRS puanı 3000 puanı aşan kalibre öngörücüler.</p>' +
                    '<p>4. <strong>Profesör:</strong> Brier sapması düşük, uzun süreli istikrar yakalayanlar.</p>' +
                    '<p>5. <strong>Ordinaryüs:</strong> Türkiye genelinde ilk 10 sıradaki en isabetli beyinler.</p>';
            }
        }

        function promptLogin() {
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
            } else if (act === 'nav-vs') {
                switchTab('vs');
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
            } else if (act === 'open-duel-archive') {
                openDuelArchive();
            } else if (act === 'close-duel-archive') {
                document.getElementById('duel-archive-modal').classList.add('hidden');
            } else if (act === 'open-leaderboard') {
                openLeaderboard();
            } else if (act === 'close-leaderboard') {
                document.getElementById('leaderboard-modal').classList.add('hidden');
            } else if (act === 'open-drawer') {
                openDrawer(btn.getAttribute('data-drawer'));
            } else if (act === 'close-info-modal') {
                document.getElementById('info-modal').classList.add('hidden');
            } else if (act === 'prompt-login') {
                promptLogin();
            } else if (act === 'cat-filter') {
                document.querySelectorAll('.cat-btn').forEach(function(b){
                    b.className = 'cat-btn bg-slate-900 hover:bg-slate-800 text-slate-300 px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap flex items-center gap-1';
                });
                btn.className = 'cat-btn active bg-indigo-600 text-white px-4 py-1.5 rounded-xl text-xs font-bold transition whitespace-nowrap flex items-center gap-1';
                activeCategory = btn.getAttribute('data-cat');
                activeSubCategory = 'ALL';

                var subBox = document.getElementById('sub-cat-container');
                var pillsHtml = '';

                if (activeCategory === 'BORSA') {
                    pillsHtml = '<span class="text-[10px] text-slate-500 font-bold uppercase mr-1">Borsa:</span>' +
                        '<button data-action="subcat-filter" data-sub="ALL" class="sub-btn active px-2.5 py-1 rounded-lg bg-slate-800 text-white text-[11px] font-bold">Tümü</button>' +
                        '<button data-action="subcat-filter" data-sub="BIST 100" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">BIST 100</button>' +
                        '<button data-action="subcat-filter" data-sub="Halka Arz" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">Halka Arz</button>' +
                        '<button data-action="subcat-filter" data-sub="Sektör Endeksi" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">XBANK Banka</button>';
                } else if (activeCategory === 'FİNANS') {
                    pillsHtml = '<span class="text-[10px] text-slate-500 font-bold uppercase mr-1">Finans:</span>' +
                        '<button data-action="subcat-filter" data-sub="ALL" class="sub-btn active px-2.5 py-1 rounded-lg bg-slate-800 text-white text-[11px] font-bold">Tümü</button>' +
                        '<button data-action="subcat-filter" data-sub="Kıymetli Maden" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">Altın & Gümüş</button>' +
                        '<button data-action="subcat-filter" data-sub="Para Politikası" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">TCMB Faiz</button>';
                } else if (activeCategory === 'EKONOMİ') {
                    pillsHtml = '<span class="text-[10px] text-slate-500 font-bold uppercase mr-1">Ekonomi:</span>' +
                        '<button data-action="subcat-filter" data-sub="ALL" class="sub-btn active px-2.5 py-1 rounded-lg bg-slate-800 text-white text-[11px] font-bold">Tümü</button>' +
                        '<button data-action="subcat-filter" data-sub="Gelir & Ücret" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">Asgari Ücret</button>' +
                        '<button data-action="subcat-filter" data-sub="Fiyat İstikrarı" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">Enflasyon</button>' +
                        '<button data-action="subcat-filter" data-sub="Milli Gelir" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">Büyüme (GSYH)</button>';
                } else if (activeCategory === 'TEKNOLOJİ') {
                    pillsHtml = '<span class="text-[10px] text-slate-500 font-bold uppercase mr-1">Teknoloji:</span>' +
                        '<button data-action="subcat-filter" data-sub="ALL" class="sub-btn active px-2.5 py-1 rounded-lg bg-slate-800 text-white text-[11px] font-bold">Tümü</button>' +
                        '<button data-action="subcat-filter" data-sub="Regülasyon" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">Yapay Zeka Yasası</button>' +
                        '<button data-action="subcat-filter" data-sub="Uzay & Uydu" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">TÜRKSAT 6A</button>' +
                        '<button data-action="subcat-filter" data-sub="Girişimcilik" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">Unicorn Girişim</button>';
                } else if (activeCategory === 'DİZİ & MEDYA') {
                    pillsHtml = '<span class="text-[10px] text-slate-500 font-bold uppercase mr-1">Medya:</span>' +
                        '<button data-action="subcat-filter" data-sub="ALL" class="sub-btn active px-2.5 py-1 rounded-lg bg-slate-800 text-white text-[11px] font-bold">Tümü</button>' +
                        '<button data-action="subcat-filter" data-sub="Televizyon" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">Dizi Reyting</button>' +
                        '<button data-action="subcat-filter" data-sub="Sinema" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">Gişe Rekoru</button>';
                } else if (activeCategory === 'OTOMOTİV') {
                    pillsHtml = '<span class="text-[10px] text-slate-500 font-bold uppercase mr-1">Mobilite:</span>' +
                        '<button data-action="subcat-filter" data-sub="ALL" class="sub-btn active px-2.5 py-1 rounded-lg bg-slate-800 text-white text-[11px] font-bold">Tümü</button>' +
                        '<button data-action="subcat-filter" data-sub="Elektrikli Araç" class="sub-btn px-2.5 py-1 rounded-lg bg-slate-900 text-slate-400 hover:text-white text-[11px] font-bold">TOGG Sedan</button>';
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
            } else if (act === 'vote-vs-card') {
                voteVsPoll(btn.getAttribute('data-id'), btn.getAttribute('data-choice'));
            } else if (act === 'dt-switch-tab') {
                switchDetailTab(btn.getAttribute('data-tab'));
            } else if (act === 'dt-set-choice') {
                activeDetailChoice = btn.getAttribute('data-choice');
                updateDetailChoiceBtns();
                fetchDetailQuote();
            } else if (act === 'dt-predict-submit') {
                submitPredict();
            } else if (act === 'submit-comment') {
                submitComment();
            } else if (act === 'download-cohort-csv') {
                window.location.href = '/api/markets/' + activeDetailMarket.slug + '/export';
            } else if (act === 'sell-position') {
                sellPosition(btn.getAttribute('data-id'), btn.getAttribute('data-outcome'), btn.getAttribute('data-shares'));
            } else if (act === 'submit-b2b-form') {
                var comp = document.getElementById('b2b-company').value;
                var em = document.getElementById('b2b-email').value;
                var msg = document.getElementById('b2b-msg').value;
                if (!comp || !em) return showToast('Lütfen alanları doldurun', 'error');
                fetch('/api/contact', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ type: 'B2B', name: comp, email: em, message: msg })
                }).then(function(){
                    showToast('Talebiniz kaydedildi.');
                    document.getElementById('info-modal').classList.add('hidden');
                });
            } else if (act === 'submit-contact-form') {
                var n = document.getElementById('cnt-name').value;
                var eMail = document.getElementById('cnt-email').value;
                var m = document.getElementById('cnt-msg').value;
                if (!n || !eMail) return showToast('Lütfen alanları doldurun', 'error');
                fetch('/api/contact', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ type: 'GENERAL', name: n, email: eMail, message: m })
                }).then(function(){
                    showToast('Mesajınız iletildi.');
                    document.getElementById('info-modal').classList.add('hidden');
                });
            }
        });

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
                    if (msg.type === 'MARKET_RESOLVED') {
                        showToast('📢 Bir pazar sonuçlandı: ' + msg.marketTitle);
                        loadPortfolio();
                    }
                } catch(e){}
            };
            ws.onclose = function() { setTimeout(connectWebSocket, 2500); };
        }

        // BAŞLATICI
        async function init() {
            try {
                var me = await fetch('/api/me').then(function(r){ return r.json(); });
                currentBalance = Math.round(parseFloat(me.balance_kor || 0));
                currentUserRole = me.role || 'GUEST';
                document.getElementById('user-balance').textContent = currentBalance.toLocaleString('tr-TR') + ' KOR';
                document.getElementById('user-streak').textContent = (me.streak || 0) + 'g';
                document.getElementById('dd-username').textContent = me.username || 'Misafir';
                document.getElementById('dd-tier').textContent = 'Kademesi: ' + (me.tier || 'Gözlemci');
                document.getElementById('dd-archetype').textContent = me.personality_archetype || '-';

                await loadHeroDuels();
                var res = await fetch('/api/markets').then(function(r){ return r.json(); });
                markets = res.markets || [];
                renderMarkets();

                var path = window.location.pathname;
                if (path.startsWith('/market/')) {
                    var slug = path.split('/')[2];
                    openMarketDetail(slug);
                }
            } catch(e) {
                console.error('[OYVER INIT ENGINE ERROR]', e);
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
    console.log(`[OYVER PRO] v1.2-PHASE2 Aktif: ${address}`);
});
