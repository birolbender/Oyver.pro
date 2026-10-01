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
// 2. CITARDAUQ AMM MATEMATİK MOTORU (LOCKED)
// ==========================================
class AMMEngine {
    static calculateBuy(state, mGross, feeRate) {
        const fee = MoneyMath.roundUp(mGross.mul(feeRate), 6);
        const mNet = mGross.minus(fee);
        if (mNet.lte(0)) throw new Error('INVALID_AMOUNT: Net yatirim 0 veya negatif olamaz');

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

        // Citardauq kararlı kökü: 2C / (B + sqrt(B^2 - 4C))
        const sqrtDisc = discriminant.sqrt();
        const grossPayout = C.mul(2).div(B.plus(sqrtDisc));

        if (grossPayout.gte(N)) throw new Error('SOLVENCY_VIOLATION: Brüt odeme NO rezervini asamaz');

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
// 3. VERİTABANI BAĞLANTISI VE OTOMATİK MİGRASYON
// ==========================================
pg.types.setTypeParser(1700, (val) => val);
const pool = new pg.Pool({
    connectionString: process.env.DATABASE_URL || 'postgresql://postgres:oyver_secure_password@localhost:5432/oyver_core',
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
                balance_kor NUMERIC(24,6) NOT NULL DEFAULT 0.000000 CHECK (balance_kor >= 0),
                status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS markets (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                slug VARCHAR(255) UNIQUE NOT NULL,
                question TEXT NOT NULL,
                description TEXT,
                status VARCHAR(32) NOT NULL DEFAULT 'TRADING',
                resolution VARCHAR(16),
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                closed_at TIMESTAMPTZ,
                resolved_at TIMESTAMPTZ
            );

            CREATE TABLE IF NOT EXISTS amm_state (
                market_id UUID PRIMARY KEY REFERENCES markets(id) ON DELETE CASCADE,
                yes_reserve NUMERIC(30,12) NOT NULL CHECK (yes_reserve > 0),
                no_reserve NUMERIC(30,12) NOT NULL CHECK (no_reserve > 0),
                yes_supply NUMERIC(30,12) NOT NULL DEFAULT 0,
                no_supply NUMERIC(30,12) NOT NULL DEFAULT 0,
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

            CREATE TABLE IF NOT EXISTS settlements (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                market_id UUID NOT NULL REFERENCES markets(id) ON DELETE CASCADE,
                user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                amount NUMERIC(24,6) NOT NULL CHECK (amount >= 0),
                outcome VARCHAR(16) NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT uq_settlement_market_user UNIQUE (market_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS idempotency_keys (
                user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                endpoint VARCHAR(128) NOT NULL,
                key VARCHAR(64) NOT NULL,
                request_hash CHAR(64) NOT NULL,
                response_status INT,
                response_body JSONB,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (user_id, endpoint, key)
            );

            CREATE TABLE IF NOT EXISTS sessions (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                token_hash CHAR(64) UNIQUE NOT NULL,
                expires_at TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS market_activities (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                market_id UUID NOT NULL REFERENCES markets(id) ON DELETE CASCADE,
                user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                action_type VARCHAR(16) NOT NULL,
                outcome VARCHAR(8),
                amount_kor NUMERIC(24,6) NOT NULL DEFAULT 0,
                shares NUMERIC(30,12) NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);

        // Başlangıç Pazar Tohumlaması (Genesis Seed)
        const checkMarket = await client.query(`SELECT id FROM markets WHERE slug = 'bist-100-2026'`);
        if (checkMarket.rows.length === 0) {
            console.info('[GENESIS] İlk pazar ve sistem hesapları açılıyor...');
            for (const code of ['1000', '3000', '4000', '5000']) {
                await client.query(`INSERT INTO accounts (code) VALUES ($1)`, [code]);
            }
            const mRes = await client.query(`
                INSERT INTO markets (slug, question, description) 
                VALUES ('bist-100-2026', 'BIST 100 Endeksi 2026 Yilinda 15.000 Puanini Asar mi?', 'Borsa Istanbul resmi kapanisi esas alinir.') 
                RETURNING id
            `);
            const mId = mRes.rows[0].id;
            await client.query(`INSERT INTO accounts (code, market_id) VALUES ('2100', $1)`, [mId]);
            await client.query(`INSERT INTO amm_state (market_id, yes_reserve, no_reserve) VALUES ($1, 10000, 10000)`, [mId]);
        }
        console.info('[DATABASE] Veritabanı ve finansal kilitler hazır.');
    } finally {
        client.release();
    }
}

// ==========================================
// 4. ÇİFT TARAFLI DEFTER MOTORU (LEDGER)
// ==========================================
class LedgerEngine {
    static async recordEntry(client, entryType, refId, idempKey, lines) {
        let totalD = new Decimal(0), totalC = new Decimal(0);
        for (const l of lines) { totalD = totalD.plus(l.debit); totalC = totalC.plus(l.credit); }
        if (!totalD.eq(totalC) || totalD.lte(0)) {
            throw new Error(`LEDGER_UNBALANCED: Borç (${totalD}) ve Alacak (${totalC}) eşit olmalıdır`);
        }

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
// 5. TİCARET VE TASFİYE SERVİSLERİ
// ==========================================
class TradeService {
    static async executeBuy(client, { userId, marketId, outcome, amountGross, minSharesOut, feeRate, idempotencyKey }) {
        const mr = await client.query(
            `SELECT m.id, m.status, a.yes_reserve, a.no_reserve, a.yes_supply, a.no_supply 
             FROM markets m JOIN amm_state a ON m.id = a.market_id 
             WHERE m.id = $1 FOR UPDATE`,
            [marketId]
        );
        if (mr.rows.length === 0 || mr.rows[0].status !== 'TRADING') throw new Error('MARKET_NOT_TRADING');

        const ur = await client.query(
            `UPDATE users SET balance_kor = balance_kor - $1 WHERE id = $2 AND balance_kor >= $1 RETURNING balance_kor`,
            [amountGross.toFixed(6), userId]
        );
        if (ur.rows.length === 0) throw new Error('INSUFFICIENT_BALANCE: Bakiye yetersiz');

        const isYes = outcome === 'YES';
        const amm = {
            yesReserve: new Decimal(mr.rows[0].yes_reserve),
            noReserve: new Decimal(mr.rows[0].no_reserve),
            yesSupply: new Decimal(mr.rows[0].yes_supply),
            noSupply: new Decimal(mr.rows[0].no_supply)
        };
        const calc = AMMEngine.calculateBuy(
            isYes ? amm : { yesReserve: amm.noReserve, noReserve: amm.yesReserve, yesSupply: amm.noSupply, noSupply: amm.yesSupply },
            amountGross,
            feeRate
        );

        if (calc.sharesOut.lt(minSharesOut)) throw new Error('SLIPPAGE_EXCEEDED');

        const ny = isYes ? calc.newYesReserve : calc.newNoReserve;
        const nn = isYes ? calc.newNoReserve : calc.newYesReserve;
        await client.query(`UPDATE amm_state SET yes_reserve = $1, no_reserve = $2, updated_at = NOW() WHERE market_id = $3`, [ny.toFixed(12), nn.toFixed(12), marketId]);

        await LedgerEngine.recordEntry(client, 'TRADE_BUY', marketId, idempotencyKey, [
            { accountCode: '2000', userId, debit: amountGross, credit: new Decimal(0) },
            { accountCode: '2100', marketId, debit: new Decimal(0), credit: calc.mNet },
            { accountCode: '4000', debit: new Decimal(0), credit: calc.fee }
        ]);

        const pr = await client.query(`SELECT shares, avg_price, realized_pnl FROM positions WHERE market_id = $1 AND user_id = $2 AND outcome = $3 FOR UPDATE`, [marketId, userId, outcome]);
        const curShares = pr.rows.length > 0 ? new Decimal(pr.rows[0].shares) : new Decimal(0);
        const curPrice = pr.rows.length > 0 ? new Decimal(pr.rows[0].avg_price) : new Decimal(0);
        const newShares = curShares.plus(calc.sharesOut);
        const newAvg = curShares.mul(curPrice).plus(amountGross).div(newShares);

        await client.query(
            `INSERT INTO positions (market_id, user_id, outcome, shares, avg_price, realized_pnl)
             VALUES ($1, $2, $3, $4, $5, 0)
             ON CONFLICT (user_id, market_id, outcome) DO UPDATE SET shares = $4, avg_price = $5, updated_at = NOW()`,
            [marketId, userId, outcome, newShares.toFixed(12), newAvg.toFixed(18)]
        );

        await client.query(
            `INSERT INTO market_activities (market_id, user_id, action_type, outcome, amount_kor, shares) VALUES ($1, $2, 'BUY', $3, $4, $5)`,
            [marketId, userId, outcome, amountGross.toFixed(6), calc.sharesOut.toFixed(12)]
        );

        return { sharesOut: calc.sharesOut, feePaid: calc.fee, userBalance: new Decimal(ur.rows[0].balance_kor) };
    }

    static async executeSell(client, { userId, marketId, outcome, sharesToSell, minPayoutKor, feeRate, idempotencyKey }) {
        const mr = await client.query(
            `SELECT m.id, m.status, a.yes_reserve, a.no_reserve, a.yes_supply, a.no_supply 
             FROM markets m JOIN amm_state a ON m.id = a.market_id 
             WHERE m.id = $1 FOR UPDATE`,
            [marketId]
        );
        if (mr.rows.length === 0 || mr.rows[0].status !== 'TRADING') throw new Error('MARKET_NOT_TRADING');

        const pr = await client.query(`SELECT shares, avg_price, realized_pnl FROM positions WHERE market_id = $1 AND user_id = $2 AND outcome = $3 FOR UPDATE`, [marketId, userId, outcome]);
        if (pr.rows.length === 0 || new Decimal(pr.rows[0].shares).lt(sharesToSell)) throw new Error('INSUFFICIENT_SHARES');

        const isYes = outcome === 'YES';
        const amm = {
            yesReserve: new Decimal(mr.rows[0].yes_reserve),
            noReserve: new Decimal(mr.rows[0].no_reserve),
            yesSupply: new Decimal(mr.rows[0].yes_supply),
            noSupply: new Decimal(mr.rows[0].no_supply)
        };
        const calc = AMMEngine.calculateSell(
            isYes ? amm : { yesReserve: amm.noReserve, noReserve: amm.yesReserve, yesSupply: amm.noSupply, noSupply: amm.yesSupply },
            sharesToSell,
            feeRate
        );

        if (calc.netPayout.lt(minPayoutKor)) throw new Error('SLIPPAGE_EXCEEDED');

        const ny = isYes ? calc.newYesReserve : calc.newNoReserve;
        const nn = isYes ? calc.newNoReserve : calc.newYesReserve;
        await client.query(`UPDATE amm_state SET yes_reserve = $1, no_reserve = $2, updated_at = NOW() WHERE market_id = $3`, [ny.toFixed(12), nn.toFixed(12), marketId]);

        await LedgerEngine.recordEntry(client, 'TRADE_SELL', marketId, idempotencyKey, [
            { accountCode: '2100', marketId, debit: calc.grossPayout, credit: new Decimal(0) },
            { accountCode: '2000', userId, debit: new Decimal(0), credit: calc.netPayout },
            { accountCode: '4000', debit: new Decimal(0), credit: calc.fee }
        ]);

        const ur = await client.query(`UPDATE users SET balance_kor = balance_kor + $1 WHERE id = $2 RETURNING balance_kor`, [calc.netPayout.toFixed(6), userId]);
        const remShares = new Decimal(pr.rows[0].shares).minus(sharesToSell);
        await client.query(`UPDATE positions SET shares = $1, updated_at = NOW() WHERE market_id = $2 AND user_id = $3 AND outcome = $4`, [remShares.toFixed(12), marketId, userId, outcome]);

        await client.query(
            `INSERT INTO market_activities (market_id, user_id, action_type, outcome, amount_kor, shares) VALUES ($1, $2, 'SELL', $3, $4, $5)`,
            [marketId, userId, outcome, calc.netPayout.toFixed(6), sharesToSell.toFixed(12)]
        );

        return { netPayout: calc.netPayout, feePaid: calc.fee, userBalance: new Decimal(ur.rows[0].balance_kor) };
    }
}

class SettlementService {
    static async settleUserPosition(client, marketId, userId, idempKey) {
        const mr = await client.query(`SELECT status, resolution FROM markets WHERE id = $1 FOR UPDATE`, [marketId]);
        if (mr.rows.length === 0 || (mr.rows[0].status !== 'RESOLVED' && mr.rows[0].status !== 'VOID')) {
            throw new Error('MARKET_NOT_RESOLVED: Piyasa henüz sonuçlanmadı');
        }

        const pr = await client.query(`SELECT id, outcome, shares FROM positions WHERE market_id = $1 AND user_id = $2 AND status = 'OPEN' FOR UPDATE`, [marketId, userId]);
        if (pr.rows.length === 0) return { settled: false, payout: new Decimal(0) };

        const shares = new Decimal(pr.rows[0].shares);
        let payout = new Decimal(0);
        if (mr.rows[0].status === 'RESOLVED' && pr.rows[0].outcome === mr.rows[0].resolution) {
            payout = MoneyMath.roundDown(shares, 6);
        } else if (mr.rows[0].status === 'VOID') {
            payout = MoneyMath.roundDown(shares.mul(0.5), 6);
        }

        const sr = await client.query(
            `INSERT INTO settlements (market_id, user_id, amount, outcome) VALUES ($1, $2, $3, $4) ON CONFLICT (market_id, user_id) DO NOTHING RETURNING id`,
            [marketId, userId, payout.toFixed(6), mr.rows[0].resolution]
        );
        if (sr.rows.length === 0) return { settled: false, payout: new Decimal(0) };

        if (payout.gt(0)) {
            await LedgerEngine.recordEntry(client, 'SETTLEMENT', marketId, `SETTLE:${marketId}:${userId}`, [
                { accountCode: '2100', marketId, debit: payout, credit: new Decimal(0) },
                { accountCode: '2000', userId, debit: new Decimal(0), credit: payout }
            ]);
            await client.query(`UPDATE users SET balance_kor = balance_kor + $1 WHERE id = $2`, [payout.toFixed(6), userId]);
        }
        await client.query(`UPDATE positions SET status = 'SETTLED', updated_at = NOW() WHERE id = $1`, [pr.rows[0].id]);
        return { settled: true, payout };
    }
}

// ==========================================
// 6. GÜVENLİK VE OTURUM MOTORU
// ==========================================
class SessionService {
    static async createSession(client, userId) {
        const rawToken = crypto.randomBytes(32).toString('hex');
        const secret = process.env.SESSION_SECRET || 'oyver_gizli_anahtar_2026';
        const hash = crypto.createHmac('sha256', secret).update(rawToken).digest('hex');
        const exp = new Date(Date.now() + 7 * 24 * 3600 * 1000);
        await client.query(`INSERT INTO sessions (user_id, token_hash, expires_at) VALUES ($1, $2, $3)`, [userId, hash, exp]);
        return rawToken;
    }
    static async validateSession(client, token) {
        const secret = process.env.SESSION_SECRET || 'oyver_gizli_anahtar_2026';
        const hash = crypto.createHmac('sha256', secret).update(token).digest('hex');
        const r = await client.query(`SELECT user_id FROM sessions WHERE token_hash = $1 AND expires_at > NOW()`, [hash]);
        return r.rows.length > 0 ? r.rows[0].user_id : null;
    }
}

// ==========================================
// 7. WEBSOCKET VE ETKİLEŞİM
// ==========================================
const wsClients = new Set();
function broadcast(channel, data) {
    const msg = JSON.stringify({ channel, data });
    for (const ws of wsClients) {
        if (ws.readyState === WebSocket.OPEN) ws.send(msg);
    }
}

// ==========================================
// 8. SERVER VE REST ROTLARI
// ==========================================
const app = fastify({ logger: false });
await app.register(fastifyWebsocket);

// Kimlik Denetim Kancası
app.addHook('onRequest', async (req, reply) => {
    let token;
    const cookie = req.headers.cookie;
    if (cookie) {
        const m = cookie.split(';').find(c => c.trim().startsWith('__Host-oyver_session='));
        if (m) token = m.split('=')[1];
    }
    if (!token && req.headers.authorization?.startsWith('Bearer ')) token = req.headers.authorization.split(' ')[1];
    if (token) {
        const client = await pool.connect();
        try {
            req.userId = await SessionService.validateSession(client, token);
        } finally { client.release(); }
    }
});

// WebSocket Rotası
app.get('/ws', { websocket: true }, (connection) => {
    const ws = connection.socket;
    wsClients.add(ws);
    ws.on('close', () => wsClients.delete(ws));
});

// REST API
app.get('/health', async () => ({ status: 'UP', timestamp: new Date().toISOString() }));

app.post('/auth/register', async (req, rep) => {
    const { email, username, password } = req.body || {};
    if (!email || !username || !password) return rep.status(400).send({ error: 'Eksik alan' });
    const salt = crypto.randomBytes(16).toString('hex');
    const pass = crypto.scryptSync(password, salt, 64).toString('hex') + ':' + salt;

    const res = await runInTransaction(async (c) => {
        const ur = await c.query(`INSERT INTO users (email, username, password_hash, balance_kor) VALUES ($1, $2, $3, 1000) RETURNING id, username, balance_kor`, [email, username, pass]);
        const u = ur.rows[0];
        await c.query(`INSERT INTO accounts (code, owner_user_id) VALUES ('2000', $1)`, [u.id]);
        await LedgerEngine.recordEntry(c, 'REGISTER_BONUS', u.id, `BONUS_${u.id}`, [
            { accountCode: '5000', debit: new Decimal(1000), credit: new Decimal(0) },
            { accountCode: '2000', userId: u.id, debit: new Decimal(0), credit: new Decimal(1000) }
        ]);
        const token = await SessionService.createSession(c, u.id);
        return { u, token };
    });

    rep.header('Set-Cookie', `__Host-oyver_session=${res.token}; Path=/; Secure; HttpOnly; SameSite=Strict; Max-Age=604800`);
    return rep.status(201).send({ user: res.u });
});

app.post('/auth/login', async (req, rep) => {
    const { email, password } = req.body || {};
    const ur = await pool.query(`SELECT id, username, password_hash, balance_kor FROM users WHERE email = $1`, [email]);
    if (ur.rows.length === 0) return rep.status(401).send({ error: 'Kullanici bulunamadi' });
    const [saved, salt] = ur.rows[0].password_hash.split(':');
    if (crypto.scryptSync(password, salt, 64).toString('hex') !== saved) return rep.status(401).send({ error: 'Hatali sifre' });

    const token = await runInTransaction(c => SessionService.createSession(c, ur.rows[0].id));
    rep.header('Set-Cookie', `__Host-oyver_session=${token}; Path=/; Secure; HttpOnly; SameSite=Strict; Max-Age=604800`);
    return rep.send({ user: { id: ur.rows[0].id, username: ur.rows[0].username, balanceKor: ur.rows[0].balance_kor } });
});

app.post('/auth/logout', async (req, rep) => {
    rep.header('Set-Cookie', '__Host-oyver_session=; Path=/; Secure; HttpOnly; SameSite=Strict; Max-Age=0');
    return rep.send({ success: true });
});

app.get('/wallet', async (req, rep) => {
    if (!req.userId) return rep.status(401).send({ error: 'Yetkisiz istek' });
    const r = await pool.query(`SELECT balance_kor, username FROM users WHERE id = $1`, [req.userId]);
    return rep.send({ balanceKor: r.rows[0].balance_kor, username: r.rows[0].username });
});

app.get('/markets', async () => {
    const r = await pool.query(`SELECT m.*, a.yes_reserve, a.no_reserve FROM markets m JOIN amm_state a ON m.id = a.market_id ORDER BY m.created_at DESC`);
    return { markets: r.rows };
});

app.get('/markets/:id/positions', async (req, rep) => {
    if (!req.userId) return rep.send({ positions: [] });
    const r = await pool.query(`SELECT outcome, shares, avg_price FROM positions WHERE market_id = $1 AND user_id = $2`, [req.params.id, req.userId]);
    return rep.send({ positions: r.rows });
});

app.post('/markets/:id/buy', async (req, rep) => {
    if (!req.userId) return rep.status(401).send({ error: 'Giris yapiniz' });
    const { outcome, amountGross } = req.body || {};
    const key = req.headers['x-idempotency-key'] || 'idemp_' + Date.now();

    try {
        const res = await runInTransaction(c => TradeService.executeBuy(c, {
            userId: req.userId,
            marketId: req.params.id,
            outcome,
            amountGross: new Decimal(amountGross),
            minSharesOut: new Decimal(0),
            feeRate: new Decimal(0.02),
            idempotencyKey: key
        }));
        broadcast(`market:${req.params.id}`, { type: 'TRADE_BUY' });
        return rep.send({ success: true, sharesOut: res.sharesOut.toFixed(6), balanceKor: res.userBalance.toFixed(6) });
    } catch (e) {
        return rep.status(400).send({ error: e.message });
    }
});

app.post('/markets/:id/sell', async (req, rep) => {
    if (!req.userId) return rep.status(401).send({ error: 'Giris yapiniz' });
    const { outcome, sharesToSell } = req.body || {};
    const key = req.headers['x-idempotency-key'] || 'idemp_' + Date.now();

    try {
        const res = await runInTransaction(c => TradeService.executeSell(c, {
            userId: req.userId,
            marketId: req.params.id,
            outcome,
            sharesToSell: new Decimal(sharesToSell),
            minPayoutKor: new Decimal(0),
            feeRate: new Decimal(0.02),
            idempotencyKey: key
        }));
        broadcast(`market:${req.params.id}`, { type: 'TRADE_SELL' });
        return rep.send({ success: true, netPayout: res.netPayout.toFixed(6), balanceKor: res.userBalance.toFixed(6) });
    } catch (e) {
        return rep.status(400).send({ error: e.message });
    }
});

app.post('/markets/:id/settle', async (req, rep) => {
    if (!req.userId) return rep.status(401).send({ error: 'Giris yapiniz' });
    try {
        const res = await runInTransaction(c => SettlementService.settleUserPosition(c, req.params.id, req.userId, 'SETTLE_' + Date.now()));
        return rep.send({ success: true, ...res, payout: res.payout.toFixed(6) });
    } catch (e) {
        return rep.status(400).send({ error: e.message });
    }
});

// ==========================================
// 9. GÜVENLİ VE ENTEGRE ÖN YÜZ (HTML/DOM)
// ==========================================
app.get('/', async (req, reply) => {
    return reply.type('text/html').send(`<!DOCTYPE html>
<html lang="tr">
<head>
    <meta charset="UTF-8"><title>OYVER Tahmin Pazarı</title>
    <style>
        body { background: #0f172a; color: #f8fafc; font-family: sans-serif; margin: 0; }
        .header { display: flex; justify-content: space-between; padding: 1rem 2rem; background: #1e293b; align-items: center; }
        .logo { font-size: 1.5rem; font-weight: bold; color: #3b82f6; }
        .container { display: grid; grid-template-columns: 1fr 400px; gap: 2rem; padding: 2rem; max-width: 1400px; margin: auto; }
        .market-card { background: #1e293b; padding: 1rem; border-radius: 8px; cursor: pointer; border: 1px solid #334155; margin-bottom: 1rem; }
        .market-card.selected { border-color: #3b82f6; }
        .console { background: #1e293b; padding: 1.5rem; border-radius: 8px; border: 1px solid #334155; }
        .metrics { display: flex; gap: 1rem; margin: 1rem 0; }
        .metric { background: #0f172a; padding: 0.75rem; border-radius: 6px; flex: 1; text-align: center; }
        .green { color: #10b981; font-weight: bold; } .red { color: #ef4444; font-weight: bold; }
        .btn { padding: 0.6rem 1rem; border-radius: 6px; border: none; cursor: pointer; font-weight: bold; }
        .btn-primary { background: #3b82f6; color: white; width: 100%; margin-top: 0.5rem; }
        .btn-danger { background: #ef4444; color: white; width: 100%; margin-top: 0.5rem; }
        .btn-success { background: #10b981; color: white; width: 100%; margin-top: 0.5rem; }
        .btn-secondary { background: #334155; color: white; }
        input { width: 100%; padding: 0.6rem; background: #0f172a; border: 1px solid #334155; border-radius: 6px; color: white; margin-top: 0.4rem; box-sizing: border-box; }
        .hidden { display: none !important; }
        .tabs { display: flex; gap: 0.5rem; margin-bottom: 1rem; }
        .tab-btn { flex: 1; padding: 0.5rem; background: #0f172a; border: 1px solid #334155; color: white; border-radius: 4px; cursor: pointer; }
        .tab-btn.active { border-color: #3b82f6; background: #1e293b; }
        .outcomes { display: flex; gap: 0.5rem; margin: 0.5rem 0; }
        .btn-out { flex: 1; padding: 0.5rem; background: transparent; border: 1px solid #334155; color: white; border-radius: 4px; cursor: pointer; }
        .btn-out.active[data-o="YES"] { background: rgba(16, 185, 129, 0.2); border-color: #10b981; color: #10b981; }
        .btn-out.active[data-o="NO"] { background: rgba(239, 68, 68, 0.2); border-color: #ef4444; color: #ef4444; }
        .toast { position: fixed; bottom: 1rem; right: 1rem; background: #1e293b; border-left: 4px solid #10b981; padding: 0.75rem 1.5rem; border-radius: 4px; }
    </style>
</head>
<body>
    <div class="header">
        <div class="logo">OYVER</div>
        <div id="auth-box"><button class="btn btn-secondary" id="btn-login-modal">Giriş Yap / Kayıt</button></div>
    </div>
    <div class="container">
        <div>
            <h2>Aktif Pazarlar</h2>
            <div id="markets"></div>
        </div>
        <div class="console">
            <div id="no-market">İşlem yapmak için bir pazar seçin.</div>
            <div id="market-view" class="hidden">
                <h3 id="m-question"></h3>
                <div class="metrics">
                    <div class="metric"><label>YES</label><div id="m-yes" class="green">--%</div></div>
                    <div class="metric"><label>NO</label><div id="m-no" class="red">--%</div></div>
                </div>
                <div class="tabs">
                    <button class="tab-btn active" id="tab-buy">Al (BUY)</button>
                    <button class="tab-btn" id="tab-sell">Sat (SELL)</button>
                    <button class="tab-btn" id="tab-settle">Tasfiye</button>
                </div>
                <div id="box-buy">
                    <div class="outcomes">
                        <button class="btn-out active" id="b-yes" data-o="YES">YES</button>
                        <button class="btn-out" id="b-no" data-o="NO">NO</button>
                    </div>
                    <input type="number" id="in-buy" placeholder="Tutar (KOR)" value="50">
                    <button class="btn btn-primary" id="btn-buy">Alım Emri Gönder</button>
                </div>
                <div id="box-sell" class="hidden">
                    <div class="outcomes">
                        <button class="btn-out active" id="s-yes" data-o="YES">YES</button>
                        <button class="btn-out" id="s-no" data-o="NO">NO</button>
                    </div>
                    <input type="number" id="in-sell" placeholder="Pay Adedi">
                    <button class="btn btn-danger" id="btn-sell">Payları Sat</button>
                </div>
                <div id="box-settle" class="hidden">
                    <p style="font-size:0.85rem; color:#94a3b8;">Sonuçlanan pazarlardaki hak edişinizi çekin.</p>
                    <button class="btn btn-success" id="btn-settle">Tasfiyeyi Gerçekleştir</button>
                </div>
                <div id="positions" style="margin-top:1rem; padding-top:1rem; border-top:1px solid #334155; font-size:0.85rem;"></div>
            </div>
        </div>
    </div>
    <div id="toast" class="toast hidden"></div>
    <script>
        let curMkt = null, buyOut = 'YES', sellOut = 'YES', user = null;
        const toast = (msg) => { const t = document.getElementById('toast'); t.textContent = msg; t.classList.remove('hidden'); setTimeout(() => t.classList.add('hidden'), 3500); };
        const req = async (url, opt = {}) => { const r = await fetch(url, { headers: { 'Content-Type': 'application/json' }, credentials: 'include', ...opt }); const d = await r.json().catch(() => ({})); if (!r.ok) throw new Error(d.error || 'İşlem hatası'); return d; };

        async function init() {
            try { const w = await req('/wallet'); user = w; renderAuth(); } catch { user = null; renderAuth(); }
            loadMarkets();
        }
        function renderAuth() {
            const b = document.getElementById('auth-box');
            if (user) { b.textContent = user.username + ' | ' + user.balanceKor + ' KOR'; }
            else { b.innerHTML = '<button class="btn btn-secondary" onclick="loginPrompt()">Giriş Yap / Kayıt</button>'; }
        }
        window.loginPrompt = async () => {
            const email = prompt('E-Posta:'); const password = prompt('Şifre:');
            if (!email || !password) return;
            try {
                const res = await req('/auth/login', { method: 'POST', body: JSON.stringify({ email, password }) });
                user = { username: res.user.username, balanceKor: res.user.balanceKor }; renderAuth(); toast('Giriş başarılı');
            } catch {
                const res = await req('/auth/register', { method: 'POST', body: JSON.stringify({ email, username: email.split('@')[0], password }) });
                user = { username: res.user.username, balanceKor: res.user.balanceKor }; renderAuth(); toast('Kayıt oluşturuldu (+1000 KOR Bonus)');
            }
        };
        async function loadMarkets() {
            const d = await req('/markets'); const el = document.getElementById('markets'); el.innerHTML = '';
            for (const m of d.markets) {
                const c = document.createElement('div'); c.className = 'market-card' + (curMkt?.id === m.id ? ' selected' : '');
                c.textContent = m.question; c.onclick = () => selectMarket(m); el.appendChild(c);
            }
        }
        function selectMarket(m) {
            curMkt = m; document.getElementById('no-market').classList.add('hidden'); document.getElementById('market-view').classList.remove('hidden');
            document.getElementById('m-question').textContent = m.question;
            const y = parseFloat(m.yes_reserve), n = parseFloat(m.no_reserve);
            document.getElementById('m-yes').textContent = '%' + ((n / (y + n)) * 100).toFixed(1);
            document.getElementById('m-no').textContent = '%' + ((y / (y + n)) * 100).toFixed(1);
            loadPositions();
        }
        async function loadPositions() {
            if (!user || !curMkt) return;
            const p = await req('/markets/' + curMkt.id + '/positions');
            const el = document.getElementById('positions'); el.innerHTML = '<strong>Pozisyonlarınız:</strong><br>';
            if (p.positions.length === 0) el.innerHTML += 'Açık payınız yok.';
            for (const pos of p.positions) el.innerHTML += pos.outcome + ': ' + parseFloat(pos.shares).toFixed(2) + ' Pay<br>';
        }
        document.getElementById('tab-buy').onclick = () => switchTab('BUY');
        document.getElementById('tab-sell').onclick = () => switchTab('SELL');
        document.getElementById('tab-settle').onclick = () => switchTab('SETTLE');
        function switchTab(t) {
            document.getElementById('tab-buy').classList.toggle('active', t === 'BUY');
            document.getElementById('tab-sell').classList.toggle('active', t === 'SELL');
            document.getElementById('tab-settle').classList.toggle('active', t === 'SETTLE');
            document.getElementById('box-buy').classList.toggle('hidden', t !== 'BUY');
            document.getElementById('box-sell').classList.toggle('hidden', t !== 'SELL');
            document.getElementById('box-settle').classList.toggle('hidden', t !== 'SETTLE');
        }
        document.getElementById('b-yes').onclick = () => { buyOut = 'YES'; document.getElementById('b-yes').classList.add('active'); document.getElementById('b-no').classList.remove('active'); };
        document.getElementById('b-no').onclick = () => { buyOut = 'NO'; document.getElementById('b-no').classList.add('active'); document.getElementById('b-yes').classList.remove('active'); };
        document.getElementById('s-yes').onclick = () => { sellOut = 'YES'; document.getElementById('s-yes').classList.add('active'); document.getElementById('s-no').classList.remove('active'); };
        document.getElementById('s-no').onclick = () => { sellOut = 'NO'; document.getElementById('s-no').classList.add('active'); document.getElementById('s-yes').classList.remove('active'); };

        document.getElementById('btn-buy').onclick = async () => {
            if (!user) return loginPrompt();
            const val = document.getElementById('in-buy').value;
            try {
                const r = await req('/markets/' + curMkt.id + '/buy', { method: 'POST', body: JSON.stringify({ outcome: buyOut, amountGross: val }) });
                toast('Alım Başarılı: ' + r.sharesOut + ' pay'); user.balanceKor = r.balanceKor; renderAuth(); loadPositions(); loadMarkets();
            } catch(e) { toast(e.message); }
        };
        document.getElementById('btn-sell').onclick = async () => {
            if (!user) return loginPrompt();
            const val = document.getElementById('in-sell').value;
            try {
                const r = await req('/markets/' + curMkt.id + '/sell', { method: 'POST', body: JSON.stringify({ outcome: sellOut, sharesToSell: val }) });
                toast('Satış Başarılı: +' + r.netPayout + ' KOR'); user.balanceKor = r.balanceKor; renderAuth(); loadPositions(); loadMarkets();
            } catch(e) { toast(e.message); }
        };
        document.getElementById('btn-settle').onclick = async () => {
            if (!user) return loginPrompt();
            try {
                const r = await req('/markets/' + curMkt.id + '/settle', { method: 'POST' });
                toast(r.settled ? 'Tasfiye Edildi: +' + r.payout + ' KOR' : 'Tasfiye edilecek pay yok');
                const w = await req('/wallet'); user = w; renderAuth(); loadPositions();
            } catch(e) { toast(e.message); }
        };
        init();
    </script>
</body>
</html>`);
});

// ==========================================
// 10. BAŞLATICI
// ==========================================
await initDatabase();
const port = Number(process.env.PORT) || 3000;
app.listen({ port, host: '0.0.0.0' }, (err, address) => {
    if (err) { console.error(err); process.exit(1); }
    console.log(`[OYVER] Sistem ayakta: ${address}`);
});
