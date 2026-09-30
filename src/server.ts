import Fastify from 'fastify';
import cors from '@fastify/cors';
import helmet from '@fastify/helmet';
import WebSocket, { WebSocketServer } from 'ws';
import crypto from 'crypto';
import { db } from './db';
import { markets, users, positions, dailyAnswers } from './db/schema';
import { eq, and, sql } from 'drizzle-orm';
import { AmmEngine } from './modules/amm/engine';
import { SYSTEM_CONFIG } from './config/system';

const app = Fastify({ logger: false });

async function bootstrap() {
  await app.register(helmet, { contentSecurityPolicy: false });
  await app.register(cors, { origin: '*' });

  // ============================================================================
  // 1. VERİTABANI VE ŞEMA OLUŞTURUCU (MIGRATION ENGINE)
  // ============================================================================
  async function initDatabaseTables() {
    try {
      await db.execute(sql`
        -- Eski hatalı tabloları arındır
        ALTER TABLE IF EXISTS users DROP COLUMN IF EXISTS tckn;
        ALTER TABLE IF EXISTS users DROP COLUMN IF EXISTS iban;
        ALTER TABLE IF EXISTS users DROP COLUMN IF EXISTS legal_first_name;
        ALTER TABLE IF EXISTS users DROP COLUMN IF EXISTS legal_last_name;
        ALTER TABLE IF EXISTS users DROP COLUMN IF EXISTS birth_year;
        ALTER TABLE IF EXISTS users DROP COLUMN IF EXISTS kyc_status;
        ALTER TABLE IF EXISTS users DROP COLUMN IF EXISTS balance_vera_withdrawable;
        ALTER TABLE IF EXISTS users DROP COLUMN IF EXISTS balance_vera_promo;
        ALTER TABLE IF EXISTS vera_transactions DROP COLUMN IF EXISTS tax_withheld_vera;

        CREATE TABLE IF NOT EXISTS users (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          session_token VARCHAR(64) NOT NULL UNIQUE,
          username VARCHAR(32) NOT NULL UNIQUE,
          phone_number VARCHAR(20),
          is_phone_verified BOOLEAN NOT NULL DEFAULT false,
          balance_kor NUMERIC(18, 4) NOT NULL DEFAULT 1000.0000,
          rating_edge INTEGER NOT NULL DEFAULT 500,
          current_streak INTEGER NOT NULL DEFAULT 1,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS markets (
          id SERIAL PRIMARY KEY,
          slug VARCHAR(128) NOT NULL UNIQUE,
          title VARCHAR(255) NOT NULL,
          category VARCHAR(64) NOT NULL,
          rules TEXT NOT NULL,
          source_name VARCHAR(128) NOT NULL,
          source_url VARCHAR(512),
          starts_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          closes_at TIMESTAMPTZ NOT NULL,
          resolved_at TIMESTAMPTZ,
          status VARCHAR(32) NOT NULL DEFAULT 'TRADING',
          winning_outcome VARCHAR(8),
          pool_yes NUMERIC(18, 4) NOT NULL DEFAULT 10000.0000,
          pool_no NUMERIC(18, 4) NOT NULL DEFAULT 10000.0000,
          volume_kor NUMERIC(18, 4) NOT NULL DEFAULT 0.0000,
          total_predictions_count INTEGER NOT NULL DEFAULT 0,
          is_hero BOOLEAN NOT NULL DEFAULT false,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS positions (
          id BIGSERIAL PRIMARY KEY,
          user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          market_id INTEGER NOT NULL REFERENCES markets(id) ON DELETE CASCADE,
          outcome VARCHAR(8) NOT NULL,
          shares_count NUMERIC(18, 4) NOT NULL,
          total_cost_kor NUMERIC(18, 4) NOT NULL,
          is_settled BOOLEAN NOT NULL DEFAULT false,
          is_closed_early BOOLEAN NOT NULL DEFAULT false,
          cashout_return_kor NUMERIC(18, 4),
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS daily_answers (
          id BIGSERIAL PRIMARY KEY,
          user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          question_id INTEGER NOT NULL,
          answer_date VARCHAR(10) NOT NULL,
          outcome VARCHAR(8) NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          CONSTRAINT user_daily_q_unq UNIQUE (user_id, question_id, answer_date)
        );
      `);
      console.log('✓ PostgreSQL tabloları ve KOR şeması doğrulandı.');
    } catch (err) {
      console.error('Veritabanı başlatma hatası:', err);
    }
  }

  // ============================================================================
  // 2. KULLANICI OTURUM İZOLASYONU (HER CİHAZA ÖZEL MÜSTAKİL CÜZDAN)
  // ============================================================================
  async function resolveUserFromRequest(req: any) {
    const token = req.headers['x-session-token'] || req.cookies?.['oyver_session'];
    
    if (token) {
      const user = await db.query.users.findFirst({
        where: eq(users.sessionToken, String(token))
      });
      if (user) return user;
    }

    // Yeni Tekil Oturum Oluştur
    const newToken = crypto.randomBytes(24).toString('hex');
    const randomSuffix = Math.floor(1000 + Math.random() * 9000);
    const username = `Analist_${randomSuffix}`;

    const [newUser] = await db.insert(users).values({
      sessionToken: newToken,
      username: username,
      balanceKor: '1000.0000',
      ratingEdge: 500,
      currentStreak: 1
    }).returning();

    return newUser;
  }

  // ============================================================================
  // 3. TOHUM PAZARLAR
  // ============================================================================
  async function seedMarketsIfEmpty() {
    try {
      const countRes = await db.execute(sql`SELECT count(*)::int as count FROM markets`);
      const count = (countRes[0] as any)?.count || 0;

      if (count === 0) {
        const now = new Date();
        await db.insert(markets).values([
          {
            slug: 'tcmb-faiz-karari-ekim',
            title: 'TCMB Para Politikası Kurulu, Ekim Toplantısında Politika Faizini İndirecek mi?',
            category: 'Ekonomi',
            rules: 'TCMB PPK basın duyurusunda politika faizinde indirim açıklandığı an EVET sayılır.',
            sourceName: 'TCMB Resmî Basın Bülteni',
            startsAt: now,
            closesAt: new Date(now.getTime() + 18 * 24 * 60 * 60 * 1000),
            poolYes: '11500.0000',
            poolNo: '18500.0000',
            volumeKor: '30000.0000',
            totalPredictionsCount: 42,
            isHero: true
          },
          {
            slug: 'super-lig-derbi-gol-baraji',
            title: 'Hafta Sonu Oynanacak Süper Lig Derbisinde 2.5 Gol Barajı Aşılır mı?',
            category: 'Spor',
            rules: 'Müsabakanın normal süresi ve uzatmalarda toplam gol sayısı en az 3 ise EVET sayılır.',
            sourceName: 'TFF Resmî Hakem Raporu',
            startsAt: now,
            closesAt: new Date(now.getTime() + 3 * 24 * 60 * 60 * 1000 + 4 * 60 * 60 * 1000),
            poolYes: '14200.0000',
            poolNo: '10800.0000',
            volumeKor: '25000.0000',
            totalPredictionsCount: 31,
            isHero: true
          },
          {
            slug: 'bist100-yil-sonu-kapanis',
            title: 'BIST 100 Endeksi 2026 Yılını 12.000 Puan Üzerinde Kapatır mı?',
            category: 'Borsa',
            rules: '2026 yılının son işlem gününde BIST 100 seans kapanış değeri 12.000,01 ve üzeri ise EVET sayılır.',
            sourceName: 'Borsa İstanbul Resmî Verileri',
            startsAt: now,
            closesAt: new Date(now.getTime() + 90 * 24 * 60 * 60 * 1000),
            poolYes: '12000.0000',
            poolNo: '13000.0000',
            volumeKor: '22500.0000',
            totalPredictionsCount: 29,
            isHero: false
          },
          {
            slug: 'togg-yeni-model-teslimat',
            title: 'TOGG Yeni Sedan Modelinin İlk Müşteri Teslimatları 2026 İçinde Yapılır mı?',
            category: 'Teknoloji',
            rules: 'Yıl sonuna kadar nihai tescilli müşteri teslimatı yapıldığı resmen duyurulursa EVET sonuçlanır.',
            sourceName: 'TOGG Resmî Duyurusu',
            startsAt: now,
            closesAt: new Date(now.getTime() + 75 * 24 * 60 * 60 * 1000),
            poolYes: '12500.0000',
            poolNo: '12500.0000',
            volumeKor: '18500.0000',
            totalPredictionsCount: 19,
            isHero: false
          }
        ]);
        console.log('✓ Tohum pazarlar eklendi.');
      }
    } catch (e) {
      console.error('Tohum pazar yükleme hatası:', e);
    }
  }

  // ============================================================================
  // 4. REST API UÇLARI
  // ============================================================================

  // Durum Sorgulama
  app.get('/api/state', async (req) => {
    const user = await resolveUserFromRequest(req);
    const marketList = await db.query.markets.findMany();
    const userPositions = await db.query.positions.findMany({
      where: and(eq(positions.userId, user.id), eq(positions.isClosedEarly, false))
    });

    const enrichedMarkets = marketList.map(m => {
      const prices = AmmEngine.getSpotPrices(Number(m.poolYes), Number(m.poolNo));
      return {
        ...m,
        probYes: prices.probYes,
        probNo: prices.probNo,
        priceYesKor: prices.priceYesVera,
        priceNoKor: prices.priceNoVera
      };
    });

    // Erken satış / Nakde dönüş teklifi (Cash-Out)
    const enrichedPositions = userPositions.map(pos => {
      const m = marketList.find(x => x.id === pos.marketId);
      let currentCashoutValue = 0;
      if (m && m.status === 'TRADING') {
        const sellQuote = AmmEngine.calculateSell(
          Number(m.poolYes),
          Number(m.poolNo),
          pos.outcome as any,
          Number(pos.sharesCount)
        );
        currentCashoutValue = sellQuote.veraReturned;
      }
      const cost = Number(pos.totalCostKor);
      const pnlKor = Math.round((currentCashoutValue - cost) * 100) / 100;
      const pnlPercent = cost > 0 ? Math.round(((currentCashoutValue - cost) / cost) * 100) : 0;

      return {
        ...pos,
        currentCashoutValue,
        pnlKor,
        pnlPercent,
        marketTitle: m?.title || 'Pazar'
      };
    });

    // Günün 3 Sorusu Kontrolü
    const todayStr = new Date().toISOString().slice(0, 10);
    const userDailyAnswers = await db.query.dailyAnswers.findMany({
      where: and(eq(dailyAnswers.userId, user.id), eq(dailyAnswers.answerDate, todayStr))
    });

    const dailyQuestions = [
      { id: 101, title: 'T.C. Hazine bütçe açığı yıl sonu hedefi altında kalır mı?', category: 'Ekonomi', closeText: '31 Aralık', probYes: 68, probNo: 32 },
      { id: 102, title: 'EuroLeague temsilcimiz bu haftaki maçını kazanır mı?', category: 'Spor', closeText: 'Yarın 21:00', probYes: 54, probNo: 46 },
      { id: 103, title: 'Yeni Yapay Zekâ Mevzuat Taslağı bu ay Meclis Komisyonuna gelir mi?', category: 'Teknoloji', closeText: '25 Ekim', probYes: 61, probNo: 39 }
    ].map(q => {
      const ans = userDailyAnswers.find(a => a.questionId === q.id);
      return { ...q, userAnswer: ans ? ans.outcome : null };
    });

    return {
      success: true,
      currency: SYSTEM_CONFIG.CURRENCY_SYMBOL,
      currentUser: {
        id: user.id,
        sessionToken: user.sessionToken,
        username: user.username,
        isPhoneVerified: user.isPhoneVerified,
        balanceKor: Number(user.balanceKor),
        ratingEdge: user.ratingEdge,
        currentStreak: user.currentStreak,
        positions: enrichedPositions
      },
      categories: ['Hepsi', 'Ekonomi', 'Borsa', 'Spor', 'Teknoloji', 'Siyaset'],
      dailyQuestions,
      dailyCompletedCount: userDailyAnswers.length,
      markets: enrichedMarkets
    };
  });

  // ACID Korumalı Tahmin Alımı (Satır Kilidi ile Yarış Durumu Engeli)
  app.post('/api/trade/buy', async (req: any, reply) => {
    const { marketId, outcome, amountKor } = req.body;
    const amount = Number(amountKor);

    if (!marketId || !amount || amount <= 0) {
      return reply.status(400).send({ success: false, message: 'Geçersiz işlem tutarı.' });
    }

    try {
      const result = await db.transaction(async (tx) => {
        const user = await resolveUserFromRequest(req);

        // 1. Kullanıcı Bakiye Kilidi (Pessimistic Row Lock)
        const [lockedUser] = await tx.execute(
          sql`SELECT * FROM users WHERE id = ${user.id} FOR UPDATE`
        );
        if (Number(lockedUser.balance_kor) < amount) {
          throw new Error('Yetersiz KOR bakiyesi.');
        }

        // 2. Pazar ve Havuz Kilidi
        const [lockedMarket] = await tx.execute(
          sql`SELECT * FROM markets WHERE id = ${Number(marketId)} AND status = 'TRADING' FOR UPDATE`
        );
        if (!lockedMarket || new Date() >= new Date(lockedMarket.closes_at)) {
          throw new Error('Bu pazar işlemlere kapanmıştır.');
        }

        // 3. %1.0 Protokol İşlem Harcı Yakımı (Burn)
        const feeBurn = amount * SYSTEM_CONFIG.TRANSACTION_FEE_BURN_RATE;
        const netTradeAmount = amount - feeBurn;

        // 4. AMM Hesaplaması
        const quote = AmmEngine.calculateBuy(
          Number(lockedMarket.pool_yes),
          Number(lockedMarket.pool_no),
          outcome,
          netTradeAmount
        );

        const newBalance = Number(lockedUser.balance_kor) - amount;

        // 5. Veritabanı Mutasyonları
        await tx.update(users).set({
          balanceKor: newBalance.toFixed(4)
        }).where(eq(users.id, user.id));

        await tx.update(markets).set({
          poolYes: quote.nextPoolYes.toFixed(4),
          poolNo: quote.nextPoolNo.toFixed(4),
          volumeKor: (Number(lockedMarket.volume_kor) + amount).toFixed(4),
          totalPredictionsCount: Number(lockedMarket.total_predictions_count) + 1
        }).where(eq(markets.id, Number(marketId)));

        await tx.insert(positions).values({
          userId: user.id,
          marketId: Number(marketId),
          outcome,
          sharesCount: quote.sharesReceived.toFixed(4),
          totalCostKor: amount.toFixed(4)
        });

        return { quote, newBalance };
      });

      broadcast({ type: 'MARKET_UPDATED', marketId });
      return { success: true, quote: result.quote, newBalance: result.newBalance };
    } catch (err: any) {
      return reply.status(400).send({ success: false, message: err.message });
    }
  });

  // ACID Korumalı Erken Satış (Cash-Out)
  app.post('/api/trade/cashout', async (req: any, reply) => {
    const { positionId } = req.body;

    try {
      const result = await db.transaction(async (tx) => {
        const user = await resolveUserFromRequest(req);

        // 1. Pozisyon Kilidi
        const [lockedPos] = await tx.execute(
          sql`SELECT * FROM positions WHERE id = ${Number(positionId)} AND user_id = ${user.id} AND is_closed_early = false FOR UPDATE`
        );
        if (!lockedPos) throw new Error('Açık pozisyon bulunamadı.');

        // 2. Pazar Kilidi
        const [lockedMarket] = await tx.execute(
          sql`SELECT * FROM markets WHERE id = ${lockedPos.market_id} AND status = 'TRADING' FOR UPDATE`
        );
        if (!lockedMarket) throw new Error('Kapalı pazarlarda erken satış yapılamaz.');

        const sellQuote = AmmEngine.calculateSell(
          Number(lockedMarket.pool_yes),
          Number(lockedMarket.pool_no),
          lockedPos.outcome as any,
          Number(lockedPos.shares_count)
        );

        // %1 Harç Kesintisi
        const fee = sellQuote.veraReturned * SYSTEM_CONFIG.TRANSACTION_FEE_BURN_RATE;
        const returnKor = Math.round((sellQuote.veraReturned - fee) * 100) / 100;

        const [lockedUser] = await tx.execute(
          sql`SELECT * FROM users WHERE id = ${user.id} FOR UPDATE`
        );
        const newBalance = Number(lockedUser.balance_kor) + returnKor;

        await tx.update(users).set({ balanceKor: newBalance.toFixed(4) }).where(eq(users.id, user.id));
        await tx.update(positions).set({
          isClosedEarly: true,
          cashoutReturnKor: returnKor.toFixed(4),
          updatedAt: new Date()
        }).where(eq(positions.id, Number(positionId)));

        await tx.update(markets).set({
          poolYes: sellQuote.nextPoolYes.toFixed(4),
          poolNo: sellQuote.nextPoolNo.toFixed(4)
        }).where(eq(markets.id, lockedMarket.id));

        return { returnKor, newBalance };
      });

      broadcast({ type: 'MARKET_UPDATED' });
      return { success: true, message: `${result.returnKor} KOR iade edildi.`, newBalance: result.newBalance };
    } catch (err: any) {
      return reply.status(400).send({ success: false, message: err.message });
    }
  });

  // Günün 3 Sorusu Cevaplama (A1 Açığı Kapatıldı: Sadece 3'ü bitince +50 KOR)
  app.post('/api/daily/answer', async (req: any, reply) => {
    const { questionId, outcome } = req.body;
    const user = await resolveUserFromRequest(req);
    const todayStr = new Date().toISOString().slice(0, 10);

    try {
      await db.insert(dailyAnswers).values({
        userId: user.id,
        questionId: Number(questionId),
        answerDate: todayStr,
        outcome: String(outcome)
      });
    } catch (e) {
      return reply.status(400).send({ success: false, message: 'Bu soruyu bugün zaten yanıtladınız.' });
    }

    const currentCount = await db.query.dailyAnswers.findMany({
      where: and(eq(dailyAnswers.userId, user.id), eq(dailyAnswers.answerDate, todayStr))
    });

    let bonusGiven = false;
    let newBalance = Number(user.balanceKor);

    // 3'ü de ilk kez tamamlandıysa +50 KOR teşviki
    if (currentCount.length === 3) {
      newBalance += 50;
      await db.update(users).set({
        balanceKor: newBalance.toFixed(4),
        currentStreak: user.currentStreak + 1
      }).where(eq(users.id, user.id));
      bonusGiven = true;
    }

    return {
      success: true,
      message: bonusGiven ? 'Tebrikler! Günün 3 sorusunu tamamladınız: +50 KOR yüklendi.' : 'Tahmin kaydedildi.',
      newBalance,
      completedCount: currentCount.length
    };
  });

  // ============================================================================
  // 5. KULLANICI ARAYÜZÜ (YALIN ÜST MENÜ & ZENGİN ALT ÇEKMECE)
  // ============================================================================
  app.get('/', async (_req, reply) => {
    reply.type('text/html; charset=utf-8');
    return `
<!DOCTYPE html>
<html lang="tr">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>OYVER — Kolektif Bilgi ve Tahmin Pazarı</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800;900&display=swap');
    body { background-color: #07090E; color: #F1F5F9; font-family: 'Plus Jakarta Sans', sans-serif; }
    .tabular { font-variant-numeric: tabular-nums; }
    .glass-card { background: rgba(15, 23, 42, 0.65); backdrop-filter: blur(12px); border: 1px solid rgba(255, 255, 255, 0.07); }
    .no-scrollbar::-webkit-scrollbar { display: none; }
  </style>
</head>
<body class="min-h-screen flex flex-col justify-between text-slate-100">

  <!-- TOAST BİLDİRİM -->
  <div id="toast" class="fixed top-5 right-5 z-50 transform -translate-y-28 opacity-0 transition-all duration-300 bg-slate-900 border border-indigo-500/40 text-white text-xs px-4 py-3 rounded-2xl shadow-2xl flex items-center gap-2 backdrop-blur-md">
    <span id="toast-msg">Bildirim</span>
  </div>

  <!-- ÜST HEADER (YALIN & KARMAŞADAN UZAK) -->
  <header class="sticky top-0 z-40 bg-[#07090E]/90 backdrop-blur-md border-b border-white/5 px-4 lg:px-8 py-3">
    <div class="max-w-6xl mx-auto flex items-center justify-between">
      
      <!-- LOGO -->
      <div class="flex items-center gap-2.5 cursor-pointer" onclick="window.scrollTo({top:0, behavior:'smooth'})">
        <div class="w-8 h-8 rounded-xl bg-gradient-to-tr from-indigo-600 to-violet-500 flex items-center justify-center font-black text-white text-base shadow-lg shadow-indigo-500/25">O</div>
        <span class="font-extrabold text-lg text-white tracking-tight">OYVER</span>
        <span class="text-[9px] bg-indigo-500/10 text-indigo-400 border border-indigo-500/20 px-1.5 py-0.5 rounded-md font-bold uppercase">Terminal</span>
      </div>

      <!-- SAĞDA YALIN PROFİL & ALTINDA KÜÇÜK BAKİYE -->
      <div onclick="openPortfolioDrawer()" class="cursor-pointer flex items-center gap-2.5 p-1.5 pr-3 bg-slate-900/60 hover:bg-slate-800 border border-white/5 rounded-2xl transition">
        <div class="w-7 h-7 rounded-xl bg-indigo-600/20 border border-indigo-500/30 flex items-center justify-center text-xs">👤</div>
        <div class="flex flex-col text-right">
          <span id="header-username" class="text-xs font-bold text-slate-200">Analist</span>
          <span class="text-[10px] text-slate-400 font-semibold tabular">
            <strong id="header-balance" class="text-emerald-400">1.000</strong> KOR
          </span>
        </div>
      </div>

    </div>
  </header>

  <!-- KATEGORİ ŞERİDİ -->
  <div class="max-w-6xl mx-auto px-4 pt-4 w-full">
    <div id="category-bar" class="flex gap-2 overflow-x-auto no-scrollbar pb-1 text-xs"></div>
  </div>

  <!-- ANA AKIŞ -->
  <main class="max-w-6xl mx-auto px-4 py-4 space-y-6 w-full flex-1">

    <!-- GÜNÜN 3 HIZLI SORUSU (+50 KOR) -->
    <div class="glass-card rounded-3xl p-5 space-y-3 relative overflow-hidden">
      <div class="flex justify-between items-center text-xs">
        <div class="flex items-center gap-2">
          <span class="w-2 h-2 rounded-full bg-amber-400 animate-ping"></span>
          <span class="font-extrabold text-slate-100 uppercase text-[11px]">Günün 3 Hızlı Sorusu</span>
          <span class="text-[10px] text-amber-400 bg-amber-400/10 border border-amber-400/20 px-2 py-0.5 rounded-full font-bold">+50 KOR</span>
        </div>
        <span id="daily-counter-text" class="text-[11px] text-slate-400">0/3 Tamamlandı</span>
      </div>
      <div id="daily-questions-list" class="space-y-2.5"></div>
    </div>

    <!-- GÜNÜN MANŞET İKİLİSİ (CANLI GERİ SAYIM SAYACIYLA) -->
    <div class="space-y-3">
      <h2 class="text-xs font-bold text-slate-400 uppercase tracking-wider flex items-center gap-1.5 text-[11px]">
        <span>⭐</span> Günün Manşet İkilisi
      </h2>
      <div id="hero-markets-grid" class="grid grid-cols-1 md:grid-cols-2 gap-4"></div>
    </div>

    <!-- EN ÇOK TAHMİN EDİLENLER -->
    <div class="space-y-3">
      <div class="flex justify-between items-center text-xs">
        <h2 class="font-bold text-slate-400 uppercase tracking-wider flex items-center gap-1.5 text-[11px]">
          <span>🔥</span> En Çok Tahmin Edilenler
        </h2>
        <span class="text-slate-500 text-[11px]">1 Kazanan Pay = 1.00 KOR</span>
      </div>
      <div id="top-depth-board" class="glass-card rounded-3xl overflow-hidden divide-y divide-white/5"></div>
    </div>

  </main>

  <!-- İŞLEM MODALI (SLIPPAGE VE TAHMİN MASASI) -->
  <div id="modal-backdrop" onclick="closeModal()" class="fixed inset-0 bg-black/80 backdrop-blur-md z-50 hidden"></div>
  <div id="trade-modal" class="fixed bottom-0 sm:bottom-auto sm:top-1/2 sm:left-1/2 sm:-translate-x-1/2 sm:-translate-y-1/2 w-full sm:max-w-md bg-slate-900 border-t sm:border border-white/10 rounded-t-3xl sm:rounded-3xl p-6 z-50 hidden space-y-4 shadow-2xl">
    <div id="modal-content"></div>
  </div>

  <!-- PORTFÖY & CÜZDAN ÇEKMECESİ (SAĞDAN YA DA ALT ÇEKMECE OLARAK AÇILIR) -->
  <div id="portfolio-drawer" class="fixed inset-y-0 right-0 w-full sm:max-w-md bg-slate-900 border-l border-white/10 p-6 z-50 hidden flex-col justify-between overflow-y-auto">
    <div class="space-y-5">
      <div class="flex justify-between items-center border-b border-white/5 pb-3">
        <div>
          <h3 id="prof-drawer-user" class="text-base font-black text-white">Analist</h3>
          <span class="text-[10px] text-slate-400">Kapalı Devre Sanal Cüzdan</span>
        </div>
        <button onclick="closePortfolioDrawer()" class="text-slate-400 hover:text-white text-lg">✕</button>
      </div>

      <div class="p-4 bg-slate-950 rounded-2xl border border-white/5 space-y-1">
        <div class="text-xs text-slate-400">Toplam Bakiye</div>
        <div id="prof-drawer-balance" class="text-2xl font-black text-emerald-400 tabular">1.000 KOR</div>
        <div class="text-[10px] text-slate-500">Maddi/nakit değeri yoktur. İtibar göstergesidir.</div>
      </div>

      <div class="space-y-3">
        <h4 class="text-xs font-bold text-slate-400 uppercase tracking-wider">Açık Pozisyonlar & Erken Satış</h4>
        <div id="drawer-positions" class="space-y-2.5"></div>
      </div>
    </div>
  </div>

  <!-- ALT KURUMSAL ALAN & ÇEKMECE KONTROLÜ -->
  <footer class="max-w-6xl mx-auto px-4 py-8 w-full border-t border-white/5 space-y-4 text-xs text-slate-400">
    <div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
      <button onclick="openCorporateSection('about')" class="p-3 bg-slate-900/60 hover:bg-slate-800 rounded-2xl border border-white/5 text-left transition">
        <div class="font-bold text-slate-200">Biz Kimiz?</div>
        <div class="text-[11px] text-slate-500 mt-0.5">Kolektif zekâ vizyonumuz</div>
      </button>

      <button onclick="openCorporateSection('rules')" class="p-3 bg-slate-900/60 hover:bg-slate-800 rounded-2xl border border-white/5 text-left transition">
        <div class="font-bold text-slate-200">Çözümleme İlkeleri</div>
        <div class="text-[11px] text-slate-500 mt-0.5">Resmî birincil kaynaklar</div>
      </button>

      <button onclick="openCorporateSection('partnership')" class="p-3 bg-slate-900/60 hover:bg-slate-800 rounded-2xl border border-white/5 text-left transition">
        <div class="font-bold text-slate-200">İş Ortaklıkları</div>
        <div class="text-[11px] text-slate-500 mt-0.5">B2B Tahmin & Araştırma</div>
      </button>

      <button onclick="openCorporateSection('contact')" class="p-3 bg-slate-900/60 hover:bg-slate-800 rounded-2xl border border-white/5 text-left transition">
        <div class="font-bold text-slate-200">Bize Ulaşın</div>
        <div class="text-[11px] text-slate-500 mt-0.5">İletişim & Destek</div>
      </button>
    </div>

    <!-- KURUMSAL ÇEKMECE İÇERİK KUTUSU -->
    <div id="corporate-box" class="hidden p-5 glass-card rounded-3xl space-y-3 transition-all">
      <div class="flex justify-between items-center">
        <h4 id="corp-box-title" class="font-black text-white text-sm">Kurumsal</h4>
        <button onclick="document.getElementById('corporate-box').classList.add('hidden')" class="text-slate-400">✕</button>
      </div>
      <p id="corp-box-desc" class="text-xs text-slate-300 leading-relaxed"></p>
    </div>

    <div class="p-3.5 bg-slate-950/80 rounded-2xl border border-white/5 text-[11px] text-slate-500 leading-relaxed">
      <strong>⚠️ Yasal Bilgilendirme [Taslak]:</strong> OYVER üzerindeki KOR puanları, kapalı devre bir tahmin ve analitik itibar göstergesidir. Platformumuzda gerçek para ile bahis oynatılmaz; 7258 ve 6362 sayılı mevzuat sınırları gözetilmektedir.
    </div>
  </footer>

  <!-- İSTEMCİ SCRIPTLERİ -->
  <script>
    let appData = null;
    let selectedMarket = null;
    let selectedOutcome = 'YES';
    let currentCategory = 'Hepsi';

    function showToast(msg) {
      const toast = document.getElementById('toast');
      document.getElementById('toast-msg').innerText = msg;
      toast.classList.remove('-translate-y-28', 'opacity-0');
      setTimeout(() => toast.classList.add('-translate-y-28', 'opacity-0'), 3000);
    }

    async function loadData() {
      // LocalStorage'daki Token'ı Header olarak ilet
      const token = localStorage.getItem('oyver_session') || '';
      const res = await fetch('/api/state', {
        headers: { 'x-session-token': token }
      });
      appData = await res.json();
      
      if (appData.currentUser?.sessionToken) {
        localStorage.setItem('oyver_session', appData.currentUser.sessionToken);
      }
      renderUI();
    }

    function renderUI() {
      document.getElementById('header-username').innerText = appData.currentUser.username;
      document.getElementById('header-balance').innerText = Math.round(appData.currentUser.balanceKor).toLocaleString();
      document.getElementById('prof-drawer-user').innerText = appData.currentUser.username;
      document.getElementById('prof-drawer-balance').innerText = Math.round(appData.currentUser.balanceKor).toLocaleString() + ' KOR';
      document.getElementById('daily-counter-text').innerText = appData.dailyCompletedCount + '/3 Tamamlandı';

      renderCategories();
      renderDaily();
      renderHero();
      renderTopDepth();
      renderDrawerPositions();
    }

    function renderCategories() {
      const bar = document.getElementById('category-bar');
      bar.innerHTML = appData.categories.map(c => \`
        <button onclick="filterCat('\${c}')" class="px-3.5 py-1.5 rounded-xl font-bold whitespace-nowrap transition text-xs \${
          currentCategory === c 
            ? 'bg-indigo-600 text-white shadow-lg shadow-indigo-600/30' 
            : 'bg-slate-900/80 border border-white/5 text-slate-400 hover:text-white'
        }">\${c}</button>
      \`).join('');
    }

    function filterCat(c) {
      currentCategory = c;
      renderCategories();
      renderHero();
      renderTopDepth();
    }

    function renderDaily() {
      const container = document.getElementById('daily-questions-list');
      container.innerHTML = appData.dailyQuestions.map(q => \`
        <div class="bg-slate-950/70 p-3.5 rounded-2xl border border-white/5 flex flex-col sm:flex-row justify-between sm:items-center gap-2.5 text-xs">
          <div class="min-w-0">
            <span class="text-[10px] text-slate-500 font-bold uppercase">\${q.category} • Kapanış: \${q.closeText}</span>
            <h4 class="font-bold text-slate-200 truncate mt-0.5">\${q.title}</h4>
          </div>
          <div class="shrink-0 flex gap-2">
            \${q.userAnswer ? \`
              <span class="px-3 py-1 rounded-xl text-[10px] font-black bg-emerald-500/10 text-emerald-400 border border-emerald-500/30">
                Seçim: \${q.userAnswer}
              </span>
            \` : \`
              <button onclick="answerDaily(\${q.id}, 'YES')" class="px-3 py-1.5 rounded-xl font-bold bg-slate-900 border border-emerald-500/30 text-emerald-400 hover:bg-emerald-500/10 transition">EVET (%\${q.probYes})</button>
              <button onclick="answerDaily(\${q.id}, 'NO')" class="px-3 py-1.5 rounded-xl font-bold bg-slate-900 border border-rose-500/30 text-rose-400 hover:bg-rose-500/10 transition">HAYIR (%\${q.probNo})</button>
            \`}
          </div>
        </div>
      \`).join('');
    }

    async function answerDaily(id, outcome) {
      const token = localStorage.getItem('oyver_session') || '';
      const res = await fetch('/api/daily/answer', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-session-token': token },
        body: JSON.stringify({ questionId: id, outcome })
      });
      const data = await res.json();
      showToast(data.message);
      loadData();
    }

    function formatCountdown(targetDateStr) {
      const diff = new Date(targetDateStr).getTime() - new Date().getTime();
      if (diff <= 0) return '⏱️ KAPANDI';
      const d = Math.floor(diff / (1000 * 60 * 60 * 24));
      const h = Math.floor((diff / (1000 * 60 * 60)) % 24);
      const m = Math.floor((diff / (1000 * 60)) % 60);
      const s = Math.floor((diff / 1000) % 60);
      return \`⏱️ \${d}g : \${h < 10 ? '0' + h : h}s : \${m < 10 ? '0' + m : m}d : \${s < 10 ? '0' + s : s}sn\`;
    }

    function renderHero() {
      const filtered = appData.markets.filter(m => currentCategory === 'Hepsi' || m.category === currentCategory);
      const heroes = filtered.filter(m => m.isHero);
      const container = document.getElementById('hero-markets-grid');
      
      container.innerHTML = heroes.map(m => \`
        <div class="glass-card rounded-3xl p-5 flex flex-col justify-between space-y-4">
          <div>
            <div class="flex justify-between items-center text-[11px] mb-2">
              <span class="bg-indigo-500/10 text-indigo-400 font-bold px-2 py-0.5 rounded-lg border border-indigo-500/20">\${m.category}</span>
              <span class="font-mono text-amber-400 font-bold countdown-timer" data-target="\${m.closesAt}">\${formatCountdown(m.closesAt)}</span>
            </div>
            <h3 class="font-bold text-slate-100 text-sm leading-snug">\${m.title}</h3>
          </div>
          <div class="space-y-2">
            <div class="grid grid-cols-2 gap-2">
              <button onclick="openTrade(\${m.id}, 'YES')" class="py-2.5 rounded-2xl font-bold text-xs bg-slate-950/80 hover:bg-emerald-500/20 border border-emerald-500/30 text-emerald-400 flex justify-between px-3 transition">
                <span>EVET</span> <span class="tabular font-black">%\${m.probYes}</span>
              </button>
              <button onclick="openTrade(\${m.id}, 'NO')" class="py-2.5 rounded-2xl font-bold text-xs bg-slate-950/80 hover:bg-rose-500/20 border border-rose-500/30 text-rose-400 flex justify-between px-3 transition">
                <span>HAYIR</span> <span class="tabular font-black">%\${m.probNo}</span>
              </button>
            </div>
            <div class="flex justify-between text-[10px] text-slate-500 pt-1">
              <span>1 Pay = 1.00 KOR</span>
              <span>👥 \${m.totalPredictionsCount} Tahmin</span>
            </div>
          </div>
        </div>
      \`).join('');
    }

    function renderTopDepth() {
      const filtered = appData.markets.filter(m => currentCategory === 'Hepsi' || m.category === currentCategory);
      const container = document.getElementById('top-depth-board');
      container.innerHTML = filtered.map((m, idx) => \`
        <div onclick="openTrade(\${m.id}, 'YES')" class="p-4 flex items-center justify-between gap-4 text-xs hover:bg-white/5 transition cursor-pointer">
          <div class="flex items-center gap-3 min-w-0">
            <span class="font-black text-slate-500 w-5">#\${idx + 1}</span>
            <span class="font-bold text-slate-200 truncate">\${m.title}</span>
          </div>
          <div class="flex items-center gap-3 shrink-0 w-28">
            <span class="text-emerald-400 font-bold tabular">%\${m.probYes}</span>
            <div class="flex-1 h-1.5 bg-slate-800 rounded-full overflow-hidden flex">
              <div class="bg-emerald-500 h-full" style="width: \${m.probYes}%"></div>
              <div class="bg-rose-500 h-full" style="width: \${m.probNo}%"></div>
            </div>
            <span class="text-rose-400 font-bold tabular">%\${m.probNo}</span>
          </div>
        </div>
      \`).join('');
    }

    setInterval(() => {
      document.querySelectorAll('.countdown-timer').forEach(el => {
        el.innerText = formatCountdown(el.getAttribute('data-target'));
      });
    }, 1000);

    function renderDrawerPositions() {
      const container = document.getElementById('drawer-positions');
      if (!appData.currentUser.positions || appData.currentUser.positions.length === 0) {
        container.innerHTML = '<div class="p-4 text-center text-xs text-slate-500 bg-slate-950 rounded-xl">Açık pozisyonunuz yok.</div>';
        return;
      }
      container.innerHTML = appData.currentUser.positions.map(p => \`
        <div class="p-3 bg-slate-950 rounded-xl border border-white/5 text-xs flex justify-between items-center">
          <div>
            <div class="font-bold text-slate-200">\${p.marketTitle}</div>
            <div class="text-[10px] text-slate-500 mt-0.5">\${p.sharesCount} Pay • Yatırılan: \${Math.round(p.totalCostKor)} KOR</div>
          </div>
          <div class="text-right space-y-1">
            <div class="font-bold \${p.pnlKor >= 0 ? 'text-emerald-400' : 'text-rose-400'}">
              \${p.pnlKor >= 0 ? '+' : ''}\${p.pnlKor} KOR
            </div>
            <button onclick="cashoutPosition(\${p.id})" class="px-2.5 py-1 bg-amber-500/10 hover:bg-amber-500/20 text-amber-300 border border-amber-500/30 rounded-lg text-[10px] font-bold">
              Erken Sat (\${Math.round(p.currentCashoutValue)} KOR)
            </button>
          </div>
        </div>
      \`).join('');
    }

    function openTrade(mId, outcome) {
      selectedMarket = appData.markets.find(x => x.id === mId);
      selectedOutcome = outcome;
      renderModal();
      document.getElementById('modal-backdrop').classList.remove('hidden');
      document.getElementById('trade-modal').classList.remove('hidden');
    }

    function closeModal() {
      document.getElementById('modal-backdrop').classList.add('hidden');
      document.getElementById('trade-modal').classList.add('hidden');
    }

    function renderModal() {
      const m = selectedMarket;
      document.getElementById('modal-content').innerHTML = \`
        <div class="space-y-1">
          <span class="text-[10px] font-bold text-indigo-400 uppercase">\${m.category}</span>
          <h3 class="font-bold text-white text-sm leading-snug">\${m.title}</h3>
        </div>

        <div class="grid grid-cols-2 gap-2 pt-2">
          <button onclick="selectedOutcome='YES'; renderModal()" class="py-2.5 rounded-2xl font-bold text-xs border transition \${selectedOutcome === 'YES' ? 'bg-emerald-500 border-emerald-400 text-slate-950 font-black' : 'bg-slate-950 border-white/10 text-slate-400'}">EVET (%\${m.probYes})</button>
          <button onclick="selectedOutcome='NO'; renderModal()" class="py-2.5 rounded-2xl font-bold text-xs border transition \${selectedOutcome === 'NO' ? 'bg-rose-500 border-rose-400 text-slate-950 font-black' : 'bg-slate-950 border-white/10 text-slate-400'}">HAYIR (%\${m.probNo})</button>
        </div>

        <div class="space-y-1.5 pt-2">
          <label class="text-[11px] text-slate-400 font-bold">Yatırılacak Tutar (KOR)</label>
          <input type="number" id="trade-input" value="100" class="w-full bg-slate-950 border border-white/10 rounded-2xl px-4 py-2.5 text-base font-bold text-white text-center focus:outline-none">
          <div class="flex justify-between text-[11px] text-slate-400 pt-1">
            <span>Bakiye: \${Math.round(appData.currentUser.balanceKor)} KOR</span>
            <span>%1 Harç Yakımı Dahil</span>
          </div>
        </div>

        <button onclick="executeBuy()" class="w-full py-3.5 bg-indigo-600 hover:bg-indigo-500 text-white rounded-2xl font-bold text-xs shadow-xl shadow-indigo-600/30 transition">Tahmini Onayla</button>
      \`;
    }

    async function executeBuy() {
      const amount = Number(document.getElementById('trade-input').value);
      const token = localStorage.getItem('oyver_session') || '';
      const res = await fetch('/api/trade/buy', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-session-token': token },
        body: JSON.stringify({ marketId: selectedMarket.id, outcome: selectedOutcome, amountKor: amount })
      });
      const data = await res.json();
      if (data.success) {
        closeModal();
        showToast(\`Tahmin onaylandı: \${data.quote.sharesReceived} Pay alındı.\`);
        loadData();
      } else {
        showToast(data.message);
      }
    }

    async function cashoutPosition(posId) {
      if (!confirm('Pozisyonunuzu havuza satarak erken kâr almak / çıkmak istiyor musunuz?')) return;
      const token = localStorage.getItem('oyver_session') || '';
      const res = await fetch('/api/trade/cashout', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-session-token': token },
        body: JSON.stringify({ positionId: posId })
      });
      const data = await res.json();
      showToast(data.message);
      loadData();
    }

    function openPortfolioDrawer() {
      document.getElementById('portfolio-drawer').classList.remove('hidden');
    }

    function closePortfolioDrawer() {
      document.getElementById('portfolio-drawer').classList.add('hidden');
    }

    function openCorporateSection(type) {
      const box = document.getElementById('corporate-box');
      const title = document.getElementById('corp-box-title');
      const desc = document.getElementById('corp-box-desc');

      const data = {
        about: {
          t: 'Biz Kimiz? — OYVER Vizyonu',
          d: 'OYVER; Türkiye gündemindeki ekonomik, kurumsal ve teknolojik gelişmeleri kolektif kitle zekâsıyla öngören bağımsız bir tahmin ve araştırma platformudur. Şansa değil rasyonel analize ve araştırma disiplinine dayanır.'
        },
        rules: {
          t: 'Çözümleme ve Hakemlik İlkeleri',
          d: 'Pazarlarımız yalnızca önceden ilan edilen resmî birincil kaynaklar (TCMB Basın Duyuruları, Resmî Gazete, TFF Raporları) ile sonuçlandırılır. İkincil yorumlar veya taraflı haberler bağlayıcı kabul edilmez.'
        },
        partnership: {
          t: 'İş Ortaklıkları & B2B Tahmin Radarı',
          d: 'Şirketler ve kurumlar için tüketici eğilimleri, marka ikilemleri (Zarla Modülü) ve sektörel tahmin kupaları düzenliyoruz. Kurumsal API ve iş birliği için: ortaklik@oyver.pro'
        },
        contact: {
          t: 'Bize Ulaşın & İletişim',
          d: 'Her türlü görüş, soru, itiraz veya kurumsal geri bildiriminiz için resmî iletişim kanalımız: iletisim@oyver.pro'
        }
      };

      title.innerText = data[type].t;
      desc.innerText = data[type].d;
      box.classList.remove('hidden');
      box.scrollIntoView({ behavior: 'smooth' });
    }

    window.onload = loadData;
  </script>
</body>
</html>
    `;
  });

  const PORT = Number(process.env.PORT) || 3000;
  const HOST = '0.0.0.0';

  await initDatabaseTables();
  await seedMarketsIfEmpty();

  await app.listen({ port: PORT, host: HOST });
  console.log(`OYVER Motoru ayakta: http://${HOST}:${PORT}`);

  const wss = new WebSocketServer({ server: app.server });
  function broadcast(payload: any) {
    const data = JSON.stringify(payload);
    wss.clients.forEach(c => {
      if (c.readyState === WebSocket.OPEN) c.send(data);
    });
  }
}

bootstrap().catch(err => {
  console.error('Başlatma hatası:', err);
  process.exit(1);
});
