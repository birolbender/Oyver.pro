import Fastify from 'fastify';
import cors from '@fastify/cors';
import helmet from '@fastify/helmet';
import WebSocket, { WebSocketServer } from 'ws';
import { db } from './db';
import { markets, users, positions, veraTransactions } from './db/schema';
import { eq, and, sql } from 'drizzle-orm';
import { AmmEngine } from './modules/amm/engine';
import { SYSTEM_CONFIG } from './config/system';

const app = Fastify({ logger: false });

async function bootstrap() {
  await app.register(helmet, { contentSecurityPolicy: false });
  await app.register(cors, { origin: '*' });

  // ============================================================================
  // 1. OTOMATİK VERİTABANI KURULUMU (POSTGRESQL TABLOLARI)
  // ============================================================================
  async function initDatabaseTables() {
    try {
      await db.execute(sql`
        CREATE TABLE IF NOT EXISTS users (
          id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
          username VARCHAR(32) NOT NULL UNIQUE,
          phone_number VARCHAR(20) UNIQUE,
          is_phone_verified BOOLEAN NOT NULL DEFAULT false,
          tckn VARCHAR(11) UNIQUE,
          legal_first_name VARCHAR(64),
          legal_last_name VARCHAR(64),
          birth_year INTEGER,
          iban VARCHAR(34),
          kyc_status VARCHAR(16) NOT NULL DEFAULT 'UNVERIFIED',
          balance_vera_withdrawable NUMERIC(18, 4) NOT NULL DEFAULT 0.0000,
          balance_vera_promo NUMERIC(18, 4) NOT NULL DEFAULT 500.0000,
          current_streak INTEGER NOT NULL DEFAULT 3,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS vera_transactions (
          id BIGSERIAL PRIMARY KEY,
          user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          account_type VARCHAR(16) NOT NULL,
          type VARCHAR(32) NOT NULL,
          amount NUMERIC(18, 4) NOT NULL,
          tax_withheld_vera NUMERIC(18, 4) NOT NULL DEFAULT 0.0000,
          balance_after NUMERIC(18, 4) NOT NULL,
          reference_id VARCHAR(64),
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
          volume_vera NUMERIC(18, 4) NOT NULL DEFAULT 0.0000,
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
          total_cost_vera NUMERIC(18, 4) NOT NULL,
          is_settled BOOLEAN NOT NULL DEFAULT false,
          is_closed_early BOOLEAN NOT NULL DEFAULT false,
          cashout_return_vera NUMERIC(18, 4),
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
      `);
      console.log('✓ PostgreSQL tabloları doğrulandı.');
    } catch (err) {
      console.error('Veritabanı başlatma hatası:', err);
    }
  }

  // ============================================================================
  // 2. OTURUM & KULLANICI YÖNETİMİ
  // ============================================================================
  async function getOrCreateActiveUser() {
    let user = await db.query.users.findFirst();
    if (!user) {
      const [newUser] = await db.insert(users).values({
        username: 'Piyasa_Analisti',
        balanceVeraPromo: '500.0000',
        balanceVeraWithdrawable: '0.0000',
        isPhoneVerified: false,
        currentStreak: 3
      }).returning();

      await db.insert(veraTransactions).values({
        userId: newUser.id,
        accountType: 'PROMO',
        type: 'INITIAL_GRANT',
        amount: '500.0000',
        balanceAfter: '500.0000',
        referenceId: 'WELCOME_500'
      });
      return newUser;
    }
    return user;
  }

  // ============================================================================
  // 3. TOHUM PAZARLARI (İLK KURULUM)
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
            closesAt: new Date(now.getTime() + 18 * 24 * 60 * 60 * 1000), // 18 Gün
            poolYes: '9880.0000',
            poolNo: '16120.0000',
            volumeVera: '26000.0000',
            totalPredictionsCount: 34,
            isHero: true
          },
          {
            slug: 'super-lig-derbi-gol-baraji',
            title: 'Hafta Sonu Oynanacak Süper Lig Derbisinde 2.5 Gol Barajı Aşılır mı?',
            category: 'Spor',
            rules: 'Müsabakanın normal süresi ve hakem uzatmalarında toplam gol sayısı en az 3 ise EVET sayılır.',
            sourceName: 'TFF Resmî Hakem Raporu',
            startsAt: now,
            closesAt: new Date(now.getTime() + 3 * 24 * 60 * 60 * 1000 + 4 * 60 * 60 * 1000), // 3 Gün 4 Saat
            poolYes: '13750.0000',
            poolNo: '11250.0000',
            volumeVera: '25000.0000',
            totalPredictionsCount: 28,
            isHero: true
          },
          {
            slug: '2027-asgari-ucret-karari',
            title: '2027 Yılı Asgari Ücret Tespit Komisyonu Net Ücreti 30.000 TL Üzerine Çıkarır mı?',
            category: 'Ekonomi',
            rules: 'Resmî Gazete\'de yayımlanan tebliğde net tutar 30.000 TL ve üstü ise EVET sayılır.',
            sourceName: 'Resmî Gazete Tebliği',
            startsAt: now,
            closesAt: new Date(now.getTime() + 92 * 24 * 60 * 60 * 1000), // 92 Gün
            poolYes: '7250.0000',
            poolNo: '17750.0000',
            volumeVera: '25000.0000',
            totalPredictionsCount: 42,
            isHero: false
          },
          {
            slug: 'togg-yeni-segment-teslimat',
            title: 'TOGG Yeni Modelinin İlk Müşteri Teslimatları Yıl Sonuna Kadar Başlar mı?',
            category: 'Teknoloji',
            rules: 'Yıl sonuna kadar nihai kullanıcılara tescilli teslimat yapıldığı duyurulursa EVET sonuçlanır.',
            sourceName: 'TOGG Resmî Basın Açıklaması',
            startsAt: now,
            closesAt: new Date(now.getTime() + 75 * 24 * 60 * 60 * 1000),
            poolYes: '12500.0000',
            poolNo: '12500.0000',
            volumeVera: '18500.0000',
            totalPredictionsCount: 19,
            isHero: false
          },
          {
            slug: 'istanbul-baraj-doluluk-ekim',
            title: 'İSKİ İstanbul Baraj Doluluk Oranı Ekim Ayı Sonunda %45 Altına Düşer mi?',
            category: 'Yaşam',
            rules: '31 Ekim günü saat 17:00 İSKİ resmî bülteninde doluluk %44.99 veya altı ise EVET sayılır.',
            sourceName: 'İSKİ Resmî Bülteni',
            startsAt: now,
            closesAt: new Date(now.getTime() + 31 * 24 * 60 * 60 * 1000),
            poolYes: '9250.0000',
            poolNo: '15750.0000',
            volumeVera: '15000.0000',
            totalPredictionsCount: 15,
            isHero: false
          }
        ]);
        console.log('✓ Tohum pazarlar yüklendi.');
      }
    } catch (e) {
      console.error('Tohum pazar hatası:', e);
    }
  }

  // ============================================================================
  // 4. API UÇLARI (REST ENDPOINTS)
  // ============================================================================

  // Durum Sorgulama
  app.get('/api/state', async () => {
    const user = await getOrCreateActiveUser();
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
        priceYesVera: prices.priceYesVera,
        priceNoVera: prices.priceNoVera
      };
    });

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
      const cost = Number(pos.totalCostVera);
      const pnlVera = Math.round((currentCashoutValue - cost) * 100) / 100;
      const pnlPercent = cost > 0 ? Math.round(((currentCashoutValue - cost) / cost) * 100) : 0;

      return {
        ...pos,
        currentCashoutValue,
        pnlVera,
        pnlPercent,
        marketTitle: m?.title || 'Pazar'
      };
    });

    const totalBalance = Number(user.balanceVeraPromo) + Number(user.balanceVeraWithdrawable);

    // Günün 3 Hızlı Sorusu
    const dailyQuestions = [
      { id: 101, title: 'T.C. Hazine bütçe açığı yıl sonu hedefi altında kalır mı?', category: 'Ekonomi', closeText: '31 Aralık', probYes: 68, probNo: 32, userAnswer: 'YES' },
      { id: 102, title: 'EuroLeague temsilcimiz bu haftaki maçını kazanır mı?', category: 'Spor', closeText: 'Yarın 21:00', probYes: 54, probNo: 46, userAnswer: null },
      { id: 103, title: 'Yeni Yapay Zekâ Mevzuat Taslağı bu ay Meclis Komisyonuna gelir mi?', category: 'Teknoloji', closeText: '25 Ekim', probYes: 61, probNo: 39, userAnswer: null }
    ];

    // Zarla Soruları
    const zarlaQuestions = [
      { id: 1, itemA: 'Nutella', itemB: 'Sarelle', votesA: 64, votesB: 36, sponsor: 'Gıda & Tüketim' },
      { id: 2, itemA: 'iOS (iPhone)', itemB: 'Android', votesA: 58, votesB: 42, sponsor: 'Teknoloji' },
      { id: 3, itemA: 'Çay', itemB: 'Kahve', votesA: 71, votesB: 29, sponsor: 'Kültür' },
      { id: 4, itemA: 'Kadıköy', itemB: 'Beşiktaş', votesA: 52, votesB: 48, sponsor: 'Yaşam' },
      { id: 5, itemA: 'Trendyol', itemB: 'Hepsiburada', votesA: 55, votesB: 45, sponsor: 'E-Ticaret' }
    ];

    // Top 100 Analistler
    const topAnalysts = [
      { rank: 1, username: 'Makro_Ufuk', tier: 'USTA', rating: 894, brier: 0.053, settledCount: 48 },
      { rank: 2, username: 'Selin_Finans', tier: 'USTA', rating: 865, brier: 0.067, settledCount: 39 },
      { rank: 3, username: user.username, tier: 'KIDEMLİ', rating: 716, brier: 0.142, settledCount: 18 },
      { rank: 4, username: 'Bist_Pusulasi', tier: 'ANALİST', rating: 540, brier: 0.230, settledCount: 16 }
    ];

    return {
      success: true,
      systemMode: SYSTEM_CONFIG.MODE,
      currency: SYSTEM_CONFIG.CURRENCY_SYMBOL,
      currentUser: {
        id: user.id,
        username: user.username,
        isPhoneVerified: user.isPhoneVerified,
        balanceTotal: totalBalance,
        balancePromo: Number(user.balanceVeraPromo),
        balanceWithdrawable: Number(user.balanceVeraWithdrawable),
        currentStreak: user.currentStreak,
        positions: enrichedPositions
      },
      categories: ['Trendler', 'Borsa', 'Siyaset', 'Spor', 'Ekonomi', 'Haber', 'Teknoloji', 'Yaşam'],
      dailyQuestions,
      zarlaQuestions,
      topAnalysts,
      activeTournament: {
        id: 1,
        title: 'Geleceğin Türkiyesi Öngörü Kupası',
        sponsor: 'Kurumsal İnovasyon Fonu',
        reward: 'İlk 3 Analiste Teknoloji Çeki & Sertifika',
        endsAt: '15 Kasım 2026',
        minPredictionsRequired: 10,
        participantsCount: 42,
        isJoined: false
      },
      markets: enrichedMarkets
    };
  });

  // Tahmin Alış (Buy)
  app.post('/api/trade/buy', async (req: any, reply) => {
    const { marketId, outcome, amountVera } = req.body;
    const user = await getOrCreateActiveUser();
    const amount = Number(amountVera);

    if (!marketId || !amount || amount <= 0) {
      return reply.status(400).send({ success: false, message: 'Geçersiz işlem tutarı.' });
    }

    const totalBalance = Number(user.balanceVeraPromo) + Number(user.balanceVeraWithdrawable);
    if (totalBalance < amount) {
      return reply.status(400).send({ success: false, message: 'Yetersiz VERA bakiyesi.' });
    }

    const market = await db.query.markets.findFirst({ where: eq(markets.id, Number(marketId)) });
    if (!market || market.status !== 'TRADING' || new Date() >= new Date(market.closesAt)) {
      return reply.status(400).send({ success: false, message: 'Bu pazar işlemlere kapanmıştır.' });
    }

    const quote = AmmEngine.calculateBuy(Number(market.poolYes), Number(market.poolNo), outcome, amount);

    let newPromo = Number(user.balanceVeraPromo);
    let newWithdrawable = Number(user.balanceVeraWithdrawable);

    if (newPromo >= amount) {
      newPromo -= amount;
    } else {
      const remainder = amount - newPromo;
      newPromo = 0;
      newWithdrawable -= remainder;
    }

    await db.update(users).set({
      balanceVeraPromo: newPromo.toFixed(4),
      balanceVeraWithdrawable: newWithdrawable.toFixed(4)
    }).where(eq(users.id, user.id));

    await db.update(markets).set({
      poolYes: quote.nextPoolYes.toFixed(4),
      poolNo: quote.nextPoolNo.toFixed(4),
      volumeVera: (Number(market.volumeVera) + amount).toFixed(4),
      totalPredictionsCount: market.totalPredictionsCount + 1
    }).where(eq(markets.id, market.id));

    await db.insert(veraTransactions).values({
      userId: user.id,
      accountType: 'PROMO',
      type: 'TRADE_BUY',
      amount: (-amount).toFixed(4),
      balanceAfter: (newPromo + newWithdrawable).toFixed(4),
      referenceId: `BUY_MKT_${market.id}`
    });

    await db.insert(positions).values({
      userId: user.id,
      marketId: market.id,
      outcome,
      sharesCount: quote.sharesReceived.toFixed(4),
      totalCostVera: amount.toFixed(4)
    });

    broadcast({ type: 'MARKET_UPDATED', marketId: market.id });

    return { success: true, quote, newBalance: newPromo + newWithdrawable };
  });

  // Erken Satış (Cash-Out)
  app.post('/api/trade/cashout', async (req: any, reply) => {
    const { positionId } = req.body;
    const user = await getOrCreateActiveUser();

    const pos = await db.query.positions.findFirst({
      where: and(eq(positions.id, Number(positionId)), eq(positions.userId, user.id), eq(positions.isClosedEarly, false))
    });

    if (!pos) {
      return reply.status(404).send({ success: false, message: 'Açık pozisyon bulunamadı.' });
    }

    const market = await db.query.markets.findFirst({ where: eq(markets.id, pos.marketId) });
    if (!market || market.status !== 'TRADING') {
      return reply.status(400).send({ success: false, message: 'Kapalı pazarlarda erken satış yapılamaz.' });
    }

    const sellQuote = AmmEngine.calculateSell(
      Number(market.poolYes),
      Number(market.poolNo),
      pos.outcome as any,
      Number(pos.sharesCount)
    );

    const returnVera = sellQuote.veraReturned;
    const newWithdrawable = Number(user.balanceVeraWithdrawable) + returnVera;

    await db.update(users).set({
      balanceVeraWithdrawable: newWithdrawable.toFixed(4)
    }).where(eq(users.id, user.id));

    await db.update(positions).set({
      isClosedEarly: true,
      cashoutReturnVera: returnVera.toFixed(4),
      updatedAt: new Date()
    }).where(eq(positions.id, pos.id));

    await db.update(markets).set({
      poolYes: sellQuote.nextPoolYes.toFixed(4),
      poolNo: sellQuote.nextPoolNo.toFixed(4)
    }).where(eq(markets.id, market.id));

    await db.insert(veraTransactions).values({
      userId: user.id,
      accountType: 'WITHDRAWABLE',
      type: 'TRADE_CASHOUT',
      amount: returnVera.toFixed(4),
      balanceAfter: (newWithdrawable + Number(user.balanceVeraPromo)).toFixed(4),
      referenceId: `CASHOUT_POS_${pos.id}`
    });

    broadcast({ type: 'MARKET_UPDATED', marketId: market.id });

    return {
      success: true,
      message: `${returnVera} VERA kâr alarak cüzdanınıza aktarıldı.`,
      newBalance: newWithdrawable + Number(user.balanceVeraPromo)
    };
  });

  // Telefon Doğrulama (+9.500 VERA)
  app.post('/api/auth/verify-phone', async (req: any, reply) => {
    const { phone, code } = req.body;
    const user = await getOrCreateActiveUser();

    if (user.isPhoneVerified) {
      return reply.status(400).send({ success: false, message: 'Telefonunuz zaten doğrulanmış.' });
    }

    if (code === '123456' && phone && phone.length >= 10) {
      const newPromo = Number(user.balanceVeraPromo) + 9500;
      await db.update(users).set({
        isPhoneVerified: true,
        phoneNumber: phone,
        balanceVeraPromo: newPromo.toFixed(4)
      }).where(eq(users.id, user.id));

      await db.insert(veraTransactions).values({
        userId: user.id,
        accountType: 'PROMO',
        type: 'PHONE_BONUS',
        amount: '9500.0000',
        balanceAfter: (newPromo + Number(user.balanceVeraWithdrawable)).toFixed(4),
        referenceId: 'SMS_VERIFIED_9500'
      });

      return { success: true, message: 'Tebrikler! +9.500 VERA bakiyeniz yüklendi.' };
    }

    return reply.status(400).send({ success: false, message: 'Geçersiz SMS kodu.' });
  });

  // ============================================================================
  // 5. TEK PARÇA KULLANICI ARAYÜZÜ (HTML / SPA)
  // ============================================================================
  app.get('/', async (_req, reply) => {
    reply.type('text/html; charset=utf-8');
    return `
<!DOCTYPE html>
<html lang="tr">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>OYVER — Türkiye'nin Kolektif Bilgi ve Tahmin Pazarı</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');
    body { background-color: #030712; color: #f8fafc; font-family: 'Inter', sans-serif; -webkit-tap-highlight-color: transparent; }
    .tabular { font-variant-numeric: tabular-nums; }
    .no-scrollbar::-webkit-scrollbar { display: none; }
  </style>
</head>
<body class="min-h-screen pb-24 md:pb-12 text-slate-100">

  <!-- TOAST BİLDİRİM -->
  <div id="toast" class="fixed top-4 right-4 z-50 transform -translate-y-24 opacity-0 transition-all duration-300 bg-slate-900 border border-slate-700 text-white text-xs px-4 py-3 rounded-xl shadow-2xl flex items-center gap-2">
    <span id="toast-msg">Bildirim</span>
  </div>

  <!-- HEADER (YALIN: Piyasalar yukarıdan kaldırıldı, Bakiye profilin altında küçük) -->
  <header class="sticky top-0 z-40 bg-slate-950/90 backdrop-blur-md border-b border-slate-800 px-4 py-2.5">
    <div class="max-w-6xl mx-auto flex items-center justify-between">
      
      <!-- Sol: Logo -->
      <div class="flex items-center gap-2 cursor-pointer" onclick="navigate('markets')">
        <div class="w-8 h-8 rounded-lg bg-indigo-600 flex items-center justify-center font-black text-white text-base shadow-lg shadow-indigo-600/30">O</div>
        <span class="font-extrabold text-lg text-white tracking-tight">OYVER</span>
        <span class="text-[9px] bg-slate-800 text-slate-400 border border-slate-700 px-1.5 py-0.2 rounded font-bold ml-1">PRO</span>
      </div>

      <!-- Sağ: Profil ve Altında Küçük Bakiye -->
      <div onclick="navigate('portfolio')" class="cursor-pointer flex flex-col items-end group">
        <div class="flex items-center gap-1.5 text-xs font-bold text-slate-300 group-hover:text-white transition">
          <span class="w-6 h-6 rounded-full bg-indigo-600/20 border border-indigo-500/40 flex items-center justify-center text-[11px]">👤</span>
          <span id="header-username">Analist</span>
        </div>
        <!-- Bakiye: Profilin altına küçük ve zarif konumlandırıldı -->
        <div class="text-[10px] text-slate-400 font-semibold tabular mt-0.5">
          <span id="header-balance" class="text-emerald-400 font-bold">500</span> <span class="text-[9px] text-indigo-400">VERA</span>
        </div>
      </div>

    </div>
  </header>

  <!-- KATEGORİ ŞERİDİ -->
  <div class="max-w-6xl mx-auto px-4 pt-3">
    <div id="category-bar" class="flex gap-2 overflow-x-auto no-scrollbar pb-1 text-xs"></div>
  </div>

  <!-- ANA İÇERİK KONTEYNERİ -->
  <main class="max-w-6xl mx-auto px-4 py-4 space-y-6">

    <!-- 1. GÖRÜNÜM: PİYASALAR (ANA AKIŞ) -->
    <section id="tab-markets" class="space-y-6">
      
      <!-- GÜNÜN 3 HIZLI SORUSU -->
      <div class="bg-gradient-to-r from-slate-900 to-indigo-950/40 border border-slate-800 rounded-2xl p-4 space-y-3">
        <div class="flex justify-between items-center text-xs">
          <div class="flex items-center gap-2">
            <span class="w-2 h-2 rounded-full bg-amber-400 animate-ping"></span>
            <span class="font-bold text-slate-200">Günün 3 Hızlı Sorusu</span>
            <span class="text-[10px] text-amber-400 bg-amber-950/60 border border-amber-800/60 px-1.5 py-0.5 rounded font-black">+50 VERA</span>
          </div>
          <span class="text-[11px] text-slate-400 font-semibold">1/3 Tamamlandı</span>
        </div>
        <div id="daily-questions-list" class="space-y-2"></div>
      </div>

      <!-- MANŞET İKİLİSİ (Canlı Saniye Sayacıyla) -->
      <div class="space-y-3">
        <h2 class="text-xs font-bold text-slate-400 uppercase tracking-wider flex items-center gap-1.5">
          <span>⭐</span> Günün Manşet İkilisi
        </h2>
        <div id="hero-markets-grid" class="grid grid-cols-1 md:grid-cols-2 gap-4"></div>
      </div>

      <!-- TOP 10 DERİNLİK TAHTASI -->
      <div class="space-y-3">
        <div class="flex justify-between items-center text-xs">
          <h2 class="font-bold text-slate-400 uppercase tracking-wider flex items-center gap-1.5">
            <span>🔥</span> En Çok Tahmin Edilen Top 10
          </h2>
          <span class="text-slate-500 text-[11px]">1 VERA = 1.00 TL Nominal</span>
        </div>
        <div id="top10-depth-board" class="bg-slate-900 border border-slate-800 rounded-2xl overflow-hidden divide-y divide-slate-800/80"></div>
      </div>

    </section>

    <!-- 2. GÖRÜNÜM: TOP 100 ANALİST LİGİ -->
    <section id="tab-top100" class="hidden space-y-4">
      <div class="border-b border-slate-800 pb-3">
        <h2 class="text-xl font-black text-white">🏆 OYVER Top 100 Analist Ligi</h2>
        <p class="text-xs text-slate-400">Analist Yetenek Puanı (AYP) sıralaması. Asgari 15 sonuçlanmış pazar şartı aranır.</p>
      </div>
      <div id="top100-list" class="bg-slate-900 border border-slate-800 rounded-2xl divide-y divide-slate-800"></div>
    </section>

    <!-- 3. GÖRÜNÜM: 🎲 ZARLA (TÜKETİCİ İKİLEMLERİ) -->
    <section id="tab-zarla" class="hidden space-y-4 max-w-lg mx-auto">
      <div class="text-center space-y-1">
        <h2 class="text-2xl font-black text-amber-400">🎲 ZARLA</h2>
        <p class="text-xs text-slate-400">10 soruluk tüketici ikilemini tamamla, kitleyle uyuşma oranını öğren.</p>
      </div>
      <div id="zarla-card" class="bg-slate-900 border border-slate-800 rounded-3xl p-6 space-y-5 text-center shadow-xl"></div>
    </section>

    <!-- 4. GÖRÜNÜM: PORTFÖY & CÜZDAN (ERKEN SATIŞ / CASHOUT) -->
    <section id="tab-portfolio" class="hidden space-y-6 max-w-2xl mx-auto">
      <div class="bg-slate-900 border border-slate-800 rounded-2xl p-5 space-y-4">
        <div class="flex justify-between items-start">
          <div>
            <div class="flex items-center gap-2">
              <h3 id="prof-username" class="text-lg font-black text-white">Analist</h3>
              <span class="text-[9px] font-black px-2 py-0.5 rounded bg-purple-950 text-purple-400 border border-purple-800">KIDEMLİ</span>
            </div>
            <div class="text-xs text-slate-400 mt-1">AYP Skoru: <strong class="text-white">716</strong> • Seri: <strong class="text-amber-400">3 Gün 🔥</strong></div>
          </div>
          <div class="text-right">
            <div id="portfolio-balance" class="text-xl font-black text-emerald-400 tabular">500 VERA</div>
            <div class="text-[10px] text-slate-500">1 VERA = 1.00 TL Nominal</div>
          </div>
        </div>

        <!-- GSM ONAY KUTUSU -->
        <div id="gsm-box" class="p-4 bg-slate-950 rounded-xl border border-indigo-900/60 space-y-2">
          <div class="flex justify-between items-center text-xs">
            <span class="font-bold text-white">📱 Telefon Doğrulaması (+9.500 VERA)</span>
            <span class="text-[10px] text-amber-400 font-black">BEKLİYOR</span>
          </div>
          <p class="text-[11px] text-slate-400">Kalan hoş geldin bakiyenizi serbest bırakmak için telefonunuzu doğrulayın.</p>
          <div class="flex gap-2">
            <input type="text" id="gsm-phone" placeholder="+905XXXXXXXXX" class="flex-1 bg-slate-900 border border-slate-800 rounded-xl px-3 py-1.5 text-xs text-white focus:outline-none">
            <input type="text" id="gsm-code" placeholder="123456" class="w-24 bg-slate-900 border border-slate-800 rounded-xl px-3 py-1.5 text-xs text-white text-center focus:outline-none">
            <button onclick="verifyPhone()" class="px-3 py-1.5 bg-indigo-600 hover:bg-indigo-500 text-white rounded-xl text-xs font-bold transition">Onayla</button>
          </div>
        </div>
      </div>

      <!-- AÇIK POZİSYONLAR & ERKEN SATIŞ -->
      <div class="space-y-3">
        <h3 class="text-xs font-bold text-slate-400 uppercase tracking-wider">Açık Pozisyonlar & Erken Satış (Kâr Al)</h3>
        <div id="positions-container" class="space-y-3"></div>
      </div>
    </section>

  </main>

  <!-- İŞLEM MASASI MODALI (Slippage / Fiyat Kayması Önizlemeli) -->
  <div id="modal-backdrop" onclick="closeModal()" class="fixed inset-0 bg-black/80 backdrop-blur-sm z-50 hidden"></div>
  <div id="trade-modal" class="fixed bottom-0 md:top-1/2 md:left-1/2 md:-translate-x-1/2 md:-translate-y-1/2 w-full md:max-w-md bg-slate-900 border border-slate-800 rounded-t-3xl md:rounded-3xl p-6 z-50 hidden space-y-4">
    <div id="modal-content"></div>
  </div>

  <!-- MOBİL 4 SEKMELİ SABİT ALT BAR (PİYASALAR BURADA) -->
  <nav class="md:hidden fixed bottom-0 left-0 right-0 bg-slate-950/95 border-t border-slate-800 backdrop-blur-md px-6 py-2.5 z-40 flex justify-between items-center text-[10px] font-bold text-slate-400">
    <button onclick="navigate('markets')" id="btn-nav-mkt" class="flex flex-col items-center gap-1 text-indigo-400">
      <span class="text-base">🌐</span>
      <span>Piyasalar</span>
    </button>
    <button onclick="navigate('top100')" id="btn-nav-top100" class="flex flex-col items-center gap-1 hover:text-white">
      <span class="text-base">🏆</span>
      <span>Top 100</span>
    </button>
    <button onclick="navigate('zarla')" id="btn-nav-zarla" class="flex flex-col items-center gap-1 hover:text-white">
      <span class="text-base">🎲</span>
      <span>Zarla</span>
    </button>
    <button onclick="navigate('portfolio')" id="btn-nav-port" class="flex flex-col items-center gap-1 hover:text-white">
      <span class="text-base">💼</span>
      <span>Portföy</span>
    </button>
  </nav>

  <!-- AKORDİYON KURUMSAL FOOTER -->
  <footer class="max-w-6xl mx-auto px-4 mt-12 border-t border-slate-800 pt-6 text-xs text-slate-400 space-y-4">
    <div class="space-y-2">
      <details class="group bg-slate-900 border border-slate-800 rounded-xl p-3">
        <summary class="font-bold text-slate-200 cursor-pointer list-none flex justify-between items-center">
          <span>Platform & Turnuvalar</span>
          <span class="group-open:rotate-180 transition-transform">▾</span>
        </summary>
        <div class="pt-2.5 flex flex-col space-y-1.5 text-slate-400">
          <p>OYVER, kitle zekâsı ve tahmin piyasaları araştırma altyapısıdır.</p>
          <p>Kurumsal sponsorlu turnuvalar şansa değil analitik beceriye dayalıdır.</p>
        </div>
      </details>

      <details class="group bg-slate-900 border border-slate-800 rounded-xl p-3">
        <summary class="font-bold text-slate-200 cursor-pointer list-none flex justify-between items-center">
          <span>Şeffaflık & Hukuki Durum</span>
          <span class="group-open:rotate-180 transition-transform">▾</span>
        </summary>
        <div class="pt-2.5 flex flex-col space-y-1.5 text-slate-400 leading-relaxed">
          <p>Tüm pazarlar resmî birincil kaynaklara (TCMB, Resmî Gazete, TFF) göre sonuçlandırılır.</p>
          <p>Platformumuz 7258 sayılı Bahis Kanunu ve 6362 sayılı SPK Kanunu sınırları gözetilerek tasarlanmıştır.</p>
        </div>
      </details>
    </div>

    <div class="p-3 bg-slate-950 border border-slate-800/80 rounded-xl text-[11px] leading-relaxed text-slate-500">
      <strong>⚠️ Hukuki Bilgilendirme [Taslak]:</strong> OYVER üzerindeki VERA, kapalı devre bir simülasyon ve itibar göstergesidir (1 VERA = 1.00 TL nominal). Yatırım tavsiyesi içermez.
    </div>
  </footer>

  <!-- İSTEMCİ JAVASCRIPT MOTORU -->
  <script>
    let appData = null;
    let selectedMarket = null;
    let selectedOutcome = 'YES';
    let currentZarlaIndex = 0;

    function showToast(msg) {
      const toast = document.getElementById('toast');
      document.getElementById('toast-msg').innerText = msg;
      toast.classList.remove('-translate-y-24', 'opacity-0');
      setTimeout(() => toast.classList.add('-translate-y-24', 'opacity-0'), 3000);
    }

    async function loadData() {
      try {
        const res = await fetch('/api/state');
        appData = await res.json();
        if (appData.success) {
          renderUI();
        }
      } catch (e) {
        console.error('Yükleme hatası:', e);
      }
    }

    function renderUI() {
      // Header ve Portföy Bakiye
      document.getElementById('header-balance').innerText = Math.round(appData.currentUser.balanceTotal).toLocaleString();
      document.getElementById('header-username').innerText = appData.currentUser.username;
      document.getElementById('portfolio-balance').innerText = Math.round(appData.currentUser.balanceTotal).toLocaleString() + ' VERA';

      if (appData.currentUser.isPhoneVerified) {
        document.getElementById('gsm-box').classList.add('hidden');
      }

      renderCategories();
      renderDailyQuestions();
      renderHeroMarkets();
      renderTop10Depth();
      renderTop100();
      renderZarla();
      renderPositions();
    }

    function renderCategories() {
      const bar = document.getElementById('category-bar');
      bar.innerHTML = appData.categories.map((c, i) => \`
        <button class="px-3 py-1.5 rounded-lg font-bold whitespace-nowrap transition \${
          i === 0 ? 'bg-indigo-600 text-white' : 'bg-slate-900 border border-slate-800 text-slate-400 hover:text-white'
        }">\${c}</button>
      \`).join('');
    }

    function renderDailyQuestions() {
      const container = document.getElementById('daily-questions-list');
      container.innerHTML = appData.dailyQuestions.map(q => \`
        <div class="bg-slate-950 p-3 rounded-xl border border-slate-800/80 flex flex-col sm:flex-row justify-between sm:items-center gap-2 text-xs">
          <div class="min-w-0">
            <span class="text-[10px] text-slate-500 font-bold uppercase tracking-wider">\${q.category} • \${q.closeText}</span>
            <h4 class="font-bold text-slate-200 truncate mt-0.5">\${q.title}</h4>
          </div>
          <div class="flex items-center gap-2 shrink-0">
            \${q.userAnswer ? \`
              <span class="px-3 py-1 rounded-lg text-[10px] font-black bg-emerald-950 text-emerald-400 border border-emerald-800">
                Seçim: \${q.userAnswer}
              </span>
            \` : \`
              <button onclick="showToast('Tahmin kaydedildi: +50 VERA!')" class="px-3 py-1 rounded-lg font-bold bg-slate-900 border border-slate-800 hover:border-emerald-500 text-emerald-400 transition">EVET (%\${q.probYes})</button>
              <button onclick="showToast('Tahmin kaydedildi: +50 VERA!')" class="px-3 py-1 rounded-lg font-bold bg-slate-900 border border-slate-800 hover:border-rose-500 text-rose-400 transition">HAYIR (%\${q.probNo})</button>
            \`}
          </div>
        </div>
      \`).join('');
    }

    // Canlı Geri Sayım Biçimlendirici
    function formatCountdown(targetDateStr) {
      const diff = new Date(targetDateStr).getTime() - new Date().getTime();
      if (diff <= 0) return '⏱️ İŞLEME KAPANDI';

      const d = Math.floor(diff / (1000 * 60 * 60 * 24));
      const h = Math.floor((diff / (1000 * 60 * 60)) % 24);
      const m = Math.floor((diff / (1000 * 60)) % 60);
      const s = Math.floor((diff / 1000) % 60);

      return \`⏱️ \${d}g : \${h < 10 ? '0' + h : h}s : \${m < 10 ? '0' + m : m}d : \${s < 10 ? '0' + s : s}sn\`;
    }

    function renderHeroMarkets() {
      const heroes = appData.markets.filter(m => m.isHero);
      const container = document.getElementById('hero-markets-grid');
      container.innerHTML = heroes.map(m => \`
        <div class="bg-slate-900 border border-slate-800 rounded-2xl p-5 flex flex-col justify-between space-y-4">
          <div>
            <div class="flex justify-between items-center text-[11px] mb-2">
              <span class="bg-indigo-950 text-indigo-400 font-bold px-2 py-0.5 rounded border border-indigo-900">\${m.category}</span>
              <span class="font-mono text-amber-400 font-bold countdown-timer" data-target="\${m.closesAt}">\${formatCountdown(m.closesAt)}</span>
            </div>
            <h3 class="font-bold text-slate-100 text-sm leading-snug">\${m.title}</h3>
          </div>
          <div class="space-y-2">
            <div class="grid grid-cols-2 gap-2">
              <button onclick="openTrade(\${m.id}, 'YES')" class="py-2.5 rounded-xl font-bold text-xs bg-slate-950 hover:border-emerald-500 border border-slate-800 text-emerald-400 flex justify-between px-3">
                <span>EVET</span> <span class="tabular">%\${m.probYes}</span>
              </button>
              <button onclick="openTrade(\${m.id}, 'NO')" class="py-2.5 rounded-xl font-bold text-xs bg-slate-950 hover:border-rose-500 border border-slate-800 text-rose-400 flex justify-between px-3">
                <span>HAYIR</span> <span class="tabular">%\${m.probNo}</span>
              </button>
            </div>
            <div class="flex justify-between text-[10px] text-slate-500 pt-1">
              <span>1 Kazanan Pay = 1.00 VERA</span>
              <span>👥 \${m.totalPredictionsCount} Tahmin</span>
            </div>
          </div>
        </div>
      \`).join('');
    }

    function renderTop10Depth() {
      const container = document.getElementById('top10-depth-board');
      container.innerHTML = appData.markets.map((m, idx) => \`
        <div onclick="openTrade(\${m.id}, 'YES')" class="p-3.5 flex items-center justify-between gap-4 text-xs hover:bg-slate-800/40 transition cursor-pointer">
          <div class="flex items-center gap-3 min-w-0">
            <span class="font-black text-slate-500 w-5">#\${idx + 1}</span>
            <span class="font-bold text-slate-200 truncate">\${m.title}</span>
          </div>
          <div class="flex items-center gap-4 shrink-0">
            <span class="text-slate-400 text-[11px] hidden sm:inline">\${m.totalPredictionsCount} Tahmin</span>
            <div class="flex items-center gap-1.5 w-28">
              <span class="text-emerald-400 font-bold tabular">%\${m.probYes}</span>
              <div class="flex-1 h-1.5 bg-slate-800 rounded-full overflow-hidden flex">
                <div class="bg-emerald-500 h-full" style="width: \${m.probYes}%"></div>
                <div class="bg-rose-500 h-full" style="width: \${m.probNo}%"></div>
              </div>
              <span class="text-rose-400 font-bold tabular">%\${m.probNo}</span>
            </div>
          </div>
        </div>
      \`).join('');
    }

    setInterval(() => {
      document.querySelectorAll('.countdown-timer').forEach(el => {
        el.innerText = formatCountdown(el.getAttribute('data-target'));
      });
    }, 1000);

    function renderTop100() {
      const container = document.getElementById('top100-list');
      container.innerHTML = appData.topAnalysts.map(a => \`
        <div class="p-4 flex items-center justify-between gap-4 text-xs">
          <div class="flex items-center gap-3">
            <span class="font-black text-slate-500 w-6">#\${a.rank}</span>
            <div>
              <div class="font-bold text-slate-200">\${a.username}</div>
              <div class="text-[10px] text-slate-500">\${a.settledCount} Pazar Geçmişi</div>
            </div>
            <span class="text-[9px] font-black px-1.5 py-0.2 rounded \${a.tier === 'USTA' ? 'bg-amber-950 text-amber-400 border border-amber-800' : 'bg-purple-950 text-purple-400 border border-purple-800'}">\${a.tier}</span>
          </div>
          <div class="text-right">
            <div class="font-black text-emerald-400 text-sm tabular">\${a.rating} AYP</div>
            <div class="text-[10px] text-slate-500">Brier: \${a.brier}</div>
          </div>
        </div>
      \`).join('');
    }

    function renderZarla() {
      const q = appData.zarlaQuestions[currentZarlaIndex];
      const total = q.votesA + q.votesB;
      const pctA = Math.round((q.votesA / total) * 100);
      const pctB = 100 - pctA;

      document.getElementById('zarla-card').innerHTML = \`
        <div class="text-[11px] text-slate-500 font-bold uppercase tracking-wider">\${currentZarlaIndex + 1} / \${appData.zarlaQuestions.length} • \${q.sponsor}</div>
        <h3 class="text-xl font-black text-white">\${q.itemA} mı, \${q.itemB} mi?</h3>
        <div class="grid grid-cols-2 gap-3 pt-2">
          <button onclick="voteZarla()" class="py-6 rounded-2xl bg-slate-950 border border-slate-800 hover:border-indigo-500 text-sm font-black transition">
            \${q.itemA}
            <div class="text-[10px] text-slate-500 font-normal mt-1">%\${pctA} Kitle Oyu</div>
          </button>
          <button onclick="voteZarla()" class="py-6 rounded-2xl bg-slate-950 border border-slate-800 hover:border-indigo-500 text-sm font-black transition">
            \${q.itemB}
            <div class="text-[10px] text-slate-500 font-normal mt-1">%\${pctB} Kitle Oyu</div>
          </button>
        </div>
      \`;
    }

    function voteZarla() {
      if (currentZarlaIndex < appData.zarlaQuestions.length - 1) {
        currentZarlaIndex++;
        renderZarla();
      } else {
        showToast('Zarla serisi tamamlandı! Tercihlerin kitleyle kaydedildi.');
        currentZarlaIndex = 0;
        renderZarla();
      }
    }

    function renderPositions() {
      const container = document.getElementById('positions-container');
      if (!appData.currentUser.positions || appData.currentUser.positions.length === 0) {
        container.innerHTML = '<div class="p-4 text-center text-xs text-slate-500 bg-slate-950 rounded-xl">Açık pozisyonunuz bulunmuyor.</div>';
        return;
      }

      container.innerHTML = appData.currentUser.positions.map(p => \`
        <div class="bg-slate-950 p-4 rounded-xl border border-slate-800 flex justify-between items-center text-xs">
          <div class="space-y-1">
            <div class="flex items-center gap-2">
              <span class="text-[9px] font-black px-1.5 py-0.5 rounded \${p.outcome === 'YES' ? 'bg-emerald-950 text-emerald-400' : 'bg-rose-950 text-rose-400'}">\${p.outcome}</span>
              <h4 class="font-bold text-white">\${p.marketTitle}</h4>
            </div>
            <div class="text-[11px] text-slate-400">Yatırılan: \${Math.round(p.totalCostVera)} VERA • Pay: \${p.sharesCount}</div>
          </div>
          <div class="text-right space-y-1.5">
            <div class="font-bold \${p.pnlVera >= 0 ? 'text-emerald-400' : 'text-rose-400'}">
              \${p.pnlVera >= 0 ? '+' : ''}\${p.pnlVera} VERA (%\${p.pnlPercent})
            </div>
            <button onclick="cashoutPosition(\${p.id})" class="px-3 py-1 bg-amber-500/20 hover:bg-amber-500/30 border border-amber-500/40 text-amber-300 rounded-lg font-bold text-[10px] transition">
              Erken Sat (\${Math.round(p.currentCashoutValue)} VERA)
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
          <button onclick="selectedOutcome='YES'; renderModal()" class="py-2 rounded-xl font-bold text-xs border \${selectedOutcome === 'YES' ? 'bg-emerald-600 border-emerald-500 text-white' : 'bg-slate-800 border-slate-700 text-slate-400'}">EVET (%\${m.probYes})</button>
          <button onclick="selectedOutcome='NO'; renderModal()" class="py-2 rounded-xl font-bold text-xs border \${selectedOutcome === 'NO' ? 'bg-rose-600 border-rose-500 text-white' : 'bg-slate-800 border-slate-700 text-slate-400'}">HAYIR (%\${m.probNo})</button>
        </div>

        <div class="space-y-1 pt-2">
          <label class="text-[11px] text-slate-400 font-bold">Yatırılacak Tutar (VERA)</label>
          <input type="number" id="trade-input" value="100" class="w-full bg-slate-950 border border-slate-800 rounded-xl px-4 py-2 text-base font-bold text-white text-center focus:outline-none">
          <div class="flex justify-between text-[11px] text-slate-500 pt-1">
            <span>Mevcut: \${Math.round(appData.currentUser.balanceTotal)} VERA</span>
            <span>1 Kazanan Pay = 1.00 VERA (1 TL)</span>
          </div>
        </div>

        <button onclick="executeBuy()" class="w-full py-3 bg-indigo-600 hover:bg-indigo-500 text-white rounded-xl font-bold text-xs transition">Tahmini Onayla</button>
      \`;
    }

    async function executeBuy() {
      const amount = Number(document.getElementById('trade-input').value);
      const res = await fetch('/api/trade/buy', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ marketId: selectedMarket.id, outcome: selectedOutcome, amountVera: amount })
      });
      const data = await res.json();
      if (data.success) {
        closeModal();
        showToast(\`Tahmin onaylandı: \${data.quote.sharesReceived} Pay alındı.\`);
        loadData();
      } else {
        alert(data.message);
      }
    }

    async function cashoutPosition(posId) {
      if (!confirm('Pozisyonunuzu anlık piyasa fiyatından havuza satarak erken nakde çıkmak istiyor musunuz?')) return;
      const res = await fetch('/api/trade/cashout', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ positionId: posId })
      });
      const data = await res.json();
      if (data.success) {
        showToast(data.message);
        loadData();
      } else {
        alert(data.message);
      }
    }

    async function verifyPhone() {
      const phone = document.getElementById('gsm-phone').value;
      const code = document.getElementById('gsm-code').value;
      const res = await fetch('/api/auth/verify-phone', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ phone, code })
      });
      const data = await res.json();
      showToast(data.message);
      if (data.success) loadData();
    }

    function navigate(tab) {
      ['markets', 'top100', 'zarla', 'portfolio'].forEach(t => {
        document.getElementById('tab-' + t).classList.add('hidden');
      });
      document.getElementById('tab-' + tab).classList.remove('hidden');

      // Buton Aktiflik Renkleri
      ['mkt', 'top100', 'zarla', 'port'].forEach(b => {
        document.getElementById('btn-nav-' + b).className = 'flex flex-col items-center gap-1 text-slate-400 hover:text-white';
      });
      const activeMap = { markets: 'mkt', top100: 'top100', zarla: 'zarla', portfolio: 'port' };
      document.getElementById('btn-nav-' + activeMap[tab]).className = 'flex flex-col items-center gap-1 text-indigo-400';
      window.scrollTo(0, 0);
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
