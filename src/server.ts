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

  // 1. VERİTABANI TABLOLARINI OLUŞTUR
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
      console.log('✓ PostgreSQL tabloları hazır.');
    } catch (err) {
      console.error('Veritabanı başlatma hatası:', err);
    }
  }

  // 2. OTURUM KULLANICISI
  async function getOrCreateActiveUser() {
    let user = await db.query.users.findFirst();
    if (!user) {
      const [newUser] = await db.insert(users).values({
        username: 'Piyasa_Uzmanı',
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

  // 3. TOHUM PAZARLARI
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
            rules: 'TCMB PPK toplantısı resmî duyurusunda politika faizinde indirim açıklandığı an EVET sayılır.',
            sourceName: 'TCMB Resmî Basın Duyurusu',
            startsAt: now,
            closesAt: new Date(now.getTime() + 18 * 24 * 60 * 60 * 1000),
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
            rules: 'Normal süre ve hakem uzatmalarında toplam gol sayısı en az 3 ise EVET sayılır.',
            sourceName: 'TFF Resmî Hakem Raporu',
            startsAt: now,
            closesAt: new Date(now.getTime() + 3 * 24 * 60 * 60 * 1000 + 4 * 60 * 60 * 1000),
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
            rules: 'Resmî Gazete\'de yayımlanan tebliğde 2027 net tutarı 30.000 TL ve üstü ise EVET sayılır.',
            sourceName: 'Resmî Gazete Tebliği',
            startsAt: now,
            closesAt: new Date(now.getTime() + 92 * 24 * 60 * 60 * 1000),
            poolYes: '7250.0000',
            poolNo: '17750.0000',
            volumeVera: '25000.0000',
            totalPredictionsCount: 42,
            isHero: false
          },
          {
            slug: 'bist100-yil-sonu-rekoru',
            title: 'BIST 100 Endeksi 2026 Yılını 12.000 Puan Seviyesinin Üzerinde Kapatır mı?',
            category: 'Borsa',
            rules: 'Borsa İstanbul 2026 son işlem günü seans kapanış değeri 12.000,01 ve üzeri ise EVET sayılır.',
            sourceName: 'Borsa İstanbul Resmî Verileri',
            startsAt: now,
            closesAt: new Date(now.getTime() + 90 * 24 * 60 * 60 * 1000),
            poolYes: '11000.0000',
            poolNo: '14000.0000',
            volumeVera: '21000.0000',
            totalPredictionsCount: 31,
            isHero: false
          },
          {
            slug: 'togg-yeni-segment-teslimat',
            title: 'TOGG Yeni Modelinin İlk Müşteri Teslimatları 2026 İçinde Başlar mı?',
            category: 'Teknoloji',
            rules: 'Yıl sonuna kadar nihai kullanıcılara tescilli teslimat yapıldığı duyurulursa EVET sonuçlanır.',
            sourceName: 'TOGG Resmî Kurumsal Açıklaması',
            startsAt: now,
            closesAt: new Date(now.getTime() + 75 * 24 * 60 * 60 * 1000),
            poolYes: '12500.0000',
            poolNo: '12500.0000',
            volumeVera: '18500.0000',
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

  // 4. API DURUM SERVİSİ
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
      categories: ['Hepsi', 'Ekonomi', 'Borsa', 'Spor', 'Teknoloji', 'Siyaset', 'Yaşam'],
      dailyQuestions: [
        { id: 101, title: 'T.C. Hazine bütçe açığı yıl sonu hedefi altında kalır mı?', category: 'Ekonomi', closeText: '31 Aralık', probYes: 68, probNo: 32, userAnswer: 'YES' },
        { id: 102, title: 'EuroLeague temsilcimiz bu haftaki maçını kazanır mı?', category: 'Spor', closeText: 'Yarın 21:00', probYes: 54, probNo: 46, userAnswer: null },
        { id: 103, title: 'Yeni Yapay Zekâ Mevzuat Taslağı bu ay Meclis Komisyonuna gelir mi?', category: 'Teknoloji', closeText: '25 Ekim', probYes: 61, probNo: 39, userAnswer: null }
      ],
      zarlaQuestions: [
        { id: 1, itemA: 'Nutella', itemB: 'Sarelle', votesA: 64, votesB: 36, sponsor: 'Gıda & Tüketim' },
        { id: 2, itemA: 'Apple iOS', itemB: 'Google Android', votesA: 58, votesB: 42, sponsor: 'Teknoloji' },
        { id: 3, itemA: 'Geleneksel Çay', itemB: 'Filtre Kahve', votesA: 71, votesB: 29, sponsor: 'Yaşam & Kültür' },
        { id: 4, itemA: 'Trendyol', itemB: 'Hepsiburada', votesA: 55, votesB: 45, sponsor: 'E-Ticaret' }
      ],
      topAnalysts: [
        { rank: 1, username: 'Makro_Ufuk', tier: 'USTA', rating: 894, brier: 0.053, settledCount: 48 },
        { rank: 2, username: 'Selin_Finans', tier: 'USTA', rating: 865, brier: 0.067, settledCount: 39 },
        { rank: 3, username: user.username, tier: 'KIDEMLİ', rating: 716, brier: 0.142, settledCount: 18 },
        { rank: 4, username: 'Bist_Analitik', tier: 'ANALİST', rating: 540, brier: 0.230, settledCount: 16 }
      ],
      markets: enrichedMarkets
    };
  });

  // TAHMİN ALIMI (BUY)
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

  // ERKEN SATIŞ (CASHOUT)
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

  // TELEFON DOĞRULAMA
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

  // GÜNÜN SORUSU YANITI
  app.post('/api/daily/answer', async (req: any, reply) => {
    const user = await getOrCreateActiveUser();
    const newPromo = Number(user.balanceVeraPromo) + 50;

    await db.update(users).set({ balanceVeraPromo: newPromo.toFixed(4) }).where(eq(users.id, user.id));
    await db.insert(veraTransactions).values({
      userId: user.id,
      accountType: 'PROMO',
      type: 'DAILY_BONUS',
      amount: '50.0000',
      balanceAfter: (newPromo + Number(user.balanceVeraWithdrawable)).toFixed(4),
      referenceId: 'DAILY_Q_50'
    });

    return { success: true, message: 'Tahmin kaydedildi! +50 VERA yüklendi.', newBalance: newPromo + Number(user.balanceVeraWithdrawable) };
  });

  // 5. YENİLENMİŞ ARAYÜZ (HTML / CSS / JS)
  app.get('/', async (_req, reply) => {
    reply.type('text/html; charset=utf-8');
    return `
<!DOCTYPE html>
<html lang="tr">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=5.0">
  <title>OYVER — Türkiye'nin Kolektif Bilgi ve Tahmin Pazarı</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800;900&display=swap');
    body { background-color: #07090E; color: #F1F5F9; font-family: 'Plus Jakarta Sans', sans-serif; -webkit-tap-highlight-color: transparent; }
    .tabular { font-variant-numeric: tabular-nums; }
    .glass-card { background: rgba(15, 23, 42, 0.65); backdrop-filter: blur(12px); border: 1px solid rgba(255, 255, 255, 0.07); }
    .glass-card-hover:hover { border-color: rgba(99, 102, 241, 0.4); transform: translateY(-1px); transition: all 0.2s ease; }
    .no-scrollbar::-webkit-scrollbar { display: none; }
  </style>
</head>
<body class="min-h-screen pb-28 text-slate-100 flex flex-col justify-between">

  <!-- TOAST BİLDİRİM BİLEŞENİ -->
  <div id="toast" class="fixed top-5 right-5 z-50 transform -translate-y-28 opacity-0 transition-all duration-300 bg-slate-900/95 border border-indigo-500/40 text-white text-xs px-4 py-3 rounded-2xl shadow-2xl flex items-center gap-2 backdrop-blur-md">
    <span id="toast-icon">✓</span>
    <span id="toast-msg">İşlem başarılı</span>
  </div>

  <!-- ÜST HEADER (YALIN & KALİTELİ) -->
  <header class="sticky top-0 z-40 bg-[#07090E]/90 backdrop-blur-md border-b border-white/5 px-4 lg:px-8 py-3">
    <div class="max-w-6xl mx-auto flex items-center justify-between">
      
      <!-- LOGO -->
      <div class="flex items-center gap-2.5 cursor-pointer" onclick="navigate('markets')">
        <div class="w-8 h-8 rounded-xl bg-gradient-to-tr from-indigo-600 to-violet-500 flex items-center justify-center font-black text-white text-base shadow-lg shadow-indigo-500/25">O</div>
        <div class="flex items-center gap-1.5">
          <span class="font-extrabold text-lg text-white tracking-tight">OYVER</span>
          <span class="text-[9px] bg-indigo-500/10 text-indigo-400 border border-indigo-500/20 px-1.5 py-0.5 rounded-md font-bold uppercase tracking-wider">Terminal</span>
        </div>
      </div>

      <!-- MASAÜSTÜ / TABLET ÜST GEZİNME (ASLA KAYBOLMAZ) -->
      <nav class="hidden sm:flex items-center gap-1 bg-slate-900/80 p-1 rounded-xl border border-white/5 text-xs font-semibold">
        <button onclick="navigate('markets')" id="top-nav-markets" class="px-3.5 py-1.5 rounded-lg bg-indigo-600 text-white transition">Piyasalar</button>
        <button onclick="navigate('top100')" id="top-nav-top100" class="px-3.5 py-1.5 rounded-lg text-slate-400 hover:text-white transition">Top 100</button>
        <button onclick="navigate('zarla')" id="top-nav-zarla" class="px-3.5 py-1.5 rounded-lg text-slate-400 hover:text-white transition flex items-center gap-1"><span>🎲</span> Zarla</button>
        <button onclick="navigate('corporate')" id="top-nav-corporate" class="px-3.5 py-1.5 rounded-lg text-slate-400 hover:text-white transition">Kurumsal</button>
      </nav>

      <!-- SAĞ PROFİL BUTONU (TIKLANABİLİR DOKUNMATİK ALAN) -->
      <div onclick="navigate('portfolio')" class="cursor-pointer flex items-center gap-3 p-1.5 pr-3 bg-slate-900/60 hover:bg-slate-800/80 border border-white/5 hover:border-white/10 rounded-2xl transition">
        <div class="w-7 h-7 rounded-xl bg-gradient-to-tr from-emerald-500/20 to-indigo-500/20 border border-emerald-500/30 flex items-center justify-center text-xs font-black text-emerald-400">
          👤
        </div>
        <div class="flex flex-col text-right">
          <span id="header-username" class="text-xs font-bold text-slate-200 leading-tight">Piyasa_Uzmanı</span>
          <span class="text-[10px] text-slate-400 font-semibold tabular">
            <strong id="header-balance" class="text-emerald-400 font-bold">500</strong> VERA
          </span>
        </div>
      </div>

    </div>
  </header>

  <!-- KATEGORİ ŞERİDİ (FİLTRE ÇALIŞIR HALE GETİRİLDİ) -->
  <div class="max-w-6xl mx-auto px-4 pt-4 w-full">
    <div id="category-bar" class="flex gap-2 overflow-x-auto no-scrollbar pb-1 text-xs"></div>
  </div>

  <!-- ANA İÇERİK KONTEYNERİ -->
  <main class="max-w-6xl mx-auto px-4 py-4 space-y-6 w-full flex-1">

    <!-- 1. GÖRÜNÜM: PİYASALAR -->
    <section id="view-markets" class="space-y-6">
      
      <!-- GÜNÜN 3 HIZLI SORUSU -->
      <div class="glass-card rounded-3xl p-5 space-y-3.5 relative overflow-hidden">
        <div class="absolute -right-10 -top-10 w-40 h-40 bg-indigo-500/5 rounded-full blur-3xl pointer-events-none"></div>
        <div class="flex justify-between items-center text-xs">
          <div class="flex items-center gap-2">
            <span class="w-2 h-2 rounded-full bg-amber-400 animate-ping"></span>
            <span class="font-extrabold text-slate-100 tracking-wide uppercase text-[11px]">Günün 3 Hızlı Sorusu</span>
            <span class="text-[10px] text-amber-400 bg-amber-400/10 border border-amber-400/20 px-2 py-0.5 rounded-full font-bold">+50 VERA</span>
          </div>
          <span id="daily-status-counter" class="text-[11px] text-slate-400 font-medium">1/3 Yanıtlandı</span>
        </div>
        <div id="daily-questions-list" class="space-y-2.5"></div>
      </div>

      <!-- GÜNÜN MANŞET İKİLİSİ (CANLI GERİ SAYIM) -->
      <div class="space-y-3">
        <div class="flex justify-between items-center text-xs">
          <h2 class="font-bold text-slate-400 uppercase tracking-wider flex items-center gap-1.5 text-[11px]">
            <span>⭐</span> Günün Manşet İkilisi
          </h2>
          <span class="text-slate-500 text-[11px]">Canlı Likidite Havuzu</span>
        </div>
        <div id="hero-markets-grid" class="grid grid-cols-1 md:grid-cols-2 gap-4"></div>
      </div>

      <!-- TOP 10 DERİNLİK TAHTASI -->
      <div class="space-y-3">
        <div class="flex justify-between items-center text-xs">
          <h2 class="font-bold text-slate-400 uppercase tracking-wider flex items-center gap-1.5 text-[11px]">
            <span>🔥</span> En Çok Tahmin Edilenler
          </h2>
          <span class="text-slate-500 text-[11px]">1 Kazanan Pay = 1.00 VERA (1 TL)</span>
        </div>
        <div id="top10-depth-board" class="glass-card rounded-3xl overflow-hidden divide-y divide-white/5"></div>
      </div>

    </section>

    <!-- 2. GÖRÜNÜM: TOP 100 ANALİST LİGİ -->
    <section id="view-top100" class="hidden space-y-4">
      <div class="border-b border-white/5 pb-3">
        <h2 class="text-xl font-black text-white">🏆 OYVER Analist Ligi</h2>
        <p class="text-xs text-slate-400 mt-0.5">Analist Yetenek Puanı (AYP) ve Brier Skoru sıralaması. Asgari 15 sonuçlanmış tahmin gerekir.</p>
      </div>
      <div id="top100-list" class="glass-card rounded-3xl divide-y divide-white/5"></div>
    </section>

    <!-- 3. GÖRÜNÜM: 🎲 ZARLA ARENASI -->
    <section id="view-zarla" class="hidden space-y-4 max-w-lg mx-auto">
      <div class="text-center space-y-1">
        <h2 class="text-2xl font-black text-amber-400 flex items-center justify-center gap-2"><span>🎲</span> ZARLA</h2>
        <p class="text-xs text-slate-400">Tüketici ikilemlerini oyla, kitleyle uyuşma oranını anında gör.</p>
      </div>
      <div id="zarla-card" class="glass-card rounded-3xl p-6 space-y-5 text-center shadow-2xl"></div>
    </section>

    <!-- 4. GÖRÜNÜM: PORTFÖY & CÜZDAN (ERKEN SATIŞ / CASHOUT) -->
    <section id="view-portfolio" class="hidden space-y-6 max-w-2xl mx-auto">
      
      <!-- CÜZDAN KARTI -->
      <div class="glass-card rounded-3xl p-6 space-y-4">
        <div class="flex justify-between items-start">
          <div>
            <div class="flex items-center gap-2">
              <h3 id="prof-username" class="text-lg font-black text-white">Piyasa_Uzmanı</h3>
              <span class="text-[9px] font-black px-2 py-0.5 rounded-full bg-indigo-500/10 text-indigo-400 border border-indigo-500/30">KIDEMLİ</span>
            </div>
            <div class="text-xs text-slate-400 mt-1">AYP Reytingi: <strong class="text-white">716</strong> • Seri: <strong class="text-amber-400">3 Gün 🔥</strong></div>
          </div>
          <div class="text-right">
            <div id="portfolio-balance" class="text-2xl font-black text-emerald-400 tabular">500 VERA</div>
            <div class="text-[10px] text-slate-500">1 VERA = 1.00 TL Nominal</div>
          </div>
        </div>

        <!-- GSM DOĞRULAMA -->
        <div id="gsm-box" class="p-4 bg-slate-950/70 rounded-2xl border border-indigo-500/20 space-y-2">
          <div class="flex justify-between items-center text-xs">
            <span class="font-bold text-white">📱 Telefon Doğrulaması (+9.500 VERA)</span>
            <span class="text-[10px] text-amber-400 font-bold bg-amber-400/10 px-2 py-0.5 rounded-full">BEKLİYOR</span>
          </div>
          <p class="text-[11px] text-slate-400">Bot hesapları engellemek için numaranızı onaylayıp hoş geldin bakiyenizi açın.</p>
          <div class="flex gap-2 pt-1">
            <input type="text" id="gsm-phone" placeholder="+905XXXXXXXXX" class="flex-1 bg-slate-900 border border-white/10 rounded-xl px-3 py-2 text-xs text-white focus:outline-none focus:border-indigo-500">
            <input type="text" id="gsm-code" placeholder="123456" class="w-24 bg-slate-900 border border-white/10 rounded-xl px-3 py-2 text-xs text-white text-center focus:outline-none focus:border-indigo-500">
            <button onclick="verifyPhone()" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-500 text-white rounded-xl text-xs font-bold transition">Onayla</button>
          </div>
        </div>
      </div>

      <!-- AÇIK POZİSYONLAR & ERKEN SATIŞ -->
      <div class="space-y-3">
        <h3 class="text-xs font-bold text-slate-400 uppercase tracking-wider">Açık Pozisyonlar & Kâr Al (Erken Satış)</h3>
        <div id="positions-container" class="space-y-3"></div>
      </div>
    </section>

    <!-- 5. GÖRÜNÜM: KURUMSAL & HAKKIMIZDA & İLETİŞİM -->
    <section id="view-corporate" class="hidden space-y-6 max-w-3xl mx-auto">
      <div class="glass-card rounded-3xl p-6 space-y-6">
        <div class="border-b border-white/5 pb-4">
          <h2 class="text-xl font-black text-white">OYVER Kurumsal & Şeffaflık Raporu</h2>
          <p class="text-xs text-slate-400 mt-1">Türkiye'nin kolektif istihbarat ve tahmin piyasası altyapısı.</p>
        </div>

        <div class="space-y-3">
          <h3 class="text-sm font-bold text-indigo-400 uppercase tracking-wider">Biz Kimiz?</h3>
          <p class="text-xs text-slate-300 leading-relaxed">
            OYVER; ekonomi, borsa, teknoloji ve toplumsal gündemdeki belirsizlikleri kitle zekâsıyla öngören bağımsız bir tahmin ve araştırma terminalidir. Sanal puan tabanlı çift kayıtlı defter altyapısıyla çalışır; şansa değil, rasyonel analize ve araştırma disiplinine dayanır.
          </p>
        </div>

        <div class="space-y-3">
          <h3 class="text-sm font-bold text-indigo-400 uppercase tracking-wider">Çözümleme ve Hakemlik İlkeleri</h3>
          <p class="text-xs text-slate-300 leading-relaxed">
            Her tahmin pazarı, yalnızca önceden ilan edilen resmî birincil kaynaklarla (T.C. Merkez Bankası bültenleri, Resmî Gazete tebliğleri, TFF hakem raporları, TÜİK bültenleri) çözümlenir. İkincil yorum veya söylentiler bağlayıcı kabul edilmez.
          </p>
        </div>

        <div class="space-y-3">
          <h3 class="text-sm font-bold text-indigo-400 uppercase tracking-wider">Hukuki Sınırlar & Regülasyon</h3>
          <p class="text-xs text-slate-300 leading-relaxed">
            OYVER üzerindeki VERA, kapalı devre bir oyunlaştırma ve analitik itibar göstergesidir; doğrudan nakdi bir karşılığı yoktur. Platformumuz 7258 sayılı Bahis Kanunu ve 6362 sayılı SPK Kanunu'nun piyasa dolandırıcılığı sınırları gözetilerek tasarlanmıştır.
          </p>
        </div>

        <div class="p-4 bg-slate-950/80 rounded-2xl border border-white/5 space-y-2">
          <h4 class="text-xs font-bold text-white">Kurumsal İletişim & Sponsorluk</h4>
          <p class="text-xs text-slate-400">Pazar araştırmaları, kurumsal tahmin kupaları ve API entegrasyonu için:</p>
          <div class="text-xs font-mono text-indigo-400">iletisim@oyver.pro • kurumsal@oyver.pro</div>
        </div>
      </div>
    </section>

  </main>

  <!-- İŞLEM MODALI (SLIPPAGE / FİYAT KAYMASI HESAPLAYICILI) -->
  <div id="modal-backdrop" onclick="closeModal()" class="fixed inset-0 bg-black/80 backdrop-blur-md z-50 hidden transition-opacity"></div>
  <div id="trade-modal" class="fixed bottom-0 sm:bottom-auto sm:top-1/2 sm:left-1/2 sm:-translate-x-1/2 sm:-translate-y-1/2 w-full sm:max-w-md bg-slate-900 border-t sm:border border-white/10 rounded-t-3xl sm:rounded-3xl p-6 z-50 hidden space-y-4 shadow-2xl">
    <div id="modal-content"></div>
  </div>

  <!-- MOBİL / TABLET SABİT ALT NAVİGASYON (HER ZAMAN GÖRÜNÜR) -->
  <nav class="sm:hidden fixed bottom-0 left-0 right-0 bg-[#07090E]/95 border-t border-white/10 backdrop-blur-xl px-4 py-2.5 z-40 flex justify-around items-center text-[10px] font-bold text-slate-400">
    <button onclick="navigate('markets')" id="m-nav-markets" class="flex flex-col items-center gap-1 text-indigo-400">
      <span class="text-base">🌐</span>
      <span>Piyasalar</span>
    </button>
    <button onclick="navigate('top100')" id="m-nav-top100" class="flex flex-col items-center gap-1 hover:text-white">
      <span class="text-base">🏆</span>
      <span>Top 100</span>
    </button>
    <button onclick="navigate('zarla')" id="m-nav-zarla" class="flex flex-col items-center gap-1 hover:text-white">
      <span class="text-base">🎲</span>
      <span>Zarla</span>
    </button>
    <button onclick="navigate('portfolio')" id="m-nav-portfolio" class="flex flex-col items-center gap-1 hover:text-white">
      <span class="text-base">💼</span>
      <span>Portföy</span>
    </button>
    <button onclick="navigate('corporate')" id="m-nav-corporate" class="flex flex-col items-center gap-1 hover:text-white">
      <span class="text-base">🏛️</span>
      <span>Kurumsal</span>
    </button>
  </nav>

  <!-- İSTEMCİ JAVASCRIPT KONTROL MOTORU -->
  <script>
    let appData = null;
    let selectedMarket = null;
    let selectedOutcome = 'YES';
    let currentCategory = 'Hepsi';
    let currentZarlaIndex = 0;

    function showToast(msg, isSuccess = true) {
      const toast = document.getElementById('toast');
      const icon = document.getElementById('toast-icon');
      document.getElementById('toast-msg').innerText = msg;
      icon.innerText = isSuccess ? '✓' : '⚠️';
      icon.className = isSuccess ? 'text-emerald-400 font-bold' : 'text-rose-400 font-bold';
      toast.classList.remove('-translate-y-28', 'opacity-0');
      setTimeout(() => toast.classList.add('-translate-y-28', 'opacity-0'), 3200);
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
      // Header ve Portföy
      document.getElementById('header-balance').innerText = Math.round(appData.currentUser.balanceTotal).toLocaleString();
      document.getElementById('header-username').innerText = appData.currentUser.username;
      document.getElementById('prof-username').innerText = appData.currentUser.username;
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
      bar.innerHTML = appData.categories.map(c => \`
        <button onclick="filterCategory('\${c}')" class="px-3.5 py-1.5 rounded-xl font-bold whitespace-nowrap transition text-xs \${
          currentCategory === c 
            ? 'bg-indigo-600 text-white shadow-lg shadow-indigo-600/30' 
            : 'bg-slate-900/80 border border-white/5 text-slate-400 hover:text-white'
        }">\${c}</button>
      \`).join('');
    }

    function filterCategory(cat) {
      currentCategory = cat;
      renderCategories();
      renderHeroMarkets();
      renderTop10Depth();
    }

    function renderDailyQuestions() {
      const container = document.getElementById('daily-questions-list');
      container.innerHTML = appData.dailyQuestions.map(q => \`
        <div class="bg-slate-950/70 p-3.5 rounded-2xl border border-white/5 flex flex-col sm:flex-row justify-between sm:items-center gap-2.5 text-xs">
          <div class="min-w-0">
            <span class="text-[10px] text-slate-500 font-bold uppercase tracking-wider">\${q.category} • Kapanış: \${q.closeText}</span>
            <h4 class="font-bold text-slate-200 truncate mt-0.5">\${q.title}</h4>
          </div>
          <div class="flex items-center gap-2 shrink-0">
            \${q.userAnswer ? \`
              <span class="px-3 py-1 rounded-xl text-[10px] font-black bg-emerald-500/10 text-emerald-400 border border-emerald-500/30">
                Seçim: \${q.userAnswer}
              </span>
            \` : \`
              <button onclick="submitDailyAnswer(\${q.id}, 'YES')" class="px-3 py-1.5 rounded-xl font-bold bg-slate-900 border border-emerald-500/30 hover:bg-emerald-500/20 text-emerald-400 transition">EVET (%\${q.probYes})</button>
              <button onclick="submitDailyAnswer(\${q.id}, 'NO')" class="px-3 py-1.5 rounded-xl font-bold bg-slate-900 border border-rose-500/30 hover:bg-rose-500/20 text-rose-400 transition">HAYIR (%\${q.probNo})</button>
            \`}
          </div>
        </div>
      \`).join('');
    }

    async function submitDailyAnswer(id, outcome) {
      const res = await fetch('/api/daily/answer', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ questionId: id, outcome })
      });
      const data = await res.json();
      if (data.success) {
        showToast(data.message);
        const q = appData.dailyQuestions.find(x => x.id === id);
        if (q) q.userAnswer = outcome;
        appData.currentUser.balanceTotal = data.newBalance;
        document.getElementById('header-balance').innerText = Math.round(data.newBalance).toLocaleString();
        renderDailyQuestions();
      }
    }

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
      const filtered = appData.markets.filter(m => currentCategory === 'Hepsi' || m.category === currentCategory);
      const heroes = filtered.filter(m => m.isHero);
      const container = document.getElementById('hero-markets-grid');

      if (heroes.length === 0) {
        container.innerHTML = '<div class="col-span-2 glass-card p-6 rounded-2xl text-center text-xs text-slate-500">Bu kategoride öne çıkan manşet pazar bulunamadı.</div>';
        return;
      }

      container.innerHTML = heroes.map(m => \`
        <div class="glass-card glass-card-hover rounded-3xl p-5 flex flex-col justify-between space-y-4">
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
              <span>1 Kazanan Pay = 1.00 VERA</span>
              <span>👥 \${m.totalPredictionsCount} Tahmin</span>
            </div>
          </div>
        </div>
      \`).join('');
    }

    function renderTop10Depth() {
      const filtered = appData.markets.filter(m => currentCategory === 'Hepsi' || m.category === currentCategory);
      const container = document.getElementById('top10-depth-board');

      if (filtered.length === 0) {
        container.innerHTML = '<div class="p-6 text-center text-xs text-slate-500">Pazar bulunamadı.</div>';
        return;
      }

      container.innerHTML = filtered.map((m, idx) => \`
        <div onclick="openTrade(\${m.id}, 'YES')" class="p-4 flex items-center justify-between gap-4 text-xs hover:bg-white/5 transition cursor-pointer">
          <div class="flex items-center gap-3 min-w-0">
            <span class="font-black text-slate-500 w-5">#\${idx + 1}</span>
            <span class="font-bold text-slate-200 truncate">\${m.title}</span>
          </div>
          <div class="flex items-center gap-4 shrink-0">
            <span class="text-slate-500 text-[11px] hidden sm:inline">\${m.totalPredictionsCount} Tahmin</span>
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
              <div class="text-[10px] text-slate-500">\${a.settledCount} Pazar Tahmini</div>
            </div>
            <span class="text-[9px] font-black px-2 py-0.5 rounded-full \${
              a.tier === 'USTA' ? 'bg-amber-400/10 text-amber-400 border border-amber-400/30' : 'bg-indigo-400/10 text-indigo-400 border border-indigo-400/30'
            }">\${a.tier}</span>
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
        <div class="text-[11px] text-indigo-400 font-bold uppercase tracking-wider">\${currentZarlaIndex + 1} / \${appData.zarlaQuestions.length} • \${q.sponsor}</div>
        <h3 class="text-xl font-black text-white">\${q.itemA} mı, \${q.itemB} mi?</h3>
        <div class="grid grid-cols-2 gap-3 pt-2">
          <button onclick="voteZarla()" class="py-7 rounded-3xl bg-slate-950/80 border border-white/10 hover:border-indigo-500 text-sm font-black transition">
            \${q.itemA}
            <div class="text-[11px] text-slate-400 font-normal mt-1">%\${pctA} Kitle Oyu</div>
          </button>
          <button onclick="voteZarla()" class="py-7 rounded-3xl bg-slate-950/80 border border-white/10 hover:border-indigo-500 text-sm font-black transition">
            \${q.itemB}
            <div class="text-[11px] text-slate-400 font-normal mt-1">%\${pctB} Kitle Oyu</div>
          </button>
        </div>
      \`;
    }

    function voteZarla() {
      if (currentZarlaIndex < appData.zarlaQuestions.length - 1) {
        currentZarlaIndex++;
        renderZarla();
      } else {
        showToast('Zarla anketi tamamlandı! Tercihlerin kitleyle kaydedildi.');
        currentZarlaIndex = 0;
        renderZarla();
      }
    }

    function renderPositions() {
      const container = document.getElementById('positions-container');
      if (!appData.currentUser.positions || appData.currentUser.positions.length === 0) {
        container.innerHTML = '<div class="p-6 text-center text-xs text-slate-500 glass-card rounded-2xl">Açık pozisyonunuz bulunmuyor.</div>';
        return;
      }

      container.innerHTML = appData.currentUser.positions.map(p => \`
        <div class="glass-card p-4 rounded-2xl flex justify-between items-center text-xs">
          <div class="space-y-1">
            <div class="flex items-center gap-2">
              <span class="text-[9px] font-black px-2 py-0.5 rounded-md \${p.outcome === 'YES' ? 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30' : 'bg-rose-500/20 text-rose-400 border border-rose-500/30'}">\${p.outcome}</span>
              <h4 class="font-bold text-white">\${p.marketTitle}</h4>
            </div>
            <div class="text-[11px] text-slate-400">Yatırılan: \${Math.round(p.totalCostVera)} VERA • Pay: \${p.sharesCount}</div>
          </div>
          <div class="text-right space-y-1.5">
            <div class="font-bold \${p.pnlVera >= 0 ? 'text-emerald-400' : 'text-rose-400'}">
              \${p.pnlVera >= 0 ? '+' : ''}\${p.pnlVera} VERA (%\${p.pnlPercent})
            </div>
            <button onclick="cashoutPosition(\${p.id})" class="px-3 py-1.5 bg-amber-500/10 hover:bg-amber-500/20 border border-amber-500/30 text-amber-300 rounded-xl font-bold text-[10px] transition">
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
          <span class="text-[10px] font-bold text-indigo-400 bg-indigo-500/10 border border-indigo-500/20 px-2 py-0.5 rounded-md uppercase">\${m.category}</span>
          <h3 class="font-bold text-white text-sm leading-snug mt-1">\${m.title}</h3>
        </div>

        <div class="grid grid-cols-2 gap-2 pt-2">
          <button onclick="selectedOutcome='YES'; renderModal()" class="py-2.5 rounded-2xl font-bold text-xs border transition \${selectedOutcome === 'YES' ? 'bg-emerald-500 border-emerald-400 text-slate-950 font-black' : 'bg-slate-950 border-white/10 text-slate-400'}">EVET (%\${m.probYes})</button>
          <button onclick="selectedOutcome='NO'; renderModal()" class="py-2.5 rounded-2xl font-bold text-xs border transition \${selectedOutcome === 'NO' ? 'bg-rose-500 border-rose-400 text-slate-950 font-black' : 'bg-slate-950 border-white/10 text-slate-400'}">HAYIR (%\${m.probNo})</button>
        </div>

        <div class="space-y-1.5 pt-2">
          <label class="text-[11px] text-slate-400 font-bold">Yatırılacak Tutar (VERA)</label>
          <input type="number" id="trade-input" value="100" class="w-full bg-slate-950 border border-white/10 rounded-2xl px-4 py-2.5 text-base font-bold text-white text-center focus:outline-none focus:border-indigo-500">
          <div class="flex justify-between text-[11px] text-slate-400 pt-1">
            <span>Bakiye: \${Math.round(appData.currentUser.balanceTotal)} VERA</span>
            <span>1 Pay = 1.00 VERA</span>
          </div>
        </div>

        <div class="p-3 bg-slate-950/80 rounded-2xl border border-white/5 text-[11px] text-slate-400 space-y-1">
          <div class="font-bold text-slate-300">Çözümleme Belgesi</div>
          <p class="leading-relaxed">\${m.rules}</p>
        </div>

        <button onclick="executeBuy()" class="w-full py-3.5 bg-indigo-600 hover:bg-indigo-500 text-white rounded-2xl font-bold text-xs shadow-xl shadow-indigo-600/30 transition">Tahmini Onayla</button>
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
        showToast(data.message, false);
      }
    }

    async function cashoutPosition(posId) {
      if (!confirm('Pozisyonunuzu anlık piyasa fiyatından erken satarak nakde dönmek istiyor musunuz?')) return;
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
        showToast(data.message, false);
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
      showToast(data.message, data.success);
      if (data.success) loadData();
    }

    // EVRENSEL VE GARANTİLİ NAVİGASYON (HER EKRANDA ÇALIŞIR)
    function navigate(tab) {
      ['markets', 'top100', 'zarla', 'portfolio', 'corporate'].forEach(t => {
        const el = document.getElementById('view-' + t);
        if (el) el.classList.add('hidden');

        // Üst Menü Aktifliği
        const topBtn = document.getElementById('top-nav-' + t);
        if (topBtn) {
          topBtn.className = 'px-3.5 py-1.5 rounded-lg text-slate-400 hover:text-white transition';
        }

        // Alt Menü Aktifliği
        const mBtn = document.getElementById('m-nav-' + t);
        if (mBtn) {
          mBtn.className = 'flex flex-col items-center gap-1 text-slate-400 hover:text-white';
        }
      });

      const activeView = document.getElementById('view-' + tab);
      if (activeView) activeView.classList.remove('hidden');

      const activeTop = document.getElementById('top-nav-' + tab);
      if (activeTop) activeTop.className = 'px-3.5 py-1.5 rounded-lg bg-indigo-600 text-white transition';

      const activeMob = document.getElementById('m-nav-' + tab);
      if (activeMob) activeMob.className = 'flex flex-col items-center gap-1 text-indigo-400';

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
