import Fastify from 'fastify';
import cors from '@fastify/cors';
import helmet from '@fastify/helmet';
import WebSocket, { WebSocketServer } from 'ws';
import { db } from './db';
import { markets, users, positions, veraTransactions } from './db/schema';
import { eq, and } from 'drizzle-orm';
import { AmmEngine } from './modules/amm/engine';
import { SYSTEM_CONFIG } from './config/system';

const app = Fastify({ logger: false });

async function bootstrap() {
  await app.register(helmet, { contentSecurityPolicy: false });
  await app.register(cors, { origin: '*' });

  async function getOrCreateActiveUser() {
    let user = await db.query.users.findFirst();
    if (!user) {
      const [newUser] = await db.insert(users).values({
        username: 'Piyasa_Uzmani',
        balanceVeraPromo: '500.0000',
        balanceVeraWithdrawable: '0.0000',
        isPhoneVerified: false
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

  async function seedMarketsIfEmpty() {
    try {
      const existing = await db.query.markets.findFirst();
      if (!existing) {
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
            rules: 'Normal süre ve uzatmalarda toplam gol sayısı en az 3 ise EVET sayılır.',
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
            rules: 'Resmî Gazete\'de yayımlanan tebliğde net tutar 30.000 TL ve üstü ise EVET sayılır.',
            sourceName: 'Resmî Gazete Tebliği',
            startsAt: now,
            closesAt: new Date(now.getTime() + 92 * 24 * 60 * 60 * 1000),
            poolYes: '7250.0000',
            poolNo: '17750.0000',
            volumeVera: '25000.0000',
            totalPredictionsCount: 42,
            isHero: false
          }
        ]);
      }
    } catch (e) {
      console.log('Tablo kontrolü tamamlandı.');
    }
  }

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
        positions: enrichedPositions
      },
      markets: enrichedMarkets
    };
  });

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

  app.get('/', async (_req, reply) => {
    reply.type('text/html; charset=utf-8');
    return `
<!DOCTYPE html>
<html lang="tr">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>OYVER — Bilgi ve Tahmin Pazarı</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');
    body { background-color: #030712; color: #f8fafc; font-family: 'Inter', sans-serif; }
    .tabular { font-variant-numeric: tabular-nums; }
  </style>
</head>
<body class="min-h-screen pb-24 text-slate-100">

  <div id="toast" class="fixed top-4 right-4 z-50 transform -translate-y-24 opacity-0 transition-all duration-300 bg-slate-900 border border-slate-700 text-white text-xs px-4 py-3 rounded-xl shadow-2xl flex items-center gap-2">
    <span id="toast-msg">Bildirim</span>
  </div>

  <header class="sticky top-0 z-40 bg-slate-950/90 backdrop-blur-md border-b border-slate-800 px-4 py-3">
    <div class="max-w-6xl mx-auto flex items-center justify-between">
      <div class="flex items-center gap-2 cursor-pointer" onclick="navigate('markets')">
        <div class="w-8 h-8 rounded-lg bg-indigo-600 flex items-center justify-center font-black text-white text-base shadow-lg shadow-indigo-600/30">O</div>
        <span class="font-extrabold text-lg text-white">OYVER</span>
        <span class="text-[10px] bg-slate-800 text-slate-400 px-2 py-0.5 rounded font-bold ml-2">PRO</span>
      </div>

      <div class="flex items-center gap-3">
        <div onclick="navigate('portfolio')" class="cursor-pointer bg-slate-900 border border-slate-800 hover:border-slate-700 rounded-xl px-3 py-1.5 flex items-center gap-2">
          <span class="text-xs text-slate-400">Bakiye:</span>
          <span id="header-balance" class="font-bold text-xs text-emerald-400 tabular">500</span>
          <span class="text-[10px] font-black text-indigo-400 bg-indigo-950 px-1.5 py-0.5 rounded border border-indigo-900">VERA</span>
        </div>
      </div>
    </div>
  </header>

  <main class="max-w-6xl mx-auto px-4 py-4 space-y-6">

    <section id="tab-markets" class="space-y-6">
      <div class="space-y-3">
        <div class="flex justify-between items-center">
          <h2 class="text-xs font-bold text-slate-400 uppercase tracking-wider flex items-center gap-1.5">
            <span>⭐</span> Canlı Tahmin Pazarları
          </h2>
          <span class="text-[11px] text-slate-500">1 VERA = 1.00 TL</span>
        </div>
        <div id="markets-container" class="grid grid-cols-1 md:grid-cols-2 gap-4"></div>
      </div>
    </section>

    <section id="tab-portfolio" class="hidden space-y-6 max-w-2xl mx-auto">
      <div class="bg-slate-900 border border-slate-800 rounded-2xl p-5 space-y-4">
        <div class="flex justify-between items-center">
          <div>
            <h3 class="text-base font-black text-white">Cüzdanım</h3>
            <p class="text-xs text-slate-400">Pazar bitmeden kâr alıp çıkabilir veya sonucu bekleyebilirsiniz.</p>
          </div>
          <div class="text-right">
            <div id="portfolio-balance" class="text-xl font-black text-emerald-400 tabular">500 VERA</div>
            <div class="text-[10px] text-slate-500">1 VERA = 1.00 TL Nominal</div>
          </div>
        </div>

        <div id="gsm-box" class="p-4 bg-slate-950 rounded-xl border border-indigo-900/60 space-y-2">
          <div class="flex justify-between items-center text-xs">
            <span class="font-bold text-white">📱 Telefon Doğrulaması (+9.500 VERA)</span>
            <span class="text-[10px] text-amber-400 font-black">BEKLİYOR</span>
          </div>
          <p class="text-[11px] text-slate-400">Tekil Türkiye GSM numaranızı doğrulayarak hoş geldin bakiyenizi tamamlayın.</p>
          <div class="flex gap-2">
            <input type="text" id="gsm-phone" placeholder="+905XXXXXXXXX" class="flex-1 bg-slate-900 border border-slate-800 rounded-xl px-3 py-1.5 text-xs text-white">
            <input type="text" id="gsm-code" placeholder="123456" class="w-24 bg-slate-900 border border-slate-800 rounded-xl px-3 py-1.5 text-xs text-white text-center">
            <button onclick="verifyPhone()" class="px-3 py-1.5 bg-indigo-600 text-white rounded-xl text-xs font-bold">Onayla</button>
          </div>
        </div>
      </div>

      <div class="space-y-3">
        <h3 class="text-xs font-bold text-slate-400 uppercase tracking-wider">Açık Pozisyonlar & Erken Nakde Çıkış (Kâr Al)</h3>
        <div id="positions-container" class="space-y-3"></div>
      </div>
    </section>

  </main>

  <div id="modal-backdrop" onclick="closeModal()" class="fixed inset-0 bg-black/80 backdrop-blur-sm z-50 hidden"></div>
  <div id="trade-modal" class="fixed bottom-0 md:top-1/2 md:left-1/2 md:-translate-x-1/2 md:-translate-y-1/2 w-full md:max-w-md bg-slate-900 border border-slate-800 rounded-t-3xl md:rounded-3xl p-6 z-50 hidden space-y-4">
    <div id="modal-content"></div>
  </div>

  <nav class="md:hidden fixed bottom-0 left-0 right-0 bg-slate-950/95 border-t border-slate-800 px-8 py-3 z-40 flex justify-around text-xs font-bold text-slate-400">
    <button onclick="navigate('markets')" id="btn-nav-mkt" class="text-indigo-400 flex flex-col items-center gap-1">
      <span>🌐</span> <span>Piyasalar</span>
    </button>
    <button onclick="navigate('portfolio')" id="btn-nav-port" class="hover:text-white flex flex-col items-center gap-1">
      <span>💼</span> <span>Portföyüm</span>
    </button>
  </nav>

  <script>
    let appData = null;
    let selectedMarket = null;
    let selectedOutcome = 'YES';

    function showToast(msg) {
      const toast = document.getElementById('toast');
      document.getElementById('toast-msg').innerText = msg;
      toast.classList.remove('-translate-y-24', 'opacity-0');
      setTimeout(() => toast.classList.add('-translate-y-24', 'opacity-0'), 3000);
    }

    async function loadData() {
      const res = await fetch('/api/state');
      appData = await res.json();
      renderUI();
    }

    function renderUI() {
      document.getElementById('header-balance').innerText = Math.round(appData.currentUser.balanceTotal).toLocaleString();
      document.getElementById('portfolio-balance').innerText = Math.round(appData.currentUser.balanceTotal).toLocaleString() + ' VERA';

      if (appData.currentUser.isPhoneVerified) {
        document.getElementById('gsm-box').classList.add('hidden');
      }

      renderMarkets();
      renderPositions();
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

    function renderMarkets() {
      const container = document.getElementById('markets-container');
      container.innerHTML = appData.markets.map(m => \`
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

    setInterval(() => {
      document.querySelectorAll('.countdown-timer').forEach(el => {
        el.innerText = formatCountdown(el.getAttribute('data-target'));
      });
    }, 1000);

    function renderPositions() {
      const container = document.getElementById('positions-container');
      if (appData.currentUser.positions.length === 0) {
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
      if (tab === 'markets') {
        document.getElementById('tab-markets').classList.remove('hidden');
        document.getElementById('tab-portfolio').classList.add('hidden');
        document.getElementById('btn-nav-mkt').className = 'text-indigo-400 flex flex-col items-center gap-1';
        document.getElementById('btn-nav-port').className = 'text-slate-400 flex flex-col items-center gap-1';
      } else {
        document.getElementById('tab-markets').classList.add('hidden');
        document.getElementById('tab-portfolio').classList.remove('hidden');
        document.getElementById('btn-nav-mkt').className = 'text-slate-400 flex flex-col items-center gap-1';
        document.getElementById('btn-nav-port').className = 'text-indigo-400 flex flex-col items-center gap-1';
      }
    }

    window.onload = loadData;
  </script>
</body>
</html>
    `;
  });

  const PORT = Number(process.env.PORT) || 3000;
  const HOST = '0.0.0.0';

  await app.listen({ port: PORT, host: HOST });
  console.log(`OYVER Motoru ayakta: http://${HOST}:${PORT}`);

  const wss = new WebSocketServer({ server: app.server });
  function broadcast(payload: any) {
    const data = JSON.stringify(payload);
    wss.clients.forEach(c => {
      if (c.readyState === WebSocket.OPEN) c.send(data);
    });
  }

  await seedMarketsIfEmpty();
}

bootstrap().catch(err => {
  console.error('Başlatma hatası:', err);
  process.exit(1);
});
