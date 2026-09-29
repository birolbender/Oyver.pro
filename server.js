const http = require('http');
const WebSocket = require('ws');

// ============================================================================
// 1. OYVER ÇEKİRDEK VERİ MODELİ VE DURUM YÖNETİMİ
// ============================================================================
const state = {
  activeSeason: {
    id: 1,
    name: 'Ekim 2026 Sezonu',
    daysLeft: 14,
    rewardPool: '100.000 OVP Değerinde Sponsorluk Ödülü'
  },
  users: {
    'demo-user': {
      id: 'demo-user',
      username: 'Tahminci_Leisan',
      balance: 10000,
      isPro: true,
      rank: 42,
      positions: []
    }
  },
  categories: ['TÜMÜ', 'EKONOMİ & FİNANS', 'SPOR', 'BIST & ŞİRKETLER', 'TEKNOLOJİ & AI', 'TÜRKİYE GÜNDEMİ'],
  markets: [
    {
      id: 1,
      slug: 'tcmb-faiz-karari-ekim-2026',
      title: 'TCMB Bir Sonraki Toplantıda Politika Faizini İndirecek mi?',
      category: 'EKONOMİ & FİNANS',
      closeTime: '25 Ekim 2026 14:00',
      yesPrice: 66,
      volumeOvp: 485200,
      participantsCount: 1420,
      verifiedSource: 'https://tcmb.gov.tr',
      sourceName: 'TCMB Resmi Basın Duyuruları',
      rules: `1. Bu pazar, Türkiye Cumhuriyet Merkez Bankası (TCMB) Para Politikası Kurulu (PPK) toplantısı sonrasında açıklanacak politika faizi kararına göre sonuçlandırılır.
2. Karar metninde bir haftalık repo faiz oranında indirim yapıldığı resmi olarak açıklanırsa pazar "EVET" olarak sonuçlanır.
3. Faizin sabit bırakılması veya artırılması durumunda pazar "HAYIR" olarak çözümlenir.
4. Çözümleme kararı resmi tcmb.gov.tr açıklamasıyla kesinleşir.`,
      history: [
        { time: '23 Eyl', price: 58 },
        { time: '24 Eyl', price: 61 },
        { time: '25 Eyl', price: 59 },
        { time: '26 Eyl', price: 64 },
        { time: '27 Eyl', price: 63 },
        { time: '28 Eyl', price: 65 },
        { time: '29 Eyl', price: 66 }
      ],
      orderBook: {
        bids: [{ price: 65, shares: 1250 }, { price: 64, shares: 3400 }, { price: 63, shares: 5100 }],
        asks: [{ price: 67, shares: 980 }, { price: 68, shares: 2100 }, { price: 69, shares: 4300 }]
      },
      comments: [
        { id: 101, user: 'Kerem_Macro', stance: 'YES', text: 'Son enflasyon öncü verileri beklenti altı geldi. 150 baz puanlık sembolik indirim gelecektir.', time: '2 saat önce', likes: 14 },
        { id: 102, user: 'SelinFinans', stance: 'NO', text: 'Kur baskısı devam ederken PPK risk almaz, pas geçer.', time: '4 saat önce', likes: 9 }
      ],
      recentTrades: [
        { id: 't1', user: 'Ahmet_K', outcome: 'YES', amount: 500, price: 66, time: '2 dk önce' },
        { id: 't2', user: 'Canan_T', outcome: 'NO', amount: 1200, price: 34, time: '7 dk önce' }
      ]
    },
    {
      id: 2,
      slug: 'super-lig-derbi-galibiyet',
      title: 'Hafta Sonu Oynanacak Derbiyi Ev Sahibi Takım Kazanacak mı?',
      category: 'SPOR',
      closeTime: '18 Ekim 2026 21:45',
      yesPrice: 48,
      volumeOvp: 290400,
      participantsCount: 890,
      verifiedSource: 'https://tff.org',
      sourceName: 'TFF Resmi Maç Raporu',
      rules: `1. Bu pazar, TFF resmi hakem raporunda belirtilen 90 dakika ve uzatma dakikaları sonundaki nihai skoru baz alır.
2. Ev sahibi takımın galibiyeti durumunda "EVET", beraberlik veya deplasman takımının galibiyeti halinde "HAYIR" geçerli sayılır.
3. Olası yarıda kalma durumunda TFF'nin resmi tescil kararı beklenir.`,
      history: [
        { time: '25 Eyl', price: 52 },
        { time: '26 Eyl', price: 50 },
        { time: '27 Eyl', price: 47 },
        { time: '28 Eyl', price: 49 },
        { time: '29 Eyl', price: 48 }
      ],
      orderBook: {
        bids: [{ price: 47, shares: 800 }, { price: 46, shares: 1400 }],
        asks: [{ price: 49, shares: 950 }, { price: 50, shares: 1800 }]
      },
      comments: [
        { id: 201, user: 'TribunLideri', stance: 'YES', text: 'Sakatlar döndü, taraftar desteğiyle ev sahibi net alır.', time: '1 saat önce', likes: 6 }
      ],
      recentTrades: [
        { id: 't3', user: 'Volkan_9', outcome: 'YES', amount: 250, price: 48, time: '12 dk önce' }
      ]
    }
  ]
};

// ============================================================================
// 2. TİCARET VE VERİ MUTASYON İŞLEMLERİ
// ============================================================================
function executeTrade(marketId, outcome, amountOvp) {
  const market = state.markets.find(m => m.id === marketId);
  const user = state.users['demo-user'];

  if (!market || user.balance < amountOvp || amountOvp <= 0) {
    return { success: false, message: 'Yetersiz OVP bakiyesi veya geçersiz işlem tutarı.' };
  }

  const effectivePrice = outcome === 'YES' ? market.yesPrice : (100 - market.yesPrice);
  const shares = Number((amountOvp / effectivePrice).toFixed(1));

  user.balance -= amountOvp;
  market.volumeOvp += amountOvp;
  market.participantsCount += 1;

  // Algoritmik fiyat kayması (+-%1)
  const shift = outcome === 'YES' ? 1 : -1;
  market.yesPrice = Math.min(99, Math.max(1, market.yesPrice + shift));

  // Tarihçeye son fiyatı ekle
  const todayLabel = 'Bugün';
  const lastHistory = market.history[market.history.length - 1];
  if (lastHistory && lastHistory.time === todayLabel) {
    lastHistory.price = market.yesPrice;
  } else {
    market.history.push({ time: todayLabel, price: market.yesPrice });
  }

  // Kullanıcı portföyüne ekle
  user.positions.push({
    id: Date.now(),
    marketId: market.id,
    marketTitle: market.title,
    outcome,
    shares,
    entryPrice: effectivePrice,
    cost: amountOvp,
    createdAt: new Date().toLocaleTimeString('tr-TR', { hour: '2-digit', minute: '2-digit' })
  });

  // Son işlemlere ekle
  market.recentTrades.unshift({
    id: 't_' + Date.now(),
    user: user.username,
    outcome,
    amount: amountOvp,
    price: effectivePrice,
    time: 'Az önce'
  });

  return {
    success: true,
    newBalance: user.balance,
    newPrice: market.yesPrice,
    shares,
    market
  };
}

function addComment(marketId, text, stance) {
  const market = state.markets.find(m => m.id === marketId);
  const user = state.users['demo-user'];
  if (!market || !text || text.trim().length < 3) return { success: false };

  const newComment = {
    id: Date.now(),
    user: user.username,
    stance: stance || 'NEUTRAL',
    text: text.trim().slice(0, 300),
    time: 'Az önce',
    likes: 0
  };

  market.comments.unshift(newComment);
  return { success: true, comment: newComment };
}

// ============================================================================
// 3. HTTP REST API VE DAĞITIM SUNUCUSU
// ============================================================================
const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  // REST: Global Durum
  if (req.url === '/api/state' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(state));
    return;
  }

  // REST: İşlem Yürütme
  if (req.url === '/api/trade' && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      try {
        const { marketId, outcome, amount } = JSON.parse(body);
        const result = executeTrade(Number(marketId), outcome, Number(amount));
        if (result.success) {
          broadcastPriceUpdate(marketId, result.newPrice, result.market);
        }
        res.writeHead(result.success ? 200 : 400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(result));
      } catch (err) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: false, message: 'Geçersiz veri biçimi.' }));
      }
    });
    return;
  }

  // REST: Yorum Gönderimi
  if (req.url === '/api/comments' && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      try {
        const { marketId, text, stance } = JSON.parse(body);
        const result = addComment(Number(marketId), text, stance);
        res.writeHead(result.success ? 200 : 400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(result));
      } catch (err) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: false }));
      }
    });
    return;
  }

  // ============================================================================
  // 4. FRONTEND SPA ARAYÜZÜ (HTML, CSS, CLIENT LOGIC)
  // ============================================================================
  if (req.url === '/' || req.url.startsWith('/market') || req.url === '/index.html') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(`
<!DOCTYPE html>
<html lang="tr">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
  <title>OYVER — Türkiye'nin Tahmin Pazarı</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');
    body {
      background-color: #030712;
      color: #f8fafc;
      font-family: 'Inter', -apple-system, sans-serif;
      -webkit-tap-highlight-color: transparent;
    }
    .tabular { font-variant-numeric: tabular-nums; }
    .no-scrollbar::-webkit-scrollbar { display: none; }
    .no-scrollbar { -ms-overflow-style: none; scrollbar-width: none; }
  </style>
</head>
<body class="min-h-screen pb-24 selection:bg-indigo-600 selection:text-white">

  <!-- ==================== GLOBAL NAVBAR ==================== -->
  <header class="sticky top-0 z-40 bg-slate-950/80 backdrop-blur-md border-b border-slate-800/80 px-4 py-3">
    <div class="max-w-6xl mx-auto flex items-center justify-between gap-4">
      <div class="flex items-center gap-6 cursor-pointer" onclick="navigateTo('feed')">
        <div class="flex items-center gap-2">
          <div class="w-9 h-9 rounded-xl bg-indigo-600 flex items-center justify-center font-black text-white text-lg shadow-lg shadow-indigo-600/30">O</div>
          <span class="font-extrabold text-xl tracking-tight text-white">OYVER</span>
        </div>
        <nav class="hidden md:flex items-center gap-1 text-xs font-semibold text-slate-400">
          <button onclick="navigateTo('feed')" class="px-3 py-1.5 rounded-lg hover:text-white hover:bg-slate-800 transition">Piyasalar</button>
          <button onclick="navigateTo('leaderboard')" class="px-3 py-1.5 rounded-lg hover:text-white hover:bg-slate-800 transition">Sıralama</button>
          <button onclick="navigateTo('portfolio')" class="px-3 py-1.5 rounded-lg hover:text-white hover:bg-slate-800 transition">Portföyüm</button>
        </nav>
      </div>

      <!-- Arama Barı (Desktop) -->
      <div class="hidden md:flex flex-1 max-w-sm">
        <input type="text" id="search-input" oninput="handleSearch(this.value)" placeholder="Pazar veya kurum ara (Örn: TCMB, Faiz, Derbi)..." class="w-full bg-slate-900 border border-slate-800 rounded-xl px-4 py-1.5 text-xs text-white placeholder-slate-500 focus:outline-none focus:border-indigo-500 transition">
      </div>

      <!-- Bakiye & Profil -->
      <div class="flex items-center gap-3">
        <div onclick="navigateTo('portfolio')" class="cursor-pointer bg-slate-900 border border-slate-800 hover:border-slate-700 rounded-xl px-3 py-1.5 flex items-center gap-2 transition">
          <span class="w-2 h-2 rounded-full bg-emerald-400 animate-pulse"></span>
          <span id="user-balance" class="font-bold text-xs text-white tabular">10,000</span>
          <span class="text-[10px] font-black text-amber-400 bg-amber-400/10 px-1.5 py-0.5 rounded border border-amber-400/20">OVP</span>
        </div>
      </div>
    </div>
  </header>

  <!-- ==================== ANA KONTEYNER ==================== -->
  <main class="max-w-6xl mx-auto px-4 py-6">

    <!-- VIEW 1: PAZAR KEŞİF VE ANA AKIŞ -->
    <section id="view-feed" class="space-y-6">
      <!-- Gündem Spotu (Hero Market) -->
      <div id="hero-container"></div>

      <!-- Kategori Çipleri -->
      <div id="categories-container" class="flex gap-2 overflow-x-auto no-scrollbar py-1"></div>

      <!-- Pazar Kartları Izgarası -->
      <div class="space-y-2">
        <div class="flex justify-between items-center text-xs font-semibold text-slate-400 px-1">
          <span id="markets-count">Yükleniyor...</span>
          <div class="flex gap-2">
            <span class="text-indigo-400">🔥 Trend Sıralama</span>
          </div>
        </div>
        <div id="markets-grid" class="grid grid-cols-1 md:grid-cols-2 gap-4"></div>
      </div>
    </section>

    <!-- VIEW 2: PAZAR DETAY VE ANALİZ TERMİNALİ -->
    <section id="view-detail" class="hidden space-y-6">
      <button onclick="navigateTo('feed')" class="flex items-center gap-2 text-xs font-bold text-slate-400 hover:text-white transition">
        ← Geri Dön (Piyasalar)
      </button>
      <div id="detail-container"></div>
    </section>

    <!-- VIEW 3: LİDERLİK TABLOSU / SIRALAMA -->
    <section id="view-leaderboard" class="hidden space-y-6">
      <div class="text-center max-w-md mx-auto space-y-2">
        <h2 class="text-2xl font-black text-white">🏆 Ekim 2026 Sezonu</h2>
        <p class="text-xs text-slate-400">Sezon bitiminde ilk 100 tahminciye sponsorlu büyük analitik başarı ödülleri dağıtılacaktır.</p>
        <div class="inline-block bg-indigo-950/60 border border-indigo-800/40 text-indigo-400 text-xs font-bold px-3 py-1 rounded-full">
          Kalan Süre: 14 Gün 08 Saat
        </div>
      </div>
      <div id="leaderboard-table" class="bg-slate-900 border border-slate-800 rounded-2xl overflow-hidden shadow-xl max-w-2xl mx-auto divide-y divide-slate-800"></div>
    </section>

    <!-- VIEW 4: KULLANICI PORTFÖYÜ -->
    <section id="view-portfolio" class="hidden space-y-6">
      <div class="space-y-1">
        <h2 class="text-2xl font-black text-white">Portföyüm & Açık Tahminler</h2>
        <p class="text-xs text-slate-400">Katıldığınız tüm canlı pozisyonlar ve tahmini OVP getirileri.</p>
      </div>
      <div id="portfolio-list" class="space-y-3"></div>
    </section>

  </main>

  <!-- ==================== MOBİL ALTTAN AÇILIR İŞLEM ÇEKMECESİ ==================== -->
  <div id="trade-drawer-backdrop" onclick="closeDrawer()" class="fixed inset-0 bg-black/70 backdrop-blur-sm z-50 hidden transition-opacity"></div>
  <div id="trade-drawer" class="fixed bottom-0 left-0 right-0 max-w-lg mx-auto bg-slate-900 border-t border-slate-800 rounded-t-3xl p-6 z-50 transform translate-y-full transition-transform duration-300">
    <div class="w-12 h-1.5 bg-slate-700 rounded-full mx-auto mb-4"></div>
    <div id="drawer-content" class="space-y-4"></div>
  </div>

  <!-- ==================== MOBİL SABİT ALT NAVİGASYON ==================== -->
  <nav class="md:hidden fixed bottom-0 left-0 right-0 bg-slate-950/95 border-t border-slate-800/90 backdrop-blur-md px-6 py-2.5 z-40 flex justify-between text-[11px] font-bold text-slate-400">
    <button onclick="navigateTo('feed')" id="nav-feed" class="flex flex-col items-center gap-1 text-indigo-400">
      <span>🌐</span>
      <span>Piyasalar</span>
    </button>
    <button onclick="navigateTo('leaderboard')" id="nav-leaderboard" class="flex flex-col items-center gap-1 hover:text-white">
      <span>🏆</span>
      <span>Sıralama</span>
    </button>
    <button onclick="navigateTo('portfolio')" id="nav-portfolio" class="flex flex-col items-center gap-1 hover:text-white">
      <span>💼</span>
      <span>Portföyüm</span>
    </button>
  </nav>

  <!-- ==================== İSTEMCİ LOGİĞİ VE SPA MOTORU ==================== -->
  <script>
    let globalState = null;
    let currentCategory = 'TÜMÜ';
    let activeMarketId = null;
    let selectedOutcome = 'YES';
    let selectedAmount = 500;
    let ws = null;

    // WebSocket Kurulumu
    function initWebSocket() {
      const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      ws = new WebSocket(proto + '//' + window.location.host);
      ws.onmessage = (e) => {
        const msg = JSON.parse(e.data);
        if (msg.type === 'PRICE_UPDATE') {
          // Durumu güncelle
          const idx = globalState.markets.findIndex(m => m.id === msg.marketId);
          if (idx !== -1) {
            globalState.markets[idx].yesPrice = msg.newPrice;
            if (msg.market) globalState.markets[idx] = msg.market;
            renderCurrentView();
          }
        }
      };
    }

    async function fetchData() {
      const res = await fetch('/api/state');
      globalState = await res.json();
      document.getElementById('user-balance').innerText = globalState.users['demo-user'].balance.toLocaleString();
      renderFeed();
      initWebSocket();
    }

    // SPA Yönlendiricisi
    function navigateTo(view, param) {
      document.querySelectorAll('main > section').forEach(s => s.classList.add('hidden'));
      window.scrollTo(0, 0);

      if (view === 'feed') {
        document.getElementById('view-feed').classList.remove('hidden');
        renderFeed();
      } else if (view === 'detail') {
        document.getElementById('view-detail').classList.remove('hidden');
        renderDetail(param);
      } else if (view === 'leaderboard') {
        document.getElementById('view-leaderboard').classList.remove('hidden');
        renderLeaderboard();
      } else if (view === 'portfolio') {
        document.getElementById('view-portfolio').classList.remove('hidden');
        renderPortfolio();
      }
    }

    function renderCurrentView() {
      if (!document.getElementById('view-detail').classList.contains('hidden')) {
        renderDetail(activeMarketId);
      } else if (!document.getElementById('view-feed').classList.contains('hidden')) {
        renderFeed();
      }
    }

    // Sparkline SVG Üretici (Kartlar İçin)
    function generateSparkline(history, yesPrice) {
      if (!history || history.length < 2) return '';
      const min = Math.min(...history.map(h => h.price), 20);
      const max = Math.max(...history.map(h => h.price), 80);
      const w = 100, h = 32;
      const points = history.map((pt, i) => {
        const x = (i / (history.length - 1)) * w;
        const y = h - ((pt.price - min) / (max - min || 1)) * h;
        return x + ',' + y;
      }).join(' ');

      return \`
        <svg viewBox="0 0 \${w} \${h}" class="w-24 h-8 overflow-visible">
          <polyline fill="none" stroke="#10b981" stroke-width="2" stroke-linecap="round" points="\${points}" />
        </svg>
      \`;
    }

    // Ana Akış ve Kartları Çizme
    function renderFeed() {
      // 1. Kategoriler
      const catContainer = document.getElementById('categories-container');
      catContainer.innerHTML = globalState.categories.map(c => \`
        <button onclick="filterCategory('\${c}')" class="px-3.5 py-1.5 rounded-xl text-xs font-bold whitespace-nowrap transition \${
          currentCategory === c
            ? 'bg-indigo-600 text-white shadow-md shadow-indigo-600/30'
            : 'bg-slate-900 border border-slate-800 text-slate-400 hover:text-white hover:bg-slate-800'
        }">\${c}</button>
      \`).join('');

      // 2. Filtrelenmiş Pazarlar
      const filtered = globalState.markets.filter(m => currentCategory === 'TÜMÜ' || m.category === currentCategory);
      document.getElementById('markets-count').innerText = \`\${filtered.length} Aktif Tahmin Pazarı\`;

      // 3. Hero Spot (İlk Pazar)
      const hero = filtered[0];
      const heroContainer = document.getElementById('hero-container');
      if (hero && currentCategory === 'TÜMÜ') {
        heroContainer.innerHTML = \`
          <div class="relative overflow-hidden bg-gradient-to-br from-indigo-950/60 via-slate-900 to-slate-900 border border-indigo-800/40 rounded-3xl p-6 shadow-2xl">
            <div class="flex justify-between items-center text-xs font-semibold mb-3">
              <span class="bg-indigo-600/20 text-indigo-400 border border-indigo-500/30 px-2.5 py-0.5 rounded-md uppercase tracking-wider text-[10px] font-bold">Günün En Çok Konuşulanı</span>
              <span class="text-slate-400 font-medium">Bitiş: \${hero.closeTime}</span>
            </div>
            <h2 onclick="navigateTo('detail', \${hero.id})" class="text-xl md:text-2xl font-black text-white hover:text-indigo-400 transition cursor-pointer mb-4 leading-snug">
              \${hero.title}
            </h2>
            <div class="grid grid-cols-1 md:grid-cols-2 gap-4 items-center pt-2 border-t border-slate-800/80">
              <div>
                <div class="text-xs text-slate-400 mb-1">Kolektif Piyasa Beklentisi</div>
                <div class="flex items-baseline gap-3">
                  <span class="text-4xl font-black text-emerald-400 tabular">%\${hero.yesPrice}</span>
                  <span class="text-xs text-slate-400 font-semibold">EVET Olasılığı</span>
                </div>
              </div>
              <div class="grid grid-cols-2 gap-2">
                <button onclick="openTradeDrawer(\${hero.id}, 'YES')" class="py-3 bg-emerald-600 hover:bg-emerald-500 text-white rounded-xl font-bold text-xs shadow-lg shadow-emerald-600/20 transition">
                  EVET (%\${hero.yesPrice})
                </button>
                <button onclick="openTradeDrawer(\${hero.id}, 'NO')" class="py-3 bg-rose-600 hover:bg-rose-500 text-white rounded-xl font-bold text-xs shadow-lg shadow-rose-600/20 transition">
                  HAYIR (%\${100 - hero.yesPrice})
                </button>
              </div>
            </div>
          </div>
        \`;
      } else {
        heroContainer.innerHTML = '';
      }

      // 4. Kart Listesi
      const grid = document.getElementById('markets-grid');
      grid.innerHTML = filtered.map(m => \`
        <div class="bg-slate-900 border border-slate-800/80 hover:border-slate-700/80 rounded-2xl p-5 shadow-lg flex flex-col justify-between transition-all group">
          <div>
            <div class="flex justify-between items-center text-xs mb-2.5">
              <span class="text-[11px] font-bold text-indigo-400 bg-indigo-950/80 border border-indigo-900/60 px-2.5 py-0.5 rounded-md">\${m.category}</span>
              <span class="text-slate-400 text-[11px] font-medium">\${m.closeTime}</span>
            </div>
            <h3 onclick="navigateTo('detail', \${m.id})" class="font-bold text-slate-100 text-base leading-snug hover:text-indigo-400 cursor-pointer transition mb-4">
              \${m.title}
            </h3>
          </div>

          <div class="space-y-4">
            <div class="flex items-center justify-between bg-slate-950/70 p-3 rounded-xl border border-slate-800/80">
              <div>
                <div class="text-[10px] text-slate-400 uppercase font-semibold tracking-wider">İhtimal</div>
                <div class="text-2xl font-black text-emerald-400 tabular">%\${m.yesPrice}</div>
              </div>
              <div>\${generateSparkline(m.history, m.yesPrice)}</div>
            </div>

            <div class="flex items-center justify-between text-[11px] text-slate-400 font-medium px-1">
              <span>👥 \${m.participantsCount.toLocaleString()} Katılımcı</span>
              <span>💰 \${(m.volumeOvp / 1000).toFixed(0)}k OVP Hacim</span>
            </div>

            <div class="grid grid-cols-2 gap-2 pt-1 border-t border-slate-800/60">
              <button onclick="openTradeDrawer(\${m.id}, 'YES')" class="py-2.5 rounded-xl font-bold text-xs bg-slate-800 hover:bg-emerald-600 text-slate-200 hover:text-white border border-slate-700 hover:border-emerald-500 transition">
                EVET (%\${m.yesPrice})
              </button>
              <button onclick="openTradeDrawer(\${m.id}, 'NO')" class="py-2.5 rounded-xl font-bold text-xs bg-slate-800 hover:bg-rose-600 text-slate-200 hover:text-white border border-slate-700 hover:border-rose-500 transition">
                HAYIR (%\${100 - m.yesPrice})
              </button>
            </div>
          </div>
        </div>
      \`).join('');
    }

    function filterCategory(cat) {
      currentCategory = cat;
      renderFeed();
    }

    function handleSearch(query) {
      const q = query.toLowerCase().trim();
      const cards = document.querySelectorAll('#markets-grid > div');
      cards.forEach(card => {
        const text = card.innerText.toLowerCase();
        card.style.display = text.includes(q) ? 'flex' : 'none';
      });
    }

    // ============================================================================
    // VIEW 2: PAZAR DETAY VE ANALİZ TERMİNALİ
    // ============================================================================
    function renderDetail(marketId) {
      activeMarketId = marketId;
      const m = globalState.markets.find(x => x.id === marketId);
      if (!m) return;

      const container = document.getElementById('detail-container');

      // Büyük İnteraktif SVG Zaman Grafiği
      const min = Math.min(...m.history.map(h => h.price), 10);
      const max = Math.max(...m.history.map(h => h.price), 90);
      const w = 600, h = 180;
      const points = m.history.map((pt, i) => {
        const x = (i / (m.history.length - 1 || 1)) * (w - 40) + 20;
        const y = h - ((pt.price - min) / (max - min || 1)) * (h - 40) - 20;
        return x + ',' + y;
      }).join(' ');

      container.innerHTML = \`
        <div class="space-y-3">
          <div class="flex items-center gap-2 text-xs">
            <span class="bg-indigo-950 text-indigo-400 border border-indigo-800 px-2.5 py-0.5 rounded-md font-bold">\${m.category}</span>
            <span class="text-slate-400">Kapanış: \${m.closeTime}</span>
          </div>
          <h1 class="text-2xl md:text-3xl font-black text-white leading-tight">\${m.title}</h1>
        </div>

        <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
          <!-- Sol Kolon: Grafik, Kurallar, Yorumlar -->
          <div class="lg:col-span-2 space-y-6">

            <!-- Fiyat & Büyük Grafik Kartı -->
            <div class="bg-slate-900 border border-slate-800 rounded-3xl p-6 shadow-xl space-y-4">
              <div class="flex justify-between items-baseline">
                <div>
                  <div class="text-xs text-slate-400 font-semibold uppercase">Piyasa Olasılığı</div>
                  <div class="text-5xl font-black text-emerald-400 tabular">%\${m.yesPrice}</div>
                </div>
                <div class="text-right">
                  <div class="text-xs text-slate-400 font-semibold">Toplam Hacim</div>
                  <div class="text-lg font-bold text-white">\${m.volumeOvp.toLocaleString()} OVP</div>
                </div>
              </div>

              <!-- SVG Zaman Serisi Grafiği -->
              <div class="relative w-full h-48 bg-slate-950/60 rounded-2xl p-2 border border-slate-800/80 flex items-center justify-center">
                <svg viewBox="0 0 \${w} \${h}" class="w-full h-full overflow-visible">
                  <polyline fill="none" stroke="#10b981" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" points="\${points}" />
                  \${m.history.map((pt, i) => {
                    const x = (i / (m.history.length - 1 || 1)) * (w - 40) + 20;
                    const y = h - ((pt.price - min) / (max - min || 1)) * (h - 40) - 20;
                    return \`
                      <circle cx="\${x}" cy="\${y}" r="4" fill="#030712" stroke="#10b981" stroke-width="2" />
                      <text x="\${x}" y="\${h - 2}" font-size="10" fill="#64748b" text-anchor="middle">\${pt.time}</text>
                      <text x="\${x}" y="\${y - 8}" font-size="10" font-weight="bold" fill="#10b981" text-anchor="middle">%\${pt.price}</text>
                    \`;
                  }).join('')}
                </svg>
              </div>
            </div>

            <!-- Çözümleme Kuralları ve Resmi Kaynak -->
            <div class="bg-slate-900 border border-slate-800 rounded-3xl p-6 space-y-3">
              <h3 class="text-sm font-bold text-white flex items-center gap-2">
                <span>⚖️</span> Çözümleme Kuralları ve Doğrulama
              </h3>
              <div class="text-xs text-slate-300 leading-relaxed whitespace-pre-line bg-slate-950/60 p-4 rounded-xl border border-slate-800/80">
                \${m.rules}
              </div>
              <div class="flex justify-between items-center text-xs pt-2 text-slate-400">
                <span>Resmi Doğrulama Kaynağı:</span>
                <a href="\${m.verifiedSource}" target="_blank" class="text-indigo-400 font-bold hover:underline">
                  \${m.sourceName} ↗
                </a>
              </div>
            </div>

            <!-- Tartışma ve Yorum Alanı -->
            <div class="bg-slate-900 border border-slate-800 rounded-3xl p-6 space-y-4">
              <h3 class="text-sm font-bold text-white flex items-center gap-2">
                <span>💬</span> Topluluk Görüşleri (\${m.comments.length})
              </h3>

              <!-- Yorum Giriş Kutusu -->
              <div class="space-y-2 bg-slate-950/60 p-3.5 rounded-2xl border border-slate-800">
                <textarea id="comment-text" rows="2" placeholder="Gerekçenizi paylaşın (Örn: Enflasyon verisi açıklandı...)" class="w-full bg-transparent text-xs text-white placeholder-slate-500 focus:outline-none resize-none"></textarea>
                <div class="flex justify-between items-center pt-2 border-t border-slate-800">
                  <div class="flex gap-2">
                    <button type="button" onclick="setCommentStance('YES')" id="stance-yes" class="px-2.5 py-1 rounded-lg text-[10px] font-bold bg-emerald-950/60 text-emerald-400 border border-emerald-800">EVET Diyorum</button>
                    <button type="button" onclick="setCommentStance('NO')" id="stance-no" class="px-2.5 py-1 rounded-lg text-[10px] font-bold bg-slate-800 text-slate-400 border border-slate-700">HAYIR Diyorum</button>
                  </div>
                  <button onclick="postComment(\${m.id})" class="px-4 py-1.5 bg-indigo-600 hover:bg-indigo-500 text-white rounded-xl text-xs font-bold transition">Paylaş</button>
                </div>
              </div>

              <!-- Yorum Listesi -->
              <div id="comments-list" class="space-y-3">
                \${m.comments.map(c => \`
                  <div class="p-3.5 rounded-2xl bg-slate-950/40 border border-slate-800/60 space-y-1.5 text-xs">
                    <div class="flex justify-between items-center">
                      <div class="flex items-center gap-2">
                        <span class="font-bold text-slate-200">\${c.user}</span>
                        <span class="text-[9px] font-black px-1.5 py-0.5 rounded \${
                          c.stance === 'YES' ? 'bg-emerald-950 text-emerald-400 border border-emerald-800' : 'bg-rose-950 text-rose-400 border border-rose-800'
                        }">\${c.stance}</span>
                      </div>
                      <span class="text-[10px] text-slate-500">\${c.time}</span>
                    </div>
                    <p class="text-slate-300 leading-relaxed">\${c.text}</p>
                  </div>
                \`).join('')}
              </div>
            </div>

          </div>

          <!-- Sağ Kolon: Terminal / İşlem Masası -->
          <div class="space-y-6">
            <div class="bg-slate-900 border border-slate-800 rounded-3xl p-6 shadow-xl space-y-5 sticky top-20">
              <h3 class="text-sm font-bold text-white flex items-center justify-between">
                <span>Hızlı Tahmin Terminali</span>
                <span class="text-xs text-indigo-400 font-medium">Bakiye: \${globalState.users['demo-user'].balance.toLocaleString()} OVP</span>
              </h3>

              <div class="grid grid-cols-2 gap-2">
                <button onclick="setDetailTradeOutcome('YES')" id="detail-btn-yes" class="py-3 rounded-xl font-bold text-xs transition border \${selectedOutcome === 'YES' ? 'bg-emerald-600 border-emerald-500 text-white shadow-lg shadow-emerald-600/30' : 'bg-slate-800 border-slate-700 text-slate-400'}">
                  EVET (%\${m.yesPrice})
                </button>
                <button onclick="setDetailTradeOutcome('NO')" id="detail-btn-no" class="py-3 rounded-xl font-bold text-xs transition border \${selectedOutcome === 'NO' ? 'bg-rose-600 border-rose-500 text-white shadow-lg shadow-rose-600/30' : 'bg-slate-800 border-slate-700 text-slate-400'}">
                  HAYIR (%\${100 - m.yesPrice})
                </button>
              </div>

              <div class="space-y-2">
                <div class="flex justify-between text-xs text-slate-400 font-medium">
                  <span>Yatırılacak OVP</span>
                  <span id="detail-calc-shares">-- Pay</span>
                </div>
                <input type="number" id="detail-trade-amount" value="500" oninput="calculateDetailPayout()" class="w-full bg-slate-950 border border-slate-800 rounded-xl px-4 py-2.5 text-base font-bold text-white focus:outline-none focus:border-indigo-500">
                <div class="flex gap-1.5">
                  <button onclick="setAmount(250)" class="flex-1 py-1 bg-slate-800 hover:bg-slate-700 rounded-lg text-[10px] font-bold text-slate-300">+250</button>
                  <button onclick="setAmount(500)" class="flex-1 py-1 bg-slate-800 hover:bg-slate-700 rounded-lg text-[10px] font-bold text-slate-300">+500</button>
                  <button onclick="setAmount(1000)" class="flex-1 py-1 bg-slate-800 hover:bg-slate-700 rounded-lg text-[10px] font-bold text-slate-300">+1.000</button>
                </div>
              </div>

              <!-- Hesaplanan Kazanç Özeti -->
              <div class="bg-slate-950/80 p-3.5 rounded-xl border border-slate-800 space-y-1.5 text-xs">
                <div class="flex justify-between text-slate-400">
                  <span>Birim Fiyat:</span>
                  <span id="detail-unit-price" class="text-slate-200 font-bold">-- OVP</span>
                </div>
                <div class="flex justify-between text-slate-400 pt-1 border-t border-slate-800">
                  <span>Olası Dönüş:</span>
                  <span id="detail-potential-payout" class="text-emerald-400 font-bold">-- OVP</span>
                </div>
              </div>

              <button onclick="confirmDetailTrade()" class="w-full py-3.5 bg-indigo-600 hover:bg-indigo-500 active:scale-[0.98] text-white rounded-xl font-bold text-xs shadow-lg shadow-indigo-600/30 transition">
                Tahmini Onayla
              </button>
            </div>
          </div>
        </div>
      \`;

      calculateDetailPayout();
    }

    let currentCommentStance = 'YES';
    function setCommentStance(st) {
      currentCommentStance = st;
      document.getElementById('stance-yes').className = st === 'YES'
        ? 'px-2.5 py-1 rounded-lg text-[10px] font-bold bg-emerald-950/60 text-emerald-400 border border-emerald-800'
        : 'px-2.5 py-1 rounded-lg text-[10px] font-bold bg-slate-800 text-slate-400 border border-slate-700';
      document.getElementById('stance-no').className = st === 'NO'
        ? 'px-2.5 py-1 rounded-lg text-[10px] font-bold bg-rose-950/60 text-rose-400 border border-rose-800'
        : 'px-2.5 py-1 rounded-lg text-[10px] font-bold bg-slate-800 text-slate-400 border border-slate-700';
    }

    async function postComment(marketId) {
      const text = document.getElementById('comment-text').value;
      if (!text || text.trim().length < 3) return alert('Lütfen geçerli bir yorum yazın.');

      const res = await fetch('/api/comments', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ marketId, text, stance: currentCommentStance })
      });
      const data = await res.json();
      if (data.success) {
        document.getElementById('comment-text').value = '';
        const m = globalState.markets.find(x => x.id === marketId);
        m.comments.unshift(data.comment);
        renderDetail(marketId);
      }
    }

    function setDetailTradeOutcome(side) {
      selectedOutcome = side;
      const m = globalState.markets.find(x => x.id === activeMarketId);
      document.getElementById('detail-btn-yes').className = side === 'YES'
        ? 'py-3 rounded-xl font-bold text-xs bg-emerald-600 border border-emerald-500 text-white shadow-lg shadow-emerald-600/30'
        : 'py-3 rounded-xl font-bold text-xs bg-slate-800 border border-slate-700 text-slate-400';
      document.getElementById('detail-btn-no').className = side === 'NO'
        ? 'py-3 rounded-xl font-bold text-xs bg-rose-600 border border-rose-500 text-white shadow-lg shadow-rose-600/30'
        : 'py-3 rounded-xl font-bold text-xs bg-slate-800 border border-slate-700 text-slate-400';
      calculateDetailPayout();
    }

    function setAmount(val) {
      document.getElementById('detail-trade-amount').value = val;
      calculateDetailPayout();
    }

    function calculateDetailPayout() {
      const m = globalState.markets.find(x => x.id === activeMarketId);
      if (!m) return;
      const amount = Number(document.getElementById('detail-trade-amount').value) || 0;
      const effectivePrice = selectedOutcome === 'YES' ? m.yesPrice : (100 - m.yesPrice);
      const shares = effectivePrice > 0 ? (amount / effectivePrice).toFixed(1) : 0;
      const payout = (shares * 100).toFixed(0);

      document.getElementById('detail-unit-price').innerText = effectivePrice + ' OVP';
      document.getElementById('detail-calc-shares').innerText = shares + ' Pay';
      document.getElementById('detail-potential-payout').innerText = payout + ' OVP';
    }

    async function confirmDetailTrade() {
      const amount = Number(document.getElementById('detail-trade-amount').value);
      const res = await fetch('/api/trade', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ marketId: activeMarketId, outcome: selectedOutcome, amount })
      });
      const data = await res.json();
      if (data.success) {
        document.getElementById('user-balance').innerText = data.newBalance.toLocaleString();
        globalState.users['demo-user'].balance = data.newBalance;
        const idx = globalState.markets.findIndex(x => x.id === activeMarketId);
        globalState.markets[idx] = data.market;
        renderDetail(activeMarketId);
      } else {
        alert(data.message);
      }
    }

    // ============================================================================
    // MOBİL ÇEKMECE İŞLEMLERİ (ONE-TAP DRAWER)
    // ============================================================================
    function openTradeDrawer(marketId, side) {
      activeMarketId = marketId;
      selectedOutcome = side;
      const m = globalState.markets.find(x => x.id === marketId);
      if (!m) return;

      const effectivePrice = side === 'YES' ? m.yesPrice : (100 - m.yesPrice);

      document.getElementById('drawer-content').innerHTML = \`
        <div class="flex justify-between items-center text-xs pb-2 border-b border-slate-800">
          <span class="font-bold text-slate-400">Hızlı Tahmin</span>
          <span class="text-indigo-400 font-semibold">Bakiye: \${globalState.users['demo-user'].balance.toLocaleString()} OVP</span>
        </div>
        <h4 class="font-bold text-white text-sm leading-snug">\${m.title}</h4>
        <div class="grid grid-cols-2 gap-2">
          <button onclick="drawerSetSide('YES')" id="drawer-btn-yes" class="py-3 rounded-xl font-bold text-xs border \${side === 'YES' ? 'bg-emerald-600 border-emerald-500 text-white' : 'bg-slate-800 border-slate-700 text-slate-400'}">
            EVET (%\${m.yesPrice})
          </button>
          <button onclick="drawerSetSide('NO')" id="drawer-btn-no" class="py-3 rounded-xl font-bold text-xs border \${side === 'NO' ? 'bg-rose-600 border-rose-500 text-white' : 'bg-slate-800 border-slate-700 text-slate-400'}">
            HAYIR (%\${100 - m.yesPrice})
          </button>
        </div>
        <div class="space-y-1.5">
          <input type="number" id="drawer-amount" value="500" class="w-full bg-slate-950 border border-slate-800 rounded-xl px-4 py-3 text-lg font-bold text-white text-center focus:outline-none">
          <div class="flex gap-1.5">
            <button onclick="document.getElementById('drawer-amount').value = 250" class="flex-1 py-1.5 bg-slate-800 rounded-lg text-xs font-bold text-slate-300">+250</button>
            <button onclick="document.getElementById('drawer-amount').value = 500" class="flex-1 py-1.5 bg-slate-800 rounded-lg text-xs font-bold text-slate-300">+500</button>
            <button onclick="document.getElementById('drawer-amount').value = 1000" class="flex-1 py-1.5 bg-slate-800 rounded-lg text-xs font-bold text-slate-300">+1.000</button>
          </div>
        </div>
        <button onclick="drawerExecuteTrade()" class="w-full py-4 bg-indigo-600 hover:bg-indigo-500 active:scale-95 text-white rounded-xl font-bold text-sm shadow-xl shadow-indigo-600/30 transition">
          Tahmini Onayla
        </button>
      \`;

      document.getElementById('trade-drawer-backdrop').classList.remove('hidden');
      document.getElementById('trade-drawer').classList.remove('translate-y-full');
    }

    function drawerSetSide(s) {
      selectedOutcome = s;
      openTradeDrawer(activeMarketId, s);
    }

    function closeDrawer() {
      document.getElementById('trade-drawer').classList.add('translate-y-full');
      document.getElementById('trade-drawer-backdrop').classList.add('hidden');
    }

    async function drawerExecuteTrade() {
      const amount = Number(document.getElementById('drawer-amount').value);
      const res = await fetch('/api/trade', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ marketId: activeMarketId, outcome: selectedOutcome, amount })
      });
      const data = await res.json();
      if (data.success) {
        document.getElementById('user-balance').innerText = data.newBalance.toLocaleString();
        globalState.users['demo-user'].balance = data.newBalance;
        const idx = globalState.markets.findIndex(x => x.id === activeMarketId);
        globalState.markets[idx] = data.market;
        closeDrawer();
        renderCurrentView();
      } else {
        alert(data.message);
      }
    }

    // ============================================================================
    // VIEW 3 & 4: LİDERLİK VE PORTFÖY
    // ============================================================================
    function renderLeaderboard() {
      const sampleBoard = [
        { rank: 1, name: 'MakroUstadı', balance: 142500, pro: true },
        { rank: 2, name: 'BistAnaliz_Ali', balance: 118400, pro: true },
        { rank: 3, name: 'Ece_Finans', balance: 94200, pro: false },
        { rank: 42, name: 'Tahminci_Leisan (Sen)', balance: globalState.users['demo-user'].balance, pro: true }
      ];

      document.getElementById('leaderboard-table').innerHTML = sampleBoard.map(u => \`
        <div class="flex items-center justify-between p-4 px-6 \${u.rank === 42 ? 'bg-indigo-950/40 border-l-4 border-indigo-500' : ''}">
          <div class="flex items-center gap-3">
            <span class="w-6 font-black text-sm \${u.rank === 1 ? 'text-amber-400' : u.rank === 2 ? 'text-slate-300' : u.rank === 3 ? 'text-amber-600' : 'text-slate-500'}">#\${u.rank}</span>
            <span class="font-bold text-sm text-slate-200">\${u.name}</span>
            \${u.pro ? '<span class="text-[9px] font-black bg-amber-400/20 text-amber-400 px-1.5 py-0.5 rounded border border-amber-400/30">PRO</span>' : ''}
          </div>
          <span class="font-black text-sm text-emerald-400 tabular">\${u.balance.toLocaleString()} OVP</span>
        </div>
      \`).join('');
    }

    function renderPortfolio() {
      const pos = globalState.users['demo-user'].positions;
      const container = document.getElementById('portfolio-list');
      if (pos.length === 0) {
        container.innerHTML = \`
          <div class="text-center py-12 bg-slate-900 border border-slate-800 rounded-3xl p-6 text-slate-500 text-sm">
            Henüz açık bir tahmininiz bulunmuyor. Piyasaları keşfedip öngörünüzü belirtin.
          </div>
        \`;
        return;
      }
      container.innerHTML = pos.map(p => \`
        <div class="bg-slate-900 border border-slate-800 rounded-2xl p-4 flex justify-between items-center shadow-lg">
          <div>
            <span class="text-[10px] font-black px-2 py-0.5 rounded \${p.outcome === 'YES' ? 'bg-emerald-950 text-emerald-400 border border-emerald-800' : 'bg-rose-950 text-rose-400 border border-rose-800'}">\${p.outcome} TAHMİNİ</span>
            <h4 class="font-bold text-white text-sm mt-1">\${p.marketTitle}</h4>
            <div class="text-[11px] text-slate-400 mt-0.5">Alış Maliyeti: \${p.cost} OVP (\${p.shares} Pay @ \${p.entryPrice} OVP)</div>
          </div>
          <div class="text-right">
            <div class="text-xs text-slate-400 font-semibold">Olası Dönüş</div>
            <div class="text-base font-black text-emerald-400 tabular">\${(p.shares * 100).toFixed(0)} OVP</div>
          </div>
        </div>
      \`).join('');
    }

    window.onload = fetchData;
  </script>
</body>
</html>
    `);
    return;
  }

  res.writeHead(404);
  res.end('Not Found');
});

// ============================================================================
// 5. WEBSOCKET YAYIN SUNUCUSU
// ============================================================================
const wss = new WebSocket.Server({ server });

function broadcastPriceUpdate(marketId, newPrice, market) {
  const payload = JSON.stringify({
    type: 'PRICE_UPDATE',
    marketId,
    newPrice,
    market
  });
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  });
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`OYVER Core Platform Engine v1.0 http://localhost:${PORT} portunda devrede.`);
});
