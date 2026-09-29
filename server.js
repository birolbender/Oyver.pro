const http = require('http');
const WebSocket = require('ws');

// ============================================================================
// 1. OYVER ÇEKİRDEK VERİ MODELİ (BELLEK İÇİ DURUM)
// ============================================================================
const state = {
  activeSeason: {
    name: 'Ekim 2026 Sezonu',
    daysLeft: 14,
    rewardPool: '100.000 OVP Değerinde Sponsorluk Ödülü'
  },
  user: {
    id: 'usr_leisan',
    username: 'Tahminci_Leisan',
    balance: 10000,
    isPro: true,
    rank: 42,
    positions: []
  },
  categories: ['TÜMÜ', 'EKONOMİ & FİNANS', 'SPOR', 'BIST & ŞİRKETLER', 'TEKNOLOJİ', 'TÜRKİYE GÜNDEMİ'],
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
      sourceName: 'TCMB Resmî Basın Duyurusu',
      newsFeed: [
        { title: 'TCMB Piyasa Katılımcıları Anketi Yayımlandı', source: 'KAP / TCMB', time: '1 saat önce' },
        { title: 'Öncü Enflasyon Göstergelerinde İyileşme Sinyali', source: 'Finans Portalı', time: '3 saat önce' }
      ],
      rules: `1. Bu pazar, TCMB Para Politikası Kurulu toplantısı sonrasındaki resmî karara göre sonuçlandırılır.
2. Politika faizinde indirim açıklanırsa pazar "EVET" sayılır.
3. Faizin sabit tutulması veya artırılması durumunda pazar "HAYIR" olarak çözümlenir.`,
      history: [
        { time: '23 Eyl', price: 58 },
        { time: '24 Eyl', price: 61 },
        { time: '25 Eyl', price: 59 },
        { time: '26 Eyl', price: 64 },
        { time: '27 Eyl', price: 63 },
        { time: '28 Eyl', price: 65 },
        { time: '29 Eyl', price: 66 }
      ],
      comments: [
        {
          id: 101,
          user: 'Kerem_Macro',
          stance: 'YES',
          text: 'Öncü göstergeler beklenti altı geldi. 150 baz puanlık sembolik indirim kuvvetle muhtemel.',
          time: '2 saat önce',
          likes: 14,
          replies: [
            {
              id: 1011,
              user: 'SelinFinans',
              stance: 'NO',
              text: 'Kur baskısı devam ederken PPK bu toplantıda risk almaz, pas geçer.',
              time: '1 saat önce'
            }
          ]
        },
        {
          id: 102,
          user: 'Bist_Avcisi',
          stance: 'NEUTRAL',
          text: 'Karar metnindeki şahin/güvercin tonlama faiz miktarından daha kritik olacak.',
          time: '3 saat önce',
          likes: 5,
          replies: []
        }
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
      sourceName: 'TFF Resmî Maç Cetveli',
      newsFeed: [
        { title: 'Derbi Öncesi Sakat Oyuncuların Son Durumu', source: 'Spor Portalı', time: '40 dk önce' }
      ],
      rules: `1. Bu pazar, TFF resmî hakem raporundaki 90 dakika ve uzatma dakikaları sonundaki skoru baz alır.
2. Ev sahibi galibiyeti "EVET", beraberlik veya deplasman galibiyeti "HAYIR" sayılır.`,
      history: [
        { time: '26 Eyl', price: 50 },
        { time: '27 Eyl', price: 47 },
        { time: '28 Eyl', price: 49 },
        { time: '29 Eyl', price: 48 }
      ],
      comments: [
        {
          id: 201,
          user: 'TribunLideri',
          stance: 'YES',
          text: 'Taraftar desteğiyle ev sahibi maçı net çözer.',
          time: '45 dk önce',
          likes: 8,
          replies: []
        }
      ]
    }
  ]
};

// ============================================================================
// 2. İŞLEM VE SOSYAL DİYALOG FONKSİYONLARI
// ============================================================================
function executeTrade(marketId, outcome, amountOvp) {
  const market = state.markets.find(m => m.id === marketId);
  const user = state.user;

  if (!market || user.balance < amountOvp || amountOvp <= 0) {
    return { success: false, message: 'Yetersiz OVP bakiyesi veya geçersiz işlem.' };
  }

  const effectivePrice = outcome === 'YES' ? market.yesPrice : (100 - market.yesPrice);
  const shares = Number((amountOvp / effectivePrice).toFixed(1));

  user.balance -= amountOvp;
  market.volumeOvp += amountOvp;
  market.participantsCount += 1;

  // Fiyat kayması simülasyonu
  const shift = outcome === 'YES' ? 1 : -1;
  market.yesPrice = Math.min(99, Math.max(1, market.yesPrice + shift));

  // Son fiyata ekle
  const last = market.history[market.history.length - 1];
  if (last && last.time === 'Bugün') {
    last.price = market.yesPrice;
  } else {
    market.history.push({ time: 'Bugün', price: market.yesPrice });
  }

  // Pozisyonlara ekle
  user.positions.push({
    id: Date.now(),
    marketId: market.id,
    marketTitle: market.title,
    outcome,
    shares,
    entryPrice: effectivePrice,
    cost: amountOvp,
    time: new Date().toLocaleTimeString('tr-TR', { hour: '2-digit', minute: '2-digit' })
  });

  return { success: true, newBalance: user.balance, newPrice: market.yesPrice, market };
}

function addComment(marketId, text, stance) {
  const market = state.markets.find(m => m.id === marketId);
  if (!market || !text || text.trim().length < 2) return { success: false };

  const newComm = {
    id: Date.now(),
    user: state.user.username,
    stance: stance || 'NEUTRAL',
    text: text.trim().slice(0, 300),
    time: 'Az önce',
    likes: 0,
    replies: []
  };

  market.comments.unshift(newComm);
  return { success: true, comment: newComm };
}

function addReply(marketId, parentCommentId, text) {
  const market = state.markets.find(m => m.id === marketId);
  if (!market || !text || text.trim().length < 2) return { success: false };

  const parent = market.comments.find(c => c.id === parentCommentId);
  if (!parent) return { success: false };

  // Kullanıcının bu pazardaki son duruşunu bul
  const pos = state.user.positions.filter(p => p.marketId === marketId);
  const userStance = pos.length > 0 ? pos[pos.length - 1].outcome : 'NEUTRAL';

  const reply = {
    id: Date.now(),
    user: state.user.username,
    stance: userStance,
    text: text.trim().slice(0, 300),
    time: 'Az önce'
  };

  parent.replies.push(reply);
  return { success: true, reply };
}

// ============================================================================
// 3. HTTP VE REST API
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

  if (req.url === '/api/state' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(state));
    return;
  }

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
      } catch (e) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: false }));
      }
    });
    return;
  }

  if (req.url === '/api/comments' && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      try {
        const { marketId, text, stance } = JSON.parse(body);
        const result = addComment(Number(marketId), text, stance);
        res.writeHead(result.success ? 200 : 400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(result));
      } catch (e) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: false }));
      }
    });
    return;
  }

  if (req.url === '/api/comments/reply' && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      try {
        const { marketId, parentCommentId, text } = JSON.parse(body);
        const result = addReply(Number(marketId), Number(parentCommentId), text);
        res.writeHead(result.success ? 200 : 400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(result));
      } catch (e) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ success: false }));
      }
    });
    return;
  }

  // ============================================================================
  // 4. FRONTEND ARAYÜZÜ (HTML + TAILWIND + CLIENT APP)
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
    body { background-color: #030712; color: #f8fafc; font-family: 'Inter', sans-serif; -webkit-tap-highlight-color: transparent; }
    .tabular { font-variant-numeric: tabular-nums; }
    .no-scrollbar::-webkit-scrollbar { display: none; }
  </style>
</head>
<body class="min-h-screen pb-24">

  <!-- ÜST MENÜ (NAVBAR) -->
  <header class="sticky top-0 z-40 bg-slate-950/85 backdrop-blur-md border-b border-slate-800 px-4 py-3">
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
          <button onclick="navigateTo('corporate')" class="px-3 py-1.5 rounded-lg hover:text-white hover:bg-slate-800 transition">Kurumsal</button>
        </nav>
      </div>

      <div class="flex items-center gap-3">
        <div onclick="navigateTo('portfolio')" class="cursor-pointer bg-slate-900 border border-slate-800 rounded-xl px-3 py-1.5 flex items-center gap-2">
          <span class="w-2 h-2 rounded-full bg-emerald-400 animate-pulse"></span>
          <span id="user-balance" class="font-bold text-xs text-white tabular">10.000</span>
          <span class="text-[10px] font-black text-amber-400 bg-amber-400/10 px-1.5 py-0.5 rounded border border-amber-400/20">OVP</span>
        </div>
      </div>
    </div>
  </header>

  <!-- ANA İÇERİK KONTEYNERİ -->
  <main class="max-w-6xl mx-auto px-4 py-6">

    <!-- 1. GÖRÜNÜM: PAZAR KEŞİF LİSTESİ -->
    <section id="view-feed" class="space-y-6">
      <div id="hero-container"></div>
      <div id="categories-container" class="flex gap-2 overflow-x-auto no-scrollbar py-1"></div>
      <div class="space-y-2">
        <div class="flex justify-between items-center text-xs font-semibold text-slate-400 px-1">
          <span id="markets-count">Yükleniyor...</span>
          <span class="text-indigo-400">🔥 Trend Sıralama</span>
        </div>
        <div id="markets-grid" class="grid grid-cols-1 md:grid-cols-2 gap-4"></div>
      </div>
    </section>

    <!-- 2. GÖRÜNÜM: PAZAR DETAY VE SOSYAL DİYALOG TERMİNALİ -->
    <section id="view-detail" class="hidden space-y-6">
      <button onclick="navigateTo('feed')" class="text-xs font-bold text-slate-400 hover:text-white flex items-center gap-1">
        ← Geri Dön (Piyasalar)
      </button>
      <div id="detail-container"></div>
    </section>

    <!-- 3. GÖRÜNÜM: SEZON SIRALAMASI -->
    <section id="view-leaderboard" class="hidden space-y-6">
      <div class="text-center max-w-md mx-auto space-y-2">
        <h2 class="text-2xl font-black text-white">🏆 Ekim 2026 Sezonu</h2>
        <p class="text-xs text-slate-400">İlk 100 tahminciye sponsorlu analitik başarı ödülleri verilecektir.</p>
      </div>
      <div id="leaderboard-table" class="bg-slate-900 border border-slate-800 rounded-2xl overflow-hidden max-w-2xl mx-auto divide-y divide-slate-800"></div>
    </section>

    <!-- 4. GÖRÜNÜM: KULLANICI PORTFÖYÜ -->
    <section id="view-portfolio" class="hidden space-y-6">
      <h2 class="text-2xl font-black text-white">Portföyüm & Pozisyonlar</h2>
      <div id="portfolio-list" class="space-y-3"></div>
    </section>

    <!-- 5. GÖRÜNÜM: KURUMSAL BİLGİLENDİRME VE YASAL UYUM -->
    <section id="view-corporate" class="hidden space-y-6 max-w-3xl mx-auto">
      <div class="border-b border-slate-800 pb-4">
        <h2 class="text-2xl font-black text-white">Kurumsal Bilgilendirme ve Güvence</h2>
        <p class="text-xs text-slate-400">Türkiye mevzuatına (7258 sayılı kanun ve SPK) tam uyumlu bilgi platformu.</p>
      </div>
      <div class="bg-slate-900 border border-slate-800 rounded-2xl p-6 space-y-4 text-xs leading-relaxed text-slate-300">
        <h3 class="text-sm font-bold text-white">1. OVP (OYVER Puanı) Nedir?</h3>
        <p>OVP, platform içi öngörü isabetini ve lig sıralamasını belirleyen kapalı devre bir oyunlaştırma ve itibar puanıdır. Kesinlikle gerçek paraya dönüştürülemez, devredilemez ve parayla satın alınamaz.</p>
        <h3 class="text-sm font-bold text-white">2. Yatırım Tavsiyesi Değildir</h3>
        <p>Sitedeki tüm oranlar ve olasılıklar topluluğun kolektif fikir beklentisini temsil eder. Sermaye piyasası mevzuatı kapsamında yatırım danışmanlığı teşkil etmez.</p>
      </div>
    </section>

  </main>

  <!-- MOBİL ALTTAN AÇILIR İŞLEM ÇEKMECESİ -->
  <div id="trade-drawer-backdrop" onclick="closeDrawer()" class="fixed inset-0 bg-black/70 backdrop-blur-sm z-50 hidden transition-opacity"></div>
  <div id="trade-drawer" class="fixed bottom-0 left-0 right-0 max-w-lg mx-auto bg-slate-900 border-t border-slate-800 rounded-t-3xl p-6 z-50 transform translate-y-full transition-transform duration-300">
    <div class="w-12 h-1.5 bg-slate-700 rounded-full mx-auto mb-4"></div>
    <div id="drawer-content" class="space-y-4"></div>
  </div>

  <!-- MOBİL SABİT ALT NAVİGASYON -->
  <nav class="md:hidden fixed bottom-0 left-0 right-0 bg-slate-950/95 border-t border-slate-800 backdrop-blur-md px-6 py-2.5 z-40 flex justify-between text-[11px] font-bold text-slate-400">
    <button onclick="navigateTo('feed')" class="flex flex-col items-center gap-1 hover:text-white"><span>🌐</span><span>Piyasalar</span></button>
    <button onclick="navigateTo('leaderboard')" class="flex flex-col items-center gap-1 hover:text-white"><span>🏆</span><span>Sıralama</span></button>
    <button onclick="navigateTo('portfolio')" class="flex flex-col items-center gap-1 hover:text-white"><span>💼</span><span>Portföy</span></button>
    <button onclick="navigateTo('corporate')" class="flex flex-col items-center gap-1 hover:text-white"><span>⚖️</span><span>Hakkında</span></button>
  </nav>

  <!-- İSTEMCİ MOTORU -->
  <script>
    let globalState = null;
    let currentCategory = 'TÜMÜ';
    let activeMarketId = null;
    let selectedOutcome = 'YES';
    let activeReplyBoxId = null;
    let ws = null;

    function initWebSocket() {
      const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      ws = new WebSocket(proto + '//' + window.location.host);
      ws.onmessage = (e) => {
        const msg = JSON.parse(e.data);
        if (msg.type === 'PRICE_UPDATE') {
          const idx = globalState.markets.findIndex(m => m.id === msg.marketId);
          if (idx !== -1) {
            globalState.markets[idx].yesPrice = msg.newPrice;
            if (msg.market) globalState.markets[idx] = msg.market;
            // Tüm sayfayı silmeden sadece fiyat rozetlerini güncelle (DOM Thrashing Koruması)
            updatePriceElements(msg.marketId, msg.newPrice);
          }
        }
      };
    }

    function updatePriceElements(marketId, newPrice) {
      const probEl = document.getElementById('prob-' + marketId);
      if (probEl) probEl.innerText = '%' + newPrice;
      const bigProb = document.getElementById('big-prob-' + marketId);
      if (bigProb) bigProb.innerText = '%' + newPrice;
    }

    async function fetchData() {
      const res = await fetch('/api/state');
      globalState = await res.json();
      document.getElementById('user-balance').innerText = globalState.user.balance.toLocaleString();
      renderFeed();
      initWebSocket();
    }

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
      } else if (view === 'corporate') {
        document.getElementById('view-corporate').classList.remove('hidden');
      }
    }

    function generateSparkline(history) {
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

    function renderFeed() {
      const catContainer = document.getElementById('categories-container');
      catContainer.innerHTML = globalState.categories.map(c => \`
        <button onclick="filterCategory('\${c}')" class="px-3.5 py-1.5 rounded-xl text-xs font-bold whitespace-nowrap transition \${
          currentCategory === c ? 'bg-indigo-600 text-white' : 'bg-slate-900 border border-slate-800 text-slate-400 hover:text-white'
        }">\${c}</button>
      \`).join('');

      const filtered = globalState.markets.filter(m => currentCategory === 'TÜMÜ' || m.category === currentCategory);
      document.getElementById('markets-count').innerText = \`\${filtered.length} Aktif Pazar\`;

      const grid = document.getElementById('markets-grid');
      grid.innerHTML = filtered.map(m => \`
        <div class="bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-lg flex flex-col justify-between">
          <div>
            <div class="flex justify-between items-center text-xs mb-2.5">
              <span class="text-[11px] font-bold text-indigo-400 bg-indigo-950 border border-indigo-900 px-2 py-0.5 rounded">\${m.category}</span>
              <span class="text-slate-400 text-[11px]">\${m.closeTime}</span>
            </div>
            <h3 onclick="navigateTo('detail', \${m.id})" class="font-bold text-slate-100 text-base leading-snug hover:text-indigo-400 cursor-pointer transition mb-4">
              \${m.title}
            </h3>
          </div>

          <div class="space-y-4">
            <div class="flex items-center justify-between bg-slate-950 p-3 rounded-xl border border-slate-800">
              <div>
                <div class="text-[10px] text-slate-400 font-semibold uppercase">Olasılık</div>
                <div id="prob-\${m.id}" class="text-2xl font-black text-emerald-400 tabular">%\${m.yesPrice}</div>
              </div>
              <div>\${generateSparkline(m.history)}</div>
            </div>

            <div class="flex items-center justify-between text-[11px] text-slate-400">
              <span>👥 \${m.participantsCount} Katılımcı</span>
              <span>💬 \${m.comments.length} Görüş</span>
            </div>

            <div class="grid grid-cols-2 gap-2 pt-2 border-t border-slate-800">
              <button onclick="openDrawer(\${m.id}, 'YES')" class="py-2.5 rounded-xl font-bold text-xs bg-slate-800 hover:bg-emerald-600 text-slate-200 hover:text-white border border-slate-700 transition">
                EVET (%\${m.yesPrice})
              </button>
              <button onclick="openDrawer(\${m.id}, 'NO')" class="py-2.5 rounded-xl font-bold text-xs bg-slate-800 hover:bg-rose-600 text-slate-200 hover:text-white border border-slate-700 transition">
                HAYIR (%\${100 - m.yesPrice})
              </button>
            </div>
          </div>
        </div>
      \`).join('');
    }

    function filterCategory(c) {
      currentCategory = c;
      renderFeed();
    }

    // ============================================================================
    // DETAY VE HİYERARŞİK YORUMLAR (SOSYAL DİYALOG)
    // ============================================================================
    function renderDetail(marketId) {
      activeMarketId = marketId;
      const m = globalState.markets.find(x => x.id === marketId);
      if (!m) return;

      const container = document.getElementById('detail-container');
      container.innerHTML = \`
        <div class="space-y-3">
          <div class="flex items-center gap-2 text-xs">
            <span class="bg-indigo-950 text-indigo-400 border border-indigo-800 px-2.5 py-0.5 rounded font-bold">\${m.category}</span>
            <span class="text-slate-400">Kapanış: \${m.closeTime}</span>
          </div>
          <h1 class="text-2xl md:text-3xl font-black text-white leading-tight">\${m.title}</h1>
        </div>

        <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
          <div class="lg:col-span-2 space-y-6">

            <!-- Fiyat & Hacim Kartı -->
            <div class="bg-slate-900 border border-slate-800 rounded-3xl p-6 flex justify-between items-baseline">
              <div>
                <div class="text-xs text-slate-400 font-semibold uppercase">Piyasa Olasılığı</div>
                <div id="big-prob-\${m.id}" class="text-5xl font-black text-emerald-400 tabular">%\${m.yesPrice}</div>
              </div>
              <div class="text-right">
                <div class="text-xs text-slate-400 font-semibold">Toplam Hacim</div>
                <div class="text-lg font-bold text-white">\${m.volumeOvp.toLocaleString()} OVP</div>
              </div>
            </div>

            <!-- Resmî KAP & Haber Akışı Bandı -->
            <div class="bg-slate-900 border border-slate-800 rounded-3xl p-5 space-y-3">
              <h3 class="text-xs font-bold text-slate-400 uppercase tracking-wider flex items-center gap-2">
                <span>📰</span> İlgili Haber & Resmî KAP Bildirimleri
              </h3>
              <div class="space-y-2">
                \${m.newsFeed.map(n => \`
                  <div class="p-3 bg-slate-950 rounded-xl border border-slate-800/80 flex justify-between items-center text-xs">
                    <span class="font-semibold text-slate-200">\${n.title}</span>
                    <span class="text-[10px] text-indigo-400 bg-indigo-950 px-2 py-0.5 rounded font-bold">\${n.source}</span>
                  </div>
                \`).join('')}
              </div>
            </div>

            <!-- Kurallar & Çözümleme -->
            <div class="bg-slate-900 border border-slate-800 rounded-3xl p-6 space-y-3">
              <h3 class="text-sm font-bold text-white flex items-center gap-2"><span>⚖️</span> Çözümleme Kuralları</h3>
              <div class="text-xs text-slate-300 leading-relaxed whitespace-pre-line bg-slate-950 p-4 rounded-xl border border-slate-800">
                \${m.rules}
              </div>
              <div class="text-xs text-slate-400 pt-1">
                Resmî Kaynak: <a href="\${m.verifiedSource}" target="_blank" class="text-indigo-400 font-bold hover:underline">\${m.sourceName} ↗</a>
              </div>
            </div>

            <!-- Hiyerarşik Tartışma ve Sosyal Diyalog -->
            <div class="bg-slate-900 border border-slate-800 rounded-3xl p-6 space-y-5">
              <h3 class="text-sm font-bold text-white flex items-center gap-2"><span>💬</span> Topluluk Analizleri ve Yanıtlar</h3>

              <!-- Kök Yorum Girişi -->
              <div class="space-y-2 bg-slate-950 p-4 rounded-2xl border border-slate-800">
                <textarea id="comment-text" rows="2" placeholder="Gerekçenizi paylaşın (Örn: Enflasyon verisi açıklandı...)" class="w-full bg-transparent text-xs text-white placeholder-slate-500 focus:outline-none resize-none"></textarea>
                <div class="flex justify-between items-center pt-2 border-t border-slate-800">
                  <div class="flex gap-2">
                    <button type="button" onclick="setStance('YES')" id="st-yes" class="px-2.5 py-1 rounded text-[10px] font-bold bg-emerald-950 text-emerald-400 border border-emerald-800">EVET Savun</button>
                    <button type="button" onclick="setStance('NO')" id="st-no" class="px-2.5 py-1 rounded text-[10px] font-bold bg-slate-800 text-slate-400 border border-slate-700">HAYIR Savun</button>
                  </div>
                  <button onclick="postComment(\${m.id})" class="px-4 py-1.5 bg-indigo-600 hover:bg-indigo-500 text-white rounded-xl text-xs font-bold transition">Yayınla</button>
                </div>
              </div>

              <!-- İç İçe Yorum Listesi -->
              <div class="space-y-4">
                \${m.comments.map(c => \`
                  <div class="bg-slate-950 p-4 rounded-2xl border border-slate-800 space-y-2.5 text-xs">
                    <div class="flex justify-between items-center">
                      <div class="flex items-center gap-2">
                        <span class="font-bold text-slate-200 text-sm">\${c.user}</span>
                        <span class="text-[9px] font-black px-2 py-0.5 rounded \${
                          c.stance === 'YES' ? 'bg-emerald-950 text-emerald-400 border border-emerald-800' :
                          c.stance === 'NO' ? 'bg-rose-950 text-rose-400 border border-rose-800' : 'bg-slate-800 text-slate-400'
                        }">\${c.stance === 'YES' ? 'EVET Tarafında' : c.stance === 'NO' ? 'HAYIR Tarafında' : 'Nötr'}</span>
                      </div>
                      <span class="text-[10px] text-slate-500">\${c.time}</span>
                    </div>

                    <p class="text-slate-300 leading-relaxed">\${c.text}</p>

                    <div class="flex items-center gap-3 pt-2 text-[11px] text-slate-400 border-t border-slate-900">
                      <button onclick="toggleReplyBox(\${c.id})" class="text-indigo-400 hover:underline font-bold">↳ Yanıtla</button>
                    </div>

                    <!-- Alt Yanıt Yazma Kutusu -->
                    <div id="reply-box-\${c.id}" class="hidden pt-2">
                      <div class="flex gap-2">
                        <input type="text" id="reply-input-\${c.id}" placeholder="\${c.user} kullanıcısına yanıt ver..." class="flex-1 bg-slate-900 border border-slate-800 rounded-xl px-3 py-1.5 text-xs text-white focus:outline-none">
                        <button onclick="submitReply(\${m.id}, \${c.id})" class="px-3 py-1.5 bg-indigo-600 text-white rounded-xl text-xs font-bold">Gönder</button>
                      </div>
                    </div>

                    <!-- Alt Yanıtlar (Thread) -->
                    \${c.replies && c.replies.length > 0 ? \`
                      <div class="pl-4 border-l-2 border-indigo-900/40 space-y-2 mt-2">
                        \${c.replies.map(r => \`
                          <div class="bg-slate-900/60 p-3 rounded-xl border border-slate-800/60 text-xs space-y-1">
                            <div class="flex justify-between items-center">
                              <div class="flex items-center gap-1.5">
                                <span class="font-bold text-slate-200">\${r.user}</span>
                                <span class="text-[8px] font-black px-1.5 py-0.2 rounded \${r.stance === 'YES' ? 'bg-emerald-950 text-emerald-400' : 'bg-rose-950 text-rose-400'}">\${r.stance}</span>
                              </div>
                              <span class="text-[10px] text-slate-500">\${r.time}</span>
                            </div>
                            <p class="text-slate-300">\${r.text}</p>
                          </div>
                        \`).join('')}
                      </div>
                    \` : ''}
                  </div>
                \`).join('')}
              </div>
            </div>

          </div>

          <!-- Sağ Kolon: Tahmin Terminali -->
          <div class="space-y-6">
            <div class="bg-slate-900 border border-slate-800 rounded-3xl p-6 space-y-5 sticky top-20 shadow-xl">
              <h3 class="text-sm font-bold text-white">Hızlı Tahmin Masası</h3>

              <div class="grid grid-cols-2 gap-2">
                <button onclick="setDetailSide('YES')" id="d-btn-yes" class="py-3 rounded-xl font-bold text-xs border bg-emerald-600 border-emerald-500 text-white">
                  EVET (%\${m.yesPrice})
                </button>
                <button onclick="setDetailSide('NO')" id="d-btn-no" class="py-3 rounded-xl font-bold text-xs border bg-slate-800 border-slate-700 text-slate-400">
                  HAYIR (%\${100 - m.yesPrice})
                </button>
              </div>

              <div class="space-y-1.5">
                <input type="number" id="detail-amount" value="500" class="w-full bg-slate-950 border border-slate-800 rounded-xl px-4 py-2.5 text-base font-bold text-white focus:outline-none">
                <div class="flex gap-1.5">
                  <button onclick="setDetailAmount(250)" class="flex-1 py-1 bg-slate-800 rounded-lg text-xs font-bold text-slate-300">+250</button>
                  <button onclick="setDetailAmount(500)" class="flex-1 py-1 bg-slate-800 rounded-lg text-xs font-bold text-slate-300">+500</button>
                  <button onclick="setDetailAmount(1000)" class="flex-1 py-1 bg-slate-800 rounded-lg text-xs font-bold text-slate-300">+1.000</button>
                </div>
              </div>

              <button onclick="confirmDetailTrade()" class="w-full py-4 bg-indigo-600 hover:bg-indigo-500 text-white rounded-xl font-bold text-sm shadow-xl shadow-indigo-600/30 transition">
                Tahmini Onayla
              </button>
            </div>
          </div>
        </div>
      \`;
    }

    let commentStance = 'YES';
    function setStance(s) {
      commentStance = s;
      document.getElementById('st-yes').className = s === 'YES'
        ? 'px-2.5 py-1 rounded text-[10px] font-bold bg-emerald-950 text-emerald-400 border border-emerald-800'
        : 'px-2.5 py-1 rounded text-[10px] font-bold bg-slate-800 text-slate-400 border border-slate-700';
      document.getElementById('st-no').className = s === 'NO'
        ? 'px-2.5 py-1 rounded text-[10px] font-bold bg-rose-950 text-rose-400 border border-rose-800'
        : 'px-2.5 py-1 rounded text-[10px] font-bold bg-slate-800 text-slate-400 border border-slate-700';
    }

    async function postComment(mId) {
      const text = document.getElementById('comment-text').value;
      if (!text || text.trim().length < 2) return alert('Lütfen geçerli bir yorum yazın.');
      const res = await fetch('/api/comments', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ marketId: mId, text, stance: commentStance })
      });
      const data = await res.json();
      if (data.success) {
        const m = globalState.markets.find(x => x.id === mId);
        m.comments.unshift(data.comment);
        renderDetail(mId);
      }
    }

    function toggleReplyBox(commId) {
      const box = document.getElementById('reply-box-' + commId);
      box.classList.toggle('hidden');
    }

    async function submitReply(mId, commId) {
      const inp = document.getElementById('reply-input-' + commId);
      const text = inp.value;
      if (!text || text.trim().length < 2) return alert('Lütfen geçerli bir yanıt yazın.');

      const res = await fetch('/api/comments/reply', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ marketId: mId, parentCommentId: commId, text })
      });
      const data = await res.json();
      if (data.success) {
        const m = globalState.markets.find(x => x.id === mId);
        const parent = m.comments.find(c => c.id === commId);
        parent.replies.push(data.reply);
        renderDetail(mId);
      }
    }

    function setDetailSide(side) {
      selectedOutcome = side;
      document.getElementById('d-btn-yes').className = side === 'YES'
        ? 'py-3 rounded-xl font-bold text-xs border bg-emerald-600 border-emerald-500 text-white'
        : 'py-3 rounded-xl font-bold text-xs border bg-slate-800 border-slate-700 text-slate-400';
      document.getElementById('d-btn-no').className = side === 'NO'
        ? 'py-3 rounded-xl font-bold text-xs border bg-rose-600 border-rose-500 text-white'
        : 'py-3 rounded-xl font-bold text-xs border bg-slate-800 border-slate-700 text-slate-400';
    }

    function setDetailAmount(val) {
      document.getElementById('detail-amount').value = val;
    }

    async function confirmDetailTrade() {
      const amount = Number(document.getElementById('detail-amount').value);
      const res = await fetch('/api/trade', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ marketId: activeMarketId, outcome: selectedOutcome, amount })
      });
      const data = await res.json();
      if (data.success) {
        document.getElementById('user-balance').innerText = data.newBalance.toLocaleString();
        globalState.user.balance = data.newBalance;
        const idx = globalState.markets.findIndex(x => x.id === activeMarketId);
        globalState.markets[idx] = data.market;
        renderDetail(activeMarketId);
      } else {
        alert(data.message);
      }
    }

    // ============================================================================
    // PORTFÖY VE LİDERLİK
    // ============================================================================
    function renderLeaderboard() {
      const sample = [
        { rank: 1, name: 'MakroUstadı', balance: 142500 },
        { rank: 2, name: 'BistAnaliz_Ali', balance: 118400 },
        { rank: 42, name: 'Tahminci_Leisan (Sen)', balance: globalState.user.balance }
      ];
      document.getElementById('leaderboard-table').innerHTML = sample.map(u => \`
        <div class="flex items-center justify-between p-4 px-6 \${u.rank === 42 ? 'bg-indigo-950/40 border-l-4 border-indigo-500' : ''}">
          <div class="flex items-center gap-3">
            <span class="w-6 font-black text-sm text-slate-400">#\${u.rank}</span>
            <span class="font-bold text-sm text-slate-200">\${u.name}</span>
          </div>
          <span class="font-black text-sm text-emerald-400 tabular">\${u.balance.toLocaleString()} OVP</span>
        </div>
      \`).join('');
    }

    function renderPortfolio() {
      const list = globalState.user.positions;
      const c = document.getElementById('portfolio-list');
      if (list.length === 0) {
        c.innerHTML = '<div class="p-8 text-center text-slate-500 text-xs bg-slate-900 rounded-2xl border border-slate-800">Henüz açık bir pozisyonunuz yok.</div>';
        return;
      }
      c.innerHTML = list.map(p => \`
        <div class="bg-slate-900 border border-slate-800 rounded-2xl p-4 flex justify-between items-center">
          <div>
            <span class="text-[10px] font-black px-2 py-0.5 rounded \${p.outcome === 'YES' ? 'bg-emerald-950 text-emerald-400' : 'bg-rose-950 text-rose-400'}">\${p.outcome} TAHMİNİ</span>
            <h4 class="font-bold text-white text-sm mt-1">\${p.marketTitle}</h4>
            <div class="text-[11px] text-slate-400">\${p.shares} Pay @ \${p.entryPrice} OVP</div>
          </div>
          <div class="text-right">
            <div class="text-xs text-slate-400">Olası Dönüş</div>
            <div class="text-base font-black text-emerald-400 tabular">\${(p.shares * 100).toFixed(0)} OVP</div>
          </div>
        </div>
      \`).join('');
    }

    // Mobil Çekmece
    function openDrawer(mId, side) {
      activeMarketId = mId;
      selectedOutcome = side;
      const m = globalState.markets.find(x => x.id === mId);
      document.getElementById('drawer-content').innerHTML = \`
        <h4 class="font-bold text-white text-sm">\${m.title}</h4>
        <div class="grid grid-cols-2 gap-2">
          <button onclick="selectedOutcome='YES'; openDrawer(\${mId},'YES')" class="py-2.5 rounded-xl font-bold text-xs border \${side==='YES'?'bg-emerald-600 border-emerald-500 text-white':'bg-slate-800 border-slate-700 text-slate-400'}">EVET (%\${m.yesPrice})</button>
          <button onclick="selectedOutcome='NO'; openDrawer(\${mId},'NO')" class="py-2.5 rounded-xl font-bold text-xs border \${side==='NO'?'bg-rose-600 border-rose-500 text-white':'bg-slate-800 border-slate-700 text-slate-400'}">HAYIR (%\${100-m.yesPrice})</button>
        </div>
        <input type="number" id="drawer-amt" value="500" class="w-full bg-slate-950 border border-slate-800 rounded-xl px-4 py-2.5 text-base font-bold text-white text-center focus:outline-none">
        <button onclick="execDrawerTrade()" class="w-full py-3.5 bg-indigo-600 text-white rounded-xl font-bold text-sm shadow-xl shadow-indigo-600/30">Tahmini Onayla</button>
      \`;
      document.getElementById('trade-drawer-backdrop').classList.remove('hidden');
      document.getElementById('trade-drawer').classList.remove('translate-y-full');
    }

    function closeDrawer() {
      document.getElementById('trade-drawer').classList.add('translate-y-full');
      document.getElementById('trade-drawer-backdrop').classList.add('hidden');
    }

    async function execDrawerTrade() {
      const amount = Number(document.getElementById('drawer-amt').value);
      const res = await fetch('/api/trade', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ marketId: activeMarketId, outcome: selectedOutcome, amount })
      });
      const data = await res.json();
      if (data.success) {
        document.getElementById('user-balance').innerText = data.newBalance.toLocaleString();
        globalState.user.balance = data.newBalance;
        const idx = globalState.markets.findIndex(x => x.id === activeMarketId);
        globalState.markets[idx] = data.market;
        closeDrawer();
        renderFeed();
      } else {
        alert(data.message);
      }
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
// 5. WEBSOCKET YAYINI
// ============================================================================
const wss = new WebSocket.Server({ server });

function broadcastPriceUpdate(marketId, newPrice, market) {
  const payload = JSON.stringify({ type: 'PRICE_UPDATE', marketId, newPrice, market });
  wss.clients.forEach(c => {
    if (c.readyState === WebSocket.OPEN) c.send(payload);
  });
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`OYVER Sosyal & İnce Ayarlı Motor http://localhost:${PORT} portunda devrede.`);
});
