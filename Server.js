const http = require('http');
const WebSocket = require('ws');

// --- 1. BELLEK İÇİ VERİTABANI & EMİR DEFTERİ MOTORU ---
const state = {
  users: {
    'demo-user': { id: 'demo-user', username: 'Tahminci_Demo', balance: 10000, isPro: true }
  },
  markets: [
    {
      id: 1,
      title: 'TCMB Bir Sonraki Toplantıda Politika Faizini İndirecek mi?',
      category: 'EKONOMİ',
      closeTime: '2026-10-25',
      yesPrice: 65,
      orderBook: {
        bids: [{ price: 64, shares: 150 }, { price: 63, shares: 300 }],
        asks: [{ price: 66, shares: 120 }, { price: 67, shares: 250 }]
      }
    },
    {
      id: 2,
      title: 'Süper Lig Derbisini Ev Sahibi Takım Kazanacak mı?',
      category: 'SPOR',
      closeTime: '2026-10-18',
      yesPrice: 42,
      orderBook: {
        bids: [{ price: 41, shares: 80 }, { price: 40, shares: 140 }],
        asks: [{ price: 43, shares: 90 }, { price: 44, shares: 210 }]
      }
    }
  ],
  positions: []
};

// Basit Eşleştirme Mantığı
function executeTrade(marketId, outcome, amountOvp) {
  const market = state.markets.find(m => m.id === marketId);
  const user = state.users['demo-user'];

  if (!market || user.balance < amountOvp) {
    return { success: false, message: 'Yetersiz bakiye veya geçersiz pazar.' };
  }

  const effectivePrice = outcome === 'YES' ? market.yesPrice : (100 - market.yesPrice);
  const shares = Number((amountOvp / effectivePrice).toFixed(1));

  user.balance -= amountOvp;

  // Fiyatı dinamik kaydır (Oyunlaştırılmış AMM/Orderbook Kayması)
  const priceShift = outcome === 'YES' ? 1 : -1;
  market.yesPrice = Math.min(99, Math.max(1, market.yesPrice + priceShift));

  state.positions.push({
    id: Date.now(),
    marketId,
    outcome,
    shares,
    entryPrice: effectivePrice,
    cost: amountOvp
  });

  return { success: true, newBalance: user.balance, newPrice: market.yesPrice, shares };
}

// --- 2. HTTP VE REST API SUNUCUSU ---
const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  // API Uçları
  if (req.url === '/api/state' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(state));
    return;
  }

  if (req.url === '/api/trade' && req.method === 'POST') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      const { marketId, outcome, amount } = JSON.parse(body);
      const result = executeTrade(Number(marketId), outcome, Number(amount));

      if (result.success) {
        broadcastPriceUpdate(marketId, result.newPrice);
      }

      res.writeHead(result.success ? 200 : 400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(result));
    });
    return;
  }

  // --- 3. MOBİL ÖNCELİKLİ ÖZGÜN ARAYÜZ (HTML / CSS / JS) ---
  if (req.url === '/' || req.url === '/index.html') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(`
<!DOCTYPE html>
<html lang="tr">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>OYVER — Prediction Market</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    body { background-color: #020617; color: #f8fafc; font-family: system-ui, sans-serif; }
  </style>
</head>
<body class="pb-20">
  <!-- Üst Bar -->
  <header class="border-b border-slate-800 bg-slate-900/80 backdrop-blur sticky top-0 z-50 p-4">
    <div class="max-w-md mx-auto flex justify-between items-center">
      <div class="flex items-center gap-2">
        <div class="w-8 h-8 rounded-lg bg-indigo-600 flex items-center justify-center font-black text-white text-lg">O</div>
        <span class="font-extrabold tracking-wider text-xl text-white">OYVER</span>
      </div>
      <div class="bg-slate-800 border border-slate-700 rounded-full px-3 py-1 flex items-center gap-1.5 text-xs font-semibold">
        <span class="w-2 h-2 rounded-full bg-emerald-400 animate-pulse"></span>
        <span id="user-balance">10,000</span> <span class="text-indigo-400">OVP</span>
      </div>
    </div>
  </header>

  <!-- Ana İçerik -->
  <main class="max-w-md mx-auto p-4 space-y-6">
    <div class="space-y-1">
      <h2 class="text-lg font-bold text-slate-100">Gündemdeki Pazarlar</h2>
      <p class="text-xs text-slate-400">Tahminini seç, tek tuşla işlem yap.</p>
    </div>

    <div id="markets-container" class="space-y-4">
      <!-- JavaScript dinamik dolduracak -->
    </div>
  </main>

  <!-- Mobil Alt Menü -->
  <nav class="fixed bottom-0 left-0 right-0 bg-slate-900/90 border-t border-slate-800 backdrop-blur p-3 max-w-md mx-auto flex justify-around text-xs font-semibold text-slate-400">
    <span class="text-indigo-400 cursor-pointer">Piyasalar</span>
    <span class="hover:text-slate-200 cursor-pointer">Ligler</span>
    <span class="hover:text-slate-200 cursor-pointer">Pazar Öner</span>
    <span class="hover:text-slate-200 cursor-pointer">Profil</span>
  </nav>

  <script>
    let ws;
    let selectedOutcomes = {};

    function connectWs() {
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      ws = new WebSocket(protocol + '//' + window.location.host);
      ws.onmessage = (event) => {
        const msg = JSON.parse(event.data);
        if (msg.type === 'PRICE_UPDATE') {
          const priceBadge = document.getElementById('prob-' + msg.marketId);
          if (priceBadge) {
            priceBadge.innerText = '%' + msg.newPrice;
            priceBadge.classList.add('scale-110');
            setTimeout(() => priceBadge.classList.remove('scale-110'), 300);
          }
        }
      };
    }

    async function loadData() {
      const res = await fetch('/api/state');
      const data = await res.json();
      document.getElementById('user-balance').innerText = Number(data.users['demo-user'].balance).toLocaleString();

      const container = document.getElementById('markets-container');
      container.innerHTML = '';

      data.markets.forEach(m => {
        selectedOutcomes[m.id] = selectedOutcomes[m.id] || 'YES';
        const card = document.createElement('div');
        card.className = 'bg-slate-900 border border-slate-800 rounded-2xl p-5 shadow-lg space-y-4';
        card.innerHTML = \`
          <div class="flex justify-between items-center text-xs">
            <span class="px-2 py-0.5 rounded bg-indigo-950 text-indigo-400 border border-indigo-800/40 font-bold">\${m.category}</span>
            <span class="text-slate-400">Bitiş: \${m.closeTime}</span>
          </div>

          <h3 class="font-bold text-slate-100 text-base leading-snug">\${m.title}</h3>

          <div class="flex items-center justify-between bg-slate-950 p-3 rounded-xl border border-slate-800">
            <span class="text-xs text-slate-400 font-medium">Piyasa Olasılığı:</span>
            <span id="prob-\${m.id}" class="text-xl font-black text-emerald-400 transition-all duration-300">%\${m.yesPrice}</span>
          </div>

          <!-- Seçim Butonları -->
          <div class="grid grid-cols-2 gap-2">
            <button onclick="setOutcome(\${m.id}, 'YES')" id="btn-yes-\${m.id}" class="py-2.5 rounded-xl font-bold text-xs border transition \${selectedOutcomes[m.id] === 'YES' ? 'bg-emerald-600 border-emerald-500 text-white' : 'bg-slate-800 border-slate-700 text-slate-400'}">
              EVET (%\${m.yesPrice})
            </button>
            <button onclick="setOutcome(\${m.id}, 'NO')" id="btn-no-\${m.id}" class="py-2.5 rounded-xl font-bold text-xs border transition \${selectedOutcomes[m.id] === 'NO' ? 'bg-rose-600 border-rose-500 text-white' : 'bg-slate-800 border-slate-700 text-slate-400'}">
              HAYIR (%\${100 - m.yesPrice})
            </button>
          </div>

          <!-- Hızlı İşlem Butonu -->
          <button onclick="trade(\${m.id})" class="w-full py-3 bg-indigo-600 hover:bg-indigo-500 active:scale-[0.98] transition rounded-xl font-bold text-sm text-white shadow-lg shadow-indigo-600/30">
            500 OVP ile Katıl
          </button>
        \`;
        container.appendChild(card);
      });
    }

    function setOutcome(marketId, outcome) {
      selectedOutcomes[marketId] = outcome;
      loadData();
    }

    async function trade(marketId) {
      const outcome = selectedOutcomes[marketId];
      const res = await fetch('/api/trade', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ marketId, outcome, amount: 500 })
      });
      const data = await res.json();
      if (data.success) {
        document.getElementById('user-balance').innerText = Number(data.newBalance).toLocaleString();
      } else {
        alert(data.message);
      }
    }

    window.onload = () => {
      loadData();
      connectWs();
    };
  </script>
</body>
</html>
    `);
    return;
  }

  res.writeHead(404);
  res.end('Not Found');
});

// --- 4. WEBSOCKET SUNUCUSU ---
const wss = new WebSocket.Server({ server });

function broadcastPriceUpdate(marketId, newPrice) {
  const payload = JSON.stringify({
    type: 'PRICE_UPDATE',
    marketId,
    newPrice
  });
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  });
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`OYVER Engine http://localhost:${PORT} üzerinde çalışıyor.`);
});
