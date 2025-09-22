import fetch from 'node-fetch';
import { WebSocketServer } from 'ws';

// Configuration
const PORT = 8081; // Port WebSocket
const API_KEY = '1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2'; // Ta clé Supra
const BASE_URL = "https://prod-kline-rest.supra.com";

// Liste des paires à surveiller
const PAIRS = [
  "aapl_usd", "amzn_usd", "coin_usd", "goog_usd", "gme_usd",
  "intc_usd", "ko_usd", "mcd_usd", "msft_usd", "ibm_usd",
  "meta_usd", "nvda_usd", "tsla_usd",
  "aud_usd", "eur_usd", "gbp_usd", "nzd_usd",
  "usd_cad", "usd_chf", "usd_jpy",
  "xag_usd", "xau_usd",
  "btc_usdt", "eth_usdt", "sol_usdt", "xrp_usdt",
  "avax_usdt", "doge_usdt", "trx_usdt", "ada_usdt",
  "sui_usdt", "link_usdt"
];

const PAIR_METADATA = {
  "aapl_usd": { id: 6004, name: "APPLE INC." },
  "amzn_usd": { id: 6005, name: "AMAZON" },
  "coin_usd": { id: 6010, name: "COINBASE" },
  "goog_usd": { id: 6003, name: "ALPHABET INC." },
  "gme_usd": { id: 6011, name: "GAMESTOP CORP." },
  "intc_usd": { id: 6009, name: "INTEL CORPORATION" },
  "ko_usd": { id: 6059, name: "COCA-COLA CO" },
  "mcd_usd": { id: 6068, name: "MCDONALD'S CORP" },
  "msft_usd": { id: 6001, name: "MICROSOFT CORP" },
  "ibm_usd": { id: 6066, name: "IBM" },
  "meta_usd": { id: 6006, name: "META PLATFORMS INC." },
  "nvda_usd": { id: 6002, name: "NVIDIA CORP" },
  "tsla_usd": { id: 6000, name: "TESLA INC" },
  "aud_usd": { id: 5010, name: "AUSTRALIAN DOLLAR" },
  "eur_usd": { id: 5000, name: "EURO" },
  "gbp_usd": { id: 5002, name: "GREAT BRITAIN POUND" },
  "nzd_usd": { id: 5013, name: "NEW ZEALAND DOLLAR" },
  "usd_cad": { id: 5011, name: "CANADIAN DOLLAR" },
  "usd_chf": { id: 5012, name: "SWISS FRANC" },
  "usd_jpy": { id: 5001, name: "JAPANESE YEN" },
  "xag_usd": { id: 5501, name: "SILVER" },
  "xau_usd": { id: 5500, name: "GOLD" },
  "btc_usdt": { id: 0, name: "BITCOIN" },
  "eth_usdt": { id: 1, name: "ETHEREUM" },
  "sol_usdt": { id: 10, name: "SOLANA" },
  "xrp_usdt": { id: 14, name: "RIPPLE" },
  "avax_usdt": { id: 5, name: "AVALANCHE" },
  "doge_usdt": { id: 3, name: "DOGECOIN" },
  "trx_usdt": { id: 15, name: "TRON" },
  "ada_usdt": { id: 16, name: "CARDANO" },
  "sui_usdt": { id: 90, name: "SUI" },
  "link_usdt": { id: 2, name: "CHAINLINK" }
};

// ---- Cache des derniers résultats valides par paire ----
const lastByPair = new Map(); // key = pair string, value = payload { id, name, ...data }
const nowISO = () => new Date().toISOString();

// WebSocket server avec compression
const wss = new WebSocketServer({
  port: PORT,
  perMessageDeflate: {
    zlibDeflateOptions: { level: 9 },
    zlibInflateOptions: { chunkSize: 1024 },
    clientNoContextTakeover: true,
    serverNoContextTakeover: true,
    threshold: 0
  }
});

console.log(`✅ Serveur WebSocket lancé sur le port ${PORT} (refresh ~1000ms).`);

// --- helpers ---
function isValidSupraPayload(data) {
  // On considère "valide" s'il y a au moins un instrument avec currentPrice
  try {
    const arr = data?.instruments;
    return Array.isArray(arr) && arr.length > 0 && arr[0]?.currentPrice != null;
  } catch {
    return false;
  }
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 8000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, { ...options, signal: ctrl.signal });
    return res;
  } finally {
    clearTimeout(t);
  }
}

// Fonction pour récupérer et diffuser tous les prix
async function fetchAllPricesAndBroadcast() {
  try {
    const responses = await Promise.allSettled(
      PAIRS.map(pair =>
        fetchWithTimeout(`${BASE_URL}/latest?trading_pair=${pair}`, {
          headers: { 'x-api-key': API_KEY }
        }).then(async (res) => {
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          const data = await res.json();
          return { pair, data };
        })
      )
    );

    const results = {};

    for (const r of responses) {
      if (r.status !== 'fulfilled') {
        // requête échouée -> fallback au cache si dispo
        const pair = r.reason?.pair || null; // pas toujours dispo
        // on ne peut pas connaître la pair depuis reason; on passe
        continue;
      }

      const { pair, data } = r.value;

      if (isValidSupraPayload(data)) {
        // OK: construire le payload et MAJ le cache
        const composed = {
          id: PAIR_METADATA[pair]?.id ?? null,
          name: PAIR_METADATA[pair]?.name || "UNKNOWN",
          refreshedAt: nowISO(),
          ...data
        };
        lastByPair.set(pair, composed);
        results[pair] = composed;
      } else {
        // payload vide / invalide -> fallback au cache
        const cached = lastByPair.get(pair);
        if (cached) {
          results[pair] = { ...cached, fallback: true, servedAt: nowISO() };
        } else {
          // pas encore de cache: on n’envoie rien pour cette pair
          // (optionnel) tu peux envoyer un stub si tu préfères:
          // results[pair] = { id: PAIR_METADATA[pair]?.id ?? null, name: PAIR_METADATA[pair]?.name || "UNKNOWN", instruments: [], fallback: true }
        }
      }
    }

    // Inclure aussi les paires non rafraîchies mais déjà en cache (pour “toujours renvoyer quelque chose”)
    for (const pair of PAIRS) {
      if (!(pair in results)) {
        const cached = lastByPair.get(pair);
        if (cached) {
          results[pair] = { ...cached, fallback: true, servedAt: nowISO() };
        }
      }
    }

    // Si on n'a strictement rien (premier tour et API down), on évite d'envoyer un payload vide
    if (Object.keys(results).length === 0) {
      console.warn("⚠️ Aucun résultat à diffuser (pas de cache encore disponible).");
      return;
    }

    const payload = JSON.stringify(results);

    wss.clients.forEach(client => {
      if (client.readyState === 1) {
        client.send(payload);
      }
    });
  } catch (err) {
    console.error("❌ Erreur récupération/diffusion:", err.message);
  }
}

// Rafraîchissement toutes les 1000 ms
setInterval(fetchAllPricesAndBroadcast, 1000);

// Connexion client
wss.on('connection', ws => {
  console.log("🟢 Nouveau client connecté");
  // Envoie immédiat du cache si dispo, pour ne pas attendre 1s
  if (lastByPair.size > 0) {
    const snapshot = {};
    for (const [pair, data] of lastByPair) snapshot[pair] = { ...data, servedAt: nowISO(), snapshot: true };
    try { ws.send(JSON.stringify(snapshot)); } catch {}
  }
});
