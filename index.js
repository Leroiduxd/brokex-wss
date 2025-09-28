import fetch from 'node-fetch';
import { WebSocketServer } from 'ws';

// Configuration
const PORT = 8081; // Port WebSocket
const API_KEY = '1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2'; // Ta clé Supra
const BASE_URL = 'https://prod-kline-rest.supra.com';

// Liste des paires à surveiller
const PAIRS = [
  'aapl_usd', 'amzn_usd', 'coin_usd', 'goog_usd', 'gme_usd',
  'intc_usd', 'ko_usd', 'mcd_usd', 'msft_usd', 'ibm_usd',
  'meta_usd', 'nvda_usd', 'tsla_usd',
  'aud_usd', 'eur_usd', 'gbp_usd', 'nzd_usd',
  'usd_cad', 'usd_chf', 'usd_jpy',
  'xag_usd', 'xau_usd',
  'btc_usdt', 'eth_usdt', 'sol_usdt', 'xrp_usdt',
  'avax_usdt', 'doge_usdt', 'trx_usdt', 'ada_usdt',
  'sui_usdt', 'link_usdt'
];

const PAIR_METADATA = {
  aapl_usd: { id: 6004, name: 'APPLE INC.' },
  amzn_usd: { id: 6005, name: 'AMAZON' },
  coin_usd: { id: 6010, name: 'COINBASE' },
  goog_usd: { id: 6003, name: 'ALPHABET INC.' },
  gme_usd:  { id: 6011, name: 'GAMESTOP CORP.' },
  intc_usd: { id: 6009, name: 'INTEL CORPORATION' },
  ko_usd:   { id: 6059, name: 'COCA-COLA CO' },
  mcd_usd:  { id: 6068, name: "MCDONALD'S CORP" },
  msft_usd: { id: 6001, name: 'MICROSOFT CORP' },
  ibm_usd:  { id: 6066, name: 'IBM' },
  meta_usd: { id: 6006, name: 'META PLATFORMS INC.' },
  nvda_usd: { id: 6002, name: 'NVIDIA CORP' },
  tsla_usd: { id: 6000, name: 'TESLA INC' },
  aud_usd:  { id: 5010, name: 'AUSTRALIAN DOLLAR' },
  eur_usd:  { id: 5000, name: 'EURO' },
  gbp_usd:  { id: 5002, name: 'GREAT BRITAIN POUND' },
  nzd_usd:  { id: 5013, name: 'NEW ZEALAND DOLLAR' },
  usd_cad:  { id: 5011, name: 'CANADIAN DOLLAR' },
  usd_chf:  { id: 5012, name: 'SWISS FRANC' },
  usd_jpy:  { id: 5001, name: 'JAPANESE YEN' },
  xag_usd:  { id: 5501, name: 'SILVER' },
  xau_usd:  { id: 5500, name: 'GOLD' },
  btc_usdt: { id: 0,    name: 'BITCOIN' },
  eth_usdt: { id: 1,    name: 'ETHEREUM' },
  sol_usdt: { id: 10,   name: 'SOLANA' },
  xrp_usdt: { id: 14,   name: 'RIPPLE' },
  avax_usdt:{ id: 5,    name: 'AVALANCHE' },
  doge_usdt:{ id: 3,    name: 'DOGECOIN' },
  trx_usdt: { id: 15,   name: 'TRON' },
  ada_usdt: { id: 16,   name: 'CARDANO' },
  sui_usdt: { id: 90,   name: 'SUI' },
  link_usdt:{ id: 2,    name: 'CHAINLINK' }
};

// ✅ Cache des dernières valeurs valides
const lastValidPrices = {};

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

console.log(`✅ Serveur WebSocket lancé sur le port ${PORT}.
- Appels API: séquentiels, ≤ 10 req/s
- Broadcast: 500ms
- Paires: ${PAIRS.length}`);

// ---------- Helpers de validation/prix ----------
function toNumberMaybe(x) {
  if (typeof x === 'number') return Number.isFinite(x) ? x : NaN;
  if (typeof x === 'string') {
    const s = x.replace(/,/g, ''); // supprime séparateurs de milliers éventuels
    const n = parseFloat(s);
    return Number.isFinite(n) ? n : NaN;
  }
  return NaN;
}

// On accepte un payload SEULEMENT s’il contient un champ prix numérique.
// On ne change PAS le nom de la clé (même format que l’API).
function normalizeValidPayload(obj) {
  if (!obj || typeof obj !== 'object') return null;

  // Priorité au champ "price" (endpoint /latest renvoie normalement price)
  const candidates = ['price', 'last', 'close', 'markPrice', 'value'];
  let foundKey = null;
  for (const k of candidates) {
    if (k in obj) {
      const n = toNumberMaybe(obj[k]);
      if (Number.isFinite(n)) {
        foundKey = k;
        // clone + force number pour éviter les strings
        const normalized = { ...obj };
        normalized[k] = n;
        return normalized;
      }
    }
  }
  return null; // pas de prix numérique → invalide
}

function defaultEntry(pair) {
  return {
    id: PAIR_METADATA[pair]?.id ?? null,
    name: PAIR_METADATA[pair]?.name || 'UNKNOWN',
    price: 0
  };
}

// ---------- Broadcast (500ms) : EXACTEMENT le même format ----------
function broadcastFromCache() {
  try {
    const results = {};
    for (const pair of PAIRS) {
      // Même structure que ton script original :
      // { id, name, ...data } ; si pas de data valide encore → price:0
      results[pair] = lastValidPrices[pair] ?? defaultEntry(pair);
    }
    const payload = JSON.stringify(results);
    wss.clients.forEach(client => {
      if (client.readyState === 1) client.send(payload);
    });
  } catch (err) {
    console.error('❌ Erreur broadcast:', err?.message || err);
  }
}
setInterval(broadcastFromCache, 500);

// Connexion client (log seulement, format strictement inchangé)
wss.on('connection', () => {
  console.log('🟢 Nouveau client connecté');
});

// ---------- Poller séquentiel (≤ 10 req/s) ----------
const MAX_RPS = 10;
const MIN_GAP_MS = Math.ceil(1000 / MAX_RPS); // 100ms
let pairIndex = 0;
let lastCallTs = 0;

// Appelle UNE paire, met à jour le cache uniquement si réponse VALIDE (prix numérique)
async function fetchOnePairAndUpdateCache(pair) {
  try {
    const res = await fetch(`${BASE_URL}/latest?trading_pair=${pair}`, {
      headers: { 'x-api-key': API_KEY }
    });

    if (!res.ok) {
      // 429 / 5xx etc. → n’écrase rien
      return;
    }

    const raw = await res.json().catch(() => null);
    const normalized = normalizeValidPayload(raw);

    if (normalized) {
      // EXACTEMENT le même format que ton script : { id, name, ...data }
      lastValidPrices[pair] = {
        id: PAIR_METADATA[pair]?.id ?? null,
        name: PAIR_METADATA[pair]?.name || 'UNKNOWN',
        ...normalized
      };
    }
    // sinon: ignore et garde l’ancien cache
  } catch {
    // erreur réseau → ignore
  } finally {
    // Si aucune valeur jamais reçue: initialise une seule fois
    if (!lastValidPrices[pair]) {
      lastValidPrices[pair] = defaultEntry(pair);
    }
  }
}

// Boucle: 1 requête à la fois, écart min 100ms (≤ 10 req/s), passe au suivant
function rateLimitedLoop() {
  const now = Date.now();
  const sinceLast = now - lastCallTs;
  const wait = Math.max(0, MIN_GAP_MS - sinceLast);

  setTimeout(async () => {
    const pair = PAIRS[pairIndex];
    await fetchOnePairAndUpdateCache(pair);

    lastCallTs = Date.now();
    pairIndex = (pairIndex + 1) % PAIRS.length;

    rateLimitedLoop();
  }, wait);
}

// Lancement du poller
rateLimitedLoop();

