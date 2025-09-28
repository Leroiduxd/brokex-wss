import fetch from 'node-fetch';
import { WebSocketServer } from 'ws';

/**
 * ---------------------------
 * Configuration
 * ---------------------------
 */
const PORT = 8081; // Port WebSocket
const API_KEY = '1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2'; // Ta clé Supra
const BASE_URL = 'https://prod-kline-rest.supra.com';

const MAX_RPS = 10;                                 // plafond API (req/sec)
const MIN_GAP_MS = Math.ceil(1000 / MAX_RPS);       // écart minimal entre 2 appels
const BROADCAST_INTERVAL_MS = 500;                  // cadence d'envoi WSS

/**
 * ---------------------------
 * Paires & Métadonnées
 * ---------------------------
 */
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

/**
 * ---------------------------
 * Cache (dernières valeurs valides)
 * ---------------------------
 */
const lastValidPrices = {}; // { pair: { id, name, ...dataValide } }

/**
 * ---------------------------
 * WebSocket Server
 * ---------------------------
 */
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

console.log(`✅ WSS prêt sur : ${PORT}
   - Rate limit: <= ${MAX_RPS} req/s (gap min ${MIN_GAP_MS}ms)
   - Broadcast: ${BROADCAST_INTERVAL_MS}ms
   - Paires: ${PAIRS.length}`);

/**
 * ---------------------------
 * Helpers
 * ---------------------------
 */

// Convertit en nombre (gère "1234.56" et "1,234.56" → 1234.56). Laisse tomber si NaN/Inf.
function toNumberMaybe(x) {
  if (typeof x === 'number') return Number.isFinite(x) ? x : NaN;
  if (typeof x === 'string') {
    // enlever séparateurs de milliers éventuels
    const s = x.replace(/,/g, '');
    const n = parseFloat(s);
    return Number.isFinite(n) ? n : NaN;
  }
  return NaN;
}

// Cherche un champ prix plausible et renvoie { key, valueNumber } si trouvé
function extractNumericPriceField(obj) {
  if (!obj || typeof obj !== 'object') return null;
  const candidates = ['price', 'last', 'close', 'markPrice', 'value'];
  for (const k of candidates) {
    if (k in obj) {
      const n = toNumberMaybe(obj[k]);
      if (Number.isFinite(n)) return { key: k, valueNumber: n };
    }
  }
  return null;
}

// Réponse "valide" = possède un champ prix numérique ; on ne CHANGE PAS la clé (on la garde telle quelle)
function normalizeValidPayload(obj) {
  const hit = extractNumericPriceField(obj);
  if (!hit) return null;
  // Cloner sans muter l'original
  const normalized = { ...obj };
  normalized[hit.key] = hit.valueNumber; // forcer en nombre (décimales ok)
  return normalized;
}

// Objet par défaut si aucune donnée encore reçue
function defaultEntry(pair) {
  return {
    id: PAIR_METADATA[pair]?.id ?? null,
    name: PAIR_METADATA[pair]?.name || 'UNKNOWN',
    price: 0
  };
}

/**
 * ---------------------------
 * Broadcast (toutes les 500ms)
 * ---------------------------
 */
function broadcastFromCache() {
  try {
    const results = {};
    for (const pair of PAIRS) {
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
setInterval(broadcastFromCache, BROADCAST_INTERVAL_MS);

wss.on('connection', (client) => {
  console.log('🟢 Client connecté');
  // Snapshot immédiat
  try {
    const initial = {};
    for (const pair of PAIRS) {
      initial[pair] = lastValidPrices[pair] ?? defaultEntry(pair);
    }
    client.send(JSON.stringify(initial));
  } catch (e) {
    console.error('❌ Erreur envoi snapshot initial:', e?.message || e);
  }
});

/**
 * ---------------------------
 * Poller séquentiel (≤ 10 req/s)
 * ---------------------------
 */
let pairIndex = 0;
let lastCallTs = 0;

async function fetchOnePairAndUpdateCache(pair) {
  try {
    const url = `${BASE_URL}/latest?trading_pair=${pair}`;
    const res = await fetch(url, { headers: { 'x-api-key': API_KEY } });

    if (!res.ok) {
      // 429, 5xx, etc. → on NE PASSE PAS en cache
      return;
    }

    const raw = await res.json().catch(() => null);

    // On n’accepte QUE si on a un prix numérique ; sinon on ignore (on garde l’ancien)
    const normalized = normalizeValidPayload(raw);
    if (normalized) {
      lastValidPrices[pair] = {
        id: PAIR_METADATA[pair]?.id ?? null,
        name: PAIR_METADATA[pair]?.name || 'UNKNOWN',
        ...normalized // conserve TOUTES les clés d'origine ; prix converti en number
      };
    }
  } catch {
    // Erreur réseau → on ignore (garde l’ancienne valeur)
  } finally {
    // Si jamais on n'a toujours rien pour cette paire, init une valeur neutre (une seule fois)
    if (!lastValidPrices[pair]) {
      lastValidPrices[pair] = defaultEntry(pair);
    }
  }
}

// Boucle séquentielle rate-limitée : 1 paire à la fois, gap >= MIN_GAP_MS
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
