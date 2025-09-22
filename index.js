// index.js — WSS price broadcaster avec fallback + defaults
// Node 18+ (fetch natif)

import { WebSocketServer } from 'ws';
import fs from 'fs';
import path from 'path';

// --------- Charge l'env (.env OU env) ---------
function loadEnv() {
  const cwd = process.cwd();
  const candidates = [
    path.resolve(cwd, '.env'),
    path.resolve(cwd, 'env'),
  ];
  for (const p of candidates) {
    if (fs.existsSync(p)) {
      const text = fs.readFileSync(p, 'utf8');
      for (const line of text.split('\n')) {
        const s = line.trim();
        if (!s || s.startsWith('#')) continue;
        const eq = s.indexOf('=');
        if (eq === -1) continue;
        const k = s.slice(0, eq).trim();
        const v = s.slice(eq + 1).trim().replace(/^"(.*)"$/, '$1').replace(/^'(.*)'$/, '$1');
        if (!(k in process.env)) process.env[k] = v;
      }
      break;
    }
  }
}
loadEnv();

// --------- Config ---------
const PORT = parseInt(process.env.PORT || '8081', 10);
const API_KEY = process.env.SUPRA_API_KEY || '1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2';
const BASE_URL = 'https://prod-kline-rest.supra.com';
const REFRESH_MS = 1000;

const PAIRS = [
  'aapl_usd', 'amzn_usd', 'coin_usd', 'goog_usd', 'gme_usd',
  'intc_usd', 'ko_usd', 'mcd_usd', 'msft_usd', 'ibm_usd',
  'meta_usd', 'nvda_usd', 'tsla_usd',
  'aud_usd', 'eur_usd', 'gbp_usd', 'nzd_usd',
  'usd_cad', 'usd_chf', 'usd_jpy',
  'xag_usd', 'xau_usd',
  'btc_usdt', 'eth_usdt', 'sol_usdt', 'xrp_usdt',
  'avax_usdt', 'doge_usdt', 'trx_usdt', 'ada_usdt',
  'sui_usdt', 'link_usdt',
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
  link_usdt:{ id: 2,    name: 'CHAINLINK' },
};

// --------- Cache et helpers ---------
const lastByPair = new Map(); // pair -> dernier payload "valide"
const nowISO = () => new Date().toISOString();

function composeDefault(pair) {
  const meta = PAIR_METADATA[pair] || {};
  // Structure inspirée de Supra: instruments[0].currentPrice, percentChange...
  return {
    id: meta.id ?? null,
    name: meta.name || 'UNKNOWN',
    default: true,
    refreshedAt: nowISO(),
    instruments: [{
      tradingPair: pair.toUpperCase(),
      currentPrice: 0,
      percentChange: 0,
      timestamp: Date.now(),
    }],
  };
}

function isValidSupraPayload(data) {
  try {
    const it = data?.instruments?.[0];
    return it && it.currentPrice != null && !Number.isNaN(Number(it.currentPrice));
  } catch {
    return false;
  }
}

async function fetchJson(url, opts = {}, timeoutMs = 8000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, { ...opts, signal: ctrl.signal });
    const ct = res.headers.get('content-type') || '';
    if (!res.ok || !ct.includes('application/json')) {
      const short = (await res.text().catch(() => '')).slice(0, 120);
      throw new Error(`HTTP ${res.status} ${res.statusText} | ct=${ct} | body=${short}`);
    }
    return res.json();
  } finally {
    clearTimeout(t);
  }
}

// --------- WebSocket server ---------
const wss = new WebSocketServer({
  port: PORT,
  perMessageDeflate: {
    zlibDeflateOptions: { level: 9 },
    zlibInflateOptions: { chunkSize: 1024 },
    clientNoContextTakeover: true,
    serverNoContextTakeover: true,
    threshold: 0,
  },
});
console.log(`✅ WSS lancé sur port ${PORT} (refresh ${REFRESH_MS}ms).`);

// --------- Boucle de rafraîchissement ---------
async function fetchAllPricesAndBroadcast() {
  try {
    const responses = await Promise.allSettled(
      PAIRS.map(async (pair) => {
        const data = await fetchJson(`${BASE_URL}/latest?trading_pair=${pair}`, {
          headers: { 'x-api-key': API_KEY },
        });
        return { pair, data };
      }),
    );

    const results = {};

    for (const r of responses) {
      if (r.status === 'fulfilled') {
        const { pair, data } = r.value;
        if (isValidSupraPayload(data)) {
          const payload = {
            id: PAIR_METADATA[pair]?.id ?? null,
            name: PAIR_METADATA[pair]?.name || 'UNKNOWN',
            refreshedAt: nowISO(),
            ...data,
          };
          lastByPair.set(pair, payload);
          results[pair] = payload;
        } else {
          // payload invalide -> fallback cache ou zeros si aucun cache
          const cached = lastByPair.get(pair);
          results[pair] = cached ? { ...cached, fallback: true, servedAt: nowISO() }
                                 : composeDefault(pair);
        }
      } else {
        // requête échouée -> fallback cache ou zeros
        // (on ne connait pas la pair ici; on complétera après avec la passe suivante)
      }
    }

    // Compléter TOUTES les paires: cache sinon defaults
    for (const pair of PAIRS) {
      if (!results[pair]) {
        const cached = lastByPair.get(pair);
        results[pair] = cached ? { ...cached, fallback: true, servedAt: nowISO() }
                               : composeDefault(pair);
      }
    }

    const payloadStr = JSON.stringify(results);
    wss.clients.forEach((c) => {
      if (c.readyState === 1) c.send(payloadStr);
    });
  } catch (err) {
    console.error('❌ Erreur récupération/diffusion:', err.message);
    // En cas d’erreur globale, on diffuse au moins des defaults pour tout le monde
    const results = {};
    for (const pair of PAIRS) {
      const cached = lastByPair.get(pair);
      results[pair] = cached ? { ...cached, fallback: true, servedAt: nowISO() }
                             : composeDefault(pair);
    }
    const payloadStr = JSON.stringify(results);
    wss.clients.forEach((c) => {
      if (c.readyState === 1) c.send(payloadStr);
    });
  }
}

setInterval(fetchAllPricesAndBroadcast, REFRESH_MS);

// À la connexion, on envoie immédiatement un snapshot (cache ou zeros)
wss.on('connection', (ws) => {
  const snapshot = {};
  for (const pair of PAIRS) {
    const cached = lastByPair.get(pair);
    snapshot[pair] = cached ? { ...cached, servedAt: nowISO(), snapshot: true }
                            : composeDefault(pair);
  }
  try { ws.send(JSON.stringify(snapshot)); } catch {}
  console.log('🟢 Client connecté — snapshot envoyé.');
});

