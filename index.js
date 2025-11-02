// hybrid_bridge.mjs
import fetch from "node-fetch";
import { WebSocket, WebSocketServer } from "ws";

// ============ CONFIG ============
const PORT = 8081;
const API_KEY = "1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2";
const REST_BASE = "https://prod-kline-rest.supra.com";
const WS_URL = "wss://prod-kline-ws.supra.com"; // (docs: prod-kline-ws.supra.com)
const RESOLUTION = 1;       // bar/tick fréquence côté WS
const CHUNK_SIZE = 30;      // souscription WS par paquets
const REFRESH_MS = 2 * 60 * 1000; // re-évalue horaires toutes les 2 min
const MIN_GAP_MS = 100;     // 10 req/s sur REST
const TZ_PARIS = "Europe/Paris";
const TZ_NY = "America/New_York";

// ========= PAIRES & METADATA =========
const PAIRS = [
  "aapl_usd", "amzn_usd", "coin_usd", "goog_usd", "gme_usd",
  "intc_usd", "ko_usd", "mcd_usd", "msft_usd", "ibm_usd",
  "meta_usd", "nvda_usd", "tsla_usd",
  "aud_usd", "eur_usd", "gbp_usd", "nzd_usd",
  "usd_cad", "usd_chf", "usd_jpy",
  "xag_usd", "xau_usd",
  "btc_usdt", "eth_usdt", "sol_usdt", "xrp_usdt",
  "avax_usdt", "doge_usdt", "trx_usdt", "ada_usdt",
  "sui_usdt", "link_usdt",
  "orcle_usd", "wti_usd",
  "nike_usd", "spdia_usd", "qqqm_usd", "iwm_usd",
];

const ALIASES = { orcle_usd: "orcl_usd", nike_usd: "nke_usd", spdia_usd: "dia_usd" };

const PAIR_METADATA = {
  aapl_usd: { id: 6004, name: "APPLE INC." },
  amzn_usd: { id: 6005, name: "AMAZON" },
  coin_usd: { id: 6010, name: "COINBASE" },
  goog_usd: { id: 6003, name: "ALPHABET INC." },
  gme_usd: { id: 6011, name: "GAMESTOP CORP." },
  intc_usd: { id: 6009, name: "INTEL CORPORATION" },
  ko_usd: { id: 6059, name: "COCA-COLA CO" },
  mcd_usd: { id: 6068, name: "MCDONALD'S CORP" },
  msft_usd: { id: 6001, name: "MICROSOFT CORP" },
  ibm_usd: { id: 6066, name: "IBM" },
  meta_usd: { id: 6006, name: "META PLATFORMS INC." },
  nvda_usd: { id: 6002, name: "NVIDIA CORP" },
  tsla_usd: { id: 6000, name: "TESLA INC" },

  aud_usd: { id: 5010, name: "AUSTRALIAN DOLLAR" },
  eur_usd: { id: 5000, name: "EURO" },
  gbp_usd: { id: 5002, name: "GREAT BRITAIN POUND" },
  nzd_usd: { id: 5013, name: "NEW ZEALAND DOLLAR" },
  usd_cad: { id: 5011, name: "CANADIAN DOLLAR" },
  usd_chf: { id: 5012, name: "SWISS FRANC" },
  usd_jpy: { id: 5001, name: "JAPANESE YEN" },

  xag_usd: { id: 5501, name: "SILVER" },
  xau_usd: { id: 5500, name: "GOLD" },
  wti_usd: { id: 5503, name: "WEST TEXAS INTERMEDIATE CRUDE" },

  btc_usdt: { id: 0, name: "BITCOIN" },
  eth_usdt: { id: 1, name: "ETHEREUM" },
  sol_usdt: { id: 10, name: "SOLANA" },
  xrp_usdt: { id: 14, name: "RIPPLE" },
  avax_usdt: { id: 5, name: "AVALANCHE" },
  doge_usdt: { id: 3, name: "DOGECOIN" },
  trx_usdt: { id: 15, name: "TRON" },
  ada_usdt: { id: 16, name: "CARDANO" },
  sui_usdt: { id: 90, name: "SUI" },
  link_usdt: { id: 2, name: "CHAINLINK" },

  orcl_usd: { id: 6038, name: "ORACLE CORPORATION" },
  dia_usd: { id: 6113, name: "SPDR DOW JONES (DIA)" },
  qqqm_usd: { id: 6114, name: "NASDAQ-100 ETF (QQQM)" },
  iwm_usd: { id: 6115, name: "ISHARES RUSSELL 2000 ETF (IWM)" },
  nke_usd: { id: 6034, name: "NIKE INC" },
};

// Groupes de marché
const CRYPTO = [
  "btc_usdt", "eth_usdt", "sol_usdt", "xrp_usdt",
  "avax_usdt", "doge_usdt", "trx_usdt", "ada_usdt",
  "sui_usdt", "link_usdt",
];
const FOREX = ["aud_usd", "eur_usd", "gbp_usd", "nzd_usd", "usd_cad", "usd_chf", "usd_jpy"];
const COMMODITIES = ["xau_usd", "xag_usd", "wti_usd"];
const US_EQ = [
  "aapl_usd", "amzn_usd", "coin_usd", "goog_usd", "gme_usd",
  "intc_usd", "ko_usd", "mcd_usd", "msft_usd", "ibm_usd",
  "meta_usd", "nvda_usd", "tsla_usd", "orcl_usd", "nke_usd",
];
const US_ETF = ["dia_usd", "qqqm_usd", "iwm_usd"];

function normalize(t) { return ALIASES[t] || t; }

// =========== TIME UTILS ===========
const WD = { Sun:0, Mon:1, Tue:2, Wed:3, Thu:4, Fri:5, Sat:6 };
function partsFromTZ(date, timeZone) {
  const fmt = new Intl.DateTimeFormat("en-US", {
    timeZone, weekday:"short", hour:"2-digit", minute:"2-digit", hour12:false,
    year:"numeric", month:"2-digit", day:"2-digit",
  });
  const parts = fmt.formatToParts(date);
  const get = (t) => parts.find(p => p.type === t)?.value;
  return { wd: WD[get("weekday")], hour: +get("hour"), minute: +get("minute") };
}
// US equities 09:30–16:30 NY, Mon–Fri
function isUsEquityOpen(d = new Date()) {
  const { wd, hour, minute } = partsFromTZ(d, TZ_NY);
  if (wd <= 0 || wd === 6) return false;
  const m = hour * 60 + minute;
  return m >= 9 * 60 + 30 && m < 16 * 60 + 30;
}
// Forex/Commo Sun 22:00 Paris → Fri 23:00 Paris (approx)
function isForexLikeOpen(d = new Date()) {
  const { wd, hour, minute } = partsFromTZ(d, TZ_PARIS);
  if (wd === 0) return hour > 22 || (hour === 22 && minute >= 0);
  if (wd >= 1 && wd <= 4) return true;
  if (wd === 5) return hour < 23 || (hour === 23 && minute === 0);
  return false;
}
const isCryptoOpen = () => true;

// ========== CACHE & FORMATS ==========
const lastValid = {}; // pair -> { id, name, ...data }

// Transforme payload WS OHLC en format { id, name, price?, ...fields }
function upsertFromWS(item) {
  const p = normalize(item.tradingPair || "");
  if (!p) return;
  const meta = PAIR_METADATA[p] || { id: null, name: "UNKNOWN" };
  const price = Number(item.currentPrice ?? item.close ?? 0);
  lastValid[p] = {
    id: meta.id ?? null,
    name: meta.name || "UNKNOWN",
    // on garde aussi open/high/low/close/timestamp (utile pour graph)
    price,
    open: item.open, high: item.high, low: item.low, close: item.close,
    timestamp: item.timestamp, time: item.time,
    tradingPair: p,
    event: "ohlc_datafeed",
  };
}

// Pour REST /latest : on merge et essaie de déduire price
function upsertFromREST(pair, data) {
  const p = normalize(pair);
  const meta = PAIR_METADATA[p] || { id: null, name: "UNKNOWN" };
  const price = Number(
    data?.price ?? data?.currentPrice ?? data?.close ?? data?.last ?? 0
  );
  lastValid[p] = {
    id: meta.id ?? null,
    name: meta.name || "UNKNOWN",
    price,
    ...data,
  };
}

// Snapshot envoyé aux clients — EXACTEMENT même forme que ton script précédent
function buildSnapshot() {
  const out = {};
  for (const raw of PAIRS) {
    const p = normalize(raw);
    out[p] = lastValid[p] || {
      id: PAIR_METADATA[p]?.id ?? null,
      name: PAIR_METADATA[p]?.name || "UNKNOWN",
      price: 0,
    };
  }
  return JSON.stringify(out);
}

// ========== REST FETCH (100ms spacing) ==========
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function fetchOnceREST(pairs) {
  for (const raw of pairs) {
    const p = normalize(raw);
    try {
      const res = await fetch(`${REST_BASE}/latest?trading_pair=${p}`, {
        headers: { "x-api-key": API_KEY },
      });
      const data = await res.json().catch(() => ({}));
      upsertFromREST(p, data);
    } catch (e) {
      // garde cache ou placeholder
      if (!lastValid[p]) {
        lastValid[p] = {
          id: PAIR_METADATA[p]?.id ?? null,
          name: PAIR_METADATA[p]?.name || "UNKNOWN",
          price: 0,
        };
      }
    }
    await sleep(MIN_GAP_MS);
  }
}

// ========== WS SUPRA CLIENT (dyn. resub) ==========
let supraWS = null;
let currentWSSet = [];

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function openSupraWS(pairs) {
  // ferme précédente
  if (supraWS) try { supraWS.close(); } catch {}
  currentWSSet = [...pairs];

  supraWS = new WebSocket(WS_URL, { headers: { "x-api-key": API_KEY } });

  supraWS.on("open", () => {
    const groups = chunk(pairs, CHUNK_SIZE);
    for (const g of groups) {
      const msg = {
        action: "subscribe",
        channels: [{ name: "ohlc_datafeed", resolution: RESOLUTION, tradingPairs: g }],
      };
      supraWS.send(JSON.stringify(msg));
    }
  });

  supraWS.on("message", (buf) => {
    try {
      const msg = JSON.parse(buf.toString());
      if (msg.event === "ohlc_datafeed" && Array.isArray(msg.payload)) {
        for (const k of msg.payload) upsertFromWS(k);
      }
    } catch {}
  });

  supraWS.on("error", (e) => console.error("WS error:", e?.message || e));
  supraWS.on("close", () => {});
}

function setsDiff(a, b) {
  const A = new Set(a), B = new Set(b);
  const add = [...B].filter(x => !A.has(x));
  const del = [...A].filter(x => !B.has(x));
  return { add, del, changed: add.length || del.length };
}

// ========== LOGIQUE D’HORAIRES & BASCULE ==========
function computeOpenSets() {
  const openCrypto = isCryptoOpen();
  const openFx = isForexLikeOpen();
  const openEq = isUsEquityOpen();

  const openPairs = new Set();
  const closedPairs = new Set();

  // Crypto
  for (const p of CRYPTO) (openCrypto ? openPairs : closedPairs).add(p);
  // Forex & Commodities
  for (const p of [...FOREX, ...COMMODITIES]) (openFx ? openPairs : closedPairs).add(p);
  // US equities & ETFs
  for (const p of [...US_EQ, ...US_ETF]) (openEq ? openPairs : closedPairs).add(p);

  // Ajoute aussi toutes les paires restantes (normalisées)
  for (const raw of PAIRS) {
    const p = normalize(raw);
    if (!openPairs.has(p) && !closedPairs.has(p)) {
      // inconnus → snapshot REST une fois
      closedPairs.add(p);
    }
  }
  return { open: [...openPairs], closed: [...closedPairs], flags: { openCrypto, openFx, openEq } };
}

async function rebalance() {
  const { open, closed } = computeOpenSets();

  // (1) Re-souscrire WS si set a changé
  const { changed } = setsDiff(currentWSSet, open);
  if (changed) openSupraWS(open);

  // (2) Snapshot REST pour les fermés (une passe)
  if (closed.length) await fetchOnceREST(closed);
}

// ========== WEBSOCKET SERVER (clients) ==========
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
console.log(`✅ WebSocket server running on :${PORT}`);

wss.on("connection", (ws) => {
  console.log("🟢 Client connected");
  // Snapshot immédiat
  try { ws.send(buildSnapshot()); } catch {}
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });
});

// diffuse un snapshot régulier (même format que ton script original)
setInterval(() => {
  const payload = buildSnapshot();
  for (const client of wss.clients) {
    if (client.readyState === 1) {
      try { client.send(payload); } catch {}
    }
  }
}, 1000);

// heartbeat
setInterval(() => {
  for (const client of wss.clients) {
    if (client.isAlive === false) client.terminate();
    client.isAlive = false;
    try { client.ping(); } catch {}
  }
}, 30000);

// ========== BOOTSTRAP ==========
(async () => {
  // 1) premier calcul + snapshots fermés + ouverture WS pour ouverts
  await rebalance();

  // 2) re-éval régulière (bascule auto marché ouvert/fermé)
  setInterval(rebalance, REFRESH_MS);
})();
