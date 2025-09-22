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

console.log(`✅ Serveur WebSocket lancé sur le port ${PORT} (update toutes les 1000ms).`);

// Fonction pour récupérer et diffuser tous les prix
async function fetchAllPricesAndBroadcast() {
  try {
    const responses = await Promise.all(
      PAIRS.map(pair =>
        fetch(`${BASE_URL}/latest?trading_pair=${pair}`, {
          headers: { 'x-api-key': API_KEY }
        }).then(res => res.json().then(data => ({ pair, data })))
      )
    );

    const results = {};
    for (const { pair, data } of responses) {
      results[pair] = {
        id: PAIR_METADATA[pair]?.id ?? null,
        name: PAIR_METADATA[pair]?.name || 'UNKNOWN',
        ...data
      };
    }

    const payload = JSON.stringify(results);

    wss.clients.forEach(client => {
      if (client.readyState === 1) {
        client.send(payload);
      }
    });
  } catch (err) {
    console.error('❌ Erreur récupération prix:', err.message);
  }
}

// Rafraîchissement toutes les 1000 ms
setInterval(fetchAllPricesAndBroadcast, 1000);

// Connexion client
wss.on('connection', () => {
  console.log('🟢 Nouveau client connecté');
});

