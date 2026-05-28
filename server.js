'use strict';
/**
 * HumanLens — Background Scanner Server
 * ─────────────────────────────────────
 * Runs on Render.com (free tier) — stays alive 24/7
 * Scans Binance Futures every 3 minutes
 * Sends FCM push notifications to all subscribed devices
 * when any coin scores >= ALERT_THRESHOLD (default 11)
 */

const express  = require('express');
const cors     = require('cors');
const admin    = require('firebase-admin');
const fetch    = (...a) => import('node-fetch').then(({default:f})=>f(...a));
const fs       = require('fs');
const path     = require('path');

// ── Config ────────────────────────────────────────────────────
const PORT            = process.env.PORT || 3000;
const ALERT_THRESHOLD = parseInt(process.env.ALERT_THRESHOLD || '11');
const SCAN_INTERVAL   = parseInt(process.env.SCAN_INTERVAL   || '180000'); // 3 min
const BASE            = 'https://fapi.binance.com';
const SKIP = new Set([
  'BUSDUSDT','USDCUSDT','TUSDUSDT','FDUSDUSDT',
  'USDPUSDT','BTCDOMUSDT','DEFIUSDT','COCOSUSDT'
]);

// ── Firebase Admin init ───────────────────────────────────────
// Reads FIREBASE_SERVICE_ACCOUNT env var (JSON string) set in Render dashboard
let fcmReady = false;
try {
  const sa = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT || '{}');
  if (sa.project_id) {
    admin.initializeApp({ credential: admin.credential.cert(sa) });
    fcmReady = true;
    console.log('✅ Firebase Admin initialised — project:', sa.project_id);
  } else {
    console.warn('⚠️  FIREBASE_SERVICE_ACCOUNT not set — push notifications disabled');
  }
} catch(e) {
  console.error('Firebase init error:', e.message);
}

// ── In-memory subscription store ─────────────────────────────
// In production you'd use a DB, but for personal use a JSON file is fine
const SUBS_FILE = path.join(__dirname, 'subscriptions.json');
let subscriptions = {}; // { fcmToken: { addedAt, userAgent } }

function loadSubs() {
  try {
    if (fs.existsSync(SUBS_FILE))
      subscriptions = JSON.parse(fs.readFileSync(SUBS_FILE, 'utf8'));
  } catch(e) {}
}
function saveSubs() {
  try { fs.writeFileSync(SUBS_FILE, JSON.stringify(subscriptions, null, 2)); }
  catch(e) {}
}
loadSubs();

// ── Express API ───────────────────────────────────────────────
const app = express();
app.use(express.json());
app.use(cors({
  origin: '*', // GitHub Pages origin — tighten this to your URL in production
  methods: ['GET','POST','DELETE']
}));

// Health check — Render pings this to keep free tier awake
app.get('/', (req, res) => {
  res.json({
    status: 'ok',
    name: 'HumanLens Scanner',
    uptime: Math.floor(process.uptime()),
    subscribers: Object.keys(subscriptions).length,
    lastScan: lastScanTime ? new Date(lastScanTime).toISOString() : null,
    lastAlerts: lastAlertCount,
    fcmReady,
    threshold: ALERT_THRESHOLD,
    scanInterval: SCAN_INTERVAL / 1000 + 's'
  });
});

// Register FCM token from the PWA
app.post('/subscribe', (req, res) => {
  const { token, userAgent } = req.body;
  if (!token) return res.status(400).json({ error: 'token required' });
  subscriptions[token] = { addedAt: Date.now(), userAgent: userAgent || 'unknown' };
  saveSubs();
  console.log(`📱 Subscribed: ${token.slice(0,20)}... (${Object.keys(subscriptions).length} total)`);
  res.json({ ok: true, message: 'Subscribed to HumanLens alerts' });
});

// Unregister
app.delete('/subscribe', (req, res) => {
  const { token } = req.body;
  if (token && subscriptions[token]) {
    delete subscriptions[token];
    saveSubs();
  }
  res.json({ ok: true });
});

// Manual scan trigger (for testing)
app.get('/scan', async (req, res) => {
  const results = await runScan();
  res.json({ ok: true, alerts: results.length, top: results.slice(0, 5) });
});

// Get last scan results
app.get('/last', (req, res) => {
  res.json({ alerts: lastAlerts, ts: lastScanTime });
});

// ── Scanner ───────────────────────────────────────────────────
let lastScanTime  = null;
let lastAlertCount = 0;
let lastAlerts    = [];
let scanning      = false;

function calcRSI(C, p = 14) {
  if (C.length < p + 2) return 50;
  const d = [];
  for (let i = 1; i < C.length; i++) d.push(C[i] - C[i - 1]);
  let ag = 0, al = 0;
  for (let i = 0; i < p; i++) { if (d[i] > 0) ag += d[i]; else al += Math.abs(d[i]); }
  ag /= p; al /= p;
  for (let i = p; i < d.length; i++) {
    const g = d[i] > 0 ? d[i] : 0, l = d[i] < 0 ? Math.abs(d[i]) : 0;
    ag = (ag * (p - 1) + g) / p; al = (al * (p - 1) + l) / p;
  }
  return al === 0 ? 100 : 100 - 100 / (1 + ag / al);
}

function calcBBW(C, p = 20) {
  if (C.length < p) return null;
  const sl = C.slice(-p), m = sl.reduce((a, b) => a + b, 0) / p;
  const s = Math.sqrt(sl.reduce((a, b) => a + (b - m) ** 2, 0) / p);
  return m > 0 ? (4 * s) / m : null;
}

function scoreCoin(klines, fundingRate, ticker24h) {
  if (!klines || klines.length < 25) return null;
  const C = klines.map(k => +k[4]);
  const V = klines.map(k => +k[5]);
  const H = klines.map(k => +k[2]);
  const L = klines.map(k => +k[3]);
  const O = klines.map(k => +k[1]);
  const last = C.length - 1;
  const avg20 = V.slice(-21, -1).reduce((a, b) => a + b, 0) / 20 || 1;
  let sc = 0, bias = 0;
  const sigs = [];

  // S1: Volume compression
  const rec3 = V.slice(-4, -1).reduce((a, b) => a + b, 0) / 3;
  const cr = rec3 / avg20;
  if (cr < 0.22) { sc += 2; sigs.push('Vol compressed'); }
  else if (cr < 0.48) { sc += 1; sigs.push('Vol tightening'); }

  // S2: Volume spike
  const sR = V[last] / avg20;
  const bull = C[last] > O[last];
  if (sR >= 5) { sc += 3; sigs.push(`VOL EXPLOSION ${sR.toFixed(1)}x`); bias += bull ? 3 : -3; }
  else if (sR >= 3) { sc += 2; sigs.push(`VOL SPIKE ${sR.toFixed(1)}x`); bias += bull ? 2 : -2; }
  else if (sR >= 1.9) { sc += 1; sigs.push(`Vol building ${sR.toFixed(1)}x`); bias += bull ? 1 : -1; }

  // S3: RSI
  const rsi = calcRSI(C, 14);
  if (rsi <= 22) { sc += 3; sigs.push(`RSI EXTREME ${rsi.toFixed(0)}`); bias += 3; }
  else if (rsi >= 78) { sc += 3; sigs.push(`RSI EXTREME ${rsi.toFixed(0)}`); bias -= 3; }
  else if (rsi <= 30) { sc += 2; sigs.push(`RSI oversold ${rsi.toFixed(0)}`); bias += 2; }
  else if (rsi >= 70) { sc += 2; sigs.push(`RSI overbought ${rsi.toFixed(0)}`); bias -= 2; }
  else if (rsi <= 38) { sc += 1; sigs.push(`RSI low ${rsi.toFixed(0)}`); bias += 1; }
  else if (rsi >= 62) { sc += 1; sigs.push(`RSI elevated ${rsi.toFixed(0)}`); bias -= 1; }

  // S4: Funding rate
  const fr = parseFloat(fundingRate || 0) * 100;
  if (Math.abs(fr) >= 0.08) { sc += 2; sigs.push(`FR extreme ${fr.toFixed(4)}%`); bias += fr < 0 ? 2 : -2; }
  else if (Math.abs(fr) >= 0.03) { sc += 1; sigs.push(`FR elevated ${fr.toFixed(4)}%`); bias += fr < 0 ? 1 : -1; }

  // S5: BB squeeze
  const curW = calcBBW(C, 20);
  if (curW !== null) {
    const hw = [];
    for (let i = 20; i < C.length - 1; i++) {
      const w = calcBBW(C.slice(0, i + 1), 20);
      if (w !== null) hw.push(w);
    }
    if (hw.length >= 5) {
      const aW = hw.reduce((a, b) => a + b, 0) / hw.length;
      const sq = curW / aW;
      if (sq < 0.38) { sc += 2; sigs.push('BB squeeze extreme'); }
      else if (sq < 0.62) { sc += 1; sigs.push('BB squeezing'); }
    }
  }

  // S6: Price coiling
  const l5H = Math.max(...H.slice(-5)), l5L = Math.min(...L.slice(-5));
  const l5R = (l5H - l5L) / C[last] * 100;
  const p5H = Math.max(...H.slice(-10, -5)), p5L = Math.min(...L.slice(-10, -5));
  const p5R = (p5H - p5L) / (C[last - 5] || C[last]) * 100;
  if (p5R > 0 && l5R < p5R * 0.28) { sc += 2; sigs.push('Price coiling tight'); }
  else if (p5R > 0 && l5R < p5R * 0.52) { sc += 1; sigs.push('Range narrowing'); }

  // S7: 24h extremes
  if (ticker24h) {
    const pH = Math.max(...H.slice(-60)), pL = Math.min(...L.slice(-60));
    const toH = (pH - C[last]) / C[last] * 100;
    const toL = (C[last] - pL) / C[last] * 100;
    if (toL <= 1.2) { sc += 2; sigs.push('At 60-period LOW'); bias += 2; }
    else if (toH <= 1.2) { sc += 2; sigs.push('At 60-period HIGH'); bias -= 2; }
    else if (toL <= 4.0) { sc += 1; sigs.push('Near period low'); bias += 1; }
    else if (toH <= 4.0) { sc += 1; sigs.push('Near period high'); bias -= 1; }

    // 24h move
    const ch24 = +ticker24h.priceChangePercent;
    if (Math.abs(ch24) >= 20) { sc += 2; sigs.push(`24h move ${ch24 > 0 ? '+' : ''}${ch24.toFixed(0)}%`); bias += ch24 > 0 ? 1 : -1; }
    else if (Math.abs(ch24) >= 10) { sc += 1; sigs.push(`24h ${ch24 > 0 ? '+' : ''}${ch24.toFixed(0)}%`); }
  }

  let dir = 'WATCH';
  if (sc >= 6) dir = bias > 1 ? 'PUMP' : bias < -1 ? 'DUMP' : 'COILING';

  return { score: sc, signals: sigs, direction: dir, rsi: rsi.toFixed(1), fr };
}

const sleep = ms => new Promise(r => setTimeout(r, ms));
const fmtP = p => p === 0 ? '0' : p < 0.01 ? p.toFixed(6) : p < 1 ? p.toFixed(5) : p < 100 ? p.toFixed(3) : p.toFixed(2);
const fmtVol = v => v >= 1e9 ? (v / 1e9).toFixed(1) + 'B' : v >= 1e6 ? (v / 1e6).toFixed(0) + 'M' : (v / 1e3).toFixed(0) + 'K';

async function runScan() {
  if (scanning) return lastAlerts;
  scanning = true;
  const startTime = Date.now();

  try {
    console.log(`\n🔍 [${new Date().toISOString()}] Starting scan...`);

    const [tickers, premiums] = await Promise.all([
      fetch(`${BASE}/fapi/v1/ticker/24hr`).then(r => r.json()),
      fetch(`${BASE}/fapi/v1/premiumIndex`).then(r => r.json()),
    ]);

    const fMap = {}, mMap = {}, tMap = {};
    premiums.forEach(p => { fMap[p.symbol] = p.lastFundingRate; mMap[p.symbol] = p.markPrice; });
    tickers.forEach(t => { tMap[t.symbol] = t; });

    const perps = tickers
      .filter(t => t.symbol.endsWith('USDT') && !SKIP.has(t.symbol) && +t.quoteVolume > 200000)
      .sort((a, b) => +b.quoteVolume - +a.quoteVolume)
      .slice(0, 150); // top 150 by volume for speed on free tier

    console.log(`   Scanning ${perps.length} symbols...`);

    const alerts = [];

    // Process in batches to avoid rate limits
    for (let i = 0; i < perps.length; i += 8) {
      const batch = perps.slice(i, i + 8);
      const results = await Promise.all(batch.map(async t => {
        try {
          const kl = await fetch(
            `${BASE}/fapi/v1/klines?symbol=${t.symbol}&interval=5m&limit=75`
          ).then(r => r.json());
          const res = scoreCoin(kl, fMap[t.symbol], tMap[t.symbol]);
          if (!res || res.score < ALERT_THRESHOLD) return null;
          return {
            symbol: t.symbol,
            base: t.symbol.replace('USDT', ''),
            price: +t.lastPrice,
            change24h: +t.priceChangePercent,
            volume: +t.quoteVolume,
            fundingRate: +(fMap[t.symbol] || 0) * 100,
            ...res
          };
        } catch { return null; }
      }));
      alerts.push(...results.filter(Boolean));
      if (i + 8 < perps.length) await sleep(80);
    }

    alerts.sort((a, b) => b.score - a.score);
    lastAlerts = alerts;
    lastScanTime = Date.now();
    lastAlertCount = alerts.length;

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`   ✓ Done in ${elapsed}s — ${alerts.length} alerts (score≥${ALERT_THRESHOLD})`);
    if (alerts.length) {
      alerts.slice(0, 5).forEach(a =>
        console.log(`   🎯 ${a.base} score=${a.score} dir=${a.direction} rsi=${a.rsi} $${fmtP(a.price)} vol=${fmtVol(a.volume)}`)
      );
    }

    // Send FCM push if we have subscribers
    if (fcmReady && alerts.length > 0) {
      await sendPushNotifications(alerts);
    }

    return alerts;

  } catch (e) {
    console.error('Scan error:', e.message);
    return [];
  } finally {
    scanning = false;
  }
}

// ── FCM Push ──────────────────────────────────────────────────
const notifiedThisCycle = new Set(); // avoid re-notifying same coin every scan

async function sendPushNotifications(alerts) {
  const tokens = Object.keys(subscriptions);
  if (!tokens.length) { console.log('   No subscribers yet'); return; }

  // Only notify coins not already notified in last 15 minutes
  const now = Date.now();
  const fresh = alerts.filter(a => {
    const key = a.symbol;
    if (notifiedThisCycle.has(key)) return false;
    notifiedThisCycle.set ? notifiedThisCycle.add(key) : null;
    return true;
  });

  // Clear stale entries every hour
  if (!sendPushNotifications._lastClear || now - sendPushNotifications._lastClear > 15 * 60 * 1000) {
    notifiedThisCycle.clear();
    sendPushNotifications._lastClear = now;
  }

  if (!fresh.length) { console.log('   All alerts already notified this cycle'); return; }

  const top = fresh[0];
  const dir = top.direction === 'PUMP' ? '▲ PUMP' : top.direction === 'DUMP' ? '▼ DUMP' : '◈ MOVE';
  const title = `🎯 HumanLens — ${top.base} Score ${top.score}`;
  const body = `${dir} · $${fmtP(top.price)} · RSI ${top.rsi}\n${top.signals.slice(0, 3).join(' · ')}`;

  const message = {
    notification: { title, body },
    data: {
      sym: top.base,
      score: String(top.score),
      direction: top.direction,
      price: String(top.price),
      rsi: String(top.rsi),
      allAlerts: JSON.stringify(fresh.slice(0, 5).map(a => ({
        base: a.base, score: a.score, direction: a.direction, price: a.price
      }))),
      click_action: 'FLUTTER_NOTIFICATION_CLICK',
      url: '/'
    },
    android: {
      priority: 'high',
      notification: {
        sound: 'default',
        channelId: 'humanlens_alerts',
        priority: 'max',
        visibility: 'public',
        vibrateTimingsMillis: [0, 200, 100, 300, 100, 200],
        defaultVibrateTimings: false,
        notificationCount: fresh.length
      }
    },
    apns: {
      payload: { aps: { sound: 'default', badge: fresh.length, contentAvailable: true } },
      headers: { 'apns-priority': '10', 'apns-push-type': 'alert' }
    },
    webpush: {
      headers: { Urgency: 'high', TTL: '900' },
      notification: {
        title,
        body,
        icon: '/icon.svg',
        badge: '/icon.svg',
        tag: `hl-${top.base}`,
        renotify: true,
        requireInteraction: true,
        vibrate: [200, 100, 300, 100, 200],
        actions: [{ action: 'open', title: '📊 Open App' }],
        data: { url: '/', sym: top.base }
      }
    }
  };

  // Also send a summary if multiple alerts
  let extraMessage = null;
  if (fresh.length > 1) {
    const others = fresh.slice(1, 4).map(a => `${a.base} ${a.direction} (${a.score})`).join(' · ');
    extraMessage = {
      notification: {
        title: `+${fresh.length - 1} more HumanLens signals`,
        body: others
      },
      data: { url: '/' },
      webpush: {
        headers: { Urgency: 'high', TTL: '900' },
        notification: {
          title: `+${fresh.length - 1} more signals`,
          body: others,
          icon: '/icon.svg',
          badge: '/icon.svg',
          tag: 'hl-multi',
          renotify: true
        }
      }
    };
  }

  // Send to all tokens, remove invalid ones
  const deadTokens = [];
  for (const token of tokens) {
    try {
      await admin.messaging().send({ ...message, token });
      if (extraMessage) {
        await admin.messaging().send({ ...extraMessage, token });
      }
    } catch (e) {
      if (e.code === 'messaging/registration-token-not-registered' ||
          e.code === 'messaging/invalid-registration-token') {
        deadTokens.push(token);
        console.log(`   Removed dead token: ${token.slice(0, 20)}...`);
      } else {
        console.error(`   FCM error for token: ${e.message}`);
      }
    }
  }

  // Clean up dead tokens
  if (deadTokens.length) {
    deadTokens.forEach(t => delete subscriptions[t]);
    saveSubs();
  }

  console.log(`   📨 Push sent to ${tokens.length - deadTokens.length} device(s)`);
}

// ── Scan loop ─────────────────────────────────────────────────
console.log(`\n🚀 HumanLens Scanner starting...`);
console.log(`   Threshold: score ≥ ${ALERT_THRESHOLD}`);
console.log(`   Interval: every ${SCAN_INTERVAL / 1000}s`);
console.log(`   FCM: ${fcmReady ? 'enabled' : 'disabled (set FIREBASE_SERVICE_ACCOUNT)'}`);
console.log(`   Port: ${PORT}\n`);

// Run immediately on start, then every SCAN_INTERVAL
runScan();
setInterval(runScan, SCAN_INTERVAL);

// Start HTTP server
app.listen(PORT, () => console.log(`✅ API listening on port ${PORT}`));
