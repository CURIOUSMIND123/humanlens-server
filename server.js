'use strict';
/**
 * HumanLens — Background Scanner Server
 * Runs on Render.com free tier — scans Binance every 3 min
 * Sends FCM push when any coin scores >= ALERT_THRESHOLD
 */

const express = require('express');
const cors    = require('cors');
const admin   = require('firebase-admin');
const https   = require('https');
const http    = require('http');
const fs      = require('fs');
const path    = require('path');

// ── Config ────────────────────────────────────────────────────
const PORT            = process.env.PORT || 3000;
const ALERT_THRESHOLD = parseInt(process.env.ALERT_THRESHOLD || '11');
const SCAN_INTERVAL   = parseInt(process.env.SCAN_INTERVAL   || '180000');
const BINANCE         = 'fapi.binance.com';
const SKIP = new Set([
  'BUSDUSDT','USDCUSDT','TUSDUSDT','FDUSDUSDT',
  'USDPUSDT','BTCDOMUSDT','DEFIUSDT','COCOSUSDT'
]);

// ── Simple fetch using native https (no node-fetch needed) ────
function get(hostname, path) {
  return new Promise((resolve, reject) => {
    const req = https.request(
      { hostname, path, method: 'GET',
        headers: { 'User-Agent': 'HumanLens/1.0' } },
      res => {
        let data = '';
        res.on('data', c => data += c);
        res.on('end', () => {
          try { resolve(JSON.parse(data)); }
          catch(e) { reject(new Error('JSON parse: ' + data.slice(0,120))); }
        });
      }
    );
    req.setTimeout(15000, () => { req.destroy(); reject(new Error('Timeout')); });
    req.on('error', reject);
    req.end();
  });
}

// ── Firebase Admin ────────────────────────────────────────────
let fcmReady = false;
try {
  const raw = process.env.FIREBASE_SERVICE_ACCOUNT || '{}';
  const sa  = JSON.parse(raw);
  if (sa.project_id) {
    admin.initializeApp({ credential: admin.credential.cert(sa) });
    fcmReady = true;
    console.log('✅ Firebase ready — project:', sa.project_id);
  } else {
    console.warn('⚠️  FIREBASE_SERVICE_ACCOUNT missing or invalid');
  }
} catch(e) {
  console.error('❌ Firebase init error:', e.message);
}

// ── Subscription store ────────────────────────────────────────
const SUBS_FILE = path.join(__dirname, 'subscriptions.json');
let subscriptions = {};
try {
  if (fs.existsSync(SUBS_FILE))
    subscriptions = JSON.parse(fs.readFileSync(SUBS_FILE, 'utf8'));
  console.log(`📱 Loaded ${Object.keys(subscriptions).length} subscriber(s)`);
} catch(e) {}

function saveSubs() {
  try { fs.writeFileSync(SUBS_FILE, JSON.stringify(subscriptions, null, 2)); }
  catch(e) { console.error('saveSubs error:', e.message); }
}

// ── Express ───────────────────────────────────────────────────
const app = express();
app.use(express.json());
app.use(cors({ origin: '*' }));

app.get('/', (req, res) => res.json({
  status: 'ok', name: 'HumanLens Scanner',
  uptime: Math.floor(process.uptime()),
  subscribers: Object.keys(subscriptions).length,
  lastScan: lastScanTime ? new Date(lastScanTime).toISOString() : null,
  lastAlerts: lastAlertCount,
  fcmReady, threshold: ALERT_THRESHOLD,
  scanInterval: SCAN_INTERVAL / 1000 + 's'
}));

app.post('/subscribe', (req, res) => {
  const { token, userAgent } = req.body;
  if (!token) return res.status(400).json({ error: 'token required' });
  subscriptions[token] = { addedAt: Date.now(), userAgent: userAgent || 'unknown' };
  saveSubs();
  console.log(`📱 New subscriber (${Object.keys(subscriptions).length} total)`);
  res.json({ ok: true, message: 'Subscribed to HumanLens alerts' });
});

app.delete('/subscribe', (req, res) => {
  const { token } = req.body;
  if (token && subscriptions[token]) { delete subscriptions[token]; saveSubs(); }
  res.json({ ok: true });
});

app.get('/scan', async (req, res) => {
  try {
    const results = await runScan();
    res.json({ ok: true, alerts: results.length, top: results.slice(0, 5) });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/last', (req, res) => res.json({ alerts: lastAlerts, ts: lastScanTime }));

app.get('/subs', (req, res) => res.json({
  count: Object.keys(subscriptions).length,
  tokens: Object.keys(subscriptions).map(t => t.slice(0,20) + '...')
}));

// ── Scoring helpers ───────────────────────────────────────────
function calcRSI(C, p=14) {
  if (C.length < p+2) return 50;
  const d = [];
  for (let i=1; i<C.length; i++) d.push(C[i]-C[i-1]);
  let ag=0, al=0;
  for (let i=0; i<p; i++) { ag += d[i]>0?d[i]:0; al += d[i]<0?Math.abs(d[i]):0; }
  ag/=p; al/=p;
  for (let i=p; i<d.length; i++) {
    const g=d[i]>0?d[i]:0, l=d[i]<0?Math.abs(d[i]):0;
    ag=(ag*(p-1)+g)/p; al=(al*(p-1)+l)/p;
  }
  return al===0 ? 100 : 100 - 100/(1+ag/al);
}

function calcBBW(C, p=20) {
  if (C.length<p) return null;
  const sl=C.slice(-p), m=sl.reduce((a,b)=>a+b,0)/p;
  const s=Math.sqrt(sl.reduce((a,b)=>a+(b-m)**2,0)/p);
  return m>0?(4*s)/m:null;
}

function scoreCoin(klines, fundingRate, ticker) {
  if (!klines || klines.length < 25) return null;
  const C=klines.map(k=>+k[4]), V=klines.map(k=>+k[5]);
  const H=klines.map(k=>+k[2]), L=klines.map(k=>+k[3]);
  const O=klines.map(k=>+k[1]);
  const last=C.length-1;
  const avg20=V.slice(-21,-1).reduce((a,b)=>a+b,0)/20||1;
  let sc=0, bias=0;
  const sigs=[];

  // Volume compression
  const rec3=V.slice(-4,-1).reduce((a,b)=>a+b,0)/3;
  const cr=rec3/avg20;
  if (cr<0.22){sc+=2;sigs.push('Vol compressed');}
  else if (cr<0.48){sc+=1;sigs.push('Vol tightening');}

  // Volume spike
  const sR=V[last]/avg20, bull=C[last]>O[last];
  if (sR>=5){sc+=3;sigs.push(`VOL EXPLOSION ${sR.toFixed(1)}x`);bias+=bull?3:-3;}
  else if (sR>=3){sc+=2;sigs.push(`VOL SPIKE ${sR.toFixed(1)}x`);bias+=bull?2:-2;}
  else if (sR>=1.9){sc+=1;sigs.push(`Vol ${sR.toFixed(1)}x`);bias+=bull?1:-1;}

  // RSI
  const rsi=calcRSI(C,14);
  if (rsi<=22){sc+=3;sigs.push(`RSI EXTREME ${rsi.toFixed(0)}`);bias+=3;}
  else if (rsi>=78){sc+=3;sigs.push(`RSI EXTREME ${rsi.toFixed(0)}`);bias-=3;}
  else if (rsi<=30){sc+=2;sigs.push(`RSI OS ${rsi.toFixed(0)}`);bias+=2;}
  else if (rsi>=70){sc+=2;sigs.push(`RSI OB ${rsi.toFixed(0)}`);bias-=2;}
  else if (rsi<=38){sc+=1;sigs.push(`RSI low ${rsi.toFixed(0)}`);bias+=1;}
  else if (rsi>=62){sc+=1;sigs.push(`RSI hi ${rsi.toFixed(0)}`);bias-=1;}

  // Funding rate
  const fr=parseFloat(fundingRate||0)*100;
  if (Math.abs(fr)>=0.08){sc+=2;sigs.push(`FR ${fr.toFixed(4)}%`);bias+=fr<0?2:-2;}
  else if (Math.abs(fr)>=0.03){sc+=1;sigs.push(`FR ${fr.toFixed(4)}%`);bias+=fr<0?1:-1;}

  // BB squeeze
  const curW=calcBBW(C,20);
  if (curW!==null) {
    const hw=[];
    for (let i=20;i<C.length-1;i++){const w=calcBBW(C.slice(0,i+1),20);if(w!==null)hw.push(w);}
    if (hw.length>=5){
      const aW=hw.reduce((a,b)=>a+b,0)/hw.length, sq=curW/aW;
      if (sq<0.38){sc+=2;sigs.push('BB squeeze');}
      else if (sq<0.62){sc+=1;sigs.push('BB tightening');}
    }
  }

  // Price coiling
  const l5H=Math.max(...H.slice(-5)), l5L=Math.min(...L.slice(-5));
  const l5R=(l5H-l5L)/C[last]*100;
  const p5H=Math.max(...H.slice(-10,-5)), p5L=Math.min(...L.slice(-10,-5));
  const p5R=(p5H-p5L)/(C[last-5]||C[last])*100;
  if (p5R>0&&l5R<p5R*0.28){sc+=2;sigs.push('Coiling tight');}
  else if (p5R>0&&l5R<p5R*0.52){sc+=1;sigs.push('Range narrowing');}

  // Near highs/lows
  if (ticker) {
    const pH=Math.max(...H.slice(-60)), pL=Math.min(...L.slice(-60));
    const toH=(pH-C[last])/C[last]*100, toL=(C[last]-pL)/C[last]*100;
    if (toL<=1.2){sc+=2;sigs.push('At period LOW');bias+=2;}
    else if (toH<=1.2){sc+=2;sigs.push('At period HIGH');bias-=2;}
    else if (toL<=4.0){sc+=1;sigs.push('Near low');bias+=1;}
    else if (toH<=4.0){sc+=1;sigs.push('Near high');bias-=1;}

    const ch24=+ticker.priceChangePercent;
    if (Math.abs(ch24)>=20){sc+=2;sigs.push(`24h ${ch24>0?'+':''}${ch24.toFixed(0)}%`);bias+=ch24>0?1:-1;}
    else if (Math.abs(ch24)>=10){sc+=1;sigs.push(`24h ${ch24>0?'+':''}${ch24.toFixed(0)}%`);}
  }

  const dir = sc>=6 ? (bias>1?'PUMP':bias<-1?'DUMP':'COILING') : 'WATCH';
  return { score:sc, signals:sigs, direction:dir, rsi:rsi.toFixed(1), fr };
}

const sleep = ms => new Promise(r => setTimeout(r, ms));
const fmtP  = p => p===0?'0':p<0.01?p.toFixed(6):p<1?p.toFixed(5):p<100?p.toFixed(3):p.toFixed(2);
const fmtV  = v => v>=1e9?(v/1e9).toFixed(1)+'B':v>=1e6?(v/1e6).toFixed(0)+'M':(v/1e3).toFixed(0)+'K';

// ── Main scanner ──────────────────────────────────────────────
let lastScanTime  = null;
let lastAlertCount = 0;
let lastAlerts    = [];
let scanning      = false;

async function runScan() {
  if (scanning) { console.log('⏭ Scan already running, skipping'); return lastAlerts; }
  scanning = true;
  const t0 = Date.now();
  console.log(`\n🔍 [${new Date().toISOString()}] Scan starting...`);

  try {
    console.log('   Fetching tickers + funding rates...');
    const [tickers, premiums] = await Promise.all([
      get(BINANCE, '/fapi/v1/ticker/24hr'),
      get(BINANCE, '/fapi/v1/premiumIndex'),
    ]);

    if (!Array.isArray(tickers)) throw new Error('tickers not array: ' + JSON.stringify(tickers).slice(0,100));
    if (!Array.isArray(premiums)) throw new Error('premiums not array: ' + JSON.stringify(premiums).slice(0,100));
    console.log(`   Got ${tickers.length} tickers, ${premiums.length} funding rates`);

    const fMap={}, tMap={};
    premiums.forEach(p => { fMap[p.symbol]=p.lastFundingRate; });
    tickers.forEach(t  => { tMap[t.symbol]=t; });

    // Top 120 by volume
    const perps = tickers
      .filter(t => t.symbol.endsWith('USDT') && !SKIP.has(t.symbol) && +t.quoteVolume > 200000)
      .sort((a,b) => +b.quoteVolume - +a.quoteVolume)
      .slice(0, 120);
    console.log(`   Scanning ${perps.length} symbols with klines...`);

    const alerts = [];

    // Batches of 6 with 100ms gap — stays well under Binance rate limit
    for (let i=0; i<perps.length; i+=6) {
      const batch = perps.slice(i, i+6);
      const results = await Promise.all(batch.map(async t => {
        try {
          const kl = await get(BINANCE, `/fapi/v1/klines?symbol=${t.symbol}&interval=5m&limit=75`);
          if (!Array.isArray(kl) || kl.length < 25) return null;
          const res = scoreCoin(kl, fMap[t.symbol], tMap[t.symbol]);
          if (!res || res.score < ALERT_THRESHOLD) return null;
          return {
            symbol: t.symbol, base: t.symbol.replace('USDT',''),
            price: +t.lastPrice, change24h: +t.priceChangePercent,
            volume: +t.quoteVolume, fundingRate: +(fMap[t.symbol]||0)*100,
            ...res
          };
        } catch(e) {
          // Silently skip individual coin errors
          return null;
        }
      }));
      alerts.push(...results.filter(Boolean));
      if (i+6 < perps.length) await sleep(100);
    }

    alerts.sort((a,b) => b.score - a.score);
    lastAlerts     = alerts;
    lastScanTime   = Date.now();
    lastAlertCount = alerts.length;

    const elapsed = ((Date.now()-t0)/1000).toFixed(1);
    console.log(`✅ Scan done in ${elapsed}s — ${alerts.length} alert(s) (score≥${ALERT_THRESHOLD})`);
    alerts.slice(0,5).forEach(a =>
      console.log(`   🎯 ${a.base} score=${a.score} ${a.direction} RSI=${a.rsi} $${fmtP(a.price)} vol=${fmtV(a.volume)}`)
    );

    if (fcmReady && alerts.length > 0) await sendPush(alerts);
    else if (!fcmReady) console.log('   FCM not ready — skipping push');
    else console.log('   No alerts above threshold — no push sent');

    return alerts;

  } catch(e) {
    console.error('❌ Scan failed:', e.message);
    return [];
  } finally {
    scanning = false;
  }
}

// ── FCM push ──────────────────────────────────────────────────
let lastNotified = {}; // sym -> timestamp, prevent spam

async function sendPush(alerts) {
  const tokens = Object.keys(subscriptions);
  if (!tokens.length) { console.log('   No subscribers'); return; }

  const now = Date.now();
  const COOLDOWN = 15 * 60 * 1000; // don't re-notify same coin for 15 min
  const fresh = alerts.filter(a => !lastNotified[a.symbol] || now-lastNotified[a.symbol] > COOLDOWN);
  if (!fresh.length) { console.log('   All alerts in cooldown'); return; }

  fresh.forEach(a => { lastNotified[a.symbol] = now; });

  const top  = fresh[0];
  const dir  = top.direction==='PUMP'?'▲ PUMP':top.direction==='DUMP'?'▼ DUMP':'◈ MOVE';
  const title = `🎯 HumanLens — ${top.base}  Score ${top.score}`;
  const body  = `${dir} · $${fmtP(top.price)} · RSI ${top.rsi}\n${top.signals.slice(0,3).join(' · ')}`;

  const deadTokens = [];
  let sent = 0;

  for (const token of tokens) {
    try {
      await admin.messaging().send({
        token,
        notification: { title, body },
        data: {
          sym:       top.base,
          score:     String(top.score),
          direction: top.direction,
          price:     String(top.price),
          url:       '/'
        },
        android: {
          priority: 'high',
          notification: {
            channelId:    'humanlens_alerts',
            priority:     'max',
            visibility:   'public',
            sound:        'default',
            vibrateTimingsMillis: [0,200,100,300,100,200],
            defaultVibrateTimings: false,
            notificationCount: fresh.length
          }
        },
        apns: {
          headers: { 'apns-priority':'10', 'apns-push-type':'alert' },
          payload: { aps: { sound:'default', badge:fresh.length } }
        },
        webpush: {
          headers: { Urgency:'high', TTL:'900' },
          notification: {
            title, body,
            icon:               '/icon.svg',
            badge:              '/icon.svg',
            tag:                `hl-${top.base}`,
            renotify:           true,
            requireInteraction: true,
            vibrate:            [200,100,300,100,200],
            data:               { url:'/', sym:top.base }
          }
        }
      });
      sent++;
    } catch(e) {
      if (e.code==='messaging/registration-token-not-registered' ||
          e.code==='messaging/invalid-registration-token') {
        deadTokens.push(token);
      } else {
        console.error('   FCM send error:', e.message);
      }
    }
  }

  if (deadTokens.length) {
    deadTokens.forEach(t => delete subscriptions[t]);
    saveSubs();
    console.log(`   Removed ${deadTokens.length} dead token(s)`);
  }

  // Send summary if multiple alerts
  if (fresh.length > 1) {
    const summary = fresh.slice(1,4).map(a=>`${a.base} ${a.direction} (${a.score})`).join(' · ');
    for (const token of Object.keys(subscriptions)) {
      try {
        await admin.messaging().send({
          token,
          notification: { title:`+${fresh.length-1} more signals`, body:summary },
          webpush: {
            headers: { Urgency:'high', TTL:'900' },
            notification: {
              title:`+${fresh.length-1} more signals`, body:summary,
              icon:'/icon.svg', badge:'/icon.svg', tag:'hl-multi', renotify:true
            }
          }
        });
      } catch(e) {}
    }
  }

  console.log(`   📨 Push sent to ${sent} device(s) — ${fresh.length} fresh alert(s)`);
}

// ── Start ─────────────────────────────────────────────────────
console.log('\n🚀 HumanLens Scanner');
console.log(`   Threshold  : score ≥ ${ALERT_THRESHOLD}`);
console.log(`   Interval   : every ${SCAN_INTERVAL/1000}s`);
console.log(`   FCM        : ${fcmReady?'enabled':'DISABLED'}`);
console.log(`   Subscribers: ${Object.keys(subscriptions).length}`);
console.log(`   Port       : ${PORT}\n`);

app.listen(PORT, () => {
  console.log(`✅ API listening on :${PORT}`);

  // First scan immediately, then on interval
  setTimeout(runScan, 3000); // 3s delay so server is fully ready
  setInterval(runScan, SCAN_INTERVAL);
});
