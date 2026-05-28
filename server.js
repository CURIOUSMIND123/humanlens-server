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

// Binance endpoint — env var lets you override without redeploying
// On Railway (EU) fapi.binance.com works fine
// If still blocked, set BINANCE_HOST env var to: fapi1.binance.com
const BINANCE_HOST = process.env.BINANCE_HOST || 'fapi.binance.com';

const SKIP = new Set([
  'BUSDUSDT','USDCUSDT','TUSDUSDT','FDUSDUSDT',
  'USDPUSDT','BTCDOMUSDT','DEFIUSDT','COCOSUSDT'
]);

// ── Robust native https GET ───────────────────────────────────
// Handles: 302 redirects, gzip, chunked, timeout, retries
function get(urlPath, retries=2) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: BINANCE_HOST,
      path: urlPath,
      method: 'GET',
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json',
        'Accept-Encoding': 'identity', // avoid gzip complications
        'Connection': 'keep-alive'
      }
    };

    const attempt = (attemptsLeft) => {
      const req = https.request(options, res => {
        // Handle redirect (302/301/307)
        if ((res.statusCode === 301 || res.statusCode === 302 || res.statusCode === 307)
            && res.headers.location) {
          const loc = res.headers.location;
          // If redirect is to a non-Binance domain it's a block page — fail fast
          if (!loc.includes('binance.com')) {
            return reject(new Error(
              `GEO_BLOCKED: Redirected to ${loc.slice(0,60)} — server IP is geo-blocked by Binance`
            ));
          }
          // Internal Binance redirect — follow it
          const newPath = loc.startsWith('http') ? new URL(loc).pathname + new URL(loc).search : loc;
          options.path = newPath;
          if (attemptsLeft > 0) return attempt(attemptsLeft - 1);
          return reject(new Error('Too many redirects'));
        }

        if (res.statusCode !== 200) {
          res.resume(); // drain
          return reject(new Error(`HTTP ${res.statusCode} for ${urlPath}`));
        }

        let data = '';
        res.setEncoding('utf8');
        res.on('data', chunk => data += chunk);
        res.on('end', () => {
          // Check for HTML block page (starts with < or <!DOCTYPE)
          const trimmed = data.trimStart();
          if (trimmed.startsWith('<')) {
            return reject(new Error(
              `GEO_BLOCKED: Got HTML instead of JSON — server IP blocked by Binance.\n` +
              `Fix: Set BINANCE_HOST env var to fapi1.binance.com or deploy on Railway (EU).`
            ));
          }
          try {
            const json = JSON.parse(data);
            // Binance API-level restriction message
            if (json && json.msg && (
              json.msg.toLowerCase().includes('restricted') ||
              json.msg.toLowerCase().includes('unavailable')
            )) {
              return reject(new Error(`GEO_BLOCKED: ${json.msg.slice(0,100)}`));
            }
            resolve(json);
          } catch(e) {
            reject(new Error(`JSON parse: ${data.slice(0,120)}`));
          }
        });
        res.on('error', reject);
      });

      req.setTimeout(20000, () => { req.destroy(); reject(new Error(`Timeout: ${urlPath}`)); });
      req.on('error', err => {
        if (attemptsLeft > 0) {
          console.warn(`   Retrying ${urlPath} (${attemptsLeft} left)...`);
          setTimeout(() => attempt(attemptsLeft - 1), 1000);
        } else {
          reject(err);
        }
      });
      req.end();
    };

    attempt(retries);
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

// ═══════════════════════════════════════════════════════════
// SCORE ENGINE — EXACT COPY FROM PWA (23-point system)
// S1 Vol Compression, S2 Vol Spike, S3 RSI, S4 Funding Rate,
// S5 BB Squeeze, S6 Price Coiling, S7 Mark/Last Spread,
// S8 Candle Pattern, S9 Period Extreme, S10 Whale/Manipulation
// ═══════════════════════════════════════════════════════════
const fmtPct=(p,d=2)=>(p>=0?'+':'')+p.toFixed(d)+'%';


const estLev=v=>v>=5e8?'125x':v>=1e8?'75x':v>=5e7?'50x':v>=1e7?'25x':v>=2e6?'20x':'10x';
// ═══════════════════════════════════════
// MATH HELPERS
// ═══════════════════════════════════════
function calcRSI(C,p=14){
  if(C.length<p+2)return 50;
  const d=[];for(let i=1;i<C.length;i++)d.push(C[i]-C[i-1]);
  let ag=0,al=0;
  for(let i=0;i<p;i++){if(d[i]>0)ag+=d[i];else al+=Math.abs(d[i]);}
  ag/=p;al/=p;
  for(let i=p;i<d.length;i++){
    const g=d[i]>0?d[i]:0,l=d[i]<0?Math.abs(d[i]):0;
    ag=(ag*(p-1)+g)/p;al=(al*(p-1)+l)/p;
  }
  return al===0?100:100-100/(1+ag/al);
}

function calcBBW(C,p=20){
  if(C.length<p)return null;
  const sl=C.slice(-p),m=sl.reduce((a,b)=>a+b,0)/p;
  const s=Math.sqrt(sl.reduce((a,b)=>a+(b-m)**2,0)/p);
  return m>0?(4*s)/m:null;
}

function calcATR(H,L,C,p=14){
  if(C.length<p+2)return C[C.length-1]*0.02; // fallback 2%
  const tr=[];
  for(let i=1;i<C.length;i++){
    tr.push(Math.max(H[i]-L[i],Math.abs(H[i]-C[i-1]),Math.abs(L[i]-C[i-1])));
  }
  return tr.slice(-p).reduce((a,b)=>a+b,0)/p;
}

function calcSLTP(entry,atr,dir){
  const isPump=dir==='PUMP';
  const sl  =isPump?entry-atr*1.5 :entry+atr*1.5;
  const tp1 =isPump?entry+atr*2   :entry-atr*2;
  const tp2 =isPump?entry+atr*4   :entry-atr*4;
  const tp3 =isPump?entry+atr*7   :entry-atr*7;
  const sign=isPump?1:-1;
  return{
    sl,slPct:sign*(sl-entry)/entry*100,
    tp1,tp1Pct:sign*(tp1-entry)/entry*100,
    tp2,tp2Pct:sign*(tp2-entry)/entry*100,
    tp3,tp3Pct:sign*(tp3-entry)/entry*100,
    atr
  };
}

// ═══════════════════════════════════════
// SCORE ENGINE (MAX 23 pts)
// ═══════════════════════════════════════
function scoreCoin(klines,fundingRate,markPrice,lastPrice,ticker24h){
  if(!klines||klines.length<25)return null;
  const C=klines.map(k=>+k[4]),V=klines.map(k=>+k[5]),
        H=klines.map(k=>+k[2]),L=klines.map(k=>+k[3]),O=klines.map(k=>+k[1]);
  const sigs=[];let sc=0,bias=0;const last=C.length-1;
  const avg20=V.slice(-21,-1).reduce((a,b)=>a+b,0)/20||1;

  // S1: Volume Compression (0-2)
  const rec3=V.slice(-4,-1).reduce((a,b)=>a+b,0)/3;
  const cr=rec3/avg20;
  if(cr<0.22){sc+=2;sigs.push({l:'Vol compressed',d:`${(cr*100).toFixed(0)}% of avg`,w:2,c:'neutral'});}
  else if(cr<0.48){sc+=1;sigs.push({l:'Vol tightening',d:`${(cr*100).toFixed(0)}% of avg`,w:1,c:'neutral'});}

  // S2: Volume Spike (0-3)
  const sR=V[last]/avg20;
  const bull=C[last]>O[last];
  if(sR>=5){sc+=3;sigs.push({l:'VOL EXPLOSION',d:`${sR.toFixed(1)}x avg`,w:3,c:'alert'});bias+=bull?3:-3;}
  else if(sR>=3){sc+=2;sigs.push({l:'VOL SPIKE',d:`${sR.toFixed(1)}x avg`,w:2,c:'alert'});bias+=bull?2:-2;}
  else if(sR>=1.9){sc+=1;sigs.push({l:'Vol building',d:`${sR.toFixed(1)}x avg`,w:1,c:'caution'});bias+=bull?1:-1;}

  // S3: RSI Extreme (0-3)
  const rsi=calcRSI(C,14);
  if(rsi<=22){sc+=3;sigs.push({l:'RSI EXTREME oversold',d:`RSI ${rsi.toFixed(1)} — STRONG BOUNCE`,w:3,c:'bull'});bias+=3;}
  else if(rsi>=78){sc+=3;sigs.push({l:'RSI EXTREME overbought',d:`RSI ${rsi.toFixed(1)} — STRONG DUMP`,w:3,c:'bear'});bias-=3;}
  else if(rsi<=30){sc+=2;sigs.push({l:'RSI oversold',d:`RSI ${rsi.toFixed(1)}`,w:2,c:'bull'});bias+=2;}
  else if(rsi>=70){sc+=2;sigs.push({l:'RSI overbought',d:`RSI ${rsi.toFixed(1)}`,w:2,c:'bear'});bias-=2;}
  else if(rsi<=38){sc+=1;sigs.push({l:'RSI low',d:`RSI ${rsi.toFixed(1)}`,w:1,c:'bull'});bias+=1;}
  else if(rsi>=62){sc+=1;sigs.push({l:'RSI elevated',d:`RSI ${rsi.toFixed(1)}`,w:1,c:'bear'});bias-=1;}

  // S4: Funding Rate (0-2)
  const fr=parseFloat(fundingRate||0)*100;
  if(Math.abs(fr)>=0.08){sc+=2;sigs.push({l:fr<0?'Funding SQUEEZE fuel':'Funding FLUSH fuel',d:`FR ${fr>0?'+':''}${fr.toFixed(4)}%`,w:2,c:fr<0?'bull':'bear'});bias+=fr<0?2:-2;}
  else if(Math.abs(fr)>=0.03){sc+=1;sigs.push({l:'Funding elevated',d:`FR ${fr>0?'+':''}${fr.toFixed(4)}%`,w:1,c:fr<0?'bull':'bear'});bias+=fr<0?1:-1;}

  // S5: BB Squeeze (0-2)
  const curW=calcBBW(C,20);
  if(curW!==null){
    const hw=[];
    for(let i=20;i<C.length-1;i++){const w=calcBBW(C.slice(0,i+1),20);if(w!==null)hw.push(w);}
    if(hw.length>=5){
      const aW=hw.reduce((a,b)=>a+b,0)/hw.length,sq=curW/aW;
      if(sq<0.38){sc+=2;sigs.push({l:'BB squeeze extreme',d:`${(sq*100).toFixed(0)}% of avg width`,w:2,c:'neutral'});}
      else if(sq<0.62){sc+=1;sigs.push({l:'BB squeezing',d:`${(sq*100).toFixed(0)}% of avg width`,w:1,c:'neutral'});}
    }
  }

  // S6: Price Coiling (0-2)
  const l5H=Math.max(...H.slice(-5)),l5L=Math.min(...L.slice(-5));
  const l5R=(l5H-l5L)/C[last]*100;
  const p5H=Math.max(...H.slice(-10,-5)),p5L=Math.min(...L.slice(-10,-5));
  const p5R=(p5H-p5L)/(C[last-5]||C[last])*100;
  if(p5R>0&&l5R<p5R*0.28){sc+=2;sigs.push({l:'Price coiling tight',d:`${l5R.toFixed(2)}% range`,w:2,c:'neutral'});}
  else if(p5R>0&&l5R<p5R*0.52){sc+=1;sigs.push({l:'Range narrowing',d:`${l5R.toFixed(2)}% range`,w:1,c:'neutral'});}

  // S7: Mark/Last Spread (0-2)
  if(markPrice&&lastPrice&&+lastPrice>0){
    const sp=(+markPrice-+lastPrice)/+lastPrice*100;
    if(Math.abs(sp)>=0.4){sc+=2;sigs.push({l:'Mark/Last diverge',d:`${sp>0?'+':''}${sp.toFixed(3)}%`,w:2,c:sp>0?'bull':'bear'});bias+=sp>0?2:-2;}
    else if(Math.abs(sp)>=0.15){sc+=1;sigs.push({l:'Mark/Last spread',d:`${sp>0?'+':''}${sp.toFixed(3)}%`,w:1,c:sp>0?'bull':'bear'});bias+=sp>0?1:-1;}
  }

  // S8: Candle Pattern (0-2)
  const bodies=C.map((c,i)=>Math.abs(c-O[i])/c*100);
  const tiny3=bodies.slice(-4,-1).every(b=>b<0.6);
  const lc=C[last]-O[last],lb=Math.abs(lc)/C[last]*100;
  if(tiny3&&lb>0.8){sc+=2;sigs.push({l:'Coil → Engulfing',d:lc>0?'Bullish breakout':'Bearish breakdown',w:2,c:lc>0?'bull':'bear'});bias+=lc>0?2:-2;}
  else if(tiny3){sc+=1;sigs.push({l:'3 Doji coil',d:'Breakout pending',w:1,c:'neutral'});}
  else{
    const wick=H[last]-Math.max(C[last],O[last]),tail=Math.min(C[last],O[last])-L[last],body=Math.abs(C[last]-O[last]);
    if(body>0&&tail>=body*2.2&&wick<body*0.8){sc+=1;sigs.push({l:'Hammer',d:'Bullish reversal candle',w:1,c:'bull'});bias+=1;}
    else if(body>0&&wick>=body*2.2&&tail<body*0.8){sc+=1;sigs.push({l:'Shooting star',d:'Bearish reversal candle',w:1,c:'bear'});bias-=1;}
  }

  // S9: Period Extreme (0-2)
  const pH=Math.max(...H.slice(-60)),pL=Math.min(...L.slice(-60));
  const toH=(pH-C[last])/C[last]*100,toL=(C[last]-pL)/C[last]*100;
  if(toL<=1.2){sc+=2;sigs.push({l:'At 60-period LOW',d:`${toL.toFixed(2)}% above low`,w:2,c:'bull'});bias+=2;}
  else if(toH<=1.2){sc+=2;sigs.push({l:'At 60-period HIGH',d:`${toH.toFixed(2)}% below high`,w:2,c:'bear'});bias-=2;}
  else if(toL<=4.0){sc+=1;sigs.push({l:'Near period low',d:`${toL.toFixed(2)}% above low`,w:1,c:'bull'});bias+=1;}
  else if(toH<=4.0){sc+=1;sigs.push({l:'Near period high',d:`${toH.toFixed(2)}% below high`,w:1,c:'bear'});bias-=1;}

  // S10: WHALE / MANIPULATION PATTERN (0-3) — The EDEN Pattern
  // Detect: Recent huge spike (EDEN-type pump), then volume collapse = distribution
  // Find the single highest volume candle in history
  let whaleScore=0,whaleSigs=[];
  const maxVol=Math.max(...V);
  const maxVolIdx=V.indexOf(maxVol);
  const spikeRatio=maxVol/avg20;
  const priceAtSpike=C[maxVolIdx];
  const priceDrop=(priceAtSpike-C[last])/priceAtSpike*100;

  // Pattern A: Huge spike >8x happened, price has since dropped >15% from that candle
  if(spikeRatio>=8&&priceDrop>=15&&maxVolIdx<last-5){
    whaleScore+=2;
    whaleSigs.push({l:'WHALE dump pattern',d:`Spike ${spikeRatio.toFixed(0)}x avg, then -${priceDrop.toFixed(0)}% drop — post-distribution`,w:2,c:'whale'});
    bias-=1; // tends toward further dump after distribution
  } else if(spikeRatio>=5&&priceDrop>=20&&maxVolIdx<last-5){
    whaleScore+=1;
    whaleSigs.push({l:'Large dump pattern',d:`Spike ${spikeRatio.toFixed(0)}x, -${priceDrop.toFixed(0)}% since — bounce or continue`,w:1,c:'whale'});
  }

  // Pattern B: 24h change extreme + current compression = EDEN moment
  const ch24=ticker24h?+ticker24h.priceChangePercent:0;
  const highLow24Pct=ticker24h?((+ticker24h.highPrice-+ticker24h.lowPrice)/+ticker24h.lowPrice*100):0;
  if(Math.abs(ch24)>=25&&cr<0.40){
    whaleScore+=1;
    whaleSigs.push({l:`${ch24>0?'Pump':'Dump'} + compression`,d:`24h: ${fmtPct(ch24)}, vol now quiet — next move near`,w:1,c:'whale'});
    bias+=ch24<0?1:-1; // counter-move more likely
  }

  // Pattern C: 24h high-low range > 35% = volatile, low-float coin being played
  if(highLow24Pct>=35){
    whaleScore+=1;
    whaleSigs.push({l:'Low float play',d:`24h H-L range: ${highLow24Pct.toFixed(0)}% — whale territory`,w:1,c:'whale'});
  }

  if(whaleScore>0){
    sc+=Math.min(whaleScore,3);
    sigs.push(...whaleSigs);
  }

  // Direction
  let dir='WATCH';
  if(sc>=6){dir=bias>1?'PUMP':bias<-1?'DUMP':'COILING';}

  const isWhale=whaleScore>0;
  const atr=calcATR(H,L,C,14);

  return{score:sc,maxScore:23,signals:sigs,direction:dir,bias,rsi:rsi.toFixed(1),fr,isWhale,atr,
    ch24:ticker24h?+ticker24h.priceChangePercent:0,
    high24:ticker24h?+ticker24h.highPrice:+lastPrice,
    low24:ticker24h?+ticker24h.lowPrice:+lastPrice};
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
      get('/fapi/v1/ticker/24hr'),
      get('/fapi/v1/premiumIndex'),
    ]);

    if (!Array.isArray(tickers)) throw new Error('tickers not array: ' + JSON.stringify(tickers).slice(0,100));
    if (!Array.isArray(premiums)) throw new Error('premiums not array: ' + JSON.stringify(premiums).slice(0,100));
    console.log(`   Got ${tickers.length} tickers, ${premiums.length} funding rates`);

    const fMap={}, mMap={}, tMap={};
    premiums.forEach(p => { fMap[p.symbol]=p.lastFundingRate; mMap[p.symbol]=p.markPrice; });
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
          const kl = await get(`/fapi/v1/klines?symbol=${t.symbol}&interval=5m&limit=75`);
          if (!Array.isArray(kl) || kl.length < 25) return null;
          const res = scoreCoin(kl, fMap[t.symbol], mMap[t.symbol], t.lastPrice, tMap[t.symbol]);
          if (!res || res.score < 10) return null; // collect all ≥10 like PWA
          return {
            symbol: t.symbol, base: t.symbol.replace('USDT',''),
            price: +t.lastPrice, change24h: +t.priceChangePercent,
            volume: +t.quoteVolume, fundingRate: +(fMap[t.symbol]||0)*100,
            markPrice: +(mMap[t.symbol]||t.lastPrice),
            high24: +t.highPrice, low24: +t.lowPrice,
            maxLev: estLev(+t.quoteVolume),
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

    const pushAlerts = alerts.filter(a => a.score >= ALERT_THRESHOLD);
    if (fcmReady && pushAlerts.length > 0) await sendPush(pushAlerts);
    else if (alerts.length > 0) console.log(`   ${alerts.length} coins scored ≥10, none hit push threshold ${ALERT_THRESHOLD}`);
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
