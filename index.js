'use strict';
const axios=require('axios');
const express=require('express');
const mongoose=require('mongoose');
const {Telegraf}=require('telegraf');

const VERSION='LOMY FOREX V1.5.1 COMPACT BASIC-SAFE';
const MODE='PAPER',LIVE_TRADING=false;
const PORT=Number(process.env.PORT||10000);
const TELEGRAM_BOT_TOKEN=String(process.env.TELEGRAM_BOT_TOKEN||'').trim();
const TELEGRAM_CHAT_ID_ENV=String(process.env.TELEGRAM_CHAT_ID||'').trim();
const TWELVE_DATA_API_KEY=String(process.env.TWELVE_DATA_API_KEY||'').trim();
const MONGODB_URI=String(process.env.MONGODB_URI||'').trim();
const GEMINI_API_KEY=String(process.env.GEMINI_API_KEY||'').trim();
const GEMINI_MODEL=String(process.env.GEMINI_MODEL||'gemini-3.6-flash').trim();
const TWELVE_BASE='https://api.twelvedata.com';

const TF_MS=15*60*1000;
const CORE_MIN_HISTORY=60;
const INITIAL_HISTORY=1100;
const REFRESH_HISTORY=8;

const TWELVE_MIN_GAP_MS=10000;
const TWELVE_DAILY_SOFT_CAP=620;

const GEMINI_ENTRY_DAILY_CAP=15;
const GEMINI_MANAGE_DAILY_CAP=3;
const GEMINI_ENTRY_MIN_GAP_MS=75*60*1000;
const GEMINI_CALL_GAP_MS=4000;

const NEWS_REFRESH_MS=12*60*60*1000;
const NEWS_BLOCK_MIN=30;

const ALL_INSTRUMENTS=[
'EURUSD','GBPUSD','USDJPY','USDCHF','AUDUSD','NZDUSD','USDCAD',
'EURGBP','EURJPY','EURCHF','EURAUD','EURNZD','EURCAD',
'GBPJPY','GBPCHF','GBPAUD','GBPNZD','GBPCAD',
'AUDJPY','AUDCHF','AUDNZD','AUDCAD',
'NZDJPY','NZDCHF','NZDCAD',
'CADJPY','CADCHF','CHFJPY',
'GBPSGD','EURSGD','XAUUSD'
];

const DEFAULT_ACTIVE=['EURUSD','GBPUSD','USDJPY','AUDUSD','USDCAD'];

const ACTIVE_SYMBOLS=(()=>{
  const raw=String(process.env.ACTIVE_SYMBOLS||'').trim();
  if(!raw)return DEFAULT_ACTIVE;
  const x=[...new Set(
    raw.split(',')
      .map(s=>s.trim().toUpperCase())
      .filter(s=>ALL_INSTRUMENTS.includes(s))
  )];
  return x.length?x:DEFAULT_ACTIVE;
})();

const RULES=Object.freeze({
  riskReward:2,
  breakEvenTriggerR:.6,
  partialTpTriggerR:2,
  trailingStartStopR:1,
  trailingStepR:.5,
  minStopAtr:.25,
  maxStopAtr:6,
  maxSpreadRiskFraction:.2,
  minEntryConfidence:62,
  minCloseConfidence:68
});

const PAPER=Object.freeze({
  startingBalance:300,
  maxCapitalRiskPct:1,
  portfolioRiskCapPct:4,
  maxOpenTrades:31,
  accountKey:'lomy-forex-v15-gemini-pro-300usd'
});

const DYNAMIC_RISK=Object.freeze({
  highConfidence:85,
  highRiskPct:1,
  medConfidence:75,
  medRiskPct:.75,
  lowConfidence:62,
  lowRiskPct:.5
});

const TECH=Object.freeze({
  emaFast:9,
  emaMedium:21,
  emaTrend:50,
  emaLong:100,
  emaMacro:200,
  rsiLen:14,
  cmoLen:9,
  atrLen:14,
  adxLen:14,
  stochasticLen:14,
  stochasticSmooth:3,
  rocLen:12,
  bbLen:20,
  bbStd:2,
  keltnerLen:20,
  keltnerAtrLen:14,
  keltnerMult:1.5,
  volumeLen:20,
  srLen:40,
  fibLookback:60,
  swingLeft:3,
  swingRight:3,
  liquidityLookback:20,
  vwapLookback:50,
  mfiLen:14
});

const AI=Object.freeze({
  entryCommanderEnabled:true,
  managementEnabled:true,
  memoryClosedTrades:40,
  temperature:.1,
  timeoutMs:20000
});

const state={
  startedAt:new Date(),
  mongoReady:false,
  telegramReady:false,
  marketReady:false,
  geminiReady:false,
  newsReady:false,
  scanBusy:false,
  loopsStarted:false,
  lastMarketError:null,
  lastAiError:null,
  lastNewsError:null,
  scannedBars:0,
  aiEntryCalls:0,
  aiManageCalls:0,
  aiBuyDecisions:0,
  aiSellDecisions:0,
  aiNoTradeDecisions:0,
  aiCloseDecisions:0,
  aiHoldDecisions:0,
  executedSignals:0,
  skippedSignals:0,
  pairState:new Map(),
  openTrades:new Map(),
  twelveBlockedUntil:0,
  geminiBlockedUntil:0,
  nextScanAt:null,
  lastEntryAiAt:0
};

let account=null;
let telegramBot=null;
let telegramChatId=TELEGRAM_CHAT_ID_ENV;
let economicNews=[];
let twelveChain=Promise.resolve();
let geminiChain=Promise.resolve();
let lastTwelveAt=0;
let lastGeminiAt=0;

const http=axios.create({
  timeout:20000,
  headers:{'User-Agent':'LOMY-FOREX-V1.5.1'}
});

function n(v,f=0){
  const x=Number(v);
  return Number.isFinite(x)?x:f;
}

function clamp(v,a,b){
  return Math.max(a,Math.min(b,v));
}

function sleep(ms){
  return new Promise(r=>setTimeout(r,ms));
}

function sum(a){
  return a.filter(Number.isFinite).reduce((x,y)=>x+y,0);
}

function avg(a){
  const v=a.filter(Number.isFinite);
  return v.length?sum(v)/v.length:NaN;
}

function sd(a){
  const v=a.filter(Number.isFinite);
  if(!v.length)return NaN;
  const m=avg(v);
  return Math.sqrt(v.reduce((s,x)=>s+(x-m)**2,0)/v.length);
}

function last(a){
  return Array.isArray(a)&&a.length?a[a.length-1]:undefined;
}

function pct(a,b){
  a=n(a,NaN);
  b=n(b,NaN);
  return Number.isFinite(a)&&Number.isFinite(b)&&a!==0
    ?(b-a)/Math.abs(a)*100
    :NaN;
}

function safeError(e){
  const d=e?.response?.data;
  return d?.error?.message||
    d?.message||
    d?.error||
    e?.message||
    String(e);
}

function fmtMoney(v){
  return '$'+n(v).toFixed(2);
}

function fmtPrice(v,s=''){
  if(!Number.isFinite(Number(v)))return'n/a';
  v=Number(v);
  if(s==='XAUUSD')return v.toFixed(2);
  if(s.endsWith('JPY'))return v.toFixed(3);
  return v.toFixed(5);
}

function parseTime(v){
  if(v instanceof Date)return v.getTime();
  if(typeof v==='number')return v;

  let s=String(v||'').trim();

  if(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(s)){
    s=s.replace(' ','T')+'Z';
  }

  const t=new Date(s).getTime();
  return Number.isFinite(t)?t:0;
}

function barTimeMs(b){
  return parseTime(b?.openTime);
}

function highestHigh(b){
  return b?.length?Math.max(...b.map(x=>x.high)):NaN;
}

function lowestLow(b){
  return b?.length?Math.min(...b.map(x=>x.low)):NaN;
}

function utcDay(){
  return new Date().toISOString().slice(0,10);
}

function nextUtcMidnight(){
  const d=new Date();
  return Date.UTC(
    d.getUTCFullYear(),
    d.getUTCMonth(),
    d.getUTCDate()+1,
    0,0,5
  );
}

function normalizeBars(raw){
  if(!Array.isArray(raw))return[];

  return raw.map(x=>({
    openTime:x.openTime||x.datetime||x.time||x.timestamp,
    open:n(x.open,NaN),
    high:n(x.high,NaN),
    low:n(x.low,NaN),
    close:n(x.close,NaN),
    volume:n(x.tickVolume,n(x.volume,0)),
    isOpen:x.isOpen===true
  }))
  .filter(x=>
    x.openTime&&
    [x.open,x.high,x.low,x.close].every(Number.isFinite)
  )
  .sort((a,b)=>barTimeMs(a)-barTimeMs(b));
}

function closed15(b){
  const now=Date.now();

  return b.filter(x=>
    !x.isOpen&&
    barTimeMs(x)>0&&
    barTimeMs(x)+TF_MS<=now+5000
  );
}

function mergeBars(a,b,max=INITIAL_HISTORY){
  const m=new Map();

  for(const x of [...(a||[]),...(b||[])]){
    m.set(barTimeMs(x),x);
  }

  return [...m.values()]
    .sort((x,y)=>barTimeMs(x)-barTimeMs(y))
    .slice(-max);
}

function aggregateBars(bars,minutes){
  const ms=minutes*60000;
  const m=new Map();
  const now=Date.now();

  for(const b of bars){
    const t=barTimeMs(b);
    if(!t)continue;

    const k=Math.floor(t/ms)*ms;
    let x=m.get(k);

    if(!x){
      x={
        openTime:new Date(k).toISOString(),
        open:b.open,
        high:b.high,
        low:b.low,
        close:b.close,
        volume:n(b.volume,0),
        isOpen:false
      };
      m.set(k,x);
    }else{
      x.high=Math.max(x.high,b.high);
      x.low=Math.min(x.low,b.low);
      x.close=b.close;
      x.volume+=n(b.volume,0);
    }
  }

  return [...m.values()]
    .filter(x=>barTimeMs(x)+ms<=now+5000)
    .sort((a,b)=>barTimeMs(a)-barTimeMs(b));
}

function emaSeries(v,l){
  if(!Array.isArray(v)||v.length<l)return[];

  const o=new Array(v.length).fill(NaN);
  const k=2/(l+1);
  const seed=v.slice(0,l);

  if(!seed.every(Number.isFinite))return o;

  o[l-1]=sum(seed)/l;

  for(let i=l;i<v.length;i++){
    if(Number.isFinite(v[i])&&Number.isFinite(o[i-1])){
      o[i]=v[i]*k+o[i-1]*(1-k);
    }
  }

  return o;
}

function emaLast(v,l){
  const s=emaSeries(v,l);
  return s[s.length-1];
}

function trSeries(b){
  const o=[];

  for(let i=1;i<(b?.length||0);i++){
    o.push(Math.max(
      b[i].high-b[i].low,
      Math.abs(b[i].high-b[i-1].close),
      Math.abs(b[i].low-b[i-1].close)
    ));
  }

  return o;
}

function atrLast(b,l=14){
  const r=trSeries(b);
  if(r.length<l)return NaN;

  let a=sum(r.slice(0,l))/l;

  for(let i=l;i<r.length;i++){
    a=(a*(l-1)+r[i])/l;
  }

  return a;
}

function rsiLast(v,l=14){
  if(!v||v.length<=l)return NaN;

  let g=0,loss=0;

  for(let i=1;i<=l;i++){
    const c=v[i]-v[i-1];
    c>=0?g+=c:loss+=Math.abs(c);
  }

  let ag=g/l;
  let al=loss/l;

  for(let i=l+1;i<v.length;i++){
    const c=v[i]-v[i-1];
    ag=(ag*(l-1)+Math.max(c,0))/l;
    al=(al*(l-1)+Math.max(-c,0))/l;
  }

  return al===0?100:100-100/(1+ag/al);
}

function cmoLast(v,l=9){
  if(!v||v.length<=l)return NaN;

  let up=0,dn=0;

  for(let i=v.length-l;i<v.length;i++){
    const c=v[i]-v[i-1];
    if(c>0)up+=c;
    else if(c<0)dn+=-c;
  }

  return up+dn?100*(up-dn)/(up+dn):0;
}

function macdLast(v){
  if(!v||v.length<35){
    return{macd:NaN,signal:NaN,histogram:NaN};
  }

  const f=emaSeries(v,12);
  const s=emaSeries(v,26);
  const m=[];

  for(let i=0;i<v.length;i++){
    if(Number.isFinite(f[i])&&Number.isFinite(s[i])){
      m.push(f[i]-s[i]);
    }
  }

  if(m.length<9){
    return{macd:NaN,signal:NaN,histogram:NaN};
  }

  const sg=emaSeries(m,9);
  const mv=last(m);
  const sv=last(sg);

  return{
    macd:mv,
    signal:sv,
    histogram:mv-sv
  };
}

function stochasticLast(b,l=14){
  if(!b||b.length<l)return{k:NaN,d:NaN};

  const ks=[];

  for(let i=Math.max(l-1,b.length-5);i<b.length;i++){
    const s=b.slice(i-l+1,i+1);
    const h=highestHigh(s);
    const lo=lowestLow(s);
    const r=h-lo;

    ks.push(
      r===0?50:(b[i].close-lo)/r*100
    );
  }

  return{
    k:last(ks),
    d:avg(ks.slice(-TECH.stochasticSmooth))
  };
}

function williamsRLast(b,l=14){
  if(!b||b.length<l)return NaN;

  const s=b.slice(-l);
  const h=highestHigh(s);
  const lo=lowestLow(s);
  const c=last(s).close;

  return h===lo?-50:-100*(h-c)/(h-lo);
}

function rocLast(v,l=12){
  return !v||v.length<=l
    ?NaN
    :pct(v[v.length-1-l],last(v));
}

function bollingerLast(v,l=20,m=2){
  if(!v||v.length<l){
    return{
      middle:NaN,
      upper:NaN,
      lower:NaN,
      widthPct:NaN
    };
  }

  const s=v.slice(-l);
  const mid=avg(s);
  const d=sd(s);
  const u=mid+m*d;
  const lo=mid-m*d;

  return{
    middle:mid,
    upper:u,
    lower:lo,
    widthPct:mid?(u-lo)/Math.abs(mid)*100:NaN
  };
}

function keltnerLast(b){
  const c=b.map(x=>x.close);
  const mid=emaLast(c,TECH.keltnerLen);
  const a=atrLast(b,TECH.keltnerAtrLen);

  return{
    middle:mid,
    upper:mid+TECH.keltnerMult*a,
    lower:mid-TECH.keltnerMult*a
  };
}

function obvLast(b){
  let o=0;

  for(let i=1;i<(b?.length||0);i++){
    const v=n(b[i].volume);

    if(b[i].close>b[i-1].close)o+=v;
    else if(b[i].close<b[i-1].close)o-=v;
  }

  return o;
}

function mfiLast(b,l=14){
  if(!b||b.length<=l)return NaN;

  let p=0,ng=0;

  for(let i=b.length-l;i<b.length;i++){
    const t=(b[i].high+b[i].low+b[i].close)/3;
    const pt=(b[i-1].high+b[i-1].low+b[i-1].close)/3;
    const f=t*n(b[i].volume);

    if(t>pt)p+=f;
    else if(t<pt)ng+=f;
  }

  if(!p&&!ng)return 50;
  if(!ng)return 100;

  return 100-100/(1+p/ng);
}

function vwapLast(b,l=50){
  const s=b.slice(-l);
  let w=0,v=0;

  for(const x of s){
    const t=(x.high+x.low+x.close)/3;
    const q=n(x.volume);
    w+=t*q;
    v+=q;
  }

  return v>0
    ?w/v
    :avg(s.map(x=>(x.high+x.low+x.close)/3));
}

function candleContext(b){
  if(!b){
    return{
      direction:'UNKNOWN',
      bodyRatio:0,
      upperWickRatio:0,
      lowerWickRatio:0
    };
  }

  const r=Math.max(b.high-b.low,Number.EPSILON);
  const body=Math.abs(b.close-b.open);

  return{
    direction:b.close>b.open
      ?'BULLISH'
      :b.close<b.open
        ?'BEARISH'
        :'DOJI',
    bodyRatio:body/r,
    upperWickRatio:(b.high-Math.max(b.open,b.close))/r,
    lowerWickRatio:(Math.min(b.open,b.close)-b.low)/r
  };
}

function supportResistance(b,l=40){
  const s=b.slice(-(l+1),-1);

  return{
    support:lowestLow(s),
    resistance:highestHigh(s)
  };
}

function findSwings(b,left=3,right=3){
  const highs=[];
  const lows=[];

  for(let i=left;i<b.length-right;i++){
    let sh=true,sl=true;

    for(let j=1;j<=left;j++){
      if(b[i].high<=b[i-j].high)sh=false;
      if(b[i].low>=b[i-j].low)sl=false;
    }

    for(let j=1;j<=right;j++){
      if(b[i].high<=b[i+j].high)sh=false;
      if(b[i].low>=b[i+j].low)sl=false;
    }

    if(sh){
      highs.push({
        index:i,
        price:b[i].high,
        time:b[i].openTime
      });
    }

    if(sl){
      lows.push({
        index:i,
        price:b[i].low,
        time:b[i].openTime
      });
    }
  }

  return{highs,lows};
}

function marketStructure(b){
  const s=findSwings(
    b,
    TECH.swingLeft,
    TECH.swingRight
  );

  const h=s.highs.slice(-2);
  const l=s.lows.slice(-2);

  let structure='NEUTRAL';

  if(h.length>=2&&l.length>=2){
    if(
      h[1].price>h[0].price&&
      l[1].price>l[0].price
    ){
      structure='BULLISH';
    }else if(
      h[1].price<h[0].price&&
      l[1].price<l[0].price
    ){
      structure='BEARISH';
    }
  }

  const c=last(b)?.close;
  const lh=last(s.highs)?.price;
  const ll=last(s.lows)?.price;

  let bos='NONE';

  if(
    Number.isFinite(c)&&
    Number.isFinite(lh)&&
    c>lh
  ){
    bos='BULLISH_BOS';
  }else if(
    Number.isFinite(c)&&
    Number.isFinite(ll)&&
    c<ll
  ){
    bos='BEARISH_BOS';
  }

  return{
    structure,
    bos,
    lastSwingHigh:lh,
    lastSwingLow:ll
  };
}

function dmiAdx(b,l=14){
  if(!b||b.length<l*2+2){
    return{
      adx:NaN,
      plusDI:NaN,
      minusDI:NaN
    };
  }

  const tr=[];
  const pdm=[];
  const mdm=[];

  for(let i=1;i<b.length;i++){
    const u=b[i].high-b[i-1].high;
    const d=b[i-1].low-b[i].low;

    pdm.push(u>d&&u>0?u:0);
    mdm.push(d>u&&d>0?d:0);

    tr.push(Math.max(
      b[i].high-b[i].low,
      Math.abs(b[i].high-b[i-1].close),
      Math.abs(b[i].low-b[i-1].close)
    ));
  }

  let st=sum(tr.slice(0,l));
  let sp=sum(pdm.slice(0,l));
  let sm=sum(mdm.slice(0,l));
  let p=NaN,m=NaN;
  const dx=[];

  for(let i=l;i<tr.length;i++){
    if(i>l){
      st=st-st/l+tr[i];
      sp=sp-sp/l+pdm[i];
      sm=sm-sm/l+mdm[i];
    }

    p=st?100*sp/st:0;
    m=st?100*sm/st:0;

    dx.push(
      p+m
        ?100*Math.abs(p-m)/(p+m)
        :0
    );
  }

  if(dx.length<l){
    return{
      adx:NaN,
      plusDI:p,
      minusDI:m
    };
  }

  let a=avg(dx.slice(0,l));

  for(let i=l;i<dx.length;i++){
    a=(a*(l-1)+dx[i])/l;
  }

  return{
    adx:a,
    plusDI:p,
    minusDI:m
  };
}

function volumeContext(b){
  const cur=n(last(b)?.volume);
  const prior=b
    .slice(-(TECH.volumeLen+1),-1)
    .map(x=>n(x.volume));

  const a=avg(prior);
  const r=Number.isFinite(a)&&a>0
    ?cur/a
    :NaN;

  return{
    current:cur,
    average:n(a),
    ratio:r,
    spike:Number.isFinite(r)&&r>=1.5
  };
}

function volatilityContext(b){
  const a=atrLast(b,TECH.atrLen);
  const c=last(b);
  const hist=[];

  for(
    let i=Math.max(
      TECH.atrLen+2,
      b.length-50
    );
    i<=b.length;
    i++
  ){
    const x=atrLast(
      b.slice(0,i),
      TECH.atrLen
    );

    if(Number.isFinite(x)){
      hist.push(x);
    }
  }

  const aa=avg(hist);

  const r=
    Number.isFinite(a)&&
    Number.isFinite(aa)&&
    aa>0
      ?a/aa
      :NaN;

  return{
    atr:a,
    atrPct:
      c&&
      c.close>0&&
      Number.isFinite(a)
        ?a/c.close*100
        :NaN,
    averageAtr:aa,
    atrRatio:r,
    regime:Number.isFinite(r)
      ?r>=1.5
        ?'HIGH'
        :r<=.7
          ?'LOW'
          :'NORMAL'
      :'NORMAL'
  };
}

function liquidityContext(
  b,
  l=TECH.liquidityLookback
){
  if(!b||b.length<l+1){
    return{
      bullishSweep:false,
      bearishSweep:false,
      priorHigh:NaN,
      priorLow:NaN
    };
  }

  const c=last(b);
  const p=b.slice(-(l+1),-1);
  const h=highestHigh(p);
  const lo=lowestLow(p);

  return{
    bullishSweep:
      c.low<lo&&
      c.close>lo,

    bearishSweep:
      c.high>h&&
      c.close<h,

    priorHigh:h,
    priorLow:lo
  };
}

function fvgContext(b){
  if(!b||b.length<3){
    return{
      bullish:false,
      bearish:false
    };
  }

  const a=b[b.length-3];
  const c=last(b);

  return{
    bullish:c.low>a.high,
    bearish:c.high<a.low,
    bullGapLow:c.low>a.high?a.high:NaN,
    bullGapHigh:c.low>a.high?c.low:NaN,
    bearGapLow:c.high<a.low?c.high:NaN,
    bearGapHigh:c.high<a.low?a.low:NaN
  };
}

function fibonacciContext(
  b,
  l=TECH.fibLookback
){
  const s=b.slice(-l);
  const h=highestHigh(s);
  const lo=lowestLow(s);
  const r=h-lo;

  if(!Number.isFinite(r)||r<=0){
    return null;
  }

  return{
    swingHigh:h,
    swingLow:lo,
    r382:h-r*.382,
    r500:h-r*.5,
    r618:h-r*.618,
    r786:h-r*.786
  };
}

function supertrendContext(b,l=10,m=3){
  if(!b||b.length<l+5){
    return{
      direction:'UNKNOWN',
      value:NaN
    };
  }

  let fu=NaN;
  let fl=NaN;
  let st=NaN;
  let prevFu=NaN;
  let prevFl=NaN;
  let prevSt=NaN;

  for(let i=l+1;i<b.length;i++){
    const a=atrLast(
      b.slice(0,i+1),
      l
    );

    if(!Number.isFinite(a)){
      continue;
    }

    const c=b[i];
    const p=b[i-1];
    const mid=(c.high+c.low)/2;
    const bu=mid+m*a;
    const bl=mid-m*a;

    prevFu=fu;
    prevFl=fl;
    prevSt=st;

    fu=
      !Number.isFinite(prevFu)||
      bu<prevFu||
      p.close>prevFu
        ?bu
        :prevFu;

    fl=
      !Number.isFinite(prevFl)||
      bl>prevFl||
      p.close<prevFl
        ?bl
        :prevFl;

    if(!Number.isFinite(prevSt)){
      st=c.close>=mid?fl:fu;
    }else if(prevSt===prevFu){
      st=c.close<=fu?fu:fl;
    }else{
      st=c.close>=fl?fl:fu;
    }
  }

  const c=last(b);

  return{
    direction:Number.isFinite(st)
      ?c.close>st
        ?'BULL'
        :'BEAR'
      :'UNKNOWN',
    value:st
  };
}

function ichimokuContext(b){
  if(!b||b.length<52){
    return{
      tenkan:NaN,
      kijun:NaN,
      spanA:NaN,
      spanB:NaN,
      bias:'UNKNOWN'
    };
  }

  const mid=l=>(
    highestHigh(b.slice(-l))+
    lowestLow(b.slice(-l))
  )/2;

  const t=mid(9);
  const k=mid(26);
  const a=(t+k)/2;
  const s=mid(52);
  const c=last(b).close;
  const top=Math.max(a,s);
  const bot=Math.min(a,s);

  return{
    tenkan:t,
    kijun:k,
    spanA:a,
    spanB:s,
    bias:
      c>top&&t>k
        ?'BULL'
        :c<bot&&t<k
          ?'BEAR'
          :'MIXED'
  };
}

function chochContext(b){
  const s=findSwings(
    b,
    TECH.swingLeft,
    TECH.swingRight
  );

  const h=s.highs.slice(-2);
  const l=s.lows.slice(-2);
  const c=last(b);

  if(!c||h.length<2||l.length<2){
    return{
      bullish:false,
      bearish:false,
      direction:'NONE'
    };
  }

  const prevBear=
    h[1].price<h[0].price&&
    l[1].price<l[0].price;

  const prevBull=
    h[1].price>h[0].price&&
    l[1].price>l[0].price;

  const bull=
    prevBear&&
    c.close>h[1].price;

  const bear=
    prevBull&&
    c.close<l[1].price;

  return{
    bullish:bull,
    bearish:bear,
    direction:
      bull
        ?'BULLISH_CHOCH'
        :bear
          ?'BEARISH_CHOCH'
          :'NONE'
  };
}

function trendContext(b){
  const c=b.map(x=>x.close);
  const cl=last(c);

  const e9=emaLast(c,9);
  const e21=emaLast(c,21);
  const e50=emaLast(c,50);
  const e100=emaLast(c,100);
  const e200=c.length>=200
    ?emaLast(c,200)
    :NaN;

  const alignment=
    Number.isFinite(e50)&&
    cl>e9&&
    e9>e21&&
    e21>e50
      ?'BULL'
      :Number.isFinite(e50)&&
       cl<e9&&
       e9<e21&&
       e21<e50
        ?'BEAR'
        :'MIXED';

  const macro=
    Number.isFinite(e200)
      ?cl>e200
        ?'BULL'
        :cl<e200
          ?'BEAR'
          :'FLAT'
      :Number.isFinite(e100)
        ?cl>e100
          ?'BULL'
          :'BEAR'
        :'UNKNOWN';

  return{
    close:cl,
    ema9:e9,
    ema21:e21,
    ema50:e50,
    ema100:e100,
    ema200:e200,
    alignment,
    macro
  };
}

function momentumContext(b){
  const c=b.map(x=>x.close);

  return{
    rsi:rsiLast(c,TECH.rsiLen),
    cmo:cmoLast(c,TECH.cmoLen),
    macd:macdLast(c),
    stochastic:stochasticLast(
      b,
      TECH.stochasticLen
    ),
    williamsR:williamsRLast(
      b,
      TECH.stochasticLen
    ),
    roc:rocLast(c,TECH.rocLen)
  };
}

function buildTechnicalIntelligence(b){
  if(!Array.isArray(b)||b.length<CORE_MIN_HISTORY){
    return null;
  }

  const c=b.map(x=>x.close);
  const bar=last(b);

  const trend=trendContext(b);
  const momentum=momentumContext(b);
  const volatility=volatilityContext(b);
  const dmi=dmiAdx(b,TECH.adxLen);
  const bollinger=bollingerLast(
    c,
    TECH.bbLen,
    TECH.bbStd
  );
  const keltner=keltnerLast(b);
  const volume=volumeContext(b);
  const structure=marketStructure(b);
  const liquidity=liquidityContext(b);
  const fvg=fvgContext(b);
  const fibonacci=fibonacciContext(b);
  const candle=candleContext(bar);
  const sr=supportResistance(b,TECH.srLen);
  const supertrend=supertrendContext(b);
  const ichimoku=ichimokuContext(b);
  const choch=chochContext(b);
  const vwap=vwapLast(b,TECH.vwapLookback);
  const obv=obvLast(b);
  const mfi=mfiLast(b,TECH.mfiLen);

  let bull=0,bear=0;

  if(trend.alignment==='BULL')bull+=2;
  if(trend.alignment==='BEAR')bear+=2;

  if(trend.macro==='BULL')bull++;
  if(trend.macro==='BEAR')bear++;

  if(supertrend.direction==='BULL')bull++;
  if(supertrend.direction==='BEAR')bear++;

  if(ichimoku.bias==='BULL')bull++;
  if(ichimoku.bias==='BEAR')bear++;

  if(Number.isFinite(dmi.adx)&&dmi.adx>=20){
    if(dmi.plusDI>dmi.minusDI)bull++;
    else if(dmi.minusDI>dmi.plusDI)bear++;
  }

  if(Number.isFinite(momentum.rsi)){
    if(
      momentum.rsi>=52&&
      momentum.rsi<=75
    ){
      bull++;
    }

    if(
      momentum.rsi<=48&&
      momentum.rsi>=25
    ){
      bear++;
    }
  }

  if(momentum.cmo>0)bull++;
  if(momentum.cmo<0)bear++;

  if(momentum.macd?.histogram>0)bull++;
  if(momentum.macd?.histogram<0)bear++;

  if(
    structure.structure==='BULLISH'||
    structure.bos==='BULLISH_BOS'
  ){
    bull++;
  }

  if(
    structure.structure==='BEARISH'||
    structure.bos==='BEARISH_BOS'
  ){
    bear++;
  }

  if(choch.bullish)bull+=2;
  if(choch.bearish)bear+=2;

  if(liquidity.bullishSweep)bull++;
  if(liquidity.bearishSweep)bear++;

  if(
    candle.direction==='BULLISH'&&
    candle.bodyRatio>=.5
  ){
    bull++;
  }

  if(
    candle.direction==='BEARISH'&&
    candle.bodyRatio>=.5
  ){
    bear++;
  }

  if(Number.isFinite(vwap)){
    if(bar.close>vwap)bull++;
    else if(bar.close<vwap)bear++;
  }

  return{
    barTime:bar.openTime,
    price:bar.close,
    bias:
      bull>bear
        ?'BULL'
        :bear>bull
          ?'BEAR'
          :'NEUTRAL',
    score:{
      bullish:bull,
      bearish:bear
    },
    trend,
    momentum,
    volatility,
    dmi,
    bollinger,
    keltner,
    volume,
    structure,
    liquidity,
    fvg,
    fibonacci,
    candle,
    supportResistance:sr,
    supertrend,
    ichimoku,
    choch,
    vwap,
    obv,
    mfi
  };
}

function compactTechnical(t){
  if(!t)return null;

  return{
    price:t.price,
    bias:t.bias,
    score:t.score,

    trend:{
      alignment:t.trend?.alignment,
      macro:t.trend?.macro,
      ema9:t.trend?.ema9,
      ema21:t.trend?.ema21,
      ema50:t.trend?.ema50,
      ema200:t.trend?.ema200
    },

    momentum:{
      rsi:t.momentum?.rsi,
      cmo:t.momentum?.cmo,
      macdHistogram:t.momentum?.macd?.histogram,
      stochastic:t.momentum?.stochastic,
      williamsR:t.momentum?.williamsR,
      roc:t.momentum?.roc
    },

    adx:t.dmi?.adx,
    plusDI:t.dmi?.plusDI,
    minusDI:t.dmi?.minusDI,

    atr:t.volatility?.atr,
    volatilityRegime:t.volatility?.regime,

    structure:t.structure,
    choch:t.choch,
    supertrend:t.supertrend,

    ichimoku:{
      bias:t.ichimoku?.bias,
      tenkan:t.ichimoku?.tenkan,
      kijun:t.ichimoku?.kijun
    },

    liquidity:t.liquidity,
    fvg:t.fvg,
    supportResistance:t.supportResistance,
    volume:t.volume,
    vwap:t.vwap,
    mfi:t.mfi
  };
}

function localCandidate(symbol,t){
  if(!t||t.bias==='NEUTRAL'){
    return null;
  }

  const edge=Math.abs(
    n(t.score?.bullish)-
    n(t.score?.bearish)
  );

  const confirms=
    t.bias==='BULL'
      ?[
        t.trend?.alignment==='BULL',
        t.supertrend?.direction==='BULL',
        t.ichimoku?.bias==='BULL',
        t.structure?.structure==='BULLISH',
        t.choch?.bullish
      ].filter(Boolean).length
      :[
        t.trend?.alignment==='BEAR',
        t.supertrend?.direction==='BEAR',
        t.ichimoku?.bias==='BEAR',
        t.structure?.structure==='BEARISH',
        t.choch?.bearish
      ].filter(Boolean).length;

  if(
    edge<3||
    confirms<1||
    !Number.isFinite(t.volatility?.atr)||
    t.volatility.atr<=0
  ){
    return null;
  }

  return{
    symbol,
    technical:t,
    edge,
    rank:
      edge+
      confirms+
      (n(t.dmi?.adx)>=20?1:0)
  };
}

const accountSchema=new mongoose.Schema({
  accountKey:{
    type:String,
    unique:true,
    required:true
  },
  balance:{
    type:Number,
    required:true
  },
  startingBalance:{
    type:Number,
    required:true
  },
  version:String,
  mode:String,
  telegramChatId:String,
  apiUsageDay:String,
  twelveCreditsUsed:{
    type:Number,
    default:0
  },
  geminiEntryUsed:{
    type:Number,
    default:0
  },
  geminiManageUsed:{
    type:Number,
    default:0
  }
},{
  timestamps:true
});

const tradeSchema=new mongoose.Schema({
  tradeId:{
    type:String,
    unique:true,
    required:true
  },
  symbol:String,
  direction:String,
  status:String,
  entryPrice:Number,
  stopLoss:Number,
  initialStopLoss:Number,
  partialTargetPrice:Number,
  quantity:Number,
  initialQuantity:Number,
  riskAmount:Number,
  riskPct:Number,
  confidence:Number,
  entryReason:String,
  managementReason:String,
  openedAt:Date,
  closedAt:Date,
  exitPrice:Number,
  realizedPartialPnl:{
    type:Number,
    default:0
  },
  totalPnl:{
    type:Number,
    default:0
  },
  resultR:Number,
  partialClosed:{
    type:Boolean,
    default:false
  },
  trailingLevelR:{
    type:Number,
    default:0
  },
  breakEvenActivated:{
    type:Boolean,
    default:false
  },
  maxFavorablePrice:Number,
  maxAdversePrice:Number,
  mfeR:Number,
  maeR:Number,
  aiEntryDecision:
    mongoose.Schema.Types.Mixed,
  technicalSnapshot:
    mongoose.Schema.Types.Mixed
},{
  timestamps:true
});

const journalSchema=new mongoose.Schema({
  type:String,
  symbol:String,
  tradeId:String,
  message:String,
  data:mongoose.Schema.Types.Mixed,
  createdAt:{
    type:Date,
    default:Date.now
  }
},{
  collection:'lomyforexjournalv151'
});

const Account=
  mongoose.models.LomyForexAccountV15||
  mongoose.model(
    'LomyForexAccountV15',
    accountSchema
  );

const Trade=
  mongoose.models.LomyForexTradeV15||
  mongoose.model(
    'LomyForexTradeV15',
    tradeSchema
  );

const Journal=
  mongoose.models.LomyForexJournalV151||
  mongoose.model(
    'LomyForexJournalV151',
    journalSchema
  );

async function saveAccount(){
  if(account){
    await account.save();
  }
}

async function journal(type,data={}){
  try{
    if(state.mongoReady){
      await Journal.create({
        type,
        symbol:data.symbol||null,
        tradeId:data.tradeId||null,
        message:data.message||'',
        data
      });
    }
  }catch(e){
    console.error(
      '[JOURNAL]',
      safeError(e)
    );
  }
}

async function ensureUsageDay(){
  if(!account)return;

  const d=utcDay();

  if(account.apiUsageDay!==d){
    account.apiUsageDay=d;
    account.twelveCreditsUsed=0;
    account.geminiEntryUsed=0;
    account.geminiManageUsed=0;
    await saveAccount();
  }
}

async function initMongo(){
  if(!MONGODB_URI){
    throw new Error(
      'MONGODB_URI is required'
    );
  }

  await mongoose.connect(
    MONGODB_URI
  );

  state.mongoReady=true;

  account=await Account.findOne({
    accountKey:PAPER.accountKey
  });

  if(!account){
    account=await Account.create({
      accountKey:PAPER.accountKey,
      balance:PAPER.startingBalance,
      startingBalance:PAPER.startingBalance,
      version:VERSION,
      mode:MODE,
      telegramChatId:'',
      apiUsageDay:utcDay(),
      twelveCreditsUsed:0,
      geminiEntryUsed:0,
      geminiManageUsed:0
    });
  }

  account.version=VERSION;
  account.mode=MODE;

  await ensureUsageDay();
  await saveAccount();

  if(!telegramChatId){
    telegramChatId=String(
      account.telegramChatId||''
    );
  }

  console.log(
    `[MONGO] connected | balance ${fmtMoney(account.balance)}`
  );
}

async function restoreOpenTrades(){
  const rows=await Trade.find({
    status:'OPEN'
  }).lean();

  state.openTrades.clear();

  for(const r of rows){
    const t={
      ...r,
      initialQuantity:n(
        r.initialQuantity,
        n(r.quantity)
      ),
      realizedPartialPnl:n(
        r.realizedPartialPnl
      ),
      trailingLevelR:n(
        r.trailingLevelR
      ),
      partialClosed:
        r.partialClosed===true,
      breakEvenActivated:
        r.breakEvenActivated===true
    };

    state.openTrades.set(
      t.symbol,
      t
    );
  }

  console.log(
    `[TRADES] restored ${state.openTrades.size}`
  );
}

function toTwelveSymbol(s){
  return s==='XAUUSD'
    ?'XAU/USD'
    :s.length===6
      ?s.slice(0,3)+'/'+s.slice(3)
      :s;
}

function quotaBlockFromMessage(msg){
  const x=String(msg||'').toLowerCase();

  if(
    x.includes('day')||
    x.includes('daily')
  ){
    return nextUtcMidnight();
  }

  if(x.includes('minute')){
    return(
      Math.floor(Date.now()/60000)*
      60000+
      62000
    );
  }

  return 0;
}

async function queueTwelve(task){
  const run=twelveChain.then(async()=>{
    await ensureUsageDay();

    if(Date.now()<state.twelveBlockedUntil){
      throw new Error(
        `Twelve Data paused until ${new Date(state.twelveBlockedUntil).toISOString()}`
      );
    }

    if(
      n(account?.twelveCreditsUsed)>=
      TWELVE_DAILY_SOFT_CAP
    ){
      state.twelveBlockedUntil=
        nextUtcMidnight();

      throw new Error(
        'Twelve Data local daily soft cap reached'
      );
    }

    const wait=
      TWELVE_MIN_GAP_MS-
      (Date.now()-lastTwelveAt);

    if(wait>0){
      await sleep(wait);
    }

    lastTwelveAt=Date.now();

    if(account){
      account.twelveCreditsUsed=
        n(account.twelveCreditsUsed)+1;

      await saveAccount();
    }

    try{
      return await task();
    }catch(e){
      const b=quotaBlockFromMessage(
        safeError(e)
      );

      if(b){
        state.twelveBlockedUntil=b;
      }

      throw e;
    }
  });

  twelveChain=run.catch(()=>{});
  return run;
}

function assertTwelve(data){
  if(!data||data.status==='error'){
    throw new Error(
      data?.message||
      data?.code||
      'Twelve Data API error'
    );
  }
}

async function fetchBars(
  symbol,
  outputSize=REFRESH_HISTORY
){
  return queueTwelve(async()=>{
    const r=await http.get(
      `${TWELVE_BASE}/time_series`,
      {
        params:{
          symbol:toTwelveSymbol(symbol),
          interval:'15min',
          outputsize:outputSize,
          order:'asc',
          timezone:'UTC',
          apikey:TWELVE_DATA_API_KEY
        }
      }
    );

    assertTwelve(r.data);

    if(!Array.isArray(r.data.values)){
      throw new Error(
        `No OHLC values for ${symbol}`
      );
    }

    return normalizeBars(
      r.data.values.map(x=>({
        openTime:x.datetime,
        open:x.open,
        high:x.high,
        low:x.low,
        close:x.close,
        volume:x.volume,
        isOpen:false
      }))
    );
  });
}

async function fetchPrice(symbol){
  return queueTwelve(async()=>{
    const r=await http.get(
      `${TWELVE_BASE}/price`,
      {
        params:{
          symbol:toTwelveSymbol(symbol),
          apikey:TWELVE_DATA_API_KEY
        }
      }
    );

    assertTwelve(r.data);

    const p=n(
      r.data.price,
      NaN
    );

    if(!Number.isFinite(p)||p<=0){
      throw new Error(
        `Invalid price for ${symbol}`
      );
    }

    return p;
  });
}

async function fetchQuote(symbol){
  return queueTwelve(async()=>{
    const r=await http.get(
      `${TWELVE_BASE}/quote`,
      {
        params:{
          symbol:toTwelveSymbol(symbol),
          apikey:TWELVE_DATA_API_KEY
        }
      }
    );

    assertTwelve(r.data);

    const close=n(
      r.data.close,
      NaN
    );

    const bid=n(
      r.data.bid,
      NaN
    );

    const ask=n(
      r.data.ask,
      NaN
    );

    const cached=
      last(
        state.pairState.get(symbol)?.bars15m
      )?.close;

    const base=
      Number.isFinite(close)
        ?close
        :n(cached,NaN);

    if(!Number.isFinite(base)){
      throw new Error(
        `Invalid quote for ${symbol}`
      );
    }

    const spreadKnown=
      Number.isFinite(bid)&&
      Number.isFinite(ask)&&
      bid>0&&
      ask>0;

    return{
      symbol,
      bid:spreadKnown?bid:base,
      ask:spreadKnown?ask:base,
      mid:spreadKnown
        ?(bid+ask)/2
        :base,
      spread:spreadKnown
        ?Math.max(0,ask-bid)
        :0,
      spreadKnown,
      fetchedAt:new Date()
    };
  });
}

function barQuote(symbol,price){
  return{
    symbol,
    bid:price,
    ask:price,
    mid:price,
    spread:0,
    spreadKnown:false,
    fetchedAt:new Date()
  };
}

function entryExecutionPrice(d,q){
  return d==='BUY'
    ?q.ask
    :q.bid;
}

function exitExecutionPrice(d,q){
  return d==='BUY'
    ?q.bid
    :q.ask;
}

async function initializeSymbol(symbol){
  const closed=closed15(
    await fetchBars(
      symbol,
      INITIAL_HISTORY
    )
  );

  if(closed.length<CORE_MIN_HISTORY){
    throw new Error(
      `${symbol}: insufficient history`
    );
  }

  const pair={
    symbol,
    bars15m:closed.slice(-INITIAL_HISTORY),
    bars1h:aggregateBars(closed,60),
    bars4h:aggregateBars(closed,240),
    lastClosedBarTime:last(closed).openTime,
    lastRefreshAt:new Date(),
    lastError:null
  };

  state.pairState.set(
    symbol,
    pair
  );

  console.log(
    `[MARKET] ${symbol} ready`
  );

  return pair;
}

async function initializeMarket(){
  if(!TWELVE_DATA_API_KEY){
    state.lastMarketError=
      'TWELVE_DATA_API_KEY missing';

    return false;
  }

  console.log(
    `[MARKET] initializing ${ACTIVE_SYMBOLS.length} active symbols...`
  );

  let ok=0;

  for(const s of ACTIVE_SYMBOLS){
    try{
      await initializeSymbol(s);
      ok++;
    }catch(e){
      state.lastMarketError=
        safeError(e);

      console.error(
        `[MARKET] ${s}:`,
        safeError(e)
      );

      if(
        Date.now()<
        state.twelveBlockedUntil
      ){
        break;
      }
    }
  }

  state.marketReady=ok>0;

  console.log(
    `[MARKET] ${ok}/${ACTIVE_SYMBOLS.length} ready`
  );

  return state.marketReady;
}

async function refreshSymbol(symbol){
  let pair=
    state.pairState.get(symbol);

  if(!pair){
    try{
      pair=await initializeSymbol(symbol);
    }catch(e){
      return null;
    }
  }

  try{
    const closed=closed15(
      await fetchBars(
        symbol,
        REFRESH_HISTORY
      )
    );

    if(!closed.length){
      return null;
    }

    pair.bars15m=mergeBars(
      pair.bars15m,
      closed
    );

    pair.bars1h=
      aggregateBars(
        pair.bars15m,
        60
      );

    pair.bars4h=
      aggregateBars(
        pair.bars15m,
        240
      );

    pair.lastRefreshAt=
      new Date();

    pair.lastError=null;

    const latest=
      last(pair.bars15m);

    const prev=
      parseTime(
        pair.lastClosedBarTime
      );

    const cur=
      barTimeMs(latest);

    return cur>prev
      ?{pair,latest}
      :null;

  }catch(e){
    pair.lastError=
      safeError(e);

    state.lastMarketError=
      safeError(e);

    console.error(
      `[REFRESH] ${symbol}:`,
      safeError(e)
    );

    return null;
  }
}

function sessionContext(){
  const h=
    new Date().getUTCHours();

  const s=[];

  if(h<9)s.push('ASIA');
  if(h>=7&&h<16)s.push('LONDON');
  if(h>=12&&h<21)s.push('NEW_YORK');

  return{
    utcHour:h,
    sessions:s.length
      ?s
      :['OFF_HOURS'],
    londonNewYorkOverlap:
      h>=12&&h<16
  };
}

function symbolCurrencies(s){
  return s==='XAUUSD'
    ?['XAU','USD']
    :[
      s.slice(0,3),
      s.slice(3,6)
    ];
}

function normalizeImpact(v){
  const x=
    String(v||'').toLowerCase();

  if(
    x.includes('high')||
    x.includes('red')
  ){
    return'HIGH';
  }

  if(
    x.includes('medium')||
    x.includes('orange')
  ){
    return'MEDIUM';
  }

  return'LOW';
}

async function fetchEconomicNews(){
  try{
    const r=await http.get(
      'https://nfs.faireconomy.media/ff_calendar_thisweek.json',
      {
        timeout:15000,
        headers:{
          Accept:'application/json'
        }
      }
    );

    const rows=
      Array.isArray(r.data)
        ?r.data
        :[];

    economicNews=rows
      .map(x=>({
        title:String(
          x.title||
          x.event||
          ''
        ),
        country:String(
          x.country||
          ''
        ).trim().toUpperCase(),
        impact:normalizeImpact(
          x.impact
        ),
        time:new Date(
          x.date||
          x.datetime||
          x.time||
          x.timestamp
        )
      }))
      .filter(x=>
        x.title&&
        Number.isFinite(
          x.time.getTime()
        )
      );

    state.newsReady=true;
    state.lastNewsError=null;

    console.log(
      `[NEWS] loaded ${economicNews.length}`
    );

  }catch(e){
    state.newsReady=false;
    state.lastNewsError=
      safeError(e);

    console.error(
      '[NEWS]',
      safeError(e)
    );
  }
}

function newsBlock(symbol){
  if(!state.newsReady){
    return{
      blocked:false,
      available:false,
      events:[]
    };
  }

  const c=
    symbolCurrencies(symbol);

  const now=Date.now();
  const w=NEWS_BLOCK_MIN*60000;

  const events=
    economicNews
      .filter(x=>
        x.impact==='HIGH'&&
        c.includes(x.country)&&
        Math.abs(
          x.time.getTime()-now
        )<=w
      )
      .slice(0,5);

  return{
    blocked:events.length>0,
    available:true,
    events
  };
}

function calculateCurrencyStrength(){
  const values=new Map();
  const counts=new Map();

  for(const [s,p] of state.pairState){
    if(
      s==='XAUUSD'||
      !p?.bars15m?.length
    ){
      continue;
    }

    const b=p.bars15m;

    if(b.length<13){
      continue;
    }

    const ch=pct(
      b[b.length-13].close,
      last(b).close
    );

    if(!Number.isFinite(ch)){
      continue;
    }

    const a=s.slice(0,3);
    const q=s.slice(3,6);

    values.set(
      a,
      n(values.get(a))+ch
    );

    counts.set(
      a,
      n(counts.get(a))+1
    );

    values.set(
      q,
      n(values.get(q))-ch
    );

    counts.set(
      q,
      n(counts.get(q))+1
    );
  }

  const out={};

  for(const [k,v] of values){
    out[k]=
      v/
      Math.max(
        1,
        n(counts.get(k),1)
      );
  }

  return out;
}

function pairStrengthContext(s){
  const m=
    calculateCurrencyStrength();

  const a=s.slice(0,3);
  const q=s.slice(3,6);

  return{
    base:a,
    quote:q,
    baseStrength:
      a==='XAU'
        ?null
        :n(m[a]),
    quoteStrength:n(m[q]),
    differential:
      a==='XAU'
        ?null
        :n(m[a])-n(m[q])
  };
}

function buildMtf(pair){
  return{
    m15:
      buildTechnicalIntelligence(
        pair.bars15m
      ),

    h1:
      pair.bars1h.length>=
      CORE_MIN_HISTORY
        ?buildTechnicalIntelligence(
          pair.bars1h
        )
        :null,

    h4:
      pair.bars4h.length>=
      CORE_MIN_HISTORY
        ?buildTechnicalIntelligence(
          pair.bars4h
        )
        :null
  };
}

function isGeminiQuotaError(msg){
  const x=
    String(msg||'').toLowerCase();

  return(
    x.includes('quota')||
    x.includes('rate limit')||
    x.includes('resource_exhausted')
  );
}

async function queueGemini(task,kind){
  const run=
    geminiChain.then(async()=>{

      await ensureUsageDay();

      if(
        Date.now()<
        state.geminiBlockedUntil
      ){
        throw new Error(
          `Gemini paused until ${new Date(state.geminiBlockedUntil).toISOString()}`
        );
      }

      const used=
        kind==='entry'
          ?n(account?.geminiEntryUsed)
          :n(account?.geminiManageUsed);

      const cap=
        kind==='entry'
          ?GEMINI_ENTRY_DAILY_CAP
          :GEMINI_MANAGE_DAILY_CAP;

      if(used>=cap){
        throw new Error(
          `Gemini ${kind} local daily cap reached`
        );
      }

      const wait=
        GEMINI_CALL_GAP_MS-
        (Date.now()-lastGeminiAt);

      if(wait>0){
        await sleep(wait);
      }

      lastGeminiAt=
        Date.now();

      if(account){
        if(kind==='entry'){
          account.geminiEntryUsed=
            n(account.geminiEntryUsed)+1;
        }else{
          account.geminiManageUsed=
            n(account.geminiManageUsed)+1;
        }

        await saveAccount();
      }

      try{
        return await task();
      }catch(e){
        const msg=safeError(e);

        if(isGeminiQuotaError(msg)){
          state.geminiBlockedUntil=
            nextUtcMidnight();
        }

        throw e;
      }
    });

  geminiChain=
    run.catch(()=>{});

  return run;
}

function extractJson(text){
  let s=String(text||'')
    .trim()
    .replace(/^```json/i,'')
    .replace(/^```/,'')
    .replace(/```$/,'')
    .trim();

  try{
    return JSON.parse(s);
  }catch(_){
    const a=s.indexOf('{');
    const b=s.lastIndexOf('}');

    if(a<0||b<=a){
      throw new Error(
        'No JSON object in Gemini response'
      );
    }

    return JSON.parse(
      s.slice(a,b+1)
    );
  }
}

async function callGemini(prompt,kind){
  if(!GEMINI_API_KEY){
    throw new Error(
      'GEMINI_API_KEY is missing'
    );
  }

  return queueGemini(
    async()=>{

      try{
        const r=await http.post(
          `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(GEMINI_MODEL)}:generateContent`,
          {
            contents:[
              {
                role:'user',
                parts:[
                  {text:prompt}
                ]
              }
            ],
            generationConfig:{
              temperature:AI.temperature,
              responseMimeType:
                'application/json',
              maxOutputTokens:500
            }
          },
          {
            headers:{
              'x-goog-api-key':
                GEMINI_API_KEY
            },
            timeout:AI.timeoutMs
          }
        );

        const text=
          r?.data
            ?.candidates?.[0]
            ?.content?.parts
            ?.map(x=>x.text||'')
            .join('');

        if(!text){
          throw new Error(
            'Gemini returned empty response'
          );
        }

        state.geminiReady=true;
        state.lastAiError=null;

        return extractJson(text);

      }catch(e){
        state.geminiReady=false;
        state.lastAiError=
          safeError(e);

        throw e;
      }

    },
    kind
  );
}

async function getAiMemory(symbol){
  if(!state.mongoReady){
    return[];
  }

  const rows=
    await Trade.find({
      status:'CLOSED'
    })
    .sort({
      closedAt:-1
    })
    .limit(
      AI.memoryClosedTrades
    )
    .lean();

  return rows.map(t=>({
    symbol:t.symbol,
    sameSymbol:
      t.symbol===symbol,
    direction:t.direction,
    confidence:t.confidence,
    resultR:n(t.resultR),
    pnl:n(t.totalPnl),
    mfeR:n(t.mfeR),
    maeR:n(t.maeR),
    entryReason:
      String(
        t.entryReason||''
      ).slice(0,250),
    managementReason:
      String(
        t.managementReason||''
      ).slice(0,250)
  }));
}

async function askEntryCommander({
  symbol,
  mtf,
  session,
  strength,
  news,
  memory
}){
  state.aiEntryCalls++;

  const prompt=`
You are LOMY FOREX V1.5.1 entry commander.
PAPER ONLY.

Decide BUY, SELL, or NO_TRADE using only supplied data.

If BUY or SELL:
- confidence must be 62-100.
- stopLoss must be a technical price.
- stop must be based on structure, volatility, support/resistance, swing, or liquidity.
- do not size positions.

If evidence is weak or conflicting, return NO_TRADE.

Return JSON only:
{
  "decision":"BUY|SELL|NO_TRADE",
  "confidence":0,
  "stopLoss":null,
  "reason":"short precise reason"
}

DATA:
${JSON.stringify({
  symbol,
  session,
  strength,
  news,
  mtf:{
    m15:compactTechnical(mtf.m15),
    h1:compactTechnical(mtf.h1),
    h4:compactTechnical(mtf.h4)
  },
  recentClosedTrades:memory
})}
`;

  const r=
    await callGemini(
      prompt,
      'entry'
    );

  const decision=
    String(
      r?.decision||
      'NO_TRADE'
    ).toUpperCase();

  const confidence=
    clamp(
      n(r?.confidence),
      0,
      100
    );

  const stopLoss=
    n(
      r?.stopLoss,
      NaN
    );

  const reason=
    String(
      r?.reason||
      ''
    ).slice(0,800);

  if(
    ![
      'BUY',
      'SELL',
      'NO_TRADE'
    ].includes(decision)||
    decision==='NO_TRADE'||
    confidence<
      RULES.minEntryConfidence||
    !Number.isFinite(stopLoss)
  ){
    state.aiNoTradeDecisions++;

    return{
      decision:'NO_TRADE',
      confidence,
      stopLoss:NaN,
      reason:
        reason||
        'Rejected Gemini entry'
    };
  }

  if(decision==='BUY'){
    state.aiBuyDecisions++;
  }else{
    state.aiSellDecisions++;
  }

  return{
    decision,
    confidence,
    stopLoss,
    reason
  };
}

async function askTradeManager({
  trade,
  technical,
  memory,
  currentPrice
}){
  state.aiManageCalls++;

  const prompt=`
Manage this existing PAPER forex trade.

You may ONLY return HOLD or CLOSE.

Do not change:
- stop loss
- take profit
- trailing stop
- position size
- partial rules

CLOSE only if the original trade thesis is materially invalidated.

Return JSON only:
{
  "decision":"HOLD|CLOSE",
  "confidence":0,
  "reason":"short precise reason"
}

DATA:
${JSON.stringify({
  symbol:trade.symbol,
  direction:trade.direction,
  entryPrice:trade.entryPrice,
  currentPrice,
  stopLoss:trade.stopLoss,
  currentR:
    tradePriceR(
      trade,
      currentPrice
    ),
  partialClosed:
    trade.partialClosed,
  trailingLevelR:
    trade.trailingLevelR,
  entryReason:
    trade.entryReason,
  technical:
    compactTechnical(
      technical
    ),
  recentMemory:memory
})}
`;

  const r=
    await callGemini(
      prompt,
      'manage'
    );

  const d=
    String(
      r?.decision||
      'HOLD'
    ).toUpperCase();

  const c=
    clamp(
      n(r?.confidence),
      0,
      100
    );

  const reason=
    String(
      r?.reason||
      ''
    ).slice(0,800);

  if(
    d==='CLOSE'&&
    c>=RULES.minCloseConfidence
  ){
    state.aiCloseDecisions++;

    return{
      decision:'CLOSE',
      confidence:c,
      reason
    };
  }

  state.aiHoldDecisions++;

  return{
    decision:'HOLD',
    confidence:c,
    reason
  };
}

function riskPctFromConfidence(c){
  if(c>=DYNAMIC_RISK.highConfidence){
    return DYNAMIC_RISK.highRiskPct;
  }

  if(c>=DYNAMIC_RISK.medConfidence){
    return DYNAMIC_RISK.medRiskPct;
  }

  if(c>=DYNAMIC_RISK.lowConfidence){
    return DYNAMIC_RISK.lowRiskPct;
  }

  return 0;
}

function quoteCurrency(s){
  return s==='XAUUSD'
    ?'USD'
    :s.slice(3,6);
}

function cachedPairPrice(s){
  return n(
    last(
      state.pairState.get(s)?.bars15m
    )?.close,
    NaN
  );
}

async function currencyToUsdRate(currency){
  currency=
    String(currency||'')
      .toUpperCase();

  if(
    currency==='USD'||
    !currency
  ){
    return 1;
  }

  const d=`${currency}USD`;
  const i=`USD${currency}`;

  const dp=
    cachedPairPrice(d);

  const ip=
    cachedPairPrice(i);

  if(
    Number.isFinite(dp)&&
    dp>0
  ){
    return dp;
  }

  if(
    Number.isFinite(ip)&&
    ip>0
  ){
    return 1/ip;
  }

  try{
    return await fetchPrice(d);
  }catch(_){
    const p=
      await fetchPrice(i);

    return 1/p;
  }
}

async function calculatePositionSize(
  symbol,
  entry,
  stop,
  riskAmount
){
  const dist=
    Math.abs(entry-stop);

  if(
    !Number.isFinite(dist)||
    dist<=0
  ){
    throw new Error(
      'Invalid stop distance'
    );
  }

  const rate=
    await currencyToUsdRate(
      quoteCurrency(symbol)
    );

  const qty=
    riskAmount/
    (dist*rate);

  if(
    !Number.isFinite(qty)||
    qty<=0
  ){
    throw new Error(
      'Invalid quantity'
    );
  }

  return{
    quantity:qty,
    stopDistance:dist,
    quoteToUsd:rate
  };
}

async function calculatePnlUsd(
  symbol,
  direction,
  entry,
  exit,
  qty
){
  const raw=
    (
      direction==='BUY'
        ?exit-entry
        :entry-exit
    )*qty;

  return raw*
    await currencyToUsdRate(
      quoteCurrency(symbol)
    );
}

function currentPortfolioRiskUsd(){
  let total=0;

  for(const t of state.openTrades.values()){
    const iq=
      Math.max(
        Number.EPSILON,
        n(
          t.initialQuantity,
          n(t.quantity)
        )
      );

    const rf=
      clamp(
        n(t.quantity)/iq,
        0,
        1
      );

    total+=
      Math.max(
        0,
        n(t.riskAmount)
      )*rf;
  }

  return total;
}

function portfolioRiskCapUsd(){
  return(
    n(
      account?.balance,
      PAPER.startingBalance
    )*
    PAPER.portfolioRiskCapPct/
    100
  );
}

async function validateEntryRisk({
  symbol,
  direction,
  confidence,
  technicalStop,
  quote,
  technical
}){
  if(!account){
    return{
      approved:false,
      reason:'Account unavailable'
    };
  }

  if(
    state.openTrades.size>=
    PAPER.maxOpenTrades
  ){
    return{
      approved:false,
      reason:'Max open trades'
    };
  }

  if(state.openTrades.has(symbol)){
    return{
      approved:false,
      reason:'Symbol already open'
    };
  }

  const entry=
    entryExecutionPrice(
      direction,
      quote
    );

  const stop=
    n(
      technicalStop,
      NaN
    );

  const atr=
    n(
      technical?.volatility?.atr,
      NaN
    );

  if(
    !Number.isFinite(entry)||
    !Number.isFinite(stop)||
    !Number.isFinite(atr)||
    atr<=0
  ){
    return{
      approved:false,
      reason:'Invalid entry/stop/ATR'
    };
  }

  if(
    direction==='BUY'&&
    stop>=entry
  ){
    return{
      approved:false,
      reason:'BUY stop must be below entry'
    };
  }

  if(
    direction==='SELL'&&
    stop<=entry
  ){
    return{
      approved:false,
      reason:'SELL stop must be above entry'
    };
  }

  const dist=
    Math.abs(entry-stop);

  const stopAtr=
    dist/atr;

  if(
    stopAtr<RULES.minStopAtr||
    stopAtr>RULES.maxStopAtr
  ){
    return{
      approved:false,
      reason:
        `Stop ${stopAtr.toFixed(2)} ATR outside limits`
    };
  }

  if(
    quote?.spreadKnown&&
    n(quote.spread)>
      dist*
      RULES.maxSpreadRiskFraction
  ){
    return{
      approved:false,
      reason:
        'Spread too large versus stop'
    };
  }

  const riskPct=
    riskPctFromConfidence(
      confidence
    );

  const riskAmount=
    n(account.balance)*
    riskPct/
    100;

  if(
    riskPct<=0||
    riskPct>
      PAPER.maxCapitalRiskPct
  ){
    return{
      approved:false,
      reason:'Invalid risk pct'
    };
  }

  if(
    currentPortfolioRiskUsd()+
    riskAmount>
    portfolioRiskCapUsd()+
    1e-9
  ){
    return{
      approved:false,
      reason:'Portfolio risk cap'
    };
  }

  const s=
    await calculatePositionSize(
      symbol,
      entry,
      stop,
      riskAmount
    );

  return{
    approved:true,
    entryPrice:entry,
    stopLoss:stop,
    riskDistance:dist,
    riskPct,
    riskAmount,
    quantity:s.quantity,
    initialQuantity:s.quantity,
    partialTargetPrice:
      direction==='BUY'
        ?entry+
         dist*
         RULES.partialTpTriggerR
        :entry-
         dist*
         RULES.partialTpTriggerR
  };
}

async function openPaperTrade({
  symbol,
  direction,
  confidence,
  reason,
  aiDecision,
  technical,
  risk
}){
  if(
    MODE!=='PAPER'||
    LIVE_TRADING
  ){
    throw new Error(
      'Live trading forbidden'
    );
  }

  const trade={
    tradeId:
      `${symbol}-${Date.now()}-${Math.random().toString(36).slice(2,8)}`,

    symbol,
    direction,
    status:'OPEN',

    entryPrice:
      risk.entryPrice,

    stopLoss:
      risk.stopLoss,

    initialStopLoss:
      risk.stopLoss,

    partialTargetPrice:
      risk.partialTargetPrice,

    quantity:
      risk.quantity,

    initialQuantity:
      risk.initialQuantity,

    riskAmount:
      risk.riskAmount,

    riskPct:
      risk.riskPct,

    confidence,

    entryReason:reason,

    managementReason:'',

    openedAt:new Date(),

    realizedPartialPnl:0,
    totalPnl:0,
    partialClosed:false,
    trailingLevelR:0,
    breakEvenActivated:false,

    maxFavorablePrice:
      risk.entryPrice,

    maxAdversePrice:
      risk.entryPrice,

    mfeR:0,
    maeR:0,

    aiEntryDecision:
      aiDecision,

    technicalSnapshot:
      compactTechnical(
        technical
      )
  };

  const doc=
    await Trade.create(trade);

  const t=
    doc.toObject();

  state.openTrades.set(
    symbol,
    t
  );

  state.executedSignals++;

  await journal(
    'TRADE_OPENED',
    {
      symbol,
      tradeId:t.tradeId,
      direction,
      confidence,
      riskPct:t.riskPct,
      riskAmount:t.riskAmount,
      message:reason
    }
  );

  await sendTelegram(
`LOMY PAPER TRADE OPENED
${symbol} ${direction}
Entry: ${fmtPrice(t.entryPrice,symbol)}
SL: ${fmtPrice(t.stopLoss,symbol)}
+2R: ${fmtPrice(t.partialTargetPrice,symbol)}
Risk: ${t.riskPct.toFixed(2)}% (${fmtMoney(t.riskAmount)})
Confidence: ${confidence.toFixed(0)}%`
  );

  return t;
}

function tradePriceR(t,p){
  const d=
    Math.abs(
      t.entryPrice-
      t.initialStopLoss
    );

  if(d<=0)return 0;

  return t.direction==='BUY'
    ?(p-t.entryPrice)/d
    :(t.entryPrice-p)/d;
}

function stopPriceAtR(t,r){
  const d=
    Math.abs(
      t.entryPrice-
      t.initialStopLoss
    );

  return t.direction==='BUY'
    ?t.entryPrice+d*r
    :t.entryPrice-d*r;
}

function improveStop(t,c){
  if(!Number.isFinite(c)){
    return false;
  }

  if(
    t.direction==='BUY'&&
    c>t.stopLoss
  ){
    t.stopLoss=c;
    return true;
  }

  if(
    t.direction==='SELL'&&
    c<t.stopLoss
  ){
    t.stopLoss=c;
    return true;
  }

  return false;
}

async function saveTrade(t){
  await Trade.updateOne(
    {
      tradeId:t.tradeId
    },
    {
      $set:{
        status:t.status,
        quantity:t.quantity,
        initialQuantity:t.initialQuantity,
        stopLoss:t.stopLoss,
        partialClosed:t.partialClosed,
        trailingLevelR:t.trailingLevelR,
        breakEvenActivated:t.breakEvenActivated,
        realizedPartialPnl:t.realizedPartialPnl,
        totalPnl:t.totalPnl,
        managementReason:t.managementReason,
        maxFavorablePrice:t.maxFavorablePrice,
        maxAdversePrice:t.maxAdversePrice,
        mfeR:t.mfeR,
        maeR:t.maeR,
        exitPrice:t.exitPrice,
        closedAt:t.closedAt,
        resultR:t.resultR
      }
    }
  );
}

function updateExcursionsFromBar(t,b){
  if(t.direction==='BUY'){
    t.maxFavorablePrice=
      Math.max(
        n(
          t.maxFavorablePrice,
          t.entryPrice
        ),
        b.high
      );

    t.maxAdversePrice=
      Math.min(
        n(
          t.maxAdversePrice,
          t.entryPrice
        ),
        b.low
      );

  }else{
    t.maxFavorablePrice=
      Math.min(
        n(
          t.maxFavorablePrice,
          t.entryPrice
        ),
        b.low
      );

    t.maxAdversePrice=
      Math.max(
        n(
          t.maxAdversePrice,
          t.entryPrice
        ),
        b.high
      );
  }

  t.mfeR=
    Math.max(
      n(t.mfeR),
      tradePriceR(
        t,
        t.maxFavorablePrice
      )
    );

  t.maeR=
    Math.min(
      n(t.maeR),
      tradePriceR(
        t,
        t.maxAdversePrice
      )
    );
}

async function closeTradeAtPrice(
  t,
  price,
  reason
){
  if(
    !t||
    t.status!=='OPEN'
  ){
    return null;
  }

  const qty=
    Math.max(
      0,
      n(t.quantity)
    );

  const remaining=
    qty
      ?await calculatePnlUsd(
        t.symbol,
        t.direction,
        t.entryPrice,
        price,
        qty
      )
      :0;

  const total=
    n(t.realizedPartialPnl)+
    remaining;

  account.balance=
    n(account.balance)+
    remaining;

  await saveAccount();

  t.status='CLOSED';
  t.exitPrice=price;
  t.closedAt=new Date();
  t.quantity=0;
  t.totalPnl=total;

  t.managementReason=
    String(reason).slice(
      0,
      800
    );

  t.resultR=
    total/
    Math.max(
      Number.EPSILON,
      n(t.riskAmount)
    );

  await saveTrade(t);

  state.openTrades.delete(
    t.symbol
  );

  await journal(
    'TRADE_CLOSED',
    {
      symbol:t.symbol,
      tradeId:t.tradeId,
      totalPnl:total,
      resultR:t.resultR,
      message:reason
    }
  );

  await sendTelegram(
`LOMY PAPER TRADE CLOSED
${t.symbol} ${t.direction}
Exit: ${fmtPrice(price,t.symbol)}
PnL: ${fmtMoney(total)}
Result: ${t.resultR.toFixed(2)}R
Balance: ${fmtMoney(account.balance)}
Reason: ${reason}`
  );

  return t;
}

async function partialCloseAtPrice(t,price){
  const q=
    Math.min(
      n(t.initialQuantity)*.5,
      n(t.quantity)
    );

  if(q<=0)return;

  const pnl=
    await calculatePnlUsd(
      t.symbol,
      t.direction,
      t.entryPrice,
      price,
      q
    );

  t.quantity=
    Math.max(
      0,
      n(t.quantity)-q
    );

  t.realizedPartialPnl=
    n(t.realizedPartialPnl)+
    pnl;

  account.balance=
    n(account.balance)+
    pnl;

  await saveAccount();

  t.partialClosed=true;

  improveStop(
    t,
    stopPriceAtR(
      t,
      RULES.trailingStartStopR
    )
  );

  t.trailingLevelR=
    RULES.trailingStartStopR;

  await saveTrade(t);

  await journal(
    'PARTIAL_CLOSE',
    {
      symbol:t.symbol,
      tradeId:t.tradeId,
      partialPnl:pnl,
      message:'50% closed at +2R'
    }
  );

  await sendTelegram(
`LOMY PARTIAL CLOSE
${t.symbol} ${t.direction}
Closed 50% at +2R
Partial PnL: ${fmtMoney(pnl)}
New SL: ${fmtPrice(t.stopLoss,t.symbol)}`
  );
}

function stopFillFromBar(t,b){
  if(
    t.direction==='BUY'&&
    b.low<=t.stopLoss
  ){
    return b.open<t.stopLoss
      ?b.open
      :t.stopLoss;
  }

  if(
    t.direction==='SELL'&&
    b.high>=t.stopLoss
  ){
    return b.open>t.stopLoss
      ?b.open
      :t.stopLoss;
  }

  return NaN;
}

async function manageMechanicalOnBar(t,b){
  updateExcursionsFromBar(t,b);

  let stopFill=
    stopFillFromBar(t,b);

  if(Number.isFinite(stopFill)){
    await closeTradeAtPrice(
      t,
      stopFill,
      t.partialClosed
        ?'TRAILING_STOP_HIT'
        :t.breakEvenActivated
          ?'BREAK_EVEN_STOP_HIT'
          :'STOP_LOSS_HIT'
    );

    return false;
  }

  const fav=
    t.direction==='BUY'
      ?b.high
      :b.low;

  const currentR=
    tradePriceR(t,fav);

  let changed=false;

  if(
    !t.breakEvenActivated&&
    currentR>=
      RULES.breakEvenTriggerR
  ){
    improveStop(
      t,
      t.entryPrice
    );

    t.breakEvenActivated=true;
    changed=true;
  }

  if(
    !t.partialClosed&&
    currentR>=
      RULES.partialTpTriggerR
  ){
    await partialCloseAtPrice(
      t,
      t.partialTargetPrice
    );

    changed=false;
  }

  if(t.status!=='OPEN'){
    return false;
  }

  if(
    t.partialClosed&&
    currentR>
      RULES.partialTpTriggerR
  ){
    const steps=
      Math.floor(
        (
          currentR-
          RULES.partialTpTriggerR
        )/
        RULES.trailingStepR
      );

    const r=
      RULES.trailingStartStopR+
      steps*
      RULES.trailingStepR;

    if(
      r>n(t.trailingLevelR)&&
      improveStop(
        t,
        stopPriceAtR(t,r)
      )
    ){
      t.trailingLevelR=r;
      changed=true;
    }
  }

  stopFill=
    stopFillFromBar(t,b);

  if(Number.isFinite(stopFill)){
    await closeTradeAtPrice(
      t,
      stopFill,
      t.partialClosed
        ?'TRAILING_STOP_HIT'
        :'BREAK_EVEN_STOP_HIT'
    );

    return false;
  }

  if(changed){
    await saveTrade(t);
  }

  return true;
}

function shouldAskManager(t,tech){
  if(
    !AI.managementEnabled||
    !tech
  ){
    return false;
  }

  const opposite=
    t.direction==='BUY'
      ?tech.bias==='BEAR'
      :tech.bias==='BULL';

  return(
    opposite||
    tradePriceR(
      t,
      tech.price
    )<=-.25
  );
}

async function sendTelegram(msg){
  if(
    !telegramBot||
    !state.telegramReady||
    !telegramChatId
  ){
    return;
  }

  try{
    await telegramBot.telegram.sendMessage(
      telegramChatId,
      msg
    );
  }catch(e){
    console.error(
      '[TELEGRAM SEND]',
      safeError(e)
    );
  }
}

function telegramStatusText(){
  return`${VERSION}
Mode: ${MODE}
Balance: ${account?fmtMoney(account.balance):'n/a'}
Open trades: ${state.openTrades.size}
Market: ${state.marketReady?'READY':'NOT READY'}
Mongo: ${state.mongoReady?'READY':'NOT READY'}
Gemini: ${state.geminiReady?'READY':'NOT READY'}
News: ${state.newsReady?'READY':'NOT READY'}
Scanned bars: ${state.scannedBars}
Executed: ${state.executedSignals}
Skipped: ${state.skippedSignals}`;
}

async function initTelegram(){
  if(!TELEGRAM_BOT_TOKEN){
    console.log(
      '[TELEGRAM] disabled'
    );
    return;
  }

  try{
    telegramBot=
      new Telegraf(
        TELEGRAM_BOT_TOKEN
      );

    telegramBot.start(
      async ctx=>{
        telegramChatId=
          String(ctx.chat.id);

        if(account){
          account.telegramChatId=
            telegramChatId;

          await saveAccount();
        }

        await ctx.reply(
          telegramStatusText()
        );
      }
    );

    telegramBot.command(
      'status',
      ctx=>ctx.reply(
        telegramStatusText()
      )
    );

    telegramBot.command(
      'balance',
      ctx=>ctx.reply(
        account
          ?`Balance: ${fmtMoney(account.balance)}`
          :'Account not ready'
      )
    );

    telegramBot.command(
      'positions',
      ctx=>{
        const x=[
          ...state.openTrades.values()
        ];

        return ctx.reply(
          x.length
            ?x.map(t=>
              `${t.symbol} ${t.direction} | Entry ${fmtPrice(t.entryPrice,t.symbol)} | SL ${fmtPrice(t.stopLoss,t.symbol)}`
            ).join('\n')
            :'No open trades.'
        );
      }
    );

    await telegramBot.telegram.getMe();

    telegramBot.launch()
      .catch(e=>{
        state.telegramReady=false;

        console.error(
          '[TELEGRAM POLLING]',
          safeError(e)
        );
      });

    state.telegramReady=true;

    console.log(
      '[TELEGRAM] ready'
    );

  }catch(e){
    state.telegramReady=false;

    console.error(
      '[TELEGRAM]',
      safeError(e)
    );
  }
}

async function processCandidate(c){
  const{
    symbol,
    technical
  }=c;

  const pair=
    state.pairState.get(symbol);

  if(!pair)return;

  const news=
    newsBlock(symbol);

  if(news.blocked){
    state.skippedSignals++;

    pair.lastClosedBarTime=
      technical.barTime;

    await journal(
      'NEWS_BLOCK',
      {
        symbol,
        message:'High-impact news'
      }
    );

    return;
  }

  if(
    Date.now()-
    state.lastEntryAiAt<
    GEMINI_ENTRY_MIN_GAP_MS
  ){
    state.skippedSignals++;

    pair.lastClosedBarTime=
      technical.barTime;

    return;
  }

  await ensureUsageDay();

  if(
    n(account.geminiEntryUsed)>=
    GEMINI_ENTRY_DAILY_CAP
  ){
    state.skippedSignals++;

    pair.lastClosedBarTime=
      technical.barTime;

    return;
  }

  const mtf=
    buildMtf(pair);

  const memory=
    await getAiMemory(symbol);

  let d;

  try{
    d=await askEntryCommander({
      symbol,
      mtf,
      session:sessionContext(),
      strength:
        pairStrengthContext(symbol),
      news,
      memory
    });

    state.lastEntryAiAt=
      Date.now();

  }catch(e){
    state.skippedSignals++;

    state.lastAiError=
      safeError(e);

    console.error(
      `[AI ENTRY] ${symbol}:`,
      safeError(e)
    );

    pair.lastClosedBarTime=
      technical.barTime;

    return;
  }

  if(d.decision==='NO_TRADE'){
    state.skippedSignals++;

    pair.lastClosedBarTime=
      technical.barTime;

    await journal(
      'NO_TRADE',
      {
        symbol,
        confidence:d.confidence,
        message:d.reason
      }
    );

    return;
  }

  let q=
    barQuote(
      symbol,
      technical.price
    );

  try{
    q=await fetchQuote(symbol);
  }catch(e){
    console.warn(
      `[QUOTE FALLBACK] ${symbol}:`,
      safeError(e)
    );
  }

  let risk;

  try{
    risk=
      await validateEntryRisk({
        symbol,
        direction:d.decision,
        confidence:d.confidence,
        technicalStop:d.stopLoss,
        quote:q,
        technical
      });

  }catch(e){
    risk={
      approved:false,
      reason:safeError(e)
    };
  }

  if(!risk.approved){
    state.skippedSignals++;

    pair.lastClosedBarTime=
      technical.barTime;

    await journal(
      'RISK_REJECT',
      {
        symbol,
        message:risk.reason
      }
    );

    return;
  }

  try{
    await openPaperTrade({
      symbol,
      direction:d.decision,
      confidence:d.confidence,
      reason:d.reason,
      aiDecision:d,
      technical,
      risk
    });

  }catch(e){
    state.skippedSignals++;

    await journal(
      'OPEN_ERROR',
      {
        symbol,
        message:safeError(e)
      }
    );

    console.error(
      `[OPEN] ${symbol}:`,
      safeError(e)
    );
  }

  pair.lastClosedBarTime=
    technical.barTime;
}

async function maybeAiManage(t,tech){
  if(!shouldAskManager(t,tech)){
    return;
  }

  await ensureUsageDay();

  if(
    n(account.geminiManageUsed)>=
    GEMINI_MANAGE_DAILY_CAP
  ){
    return;
  }

  try{
    const d=
      await askTradeManager({
        trade:t,
        technical:tech,
        memory:
          await getAiMemory(
            t.symbol
          ),
        currentPrice:
          tech.price
      });

    if(d.decision==='CLOSE'){
      await closeTradeAtPrice(
        t,
        tech.price,
        `AI_CLOSE ${d.confidence.toFixed(0)}%: ${d.reason}`
      );

    }else{
      t.managementReason=
        `AI_HOLD ${d.confidence.toFixed(0)}%: ${d.reason}`;

      await saveTrade(t);
    }

  }catch(e){
    console.error(
      `[AI MANAGE] ${t.symbol}:`,
      safeError(e)
    );
  }
}

async function scanMarket(){
  if(state.scanBusy){
    return;
  }

  state.scanBusy=true;

  try{
    const candidates=[];

    const symbols=[
      ...new Set([
        ...ACTIVE_SYMBOLS,
        ...state.openTrades.keys()
      ])
    ];

    for(const symbol of symbols){
      if(
        Date.now()<
        state.twelveBlockedUntil
      ){
        break;
      }

      const r=
        await refreshSymbol(symbol);

      if(!r){
        continue;
      }

      const{
        pair,
        latest
      }=r;

      const tech=
        buildTechnicalIntelligence(
          pair.bars15m
        );

      state.scannedBars++;

      const existing=
        state.openTrades.get(symbol);

      if(existing){
        await manageMechanicalOnBar(
          existing,
          latest
        );

        if(
          state.openTrades.has(symbol)
        ){
          await maybeAiManage(
            existing,
            tech
          );
        }

        pair.lastClosedBarTime=
          latest.openTime;

        continue;
      }

      const c=
        localCandidate(
          symbol,
          tech
        );

      if(c){
        candidates.push(c);
      }else{
        state.skippedSignals++;

        pair.lastClosedBarTime=
          latest.openTime;
      }
    }

    candidates.sort(
      (a,b)=>b.rank-a.rank
    );

    if(candidates.length){
      await processCandidate(
        candidates[0]
      );
    }

    for(const c of candidates.slice(1)){
      const p=
        state.pairState.get(
          c.symbol
        );

      if(p){
        p.lastClosedBarTime=
          c.technical.barTime;
      }

      state.skippedSignals++;
    }

    state.marketReady=
      ACTIVE_SYMBOLS.some(
        s=>state.pairState.has(s)
      );

  }finally{
    state.scanBusy=false;
  }
}

function scheduleNextScan(){
  const now=Date.now();
  const slot=TF_MS;

  const next=
    Math.floor(now/slot)*
    slot+
    slot+
    25000;

  const delay=
    Math.max(
      5000,
      next-now
    );

  state.nextScanAt=
    new Date(now+delay);

  setTimeout(
    async()=>{
      try{
        await scanMarket();
      }catch(e){
        console.error(
          '[SCAN]',
          safeError(e)
        );
      }

      scheduleNextScan();
    },
    delay
  );
}

function startLoops(){
  if(state.loopsStarted){
    return;
  }

  state.loopsStarted=true;

  scheduleNextScan();

  setInterval(
    ()=>fetchEconomicNews()
      .catch(()=>{}),
    NEWS_REFRESH_MS
  );

  console.log(
    '[LOOPS] started'
  );
}

function buildStatus(){
  return{
    version:VERSION,
    mode:MODE,
    liveTrading:LIVE_TRADING,

    uptimeSeconds:
      Math.floor(
        (
          Date.now()-
          state.startedAt.getTime()
        )/1000
      ),

    activeSymbols:
      ACTIVE_SYMBOLS,

    ready:{
      mongo:state.mongoReady,
      telegram:state.telegramReady,
      market:state.marketReady,
      gemini:state.geminiReady,
      news:state.newsReady
    },

    account:{
      startingBalance:
        PAPER.startingBalance,

      balance:
        account
          ?n(account.balance)
          :null,

      maxTradeRiskPct:
        PAPER.maxCapitalRiskPct,

      portfolioRiskCapPct:
        PAPER.portfolioRiskCapPct,

      currentPortfolioRiskUsd:
        currentPortfolioRiskUsd(),

      portfolioRiskCapUsd:
        portfolioRiskCapUsd()
    },

    usage:{
      utcDay:
        account?.apiUsageDay,

      twelveUsed:
        n(
          account?.twelveCreditsUsed
        ),

      twelveSoftCap:
        TWELVE_DAILY_SOFT_CAP,

      twelveBlockedUntil:
        state.twelveBlockedUntil
          ?new Date(
            state.twelveBlockedUntil
          )
          :null,

      geminiEntryUsed:
        n(
          account?.geminiEntryUsed
        ),

      geminiEntryCap:
        GEMINI_ENTRY_DAILY_CAP,

      geminiManageUsed:
        n(
          account?.geminiManageUsed
        ),

      geminiManageCap:
        GEMINI_MANAGE_DAILY_CAP,

      geminiBlockedUntil:
        state.geminiBlockedUntil
          ?new Date(
            state.geminiBlockedUntil
          )
          :null
    },

    trades:{
      open:
        state.openTrades.size,

      max:
        PAPER.maxOpenTrades,

      executed:
        state.executedSignals,

      skipped:
        state.skippedSignals
    },

    ai:{
      entryCalls:
        state.aiEntryCalls,

      manageCalls:
        state.aiManageCalls,

      buy:
        state.aiBuyDecisions,

      sell:
        state.aiSellDecisions,

      noTrade:
        state.aiNoTradeDecisions,

      close:
        state.aiCloseDecisions,

      hold:
        state.aiHoldDecisions,

      lastError:
        state.lastAiError
    },

    market:{
      initializedSymbols:
        state.pairState.size,

      totalActiveSymbols:
        ACTIVE_SYMBOLS.length,

      scannedBars:
        state.scannedBars,

      lastError:
        state.lastMarketError,

      nextScanAt:
        state.nextScanAt
    },

    news:{
      ready:
        state.newsReady,

      lastError:
        state.lastNewsError
    },

    exits:{
      breakEvenAtR:.6,
      partialCloseAtR:2,
      partialClosePct:50,
      trailingStartStopR:1,
      trailingStepR:.5
    }
  };
}

function startWebServer(){
  const app=express();

  app.use(
    express.json({
      limit:'1mb'
    })
  );

  app.get(
    '/',
    (req,res)=>
      res.json({
        service:VERSION,
        mode:MODE,
        status:'RUNNING'
      })
  );

  app.get(
    '/health',
    (req,res)=>
      res.json({
        ok:true,
        version:VERSION,
        mongoReady:
          state.mongoReady,
        marketReady:
          state.marketReady,
        geminiReady:
          state.geminiReady,
        telegramReady:
          state.telegramReady,
        newsReady:
          state.newsReady,
        openTrades:
          state.openTrades.size
      })
  );

  app.get(
    '/api/status',
    (req,res)=>
      res.json(
        buildStatus()
      )
  );

  app.get(
    '/api/trades',
    (req,res)=>
      res.json(
        [
          ...state.openTrades.values()
        ]
      )
  );

  app.post(
    '/api/trades/:symbol/close',
    async(req,res)=>{
      try{
        const s=
          String(
            req.params.symbol||
            ''
          ).toUpperCase();

        const t=
          state.openTrades.get(s);

        if(!t){
          return res
            .status(404)
            .json({
              ok:false,
              error:
                'No open trade for symbol'
            });
        }

        const q=
          await fetchQuote(s);

        const x=
          exitExecutionPrice(
            t.direction,
            q
          );

        return res.json({
          ok:true,
          trade:
            await closeTradeAtPrice(
              t,
              x,
              'MANUAL_API_CLOSE'
            )
        });

      }catch(e){
        return res
          .status(500)
          .json({
            ok:false,
            error:safeError(e)
          });
      }
    }
  );

  app.listen(
    PORT,
    '0.0.0.0',
    ()=>{
      console.log(
        `[WEB] listening on ${PORT}`
      );
    }
  );
}

function validateStartupConfig(){
  if(
    MODE!=='PAPER'||
    LIVE_TRADING
  ){
    throw new Error(
      'PAPER only'
    );
  }

  if(
    PAPER.maxCapitalRiskPct>1||
    PAPER.portfolioRiskCapPct>4
  ){
    throw new Error(
      'Risk limits invalid'
    );
  }

  if(!TWELVE_DATA_API_KEY){
    throw new Error(
      'TWELVE_DATA_API_KEY is missing'
    );
  }

  if(!MONGODB_URI){
    throw new Error(
      'MONGODB_URI is missing'
    );
  }

  if(!GEMINI_API_KEY){
    console.warn(
      '[STARTUP] GEMINI_API_KEY missing; no new entries'
    );
  }
}

async function shutdown(sig){
  console.log(
    `[SHUTDOWN] ${sig}`
  );

  try{
    telegramBot?.stop(sig);
  }catch(_){}

  try{
    if(
      mongoose.connection
        .readyState
    ){
      await mongoose.disconnect();
    }
  }catch(_){}

  process.exit(0);
}

process.on(
  'SIGTERM',
  ()=>shutdown('SIGTERM')
);

process.on(
  'SIGINT',
  ()=>shutdown('SIGINT')
);

process.on(
  'unhandledRejection',
  e=>console.error(
    '[UNHANDLED]',
    safeError(e)
  )
);

process.on(
  'uncaughtException',
  e=>console.error(
    '[UNCAUGHT]',
    safeError(e)
  )
);

async function boot(){
  console.log(
`\n${VERSION}
MODE=${MODE}
ACTIVE=${ACTIVE_SYMBOLS.join(',')}
`
  );

  validateStartupConfig();

  startWebServer();

  await initMongo();
  await restoreOpenTrades();
  await initTelegram();
  await fetchEconomicNews();
  await initializeMarket();

  startLoops();

  await journal(
    'BOT_STARTED',
    {
      message:VERSION,
      activeSymbols:
        ACTIVE_SYMBOLS
    }
  );

  console.log(
    '[BOOT] READY'
  );
}

boot().catch(e=>{
  console.error(
    '[BOOT FATAL]',
    safeError(e)
  );

  process.exit(1);
});
