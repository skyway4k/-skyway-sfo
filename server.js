#!/usr/bin/env node
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8766, WS_PORT = 8765;
const OSKY_ID = 'skyway-api-client';
const OSKY_SECRET = 'TzrrCV2IoPlIqmRiRcpUIVscZQheFBQS';
const TOKEN_URL = 'https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token';

// FAA SWIM SCDS
const SWIM_USER = 'skyway.4k.gmail.com';
const SWIM_PASS = process.env.SWIM_PASS || 'V3_iPPvMTtGptG8MiwKNaw';
const SWIM_QUEUE = 'skyway.4k.gmail.com.FDPS.019d315a-6ec1-4b83-a699-413b0d0c8012.OUT';
const SWIM_URL = 'tcps://ems2.swim.faa.gov:55443';
const SWIM_VPN = 'FDPS';

let oskyToken = null, oskyExp = 0;
let wsClients = new Set();

function log(m,l='INFO'){const t=new Date().toLocaleTimeString('en-US',{hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false});const c={INFO:'\x1b[37m',OK:'\x1b[32m',WARN:'\x1b[33m',ERR:'\x1b[31m',MSG:'\x1b[36m'};console.log(`${c[l]||c.INFO}[${t}] [${l}]\x1b[0m ${m}`);}

async function getToken(){
  if(oskyToken&&Date.now()<oskyExp-30000)return oskyToken;
  try{
    const r=await fetch(TOKEN_URL,{method:'POST',headers:{'Content-Type':'application/x-www-form-urlencoded'},body:`grant_type=client_credentials&client_id=${encodeURIComponent(OSKY_ID)}&client_secret=${encodeURIComponent(OSKY_SECRET)}`});
    if(!r.ok)throw new Error('HTTP '+r.status);
    const d=await r.json();oskyToken=d.access_token;oskyExp=Date.now()+(d.expires_in||1800)*1000;
    log('OpenSky token OK','OK');return oskyToken;
  }catch(e){log('Token error: '+e.message,'ERR');return null;}
}

async function proxyOsky(apiPath,res){
  try{
    const tk=await getToken();
    const h=tk?{'Authorization':'Bearer '+tk}:{};
    const url='https://opensky-network.org/api'+apiPath;
    log('-> '+url,'MSG');
    const r=await fetch(url,{headers:h});
    const body=await r.arrayBuffer();
    const buf=Buffer.from(body);
    log('<- '+r.status+' ('+buf.length+' bytes)','MSG');
    res.writeHead(r.status,{'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
    res.end(buf);
  }catch(e){
    log('Proxy err: '+e.message,'ERR');
    res.writeHead(502,{'Content-Type':'application/json','Access-Control-Allow-Origin':'*'});
    res.end(JSON.stringify({error:e.message}));
  }
}

// FlightAware AeroAPI
const FA_KEY = 'hDZ46pJAZ1aif2ZRFaaIvWVww59ziFiv';
const FA_BASE = 'https://aeroapi.flightaware.com/aeroapi';
var faArrivals = [], faDepartures = [], faLastFetch = 0;
// Persistent cache of all arrived aircraft (accumulates over time, survives refreshes)
var groundCache = {}; // key: ident, value: arrival data

async function fetchFlightAware(){
  if(Date.now()-faLastFetch < 120000) return; // cache 2 min
  faLastFetch = Date.now();
  try{
    const url = FA_BASE+'/airports/KSFO/flights?type=General_Aviation&max_pages=2';
    log('FA -> '+url,'MSG');
    const r = await fetch(url,{headers:{'x-apikey':FA_KEY}});
    if(!r.ok){log('FA error: HTTP '+r.status,'ERR');return;}
    const d = await r.json();
    // Process arrivals
    var now=new Date().toISOString();
    if(d.arrivals){
      faArrivals = d.arrivals.map(function(f){
        var arrISO=f.actual_on||f.estimated_on||f.scheduled_on||'';
        var depISO=f.actual_off||f.estimated_off||f.scheduled_off||'';
        var orig=cleanCode(f,'origin');
        var intl=orig&&orig.length>=2&&orig.charAt(0)!=='K'&&orig.charAt(0)!=='P';
        var city=f.origin&&f.origin.city?f.origin.city:'';
        var tail=f.registration||'';
        var flightId=f.ident||'';
        var blocked=f.blocked||(!tail&&!flightId);
        var displayIdent=tail||flightId||'BLOCKED';
        var displayCallsign='';
        if(blocked&&!tail&&!flightId)displayIdent='BLOCKED';
        else if(blocked&&flightId)displayIdent=flightId;
        if(tail&&flightId&&flightId!==tail)displayCallsign=flightId;
        return{ident:displayIdent,callsign:displayCallsign,blocked:blocked,type:f.aircraft_type||'',from:orig||'',intl:intl,city:intl?'':city,country:intl?city:'',
        depart:fmtTime(depISO)||'',departISO:depISO,arrive:fmtTime(arrISO)||'',arriveISO:arrISO,
        progress:typeof f.progress_percent==='number'?f.progress_percent:(f.actual_on?100:(f.actual_off?50:0)),
        arrived:!!f.actual_on,status:f.status||'',operator:getOperator(flightId)};
      });
    }
    if(d.scheduled_arrivals){
      var sa=d.scheduled_arrivals.map(function(f){
        var arrISO=f.estimated_on||f.scheduled_on||'';
        var depISO=f.actual_off||f.estimated_off||f.scheduled_off||'';
        var orig=cleanCode(f,'origin');
        var intl=orig&&orig.length>=2&&orig.charAt(0)!=='K'&&orig.charAt(0)!=='P';
        var city=f.origin&&f.origin.city?f.origin.city:'';
        var tail=f.registration||'';var flightId=f.ident||'';
        var blocked=f.blocked||(!tail&&!flightId);
        var displayIdent=tail||flightId||'BLOCKED';
        var displayCallsign=(!blocked&&tail&&flightId&&flightId!==tail)?flightId:'';
        return{ident:displayIdent,callsign:displayCallsign,blocked:blocked,type:f.aircraft_type||'',from:orig||'',intl:intl,city:intl?'':city,country:intl?city:'',
        depart:fmtTime(depISO),departISO:depISO,arrive:fmtTime(arrISO),arriveISO:arrISO,
        progress:typeof f.progress_percent==='number'?f.progress_percent:(f.actual_on?100:(f.actual_off?50:0)),
        arrived:false,status:f.status||'',operator:getOperator(flightId)};
      });
      faArrivals=faArrivals.concat(sa);
    }
    faArrivals.sort(function(a,b){return(a.arriveISO||'').localeCompare(b.arriveISO||'');});
    // Process departures
    if(d.departures){
      faDepartures = d.departures.map(function(f){
        var depISO=f.actual_off||f.estimated_off||f.scheduled_off||'';
        var arrISO=f.actual_on||f.estimated_on||f.scheduled_on||'';
        var dest=cleanCode(f,'destination');
        var intl=dest&&dest.length>=2&&dest.charAt(0)!=='K'&&dest.charAt(0)!=='P';
        var city=f.destination&&f.destination.city?f.destination.city:'';
        var tail=f.registration||'';var flightId=f.ident||'';
        var blocked=f.blocked||(!tail&&!flightId);
        var displayIdent=tail||flightId||'BLOCKED';
        var displayCallsign=(!blocked&&tail&&flightId&&flightId!==tail)?flightId:'';
        return{ident:displayIdent,callsign:displayCallsign,blocked:blocked,type:f.aircraft_type||'',to:dest||'',intl:intl,city:intl?'':city,country:intl?city:'',
        depart:fmtTime(depISO),departISO:depISO,arrive:fmtTime(arrISO),arriveISO:arrISO,
        progress:typeof f.progress_percent==='number'?f.progress_percent:(f.actual_on?100:(f.actual_off?50:0)),
        departed:!!f.actual_off,arrived:!!f.actual_on,status:f.status||'',operator:getOperator(flightId)};
      });
    }
    if(d.scheduled_departures){
      var sd=d.scheduled_departures.map(function(f){
        var depISO=f.estimated_off||f.scheduled_off||'';
        var arrISO=f.estimated_on||f.scheduled_on||'';
        var dest=cleanCode(f,'destination');
        var intl=dest&&dest.length>=2&&dest.charAt(0)!=='K'&&dest.charAt(0)!=='P';
        var city=f.destination&&f.destination.city?f.destination.city:'';
        var tail=f.registration||'';var flightId=f.ident||'';
        var blocked=f.blocked||(!tail&&!flightId);
        var displayIdent=tail||flightId||'BLOCKED';
        var displayCallsign=(!blocked&&tail&&flightId&&flightId!==tail)?flightId:'';
        return{ident:displayIdent,callsign:displayCallsign,blocked:blocked,type:f.aircraft_type||'',to:dest||'',intl:intl,city:intl?'':city,country:intl?city:'',
        depart:fmtTime(depISO),departISO:depISO,arrive:fmtTime(arrISO),arriveISO:arrISO,
        progress:typeof f.progress_percent==='number'?f.progress_percent:(f.actual_on?100:(f.actual_off?50:0)),
        departed:false,arrived:false,status:f.status||'',operator:getOperator(flightId)};
      });
      faDepartures=faDepartures.concat(sd);
    }
    faDepartures.sort(function(a,b){return(a.departISO||'').localeCompare(b.departISO||'');});
    // Filter out coast guard helicopters (C followed by 4+ digits)
    function isCoastGuard(id){if(!id)return false;var u=id.toUpperCase();return u.charAt(0)==='C'&&u.length>=5&&u.charAt(1)>='0'&&u.charAt(1)<='9'&&u.charAt(2)>='0'&&u.charAt(2)<='9'&&u.charAt(3)>='0'&&u.charAt(3)<='9'&&u.charAt(4)>='0'&&u.charAt(4)<='9';}
    faArrivals=faArrivals.filter(function(f){return !isCoastGuard(f.ident);});
    faDepartures=faDepartures.filter(function(f){return !isCoastGuard(f.ident);});
    // Accumulate arrived aircraft into groundCache
    for(var i=0;i<faArrivals.length;i++){
      var f=faArrivals[i];
      if(f.arrived&&f.ident&&f.ident!=='BLOCKED'){
        groundCache[f.ident.toUpperCase()]={ident:f.ident,callsign:f.callsign,type:f.type,from:f.from,city:f.city,country:f.country,intl:f.intl,
          arrivedTime:f.arrive,arrivedISO:f.arriveISO,departISO:f.departISO};
      }
    }
    // Remove aircraft that have departed from groundCache
    for(var i=0;i<faDepartures.length;i++){
      var f=faDepartures[i];
      if(f.departed&&f.ident){delete groundCache[f.ident.toUpperCase()];}
    }
    // Expire entries older than 30 days
    var thirtyDaysAgo=Date.now()-(30*24*60*60*1000);
    for(var k in groundCache){
      if(groundCache[k].arrivedISO&&new Date(groundCache[k].arrivedISO).getTime()<thirtyDaysAgo)delete groundCache[k];
    }
    log('FA: '+faArrivals.length+' arr, '+faDepartures.length+' dep, '+Object.keys(groundCache).length+' on ground','OK');
  }catch(e){log('FA error: '+e.message,'ERR');}
}


function getOperator(ident){
  if(!ident)return'';
  var pfx=ident.replace(/[0-9]/g,'').toUpperCase();
  var OPS={EJA:'NetJets',LXJ:'Flexjet',VJT:'VistaJet',NJE:'NetJets EU',XOJ:'XO',KOW:'Wheels Up',TWY:'Solairus',JRE:'JetEdge',ASP:'Jet Access',CLY:'Clay Lacy',RKK:'K2 Aviation',BBJ:'Boeing BBJ',GAJ:'Gulfstream',SLR:'Solaris',FLX:'Flexjet',MMD:'Priester',GCK:'Jet Linx',LNX:'Lynx Air',NJT:'NetJets',VCG:'VistaJet',SIO:'Sirio',SVW:'VistaJet',PEX:'PlaneSense',TCJ:'Jet Aviation',JFA:'Jetfly',AOJ:'ASL',IJM:'VistaJet',TRS:'TriStar',SCX:'Sun Country',FLG:'Flagler',BBB:'Air Hamburg',SXN:'Saxon',HYP:'Titan Airways',NJB:'NetJets',FYL:'Jetfly',GAF:'German AF',XJT:'XO',LPZ:'Luxair'};
  return OPS[pfx]||'';
}

function fmtTime(iso){
  if(!iso)return '';
  var d=new Date(iso);
  return d.toLocaleTimeString('en-US',{hour:'2-digit',minute:'2-digit',hour12:false,timeZone:'America/Los_Angeles'});
}

function cleanCode(f,field){
  if(!f||!f[field])return '';
  var o=f[field];
  var code=o.code_icao||o.code||'';
  // Filter out codes that look like coordinates or are too long
  if(!code)return '';
  if(code.indexOf('.')>=0)return '';
  if(code.length>6)return '';
  return code;
}

// Fetch immediately and then every 2 minutes
fetchFlightAware();
setInterval(fetchFlightAware, 120000);

// Generate HTML file on startup
const htmlPath = path.join(__dirname,'skyway.html');
fs.writeFileSync(htmlPath, buildHTML());
log('Generated skyway.html','OK');

const server = http.createServer(async(req,res)=>{
  res.setHeader('Access-Control-Allow-Origin','*');
  if(req.method==='OPTIONS'){res.writeHead(204);res.end();return;}
  if(req.url==='/'||req.url==='/skyway.html'){
    res.writeHead(200,{'Content-Type':'text/html; charset=utf-8'});
    res.end(fs.readFileSync(htmlPath));return;
  }
  if(req.url.startsWith('/osky/')){await proxyOsky(req.url.replace('/osky',''),res);return;}
  if(req.url==='/fa/arrivals'){
    res.writeHead(200,{'Content-Type':'application/json'});
    res.end(JSON.stringify(faArrivals));return;
  }
  if(req.url==='/fa/departures'){
    res.writeHead(200,{'Content-Type':'application/json'});
    res.end(JSON.stringify(faDepartures));return;
  }
  if(req.url==='/fa/raw'){
    // Show raw FA arrival data to debug progress bar
    res.writeHead(200,{'Content-Type':'application/json'});
    res.end(JSON.stringify(faArrivals.slice(0,10).map(function(f){
      return {ident:f.ident,type:f.type,departISO:f.departISO,arriveISO:f.arriveISO,depart:f.depart,arrive:f.arrive,arrived:f.arrived,progress:f.progress};
    }),null,2));return;
  }
  if(req.url==='/fa/debug'){
    var dbg=faArrivals.slice(0,5).map(function(f){return{ident:f.ident,departISO:f.departISO,arriveISO:f.arriveISO,arrived:f.arrived,depart:f.depart,arrive:f.arrive,type:f.type};});
    res.writeHead(200,{'Content-Type':'application/json'});
    res.end(JSON.stringify(dbg,null,2));return;
  }
  if(req.url==='/fa/ground'){
    // Use groundCache which accumulates all arrived aircraft and removes departed ones
    var ground=Object.values(groundCache).map(function(f){
      // Check if there's a scheduled departure for this aircraft
      var dep=null;
      for(var i=0;i<faDepartures.length;i++){
        if(faDepartures[i].ident===f.ident&&!faDepartures[i].departed){dep=faDepartures[i];break;}
      }
      return{ident:f.ident,callsign:f.callsign,type:f.type,from:f.from,city:f.city,country:f.country,intl:f.intl,
        arrivedTime:f.arrivedTime,arrivedISO:f.arrivedISO,departISO:f.departISO,
        nextDest:dep?dep.to:'',nextDepart:dep?dep.depart:'',nextDepartISO:dep?dep.departISO:'',nextDestCity:dep?dep.city:'',
        nextArriveISO:dep?dep.arriveISO:''};
    });
    // Sort: most recent arrival first
    ground.sort(function(a,b){return(b.arrivedISO||'').localeCompare(a.arrivedISO||'');});
    res.writeHead(200,{'Content-Type':'application/json'});
    res.end(JSON.stringify(ground));return;
  }
  res.writeHead(200,{'Content-Type':'application/json'});
  res.end(JSON.stringify({name:'Skyway',url:'http://localhost:'+PORT}));
});
server.listen(PORT,'0.0.0.0',()=>log('http://0.0.0.0:'+PORT,'OK'));

const wss=new WebSocketServer({port:WS_PORT});
wss.on('connection',ws=>{wsClients.add(ws);log('WS connected ('+wsClients.size+')');ws.send(JSON.stringify({type:'init',swimArr:swimArr.slice(0,50),swimDep:swimDep.slice(0,50),swimStats:swimStats}));ws.on('close',()=>wsClients.delete(ws));});

// SWIM SCDS Connection
var swimStats = {connected:false, msgs:0, arrivals:0, departures:0};
var swimArr=[], swimDep=[];

function connectSWIM(){
  if(!SWIM_PASS||SWIM_PASS==='skip'){log('SWIM skipped.','WARN');return;}
  var solace;
  try{solace=require('solclientjs');}catch(e){log('solclientjs missing: npm install','ERR');return;}

  var fp=new solace.SolclientFactoryProperties();
  fp.profile=solace.SolclientFactoryProfiles.version10;
  solace.SolclientFactory.init(fp);
  log('Connecting to SWIM SCDS...');

  var sess=solace.SolclientFactory.createSession({
    url:SWIM_URL, vpnName:SWIM_VPN, userName:SWIM_USER, password:SWIM_PASS,
    connectRetries:3, reconnectRetries:10, reconnectRetryWaitInMsecs:5000
  });

  sess.on(solace.SessionEventCode.UP_NOTICE,function(){
    swimStats.connected=true;
    log('SWIM connected ✓','OK');
    broadcast({type:'swimStatus',status:'connected'});
    try{
      var consumer=sess.createMessageConsumer({
        queueDescriptor:{name:SWIM_QUEUE,type:solace.QueueType.QUEUE},
        acknowledgeMode:solace.MessageConsumerAcknowledgeMode.AUTO,
        createIfMissing:false
      });
      consumer.on(solace.MessageConsumerEventName.UP,function(){log('SWIM queue consumer UP ✓','OK');});
      consumer.on(solace.MessageConsumerEventName.MESSAGE,function(msg){handleSwimMsg(msg);});
      consumer.on(solace.MessageConsumerEventName.DOWN_ERROR,function(){log('SWIM queue error','ERR');});
      consumer.connect();
    }catch(e){log('SWIM queue err: '+e.message,'ERR');}
  });

  sess.on(solace.SessionEventCode.CONNECT_FAILED_ERROR,function(e){
    swimStats.connected=false;
    log('SWIM FAILED: '+(e.infoStr||'unknown'),'ERR');
    broadcast({type:'swimStatus',status:'failed'});
  });
  sess.on(solace.SessionEventCode.DISCONNECTED,function(){swimStats.connected=false;log('SWIM disconnected','WARN');});
  sess.on(solace.SessionEventCode.RECONNECTED_NOTICE,function(){swimStats.connected=true;log('SWIM reconnected ✓','OK');});
  sess.connect();
}

function handleSwimMsg(message){
  swimStats.msgs++;
  var payload='';
  try{
    var bin=message.getBinaryAttachment();
    if(bin){payload=typeof bin==='string'?bin:Buffer.isBuffer(bin)?bin.toString('utf-8'):String(bin);}
    if(!payload&&message.getSdtContainer){payload=message.getSdtContainer().getValue();}
  }catch(e){}
  if(!payload)return;

  // Parse FIXM
  var type='UNKNOWN';
  if(payload.indexOf('DepartureInformation')>=0||payload.indexOf('flightDeparture')>=0)type='DEPARTURE';
  else if(payload.indexOf('ArrivalInformation')>=0||payload.indexOf('flightArrival')>=0)type='ARRIVAL';
  else if(payload.indexOf('EnRoute')>=0||payload.indexOf('enRoute')>=0)type='EN_ROUTE';

  var cs=xval(payload,'aircraftIdentification','callsign');
  var orig=xval(payload,'departureAerodrome.*?locationIndicator','departureAirport','originAirport');
  var dest=xval(payload,'destinationAerodrome.*?locationIndicator','arrivalAirport','destinationAirport');
  var acType=xval(payload,'aircraftType','typeDesignator');

  var entry={type:type,callsign:cs,origin:orig,destination:dest,aircraftType:acType,timestamp:new Date().toISOString()};

  if(type==='ARRIVAL'){swimStats.arrivals++;swimArr.unshift(entry);if(swimArr.length>100)swimArr.pop();}
  else if(type==='DEPARTURE'){swimStats.departures++;swimDep.unshift(entry);if(swimDep.length>100)swimDep.pop();}

  broadcast({type:'swimMsg',data:entry});
  log('[SWIM '+type+'] '+(cs||'?')+' '+(orig||'?')+' -> '+(dest||'?'),'MSG');
}

function xval(xml){
  for(var i=1;i<arguments.length;i++){
    var tag=arguments[i];
    var re=new RegExp('<[^>]*?'+tag+'[^>]*?>\\s*([^<]+)','is');
    var m=xml.match(re);
    if(m&&m[1]&&m[1].trim().length<200)return m[1].trim();
  }
  return null;
}

function broadcast(d){var m=JSON.stringify(d);wsClients.forEach(function(c){if(c.readyState===1)try{c.send(m);}catch(e){}});}

connectSWIM();

console.log(`\n  Skyway v4 — http://localhost:${PORT}\n`);
getToken();
process.on('SIGINT',()=>{log('Bye','WARN');process.exit(0);});

// ═══════════════════════════════════════
// HTML BUILDER — writes a real .html file
// ═══════════════════════════════════════
function buildHTML(){
return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Skyway</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.css"/>
<script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.js"></` + `script>
<style>
:root{--b0:#e8eaed;--b1:#f0f1f3;--b2:#d9dce1;--b3:#c8ccd3;--bd:#b8bcc5;--t1:#1c1f26;--t2:#3d4352;--t3:#7a8194;--blue:#2563eb;--cyan:#0891b2;--green:#16a34a;--amber:#d97706;--red:#dc2626;--violet:#7c3aed;--mono:'JetBrains Mono',ui-monospace,monospace;--sans:'Inter','Outfit',system-ui,sans-serif}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:var(--sans);background:var(--b0);color:var(--t1);min-height:100vh;-webkit-font-smoothing:antialiased}
.topbar{display:flex;align-items:center;justify-content:space-between;padding:0 24px;height:54px;background:linear-gradient(180deg,#2c3040,#1e222e);border-bottom:1px solid rgba(255,255,255,.06);position:sticky;top:0;z-index:100;box-shadow:0 2px 8px rgba(0,0,0,.15)}
.tb-l{display:flex;align-items:center;gap:16px}
.brand{display:flex;align-items:center;gap:8px}
.brand-i{width:28px;height:28px;border-radius:6px;background:linear-gradient(135deg,#3b82f6,#60a5fa);display:flex;align-items:center;justify-content:center;font-size:14px;font-weight:800;color:#1a1f2e}
.brand-n{font-family:var(--mono);font-size:16px;font-weight:700;color:#fff}
.sep{width:1px;height:24px;background:rgba(255,255,255,.12)}
.pill{display:flex;align-items:center;gap:5px;padding:4px 10px;border-radius:10px;font-family:var(--mono);font-size:10px;font-weight:600}
.pill.live{color:#4ade80;background:rgba(74,222,128,.1)}
.pill.live .dot{width:6px;height:6px;border-radius:50%;background:#4ade80;animation:bk 2s infinite}
.pill.err{color:#f87171;background:rgba(248,113,113,.1)}
@keyframes bk{0%,100%{opacity:1}50%{opacity:.2}}
.ga{font-family:var(--mono);font-size:9px;font-weight:700;color:#8ba3c4;padding:3px 8px;border-radius:8px;background:rgba(139,163,196,.08);border:1px solid rgba(139,163,196,.15)}
.tb-c{display:flex;align-items:center;gap:10px}
.apt{font-family:var(--mono);font-size:15px;font-weight:800;color:#b4c4d8}
.apn{font-size:13px;color:rgba(255,255,255,.5);font-weight:500}
.tb-r{display:flex;align-items:center;gap:10px}
.fd{display:flex;align-items:center;gap:4px;padding:3px 8px;border-radius:8px;font-family:var(--mono);font-size:9px;font-weight:600}
.fd .d{width:5px;height:5px;border-radius:50%;background:currentColor}
.fd.on{color:#4ade80;background:rgba(74,222,128,.08);border:1px solid rgba(74,222,128,.15)}
.fd.off{color:rgba(255,255,255,.3);border:1px solid rgba(255,255,255,.1)}
.clk{font-family:var(--mono);font-size:11px;font-weight:600;color:rgba(255,255,255,.6)}
.ib{width:30px;height:30px;display:flex;align-items:center;justify-content:center;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.1);border-radius:6px;cursor:pointer;color:rgba(255,255,255,.5);font-size:13px;transition:all .15s}
.ib:hover{border-color:rgba(232,212,139,.4);color:#b4c4d8}
.map-row{display:flex;gap:10px;margin:10px 12px;height:260px;min-height:260px;flex-shrink:0}
.map-area{position:relative;flex:2.5;border-radius:10px;border:1px solid var(--bd);overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.04),0 4px 12px rgba(0,0,0,.02)}
#map{width:100%;height:100%;background:var(--b0)}
.chart-area{flex:1;border-radius:10px;border:1px solid var(--bd);background:var(--b1);padding:12px 14px;display:flex;flex-direction:column;box-shadow:0 1px 3px rgba(0,0,0,.04),0 4px 12px rgba(0,0,0,.02)}
.leaflet-tile-pane{filter:saturate(.5) brightness(1.02) contrast(1.02)}
.leaflet-control-zoom a{background:var(--b1)!important;color:var(--t1)!important;border-color:var(--bd)!important;font-weight:700!important}
.leaflet-control-attribution{display:none!important}
.ac-tip{background:rgba(255,255,255,.92)!important;border:1px solid rgba(0,0,0,.08)!important;border-radius:4px!important;padding:2px 5px!important;font-family:'JetBrains Mono',monospace!important;font-size:9px!important;font-weight:700!important;color:#111827!important;box-shadow:0 1px 4px rgba(0,0,0,.08)!important;white-space:nowrap!important}
.ac-tip:before{display:none!important}
.hud{position:absolute;top:8px;left:8px;z-index:500;display:flex;gap:1px;background:var(--bd);border-radius:8px;overflow:hidden;box-shadow:0 2px 10px rgba(0,0,0,.06)}
.hc{display:flex;flex-direction:column;align-items:center;padding:5px 10px;background:rgba(255,255,255,.95);backdrop-filter:blur(16px);min-width:48px}
.hc .v{font-family:var(--mono);font-size:14px;font-weight:800;line-height:1}
.hc .l{font-family:var(--mono);font-size:6px;color:var(--t3);text-transform:uppercase;letter-spacing:.6px;margin-top:2px;font-weight:600}
.hc.b .v{color:var(--blue)}.hc.g .v{color:var(--green)}.hc.a .v{color:var(--amber)}.hc.c .v{color:var(--cyan)}.hc.r .v{color:var(--red)}
.boards{display:grid;grid-template-columns:1fr 1fr;border-top:1px solid var(--bd);background:var(--b0)}
.board{display:flex;flex-direction:column;background:var(--b1)}
.board:first-child{border-right:1px solid var(--bd)}
.bh{display:flex;align-items:center;justify-content:space-between;padding:10px 16px;background:linear-gradient(180deg,#e4e6eb,#d8dae0);border-bottom:1px solid var(--bd)}
.bt{display:flex;align-items:center;gap:8px;font-family:var(--mono);font-size:11px;font-weight:800;text-transform:uppercase;letter-spacing:1.2px}
.bt.ar{color:var(--blue)}.bt.de{color:var(--red)}
.bc{font-family:var(--mono);font-size:11px;font-weight:700;color:var(--t3)}
.cols{display:grid;grid-template-columns:1fr 1fr 1fr 1fr 1fr .7fr .7fr .5fr;gap:6px;padding:7px 12px;font-family:var(--mono);font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:var(--t3);border-bottom:1px solid var(--bd);text-align:center;background:var(--b2)}
.cols.ca-done{display:grid;grid-template-columns:1fr 1fr 1fr 1fr 1fr .8fr .7fr;gap:6px;padding:7px 12px;font-family:var(--mono);font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:var(--t3);border-bottom:1px solid var(--bd);text-align:center;background:var(--b2)}
.cols.cd-done{display:grid;grid-template-columns:1fr 1fr 1fr 1fr .8fr .7fr;gap:6px;padding:7px 12px;font-family:var(--mono);font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:var(--t3);border-bottom:1px solid var(--bd);text-align:center;background:var(--b2)}
.cols.ca{grid-template-columns:1fr 1fr 1fr 1fr 1fr .7fr .7fr .5fr}
.cols.cd{grid-template-columns:1fr 1fr 1fr 1fr .7fr .7fr .4fr .4fr .4fr .4fr .4fr}
.cols span:first-child{text-align:left;padding-left:16px}
.bb{flex:1;padding:5px 8px}
.fr{display:grid;grid-template-columns:1fr 1fr 1fr 1fr 1fr .7fr .7fr .5fr;gap:6px;align-items:center;padding:7px 8px;background:var(--b1);border:1px solid var(--bd);border-radius:7px;margin-bottom:3px;cursor:pointer;transition:all .12s;box-shadow:0 1px 2px rgba(0,0,0,.02)}
.fr.arr-done{grid-template-columns:1fr 1fr 1fr 1fr 1fr .8fr .7fr}
.fr.dep{grid-template-columns:1fr 1fr 1fr 1fr .8fr .5fr .5fr .5fr .5fr .5fr}
.fr.dep-simple{grid-template-columns:1fr 1fr 1fr 1fr .8fr .7fr}
.fr:hover{background:var(--b2);border-color:var(--bd)}
.fr.done{opacity:1}
.fr.arr-active{background:rgba(37,99,235,.03);border-color:rgba(37,99,235,.12)}
.fr.arr-active:hover{background:rgba(37,99,235,.06)}
.fr.dep-ground{background:rgba(220,38,38,.02);border-color:rgba(220,38,38,.1)}
.fr.dep-gone{background:rgba(220,38,38,.04);border-color:rgba(220,38,38,.12)}
.chk{width:16px;height:16px;accent-color:var(--green);cursor:pointer;margin:0 auto;display:block}
.fi{font-family:var(--mono);font-weight:800;font-size:17px;color:var(--cyan);overflow:visible;text-overflow:ellipsis;white-space:nowrap;line-height:1.2;position:relative;padding-left:16px}
.fi
#map .leaflet-tile-pane{filter:invert(1) hue-rotate(180deg) brightness(0.6) contrast(1.2) saturate(0.3)}
.spot-dd{position:absolute;z-index:100;background:var(--b1);border:1px solid var(--bd);border-radius:4px;box-shadow:0 4px 12px rgba(0,0,0,.2);padding:4px 0;max-height:200px;overflow-y:auto}
.spot-dd-item{font-family:var(--mono);font-size:9px;padding:3px 10px;cursor:pointer;color:var(--t1);white-space:nowrap}
.spot-dd-item:hover{background:var(--blue);color:#fff}
.zp{cursor:pointer}.fi.zp:hover{color:var(--blue);text-decoration:underline}
.fi .sub{font-size:9px;font-weight:500;color:var(--t3);display:block}
.ft{font-family:var(--mono);font-size:16px;font-weight:700;color:var(--t1);text-align:center;line-height:1.2}
.ft .sub{font-size:8px;font-weight:500;color:var(--t3);display:block}
.ff{font-family:var(--mono);font-size:14px;font-weight:600;color:var(--t2);text-align:center;line-height:1.2}
.ff.intl{color:var(--red);font-weight:700}
.ff .sub{font-size:8px;font-weight:400;color:var(--t3);display:block}
.ff.intl .sub{color:var(--red);font-weight:500;opacity:.7;font-size:8px}
.fm{font-family:var(--mono);font-size:13px;font-weight:600;color:var(--t2);text-align:center}
.fe{font-family:var(--mono);font-size:13px;font-weight:700;text-align:center}
.fe.soon{color:var(--green)}.fe.mid{color:var(--amber)}.fe.far{color:var(--t3)}
.done-hdr{border-top:2px solid var(--bd)}
.prog{display:flex;align-items:center;padding:0 2px;width:100%;min-width:20px;min-height:6px}
.prog-bar{width:100%;height:6px;background:var(--bd);border-radius:3px;overflow:hidden}
.prog-fill{height:100%;border-radius:3px;background:linear-gradient(90deg,#3b82f6,#60a5fa);transition:width .8s;min-width:1px}
.pax{width:36px;padding:4px 3px;border:1px solid var(--bd);border-radius:5px;font-family:var(--mono);font-size:11px;font-weight:600;text-align:center;background:var(--b1);color:var(--t1);outline:none;transition:all .15s;justify-self:center;margin:0 auto}
.pax:focus{border-color:var(--blue);box-shadow:0 0 0 3px rgba(37,99,235,.1)}
.pax::placeholder{color:#d1d5db;font-weight:400}
.valet{width:44px;padding:4px 3px;border:1px solid var(--bd);border-radius:5px;font-family:var(--mono);font-size:11px;font-weight:600;text-align:center;background:var(--b1);color:var(--t1);outline:none;transition:all .15s}
.valet:focus{border-color:var(--violet);box-shadow:0 0 0 3px rgba(124,58,237,.1)}
.valet::placeholder{color:#d1d5db;font-weight:400}
.resize-bar{height:8px;cursor:ns-resize;display:flex;align-items:center;justify-content:center;background:var(--b0);margin:0 12px;flex-shrink:0}
.resize-grip{width:40px;height:3px;border-radius:2px;background:var(--bd)}
.resize-bar:hover .resize-grip{background:var(--blue)}
.empty{text-align:center;padding:28px;color:var(--t3);font-family:var(--mono);font-size:11px}
::-webkit-scrollbar{width:5px}::-webkit-scrollbar-thumb{background:#d1d5db;border-radius:3px}::-webkit-scrollbar-thumb:hover{background:#9ca3af}
.bb .fr:nth-child(even){background:rgba(0,0,0,.02)}
.bb .fr:nth-child(odd){background:var(--b1)}

#map .leaflet-tile-pane{filter:invert(1) hue-rotate(180deg) brightness(0.6) contrast(1.2) saturate(0.3)}
.spot-dd{position:absolute;z-index:100;background:var(--b1);border:1px solid var(--bd);border-radius:4px;box-shadow:0 4px 12px rgba(0,0,0,.2);padding:4px 0;max-height:200px;overflow-y:auto}
.spot-dd-item{font-family:var(--mono);font-size:9px;padding:3px 10px;cursor:pointer;color:var(--t1);white-space:nowrap}
.spot-dd-item:hover{background:var(--blue);color:#fff}
.zp{cursor:pointer;transition:color .15s}.zp:hover{color:var(--blue)!important;text-decoration:underline}
.leaflet-popup-content-wrapper{background:rgba(15,23,42,.85)!important;color:#e2e8f0!important;border-radius:6px!important;box-shadow:0 2px 8px rgba(0,0,0,.4)!important;font-size:9px!important;padding:0!important}
.leaflet-popup-content{margin:6px 8px!important;color:#e2e8f0!important}
.leaflet-popup-tip{background:rgba(15,23,42,.85)!important}
</style>
<script src="https://cdnjs.cloudflare.com/ajax/libs/three.js/r128/three.min.js"></script>
<style>
#cesium-overlay{display:none;position:fixed;top:0;left:0;right:0;bottom:0;z-index:9997;background:#050a15}
#cesiumContainer{width:100%;height:100%;overflow:hidden}
#cesium-hud{position:absolute;top:12px;left:12px;z-index:10;font-family:var(--mono)}
#cesium-close{position:absolute;top:12px;right:12px;z-index:10;background:rgba(0,0,0,.6);color:#fff;border:1px solid rgba(255,255,255,.2);border-radius:6px;padding:6px 14px;font-family:var(--mono);font-size:12px;cursor:pointer;font-weight:700}
#cesium-close:hover{background:rgba(255,255,255,.15)}
#cesium-title{background:rgba(0,0,0,.6);color:#fff;padding:6px 12px;border-radius:6px;font-size:11px;font-weight:700;border:1px solid rgba(255,255,255,.1)}
#plane-labels{position:absolute;top:0;left:0;width:100%;height:100%;pointer-events:none;z-index:50;overflow:hidden}
.plane-label{pointer-events:auto;position:absolute;font-family:monospace;font-size:9px;font-weight:700;color:#fff;background:rgba(0,0,0,.6);padding:1px 5px;border-radius:3px;white-space:nowrap;cursor:pointer;transform:translate(-50%,-100%);transition:background .15s}.plane-label:hover{background:rgba(59,130,246,.6)}
</style>
</head>
<body>
<div class="topbar">
<div class="tb-l"><div class="brand"><div class="brand-i">S</div><span class="brand-n">Skyway</span></div><div class="sep"></div><span style="font-family:var(--sans);font-size:15px;font-weight:700;color:rgba(255,255,255,.65);letter-spacing:3px;text-transform:uppercase">Signature Aviation</span><div class="sep"></div><span class="clk" id="ck">--:--:--</span><span class="ga">GA ONLY</span></div>
<div class="tb-c"><span class="apt" id="ac">KSFO</span><span class="apn" id="an">San Francisco Intl</span></div>
<div class="tb-r"><div class="pill live" id="pill"><div class="dot"></div><span id="pt">CONNECTING</span></div><div class="sep"></div><div class="fd on" id="fo"><div class="d"></div>OPENSKY</div><div class="fd off" id="fadsb"><div class="d"></div>ADSB</div><div class="fd on" id="ffa"><div class="d"></div>FLIGHTAWARE</div><div class="fd off" id="fs"><div class="d"></div>SWIM</div><div class="sep"></div><button class="ib" onclick="doRefresh()">⟳</button><button class="ib" onclick="doCenter()">⊕</button></div>
</div>
<div class="map-row" id="mapRow">
<div class="map-area"><div id="map"></div>
<div class="hud"><div class="hc b"><span class="v" id="ht">0</span><span class="l">Arr &lt;60m</span></div><div class="hc a" style="cursor:pointer" onclick="showGround()"><span class="v" id="hg">0</span><span class="l">On Ground</span></div><div class="hc c"><span class="v" id="hd">0</span><span class="l">Dep <60m</span></div><div class="hc" style="cursor:pointer;background:rgba(59,130,246,.15);border-color:rgba(59,130,246,.3)" onclick="showRampView()"><span class="v" style="font-size:14px">🗺</span><span class="l">Ramp View</span></div></div>
</div>
<div class="chart-area">
<div style="font-family:var(--mono);font-size:8px;font-weight:700;text-transform:uppercase;letter-spacing:1px;color:var(--t3);margin-bottom:6px;display:flex;justify-content:space-between;align-items:center"><span>Hourly Forecast</span><span style="display:flex;gap:12px;font-size:7px"><span style="display:flex;align-items:center;gap:3px"><span style="width:6px;height:6px;border-radius:1px;background:#3b82f6"></span>Arr</span><span style="display:flex;align-items:center;gap:3px"><span style="width:6px;height:6px;border-radius:1px;background:#dc2626"></span>Dep</span></span></div>
<div id="chartBars" style="flex:1;display:flex;align-items:stretch;gap:2px"></div>
</div>
</div>
<div id="gnd-overlay" style="display:none;position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,.3);z-index:9999" onclick="if(event.target===this)closeGround()">
<div style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);width:90%;max-width:900px;max-height:85vh;background:var(--b1);border:1px solid var(--bd);border-radius:10px;overflow:hidden;box-shadow:0 20px 60px rgba(0,0,0,.2)">
<div style="display:flex;align-items:center;justify-content:space-between;padding:12px 20px;border-bottom:1px solid var(--bd);background:var(--b0)"><span style="font-family:var(--mono);font-size:13px;font-weight:700;color:var(--t1)">✈ Aircraft On Ground at KSFO</span><button onclick="closeGround()" style="background:none;border:none;font-size:18px;cursor:pointer;color:var(--t3)">✕</button></div>
<div id="gnd-table" style="padding:10px 16px;overflow-y:auto;max-height:calc(85vh - 60px)"></div>
</div>
</div>
<div id="cesium-overlay">
<div id="cesiumContainer"></div>
<div id="cesium-hud"><span id="cesium-title">SKYWAY 3D — KSFO Airspace</span></div>
<button id="cesium-close" onclick="close3D()">✕ Close</button>
</div>
<div id="ramp-overlay" style="display:none;position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,.5);z-index:9998" onclick="if(event.target===this)closeRamp()">
<div style="position:absolute;top:2%;left:2%;right:2%;bottom:2%;background:var(--b1);border:1px solid var(--bd);border-radius:12px;overflow:hidden;box-shadow:0 20px 60px rgba(0,0,0,.3);display:flex;flex-direction:column">
<div style="display:flex;align-items:center;justify-content:space-between;padding:10px 16px;border-bottom:1px solid var(--bd);background:var(--b0);flex-shrink:0">
<span style="font-family:var(--mono);font-size:13px;font-weight:700;color:var(--t1)">🗺 RAMP VIEW — Signature SFO</span>
<div style="display:flex;gap:8px;align-items:center">
<span style="font-family:var(--mono);font-size:9px;color:var(--t3)">Drag planes to reassign spots</span>
<button onclick="closeRamp()" style="background:none;border:none;font-size:18px;cursor:pointer;color:var(--t3)">✕</button>
</div>
</div>
<div id="rampMap" style="flex:1"></div>
</div>
</div>
<div class="resize-bar" id="resizeBar"><div class="resize-grip"></div></div>
<div class="boards">
<div class="board"><div class="bh"><div class="bt ar">🛬 En Route / Scheduled to SFO</div><span class="bc" id="anc">0</span></div><div class="cols ca"><span>Ident</span><span>Type</span><span>From</span><span>Depart</span><span>Arrive</span><span>ETA</span><span>Spot</span><span>PAX</span></div><div class="bb" id="ab"></div></div>
<div class="board"><div class="bh"><div class="bt de">🛫 Scheduled Departures</div><span class="bc" id="dnc">0</span></div><div class="cols cd"><span>Ident</span><span>Type</span><span>To</span><span>Depart</span><span>ETD</span><span>Spot</span><span>Cat</span><span>CIP</span><span>Fuel</span><span>Lav</span><span>H2O</span></div><div class="bb" id="db"></div></div>
</div>
<div class="boards done-boards">
<div class="board"><div class="bh done-hdr" id="ah" style="display:none"><div class="bt ar" style="opacity:.5">✓ Arrived</div><span class="bc" id="adc">0</span></div><div class="cols ca-done" id="ahcols" style="display:none"><span>Ident</span><span>Type</span><span>From</span><span>Depart</span><span>Arrive</span><span>Time</span><span>Spot</span></div><div class="bb" id="adb" style="display:none"></div></div>
<div class="board"><div class="bh done-hdr" id="dh" style="display:none"><div class="bt de" style="opacity:.5">✓ Departed</div><span class="bc" id="ddc">0</span></div><div class="cols cd-done" id="dhcols" style="display:none"><span>Ident</span><span>Type</span><span>To</span><span>Depart</span><span>Time</span><span>Spot</span></div><div class="bb" id="ddb" style="display:none"></div></div>
</div>
</body>
<` + `script>
// === SKYWAY DASHBOARD ===
var MODEL={C172:'Skyhawk',C182:'Skylane',C206:'Stationair',C208:'Caravan',C210:'Centurion',C25A:'CJ2',C25B:'CJ3',C25C:'CJ4',C500:'Citation I',C510:'Mustang',C525:'CJ1',C550:'Citation II',C560:'Citation V',C56X:'Excel',C680:'Sovereign',C68A:'Latitude',C700:'Longitude',C750:'Citation X',CL30:'Challenger 300',CL35:'Challenger 350',CL60:'Challenger 600',CRJ2:'CRJ-200',E35L:'Legacy 600',E545:'Legacy 450',E550:'Praetor 600',E55P:'Phenom 300',EA50:'Eclipse 500',F2TH:'Falcon 2000',F900:'Falcon 900',FA6X:'Falcon 6X',FA7X:'Falcon 7X',FA8X:'Falcon 8X',G150:'G150',G200:'G200',G280:'G280',GA4C:'G400',GA5C:'G500',GA6C:'G600',GA7C:'G700',GA8C:'G800',GALX:'Galaxy',GL5T:'Global 5500',GL7T:'Global 7500',GLEX:'G650',GLF2:'GII',GLF3:'GIII',GLF4:'GIV',GLF5:'GV/G550',GLF6:'G650/G650ER',GX6C:'Global 6500',H25B:'Hawker 800',HA4T:'HondaJet',HDJT:'HondaJet',LJ35:'Learjet 35',LJ45:'Learjet 45',LJ60:'Learjet 60',LJ75:'Learjet 75',PA28:'Cherokee',PA32:'Saratoga',PA46:'Malibu',PC12:'PC-12',PC24:'PC-24',PRM1:'Premier I',SF50:'Vision Jet',SR22:'SR22',SR20:'SR20',TBM7:'TBM 700',TBM8:'TBM 850',TBM9:'TBM 900',B350:'King Air 350',BE20:'King Air 200',BE36:'Bonanza',BE40:'Beechjet 400',BE58:'Baron',BE9L:'King Air C90',ASTR:'Astra',B06:'JetRanger',EC35:'EC135',EC45:'EC145',S76:'S-76',A139:'AW139'};
var HELI={S76:1,EC35:1,EC45:1,B06:1,A139:1,AS50:1,AS55:1,EC30:1,EC55:1,H60:1,R22:1,R44:1,R66:1,BK17:1,B407:1,B412:1,B429:1,H500:1,MD52:1,MD60:1,AS65:1,S92:1,AW09:1,AW69:1,AW18:1,H135:1,H145:1,H160:1,H175:1,H215:1,H225:1,B505:1,B105:1,B212:1};
var SPAN={GL7T:104,GL5T:94,GLEX:99,GLF6:99,GLF5:93,GLF4:78,GLF3:78,GLF2:69,GA7C:103,GA8C:103,GA6C:94,GA5C:87,GA4C:78,G280:63,G200:58,G150:55,GX6C:94,CL30:64,CL35:69,CL60:64,E35L:69,E545:66,E550:69,E55P:52,EA50:38,F2TH:70,F900:70,FA6X:86,FA7X:86,FA8X:86,C25A:47,C25B:47,C25C:50,C500:47,C510:43,C525:47,C550:52,C560:55,C56X:56,C680:64,C68A:72,C700:69,C750:64,H25B:54,HA4T:40,HDJT:40,LJ35:44,LJ45:48,LJ60:44,LJ75:51,PC12:53,PC24:56,B350:58,BE20:55,BE40:44,BE9L:54,SF50:39,SR22:38,SR20:37,PRM1:44,TBM7:42,TBM8:42,TBM9:42,PA46:43,PA32:37,PA28:35,C172:36,C182:36,C206:36,C208:52,C210:37,B06:33,EC35:33,EC45:36,S76:44,A139:46,ASTR:55};
// Aircraft wingspan categories
function getWsCat(acType){
  var ws=SPAN[acType]||60;
  if(ws<45)return 1;if(ws<55)return 2;if(ws<70)return 3;if(ws<95)return 4;if(ws<105)return 5;return 6;
}
// Hangar tenants - specific tail numbers only
var HANGAR_B=['N650SB','N950X'];
var HANGAR_C=['N800DL','N808XX'];
// Spot definitions: name, cats allowed, priority (1=closest to FBO), notes
var SPOT_DEFS=[
  {name:'Spot A',cats:[1],pri:1,qt:true},
  {name:'Spot 1',cats:[1,2],pri:1,qt:true},
  {name:'Spot 2',cats:[1,2],pri:1,qt:true},
  {name:'Spot 3',cats:[1,2,3],pri:2,qt:true},
  {name:'Spot 4',cats:[3,4,5],pri:2,qt:true},
  {name:'Spot 5',cats:[3,4,5],pri:2,qt:true},
  {name:'Ken Salvage',cats:[2,3],pri:3,qt:false},
  {name:'2nd Line',cats:[1,2,3],pri:4,qt:false},
  {name:'Btwn Hangars 1',cats:[4,5],pri:6},
  {name:'Btwn Hangars 2',cats:[4,5],pri:6},
  {name:'Btwn Hangars 3',cats:[4,5],pri:6},
  {name:'Btwn Hangars 4',cats:[4,5],pri:6},
  {name:'Overflow',cats:[4,5,6],pri:5,qt:false},
  {name:'3rd Line',cats:[1,2,3,4],pri:5,qt:false},
  {name:'The Shop',cats:[4,5],pri:6,qt:false},
  {name:'Airfield Safety',cats:[4,5,6],pri:6,qt:false},
  {name:'The Island',cats:[4,5],pri:7,qt:false,note:'tow only'},
  {name:'The Fence',cats:[1,2],pri:7,qt:false,note:'tow only'},
  {name:'42 West 1',cats:[4,5],pri:8},
  {name:'42 West 2',cats:[4,5],pri:8},
  {name:'42 West 3',cats:[4,5],pri:8},
  {name:'42 West 4',cats:[4,5],pri:8},
  {name:'4th Line',cats:[4,5,6],pri:9,qt:false,note:'call United/Airfield Safety to reserve'}
];
// Global spot tracker - shared across all views
if(!window._globalSpotMap)window._globalSpotMap={};
function getNextAvailable(preferred,cat){
  // If preferred is free, use it
  if(!window._globalSpotMap[preferred])return preferred;
  // Find next spot of same line/type that fits this cat
  for(var i=0;i<SPOT_DEFS.length;i++){
    var sd=SPOT_DEFS[i];
    if(sd.cats.indexOf(cat)>=0&&!window._globalSpotMap[sd.name])return sd.name;
  }
  return preferred; // fallback
}
function suggestSpot(acType,stayHrs,isHeli,tailNum){
  if(isHeli)return {spot:getNextAvailable('42 West 1',5),tow:''};
  var tail=(tailNum||'').toUpperCase();
  if(tail==='N650SB'||tail==='N950X')return {spot:'Hangar B',tow:''};
  if(tail==='N800DL'||tail==='N808XX')return {spot:'Hangar C',tow:''};
  var cat=getWsCat(acType);
  var safeT=(tailNum||'').replace(/[^a-zA-Z0-9]/g,'');
  // Check manual assignment first
  if(window._parkingAssignments&&window._parkingAssignments[safeT]){
    var ms=window._parkingAssignments[safeT];
    window._globalSpotMap[ms]=safeT;
    return {spot:ms,tow:''};
  }
  var spot='',tow='';
  // Quick turn (< 2hrs)
  if(stayHrs<=2){
    if(cat<=1)spot=getNextAvailable('Spot A',cat);
    else if(cat<=2)spot=getNextAvailable('Spot 1',cat);
    else if(cat<=3)spot=getNextAvailable('Spot 3',cat);
    else if(cat<=5)spot=getNextAvailable('Spot 4',cat);
    else spot=getNextAvailable('41-7 A',cat);
  }
  // Medium stay (2-8hrs)
  else if(stayHrs<=8){
    if(cat<=3)spot=getNextAvailable('2nd Line 1',cat);
    else if(cat<=5)spot=getNextAvailable('Btwn Hangars 1',cat);
    else spot=getNextAvailable('41-7 A',cat);
  }
  // Long stay (8-24hrs)
  else if(stayHrs<=24){
    if(cat<=3)spot=getNextAvailable('3rd Line 1',cat);
    else if(cat<=5)spot=getNextAvailable('4th Line 1',cat);
    else spot=getNextAvailable('41-7 A',cat);
  }
  // Very long stay (24hrs+)
  else{
    if(cat<=2){spot=getNextAvailable('3rd Line 1',cat);tow='Fence if needed';}
    else if(cat<=3)spot=getNextAvailable('3rd Line 1',cat);
    else if(cat<=5){spot=getNextAvailable('42 West 1',cat);tow='Island if full';}
    else spot=getNextAvailable('41-7 A',cat);
  }
  // Mark spot as taken
  window._globalSpotMap[spot]=safeT;
  return {spot:spot,tow:tow};
}
function getFlag(code){
  if(!code||code.length<2)return'';
  var p=code.substring(0,1).toUpperCase();
  var FLAGS={K:'🇺🇸',P:'🇺🇸',C:'🇨🇦',M:'🇲🇽',T:'🇲🇽',L:'🇪🇺',E:'🇪🇺',U:'🇷🇺',Z:'🇨🇳',R:'🇰🇷',V:'🇦🇺',S:'🇧🇷',Y:'🇦🇺',O:'🇯🇵',W:'🇮🇩',F:'🇿🇦',H:'🇪🇬',D:'🇩🇪',B:'🇮🇨'};
  // More specific 2-char prefixes
  var p2=code.substring(0,2).toUpperCase();
  var F2={EG:'🇬🇧',LF:'🇫🇷',ED:'🇩🇪',LI:'🇮🇹',LE:'🇪🇸',EH:'🇳🇱',EB:'🇧🇪',LS:'🇨🇭',LO:'🇦🇹',EK:'🇩🇰',EN:'🇳🇴',ES:'🇸🇪',EF:'🇫🇮',EI:'🇮🇪',LP:'🇵🇹',LG:'🇬🇷',LT:'🇹🇷',LK:'🇨🇿',EP:'🇵🇱',LH:'🇭🇺',LR:'🇷🇴',OE:'🇦🇪',OB:'🇧🇭',OK:'🇰🇼',OI:'🇮🇷',OL:'🇱🇧',OJ:'🇯🇴',LL:'🇮🇱',OO:'🇸🇦',OP:'🇵🇰',VI:'🇮🇳',VE:'🇮🇳',VA:'🇮🇳',RJ:'🇯🇵',RK:'🇰🇷',RC:'🇹🇼',VH:'🇭🇰',WS:'🇸🇬',ZS:'🇨🇳',ZB:'🇨🇳',ZG:'🇨🇳',PH:'🇺🇸',PA:'🇺🇸',SB:'🇧🇷',SC:'🇨🇱',SK:'🇨🇴',SE:'🇪🇨',SP:'🇵🇪',SV:'🇻🇪',TJ:'🇵🇷',TN:'🇦🇼',MK:'🇯🇲',MM:'🇲🇽',MU:'🇨🇺',MY:'🇧🇸',NT:'🇵🇫',NZ:'🇳🇿',YM:'🇦🇺',CY:'🇨🇦',CZ:'🇨🇦',FA:'🇿🇦',FI:'🇿🇦',DN:'🇳🇬',HA:'🇪🇹',HR:'🇪🇬',HB:'🇪🇹',HK:'🇰🇪'};
  return F2[p2]||FLAGS[p]||'';
}
function opBadge(op){
  if(!op)return'';
  var colors={NetJets:'#1a365d','NetJets EU':'#1a365d',Flexjet:'#8b0000',VistaJet:'#c41e3a',XO:'#000',Solairus:'#2d5a88','Wheels Up':'#1a1a2e','Clay Lacy':'#0a3d62','Jet Linx':'#1e3a5f',JetEdge:'#333'};
  var c=colors[op]||'#4a5568';
  return '<span style="display:inline-block;font-family:var(--sans);font-size:7px;font-weight:700;color:#fff;background:'+c+';padding:1px 4px;border-radius:3px;margin-right:3px;letter-spacing:.3px;line-height:1.3;vertical-align:middle">'+op+'</span>';
}
var AIRLINES=['AAL','ACA','AFR','AIC','AMX','ANA','ANZ','ASA','AWE','BAW','BER','CAL','CCA','CES','CLX','CPA','CSN','DAL','DLH','EIN','ETD','ETH','EVA','FDX','FFT','FIN','GIA','HAL','IBE','ICE','JAL','JBU','KAL','KLM','LAN','LOT','MEA','NAX','NKS','OAL','PAL','QFA','QTR','RAM','RPA','RYR','SAS','SAA','SIA','SKW','SLK','SWA','SWR','TAM','TAP','THA','THY','TSC','TUI','TVF','UAE','UAL','UPS','USA','VIR','VOI','WJA','ENY','PDT','ROU','JZA','GJS','OHY','XJT','TCF','AIP','CPZ','TRS','SCX','FLG'];
var AL=new Set(AIRLINES);
var CATS_GA=[2,3,8,9,10,12];
var CATS_MAYBE=[4];
var CATS_NO=[5,6,7];
function isGA(cs,cat){
  var c=(cs||'').toUpperCase().trim();
  var p=c.substring(0,3);
  var isAL=AL.has(p);
  if(cat>0){
    if(CATS_NO.indexOf(cat)>=0)return false;
    if(CATS_GA.indexOf(cat)>=0)return !isAL;
    if(CATS_MAYBE.indexOf(cat)>=0)return !isAL;
  }
  if(!c)return false;
  if(c.charAt(0)==='N'&&c.length>1&&c.charAt(1)>='0'&&c.charAt(1)<='9')return true;
  if(isAL)return false;
  return true;
}

var APT={KSFO:{lat:37.621,lon:-122.379,n:'San Francisco Intl'},KJFK:{lat:40.641,lon:-73.778,n:'JFK Intl'},KLAX:{lat:33.943,lon:-118.408,n:'Los Angeles Intl'},KORD:{lat:41.974,lon:-87.907,n:'Chicago OHare'},KDEN:{lat:39.856,lon:-104.674,n:'Denver Intl'},KSEA:{lat:47.450,lon:-122.309,n:'Seattle-Tacoma'}};
var airport='KSFO',radius=200;
var leafMap,markers={},allAC=[],gaAC=[],trackHistory={},trackLines={},faMapSet={},identToMarkerId={};

function buildFaMapSet(){
  faMapSet={};
  Promise.all([
    fetch('/fa/arrivals').then(function(r){return r.json();}),
    fetch('/fa/departures').then(function(r){return r.json();})
  ]).then(function(res){
    var arr=res[0]||[],dep=res[1]||[];
    var now2=Date.now();
    for(var i=0;i<arr.length;i++){
      var f=arr[i];
      if(f.arrived){var at=f.arriveISO?new Date(f.arriveISO).getTime():0;if(at>0&&(now2-at)>300000)continue;}
      var info={type:'arr',ident:f.ident,callsign:f.callsign,acType:f.type,from:f.from,city:f.city,country:f.country,to:'KSFO',depart:f.depart,arrive:f.arrive,departISO:f.departISO,arriveISO:f.arriveISO};
      if(f.ident){faMapSet[f.ident.toUpperCase()]=info;faMapSet[f.ident.toUpperCase().replace(/[^A-Z0-9]/g,'')]=info;}
      if(f.callsign){faMapSet[f.callsign.toUpperCase()]=info;faMapSet[f.callsign.toUpperCase().replace(/[^A-Z0-9]/g,'')]=info;}
    }
    for(var i=0;i<dep.length;i++){
      var f=dep[i];
      if(f.departed){var dt=f.departISO?new Date(f.departISO).getTime():0;if(dt>0&&(now2-dt)>300000)continue;}
      var info2={type:'dep',ident:f.ident,callsign:f.callsign,acType:f.type,from:'KSFO',to:f.to,city:f.city,country:f.country,depart:f.depart,arrive:f.arrive,departISO:f.departISO,arriveISO:f.arriveISO};
      if(f.ident&&!faMapSet[f.ident.toUpperCase()]){faMapSet[f.ident.toUpperCase()]=info2;faMapSet[f.ident.toUpperCase().replace(/-/g,'')]=info2;}
      if(f.callsign&&!faMapSet[f.callsign.toUpperCase()]){faMapSet[f.callsign.toUpperCase()]=info2;faMapSet[f.callsign.toUpperCase().replace(/-/g,'')]=info2;}
    }
  }).catch(function(){});
}

window.onload=function(){
  document.getElementById('ac').textContent=airport;
  var a=APT[airport];if(a)document.getElementById('an').textContent=a.n;
  initMap();
  fetchBoards();
  buildFaMapSet();
  // Delay first map refresh to let faMapSet populate
  setTimeout(function(){buildFaMapSet();setTimeout(refresh,2000);},3000);
  setInterval(refresh,10000);
  setInterval(fetchBoards,30000);
  setInterval(buildFaMapSet,15000);
  // Spot click - inline dropdown
  document.addEventListener('click',function(e){
    var el=e.target.closest('.spot-click');
    // Close any existing dropdown first
    var old=document.querySelector('.spot-dd');if(old)old.remove();
    if(!el)return;
    e.stopPropagation();
    var planeId=el.dataset.plane;
    var rect=el.getBoundingClientRect();
    var dd=document.createElement('div');dd.className='spot-dd';
    dd.style.left=rect.left+'px';dd.style.top=rect.bottom+'px';
    var spotNames=['Spot A','Spot 1','Spot 2','Spot 3','Spot 4','Spot 5','Ken Salvage','2nd Line 1','2nd Line 2','2nd Line 3','2nd Line 4','Overflow 1','Overflow 2','Btwn Hangars 1','Btwn Hangars 2','Btwn Hangars 3','Btwn Hangars 4','3rd Line 1','3rd Line 2','3rd Line 3','3rd Line 4','3rd Line 5','3rd Line 6','3rd Line 7','3rd Line 8','The Shop','Airfield Safety 1','Airfield Safety 2','4th Line 1','4th Line 2','4th Line 3','4th Line 4','41-7 A','41-7 B','41-11','42 West 1','42 West 2','42 West 3','42 West 4','The Island','The Fence','Hangar A','Hangar B','Hangar C'];
    for(var i=0;i<spotNames.length;i++){
      var item=document.createElement('div');item.className='spot-dd-item';
      item.textContent=spotNames[i];item.dataset.spot=spotNames[i];
      item.addEventListener('click',function(ev){
        ev.stopPropagation();
        var newSpot=ev.target.dataset.spot;
        if(!window._parkingAssignments)window._parkingAssignments={};
        window._parkingAssignments[planeId]=newSpot;
        el.innerHTML=newSpot;
        dd.remove();
      });
      dd.appendChild(item);
    }
    document.body.appendChild(dd);
    // Close on outside click
    setTimeout(function(){
      document.addEventListener('click',function closeDD(){dd.remove();document.removeEventListener('click',closeDD);},{once:true});
    },10);
  });
  // Hover delegation for tail number zoom-to-map
  var hoverTimer=null;
  document.addEventListener('mouseover',function(e){
    var el=e.target.closest('.zp');
    if(el&&el.dataset.zp){
      clearTimeout(hoverTimer);
      hoverTimer=setTimeout(function(){zoomToPlane(el.dataset.zp);},200);
    }
  });
  document.addEventListener('mouseout',function(e){
    var el=e.target.closest('.zp');
    if(el&&el.dataset.zp){
      clearTimeout(hoverTimer);
      hoverTimer=setTimeout(function(){resetMapView();},500);
    }
  });
  // Click still works for immediate zoom
  document.addEventListener('click',function(e){
    var el=e.target.closest('.zp');
    if(el&&el.dataset.zp){e.stopPropagation();zoomToPlane(el.dataset.zp);}
  });

  setInterval(buildFaMapSet,30000);
  setInterval(function(){document.getElementById('ck').textContent=new Date().toLocaleTimeString('en-US',{hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false})},1000);
};

function getAP(){return APT[airport]||{lat:37.621,lon:-122.379,n:airport};}
function doCenter(){var a=getAP();leafMap.setView([a.lat,a.lon],10);}
function doRefresh(){refresh();}

function initMap(){
  var a=getAP();
  leafMap=L.map('map',{center:[a.lat,a.lon],zoom:7,zoomControl:false,attributionControl:false});
  
  // Base map: CartoDB Voyager - blue ocean, terrain hints, city labels
  window._tileLayer=L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{maxZoom:18}).addTo(leafMap);
  window._darkTileUrl='https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png';
  window._lightTileUrl='https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png';
  // Weather radar overlay from RainViewer (free, no API key)
  window._radarLayer=null;
  function loadRadar(){
    fetch('https://api.rainviewer.com/public/weather-maps.json')
    .then(function(r){return r.json();})
    .then(function(d){
      if(d&&d.radar&&d.radar.past&&d.radar.past.length>0){
        var latest=d.radar.past[d.radar.past.length-1].path;
        if(window._radarLayer)leafMap.removeLayer(window._radarLayer);
        window._radarLayer=L.tileLayer('https://tilecache.rainviewer.com'+latest+'/256/{z}/{x}/{y}/6/1_1.png',{opacity:0.2,maxZoom:18,zIndex:10}).addTo(leafMap);
      }
    }).catch(function(e){console.log('Radar fetch error:',e);});
  }
  loadRadar();
  // 3D view button
  var btn3d=L.DomUtil.create('div');
  btn3d.innerHTML='<button onclick="show3D()" style="font-family:var(--mono);font-size:10px;font-weight:700;color:#fff;background:rgba(59,130,246,.8);border:1px solid rgba(59,130,246,.4);border-radius:6px;padding:5px 10px;cursor:pointer;box-shadow:0 2px 6px rgba(0,0,0,.3)">🌐 3D View</button>';
  btn3d.style.cssText='position:absolute;bottom:12px;left:12px;z-index:1000';
  document.getElementById('map').appendChild(btn3d);
  setInterval(loadRadar,300000); // refresh radar every 5 min
  // Lightning overlay from Blitzortung
  window._lightningLayer=L.tileLayer('https://map.blitzortung.org/GETlightning.php?&north={n}&south={s}&east={e}&west={w}&z={z}&x={x}&y={y}',{opacity:0.7,maxZoom:18,zIndex:11,attribution:''});
  // Use simpler lightning tile approach
  window._lightningLayer=L.tileLayer('https://tiles.blitzortung.org/strikes/1/{z}/{x}/{y}.png',{opacity:0.3,maxZoom:18,zIndex:11}).addTo(leafMap);
  // KSFO marker
  var sfoIcon=L.divIcon({className:'',html:'<div style="width:8px;height:8px;background:#1e3a5f;border:2px solid #fff;border-radius:50%;box-shadow:0 0 6px rgba(30,58,95,.4)"></div>',iconSize:[8,8],iconAnchor:[4,4]});
  L.marker([a.lat,a.lon],{icon:sfoIcon,interactive:false}).addTo(leafMap);
  // Range rings - clean, minimal
  var rings=[5,20,50];
  var ringColors=['rgba(100,116,139,.2)','rgba(100,116,139,.15)','rgba(100,116,139,.1)'];
  var ringWeights=[0.6,0.5,0.5];
  for(var r=0;r<rings.length;r++){
    L.circle([a.lat,a.lon],{radius:rings[r]*1852,color:ringColors[r],fillColor:'transparent',fillOpacity:0,weight:ringWeights[r],opacity:1,dashArray:'4,6'}).addTo(leafMap);
    var labelLat=a.lat+(rings[r]/60);
    var labelIcon=L.divIcon({className:'',html:'<span style="font-family:var(--mono);font-size:7px;font-weight:600;color:rgba(148,163,184,.5);padding:0 2px">'+rings[r]+'</span>',iconSize:[20,10],iconAnchor:[10,5]});
    L.marker([labelLat,a.lon],{icon:labelIcon,interactive:false}).addTo(leafMap);
  }
}

function bbox(){
  var a=getAP(),dLat=radius/60,dLon=radius/(60*Math.cos(a.lat*Math.PI/180));
  return{la1:(a.lat-dLat).toFixed(4),la2:(a.lat+dLat).toFixed(4),lo1:(a.lon-dLon).toFixed(4),lo2:(a.lon+dLon).toFixed(4)};
}

function refresh(){
  var b=bbox();
  fetch('/osky/states/all?extended=1&lamin='+b.la1+'&lomin='+b.lo1+'&lamax='+b.la2+'&lomax='+b.lo2)
  .then(function(r){if(!r.ok)throw new Error(r.status);return r.json();})
  .then(function(d){
    if(!d.states||!d.states.length){setPill('err','NO DATA');return;}
    allAC=[];
    for(var i=0;i<d.states.length;i++){
      var s=d.states[i];
      allAC.push({id:s[0],cs:(s[1]||'').trim(),co:s[2],lon:s[5],lat:s[6],alt:s[7],gnd:s[8],vel:s[9],trk:s[10],vr:s[11],sq:s[14],cat:s[17]||0,
        ft:s[7]!=null?Math.round(s[7]*3.28084):null,
        kts:s[9]!=null?Math.round(s[9]*1.94384):null,
        fpm:s[11]!=null?Math.round(s[11]*196.85):null});
    }
    drawMap(allAC);
    drawHUD();
    setPill('live','LIVE');
    document.getElementById('fo').className='fd on';
    document.getElementById('fadsb').className='fd on';
  })
  .catch(function(e){
    console.error('Fetch error:',e);
    setPill('err','ERROR: '+e.message);
    document.getElementById('fo').className='fd off';
  });
}

function drawMap(ac){
  var fmKeys=Object.keys(faMapSet);
  console.log('[MAP] faMapSet keys:',fmKeys.length,'aircraft from OpenSky:',ac.length);
  if(fmKeys.length===0){console.log('[MAP] faMapSet empty, skipping');return;}
  if(fmKeys.length<20)console.log('[MAP] faMapSet sample:',fmKeys.slice(0,10).join(', '));
  
  var filtered=ac.filter(function(a){
    if(a.lat==null||a.lon==null)return false;
    var cs=(a.cs||'').toUpperCase().replace(/ /g,'');
    if(cs&&faMapSet[cs])return true;
    return false;
  });
  console.log('[MAP] matched:',filtered.length);

  var cur={};
  for(var i=0;i<filtered.length;i++)cur[filtered[i].id]=true;
  for(var k in markers){if(!cur[k]){leafMap.removeLayer(markers[k]);delete markers[k];if(trackLines[k]){leafMap.removeLayer(trackLines[k]);delete trackLines[k];}delete trackHistory[k];}}
  for(var i=0;i<filtered.length;i++){
    var a=filtered[i];
    var h=a.trk||0,alt=a.ft||0;
    var cs=a.cs||a.id;
    var fi=faMapSet[(cs||'').toUpperCase().replace(/ /g,'')];
    var faType=fi?fi.type:'';
    var col=faType==='arr'?'#f59e0b':(faType==='dep'?'#dc2626':'#64748b');
    var sz=a.gnd?22:32;
    var path='M12 1.5C12.4 1.5 12.7 2.5 12.8 4L13 7.5L19.5 11.5C20 11.8 20 12.2 19.5 12.5L13 11L13.2 18L15.5 20C15.8 20.2 15.8 20.6 15.5 20.8L12 19.5L8.5 20.8C8.2 20.6 8.2 20.2 8.5 20L10.8 18L11 11L4.5 12.5C4 12.2 4 11.8 4.5 11.5L11 7.5L11.2 4C11.3 2.5 11.6 1.5 12 1.5Z';
    // Label: tail + altitude/speed
    var label=cs;
    var altSpd='';
    if(!a.gnd&&alt>0)altSpd=(alt>=1000?Math.round(alt/100)+'00':''+alt)+'ft '+(a.kts||0)+'kt';
    var icon=L.divIcon({className:'',html:'<div style="position:relative"><svg width="'+sz+'" height="'+sz+'" viewBox="0 0 24 24" style="transform:rotate('+Math.round(h)+'deg);filter:drop-shadow(0 1px 2px rgba(0,0,0,.2))"><path d="'+path+'" fill="'+col+'" stroke="#fff" stroke-width="1.2" stroke-linejoin="round"/></svg><div style="position:absolute;left:'+(sz+3)+'px;top:-3px;white-space:nowrap"><div style="font-family:JetBrains Mono,monospace;font-size:9px;font-weight:800;color:'+col+';text-shadow:-1px 0 2px #fff,1px 0 2px #fff,0 -1px 2px #fff,0 1px 2px #fff;line-height:1.1">'+label+'</div>'+(altSpd?'<div style="font-family:JetBrains Mono,monospace;font-size:7px;font-weight:600;color:#64748b;text-shadow:-1px 0 1px #fff,1px 0 1px #fff;line-height:1.1">'+altSpd+'</div>':'')+'</div></div>',iconSize:[sz+90,sz+10],iconAnchor:[sz/2,sz/2]});
    // Track line
    if(!trackHistory[a.id])trackHistory[a.id]=[];
    var th=trackHistory[a.id];
    if(th.length===0||th[th.length-1][0]!==a.lat||th[th.length-1][1]!==a.lon){th.push([a.lat,a.lon]);if(th.length>60)th.shift();}
    if(th.length>1&&!a.gnd){
      if(trackLines[a.id])trackLines[a.id].setLatLngs(th);
      else trackLines[a.id]=L.polyline(th,{color:col,weight:2,opacity:.4,dashArray:'6,4'}).addTo(leafMap);
    }
    // Rich popup
    var popParts='<div style="font-family:monospace;font-size:9px;line-height:1.5;padding:2px">';
    popParts+='<b style="color:'+col+'">'+cs+'</b>';
    if(fi){
      popParts+=' <span style="color:#9ca3af;font-size:8px">'+(fi.acType||'')+'</span>';
      popParts+='<br><span style="color:#9ca3af">'+((fi.from||'?')+' → '+(fi.to||'?'))+'</span>';
    }
    popParts+='<br><span style="color:#9ca3af">'+(alt>0?alt.toLocaleString()+'ft':'GND')+' '+((a.kts||0))+'kts</span>';
    popParts+='</div>';
    if(markers[a.id]){markers[a.id].setLatLng([a.lat,a.lon]);markers[a.id].setIcon(icon);markers[a.id].setPopupContent(popParts);}
    else{markers[a.id]=L.marker([a.lat,a.lon],{icon:icon}).addTo(leafMap).bindPopup(popParts);}
    // Map callsign/ident to marker ID for click-to-zoom
    var csUp=(a.cs||'').toUpperCase().replace(/ /g,'');
    if(csUp)identToMarkerId[csUp]=a.id;
    if(fi&&fi.ident)identToMarkerId[fi.ident.toUpperCase().replace(/[^A-Z0-9]/g,'')]=a.id;
    if(fi&&fi.callsign)identToMarkerId[fi.callsign.toUpperCase().replace(/[^A-Z0-9]/g,'')]=a.id;
    if(fi&&fi.registration)identToMarkerId[fi.registration.toUpperCase().replace(/[^A-Z0-9]/g,'')]=a.id;
    // Also map without dashes
    if(csUp)identToMarkerId[csUp.replace(/-/g,'')]=a.id;
  }
}
function zoomToPlane(ident){
  if(!ident)return;
  var key=ident.toUpperCase().replace(/[^A-Z0-9]/g,'');
  var mid=identToMarkerId[key];
  if(!mid){
    // Try without leading N for US registrations
    var noN=key.replace(/^N/,'');
    if(noN&&identToMarkerId[noN])mid=identToMarkerId[noN];
  }
  if(!mid){
    // Try numeric part only - N816QS -> 816QS, EJA816 -> 816
    var nums=key.replace(/^[A-Z]+/,'');
    if(nums.length>=3){
      for(var k in identToMarkerId){
        var kNums=k.replace(/^[A-Z]+/,'');
        if(kNums===nums||k.indexOf(nums)>=0){mid=identToMarkerId[k];break;}
      }
    }
  }
  if(!mid){
    // Brute force - check all markers for matching callsign in popup
    for(var k in identToMarkerId){if(k.indexOf(key)>=0||key.indexOf(k)>=0){mid=identToMarkerId[k];break;}}
  }
  if(mid&&markers[mid]){
    var ll=markers[mid].getLatLng();
    var ksfo=L.latLng(37.6213,-122.3790);
    // Calculate bounds that include both the plane and KSFO
    var bounds=L.latLngBounds([ll,ksfo]);
    leafMap.fitBounds(bounds.pad(0.15),{animate:true,maxZoom:13});
    markers[mid].openPopup();
    // Flash effect
    var el=markers[mid].getElement();
    if(el){el.style.filter='brightness(2) drop-shadow(0 0 8px #fff)';setTimeout(function(){el.style.filter='';},2000);}
  } else {
    // Plane not on map - don't scroll, just log
    console.log('Plane not found on map:',ident);
  }
}
// Reset map on mouse leave
function resetMapView(){
  var a=getAP();
  leafMap.setView([a.lat,a.lon],8,{animate:true,duration:0.3});
}
function altCol(a,g){if(g||a<=0)return'#1e40af';if(a<5000)return'#16a34a';if(a<12000)return'#d97706';if(a<25000)return'#2563eb';return'#7c3aed';}

// Color assignment for board<->map matching
var colorPalette=['#e63946','#457b9d','#2a9d8f','#e9c46a','#f4a261','#264653','#6a4c93','#1982c4','#8ac926','#ff595e','#ff924c','#ffca3a','#c77dff','#72efdd','#06d6a0','#118ab2','#ef476f','#ffd166','#073b4c','#48cae4'];
var identColorMap={};
var colorIdx=0;
function getIdentColor(ident){
  if(!ident)return null;
  if(identColorMap[ident])return identColorMap[ident];
  identColorMap[ident]=colorPalette[colorIdx%colorPalette.length];
  colorIdx++;
  return identColorMap[ident];
}
// Match OpenSky callsign to FlightAware ident
var faIdents={};
function acColor(cs,icao){
  var up=(cs||'').toUpperCase().replace(/ /g,'');
  if(faIdents[up])return faIdents[up];
  // Try icao24 hex match
  var lo=(icao||'').toLowerCase();
  if(faIdents[lo])return faIdents[lo];
  return null;
}

function drawHUD(){
  Promise.all([
    fetch('/fa/arrivals').then(function(r){return r.json();}),
    fetch('/fa/departures').then(function(r){return r.json();})
  ]).then(function(res){
    var arr=res[0]||[],dep=res[1]||[];
    var now=Date.now(),h60=now+3600000;
    // Build set of idents that have departed
    var departedSet={};
    for(var i=0;i<dep.length;i++){
      if(dep[i].departed&&dep[i].ident)departedSet[dep[i].ident.toUpperCase()]=true;
    }
    var next60=0,onGnd=0;
    for(var i=0;i<arr.length;i++){
      var f=arr[i];
      if(f.arrived){
        // Only count as on ground if NOT in departed set
        var id=(f.ident||'').toUpperCase();
        if(!departedSet[id])onGnd++;
        continue;
      }
      if(f.arriveISO){var t=new Date(f.arriveISO).getTime();if(t>now&&t<=h60)next60++;}
    }
    document.getElementById('ht').textContent=next60;
    document.getElementById('hg').textContent=onGnd;
    // Departing count: next 60 min, not departed
    var depCount=0;
    for(var i=0;i<dep.length;i++){if(!dep[i].departed&&dep[i].departISO){var t2=new Date(dep[i].departISO).getTime();if(t2>now&&t2<=h60)depCount++;}}
    document.getElementById('hd').textContent=depCount;
  }).catch(function(){});
}

function mkRow(f,cls,done){
  var id=f.ident||'';
  var col=getIdentColor(id);
  if(id)faIdents[id.toUpperCase().replace(/ /g,'')]=col;
  if(f.callsign)faIdents[f.callsign.toUpperCase().replace(/ /g,'')]=col;
  
  var heliTag=HELI[f.type]?'🚁 ':'';
  var csSub=f.callsign?'<span class="sub">'+f.callsign+'</span>':'';
  var loc=cls==='ar'?(f.from||''):(f.to||'');
  var flag=getFlag(loc);
  var locSub='';
  if(f.intl&&f.country)locSub='<span class="sub">'+f.country+'</span>';
  else if(f.city)locSub='<span class="sub">'+f.city+'</span>';
  var ffcls=f.intl?'ff intl':'ff';
  // Type with model name sub
  var typeCode=f.type||'';
  var modelName=MODEL[typeCode]||'';
  var typeSub=modelName?'<span class="sub">'+modelName+'</span>':'';
  var rawEta=cls==='ar'?calcMins(f.arriveISO):calcMins(f.departISO);
  var eta=rawEta<0?0:rawEta;
  var fecls='fe '+(eta<=15?'soon':eta<=60?'mid':'far');
  var etaStr='—';
  if(done&&cls==='ar'){
    var ago=Math.round((Date.now()-new Date(f.arriveISO||0).getTime())/60000);
    etaStr=ago>=0?'<span style="color:var(--green)">'+fmtHM(ago)+'</span>':'—';
  }
  else if(done&&cls==='de'){
    var ago2=Math.round((Date.now()-new Date(f.departISO||0).getTime())/60000);
    etaStr=ago2>=0?'<span style="color:var(--red)">'+fmtHM(ago2)+'</span>':'—';
  }
  else if(cls==='ar'&&!done&&f._landed)etaStr='<span style="color:var(--green);font-weight:700">LANDED</span>';
  else if(cls==='ar'&&!done&&rawEta<=5&&rawEta>0)etaStr='<span style="color:var(--amber);font-weight:700">FINAL</span>';
  else if(cls==='ar'&&!done&&rawEta<=0)etaStr='<span style="color:var(--green);font-weight:700">LANDED</span>';
  else if(rawEta>0)etaStr=fmtHM(rawEta);
  var rowCls='';
  if(cls==='ar'&&!done)rowCls=' arr-active';
  else if(cls==='de'&&!done&&!f.departed)rowCls=' dep-ground';
  else if(cls==='de'&&f.departed&&!done)rowCls=' dep-gone';
  if(cls==='de'){
    var cid=(id).replace(/[^a-zA-Z0-9]/g,'');
    if(done){
      var isHD2=HELI[f.type]?true:false;
      var sugD2=suggestSpot(f.type,2,isHD2,f.ident);
      var cidDone=cid;
      var manSpotD2=window._parkingAssignments&&window._parkingAssignments[cidDone]?window._parkingAssignments[cidDone]:'';
      var depDoneSpot=manSpotD2||sugD2.spot;
      var depDoneCell='<span class="fm spot-click" data-plane="'+cidDone+'" style="font-size:9px;font-weight:700;color:var(--cyan);line-height:1.2;cursor:pointer">'+depDoneSpot+'</span>';
      return '<div class="fr dep-simple"><span class="fi">'+id+csSub+'</span><span class="ft" style="line-height:1.1">'+(typeCode||'—')+typeSub+'</span><span class="'+ffcls+'">'+flag+(flag?' ':'')+(loc||'—')+locSub+'</span><span class="fm">'+(f.depart||'—')+'</span><span class="fm">'+etaStr+'</span>'+depDoneCell+'</div>';
    }
    // Spot for departure
    var isHD=HELI[f.type]?true:false;
    var sugD=suggestSpot(f.type,2,isHD,f.ident);
    var manSpotD=window._parkingAssignments&&window._parkingAssignments[cid]?window._parkingAssignments[cid]:'';
    var depSpotText=manSpotD||sugD.spot;
    var depSpotCell='<span class="fm spot-click" data-plane="'+cid+'" style="font-size:9px;font-weight:700;color:var(--cyan);line-height:1.2;cursor:pointer">'+depSpotText+'</span>';
    return '<div class="fr dep'+rowCls+'"><span class="fi">'+id+csSub+'</span><span class="ft" style="line-height:1.1">'+(typeCode||'—')+typeSub+'</span><span class="'+ffcls+'">'+flag+(flag?' ':'')+(loc||'—')+locSub+'</span><span class="fm">'+(f.depart||'—')+'</span><span class="'+fecls+'">'+etaStr+'</span>'+depSpotCell+'<input class="chk" type="checkbox" id="cat_'+cid+'" onclick="event.stopPropagation()" /><input class="chk" type="checkbox" id="cip_'+cid+'" onclick="event.stopPropagation()" /><input class="chk" type="checkbox" id="fuel_'+cid+'" onclick="event.stopPropagation()" /><input class="chk" type="checkbox" id="lav_'+cid+'" onclick="event.stopPropagation()" /><input class="chk" type="checkbox" id="h2o_'+cid+'" onclick="event.stopPropagation()" /></div>';
  }
  var paxId='ar_'+(id).replace(/[^a-zA-Z0-9]/g,'');
  // PROGRESS BAR - simple logic:
  // 1. If we have departISO and arriveISO, calculate percentage from elapsed time
  // 2. If departISO is in the future or missing, show empty bar
  // 3. If arriveISO is in the past, show full bar
  var pPct=0;
  var hasDep=false;
  var nowMs=Date.now();
  var depMs=f.departISO?new Date(f.departISO).getTime():0;
  var arrMs=f.arriveISO?new Date(f.arriveISO).getTime():0;
  if(depMs>0&&depMs<nowMs)hasDep=true;
  if(hasDep&&depMs>0&&arrMs>0){
    if(nowMs>=arrMs){pPct=100;}
    else if(arrMs>depMs){pPct=Math.round(((nowMs-depMs)/(arrMs-depMs))*100);}
  }
  if(pPct<0)pPct=0;if(pPct>100)pPct=100;
  if(f._landed)pPct=100;

  // Add "on ground" for arrivals not yet airborne
  if(cls==='ar'&&!done&&!f._landed&&!hasDep){
    etaStr+='<br><span style="font-size:7px;color:var(--t3);font-weight:600;letter-spacing:.3px">on ground</span>';
  }

  if(done){
    // Spot for arrived aircraft
    var isH2=HELI[f.type]?true:false;
    var sug2=suggestSpot(f.type,24,isH2,f.ident);
    var safeId3=id.replace(/[^a-zA-Z0-9]/g,'');
    var manSpot2=window._parkingAssignments&&window._parkingAssignments[safeId3]?window._parkingAssignments[safeId3]:'';
    var arrSpotText=manSpot2||sug2.spot;
    var arrSpotCell='<span class="fm spot-click" data-plane="'+safeId3+'" style="font-size:9px;font-weight:700;color:var(--cyan);line-height:1.2;cursor:pointer">'+arrSpotText+'</span>';
    return '<div class="fr arr-done"><span class="fi">'+id+csSub+'</span><span class="ft" style="line-height:1.1">'+(typeCode||'—')+typeSub+'</span><span class="'+ffcls+'">'+flag+(flag?' ':'')+(loc||'—')+locSub+'</span><span class="fm">'+(f.depart||'—')+'</span><span class="fm">'+(f.arrive||'—')+'</span><span class="'+fecls+'">'+etaStr+'</span>'+arrSpotCell+'</div>';
  }
  var enRoute=hasDep&&!done&&!f._landed;
  var gd=enRoute?'<span style="position:absolute;left:4px;top:8px;width:7px;height:7px;border-radius:50%;background:#22c55e;box-shadow:0 0 5px rgba(34,197,94,.5)"></span>':'';
  var safeId=id.replace(/[^a-zA-Z0-9]/g,'');
  // Calculate suggested parking spot (avoiding already-assigned spots)
  var isH=HELI[f.type]?true:false;
  var stayHrs=24;
  var safeId2=id.replace(/[^a-zA-Z0-9]/g,'');
  // Check if manually assigned
  var manualSpot=window._parkingAssignments&&window._parkingAssignments[safeId2]?window._parkingAssignments[safeId2]:'';
  var finalSpot='';
  var towNote='';
  if(manualSpot){
    finalSpot=manualSpot;
  } else {
    var sug=suggestSpot(f.type,stayHrs,isH,f.ident);
    finalSpot=sug.spot;towNote=sug.tow||'';
    // If spot already taken, find next available of same type
    if(window._spotOccupied&&window._spotOccupied[finalSpot]){
      var cat2=getWsCat(f.type);
      var alternatives=[];
      for(var si=0;si<SPOT_DEFS.length;si++){
        var sd=SPOT_DEFS[si];
        if(sd.cats.indexOf(cat2)>=0&&!window._spotOccupied[sd.name]&&sd.name!==finalSpot){
          alternatives.push(sd.name);
        }
      }
      if(alternatives.length>0)finalSpot=alternatives[0];
    }
  }
  // Mark spot as occupied
  if(window._spotOccupied)window._spotOccupied[finalSpot]=safeId2;
  var spotText=finalSpot;
  if(towNote)spotText+='<br><span style="font-size:7px;color:var(--t3)">'+towNote+'</span>';
  var spotCell='<span class="fm spot-click" data-plane="'+safeId2+'" style="font-size:9px;font-weight:700;color:var(--cyan);line-height:1.2;cursor:pointer">'+spotText+'</span>';
  return '<div class="fr'+rowCls+'"><span class="fi zp" data-zp="'+safeId+'">'+gd+id+csSub+'</span><span class="ft" style="line-height:1.1">'+(typeCode||'—')+typeSub+'</span><span class="'+ffcls+'">'+flag+(flag?' ':'')+(loc||'—')+locSub+'</span><span class="fm">'+(f.depart||'—')+'</span><span class="fm">'+(f.arrive||'—')+'</span><span class="'+fecls+'">'+etaStr+'</span>'+spotCell+'<input class="pax" type="number" min="0" max="99" placeholder="—" id="pax_'+paxId+'" onclick="event.stopPropagation()" /></div>';
}

function calcMins(isoTime){
  if(!isoTime)return -1;
  var diff=Math.round((new Date(isoTime).getTime()-Date.now())/60000);
  return diff;
}

function calcProgClient(depISO,arrISO){
  if(!depISO||!arrISO)return 0;
  var dep=new Date(depISO).getTime();
  var arr=new Date(arrISO).getTime();
  var now=Date.now();
  if(now>=arr)return 100;
  if(now<=dep)return 0;
  var total=arr-dep;
  if(total<=0)return 0;
  return Math.round(((now-dep)/total)*100);
}

function fetchBoards(){
  console.log('[BOARDS] Fetching arrivals...');
  fetch('/fa/arrivals').then(function(r){
    return r.json();
  }).then(function(arr){
    // Reset spot trackers for this render cycle
    window._spotOccupied={};
    window._globalSpotMap={};
    document.getElementById('ffa').className='fd on';
    var ab=document.getElementById('ab');
    var adb=document.getElementById('adb');
    var ah=document.getElementById('ah');
    var active=[],completed=[];
    var now=Date.now();
    if(arr)for(var i=0;i<arr.length;i++){
      var f=arr[i];
      var arrTime=f.arriveISO?new Date(f.arriveISO).getTime():0;
      var minsSinceArr=arrTime>0?Math.round((now-arrTime)/60000):-999;
      if(f.arrived||minsSinceArr>5){
        // Landed: keep in active for 5 min with LANDED tag, then move to completed
        if(minsSinceArr>=0&&minsSinceArr<5){f._landed=true;active.push(f);}
        else completed.push(f);
      }
      else if(minsSinceArr>=0&&minsSinceArr<=5){
        // Arrival time passed but <5 min ago — show as LANDED in active
        f._landed=true;active.push(f);
      }
      else active.push(f);
    }
    // Sort active: soonest arrival first
    active.sort(function(a,b){return(a.arriveISO||'').localeCompare(b.arriveISO||'');});
    // Sort completed: most recent first
    completed.sort(function(a,b){return(b.arriveISO||'').localeCompare(a.arriveISO||'');});
    if(active.length>0){
      var h='';
      for(var i=0;i<Math.min(active.length,30);i++){h+=mkRow(active[i],'ar',false);}
      ab.innerHTML=h;
      document.getElementById('anc').textContent=active.length;
    }else{ab.innerHTML='<div class="empty">No inbound GA</div>';document.getElementById('anc').textContent='0';}
    if(completed.length>0){
      ah.style.display='';adb.style.display='';
      var ahc=document.getElementById('ahcols');if(ahc)ahc.style.display='';
      var h='';
      for(var i=0;i<Math.min(completed.length,20);i++){h+=mkRow(completed[i],'ar',true);}
      adb.innerHTML=h;document.getElementById('adc').textContent=completed.length;
    }else{ah.style.display='none';adb.style.display='none';var ahc=document.getElementById('ahcols');if(ahc)ahc.style.display='none';}
  }).catch(function(e){console.warn('FA arr:',e);});

  fetch('/fa/departures').then(function(r){return r.json();}).then(function(dep){
    var db=document.getElementById('db');
    var ddb=document.getElementById('ddb');
    var dh=document.getElementById('dh');
    var active=[],completed=[];
    if(dep)for(var i=0;i<dep.length;i++){
      var f=dep[i];
      if(f.departed)completed.push(f);
      else active.push(f);
    }
    // Sort active: soonest departure first
    active.sort(function(a,b){return(a.departISO||'').localeCompare(b.departISO||'');});
    // Sort completed: most recent departure first
    completed.sort(function(a,b){return(b.departISO||'').localeCompare(a.departISO||'');});
    if(active.length>0){
      var h='';
      for(var i=0;i<Math.min(active.length,30);i++){h+=mkRow(active[i],'de',false);}
      db.innerHTML=h;
      document.getElementById('dnc').textContent=active.length;
    }else{db.innerHTML='<div class="empty">No outbound GA</div>';document.getElementById('dnc').textContent='0';}
    if(completed.length>0){
      dh.style.display='';ddb.style.display='';
      var dhc=document.getElementById('dhcols');if(dhc)dhc.style.display='';
      var h='';
      for(var i=0;i<Math.min(completed.length,20);i++){h+=mkRow(completed[i],'de',true);}
      ddb.innerHTML=h;document.getElementById('ddc').textContent=completed.length;
    }else{dh.style.display='none';ddb.style.display='none';var dhc=document.getElementById('dhcols');if(dhc)dhc.style.display='none';}
  }).catch(function(e){console.warn('FA dep:',e);});
  setTimeout(drawHUD,500);
  setTimeout(drawChart,600);
}

function now(){return new Date().toLocaleTimeString('en-US',{hour:'2-digit',minute:'2-digit',hour12:false});}
function setPill(c,t){document.getElementById('pill').className='pill '+c;document.getElementById('pt').textContent=t;}

// WebSocket for SWIM status
function connectWS(){
  try{
    var ws=new WebSocket('ws://'+location.hostname+':8765');
    ws.onopen=function(){document.getElementById('fs').className='fd on';};
    ws.onmessage=function(e){
      try{
        var m=JSON.parse(e.data);
        if(m.type==='swimStatus'){
          document.getElementById('fs').className=m.status==='connected'?'fd on':'fd off';
        }
      }catch(x){}
    };
    ws.onerror=function(){document.getElementById('fs').className='fd off';};
    ws.onclose=function(){document.getElementById('fs').className='fd off';setTimeout(connectWS,10000);};
  }catch(e){}
}
connectWS();

function fmtHM(totalMins){
  var h=Math.floor(totalMins/60);
  var m=totalMins%60;
  return h+':'+(m<10?'0':'')+m;
}
function timeAgo(iso){
  if(!iso)return '—';
  var ms=Date.now()-new Date(iso).getTime();
  if(ms<0)return '—';
  var mins=Math.floor(ms/60000);
  return fmtHM(mins);
}
function timeUntil(iso){
  if(!iso)return '—';
  var ms=new Date(iso).getTime()-Date.now();
  if(ms<=0)return 'Now';
  var mins=Math.floor(ms/60000);
  return fmtHM(mins);
}
// Track previous chart counts for delta display
if(!window._prevAC)window._prevAC=[];
if(!window._prevDC)window._prevDC=[];
if(!window._deltaTime)window._deltaTime=[];

function drawChart(){
  var el=document.getElementById('chartBars');
  if(!el)return;
  Promise.all([
    fetch('/fa/arrivals').then(function(r){return r.json();}),
    fetch('/fa/departures').then(function(r){return r.json();})
  ]).then(function(res){
    var arr=res[0]||[],dep=res[1]||[];
    var now=new Date();var curH=now.getHours();var nowMs=Date.now();
    var hours=[];for(var i=0;i<5;i++)hours.push((curH+i+1)%24);
    var ac=[],dc=[],mx=1;
    for(var i=0;i<5;i++){
      var hr=hours[i];var a=0,d=0;
      for(var j=0;j<arr.length;j++){if(!arr[j].arriveISO||arr[j].arrived)continue;if(new Date(arr[j].arriveISO).getHours()===hr)a++;}
      for(var j=0;j<dep.length;j++){if(!dep[j].departISO||dep[j].departed)continue;if(new Date(dep[j].departISO).getHours()===hr)d++;}
      ac.push(a);dc.push(d);if(a>mx)mx=a;if(d>mx)mx=d;
    }
    // Calculate deltas
    var aDelta=[],dDelta=[];
    for(var i=0;i<5;i++){
      var ad=(window._prevAC.length>i)?(ac[i]-window._prevAC[i]):0;
      var dd=(window._prevDC.length>i)?(dc[i]-window._prevDC[i]):0;
      // Only show delta if changed and within 3 min
      if(ad!==0){window._deltaTime[i+'a']=nowMs;}
      if(dd!==0){window._deltaTime[i+'d']=nowMs;}
      var aShow=(window._deltaTime[i+'a']&&(nowMs-window._deltaTime[i+'a'])<180000);
      var dShow=(window._deltaTime[i+'d']&&(nowMs-window._deltaTime[i+'d'])<180000);
      aDelta.push(aShow?ad:0);
      dDelta.push(dShow?dd:0);
    }
    window._prevAC=ac.slice();window._prevDC=dc.slice();
    var h='';
    for(var i=0;i<5;i++){
      var lbl=hours[i]>12?(hours[i]-12)+'p':(hours[i]===0?'12a':(hours[i]===12?'12p':hours[i]+'a'));
      var aPct=mx>0?Math.round((ac[i]/mx)*100):0;
      var dPct=mx>0?Math.round((dc[i]/mx)*100):0;
      var aDeltaStr=aDelta[i]>0?'<span style="font-size:8px;font-weight:800;color:#22c55e;margin-left:2px">+'+aDelta[i]+'</span>':(aDelta[i]<0?'<span style="font-size:8px;font-weight:800;color:#ef4444;margin-left:2px">'+aDelta[i]+'</span>':'');
      var dDeltaStr=dDelta[i]>0?'<span style="font-size:8px;font-weight:800;color:#22c55e;margin-left:2px">+'+dDelta[i]+'</span>':(dDelta[i]<0?'<span style="font-size:8px;font-weight:800;color:#ef4444;margin-left:2px">'+dDelta[i]+'</span>':'');
      h+='<div style="flex:1;display:flex;flex-direction:column;align-items:center;gap:0">';
      h+='<div style="flex:1;display:flex;flex-direction:column;justify-content:flex-end;align-items:center;width:100%">';
      if(ac[i]>0)h+='<span style="font-family:var(--mono);font-size:10px;font-weight:800;color:#3b82f6;margin-bottom:2px">'+ac[i]+aDeltaStr+'</span>';
      h+='<div style="width:60%;min-height:2px;height:'+aPct+'%;background:linear-gradient(180deg,#3b82f6,#60a5fa);border-radius:3px 3px 0 0;transition:height .4s"></div>';
      h+='</div>';
      h+='<div style="width:100%;height:1px;background:#e2e4e9;flex-shrink:0"></div>';
      h+='<div style="flex:1;display:flex;flex-direction:column;justify-content:flex-start;align-items:center;width:100%">';
      h+='<div style="width:60%;min-height:2px;height:'+dPct+'%;background:linear-gradient(180deg,#f87171,#dc2626);border-radius:0 0 3px 3px;transition:height .4s"></div>';
      if(dc[i]>0)h+='<span style="font-family:var(--mono);font-size:10px;font-weight:800;color:#dc2626;margin-top:2px">'+dc[i]+dDeltaStr+'</span>';
      h+='</div>';
      h+='<span style="font-family:var(--mono);font-size:8px;font-weight:700;color:#9ca3af;padding-top:4px;flex-shrink:0">'+lbl+'</span>';
      h+='</div>';
    }
    el.innerHTML=h;
  }).catch(function(){});
}

function showGround(){
  document.getElementById('gnd-overlay').style.display='block';
  fetch('/fa/ground').then(function(r){return r.json();}).then(function(g){
    var tb=document.getElementById('gnd-table');
    window._globalSpotMap={};
    if(!g||!g.length){tb.innerHTML='<p style="text-align:center;color:var(--t3);padding:30px;font-family:var(--mono)">No aircraft on ground</p>';return;}
    var spotNames=['','Spot A','Spot 1','Spot 2','Spot 3','Spot 4','Spot 5','Hangar A','Hangar B','Hangar C','Btwn Hangars 1','Btwn Hangars 2','Btwn Hangars 3','Btwn Hangars 4','Ken Salvage','2nd Line','Overflow','3rd Line','The Shop','Airfield Safety','The Island','The Fence','4th Line','42 West 1','42 West 2','42 West 3','42 West 4'];
    var h='<table style="width:100%;border-collapse:collapse;font-family:var(--mono);font-size:11px">';
    h+='<tr style="background:var(--b0);font-size:8px;font-weight:700;text-transform:uppercase;letter-spacing:.7px;color:var(--t3)"><th style="padding:8px 6px;text-align:left">Tail</th><th>Type</th><th>From</th><th>On Ground</th><th>Next Dest</th><th>Departing</th><th style="min-width:90px">Parked At</th></tr>';
    for(var i=0;i<g.length;i++){
      var f=g[i];
      var fromStr=(f.from||'—')+(f.city?' <span style="color:var(--t3);font-size:9px">'+f.city+'</span>':'')+(f.country?' <span style="color:var(--red);font-size:9px">'+f.country+'</span>':'');
      var typeStr=(f.type||'—');
      var mdl=MODEL[f.type]||'';
      if(mdl)typeStr+='<br><span style="font-size:8px;color:var(--t3)">'+mdl+'</span>';
      var sinceArr=timeAgo(f.arrivedISO);
      var untilDep=f.nextDepart?timeUntil(f.nextDepartISO||''):'—';
      var destStr=(f.nextDest||'—')+(f.nextDestCity?' <span style="font-size:9px;color:var(--t3)">'+f.nextDestCity+'</span>':'');
      var bg=i%2===0?'var(--b1)':'var(--b2)';
      var depColor=f.nextDest?'var(--green)':'var(--t3)';
      var safeId=(f.ident||'').replace(/[^a-zA-Z0-9]/g,'');
      // Suggested spot
      var isH=HELI[f.type]?true:false;
      var stayHrs=24;
      if(f.nextDepartISO){var dMs=new Date(f.nextDepartISO).getTime();stayHrs=Math.max(0,(dMs-Date.now())/3600000);}
      var sug=suggestSpot(f.type,stayHrs,isH,f.ident);
      var sugName=sug.spot+(sug.tow?' → '+sug.tow:'')+(sug.note?' ('+sug.note+')':'');
      // Build dropdown
      var saved=window._parkingAssignments&&window._parkingAssignments[safeId]?window._parkingAssignments[safeId]:'';
      var sel='<select style="font-family:var(--mono);font-size:9px;font-weight:600;padding:2px 3px;border:1px solid var(--bd);border-radius:3px;background:var(--b1);color:var(--t1);cursor:pointer" onchange="assignParking(this.dataset.id,this.value)" data-id="'+safeId+'">';
      for(var s=0;s<spotNames.length;s++){
        var selected=(saved===spotNames[s]||(! saved&&spotNames[s]===sug.spot))?'selected':'';
        var label=spotNames[s]||'—';
        sel+='<option value="'+spotNames[s]+'" '+selected+'>'+label+'</option>';
      }
      sel+='</select>';
      h+='<tr style="background:'+bg+'"><td style="padding:7px 6px;font-weight:700;color:var(--cyan)">'+f.ident+'</td><td style="text-align:center">'+typeStr+'</td><td style="text-align:center">'+fromStr+'</td><td style="text-align:center;color:var(--amber)">'+sinceArr+'</td><td style="text-align:center">'+destStr+'</td><td style="text-align:center;color:'+depColor+'">'+untilDep+'</td><td style="text-align:center">'+sel+'</td></tr>';
    }
    h+='</table>';
    tb.innerHTML=h;
  }).catch(function(e){document.getElementById('gnd-table').innerHTML='<p style="color:var(--red);padding:20px">Error loading data</p>';});
}
// Parking assignments stored in memory
if(!window._parkingAssignments)window._parkingAssignments={};
function assignParking(id,spot){
  if(!id&&this&&this.dataset)id=this.dataset.id;
  window._parkingAssignments[id]=spot;
}
function closeGround(){document.getElementById('gnd-overlay').style.display='none';}

// === RAMP VIEW ===
var rampMap=null,rampMarkers=[],rampSpotLabels=[],rampPolygons={},rampLabels={};
// Spot locations on the Signature SFO ramp (lat/lng from satellite)
var RAMP_SPOTS={
'42 West 1':{lat:37.629350,lng:-122.388700,angle:280},
'42 West 2':{lat:37.629200,lng:-122.388500,angle:280},
'42 West 3':{lat:37.628900,lng:-122.388250,angle:280},
'42 West 4':{lat:37.628750,lng:-122.388100,angle:280},
'Hangar B':{lat:37.628827,lng:-122.387221,angle:280},
'Btwn Hangars 1':{lat:37.628700,lng:-122.387600,angle:280},
'Btwn Hangars 2':{lat:37.628450,lng:-122.387100,angle:280},
'Btwn Hangars 3':{lat:37.628200,lng:-122.386600,angle:280},
'Btwn Hangars 4':{lat:37.627950,lng:-122.386100,angle:280},
'Hangar C':{lat:37.628231,lng:-122.385973,angle:280},
'The Island':{lat:37.627653,lng:-122.385350,angle:280},
'The Fence':{lat:37.627751,lng:-122.385125,angle:20},
'Spot 5':{lat:37.627326,lng:-122.384643,angle:280},
'Spot 4':{lat:37.627568,lng:-122.384458,angle:280},
'Spot 3':{lat:37.627760,lng:-122.384262,angle:280},
'Spot 2':{lat:37.627928,lng:-122.384177,angle:280},
'Spot 1':{lat:37.628096,lng:-122.384092,angle:280},
'Spot A':{lat:37.628256,lng:-122.383758,angle:280},
'Hangar A':{lat:37.628585,lng:-122.383650,angle:280},
'Ken Salvage':{lat:37.628362,lng:-122.383080,angle:280},
'2nd Line 1':{lat:37.627543,lng:-122.383604,angle:280},
'2nd Line 2':{lat:37.627737,lng:-122.383482,angle:280},
'2nd Line 3':{lat:37.627931,lng:-122.383363,angle:280},
'2nd Line 4':{lat:37.628084,lng:-122.383212,angle:280},
'Overflow 1':{lat:37.627060,lng:-122.383913,angle:280},
'Overflow 2':{lat:37.627272,lng:-122.383729,angle:280},
'3rd Line 1':{lat:37.627275,lng:-122.383044,angle:280},
'3rd Line 2':{lat:37.627487,lng:-122.382926,angle:280},
'3rd Line 3':{lat:37.627666,lng:-122.382821,angle:280},
'3rd Line 4':{lat:37.627842,lng:-122.382658,angle:280},
'3rd Line 5':{lat:37.628033,lng:-122.382537,angle:280},
'3rd Line 6':{lat:37.628219,lng:-122.382413,angle:280},
'3rd Line 7':{lat:37.628422,lng:-122.382279,angle:280},
'3rd Line 8':{lat:37.628618,lng:-122.382191,angle:280},
'The Shop':{lat:37.628940,lng:-122.382102,angle:280},
'Airfield Safety 1':{lat:37.629319,lng:-122.382095,angle:280},
'Airfield Safety 2':{lat:37.629247,lng:-122.381872,angle:280},
'41-7 A':{lat:37.626364,lng:-122.382247,angle:280},
'41-7 B':{lat:37.626241,lng:-122.381951,angle:280},
'41-11':{lat:37.626503,lng:-122.381747,angle:280},
'4th Line 1':{lat:37.627172,lng:-122.381724,angle:280},
'4th Line 2':{lat:37.627632,lng:-122.381463,angle:280},
'4th Line 3':{lat:37.628093,lng:-122.381203,angle:280},
'4th Line 4':{lat:37.628553,lng:-122.380942,angle:280}
};

function showRampView(){
  document.getElementById('ramp-overlay').style.display='block';
  if(!rampMap){
    rampMap=L.map('rampMap',{center:[37.6278,-122.3840],zoom:17,zoomControl:false,attributionControl:false});
    L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',{maxZoom:20,maxNativeZoom:19}).addTo(rampMap);
    // Draw angled spot boundary polygons matching ramp orientation
    // Ramp angle ~150 degrees (NW-SE), bays perpendicular to taxilane
    var rad=120*Math.PI/180; // ramp heading in radians
    var perpRad=rad-Math.PI/2; // perpendicular to ramp
    function bayPoly(lat,lng,depth,noseW,wingW,col){
      // Aircraft envelope: fuselage + wings + tail silhouette
      var sc=0.0000027;
      var scLng=0.0000034;
      var cosR=Math.cos(rad),sinR=Math.sin(rad);
      var cosP=Math.cos(perpRad),sinP=Math.sin(perpRad);
      var fuseW=noseW*0.7;
      var tailW=noseW*1.2;
      var noseD=depth*0.5;
      var wingD=depth*0.15;
      var tailD=depth*0.45;
      var tailTopD=depth*0.55;
      function pt(a,p){return[lat+a*sc*cosR+p*sc*cosP,lng+a*scLng*sinR+p*scLng*sinP];}
      var pts=[
        pt(-noseD,fuseW),pt(-noseD,-fuseW),
        pt(-wingD-5,-fuseW),pt(-wingD,-wingW),pt(wingD,-wingW),pt(wingD+5,-fuseW),
        pt(tailD,-fuseW),pt(tailD,-tailW),pt(tailTopD,-tailW*0.6),
        pt(tailTopD,tailW*0.6),pt(tailD,tailW),pt(tailD,fuseW),
        pt(wingD+5,fuseW),pt(wingD,wingW),pt(-wingD,wingW),pt(-wingD-5,fuseW)
      ];
      L.polygon(pts,{color:col,weight:1.5,fillColor:col,fillOpacity:0.1}).addTo(rampMap);
    }
    for(var name in RAMP_SPOTS){
      var s=RAMP_SPOTS[name];
      var col='#f59e0b';
      if(name==='Btwn Hangars 1','Btwn Hangars 2','Btwn Hangars 3','Btwn Hangars 4')col='#f59e0b';
      else if(name.indexOf('Hangar')>=0)col='#8b5cf6';
      else if(name.indexOf('Spot')>=0||name==='Spot A')col='#3b82f6';
      else if(name.indexOf('2nd')>=0)col='#22c55e';
      else if(name.indexOf('3rd')>=0)col='#06b6d4';
      else if(name.indexOf('4th')>=0||name.indexOf('41-')>=0)col='#ef4444';
      else if(name==='Overflow 1'||name==='Overflow 2')col='#f97316';
      else if(name==='42 West 1','42 West 2','42 West 3','42 West 4')col='#a855f7';
      else if(name==='The Island'||name==='The Fence')col='#64748b';
      // Size bays by spot type - trapezoid: depth, noseW (narrow back), wingW (wide front)
      var depth=40,noseW=10,wingW=30;
      if(name.indexOf('Hangar')>=0||name==='Btwn Hangars 1','Btwn Hangars 2','Btwn Hangars 3','Btwn Hangars 4'||true||name==='Spot 1'||name==='Spot 2'||name==='Spot 3'||name==='Spot 4'||name==='Spot 5'){
        // Custom polygons drawn separately below
      } else {
      if(name.indexOf('Spot 1')>=0||name.indexOf('Spot 2')>=0||name==='Spot A'){depth=35;noseW=8;wingW=22;}
      else if(name.indexOf('Spot 3')>=0){depth=40;noseW=10;wingW=28;}
      else if(name.indexOf('Spot 4')>=0||name.indexOf('Spot 5')>=0){depth=45;noseW=12;wingW=42;}
      else if(name==='42 West 1','42 West 2','42 West 3','42 West 4'){depth=0;noseW=0;wingW=0;} // custom polygon drawn separately
      else if(name.indexOf('4th')>=0||name.indexOf('41-')>=0){depth=50;noseW=14;wingW=45;}
      else if(name==='The Island'){depth=48;noseW=12;wingW=42;}
      else if(name.indexOf('Overflow')>=0){depth=45;noseW=12;wingW=40;}
      else if(name==='Btwn Hangars 1','Btwn Hangars 2','Btwn Hangars 3','Btwn Hangars 4'){depth=0;noseW=0;wingW=0;} // custom polygon drawn separately
      else if(name==='The Shop'||name.indexOf('AFS')>=0||name.indexOf('Airfield')>=0){depth=45;noseW=12;wingW=40;}
      else if(name.indexOf('2nd')>=0){depth=40;noseW=10;wingW=28;}
      else if(name.indexOf('3rd')>=0){depth=40;noseW=10;wingW=28;}
      else if(name==='The Fence'){depth=35;noseW=8;wingW=22;}
      else if(name==='Ken Salvage'||name.indexOf('KenS')>=0){depth=40;noseW=10;wingW=28;}
      var fm=1.25,bm=1.15;
      if(name.indexOf('Spot 1')>=0||name.indexOf('Spot 2')>=0)bm=1.10;
      else if(name.indexOf('Spot 4')>=0||name.indexOf('Spot 5')>=0)bm=1.20;
      else if(name.indexOf('4th')>=0||name.indexOf('41-')>=0)bm=1.20;
      bayPoly(s.lat,s.lng,depth,noseW,wingW,col,fm,bm,name);
      }
      var shortName=name.replace('Airfield Safety','AFS').replace('Ken Salvage','KenS').replace('Btwn Hangars 1','Btwn Hangars 2','Btwn Hangars 3','Btwn Hangars 4','Btwn Hngrs');
      var lbl=L.marker([s.lat,s.lng],{interactive:false,icon:L.divIcon({className:'',html:'<div id="rl_'+name.replace(/[^a-zA-Z0-9]/g,'')+'" style="font-family:monospace;font-size:8px;font-weight:800;color:#22c55e;background:rgba(0,0,0,.75);padding:1px 5px;border-radius:3px;white-space:nowrap;text-align:center">'+shortName+'</div>',iconSize:[80,14],iconAnchor:[40,7]})}).addTo(rampMap);
      rampSpotLabels.push(lbl);
      if(!window._rampLabelMarkers)window._rampLabelMarkers={};
      window._rampLabelMarkers[name]=lbl;
      rampLabels[name]=name.replace(/[^a-zA-Z0-9]/g,'');
    }
    // Custom polygons for specific spots
    // 42 West - custom boundary
    var _p42West=L.polygon([
      [37.629010,-122.388984],[37.629537,-122.388568],
      [37.629174,-122.387716],[37.628652,-122.388048]
    ],{color:'#a855f7',weight:2,fillColor:'#a855f7',fillOpacity:0.08}).addTo(rampMap);rampPolygons['42 West 1','42 West 2','42 West 3','42 West 4']=_p42West;
    // Between Hangars - custom boundary
    var _pBtwnHangars=L.polygon([
      [37.628872,-122.387839],[37.628620,-122.388037],
      [37.627600,-122.385645],[37.627848,-122.385444]
    ],{color:'#f59e0b',weight:2,fillColor:'#f59e0b',fillOpacity:0.08}).addTo(rampMap);rampPolygons['Btwn Hangars 1','Btwn Hangars 2','Btwn Hangars 3','Btwn Hangars 4']=_pBtwnHangars;
    // Hangar B - custom boundary
    var _pHangarB=L.polygon([
      [37.629083,-122.387515],[37.628863,-122.387665],
      [37.628546,-122.386927],[37.628769,-122.386773]
    ],{color:'#8b5cf6',weight:2,fillColor:'#8b5cf6',fillOpacity:0.1}).addTo(rampMap);rampPolygons['Hangar B']=_pHangarB;
    // Hangar C - custom boundary
    var _pHangarC=L.polygon([
      [37.628477,-122.386187],[37.628248,-122.386333],
      [37.627982,-122.385700],[37.628205,-122.385544]
    ],{color:'#8b5cf6',weight:2,fillColor:'#8b5cf6',fillOpacity:0.1}).addTo(rampMap);rampPolygons['Hangar C']=_pHangarC;
    // Hangar A - custom boundary
    var _pHangarA=L.polygon([
      [37.628572,-122.384067],[37.628330,-122.383919],
      [37.628613,-122.383134],[37.628854,-122.383262]
    ],{color:'#8b5cf6',weight:2,fillColor:'#8b5cf6',fillOpacity:0.1}).addTo(rampMap);rampPolygons['Hangar A']=_pHangarA;
    // The Island - custom boundary
    var _pTheIsland=L.polygon([
      [37.627782,-122.385426],[37.627610,-122.385536],
      [37.627525,-122.385275],[37.627719,-122.385216]
    ],{color:'#64748b',weight:2,fillColor:'#64748b',fillOpacity:0.1}).addTo(rampMap);rampPolygons['The Island']=_pTheIsland;
    // The Fence - custom boundary
    var _pTheFence=L.polygon([
      [37.627811,-122.385142],[37.627722,-122.385190],
      [37.627668,-122.385064],[37.627777,-122.384990]
    ],{color:'#64748b',weight:2,fillColor:'#64748b',fillOpacity:0.1}).addTo(rampMap);rampPolygons['The Fence']=_pTheFence;
    // Spot A - custom boundary
    var _pSpotA=L.polygon([
      [37.628326,-122.383791],[37.628280,-122.383674],
      [37.628184,-122.383727],[37.628233,-122.383840]
    ],{color:'#3b82f6',weight:2,fillColor:'#3b82f6',fillOpacity:0.1}).addTo(rampMap);rampPolygons['Spot A']=_pSpotA;


    // Spot 1 - aircraft-shaped bay
    L.polygon([[37.628157,-122.384169],[37.628119,-122.384197],[37.628038,-122.384167],[37.628038,-122.384035],[37.628086,-122.384000],[37.628176,-122.384066]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);
    // Spot 2 - aircraft-shaped bay
    L.polygon([[37.628030,-122.384237],[37.627992,-122.384265],[37.627911,-122.384235],[37.627911,-122.384103],[37.627959,-122.384068],[37.628049,-122.384134]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);
    // Spot 3 - aircraft-shaped bay
    L.polygon([[37.627832,-122.384364],[37.627794,-122.384392],[37.627686,-122.384358],[37.627693,-122.384185],[37.627741,-122.384150],[37.627862,-122.384229]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);
    // Spot 4 - aircraft-shaped bay
    L.polygon([[37.627663,-122.384609],[37.627625,-122.384637],[37.627463,-122.384595],[37.627482,-122.384340],[37.627530,-122.384305],[37.627715,-122.384411]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);
    // Spot 5 - aircraft-shaped bay
    L.polygon([[37.627421,-122.384794],[37.627383,-122.384822],[37.627221,-122.384780],[37.627240,-122.384525],[37.627288,-122.384490],[37.627473,-122.384596]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);

    // Spot 1
    var _psSpot1=L.polygon([[37.628157,-122.384169],[37.628119,-122.384197],[37.628038,-122.384167],[37.628038,-122.384035],[37.628086,-122.384000],[37.628176,-122.384066]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);rampPolygons['Spot 1']=_psSpot1;
    // Spot 2
    var _psSpot2=L.polygon([[37.627989,-122.384254],[37.627951,-122.384282],[37.627870,-122.384252],[37.627870,-122.384120],[37.627918,-122.384085],[37.628008,-122.384151]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);rampPolygons['Spot 2']=_psSpot2;
    // Spot 3
    var _psSpot3=L.polygon([[37.627832,-122.384364],[37.627794,-122.384392],[37.627686,-122.384358],[37.627693,-122.384185],[37.627741,-122.384150],[37.627862,-122.384229]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);rampPolygons['Spot 3']=_psSpot3;
    // Spot 4
    var _psSpot4=L.polygon([[37.627663,-122.384609],[37.627625,-122.384637],[37.627463,-122.384595],[37.627482,-122.384340],[37.627530,-122.384305],[37.627715,-122.384411]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);rampPolygons['Spot 4']=_psSpot4;
    // Spot 5
    var _psSpot5=L.polygon([[37.627421,-122.384794],[37.627383,-122.384822],[37.627221,-122.384780],[37.627240,-122.384525],[37.627288,-122.384490],[37.627473,-122.384596]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);rampPolygons['Spot 5']=_psSpot5;

    // 2nd Line 1
    L.polygon([[37.627615,-122.383706],[37.627577,-122.383734],[37.627469,-122.383700],[37.627476,-122.383527],[37.627524,-122.383492],[37.627645,-122.383571]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);
    // 2nd Line 2
    L.polygon([[37.627809,-122.383584],[37.627771,-122.383612],[37.627663,-122.383578],[37.627670,-122.383405],[37.627718,-122.383370],[37.627839,-122.383449]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);
    // 2nd Line 3
    L.polygon([[37.628003,-122.383465],[37.627965,-122.383493],[37.627857,-122.383459],[37.627864,-122.383286],[37.627912,-122.383251],[37.628033,-122.383330]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);
    // 2nd Line 4
    L.polygon([[37.628156,-122.383314],[37.628118,-122.383342],[37.628010,-122.383308],[37.628017,-122.383135],[37.628065,-122.383100],[37.628186,-122.383179]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);
    // Overflow 1
    L.polygon([[37.627155,-122.384064],[37.627117,-122.384092],[37.626955,-122.384050],[37.626974,-122.383795],[37.627022,-122.383760],[37.627207,-122.383866]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);
    // Overflow 2
    L.polygon([[37.627367,-122.383880],[37.627329,-122.383908],[37.627167,-122.383866],[37.627186,-122.383611],[37.627234,-122.383576],[37.627419,-122.383682]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);
    // 3rd Line 1
    L.polygon([[37.627347,-122.383146],[37.627309,-122.383174],[37.627201,-122.383140],[37.627208,-122.382967],[37.627256,-122.382932],[37.627377,-122.383011]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);
    // 3rd Line 2
    L.polygon([[37.627559,-122.383028],[37.627521,-122.383056],[37.627413,-122.383022],[37.627420,-122.382849],[37.627468,-122.382814],[37.627589,-122.382893]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);
    // 3rd Line 3
    L.polygon([[37.627738,-122.382923],[37.627700,-122.382951],[37.627592,-122.382917],[37.627599,-122.382744],[37.627647,-122.382709],[37.627768,-122.382788]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);
    // 3rd Line 4
    L.polygon([[37.627914,-122.382760],[37.627876,-122.382788],[37.627768,-122.382754],[37.627775,-122.382581],[37.627823,-122.382546],[37.627944,-122.382625]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);
    // 3rd Line 5
    L.polygon([[37.628105,-122.382639],[37.628067,-122.382667],[37.627959,-122.382633],[37.627966,-122.382460],[37.628014,-122.382425],[37.628135,-122.382504]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);
    // 3rd Line 6
    L.polygon([[37.628291,-122.382515],[37.628253,-122.382543],[37.628145,-122.382509],[37.628152,-122.382336],[37.628200,-122.382301],[37.628321,-122.382380]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);
    // 3rd Line 7
    L.polygon([[37.628494,-122.382381],[37.628456,-122.382409],[37.628348,-122.382375],[37.628355,-122.382202],[37.628403,-122.382167],[37.628524,-122.382246]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);
    // 3rd Line 8
    L.polygon([[37.628690,-122.382293],[37.628652,-122.382321],[37.628544,-122.382287],[37.628551,-122.382114],[37.628599,-122.382079],[37.628720,-122.382158]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);
    // The Shop
    L.polygon([[37.629031,-122.382245],[37.628993,-122.382273],[37.628845,-122.382228],[37.628857,-122.381991],[37.628905,-122.381956],[37.629074,-122.382062]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);
    // Airfield Safety 1
    L.polygon([[37.629414,-122.382246],[37.629376,-122.382274],[37.629214,-122.382232],[37.629233,-122.381977],[37.629281,-122.381942],[37.629466,-122.382048]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);
    // Airfield Safety 2
    L.polygon([[37.629342,-122.382023],[37.629304,-122.382051],[37.629142,-122.382009],[37.629161,-122.381754],[37.629209,-122.381719],[37.629394,-122.381825]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);
    // Ken Salvage
    L.polygon([[37.628434,-122.383182],[37.628396,-122.383210],[37.628288,-122.383176],[37.628295,-122.383003],[37.628343,-122.382968],[37.628464,-122.383047]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);
    // 4th Line 1
    L.polygon([[37.627267,-122.381875],[37.627229,-122.381903],[37.627067,-122.381861],[37.627086,-122.381606],[37.627134,-122.381571],[37.627319,-122.381677]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);
    // 4th Line 2
    L.polygon([[37.627727,-122.381614],[37.627689,-122.381642],[37.627527,-122.381600],[37.627546,-122.381345],[37.627594,-122.381310],[37.627779,-122.381416]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);
    // 4th Line 3
    L.polygon([[37.628188,-122.381354],[37.628150,-122.381382],[37.627988,-122.381340],[37.628007,-122.381085],[37.628055,-122.381050],[37.628240,-122.381156]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);
    // 4th Line 4
    L.polygon([[37.628648,-122.381093],[37.628610,-122.381121],[37.628448,-122.381079],[37.628467,-122.380824],[37.628515,-122.380789],[37.628700,-122.380895]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);
    // 41-7 A
    L.polygon([[37.626466,-122.382415],[37.626428,-122.382443],[37.626249,-122.382397],[37.626272,-122.382116],[37.626320,-122.382081],[37.626525,-122.382196]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);
    // 41-7 B
    L.polygon([[37.626343,-122.382119],[37.626305,-122.382147],[37.626126,-122.382101],[37.626149,-122.381820],[37.626197,-122.381785],[37.626402,-122.381900]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);
    // 41-11
    L.polygon([[37.626598,-122.381898],[37.626560,-122.381926],[37.626398,-122.381884],[37.626417,-122.381629],[37.626465,-122.381594],[37.626650,-122.381700]],{color:'#22c55e',weight:1.5,fillColor:'#22c55e',fillOpacity:0.08}).addTo(rampMap);
    // Draw group area outlines for visual clarity
    // First Line outline
    L.polyline([[37.627326,-122.384643],[37.627568,-122.384458],[37.627760,-122.384262],[37.627969,-122.384160],[37.628096,-122.384092],[37.628255,-122.383750]],{color:'#3b82f6',weight:1,opacity:0.4,dashArray:'6,4'}).addTo(rampMap);
    // 3rd Line outline
    L.polyline([[37.627275,-122.383044],[37.627487,-122.382926],[37.627666,-122.382821],[37.627842,-122.382658],[37.628033,-122.382537],[37.628219,-122.382413],[37.628422,-122.382279],[37.628618,-122.382191]],{color:'#06b6d4',weight:1,opacity:0.4,dashArray:'6,4'}).addTo(rampMap);
    // 4th Line outline
    L.polyline([[37.626942,-122.381854],[37.628783,-122.380812]],{color:'#ef4444',weight:1.5,opacity:0.4,dashArray:'6,4'}).addTo(rampMap);
    // FBO marker
    L.marker([37.62815,-122.38476],{interactive:false,icon:L.divIcon({className:'',html:'<div style="font-family:monospace;font-size:9px;font-weight:800;color:#fff;background:rgba(30,58,95,.85);padding:2px 6px;border-radius:4px;white-space:nowrap;border:1px solid rgba(255,255,255,.3)">SIGNATURE FBO</div>',iconSize:[90,16],iconAnchor:[45,8]})}).addTo(rampMap);
    // Legend
    L.control.custom=L.Control.extend({onAdd:function(){var d=L.DomUtil.create('div');d.innerHTML='<div style="background:rgba(0,0,0,.7);padding:6px 8px;border-radius:6px;font-family:monospace;font-size:7px;color:#fff;line-height:1.6"><span style="color:#3b82f6">■</span> 1st Line <span style="color:#22c55e">■</span> 2nd <span style="color:#06b6d4">■</span> 3rd <span style="color:#ef4444">■</span> 4th <span style="color:#f97316">■</span> Overflow <span style="color:#64748b">■</span> Tow-only</div>';return d;}});
    new L.control.custom({position:'bottomleft'}).addTo(rampMap);
  }
  setTimeout(function(){rampMap.invalidateSize();},200);
  loadRampPlanes();
}

function loadRampPlanes(){
  // Clear old markers
  for(var i=0;i<rampMarkers.length;i++)rampMap.removeLayer(rampMarkers[i]);
  rampMarkers=[];
  fetch('/fa/ground').then(function(r){return r.json();}).then(function(g){
    if(!g||!g.length)return;
    for(var i=0;i<g.length;i++){
      var f=g[i];
      var safeId=(f.ident||'').replace(/[^a-zA-Z0-9]/g,'');
      // Determine spot - use manual assignment or suggestion
      var assigned=window._parkingAssignments&&window._parkingAssignments[safeId]?window._parkingAssignments[safeId]:'';
      var isH=HELI[f.type]?true:false;
      var sug=suggestSpot(f.type,24,isH,f.ident);
      var spotName=assigned||sug.spot;
      var spotPos=RAMP_SPOTS[spotName];
      if(!spotPos)spotPos={lat:37.6234,lng:-122.3815,angle:0};
      // Add slight offset to prevent stacking
      var jLat=spotPos.lat+(Math.random()-0.5)*0.00008;
      var jLng=spotPos.lng+(Math.random()-0.5)*0.00008;
      // Aircraft icon - scaled by wingspan category
      var cat=getWsCat(f.type);
      var sz=cat<=2?20:cat<=3?28:cat<=5?36:44;
      var mdl=MODEL[f.type]||'';
      var ws=SPAN[f.type]||60;
      var col=isH?'#22c55e':'#f59e0b';
      var svgPath='M12 2C12.3 2 12.5 3 12.6 4.5L12.8 7.5L18 11C18.4 11.2 18.4 11.6 18 11.8L12.8 10.5L12.9 17L14.8 18.8C15 19 15 19.3 14.8 19.5L12 18.5L9.2 19.5C9 19.3 9 19 9.2 18.8L11.1 17L11.2 10.5L6 11.8C5.6 11.6 5.6 11.2 6 11L11.2 7.5L11.4 4.5C11.5 3 11.7 2 12 2Z';
      if(isH)svgPath='M12 4L14 8L20 9L14 12L14 18L17 20L12 19L7 20L10 18L10 12L4 9L10 8Z';
      var iconHtml='<div style="text-align:center"><svg width="'+sz+'" height="'+sz+'" viewBox="0 0 24 24" style="transform:rotate(280deg);filter:drop-shadow(0 1px 3px rgba(0,0,0,.5))"><path d="'+svgPath+'" fill="'+col+'" stroke="#fff" stroke-width="1"/></svg><div style="font-family:monospace;font-size:8px;font-weight:800;color:#fff;text-shadow:0 0 3px #000,0 0 3px #000;white-space:nowrap;margin-top:-2px">'+(f.ident||'?')+'</div><div style="font-family:monospace;font-size:6px;color:#ccc;text-shadow:0 0 2px #000;white-space:nowrap">'+(f.type||'')+(mdl?' '+mdl:'')+'</div></div>';
      var icon=L.divIcon({className:'',html:iconHtml,iconSize:[sz+40,sz+20],iconAnchor:[(sz+40)/2,(sz+20)/2]});
      var m=L.marker([jLat,jLng],{icon:icon,draggable:true}).addTo(rampMap);
      m._planeId=safeId;
      m._planeName=f.ident;
      // On drag end, find nearest spot and assign
      m.on('dragend',function(e){
        var pos=e.target.getLatLng();
        var nearest='',minDist=9999;
        for(var sn in RAMP_SPOTS){
          var sp=RAMP_SPOTS[sn];
          var d=Math.sqrt(Math.pow(pos.lat-sp.lat,2)+Math.pow(pos.lng-sp.lng,2));
          if(d<minDist){minDist=d;nearest=sn;}
        }
        if(nearest){
          window._parkingAssignments[e.target._planeId]=nearest;
          console.log('Moved '+e.target._planeName+' to '+nearest);
          // Snap to spot
          var sp2=RAMP_SPOTS[nearest];
          e.target.setLatLng([sp2.lat+(Math.random()-0.5)*0.00006,sp2.lng+(Math.random()-0.5)*0.00006]);
        }
      });
      rampMarkers.push(m);
    }
    // Update polygon colors: green=empty, red=occupied
    window._globalSpotMap={};
    var occupied={};
    for(var i=0;i<g.length;i++){
      var f2=g[i];
      var sid=(f2.ident||'').replace(/[^a-zA-Z0-9]/g,'');
      var assigned=window._parkingAssignments&&window._parkingAssignments[sid]?window._parkingAssignments[sid]:'';
      var isH2=HELI[f2.type]?true:false;
      var sug2=suggestSpot(f2.type,24,isH2,f2.ident);
      var spotN=assigned||sug2.spot;
      occupied[spotN]=true;
    }
    for(var pn in rampPolygons){
      var isOcc=!!occupied[pn];
      if(isOcc){
        rampPolygons[pn].setStyle({color:'#ef4444',fillColor:'#ef4444',fillOpacity:0.15});
      } else {
        rampPolygons[pn].setStyle({color:'#22c55e',fillColor:'#22c55e',fillOpacity:0.1});
      }
      // Update label color
      if(rampLabels[pn]){
        var lel=document.getElementById('rl_'+rampLabels[pn]);
        if(lel)lel.style.color=isOcc?'#ef4444':'#22c55e';
      }
    }
  }).catch(function(e){console.error('Ramp load error:',e);});
}
function closeRamp(){document.getElementById('ramp-overlay').style.display='none';}

// === 3D AIRSPACE VIEW (Three.js) ===
var scene3d,camera3d,renderer3d,globe,planeGroup,labelDiv,animId3d=null,cesium3DInterval=null;
var orbitAngle3d=0,orbitDist=25,orbitTilt=0.7,isDragging3d=false,prevMX=0,prevMY=0;
var globeRadius=100;
function latLonToVec3(lat,lon,alt){
  var phi=(90-lat)*Math.PI/180;
  var theta=(lon+180)*Math.PI/180;
  var r=globeRadius+(alt||0)*0.0003;
  return new THREE.Vector3(-r*Math.sin(phi)*Math.cos(theta),r*Math.cos(phi),r*Math.sin(phi)*Math.sin(theta));
}
function show3D(){
  document.getElementById('cesium-overlay').style.display='block';
  var ctr=document.getElementById('cesiumContainer');
  if(!scene3d){
    scene3d=new THREE.Scene();
    camera3d=new THREE.PerspectiveCamera(45,ctr.clientWidth/ctr.clientHeight,0.1,1000);
    renderer3d=new THREE.WebGLRenderer({antialias:true});
    renderer3d.setSize(ctr.clientWidth,ctr.clientHeight);
    renderer3d.setPixelRatio(Math.min(window.devicePixelRatio,2));
    renderer3d.setClearColor(0x050a15);
    ctr.appendChild(renderer3d.domElement);
    labelDiv=document.createElement('div');labelDiv.id='plane-labels';ctr.appendChild(labelDiv);
    // Click handler on overlay for label clicks
    document.getElementById('cesium-overlay').addEventListener('click',function(ev){
      var el=ev.target.closest('.plane-label');
      if(el&&el.dataset.ident){
        console.log('[3D] Label clicked:',el.dataset.ident);
        ev.stopPropagation();
        jumpToFlight(el.dataset.ident);
      }
    });
    // Hi-res globe
    var geo=new THREE.SphereGeometry(globeRadius,256,256);
    var loader=new THREE.TextureLoader();loader.crossOrigin='anonymous';
    var mat=new THREE.MeshPhongMaterial({color:0x4466aa,emissive:0x0a1020,specular:0x222222,shininess:15});
    globe=new THREE.Mesh(geo,mat);scene3d.add(globe);
    // Load hi-res earth texture
    // Load earth texture with fallback chain (CORS-friendly URLs only)
    function loadTex(url,fallback){
      loader.load(url,function(tex){
        tex.anisotropy=renderer3d.capabilities.getMaxAnisotropy();
        tex.minFilter=THREE.LinearMipmapLinearFilter;tex.magFilter=THREE.LinearFilter;
        globe.material.map=tex;globe.material.color.setHex(0xffffff);globe.material.needsUpdate=true;
        console.log('[3D] Earth texture loaded:',url);
      },undefined,function(err){
        console.warn('[3D] Texture failed:',url);
        if(fallback)loadTex(fallback);
      });
    }
    loadTex(
      'https://unpkg.com/three-globe@2.33.0/example/img/earth-blue-marble.jpg',
      'https://unpkg.com/three-globe@2.24.4/example/img/earth-blue-marble.jpg'
    );
    // Also load night lights overlay for contrast
    loader.load('https://unpkg.com/three-globe@2.33.0/example/img/earth-night.jpg',function(ntex){
      ntex.anisotropy=renderer3d.capabilities.getMaxAnisotropy();
      globe.material.emissiveMap=ntex;globe.material.emissive.setHex(0xffaa44);globe.material.emissiveIntensity=0.15;
      globe.material.needsUpdate=true;
      console.log('[3D] Night lights loaded');
    });
    // Atmosphere
    var aGeo=new THREE.SphereGeometry(globeRadius*1.012,64,64);
    var aMat=new THREE.MeshBasicMaterial({color:0x4488ff,transparent:true,opacity:0.06,side:THREE.BackSide});
    scene3d.add(new THREE.Mesh(aGeo,aMat));
    // Lights
    scene3d.add(new THREE.AmbientLight(0x889aab,1.0));
    var dl=new THREE.DirectionalLight(0xffffff,1.2);dl.position.set(5,3,5);scene3d.add(dl);
    // KSFO dot
    var sp=latLonToVec3(37.6213,-122.3790,300);
    var sm=new THREE.Mesh(new THREE.SphereGeometry(0.35,16,16),new THREE.MeshBasicMaterial({color:0x3b82f6}));
    sm.position.copy(sp);scene3d.add(sm);
    // KSFO label
    var ksfoLabel=document.createElement('div');
    ksfoLabel.style.cssText='position:absolute;font:bold 12px monospace;color:#3b82f6;background:rgba(0,0,0,.5);padding:2px 6px;border-radius:3px;pointer-events:none;z-index:51';
    ksfoLabel.textContent='KSFO';
    ctr.appendChild(ksfoLabel);
    window._ksfoLabel=ksfoLabel;window._ksfoPos=sp;
    // Hi-res OSM tile overlay for Bay Area
    var tc=document.createElement('canvas');tc.width=2048;tc.height=2048;
    var tx2d=tc.getContext('2d');
    var tz=10,btx=160,bty=392,gs=8,tld=0,ttl=gs*gs,tsz=tc.width/gs;
    for(var tiy=0;tiy<gs;tiy++){for(var tix=0;tix<gs;tix++){(function(txx,tyy){
      var im=new Image();im.crossOrigin='anonymous';
      im.onload=function(){
        tx2d.drawImage(im,txx*tsz,tyy*tsz,tsz,tsz);tld++;
        if(tld>=ttl){
          tx2d.globalCompositeOperation='difference';tx2d.fillStyle='white';tx2d.fillRect(0,0,2048,2048);
          tx2d.globalCompositeOperation='source-over';tx2d.fillStyle='rgba(0,0,20,0.25)';tx2d.fillRect(0,0,2048,2048);
          var tt=new THREE.CanvasTexture(tc);tt.anisotropy=renderer3d.capabilities.getMaxAnisotropy();
          var np=Math.PI-2*Math.PI*bty/Math.pow(2,tz);var sp2=Math.PI-2*Math.PI*(bty+gs)/Math.pow(2,tz);
          var lN=180/Math.PI*Math.atan(0.5*(Math.exp(np)-Math.exp(-np)));
          var lS=180/Math.PI*Math.atan(0.5*(Math.exp(sp2)-Math.exp(-sp2)));
          var lW=btx/Math.pow(2,tz)*360-180;var lE=(btx+gs)/Math.pow(2,tz)*360-180;
          var sw=64,sh=64,vts=[],uv2=[],idx2=[];
          for(var iy=0;iy<=sh;iy++){for(var ix=0;ix<=sw;ix++){
            var u2=ix/sw,v2=iy/sh;var la=lN+(lS-lN)*v2;var lo=lW+(lE-lW)*u2;
            var ph=(90-la)*Math.PI/180;var th=(lo+180)*Math.PI/180;var rr=globeRadius+0.08;
            vts.push(-rr*Math.sin(ph)*Math.cos(th),rr*Math.cos(ph),rr*Math.sin(ph)*Math.sin(th));
            uv2.push(u2,1-v2);
          }}
          for(var iy=0;iy<sh;iy++){for(var ix=0;ix<sw;ix++){
            var aa=iy*(sw+1)+ix,bb=aa+1,cc=aa+(sw+1),dd=cc+1;
            idx2.push(aa,cc,bb,bb,cc,dd);
          }}
          var pg=new THREE.BufferGeometry();
          pg.setAttribute('position',new THREE.Float32BufferAttribute(vts,3));
          pg.setAttribute('uv',new THREE.Float32BufferAttribute(uv2,2));
          pg.setIndex(idx2);pg.computeVertexNormals();
          scene3d.add(new THREE.Mesh(pg,new THREE.MeshBasicMaterial({map:tt})));
          console.log('[3D] Hi-res tiles loaded');
        }
      };
      im.onerror=function(){tld++;};
      im.src='https://tile.openstreetmap.org/'+tz+'/'+(btx+txx)+'/'+(bty+tyy)+'.png';
    })(tix,tiy);}}
    planeGroup=new THREE.Group();scene3d.add(planeGroup);
    // === CONTROLS ===
    var ov=document.getElementById('cesium-overlay');
    ov.addEventListener('wheel',function(ev){
      ev.preventDefault();ev.stopPropagation();
      orbitDist=Math.max(5,Math.min(60,orbitDist+ev.deltaY*0.02));
    },{passive:false});
    ov.addEventListener('mousedown',function(ev){isDragging3d=true;prevMX=ev.clientX;prevMY=ev.clientY;});
    window.addEventListener('mousemove',function(ev){
      if(!isDragging3d)return;
      orbitAngle3d+=(ev.clientX-prevMX)*0.004;
      orbitTilt=Math.max(0.15,Math.min(1.3,orbitTilt-(ev.clientY-prevMY)*0.003));
      prevMX=ev.clientX;prevMY=ev.clientY;
    });
    window.addEventListener('mouseup',function(){isDragging3d=false;});
    window.addEventListener('resize',function(){
      if(!renderer3d)return;
      camera3d.aspect=ctr.clientWidth/ctr.clientHeight;
      camera3d.updateProjectionMatrix();
      renderer3d.setSize(ctr.clientWidth,ctr.clientHeight);
    });
  }
  // KSFO position on globe
  var sfoPos=latLonToVec3(37.6213,-122.3790,0);
  var sfoN=sfoPos.clone().normalize();
  // Build tangent frame at SFO
  var tmpUp=new THREE.Vector3(0,1,0);
  var tanU=new THREE.Vector3().crossVectors(tmpUp,sfoN).normalize();
  var tanV=new THREE.Vector3().crossVectors(sfoN,tanU).normalize();
  function animate3d(){
    animId3d=requestAnimationFrame(animate3d);
    if(!isDragging3d)orbitAngle3d+=0.0008;
    // Camera position: orbit circle on tangent plane + height along normal
    var cx=Math.cos(orbitAngle3d)*orbitDist;
    var cy=Math.sin(orbitAngle3d)*orbitDist;
    var ch=orbitDist*orbitTilt;
    var camP=sfoPos.clone().add(tanU.clone().multiplyScalar(cx)).add(tanV.clone().multiplyScalar(cy)).add(sfoN.clone().multiplyScalar(ch));
    camera3d.position.copy(camP);
    camera3d.up.copy(sfoN);
    camera3d.lookAt(sfoPos);
    renderer3d.render(scene3d,camera3d);
    updateLabels3d();
    // Update KSFO label position
    if(window._ksfoLabel&&window._ksfoPos){
      var kp=window._ksfoPos.clone().project(camera3d);
      if(kp.z<1){
        var kx=(kp.x*0.5+0.5)*ctr.clientWidth;
        var ky=(-kp.y*0.5+0.5)*ctr.clientHeight;
        window._ksfoLabel.style.left=kx+'px';window._ksfoLabel.style.top=ky+'px';
        window._ksfoLabel.style.display='block';
      } else {window._ksfoLabel.style.display='none';}
    }
  }
  animate3d();
  load3DPlanes();
  if(cesium3DInterval)clearInterval(cesium3DInterval);
  cesium3DInterval=setInterval(load3DPlanes,10000);
}
var plane3dData={};
function load3DPlanes(){
  if(!scene3d||!planeGroup)return;
  fetch('/osky/states/all?extended=1&'+bboxQS()).then(function(r){return r.json();}).then(function(d){
    if(!d||!d.states)return;
    var seen={};
    for(var i=0;i<d.states.length;i++){
      var s=d.states[i];var cs=(s[1]||'').trim();
      var lat=s[6],lon=s[5],alt=s[7]||s[13]||0;
      if(!lat||!lon)continue;
      var csUp=cs.toUpperCase().replace(/ /g,'');
      if(!csUp)continue;
      var fi=faMapSet[csUp];if(!fi)continue;
      seen[csUp]=true;
      var pos=latLonToVec3(lat,lon,alt);
      var col=fi.type==='arr'?0xf59e0b:0xef4444;
      if(plane3dData[csUp]){
        plane3dData[csUp].mesh.position.copy(pos);
        plane3dData[csUp].glow.position.copy(pos);
        plane3dData[csUp].trail.push(pos.clone());
        if(plane3dData[csUp].trail.length>60)plane3dData[csUp].trail.shift();
        if(plane3dData[csUp].line){planeGroup.remove(plane3dData[csUp].line);plane3dData[csUp].line.geometry.dispose();}
        if(plane3dData[csUp].trail.length>1){
          var tG=new THREE.BufferGeometry().setFromPoints(plane3dData[csUp].trail);
          plane3dData[csUp].line=new THREE.Line(tG,new THREE.LineBasicMaterial({color:col,transparent:true,opacity:0.5}));
          planeGroup.add(plane3dData[csUp].line);
        }
        plane3dData[csUp].label=fi.ident||cs;
      } else {
        var pm=new THREE.Mesh(new THREE.SphereGeometry(0.25,8,8),new THREE.MeshBasicMaterial({color:col}));
        pm.position.copy(pos);planeGroup.add(pm);
        var gm=new THREE.Mesh(new THREE.SphereGeometry(0.5,8,8),new THREE.MeshBasicMaterial({color:col,transparent:true,opacity:0.25}));
        gm.position.copy(pos);planeGroup.add(gm);
        plane3dData[csUp]={mesh:pm,glow:gm,trail:[pos.clone()],line:null,label:fi.ident||cs,col:col};
      }
    }
    for(var k in plane3dData){
      if(!seen[k]){planeGroup.remove(plane3dData[k].mesh);planeGroup.remove(plane3dData[k].glow);if(plane3dData[k].line)planeGroup.remove(plane3dData[k].line);delete plane3dData[k];}
    }
  }).catch(function(e){console.error('3D:',e);});
}
function updateLabels3d(){
  if(!labelDiv||!camera3d)return;
  var w=renderer3d.domElement.clientWidth,h=renderer3d.domElement.clientHeight;
  var lbs=[];
  for(var k in plane3dData){
    var pd=plane3dData[k];var p=pd.mesh.position.clone();p.project(camera3d);
    if(p.z>1)continue;
    var x=(p.x*0.5+0.5)*w,y=(-p.y*0.5+0.5)*h;
    if(x<-50||x>w+50||y<-50||y>h+50)continue;
    var fi=faMapSet[k];var eta='';
    if(fi&&fi.arriveISO){var m=Math.round((new Date(fi.arriveISO).getTime()-Date.now())/60000);if(m>0)eta=m+'m';else if(m>-5)eta='arriving';else eta='landed';}
    lbs.push({x:x,y:y,lb:pd.label,eta:eta,c:pd.col});
  }
  for(var p2=0;p2<3;p2++){for(var i=0;i<lbs.length;i++){for(var j=i+1;j<lbs.length;j++){
    if(Math.abs(lbs[i].x-lbs[j].x)<90&&Math.abs(lbs[i].y-lbs[j].y)<22){
      var mid=(lbs[i].y+lbs[j].y)/2;lbs[i].y=mid-12;lbs[j].y=mid+12;
    }
  }}}
  var htm='';
  for(var i=0;i<lbs.length;i++){
    var l=lbs[i],ch=l.c===0xf59e0b?'#f59e0b':'#ef4444';
    var et=l.eta?'<br><span style="font-size:7px;color:'+ch+'">'+l.eta+'</span>':'';
    htm+='<div class="plane-label" data-ident="'+l.lb.replace(/[^a-zA-Z0-9]/g,'')+'" style="left:'+l.x+'px;top:'+l.y+'px;border-left:2px solid '+ch+'">'+l.lb+et+'</div>';
  }
  labelDiv.innerHTML=htm;
}
function jumpToFlight(id){
  console.log('[3D] Jumping to flight:',id);
  close3D();
  setTimeout(function(){
    var found=false;
    var rows=document.querySelectorAll('.zp');
    for(var i=0;i<rows.length;i++){
      if(rows[i].dataset.zp===id){
        found=true;
        var r=rows[i].closest('.fr');
        if(r){
          r.scrollIntoView({behavior:'smooth',block:'center'});
          r.style.background='rgba(59,130,246,.3)';
          r.style.boxShadow='inset 0 0 0 2px #3b82f6';
          r.style.transition='background 0.3s';
          var rr=r;
          setTimeout(function(){rr.style.background='';rr.style.boxShadow='';},4000);
        }
        zoomToPlane(id);
        break;
      }
    }
    if(!found){console.log('Flight not found in board:',id);}
  },500);
}
function close3D(){
  document.getElementById('cesium-overlay').style.display='none';
  if(animId3d){cancelAnimationFrame(animId3d);animId3d=null;}
  if(cesium3DInterval){clearInterval(cesium3DInterval);cesium3DInterval=null;}
}
function bboxQS(){var b=bbox();return 'lamin='+b.la1+'&lomin='+b.lo1+'&lamax='+b.la2+'&lomax='+b.lo2;}

// Resize bar drag
(function(){
  var bar=document.getElementById('resizeBar');
  var row=document.getElementById('mapRow');
  var dragging=false,startY=0,startH=0;
  bar.addEventListener('mousedown',function(e){
    dragging=true;startY=e.clientY;startH=row.offsetHeight;
    document.body.style.cursor='ns-resize';document.body.style.userSelect='none';
    e.preventDefault();
  });
  document.addEventListener('mousemove',function(e){
    if(!dragging)return;
    var dy=e.clientY-startY;
    var newH=Math.max(120,Math.min(700,startH+dy));
    row.style.height=newH+'px';row.style.minHeight=newH+'px';
    if(leafMap){leafMap.invalidateSize();var a=getAP();leafMap.setView([a.lat,a.lon]);}
    drawChart();
  });
  document.addEventListener('mouseup',function(){
    if(!dragging)return;
    dragging=false;document.body.style.cursor='';document.body.style.userSelect='';
    if(leafMap){leafMap.invalidateSize();var a=getAP();leafMap.setView([a.lat,a.lon]);}
    drawChart();
  });
})();

// Auto dark/light mode based on sunset/sunrise
function checkTheme(){
  var h=new Date().getHours();
  var isDark=h>=19||h<7;
  if(isDark)document.documentElement.classList.add('dark');
  else document.documentElement.classList.remove('dark');
  if(window._tileLayer){
    window._tileLayer.setUrl(isDark?window._darkTileUrl:window._lightTileUrl);
  }
}
checkTheme();
setInterval(checkTheme,60000);</` + `script>
</html>`;
}
