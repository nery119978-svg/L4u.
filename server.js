'use strict';
/*
 * Servidor de Los 4 Últimos.
 * - Sirve el juego (index.html) inyectándole un "puente" que reemplaza claude.use('room').
 * - Hace de servidor de salas por WebSocket (/ws) para cooperativo y versus.
 *
 * Uso:   npm install   y luego   npm start      (puerto 3000, o PORT=8080 npm start)
 * Pon el HTML del juego en esta carpeta con el nombre index.html
 * (o en partes: index.html.part1, index.html.part2; o GAME_FILE=ruta/al/archivo.html).
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = +process.env.PORT || 3000;
const HOST = process.env.HOST || '0.0.0.0';
const MAX_PER_ROOM = +process.env.MAX_PER_ROOM || 16; // tope de seguridad; el juego ya limita a 3-8
const MAX_ROOMS = +process.env.MAX_ROOMS || 500;

/* ---------- Puente que se inyecta en el navegador ---------- */
const SHIM = `<script>(function(){
function wsUrl(){return (location.protocol==='https:'?'wss://':'ws://')+location.host+'/ws'}
function join(name){
  return new Promise(function(resolve,reject){
    var peers={},me=null,cb=null,last=null,closed=false,ws=null,opened=false,retry=0;
    var ch={
      onPeers:function(f){cb=f;setTimeout(emit,0)},
      presence:function(p){last=p;if(me)peers[me]=p;if(ws&&ws.readyState===1)ws.send(JSON.stringify({t:'p',d:p}));return Promise.resolve()},
      leave:function(){closed=true;try{ws&&ws.close()}catch(e){}return Promise.resolve()}
    };
    function emit(left){
      if(!cb)return;
      var arr=Object.keys(peers).map(function(id){return{peer:id,isMe:id===me,sameTab:id===me,kind:'viewer',presence:peers[id]}});
      try{cb({peers:arr,left:left||[]})}catch(e){console.error(e)}
    }
    function connect(){
      ws=new WebSocket(wsUrl());
      ws.onopen=function(){ws.send(JSON.stringify({t:'join',room:String(name)}))};
      ws.onmessage=function(ev){
        var m;try{m=JSON.parse(ev.data)}catch(e){return}
        if(m.t==='hello'){me=m.id;peers={};(m.peers||[]).forEach(function(q){peers[q.peer]=q.presence});peers[me]=last||{};
          if(!opened){opened=true;resolve(ch)}if(last)ws.send(JSON.stringify({t:'p',d:last}));emit()}
        else if(m.t==='p'){peers[m.peer]=m.d;emit()}
        else if(m.t==='l'){var pr=peers[m.peer];delete peers[m.peer];emit([{peer:m.peer,kind:'viewer',presence:pr}])}
        else if(m.t==='err'&&!opened){opened=true;reject(new Error(m.msg))}
      };
      ws.onclose=function(){
        if(closed)return;
        if(!opened){opened=true;reject(new Error('sin conexión'));return}
        setTimeout(connect,Math.min(5000,500*(++retry)));
      };
      ws.onerror=function(){};
    }
    connect();
  });
}
window.claude=window.claude||{};
window.claude.use=function(n){return Promise.resolve(n==='room'?{join:join}:null)};
})();</script>`;

/* ---------- Carga del juego ---------- */
function loadGame() {
  // 1) un solo archivo: index.html (o GAME_FILE)
  const cands = [process.env.GAME_FILE, 'index.html', 'Los_4_Últimos.html', 'Los_4_Ultimos.html']
    .filter(Boolean).map(f => path.resolve(__dirname, f));
  const one = cands.find(f => fs.existsSync(f));
  if (one) return { name: path.basename(one), buf: fs.readFileSync(one) };
  // 2) en partes (para subir a GitHub por la web, límite de 25 MB): index.html.part1, index.html.part2, ...
  const num = f => +/(\d+)$/.exec(f)[1];
  const parts = fs.readdirSync(__dirname).filter(f => /^index\.html\.part\d+$/.test(f)).sort((a, b) => num(a) - num(b));
  if (parts.length) return { name: parts.join(' + '), buf: Buffer.concat(parts.map(f => fs.readFileSync(path.join(__dirname, f)))) };
  return null;
}
const game = loadGame();
if (!game) {
  console.error('No encuentro el juego. Pon index.html en esta carpeta (o index.html.part1, index.html.part2...).');
  process.exit(1);
}
let html = game.buf.toString('utf8');
html = html.includes('<head>') ? html.replace('<head>', '<head>' + SHIM) : SHIM + html;
const raw = Buffer.from(html, 'utf8');
html = null;
const gz = zlib.gzipSync(raw, { level: 6 });
console.log(`Juego cargado: ${game.name} (${(raw.length / 1048576).toFixed(1)} MB, ${(gz.length / 1048576).toFixed(1)} MB comprimido)`);

/* ---------- HTTP ---------- */
const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/health') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('ok'); }
  if (url === '/' || url === '/index.html') {
    const useGz = /\bgzip\b/.test(req.headers['accept-encoding'] || '');
    const body = useGz ? gz : raw;
    const h = { 'Content-Type': 'text/html; charset=utf-8', 'Content-Length': body.length, 'Cache-Control': 'no-cache' };
    if (useGz) h['Content-Encoding'] = 'gzip';
    res.writeHead(200, h);
    return res.end(req.method === 'HEAD' ? undefined : body);
  }
  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('No encontrado');
});

/* ---------- Salas ---------- */
const rooms = new Map(); // nombre -> Map(id -> {ws, presence})
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 64 * 1024 });

function send(ws, obj) { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); }
function leaveRoom(c) {
  if (!c.room) return;
  const r = rooms.get(c.room);
  if (r) {
    r.delete(c.id);
    for (const o of r.values()) send(o.ws, { t: 'l', peer: c.id });
    if (!r.size) rooms.delete(c.room);
  }
  c.room = null;
}

wss.on('connection', ws => {
  const c = { id: crypto.randomBytes(5).toString('hex'), ws, room: null, presence: {}, alive: true, last: 0 };
  ws.on('pong', () => { c.alive = true; });
  ws.on('message', data => {
    let m;
    try { m = JSON.parse(data.toString()); } catch (e) { return; }
    if (m.t === 'join' && !c.room) {
      const name = String(m.room || '').slice(0, 80);
      if (!name) return send(ws, { t: 'err', msg: 'Sala inválida' });
      let r = rooms.get(name);
      if (!r) {
        if (rooms.size >= MAX_ROOMS) return send(ws, { t: 'err', msg: 'Servidor lleno' });
        r = new Map(); rooms.set(name, r);
      }
      if (r.size >= MAX_PER_ROOM) return send(ws, { t: 'err', msg: 'Sala llena' });
      const others = [...r.values()].map(o => ({ peer: o.id, presence: o.presence }));
      r.set(c.id, c); c.room = name;
      send(ws, { t: 'hello', id: c.id, peers: others });
    } else if (m.t === 'p' && c.room && m.d && typeof m.d === 'object') {
      c.presence = m.d;
      const r = rooms.get(c.room);
      if (!r) return;
      const msg = JSON.stringify({ t: 'p', peer: c.id, d: c.presence });
      for (const o of r.values()) if (o !== c && o.ws.readyState === 1) o.ws.send(msg);
    }
  });
  ws.on('close', () => leaveRoom(c));
  ws.on('error', () => {});
});

// Limpia conexiones muertas
setInterval(() => {
  for (const ws of wss.clients) {
    if (ws._dead) { ws.terminate(); continue; }
    ws._dead = true; ws.ping();
  }
}, 30000);
wss.on('connection', ws => { ws._dead = false; ws.on('pong', () => { ws._dead = false; }); });

server.listen(PORT, HOST, () => console.log(`Los 4 Últimos en http://localhost:${PORT}  (salas por WebSocket en /ws)`));
