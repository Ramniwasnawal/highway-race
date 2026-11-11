const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');
const PORT = process.env.PORT || 3000;
const INDEX = path.join(__dirname, 'index.html');

const server = http.createServer((req, res) => {
  const u = req.url.split('?')[0];
  if (u === '/' || u === '/index.html') {
    fs.readFile(INDEX, (e, d) => {
      if (e) { res.writeHead(500); return res.end('error'); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(d);
    });
    return;
  }
  if (u === '/health') { res.writeHead(200); return res.end('ok'); }
  res.writeHead(404); res.end('not found');
});

const wss = new WebSocketServer({ noServer: true, maxPayload: 2048 });
const rooms = new Map();
let nextId = 1;

server.on('upgrade', (req, sock, head) => {
  const url = new URL(req.url, 'http://x');
  const code = (url.searchParams.get('room') || '').toLowerCase();
  if (url.pathname !== '/ws' || !/^[a-z0-9][a-z0-9_.-]{0,47}$/.test(code)) { sock.destroy(); return; }
  wss.handleUpgrade(req, sock, head, ws => onConn(ws, code));
});

function push(code) {
  const r = rooms.get(code);
  if (!r || r.timer) return;
  r.timer = setTimeout(() => {
    r.timer = null;
    const peers = [...r.clients].map(([id, c]) => ({ id, p: c.p }));
    const s = JSON.stringify({ t: 's', peers });
    for (const c of r.clients.values()) if (c.ws.readyState === 1) c.ws.send(s);
  }, 40);
}

function onConn(ws, code) {
  let r = rooms.get(code);
  if (!r) { r = { clients: new Map(), timer: null }; rooms.set(code, r); }
  if (r.clients.size >= 2) { ws.send(JSON.stringify({ t: 'full' })); ws.close(); return; }
  const id = 'p' + (nextId++);
  r.clients.set(id, { ws, p: {} });
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.send(JSON.stringify({ t: 'hi', id }));
  push(code);

  let count = 0;
  const reset = setInterval(() => { count = 0; }, 1000);
  ws.on('message', buf => {
    if (++count > 60) return;
    let m; try { m = JSON.parse(buf.toString()); } catch (e) { return; }
    if (!m || m.t !== 'p' || typeof m.p !== 'object' || !m.p || Array.isArray(m.p)) return;
    const c = r.clients.get(id); if (!c) return;
    for (const k of Object.keys(m.p).slice(0, 12)) {
      if (!/^[a-z]{1,8}$/.test(k)) continue;
      const v = m.p[k];
      if (v === null) delete c.p[k];
      else if (typeof v === 'number' || typeof v === 'boolean' || (typeof v === 'string' && v.length <= 16)) c.p[k] = v;
    }
    push(code);
  });
  ws.on('close', () => {
    clearInterval(reset);
    r.clients.delete(id);
    if (!r.clients.size) rooms.delete(code); else push(code);
  });
}

setInterval(() => {
  for (const r of rooms.values()) for (const c of r.clients.values()) {
    if (c.ws.isAlive === false) { c.ws.terminate(); continue; }
    c.ws.isAlive = false; c.ws.ping();
  }
}, 30000);

server.listen(PORT, () => console.log('listening on ' + PORT));
