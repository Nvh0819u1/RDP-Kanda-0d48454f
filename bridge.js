// KANDA RDP BRIDGE v6 - keep WS alive + lazy relay
const net = require('net');
const http = require('http');
const HTTP_PORT = parseInt(process.env.PORT || '8080', 10);
const RDP_PORT  = parseInt(process.env.RDP_PORT || '3389', 10);
const WebSocketServer = require('ws').WebSocketServer;

process.on('uncaughtException', (e) => { console.log('[bridge] uncaught: ' + e.message); });
process.on('unhandledRejection', (e) => { console.log('[bridge] unhandled: ' + e); });

function log(msg) { console.log('[bridge] ' + msg); }

const runners = new Set();
let rdpClient = null;
let activeWS = null;

const httpServer = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200); res.end('OK'); return; }
  res.writeHead(200); res.end('OK');
});

const wss = new WebSocketServer({ server: httpServer, path: '/ws' });

wss.on('connection', (ws) => {
  runners.add(ws);
  log('runner connected (total=' + runners.size + ')');

  ws._kaTimer = setInterval(() => {
    try { if (ws.readyState === 1) ws.send('__KA__'); } catch(e) {}
  }, 15000);

  ws.on('pong', () => {});

  if (rdpClient && !rdpClient.destroyed && !activeWS) {
    doPair(ws, rdpClient);
  }

  ws.on('message', (data, isBinary) => {
    try {
      const msg = Buffer.isBuffer(data) ? data : Buffer.from(data);
      if (msg.length === 6 && msg.toString() === '__KA__') return;
      if (activeWS === ws && rdpClient && !rdpClient.destroyed) {
        rdpClient.write(msg);
      }
    } catch(e) { log('msg err: ' + e.message); }
  });

  ws.on('close', (code, reason) => {
    runners.delete(ws);
    if (ws._kaTimer) clearInterval(ws._kaTimer);
    if (activeWS === ws) {
      activeWS = null;
      log('runner gone, RDP client kept alive');
    }
    log('runner gone (total=' + runners.size + ') code=' + code);
  });

  ws.on('error', () => {});
});

function doPair(ws, sock) {
  activeWS = ws;
  log('paired runner <-> RDP client');

  sock.removeAllListeners('data');
  sock.removeAllListeners('close');
  sock.removeAllListeners('error');

  sock.on('data', (buf) => {
    try { if (ws.readyState === 1) ws.send(buf); } catch(e) {}
  });
  sock.on('close', () => {
    log('RDP client disconnected - keeping runner WS alive');
    rdpClient = null;
    activeWS = null;
  });
  sock.on('error', () => {
    log('RDP client error');
    rdpClient = null;
    activeWS = null;
  });
}

const tcpServer = net.createServer((sock) => {
  log('RDP client connecting from ' + sock.remoteAddress);

  if (rdpClient && !rdpClient.destroyed) {
    log('reject - already have RDP client');
    sock.destroy();
    return;
  }

  rdpClient = sock;

  if (runners.size > 0) {
    const ws = runners.values().next().value;
    if (ws && ws.readyState === 1) {
      doPair(ws, sock);
      return;
    }
  }

  log('no runner, RDP client waiting');
  sock._waitTimer = setTimeout(() => {
    log('RDP client timeout (30s no runner)');
    try { sock.destroy(); } catch(e){}
    if (rdpClient === sock) { rdpClient = null; }
  }, 30000);
  sock.on('close', () => {
    if (sock._waitTimer) clearTimeout(sock._waitTimer);
    if (rdpClient === sock) { rdpClient = null; }
  });
});

httpServer.listen(HTTP_PORT, '0.0.0.0', () => log('HTTP+WS on :' + HTTP_PORT));
tcpServer.listen(RDP_PORT, '0.0.0.0', () => log('TCP RDP on :' + RDP_PORT));