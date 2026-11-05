const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
let OAuth2Client = null; try { ({ OAuth2Client } = require('google-auth-library')); } catch (e) {}
let webpush = null; try { webpush = require('web-push'); } catch (e) {}

const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 5e7, cors: { origin: '*' } });

// ================= STORAGE =================
const DB_FILE = path.join(process.env.DATA_DIR || __dirname, 'db.json');
let db = {
  totalVisits: 0, visitors: [], users: [], friends: {}, requests: {}, global: [], dms: {}, files: [],
  profiles: {}, blocks: {}, accounts: {}, sessions: {}, push: {}, bans: [], strikes: {}, ips: {}, cids: {}
};
let M = null;
const logErr = e => console.error('Storage error:', e.message);
const metaOf = () => ({
  totalVisits: db.totalVisits, visitors: db.visitors, users: db.users, friends: db.friends, requests: db.requests,
  profiles: db.profiles, blocks: db.blocks, accounts: db.accounts, sessions: db.sessions, push: db.push,
  bans: db.bans, strikes: db.strikes, ips: db.ips, cids: db.cids
});

async function initStorage() {
  if (process.env.MONGODB_URI) {
    try {
      const { MongoClient } = require('mongodb');
      const client = new MongoClient(process.env.MONGODB_URI);
      for (let i = 1; ; i++) {
        try { await client.connect(); break; }
        catch (e) { if (i >= 3) throw e; console.log('MongoDB retry ' + i + '...'); await new Promise(r => setTimeout(r, 3000)); }
      }
      const d = client.db(process.env.MONGODB_DB || 'geminios');
      M = { meta: d.collection('meta'), msgs: d.collection('messages'), files: d.collection('files') };
      const meta = await M.meta.findOne({ _id: 'main' });
      if (meta) { delete meta._id; Object.assign(db, meta); }
      (await M.msgs.find().sort({ t: 1 }).toArray()).forEach(m => {
        if (m.chat === 'global') db.global.push(m);
        else (db.dms[m.chat] = db.dms[m.chat] || []).push(m);
      });
      db.files = await M.files.find().sort({ t: 1 }).toArray();
      console.log('Storage: MongoDB Atlas');
      return;
    } catch (e) {
      console.error('!!! MongoDB FAILED: accounts will be LOST on restart. Reason:', e.message);
      M = null;
    }
  } else console.error('!!! MONGODB_URI is not set: accounts will be LOST on restart.');
  try { Object.assign(db, JSON.parse(fs.readFileSync(DB_FILE, 'utf8'))); } catch (e) {}
  console.log('Storage: local file');
}
async function flushMeta() {
  try {
    if (M) await M.meta.replaceOne({ _id: 'main' }, { _id: 'main', ...metaOf() }, { upsert: true });
    else fs.writeFileSync(DB_FILE, JSON.stringify(db));
  } catch (e) { logErr(e); }
}
let metaTimer;
const pMeta = () => { clearTimeout(metaTimer); metaTimer = setTimeout(flushMeta, 1000); };
const pMsg = m => { if (M) M.msgs.replaceOne({ _id: m.id }, { ...m, _id: m.id }, { upsert: true }).catch(logErr); else pMeta(); };
const pMsgDel = id => { if (M) M.msgs.deleteOne({ _id: id }).catch(logErr); else pMeta(); };
const pFile = f => { if (M) M.files.replaceOne({ _id: f.id }, { ...f, _id: f.id }, { upsert: true }).catch(logErr); else pMeta(); };
const pFileDel = id => { if (M) M.files.deleteOne({ _id: id }).catch(logErr); else pMeta(); };
process.on('SIGTERM', async () => { clearTimeout(metaTimer); await flushMeta(); process.exit(0); });

// ================= HELPERS =================
const online = {};
const recording = new Set();
const MAX_BYTES = 14e6;
const A = () => io.to('authed');
const hasOwn = (o, k) => Object.prototype.hasOwnProperty.call(o, k);
const arr = (o, k) => (hasOwn(o, k) ? o[k] : (o[k] = []));
const prof = n => (hasOwn(db.profiles, n) ? db.profiles[n] : (db.profiles[n] = { lastSeen: 0, visits: 0, status: '', emoji: '😀', wins: 0 }));
const directory = () => db.users.map(n => ({ name: n, ...prof(n) }));
const key = (a, b) => [a, b].sort().join('|');
const uid = () => Date.now().toString() + Math.random().toString(36).slice(2, 6);
const to = (name, ev, data) => { if (online[name]) io.to(online[name]).emit(ev, data); };
const findUser = n => db.users.find(u => u.toLowerCase() === String(n || '').trim().toLowerCase());
const isBlocked = (a, b) => arr(db.blocks, a).includes(b) || arr(db.blocks, b).includes(a);

const pushOn = !!(webpush && process.env.VAPID_PUBLIC && process.env.VAPID_PRIVATE);
if (pushOn) webpush.setVapidDetails(process.env.VAPID_SUBJECT || 'mailto:admin@example.com', process.env.VAPID_PUBLIC, process.env.VAPID_PRIVATE);
function sendPush(name, payload) {
  if (!pushOn) return;
  arr(db.push, name).slice().forEach(s =>
    webpush.sendNotification(s, JSON.stringify(payload), { TTL: payload.type === 'call' ? 45 : 3600, urgency: 'high' })
      .catch(e => { if (e.statusCode === 404 || e.statusCode === 410) { db.push[name] = arr(db.push, name).filter(x => x.endpoint !== s.endpoint); pMeta(); } }));
}

const gClient = (OAuth2Client && process.env.GOOGLE_CLIENT_ID) ? new OAuth2Client(process.env.GOOGLE_CLIENT_ID) : null;
const akey = n => n.toLowerCase();
const getAcc = k => (hasOwn(db.accounts, k) ? db.accounts[k] : null);
const cleanName = n => String(n || '').replace(/[|<>.$]/g, '').replace(/\s+/g, ' ').trim().slice(0, 24);
const badName = n => n.length < 2 || ['global', '__proto__'].includes(n.toLowerCase());
const hashPw = (pw, salt) => crypto.scryptSync(pw, salt, 32).toString('hex');
const sha = s => crypto.createHash('sha256').update(s).digest('hex');
const nameOfToken = t => (hasOwn(db.sessions, sha(String(t || ''))) ? db.sessions[sha(String(t))] : null);
const newSession = name => {
  Object.keys(db.sessions).forEach(h => { if (db.sessions[h] === name) delete db.sessions[h]; });
  const t = crypto.randomBytes(32).toString('hex');
  db.sessions[sha(t)] = name; pMeta(); return t;
};
const fails = {};
const tooMany = k => fails[k] && fails[k].n >= 5 && Date.now() - fails[k].t < 300000;
const addFail = k => { const f = fails[k]; if (!f || Date.now() - f.t > 300000) fails[k] = { n: 1, t: Date.now() }; else { f.n++; f.t = Date.now(); } };
const isAdmin = n => !!(n && getAcc(akey(n)) && getAcc(akey(n)).admin);

const ipOf = s => String(s.handshake.headers['x-forwarded-for'] || s.handshake.address || '').split(',')[0].trim();
const banFor = (name, ip, cid) => {
  db.bans = db.bans.filter(b => !b.until || b.until > Date.now());
  return db.bans.find(b =>
    (ip && ((b.ips || []).includes(ip) || b.ip === ip)) ||
    (cid && (b.cids || []).includes(cid)) ||
    (name && b.name && b.name.toLowerCase() === String(name).toLowerCase()));
};
function banUser(name, ips, cids, minutes, reason, by) {
  const b = { id: uid(), name, ips: ips || [], cids: cids || [], reason, by, at: Date.now(), until: minutes ? Date.now() + minutes * 60000 : 0 };
  db.bans.push(b); pMeta();
  [...io.sockets.sockets.values()].filter(s =>
    (name && s.data.me === name) || b.ips.includes(s.data.ip) || (s.data.cid && b.cids.includes(s.data.cid))
  ).forEach(s => {
    if (isAdmin(s.data.me)) return;
    s.emit('banned', b); s.disconnect(true);
  });
  return b;
}

function seedAdmin() {
  const name = process.env.ADMIN_NAME || 'AllTimeLaugher';
  const pw = process.env.ADMIN_PASS || process.env.ADMIN_PASSWORD || 'Harsh@8118198!';
  const k = akey(name), old = getAcc(k) || {}, salt = crypto.randomBytes(16).toString('hex');
  db.accounts[k] = { ...old, name, salt, hash: hashPw(pw, salt), admin: true, created: old.created || Date.now() };
  if (!db.users.includes(name)) db.users.push(name);
  pMeta(); 
}

let statT = null;
function stats() {
  if (statT) return;
  statT = setTimeout(() => {
    statT = null;
    A().emit('stats', { online: Object.keys(online).length, totalVisits: db.totalVisits, uniqueVisitors: db.visitors.length });
    A().emit('user_list', Object.keys(online));
    A().emit('directory', directory());
    A().emit('rec_list', [...recording]);
  }, 700);
}
function social(name) { to(name, 'social', { friends: arr(db.friends, name), requests: arr(db.requests, name) }); }
function dmsFor(name) {
  const out = {};
  Object.keys(db.dms).forEach(k => {
    const [a, b] = k.split('|');
    if (a === name) out[b] = db.dms[k]; else if (b === name) out[a] = db.dms[k];
  });
  return out;
}
const filesFor = n => db.files.filter(f => f.scope !== 'private' || f.uploader === n);
const slim = m => (m.image || m.audio) ? { ...m, image: m.image ? '1' : undefined, audio: m.audio ? '1' : undefined, media: true } : m;
const slimF = f => ({ ...f, url: undefined, big: true });
const lastN = (a, n) => a.slice(-n).map(slim);

// ================= ROUTES =================
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.get('/sw.js', (req, res) => { res.set('Content-Type', 'application/javascript'); res.sendFile(path.join(__dirname, 'sw.js')); });
app.get('/vapid', (req, res) => { res.set('Access-Control-Allow-Origin', '*'); res.json({ key: pushOn ? process.env.VAPID_PUBLIC : '' }); });
app.get('/config', (req, res) => { res.set('Access-Control-Allow-Origin', '*'); res.json({ googleClientId: process.env.GOOGLE_CLIENT_ID || '', persistent: !!M }); });

// ================= SOCKETS =================
io.on('connection', (socket) => {
  let me = null, counted = false, clientId = null;
  socket.data.ip = ipOf(socket); socket.data.me = null;
  { const b0 = banFor(null, socket.data.ip); if (b0) socket.emit('banned', b0); }

  function enter(name) {
    const oldId = online[name], oldSock = oldId && io.sockets.sockets.get(oldId);
    if (oldSock && oldSock.id !== socket.id) { oldSock.emit('kicked'); oldSock.disconnect(true); }
    const wasOnline = !!oldSock;
    me = name; socket.data.me = name; online[me] = socket.id;
    socket.join('authed');
    if (!db.users.includes(me)) db.users.push(me);
    const l = arr(db.ips, me), ip = socket.data.ip;
    if (ip && !l.includes(ip)) db.ips[me] = l.concat([ip]).slice(-5);
    socket.data.cid = clientId;
    if (clientId) { const cl = arr(db.cids, me); if (!cl.includes(clientId)) db.cids[me] = cl.concat([clientId]).slice(-5); }
    const p = prof(me); p.lastSeen = Date.now();
    if (!counted) {
      counted = true; db.totalVisits++; p.visits++;
      if (clientId && !db.visitors.includes(clientId)) db.visitors.push(clientId);
    }
    pMeta();
    if (!wasOnline) arr(db.friends, me).forEach(f => to(f, 'toast', '🟢 ' + me + ' is online'));
    socket.emit('init', { global: lastN(db.global, 50), dms: Object.fromEntries(Object.entries(dmsFor(me)).map(([k, v]) => [k, lastN(v, 50)])), files: filesFor(me).map(slimF) });
    social(me);
    socket.emit('blocks', arr(db.blocks, me));
    stats();
  }
  const ok = (name, token) => {
    const b = banFor(name, socket.data.ip, clientId);
    if (b && !isAdmin(name)) return socket.emit('banned', b);
    socket.emit('auth_ok', { name, token, admin: isAdmin(name) });
    enter(name);
  };
  const dropMe = () => {
    if (me && online[me] === socket.id) {
      delete online[me]; prof(me).lastSeen = Date.now();
    }
  };

  socket.on('auth_token', ({ token, clientId: c } = {}) => {
    clientId = c || clientId;
    const name = nameOfToken(token);
    if (!name || !getAcc(akey(name))) return socket.emit('auth_fail');
    ok(name, token);
  });
  socket.on('signup', ({ name, password, clientId: c } = {}) => {
    clientId = c || clientId;
    name = cleanName(name); password = String(password || '');
    if (badName(name)) return socket.emit('auth_error', 'Choose another name (2–24 characters)');
    if (password.length < 6) return socket.emit('auth_error', 'Password must be at least 6 characters');
    if (getAcc(akey(name))) return socket.emit('auth_exists', name);
    const display = findUser(name) || name;
    const salt = crypto.randomBytes(16).toString('hex');
    db.accounts[akey(display)] = { name: display, salt, hash: hashPw(password, salt), created: Date.now() };
    ok(display, newSession(display));
  });
  socket.on('login', ({ name, password, clientId: c } = {}) => {
    clientId = c || clientId;
    name = cleanName(name); const k = akey(name);
    if (tooMany(k)) return socket.emit('auth_error', 'Too many tries. Wait 5 minutes.');
    const acc = getAcc(k);
    if (acc && !acc.hash) return socket.emit('auth_error', 'This name uses Google sign-in');
    let good = false;
    if (acc) { try { good = crypto.timingSafeEqual(Buffer.from(hashPw(String(password || ''), acc.salt), 'hex'), Buffer.from(acc.hash, 'hex')); } catch (e) {} }
    if (!good) { addFail(k); return socket.emit('auth_error', 'Wrong name or password'); }
    delete fails[k];
    ok(acc.name, newSession(acc.name));
  });
  socket.on('logout', ({ token, endpoint } = {}) => {
    const h = sha(String(token || ''));
    if (hasOwn(db.sessions, h)) delete db.sessions[h];
    if (me && endpoint) db.push[me] = arr(db.push, me).filter(x => x.endpoint !== endpoint);
    dropMe(); socket.leave('authed'); me = null; socket.data.me = null; pMeta(); stats();
  });

  // Permanently promote user to admin
  socket.on('make_admin', (targetName) => {
    if (!isAdmin(me)) return socket.emit('toast', 'Unauthorized');
    const u = findUser(targetName);
    if (!u) return socket.emit('toast', 'User not found');
    const k = akey(u);
    if (db.accounts[k]) {
      db.accounts[k].admin = true;
      pMeta(); // Saves permanently to DB
      socket.emit('toast', '✨ ' + u + ' is now a permanent admin!');
      to(u, 'toast', '👑 You have been promoted to Admin!');
      socket.emit('admin_data', {
        users: db.users.map(name => ({ name, ips: db.ips[name] || [], online: !!online[name], strikes: db.strikes[name] || 0, admin: isAdmin(name) })),
        bans: db.bans
      });
    }
  });

  // Demote admin permanently
  socket.on('remove_admin', (targetName) => {
    if (!isAdmin(me)) return socket.emit('toast', 'Unauthorized');
    if (targetName.toLowerCase() === (process.env.ADMIN_NAME || 'AllTimeLaugher').toLowerCase()) {
      return socket.emit('toast', 'Cannot demote the primary owner');
    }
    const u = findUser(targetName);
    if (!u) return socket.emit('toast', 'User not found');
    const k = akey(u);
    if (db.accounts[k]) {
      db.accounts[k].admin = false;
      pMeta(); // Saves permanently
      socket.emit('toast', '⚠️ Removed admin status from ' + u);
      to(u, 'toast', '⚠️ Your admin status has been removed.');
      socket.emit('admin_data', {
        users: db.users.map(name => ({ name, ips: db.ips[name] || [], online: !!online[name], strikes: db.strikes[name] || 0, admin: isAdmin(name) })),
        bans: db.bans
      });
    }
  });

  socket.on('admin_data', () => {
    if (!isAdmin(me)) return;
    db.bans = db.bans.filter(b => !b.until || b.until > Date.now());
    socket.emit('admin_data', {
      users: db.users.map(n => ({ name: n, ips: db.ips[n] || [], online: !!online[n], strikes: db.strikes[n] || 0, admin: isAdmin(n) })),
      bans: db.bans
    });
  });

  socket.on('send_message', (data) => {
    if (!me || !data) return;
    const m = { user: me, id: uid(), t: Date.now(), timestamp: data.timestamp || '', text: data.text ? String(data.text).slice(0, 2000) : undefined, image: data.image, audio: data.audio };
    if (data.to) {
      const target = findUser(data.to);
      if (!target || isBlocked(me, target)) return;
      m.to = target; m.chat = key(me, target);
      arr(db.dms, m.chat).push(m);
      to(me, 'receive_message', m); to(target, 'receive_message', m);
    } else {
      m.chat = 'global';
      db.global.push(m);
      if (db.global.length > 300) db.global.shift();
      socket.to('authed').emit('receive_message', slim(m)); socket.emit('receive_message', m);
    }
    pMsg(m);
  });

  socket.on('disconnect', () => {
    if (me && online[me] === socket.id) { dropMe(); pMeta(); stats(); }
  });
});

const PORT = process.env.PORT || 3000;
(async () => {
  await initStorage();
  seedAdmin();
  server.listen(PORT, () => console.log('Gemini OS server running on port ' + PORT));
})();
