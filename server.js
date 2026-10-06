const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const fs = require('fs');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 5e7, cors: { origin: '*' } });

// ================= STORAGE (MongoDB Atlas, falls back to db.json) =================
const DB_FILE = path.join(process.env.DATA_DIR || __dirname, 'db.json');
let db = { totalVisits: 0, visitors: [], users: [], friends: {}, requests: {}, global: [], dms: {}, files: [] };
let M = null; // MongoDB collections when connected
const logErr = e => console.error('Storage error:', e.message);
const metaOf = () => ({ totalVisits: db.totalVisits, visitors: db.visitors, users: db.users, friends: db.friends, requests: db.requests });

async function initStorage() {
  if (process.env.MONGODB_URI) {
    try {
      const { MongoClient } = require('mongodb');
      const client = new MongoClient(process.env.MONGODB_URI);
      await client.connect();
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
    } catch (e) { console.error('MongoDB failed, using local file:', e.message); M = null; }
  }
  try { Object.assign(db, JSON.parse(fs.readFileSync(DB_FILE, 'utf8'))); } catch (e) {}
  console.log('Storage: local file (data is lost on Render redeploy unless you use MongoDB or a disk)');
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

async function fullSave() { // used after /restore
  if (!M) return flushMeta();
  await M.msgs.deleteMany({}); await M.files.deleteMany({});
  const all = [...db.global, ...Object.values(db.dms).flat()];
  if (all.length) await M.msgs.insertMany(all.map(m => ({ ...m, _id: m.id })));
  if (db.files.length) await M.files.insertMany(db.files.map(f => ({ ...f, _id: f.id })));
  await flushMeta();
}

process.on('SIGTERM', async () => { clearTimeout(metaTimer); await flushMeta(); process.exit(0); });

// ================= HELPERS =================
const online = {}; // username -> socket.id
const MAX_BYTES = 14e6; // ~10 MB file as base64
const clean = n => String(n || '').replace(/[|<>]/g, '').trim().slice(0, 24);
const findUser = n => db.users.find(u => u.toLowerCase() === String(n || '').trim().toLowerCase());
const key = (a, b) => [a, b].sort().join('|');
const uid = () => Date.now().toString() + Math.random().toString(36).slice(2, 6);
const arr = (o, k) => (o[k] = o[k] || []);
const to = (name, ev, data) => { if (online[name]) io.to(online[name]).emit(ev, data); };

function stats() {
  io.emit('stats', { online: Object.keys(online).length, totalVisits: db.totalVisits, uniqueVisitors: db.visitors.length });
  io.emit('user_list', Object.keys(online));
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

// ================= ROUTES =================
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.get('/backup', (req, res) => {
  if (!process.env.ADMIN_KEY || req.query.key !== process.env.ADMIN_KEY) return res.status(403).send('forbidden');
  res.setHeader('Content-Disposition', 'attachment; filename="db-backup.json"');
  res.json(db);
});
app.post('/restore', express.json({ limit: '200mb' }), async (req, res) => {
  if (!process.env.ADMIN_KEY || req.query.key !== process.env.ADMIN_KEY) return res.status(403).send('forbidden');
  if (!req.body || typeof req.body !== 'object') return res.status(400).send('bad data');
  db = { ...db, ...req.body };
  await fullSave();
  res.send('restored');
});

// ================= SOCKETS =================
io.on('connection', (socket) => {
  let me = null;
  let counted = false;

  socket.on('register_user', ({ name, clientId } = {}) => {
    name = clean(name);
    if (!name) return;
    if (online[name] && online[name] !== socket.id && io.sockets.sockets.get(online[name])) return socket.emit('name_taken', name);
    const wasOnline = !!online[name];
    if (me && me !== name && online[me] === socket.id) delete online[me];
    me = name;
    online[me] = socket.id;
    if (!db.users.includes(me)) { db.users.push(me); pMeta(); }
    if (!counted) {
      counted = true;
      db.totalVisits++;
      if (clientId && !db.visitors.includes(clientId)) db.visitors.push(clientId);
      pMeta();
    }
    if (!wasOnline) arr(db.friends, me).forEach(f => to(f, 'toast', '🟢 ' + me + ' is online'));
    socket.emit('init', { global: db.global, dms: dmsFor(me), files: db.files });
    social(me);
    stats();
  });

  // ---- Messages ----
  socket.on('send_message', (data) => {
    if (!me || !data) return;
    if ((data.image && data.image.length > MAX_BYTES) || (data.audio && data.audio.length > MAX_BYTES)) return socket.emit('toast', 'File too big (max 10 MB)');
    const m = { user: me, id: uid(), t: Date.now(), timestamp: data.timestamp || '', text: data.text ? String(data.text).slice(0, 2000) : undefined, image: data.image, audio: data.audio };
    if (data.to) {
      m.to = String(data.to); m.chat = key(me, m.to);
      arr(db.dms, m.chat).push(m);
      to(me, 'receive_message', m);
      if (m.to !== me) to(m.to, 'receive_message', m);
    } else {
      m.chat = 'global';
      db.global.push(m);
      if (db.global.length > 300) pMsgDel(db.global.shift().id);
      io.emit('receive_message', m);
    }
    pMsg(m);
  });

  socket.on('delete_message', ({ id, chat } = {}) => {
    if (!me) return;
    const list = chat === 'global' ? db.global : db.dms[key(me, chat)];
    if (!list) return;
    const i = list.findIndex(m => m.id === id && m.user === me);
    if (i < 0) return;
    list.splice(i, 1);
    pMsgDel(id);
    if (chat === 'global') io.emit('message_deleted', { id });
    else { to(me, 'message_deleted', { id }); to(chat, 'message_deleted', { id }); }
  });

  socket.on('typing_status', (d = {}) => {
    if (!me) return;
    if (d.to) to(d.to, 'user_typing', { user: me, to: d.to, isTyping: d.isTyping });
    else socket.broadcast.emit('user_typing', { user: me, isTyping: d.isTyping });
  });

  // ---- Friends ----
  function acceptFriend(a, b) {
    db.requests[a] = arr(db.requests, a).filter(x => x !== b);
    db.requests[b] = arr(db.requests, b).filter(x => x !== a);
    if (!arr(db.friends, a).includes(b)) db.friends[a].push(b);
    if (!arr(db.friends, b).includes(a)) db.friends[b].push(a);
    pMeta(); social(a); social(b);
    to(a, 'toast', '🤝 You are now friends with ' + b);
    to(b, 'toast', '🤝 ' + a + ' accepted your friend request');
  }
  socket.on('friend_request', (input) => {
    if (!me) return;
    const target = findUser(input);
    if (!target) return socket.emit('toast', 'User "' + String(input).trim() + '" not found');
    if (target === me) return socket.emit('toast', "You can't add yourself");
    if (arr(db.friends, me).includes(target)) return socket.emit('toast', 'Already friends with ' + target);
    if (arr(db.requests, me).includes(target)) return acceptFriend(me, target);
    if (arr(db.requests, target).includes(me)) return socket.emit('toast', 'Request already sent');
    db.requests[target].push(me);
    pMeta(); social(target);
    to(target, 'toast', '👋 ' + me + ' sent you a friend request');
    socket.emit('toast', 'Friend request sent to ' + target);
  });
  socket.on('friend_accept', (from) => { if (me && arr(db.requests, me).includes(from)) acceptFriend(me, from); });
  socket.on('friend_decline', (from) => { if (!me) return; db.requests[me] = arr(db.requests, me).filter(x => x !== from); pMeta(); social(me); });
  socket.on('friend_remove', (other) => {
    if (!me) return;
    db.friends[me] = arr(db.friends, me).filter(x => x !== other);
    db.friends[other] = arr(db.friends, other).filter(x => x !== me);
    pMeta(); social(me); social(other);
  });

  // ---- Cloud drive ----
  socket.on('upload_cloud_file', (f) => {
    if (!me || !f || !f.url) return;
    if (f.url.length > MAX_BYTES) return socket.emit('toast', 'File too big (max 10 MB)');
    const file = { id: uid(), t: Date.now(), name: String(f.name || 'file').slice(0, 100), type: String(f.type || 'application/octet-stream'), url: f.url, uploader: me };
    db.files.push(file); pFile(file);
    io.emit('new_cloud_file', file);
  });
  socket.on('delete_cloud_file', (id) => {
    const f = db.files.find(x => x.id === id);
    if (!me || !f || f.uploader !== me) return;
    db.files = db.files.filter(x => x.id !== id); pFileDel(id);
    io.emit('cloud_file_deleted', id);
  });

  // ---- Call signaling ----
  socket.on('call_user', (d = {}) => {
    if (!me) return;
    if (online[d.userToCall]) to(d.userToCall, 'incoming_call', { signalData: d.signalData, from: me, isVideo: d.isVideo });
    else socket.emit('call_failed', { reason: 'User "' + d.userToCall + '" is offline.' });
  });
  socket.on('answer_call', (d = {}) => to(d.to, 'call_accepted', { signal: d.signal, from: me }));
  socket.on('send_candidate', (d = {}) => to(d.to, 'receive_candidate', { candidate: d.candidate, from: me }));
  socket.on('end_call', (d) => { if (d && d.to) to(d.to, 'call_ended'); });

  socket.on('disconnect', () => { if (me && online[me] === socket.id) { delete online[me]; stats(); } });
});

const PORT = process.env.PORT || 3000;
(async () => {
  await initStorage();
  server.listen(PORT, () => console.log('Gemini OS server running on port ' + PORT));
})();
