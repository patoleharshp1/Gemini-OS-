const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
let OAuth2Client = null;
try { ({ OAuth2Client } = require('google-auth-library')); } catch (e) {}

const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 5e7, cors: { origin: '*' } });

// ================= STORAGE (MongoDB Atlas, falls back to db.json) =================
const DB_FILE = path.join(process.env.DATA_DIR || __dirname, 'db.json');
let db = {
  totalVisits: 0, visitors: [], users: [], friends: {}, requests: {},
  global: [], dms: {}, files: [], profiles: {}, blocks: {}, accounts: {}, sessions: {}
};
let M = null;
const logErr = e => console.error('Storage error:', e.message);
const metaOf = () => ({
  totalVisits: db.totalVisits, visitors: db.visitors, users: db.users, friends: db.friends,
  requests: db.requests, profiles: db.profiles, blocks: db.blocks, accounts: db.accounts, sessions: db.sessions
});

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
  console.log('Storage: local file (data is lost on a free Render redeploy unless you use MongoDB)');
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

async function fullSave() {
  if (!M) return flushMeta();
  await M.msgs.deleteMany({}); await M.files.deleteMany({});
  const all = [...db.global, ...Object.values(db.dms).flat()];
  if (all.length) await M.msgs.insertMany(all.map(m => ({ ...m, _id: m.id })));
  if (db.files.length) await M.files.insertMany(db.files.map(f => ({ ...f, _id: f.id })));
  await flushMeta();
}
process.on('SIGTERM', async () => { clearTimeout(metaTimer); await flushMeta(); process.exit(0); });

// ================= HELPERS =================
const online = {};
const MAX_BYTES = 14e6;
const A = () => io.to('authed');
const hasOwn = (o, k) => Object.prototype.hasOwnProperty.call(o, k);
const arr = (o, k) => (hasOwn(o, k) ? o[k] : (o[k] = []));
const prof = n => (hasOwn(db.profiles, n) ? db.profiles[n] : (db.profiles[n] = { lastSeen: 0, visits: 0, status: '', emoji: '😀' }));
const directory = () => db.users.map(n => ({ name: n, ...prof(n) }));
const key = (a, b) => [a, b].sort().join('|');
const uid = () => Date.now().toString() + Math.random().toString(36).slice(2, 6);
const to = (name, ev, data) => { if (online[name]) io.to(online[name]).emit(ev, data); };
const findUser = n => db.users.find(u => u.toLowerCase() === String(n || '').trim().toLowerCase());
const isBlocked = (a, b) => arr(db.blocks, a).includes(b) || arr(db.blocks, b).includes(a);

// ---- accounts ----
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
const addFail = k => {
  const f = fails[k];
  if (!f || Date.now() - f.t > 300000) fails[k] = { n: 1, t: Date.now() };
  else { f.n++; f.t = Date.now(); }
};

function stats() {
  A().emit('stats', { online: Object.keys(online).length, totalVisits: db.totalVisits, uniqueVisitors: db.visitors.length });
  A().emit('user_list', Object.keys(online));
  A().emit('directory', directory());
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

// ================= GROUP CALLS (mesh rooms, max 8 people) =================
const rooms = {}; // id -> { id, host, video, members:Set, pending:Map(name->timer) }
const busy = n => Object.values(rooms).some(r => r.members.has(n));

function missedNote(from, target, video) {
  const m = { user: from, id: uid(), t: Date.now(), timestamp: '', to: target, chat: key(from, target),
    text: '📞 Missed ' + (video ? 'video ' : '') + 'call from ' + from };
  arr(db.dms, m.chat).push(m); pMsg(m);
  to(from, 'receive_message', m); to(target, 'receive_message', m);
}
function checkRoom(r, reason) {
  if (r.members.size <= 1 && r.pending.size === 0) {
    r.members.forEach(n => to(n, 'call_ended', { room: r.id, reason: reason || 'Everyone else left the call' }));
    delete rooms[r.id];
  }
}
function invite(r, name, from) {
  if (r.members.has(name) || r.pending.has(name)) return;
  if (r.members.size + r.pending.size >= 8) return to(from, 'toast', 'Max 8 people in a call');
  if (!online[name]) { missedNote(from, name, r.video); return to(from, 'toast', name + ' is offline (missed-call note left)'); }
  if (busy(name)) return r.members.forEach(m => to(m, 'call_declined', { room: r.id, name, busy: true }));
  to(name, 'call_invite', { room: r.id, from, video: r.video, members: [...r.members] });
  r.pending.set(name, setTimeout(() => {
    r.pending.delete(name);
    to(name, 'call_cancelled', { room: r.id });
    missedNote(from, name, r.video);
    r.members.forEach(m => to(m, 'call_missed', { room: r.id, name }));
    checkRoom(r, 'No answer');
  }, 45000));
}
function leaveRoom(name, id) {
  const r = rooms[id]; if (!r) return;
  if (r.pending.has(name)) {
    clearTimeout(r.pending.get(name)); r.pending.delete(name);
    r.members.forEach(m => to(m, 'call_declined', { room: id, name }));
  }
  if (r.members.delete(name)) r.members.forEach(m => to(m, 'peer_left', { room: id, name }));
  if (r.members.size === 0) {
    r.pending.forEach((t, n) => { clearTimeout(t); to(n, 'call_cancelled', { room: id }); });
    delete rooms[id]; return;
  }
  checkRoom(r);
}
function leaveAllRooms(name) {
  Object.keys(rooms).forEach(id => { const r = rooms[id]; if (r && (r.members.has(name) || r.pending.has(name))) leaveRoom(name, id); });
}

// ================= TIC-TAC-TOE (online) =================
const ttt = {};
const TL = [[0,1,2],[3,4,5],[6,7,8],[0,3,6],[1,4,7],[2,5,8],[0,4,8],[2,4,6]];
const tttResult = b => {
  for (const l of TL) if (b[l[0]] && b[l[0]] === b[l[1]] && b[l[0]] === b[l[2]]) return { w: b[l[0]], line: l };
  return b.every(Boolean) ? { w: 'draw', line: [] } : null;
};
const emitG = g => {
  const s = { id: g.id, x: g.x, o: g.o, board: g.board, turn: g.turn, over: !!g.over, result: g.result || null };
  to(g.x, 'ttt_state', s); to(g.o, 'ttt_state', s);
};
function tttLeaveAll(name) {
  Object.keys(ttt).forEach(id => {
    const g = ttt[id];
    if (g.x === name || g.o === name) { to(g.x === name ? g.o : g.x, 'ttt_closed', { id }); delete ttt[id]; }
  });
}

// ================= ROUTES =================
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.get('/config', (req, res) => { res.set('Access-Control-Allow-Origin', '*'); res.json({ googleClientId: process.env.GOOGLE_CLIENT_ID || '' }); });
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
  let me = null, counted = false, clientId = null;

  // ---------- Accounts ----------
  function enter(name) {
    const oldId = online[name];
    const oldSock = oldId && io.sockets.sockets.get(oldId);
    if (oldSock && oldSock.id !== socket.id) { oldSock.emit('kicked'); oldSock.disconnect(true); }
    const wasOnline = !!oldSock;
    me = name; online[me] = socket.id;
    socket.join('authed');
    if (!db.users.includes(me)) db.users.push(me);
    const p = prof(me); p.lastSeen = Date.now();
    if (!counted) {
      counted = true; db.totalVisits++; p.visits++;
      if (clientId && !db.visitors.includes(clientId)) db.visitors.push(clientId);
    }
    pMeta();
    if (!wasOnline) arr(db.friends, me).forEach(f => to(f, 'toast', '🟢 ' + me + ' is online'));
    socket.emit('init', { global: db.global, dms: dmsFor(me), files: db.files });
    social(me);
    socket.emit('blocks', arr(db.blocks, me));
    stats();
  }
  const ok = (name, token) => { socket.emit('auth_ok', { name, token }); enter(name); };

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
    if (acc) {
      try { good = crypto.timingSafeEqual(Buffer.from(hashPw(String(password || ''), acc.salt), 'hex'), Buffer.from(acc.hash, 'hex')); } catch (e) {}
    }
    if (!good) { addFail(k); return socket.emit('auth_error', 'Wrong name or password'); }
    delete fails[k];
    ok(acc.name, newSession(acc.name));
  });

  socket.on('google_auth', async ({ credential, name, clientId: c } = {}) => {
    clientId = c || clientId;
    if (!gClient) return socket.emit('auth_error', 'Google sign-in is not set up');
    let p;
    try { p = (await gClient.verifyIdToken({ idToken: credential, audience: process.env.GOOGLE_CLIENT_ID })).getPayload(); }
    catch (e) { return socket.emit('auth_error', 'Google sign-in failed'); }
    const acc = Object.values(db.accounts).find(a => a.google === p.sub);
    if (acc) return ok(acc.name, newSession(acc.name));
    name = cleanName(name);
    if (!name) return socket.emit('google_need_name', { suggested: cleanName(p.name || '') });
    if (badName(name)) return socket.emit('google_need_name', { suggested: name, error: 'Choose another name (2–24 characters)' });
    if (getAcc(akey(name))) return socket.emit('google_need_name', { suggested: name, error: 'That name is already taken' });
    const display = findUser(name) || name;
    db.accounts[akey(display)] = { name: display, google: p.sub, email: p.email, created: Date.now() };
    ok(display, newSession(display));
  });

  socket.on('logout', ({ token } = {}) => {
    const h = sha(String(token || ''));
    if (hasOwn(db.sessions, h)) delete db.sessions[h];
    if (me && online[me] === socket.id) {
      leaveAllRooms(me); tttLeaveAll(me);
      delete online[me]; prof(me).lastSeen = Date.now();
    }
    socket.leave('authed');
    me = null; pMeta(); stats();
  });

  // ---------- Messages ----------
  socket.on('send_message', (data) => {
    if (!me || !data) return;
    if ((data.image && data.image.length > MAX_BYTES) || (data.audio && data.audio.length > MAX_BYTES)) return socket.emit('toast', 'File too big (max 10 MB)');
    const m = { user: me, id: uid(), t: Date.now(), timestamp: data.timestamp || '', text: data.text ? String(data.text).slice(0, 2000) : undefined, image: data.image, audio: data.audio };
    if (data.to) {
      const target = findUser(data.to);
      if (!target) return socket.emit('toast', 'User not found');
      if (isBlocked(me, target)) return socket.emit('toast', '🚫 You cannot message this user');
      m.to = target; m.chat = key(me, target);
      arr(db.dms, m.chat).push(m);
      to(me, 'receive_message', m);
      if (target !== me) to(target, 'receive_message', m);
    } else {
      m.chat = 'global';
      db.global.push(m);
      if (db.global.length > 300) pMsgDel(db.global.shift().id);
      A().emit('receive_message', m);
    }
    pMsg(m);
  });

  socket.on('delete_message', ({ id, chat } = {}) => {
    if (!me) return;
    const list = chat === 'global' ? db.global : db.dms[key(me, String(chat))];
    if (!list) return;
    const i = list.findIndex(m => m.id === id && m.user === me);
    if (i < 0) return;
    list.splice(i, 1);
    pMsgDel(id);
    if (chat === 'global') A().emit('message_deleted', { id });
    else { to(me, 'message_deleted', { id }); to(chat, 'message_deleted', { id }); }
  });

  socket.on('typing_status', (d = {}) => {
    if (!me) return;
    if (d.to) to(d.to, 'user_typing', { user: me, to: d.to, isTyping: d.isTyping });
    else socket.to('authed').emit('user_typing', { user: me, isTyping: d.isTyping });
  });

  // ---------- Profile & blocking ----------
  socket.on('set_profile', (d = {}) => {
    if (!me) return;
    const p = prof(me);
    p.status = String(d.status || '').replace(/[<>]/g, '').slice(0, 60);
    p.emoji = Array.from(String(d.emoji || '😀')).slice(0, 2).join('');
    pMeta(); stats();
  });
  socket.on('block_user', (u) => {
    u = findUser(u);
    if (!me || !u || u === me) return;
    if (!arr(db.blocks, me).includes(u)) db.blocks[me].push(u);
    db.friends[me] = arr(db.friends, me).filter(x => x !== u);
    db.friends[u] = arr(db.friends, u).filter(x => x !== me);
    db.requests[me] = arr(db.requests, me).filter(x => x !== u);
    db.requests[u] = arr(db.requests, u).filter(x => x !== me);
    pMeta(); socket.emit('blocks', db.blocks[me]); social(me); social(u);
  });
  socket.on('unblock_user', (u) => {
    if (!me) return;
    db.blocks[me] = arr(db.blocks, me).filter(x => x !== u);
    pMeta(); socket.emit('blocks', db.blocks[me]);
  });

  // ---------- Friends ----------
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
    if (isBlocked(me, target)) return socket.emit('toast', '🚫 Not possible');
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

  // ---------- Cloud drive ----------
  socket.on('upload_cloud_file', (f) => {
    if (!me || !f || !f.url) return;
    if (f.url.length > MAX_BYTES) return socket.emit('toast', 'File too big (max 10 MB)');
    const file = { id: uid(), t: Date.now(), name: String(f.name || 'file').slice(0, 100), type: String(f.type || 'application/octet-stream'), url: f.url, uploader: me };
    db.files.push(file); pFile(file);
    A().emit('new_cloud_file', file);
  });
  socket.on('delete_cloud_file', (id) => {
    const f = db.files.find(x => x.id === id);
    if (!me || !f || f.uploader !== me) return;
    db.files = db.files.filter(x => x.id !== id); pFileDel(id);
    A().emit('cloud_file_deleted', id);
  });

  // ---------- Group calls ----------
  socket.on('call_start', ({ invitees, isVideo } = {}) => {
    if (!me) return;
    if (busy(me)) return socket.emit('toast', 'You are already in a call');
    const names = [...new Set((Array.isArray(invitees) ? invitees : []).map(findUser).filter(n => n && n !== me && !isBlocked(me, n)))].slice(0, 7);
    if (!names.length) return socket.emit('call_failed', { reason: 'Pick someone to call' });
    const id = uid();
    const r = rooms[id] = { id, host: me, video: !!isVideo, members: new Set([me]), pending: new Map() };
    socket.emit('call_room', { room: id });
    names.forEach(n => invite(r, n, me));
    checkRoom(r, 'Nobody could be reached. A missed-call note was left.');
  });
  socket.on('call_join', ({ room } = {}) => {
    const r = rooms[room];
    if (!me) return;
    if (!r || !r.pending.has(me)) return socket.emit('call_ended', { room, reason: 'The call has ended' });
    clearTimeout(r.pending.get(me)); r.pending.delete(me);
    const others = [...r.members];
    r.members.add(me);
    socket.emit('call_joined', { room, peers: others });
    others.forEach(n => to(n, 'peer_joined', { room, name: me }));
  });
  socket.on('call_add', ({ room, names } = {}) => {
    const r = rooms[room];
    if (!me || !r || !r.members.has(me)) return;
    (Array.isArray(names) ? names : []).map(findUser)
      .filter(n => n && ![...r.members].some(m => isBlocked(m, n)))
      .forEach(n => invite(r, n, me));
  });
  socket.on('call_leave', ({ room } = {}) => { if (me) leaveRoom(me, room); });
  socket.on('rtc_signal', ({ room, to: target, data } = {}) => {
    const r = rooms[room];
    if (!me || !r || !r.members.has(me) || !r.members.has(target)) return;
    to(target, 'rtc_signal', { room, from: me, data });
  });

  // ---------- Tic-Tac-Toe (online) ----------
  socket.on('ttt_invite', (name) => {
    const t = findUser(name);
    if (!me || !t || t === me || !online[t] || isBlocked(me, t)) return socket.emit('toast', 'Player not available');
    const id = uid();
    ttt[id] = { id, x: me, o: t, board: Array(9).fill(''), turn: 'X', started: false };
    socket.emit('toast', 'Invite sent to ' + t);
    to(t, 'ttt_invited', { id, from: me });
    setTimeout(() => { if (ttt[id] && !ttt[id].started) delete ttt[id]; }, 60000);
  });
  socket.on('ttt_accept', (id) => { const g = ttt[id]; if (!g || g.o !== me) return; g.started = true; emitG(g); });
  socket.on('ttt_decline', (id) => { const g = ttt[id]; if (!g || g.o !== me) return; to(g.x, 'toast', me + ' declined your game'); delete ttt[id]; });
  socket.on('ttt_move', ({ id, i } = {}) => {
    const g = ttt[id];
    if (!g || !g.started || g.over) return;
    const sym = g.x === me ? 'X' : g.o === me ? 'O' : null;
    if (!sym || sym !== g.turn || !(i >= 0 && i < 9) || g.board[i]) return;
    g.board[i] = sym;
    const r = tttResult(g.board);
    if (r) { g.over = true; g.result = r; } else g.turn = sym === 'X' ? 'O' : 'X';
    emitG(g);
  });
  socket.on('ttt_again', (id) => {
    const g = ttt[id];
    if (!g || !g.over || (g.x !== me && g.o !== me)) return;
    [g.x, g.o] = [g.o, g.x];
    g.board = Array(9).fill(''); g.turn = 'X'; g.over = false; g.result = null;
    emitG(g);
  });
  socket.on('ttt_leave', (id) => {
    const g = ttt[id];
    if (!g || (g.x !== me && g.o !== me)) return;
    const other = g.x === me ? g.o : g.x;
    to(other, 'toast', me + ' left the game'); to(other, 'ttt_closed', { id });
    delete ttt[id];
  });

  socket.on('disconnect', () => {
    if (me && online[me] === socket.id) {
      leaveAllRooms(me); tttLeaveAll(me);
      delete online[me]; prof(me).lastSeen = Date.now(); pMeta(); stats();
    }
  });
});

const PORT = process.env.PORT || 3000;
(async () => {
  await initStorage();
  server.listen(PORT, () => console.log('Gemini OS server running on port ' + PORT));
})();
