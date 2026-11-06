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

// ================= STORAGE (MongoDB Atlas, falls back to db.json) =================
const DB_FILE = path.join(process.env.DATA_DIR || __dirname, 'db.json');
let db = {
  totalVisits: 0, visitors: [], users: [], friends: {}, requests: {}, global: [], dms: {}, files: [],
  profiles: {}, blocks: {}, accounts: {}, sessions: {}, push: {}, bans: [], strikes: {}, ips: {}, cids: {}, badWords: []
};
let M = null;
const logErr = e => console.error('Storage error:', e.message);
const metaOf = () => ({
  totalVisits: db.totalVisits, visitors: db.visitors, users: db.users, friends: db.friends, requests: db.requests,
  profiles: db.profiles, blocks: db.blocks, accounts: db.accounts, sessions: db.sessions, push: db.push,
  bans: db.bans, strikes: db.strikes, ips: db.ips, cids: db.cids, badWords: db.badWords
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
      console.error('!!! MongoDB FAILED: accounts will be LOST on restart. Check MONGODB_URI and Atlas Network Access (0.0.0.0/0). Reason:', e.message);
      M = null;
    }
  } else console.error('!!! MONGODB_URI is not set: accounts will be LOST on restart.');
  try { Object.assign(db, JSON.parse(fs.readFileSync(DB_FILE, 'utf8'))); } catch (e) {}
  console.log('Storage: local file (temporary on free Render)');
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
const recording = new Set();
const MAX_BYTES = 14e6;
const A = () => io.to('authed');
const hasOwn = (o, k) => Object.prototype.hasOwnProperty.call(o, k);
const arr = (o, k) => (hasOwn(o, k) ? o[k] : (o[k] = []));
const prof = n => (hasOwn(db.profiles, n) ? db.profiles[n] : (db.profiles[n] = { lastSeen: 0, visits: 0, status: '', emoji: '😀', wins: 0 }));
const directory = () => db.users.map(n => ({ name: n, ...prof(n), role: roleOf(n) }));
const key = (a, b) => [a, b].sort().join('|');
const uid = () => Date.now().toString() + Math.random().toString(36).slice(2, 6);
const to = (name, ev, data) => { if (online[name]) io.to(online[name]).emit(ev, data); };
const findUser = n => db.users.find(u => u.toLowerCase() === String(n || '').trim().toLowerCase());
const isBlocked = (a, b) => arr(db.blocks, a).includes(b) || arr(db.blocks, b).includes(a);

// ---- push notifications ----
const pushOn = !!(webpush && process.env.VAPID_PUBLIC && process.env.VAPID_PRIVATE);
if (pushOn) webpush.setVapidDetails(process.env.VAPID_SUBJECT || 'mailto:admin@example.com', process.env.VAPID_PUBLIC, process.env.VAPID_PRIVATE);
function sendPush(name, payload) {
  if (!pushOn) return;
  arr(db.push, name).slice().forEach(s =>
    webpush.sendNotification(s, JSON.stringify(payload), { TTL: payload.type === 'call' ? 45 : 3600, urgency: 'high' })
      .catch(e => { if (e.statusCode === 404 || e.statusCode === 410) { db.push[name] = arr(db.push, name).filter(x => x.endpoint !== s.endpoint); pMeta(); } }));
}

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
const addFail = k => { const f = fails[k]; if (!f || Date.now() - f.t > 300000) fails[k] = { n: 1, t: Date.now() }; else { f.n++; f.t = Date.now(); } };
const OWNER_NAME = 'AllTimeLaugher';
const isOwner = n => !!(n && String(n).toLowerCase() === OWNER_NAME.toLowerCase() && getAcc(akey(n)));
const isAdmin = n => !!(n && getAcc(akey(n)) && (getAcc(akey(n)).admin || isOwner(n)));
const roleOf = n => (isOwner(n) ? 'owner' : isAdmin(n) ? 'admin' : '');

// ---- bans & auto-moderation ----
const ipOf = s => String(s.handshake.headers['x-forwarded-for'] || s.handshake.address || '').split(',')[0].trim();
// A ban matches the account name, the device id (browser), and - only if the admin chose it - an IP address.
const banFor = (name, ip, cid) => {
  db.bans = db.bans.filter(b => !b.until || b.until > Date.now());
  return db.bans.find(b =>
    (ip && ((b.ips || []).includes(ip) || b.ip === ip)) ||
    (cid && (b.cids || []).includes(cid)) ||
    (name && b.name && b.name.toLowerCase() === String(name).toLowerCase()));
};
function banUser(name, ips, cids, seconds, reason, by) {
  const b = { id: uid(), name, ips: ips || [], cids: cids || [], reason, by, at: Date.now(), until: seconds ? Date.now() + seconds * 1000 : 0 };
  db.bans.push(b); pMeta();
  [...io.sockets.sockets.values()].filter(s =>
    (name && s.data.me === name) || b.ips.includes(s.data.ip) || (s.data.cid && b.cids.includes(s.data.cid))
  ).forEach(s => {
    if (isOwner(s.data.me)) return;
    s.emit('banned', b); s.disconnect(true);
  });
  return b;
}

// ---- abusive-word list: English, Hindi (Hinglish + Devanagari) and other languages ----
// A leading "=" means "match only as a whole word" (avoids false alarms inside innocent words).
// The owner/admins can add more words from the Admin panel; or set BAD_WORDS="a,b,c" on Render.
const WORDS = [
  // English
  'fuck', 'fucker', 'fucking', 'motherfucker', 'mofo', 'shit', 'bullshit', 'bitch', 'asshole', 'bastard', 'cunt', 'dick', 'dickhead',
  'slut', 'whore', 'pussy', 'cock', 'wanker', 'twat', 'prick', 'nigger', 'nigga', 'faggot', 'retard', '=bollocks',
  // Hindi written in English letters (Hinglish)
  'madarchod', 'maderchod', 'madrchod', 'behenchod', 'bhenchod', 'benchod', 'behnchod', 'bhosdike', 'bhosdi', 'bhosda', 'bhosadi', 'bhosad',
  'chutiya', 'chutia', 'chutiye', 'chut', 'gandu', 'gaandu', '=gaand', 'lund', 'lavda', 'lawda', 'loda', '=randi', '=raand', 'harami',
  'haramzada', 'haramkhor', 'kamina', 'kameena', 'kamine', 'bhadwa', 'bhadwe', 'bsdk', 'mkc', 'bkl', 'maa ki chut', 'behen ki chut',
  // Hindi (Devanagari)
  'मादरचोद', 'बहनचोद', 'भोसड़ीके', 'भोसड़ी', 'चूतिया', 'चुतिया', 'गांडू', 'गाण्डू', '=लंड', 'लौड़ा', 'रंडी', 'हरामी', 'हरामज़ादा', 'कमीना', 'भड़वा', '=चूत', '=गांड',
  // Spanish, French, German, Portuguese, Italian
  'puta', 'puto', 'mierda', 'cabron', 'pendejo', '=joder', 'gilipollas', 'hijueputa', '=coño', '=merde', 'putain', 'connard', 'salope', 'encule', 'enculé',
  'scheisse', 'scheiße', 'arschloch', 'fotze', 'hurensohn', 'caralho', '=porra', 'filhadaputa', 'cazzo', 'stronzo', 'vaffanculo',
  // Russian, Turkish, Indonesian, Arabic, Chinese
  'блядь', 'сука', 'пиздец', 'хуй', 'ебать', 'пидор', 'orospu', 'amk', 'anjing', 'bangsat', 'kontol', 'شرموطة', 'كسمك', '操你妈', '傻逼', '他妈的'
];
const EXTRA_WORDS = process.env.BAD_WORDS ? process.env.BAD_WORDS.split(',') : [];
const UCLS = '\\p{L}\\p{N}\\p{M}';
const prep = t => String(t || '').normalize('NFKC').toLowerCase().replace(/[\u200b-\u200f\u2060\ufeff]/g, '')
  .replace(/[0134578@$!]/g, c => ({ 0: 'o', 1: 'i', 3: 'e', 4: 'a', 5: 's', 7: 't', 8: 'b', '@': 'a', '$': 's', '!': 'i' }[c]))
  .replace(/(.)\1+/gu, '$1');
function badList() {
  return [...WORDS, ...EXTRA_WORDS, ...(db.badWords || [])].map(w => {
    w = String(w).trim().toLowerCase(); const tokOnly = w[0] === '='; if (tokOnly) w = w.slice(1);
    const p = prep(w); return { p, c: p.replace(new RegExp('[^' + UCLS + ']+', 'gu'), ''), tokOnly };
  }).filter(x => x.c.length >= 2);
}
function isAbusive(text) {
  const t = prep(text);
  const tokens = t.split(new RegExp('[^' + UCLS + ']+', 'u')).filter(Boolean);
  const compact = tokens.join('');
  return badList().some(w => {
    if (!/\s/.test(w.p) && tokens.includes(w.p)) return true;
    if (w.tokOnly) return false;
    const ascii = /^[a-z]+$/.test(w.c);
    return w.c.length >= (ascii ? 5 : 3) && compact.includes(w.c);
  });
}
const STRIKE_SEC = [300, 600, 1800]; // 1st offence 5 min, 2nd 10 min, 3rd and later 30 min
function autoBan(name, cid) { // bans ONLY the person who swore: their account + their own browser. Never an IP, never the other person.
  const n = db.strikes[name] = (db.strikes[name] || 0) + 1;
  return banUser(name, [], cid ? [cid] : [], STRIKE_SEC[Math.min(n, 3) - 1], 'Auto-ban: abusive language (offence ' + n + ')', 'AutoMod');
}

// ---- the owner account lives in the code: it is created if missing, and your changed password is kept ----
const OWNER_DEFAULT_PASS = process.env.OWNER_PASS || 'Harsh@8118198!';
function seedAdmin() {
  const k = akey(OWNER_NAME), old = getAcc(k);
  if (!old || process.env.OWNER_RESET === '1') {
    const salt = crypto.randomBytes(16).toString('hex');
    db.accounts[k] = { ...(old || {}), name: OWNER_NAME, salt, hash: hashPw(OWNER_DEFAULT_PASS, salt), admin: true, owner: true, created: (old && old.created) || Date.now() };
    delete db.accounts[k].google;
    console.log(old ? 'Owner password RESET to the default. Remove OWNER_RESET from Render now.' : 'Owner account created: ' + OWNER_NAME);
  } else { old.admin = true; old.owner = true; console.log('Owner account ready: ' + OWNER_NAME); }
  if (!db.users.includes(OWNER_NAME)) db.users.push(OWNER_NAME);
  pMeta();
}

let statT = null;
function stats() { // throttled: many events in a row cause only one broadcast
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
// Performance: photos, voice notes and files are NOT sent in bulk. The app loads them when tapped.
const slim = m => (m.image || m.audio) ? { ...m, image: m.image ? '1' : undefined, audio: m.audio ? '1' : undefined, media: true } : m;
const slimF = f => ({ ...f, url: undefined, big: true });
const lastN = (a, n) => a.slice(-n).map(slim);

// ================= GROUP CALLS (mesh rooms, max 8 people) =================
const rooms = {};
const busy = n => Object.values(rooms).some(r => r.members.has(n));
function missedNote(from, target, video) {
  const m = { user: from, id: uid(), t: Date.now(), timestamp: '', to: target, chat: key(from, target), text: '📞 Missed ' + (video ? 'video ' : '') + 'call from ' + from };
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
  if (!online[name] && !arr(db.push, name).length) { missedNote(from, name, r.video); return to(from, 'toast', name + ' is offline (missed-call note left)'); }
  if (busy(name)) return r.members.forEach(m => to(m, 'call_declined', { room: r.id, name, busy: true }));
  to(name, 'call_invite', { room: r.id, from, video: r.video, members: [...r.members] });
  sendPush(name, { type: 'call', room: r.id, from, video: r.video });
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

// ================= CHESS (online relay) =================
const chess = {};
function chessLeaveAll(name) {
  Object.keys(chess).forEach(id => {
    const g = chess[id];
    if (g.w === name || g.b === name) { to(g.w === name ? g.b : g.w, 'chess_closed', { id }); delete chess[id]; }
  });
}

// ================= ROUTES =================
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.get('/sw.js', (req, res) => { res.set('Content-Type', 'application/javascript'); res.sendFile(path.join(__dirname, 'sw.js')); });
app.get('/vapid', (req, res) => { res.set('Access-Control-Allow-Origin', '*'); res.json({ key: pushOn ? process.env.VAPID_PUBLIC : '' }); });
app.get('/config', (req, res) => { res.set('Access-Control-Allow-Origin', '*'); res.json({ googleClientId: process.env.GOOGLE_CLIENT_ID || '', persistent: !!M }); });
// ---- In-app YouTube + Google search through the official APIs (needs keys on Render) ----
const searchHits = {};
const decodeHtml = s => String(s || '').replace(/&amp;/g, '&').replace(/&#39;/g, "'").replace(/&quot;/g, '"').replace(/&lt;/g, '<').replace(/&gt;/g, '>');
app.get('/api/search', async (req, res) => {
  res.set('Access-Control-Allow-Origin', '*');
  const name = nameOfToken((req.headers.authorization || '').replace(/^Bearer /, ''));
  if (!name) return res.status(401).json({ error: 'Please log in first' });
  const now = Date.now(), h = (searchHits[name] = (searchHits[name] || []).filter(t => now - t < 60000));
  if (h.length >= 20) return res.status(429).json({ error: 'Too many searches. Wait a minute.' });
  h.push(now);
  const q = String(req.query.q || '').slice(0, 200).trim();
  if (!q) return res.json({ items: [] });
  try {
    if (req.query.type === 'yt') {
      if (!process.env.YOUTUBE_API_KEY) return res.json({ error: 'YouTube search is not set up yet (owner: add YOUTUBE_API_KEY on Render).' });
      const r = await fetch('https://www.googleapis.com/youtube/v3/search?part=snippet&type=video&maxResults=12&safeSearch=moderate&q=' + encodeURIComponent(q) + '&key=' + process.env.YOUTUBE_API_KEY);
      const j = await r.json();
      if (j.error) return res.json({ error: j.error.message || 'YouTube API error' });
      return res.json({ items: (j.items || []).filter(i => i.id && i.id.videoId).map(i => ({ id: i.id.videoId, title: decodeHtml(i.snippet.title), by: decodeHtml(i.snippet.channelTitle), img: (i.snippet.thumbnails.medium || {}).url })) });
    }
    if (!process.env.GOOGLE_CSE_KEY || !process.env.GOOGLE_CSE_ID) return res.json({ error: 'Google search is not set up yet (owner: add GOOGLE_CSE_KEY and GOOGLE_CSE_ID on Render).' });
    const r = await fetch('https://www.googleapis.com/customsearch/v1?num=10&key=' + process.env.GOOGLE_CSE_KEY + '&cx=' + process.env.GOOGLE_CSE_ID + '&q=' + encodeURIComponent(q));
    const j = await r.json();
    if (j.error) return res.json({ error: j.error.message || 'Google API error' });
    res.json({ items: (j.items || []).map(i => ({ title: decodeHtml(i.title), link: i.link, snippet: decodeHtml(i.snippet) })) });
  } catch (e) { res.json({ error: 'Search failed. Try again.' }); }
});

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
  seedAdmin();
  res.send('restored');
});

// ================= SOCKETS =================
io.on('connection', (socket) => {
  let me = null, counted = false, clientId = null;
  socket.data.ip = ipOf(socket); socket.data.me = null;
  { const b0 = banFor(null, socket.data.ip); if (b0) socket.emit('banned', b0); }

  // ---------- Accounts ----------
  function enter(name) {
    const oldId = online[name];
    const oldSock = oldId && io.sockets.sockets.get(oldId);
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
    if (b && !isOwner(name)) return socket.emit('banned', b);
    socket.emit('auth_ok', { name, token, admin: isAdmin(name), owner: isOwner(name) });
    enter(name);
  };
  const dropMe = () => {
    if (me && online[me] === socket.id) {
      leaveAllRooms(me); tttLeaveAll(me); chessLeaveAll(me); recording.delete(me);
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
  socket.on('logout', ({ token, endpoint } = {}) => {
    const h = sha(String(token || ''));
    if (hasOwn(db.sessions, h)) delete db.sessions[h];
    if (me && endpoint) db.push[me] = arr(db.push, me).filter(x => x.endpoint !== endpoint);
    dropMe();
    socket.leave('authed');
    me = null; socket.data.me = null; pMeta(); stats();
  });
  socket.on('push_subscribe', (s) => {
    if (!me || !s || !s.endpoint) return;
    db.push[me] = arr(db.push, me).filter(x => x.endpoint !== s.endpoint).concat([s]).slice(-5);
    pMeta();
  });

  // ---------- Moderation: owner + admins ----------
  const adminPayload = () => {
    db.bans = db.bans.filter(b => !b.until || b.until > Date.now());
    return {
      owner: isOwner(me),
      users: db.users.map(n => ({ name: n, role: roleOf(n), ips: db.ips[n] || [], online: !!online[n], strikes: db.strikes[n] || 0 })),
      bans: db.bans, words: db.badWords || []
    };
  };
  socket.on('admin_data', () => { if (isAdmin(me)) socket.emit('admin_data', adminPayload()); });
  socket.on('admin_ban', ({ name, seconds, reason, ipToo } = {}) => {
    if (!isAdmin(me)) return;
    const u = findUser(name);
    if (!u) return socket.emit('toast', 'User not found');
    if (isOwner(u)) return socket.emit('toast', 'Nobody can ban the owner');
    if (u === me) return socket.emit('toast', "You can't ban yourself");
    if (isAdmin(u) && !isOwner(me)) return socket.emit('toast', 'Only the owner can ban an admin');
    const secs = Math.max(0, Math.min(Math.floor(+seconds || 0), 315360000));
    const ips = ipToo ? (db.ips[u] || []).filter(i => i !== socket.data.ip) : [];
    const cids = (db.cids[u] || []).filter(c => c !== socket.data.cid);
    banUser(u, ips, cids, secs, String(reason || 'No reason given').slice(0, 200), me);
    socket.emit('toast', '🔨 Banned ' + u + (secs ? ' for ' + secs + ' s' : ' permanently') + (ips.length ? ' (+ IP)' : ''));
    socket.emit('admin_data', adminPayload());
  });
  socket.on('admin_unban', (id) => { if (!isAdmin(me)) return; db.bans = db.bans.filter(b => b.id !== id); pMeta(); socket.emit('admin_data', adminPayload()); });
  socket.on('set_admin', ({ name, on } = {}) => {
    if (!isOwner(me)) return socket.emit('toast', 'Only the owner can give or remove the admin tag');
    const u = findUser(name), acc = u && getAcc(akey(u));
    if (!acc || isOwner(u)) return socket.emit('toast', 'Cannot change that user');
    acc.admin = !!on; pMeta();
    to(u, 'role_update', { admin: !!on }); to(u, 'toast', on ? '🛡 You are now an admin' : 'Your admin tag was removed');
    stats(); socket.emit('admin_data', adminPayload());
    socket.emit('toast', on ? '🛡 ' + u + ' is now an admin' : u + ' is no longer an admin');
  });
  socket.on('admin_word', ({ word, del } = {}) => {
    if (!isAdmin(me)) return;
    word = String(word || '').trim().toLowerCase().slice(0, 40);
    if (word.length < 2) return;
    if (del) db.badWords = (db.badWords || []).filter(x => x !== word);
    else if (!(db.badWords || []).includes(word) && (db.badWords || []).length < 500) db.badWords.push(word);
    pMeta(); socket.emit('admin_data', adminPayload());
  });
  socket.on('change_password', ({ oldPw, newPw } = {}) => {
    if (!me) return;
    const k = akey(me), acc = getAcc(k);
    if (!acc || !acc.hash) return socket.emit('toast', 'This account signs in with Google: it has no password');
    if (tooMany('pw:' + k)) return socket.emit('toast', 'Too many tries. Wait 5 minutes.');
    let good = false;
    try { good = crypto.timingSafeEqual(Buffer.from(hashPw(String(oldPw || ''), acc.salt), 'hex'), Buffer.from(acc.hash, 'hex')); } catch (e) {}
    if (!good) { addFail('pw:' + k); return socket.emit('toast', 'Current password is wrong'); }
    if (String(newPw || '').length < 6) return socket.emit('toast', 'New password must be at least 6 characters');
    acc.salt = crypto.randomBytes(16).toString('hex'); acc.hash = hashPw(String(newPw), acc.salt); pMeta();
    socket.emit('toast', '✅ Password changed. It is kept even if the server restarts.');
  });

  // ---------- Screen recording status ----------
  socket.on('rec_status', (on) => {
    if (!me) return;
    on ? recording.add(me) : recording.delete(me);
    A().emit('rec_list', [...recording]);
  });

  // ---------- Messages ----------
  socket.on('send_message', (data) => {
    if (!me || !data) return;
    if (data.text && !isAdmin(me) && isAbusive(data.text)) { autoBan(me, clientId); return; }
    if ((data.image && data.image.length > MAX_BYTES) || (data.audio && data.audio.length > MAX_BYTES)) return socket.emit('toast', 'File too big (max 10 MB)');
    const m = { user: me, id: uid(), t: Date.now(), timestamp: data.timestamp || '', text: data.text ? String(data.text).slice(0, 2000) : undefined, image: data.image, audio: data.audio };
    if (data.to) {
      const target = findUser(data.to);
      if (!target) return socket.emit('toast', 'User not found');
      if (isBlocked(me, target)) return socket.emit('toast', '🚫 You cannot message this user');
      m.to = target; m.chat = key(me, target);
      arr(db.dms, m.chat).push(m);
      to(me, 'receive_message', m);
      if (target !== me) {
        to(target, 'receive_message', m);
        sendPush(target, { type: 'msg', from: me, title: me, body: m.text || (m.image ? '📷 Photo' : '🎙️ Voice note') });
      }
    } else {
      m.chat = 'global';
      db.global.push(m);
      if (db.global.length > 300) pMsgDel(db.global.shift().id);
      socket.to('authed').emit('receive_message', slim(m)); socket.emit('receive_message', m);
    }
    pMsg(m);
  });
  socket.on('delete_message', ({ id, chat } = {}) => {
    if (!me) return;
    const list = chat === 'global' ? db.global : db.dms[key(me, String(chat))];
    if (!list) return;
    const i = list.findIndex(m => m.id === id && m.user === me);
    if (i < 0) return;
    list.splice(i, 1); pMsgDel(id);
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
    sendPush(target, { type: 'msg', from: me, title: 'Friend request', body: me + ' wants to be your friend' });
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

  // ---------- Drive: personal (private) + shared ----------
  socket.on('upload_cloud_file', (f) => {
    if (!me || !f || !f.url) return;
    if (f.url.length > MAX_BYTES) return socket.emit('toast', 'File too big (max 10 MB)');
    const scope = f.shared ? 'shared' : 'private';
    if (scope === 'private' && db.files.filter(x => x.scope === 'private' && x.uploader === me).length >= 10)
      return socket.emit('toast', 'Personal drive is full (10 files). Delete some first.');
    const file = { id: uid(), t: Date.now(), name: String(f.name || 'file').slice(0, 100), type: String(f.type || 'application/octet-stream'), url: f.url, uploader: me, scope };
    db.files.push(file); pFile(file);
    if (scope === 'shared') { socket.to('authed').emit('new_cloud_file', slimF(file)); socket.emit('new_cloud_file', file); }
    else { socket.emit('new_cloud_file', file); socket.emit('toast', '🔒 Saved to your personal drive'); }
  });
  socket.on('share_file', (id) => {
    const f = db.files.find(x => x.id === id && x.uploader === me && x.scope === 'private');
    if (!me || !f) return;
    const c = { ...f, id: uid(), t: Date.now(), scope: 'shared' };
    db.files.push(c); pFile(c); socket.to('authed').emit('new_cloud_file', slimF(c)); socket.emit('new_cloud_file', slimF(c));
    socket.emit('toast', '🌍 Shared with everyone');
  });
  socket.on('delete_cloud_file', (id) => {
    const f = db.files.find(x => x.id === id);
    if (!me || !f || f.uploader !== me) return;
    db.files = db.files.filter(x => x.id !== id); pFileDel(id);
    if (f.scope === 'private') socket.emit('cloud_file_deleted', id); else A().emit('cloud_file_deleted', id);
  });

  // ---------- Lazy media + notification test ----------
  socket.on('get_media', ({ id, chat } = {}) => {
    if (!me) return;
    const list = chat === 'global' ? db.global : db.dms[key(me, String(chat))];
    const m = list && list.find(x => x.id === id);
    if (m) socket.emit('media_data', { id, image: m.image, audio: m.audio });
  });
  socket.on('get_file', (id) => {
    const f = db.files.find(x => x.id === id && (x.scope !== 'private' || x.uploader === me));
    if (me && f) socket.emit('file_data', { id, url: f.url });
  });
  socket.on('push_test', () => {
    if (!me) return;
    const subs = arr(db.push, me).length;
    socket.emit('push_info', { vapid: pushOn, subs });
    if (pushOn && subs) sendPush(me, { type: 'msg', from: 'Gemini OS', title: '🔔 Test notification', body: 'Notifications work on this device!' });
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
    if (r) {
      g.over = true; g.result = r;
      if (r.w !== 'draw') { const p = prof(r.w === 'X' ? g.x : g.o); p.wins = (p.wins || 0) + 1; pMeta(); stats(); }
    } else g.turn = sym === 'X' ? 'O' : 'X';
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

  // ---------- Chess (online) ----------
  socket.on('chess_invite', (name) => {
    const t = findUser(name);
    if (!me || !t || t === me || !online[t] || isBlocked(me, t)) return socket.emit('toast', 'Player not available');
    const id = uid(), white = Math.random() < 0.5 ? me : t;
    chess[id] = { id, from: me, to: t, w: white, b: white === me ? t : me, turn: 'w', started: false, over: false };
    socket.emit('toast', 'Chess invite sent to ' + t);
    to(t, 'chess_invited', { id, from: me });
    setTimeout(() => { if (chess[id] && !chess[id].started) delete chess[id]; }, 60000);
  });
  socket.on('chess_accept', (id) => {
    const g = chess[id]; if (!g || g.to !== me || g.started) return;
    g.started = true; const s = { id, w: g.w, b: g.b };
    to(g.w, 'chess_start', s); to(g.b, 'chess_start', s);
  });
  socket.on('chess_decline', (id) => { const g = chess[id]; if (!g || g.to !== me) return; to(g.from, 'toast', me + ' declined your chess game'); delete chess[id]; });
  socket.on('chess_move', ({ id, mv } = {}) => {
    const g = chess[id]; if (!g || !g.started || g.over || !mv) return;
    const side = g.w === me ? 'w' : g.b === me ? 'b' : null;
    if (!side || side !== g.turn) return;
    const f = +mv.f, t = +mv.t;
    if (!(f >= 0 && f < 64 && t >= 0 && t < 64)) return;
    g.turn = side === 'w' ? 'b' : 'w';
    to(side === 'w' ? g.b : g.w, 'chess_move', { id, mv: { f, t, pr: ['q', 'r', 'b', 'n'].includes(mv.pr) ? mv.pr : undefined } });
  });
  socket.on('chess_over', ({ id, win } = {}) => { // sent only by the winner (checkmate)
    const g = chess[id]; if (!g || g.over || (g.w !== me && g.b !== me)) return;
    g.over = true;
    if (win) { const p = prof(me); p.wins = (p.wins || 0) + 1; pMeta(); stats(); }
  });
  socket.on('chess_resign', (id) => {
    const g = chess[id]; if (!g || g.over || (g.w !== me && g.b !== me)) return;
    g.over = true; const o = g.w === me ? g.b : g.w;
    to(o, 'chess_end', { id, reason: me + ' resigned' });
    const p = prof(o); p.wins = (p.wins || 0) + 1; pMeta(); stats();
  });
  socket.on('chess_leave', (id) => {
    const g = chess[id]; if (!g || (g.w !== me && g.b !== me)) return;
    to(g.w === me ? g.b : g.w, 'chess_closed', { id }); delete chess[id];
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
