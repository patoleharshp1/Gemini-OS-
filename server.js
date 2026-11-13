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
  profiles: {}, blocks: {}, accounts: {}, sessions: {}, push: {}, bans: [], strikes: {}, ips: {}, cids: {}, badWords: [], reports: [], groups: {}
};
let M = null;
const logErr = e => console.error('Storage error:', e.message);
const metaOf = () => ({
  totalVisits: db.totalVisits, visitors: db.visitors, users: db.users, friends: db.friends, requests: db.requests,
  profiles: db.profiles, blocks: db.blocks, accounts: db.accounts, sessions: db.sessions, push: db.push,
  bans: db.bans, strikes: db.strikes, ips: db.ips, cids: db.cids, badWords: db.badWords, reports: db.reports, groups: db.groups
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
// ---- Doom rooms ----
const doomRooms = {}, doomOf = {};
function doomLeave(name) { const c = doomOf[name]; if (!c) return; delete doomOf[name]; const s = doomRooms[c]; if (!s) return; s.delete(name); if (!s.size) delete doomRooms[c]; else s.forEach(n => to(n, 'doom_players', [...s])); }
// ---- Arena multiplayer rooms (relay only; clients simulate) ----
const arenaRooms = {}, arenaOf = {};
function arenaLeave(name) { const c = arenaOf[name]; if (!c) return; delete arenaOf[name]; const set = arenaRooms[c]; if (!set) return; set.delete(name); if (!set.size) delete arenaRooms[c]; else set.forEach(n => to(n, 'arena_players', [...set])); }
const live = new Set(); // people streaming their screen LIVE (nothing is saved)
const liveViews = {}; // streamer -> Set of viewers
const MAX_VIEWERS = 6;
function stopLive(name) { if (live.delete(name)) { (liveViews[name] || new Set()).forEach(v => to(v, 'live_ended', { name })); delete liveViews[name]; A().emit('live_list', [...live]); } }
function stopWatching(v) { Object.keys(liveViews).forEach(st => { if (liveViews[st].delete(v)) to(st, 'live_viewer_left', { name: v }); }); }
const MAX_BYTES = 14e6;
const A = () => io.to('authed');
const hasOwn = (o, k) => Object.prototype.hasOwnProperty.call(o, k);
const arr = (o, k) => (hasOwn(o, k) ? o[k] : (o[k] = []));
const prof = n => {
  const p = hasOwn(db.profiles, n) ? db.profiles[n] : (db.profiles[n] = { lastSeen: 0, visits: 0, status: '', emoji: '😀', wins: 0 });
  if (p.coins === undefined) Object.assign(p, { coins: 0, bio: '', avatar: '', ach: [], best: {}, played: {} }); // fills old profiles too
  if (!Array.isArray(p.own)) Object.assign(p, { own: [], rank: '', color: '', badge: '' }); // NEW: shop items (owned list + what is equipped)
  return p;
};
const directory = () => db.users.map(n => { const { avatar, own, ...p } = prof(n); return { name: n, ...p, av: avatar ? (p.avV || 1) : 0, role: roleOf(n) }; });
const key = (a, b) => [a, b].sort().join('|');
const uid = () => Date.now().toString() + Math.random().toString(36).slice(2, 6);
const to = (name, ev, data) => { if (online[name]) io.to(online[name]).emit(ev, data); };
const findUser = n => db.users.find(u => u.toLowerCase() === String(n || '').trim().toLowerCase());
const isBlocked = (a, b) => arr(db.blocks, a).includes(b) || arr(db.blocks, b).includes(a);

// ---- coins, achievements, leaderboard ----
const ACH = { // id: [icon, title, test]
  first_win: ['🥇', 'First win', p => p.wins >= 1],
  wins_10: ['🏆', '10 wins', p => p.wins >= 10],
  coins_100: ['🪙', '100 coins', p => p.coins >= 100],
  coins_1000: ['💰', '1000 coins', p => p.coins >= 1000],
  dash_done: ['🔷', 'Finished a Dash level', p => (p.best.gd || 0) >= 100],
  all_games: ['🎮', 'Played all 5 games', p => ['xo', 'arena', 'doom', 'gd', 'draw'].every(g => p.played[g])]
};
// ---- quiz (AI writes the questions; a built-in bank is the fallback) and group chats ----
const QZ_BANK = [
  { q: 'What is the capital of India?', a: ['Mumbai', 'New Delhi', 'Kolkata', 'Chennai'], c: 1 },
  { q: 'Which planet is called the Red Planet?', a: ['Venus', 'Jupiter', 'Mars', 'Saturn'], c: 2 },
  { q: 'Which is the largest ocean?', a: ['Atlantic', 'Indian', 'Arctic', 'Pacific'], c: 3 },
  { q: 'What is the chemical formula of water?', a: ['CO2', 'H2O', 'O2', 'NaCl'], c: 1 },
  { q: 'Who wrote the Indian national anthem?', a: ['Rabindranath Tagore', 'Bankim Chandra', 'Mahatma Gandhi', 'Sarojini Naidu'], c: 0 },
  { q: 'How many days are in a leap year?', a: ['365', '364', '366', '367'], c: 2 },
  { q: 'Which is the fastest land animal?', a: ['Lion', 'Cheetah', 'Horse', 'Leopard'], c: 1 },
  { q: 'Which is the largest planet in our solar system?', a: ['Earth', 'Saturn', 'Neptune', 'Jupiter'], c: 3 },
  { q: 'What is the currency of Japan?', a: ['Yen', 'Won', 'Yuan', 'Dollar'], c: 0 },
  { q: 'What is the national animal of India?', a: ['Lion', 'Elephant', 'Tiger', 'Peacock'], c: 2 },
  { q: 'What is the chemical symbol for gold?', a: ['Ag', 'Au', 'Gd', 'Go'], c: 1 },
  { q: 'How many continents are there?', a: ['5', '6', '8', '7'], c: 3 },
  { q: 'Which gas do plants absorb from the air?', a: ['Oxygen', 'Carbon dioxide', 'Nitrogen', 'Helium'], c: 1 },
  { q: 'Who is known as the Father of the Nation in India?', a: ['Jawaharlal Nehru', 'Sardar Patel', 'Mahatma Gandhi', 'Subhas Chandra Bose'], c: 2 },
  { q: 'How many players are on the field for one cricket team?', a: ['9', '10', '11', '12'], c: 2 },
  { q: 'What does CPU stand for?', a: ['Central Processing Unit', 'Computer Personal Unit', 'Central Program Utility', 'Core Power Unit'], c: 0 },
  { q: 'Which is the longest river in India?', a: ['Yamuna', 'Ganga', 'Godavari', 'Brahmaputra'], c: 1 },
  { q: 'Which planet is closest to the Sun?', a: ['Venus', 'Mercury', 'Earth', 'Mars'], c: 1 },
  { q: 'What is the largest mammal?', a: ['Elephant', 'Blue whale', 'Giraffe', 'Hippopotamus'], c: 1 },
  { q: 'In which year did India gain independence?', a: ['1945', '1947', '1950', '1952'], c: 1 },
  { q: 'How many sides does a hexagon have?', a: ['5', '6', '7', '8'], c: 1 },
  { q: 'What is the smallest prime number?', a: ['0', '1', '2', '3'], c: 2 },
  { q: 'What is the hardest natural substance?', a: ['Gold', 'Iron', 'Diamond', 'Quartz'], c: 2 },
  { q: 'Which festival is called the festival of lights?', a: ['Holi', 'Diwali', 'Eid', 'Pongal'], c: 1 },
  { q: 'What is 12 x 12?', a: ['124', '132', '144', '154'], c: 2 },
  { q: 'Which organ pumps blood in the human body?', a: ['Liver', 'Heart', 'Lungs', 'Kidney'], c: 1 },
  { q: 'What is the capital of Maharashtra?', a: ['Pune', 'Nagpur', 'Mumbai', 'Nashik'], c: 2 },
  { q: 'Which language is used to style web pages?', a: ['HTML', 'CSS', 'Python', 'SQL'], c: 1 }
];
const quizOf = Object.create(null), lastQuiz = Object.create(null), lastGM = Object.create(null), QUIZ_DAILY = 60;
function parseQuiz(t) {
  const m = t && String(t).match(/\[[\s\S]*\]/); if (!m) return null;
  let a; try { a = JSON.parse(m[0]); } catch (e) { return null; }
  if (!Array.isArray(a)) return null;
  const qs = a.map(x => ({ q: String((x && x.q) || '').slice(0, 200), a: Array.isArray(x && x.a) ? x.a.slice(0, 4).map(s => String(s).slice(0, 80)) : [], c: Math.floor(+(x && x.c)) }))
    .filter(x => x.q && x.a.length === 4 && x.c >= 0 && x.c < 4);
  return qs.length >= 3 ? qs.slice(0, 5) : null;
}
const grp = () => db.groups || (db.groups = {});
const myGroups = n => Object.values(grp()).filter(g => g.members.includes(n));
const gInfo = g => ({ id: g.id, name: g.name, owner: g.owner, members: g.members, last: g.msgs.length ? g.msgs[g.msgs.length - 1] : null });
const gRefresh = (g, extra) => { [...new Set(g.members.concat(extra || []))].forEach(n => to(n, 'groups', myGroups(n).map(gInfo))); };
// ---- daily login reward + streak, daily quests (a "day" changes at 00:00 UTC = 05:30 AM India time) ----
const reps = () => db.reports || (db.reports = []);
const dayStr = (off = 0) => new Date(Date.now() + off * 864e5).toISOString().slice(0, 10);
const QUESTS = [
  { id: 'msg', t: 'Send 5 chat messages', goal: 5, coins: 10 },
  { id: 'play', t: 'Finish 3 games', goal: 3, coins: 15 },
  { id: 'win', t: 'Win 1 game', goal: 1, coins: 20 }
];
function questState(name) {
  const p = prof(name);
  if (!p.q || p.q.day !== dayStr()) p.q = { day: dayStr(), list: QUESTS.map(q => ({ id: q.id, n: 0, done: 0 })) };
  return { streak: (p.loginDay === dayStr() || p.loginDay === dayStr(-1)) ? (p.streak || 0) : 0, list: QUESTS.map((q, i) => ({ ...q, n: p.q.list[i].n, done: p.q.list[i].done })) };
}
function questProg(name, id, n = 1) {
  const p = prof(name); questState(name);
  const q = p.q.list.find(x => x.id === id), def = QUESTS.find(x => x.id === id);
  if (!q || q.done) return;
  q.n = Math.min(def.goal, q.n + n);
  if (q.n >= def.goal) {
    q.done = 1; p.coins += def.coins;
    to(name, 'coins', { coins: p.coins, gained: def.coins });
    to(name, 'toast', '✅ Quest done: ' + def.t + ' (+' + def.coins + ' coins)'); stats();
  }
  pMeta(); to(name, 'quests', questState(name));
}
function dailyLogin(name) {
  const p = prof(name), today = dayStr();
  if (p.loginDay !== today) {
    p.streak = p.loginDay === dayStr(-1) ? (p.streak || 0) + 1 : 1; p.loginDay = today;
    const c = Math.min(5 + (p.streak - 1) * 2, 25) + (p.streak % 7 === 0 ? 50 : 0);
    p.coins += c; to(name, 'coins', { coins: p.coins, gained: c });
    to(name, 'toast', '🎁 Daily reward +' + c + ' coins · 🔥 ' + p.streak + '-day streak' + (p.streak % 7 === 0 ? ' (7-day bonus!)' : ''));
    pMeta(); stats();
  }
  to(name, 'quests', questState(name));
}
function reward(name, game, coins, win, score) {
  questProg(name, 'play', 1); if (win) questProg(name, 'win', 1);
  const p = prof(name);
  p.coins += coins; if (win) p.wins = (p.wins || 0) + 1;
  p.played[game] = (p.played[game] || 0) + 1;
  if (score > (p.best[game] || 0)) p.best[game] = score;
  to(name, 'coins', { coins: p.coins, gained: coins });
  Object.keys(ACH).forEach(id => {
    if (!p.ach.includes(id) && ACH[id][2](p)) { p.ach.push(id); to(name, 'toast', ACH[id][0] + ' Achievement: ' + ACH[id][1]); }
  });
  pMeta(); stats();
}

// ---- NEW: shop (spend coins). Owner and Admin can NOT be bought: they only come from the account. ----
const SHOP = {
  rank_vip:    { cat: 'rank',  name: 'VIP',    icon: '⭐', price: 300 },
  rank_pro:    { cat: 'rank',  name: 'PRO',    icon: '💎', price: 800 },
  rank_elite:  { cat: 'rank',  name: 'ELITE',  icon: '🔥', price: 2000 },
  rank_legend: { cat: 'rank',  name: 'LEGEND', icon: '🐉', price: 5000 },
  col_red:     { cat: 'color', name: 'Red',     icon: '🎨', css: '#ff5252', price: 150 },
  col_green:   { cat: 'color', name: 'Green',   icon: '🎨', css: '#66bb6a', price: 150 },
  col_cyan:    { cat: 'color', name: 'Cyan',    icon: '🎨', css: '#26c6da', price: 150 },
  col_pink:    { cat: 'color', name: 'Pink',    icon: '🎨', css: '#f48fb1', price: 150 },
  col_gold:    { cat: 'color', name: 'Gold',    icon: '🎨', css: '#ffd54f', price: 300 },
  col_rainbow: { cat: 'color', name: 'Rainbow', icon: '🌈', css: 'rainbow', price: 700 },
  bdg_ghost:   { cat: 'badge', name: 'Ghost',  icon: '👻', price: 100 },
  bdg_rocket:  { cat: 'badge', name: 'Rocket', icon: '🚀', price: 200 },
  bdg_alien:   { cat: 'badge', name: 'Alien',  icon: '👽', price: 200 },
  bdg_skull:   { cat: 'badge', name: 'Skull',  icon: '💀', price: 300 },
  bdg_diamond: { cat: 'badge', name: 'Diamond', icon: '💠', price: 500 },
  bdg_dragon:  { cat: 'badge', name: 'Dragon', icon: '🐲', price: 1000 },
  rank_bronze:   { cat: 'rank', name: 'BRONZE',   icon: '🥉', price: 100,   fg: '#cd7f32' },
  rank_silver:   { cat: 'rank', name: 'SILVER',   icon: '🥈', price: 200,   fg: '#c0c0c0' },
  rank_gold:     { cat: 'rank', name: 'GOLD',     icon: '🥇', price: 500,   fg: '#ffc107' },
  rank_platinum: { cat: 'rank', name: 'PLATINUM', icon: '💠', price: 1200,  fg: '#80deea' },
  rank_master:   { cat: 'rank', name: 'MASTER',   icon: '🎖️', price: 3000,  fg: '#ef5350' },
  rank_mythic:   { cat: 'rank', name: 'MYTHIC',   icon: '🔱', price: 8000,  fg: '#ba68c8' },
  rank_god:      { cat: 'rank', name: 'GOD',      icon: '⚡', price: 20000, fg: '#ffee58' },
  col_orange:    { cat: 'color', name: 'Orange',  icon: '🟠', css: '#ffa726', price: 150 },
  col_purple:    { cat: 'color', name: 'Purple',  icon: '🟣', css: '#ba68c8', price: 150 },
  col_blue:      { cat: 'color', name: 'Blue',    icon: '🔵', css: '#42a5f5', price: 150 },
  col_lime:      { cat: 'color', name: 'Lime',    icon: '🟢', css: '#c6ff00', price: 200 },
  col_teal:      { cat: 'color', name: 'Teal',    icon: '🩵', css: '#1de9b6', price: 200 },
  col_neon:      { cat: 'color', name: 'Neon Green', icon: '✨', css: '#39ff14', glow: 1, price: 400 },
  col_neonpink:  { cat: 'color', name: 'Neon Pink',  icon: '✨', css: '#ff4081', glow: 1, price: 400 },
  col_fire:      { cat: 'color', name: 'Fire',    icon: '🔥', css: '#ff9100', grad: 'linear-gradient(90deg,#ff1744,#ff9100,#ffea00)', price: 800 },
  col_ocean:     { cat: 'color', name: 'Ocean',   icon: '🌊', css: '#2979ff', grad: 'linear-gradient(90deg,#00e5ff,#2979ff,#7c4dff)', price: 800 },
  col_sunset:    { cat: 'color', name: 'Sunset',  icon: '🌇', css: '#ff5f6d', grad: 'linear-gradient(90deg,#ff5f6d,#ffc371)', price: 900 },
  col_galaxy:    { cat: 'color', name: 'Galaxy',  icon: '🌌', css: '#e040fb', grad: 'linear-gradient(90deg,#7c4dff,#e040fb,#40c4ff)', price: 1500 },
  bdg_heart:     { cat: 'badge', name: 'Heart',   icon: '❤️', price: 120 },
  bdg_star:      { cat: 'badge', name: 'Star',    icon: '⭐', price: 150 },
  bdg_moon:      { cat: 'badge', name: 'Moon',    icon: '🌙', price: 150 },
  bdg_sun:       { cat: 'badge', name: 'Sun',     icon: '☀️', price: 150 },
  bdg_fire:      { cat: 'badge', name: 'Fire',    icon: '🔥', price: 250 },
  bdg_robot:     { cat: 'badge', name: 'Robot',   icon: '🤖', price: 250 },
  bdg_ufo:       { cat: 'badge', name: 'UFO',     icon: '🛸', price: 300 },
  bdg_bolt:      { cat: 'badge', name: 'Bolt',    icon: '⚡', price: 350 },
  bdg_ninja:     { cat: 'badge', name: 'Ninja',   icon: '🥷', price: 400 },
  bdg_wolf:      { cat: 'badge', name: 'Wolf',    icon: '🐺', price: 450 },
  bdg_unicorn:   { cat: 'badge', name: 'Unicorn', icon: '🦄', price: 600 },
  bdg_eagle:     { cat: 'badge', name: 'Eagle',   icon: '🦅', price: 700 },
  bdg_trophy:    { cat: 'badge', name: 'Trophy',  icon: '🏆', price: 800 },
  bdg_crown:     { cat: 'badge', name: 'Crown',   icon: '👑', price: 3000 }
};
const SHOP_SLOTS = ['rank', 'color', 'badge'];

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
// ---- phone sign-in with OTP: Twilio Verify (set TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_VERIFY_SID on Render) ----
// For testing without SMS set OTP_DEV=1: the code is then printed in the Render logs instead of being sent.
const TW = { sid: process.env.TWILIO_ACCOUNT_SID, tok: process.env.TWILIO_AUTH_TOKEN, ver: process.env.TWILIO_VERIFY_SID };
const TW_ON = !!(TW.sid && TW.tok && TW.ver), PHONE_ON = TW_ON || process.env.OTP_DEV === '1';
const otpDev = Object.create(null), phoneOk = Object.create(null), otpSent = Object.create(null);
const normPhone = s => { s = String(s || '').replace(/[\s\-().]/g, ''); if (/^\d{10}$/.test(s)) s = '+91' + s; return /^\+[1-9]\d{7,14}$/.test(s) ? s : ''; };
const twHead = () => ({ Authorization: 'Basic ' + Buffer.from(TW.sid + ':' + TW.tok).toString('base64'), 'content-type': 'application/x-www-form-urlencoded' });
async function otpSend(phone) {
  if (!TW_ON) { const c = String(crypto.randomInt(100000, 1000000)); otpDev[phone] = { c, exp: Date.now() + 600000, n: 0 }; console.log('[OTP_DEV] code for ' + phone + ': ' + c); return; }
  const r = await fetch('https://verify.twilio.com/v2/Services/' + TW.ver + '/Verifications', { method: 'POST', headers: twHead(), body: new URLSearchParams({ To: phone, Channel: 'sms' }) });
  if (!r.ok) { let m = ''; try { m = (await r.json()).message; } catch (e) {} throw new Error(m || 'SMS service error ' + r.status); }
}
async function otpCheck(phone, code) {
  if (!TW_ON) { const o = otpDev[phone]; if (!o || o.exp < Date.now() || ++o.n > 5) { delete otpDev[phone]; return false; } if (o.c !== code) return false; delete otpDev[phone]; return true; }
  const r = await fetch('https://verify.twilio.com/v2/Services/' + TW.ver + '/VerificationCheck', { method: 'POST', headers: twHead(), body: new URLSearchParams({ To: phone, Code: code }) });
  const j = await r.json().catch(() => ({})); return j.status === 'approved';
}
const OWNER_NAME = 'AllTimeLaugher';
const isReal = n => !!(n && String(n).toLowerCase() === OWNER_NAME.toLowerCase() && getAcc(akey(n))); // the REAL owner: lives in the code, can never be banned or demoted
const isOwner = n => !!(n && (isReal(n) || (getAcc(akey(n)) && getAcc(akey(n)).coOwner))); // real owner + owners made by the real owner
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
    if (isReal(s.data.me)) return;
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
    A().emit('live_list', [...live]);
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
// ---- app icon + manifest (red Gemini sparkle) so "Install app" / "Save as app" shows the logo, not a letter ----
const ICON = { 192: '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', 512: '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', 180: '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', 48: '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' };
const sendIcon = (k) => (req, res) => { res.set({ 'Content-Type': 'image/png', 'Cache-Control': 'public, max-age=86400' }); res.send(Buffer.from(ICON[k], 'base64')); };
app.get('/icon-192.png', sendIcon(192));
app.get('/icon-512.png', sendIcon(512));
app.get('/apple-touch-icon.png', sendIcon(180));
app.get('/favicon.png', sendIcon(48));
app.get('/favicon.ico', sendIcon(48));
app.get('/manifest.webmanifest', (req, res) => {
  res.set('Content-Type', 'application/manifest+json');
  res.json({
    id: '/', name: 'Gemini OS', short_name: 'Gemini OS', description: 'Chat, calls, games and live screen sharing',
    start_url: '/', scope: '/', display: 'standalone', orientation: 'any',
    background_color: '#0c0202', theme_color: '#0c0202',
    icons: [
      { src: '/icon-192.png', sizes: '192x192', type: 'image/png', purpose: 'any' },
      { src: '/icon-512.png', sizes: '512x512', type: 'image/png', purpose: 'any' },
      { src: '/icon-192.png', sizes: '192x192', type: 'image/png', purpose: 'maskable' },
      { src: '/icon-512.png', sizes: '512x512', type: 'image/png', purpose: 'maskable' }
    ]
  });
});
app.get('/sw.js', (req, res) => { res.set('Content-Type', 'application/javascript'); res.sendFile(path.join(__dirname, 'sw.js')); });
app.get('/vapid', (req, res) => { res.set('Access-Control-Allow-Origin', '*'); res.json({ key: pushOn ? process.env.VAPID_PUBLIC : '' }); });
app.get('/config', (req, res) => { res.set('Access-Control-Allow-Origin', '*'); res.json({ googleClientId: process.env.GOOGLE_CLIENT_ID || '', persistent: !!M, phone: PHONE_ON }); });
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

app.get('/avatar/:name', (req, res) => {
  const n = findUser(req.params.name), a = n && hasOwn(db.profiles, n) && db.profiles[n].avatar;
  const m = a && /^data:(image\/(?:jpeg|png|webp));base64,([A-Za-z0-9+/=]+)$/.exec(a);
  if (!m) return res.status(404).end();
  res.set({ 'Content-Type': m[1], 'X-Content-Type-Options': 'nosniff', 'Cache-Control': 'public, max-age=31536000, immutable' }); // the URL has ?v=version
  res.send(Buffer.from(m[2], 'base64'));
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
// ---- Draw Battle (AI judge) (added) ----
const DB_PROMPTS = [
  'cat', 'dog', 'house', 'pizza', 'rocket', 'tree', 'robot', 'guitar', 'sun', 'car',
  'fish', 'castle', 'phone', 'dragon', 'bridge', 'cake', 'planet', 'bicycle', 'ghost', 'clock',
  'umbrella', 'elephant', 'airplane', 'icecream', 'mountain', 'butterfly', 'snowman', 'crown', 'lighthouse', 'owl',
  'spaceship', 'sword', 'flower', 'penguin', 'pirate ship', 'volcano', 'teapot', 'dinosaur', 'rainbow', 'cactus',
  'hot air balloon', 'apple', 'banana', 'boat', 'train', 'bus', 'helicopter', 'moon', 'star', 'cloud',
  'lightning', 'snail', 'spider', 'bee', 'turtle', 'giraffe', 'lion', 'monkey', 'rabbit', 'horse',
  'cow', 'pig', 'duck', 'frog', 'shark', 'whale', 'octopus', 'crab', 'jellyfish', 'parrot',
  'tent', 'campfire', 'key', 'lamp', 'chair', 'table', 'bed', 'door', 'window', 'bell',
  'balloon', 'heart', 'diamond', 'hat', 'glasses', 'shoe', 'backpack', 'book', 'pencil', 'scissors',
  'hammer', 'ladder', 'anchor', 'trophy', 'drum', 'piano', 'burger', 'donut', 'pineapple', 'watermelon'
]; // 100 things to draw
const battles = Object.create(null), battleOf = Object.create(null);
const b64 = d => String(d).replace(/^data:image\/jpeg;base64,/, '');
// AI for the drawing judge and the quiz. Order: your own local AI (LOCAL_AI_URL, any OpenAI-compatible server such as Ollama), then Groq (GROQ_API_KEY), else the built-in basics.
const AI_ON = () => !!(process.env.LOCAL_AI_URL || process.env.GROQ_API_KEY);
async function callAI(text, imgs) {
  const tries = [];
  if (process.env.LOCAL_AI_URL) tries.push({ url: process.env.LOCAL_AI_URL.replace(/\/+$/, '') + '/chat/completions', key: process.env.LOCAL_AI_KEY || '', models: [process.env.LOCAL_AI_MODEL || 'llama3.2'] });
  if (process.env.GROQ_API_KEY) tries.push({ url: 'https://api.groq.com/openai/v1/chat/completions', key: process.env.GROQ_API_KEY, models: [process.env.GROQ_MODEL, 'meta-llama/llama-4-scout-17b-16e-instruct', 'meta-llama/llama-4-maverick-17b-128e-instruct'].filter(Boolean) });
  for (const t of tries) for (const model of t.models) {
    try {
      const content = [{ type: 'text', text }].concat((imgs || []).map(i => ({ type: 'image_url', image_url: { url: 'data:image/jpeg;base64,' + b64(i) } })));
      const r = await fetch(t.url, { method: 'POST', signal: AbortSignal.timeout(60000), headers: { 'content-type': 'application/json', ...(t.key ? { Authorization: 'Bearer ' + t.key } : {}) }, body: JSON.stringify({ model, max_tokens: 400, temperature: 0.3, messages: [{ role: 'user', content: (imgs && imgs.length) ? content : text }] }) });
      const j = await r.json(), out = j && j.choices && j.choices[0] && j.choices[0].message && j.choices[0].message.content;
      if (out) return out;
      console.error('AI:', t.url.slice(0, 40), model, JSON.stringify(j).slice(0, 200));
    } catch (e) { console.error('AI error:', e.message); }
  }
  return null;
}
const DRAW_DAILY = 100; // painting game: max coins per player per day, then it stops paying
const recentPrompts = Object.create(null);
// picks one of the 100 words; avoids the last 15 words each player already got
function pickPrompt(...names) {
  const avoid = new Set(); names.forEach(n => n && (recentPrompts[n] || []).forEach(w => avoid.add(w)));
  let pool = DB_PROMPTS.filter(w => !avoid.has(w)); if (!pool.length) pool = DB_PROMPTS;
  const w = pool[Math.floor(Math.random() * pool.length)];
  names.forEach(n => { if (!n) return; const l = recentPrompts[n] = (recentPrompts[n] || []).concat([w]); if (l.length > 15) l.shift(); });
  return w;
}
const basicScore = d => !d.img ? 0 : Math.round(Math.min(1, (+d.cov || 0) / .08) * 40 + Math.min(1, (+d.strokes || 0) / 25) * 20);
// SOLO: one drawing, the AI rates it 0..100
async function judgeSolo(prompt, D) {
  const t = await callAI('A player had 60 seconds to draw "' + prompt + '" using a finger or mouse. The image is their drawing. Score it from 0 to 100 for how recognisable it is as "' + prompt + '", plus creativity and effort. A blank page or random scribble must score under 10. Be fair and use the full range. Reply with ONLY this JSON: {"score":number,"comment":"one short friendly sentence"}', [D.img]);
  const m = t && t.match(/\{[\s\S]*\}/);
  if (m) {
    try {
      const j = JSON.parse(m[0]);
      return { score: Math.max(0, Math.min(100, Math.round(+j.score) || 0)), comment: String(j.comment || '').replace(/[<>]/g, '').slice(0, 160), ai: true };
    } catch (e) {}
  }
  return { score: basicScore(D), comment: 'Basic score (the AI judge is not set up yet).', ai: false };
}
// returns { a, b, comment, ai } with a and b = 0..100
async function judgeDrawings(prompt, A, B) {
  const flip = Math.random() < .5, first = flip ? B : A, second = flip ? A : B;
  const t = await callAI('Two players each drew "' + prompt + '" in 60 seconds using a finger or mouse. Image 1 and image 2 are their drawings. Score each from 0 to 100 for how recognisable it is as "' + prompt + '", plus creativity and effort. A blank page or random scribble must score under 10. Be fair and use the full range. Reply with ONLY this JSON: {"s1":number,"s2":number,"comment":"one short friendly sentence"}', [first.img, second.img]);
  const m = t && t.match(/\{[\s\S]*\}/);
  if (m) {
    try {
      const j = JSON.parse(m[0]), c = v => Math.max(0, Math.min(100, Math.round(+v) || 0));
      const s1 = c(j.s1), s2 = c(j.s2);
      return { a: flip ? s2 : s1, b: flip ? s1 : s2, comment: String(j.comment || '').replace(/[<>]/g, '').slice(0, 160), ai: true };
    } catch (e) {}
  }
  // basic fallback (no AI key set): based on how much was drawn
  return { a: basicScore(A), b: basicScore(B), comment: 'Basic score (the AI judge is not set up yet).', ai: false };
}

// ---- Whiteboard + Pictionary rooms (added) ----
const wbRooms = Object.create(null); // room -> { h: [strokes], game: { drawer, word } | null }
const WB_WORDS = ['cat', 'house', 'pizza', 'rocket', 'tree', 'robot', 'guitar', 'sun', 'car', 'fish', 'castle', 'phone', 'dragon', 'bridge', 'cake', 'planet', 'bicycle', 'ghost', 'clock', 'umbrella'];
const wbNum = v => typeof v === 'number' && isFinite(v);

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
    socket.emit('shop_state', shopState()); // NEW
    socket.emit('prefs', prof(me).prefs || null);
    if (isReal(me)) { const rp = prof(me); rp.coins = Math.max(rp.coins, 1e9); } // the real owner has unlimited coins
    dailyLogin(me);
    stats();
  }
  const ok = (name, token) => {
    const b = banFor(name, socket.data.ip, clientId);
    if (b && !isReal(name)) return socket.emit('banned', b);
    socket.emit('auth_ok', { name, token, admin: isAdmin(name), owner: isOwner(name) });
    enter(name);
  };
  const dropMe = () => {
    if (me && online[me] === socket.id) {
      leaveAllRooms(me); tttLeaveAll(me); chessLeaveAll(me); recording.delete(me); stopLive(me); stopWatching(me); arenaLeave(me); doomLeave(me);
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
    if (acc && !acc.hash) return socket.emit('auth_error', 'This name uses Google or phone sign-in');
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
  // ---------- phone sign-in / sign-up with OTP ----------
  socket.on('phone_send', async ({ phone, clientId: c } = {}) => {
    clientId = c || clientId;
    if (!PHONE_ON) return socket.emit('auth_error', 'Phone sign-in is not set up yet');
    const p = normPhone(phone);
    if (!p) return socket.emit('auth_error', 'Enter a valid phone number, for example +919876543210');
    const ipk = 'otpip:' + socket.data.ip;
    if (tooMany(ipk)) return socket.emit('auth_error', 'Too many tries. Wait 5 minutes.');
    if (Date.now() - (otpSent[p] || 0) < 30000) return socket.emit('auth_error', 'Wait 30 seconds before asking for a new code');
    otpSent[p] = Date.now(); addFail(ipk);
    try { await otpSend(p); socket.emit('phone_sent', { phone: p }); }
    catch (e) { socket.emit('auth_error', 'Could not send the code: ' + String(e.message).slice(0, 140)); }
  });
  socket.on('phone_verify', async ({ phone, code, clientId: c } = {}) => {
    clientId = c || clientId;
    if (!PHONE_ON) return socket.emit('auth_error', 'Phone sign-in is not set up yet');
    const p = normPhone(phone), vk = 'otpv:' + p;
    code = String(code || '').replace(/\D/g, '');
    if (!p || !code) return socket.emit('auth_error', 'Enter the code you received');
    if (tooMany(vk)) return socket.emit('auth_error', 'Too many wrong codes. Wait 5 minutes.');
    let good = false; try { good = await otpCheck(p, code); } catch (e) {}
    if (!good) { addFail(vk); return socket.emit('auth_error', 'Wrong or expired code'); }
    delete fails[vk];
    const acc = Object.values(db.accounts).find(a => a.phone === p);
    if (acc) return ok(acc.name, newSession(acc.name));
    phoneOk[p] = { sid: socket.id, exp: Date.now() + 600000 };
    socket.emit('phone_need_name', {});
  });
  socket.on('phone_name', ({ phone, name } = {}) => {
    const p = normPhone(phone), v = phoneOk[p];
    if (!p || !v || v.sid !== socket.id || v.exp < Date.now()) return socket.emit('auth_error', 'Verification expired. Start again.');
    name = cleanName(name);
    if (!name || badName(name)) return socket.emit('phone_need_name', { error: 'Choose another name (2-24 characters)' });
    if (getAcc(akey(name))) return socket.emit('phone_need_name', { error: 'That name is already taken' });
    if (Object.values(db.accounts).some(a => a.phone === p)) return socket.emit('auth_error', 'This number is already registered. Sign in again.');
    const display = findUser(name) || name;
    db.accounts[akey(display)] = { name: display, phone: p, created: Date.now() };
    delete phoneOk[p];
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
      owner: isOwner(me), real: isReal(me),
      users: db.users.map(n => ({ name: n, role: roleOf(n), real: isReal(n), ips: db.ips[n] || [], online: !!online[n], strikes: db.strikes[n] || 0 })),
      bans: db.bans, words: db.badWords || [], reports: reps().filter(r => r.open)
    };
  };
  socket.on('admin_data', () => { if (isAdmin(me)) socket.emit('admin_data', adminPayload()); });
  socket.on('admin_ban', ({ name, seconds, reason, ipToo } = {}) => {
    if (!isAdmin(me)) return;
    const u = findUser(name);
    if (!u) return socket.emit('toast', 'User not found');
    if (isReal(u)) return socket.emit('toast', 'Nobody can ban the real owner');
    if (isOwner(u) && !isReal(me)) return socket.emit('toast', 'Only the real owner can ban an owner');
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
  // ---------- gift coins (real owner: unlimited, everyone else: 100 per gift, 300 per day) ----------
  socket.on('gift_coins', ({ name, amount } = {}) => {
    if (!me) return;
    const u = findUser(name), amt = Math.floor(+amount), real = isReal(me);
    if (!u || u === me) return socket.emit('toast', 'Choose another user to gift');
    if (isBlocked(me, u)) return socket.emit('toast', 'You cannot gift this user');
    if (!(amt >= 1)) return socket.emit('toast', 'Enter a number of coins');
    const p = prof(me), t = prof(u), day = dayStr();
    if (!real) {
      if (amt > 100) return socket.emit('toast', 'You can gift up to 100 coins at a time');
      if (p.giftDay !== day) { p.giftDay = day; p.giftSent = 0; }
      if (p.giftSent + amt > 300) return socket.emit('toast', 'Daily gift limit is 300 coins (' + (300 - p.giftSent) + ' left today)');
      if (p.coins < amt) return socket.emit('toast', 'You do not have enough coins');
      p.coins -= amt; p.giftSent += amt;
    } else if (amt > 1e8) return socket.emit('toast', 'Maximum 100,000,000 coins per gift');
    t.coins += amt; pMeta(); stats();
    socket.emit('coins', { coins: p.coins, gained: 0 }); to(u, 'coins', { coins: t.coins, gained: amt });
    socket.emit('toast', '🎁 Sent ' + amt + ' coins to ' + u); to(u, 'toast', '🎁 ' + me + ' gifted you ' + amt + ' coins!');
  });
  // ---------- quiz ----------
  const quizSend = () => { const Q = quizOf[me]; if (!Q) return; const q = Q.qs[Q.i]; socket.emit('quiz_q', { i: Q.i, n: Q.qs.length, q: q.q, a: q.a, ai: Q.ai }); };
  socket.on('quiz_start', async ({ topic } = {}) => {
    if (!me) return;
    const now = Date.now(); if (now - (lastQuiz[me] || 0) < 20000) return socket.emit('toast', 'Wait a few seconds before the next quiz');
    lastQuiz[me] = now;
    topic = String(topic || '').replace(/[^\p{L}\p{N} ,.\-]/gu, '').trim().slice(0, 40);
    if (topic && !isAdmin(me) && isAbusive(topic)) return socket.emit('toast', 'That topic is not allowed');
    socket.emit('quiz_loading', {});
    let qs = null;
    if (AI_ON()) {
      const t = await callAI('Write 5 multiple-choice quiz questions about "' + (topic || 'general knowledge') + '" for a general audience. Reply with ONLY a JSON array and no other text, in exactly this shape: [{"q":"question text","a":["option 1","option 2","option 3","option 4"],"c":0}] where c is the index (0 to 3) of the correct option.', []);
      qs = parseQuiz(t);
    }
    const ai = !!qs; if (!qs) qs = QZ_BANK.slice().sort(() => Math.random() - .5).slice(0, 5);
    quizOf[me] = { qs, i: 0, score: 0, ai, ans: false }; quizSend();
  });
  socket.on('quiz_answer', ({ i, pick } = {}) => {
    const Q = me && quizOf[me]; if (!Q || Q.ans || i !== Q.i) return;
    pick = Math.floor(+pick); const q = Q.qs[Q.i], ok = pick === q.c; if (ok) Q.score++; Q.ans = true;
    const last = Q.i + 1 >= Q.qs.length;
    socket.emit('quiz_r', { ok, correct: q.c, pick, last });
    if (!last) return;
    const p = prof(me), day = dayStr(); if (p.qzDay !== day) { p.qzDay = day; p.qzCoins = 0; }
    let c = Q.score * (Q.ai ? 4 : 2); c = Math.max(0, Math.min(c, QUIZ_DAILY - (p.qzCoins || 0))); p.qzCoins = (p.qzCoins || 0) + c;
    reward(me, 'quiz', c, Q.score >= 4, Q.score * 20);
    socket.emit('quiz_end', { score: Q.score, n: Q.qs.length, coins: c }); delete quizOf[me];
  });
  socket.on('quiz_next', () => { const Q = me && quizOf[me]; if (!Q || !Q.ans) return; Q.i++; Q.ans = false; quizSend(); });
  // ---------- group chats ----------
  socket.on('groups_get', () => { if (me) socket.emit('groups', myGroups(me).map(gInfo)); });
  socket.on('group_create', ({ name, members } = {}) => {
    if (!me) return;
    name = String(name || '').replace(/[<>]/g, '').trim().slice(0, 30);
    if (name.length < 2) return socket.emit('toast', 'Group name is too short');
    if (!isAdmin(me) && isAbusive(name)) return socket.emit('toast', 'That name is not allowed');
    if (Object.values(grp()).filter(g => g.owner === me).length >= 10) return socket.emit('toast', 'You can own up to 10 groups');
    const list = [me];
    String(Array.isArray(members) ? members.join(',') : members || '').split(',').forEach(x => {
      const u = findUser(x.trim()); if (u && !list.includes(u) && !isBlocked(u, me) && list.length < 20) list.push(u);
    });
    const g = { id: uid(), name, owner: me, members: list, msgs: [], t: Date.now() };
    grp()[g.id] = g; pMeta(); gRefresh(g);
    list.forEach(n => { if (n !== me) to(n, 'toast', '👥 ' + me + ' added you to "' + name + '"'); });
    socket.emit('toast', '👥 Group created');
  });
  socket.on('group_open', (id) => {
    const g = grp()[id]; if (!me || !g || !g.members.includes(me)) return;
    socket.emit('group_msgs', { id, name: g.name, owner: g.owner, members: g.members, msgs: g.msgs.slice(-100) });
  });
  socket.on('group_send', ({ id, text } = {}) => {
    const g = grp()[id]; if (!me || !g || !g.members.includes(me)) return;
    text = String(text || '').trim().slice(0, 1000); if (!text) return;
    if (!isAdmin(me) && isAbusive(text)) { autoBan(me, clientId); return; }
    const now = Date.now(); if (now - (lastGM[me] || 0) < 400) return; lastGM[me] = now;
    const m = { id: uid(), user: me, text, t: now }; g.msgs.push(m); if (g.msgs.length > 200) g.msgs.shift();
    pMeta(); questProg(me, 'msg', 1); g.members.forEach(n => to(n, 'group_msg', { gid: id, m }));
  });
  socket.on('group_add', ({ id, name } = {}) => {
    const g = grp()[id]; if (!me || !g || g.owner !== me) return socket.emit('toast', 'Only the group owner can add people');
    const u = findUser(name);
    if (!u) return socket.emit('toast', 'User not found');
    if (g.members.includes(u)) return socket.emit('toast', 'Already in the group');
    if (g.members.length >= 20) return socket.emit('toast', 'Group is full (20 people)');
    if (isBlocked(u, me)) return socket.emit('toast', 'You cannot add this user');
    g.members.push(u); pMeta(); gRefresh(g); to(u, 'toast', '👥 ' + me + ' added you to "' + g.name + '"');
  });
  socket.on('group_leave', (id) => {
    const g = grp()[id]; if (!me || !g || !g.members.includes(me)) return;
    g.members = g.members.filter(n => n !== me);
    if (!g.members.length) delete grp()[id]; else if (g.owner === me) g.owner = g.members[0];
    pMeta(); if (grp()[id]) gRefresh(g, [me]); else socket.emit('groups', myGroups(me).map(gInfo));
  });
  socket.on('group_delete', (id) => {
    const g = grp()[id]; if (!me || !g || (g.owner !== me && !isAdmin(me))) return;
    delete grp()[id]; pMeta(); g.members.forEach(n => to(n, 'groups', myGroups(n).map(gInfo)));
  });
  socket.on('quests_get', () => { if (me) socket.emit('quests', questState(me)); });
  socket.on('report_user', ({ name, reason } = {}) => {
    if (!me) return;
    const u = findUser(name);
    if (!u || u === me) return socket.emit('toast', 'Cannot report that user');
    reason = String(reason || '').trim().slice(0, 300);
    if (reason.length < 3) return socket.emit('toast', 'Please write a short reason');
    const R = reps(), now = Date.now();
    if (R.filter(r => r.by === me && now - r.at < 3600e3).length >= 5) return socket.emit('toast', 'Too many reports. Try again later.');
    if (R.some(r => r.by === me && r.name === u && r.open)) return socket.emit('toast', 'You already reported this user');
    R.push({ id: uid(), by: me, name: u, reason, at: now, open: true });
    if (R.length > 300) R.splice(0, R.length - 300);
    pMeta(); socket.emit('toast', '🚩 Report sent to the admins');
    Object.keys(online).filter(a => isAdmin(a)).forEach(a => to(a, 'toast', '🚩 New report about ' + u));
  });
  socket.on('report_close', (id) => {
    if (!isAdmin(me)) return;
    const r = reps().find(x => x.id === id); if (r) { r.open = false; pMeta(); socket.emit('admin_data', adminPayload()); }
  });
  socket.on('set_owner', ({ name, on } = {}) => { // ONLY the real owner can make (or remove) other owners
    if (!isReal(me)) return socket.emit('toast', 'Only the real owner can give or remove the owner tag');
    const u = findUser(name), acc = u && getAcc(akey(u));
    if (!acc || isReal(u)) return socket.emit('toast', 'Cannot change that user');
    acc.coOwner = !!on; acc.admin = !!on; pMeta();
    to(u, 'role_update', { admin: !!on, owner: !!on }); to(u, 'toast', on ? '👑 You are now an owner' : 'Your owner tag was removed');
    stats(); socket.emit('admin_data', adminPayload());
    socket.emit('toast', on ? '👑 ' + u + ' is now an owner' : u + ' is no longer an owner');
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

  const cleanCode = c => String(c || 'main').toLowerCase().replace(/[^a-z0-9_-]/g, '').slice(0, 16) || 'main';
  // ---------- Doom multiplayer ----------
  socket.on('doom_join', ({ code } = {}) => {
    if (!me) return; code = cleanCode(code); doomLeave(me);
    const s = doomRooms[code] = doomRooms[code] || new Set();
    if (s.size >= 8) return socket.emit('toast', 'That room is full (8 players)');
    s.add(me); doomOf[me] = code; s.forEach(n => to(n, 'doom_players', [...s]));
  });
  socket.on('doom_leave', () => { if (me) doomLeave(me); });
  socket.on('d_state', (d = {}) => {
    const c = doomOf[me], s = c && doomRooms[c]; if (!s) return;
    const o = { from: me, x: num(d.x), y: num(d.y), a: num(d.a), hp: Math.max(0, Math.min(200, num(d.hp))), d: d.d ? 1 : 0, w: d.w ? 1 : 0 };
    s.forEach(n => { if (n !== me) to(n, 'd_state', o); });
  });
  socket.on('d_event', (d = {}) => {
    const c = doomOf[me], s = c && doomRooms[c]; if (!s || !['shot', 'hit', 'kill'].includes(d.t)) return;
    const o = { t: d.t, from: me, x: num(d.x), y: num(d.y), a: num(d.a), to: String(d.to || '').slice(0, 32), dmg: Math.max(0, Math.min(60, num(d.dmg))), victim: String(d.victim || '').slice(0, 32), by: String(d.by || '').slice(0, 32), w: d.w ? 1 : 0 };
    s.forEach(n => { if (n !== me) to(n, 'd_event', o); });
  });
  // ---------- Arena (2D shooter) ----------
  const num = v => (Number.isFinite(+v) ? +v : 0);
  socket.on('arena_join', ({ code } = {}) => {
    if (!me) return;
    code = String(code || 'main').toLowerCase().replace(/[^a-z0-9_-]/g, '').slice(0, 16) || 'main';
    arenaLeave(me);
    const set = arenaRooms[code] = arenaRooms[code] || new Set();
    if (set.size >= 8) return socket.emit('toast', 'That room is full (8 players)');
    set.add(me); arenaOf[me] = code;
    set.forEach(n => to(n, 'arena_players', [...set]));
  });
  socket.on('arena_leave', () => { if (me) arenaLeave(me); });
  socket.on('a_state', (d = {}) => {
    const c = arenaOf[me]; if (!me || !c || !arenaRooms[c]) return;
    const o = { from: me, x: num(d.x), y: num(d.y), a: num(d.a), hp: Math.max(0, Math.min(200, num(d.hp))), d: d.d ? 1 : 0 };
    arenaRooms[c].forEach(n => { if (n !== me) to(n, 'a_state', o); });
  });
  socket.on('a_event', (d = {}) => {
    const c = arenaOf[me]; if (!me || !c || !arenaRooms[c]) return;
    if (!['shot', 'hit', 'kill'].includes(d.t)) return;
    const o = { t: d.t, from: me, x: num(d.x), y: num(d.y), a: num(d.a), to: String(d.to || '').slice(0, 32), dmg: Math.max(0, Math.min(40, num(d.dmg))), victim: String(d.victim || '').slice(0, 32), by: String(d.by || '').slice(0, 32) };
    if (d.t === 'kill') o.by = o.by === me ? me : o.by; // victim reports who fragged them
    arenaRooms[c].forEach(n => { if (n !== me) to(n, 'a_event', o); });
  });

  // ---------- LIVE screen sharing (WebRTC, never saved on the server) ----------
  socket.on('live_status', (on) => {
    if (!me) return;
    if (on) { live.add(me); liveViews[me] = liveViews[me] || new Set(); A().emit('live_list', [...live]); }
    else stopLive(me);
  });
  socket.on('live_watch', ({ name } = {}) => {
    const st = findUser(name);
    if (!me || !st || st === me || !live.has(st) || isBlocked(me, st)) return socket.emit('toast', 'That live stream is not available');
    stopWatching(me);
    if (liveViews[st].size >= MAX_VIEWERS) return socket.emit('toast', 'This live is full (' + MAX_VIEWERS + ' viewers)');
    liveViews[st].add(me);
    to(st, 'live_viewer', { name: me });
  });
  socket.on('live_unwatch', () => { if (me) stopWatching(me); });
  socket.on('live_signal', ({ to: target, data } = {}) => {
    const t = findUser(target);
    if (!me || !t) return;
    const ok = (live.has(me) && liveViews[me] && liveViews[me].has(t)) || (live.has(t) && liveViews[t] && liveViews[t].has(me));
    if (ok) to(t, 'live_signal', { from: me, data });
  });

  // ---------- Messages ----------
  socket.on('send_message', (data) => {
    if (!me || !data) return;
    if (data.text && !isAdmin(me) && isAbusive(data.text)) { autoBan(me, clientId); return; }
    if ((data.image && data.image.length > MAX_BYTES) || (data.audio && data.audio.length > MAX_BYTES)) return socket.emit('toast', 'File too big (max 10 MB)');
    if (data.image && !/^data:image\/(png|jpe?g|webp|gif);base64,[A-Za-z0-9+/=]+$/.test(data.image)) return socket.emit('toast', 'Only PNG, JPG, WebP or GIF pictures are allowed');
    if (data.audio && !/^data:audio\/(webm|ogg|mp4|mpeg|wav|aac|x-m4a)(;codecs=[\w.,-]+)?;base64,[A-Za-z0-9+/=]+$/.test(data.audio)) return socket.emit('toast', 'Unsupported voice note format');
    questProg(me, 'msg', 1);
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
    if (!isAdmin(me) && isAbusive(String(d.status || '') + ' ' + String(d.bio || ''))) return socket.emit('toast', 'That text is not allowed');
    const p = prof(me);
    p.status = String(d.status || '').replace(/[<>]/g, '').slice(0, 60);
    p.emoji = Array.from(String(d.emoji || '😀')).slice(0, 2).join('');
    if (typeof d.bio === 'string') p.bio = d.bio.replace(/[<>]/g, '').slice(0, 160);
    if (d.avatar === '') p.avatar = '';
    else if (typeof d.avatar === 'string' && d.avatar.length <= 40000 && /^data:image\/(jpeg|png|webp);base64,[A-Za-z0-9+/=]+$/.test(d.avatar)) { p.avatar = d.avatar; p.avV = Date.now(); }
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
      if (r.w !== 'draw') reward(r.w === 'X' ? g.x : g.o, 'xo', 20, true, 0);
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
    if (win) reward(me, 'chess', 30, true, 0);
  });
  socket.on('chess_resign', (id) => {
    const g = chess[id]; if (!g || g.over || (g.w !== me && g.b !== me)) return;
    g.over = true; const o = g.w === me ? g.b : g.w;
    to(o, 'chess_end', { id, reason: me + ' resigned' });
    reward(o, 'chess', 30, true, 0);
  });
  socket.on('chess_leave', (id) => {
    const g = chess[id]; if (!g || (g.w !== me && g.b !== me)) return;
    to(g.w === me ? g.b : g.w, 'chess_closed', { id }); delete chess[id];
  });

  // ---------- Coins & leaderboard ----------
  const GAME_COINS = { xo: 8, arena: 25, doom: 25, gd: 40, snake: 10, g2048: 30, ludo: 20 }; // coins for a win in the games that run on the player's device
  const lastGR = {};
  socket.on('game_reward', ({ game, win, score, lvl } = {}) => {
    if (!me || !hasOwn(GAME_COINS, game)) return;
    score = Math.max(0, Math.min(Math.floor(+score) || 0, 1e6));
    const p = prof(me), k = me + '|' + game, now = Date.now(), day = new Date().toISOString().slice(0, 10);
    let c = 0;
    if (now - (lastGR[k] || 0) >= 15000) { // at most one reward per game every 15 seconds
      lastGR[k] = now;
      if (p.day !== day) { p.day = day; p.dayCoins = 0; }
      c = win ? GAME_COINS[game] + (game === 'gd' ? Math.min(9, Math.max(0, Math.floor(+lvl) || 0)) * 10 : 0) : Math.min(10, 2 + Math.floor(score / 10));
      c = Math.max(0, Math.min(c, 400 - (p.dayCoins || 0))); // daily limit: 400 coins from these games
      p.dayCoins = (p.dayCoins || 0) + c;
    }
    reward(me, game, c, !!win && c > 0, score);
  });
  socket.on('get_leaderboard', (by) => {
    if (!me) return;
    by = ['coins', 'wins', 'gd', 'doom', 'arena'].includes(by) ? by : 'coins';
    const val = p => (by === 'coins' ? p.coins : by === 'wins' ? p.wins : p.best[by]) || 0;
    const all = db.users.map(n => { const p = prof(n); return { name: n, emoji: p.emoji, av: p.avatar ? (p.avV || 1) : 0, v: val(p) }; })
      .filter(r => r.v > 0).sort((a, b) => b.v - a.v);
    const i = all.findIndex(r => r.name === me);
    socket.emit('leaderboard', { by, rows: all.slice(0, 20), mine: { v: val(prof(me)), rank: i < 0 ? 0 : i + 1 } });
  });

  // ---------- NEW: Shop (ranks, name colours, badges - paid with coins) ----------
  const shopState = () => { const p = prof(me); return { coins: p.coins, own: p.own, rank: p.rank, color: p.color, badge: p.badge, items: SHOP }; };
  socket.on('shop_get', () => { if (me) socket.emit('shop_state', shopState()); });
  socket.on('shop_buy', (id) => {
    if (!me) return;
    id = String(id || '');
    if (!hasOwn(SHOP, id)) return socket.emit('toast', 'That item does not exist');
    const it = SHOP[id], p = prof(me);
    if (p.own.includes(id)) return socket.emit('toast', 'You already own that');
    if (!isReal(me) && p.coins < it.price) return socket.emit('toast', '🪙 You need ' + (it.price - p.coins) + ' more coins');
    if (!isReal(me)) p.coins -= it.price; p.own.push(id); p[it.cat] = id; // a new purchase is equipped right away
    pMeta(); stats();
    socket.emit('coins', { coins: p.coins, gained: 0 });
    socket.emit('shop_state', shopState());
    socket.emit('toast', '🛒 Bought ' + it.icon + ' ' + it.name);
  });
  socket.on('shop_equip', ({ cat, id } = {}) => { // id '' = take it off
    if (!me || !SHOP_SLOTS.includes(cat)) return;
    const p = prof(me); id = String(id || '');
    if (id && !(hasOwn(SHOP, id) && SHOP[id].cat === cat && p.own.includes(id))) return;
    p[cat] = id; pMeta(); stats();
    socket.emit('shop_state', shopState());
  });

  // ---------- Draw Battle (added) ----------
  function dbEnd(B, why) {
    if (!B) return;
    clearTimeout(B.t1); clearTimeout(B.t2);
    delete battles[B.id]; if (battleOf[B.a] === B.id) delete battleOf[B.a]; if (battleOf[B.b] === B.id) delete battleOf[B.b];
    if (why) { to(B.a, 'db_cancel', why); to(B.b, 'db_cancel', why); }
  }
  async function dbJudgeSolo(B) {
    to(B.a, 'db_judging', {});
    const D = B.subs[B.a] || {};
    if (!D.img) return dbEnd(B, 'You did not submit a drawing.');
    const r = await judgeSolo(B.prompt, D);
    const p = prof(B.a), day = new Date().toISOString().slice(0, 10);
    if (p.dbDay !== day) { p.dbDay = day; p.dbCoins = 0; }
    let c = Math.round(r.score / 2); if (!r.ai) c = Math.min(c, 15); // solo pays half of a battle; basic scores pay less
    c = Math.max(0, Math.min(c, DRAW_DAILY - (p.dbCoins || 0))); p.dbCoins = (p.dbCoins || 0) + c;
    const win = r.score >= 60;
    reward(B.a, 'draw', c, win, r.score);
    to(B.a, 'db_result', { prompt: B.prompt, solo: true, players: [{ name: B.a, img: D.img, score: r.score, coins: c }], winner: win ? B.a : '', comment: r.comment, ai: r.ai });
    dbEnd(B);
  }
  async function dbJudge(B) {
    if (!B || B.state !== 'drawing') return;
    B.state = 'judging'; clearTimeout(B.t2);
    if (B.solo) return dbJudgeSolo(B);
    to(B.a, 'db_judging', {}); to(B.b, 'db_judging', {});
    const A = B.subs[B.a] || {}, Bb = B.subs[B.b] || {};
    if (!A.img && !Bb.img) return dbEnd(B, 'Nobody submitted a drawing.');
    const r = await judgeDrawings(B.prompt, A, Bb);
    const day = new Date().toISOString().slice(0, 10), out = {};
    [[B.a, r.a, A], [B.b, r.b, Bb]].forEach(([n, sc, d]) => {
      const p = prof(n); if (p.dbDay !== day) { p.dbDay = day; p.dbCoins = 0; }
      let c = d.img ? sc : 0; if (!r.ai) c = Math.min(c, 30); // basic scores pay less
      c = Math.max(0, Math.min(c, DRAW_DAILY - (p.dbCoins || 0))); p.dbCoins = (p.dbCoins || 0) + c;
      out[n] = { name: n, img: d.img || '', score: sc, coins: c };
    });
    const win = r.a === r.b ? '' : (r.a > r.b ? B.a : B.b);
    [B.a, B.b].forEach(n => reward(n, 'draw', out[n].coins, win === n, out[n].score));
    pMeta();
    const res = { prompt: B.prompt, players: [out[B.a], out[B.b]], winner: win, comment: r.comment, ai: r.ai };
    to(B.a, 'db_result', res); to(B.b, 'db_result', res);
    dbEnd(B);
  }
  socket.on('db_invite', (name) => {
    const f = findUser(name);
    if (!me || !f || f === me || !arr(db.friends, me).includes(f) || !online[f]) return socket.emit('toast', 'Your friend must be online');
    if (battleOf[me] || battleOf[f]) return socket.emit('toast', 'Someone is already in a Draw Battle');
    const B = { id: uid(), a: me, b: f, state: 'invited', subs: {} };
    battles[B.id] = B; battleOf[me] = B.id; battleOf[f] = B.id;
    B.t1 = setTimeout(() => { if (B.state === 'invited') dbEnd(B, 'Invite expired.'); }, 30000);
    to(f, 'db_invite', { id: B.id, from: me }); socket.emit('toast', '⚔️ Challenge sent to ' + f);
  });
  socket.on('db_solo', () => { // SINGLE PLAYER: the server gives you a thing to draw, the AI rates it
    if (!me) return;
    if (battleOf[me]) return socket.emit('toast', 'You are already in a Draw Battle');
    const B = { id: uid(), a: me, b: null, solo: true, state: 'drawing', subs: {}, prompt: pickPrompt(me) };
    battles[B.id] = B; battleOf[me] = B.id;
    socket.emit('db_start', { id: B.id, prompt: B.prompt, secs: 60, a: me, b: null, solo: true });
    B.t2 = setTimeout(() => dbJudge(B), 75000);
  });
  socket.on('db_decline', (id) => { const B = battles[id]; if (B && B.b === me && B.state === 'invited') dbEnd(B, me + ' declined.'); });
  socket.on('db_accept', async (id) => {
    const B = battles[id];
    if (!B || B.b !== me || B.state !== 'invited') return;
    B.state = 'starting'; clearTimeout(B.t1);
    B.prompt = pickPrompt(B.a, B.b);
    if (!battles[id]) return;
    B.state = 'drawing';
    const d = { id, prompt: B.prompt, secs: 60, a: B.a, b: B.b };
    to(B.a, 'db_start', d); to(B.b, 'db_start', d);
    B.t2 = setTimeout(() => dbJudge(B), 75000);
  });
  socket.on('db_submit', (d) => {
    const B = d && battles[d.id];
    if (!me || !B || B.state !== 'drawing' || (B.a !== me && B.b !== me) || B.subs[me]) return;
    if (typeof d.img !== 'string' || d.img.length > 150000 || !/^data:image\/jpeg;base64,[A-Za-z0-9+/=]+$/.test(d.img)) return;
    B.subs[me] = { img: d.img, cov: Math.max(0, Math.min(1, +d.cov || 0)), strokes: Math.max(0, Math.min(500, +d.strokes || 0)) };
    if (B.solo || (B.subs[B.a] && B.subs[B.b])) dbJudge(B);
  });
  socket.on('db_leave', () => { const B = battles[battleOf[me]]; if (B && B.state !== 'judging') dbEnd(B, B.solo ? '' : me + ' left the battle.'); });

  // ---------- Whiteboard + Pictionary + look & feel sync (added) ----------
  let wbRoom = null;
  const wbCh = () => 'wb:' + wbRoom;
  function wbLeave() {
    if (!wbRoom) return;
    const R = wbRooms[wbRoom];
    if (R && R.game && R.game.drawer === me) { R.game = null; io.to(wbCh()).emit('pic_ended'); io.to(wbCh()).emit('pic_msg', 'Drawer left. Game ended.'); }
    socket.leave(wbCh());
    const mem = io.sockets.adapter.rooms.get(wbCh());
    if (!mem || !mem.size) delete wbRooms[wbRoom];
    wbRoom = null;
  }
  socket.on('wb_join', (name) => {
    if (!me) return;
    wbLeave();
    wbRoom = String(name || 'lobby').replace(/[^\w -]/g, '').slice(0, 30) || 'lobby';
    socket.join(wbCh());
    const R = wbRooms[wbRoom] || (wbRooms[wbRoom] = { h: [], game: null });
    socket.emit('wb_history', R.h);
    if (R.game) socket.emit('pic_started', { drawer: R.game.drawer });
  });
  socket.on('wb_leave', wbLeave);
  socket.on('wb_stroke', (d) => {
    const R = wbRoom && wbRooms[wbRoom];
    if (!me || !R || !d) return;
    if (R.game && R.game.drawer !== me) return;
    if (![d.x0, d.y0, d.x1, d.y1, d.s].every(wbNum)) return;
    const s = { x0: d.x0, y0: d.y0, x1: d.x1, y1: d.y1, s: Math.min(60, Math.max(1, d.s)), c: /^#[0-9a-fA-F]{6}$/.test(String(d.c)) ? d.c : '#ffffff' };
    R.h.push(s); if (R.h.length > 6000) R.h.shift();
    socket.to(wbCh()).emit('wb_stroke', s);
  });
  socket.on('wb_clear', () => {
    const R = wbRoom && wbRooms[wbRoom];
    if (!me || !R || (R.game && R.game.drawer !== me)) return;
    R.h = []; io.to(wbCh()).emit('wb_clear');
  });
  socket.on('pic_start', () => {
    const R = wbRoom && wbRooms[wbRoom];
    if (!me || !R) return;
    R.h = []; io.to(wbCh()).emit('wb_clear');
    R.game = { drawer: me, word: WB_WORDS[Math.floor(Math.random() * WB_WORDS.length)] };
    socket.emit('pic_word', R.game.word);
    io.to(wbCh()).emit('pic_started', { drawer: me });
  });
  socket.on('pic_guess', (g) => {
    const R = wbRoom && wbRooms[wbRoom];
    if (!me || !R || !R.game || R.game.drawer === me) return;
    g = String(g || '').trim().toLowerCase().slice(0, 40);
    if (!g || (!isAdmin(me) && isAbusive(g))) return;
    if (g === R.game.word) { io.to(wbCh()).emit('pic_msg', '🎉 ' + me + ' guessed it! The word was "' + R.game.word + '".'); R.game = null; io.to(wbCh()).emit('pic_ended'); }
    else io.to(wbCh()).emit('pic_msg', me + ': ' + g);
  });
  socket.on('pic_stop', () => {
    const R = wbRoom && wbRooms[wbRoom];
    if (me && R && R.game && R.game.drawer === me) { R.game = null; io.to(wbCh()).emit('pic_ended'); io.to(wbCh()).emit('pic_msg', 'Game stopped.'); }
  });
  socket.on('prefs_save', (d) => {
    if (!me || !d || typeof d !== 'object') return;
    const custom = typeof d.custom === 'string' && d.custom.length <= 150000 && /^data:image\/(jpeg|png|webp);base64,[A-Za-z0-9+/=]+$/.test(d.custom) ? d.custom : null;
    prof(me).prefs = {
      wall: String(d.wall || 'Default').slice(0, 30),
      accent: /^#[0-9a-fA-F]{6}$/.test(String(d.accent)) ? d.accent : null,
      glow: Math.min(40, Math.max(0, +d.glow || 0)), crt: !!d.crt, pixel: !!d.pixel, custom
    };
    pMeta();
  });

  socket.on('disconnect', () => {
    wbLeave();
    { const B = battles[battleOf[me]]; if (me && B && B.state !== 'judging') dbEnd(B, me + ' disconnected.'); }
    if (me && online[me] === socket.id) { dropMe(); pMeta(); stats(); }
  });
});

const PORT = process.env.PORT || 3000;
(async () => {
  await initStorage();
  seedAdmin();
  server.listen(PORT, () => console.log('Gemini OS server running on port ' + PORT));
})();
