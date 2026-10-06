const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const fs = require('fs');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 5e7, cors: { origin: '*' } });

// ---------- JSON database (survives restarts) ----------
const DB_FILE = path.join(__dirname, 'db.json');
let db = { totalVisits: 0, visitors: [], users: [], friends: {}, requests: {}, global: [], dms: {}, files: [] };
try { db = { ...db, ...JSON.parse(fs.readFileSync(DB_FILE, 'utf8')) }; } catch (e) {}
let saveTimer;
const save = () => {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => fs.writeFile(DB_FILE, JSON.stringify(db), () => {}), 500);
};

const online = {}; // username -> socket.id
const clean = n => String(n || '').replace(/[|<>]/g, '').trim().slice(0, 24);
const findUser = n => db.users.find(u => u.toLowerCase() === String(n || '').trim().toLowerCase());
const key = (a, b) => [a, b].sort().join('|');
const uid = () => Date.now().toString() + Math.random().toString(36).slice(2, 6);
const arr = (o, k) => (o[k] = o[k] || []);
const to = (name, ev, data) => { if (online[name]) io.to(online[name]).emit(ev, data); };

function stats() {
  io.emit('stats', {
    online: Object.keys(online).length,
    totalVisits: db.totalVisits,
    uniqueVisitors: db.visitors.length
  });
  io.emit('user_list', Object.keys(online));
}
function social(name) {
  to(name, 'social', { friends: arr(db.friends, name), requests: arr(db.requests, name) });
}
function dmsFor(name) {
  const out = {};
  Object.keys(db.dms).forEach(k => {
    const [a, b] = k.split('|');
    if (a === name) out[b] = db.dms[k];
    else if (b === name) out[a] = db.dms[k];
  });
  return out;
}

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));

io.on('connection', (socket) => {
  let me = null;
  let counted = false;

  socket.on('register_user', ({ name, clientId }) => {
    name = clean(name);
    if (!name) return;

    // Name already used by another live connection
    if (online[name] && online[name] !== socket.id && io.sockets.sockets.get(online[name])) {
      return socket.emit('name_taken', name);
    }

    const wasOnline = !!online[name];
    if (me && me !== name && online[me] === socket.id) delete online[me];
    me = name;
    online[me] = socket.id;

    if (!db.users.includes(me)) { db.users.push(me); save(); }
    if (!counted) {
      counted = true;
      db.totalVisits++;
      if (clientId && !db.visitors.includes(clientId)) db.visitors.push(clientId);
      save();
    }
    if (!wasOnline) arr(db.friends, me).forEach(f => to(f, 'toast', '🟢 ' + me + ' is online'));

    socket.emit('init', { global: db.global, dms: dmsFor(me), files: db.files });
    social(me);
    stats();
  });

  // ---------- Messaging (global + private) ----------
  socket.on('send_message', (data) => {
    if (!me || !data) return;
    data.user = me;
    data.id = uid();
    if (data.to) {
      arr(db.dms, key(me, data.to)).push(data);
      to(me, 'receive_message', data);
      if (data.to !== me) to(data.to, 'receive_message', data);
    } else {
      db.global.push(data);
      if (db.global.length > 300) db.global.shift();
      io.emit('receive_message', data);
    }
    save();
  });

  socket.on('delete_message', ({ id, chat }) => {
    if (!me) return;
    const list = chat === 'global' ? db.global : db.dms[key(me, chat)];
    if (!list) return;
    const i = list.findIndex(m => m.id === id && m.user === me);
    if (i < 0) return;
    list.splice(i, 1);
    save();
    if (chat === 'global') io.emit('message_deleted', { id });
    else { to(me, 'message_deleted', { id }); to(chat, 'message_deleted', { id }); }
  });

  socket.on('typing_status', (d) => {
    if (!me) return;
    if (d.to) to(d.to, 'user_typing', { user: me, to: d.to, isTyping: d.isTyping });
    else socket.broadcast.emit('user_typing', { user: me, isTyping: d.isTyping });
  });

  // ---------- Friends ----------
  function acceptFriend(a, b) {
    db.requests[a] = arr(db.requests, a).filter(x => x !== b);
    db.requests[b] = arr(db.requests, b).filter(x => x !== a);
    if (!arr(db.friends, a).includes(b)) db.friends[a].push(b);
    if (!arr(db.friends, b).includes(a)) db.friends[b].push(a);
    save(); social(a); social(b);
    to(a, 'toast', '🤝 You are now friends with ' + b);
    to(b, 'toast', '🤝 ' + a + ' accepted your friend request');
  }

  socket.on('friend_request', (input) => {
    if (!me) return;
    const target = findUser(input);
    if (!target) return socket.emit('toast', 'User "' + String(input).trim() + '" not found');
    if (target === me) return socket.emit('toast', "You can't add yourself");
    if (arr(db.friends, me).includes(target)) return socket.emit('toast', 'Already friends with ' + target);
    if (arr(db.requests, me).includes(target)) return acceptFriend(me, target); // they asked first
    if (arr(db.requests, target).includes(me)) return socket.emit('toast', 'Request already sent');
    db.requests[target].push(me);
    save(); social(target);
    to(target, 'toast', '👋 ' + me + ' sent you a friend request');
    socket.emit('toast', 'Friend request sent to ' + target);
  });
  socket.on('friend_accept', (from) => { if (me && arr(db.requests, me).includes(from)) acceptFriend(me, from); });
  socket.on('friend_decline', (from) => {
    if (!me) return;
    db.requests[me] = arr(db.requests, me).filter(x => x !== from);
    save(); social(me);
  });
  socket.on('friend_remove', (other) => {
    if (!me) return;
    db.friends[me] = arr(db.friends, me).filter(x => x !== other);
    db.friends[other] = arr(db.friends, other).filter(x => x !== me);
    save(); social(me); social(other);
  });

  // ---------- Cloud drive ----------
  socket.on('upload_cloud_file', (f) => {
    if (!me || !f) return;
    f.id = uid(); f.uploader = me;
    db.files.push(f); save();
    io.emit('new_cloud_file', f);
  });
  socket.on('delete_cloud_file', (id) => {
    db.files = db.files.filter(f => f.id !== id); save();
    io.emit('cloud_file_deleted', id);
  });

  // ---------- WebRTC signaling ----------
  socket.on('call_user', (d) => {
    if (!me) return;
    if (online[d.userToCall]) to(d.userToCall, 'incoming_call', { signalData: d.signalData, from: me, isVideo: d.isVideo });
    else socket.emit('call_failed', { reason: 'User "' + d.userToCall + '" is offline.' });
  });
  socket.on('answer_call', (d) => to(d.to, 'call_accepted', { signal: d.signal, from: me }));
  socket.on('send_candidate', (d) => to(d.to, 'receive_candidate', { candidate: d.candidate, from: me }));
  socket.on('end_call', (d) => { if (d && d.to) to(d.to, 'call_ended'); });

  socket.on('disconnect', () => {
    if (me && online[me] === socket.id) { delete online[me]; stats(); }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('Gemini OS server running on port ' + PORT));
