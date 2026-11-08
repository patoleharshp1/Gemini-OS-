// HELLFRONT multiplayer: server runs the AI bots and combat, clients send position + fire.
const Sim = require('./doom-sim.js');
const okc = c => (typeof c === 'string' && /^#[0-9a-f]{6}$/i.test(c));
module.exports = io => {
  const nsp = io.of('/doom'), g = new Sim(); g.initBots();
  setInterval(() => {
    if (!Object.keys(g.players).length) return;
    g.tick(.05); nsp.emit('s', g.snap()); g.ev = [];
  }, 50);
  nsp.on('connection', s => {
    s.on('join', j => {
      const k = (j && j.skin) || {}, d = { skin: '#e0b090', shirt: '#3a6ad0', pants: '#2a2a60', hair: '#3a2210' };
      for (const c in d) if (okc(k[c])) d[c] = k[c];
      g.addPlayer(s.id, String((j && j.name) || 'Marine').slice(0, 16), d);
      s.emit('init', { id: s.id, me: g.players[s.id] });
    });
    s.on('p', p => {
      const u = g.players[s.id]; if (!u || u.dead || !p) return;
      const x = +p.x, y = +p.y, a = +p.a;
      if (isFinite(x) && isFinite(y) && isFinite(a) && g.free(x, y, .2)) { u.x = x; u.y = y; u.a = a; }
      u.wep = p.w === 1 ? 1 : 0;
    });
    s.on('fire', () => g.fire(s.id));
    s.on('disconnect', () => g.removePlayer(s.id));
  });
};
