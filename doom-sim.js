// HELLFRONT game logic: map, players, AI bots, hitscan combat. Used by browser (singleplayer) and Node (multiplayer).
(function (root) {
  const MAP = [
    "########################",
    "#......#...............#",
    "#......#.........##....#",
    "#..............#.......#",
    "#......#.......#..##...#",
    "###.####.......#.......#",
    "#......#...............#",
    "#......#######.#########",
    "#...............#......#",
    "#...##..........#..##..#",
    "#...##..........#..##..#",
    "#...............#......#",
    "######.####.#####......#",
    "#.....#........#.......#",
    "#.....#..####..#..###..#",
    "#.....#..#..#..........#",
    "#........#..#..#.......#",
    "#.....#........#.......#",
    "#.....#................#",
    "########################"
  ];
  const BT = { imp: { hp: 40, sp: 1.5 }, demon: { hp: 70, sp: 3 } };

  class Sim {
    constructor() { this.players = {}; this.bots = []; this.ev = []; this.maxBots = 6; }
    solid(x, y) { const r = MAP[Math.floor(y)]; return !r || r[Math.floor(x)] !== '.'; }
    free(x, y, r) { return !(this.solid(x - r, y - r) || this.solid(x + r, y - r) || this.solid(x - r, y + r) || this.solid(x + r, y + r)); }
    spawnPoint() {
      for (let i = 0; i < 300; i++) {
        const x = 1 + Math.random() * 22, y = 1 + Math.random() * 18;
        if (this.free(x, y, .35) && !Object.values(this.players).some(p => !p.dead && Math.hypot(p.x - x, p.y - y) < 6)) return [x, y];
      }
      return [1.5, 1.5];
    }
    addPlayer(id, name, skin) {
      const [x, y] = this.spawnPoint();
      this.players[id] = { id, name, skin, x, y, a: Math.random() * 6.28, hp: 100, frags: 0, deaths: 0, sh: 8, cd: 0, dead: 0, rt: 0, wep: 0 };
    }
    removePlayer(id) { delete this.players[id]; }
    initBots() { this.bots = []; for (let i = 0; i < this.maxBots; i++) { const b = { id: 'b' + i, type: i % 3 == 2 ? 'demon' : 'imp', cd: 1, w: 0, al: 0, rt: 0, a: 0 }; this.resetBot(b); this.bots.push(b); } }
    resetBot(b) { const [x, y] = this.spawnPoint(); b.x = x; b.y = y; b.hp = BT[b.type].hp; b.a = Math.random() * 6.28; }
    ray(x, y, a, max, skip) {
      const dx = Math.cos(a), dy = Math.sin(a), ps = Object.values(this.players);
      for (let d = 0; d < max; d += .05) {
        const px = x + dx * d, py = y + dy * d;
        if (this.solid(px, py)) return { d };
        for (const p of ps) if (p !== skip && !p.dead && (p.x - px) ** 2 + (p.y - py) ** 2 < .09) return { d, p };
        for (const b of this.bots) if (b !== skip && b.hp > 0 && (b.x - px) ** 2 + (b.y - py) ** 2 < .1) return { d, b };
      }
      return { d: max };
    }
    dmgP(p, n, by) {
      if (p.dead) return;
      p.hp -= n; this.ev.push({ t: 'h', id: p.id });
      if (p.hp <= 0) {
        p.dead = 1; p.deaths++; p.rt = 3;
        if (by && by.frags !== undefined) { by.frags++; by.sh = Math.min(24, by.sh + 2); }
        this.ev.push({ t: 'k', by: by ? by.id : null, v: p.id });
      }
    }
    fire(id) {
      const p = this.players[id]; if (!p || p.dead || p.cd > 0) return;
      const sg = p.wep === 1;
      if (sg) { if (p.sh <= 0) return; p.sh--; p.cd = .9; } else p.cd = .35;
      for (let i = 0; i < (sg ? 7 : 1); i++) {
        const r = this.ray(p.x, p.y, p.a + (Math.random() - .5) * (sg ? .18 : .02), 24, p), dmg = sg ? 9 : 14;
        if (r.p) this.dmgP(r.p, dmg, p);
        else if (r.b) {
          r.b.hp -= dmg; r.b.al = 4;
          if (r.b.hp <= 0) { r.b.rt = 4; p.frags++; p.sh = Math.min(24, p.sh + 2); p.hp = Math.min(100, p.hp + 10); this.ev.push({ t: 'k', by: p.id, v: r.b.id }); }
        }
      }
      this.ev.push({ t: 'f', id });
    }
    tick(dt) {
      const ps = Object.values(this.players);
      for (const p of ps) {
        p.cd = Math.max(0, p.cd - dt);
        if (p.dead && (p.rt -= dt) <= 0) { const [x, y] = this.spawnPoint(); p.x = x; p.y = y; p.hp = 100; p.dead = 0; p.sh = Math.max(p.sh, 8); }
      }
      for (const b of this.bots) {
        if (b.hp <= 0) { if ((b.rt -= dt) <= 0) this.resetBot(b); continue; }
        let t = null, td = 1e9;
        for (const p of ps) if (!p.dead) { const d = Math.hypot(p.x - b.x, p.y - b.y); if (d < td) { td = d; t = p; } }
        let los = false;
        if (t && td < 14) { const an = Math.atan2(t.y - b.y, t.x - b.x), r = this.ray(b.x, b.y, an, td + .3, b); if (r.p === t) { los = true; b.a = an; b.al = 3; } }
        b.al -= dt; b.cd -= dt;
        let sp = BT[b.type].sp;
        if (b.al > 0 && t) { if (!los) b.a = Math.atan2(t.y - b.y, t.x - b.x) + (Math.random() - .5) * .6; if (td < (b.type == 'imp' ? 5 : .8)) sp = 0; }
        else { sp *= .4; if ((b.w -= dt) <= 0) { b.a = Math.random() * 6.28; b.w = 1 + Math.random() * 2; } }
        const nx = b.x + Math.cos(b.a) * sp * dt, ny = b.y + Math.sin(b.a) * sp * dt;
        let blocked = false;
        if (this.free(nx, b.y, .3)) b.x = nx; else blocked = true;
        if (this.free(b.x, ny, .3)) b.y = ny; else blocked = true;
        if (blocked) { b.a += (Math.random() - .5) * 3; b.w = .3; }
        if (los && b.cd <= 0) {
          if (b.type == 'imp' && td < 12) { b.cd = 1.4; if (Math.random() < .65) this.dmgP(t, 6, null); }
          else if (b.type == 'demon' && td < 1.1) { b.cd = .8; this.dmgP(t, 12, null); }
        }
      }
    }
    snap() {
      const f = n => +n.toFixed(2);
      return {
        p: Object.values(this.players).map(p => ({ id: p.id, name: p.name, skin: p.skin, x: f(p.x), y: f(p.y), a: f(p.a), hp: p.hp, frags: p.frags, deaths: p.deaths, sh: p.sh, dead: p.dead, wep: p.wep })),
        b: this.bots.map(b => ({ id: b.id, type: b.type, x: f(b.x), y: f(b.y), hp: b.hp })),
        e: this.ev
      };
    }
  }
  Sim.MAP = MAP;
  if (typeof module !== 'undefined') module.exports = Sim; else root.DoomSim = Sim;
})(typeof window !== 'undefined' ? window : globalThis);
