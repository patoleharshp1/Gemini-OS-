self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', e => e.waitUntil(clients.claim()));

self.addEventListener('push', e => {
  let d = {}; try { d = e.data.json(); } catch (x) {}
  e.waitUntil((async () => {
    const wins = await clients.matchAll({ type: 'window', includeUncontrolled: true });
    if (wins.some(w => w.visibilityState === 'visible')) return; // app is open: it shows its own alert
    if (d.type === 'call') {
      await self.registration.showNotification('📞 ' + d.from, {
        body: (d.video ? 'Video' : 'Voice') + ' call — incoming',
        tag: 'call-' + d.room, renotify: true, requireInteraction: true,
        vibrate: [400, 200, 400, 200, 400], data: d,
        actions: [{ action: 'answer', title: '✅ Pick up' }, { action: 'dismiss', title: '❌ Dismiss' }]
      });
    } else {
      await self.registration.showNotification(d.title || 'New message', {
        body: d.body || '', tag: 'msg-' + d.from, renotify: true, vibrate: [120], data: d
      });
    }
  })());
});

self.addEventListener('notificationclick', e => {
  const d = e.notification.data || {};
  e.notification.close();
  e.waitUntil((async () => {
    const wins = await clients.matchAll({ type: 'window', includeUncontrolled: true });
    const w = wins[0];
    if (d.type === 'call' && e.action === 'dismiss') { if (w) w.postMessage({ type: 'decline', room: d.room }); return; }
    const msg = d.type === 'call' ? { type: 'answer', room: d.room, video: !!d.video, from: d.from } : { type: 'chat', from: d.from };
    if (w) { w.postMessage(msg); return w.focus(); }
    const q = d.type === 'call'
      ? 'answer=' + encodeURIComponent(d.room) + '&video=' + (d.video ? 1 : 0) + '&from=' + encodeURIComponent(d.from)
      : 'chat=' + encodeURIComponent(d.from);
    return clients.openWindow('/?' + q);
  })());
});
