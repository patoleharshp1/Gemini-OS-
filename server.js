const express = require('express');
const http = require('http');
const path = require('path');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" }
});

// Serve static files from root directory or 'public'
app.use(express.static(__dirname));
app.use(express.static(path.join(__dirname, 'public')));

// Root route handler to guarantee index.html is served
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'), (err) => {
    if (err) {
      res.sendFile(path.join(__dirname, 'public', 'index.html'));
    }
  });
});

// Track online users: { socketId: { username: string } }
const activeUsers = {};

io.on('connection', (socket) => {
  console.log('User connected:', socket.id);

  // Set default initial username
  activeUsers[socket.id] = { username: `User_${socket.id.substring(0, 4)}` };

  // Broadcast updated online users list
  io.emit('update-user-list', activeUsers);

  // Handle custom username updates
  socket.on('set-username', (name) => {
    if (name && name.trim()) {
      activeUsers[socket.id].username = name.trim();
      io.emit('update-user-list', activeUsers);
    }
  });

  // WebRTC Signaling Events
  socket.on('call-user', (data) => {
    io.to(data.userToCall).emit('incoming-call', {
      signal: data.signalData,
      from: socket.id,
      name: activeUsers[socket.id]?.username || 'Someone'
    });
  });

  socket.on('answer-call', (data) => {
    io.to(data.to).emit('call-accepted', data.signal);
  });

  socket.on('ice-candidate', (data) => {
    io.to(data.to).emit('ice-candidate', { candidate: data.candidate, from: socket.id });
  });

  socket.on('disconnect', () => {
    delete activeUsers[socket.id];
    io.emit('update-user-list', activeUsers);
    console.log('User disconnected:', socket.id);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
