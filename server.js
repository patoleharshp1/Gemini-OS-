const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" }
});

app.use(express.static('public'));

// Active online users: { socketId: { username: string } }
const activeUsers = {};

io.on('connection', (socket) => {
  console.log('User connected:', socket.id);

  // Set initial default name
  activeUsers[socket.id] = { username: `User_${socket.id.substring(0, 4)}` };

  // Send initial state to the newly connected client
  socket.emit('update-user-list', activeUsers);
  
  // Broadcast updated list to everyone
  io.emit('update-user-list', activeUsers);

  // Set user name
  socket.on('set-username', (name) => {
    if (name && name.trim()) {
      activeUsers[socket.id].username = name.trim();
      io.emit('update-user-list', activeUsers);
    }
  });

  // WebRTC Video Call Signaling
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
