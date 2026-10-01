const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);

// Enable large body payload handling for high-res camera photos & audio files
app.use(express.json({ limit: '100mb' }));
app.use(express.urlencoded({ limit: '100mb', extended: true }));

// Increase socket buffer size to 100MB for binary/image transmission
const io = new Server(server, {
  maxHttpBufferSize: 1e8, // 100 MB max packet size
  cors: { origin: "*", methods: ["GET", "POST"] }
});

const activeUsers = {};
let chatMessages = [];
let cloudStorageFiles = [];

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

io.on('connection', (socket) => {
  let registeredUser = null;

  socket.on('register_user', (username) => {
    registeredUser = username ? username.trim() : 'User';
    activeUsers[registeredUser] = socket.id;
    io.emit('user_list', Object.keys(activeUsers));
    
    socket.emit('load_all_messages', chatMessages);
    socket.emit('load_cloud_files', cloudStorageFiles);
  });

  // Chat Messaging System
  socket.on('send_message', (data) => {
    data.id = Date.now().toString() + Math.random().toString(36).substring(2, 6);
    chatMessages.push(data);
    io.emit('receive_message', data);
  });

  socket.on('delete_message', (msgId) => {
    chatMessages = chatMessages.filter(m => m.id !== msgId);
    io.emit('message_deleted', msgId);
  });

  // Cloud Drive File Upload (With Account Email Sync)
  socket.on('upload_cloud_file', (fileData) => {
    fileData.id = Date.now().toString() + Math.random().toString(36).substring(2, 6);
    fileData.email = fileData.email || "patole.harshp1@gmail.com";
    cloudStorageFiles.push(fileData);
    io.emit('load_cloud_files', cloudStorageFiles);
  });

  socket.on('delete_cloud_file', (fileId) => {
    cloudStorageFiles = cloudStorageFiles.filter(f => f.id !== fileId);
    io.emit('load_cloud_files', cloudStorageFiles);
  });

  // WEBRTC CALL SIGNALING SYSTEM
  socket.on('call_user', (data) => {
    const targetSocketId = activeUsers[data.userToCall];
    if (targetSocketId) {
      io.to(targetSocketId).emit('incoming_call', {
        signalData: data.signalData,
        from: data.from,
        isVideo: data.isVideo
      });
    } else {
      socket.emit('call_failed', { reason: 'User "' + data.userToCall + '" is offline or not registered.' });
    }
  });

  socket.on('answer_call', (data) => {
    const targetSocketId = activeUsers[data.to];
    if (targetSocketId) {
      io.to(targetSocketId).emit('call_accepted', {
        signal: data.signal,
        from: registeredUser
      });
    }
  });

  socket.on('send_candidate', (data) => {
    const targetSocketId = activeUsers[data.to];
    if (targetSocketId) {
      io.to(targetSocketId).emit('receive_candidate', {
        candidate: data.candidate,
        from: registeredUser
      });
    }
  });

  socket.on('end_call', (data) => {
    const targetSocketId = activeUsers[data ? data.to : null];
    if (targetSocketId) {
      io.to(targetSocketId).emit('call_ended');
    } else {
      socket.broadcast.emit('call_ended');
    }
  });

  socket.on('disconnect', () => {
    if (registeredUser) {
      delete activeUsers[registeredUser];
      io.emit('user_list', Object.keys(activeUsers));
    }
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Gemini OS Server running on port ${PORT}`);
});
