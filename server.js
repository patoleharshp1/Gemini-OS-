const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);

app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));

const io = new Server(server, {
  maxHttpBufferSize: 5e7,
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
    registeredUser = username.trim();
    activeUsers[registeredUser] = socket.id;
    io.emit('user_list', Object.keys(activeUsers));
    
    socket.emit('load_all_messages', chatMessages);
    socket.emit('load_cloud_files', cloudStorageFiles);
  });

  // Chat Messaging
  socket.on('send_message', (data) => {
    data.id = Date.now().toString() + Math.random().toString(36).substr(2, 4);
    chatMessages.push(data);
    io.emit('receive_message', data);
  });

  socket.on('delete_message', (msgId) => {
    chatMessages = chatMessages.filter(m => m.id !== msgId);
    io.emit('message_deleted', msgId);
  });

  socket.on('typing_status', (data) => {
    socket.broadcast.emit('user_typing', data);
  });

  // Cloud Drive File Upload
  socket.on('upload_cloud_file', (fileData) => {
    fileData.id = Date.now().toString() + Math.random().toString(36).substr(2, 4);
    cloudStorageFiles.push(fileData);
    io.emit('new_cloud_file', fileData);
  });

  socket.on('delete_cloud_file', (fileId) => {
    cloudStorageFiles = cloudStorageFiles.filter(f => f.id !== fileId);
    io.emit('cloud_file_deleted', fileId);
  });

  // WEBRTC SIGNALING SYSTEM
  socket.on('call_user', (data) => {
    const targetSocketId = activeUsers[data.userToCall];
    if (targetSocketId) {
      io.to(targetSocketId).emit('incoming_call', {
        signalData: data.signalData,
        from: data.from,
        isVideo: data.isVideo
      });
    } else {
      socket.emit('call_failed', { reason: 'User "' + data.userToCall + '" is offline or not found.' });
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
  console.log(`Server running on port ${PORT}`);
});
