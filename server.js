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
let chatMessages = []; // Server memory for saved messages
const onlineSongs = [];

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

io.on('connection', (socket) => {
  let registeredUser = null;

  socket.on('register_user', (username) => {
    registeredUser = username;
    activeUsers[username] = socket.id;
    io.emit('user_list', Object.keys(activeUsers));
    
    // Load existing messages and songs on connect
    socket.emit('load_all_messages', chatMessages);
    socket.emit('load_online_songs', onlineSongs);
  });

  // Chat Messaging
  socket.on('send_message', (data) => {
    data.id = Date.now().toString() + Math.random().toString(36).substr(2, 4);
    chatMessages.push(data);
    io.emit('receive_message', data);
  });

  // Delete Message
  socket.on('delete_message', (msgId) => {
    chatMessages = chatMessages.filter(m => m.id !== msgId);
    io.emit('message_deleted', msgId);
  });

  // Online MP3 Upload
  socket.on('upload_online_song', (songData) => {
    onlineSongs.push(songData);
    io.emit('new_online_song', songData);
  });

  // WebRTC Calling Signaling
  socket.on('call_user', (data) => {
    const targetSocketId = activeUsers[data.userToCall];
    if (targetSocketId) {
      io.to(targetSocketId).emit('incoming_call', {
        signalData: data.signalData,
        from: data.from,
        isVideo: data.isVideo
      });
    } else {
      socket.emit('call_failed', { reason: 'User not online or username invalid.' });
    }
  });

  socket.on('answer_call', (data) => {
    const targetSocketId = activeUsers[data.to];
    if (targetSocketId) {
      io.to(targetSocketId).emit('call_accepted', data.signal);
    }
  });

  socket.on('end_call', (data) => {
    const targetSocketId = activeUsers[data.to];
    if (targetSocketId) {
      io.to(targetSocketId).emit('call_ended');
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
