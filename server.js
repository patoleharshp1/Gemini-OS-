const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);

// Enable large data transfers for MP3 uploads and base64 media
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));

const io = new Server(server, {
  maxHttpBufferSize: 5e7, // 50MB payload limit
  cors: { origin: "*", methods: ["GET", "POST"] }
});

// Map active users to socket IDs for direct calling
const activeUsers = {};
const onlineSongs = []; // Server-side shared MP3 library

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'index.html'));
});

io.on('connection', (socket) => {
  let registeredUser = null;

  // Register user for direct WebRTC calling
  socket.on('register_user', (username) => {
    registeredUser = username;
    activeUsers[username] = socket.id;
    io.emit('user_list', Object.keys(activeUsers));
    // Send existing online songs to newly connected client
    socket.emit('load_online_songs', onlineSongs);
  });

  // Chat Messaging
  socket.on('send_message', (data) => {
    io.emit('receive_message', data);
  });

  // Online MP3 Upload
  socket.on('upload_online_song', (songData) => {
    onlineSongs.push(songData);
    io.emit('new_online_song', songData);
  });

  // WebRTC Signaling for Phone / Video Call
  socket.on('call_user', (data) => {
    const targetSocketId = activeUsers[data.userToCall];
    if (targetSocketId) {
      io.to(targetSocketId).emit('incoming_call', {
        signal: data.signalData,
        from: data.from,
        isVideo: data.isVideo
      });
    } else {
      socket.emit('call_failed', { reason: 'User not online or username incorrect.' });
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
