const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: "*", // Adjust for specific production domains if necessary
    methods: ["GET", "POST"]
  }
});

// Serve frontend static files
app.use(express.static(path.join(__dirname, 'public')));

// Store active socket connections / real-time messaging events
io.on('connection', (socket) => {
  console.log('User connected:', socket.id);

  // Handle incoming message
  socket.on('send_message', (data) => {
    // Broadcast the message to all connected clients
    io.emit('receive_message', {
      user: data.user || 'Anonymous',
      message: data.message,
      timestamp: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
    });
  });

  socket.on('disconnect', () => {
    console.log('User disconnected:', socket.id);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
