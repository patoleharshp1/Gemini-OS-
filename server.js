const { WebSocketServer } = require('ws');

// Use port assigned by host environment (e.g., Render, Railway, Glitch) or 8080 locally
const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

console.log(`Web Relay Server running on port ${PORT}`);

// Keep track of connected phone devices
const clients = new Set();

wss.on('connection', (ws) => {
    clients.add(ws);
    console.log(`Phone connected. Total active phones: ${clients.size}`);

    // Send confirmation to the newly connected phone
    ws.send(JSON.stringify({ type: 'status', message: 'Connected to web' }));

    ws.on('message', (message) => {
        let parsedData;
        try {
            parsedData = JSON.parse(message);
        } catch (e) {
            parsedData = { text: message.toString() };
        }

        console.log(`Received message: "${parsedData.text}". Broadcasting to ${clients.size - 1} other phones...`);

        // Broadcast to EVERY connected phone EXCEPT the sender
        clients.forEach((client) => {
            if (client !== ws && client.readyState === 1) { // 1 = OPEN
                client.send(JSON.stringify({ type: 'chat', text: parsedData.text }));
            }
        });
    });

    ws.on('close', () => {
        clients.delete(ws);
        console.log(`Phone disconnected. Remaining phones: ${clients.size}`);
    });
});
