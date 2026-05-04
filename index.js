import 'dotenv/config';
import http from 'node:http';
import path from 'node:path';
import express from 'express';
import { Server } from 'socket.io';
import { auth } from 'express-openid-connect';
import { kafkaClient } from './kafka-client-local.js';

async function main() {
  const PORT = process.env.PORT || 8000;
  const app = express();
  const server = http.createServer(app);
  const io = new Server(server);

  // ==========================================
  // OIDC / OAuth 2.0 Auth Setup (Auth0)
  // ==========================================
  const config = {
    authRequired: false, // Let frontend handle unprotected vs protected state
    auth0Logout: true,
    secret: process.env.SECRET,
    baseURL: process.env.BASE_URL || `http://localhost:${PORT}`,
    clientID: process.env.CLIENT_ID,
    issuerBaseURL: process.env.ISSUER_BASE_URL
  };

  app.use(auth(config));

  // Expose User Profile to Frontend
  app.get('/api/user', (req, res) => {
    if (req.oidc.isAuthenticated()) {
      res.json({ authenticated: true, user: req.oidc.user });
    } else {
      res.json({ authenticated: false });
    }
  });


  app.use(express.static(path.resolve('./public')));



server.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 Server running and bound to 0.0.0.0 on port ${PORT}`);
});

  // ==========================================
  // Kafka Producer & Consumer Setup

    const kafkaProducer = kafkaClient.producer();
  const kafkaConsumer = kafkaClient.consumer({
    groupId: `socket-server-${Date.now()}`, 
  });

  try {
  await kafkaProducer.connect();

  await kafkaConsumer.connect();

  await kafkaConsumer.subscribe({
    topics: ['location-updates'],
    fromBeginning: false,
  });

  // Flow: Read from Kafka -> Broadcast to WebSockets
  kafkaConsumer.run({
    eachMessage: async ({ message }) => {
      const data = JSON.parse(message.value.toString());
      io.emit('server:location:update', data);
    },
  });
  } catch (error) {
  console.error("❌ KAFKA CONNECTION FAILED:", error);
}

  // Socket.IO Setup with Authentication
 
  io.use((socket, next) => {
    const user = socket.handshake.auth.user;
    if (!user || !user.sub) {
      return next(new Error("Authentication error: Missing identity."));
    }
    socket.user = user;
    next();
  });

  io.on('connection', (socket) => {
    console.log(`[Socket:${socket.id}]: User ${socket.user.name} connected.`);

    socket.on('client:location:update', async (locationData) => {
      const { latitude, longitude } = locationData;
      
      // Basic Validation
      if (typeof latitude !== 'number' || typeof longitude !== 'number') return;

      // Flow: Receive Socket Event -> Publish to Kafka
      await kafkaProducer.send({
        topic: 'location-updates',
        messages:[{
          key: socket.user.sub, // Group by Auth0 User ID
          value: JSON.stringify({ 
            id: socket.user.sub, 
            name: socket.user.name || socket.user.nickname, 
            latitude, 
            longitude 
          }),
        }],
      });
    });

    socket.on('disconnect', async () => {
      console.log(`[Socket:${socket.id}]: User ${socket.user.name} disconnected.`);
      // Emit Tombstone/Disconnect event to gracefully clean up map markers
      await kafkaProducer.send({
        topic: 'location-updates',
        messages:[{
          key: socket.user.sub,
          value: JSON.stringify({ id: socket.user.sub, disconnect: true }),
        }],
      });
    });
  });

}

main();