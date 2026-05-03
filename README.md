# 📍 Live Location Tracker

A real-time, high-throughput live location tracking system built with **Node.js, Apache Kafka, Socket.IO, OIDC Auth0, and Leaflet.js**. 

This system allows authenticated users to share their live location and see other connected users moving on a map in real time.

## Demo Video
[Live location tracker demo](https://youtu.be/k0k8iqBzlZw)

## Tech Stack
- **Frontend:** HTML/CSS/JavaScript, Leaflet.js (Maps)
- **Backend:** Node.js, Express, Socket.IO (WebSockets)
- **Message Broker:** Apache Kafka & KRaft (Dockerized)
- **Authentication:** Auth0 (OIDC / OAuth 2.0) via `express-openid-connect`
- **Persistence:** File-based simulated database via separate Kafka Consumer Group

---

## Prerequisites
Before running this project, ensure you have the following installed:
1. [Node.js](https://nodejs.org/en/) (v18 or higher)
2. [Docker Desktop](https://www.docker.com/products/docker-desktop) (Must be running for Kafka)
3. [pnpm](https://pnpm.io/) package manager (`npm install -g pnpm`)
4. A free account on [Auth0](https://auth0.com/)

---

## Auth0 (OIDC) Setup
To enable authentication, you need to configure an Auth0 Application:
1. Go to the Auth0 Dashboard and create a **Regular Web Application**.
2. In the application settings, scroll down to **Application URIs** and configure:
   - **Allowed Callback URLs:** `http://localhost:8000/callback`
   - **Allowed Logout URLs:** `http://localhost:8000`
   - **Allowed Web Origins:** `http://localhost:8000`
3. Save changes. You will need your Domain, Client ID, and a generated Secret for the next step.

---

## Setup & Running Locally

### 1. Clone & Install Dependencies
```bash
# Clone the repository
git clone <your-repo-link>
cd kafka-learning

# Install dependencies using pnpm
pnpm install
```

### 2. Environment Variables
Create a `.env` file in the root directory and add the following keys. Replace the Auth0 values with the credentials from your Auth0 Dashboard.
```bash
cp .env.example .env
```
```env
PORT=8000

# Auth0 / OIDC Configuration
SECRET='generate_a_random_32_character_string_for_session_encryption'
BASE_URL='http://localhost:8000'
CLIENT_ID='your_auth0_client_id_here'
ISSUER_BASE_URL='https://your-tenant-name.us.auth0.com'
```

### 3. Start Apache Kafka (Docker)
Ensure Docker Desktop is open, then start the Kafka broker and Kafka UI:
```bash
docker-compose up -d
```
*(Optional: You can view the Kafka UI by visiting `http://localhost:8080` in your browser).*

### 4. Initialize Kafka Topics
Run the admin script to create the necessary `location-updates` topic inside Kafka:
```bash
node kafka-admin.js
```

### 5. Start the Application Servers
You need to run two separate processes. Open **two different terminals**:

**Terminal 1: Start the Web Server (Socket.IO & Frontend)**
```bash
node index.js
```

**Terminal 2: Start the Database Processor**
```bash
node database-processor.js
```

### 6. View the App
Open your browser and navigate to: **`http://localhost:8000`**
1. Click **Login** to authenticate via Auth0.
2. Accept the browser's location permission prompt.
3. Open a second browser or incognito window, log in with a different account, and watch the markers update in real time!

---

## 🔄 Architectural Flow & Tradeoffs

### 📡 Socket & Kafka Event Flow
1. **User Auth:** User logs in via Auth0. The authenticated profile is injected into the Socket.IO handshake.
2. **Client Emit:** The browser reads the user's GPS coordinates every 5 seconds and emits `client:location:update` to the Socket Server.
3. **Kafka Producer:** The server catches the socket event and acts as a producer, publishing the data to the `location-updates` Kafka topic, keyed by the Auth0 User ID.
4. **Kafka Consumer (Broadcast):** A consumer inside `index.js` listens to the Kafka topic and broadcasts `server:location:update` to all connected clients to update their map UI.

### 💾 Database Write Strategy (Why Kafka?)
Directly writing to a database on every rapid WebSocket event causes thread blocking, connection pool exhaustion, and server crashes during high traffic (e.g., thousands of delivery riders). 

**Our Solution:** 
The web server **only** interacts with Kafka. A standalone Node process (`database-processor.js`) runs a persistent Kafka Consumer Group. It independently consumes the location stream at its own safe pace, batching and appending records to `location_history.jsonl`. This completely decouples real-time broadcasting from heavy database I/O, guaranteeing high throughput and system stability.
```