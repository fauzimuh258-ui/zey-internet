const express = require('express');
const http = require('http');
const crypto = require('crypto');
const path = require('path');
const fs = require('fs');
const Database = require('sqlite3').Database;
const fetch = require('node-fetch');

// Services
const ChatService = require('./services/chat');
const VideoService = require('./services/video');
const WebService = require('./services/web');

class ZeyNodeV2 {
    constructor() {
        this.nodeId = crypto.randomBytes(8).toString('hex');
        this.port = process.env.PORT || 3000;
        this.relayUrl = process.env.RELAY || 'ws://localhost:8080';
        this.peers = new Map();
        this.services = {};
        this.cache = null;
        
        // Init
        this.initDatabase();
        this.initServices();
        this.initServer();
        this.connectRelay();
        
        console.log(`
╔══════════════════════════════════════╗
║     🚀 ZEY INTERNET V2 - NODE       ║
║     Node ID: ${this.nodeId}         ║
║     Port: ${this.port}              ║
║     Mode: P2P + Relay Fallback      ║
╚══════════════════════════════════════╝
        `);
    }

    initDatabase() {
        const dbPath = path.join(__dirname, 'cache', `zey_${this.nodeId}.db`);
        fs.mkdirSync(path.dirname(dbPath), { recursive: true });
        
        this.db = new Database(dbPath);
        this.db.serialize(() => {
            // Chat messages cache
            this.db.run(`CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY,
                from_node TEXT,
                to_node TEXT,
                content TEXT,
                timestamp INTEGER,
                delivered INTEGER DEFAULT 0
            )`);
            
            // Video chunks cache
            this.db.run(`CREATE TABLE IF NOT EXISTS video_cache (
                id TEXT PRIMARY KEY,
                url TEXT,
                chunk_index INTEGER,
                data BLOB,
                timestamp INTEGER
            )`);
            
            // Web cache
            this.db.run(`CREATE TABLE IF NOT EXISTS web_cache (
                url TEXT PRIMARY KEY,
                content BLOB,
                headers TEXT,
                timestamp INTEGER
            )`);
            
            // Bandwidth tracking (for rewards)
            this.db.run(`CREATE TABLE IF NOT EXISTS bandwidth (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                direction TEXT,
                bytes INTEGER,
                peer TEXT,
                timestamp INTEGER
            )`);
        });
        
        console.log('[DB] SQLite initialized');
    }

    initServices() {
        this.services.chat = new ChatService(this);
        this.services.video = new VideoService(this);
        this.services.web = new WebService(this);
        console.log('[SERVICES] Chat, Video, Web loaded');
    }

    initServer() {
        const app = express();
        app.use(express.json({ limit: '50mb' }));
        app.use(express.static('public'));
        
        // API Routes
        app.get('/api/status', (req, res) => {
            res.json({
                nodeId: this.nodeId,
                peers: this.peers.size,
                uptime: process.uptime(),
                memory: process.memoryUsage().heapUsed / 1024 / 1024,
                services: Object.keys(this.services)
            });
        });

        // Chat API
        app.post('/api/chat/send', (req, res) => this.services.chat.send(req, res));
        app.get('/api/chat/receive', (req, res) => this.services.chat.receive(req, res));
        app.get('/api/chat/history', (req, res) => this.services.chat.history(req, res));

        // Video API
        app.get('/api/video/stream', (req, res) => this.services.video.stream(req, res));
        app.post('/api/video/cache', (req, res) => this.services.video.cache(req, res));
        app.get('/api/video/list', (req, res) => this.services.video.list(req, res));

        // Web API
        app.get('/api/web/browse', (req, res) => this.services.web.browse(req, res));
        app.get('/api/web/cache', (req, res) => this.services.web.getCache(req, res));

        // P2P Mesh endpoints
        app.post('/api/mesh/message', (req, res) => this.handleMeshMessage(req, res));
        app.get('/api/mesh/peers', (req, res) => {
            res.json(Array.from(this.peers.entries()));
        });

        const server = http.createServer(app);
        server.listen(this.port, () => {
            console.log(`[HTTP] Server running on http://localhost:${this.port}`);
            console.log(`[UI] Dashboard: http://localhost:${this.port}`);
            console.log(`[UI] Chat: http://localhost:${this.port}/chat.html`);
            console.log(`[UI] Video: http://localhost:${this.port}/video.html`);
            console.log(`[UI] Browser: http://localhost:${this.port}/browser.html`);
        });
    }

    connectRelay() {
        const WebSocket = require('ws');
        
        const connect = () => {
            console.log(`[RELAY] Connecting to ${this.relayUrl}...`);
            this.ws = new WebSocket(this.relayUrl);
            
            this.ws.on('open', () => {
                console.log('[RELAY] Connected ✓');
                this.ws.send(JSON.stringify({
                    type: 'register',
                    nodeId: this.nodeId,
                    services: ['chat', 'video', 'web'],
                    timestamp: Date.now()
                }));
            });
            
            this.ws.on('message', (data) => {
                const msg = JSON.parse(data);
                this.handleRelayMessage(msg);
            });
            
            this.ws.on('close', () => {
                console.log('[RELAY] Disconnected, reconnecting in 5s...');
                setTimeout(connect, 5000);
            });
            
            this.ws.on('error', (err) => {
                console.error('[RELAY] Error:', err.message);
            });
        };
        
        connect();
    }

    handleRelayMessage(msg) {
        switch (msg.type) {
            case 'peers':
                // Update peer list
                msg.peers.forEach(peer => {
                    if (peer.nodeId !== this.nodeId) {
                        this.peers.set(peer.nodeId, peer);
                    }
                });
                console.log(`[P2P] ${this.peers.size} peers online`);
                break;
                
            case 'message':
                // Route to appropriate service
                if (msg.service === 'chat') {
                    this.services.chat.handleIncoming(msg);
                } else if (msg.service === 'video') {
                    this.services.video.handleChunk(msg);
                } else if (msg.service === 'web') {
                    this.services.web.handleRequest(msg);
                }
                break;
                
            case 'broadcast':
                console.log(`[BROADCAST] From ${msg.from}: ${msg.payload.type}`);
                break;
        }
    }

    // Send to specific peer via relay
    sendToPeer(peerId, service, payload) {
        if (this.ws && this.ws.readyState === 1) {
            this.ws.send(JSON.stringify({
                type: 'message',
                to: peerId,
                from: this.nodeId,
                service: service,
                payload: payload,
                timestamp: Date.now()
            }));
            return true;
        }
        return false;
    }

    // Broadcast to all peers
    broadcast(service, payload) {
        if (this.ws && this.ws.readyState === 1) {
            this.ws.send(JSON.stringify({
                type: 'broadcast',
                from: this.nodeId,
                service: service,
                payload: payload,
                timestamp: Date.now()
            }));
        }
    }

    // Track bandwidth (for rewards)
    trackBandwidth(direction, bytes, peer) {
        this.db.run(
            'INSERT INTO bandwidth (direction, bytes, peer, timestamp) VALUES (?, ?, ?, ?)',
            [direction, bytes, peer, Date.now()]
        );
    }
}

// Start node
const node = new ZeyNodeV2();

// CLI
const readline = require('readline');
const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    prompt: 'zey> '
});

rl.prompt();
rl.on('line', (line) => {
    const [cmd, ...args] = line.trim().split(' ');
    
    switch(cmd) {
        case 'status':
            console.log({
                nodeId: node.nodeId,
                peers: node.peers.size,
                memory: `${(process.memoryUsage().heapUsed/1024/1024).toFixed(1)}MB`
            });
            break;
        case 'chat':
            node.services.chat.sendCommand(args.join(' '));
            break;
        case 'peers':
            console.log('Peers:', Array.from(node.peers.keys()));
            break;
        case 'cache':
            node.db.get('SELECT COUNT(*) as count FROM web_cache', (err, row) => {
                console.log('Cached pages:', row.count);
            });
            break;
        case 'help':
            console.log('Commands: status, chat <msg>, peers, cache, quit');
            break;
        case 'quit':
            process.exit(0);
            break;
    }
    rl.prompt();
});
