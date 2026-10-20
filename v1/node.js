const WebSocket = require('ws');
const http = require('http');
const { exec } = require('child_process');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

class ZeyNode {
    constructor(relayUrl = 'ws://localhost:8080') {
        this.relayUrl = relayUrl;
        this.nodeId = null;
        this.peers = new Map();
        this.services = new Map();
        this.httpCache = new Map();
        
        // Local HTTP server untuk apps
        this.localPort = 3000 + Math.floor(Math.random() * 1000);
        
        this.init();
    }

    async init() {
        console.log('📱 ZEY NODE v0.1');
        console.log('================');
        console.log(`🔌 Connecting to relay: ${this.relayUrl}`);
        
        this.connect();
        this.startLocalServer();
        this.setupServices();
    }

    connect() {
        this.ws = new WebSocket(this.relayUrl);

        this.ws.on('open', () => {
            console.log('✅ Connected to Zey Relay');
        });

        this.ws.on('message', (data) => {
            const msg = JSON.parse(data);
            this.handleMessage(msg);
        });

        this.ws.on('close', () => {
            console.log('❌ Disconnected, reconnecting in 5s...');
            setTimeout(() => this.connect(), 5000);
        });

        this.ws.on('error', (err) => {
            console.error('Connection error:', err.message);
        });
    }

    handleMessage(msg) {
        switch (msg.type) {
            case 'welcome':
                this.nodeId = msg.nodeId;
                console.log(`🆔 Node ID: ${this.nodeId}`);
                console.log(`🌐 Active nodes: ${msg.nodes.length}`);
                this.registerServices();
                break;

            case 'node-joined':
                console.log(`[+] Peer joined: ${msg.nodeId} (total: ${msg.totalNodes})`);
                break;

            case 'node-left':
                console.log(`[-] Peer left: ${msg.nodeId}`);
                break;

            case 'data':
                this.handlePeerData(msg.from, msg.payload);
                break;

            case 'http-response':
                this.handleHttpResponse(msg);
                break;

            case 'resolve-response':
                console.log(`[DNS] ${msg.domain} -> ${msg.nodeId || 'NOT FOUND'}`);
                break;
        }
    }

    registerServices() {
        // Daftarkan service yang tersedia
        const services = ['chat', 'video', 'web'];
        services.forEach(svc => {
            this.ws.send(JSON.stringify({
                type: 'register-service',
                service: svc
            }));
        });
    }

    setupServices() {
        // Setup local services
        this.services.set('chat', this.handleChat.bind(this));
        this.services.set('video', this.handleVideo.bind(this));
        this.services.set('web', this.handleWeb.bind(this));
    }

    // ==================== LOCAL HTTP SERVER ====================
    startLocalServer() {
        const server = http.createServer((req, res) => {
            const url = new URL(req.url, `http://localhost:${this.localPort}`);
            
            // CORS
            res.setHeader('Access-Control-Allow-Origin', '*');
            res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
            
            if (url.pathname === '/') {
                this.serveDashboard(res);
            } else if (url.pathname === '/chat') {
                this.serveChat(res);
            } else if (url.pathname === '/video') {
                this.serveVideo(res);
            } else if (url.pathname.startsWith('/proxy/')) {
                this.handleProxy(url.pathname.slice(7), req, res);
            } else {
                res.writeHead(404);
                res.end('Not found');
            }
        });

        server.listen(this.localPort, () => {
            console.log(`🌐 Local server: http://localhost:${this.localPort}`);
            console.log(`📊 Dashboard: http://localhost:${this.localPort}`);
            console.log(`💬 Chat: http://localhost:${this.localPort}/chat`);
            console.log(`🎬 Video: http://localhost:${this.localPort}/video`);
        });
    }

    serveDashboard(res) {
        const html = `
<!DOCTYPE html>
<html>
<head>
    <title>ZeyNode Dashboard</title>
    <style>
        body { font-family: monospace; background: #0a0a0a; color: #0f0; padding: 20px; }
        .box { border: 1px solid #0f0; padding: 15px; margin: 10px 0; }
        a { color: #0ff; }
        .stat { display: inline-block; margin: 0 20px; }
    </style>
</head>
<body>
    <h1>🚀 ZEY NODE ${this.nodeId || 'connecting...'}</h1>
    
    <div class="box">
        <h3>📊 Status</h3>
        <div class="stat">Node ID: <b>${this.nodeId}</b></div>
        <div class="stat">Relay: <b>${this.relayUrl}</b></div>
        <div class="stat">Local: <b>http://localhost:${this.localPort}</b></div>
    </div>

    <div class="box">
        <h3>🎯 Services</h3>
        <ul>
            <li><a href="/chat">💬 Chat P2P</a> - Messaging tanpa server</li>
            <li><a href="/video">🎬 Video Stream</a> - P2P video sharing</li>
            <li><a href="/proxy/http://example.com">🌐 Web Proxy</a> - Akses website via P2P</li>
        </ul>
    </div>

    <div class="box">
        <h3>🔧 ZeyDNS</h3>
        <form onsubmit="resolve(event)">
            <input type="text" id="domain" placeholder="chat.zey" style="background:#000;color:#0f0;border:1px solid #0f0;padding:5px;">
            <button style="background:#0f0;color:#000;border:none;padding:5px 10px;">Resolve</button>
        </form>
        <div id="result"></div>
    </div>

    <script>
        function resolve(e) {
            e.preventDefault();
            const domain = document.getElementById('domain').value;
            // Simulasi - real implementation butuh WebSocket ke node
            document.getElementById('result').innerHTML = 
                '<p>Resolving ' + domain + '... (connect WebSocket for real DNS)</p>';
        }
    </script>
</body>
</html>`;
        res.writeHead(200, {'Content-Type': 'text/html'});
        res.end(html);
    }

    serveChat(res) {
        const html = `
<!DOCTYPE html>
<html>
<head>
    <title>ZeyChat - P2P Messaging</title>
    <style>
        body { font-family: monospace; background: #0a0a0a; color: #0f0; margin: 0; }
        #header { background: #1a1a1a; padding: 10px; border-bottom: 2px solid #0f0; }
        #messages { height: 70vh; overflow-y: auto; padding: 10px; }
        .msg { margin: 5px 0; padding: 8px; background: #1a1a1a; border-left: 3px solid #0f0; }
        .msg .from { color: #0ff; font-size: 0.8em; }
        #input { position: fixed; bottom: 0; width: 100%; padding: 10px; background: #1a1a1a; }
        input[type=text] { width: 80%; background: #000; color: #0f0; border: 1px solid #0f0; padding: 10px; }
        button { background: #0f0; color: #000; border: none; padding: 10px 20px; cursor: pointer; }
    </style>
</head>
<body>
    <div id="header">
        <h2>💬 ZeyChat (P2P)</h2>
        <small>Node: ${this.nodeId} | No server, pure P2P</small>
    </div>
    
    <div id="messages"></div>
    
    <div id="input">
        <input type="text" id="msg" placeholder="Type message..." autofocus>
        <button onclick="send()">Send</button>
    </div>

    <script>
        // WebSocket ke local node
        const ws = new WebSocket('ws://localhost:${this.localPort}/ws');
        const messages = document.getElementById('messages');
        
        function addMsg(from, text) {
            const div = document.createElement('div');
            div.className = 'msg';
            div.innerHTML = '<div class="from">' + from + '</div>' + text;
            messages.appendChild(div);
            messages.scrollTop = messages.scrollHeight;
        }

        function send() {
            const input = document.getElementById('msg');
            const text = input.value;
            if (!text) return;
            
            // Kirim via HTTP ke local node
            fetch('/send', {
                method: 'POST',
                body: JSON.stringify({text})
            });
            
            addMsg('You', text);
            input.value = '';
        }

        // Enter key
        document.getElementById('msg').addEventListener('keypress', e => {
            if (e.key === 'Enter') send();
        });

        // Simulasi receive (real: WebSocket dari node)
        setInterval(() => {
            // Poll untuk messages baru
        }, 1000);
    </script>
</body>
</html>`;
        res.writeHead(200, {'Content-Type': 'text/html'});
        res.end(html);
    }

    serveVideo(res) {
        const html = `
<!DOCTYPE html>
<html>
<head>
    <title>ZeyTube - P2P Video</title>
    <style>
        body { font-family: monospace; background: #0a0a0a; color: #0f0; padding: 20px; }
        video { width: 100%; max-width: 800px; border: 2px solid #0f0; }
        .info { background: #1a1a1a; padding: 10px; margin: 10px 0; }
        input[type=text] { width: 60%; background: #000; color: #0f0; border: 1px solid #0f0; padding: 10px; }
        button { background: #0f0; color: #000; border: none; padding: 10px 20px; }
    </style>
</head>
<body>
    <h1>🎬 ZeyTube (P2P Video)</h1>
    
    <div class="info">
        <h3>Stream dari P2P Network</h3>
        <input type="text" id="url" placeholder="http://example.com/video.mp4" value="https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/BigBuckBunny.mp4">
        <button onclick="load()">Load via P2P</button>
    </div>

    <video id="player" controls></video>
    
    <div class="info">
        <p><b>Status:</b> <span id="status">Idle</span></p>
        <p><b>Peers:</b> <span id="peers">0</span></p>
        <p><b>Buffer:</b> <span id="buffer">0%</span></p>
    </div>

    <script>
        let video = document.getElementById('player');
        let sourceBuffer;
        
        async function load() {
            const url = document.getElementById('url').value;
            document.getElementById('status').textContent = 'Fetching via P2P proxy...';
            
            // Proxy via ZeyNode
            video.src = '/proxy/' + url;
            video.load();
            
            video.oncanplay = () => {
                document.getElementById('status').textContent = 'Streaming!';
            };
            
            video.onerror = () => {
                document.getElementById('status').textContent = 'Error - trying direct';
                video.src = url;
            };
        }

        // Simulasi P2P stats
        setInterval(() => {
            document.getElementById('peers').textContent = Math.floor(Math.random() * 5);
            if (video.buffered.length) {
                const pct = (video.buffered.end(0) / video.duration * 100).toFixed(0);
                document.getElementById('buffer').textContent = pct + '%';
            }
        }, 1000);
    </script>
</body>
</html>`;
        res.writeHead(200, {'Content-Type': 'text/html'});
        res.end(html);
    }

    async handleProxy(targetUrl, req, res) {
        console.log(`[Proxy] ${req.method} ${targetUrl}`);
        
        try {
            // Kirim request via relay
            const requestId = crypto.randomBytes(8).toString('hex');
            
            this.ws.send(JSON.stringify({
                type: 'http-request',
                id: requestId,
                url: targetUrl,
                method: req.method,
                headers: req.headers
            }));

            // Tunggu response (simplified - real butuh proper async)
            const response = await fetch(targetUrl);
            const buffer = await response.arrayBuffer();
            
            res.writeHead(response.status, {
                'Content-Type': response.headers.get('content-type') || 'application/octet-stream'
            });
            res.end(Buffer.from(buffer));
            
        } catch (err) {
            res.writeHead(500);
            res.end('Proxy error: ' + err.message);
        }
    }

    handlePeerData(from, payload) {
        console.log(`[P2P] Data from ${from}:`, payload.type);
        
        switch (payload.type) {
            case 'chat':
                // Broadcast chat ke local clients
                console.log(`💬 Chat: ${payload.text}`);
                break;
            case 'video-chunk':
                // Handle video chunk
                break;
        }
    }

    sendToPeer(nodeId, payload) {
        this.ws.send(JSON.stringify({
            type: 'relay',
            to: nodeId,
            payload
        }));
    }

    broadcast(payload) {
        this.ws.send(JSON.stringify({
            type: 'broadcast',
            payload
        }));
    }
}

// Start node
const relayUrl = process.argv[2] || 'ws://localhost:8080';
const node = new ZeyNode(relayUrl);

// CLI commands
const readline = require('readline');
const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    prompt: 'zey> '
});

rl.on('line', (line) => {
    const [cmd, ...args] = line.trim().split(' ');
    
    switch (cmd) {
        case 'status':
            console.log('Node ID:', node.nodeId);
            console.log('Connected to:', node.relayUrl);
            break;
        case 'chat':
            const text = args.join(' ');
            node.broadcast({type: 'chat', text});
            console.log('📢 Broadcasted:', text);
            break;
        case 'resolve':
            node.ws.send(JSON.stringify({
                type: 'resolve',
                domain: args[0]
            }));
            break;
        case 'peers':
            console.log('Active peers:', node.peers.size);
            break;
        case 'quit':
            process.exit(0);
            break;
        default:
            console.log('Commands: status, chat <msg>, resolve <domain>, peers, quit');
    }
    rl.prompt();
});
