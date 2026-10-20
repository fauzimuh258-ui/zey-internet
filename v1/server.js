const WebSocket = require('ws');
const http = require('http');
const crypto = require('crypto');

const server = http.createServer();
const wss = new WebSocket.Server({ server });

// Node registry
const nodes = new Map(); // nodeId -> {ws, info, lastSeen}
const routes = new Map(); // domain -> nodeId

// ZeyDNS simple database
const zeyDNS = {
    'chat.zey': 'node_a',
    'video.zey': 'node_b',
    'web.zey': 'node_c'
};

console.log('🚀 ZEY RELAY v0.1 - Bootstrap Node');
console.log('==================================');

wss.on('connection', (ws, req) => {
    const nodeId = crypto.randomBytes(4).toString('hex');
    const ip = req.socket.remoteAddress;
    
    console.log(`[+] Node connected: ${nodeId} from ${ip}`);
    
    // Register node
    nodes.set(nodeId, {
        ws,
        info: { ip, connected: Date.now() },
        lastSeen: Date.now()
    });

    // Send welcome
    ws.send(JSON.stringify({
        type: 'welcome',
        nodeId,
        nodes: Array.from(nodes.keys()),
        dns: zeyDNS
    }));

    // Broadcast node joined
    broadcast({
        type: 'node-joined',
        nodeId,
        totalNodes: nodes.size
    }, nodeId);

    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data);
            handleMessage(nodeId, msg);
        } catch (e) {
            console.error('Invalid message:', e.message);
        }
    });

    ws.on('close', () => {
        console.log(`[-] Node disconnected: ${nodeId}`);
        nodes.delete(nodeId);
        broadcast({
            type: 'node-left',
            nodeId,
            totalNodes: nodes.size
        });
    });

    // Heartbeat
    ws.on('pong', () => {
        if (nodes.has(nodeId)) {
            nodes.get(nodeId).lastSeen = Date.now();
        }
    });
});

function handleMessage(fromId, msg) {
    const node = nodes.get(fromId);
    if (!node) return;
    
    node.lastSeen = Date.now();

    switch (msg.type) {
        case 'register-service':
            // Node mendaftarkan service (web, video, chat)
            routes.set(`${msg.service}.${fromId}.zey`, fromId);
            console.log(`[DNS] ${msg.service}.${fromId}.zey -> ${fromId}`);
            break;

        case 'resolve':
            // DNS query
            const target = zeyDNS[msg.domain] || routes.get(msg.domain);
            node.ws.send(JSON.stringify({
                type: 'resolve-response',
                domain: msg.domain,
                nodeId: target || null
            }));
            break;

        case 'relay':
            // Relay data ke node lain
            const targetNode = nodes.get(msg.to);
            if (targetNode) {
                targetNode.ws.send(JSON.stringify({
                    type: 'data',
                    from: fromId,
                    payload: msg.payload
                }));
            }
            break;

        case 'broadcast':
            // Broadcast ke semua node
            broadcast({
                type: 'broadcast',
                from: fromId,
                payload: msg.payload
            }, fromId);
            break;

        case 'http-request':
            // HTTP proxy request
            handleProxyRequest(fromId, msg);
            break;
    }
}

async function handleProxyRequest(fromId, msg) {
    const node = nodes.get(fromId);
    if (!node) return;

    try {
        // Forward request ke internet atau node lain
        const response = await fetch(msg.url, {
            method: msg.method || 'GET',
            headers: msg.headers || {},
            body: msg.body
        });

        const buffer = await response.arrayBuffer();
        const base64 = Buffer.from(buffer).toString('base64');

        node.ws.send(JSON.stringify({
            type: 'http-response',
            id: msg.id,
            status: response.status,
            headers: Object.fromEntries(response.headers),
            body: base64
        }));
    } catch (err) {
        node.ws.send(JSON.stringify({
            type: 'http-response',
            id: msg.id,
            error: err.message
        }));
    }
}

function broadcast(msg, excludeId = null) {
    const data = JSON.stringify(msg);
    nodes.forEach((node, id) => {
        if (id !== excludeId && node.ws.readyState === WebSocket.OPEN) {
            node.ws.send(data);
        }
    });
}

// Cleanup dead nodes
setInterval(() => {
    const now = Date.now();
    nodes.forEach((node, id) => {
        if (now - node.lastSeen > 30000) {
            console.log(`[!] Timeout: ${id}`);
            node.ws.terminate();
            nodes.delete(id);
        }
    });
}, 10000);

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => {
    console.log(`🌐 Zey Relay listening on port ${PORT}`);
    console.log(`📡 WebSocket: ws://localhost:${PORT}`);
});
