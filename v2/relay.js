const WebSocket = require('ws');
const http = require('http');

const server = http.createServer();
const wss = new WebSocket.Server({ server });

const nodes = new Map();

console.log('🚀 ZEY RELAY V2 - Lightweight Bootstrap');

wss.on('connection', (ws) => {
    let nodeId = null;
    
    ws.on('message', (data) => {
        const msg = JSON.parse(data);
        
        switch(msg.type) {
            case 'register':
                nodeId = msg.nodeId;
                nodes.set(nodeId, {
                    ws,
                    services: msg.services,
                    timestamp: Date.now()
                });
                
                console.log(`[+] Node registered: ${nodeId}`);
                console.log(`    Services: ${msg.services.join(', ')}`);
                console.log(`    Total nodes: ${nodes.size}`);
                
                // Send peer list
                ws.send(JSON.stringify({
                    type: 'peers',
                    peers: Array.from(nodes.entries()).map(([id, info]) => ({
                        nodeId: id,
                        services: info.services
                    }))
                }));
                
                // Notify others
                broadcast({
                    type: 'peer-joined',
                    nodeId,
                    services: msg.services
                }, nodeId);
                break;
                
            case 'message':
            case 'broadcast':
                // Route message
                if (msg.to) {
                    const target = nodes.get(msg.to);
                    if (target) {
                        target.ws.send(JSON.stringify(msg));
                    }
                } else {
                    broadcast(msg, nodeId);
                }
                break;
        }
    });
    
    ws.on('close', () => {
        if (nodeId) {
            nodes.delete(nodeId);
            console.log(`[-] Node disconnected: ${nodeId}`);
            broadcast({
                type: 'peer-left',
                nodeId
            });
        }
    });
});

function broadcast(msg, exclude = null) {
    const data = JSON.stringify(msg);
    nodes.forEach((info, id) => {
        if (id !== exclude && info.ws.readyState === WebSocket.OPEN) {
            info.ws.send(data);
        }
    });
}

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => {
    console.log(`📡 Relay listening on port ${PORT}`);
});
                          
