const crypto = require('crypto');

class ChatService {
    constructor(node) {
        this.node = node;
        this.encryptionKey = crypto.randomBytes(32); // AES-256
    }

    // API: Send message
    async send(req, res) {
        const { to, message, group } = req.body;
        
        const msgId = crypto.randomBytes(8).toString('hex');
        const encrypted = this.encrypt(message);
        
        const payload = {
            id: msgId,
            from: this.node.nodeId,
            to: to || 'broadcast',
            content: encrypted,
            timestamp: Date.now(),
            group: group || null
        };

        // Store in local DB (offline support)
        this.node.db.run(
            'INSERT INTO messages (id, from_node, to_node, content, timestamp) VALUES (?, ?, ?, ?, ?)',
            [msgId, this.node.nodeId, to || 'all', encrypted, Date.now()]
        );

        // Send via P2P
        if (to && to !== 'broadcast') {
            this.node.sendToPeer(to, 'chat', payload);
        } else {
            this.node.broadcast('chat', payload);
        }

        // Track bandwidth
        const bytes = Buffer.byteLength(JSON.stringify(payload));
        this.node.trackBandwidth('out', bytes, to);

        res.json({ success: true, id: msgId, timestamp: payload.timestamp });
    }

    // API: Receive messages
    async receive(req, res) {
        const { since, from } = req.query;
        
        let query = 'SELECT * FROM messages WHERE delivered = 0';
        let params = [];
        
        if (since) {
            query += ' AND timestamp > ?';
            params.push(parseInt(since));
        }
        if (from) {
            query += ' AND from_node = ?';
            params.push(from);
        }
        
        this.node.db.all(query, params, (err, rows) => {
            if (err) {
                res.status(500).json({ error: err.message });
                return;
            }
            
            // Mark as delivered
            rows.forEach(row => {
                this.node.db.run('UPDATE messages SET delivered = 1 WHERE id = ?', [row.id]);
            });
            
            // Decrypt messages
            const messages = rows.map(row => ({
                id: row.id,
                from: row.from_node,
                content: this.decrypt(row.content),
                timestamp: row.timestamp
            }));
            
            res.json({ messages, count: messages.length });
        });
    }

    // API: Chat history
    async history(req, res) {
        const { limit = 50, offset = 0 } = req.query;
        
        this.node.db.all(
            'SELECT * FROM messages ORDER BY timestamp DESC LIMIT ? OFFSET ?',
            [parseInt(limit), parseInt(offset)],
            (err, rows) => {
                if (err) {
                    res.status(500).json({ error: err.message });
                    return;
                }
                
                const messages = rows.map(row => ({
                    id: row.id,
                    from: row.from_node,
                    content: this.decrypt(row.content),
                    timestamp: row.timestamp
                }));
                
                res.json({ messages });
            }
        );
    }

    // Handle incoming P2P message
    handleIncoming(msg) {
        const { payload } = msg;
        
        // Store in DB
        this.node.db.run(
            'INSERT OR REPLACE INTO messages (id, from_node, to_node, content, timestamp, delivered) VALUES (?, ?, ?, ?, ?, 0)',
            [payload.id, payload.from, payload.to, payload.content, payload.timestamp]
        );
        
        console.log(`[CHAT] New message from ${payload.from}: ${this.decrypt(payload.content).substring(0, 50)}...`);
        
        // Track bandwidth
        const bytes = Buffer.byteLength(JSON.stringify(payload));
        this.node.trackBandwidth('in', bytes, payload.from);
    }

    // CLI command
    sendCommand(text) {
        const payload = {
            id: crypto.randomBytes(8).toString('hex'),
            from: this.node.nodeId,
            to: 'broadcast',
            content: this.encrypt(text),
            timestamp: Date.now()
        };
        
        this.node.broadcast('chat', payload);
        console.log('📢 Broadcasted:', text);
    }

    // Simple encryption (use libsodium in production!)
    encrypt(text) {
        const cipher = crypto.createCipher('aes-256-cbc', this.encryptionKey);
        let encrypted = cipher.update(text, 'utf8', 'hex');
        encrypted += cipher.final('hex');
        return encrypted;
    }

    decrypt(encrypted) {
        try {
            const decipher = crypto.createDecipher('aes-256-cbc', this.encryptionKey);
            let decrypted = decipher.update(encrypted, 'hex', 'utf8');
            decrypted += decipher.final('utf8');
            return decrypted;
        } catch (e) {
            return '[encrypted]';
        }
    }
}

module.exports = ChatService;
