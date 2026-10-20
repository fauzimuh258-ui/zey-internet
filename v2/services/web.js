const fetch = require('node-fetch');
const crypto = require('crypto');

class WebService {
    constructor(node) {
        this.node = node;
        this.cacheExpiry = 24 * 60 * 60 * 1000; // 24 hours
    }

    // API: Browse website (P2P cache first)
    async browse(req, res) {
        const { url, peer, refresh } = req.query;
        
        if (!url) {
            res.status(400).json({ error: 'URL required' });
            return;
        }

        // Add protocol if missing
        const fullUrl = url.startsWith('http') ? url : `http://${url}`;

        console.log(`[WEB] Browse request: ${fullUrl}`);

        // Check cache first (unless refresh)
        if (!refresh) {
            const cached = await this.getFromCache(fullUrl);
            if (cached) {
                console.log('[WEB] Cache HIT');
                res.setHeader('X-Cache', 'HIT');
                res.setHeader('Content-Type', cached.headers['content-type'] || 'text/html');
                res.end(cached.content);
                return;
            }
        }

        // Try P2P from specific peer
        if (peer && this.node.peers.has(peer)) {
            console.log(`[WEB] Requesting from peer: ${peer}`);
            
            this.node.sendToPeer(peer, 'web', {
                type: 'request',
                url: fullUrl,
                requester: this.node.nodeId
            });

            res.status(202).json({
                status: 'fetching',
                message: 'Request sent to peer, poll /api/web/cache'
            });
            return;
        }

        // Fetch directly
        try {
            console.log('[WEB] Fetching from internet...');
            const response = await fetch(fullUrl, {
                headers: {
                    'User-Agent': 'ZeyBrowser/2.0 (P2P)'
                }
            });
            
            const content = await response.buffer();
            const headers = Object.fromEntries(response.headers);
            
            // Cache it
            await this.cachePage(fullUrl, content, headers);
            
            // Track bandwidth
            this.node.trackBandwidth('in', content.length, 'internet');
            
            res.setHeader('X-Cache', 'MISS');
            res.setHeader('Content-Type', headers['content-type'] || 'text/html');
            res.end(content);
            
        } catch (err) {
            res.status(500).json({ error: err.message });
        }
    }

    // API: Get cached page
    async getCache(req, res) {
        const { url } = req.query;
        
        if (!url) {
            // List all cached URLs
            this.node.db.all(
                'SELECT url, timestamp, LENGTH(content) as size FROM web_cache ORDER BY timestamp DESC LIMIT 50',
                (err, rows) => {
                    res.json({ cached: rows || [] });
                }
            );
            return;
        }

        const cached = await this.getFromCache(url);
        if (cached) {
            res.json({
                url: url,
                found: true,
                size: cached.content.length,
                timestamp: cached.timestamp,
                preview: cached.content.toString('utf8').substring(0, 500)
            });
        } else {
            res.json({ found: false });
        }
    }

    // Handle incoming web request from peer
    async handleRequest(msg) {
        const { payload } = msg;
        
        if (payload.type === 'request') {
            // Serve from cache or fetch
            const cached = await this.getFromCache(payload.url);
            
            if (cached) {
                // Serve from cache
                this.node.sendToPeer(payload.requester, 'web', {
                    type: 'response',
                    url: payload.url,
                    content: cached.content.toString('base64'),
                    headers: cached.headers,
                    fromCache: true
                });
                
                console.log(`[WEB] Served ${payload.url} from cache to ${payload.requester}`);
            } else {
                // Fetch and forward
                this.fetchAndForward(payload.url, payload.requester);
            }
        } else if (payload.type === 'response') {
            // Received response from peer
            const content = Buffer.from(payload.content, 'base64');
            await this.cachePage(payload.url, content, payload.headers);
            
            console.log(`[WEB] Cached ${payload.url} from peer ${msg.from}`);
        }
    }

    async fetchAndForward(url, requester) {
        try {
            const response = await fetch(url);
            const content = await response.buffer();
            const headers = Object.fromEntries(response.headers);
            
            // Cache locally
            await this.cachePage(url, content, headers);
            
            // Forward to requester
            this.node.sendToPeer(requester, 'web', {
                type: 'response',
                url: url,
                content: content.toString('base64'),
                headers: headers,
                fromCache: false
            });
            
            console.log(`[WEB] Fetched and forwarded ${url} to ${requester}`);
        } catch (err) {
            console.error('[WEB] Fetch error:', err.message);
        }
    }

    async cachePage(url, content, headers) {
        return new Promise((resolve, reject) => {
            this.node.db.run(
                'INSERT OR REPLACE INTO web_cache (url, content, headers, timestamp) VALUES (?, ?, ?, ?)',
                [url, content, JSON.stringify(headers), Date.now()],
                (err) => {
                    if (err) reject(err);
                    else resolve();
                }
            );
        });
    }

    async getFromCache(url) {
        return new Promise((resolve, reject) => {
            this.node.db.get(
                'SELECT * FROM web_cache WHERE url = ? AND timestamp > ?',
                [url, Date.now() - this.cacheExpiry],
                (err, row) => {
                    if (err || !row) {
                        resolve(null);
                        return;
                    }
                    
                    resolve({
                        content: row.content,
                        headers: JSON.parse(row.headers),
                        timestamp: row.timestamp
                    });
                }
            );
        });
    }
}

module.exports = WebService;
