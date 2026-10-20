const crypto = require('crypto');
const fetch = require('node-fetch');

class VideoService {
    constructor(node) {
        this.node = node;
        this.chunkSize = 64 * 1024; // 64KB chunks (HP friendly)
        this.activeStreams = new Map();
    }

    // API: Stream video (P2P first, fallback to direct)
    async stream(req, res) {
        const { url, peer, quality = 'auto' } = req.query;
        
        if (!url) {
            res.status(400).json({ error: 'URL required' });
            return;
        }

        console.log(`[VIDEO] Stream request: ${url}`);

        // Check local cache first
        const cached = await this.getFromCache(url);
        if (cached) {
            console.log('[VIDEO] Serving from cache');
            res.setHeader('X-Cache', 'HIT');
            res.setHeader('Content-Type', 'video/mp4');
            res.end(cached);
            return;
        }

        // Try P2P from peer
        if (peer && this.node.peers.has(peer)) {
            console.log(`[VIDEO] Requesting from peer: ${peer}`);
            const streamId = crypto.randomBytes(8).toString('hex');
            
            this.node.sendToPeer(peer, 'video', {
                type: 'request',
                url: url,
                streamId: streamId,
                quality: quality
            });

            // Wait for chunks (simplified - real impl needs proper async)
            res.setHeader('X-Cache', 'P2P_MISS');
            res.status(202).json({ 
                status: 'streaming', 
                streamId: streamId,
                message: 'Streaming initiated via P2P'
            });
            return;
        }

        // Fallback: fetch directly and cache
        try {
            console.log('[VIDEO] Fetching from internet...');
            const response = await fetch(url);
            const buffer = await response.buffer();
            
            // Cache for future P2P sharing
            await this.cacheVideo(url, buffer);
            
            res.setHeader('X-Cache', 'MISS');
            res.setHeader('Content-Type', 'video/mp4');
            res.end(buffer);
            
        } catch (err) {
            res.status(500).json({ error: err.message });
        }
    }

    // API: Cache video
    async cache(req, res) {
        const { url } = req.body;
        
        try {
            const response = await fetch(url);
            const buffer = await response.buffer();
            
            await this.cacheVideo(url, buffer);
            
            res.json({ 
                success: true, 
                size: buffer.length,
                chunks: Math.ceil(buffer.length / this.chunkSize)
            });
        } catch (err) {
            res.status(500).json({ error: err.message });
        }
    }

    // API: List cached videos
    async list(req, res) {
        this.node.db.all(
            'SELECT DISTINCT url, COUNT(*) as chunks, MAX(timestamp) as cached_at FROM video_cache GROUP BY url',
            (err, rows) => {
                if (err) {
                    res.status(500).json({ error: err.message });
                    return;
                }
                res.json({ videos: rows });
            }
        );
    }

    // Handle incoming video chunk from peer
    handleChunk(msg) {
        const { payload } = msg;
        
        if (payload.type === 'request') {
            // Peer requesting video - serve from cache
            this.serveToPeer(payload);
        } else if (payload.type === 'chunk') {
            // Received chunk from peer
            this.storeChunk(payload);
        }
    }

    async serveToPeer(request) {
        const { url, streamId, requester } = request;
        
        this.node.db.all(
            'SELECT * FROM video_cache WHERE url = ? ORDER BY chunk_index',
            [url],
            (err, chunks) => {
                if (err || chunks.length === 0) {
                    // Not in cache, fetch and cache
                    this.fetchAndServe(url, streamId, requester);
                    return;
                }
                
                // Send chunks
                chunks.forEach((chunk, index) => {
                    setTimeout(() => {
                        this.node.sendToPeer(requester, 'video', {
                            type: 'chunk',
                            streamId: streamId,
                            index: chunk.chunk_index,
                            data: chunk.data.toString('base64'),
                            total: chunks.length
                        });
                    }, index * 10); // Stagger sends
                });
                
                console.log(`[VIDEO] Served ${chunks.length} chunks to ${requester}`);
            }
        );
    }

    async fetchAndServe(url, streamId, requester) {
        try {
            const response = await fetch(url);
            const buffer = await response.buffer();
            
            // Cache it
            await this.cacheVideo(url, buffer);
            
            // Serve to peer
            this.serveToPeer({ url, streamId, requester });
        } catch (err) {
            console.error('[VIDEO] Fetch error:', err.message);
        }
    }

    async cacheVideo(url, buffer) {
        const chunks = [];
        for (let i = 0; i < buffer.length; i += this.chunkSize) {
            chunks.push(buffer.slice(i, i + this.chunkSize));
        }
        
        const stmt = this.node.db.prepare(
            'INSERT OR REPLACE INTO video_cache (id, url, chunk_index, data, timestamp) VALUES (?, ?, ?, ?, ?)'
        );
        
        chunks.forEach((chunk, index) => {
            stmt.run(
                crypto.randomBytes(8).toString('hex'),
                url,
                index,
                chunk,
                Date.now()
            );
        });
        
        stmt.finalize();
        console.log(`[VIDEO] Cached ${chunks.length} chunks for ${url}`);
    }

    async getFromCache(url) {
        return new Promise((resolve, reject) => {
            this.node.db.all(
                'SELECT data FROM video_cache WHERE url = ? ORDER BY chunk_index',
                [url],
                (err, rows) => {
                    if (err || rows.length === 0) {
                        resolve(null);
                        return;
                    }
                    
                    const buffer = Buffer.concat(rows.map(r => r.data));
                    resolve(buffer);
                }
            );
        });
    }

    storeChunk(payload) {
        // Store incoming chunk
        this.node.db.run(
            'INSERT OR REPLACE INTO video_cache (id, url, chunk_index, data, timestamp) VALUES (?, ?, ?, ?, ?)',
            [
                crypto.randomBytes(8).toString('hex'),
                payload.url,
                payload.index,
                Buffer.from(payload.data, 'base64'),
                Date.now()
            ]
        );
        
        console.log(`[VIDEO] Stored chunk ${payload.index}/${payload.total}`);
    }
}

module.exports = VideoService;
              
