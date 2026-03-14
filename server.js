const http = require('http');

// --- CONFIGURATION ---
const MEMORY_CACHE = new Map();
const CACHE_TTL = 30 * 60 * 1000; // 30 Minutes
const ORIGIN_BASE = "https://storage.novisurf.top";
const PORT = process.env.PORT || 8080;
const INTERNAL_AUTH_TOKEN = process.env.INTERNAL_AUTH_TOKEN || "change_me_immediately";
const MAX_MEMORY_ITEMS = 5000; // Safety limit to trigger auto-eviction

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const cacheKey = url.pathname;

  // 1. AWS App Runner Health Check
  if (cacheKey === "/health" || cacheKey === "/") {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    return res.end("OK");
  }

  // --- ADMIN ROUTES (Purge & Evict) ---
  const authHeader = req.headers['x-novi-auth'];
  
  // A. NUKE: Purge All
  if (cacheKey === "/purge/all") {
    if (authHeader !== INTERNAL_AUTH_TOKEN) {
      res.writeHead(401); return res.end("Unauthorized");
    }
    MEMORY_CACHE.clear();
    console.log("!!! CACHE NUKED: /purge/all executed");
    res.writeHead(200); return res.end("Cache Cleared Successfully");
  }

  // B. BUCKET PURGE: /purge/:id (Clears all keys starting with /bucketid/)
  if (cacheKey.startsWith("/purge/")) {
    if (authHeader !== INTERNAL_AUTH_TOKEN) {
      res.writeHead(401); return res.end("Unauthorized");
    }
    const bucketId = cacheKey.split("/")[2]; // Get :id
    let count = 0;
    for (const [key] of MEMORY_CACHE) {
      if (key.startsWith(`/${bucketId}/`)) {
        MEMORY_CACHE.delete(key);
        count++;
      }
    }
    console.log(`[PURGE] Bucket: ${bucketId} | Items Removed: ${count}`);
    res.writeHead(200); return res.end(`Purged ${count} items from bucket ${bucketId}`);
  }

  // C. SELECTIVE EVICT: /evict/:bucketid?key=filename.ext
  if (cacheKey.startsWith("/evict/")) {
    if (authHeader !== INTERNAL_AUTH_TOKEN) {
      res.writeHead(401); return res.end("Unauthorized");
    }
    const bucketId = cacheKey.split("/")[2];
    const fileName = url.searchParams.get("key");
    const targetKey = `/${bucketId}/${fileName}`;
    
    if (MEMORY_CACHE.delete(targetKey)) {
      console.log(`[EVICT] Single Key: ${targetKey}`);
      res.writeHead(200); return res.end(`Evicted ${targetKey}`);
    }
    res.writeHead(404); return res.end("Key not found in cache");
  }

  // 2. RAM CACHE CHECK (Standard Request)
  if (MEMORY_CACHE.has(cacheKey)) {
    const cached = MEMORY_CACHE.get(cacheKey);
    if (Date.now() - cached.timestamp < CACHE_TTL) {
      console.log(`[HIT] ${cacheKey}`);
      
      // Update access order for LRU logic (Move to end of Map)
      MEMORY_CACHE.delete(cacheKey);
      MEMORY_CACHE.set(cacheKey, cached);

      res.writeHead(200, { 
        ...cached.headers, 
        "X-Novi-Cache": "HIT-RAM",
        "X-Novi-Node": process.env.NODE_NAME || "US-VA-1" 
      });
      return res.end(cached.body);
    }
    MEMORY_CACHE.delete(cacheKey);
  }

  // 3. PROXY TO ORIGIN
  try {
    console.log(`[MISS] Fetching: ${ORIGIN_BASE}${cacheKey}`);
    const originRes = await fetch(`${ORIGIN_BASE}${cacheKey}`, {
      headers: { "User-Agent": "Novi-Spread-Node/1.0" }
    });

    if (!originRes.ok) {
      res.writeHead(originRes.status);
      return res.end(`Origin Error: ${originRes.statusText}`);
    }

    const arrayBuffer = await originRes.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);

    const headersToMirror = [
      'content-type',
      'content-length',
      'cache-control',
      'last-modified',
      'etag'
    ];

    const responseHeaders = {};
    headersToMirror.forEach(h => {
      const val = originRes.headers.get(h);
      if (val) responseHeaders[h] = val;
    });

    // 4. AUTO-EVICT LOGIC (LRU Strategy)
    // Map in Node.js maintains insertion order. 
    // The first items in the Map are the oldest/least recently used.
    if (MEMORY_CACHE.size >= MAX_MEMORY_ITEMS) {
      const oldestKey = MEMORY_CACHE.keys().next().value;
      console.log(`[AUTO-EVICT] Memory pressure: Removing ${oldestKey}`);
      MEMORY_CACHE.delete(oldestKey);
    }

    // 5. SAVE TO MEMORY
    MEMORY_CACHE.set(cacheKey, {
      body: buffer,
      headers: responseHeaders,
      timestamp: Date.now()
    });

    // 6. SEND RESPONSE
    res.writeHead(200, { 
      ...responseHeaders, 
      "X-Novi-Cache": "MISS",
      "X-Novi-Node": process.env.NODE_NAME || "US-VA-1"
    });
    res.end(buffer);

  } catch (err) {
    console.error(`Proxy Failure: ${err.message}`);
    res.writeHead(502);
    res.end("Gateway Error");
  }
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 Novi-Spread Node Active | Port ${PORT} | Max Items: ${MAX_MEMORY_ITEMS}`);
});
