"use strict";

const http = require('http');
const https = require('https');

// --- 1. CONFIGURATION ---
const PORT = process.env.PORT || 8080;
const NODE_NAME = process.env.NODE_NAME || "SYD-NODE-01";
const INTERNAL_AUTH_TOKEN = process.env.INTERNAL_AUTH_TOKEN || "change_me_immediately";
const ORIGIN_BASE = "https://storage.novisurf.top";

const CACHE_TTL = 30 * 60 * 1000; // 30 mins
const MAX_MEMORY_ITEMS = 5000;
const MEMORY_CACHE = new Map();

// --- 2. OPTIMIZATION: CONNECTION POOLING ---
// This keeps the "pipe" to your origin open. 
// Prevents the 100ms handshaking penalty on every MISS.
const originAgent = new https.Agent({
  keepAlive: true,
  maxSockets: 100,
  timeout: 60000
});

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const cacheKey = url.pathname;

  // --- 3. HEALTH & ADMIN ROUTES ---
  if (cacheKey === "/" || cacheKey === "/health") {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    return res.end(`Node ${NODE_NAME} is Online`);
  }

  // --- 4. ADMIN: PURGE LOGIC ---
  const authHeader = req.headers['x-novi-auth'];
  if (cacheKey.startsWith("/purge") || cacheKey.startsWith("/evict")) {
    if (authHeader !== INTERNAL_AUTH_TOKEN) {
      res.writeHead(401); return res.end("Unauthorized");
    }

    if (cacheKey === "/purge/all") {
      MEMORY_CACHE.clear();
      return res.end("Nuked");
    }

    if (cacheKey.startsWith("/purge/")) {
      const bucket = cacheKey.split("/")[2];
      let count = 0;
      for (const [key] of MEMORY_CACHE) {
        if (key.startsWith(`/${bucket}/`)) { MEMORY_CACHE.delete(key); count++; }
      }
      return res.end(`Purged ${count}`);
    }
  }

  // --- 5. THE CACHE ENGINE (LRU Strategy) ---
  if (MEMORY_CACHE.has(cacheKey)) {
    const cached = MEMORY_CACHE.get(cacheKey);
    
    // Check Expiration
    if (Date.now() - cached.timestamp < CACHE_TTL) {
      // Refresh position in Map (Least Recently Used logic)
      MEMORY_CACHE.delete(cacheKey);
      MEMORY_CACHE.set(cacheKey, cached);

      res.writeHead(200, {
        ...cached.headers,
        "X-Novi-Cache": "HIT-RAM",
        "X-Novi-Node": NODE_NAME,
        "Access-Control-Allow-Origin": "*" // Prevent CORS issues
      });
      return res.end(cached.body);
    }
    MEMORY_CACHE.delete(cacheKey);
  }

  // --- 6. ORIGIN PROXY (With Physics Optimization) ---
  try {
    const originResponse = await fetch(`${ORIGIN_BASE}${cacheKey}`, {
      method: 'GET',
      headers: { 
        "User-Agent": `Novi-Spread/${NODE_NAME}`,
        "Connection": "keep-alive"
      },
      agent: originAgent
    });

    if (!originResponse.ok) {
      res.writeHead(originResponse.status);
      return res.end();
    }

    // CRITICAL: Convert streaming response to Buffer safely
    const arrayBuffer = await originResponse.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);

    // --- 7. HEADER CLEANING (Fixes the Sydney MISS loop) ---
    // We strip headers that prevent Cloudflare from caching (like Vary: Cookie)
    // We force a public Cache-Control so the Edge stays primed.
    const cleanHeaders = {
      "Content-Type": originResponse.headers.get("content-type") || "application/octet-stream",
      "Content-Length": buffer.length,
      "Cache-Control": "public, max-age=3600, s-maxage=3600", // Force 1hr Edge Cache
      "ETag": originResponse.headers.get("etag"),
      "Last-Modified": originResponse.headers.get("last-modified"),
      "Access-Control-Allow-Origin": "*",
      "Vary": "Accept-Encoding" // Remove 'Cookie' or 'User-Agent' from Vary
    };

    // --- 8. MEMORY GUARD (Prevent OOM Crashes) ---
    if (MEMORY_CACHE.size >= MAX_MEMORY_ITEMS) {
      const oldestKey = MEMORY_CACHE.keys().next().value;
      MEMORY_CACHE.delete(oldestKey);
    }

    // Store in RAM
    MEMORY_CACHE.set(cacheKey, {
      body: buffer,
      headers: cleanHeaders,
      timestamp: Date.now()
    });

    // --- 9. FINAL RESPONSE ---
    res.writeHead(200, {
      ...cleanHeaders,
      "X-Novi-Cache": "MISS",
      "X-Novi-Node": NODE_NAME
    });
    res.end(buffer);

  } catch (err) {
    console.error(`[ERROR] Proxy Failed: ${err.message}`);
    res.writeHead(502);
    res.end("Origin Gateway Error");
  }
});

// Start Server
server.listen(PORT, '0.0.0.0', () => {
  console.log(`--- NOVI SPREAD NODE ACTIVE ---`);
  console.log(`Node: ${NODE_NAME} | Port: ${PORT}`);
  console.log(`Max Items: ${MAX_MEMORY_ITEMS} | TTL: 30m`);
});
