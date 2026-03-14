const http = require('http');

// Global RAM Cache
const MEMORY_CACHE = new Map();
const CACHE_TTL = 30 * 60 * 1000; // 30 Minutes
const ORIGIN_BASE = "https://storage.novisurf.top";
const PORT = process.env.PORT || 8080;

const server = http.createServer(async (req, res) => {
  const cacheKey = req.url;

  // 1. AWS App Runner Health Check
  if (cacheKey === "/health" || cacheKey === "/") {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    return res.end("OK");
  }

  // 2. RAM Cache Check
  if (MEMORY_CACHE.has(cacheKey)) {
    const cached = MEMORY_CACHE.get(cacheKey);
    if (Date.now() - cached.timestamp < CACHE_TTL) {
      console.log(`[HIT] ${cacheKey}`);
      // Apply all stored origin headers
      res.writeHead(200, { 
        ...cached.headers, 
        "X-Novi-Cache": "HIT-RAM",
        "X-Novi-Node": "US-VA-1" 
      });
      return res.end(cached.body);
    }
    MEMORY_CACHE.delete(cacheKey);
  }

  // 3. Proxy to Origin
  try {
    console.log(`[MISS] Fetching: ${ORIGIN_BASE}${cacheKey}`);
    const originRes = await fetch(`${ORIGIN_BASE}${cacheKey}`, {
      headers: { "User-Agent": "Novi-Spread-Node/1.0" }
    });

    if (!originRes.ok) {
      res.writeHead(originRes.status);
      return res.end(`Origin Error: ${originRes.statusText}`);
    }

    // Capture the data as a Buffer
    const arrayBuffer = await originRes.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);

    // DYNAMIC HEADER CAPTURE
    // We pick the critical ones to mirror exactly
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

    // 4. Save to Memory
    MEMORY_CACHE.set(cacheKey, {
      body: buffer,
      headers: responseHeaders,
      timestamp: Date.now()
    });

    // 5. Send Response
    res.writeHead(200, { 
      ...responseHeaders, 
      "X-Novi-Cache": "MISS",
      "X-Novi-Node": "US-VA-1"
    });
    res.end(buffer);

  } catch (err) {
    console.error(`Proxy Failure: ${err.message}`);
    res.writeHead(502);
    res.end("Gateway Error");
  }
});

// Start the server and bind to 0.0.0.0
server.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 Novi-Spread Regional Node Live on port ${PORT}`);
});
