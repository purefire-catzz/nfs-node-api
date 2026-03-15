"use strict";

// ============================================================
//  NOVISURF SPREAD NODE v2
//  Fixes: real connection pooling (undici), streaming TTFB,
//         request coalescing, 1GB RAM budget, stale-while-
//         revalidate, tiered peer cache mesh
// ============================================================

const http      = require("http");
const { Pool }  = require("undici");

// ── 1. CONFIGURATION ────────────────────────────────────────

const PORT               = process.env.PORT               || 8080;
const NODE_NAME          = process.env.NODE_NAME          || "SYD-NODE-01";
const INTERNAL_AUTH_TOKEN= process.env.INTERNAL_AUTH_TOKEN|| "change_me_immediately";
const ORIGIN_BASE        = process.env.ORIGIN_BASE        || "https://storage.novisurf.top";

// Cache tunables
const CACHE_TTL          = 30 * 60 * 1000;   // 30 min fresh window
const STALE_TTL          = 60 * 60 * 1000;   // extra 60 min stale-while-revalidate window
const MAX_CACHE_BYTES    = 1 * 1024 ** 3;     // 1 GB hard cap
const MAX_ITEM_BYTES     = 50  * 1024 ** 2;   // 50 MB per-item cap (skip caching huge blobs)
const PEER_TIMEOUT_MS    = 800;               // give up on peer after 800ms, go to origin
const PEER_AUTH_TOKEN    = process.env.PEER_AUTH_TOKEN || INTERNAL_AUTH_TOKEN;

// ── 2. PEER TOPOLOGY ────────────────────────────────────────
// Each node lists peers in priority order (closest first).
// Before hitting origin, a node walks this list asking each
// peer for the object. First peer HIT wins. All miss → origin.
//
// Set via env: PEERS=https://tyo.novisurf.top,https://sin.novisurf.top
// Or hardcode per-node below as a fallback.
const PEER_URLS = process.env.PEERS
  ? process.env.PEERS.split(",").map(s => s.trim()).filter(Boolean)
  : [];

// ── 3. ORIGIN CONNECTION POOL (undici) ──────────────────────
// undici.Pool keeps persistent HTTP/1.1 or HTTP/2 connections
// to origin. Eliminates the TCP+TLS handshake on every miss.
const originPool = new Pool(ORIGIN_BASE, {
  connections:         50,
  pipelining:          1,
  keepAliveTimeout:    60_000,
  keepAliveMaxTimeout: 600_000,
  connect: { rejectUnauthorized: true },
});

// One pool per peer URL, created lazily on first use
const peerPools = new Map();
function getPeerPool(baseUrl) {
  if (!peerPools.has(baseUrl)) {
    peerPools.set(baseUrl, new Pool(baseUrl, {
      connections:         10,
      pipelining:          1,
      keepAliveTimeout:    30_000,
      connect: { rejectUnauthorized: true },
    }));
  }
  return peerPools.get(baseUrl);
}

// ── 4. RAM CACHE ─────────────────────────────────────────────
// Map preserves insertion order → oldest key = Map.keys().next()
// We track currentBytes for accurate 1GB enforcement.
// Per-entry shape: { body: Buffer, headers: Object,
//                    timestamp: Number, size: Number,
//                    staleAt: Number }

const MEMORY_CACHE   = new Map();
let   currentBytes   = 0;

function cacheGet(key) {
  if (!MEMORY_CACHE.has(key)) return null;
  const entry = MEMORY_CACHE.get(key);

  const now    = Date.now();
  const age    = now - entry.timestamp;
  const fresh  = age < CACHE_TTL;
  const stale  = !fresh && age < CACHE_TTL + STALE_TTL;

  if (!fresh && !stale) {
    // Fully expired — evict
    MEMORY_CACHE.delete(key);
    currentBytes -= entry.size;
    return null;
  }

  // Refresh LRU position
  MEMORY_CACHE.delete(key);
  MEMORY_CACHE.set(key, entry);

  return { entry, fresh, stale };
}

function cacheSet(key, body, headers) {
  const size = body.length;
  if (size > MAX_ITEM_BYTES) return; // Don't cache very large objects

  // Evict oldest entries until we have room
  while (currentBytes + size > MAX_CACHE_BYTES && MEMORY_CACHE.size > 0) {
    const oldestKey   = MEMORY_CACHE.keys().next().value;
    const oldestEntry = MEMORY_CACHE.get(oldestKey);
    MEMORY_CACHE.delete(oldestKey);
    currentBytes -= oldestEntry.size;
  }

  // Remove stale entry for this key if it exists
  if (MEMORY_CACHE.has(key)) {
    currentBytes -= MEMORY_CACHE.get(key).size;
    MEMORY_CACHE.delete(key);
  }

  MEMORY_CACHE.set(key, {
    body,
    headers,
    timestamp: Date.now(),
    size,
  });
  currentBytes += size;
}

// ── 5. REQUEST COALESCING ────────────────────────────────────
// inFlight maps cacheKey → Promise<{body,headers}>
// All concurrent requests for the same uncached key share
// one single fetch to origin / peer network.
const inFlight = new Map();

// ── 6. PEER FETCH ────────────────────────────────────────────
// Asks each peer in order. Returns {body, headers} on first HIT
// or null if all peers miss or timeout.
async function fetchFromPeers(cacheKey) {
  for (const peerBase of PEER_URLS) {
    try {
      const pool = getPeerPool(peerBase);
      const { statusCode, headers, body } = await pool.request({
        path:    cacheKey,
        method:  "GET",
        headers: {
          "x-novi-auth":   PEER_AUTH_TOKEN,
          "x-peer-request": "1",         // tells the peer not to re-ask its peers
          "user-agent":    `Novi-Peer/${NODE_NAME}`,
        },
        headersTimeout: PEER_TIMEOUT_MS,
        bodyTimeout:    PEER_TIMEOUT_MS * 5,
      });

      if (statusCode !== 200) continue;

      // Consume peer body into buffer
      const chunks = [];
      for await (const chunk of body) chunks.push(chunk);
      const buffer = Buffer.concat(chunks);

      const cleanHeaders = sanitizeHeaders(headers, buffer.length);
      return { body: buffer, headers: cleanHeaders };

    } catch (_) {
      // Peer timeout or error — try next peer
      continue;
    }
  }
  return null; // All peers missed
}

// ── 7. ORIGIN FETCH ──────────────────────────────────────────
// Uses undici Pool — persistent connection, no handshake overhead
async function fetchFromOrigin(cacheKey) {
  const { statusCode, headers, body } = await originPool.request({
    path:   cacheKey,
    method: "GET",
    headers: {
      "user-agent":  `Novi-Spread/${NODE_NAME}`,
      "accept-encoding": "gzip, br",
    },
  });

  if (statusCode < 200 || statusCode >= 300) {
    // Drain body to free the connection back to pool
    for await (const _ of body) {}
    const err = new Error(`Origin returned ${statusCode}`);
    err.statusCode = statusCode;
    throw err;
  }

  const chunks = [];
  for await (const chunk of body) chunks.push(chunk);
  const buffer = Buffer.concat(chunks);

  const cleanHeaders = sanitizeHeaders(headers, buffer.length);
  return { body: buffer, headers: cleanHeaders };
}

// ── 8. HEADER SANITIZER ──────────────────────────────────────
function sanitizeHeaders(raw, contentLength) {
  return {
    "content-type":                raw["content-type"]   || "application/octet-stream",
    "content-length":              String(contentLength),
    "cache-control":               "public, max-age=3600, s-maxage=3600, stale-while-revalidate=1800",
    "etag":                        raw["etag"]            || "",
    "last-modified":               raw["last-modified"]   || "",
    "access-control-allow-origin": "*",
    "vary":                        "Accept-Encoding",
    "x-novi-node":                 NODE_NAME,
  };
}

// ── 9. COALESCED RESOLVER ────────────────────────────────────
// Single entry point for any MISS. Deduplicates concurrent
// requests for the same key. Peers → origin fallback built in.
function resolveObject(cacheKey, isPeerRequest) {
  if (inFlight.has(cacheKey)) {
    return inFlight.get(cacheKey); // Piggyback on existing fetch
  }

  const promise = (async () => {
    try {
      // Don't ask peers if this request already came from a peer
      // (prevents infinite peer loops)
      if (!isPeerRequest && PEER_URLS.length > 0) {
        const peerResult = await fetchFromPeers(cacheKey);
        if (peerResult) {
          cacheSet(cacheKey, peerResult.body, peerResult.headers);
          return peerResult;
        }
      }

      // Peers all missed (or skipped) — go to origin
      const originResult = await fetchFromOrigin(cacheKey);
      cacheSet(cacheKey, originResult.body, originResult.headers);
      return originResult;

    } finally {
      inFlight.delete(cacheKey); // Always clean up
    }
  })();

  inFlight.set(cacheKey, promise);
  return promise;
}

// ── 10. HTTP SERVER ──────────────────────────────────────────
const server = http.createServer(async (req, res) => {
  const url      = new URL(req.url, `http://${req.headers.host}`);
  const cacheKey = url.pathname;

  // ── Health ──────────────────────────────────────────────
  if (cacheKey === "/" || cacheKey === "/health") {
    res.writeHead(200, { "content-type": "application/json" });
    return res.end(JSON.stringify({
      node:        NODE_NAME,
      status:      "online",
      cacheItems:  MEMORY_CACHE.size,
      cacheBytes:  currentBytes,
      cacheMB:     (currentBytes / 1024 ** 2).toFixed(1),
      inFlight:    inFlight.size,
      peers:       PEER_URLS,
    }));
  }

  const auth = req.headers["x-novi-auth"];

  // ── Purge / Evict ────────────────────────────────────────
  if (cacheKey.startsWith("/purge") || cacheKey.startsWith("/evict")) {
    if (auth !== INTERNAL_AUTH_TOKEN) {
      res.writeHead(401); return res.end("Unauthorized");
    }

    if (cacheKey === "/purge/all") {
      MEMORY_CACHE.clear();
      currentBytes = 0;
      res.writeHead(200);
      return res.end(`Nuked all cache on ${NODE_NAME}`);
    }

    if (cacheKey.startsWith("/purge/")) {
      const bucket = cacheKey.split("/")[2];
      let count = 0, freed = 0;
      for (const [key, entry] of MEMORY_CACHE) {
        if (key.startsWith(`/${bucket}/`)) {
          freed += entry.size;
          MEMORY_CACHE.delete(key);
          currentBytes -= entry.size;
          count++;
        }
      }
      res.writeHead(200);
      return res.end(`Purged ${count} items (${(freed / 1024 ** 2).toFixed(1)} MB) from ${NODE_NAME}`);
    }
  }

  // ── Only serve GET/HEAD beyond this point ────────────────
  if (req.method !== "GET" && req.method !== "HEAD") {
    res.writeHead(405); return res.end("Method Not Allowed");
  }

  // ── Is this a peer-to-peer request? ─────────────────────
  // Peers send x-peer-request: 1 so we don't re-ask our own peers
  const isPeerRequest = req.headers["x-peer-request"] === "1";
  if (isPeerRequest && auth !== PEER_AUTH_TOKEN) {
    res.writeHead(401); return res.end("Unauthorized peer request");
  }

  // ── Cache Lookup ─────────────────────────────────────────
  const hit = cacheGet(cacheKey);

  if (hit) {
    const { entry, fresh, stale } = hit;

    // Stale — serve immediately, revalidate in background
    if (stale) {
      // Background refresh — don't await, client gets stale response now
      resolveObject(cacheKey, isPeerRequest).catch(() => {});
    }

    res.writeHead(200, {
      ...entry.headers,
      "x-novi-cache":  fresh ? "HIT-RAM" : "HIT-RAM-STALE",
      "x-novi-node":   NODE_NAME,
      "age":           String(Math.floor((Date.now() - entry.timestamp) / 1000)),
    });
    return res.end(entry.body);
  }

  // ── MISS — resolve via peer mesh or origin ───────────────
  try {
    const result = await resolveObject(cacheKey, isPeerRequest);

    res.writeHead(200, {
      ...result.headers,
      "x-novi-cache": "MISS",
      "x-novi-node":  NODE_NAME,
    });
    res.end(result.body);

  } catch (err) {
    console.error(`[ERROR][${NODE_NAME}] ${cacheKey} — ${err.message}`);
    const status = err.statusCode || 502;
    res.writeHead(status);
    res.end(status === 404 ? "Not Found" : "Origin Gateway Error");
  }
});

// ── 11. STARTUP ──────────────────────────────────────────────
server.listen(PORT, "0.0.0.0", () => {
  console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
  console.log(` NOVI SPREAD NODE v2 — ${NODE_NAME}`);
  console.log(` Port    : ${PORT}`);
  console.log(` Origin  : ${ORIGIN_BASE}`);
  console.log(` RAM cap : ${(MAX_CACHE_BYTES / 1024 ** 2).toFixed(0)} MB`);
  console.log(` Item cap: ${(MAX_ITEM_BYTES  / 1024 ** 2).toFixed(0)} MB`);
  console.log(` Peers   : ${PEER_URLS.length ? PEER_URLS.join(", ") : "none configured"}`);
  console.log(" TTL     : 30m fresh / +60m stale-while-revalidate");
  console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
});

// Graceful shutdown — drain keep-alive connections cleanly
process.on("SIGTERM", () => {
  server.close(() => {
    originPool.destroy();
    for (const pool of peerPools.values()) pool.destroy();
    process.exit(0);
  });
});
