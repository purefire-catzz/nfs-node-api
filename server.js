"use strict";

// ============================================================
//  NOVISURF SPREAD NODE v3
//  Cache layers: RAM (1GB/30min) → Disk (ephemeral/6hr)
//                → Peer mesh → Origin
//  Disk errors are silently swallowed — node never crashes
//  on OS/disk failures, falls back to RAM-only mode.
// ============================================================

const http    = require("http");
const fs      = require("fs");
const fsp     = require("fs/promises");
const path    = require("path");
const crypto  = require("crypto");
const { Pool } = require("undici");

// ── 1. CONFIGURATION ────────────────────────────────────────

const PORT                = process.env.PORT                || 8080;
const NODE_NAME           = process.env.NODE_NAME           || "NODE-01";
const INTERNAL_AUTH_TOKEN = process.env.INTERNAL_AUTH_TOKEN || "change_me_immediately";
const ORIGIN_BASE         = process.env.ORIGIN_BASE         || "https://storage.novisurf.top";
const PEER_AUTH_TOKEN     = process.env.PEER_AUTH_TOKEN     || INTERNAL_AUTH_TOKEN;
const PEER_URLS           = process.env.PEERS
  ? process.env.PEERS.split(",").map(s => s.trim()).filter(Boolean)
  : [];

// RAM cache
const RAM_TTL             = 30 * 60 * 1000;       // 30 min fresh
const RAM_STALE_TTL       = 60 * 60 * 1000;       // +60 min stale window
const MAX_RAM_BYTES       = 1  * 1024 ** 3;       // 1 GB
const MAX_ITEM_BYTES      = 50 * 1024 ** 2;       // 50 MB per item cap

// Disk cache
const DISK_TTL            = 6  * 60 * 60 * 1000; // 6 hours
const DISK_CACHE_DIR      = process.env.DISK_CACHE_DIR || path.join(process.cwd(), "tmp", "cache");
const MAX_DISK_BYTES      = 10 * 1024 ** 3;       // 10 GB soft cap
const DISK_SWEEP_INTERVAL = 15 * 60 * 1000;       // sweep expired files every 15 min

// Peer
const PEER_TIMEOUT_MS     = 800;

// ── 2. DISK CACHE SETUP ──────────────────────────────────────
// Best-effort — if mkdir fails we just run diskless

let diskAvailable = false;

async function initDisk() {
  try {
    await fsp.mkdir(DISK_CACHE_DIR, { recursive: true });
    // Wipe everything on startup — treat as always cold
    const files = await fsp.readdir(DISK_CACHE_DIR);
    await Promise.all(files.map(f => fsp.unlink(path.join(DISK_CACHE_DIR, f)).catch(() => {})));
    diskAvailable = true;
    console.log(` Disk cache : ${DISK_CACHE_DIR} (${(MAX_DISK_BYTES / 1024 ** 3).toFixed(0)}GB cap)`);
  } catch (err) {
    diskAvailable = false;
    console.warn(` Disk cache unavailable — RAM-only mode: ${err.message}`);
  }
}

// Deterministic filename from cache key
// Uses SHA1 hex so any path is safe as a filename
function diskFilename(cacheKey) {
  const hash = crypto.createHash("sha1").update(cacheKey).digest("hex");
  return path.join(DISK_CACHE_DIR, `${hash}.bin`);
}

// Metadata file alongside the binary — stores headers + timestamps
function diskMetaFilename(cacheKey) {
  const hash = crypto.createHash("sha1").update(cacheKey).digest("hex");
  return path.join(DISK_CACHE_DIR, `${hash}.meta.json`);
}

// ── 3. DISK CACHE OPS (all silent on error) ─────────────────

async function diskGet(cacheKey) {
  if (!diskAvailable) return null;
  try {
    const metaPath = diskMetaFilename(cacheKey);
    const bodyPath = diskFilename(cacheKey);

    const metaRaw = await fsp.readFile(metaPath, "utf8");
    const meta    = JSON.parse(metaRaw);

    const age   = Date.now() - meta.timestamp;
    if (age > DISK_TTL) {
      // Expired — clean up silently
      fsp.unlink(metaPath).catch(() => {});
      fsp.unlink(bodyPath).catch(() => {});
      return null;
    }

    const body = await fsp.readFile(bodyPath);
    return { body, headers: meta.headers, timestamp: meta.timestamp };
  } catch {
    return null; // File not found or any OS error — silent miss
  }
}

async function diskSet(cacheKey, body, headers) {
  if (!diskAvailable) return;
  if (body.length > MAX_ITEM_BYTES) return; // Don't write huge objects to disk either

  try {
    const metaPath = diskMetaFilename(cacheKey);
    const bodyPath = diskFilename(cacheKey);
    const meta     = { headers, timestamp: Date.now(), size: body.length };

    // Write body + meta in parallel
    await Promise.all([
      fsp.writeFile(bodyPath, body),
      fsp.writeFile(metaPath, JSON.stringify(meta)),
    ]);
  } catch {
    // Disk full, permission error, anything — silently skip
  }
}

async function diskDelete(cacheKey) {
  if (!diskAvailable) return;
  try {
    await Promise.all([
      fsp.unlink(diskFilename(cacheKey)).catch(() => {}),
      fsp.unlink(diskMetaFilename(cacheKey)).catch(() => {}),
    ]);
  } catch { /* silent */ }
}

// ── 4. DISK SWEEP (evict expired + enforce size cap) ─────────

async function sweepDisk() {
  if (!diskAvailable) return;
  try {
    const files = await fsp.readdir(DISK_CACHE_DIR);
    const metaFiles = files.filter(f => f.endsWith(".meta.json"));

    let totalBytes = 0;
    const entries  = [];

    for (const mf of metaFiles) {
      try {
        const raw  = await fsp.readFile(path.join(DISK_CACHE_DIR, mf), "utf8");
        const meta = JSON.parse(raw);
        const age  = Date.now() - meta.timestamp;

        if (age > DISK_TTL) {
          // Expired — delete both files
          const hash = mf.replace(".meta.json", "");
          fsp.unlink(path.join(DISK_CACHE_DIR, `${hash}.bin`)).catch(() => {});
          fsp.unlink(path.join(DISK_CACHE_DIR, mf)).catch(() => {});
          continue;
        }

        totalBytes += meta.size || 0;
        entries.push({ mf, meta, timestamp: meta.timestamp });
      } catch { /* corrupt meta — skip */ }
    }

    // If over size cap, evict oldest first
    if (totalBytes > MAX_DISK_BYTES) {
      entries.sort((a, b) => a.timestamp - b.timestamp);
      for (const e of entries) {
        if (totalBytes <= MAX_DISK_BYTES * 0.8) break; // evict down to 80%
        const hash = e.mf.replace(".meta.json", "");
        fsp.unlink(path.join(DISK_CACHE_DIR, `${hash}.bin`)).catch(() => {});
        fsp.unlink(path.join(DISK_CACHE_DIR, e.mf)).catch(() => {});
        totalBytes -= (e.meta.size || 0);
      }
    }
  } catch { /* sweep failure is non-fatal */ }
}

// ── 5. RAM CACHE ─────────────────────────────────────────────

const MEMORY_CACHE = new Map();
let   currentBytes = 0;

function ramGet(key) {
  if (!MEMORY_CACHE.has(key)) return null;
  const entry = MEMORY_CACHE.get(key);
  const age   = Date.now() - entry.timestamp;
  const fresh = age < RAM_TTL;
  const stale = !fresh && age < RAM_TTL + RAM_STALE_TTL;

  if (!fresh && !stale) {
    MEMORY_CACHE.delete(key);
    currentBytes -= entry.size;
    return null;
  }

  // Refresh LRU position
  MEMORY_CACHE.delete(key);
  MEMORY_CACHE.set(key, entry);
  return { entry, fresh, stale };
}

function ramSet(key, body, headers) {
  const size = body.length;
  if (size > MAX_ITEM_BYTES) return;

  // Evict LRU entries until room
  while (currentBytes + size > MAX_RAM_BYTES && MEMORY_CACHE.size > 0) {
    const oldestKey   = MEMORY_CACHE.keys().next().value;
    const oldestEntry = MEMORY_CACHE.get(oldestKey);
    MEMORY_CACHE.delete(oldestKey);
    currentBytes -= oldestEntry.size;
  }

  if (MEMORY_CACHE.has(key)) {
    currentBytes -= MEMORY_CACHE.get(key).size;
    MEMORY_CACHE.delete(key);
  }

  MEMORY_CACHE.set(key, { body, headers, timestamp: Date.now(), size });
  currentBytes += size;
}

function ramPurgeBucket(bucket) {
  let count = 0, freed = 0;
  for (const [key, entry] of MEMORY_CACHE) {
    if (key.startsWith(`/${bucket}/`)) {
      freed += entry.size;
      MEMORY_CACHE.delete(key);
      currentBytes -= entry.size;
      count++;
    }
  }
  return { count, freed };
}

// ── 6. CONNECTION POOLS ──────────────────────────────────────

const originPool = new Pool(ORIGIN_BASE, {
  connections:         50,
  pipelining:          1,
  keepAliveTimeout:    60_000,
  keepAliveMaxTimeout: 600_000,
  connect: { rejectUnauthorized: true },
});

const peerPools = new Map();
function getPeerPool(baseUrl) {
  if (!peerPools.has(baseUrl)) {
    peerPools.set(baseUrl, new Pool(baseUrl, {
      connections:      10,
      pipelining:       1,
      keepAliveTimeout: 30_000,
      connect: { rejectUnauthorized: true },
    }));
  }
  return peerPools.get(baseUrl);
}

// ── 7. HEADER SANITIZER ──────────────────────────────────────

function sanitizeHeaders(raw, contentLength) {
  return {
    "content-type":                raw["content-type"]    || "application/octet-stream",
    "content-length":              String(contentLength),
    "cache-control":               "public, max-age=3600, s-maxage=3600, stale-while-revalidate=1800",
    "etag":                        raw["etag"]             || "",
    "last-modified":               raw["last-modified"]    || "",
    "access-control-allow-origin": "*",
    "vary":                        "Accept-Encoding",
    "x-novi-node":                 NODE_NAME,
  };
}

// ── 8. PEER FETCH ────────────────────────────────────────────

async function fetchFromPeers(cacheKey) {
  for (const peerBase of PEER_URLS) {
    try {
      const { statusCode, headers, body } = await getPeerPool(peerBase).request({
        path:    cacheKey,
        method:  "GET",
        headers: {
          "x-novi-auth":    PEER_AUTH_TOKEN,
          "x-peer-request": "1",
          "user-agent":     `Novi-Peer/${NODE_NAME}`,
        },
        headersTimeout: PEER_TIMEOUT_MS,
        bodyTimeout:    PEER_TIMEOUT_MS * 5,
      });

      if (statusCode !== 200) continue;

      const chunks = [];
      for await (const chunk of body) chunks.push(chunk);
      const buffer = Buffer.concat(chunks);
      return { body: buffer, headers: sanitizeHeaders(headers, buffer.length) };
    } catch {
      continue; // Peer timeout or error — try next
    }
  }
  return null;
}

// ── 9. ORIGIN FETCH ──────────────────────────────────────────

async function fetchFromOrigin(cacheKey) {
  const { statusCode, headers, body } = await originPool.request({
    path:    cacheKey,
    method:  "GET",
    headers: {
      "user-agent":      `Novi-Spread/${NODE_NAME}`,
      "accept-encoding": "gzip, br",
    },
  });

  if (statusCode < 200 || statusCode >= 300) {
    for await (const _ of body) {} // drain to free connection
    const err = new Error(`Origin ${statusCode}`);
    err.statusCode = statusCode;
    throw err;
  }

  const chunks = [];
  for await (const chunk of body) chunks.push(chunk);
  const buffer = Buffer.concat(chunks);
  return { body: buffer, headers: sanitizeHeaders(headers, buffer.length) };
}

// ── 10. REQUEST COALESCING ───────────────────────────────────

const inFlight = new Map();

function resolveObject(cacheKey, isPeerRequest) {
  if (inFlight.has(cacheKey)) return inFlight.get(cacheKey);

  const promise = (async () => {
    try {
      // Peers first (skip if this is already a peer request)
      if (!isPeerRequest && PEER_URLS.length > 0) {
        const peerResult = await fetchFromPeers(cacheKey);
        if (peerResult) {
          ramSet(cacheKey, peerResult.body, peerResult.headers);
          diskSet(cacheKey, peerResult.body, peerResult.headers); // fire and forget
          return peerResult;
        }
      }

      // Origin
      const result = await fetchFromOrigin(cacheKey);
      ramSet(cacheKey, result.body, result.headers);
      diskSet(cacheKey, result.body, result.headers); // fire and forget
      return result;

    } finally {
      inFlight.delete(cacheKey);
    }
  })();

  inFlight.set(cacheKey, promise);
  return promise;
}

// ── 11. HTTP SERVER ──────────────────────────────────────────

const server = http.createServer(async (req, res) => {
  const url      = new URL(req.url, `http://${req.headers.host}`);
  const cacheKey = url.pathname;
  const auth     = req.headers["x-novi-auth"];

  // ── Health ────────────────────────────────────────────────
  if (cacheKey === "/" || cacheKey === "/health") {
    res.writeHead(200, { "content-type": "application/json" });
    return res.end(JSON.stringify({
      node:        NODE_NAME,
      status:      "online",
      disk:        diskAvailable,
      ramItems:    MEMORY_CACHE.size,
      ramMB:       (currentBytes / 1024 ** 2).toFixed(1),
      inFlight:    inFlight.size,
      peers:       PEER_URLS.length,
    }));
  }

  // ── Purge ─────────────────────────────────────────────────
  if (cacheKey.startsWith("/purge") || cacheKey.startsWith("/evict")) {
    if (auth !== INTERNAL_AUTH_TOKEN) {
      res.writeHead(401); return res.end("Unauthorized");
    }

    if (cacheKey === "/purge/all") {
      MEMORY_CACHE.clear();
      currentBytes = 0;
      // Wipe disk too
      if (diskAvailable) {
        fsp.readdir(DISK_CACHE_DIR)
          .then(files => Promise.all(files.map(f => fsp.unlink(path.join(DISK_CACHE_DIR, f)).catch(() => {}))))
          .catch(() => {});
      }
      res.writeHead(200);
      return res.end(`Nuked all cache on ${NODE_NAME}`);
    }

    if (cacheKey.startsWith("/purge/")) {
      const bucket = cacheKey.split("/")[2];
      const { count, freed } = ramPurgeBucket(bucket);
      // Disk purge by bucket is harder without an index — sweep will clean expired
      res.writeHead(200);
      return res.end(`Purged ${count} RAM items (${(freed / 1024 ** 2).toFixed(1)}MB) from ${NODE_NAME}`);
    }
  }

  // ── Method guard ─────────────────────────────────────────
  if (req.method !== "GET" && req.method !== "HEAD") {
    res.writeHead(405); return res.end("Method Not Allowed");
  }

  // ── Peer request auth ────────────────────────────────────
  const isPeerRequest = req.headers["x-peer-request"] === "1";
  if (isPeerRequest && auth !== PEER_AUTH_TOKEN) {
    res.writeHead(401); return res.end("Unauthorized peer request");
  }

  // ── LAYER 1: RAM ─────────────────────────────────────────
  const ramHit = ramGet(cacheKey);
  if (ramHit) {
    const { entry, fresh, stale } = ramHit;

    if (stale) {
      // Refresh in background — don't block client
      resolveObject(cacheKey, isPeerRequest).catch(() => {});
    }

    res.writeHead(200, {
      ...entry.headers,
      "x-novi-cache": fresh ? "HIT-RAM" : "HIT-RAM-STALE",
      "age": String(Math.floor((Date.now() - entry.timestamp) / 1000)),
    });
    return res.end(entry.body);
  }

  // ── LAYER 2: DISK ─────────────────────────────────────────
  const diskHit = await diskGet(cacheKey);
  if (diskHit) {
    // Warm RAM from disk hit so next request is even faster
    ramSet(cacheKey, diskHit.body, diskHit.headers);

    res.writeHead(200, {
      ...diskHit.headers,
      "x-novi-cache": "HIT-DISK",
      "age": String(Math.floor((Date.now() - diskHit.timestamp) / 1000)),
    });
    return res.end(diskHit.body);
  }

  // ── LAYER 3+4: PEER MESH → ORIGIN ────────────────────────
  try {
    const result = await resolveObject(cacheKey, isPeerRequest);

    res.writeHead(200, {
      ...result.headers,
      "x-novi-cache": "MISS",
    });
    res.end(result.body);

  } catch (err) {
    console.error(`[${NODE_NAME}] MISS failed ${cacheKey}: ${err.message}`);
    const status = err.statusCode || 502;
    res.writeHead(status);
    res.end(status === 404 ? "Not Found" : "Origin Gateway Error");
  }
});

// ── 12. STARTUP ──────────────────────────────────────────────

async function start() {
  await initDisk();

  // Periodic disk sweep
  if (diskAvailable) {
    setInterval(sweepDisk, DISK_SWEEP_INTERVAL);
  }

  server.listen(PORT, "0.0.0.0", () => {
    console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    console.log(` NOVI SPREAD NODE v3 — ${NODE_NAME}`);
    console.log(` Port     : ${PORT}`);
    console.log(` Origin   : ${ORIGIN_BASE}`);
    console.log(` RAM cap  : ${(MAX_RAM_BYTES  / 1024 ** 2).toFixed(0)}MB | TTL: 30min + 60min stale`);
    console.log(` Disk cap : ${(MAX_DISK_BYTES / 1024 ** 3).toFixed(0)}GB  | TTL: 6hr`);
    console.log(` Peers    : ${PEER_URLS.length ? PEER_URLS.join(", ") : "none"}`);
    console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
  });
}

// ── 13. GRACEFUL SHUTDOWN ────────────────────────────────────

process.on("SIGTERM", () => {
  server.close(async () => {
    originPool.destroy();
    for (const pool of peerPools.values()) pool.destroy();
    process.exit(0);
  });
});

process.on("SIGINT", () => {
  server.close(async () => {
    originPool.destroy();
    for (const pool of peerPools.values()) pool.destroy();
    process.exit(0);
  });
});

start();
