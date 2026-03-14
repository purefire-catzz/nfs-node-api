/**
 * NOVI-SPREAD: Regional Edge Cache Node
 * Deployment: Deploy to EU and US-Virginia nodes
 */

// Global cache survives between function executions if the isolate stays warm
const MEMORY_CACHE = new Map();
const CACHE_TTL = 30 * 60 * 1000; // 30 Minutes in milliseconds
const ORIGIN_BASE = "https://storage.novisurf.top";

export default async function handler(request) {
  const url = new URL(request.url);
  const cacheKey = url.pathname; // This gets /<bucketid>/<slug>

  // 1. Check Memory Cache
  if (MEMORY_CACHE.has(cacheKey)) {
    const cached = MEMORY_CACHE.get(cacheKey);
    const isExpired = Date.now() - cached.timestamp > CACHE_TTL;

    if (!isExpired) {
      console.log(`[CACHE_HIT]: ${cacheKey}`);
      return new Response(cached.body, {
        headers: {
          ...cached.headers,
          "X-Novi-Cache": "HIT-RAM",
          "X-Novi-Region": "US-VA-1" // Change this per node (EU-1, etc)
        }
      });
    } else {
      MEMORY_CACHE.delete(cacheKey); // Clean up expired
    }
  }

  // 2. Cache Miss: Proxy to Main Origin
  console.log(`[CACHE_MISS]: Fetching from origin for ${cacheKey}`);
  try {
    const originResponse = await fetch(`${ORIGIN_BASE}${cacheKey}`, {
      headers: { "User-Agent": "Novi-Spread-Node/1.0" }
    });

    if (!originResponse.ok) {
      return new Response("Origin Error", { status: originResponse.status });
    }

    // We need the buffer to store it in memory
    const buffer = await originResponse.arrayBuffer();
    
    // Extract useful headers to store
    const responseHeaders = {
      "Content-Type": originResponse.headers.get("Content-Type") || "application/octet-stream",
      "Cache-Control": "public, max-age=3600",
    };

    // 3. Save to Memory
    MEMORY_CACHE.set(cacheKey, {
      body: buffer,
      headers: responseHeaders,
      timestamp: Date.now()
    });

    return new Response(buffer, {
      headers: {
        ...responseHeaders,
        "X-Novi-Cache": "MISS",
        "X-Novi-Region": "US-VA-1"
      }
    });

  } catch (err) {
    return new Response(`Gateway Error: ${err.message}`, { status: 502 });
  }
}
