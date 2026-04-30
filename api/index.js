export const runtime = "nodejs";

const TARGET = (process.env.TARGET_DOMAIN || "").replace(/\/$/, "");

// --- Simple Rate Limit (per instance)
const rateMap = new Map();
const WINDOW = 60 * 1000;
const MAX = 60;

function isAllowed(ip) {
  const now = Date.now();
  const rec = rateMap.get(ip) || { count: 0, time: now };

  if (now - rec.time > WINDOW) {
    rec.count = 0;
    rec.time = now;
  }

  rec.count++;
  rateMap.set(ip, rec);

  return rec.count <= MAX;
}

// --- Clean headers
function cleanHeaders(headers) {
  const result = {};

  for (const [key, value] of Object.entries(headers)) {
    const k = key.toLowerCase();

    if (
      k === "host" ||
      k === "connection" ||
      k === "content-length"
    ) continue;

    if (value) result[k] = value;
  }

  return result;
}

export default async function handler(req, res) {
  if (!TARGET) {
    return res.status(500).end("Server misconfigured");
  }

  // --- IP detection (safe)
  const ip = (req.headers["x-forwarded-for"] || "")
    .split(",")[0]
    .trim() || req.socket?.remoteAddress || "unknown";

  // --- Rate limit
  if (!isAllowed(ip)) {
    return res.status(429).end("Too Many Requests");
  }

  // --- Optional API key protection (strongly recommended)
  if (process.env.API_KEY) {
    if (req.headers["x-api-key"] !== process.env.API_KEY) {
      return res.status(403).end("Forbidden");
    }
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 30000);

  try {
    const url = TARGET + req.url;

    const isBody = !["GET", "HEAD"].includes(req.method);

    const upstream = await fetch(url, {
      method: req.method,
      headers: cleanHeaders(req.headers),
      body: isBody ? req : undefined,
      signal: controller.signal,
      redirect: "manual",
    });

    // --- status
    res.status(upstream.status);

    // --- headers
    upstream.headers.forEach((value, key) => {
      if (
        key.toLowerCase() === "transfer-encoding" ||
        key.toLowerCase() === "connection"
      ) return;

      try {
        res.setHeader(key, value);
      } catch {}
    });

    res.setHeader("x-proxy", "vercel-stable");

    // --- stream response (بدون node:stream)
    if (!upstream.body) {
      return res.end();
    }

    const reader = upstream.body.getReader();

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      res.write(value);
    }

    res.end();

  } catch (err) {
    console.error("proxy error:", err?.message);

    if (!res.headersSent) {
      if (err.name === "AbortError") {
        res.status(504).end("Timeout");
      } else {
        res.status(502).end("Bad Gateway");
      }
    }
  } finally {
    clearTimeout(timeout);
  }
}
