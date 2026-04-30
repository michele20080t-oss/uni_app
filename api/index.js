export const runtime = "nodejs";

export const config = {
  api: { bodyParser: false },
  maxDuration: 60,
};

const TARGET_BASE = (process.env.TARGET_DOMAIN || "").replace(/\/$/, "");
const API_KEY = process.env.API_KEY || "";

// اختیاری: محدودسازی مسیرها (مثلاً "/v1,/public")
const ALLOW_PREFIX = (process.env.ALLOW_PATH_PREFIX || "")
  .split(",")
  .map(s => s.trim())
  .filter(Boolean);

// هدرهایی که نباید فوروارد بشن
const STRIP_HEADERS = new Set([
  "host",
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
  "forwarded",
  "x-forwarded-host",
  "x-forwarded-proto",
  "x-forwarded-port",
]);

// --- Rate limit ساده (per instance)
const RATE = new Map();
const WINDOW = 60 * 1000; // 1 دقیقه
const MAX = 60;

function getIP(req) {
  const xf = (req.headers["x-forwarded-for"] || "")
    .split(",")[0]
    .trim();
  return xf || req.socket?.remoteAddress || "unknown";
}

function isAllowedIP(ip) {
  const now = Date.now();
  const rec = RATE.get(ip) || { count: 0, time: now };

  if (now - rec.time > WINDOW) {
    rec.count = 0;
    rec.time = now;
  }

  rec.count++;
  RATE.set(ip, rec);

  return rec.count <= MAX;
}

function isAllowedPath(path) {
  if (ALLOW_PREFIX.length === 0) return true;
  return ALLOW_PREFIX.some(p => path.startsWith(p));
}

export default async function handler(req, res) {
  if (!TARGET_BASE || !/^https:\/\//i.test(TARGET_BASE)) {
    res.statusCode = 500;
    return res.end("Misconfigured: TARGET_DOMAIN");
  }

  // --- Auth (جلوگیری از open proxy)
  if (API_KEY && req.headers["x-api-key"] !== API_KEY) {
    res.statusCode = 403;
    return res.end("Forbidden");
  }

  const ip = getIP(req);

  // --- Rate limit
  if (!isAllowedIP(ip)) {
    res.statusCode = 429;
    return res.end("Too Many Requests");
  }

  // --- Path allow-list
  if (!isAllowedPath(req.url || "/")) {
    res.statusCode = 403;
    return res.end("Path not allowed");
  }

  try {
    const targetUrl = TARGET_BASE + req.url;

    // --- header handling (همان منطق قبلی)
    const headers = {};
    let clientIp = null;

    for (const key of Object.keys(req.headers)) {
      const k = key.toLowerCase();
      const v = req.headers[key];

      if (STRIP_HEADERS.has(k)) continue;
      if (k.startsWith("x-vercel-")) continue;

      if (k === "x-real-ip") {
        clientIp = v;
        continue;
      }

      if (k === "x-forwarded-for") {
        if (!clientIp) clientIp = v;
        continue;
      }

      headers[k] = Array.isArray(v) ? v.join(", ") : v;
    }

    if (clientIp) {
      headers["x-forwarded-for"] = String(clientIp)
        .split(",")[0]
        .trim();
    }

    const method = req.method || "GET";
    const hasBody = method !== "GET" && method !== "HEAD";

    const upstream = await fetch(targetUrl, {
      method,
      headers,
      body: hasBody ? req : undefined,
      redirect: "manual",
    });

    // --- status
    res.statusCode = upstream.status;

    // --- headers
    upstream.headers.forEach((v, k) => {
      if (k.toLowerCase() === "transfer-encoding") return;
      try { res.setHeader(k, v); } catch {}
    });

    res.setHeader("x-proxy", "vercel-optimized");
    res.setHeader("x-proxy-ip", ip);

    // --- stream (معادل pipeline)
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
    console.error("relay error:", err?.message);

    if (!res.headersSent) {
      res.statusCode = 502;
      res.end("Bad Gateway");
    }
  }
}
