import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

export const config = {
  api: {
    bodyParser: false,
  },
  supportsResponseStreaming: true,
  maxDuration: 25,
};

/**
 * Upstream target
 */
const TARGET = (process.env.TARGET_DOMAIN || "")
  .trim()
  .replace(/\/$/, "");

/**
 * Basic in-memory rate limiter (per instance)
 */
const rateMap = new Map();

/**
 * Rate limit config
 */
const WINDOW_MS = 60 * 1000;
const MAX_REQ = 60;

/**
 * Check rate limit
 */
function isAllowed(ip) {
  const now = Date.now();
  const record = rateMap.get(ip) || { count: 0, time: now };

  if (now - record.time > WINDOW_MS) {
    record.count = 0;
    record.time = now;
  }

  record.count++;
  rateMap.set(ip, record);

  return record.count <= MAX_REQ;
}

/**
 * Clean headers
 */
function buildHeaders(req) {
  const headers = {};

  for (const k of Object.keys(req.headers)) {
    const key = k.toLowerCase();

    if (
      key.startsWith("x-vercel-") ||
      key === "host" ||
      key === "connection" ||
      key === "transfer-encoding"
    ) {
      continue;
    }

    const val = req.headers[k];
    if (!val) continue;

    headers[key] = Array.isArray(val) ? val.join(",") : val;
  }

  return headers;
}

/**
 * Handler
 */
export default async function handler(req, res) {
  if (!TARGET) {
    return res.status(500).end("Server misconfigured");
  }

  /**
   * Identify client
   */
  const ip =
    req.headers["x-forwarded-for"] ||
    req.socket?.remoteAddress ||
    "unknown";

  /**
   * Rate limit protection
   */
  if (!isAllowed(ip)) {
    return res.status(429).end("Too Many Requests");
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 20000);

  try {
    const url = TARGET + req.url;

    const method = req.method || "GET";

    const isBody = method !== "GET" && method !== "HEAD";

    const upstream = await fetch(url, {
      method,
      headers: buildHeaders(req),
      redirect: "manual",
      signal: controller.signal,
      duplex: isBody ? "half" : undefined,
      body: isBody ? Readable.toWeb(req) : undefined,
    });

    res.statusCode = upstream.status;

    /**
     * Forward safe headers only
     */
    upstream.headers.forEach((value, key) => {
      const k = key.toLowerCase();

      if (
        k === "transfer-encoding" ||
        k === "connection" ||
        k === "content-encoding"
      ) return;

      try {
        res.setHeader(key, value);
      } catch {}
    });

    res.setHeader("x-proxy", "stable-node");

    if (!upstream.body) {
      return res.end();
    }

    await pipeline(
      Readable.fromWeb(upstream.body),
      res
    );

  } catch (err) {
    console.error("[proxy-error]", err?.message);

    if (!res.headersSent) {
      if (err?.name === "AbortError") {
        res.statusCode = 504;
        res.end("Timeout");
      } else {
        res.statusCode = 502;
        res.end("Bad Gateway");
      }
    }
  } finally {
    clearTimeout(timeout);
  }
}
