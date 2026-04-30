// Import Node.js stream utilities for handling streaming data
import { Readable } from "node:stream";
// Pipeline utility for safely piping streams with proper error handling
import { pipeline } from "node:stream/promises";

// API configuration (used in frameworks like Next.js / Vercel)
export const config = {
  // Disable built-in body parser to allow raw stream handling
  api: { bodyParser: false },

  // Enable streaming responses to client
  supportsResponseStreaming: true,

  // Maximum execution time for this API route (in seconds)
  maxDuration: 60,
};

// Base URL of the upstream server (proxy target)
// Trailing slash is removed to avoid malformed URLs
const TARGET_BASE = (process.env.TARGET_DOMAIN || "").replace(/\/$/, "");

// List of HTTP headers that should NOT be forwarded to upstream server
// These are removed to prevent proxy loops and protocol conflicts
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

// Main API handler function (acts as a reverse proxy)
export default async function handler(req, res) {
  // Ensure target server is configured
  if (!TARGET_BASE) {
    res.statusCode = 500;
    return res.end("Misconfigured: TARGET_DOMAIN is not set");
  }

  try {
    // Construct full upstream URL using incoming request path
    const targetUrl = TARGET_BASE + req.url;

    // Object that will contain sanitized headers for upstream request
    const headers = {};

    // Variable to store client IP address if available
    let clientIp = null;

    // Iterate over all incoming request headers
    for (const key of Object.keys(req.headers)) {
      const k = key.toLowerCase();
      const v = req.headers[key];

      // Skip headers that should not be forwarded
      if (STRIP_HEADERS.has(k)) continue;

      // Remove Vercel-specific headers
      if (k.startsWith("x-vercel-")) continue;

      // Capture real client IP if provided
      if (k === "x-real-ip") {
        clientIp = v;
        continue;
      }

      // Prefer first valid forwarded IP if x-forwarded-for exists
      if (k === "x-forwarded-for") {
        if (!clientIp) clientIp = v;
        continue;
      }

      // Normalize header value (handle arrays)
      headers[k] = Array.isArray(v) ? v.join(", ") : v;
    }

    // Inject client IP into forwarded headers if available
    if (clientIp) headers["x-forwarded-for"] = clientIp;

    // HTTP method of incoming request
    const method = req.method;

    // Determine whether request contains a body (POST/PUT/PATCH/etc.)
    const hasBody = method !== "GET" && method !== "HEAD";

    // Options object for fetch request to upstream server
    const fetchOpts = {
      method,
      headers,
      redirect: "manual", // Prevent automatic redirect following
    };

    // If request has body, stream it directly to upstream
    if (hasBody) {
      // Convert Node.js stream into Web stream for fetch compatibility
      fetchOpts.body = Readable.toWeb(req);

      // Required when using streaming request bodies in Node fetch
      fetchOpts.duplex = "half";
    }

    // Send request to upstream server
    const upstream = await fetch(targetUrl, fetchOpts);

    // Set response status code to match upstream response
    res.statusCode = upstream.status;

    // Forward all upstream response headers to client
    for (const [k, v] of upstream.headers) {
      // Skip transfer-encoding to avoid stream corruption issues
      if (k.toLowerCase() === "transfer-encoding") continue;

      try {
        res.setHeader(k, v);
      } catch {
        // Ignore header setting errors (some headers are restricted)
      }
    }

    // If upstream response has a body, stream it directly to client
    if (upstream.body) {
      await pipeline(
        Readable.fromWeb(upstream.body),
        res
      );
    } else {
      // If no body exists, end response immediately
      res.end();
    }
  } catch (err) {
    // Log any proxy or network error
    console.error("relay error:", err);

    // Send fallback error response if headers are not already sent
    if (!res.headersSent) {
      res.statusCode = 502;
      res.end("Bad Gateway: Tunnel Failed");
    }
  }
}