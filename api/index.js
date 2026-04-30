/**
 * ---------------------------------------------------------
 * Optimized Streaming Relay For Vercel
 * ---------------------------------------------------------
 * Features:
 * - Native streaming
 * - Low memory footprint
 * - Better upstream compatibility
 * - Stable fetch handling
 * - Safer header forwarding
 * - Timeout protection
 * - Minimal overhead
 * ---------------------------------------------------------
 */

import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

/**
 * Vercel runtime configuration
 */
export const config = {
  api: {
    bodyParser: false,
  },

  supportsResponseStreaming: true,

  /**
   * Keep execution short for stability
   */
  maxDuration: 30,
};

/**
 * Upstream base URL
 */
const UPSTREAM_BASE =
  (process.env.TARGET_DOMAIN || "")
    .trim()
    .replace(/\/$/, "");

/**
 * Headers that should never be proxied
 */
const BLOCKED_HEADERS = new Set([
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

/**
 * Validate target URL
 */
function isValidTarget(url) {
  try {
    const parsed = new URL(url);

    return (
      parsed.protocol === "https:" ||
      parsed.protocol === "http:"
    );
  } catch {
    return false;
  }
}

/**
 * Build safe outbound headers
 */
function buildHeaders(request) {
  const result = {};

  let forwardedIp = null;

  for (const key of Object.keys(request.headers)) {
    const normalizedKey = key.toLowerCase();

    if (BLOCKED_HEADERS.has(normalizedKey)) {
      continue;
    }

    if (normalizedKey.startsWith("x-vercel-")) {
      continue;
    }

    const value = request.headers[key];

    if (!value) {
      continue;
    }

    /**
     * Preserve real client IP
     */
    if (
      normalizedKey === "x-real-ip" ||
      normalizedKey === "x-forwarded-for"
    ) {
      if (!forwardedIp) {
        forwardedIp = Array.isArray(value)
          ? value[0]
          : value;
      }

      continue;
    }

    result[normalizedKey] = Array.isArray(value)
      ? value.join(", ")
      : value;
  }

  /**
   * Restore forwarded IP
   */
  if (forwardedIp) {
    result["x-forwarded-for"] = forwardedIp;
  }

  return result;
}

/**
 * Copy upstream response headers safely
 */
function applyHeaders(response, upstreamHeaders) {
  for (const [key, value] of upstreamHeaders.entries()) {
    const normalizedKey = key.toLowerCase();

    /**
     * Prevent Node stream conflicts
     */
    if (
      normalizedKey === "transfer-encoding" ||
      normalizedKey === "connection"
    ) {
      continue;
    }

    try {
      response.setHeader(key, value);
    } catch {
      // Ignore invalid header errors
    }
  }
}

/**
 * Main relay handler
 */
export default async function handler(req, res) {
  /**
   * Validate configuration
   */
  if (!UPSTREAM_BASE || !isValidTarget(UPSTREAM_BASE)) {
    res.statusCode = 500;

    return res.end(
      "Invalid TARGET_DOMAIN"
    );
  }

  /**
   * Build target URL
   */
  const upstreamUrl =
    UPSTREAM_BASE + req.url;

  /**
   * Timeout controller
   */
  const controller = new AbortController();

  /**
   * Prevent long-running executions
   */
  const timeout = setTimeout(() => {
    controller.abort();
  }, 25000);

  try {
    const method =
      req.method || "GET";

    const hasBody =
      method !== "GET" &&
      method !== "HEAD";

    /**
     * Fetch options
     */
    const requestOptions = {
      method,
      headers: buildHeaders(req),
      redirect: "manual",
      signal: controller.signal,
    };

    /**
     * Stream request body if needed
     */
    if (hasBody) {
      requestOptions.body =
        Readable.toWeb(req);

      requestOptions.duplex = "half";
    }

    /**
     * Forward request upstream
     */
    const upstreamResponse =
      await fetch(
        upstreamUrl,
        requestOptions
      );

    /**
     * Forward status code
     */
    res.statusCode =
      upstreamResponse.status;

    /**
     * Apply upstream headers
     */
    applyHeaders(
      res,
      upstreamResponse.headers
    );

    /**
     * Optional runtime marker
     */
    res.setHeader(
      "x-runtime",
      "vercel-node"
    );

    /**
     * Stream upstream response
     */
    if (upstreamResponse.body) {
      await pipeline(
        Readable.fromWeb(
          upstreamResponse.body
        ),
        res
      );
    } else {
      res.end();
    }
  } catch (error) {
    /**
     * Minimal logging
     */
    console.error(
      "[relay-error]",
      error?.message || "unknown"
    );

    /**
     * Prevent double response
     */
    if (!res.headersSent) {
      if (error?.name === "AbortError") {
        res.statusCode = 504;

        res.end("Gateway Timeout");
      } else {
        res.statusCode = 502;

        res.end("Bad Gateway");
      }
    }
  } finally {
    /**
     * Cleanup timeout
     */
    clearTimeout(timeout);
  }
}
