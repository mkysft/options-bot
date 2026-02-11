import { extname, join, normalize } from "node:path";
import { readFile } from "node:fs/promises";
import Fastify from "fastify";
import type { FastifyReply, FastifyRequest } from "fastify";
import cors from "@fastify/cors";

import { registerRoutes } from "./api/routes";
import { settings } from "./core/config";
import { logger } from "./core/logger";
import { buildContainer } from "./services/container";
import type { ServiceContainer } from "./services/container";

declare module "fastify" {
  interface FastifyInstance {
    services: ServiceContainer;
  }
}

export const buildApp = async () => {
  const app = Fastify({ logger: false });
  app.decorate("services", buildContainer());

  const requestReasonByPath: Record<string, string> = {
    "/health": "Health check request",
    "/run-status": "Fetch scheduler/action timing and availability status",
    "/scan": "Run universe scan and rank candidates",
    "/score": "Score one symbol using current universe",
    "/decision": "Generate trade decision for a symbol",
    "/recommendations": "Fetch ranked actionable recommendations",
    "/backtest": "Run backtest/simulation",
    "/propose-order": "Create order proposal",
    "/approve-order": "Approve or reject a pending order",
    "/risk-status": "Fetch current risk status snapshot",
    "/kill-switch": "Read or update emergency trading kill switch",
    "/account-summary": "Fetch account summary and pnl trend",
    "/acceptance-gate": "Compute and fetch paper-run acceptance gate status",
    "/positions": "Fetch broker and local open positions",
    "/ibkr-status": "Check IBKR connectivity status",
    "/ibkr-readiness": "Run IBKR readiness diagnostics for data + transport health",
    "/ibkr-launch": "Trigger IBKR launcher (Gateway/TWS)",
    "/market-data-diagnostics": "Summarize IBKR market-data entitlement status by symbol",
    "/bot-policy": "Read bot policy and guidelines",
    "/bot-policy/reset": "Reset bot policy to defaults",
    "/env-config": "Read runtime environment configuration",
    "/app/refresh": "Refresh runtime config without full restart",
    "/app/restart": "Request app restart",
    "/orders/pending": "Fetch pending order approvals",
    "/orders/recent": "Fetch recent orders",
    "/config": "Fetch runtime configuration",
    "/api-request-logs": "Inspect internal/external API request logs"
  };

  const isUiRequestPath = (path: string): boolean => path === "/" || path.startsWith("/ui");
  const requestPath = (url: string): string => url.split("?")[0] ?? url;
  const summarizePayload = (payload: unknown): unknown => {
    if (payload === undefined || payload === null) return undefined;
    if (typeof payload === "string") {
      return payload.length > 1_000 ? `${payload.slice(0, 1_000)}...` : payload;
    }
    try {
      const serialized = JSON.stringify(payload);
      if (!serialized) return undefined;
      if (serialized.length > 4_000) return `${serialized.slice(0, 4_000)}...`;
      return payload;
    } catch {
      return String(payload);
    }
  };

  await app.register(cors, { origin: true });
  const uiDistRoot = join(import.meta.dir, "ui-dist");
  const uiDevServerUrlRaw = (process.env.UI_DEV_SERVER_URL ?? "").trim();
  let uiDevServerOrigin: string | null = null;
  if (uiDevServerUrlRaw.length > 0) {
    try {
      uiDevServerOrigin = new URL(uiDevServerUrlRaw).origin;
    } catch {
      uiDevServerOrigin = null;
    }
  }
  const uiDevProxyEnabled = Boolean(uiDevServerOrigin);
  const hopByHopHeaders = new Set([
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length"
  ]);

  const contentTypeByExt: Record<string, string> = {
    ".html": "text/html; charset=utf-8",
    ".js": "application/javascript; charset=utf-8",
    ".mjs": "application/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".json": "application/json; charset=utf-8",
    ".svg": "image/svg+xml",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif": "image/gif",
    ".webp": "image/webp",
    ".woff": "font/woff",
    ".woff2": "font/woff2",
    ".ttf": "font/ttf",
    ".map": "application/json; charset=utf-8"
  };

  const sanitizeUiPath = (assetPath: string): string | null => {
    const cleaned = assetPath.replace(/^\/+/, "");
    const normalized = normalize(cleaned);
    if (normalized.includes("\0")) return null;
    if (normalized.startsWith("..")) return null;
    if (normalized === "." || normalized === "") return "index.html";
    return normalized;
  };

  const sendUiAsset = async (assetPath: string, fallbackToIndex: boolean) => {
    const safePath = sanitizeUiPath(assetPath);
    if (!safePath) return { status: 400 as const, body: { message: "Invalid UI asset path" } };

    const filePath = join(uiDistRoot, safePath);
    try {
      const file = await readFile(filePath);
      const ext = extname(filePath).toLowerCase();
      return {
        status: 200 as const,
        body: file,
        contentType: contentTypeByExt[ext] ?? "application/octet-stream"
      };
    } catch {
      if (!fallbackToIndex && safePath !== "index.html") {
        return { status: 404 as const, body: { message: "UI asset not found" } };
      }
      try {
        const indexFile = await readFile(join(uiDistRoot, "index.html"));
        return {
          status: 200 as const,
          body: indexFile,
          contentType: contentTypeByExt[".html"]
        };
      } catch {
        const fallbackHtml = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>UI Build Required</title>
    <style>
      body{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;background:#f7f8f6;color:#122230;padding:24px;}
      .box{max-width:720px;margin:80px auto;background:#fff;border:1px solid #d8e2ea;border-radius:12px;padding:24px;box-shadow:0 6px 18px rgba(0,0,0,.06);}
      code{background:#f1f5f9;border-radius:6px;padding:2px 6px;}
    </style>
  </head>
  <body>
    <div class="box">
      <h1>Frontend Build Not Found</h1>
      <p>The React UI has not been built yet.</p>
      <p>Run <code>bun run ui:build</code> and refresh this page.</p>
    </div>
  </body>
</html>`;
        return {
          status: 200 as const,
          body: Buffer.from(fallbackHtml),
          contentType: contentTypeByExt[".html"]
        };
      }
    }
  };

  const proxyUiRequestToDevServer = async (
    request: FastifyRequest,
    reply: FastifyReply
  ): Promise<boolean> => {
    if (!uiDevProxyEnabled || !uiDevServerOrigin) return false;

    let requestPathAndQuery = request.url;
    if (/^https?:\/\//i.test(requestPathAndQuery)) {
      try {
        const parsed = new URL(requestPathAndQuery);
        requestPathAndQuery = `${parsed.pathname}${parsed.search}`;
      } catch {
        requestPathAndQuery = "/ui/";
      }
    }
    if (!requestPathAndQuery.startsWith("/")) {
      requestPathAndQuery = `/${requestPathAndQuery}`;
    }
    const targetUrl = new URL(requestPathAndQuery, `${uiDevServerOrigin}/`).toString();
    const forwardedHeaders = new Headers();

    for (const [key, value] of Object.entries(request.headers)) {
      if (!value) continue;
      const normalizedKey = key.toLowerCase();
      if (hopByHopHeaders.has(normalizedKey)) continue;
      if (Array.isArray(value)) {
        forwardedHeaders.set(key, value.join(", "));
      } else {
        forwardedHeaders.set(key, value);
      }
    }

    try {
      const upstream = await fetch(targetUrl, {
        method: request.method,
        headers: forwardedHeaders,
        redirect: "manual"
      });

      reply.code(upstream.status);
      upstream.headers.forEach((value, key) => {
        if (hopByHopHeaders.has(key.toLowerCase())) return;
        reply.header(key, value);
      });

      const body = Buffer.from(await upstream.arrayBuffer());
      reply.send(body);
      return true;
    } catch (error) {
      logger.warn(`UI dev proxy request failed for ${requestPathAndQuery}`, (error as Error).message);
      return false;
    }
  };

  app.get("/", async (_, reply) => {
    return reply.redirect("/ui");
  });
  app.get("/ui", async (request, reply) => {
    if (await proxyUiRequestToDevServer(request, reply)) return;
    return reply.redirect("/ui/");
  });
  app.get("/ui/", async (request, reply) => {
    if (await proxyUiRequestToDevServer(request, reply)) return;
    const result = await sendUiAsset("index.html", false);
    if (result.status !== 200) return reply.code(result.status).send(result.body);
    return reply.type(result.contentType).send(result.body);
  });
  app.get("/ui/*", async (request, reply) => {
    if (await proxyUiRequestToDevServer(request, reply)) return;
    const wildcard = (request.params as { "*": string })["*"] ?? "";
    const shouldFallbackToIndex = !wildcard.includes(".");
    const result = await sendUiAsset(wildcard, shouldFallbackToIndex);
    if (result.status !== 200) return reply.code(result.status).send(result.body);
    return reply.type(result.contentType).send(result.body);
  });

  app.addHook("preHandler", async (request) => {
    const path = requestPath(request.url);
    if (isUiRequestPath(path)) return;

    const startedMs = Date.now();
    const routePath = request.routeOptions?.url ?? path;
    const reason =
      requestReasonByPath[routePath] ??
      requestReasonByPath[path] ??
      `Handle ${request.method.toUpperCase()} ${routePath}`;
    const requestPayload = summarizePayload({
      params: request.params,
      query: request.query,
      body: request.body
    });

    (
      request as FastifyRequest & {
        __apiLogContext?: {
          startedMs: number;
          startedAt: string;
          path: string;
          reason: string;
          requestPayload?: unknown;
          errorMessage?: string;
        };
      }
    ).__apiLogContext = {
      startedMs,
      startedAt: new Date(startedMs).toISOString(),
      path: routePath,
      reason,
      requestPayload
    };
  });

  app.addHook("onError", async (request, _reply, error) => {
    const context = (
      request as FastifyRequest & {
        __apiLogContext?: {
          errorMessage?: string;
        };
      }
    ).__apiLogContext;
    if (!context) return;
    context.errorMessage = error.message;
  });

  app.addHook("onResponse", async (request, reply) => {
    const context = (
      request as FastifyRequest & {
        __apiLogContext?: {
          startedMs: number;
          startedAt: string;
          path: string;
          reason: string;
          requestPayload?: unknown;
          errorMessage?: string;
        };
      }
    ).__apiLogContext;
    if (!context) return;

    app.services.apiRequestLogStore.log({
      startedAt: context.startedAt,
      finishedAt: new Date().toISOString(),
      durationMs: Date.now() - context.startedMs,
      direction: "internal",
      provider: "options-bot",
      method: request.method.toUpperCase(),
      endpoint: context.path,
      reason: context.reason,
      status: reply.statusCode >= 400 ? "error" : "success",
      statusCode: reply.statusCode,
      correlationId: String(request.id ?? ""),
      requestPayload: context.requestPayload,
      responsePayload: {
        statusCode: reply.statusCode
      },
      errorMessage: context.errorMessage
    });
  });

  await registerRoutes(app);

  app.addHook("onClose", async () => {
    app.services.scheduler.stop();
  });

  return app;
};

if (import.meta.main) {
  const app = await buildApp();
  try {
    await app.listen({ host: settings.appHost, port: settings.appPort });
    logger.info(`${settings.appName} listening on http://${settings.appHost}:${settings.appPort}`);
  } catch (error) {
    logger.error("Failed to start server", error);
    process.exit(1);
  }
}
