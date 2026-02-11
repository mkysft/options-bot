import { Database } from "bun:sqlite";

import { settings } from "../core/config";
import { logger } from "../core/logger";
import { makeId } from "../utils/id";
import { nowIso } from "../utils/time";

export type ApiRequestDirection = "internal" | "external";
export type ApiRequestStatus = "success" | "error";

export interface ApiRequestLogEntry {
  id: string;
  startedAt: string;
  finishedAt: string;
  durationMs: number;
  direction: ApiRequestDirection;
  provider: string;
  method: string;
  endpoint: string;
  reason: string;
  status: ApiRequestStatus;
  statusCode?: number;
  correlationId?: string;
  requestPayload?: unknown;
  responsePayload?: unknown;
  errorMessage?: string;
}

export interface ApiRequestLogInput {
  startedAt?: string;
  finishedAt?: string;
  durationMs?: number;
  direction: ApiRequestDirection;
  provider: string;
  method: string;
  endpoint: string;
  reason: string;
  status: ApiRequestStatus;
  statusCode?: number;
  correlationId?: string;
  requestPayload?: unknown;
  responsePayload?: unknown;
  errorMessage?: string;
}

export interface ApiRequestLogQuery {
  limit?: number;
  direction?: ApiRequestDirection;
  status?: ApiRequestStatus;
  provider?: string;
  endpointContains?: string;
}

const stringifyPayload = (payload: unknown): string => {
  if (payload === undefined) return "";
  try {
    const json = JSON.stringify(payload);
    if (!json) return "";
    return json.length > 8_000 ? `${json.slice(0, 8_000)}...` : json;
  } catch {
    return "";
  }
};

const parsePayload = (payload: string): unknown => {
  if (!payload) return undefined;
  try {
    return JSON.parse(payload);
  } catch {
    return payload;
  }
};

export class ApiRequestLogStore {
  private db: Database;

  constructor(dbPath = settings.dbPath) {
    this.db = new Database(dbPath);
    this.init();
  }

  private init(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS api_request_logs (
        id TEXT PRIMARY KEY,
        started_at TEXT NOT NULL,
        finished_at TEXT NOT NULL,
        duration_ms INTEGER NOT NULL,
        direction TEXT NOT NULL,
        provider TEXT NOT NULL,
        method TEXT NOT NULL,
        endpoint TEXT NOT NULL,
        reason TEXT NOT NULL,
        status TEXT NOT NULL,
        status_code INTEGER,
        correlation_id TEXT,
        request_payload TEXT NOT NULL,
        response_payload TEXT NOT NULL,
        error_message TEXT
      );

      CREATE INDEX IF NOT EXISTS idx_api_request_logs_started_at
        ON api_request_logs (started_at DESC);
    `);
  }

  log(entry: ApiRequestLogInput): ApiRequestLogEntry {
    const finishedAt = entry.finishedAt ?? nowIso();
    const startedAt = entry.startedAt ?? finishedAt;
    const computedDuration =
      entry.durationMs ?? Math.max(0, new Date(finishedAt).getTime() - new Date(startedAt).getTime());

    const record: ApiRequestLogEntry = {
      id: makeId(),
      startedAt,
      finishedAt,
      durationMs: Number.isFinite(computedDuration) ? Math.round(computedDuration) : 0,
      direction: entry.direction,
      provider: entry.provider,
      method: entry.method.toUpperCase(),
      endpoint: entry.endpoint,
      reason: entry.reason,
      status: entry.status,
      statusCode: entry.statusCode,
      correlationId: entry.correlationId,
      requestPayload: entry.requestPayload,
      responsePayload: entry.responsePayload,
      errorMessage: entry.errorMessage
    };

    try {
      this.db
        .query(
          `INSERT INTO api_request_logs
           (id, started_at, finished_at, duration_ms, direction, provider, method, endpoint, reason, status, status_code, correlation_id, request_payload, response_payload, error_message)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
        )
        .run(
          record.id,
          record.startedAt,
          record.finishedAt,
          record.durationMs,
          record.direction,
          record.provider,
          record.method,
          record.endpoint,
          record.reason,
          record.status,
          record.statusCode ?? null,
          record.correlationId ?? null,
          stringifyPayload(record.requestPayload),
          stringifyPayload(record.responsePayload),
          record.errorMessage ?? null
        );
    } catch (error) {
      logger.warn("API request log write failed", (error as Error).message);
    }

    return record;
  }

  list(query: ApiRequestLogQuery = {}): ApiRequestLogEntry[] {
    const limit = Math.max(1, Math.min(query.limit ?? 200, 2_000));
    const clauses: string[] = [];
    const params: Array<string | number> = [];

    if (query.direction) {
      clauses.push("direction = ?");
      params.push(query.direction);
    }
    if (query.status) {
      clauses.push("status = ?");
      params.push(query.status);
    }
    if (query.provider) {
      clauses.push("provider = ?");
      params.push(query.provider);
    }
    if (query.endpointContains) {
      clauses.push("endpoint LIKE ?");
      params.push(`%${query.endpointContains}%`);
    }

    const where = clauses.length > 0 ? `WHERE ${clauses.join(" AND ")}` : "";
    const rows = this.db
      .query(
        `SELECT
           id,
           started_at,
           finished_at,
           duration_ms,
           direction,
           provider,
           method,
           endpoint,
           reason,
           status,
           status_code,
           correlation_id,
           request_payload,
           response_payload,
           error_message
         FROM api_request_logs
         ${where}
         ORDER BY started_at DESC
         LIMIT ?`
      )
      .all(...params, limit) as Array<{
      id: string;
      started_at: string;
      finished_at: string;
      duration_ms: number;
      direction: ApiRequestDirection;
      provider: string;
      method: string;
      endpoint: string;
      reason: string;
      status: ApiRequestStatus;
      status_code: number | null;
      correlation_id: string | null;
      request_payload: string;
      response_payload: string;
      error_message: string | null;
    }>;

    return rows.map((row) => ({
      id: row.id,
      startedAt: row.started_at,
      finishedAt: row.finished_at,
      durationMs: row.duration_ms,
      direction: row.direction,
      provider: row.provider,
      method: row.method,
      endpoint: row.endpoint,
      reason: row.reason,
      status: row.status,
      statusCode: row.status_code ?? undefined,
      correlationId: row.correlation_id ?? undefined,
      requestPayload: parsePayload(row.request_payload),
      responsePayload: parsePayload(row.response_payload),
      errorMessage: row.error_message ?? undefined
    }));
  }
}

export const apiRequestLogStore = new ApiRequestLogStore();
