import { appendFileSync } from "node:fs";
import { Database } from "bun:sqlite";

import { settings } from "../core/config";
import { makeId } from "../utils/id";
import { nowIso } from "../utils/time";
import type { AuditRecord, OrderIntent, OrderStatus, RiskState } from "../types/models";

export class AuditStore {
  private db: Database;

  constructor(dbPath = settings.dbPath, private readonly jsonlPath = settings.jsonlAuditPath) {
    this.db = new Database(dbPath);
    this.init();
  }

  private init(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS audit_records (
        id TEXT PRIMARY KEY,
        timestamp TEXT NOT NULL,
        event_type TEXT NOT NULL,
        payload TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS order_intents (
        id TEXT PRIMARY KEY,
        created_at TEXT NOT NULL,
        symbol TEXT NOT NULL,
        action TEXT NOT NULL,
        status TEXT NOT NULL,
        payload TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS risk_snapshots (
        timestamp TEXT PRIMARY KEY,
        payload TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS app_state (
        key TEXT PRIMARY KEY,
        payload TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );
    `);
  }

  logEvent(eventType: string, payload: Record<string, unknown>): AuditRecord {
    const record: AuditRecord = {
      id: makeId(),
      timestamp: nowIso(),
      eventType,
      payload
    };

    this.db
      .query(
        "INSERT INTO audit_records (id, timestamp, event_type, payload) VALUES (?, ?, ?, ?)"
      )
      .run(record.id, record.timestamp, record.eventType, JSON.stringify(record.payload));

    appendFileSync(this.jsonlPath, `${JSON.stringify(record)}\n`, { encoding: "utf8" });
    return record;
  }

  saveOrder(order: OrderIntent): void {
    this.db
      .query(
        `INSERT OR REPLACE INTO order_intents
         (id, created_at, symbol, action, status, payload)
         VALUES (?, ?, ?, ?, ?, ?)`
      )
      .run(
        order.id,
        order.createdAt,
        order.symbol,
        order.action,
        order.status,
        JSON.stringify(order)
      );
  }

  getOrder(orderId: string): OrderIntent | null {
    const row = this.db
      .query("SELECT payload FROM order_intents WHERE id = ?")
      .get(orderId) as { payload?: string } | null;
    if (!row?.payload) return null;
    return JSON.parse(row.payload) as OrderIntent;
  }

  updateOrderStatus(orderId: string, status: OrderStatus, note = ""): OrderIntent | null {
    const order = this.getOrder(orderId);
    if (!order) return null;
    order.status = status;
    if (note) order.riskNotes.push(note);
    this.saveOrder(order);
    this.logEvent("order_status_updated", { orderId, status, note });
    return order;
  }

  listOrders(params?: { status?: OrderStatus; limit?: number }): OrderIntent[] {
    const limit = params?.limit ?? 50;
    if (params?.status) {
      const rows = this.db
        .query(
          "SELECT payload FROM order_intents WHERE status = ? ORDER BY created_at DESC LIMIT ?"
        )
        .all(params.status, limit) as Array<{ payload: string }>;
      return rows.map((row) => JSON.parse(row.payload) as OrderIntent);
    }

    const rows = this.db
      .query("SELECT payload FROM order_intents ORDER BY created_at DESC LIMIT ?")
      .all(limit) as Array<{ payload: string }>;
    return rows.map((row) => JSON.parse(row.payload) as OrderIntent);
  }

  saveRiskSnapshot(riskState: RiskState): void {
    this.db
      .query("INSERT OR REPLACE INTO risk_snapshots (timestamp, payload) VALUES (?, ?)")
      .run(riskState.timestamp, JSON.stringify(riskState));
  }

  latestRiskSnapshot(): RiskState | null {
    const row = this.db
      .query("SELECT payload FROM risk_snapshots ORDER BY timestamp DESC LIMIT 1")
      .get() as { payload?: string } | null;
    if (!row?.payload) return null;
    return JSON.parse(row.payload) as RiskState;
  }

  listRiskSnapshots(limit = 200): RiskState[] {
    const safeLimit = Math.max(1, Math.min(limit, 2_000));
    const rows = this.db
      .query("SELECT payload FROM risk_snapshots ORDER BY timestamp DESC LIMIT ?")
      .all(safeLimit) as Array<{ payload: string }>;

    const snapshots = rows.map((row) => JSON.parse(row.payload) as RiskState);
    snapshots.reverse();
    return snapshots;
  }

  listAuditRecords(params?: {
    eventTypes?: string[];
    limit?: number;
    sinceTimestamp?: string;
  }): AuditRecord[] {
    const limit = Math.max(1, Math.min(params?.limit ?? 500, 10_000));
    const eventTypes = (params?.eventTypes ?? [])
      .map((eventType) => eventType.trim())
      .filter((eventType) => eventType.length > 0);
    const sinceTimestamp = params?.sinceTimestamp?.trim();

    const clauses: string[] = [];
    const values: Array<string | number> = [];
    if (eventTypes.length > 0) {
      const placeholders = eventTypes.map(() => "?").join(", ");
      clauses.push(`event_type IN (${placeholders})`);
      values.push(...eventTypes);
    }
    if (sinceTimestamp && sinceTimestamp.length > 0) {
      clauses.push("timestamp >= ?");
      values.push(sinceTimestamp);
    }
    const whereClause = clauses.length > 0 ? `WHERE ${clauses.join(" AND ")}` : "";

    const rows = this.db
      .query(
        `SELECT id, timestamp, event_type, payload
         FROM audit_records
         ${whereClause}
         ORDER BY timestamp DESC
         LIMIT ?`
      )
      .all(...values, limit) as Array<{
      id: string;
      timestamp: string;
      event_type: string;
      payload: string;
    }>;

    return rows
      .map((row) => {
        try {
          return {
            id: row.id,
            timestamp: row.timestamp,
            eventType: row.event_type,
            payload: JSON.parse(row.payload) as Record<string, unknown>
          } satisfies AuditRecord;
        } catch {
          return {
            id: row.id,
            timestamp: row.timestamp,
            eventType: row.event_type,
            payload: {}
          } satisfies AuditRecord;
        }
      })
      .reverse();
  }

  setAppState(key: string, payload: unknown): void {
    this.db
      .query(
        "INSERT OR REPLACE INTO app_state (key, payload, updated_at) VALUES (?, ?, ?)"
      )
      .run(key, JSON.stringify(payload), nowIso());
  }

  getAppState<T>(key: string): T | null {
    const row = this.db
      .query("SELECT payload FROM app_state WHERE key = ?")
      .get(key) as { payload?: string } | null;
    if (!row?.payload) return null;

    try {
      return JSON.parse(row.payload) as T;
    } catch {
      return null;
    }
  }
}
