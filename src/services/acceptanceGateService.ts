import type { AuditRecord, OrderIntent } from "../types/models";
import { AuditStore } from "../storage/auditStore";

interface CompletedTradeMetrics {
  grossPnl: number;
  netPnlAfterCosts: number;
  closedQuantity: number;
  commissions: number;
}

export interface AcceptanceGateSnapshot {
  generatedAt: string;
  config: {
    minRunDays: number;
    minProfitFactor: number;
    maxDrawdownPct: number;
    minExpectancy: number;
    estimatedCommissionPerContract: number;
  };
  period: {
    startAt: string | null;
    endAt: string;
    observedDays: number;
    observedWeeks: number;
  };
  trading: {
    totalEntriesTracked: number;
    completedTrades: number;
    openTrades: number;
    wins: number;
    losses: number;
    pushes: number;
    winRate: number | null;
    grossProfit: number;
    grossLossAbs: number;
    netPnlAfterCosts: number;
    estimatedCommissions: number;
    expectancy: number | null;
    profitFactor: number | null;
  };
  risk: {
    maxDailyDrawdownPct: number;
    maxEquityDrawdownPct: number;
    maxDrawdownPct: number;
  };
  violations: {
    count: number;
    items: Array<{
      timestamp: string;
      type: string;
      reasons: string[];
      source: string;
    }>;
  };
  checks: {
    minRunDuration: boolean;
    positiveExpectancy: boolean;
    profitFactor: boolean;
    maxDrawdown: boolean;
    noPolicyViolations: boolean;
  };
  pass: boolean;
  notes: string[];
}

const ACCEPTANCE_GATE_STATE_KEY = "acceptance_gate_v1";

const asNumber = (value: unknown): number | null => {
  const num = typeof value === "number" ? value : typeof value === "string" ? Number(value) : Number.NaN;
  return Number.isFinite(num) ? num : null;
};

const toIso = (value: number | null): string | null => {
  if (value === null || !Number.isFinite(value) || value <= 0) return null;
  return new Date(value).toISOString();
};

const closedStatus = (status: string): boolean =>
  status === "FILLED" || status === "EXITED";

export class AcceptanceGateService {
  private readonly minRunDays = 56;
  private readonly minProfitFactor = 1.2;
  private readonly maxDrawdownPct = 0.12;
  private readonly minExpectancy = 0;
  private readonly estimatedCommissionPerContract = 0.65;

  constructor(private readonly auditStore: AuditStore) {}

  private exitOrdersByParent(orders: OrderIntent[]): Map<string, OrderIntent[]> {
    const exitsByParent = new Map<string, OrderIntent[]>();
    for (const order of orders) {
      if (order.intentType !== "EXIT" || !order.parentOrderId) continue;
      if (!closedStatus(order.status)) continue;
      const list = exitsByParent.get(order.parentOrderId) ?? [];
      list.push(order);
      exitsByParent.set(order.parentOrderId, list);
    }
    for (const list of exitsByParent.values()) {
      list.sort((left, right) => {
        const leftTs = new Date(left.updatedAt ?? left.createdAt).getTime();
        const rightTs = new Date(right.updatedAt ?? right.createdAt).getTime();
        return leftTs - rightTs;
      });
    }
    return exitsByParent;
  }

  private computeCompletedTradeMetrics(
    entry: OrderIntent,
    linkedExits: OrderIntent[]
  ): CompletedTradeMetrics | null {
    if (entry.intentType !== "ENTRY") return null;
    const entryPrice = asNumber(entry.avgFillPrice) ?? asNumber(entry.limitPrice);
    if (entryPrice === null || entryPrice <= 0) return null;
    const entryFilledQty = Math.max(
      0,
      asNumber(entry.filledQuantity) ?? asNumber(entry.quantity) ?? 0
    );
    if (entryFilledQty <= 0) return null;

    let closedQty = 0;
    let grossPnl = 0;
    let commissions = 0;
    for (const exit of linkedExits) {
      const exitQty = Math.max(
        0,
        asNumber(exit.filledQuantity) ??
          (closedStatus(exit.status) ? asNumber(exit.quantity) : 0) ??
          0
      );
      if (exitQty <= 0) continue;

      const exitPrice = asNumber(exit.avgFillPrice) ?? asNumber(exit.limitPrice);
      if (exitPrice === null || exitPrice <= 0) continue;

      closedQty += exitQty;
      grossPnl += (exitPrice - entryPrice) * exitQty * 100;
      commissions += exitQty * this.estimatedCommissionPerContract * 2;
    }

    if (closedQty + 1e-6 < entryFilledQty) return null;
    return {
      grossPnl,
      netPnlAfterCosts: grossPnl - commissions,
      closedQuantity: closedQty,
      commissions
    };
  }

  private maxEquityDrawdownPct(
    snapshots: Array<{
      accountEquity: number;
    }>
  ): number {
    if (snapshots.length === 0) return 0;
    let peak = Math.max(1, snapshots[0].accountEquity || 1);
    let maxDrawdown = 0;
    for (const snapshot of snapshots) {
      const equity = Math.max(1, snapshot.accountEquity || 1);
      if (equity > peak) peak = equity;
      const drawdown = Math.max(0, (peak - equity) / peak);
      if (drawdown > maxDrawdown) maxDrawdown = drawdown;
    }
    return maxDrawdown;
  }

  private extractViolationRecords(records: AuditRecord[]): AcceptanceGateSnapshot["violations"]["items"] {
    const results: AcceptanceGateSnapshot["violations"]["items"] = [];
    for (const record of records) {
      if (record.eventType !== "order_blocked") continue;
      const payload = record.payload as { reasons?: unknown; source?: unknown };
      const reasonsRaw = Array.isArray(payload.reasons)
        ? payload.reasons.map((reason) => String(reason))
        : [];
      const reasons = reasonsRaw.filter((reason) => reason !== "live_mode_disabled_phase_1");
      if (reasons.length === 0) continue;
      results.push({
        timestamp: record.timestamp,
        type: "order_blocked",
        reasons,
        source: typeof payload.source === "string" ? payload.source : "execution_gateway"
      });
    }
    return results;
  }

  private computeSnapshot(nowMs: number): AcceptanceGateSnapshot {
    const orders = this.auditStore.listOrders({ limit: 5_000 });
    const entries = orders.filter((order) => order.intentType === "ENTRY");
    const exitsByParent = this.exitOrdersByParent(orders);
    const riskSnapshots = this.auditStore.listRiskSnapshots(5_000);

    let completedTrades = 0;
    let wins = 0;
    let losses = 0;
    let pushes = 0;
    let grossProfit = 0;
    let grossLossAbs = 0;
    let netPnlAfterCosts = 0;
    let estimatedCommissions = 0;

    for (const entry of entries) {
      const metrics = this.computeCompletedTradeMetrics(entry, exitsByParent.get(entry.id) ?? []);
      if (!metrics) continue;
      completedTrades += 1;
      netPnlAfterCosts += metrics.netPnlAfterCosts;
      estimatedCommissions += metrics.commissions;

      if (metrics.netPnlAfterCosts > 0) {
        wins += 1;
        grossProfit += metrics.netPnlAfterCosts;
      } else if (metrics.netPnlAfterCosts < 0) {
        losses += 1;
        grossLossAbs += Math.abs(metrics.netPnlAfterCosts);
      } else {
        pushes += 1;
      }
    }

    const expectancy =
      completedTrades > 0 ? netPnlAfterCosts / completedTrades : null;
    const profitFactor =
      grossLossAbs > 0
        ? grossProfit / grossLossAbs
        : grossProfit > 0
          ? 99
          : null;
    const winRate = completedTrades > 0 ? wins / completedTrades : null;

    const timestamps: number[] = [];
    for (const snapshot of riskSnapshots) {
      const ts = new Date(snapshot.timestamp).getTime();
      if (Number.isFinite(ts)) timestamps.push(ts);
    }
    for (const entry of entries) {
      const ts = new Date(entry.createdAt).getTime();
      if (Number.isFinite(ts)) timestamps.push(ts);
    }
    const startMs = timestamps.length > 0 ? Math.min(...timestamps) : null;
    const endMs = nowMs;
    const observedDays =
      startMs !== null ? Math.max(0, (endMs - startMs) / (24 * 60 * 60 * 1_000)) : 0;
    const observedWeeks = observedDays / 7;

    const maxDailyDrawdownPct = riskSnapshots.reduce(
      (max, snapshot) => Math.max(max, asNumber(snapshot.dailyDrawdownPct) ?? 0),
      0
    );
    const maxEquityDrawdownPct = this.maxEquityDrawdownPct(
      riskSnapshots.map((snapshot) => ({
        accountEquity: snapshot.accountEquity
      }))
    );
    const maxDrawdownPct = Math.max(maxDailyDrawdownPct, maxEquityDrawdownPct);

    const violationRecords = this.extractViolationRecords(
      this.auditStore.listAuditRecords({
        eventTypes: ["order_blocked"],
        limit: 2_000,
        sinceTimestamp: toIso(startMs) ?? undefined
      })
    );

    const checks: AcceptanceGateSnapshot["checks"] = {
      minRunDuration: observedDays >= this.minRunDays,
      positiveExpectancy:
        typeof expectancy === "number" && Number.isFinite(expectancy)
          ? expectancy > this.minExpectancy
          : false,
      profitFactor:
        typeof profitFactor === "number" && Number.isFinite(profitFactor)
          ? profitFactor >= this.minProfitFactor
          : false,
      maxDrawdown: maxDrawdownPct <= this.maxDrawdownPct,
      noPolicyViolations: violationRecords.length === 0
    };

    const notes: string[] = [];
    if (completedTrades === 0) {
      notes.push("No fully closed trades yet; expectancy/profit factor are not meaningful.");
    }
    if (!checks.minRunDuration) {
      notes.push(`Minimum paper-run duration not reached (${observedDays.toFixed(1)} / ${this.minRunDays} days).`);
    }
    if (!checks.noPolicyViolations && violationRecords.length > 0) {
      notes.push(`${violationRecords.length} policy/risk violation records detected.`);
    }

    return {
      generatedAt: new Date(nowMs).toISOString(),
      config: {
        minRunDays: this.minRunDays,
        minProfitFactor: this.minProfitFactor,
        maxDrawdownPct: this.maxDrawdownPct,
        minExpectancy: this.minExpectancy,
        estimatedCommissionPerContract: this.estimatedCommissionPerContract
      },
      period: {
        startAt: toIso(startMs),
        endAt: new Date(endMs).toISOString(),
        observedDays: Number(observedDays.toFixed(3)),
        observedWeeks: Number(observedWeeks.toFixed(3))
      },
      trading: {
        totalEntriesTracked: entries.length,
        completedTrades,
        openTrades: Math.max(0, entries.length - completedTrades),
        wins,
        losses,
        pushes,
        winRate,
        grossProfit: Number(grossProfit.toFixed(2)),
        grossLossAbs: Number(grossLossAbs.toFixed(2)),
        netPnlAfterCosts: Number(netPnlAfterCosts.toFixed(2)),
        estimatedCommissions: Number(estimatedCommissions.toFixed(2)),
        expectancy: expectancy === null ? null : Number(expectancy.toFixed(2)),
        profitFactor: profitFactor === null ? null : Number(profitFactor.toFixed(3))
      },
      risk: {
        maxDailyDrawdownPct: Number(maxDailyDrawdownPct.toFixed(6)),
        maxEquityDrawdownPct: Number(maxEquityDrawdownPct.toFixed(6)),
        maxDrawdownPct: Number(maxDrawdownPct.toFixed(6))
      },
      violations: {
        count: violationRecords.length,
        items: violationRecords.slice(-200)
      },
      checks,
      pass:
        checks.minRunDuration &&
        checks.positiveExpectancy &&
        checks.profitFactor &&
        checks.maxDrawdown &&
        checks.noPolicyViolations,
      notes
    };
  }

  refreshSnapshot(nowMs = Date.now()): AcceptanceGateSnapshot {
    const snapshot = this.computeSnapshot(nowMs);
    this.auditStore.setAppState(ACCEPTANCE_GATE_STATE_KEY, snapshot);
    return snapshot;
  }

  getLatestSnapshot(nowMs = Date.now()): AcceptanceGateSnapshot {
    const persisted = this.auditStore.getAppState<AcceptanceGateSnapshot>(ACCEPTANCE_GATE_STATE_KEY);
    if (!persisted) return this.refreshSnapshot(nowMs);
    return persisted;
  }
}
