import {
  IbkrAdapter,
  type IbkrAccountSnapshot,
  type IbkrConnectionStatus,
  type IbkrPositionSnapshot,
  type IbkrOrderStatusSnapshot
} from "../adapters/ibkrAdapter";
import { AlphaVantageAdapter } from "../adapters/alphaVantageAdapter";
import { SecEdgarAdapter, type SecEventSnapshot } from "../adapters/secEdgarAdapter";
import { settings } from "../core/config";
import { logger } from "../core/logger";
import type { AccountState, DecisionCard, OptionContractSnapshot, OrderIntent, OrderStatus, TradeAction } from "../types/models";
import { makeId } from "../utils/id";
import { nowIso } from "../utils/time";
import { AuditStore } from "../storage/auditStore";
import { RiskEngine } from "./riskEngine";
import { RuntimePolicyService } from "./runtimePolicyService";

const midPrice = (contract: OptionContractSnapshot): number => {
  if (contract.bid > 0 && contract.ask > 0) return (contract.bid + contract.ask) / 2;
  return contract.last;
};

export class ExecutionGateway {
  private readonly brokerSyncMinIntervalMs = 1_500;
  private readonly accountSyncMinIntervalMs = 4_000;
  private readonly exitAutomationMinIntervalMs = 6_000;
  private readonly binaryEventCacheTtlMs = 6 * 60 * 60 * 1_000;
  private readonly secEventCacheTtlMs = 2 * 60 * 60 * 1_000;
  private readonly binaryEventInflight = new Map<
    string,
    Promise<{ eventDate: string | null; source: string }>
  >();
  private readonly binaryEventCache = new Map<
    string,
    { eventDate: string | null; source: string; expiresAt: number }
  >();
  private readonly secEventInflight = new Map<string, Promise<SecEventSnapshot>>();
  private readonly secEventCache = new Map<string, { snapshot: SecEventSnapshot; expiresAt: number }>();
  private brokerSyncInFlight: Promise<void> | null = null;
  private lastBrokerSyncAt = 0;
  private accountSyncInFlight: Promise<void> | null = null;
  private lastAccountSyncAt = 0;
  private lastAccountSnapshot: IbkrAccountSnapshot | null = null;
  private exitAutomationInFlight: Promise<OrderIntent[]> | null = null;
  private lastExitAutomationAt = 0;
  private startupReconcileInFlight: Promise<void> | null = null;
  private startupReconciled = false;
  private lastConnectivityReachable: boolean | null = null;
  private lastConnectivityTransitionAtMs = 0;

  constructor(
    private readonly auditStore: AuditStore,
    private readonly riskEngine: RiskEngine,
    private readonly runtimePolicy: RuntimePolicyService,
    private readonly ibkr: IbkrAdapter = new IbkrAdapter(),
    private readonly alphaVantage: AlphaVantageAdapter = new AlphaVantageAdapter(),
    private readonly secEdgar: SecEdgarAdapter = new SecEdgarAdapter()
  ) {}

  reloadBrokerConfiguration(): void {
    this.ibkr.reloadConfiguration();
    this.lastAccountSnapshot = null;
    this.startupReconciled = false;
    this.startupReconcileInFlight = null;
    this.lastConnectivityReachable = null;
    this.lastConnectivityTransitionAtMs = 0;
    this.binaryEventInflight.clear();
    this.binaryEventCache.clear();
    this.secEventInflight.clear();
    this.secEventCache.clear();
  }

  notifyConnectivityStatus(status: IbkrConnectionStatus): void {
    const now = Date.now();
    const previous = this.lastConnectivityReachable;
    const current = Boolean(status.reachable);

    if (previous === null) {
      this.lastConnectivityReachable = current;
      this.lastConnectivityTransitionAtMs = now;
      return;
    }

    if (previous === current) return;

    this.lastConnectivityReachable = current;
    this.lastConnectivityTransitionAtMs = now;
    const mode = status.detectedMode === "unknown" ? "unknown" : status.detectedMode.toUpperCase();

    if (current) {
      this.startupReconciled = false;
      this.startupReconcileInFlight = null;
      this.lastBrokerSyncAt = 0;
      this.lastAccountSyncAt = 0;
      this.auditStore.logEvent("ibkr_connectivity_restored", {
        host: status.host,
        port: status.port,
        mode,
        message: status.message
      });
      return;
    }

    this.auditStore.logEvent("ibkr_connectivity_lost", {
      host: status.host,
      port: status.port,
      mode,
      message: status.message
    });
  }

  private parseBinaryEventAtMs(eventDate: string): number | null {
    // Treat event date as market-session day anchor when exact timestamp is unavailable.
    const parsed = Date.parse(`${eventDate}T16:00:00Z`);
    if (!Number.isFinite(parsed)) return null;
    return parsed;
  }

  private async getNextBinaryEvent(
    symbol: string
  ): Promise<{ eventDate: string | null; source: string }> {
    const symbolKey = symbol.toUpperCase();
    const cached = this.binaryEventCache.get(symbolKey);
    if (cached && cached.expiresAt > Date.now()) {
      return { eventDate: cached.eventDate, source: cached.source };
    }

    const inflight = this.binaryEventInflight.get(symbolKey);
    if (inflight) return await inflight;

    const request = (async () => {
      try {
        const next = await this.alphaVantage.getNextEarningsDate(symbolKey);
        this.binaryEventCache.set(symbolKey, {
          eventDate: next.eventDate,
          source: next.source,
          expiresAt: Date.now() + this.binaryEventCacheTtlMs
        });
        return { eventDate: next.eventDate, source: next.source };
      } catch {
        this.binaryEventCache.set(symbolKey, {
          eventDate: null,
          source: "unavailable",
          expiresAt: Date.now() + this.binaryEventCacheTtlMs
        });
        return { eventDate: null, source: "unavailable" };
      } finally {
        this.binaryEventInflight.delete(symbolKey);
      }
    })();

    this.binaryEventInflight.set(symbolKey, request);
    return await request;
  }

  private async getSecEventSnapshot(symbol: string): Promise<SecEventSnapshot> {
    const symbolKey = symbol.toUpperCase();
    const cached = this.secEventCache.get(symbolKey);
    if (cached && cached.expiresAt > Date.now()) {
      return cached.snapshot;
    }

    const inflight = this.secEventInflight.get(symbolKey);
    if (inflight) return await inflight;

    const request = (async () => {
      try {
        const snapshot = await this.secEdgar.getEventBiasAndRiskSnapshot(symbolKey);
        this.secEventCache.set(symbolKey, {
          snapshot,
          expiresAt: Date.now() + this.secEventCacheTtlMs
        });
        return snapshot;
      } catch (error) {
        const fallback: SecEventSnapshot = {
          eventBias: 0,
          eventRisk: 0.2,
          source: "fallback",
          cik: null,
          latestFilingDate: null,
          latestForm: null,
          note: (error as Error).message || "sec_snapshot_unavailable"
        };
        this.secEventCache.set(symbolKey, {
          snapshot: fallback,
          expiresAt: Date.now() + this.secEventCacheTtlMs
        });
        return fallback;
      } finally {
        this.secEventInflight.delete(symbolKey);
      }
    })();

    this.secEventInflight.set(symbolKey, request);
    return await request;
  }

  private appendUniqueRiskNote(order: OrderIntent, note: string): boolean {
    if (order.riskNotes.includes(note)) return false;
    order.riskNotes.push(note);
    return true;
  }

  private optionContractKey(contract: {
    symbol: string;
    expiration: string;
    strike: number;
    right: "CALL" | "PUT";
  }): string {
    const normalizedExpiration = contract.expiration.replace(/\D/g, "");
    const strike = Number(contract.strike).toFixed(4);
    return `${contract.symbol.toUpperCase()}|${normalizedExpiration}|${contract.right}|${strike}`;
  }

  private optionContractKeyFromPosition(position: IbkrPositionSnapshot): string | null {
    if (position.secType !== "OPT") return null;
    if (!position.expiration || !position.right || typeof position.strike !== "number") return null;
    return this.optionContractKey({
      symbol: position.symbol,
      expiration: position.expiration,
      strike: position.strike,
      right: position.right
    });
  }

  private hasActiveExitForParentOrder(parentOrderId: string): boolean {
    const activeStatuses = new Set<OrderStatus>([
      "PENDING_APPROVAL",
      "SUBMITTED_PAPER",
      "SUBMITTED_LIVE"
    ]);
    return this.auditStore
      .listOrders({ limit: 2_000 })
      .some(
        (order) =>
          order.intentType === "EXIT" &&
          order.parentOrderId === parentOrderId &&
          activeStatuses.has(order.status)
      );
  }

  private async maybeRunStartupReconciliation(accountState: AccountState): Promise<void> {
    if (this.startupReconciled) return;
    if (this.startupReconcileInFlight) {
      await this.startupReconcileInFlight;
      return;
    }

    this.startupReconcileInFlight = this.runStartupReconciliation(accountState)
      .then(() => {
        this.startupReconciled = true;
      })
      .catch((error) => {
        logger.warn(`Startup reconciliation deferred: ${(error as Error).message}`);
      })
      .finally(() => {
        this.startupReconcileInFlight = null;
      });

    await this.startupReconcileInFlight;
  }

  private async runStartupReconciliation(_accountState: AccountState): Promise<void> {
    const orders = this.auditStore.listOrders({ limit: 2_000 });
    if (orders.length === 0) {
      this.auditStore.logEvent("startup_reconcile_completed", {
        updatedOrders: 0,
        reason: "no_local_orders"
      });
      return;
    }

    await this.refreshBrokerStatuses();
    const brokerPositionsFetcher = (
      this.ibkr as unknown as {
        getPositionsSnapshot?: () => Promise<IbkrPositionSnapshot[]>;
      }
    ).getPositionsSnapshot;
    const brokerPositions =
      typeof brokerPositionsFetcher === "function"
        ? await brokerPositionsFetcher.call(this.ibkr)
        : [];
    const brokerQtyByContract = new Map<string, number>();

    for (const position of brokerPositions) {
      const key = this.optionContractKeyFromPosition(position);
      if (!key) continue;
      const qty = Math.abs(position.position);
      if (!Number.isFinite(qty) || qty <= 0) continue;
      brokerQtyByContract.set(key, (brokerQtyByContract.get(key) ?? 0) + qty);
    }

    let updatedOrders = 0;
    let inferredFilledEntries = 0;
    let exitedParents = 0;

    for (const exitOrder of orders) {
      if (exitOrder.intentType !== "EXIT" || exitOrder.status !== "FILLED") continue;
      const before = this.auditStore.getOrder(exitOrder.parentOrderId ?? "")?.status;
      this.markParentExited(exitOrder);
      const after = this.auditStore.getOrder(exitOrder.parentOrderId ?? "")?.status;
      if (before !== after && after === "EXITED") exitedParents += 1;
    }

    for (const order of orders) {
      if (order.intentType !== "ENTRY") continue;
      if (order.status !== "SUBMITTED_PAPER" && order.status !== "SUBMITTED_LIVE") continue;

      const key = this.optionContractKey({
        symbol: order.symbol,
        expiration: order.optionContract.expiration,
        strike: order.optionContract.strike,
        right: order.optionContract.right
      });
      const brokerQty = brokerQtyByContract.get(key) ?? 0;
      const localQty = Math.max(1, Math.round(order.quantity));

      // Only infer a fill if broker position size fully covers submitted quantity.
      if (brokerQty + 1e-6 < localQty) continue;

      order.status = "FILLED";
      order.filledQuantity = localQty;
      if (typeof order.avgFillPrice !== "number" || order.avgFillPrice <= 0) {
        order.avgFillPrice = order.limitPrice;
      }
      order.updatedAt = nowIso();
      this.appendUniqueRiskNote(order, "startup_reconciled:broker_position_match");
      this.auditStore.saveOrder(order);
      this.auditStore.logEvent("startup_order_reconciled", {
        orderId: order.id,
        symbol: order.symbol,
        previousStatus: "SUBMITTED",
        nextStatus: "FILLED",
        brokerQty,
        localQty
      });
      inferredFilledEntries += 1;
      updatedOrders += 1;
    }

    for (const order of orders) {
      if (order.intentType !== "ENTRY" || order.status !== "FILLED") continue;
      if (this.hasActiveExitForParentOrder(order.id)) continue;

      const key = this.optionContractKey({
        symbol: order.symbol,
        expiration: order.optionContract.expiration,
        strike: order.optionContract.strike,
        right: order.optionContract.right
      });
      const brokerQty = brokerQtyByContract.get(key) ?? 0;
      if (brokerQty > 1e-6) continue;

      const hasFilledExit = orders.some(
        (candidate) =>
          candidate.intentType === "EXIT" &&
          candidate.parentOrderId === order.id &&
          candidate.status === "FILLED"
      );
      if (!hasFilledExit) continue;

      order.status = "EXITED";
      order.updatedAt = nowIso();
      this.appendUniqueRiskNote(order, "startup_reconciled:filled_exit_detected");
      this.auditStore.saveOrder(order);
      this.auditStore.logEvent("startup_order_reconciled", {
        orderId: order.id,
        symbol: order.symbol,
        previousStatus: "FILLED",
        nextStatus: "EXITED",
        reason: "filled_exit_detected"
      });
      exitedParents += 1;
      updatedOrders += 1;
    }

    this.auditStore.logEvent("startup_reconcile_completed", {
      totalLocalOrders: orders.length,
      brokerOptionPositions: brokerQtyByContract.size,
      inferredFilledEntries,
      exitedParents,
      updatedOrders
    });
  }

  private mapBrokerStatus(current: OrderStatus, brokerStatus: string): OrderStatus {
    const normalized = brokerStatus.trim().toUpperCase();
    if (normalized === "FILLED") return "FILLED";
    if (normalized === "CANCELLED" || normalized === "APICANCELLED" || normalized === "INACTIVE") {
      return "CANCELLED";
    }
    if (
      normalized === "PENDINGSUBMIT" ||
      normalized === "PRESUBMITTED" ||
      normalized === "SUBMITTED" ||
      normalized === "PENDINGCANCEL"
    ) {
      if (current === "SUBMITTED_LIVE" || current === "SUBMITTED_PAPER") return current;
      return settings.paperMode ? "SUBMITTED_PAPER" : "SUBMITTED_LIVE";
    }
    return current;
  }

  private mapOrderSide(action: TradeAction): "BUY" | "SELL" {
    if (action === "NO_TRADE") {
      throw new Error("Cannot map NO_TRADE action to an order side.");
    }
    return "BUY";
  }

  async refreshBrokerStatuses(): Promise<void> {
    const now = Date.now();
    if (this.brokerSyncInFlight) {
      await this.brokerSyncInFlight;
      return;
    }
    if (now - this.lastBrokerSyncAt < this.brokerSyncMinIntervalMs) return;

    this.brokerSyncInFlight = this.refreshBrokerStatusesInternal();
    try {
      await this.brokerSyncInFlight;
    } finally {
      this.lastBrokerSyncAt = Date.now();
      this.brokerSyncInFlight = null;
    }
  }

  private async refreshBrokerStatusesInternal(): Promise<void> {
    const trackedOrders = this.auditStore
      .listOrders({ limit: 500 })
      .filter(
        (order) => order.status === "SUBMITTED_PAPER" || order.status === "SUBMITTED_LIVE"
      );
    if (trackedOrders.length === 0) return;

    const statusByLocalOrderId = await this.ibkr.refreshOrderStatuses(
      trackedOrders.map((order) => order.id)
    );

    for (const order of trackedOrders) {
      const brokerStatus = statusByLocalOrderId[order.id];
      if (!brokerStatus) continue;

      this.applyBrokerStatusUpdate(order, brokerStatus);
    }
  }

  private applyBrokerStatusUpdate(
    order: OrderIntent,
    brokerStatus: IbkrOrderStatusSnapshot
  ): void {
    let changed = false;
    const nextStatus = this.mapBrokerStatus(order.status, brokerStatus.status);
    if (nextStatus !== order.status) {
      order.status = nextStatus;
      changed = true;
    }

    changed =
      this.appendUniqueRiskNote(order, `broker_status:${brokerStatus.status}`) || changed;

    if (brokerStatus.brokerOrderId > 0) {
      if (order.brokerOrderId !== brokerStatus.brokerOrderId) {
        order.brokerOrderId = brokerStatus.brokerOrderId;
        changed = true;
      }
      changed =
        this.appendUniqueRiskNote(order, `broker_order_id:${brokerStatus.brokerOrderId}`) ||
        changed;
    }

    if (brokerStatus.filled > 0) {
      if (order.filledQuantity !== brokerStatus.filled) {
        order.filledQuantity = brokerStatus.filled;
        changed = true;
      }
      changed =
        this.appendUniqueRiskNote(order, `filled_qty:${brokerStatus.filled}`) || changed;
    }

    if (brokerStatus.avgFillPrice > 0) {
      if (order.avgFillPrice !== brokerStatus.avgFillPrice) {
        order.avgFillPrice = brokerStatus.avgFillPrice;
        changed = true;
      }
      changed =
        this.appendUniqueRiskNote(
          order,
          `avg_fill_price:${brokerStatus.avgFillPrice.toFixed(4)}`
        ) || changed;
    }

    if (changed) {
      order.updatedAt = nowIso();
    }

    if (!changed) return;

    this.auditStore.saveOrder(order);
    this.auditStore.logEvent("order_status_synced", {
      orderId: order.id,
      brokerOrderId: brokerStatus.brokerOrderId,
      brokerStatus: brokerStatus.status,
      mappedStatus: order.status,
      filled: brokerStatus.filled,
      remaining: brokerStatus.remaining,
      avgFillPrice: brokerStatus.avgFillPrice,
      source: brokerStatus.source
    });

    if (order.intentType === "EXIT" && order.status === "FILLED") {
      this.markParentExited(order);
    }
  }

  private markParentExited(exitOrder: OrderIntent): void {
    if (!exitOrder.parentOrderId) return;

    const parent = this.auditStore.getOrder(exitOrder.parentOrderId);
    if (!parent) return;
    if (parent.status === "EXITED") return;

    parent.status = "EXITED";
    parent.updatedAt = nowIso();
    this.appendUniqueRiskNote(parent, `closed_by_exit:${exitOrder.id}`);
    if (typeof exitOrder.avgFillPrice === "number" && exitOrder.avgFillPrice > 0) {
      parent.avgFillPrice = exitOrder.avgFillPrice;
    }
    this.auditStore.saveOrder(parent);
    this.auditStore.logEvent("entry_position_exited", {
      parentOrderId: parent.id,
      exitOrderId: exitOrder.id,
      symbol: parent.symbol
    });
  }

  proposeOrder(
    symbol: string,
    decision: DecisionCard,
    chain: OptionContractSnapshot[],
    accountEquity: number
  ): OrderIntent {
    if (decision.action === "NO_TRADE") {
      throw new Error("Cannot propose an order for NO_TRADE decision.");
    }

    const selected = this.selectContract(decision.action, chain);
    if (!selected) {
      throw new Error("No option contract met DTE/liquidity filters.");
    }

    const limitPrice = Number(midPrice(selected).toFixed(2));
    const quantity = this.riskEngine.maxContractsForPremium(limitPrice, accountEquity);
    if (quantity < 1) {
      throw new Error("Account equity too small for per-trade premium cap.");
    }

    const order: OrderIntent = {
      id: makeId(),
      createdAt: nowIso(),
      updatedAt: nowIso(),
      intentType: "ENTRY",
      side: this.mapOrderSide(decision.action),
      symbol,
      action: decision.action,
      optionContract: selected,
      quantity,
      limitPrice,
      status: "PENDING_APPROVAL",
      riskNotes: [],
      decision
    };

    this.auditStore.saveOrder(order);
    this.auditStore.logEvent("order_proposed", {
      orderId: order.id,
      symbol,
      action: order.action,
      quantity,
      limitPrice
    });

    return order;
  }

  async approveOrder(
    orderId: string,
    approve: boolean,
    accountState: AccountState,
    comment = ""
  ): Promise<OrderIntent> {
    const order = this.auditStore.getOrder(orderId);
    if (!order) throw new Error("Order not found.");
    if (order.status !== "PENDING_APPROVAL") {
      throw new Error(`Order is not pending approval. Current status: ${order.status}`);
    }

    if (!approve) {
      order.status = "REJECTED_BY_USER";
      order.updatedAt = nowIso();
      if (comment) order.riskNotes.push(comment);
      this.auditStore.saveOrder(order);
      this.auditStore.logEvent("order_rejected", { orderId, comment });
      return order;
    }

    if (order.intentType === "ENTRY") {
      const openPositions = this.listOpenPositions();
      const riskState = this.riskEngine.buildRiskState(accountState, openPositions);
      const riskCheck = this.riskEngine.validateOrder(order, riskState, openPositions);
      if (!riskCheck.allowed) {
        order.status = "BLOCKED_RISK";
        order.updatedAt = nowIso();
        order.riskNotes.push(...riskCheck.reasons);
        this.auditStore.saveOrder(order);
        this.auditStore.logEvent("order_blocked", { orderId, reasons: riskCheck.reasons });
        return order;
      }
    }

    if (!settings.paperMode) {
      order.status = "BLOCKED_RISK";
      order.updatedAt = nowIso();
      order.riskNotes.push("live_mode_disabled_phase_1");
      this.auditStore.saveOrder(order);
      this.auditStore.logEvent("order_blocked", {
        orderId,
        reasons: ["live_mode_disabled_phase_1"]
      });
      return order;
    }

    if (order.action !== "CALL" && order.action !== "PUT") {
      throw new Error(`Unsupported order action for IBKR submission: ${order.action}`);
    }

    const brokerRef = await this.ibkr.submitPaperOrder({
      orderId: order.id,
      symbol: order.symbol,
      action: order.action,
      side: order.side,
      quantity: order.quantity,
      limitPrice: order.limitPrice,
      expiration: order.optionContract.expiration,
      strike: order.optionContract.strike,
      right: order.optionContract.right
    });

    order.status = "SUBMITTED_PAPER";
    order.updatedAt = nowIso();
    if (comment) order.riskNotes.push(comment);
    if (brokerRef) order.riskNotes.push(`broker_ref:${brokerRef}`);

    this.auditStore.saveOrder(order);
    this.auditStore.logEvent("order_approved", {
      orderId,
      status: order.status,
      brokerRef
    });

    return order;
  }

  listPendingOrders(): OrderIntent[] {
    return this.auditStore.listOrders({ status: "PENDING_APPROVAL", limit: 100 });
  }

  listRecentOrders(): OrderIntent[] {
    return this.auditStore.listOrders({ limit: 100 });
  }

  listOpenPositions(): OrderIntent[] {
    const openStatuses = new Set<OrderStatus>(["SUBMITTED_PAPER", "SUBMITTED_LIVE", "FILLED"]);
    return this.auditStore
      .listOrders({ limit: 500 })
      .filter((order) => order.intentType === "ENTRY" && openStatuses.has(order.status));
  }

  getLastAccountSnapshot(): IbkrAccountSnapshot | null {
    if (!this.lastAccountSnapshot) return null;
    return { ...this.lastAccountSnapshot };
  }

  async syncAccountState(accountState: AccountState): Promise<void> {
    const now = Date.now();
    if (this.accountSyncInFlight) {
      await this.accountSyncInFlight;
      return;
    }
    if (now - this.lastAccountSyncAt < this.accountSyncMinIntervalMs) return;

    this.accountSyncInFlight = this.syncAccountStateInternal(accountState);
    try {
      await this.accountSyncInFlight;
    } finally {
      this.lastAccountSyncAt = Date.now();
      this.accountSyncInFlight = null;
    }
  }

  private async syncAccountStateInternal(accountState: AccountState): Promise<void> {
    const snapshot = await this.ibkr.getAccountSnapshot();
    if (!snapshot) return;

    this.lastAccountSnapshot = snapshot;

    let changed = false;
    if (typeof snapshot.netLiquidation === "number" && snapshot.netLiquidation > 0) {
      if (accountState.accountEquity !== snapshot.netLiquidation) {
        accountState.accountEquity = snapshot.netLiquidation;
        changed = true;
      }
    }

    if (typeof snapshot.realizedPnl === "number") {
      if (accountState.dayRealizedPnl !== snapshot.realizedPnl) {
        accountState.dayRealizedPnl = snapshot.realizedPnl;
        changed = true;
      }
    }

    if (typeof snapshot.unrealizedPnl === "number") {
      if (accountState.dayUnrealizedPnl !== snapshot.unrealizedPnl) {
        accountState.dayUnrealizedPnl = snapshot.unrealizedPnl;
        changed = true;
      }
    }

    await this.maybeRunStartupReconciliation(accountState);

    if (!changed) return;

    this.auditStore.logEvent("account_state_synced", {
      accountEquity: accountState.accountEquity,
      dayRealizedPnl: accountState.dayRealizedPnl,
      dayUnrealizedPnl: accountState.dayUnrealizedPnl,
      source: snapshot.source,
      accountCode: snapshot.accountCode
    });
  }

  async runExitAutomation(_accountState: AccountState): Promise<OrderIntent[]> {
    const now = Date.now();
    if (this.exitAutomationInFlight) {
      return await this.exitAutomationInFlight;
    }
    if (now - this.lastExitAutomationAt < this.exitAutomationMinIntervalMs) return [];

    this.exitAutomationInFlight = this.runExitAutomationInternal();
    try {
      return await this.exitAutomationInFlight;
    } finally {
      this.lastExitAutomationAt = Date.now();
      this.exitAutomationInFlight = null;
    }
  }

  getRuntimeStatus(nowMs = Date.now()): {
    startupReconciled: boolean;
    startupReconcileInFlight: boolean;
    connectivity: {
      reachable: boolean | null;
      lastTransitionAt: string | null;
      lastTransitionInMs: number | null;
    };
    brokerStatusSync: {
      inFlight: boolean;
      lastRunAt: string | null;
      nextAvailableAt: string | null;
      nextAvailableInMs: number;
      minIntervalMs: number;
    };
    accountSync: {
      inFlight: boolean;
      lastRunAt: string | null;
      nextAvailableAt: string | null;
      nextAvailableInMs: number;
      minIntervalMs: number;
    };
    exitAutomation: {
      inFlight: boolean;
      lastRunAt: string | null;
      nextAvailableAt: string | null;
      nextAvailableInMs: number;
      minIntervalMs: number;
    };
  } {
    const toIso = (epochMs: number): string | null =>
      epochMs > 0 ? new Date(epochMs).toISOString() : null;
    const nextWindow = (
      lastRunAtMs: number,
      minIntervalMs: number
    ): { nextAt: string | null; inMs: number } => {
      const nextMs = Math.max(nowMs, lastRunAtMs + minIntervalMs);
      return {
        nextAt: toIso(nextMs),
        inMs: Math.max(0, nextMs - nowMs)
      };
    };

    const broker = nextWindow(this.lastBrokerSyncAt, this.brokerSyncMinIntervalMs);
    const account = nextWindow(this.lastAccountSyncAt, this.accountSyncMinIntervalMs);
    const exit = nextWindow(this.lastExitAutomationAt, this.exitAutomationMinIntervalMs);

    return {
      startupReconciled: this.startupReconciled,
      startupReconcileInFlight: Boolean(this.startupReconcileInFlight),
      connectivity: {
        reachable: this.lastConnectivityReachable,
        lastTransitionAt: toIso(this.lastConnectivityTransitionAtMs),
        lastTransitionInMs:
          this.lastConnectivityTransitionAtMs > 0
            ? Math.max(0, nowMs - this.lastConnectivityTransitionAtMs)
            : null
      },
      brokerStatusSync: {
        inFlight: Boolean(this.brokerSyncInFlight),
        lastRunAt: toIso(this.lastBrokerSyncAt),
        nextAvailableAt: broker.nextAt,
        nextAvailableInMs: broker.inMs,
        minIntervalMs: this.brokerSyncMinIntervalMs
      },
      accountSync: {
        inFlight: Boolean(this.accountSyncInFlight),
        lastRunAt: toIso(this.lastAccountSyncAt),
        nextAvailableAt: account.nextAt,
        nextAvailableInMs: account.inMs,
        minIntervalMs: this.accountSyncMinIntervalMs
      },
      exitAutomation: {
        inFlight: Boolean(this.exitAutomationInFlight),
        lastRunAt: toIso(this.lastExitAutomationAt),
        nextAvailableAt: exit.nextAt,
        nextAvailableInMs: exit.inMs,
        minIntervalMs: this.exitAutomationMinIntervalMs
      }
    };
  }

  private hasActiveExitForParent(parentOrderId: string): boolean {
    const activeStatuses = new Set<OrderStatus>([
      "PENDING_APPROVAL",
      "SUBMITTED_PAPER",
      "SUBMITTED_LIVE",
      "FILLED",
      "REJECTED_BY_USER",
      "BLOCKED_RISK"
    ]);

    return this.auditStore
      .listOrders({ limit: 1_000 })
      .some(
        (order) =>
          order.intentType === "EXIT" &&
          order.parentOrderId === parentOrderId &&
          activeStatuses.has(order.status)
      );
  }

  private buildExitDecision(base: DecisionCard, reason: string, details: string): DecisionCard {
    return {
      ...base,
      timestamp: nowIso(),
      confidence: Math.max(base.confidence, 0.6),
      rationale: `Exit automation (${reason}): ${details}`,
      vetoFlags: Array.from(new Set([...base.vetoFlags, `auto_exit:${reason}`]))
    };
  }

  private async runExitAutomationInternal(): Promise<OrderIntent[]> {
    const policy = this.runtimePolicy.getPolicy();
    const openEntries = this.auditStore
      .listOrders({ limit: 1_000 })
      .filter((order) => order.intentType === "ENTRY" && order.status === "FILLED");

    const brokerPositionsFetcher = (
      this.ibkr as unknown as {
        getPositionsSnapshot?: () => Promise<IbkrPositionSnapshot[]>;
      }
    ).getPositionsSnapshot;
    const brokerPositions =
      typeof brokerPositionsFetcher === "function"
        ? await brokerPositionsFetcher.call(this.ibkr)
        : [];
    const brokerMarkByContract = new Map<string, number>();
    for (const position of brokerPositions) {
      const key = this.optionContractKeyFromPosition(position);
      if (!key) continue;
      if (typeof position.marketPrice !== "number" || !Number.isFinite(position.marketPrice) || position.marketPrice <= 0) continue;
      brokerMarkByContract.set(key, position.marketPrice);
    }

    const proposed: OrderIntent[] = [];

    for (const entry of openEntries) {
      if (this.hasActiveExitForParent(entry.id)) continue;

      const entryPrice = entry.avgFillPrice ?? entry.limitPrice;
      if (!Number.isFinite(entryPrice) || entryPrice <= 0) continue;

      const now = Date.now();
      const created = new Date(entry.createdAt).getTime();
      const holdDays = Number.isFinite(created)
        ? Math.max(0, (now - created) / (24 * 60 * 60 * 1000))
        : 0;
      const roundedHoldDays = Number(holdDays.toFixed(2));

      const preEventWindowHours = Math.max(0, policy.preEventExitWindowHours ?? 0);
      const secFilingLookbackHours = Math.max(
        0,
        policy.preEventSecFilingLookbackHours ?? 0
      );
      const secFilingRiskThreshold = Math.max(
        0.1,
        Math.min(1, policy.preEventSecFilingRiskThreshold ?? 0.55)
      );
      let preEventContext: {
        eventDate: string;
        eventAtMs: number;
        hoursUntilEvent: number;
        source: string;
        trigger: "earnings_upcoming" | "sec_filing_recent";
        secRisk?: number;
        secForm?: string | null;
      } | null = null;
      if (preEventWindowHours > 0) {
        const nextEvent = await this.getNextBinaryEvent(entry.symbol);
        if (nextEvent.eventDate) {
          const eventAtMs = this.parseBinaryEventAtMs(nextEvent.eventDate);
          if (eventAtMs !== null) {
            const hoursUntilEvent = (eventAtMs - now) / (60 * 60 * 1000);
            if (hoursUntilEvent >= 0 && hoursUntilEvent <= preEventWindowHours) {
              preEventContext = {
                eventDate: nextEvent.eventDate,
                eventAtMs,
                hoursUntilEvent,
                source: nextEvent.source,
                trigger: "earnings_upcoming"
              };
            }
          }
        }
      }
      if (secFilingLookbackHours > 0) {
        const secSnapshot = await this.getSecEventSnapshot(entry.symbol);
        if (
          secSnapshot.source === "sec_edgar" &&
          secSnapshot.latestFilingDate &&
          secSnapshot.eventRisk >= secFilingRiskThreshold
        ) {
          const filingAtMs = Date.parse(`${secSnapshot.latestFilingDate}T00:00:00Z`);
          if (Number.isFinite(filingAtMs)) {
            const hoursSinceFiling = (now - filingAtMs) / (60 * 60 * 1000);
            if (hoursSinceFiling >= 0 && hoursSinceFiling <= secFilingLookbackHours) {
              const secContext = {
                eventDate: secSnapshot.latestFilingDate,
                eventAtMs: filingAtMs,
                hoursUntilEvent: -hoursSinceFiling,
                source: secSnapshot.source,
                trigger: "sec_filing_recent" as const,
                secRisk: secSnapshot.eventRisk,
                secForm: secSnapshot.latestForm
              };
              if (
                !preEventContext ||
                Math.abs(secContext.hoursUntilEvent) < Math.abs(preEventContext.hoursUntilEvent)
              ) {
                preEventContext = secContext;
              }
            }
          }
        }
      }

      const contractKey = this.optionContractKey({
        symbol: entry.optionContract.symbol,
        expiration: entry.optionContract.expiration,
        strike: entry.optionContract.strike,
        right: entry.optionContract.right
      });
      const brokerMark = brokerMarkByContract.get(contractKey);
      const markPrice =
        typeof brokerMark === "number" && Number.isFinite(brokerMark) && brokerMark > 0
          ? brokerMark
          : await this.ibkr.getOptionMidPrice(entry.optionContract);
      const pnlPct =
        typeof markPrice === "number" && markPrice > 0
          ? (markPrice - entryPrice) / entryPrice
          : null;

      let exitReason: "take_profit" | "stop_loss" | "max_hold" | "pre_event" | null = null;
      if (preEventContext) exitReason = "pre_event";
      else if (pnlPct !== null && pnlPct >= policy.takeProfitPct) exitReason = "take_profit";
      else if (pnlPct !== null && pnlPct <= -policy.stopLossPct) exitReason = "stop_loss";
      else if (holdDays >= policy.maxHoldDays) exitReason = "max_hold";

      if (!exitReason) continue;

      const quantity = Math.max(1, Math.round(entry.filledQuantity ?? entry.quantity));
      const limitPrice = Number(
        Math.max(markPrice ?? entry.optionContract.last ?? entry.limitPrice, 0.01).toFixed(2)
      );

      const detailsParts = [
        `entry=${entryPrice.toFixed(2)}`,
        `mark=${(markPrice ?? limitPrice).toFixed(2)}`,
        `markSource=${typeof brokerMark === "number" ? "broker_position" : "option_quote"}`,
        pnlPct === null ? "pnl=unavailable" : `pnl=${(pnlPct * 100).toFixed(2)}%`,
        `holdDays=${roundedHoldDays}`
      ];
      if (preEventContext) {
        detailsParts.push(`eventDate=${preEventContext.eventDate}`);
        if (preEventContext.hoursUntilEvent >= 0) {
          detailsParts.push(`eventIn=${preEventContext.hoursUntilEvent.toFixed(2)}h`);
        } else {
          detailsParts.push(`eventAgo=${Math.abs(preEventContext.hoursUntilEvent).toFixed(2)}h`);
        }
        detailsParts.push(`eventTrigger=${preEventContext.trigger}`);
        detailsParts.push(`eventSource=${preEventContext.source}`);
        if (typeof preEventContext.secRisk === "number") {
          detailsParts.push(`secRisk=${preEventContext.secRisk.toFixed(3)}`);
        }
        if (preEventContext.secForm) {
          detailsParts.push(`secForm=${preEventContext.secForm}`);
        }
      }
      const details = detailsParts.join(", ");

      const order: OrderIntent = {
        id: makeId(),
        createdAt: nowIso(),
        updatedAt: nowIso(),
        intentType: "EXIT",
        side: "SELL",
        parentOrderId: entry.id,
        exitReason,
        symbol: entry.symbol,
        action: entry.action,
        optionContract: entry.optionContract,
        quantity,
        limitPrice,
        status: "PENDING_APPROVAL",
        riskNotes: [`auto_exit:${exitReason}`, details],
        decision: this.buildExitDecision(entry.decision, exitReason, details)
      };

      this.auditStore.saveOrder(order);
      this.auditStore.logEvent("exit_order_proposed", {
        orderId: order.id,
        parentOrderId: entry.id,
        symbol: order.symbol,
        action: order.action,
        quantity: order.quantity,
        limitPrice: order.limitPrice,
        exitReason,
        holdDays: roundedHoldDays,
        pnlPct,
        preEventWindowHours,
        secFilingLookbackHours,
        secFilingRiskThreshold,
        nextBinaryEventDate: preEventContext?.eventDate ?? null,
        nextBinaryEventAt: preEventContext ? new Date(preEventContext.eventAtMs).toISOString() : null,
        nextBinaryEventHoursUntil: preEventContext?.hoursUntilEvent ?? null,
        nextBinaryEventSource: preEventContext?.source ?? null,
        nextBinaryEventTrigger: preEventContext?.trigger ?? null,
        nextBinaryEventSecRisk: preEventContext?.secRisk ?? null,
        nextBinaryEventSecForm: preEventContext?.secForm ?? null
      });
      proposed.push(order);
    }

    return proposed;
  }

  private selectContract(
    action: TradeAction,
    chain: OptionContractSnapshot[]
  ): OptionContractSnapshot | null {
    const policy = this.runtimePolicy.getPolicy();
    const targetRight = action === "CALL" ? "CALL" : "PUT";
    const now = Date.now();

    const filtered = chain.filter((contract) => {
      if (contract.right !== targetRight) return false;
      if (contract.openInterest < 150 || contract.volume < 30) return false;
      const dte = Math.floor((new Date(contract.expiration).getTime() - now) / (1000 * 60 * 60 * 24));
      if (dte < policy.dteMin || dte > policy.dteMax) return false;
      const mid = midPrice(contract);
      const spreadPct = mid > 0 ? (contract.ask - contract.bid) / mid : 1;
      return spreadPct <= 0.2;
    });

    if (filtered.length === 0) return null;

    const targetDelta = 0.35;
    filtered.sort((a, b) => {
      const aMid = midPrice(a);
      const bMid = midPrice(b);
      const aSpread = aMid > 0 ? (a.ask - a.bid) / aMid : 999;
      const bSpread = bMid > 0 ? (b.ask - b.bid) / bMid : 999;

      return (
        Math.abs(Math.abs(a.delta) - targetDelta) -
        Math.abs(Math.abs(b.delta) - targetDelta) ||
        aSpread - bSpread ||
        b.openInterest - a.openInterest
      );
    });

    return filtered[0];
  }
}
