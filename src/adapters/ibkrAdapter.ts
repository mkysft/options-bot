import { settings } from "../core/config";
import { logger } from "../core/logger";
import { apiRequestLogStore } from "../storage/apiRequestLogStore";
import type { DailyBar } from "../types/models";
import { spawn } from "node:child_process";
import { existsSync, readdirSync } from "node:fs";
import type { Dirent } from "node:fs";
import os from "node:os";
import { basename, join } from "node:path";
import { Socket } from "node:net";
import type { IbScannerCode, IbScannerInstrument, IbScannerLocation, IbScannerRow } from "./ibkrClientCompat";
import { Client, Contract, Order } from "./ibkrClientCompat";

export interface IbkrQuote {
  symbol: string;
  last: number;
  bid: number;
  ask: number;
  volume: number;
}

export interface IbkrOptionQuote {
  symbol: string;
  expiration: string;
  strike: number;
  right: "CALL" | "PUT";
  bid: number;
  ask: number;
  last: number;
  mid: number;
  volume: number;
  openInterest: number;
  impliedVol: number | null;
  delta: number | null;
  gamma: number | null;
}

export interface IbkrConnectionStatus {
  enabled: boolean;
  host: string;
  port: number;
  clientId: number;
  reachable: boolean;
  latencyMs: number | null;
  detectedMode: "paper" | "live" | "unknown";
  probedPorts: number[];
  message: string;
}

export interface IbkrLaunchResult {
  target: "gateway" | "tws";
  launched: boolean;
  dryRun: boolean;
  platform: NodeJS.Platform;
  message: string;
  commandPreview: string;
  selectedApp?: string;
  attemptedApps?: string[];
}

export interface IbkrSubmitOrderPayload {
  orderId: string;
  symbol: string;
  action: "CALL" | "PUT";
  side: "BUY" | "SELL";
  quantity: number;
  limitPrice: number;
  expiration: string;
  strike: number;
  right: "CALL" | "PUT";
}

export interface IbkrOrderStatusSnapshot {
  localOrderId?: string;
  brokerOrderId: number;
  status: string;
  filled: number;
  remaining: number;
  avgFillPrice: number;
  lastFillPrice: number;
  permId: number;
  clientId: number;
  whyHeld: string;
  source: "event" | "open_order" | "submit";
  updatedAt: string;
}

export interface IbkrPositionSnapshot {
  account?: string;
  conId: number;
  symbol: string;
  secType: string;
  expiration?: string;
  strike?: number;
  right?: "CALL" | "PUT";
  multiplier: number;
  position: number;
  avgCost: number;
  marketPrice?: number;
  marketValue?: number;
  unrealizedPnl?: number;
  realizedPnl?: number;
  marketDataUpdatedAt?: string;
}

export interface IbkrAccountSnapshot {
  timestamp: string;
  accountCode?: string;
  netLiquidation?: number;
  realizedPnl?: number;
  unrealizedPnl?: number;
  totalCashValue?: number;
  availableFunds?: number;
  buyingPower?: number;
  positionCount: number;
  source: "account_updates" | "positions_mark_to_market" | "unavailable";
}

export interface IbkrScannerRequest {
  limit?: number;
  instrument?: IbScannerInstrument;
  locationCode?: IbScannerLocation;
  scanCode?: IbScannerCode;
  abovePrice?: number;
  belowPrice?: number;
  aboveVolume?: number;
  stockTypeFilter?: string;
}

export type IbkrScannerSource = "tws_socket" | "none";

export interface IbkrScannerResult {
  symbols: string[];
  source: IbkrScannerSource;
  fallbackReason: string;
}

type IbTickerSnapshot = Record<string, unknown>;
type IbHistoricalBar = {
  date?: string;
  open?: number;
  high?: number;
  low?: number;
  close?: number;
  volume?: number;
};
type IbHistoricalData = {
  bars?: IbHistoricalBar[];
};
type IbContractDetails = {
  contract?: {
    conId?: number;
  };
};
type IbSecDefOptParam = {
  exchange?: string;
  tradingClass?: string;
  expirations?: string[];
  strikes?: number[];
};
type IbOrderStatusEvent = Record<string, unknown>;
type IbAccountValueEntry = {
  value: string;
  currency: string;
  accountCode: string;
  updatedAt: string;
};
type IbPortfolioValueEntry = {
  conId: number;
  symbol: string;
  secType: string;
  marketPrice: number;
  marketValue: number;
  averageCost: number;
  unrealizedPnl: number;
  realizedPnl: number;
  position: number;
  updatedAt: string;
};
type IbOpenOrderRow = {
  contract?: Record<string, unknown>;
  order?: Record<string, unknown>;
  orderState?: Record<string, unknown>;
};
type IbPositionRowMap = Record<
  string,
  {
    contract?: Record<string, unknown>;
    position?: number;
    avgCost?: number;
  }
>;

interface IbkrQueueTask<T> {
  id: number;
  channel: string;
  operation: string;
  enqueuedAt: number;
  run: () => Promise<T>;
  resolve: (value: T) => void;
  reject: (error: unknown) => void;
}

interface IbkrScannerCacheEntry {
  key: string;
  symbols: string[];
  expiresAt: number;
}

const isTestRuntime = (): boolean =>
  settings.appEnv === "test" || Boolean(process.env.BUN_TEST);

const asFiniteNumber = (value: unknown): number | null => {
  const num = typeof value === "number" ? value : typeof value === "string" ? Number(value) : Number.NaN;
  return Number.isFinite(num) ? num : null;
};

const pickNumber = (...values: unknown[]): number | null => {
  for (const value of values) {
    const parsed = asFiniteNumber(value);
    if (parsed !== null) return parsed;
  }
  return null;
};

const parseExpirationDate = (raw: string): Date | null => {
  const compact = raw.replace(/\D/g, "");
  if (compact.length < 8) return null;

  const year = Number(compact.slice(0, 4));
  const month = Number(compact.slice(4, 6));
  const day = Number(compact.slice(6, 8));
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) return null;

  const parsed = new Date(Date.UTC(year, month - 1, day, 16, 0, 0));
  return Number.isNaN(parsed.getTime()) ? null : parsed;
};

const dteBetween = (target: Date, now: Date): number => {
  const msPerDay = 24 * 60 * 60 * 1000;
  return Math.floor((target.getTime() - now.getTime()) / msPerDay);
};

const uniq = <T>(items: T[]): T[] => [...new Set(items)];

const modeFromPort = (port: number): "paper" | "live" | "unknown" => {
  if (port === 4002 || port === 7497) return "paper";
  if (port === 4001 || port === 7496) return "live";
  return "unknown";
};

const INCOME_ACCT_VALUE = 6;
const INCOME_PORTFOLIO_VALUE = 7;
const INCOME_ACCT_UPDATE_TIME = 8;
const INCOME_ACCT_DOWNLOAD_END = 54;

const HISTORICAL_FRACTIONAL_WARNING = "fractional share size rules";

let ibIncomeNoisePatchApplied = false;
const patchIbIncomeNoise = (): void => {
  if (ibIncomeNoisePatchApplied) return;
  const candidate = Client as unknown as {
    prototype?: {
      _onMessageFieldset?: (fields: unknown) => void;
    };
  };

  const original = candidate.prototype?._onMessageFieldset;
  if (typeof original !== "function") return;

  candidate.prototype!._onMessageFieldset = function patchedOnMessageFieldset(
    this: unknown,
    fields: unknown
  ): void {
    if (Array.isArray(fields) && fields.length > 0) {
      const typeId = asFiniteNumber(fields[0]);
      if (
        typeId === INCOME_ACCT_VALUE ||
        typeId === INCOME_PORTFOLIO_VALUE ||
        typeId === INCOME_ACCT_UPDATE_TIME ||
        typeId === INCOME_ACCT_DOWNLOAD_END
      ) {
        return;
      }
    }
    original.call(this, fields);
  };

  ibIncomeNoisePatchApplied = true;
};

const normalizeOptionRightForModel = (right: unknown): "CALL" | "PUT" | undefined => {
  const normalized = String(right ?? "").trim().toUpperCase();
  if (normalized === "C" || normalized === "CALL") return "CALL";
  if (normalized === "P" || normalized === "PUT") return "PUT";
  return undefined;
};

const normalizeExpirationForModel = (raw: unknown): string | undefined => {
  const compact = String(raw ?? "").replace(/\D/g, "");
  if (compact.length !== 8) return undefined;
  return `${compact.slice(0, 4)}-${compact.slice(4, 6)}-${compact.slice(6, 8)}`;
};

/**
 * Official-API adapter shell for IBKR TWS/Gateway integration.
 *
 * This implementation intentionally returns nulls by default unless IBKR
 * integration is wired with a supported Node client in your environment.
 * It keeps the system deterministic by allowing synthetic fallback.
 */
export class IbkrAdapter {
  private readonly warnThrottleMs = 45_000;
  private readonly requestCooldownMs = 8_000;
  private readonly optionQuoteEntitlementBackoffMs = 15 * 60_000;
  private readonly invalidOptionContractBackoffMs = 6 * 60 * 60_000;
  private readonly quoteSubscriptionBackoffMs = 10 * 60_000;
  private readonly openOrdersRetryMs = 10_000;
  private readonly accountSubscriptionRetryMs = 12_000;
  private readonly connectivityRecoveryThrottleMs = 6_000;
  private readonly scannerCacheTtlMs = 5 * 60_000;
  private readonly scannerFailureCooldownBaseMs = 20_000;
  private readonly scannerFailureCooldownMaxMs = 3 * 60_000;
  private readonly queueMaxSize = 750;
  private readonly connectivityCacheTtlMs = 2_500;
  private readonly accountSnapshotTtlMs = 8_000;
  private readonly openOrdersRefreshMinIntervalMs = 5_000;
  private readonly lowPriorityQueueDepthThreshold = Math.max(
    6,
    Math.round(settings.ibkrQueueMaxConcurrent * 3)
  );
  private readonly queueChannelIntervalMs: Record<string, number> = {
    connectivity: 35,
    quote: 30,
    option_chain: 50,
    historical: 120,
    positions: 60,
    account: 50,
    order: 30,
    scanner: 120,
    default: 40
  };

  private readonly clients = new Map<number, Client>();
  private clientSignature = `${settings.ibkrHost}:${settings.ibkrClientId}`;
  private activePort = settings.ibkrPort;
  private currentMarketDataType: number | null = null;
  private marketDataDelayedOnly = false;
  private optionQuoteEntitlementUntilMs = 0;
  private optionQuoteEntitlementReason = "";
  private quoteSubscriptionBlockedUntilBySymbol = new Map<string, number>();
  private invalidOptionContracts = new Map<string, { untilMs: number; reason: string }>();
  private historicalFractionalRulesUnsupported = false;
  private historicalFractionalRulesWarned = false;
  private positionsRequestUnsupported = false;
  private readonly orderStatusStreamAttachedClients = new Set<Client>();
  private localToBrokerOrder = new Map<string, number>();
  private brokerToLocalOrder = new Map<number, string>();
  private brokerOrderStatuses = new Map<number, IbkrOrderStatusSnapshot>();
  private readonly rawFieldsetStreamAttachedClients = new Set<Client>();
  private accountUpdatesSubscribedPort: number | null = null;
  private accountValues = new Map<string, IbAccountValueEntry>();
  private portfolioValues = new Map<number, IbPortfolioValueEntry>();
  private accountUpdateTimestamp = "";
  private accountUpdateDownloadEndAt = "";
  private connectivityStatusInFlight: Promise<IbkrConnectionStatus> | null = null;
  private lastConnectivityStatus: IbkrConnectionStatus | null = null;
  private lastConnectivityStatusAtMs = 0;
  private accountSnapshotInFlight: Promise<IbkrAccountSnapshot | null> | null = null;
  private lastAccountSnapshot: IbkrAccountSnapshot | null = null;
  private lastAccountSnapshotAtMs = 0;
  private lastOpenOrdersRefreshAtMs = 0;
  private warningNextLogAt = new Map<string, number>();
  private requestCooldownUntilMs = 0;
  private requestCooldownReason = "";
  private nextOpenOrdersAttemptAt = 0;
  private nextAccountSubscriptionAttemptAt = 0;
  private connectivityRecoveryInFlight: Promise<void> | null = null;
  private lastConnectivityRecoveryAt = 0;
  private lastConnectivityReachable = false;
  private lastConnectivityPort: number | null = null;
  private queueTaskId = 0;
  private readonly queueTasks: Array<IbkrQueueTask<unknown>> = [];
  private queueActiveWorkers = 0;
  private queueDispatchScheduled = false;
  private queueNextGlobalStartAt = 0;
  private queueNextStartAtByChannel = new Map<string, number>();
  private scannerCache: IbkrScannerCacheEntry | null = null;
  private readonly scannerInFlightByKey = new Map<string, Promise<string[]>>();
  private scannerLastAttemptAtMs = 0;
  private scannerLastSuccessAtMs = 0;
  private scannerLastErrorAtMs = 0;
  private scannerLastErrorMessage = "";
  private scannerFailureStreak = 0;
  private scannerBackoffUntilMs = 0;
  private scannerLastSource: IbkrScannerSource = "none";
  private scannerLastFallbackReason = "";
  private readonly positionsSnapshotTtlMs = 4_000;
  private positionsSnapshotInFlight: Promise<IbkrPositionSnapshot[]> | null = null;
  private lastPositionsSnapshotAtMs = 0;
  private lastPositionsSnapshot: IbkrPositionSnapshot[] = [];

  private isEnabled(): boolean {
    return settings.ibkrEnabled && !isTestRuntime();
  }

  private get queueGlobalMinIntervalMs(): number {
    return Math.max(5, Math.round(settings.ibkrQueueGlobalMinIntervalMs));
  }

  private get queueMaxConcurrentWorkers(): number {
    return Math.max(1, Math.min(8, Math.round(settings.ibkrQueueMaxConcurrent)));
  }

  private channelIntervalMs(channel: string): number {
    const overrides: Record<string, number> = {
      quote: settings.ibkrQueueQuoteIntervalMs,
      option_chain: settings.ibkrQueueOptionChainIntervalMs,
      historical: settings.ibkrQueueHistoricalIntervalMs,
      scanner: settings.ibkrQueueScannerIntervalMs
    };
    const baseline = this.queueChannelIntervalMs[channel] ?? this.queueChannelIntervalMs.default;
    const override = overrides[channel];
    const candidate = Number.isFinite(override) ? override : baseline;
    return Math.max(5, Math.round(candidate));
  }

  private clonePositionsSnapshot(positions: IbkrPositionSnapshot[]): IbkrPositionSnapshot[] {
    return positions.map((position) => ({ ...position }));
  }

  private cloneConnectivityStatus(status: IbkrConnectionStatus): IbkrConnectionStatus {
    return {
      ...status,
      probedPorts: [...status.probedPorts]
    };
  }

  private isQueueBacklogged(): boolean {
    return this.queueTasks.length + this.queueActiveWorkers >= this.lowPriorityQueueDepthThreshold;
  }

  private sleep(ms: number): Promise<void> {
    if (ms <= 0) return Promise.resolve();
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  private clearQueuedRequests(reason: string): void {
    if (this.queueTasks.length === 0) {
      this.queueNextGlobalStartAt = 0;
      this.queueNextStartAtByChannel.clear();
      return;
    }

    const pending = [...this.queueTasks];
    this.queueTasks.length = 0;
    this.queueNextGlobalStartAt = 0;
    this.queueNextStartAtByChannel.clear();
    for (const task of pending) {
      task.reject(new Error(`IBKR request queue cleared: ${reason}`));
    }
  }

  private enqueueRequest<T>(params: {
    channel: string;
    operation: string;
    run: () => Promise<T>;
  }): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      if (this.queueTasks.length >= this.queueMaxSize) {
        reject(
          new Error(
            `IBKR request queue overloaded (${this.queueTasks.length}/${this.queueMaxSize}) while scheduling ${params.operation}.`
          )
        );
        return;
      }

      const task: IbkrQueueTask<T> = {
        id: ++this.queueTaskId,
        channel: params.channel,
        operation: params.operation,
        enqueuedAt: Date.now(),
        run: params.run,
        resolve,
        reject
      };
      this.queueTasks.push(task as IbkrQueueTask<unknown>);
      this.dispatchQueue();
    });
  }

  private reserveQueueStartDelay(channel: string): number {
    const now = Date.now();
    const channelNextAt = this.queueNextStartAtByChannel.get(channel) ?? 0;
    const startAt = Math.max(now, this.queueNextGlobalStartAt, channelNextAt);

    this.queueNextGlobalStartAt = startAt + this.queueGlobalMinIntervalMs;
    this.queueNextStartAtByChannel.set(channel, startAt + this.channelIntervalMs(channel));
    return Math.max(0, startAt - now);
  }

  private async runQueuedTask(task: IbkrQueueTask<unknown>): Promise<void> {
    const waitMs = this.reserveQueueStartDelay(task.channel);
    if (waitMs > 0) {
      await this.sleep(waitMs);
    }

    const queueDelayMs = Date.now() - task.enqueuedAt;
    if (queueDelayMs >= 3_500) {
      this.warnThrottled(
        `queue_delay_${task.channel}`,
        `IBKR request queue delay ${queueDelayMs}ms for ${task.operation} (depth=${this.queueTasks.length}, workers=${this.queueActiveWorkers}/${this.queueMaxConcurrentWorkers}).`,
        15_000
      );
    }

    try {
      const value = await task.run();
      task.resolve(value);
    } catch (error) {
      task.reject(error);
    }
  }

  private dispatchQueue(): void {
    if (this.queueDispatchScheduled) return;
    this.queueDispatchScheduled = true;

    queueMicrotask(() => {
      this.queueDispatchScheduled = false;

      while (
        this.queueActiveWorkers < this.queueMaxConcurrentWorkers &&
        this.queueTasks.length > 0
      ) {
        const task = this.queueTasks.shift();
        if (!task) continue;

        this.queueActiveWorkers += 1;
        void this.runQueuedTask(task)
          .catch(() => {
            // task promise handles its own rejection through task.reject.
          })
          .finally(() => {
            this.queueActiveWorkers = Math.max(0, this.queueActiveWorkers - 1);
            if (this.queueTasks.length > 0) {
              this.dispatchQueue();
            }
          });
      }
    });
  }

  private clearClients(): void {
    this.clearQueuedRequests("client reset");
    for (const client of this.clients.values()) {
      try {
        (client as unknown as { disconnect?: () => void }).disconnect?.();
      } catch {
        // ignore disconnect failures while resetting clients
      }
    }
    this.clients.clear();
    this.orderStatusStreamAttachedClients.clear();
    this.rawFieldsetStreamAttachedClients.clear();
    this.accountUpdatesSubscribedPort = null;
    this.accountValues.clear();
    this.portfolioValues.clear();
    this.accountUpdateTimestamp = "";
    this.accountUpdateDownloadEndAt = "";
    this.connectivityStatusInFlight = null;
    this.lastConnectivityStatus = null;
    this.lastConnectivityStatusAtMs = 0;
    this.accountSnapshotInFlight = null;
    this.lastAccountSnapshot = null;
    this.lastAccountSnapshotAtMs = 0;
    this.lastOpenOrdersRefreshAtMs = 0;
    this.requestCooldownUntilMs = 0;
    this.requestCooldownReason = "";
    this.nextOpenOrdersAttemptAt = 0;
    this.nextAccountSubscriptionAttemptAt = 0;
    this.warningNextLogAt.clear();
    this.positionsRequestUnsupported = false;
    this.marketDataDelayedOnly = false;
    this.optionQuoteEntitlementUntilMs = 0;
    this.optionQuoteEntitlementReason = "";
    this.quoteSubscriptionBlockedUntilBySymbol.clear();
    this.invalidOptionContracts.clear();
    this.historicalFractionalRulesUnsupported = false;
    this.historicalFractionalRulesWarned = false;
    this.connectivityRecoveryInFlight = null;
    this.lastConnectivityRecoveryAt = 0;
    this.lastConnectivityReachable = false;
    this.lastConnectivityPort = null;
    this.scannerCache = null;
    this.scannerInFlightByKey.clear();
    this.scannerLastAttemptAtMs = 0;
    this.scannerLastSuccessAtMs = 0;
    this.scannerLastErrorAtMs = 0;
    this.scannerLastErrorMessage = "";
    this.scannerFailureStreak = 0;
    this.scannerBackoffUntilMs = 0;
    this.scannerLastSource = "none";
    this.scannerLastFallbackReason = "";
    this.positionsSnapshotInFlight = null;
    this.lastPositionsSnapshotAtMs = 0;
    this.lastPositionsSnapshot = [];
  }

  private dropTransportClients(): void {
    this.clearQueuedRequests("transport clients dropped");
    for (const client of this.clients.values()) {
      try {
        (client as unknown as { disconnect?: () => void }).disconnect?.();
      } catch {
        // ignore disconnect failures while pruning clients
      }
    }
    this.clients.clear();
    this.orderStatusStreamAttachedClients.clear();
    this.rawFieldsetStreamAttachedClients.clear();
    this.accountUpdatesSubscribedPort = null;
    this.currentMarketDataType = null;
    this.connectivityStatusInFlight = null;
    this.lastConnectivityStatus = null;
    this.lastConnectivityStatusAtMs = 0;
    this.accountSnapshotInFlight = null;
    this.lastAccountSnapshot = null;
    this.lastAccountSnapshotAtMs = 0;
    this.lastOpenOrdersRefreshAtMs = 0;
    this.scannerCache = null;
    this.scannerInFlightByKey.clear();
    this.quoteSubscriptionBlockedUntilBySymbol.clear();
    this.scannerFailureStreak = 0;
    this.scannerBackoffUntilMs = 0;
    this.scannerLastSource = "none";
    this.scannerLastFallbackReason = "";
  }

  private pruneClients(keepPorts: number[]): void {
    const keep = new Set(keepPorts);
    for (const [port, client] of this.clients.entries()) {
      if (keep.has(port)) continue;
      try {
        (client as unknown as { disconnect?: () => void }).disconnect?.();
      } catch {
        // ignore disconnect failures while pruning clients
      }
      this.clients.delete(port);
      this.orderStatusStreamAttachedClients.delete(client);
      this.rawFieldsetStreamAttachedClients.delete(client);
      if (this.accountUpdatesSubscribedPort === port) {
        this.accountUpdatesSubscribedPort = null;
      }
    }
  }

  reloadConfiguration(): void {
    this.clearClients();
    this.clientSignature = `${settings.ibkrHost}:${settings.ibkrClientId}`;
    this.activePort = settings.ibkrPort;
    this.currentMarketDataType = null;
  }

  private ensureClientConfiguration(): void {
    const signature = `${settings.ibkrHost}:${settings.ibkrClientId}`;
    if (signature === this.clientSignature) return;
    this.clearClients();
    this.clientSignature = signature;
    this.currentMarketDataType = null;
  }

  private getClient(port = this.activePort): Client {
    patchIbIncomeNoise();
    this.ensureClientConfiguration();

    const existing = this.clients.get(port);
    if (existing) {
      this.ensureOrderStatusStream(existing);
      this.ensureRawFieldsetStream(existing);
      return existing;
    }

    const client = new Client({
      host: settings.ibkrHost,
      port,
      clientId: settings.ibkrClientId,
      timeoutMs: Math.max(2_000, Math.round(settings.ibkrClientTimeoutMs))
    });
    this.clients.set(port, client);
    this.ensureOrderStatusStream(client);
    this.ensureRawFieldsetStream(client);
    return client;
  }

  private selectActivePort(port: number): void {
    if (this.activePort === port) return;
    this.activePort = port;
    this.currentMarketDataType = null;
  }

  private async setMarketDataType(marketDataType: number): Promise<void> {
    if (!this.isEnabled()) return;
    if (this.isRequestCoolingDown("market data type")) return;
    if (this.currentMarketDataType === marketDataType) return;
    const startedMs = Date.now();
    const startedAt = new Date(startedMs).toISOString();
    try {
      await this.enqueueRequest({
        channel: "quote",
        operation: "reqMarketDataType",
        run: async () => {
          await this.getClient(this.activePort).reqMarketDataType(marketDataType);
        }
      });
      this.currentMarketDataType = marketDataType;
      this.onRequestSuccess();
      this.logIbkrExternalRequest({
        startedMs,
        startedAt,
        operation: "reqMarketDataType",
        reason: "Set IBKR market data mode",
        requestPayload: {
          marketDataType,
          port: this.activePort
        },
        status: "success"
      });
    } catch (error) {
      const message = (error as Error).message;
      this.logIbkrExternalRequest({
        startedMs,
        startedAt,
        operation: "reqMarketDataType",
        reason: "Set IBKR market data mode",
        requestPayload: {
          marketDataType,
          port: this.activePort
        },
        status: "error",
        errorMessage: message
      });
      if (this.isConnectivityError(error)) {
        this.enterRequestCooldown(message);
        this.warnThrottled("req_mkt_data_type_connectivity", `IBKR reqMarketDataType failed: ${message}`);
        return;
      }
      this.warnThrottled("req_mkt_data_type_generic", `IBKR reqMarketDataType failed: ${message}`);
    }
  }

  private warnThrottled(key: string, message: string, intervalMs = this.warnThrottleMs): void {
    const now = Date.now();
    const nextAllowedAt = this.warningNextLogAt.get(key) ?? 0;
    if (now < nextAllowedAt) return;
    this.warningNextLogAt.set(key, now + intervalMs);
    logger.warn(message);
  }

  private get retryMaxAttempts(): number {
    return Math.max(1, Math.min(4, Math.round(settings.ibkrRetryMaxAttempts)));
  }

  private get retryBaseDelayMs(): number {
    return Math.max(25, Math.round(settings.ibkrRetryBaseDelayMs));
  }

  private get retryMaxDelayMs(): number {
    return Math.max(this.retryBaseDelayMs, Math.round(settings.ibkrRetryMaxDelayMs));
  }

  private isConnectivityError(error: unknown): boolean {
    const message = String((error as Error)?.message ?? "").toLowerCase();
    return (
      message.includes("failed to connect") ||
      message.includes("not connected") ||
      message.includes("socket") ||
      message.includes("econnrefused") ||
      message.includes("timed out") ||
      message.includes("timeout")
    );
  }

  private isRetryableTransientError(
    error: unknown,
    options?: { allowDuplicateScanner?: boolean }
  ): boolean {
    if (this.isConnectivityError(error)) return true;
    if (options?.allowDuplicateScanner && this.isDuplicateScannerSubscriptionError(error)) {
      return true;
    }
    return false;
  }

  private async withTransientRetry<T>(
    operation: string,
    run: () => Promise<T>,
    options?: {
      maxAttempts?: number;
      baseDelayMs?: number;
      maxDelayMs?: number;
      allowDuplicateScanner?: boolean;
    }
  ): Promise<T> {
    const maxAttempts = Math.max(1, Math.round(options?.maxAttempts ?? this.retryMaxAttempts));
    const baseDelayMs = Math.max(10, Math.round(options?.baseDelayMs ?? this.retryBaseDelayMs));
    const maxDelayMs = Math.max(baseDelayMs, Math.round(options?.maxDelayMs ?? this.retryMaxDelayMs));
    let attempt = 1;
    let delayMs = baseDelayMs;

    while (true) {
      try {
        return await run();
      } catch (error) {
        const message = (error as Error).message;
        const shouldRetry =
          attempt < maxAttempts &&
          this.isRetryableTransientError(error, {
            allowDuplicateScanner: options?.allowDuplicateScanner
          }) &&
          !this.isQueueBacklogged();

        if (!shouldRetry) throw error;

        this.warnThrottled(
          `retry_${operation}`,
          `IBKR ${operation} retry ${attempt + 1}/${maxAttempts} after transient failure: ${message}`,
          8_000
        );
        await this.sleep(delayMs);
        delayMs = Math.min(maxDelayMs, delayMs * 2);
        attempt += 1;
      }
    }
  }

  private enterRequestCooldown(reason: string, cooldownMs = this.requestCooldownMs): void {
    const next = Date.now() + Math.max(1_000, cooldownMs);
    if (next > this.requestCooldownUntilMs) {
      this.requestCooldownUntilMs = next;
      this.requestCooldownReason = reason;
    }
    this.scheduleConnectivityRecovery(reason);
  }

  private onRequestSuccess(): void {
    this.requestCooldownUntilMs = 0;
    this.requestCooldownReason = "";
  }

  private isRequestCoolingDown(operation: string): boolean {
    const now = Date.now();
    if (now >= this.requestCooldownUntilMs) return false;

    const remainingMs = this.requestCooldownUntilMs - now;
    this.warnThrottled(
      `cooldown_${operation}`,
      `IBKR ${operation} skipped for ${Math.ceil(remainingMs / 1000)}s due to recent connectivity failures (${this.requestCooldownReason}).`,
      10_000
    );
    this.scheduleConnectivityRecovery(`cooldown:${operation}`);
    return true;
  }

  private recordConnectivityStatus(status: IbkrConnectionStatus): void {
    if (status.reachable) {
      if (!this.lastConnectivityReachable || this.lastConnectivityPort !== status.port) {
        const modeSuffix =
          status.detectedMode === "unknown" ? "" : ` (${status.detectedMode.toUpperCase()})`;
        logger.info(`IBKR connectivity established on ${status.host}:${status.port}${modeSuffix}`);
      }
      this.lastConnectivityReachable = true;
      this.lastConnectivityPort = status.port;
      return;
    }

    if (this.lastConnectivityReachable) {
      this.warnThrottled(
        "ibkr_connectivity_lost",
        `IBKR connectivity lost. ${status.message}`,
        15_000
      );
    }
    this.lastConnectivityReachable = false;
    this.lastConnectivityPort = null;
  }

  private scheduleConnectivityRecovery(reason: string): void {
    if (!this.isEnabled()) return;
    if (this.connectivityRecoveryInFlight) return;

    const now = Date.now();
    if (now - this.lastConnectivityRecoveryAt < this.connectivityRecoveryThrottleMs) return;

    this.lastConnectivityRecoveryAt = now;
    this.connectivityRecoveryInFlight = (async () => {
      const previousPort = this.activePort;
      const status = await this.checkConnectivity(3_500);
      if (status.reachable && status.port !== previousPort) {
        const modeSuffix =
          status.detectedMode === "unknown" ? "" : ` (${status.detectedMode.toUpperCase()})`;
        logger.info(
          `IBKR active port switched from ${previousPort} to ${status.port} after: ${reason}${modeSuffix}`
        );
      }
    })()
      .catch((error) => {
        this.warnThrottled(
          "ibkr_connectivity_recovery_failed",
          `IBKR connectivity recovery failed: ${(error as Error).message}`
        );
      })
      .finally(() => {
        this.connectivityRecoveryInFlight = null;
      });
  }

  private logIbkrExternalRequest(params: {
    startedMs: number;
    startedAt: string;
    operation: string;
    reason: string;
    requestPayload?: unknown;
    status: "success" | "error";
    statusCode?: number;
    responsePayload?: unknown;
    errorMessage?: string;
    method?: string;
    provider?: string;
    endpoint?: string;
  }): void {
    apiRequestLogStore.log({
      startedAt: params.startedAt,
      finishedAt: new Date().toISOString(),
      durationMs: Date.now() - params.startedMs,
      direction: "external",
      provider: params.provider ?? "ibkr",
      method: params.method ?? "RPC",
      endpoint: params.endpoint ?? params.operation,
      reason: params.reason,
      status: params.status,
      statusCode: params.statusCode,
      requestPayload: params.requestPayload,
      responsePayload: params.responsePayload,
      errorMessage: params.errorMessage
    });
  }

  private normalizeOptionExpiration(expiration: string): string {
    const normalized = expiration.replace(/\D/g, "");
    if (normalized.length !== 8) {
      throw new Error(`Invalid option expiration '${expiration}'. Expected YYYY-MM-DD or YYYYMMDD.`);
    }
    return normalized;
  }

  private normalizeOptionRight(right: string): "C" | "P" {
    const normalized = right.trim().toUpperCase();
    if (normalized === "CALL" || normalized === "C") return "C";
    if (normalized === "PUT" || normalized === "P") return "P";
    throw new Error(`Invalid option right '${right}'. Expected CALL/PUT.`);
  }

  private normalizeOrderStatus(status: string): string {
    const normalized = status.trim();
    return normalized.length > 0 ? normalized : "Unknown";
  }

  private ensureOrderStatusStream(client: Client): void {
    if (this.orderStatusStreamAttachedClients.has(client)) return;

    const emitter = (
      client as unknown as {
        _emitter?: {
          on?: (event: string, listener: (value: IbOrderStatusEvent) => void) => void;
        };
      }
    )._emitter;

    if (!emitter?.on) return;

    emitter.on("orderStatus", (event: IbOrderStatusEvent) => {
      this.processOrderStatusEvent(event);
    });
    this.orderStatusStreamAttachedClients.add(client);
  }

  private ensureRawFieldsetStream(client: Client): void {
    if (this.rawFieldsetStreamAttachedClients.has(client)) return;

    const protocolBytes = (
      client as unknown as {
        _protocolBytes?: {
          on?: (event: string, listener: (fields: unknown) => void) => void;
        };
      }
    )._protocolBytes;
    if (!protocolBytes?.on) return;

    protocolBytes.on("message_fieldset", (fieldset: unknown) => {
      this.processRawFieldset(fieldset);
    });
    this.rawFieldsetStreamAttachedClients.add(client);
  }

  private processRawFieldset(fieldset: unknown): void {
    if (!Array.isArray(fieldset) || fieldset.length === 0) return;
    const fields = fieldset.map((value) => String(value ?? ""));
    const messageType = asFiniteNumber(fields[0]);
    if (messageType === null) return;

    switch (messageType) {
      case INCOME_ACCT_VALUE:
        this.processAccountValueFieldset(fields);
        break;
      case INCOME_PORTFOLIO_VALUE:
        this.processPortfolioValueFieldset(fields);
        break;
      case INCOME_ACCT_UPDATE_TIME:
        this.processAccountUpdateTimeFieldset(fields);
        break;
      case INCOME_ACCT_DOWNLOAD_END:
        this.processAccountDownloadEndFieldset(fields);
        break;
      default:
        break;
    }
  }

  private accountValueMapKey(accountCode: string, key: string, currency: string): string {
    return `${accountCode}:${key}:${currency}`;
  }

  private processAccountValueFieldset(fields: string[]): void {
    if (fields.length < 5) return;

    const key = String(fields.at(-4) ?? "").trim();
    const value = String(fields.at(-3) ?? "").trim();
    const currency = String(fields.at(-2) ?? "BASE").trim() || "BASE";
    const accountCode = String(fields.at(-1) ?? "").trim();
    if (!key) return;

    const updatedAt = new Date().toISOString();
    this.accountValues.set(this.accountValueMapKey(accountCode, key, currency), {
      value,
      currency,
      accountCode,
      updatedAt
    });
    this.accountUpdateTimestamp = updatedAt;
  }

  private processPortfolioValueFieldset(fields: string[]): void {
    if (fields.length < 13) return;

    const conId = asFiniteNumber(fields[2]);
    if (conId === null || conId <= 0) return;

    const symbol = String(fields[3] ?? "").trim().toUpperCase();
    const secType = String(fields[4] ?? "").trim().toUpperCase();
    const marketPrice = asFiniteNumber(fields.at(-6)) ?? 0;
    const marketValue = asFiniteNumber(fields.at(-5)) ?? 0;
    const averageCost = asFiniteNumber(fields.at(-4)) ?? 0;
    const unrealizedPnl = asFiniteNumber(fields.at(-3)) ?? 0;
    const realizedPnl = asFiniteNumber(fields.at(-2)) ?? 0;
    const position = asFiniteNumber(fields.at(-7)) ?? 0;
    const updatedAt = new Date().toISOString();

    this.portfolioValues.set(conId, {
      conId,
      symbol,
      secType,
      marketPrice,
      marketValue,
      averageCost,
      unrealizedPnl,
      realizedPnl,
      position,
      updatedAt
    });
    this.accountUpdateTimestamp = updatedAt;
  }

  private processAccountUpdateTimeFieldset(fields: string[]): void {
    const updateTime = String(fields.at(-1) ?? "").trim();
    if (!updateTime) return;
    this.accountUpdateTimestamp = new Date().toISOString();
  }

  private processAccountDownloadEndFieldset(fields: string[]): void {
    this.accountUpdateDownloadEndAt = new Date().toISOString();
    const accountCode = String(fields.at(-1) ?? "").trim();
    if (accountCode.length === 0) return;
    this.accountUpdateTimestamp = new Date().toISOString();
  }

  private processOrderStatusEvent(event: IbOrderStatusEvent): void {
    const brokerOrderId = asFiniteNumber(event.orderId);
    if (brokerOrderId === null) return;

    const existing = this.brokerOrderStatuses.get(brokerOrderId);
    const localOrderId =
      this.brokerToLocalOrder.get(brokerOrderId) ?? existing?.localOrderId;

    const snapshot: IbkrOrderStatusSnapshot = {
      localOrderId,
      brokerOrderId,
      status: this.normalizeOrderStatus(String(event.status ?? existing?.status ?? "Unknown")),
      filled: asFiniteNumber(event.filled) ?? existing?.filled ?? 0,
      remaining: asFiniteNumber(event.remaining) ?? existing?.remaining ?? 0,
      avgFillPrice: asFiniteNumber(event.avgFillPrice) ?? existing?.avgFillPrice ?? 0,
      lastFillPrice: asFiniteNumber(event.lastFillPrice) ?? existing?.lastFillPrice ?? 0,
      permId: asFiniteNumber(event.permId) ?? existing?.permId ?? 0,
      clientId: asFiniteNumber(event.clientId) ?? existing?.clientId ?? settings.ibkrClientId,
      whyHeld: String(event.whyHeld ?? existing?.whyHeld ?? ""),
      source: "event",
      updatedAt: new Date().toISOString()
    };

    this.brokerOrderStatuses.set(brokerOrderId, snapshot);
  }

  private updateOrderMapping(localOrderId: string, brokerOrderId: number): void {
    this.localToBrokerOrder.set(localOrderId, brokerOrderId);
    this.brokerToLocalOrder.set(brokerOrderId, localOrderId);
  }

  private toOpenOrderSnapshot(row: IbOpenOrderRow): IbkrOrderStatusSnapshot | null {
    const order = row.order ?? {};
    const orderState = row.orderState ?? {};

    const brokerOrderId = asFiniteNumber(order.orderId);
    if (brokerOrderId === null) return null;

    const localOrderIdRaw = order.orderRef;
    const localOrderId =
      typeof localOrderIdRaw === "string" && localOrderIdRaw.trim().length > 0
        ? localOrderIdRaw.trim()
        : this.brokerToLocalOrder.get(brokerOrderId);
    if (localOrderId) this.updateOrderMapping(localOrderId, brokerOrderId);

    const totalQuantity = asFiniteNumber(order.totalQuantity) ?? 0;
    const remaining = asFiniteNumber(orderState.remaining) ?? totalQuantity;
    const filled = asFiniteNumber(orderState.filled) ?? Math.max(totalQuantity - remaining, 0);

    return {
      localOrderId,
      brokerOrderId,
      status: this.normalizeOrderStatus(String(orderState.status ?? "Submitted")),
      filled,
      remaining,
      avgFillPrice: asFiniteNumber(orderState.avgFillPrice) ?? 0,
      lastFillPrice: asFiniteNumber(orderState.lastFillPrice) ?? 0,
      permId: asFiniteNumber(order.permId) ?? 0,
      clientId: asFiniteNumber(order.clientId) ?? settings.ibkrClientId,
      whyHeld: String(orderState.whyHeld ?? ""),
      source: "open_order",
      updatedAt: new Date().toISOString()
    };
  }

  private async refreshOpenOrderSnapshots(): Promise<void> {
    if (!this.isEnabled()) return;
    const now = Date.now();
    if (now - this.lastOpenOrdersRefreshAtMs < this.openOrdersRefreshMinIntervalMs) return;
    if (
      this.isQueueBacklogged() &&
      now - this.lastOpenOrdersRefreshAtMs < this.openOrdersRefreshMinIntervalMs * 3
    ) {
      this.warnThrottled(
        "open_orders_refresh_deferred",
        `IBKR open orders refresh deferred due to queue backlog (depth=${this.queueTasks.length + this.queueActiveWorkers}).`,
        20_000
      );
      return;
    }
    if (now < this.nextOpenOrdersAttemptAt) return;
    if (this.isRequestCoolingDown("open orders refresh")) {
      this.nextOpenOrdersAttemptAt = now + this.openOrdersRetryMs;
      return;
    }
    const startedMs = Date.now();
    const startedAt = new Date(startedMs).toISOString();

    try {
      const rows = (await this.withTransientRetry(
        "open orders refresh",
        async () =>
          await this.enqueueRequest({
            channel: "order",
            operation: "getAllOpenOrders",
            run: async () =>
              (await this.getClient(this.activePort).getAllOpenOrders()) as IbOpenOrderRow[]
          }),
        {
          maxAttempts: 2
        }
      )) as IbOpenOrderRow[];
      for (const row of rows) {
        const snapshot = this.toOpenOrderSnapshot(row);
        if (!snapshot) continue;
        this.brokerOrderStatuses.set(snapshot.brokerOrderId, snapshot);
      }
      this.nextOpenOrdersAttemptAt = 0;
      this.lastOpenOrdersRefreshAtMs = Date.now();
      this.onRequestSuccess();
      this.logIbkrExternalRequest({
        startedMs,
        startedAt,
        operation: "getAllOpenOrders",
        reason: "Refresh broker order status snapshots",
        requestPayload: {
          port: this.activePort
        },
        responsePayload: {
          rowCount: rows.length
        },
        status: "success"
      });
    } catch (error) {
      const message = (error as Error).message;
      this.logIbkrExternalRequest({
        startedMs,
        startedAt,
        operation: "getAllOpenOrders",
        reason: "Refresh broker order status snapshots",
        requestPayload: {
          port: this.activePort
        },
        status: "error",
        errorMessage: message
      });
      if (this.isConnectivityError(error)) {
        this.enterRequestCooldown(message);
        this.nextOpenOrdersAttemptAt = now + this.openOrdersRetryMs;
      } else {
        this.nextOpenOrdersAttemptAt = now + 5_000;
      }
      this.warnThrottled("open_orders_refresh_failed", `IBKR open orders refresh failed: ${message}`);
    }
  }

  async refreshOrderStatuses(localOrderIds: string[]): Promise<Record<string, IbkrOrderStatusSnapshot>> {
    if (!this.isEnabled() || localOrderIds.length === 0) return {};

    await this.refreshOpenOrderSnapshots();

    const result: Record<string, IbkrOrderStatusSnapshot> = {};
    for (const localOrderId of localOrderIds) {
      const brokerOrderId = this.localToBrokerOrder.get(localOrderId);
      if (brokerOrderId === undefined) continue;

      const snapshot = this.brokerOrderStatuses.get(brokerOrderId);
      if (!snapshot) continue;
      result[localOrderId] = {
        ...snapshot,
        localOrderId
      };
    }

    return result;
  }

  private isSubscriptionError(error: unknown): boolean {
    const message = String((error as Error)?.message ?? "").toLowerCase();
    return (
      message.includes("requires additional subscription") ||
      message.includes("market data permissions") ||
      message.includes("not subscribed")
    );
  }

  private isDelayedDataNotEnabledError(error: unknown): boolean {
    const message = String((error as Error)?.message ?? "").toLowerCase();
    return message.includes("delayed market data is not enabled");
  }

  private isNoSecurityDefinitionError(error: unknown): boolean {
    const message = String((error as Error)?.message ?? "").toLowerCase();
    return message.includes("no security definition has been found for the request");
  }

  private isDuplicateScannerSubscriptionError(error: unknown): boolean {
    const message = String((error as Error)?.message ?? "").toLowerCase();
    return (
      message.includes("duplicate scan subscription") ||
      (message.includes("scanner") && message.includes("duplicate"))
    );
  }

  private marketMidFromTicker(ticker: IbTickerSnapshot): number | null {
    const last = pickNumber(
      ticker.last,
      ticker.delayedLast,
      ticker.close,
      ticker.delayedClose,
      ticker.markPrice
    );
    if (last === null || last <= 0) return null;

    const bid = pickNumber(ticker.bid, ticker.delayedBid, last) ?? last;
    const ask = pickNumber(ticker.ask, ticker.delayedAsk, last) ?? last;
    const mid = (bid + ask) / 2;
    if (!Number.isFinite(mid) || mid <= 0) return last;
    return mid;
  }

  private toOptionQuote(
    contract: {
      symbol: string;
      expiration: string;
      strike: number;
      right: "CALL" | "PUT";
    },
    ticker: IbTickerSnapshot
  ): IbkrOptionQuote | null {
    const last = pickNumber(
      ticker.optionLast,
      ticker.last,
      ticker.delayedLast,
      ticker.close,
      ticker.delayedClose,
      ticker.markPrice
    );
    if (last === null || last <= 0) return null;

    const bid = pickNumber(ticker.optionBid, ticker.bid, ticker.delayedBid, last) ?? last;
    const ask = pickNumber(ticker.optionAsk, ticker.ask, ticker.delayedAsk, last) ?? last;
    const midRaw = (bid + ask) / 2;
    const mid = Number.isFinite(midRaw) && midRaw > 0 ? midRaw : last;
    const impliedVol = pickNumber(
      ticker.optionImpliedVol,
      ticker.impliedVol,
      ticker.optionHistoricalVol
    );
    const optionVolume = pickNumber(
      contract.right === "CALL" ? ticker.optionCallVolume : ticker.optionPutVolume,
      ticker.volume,
      ticker.delayedVolume,
      ticker.rtVolume,
      ticker.avgOptionVolume
    );
    const optionOpenInterest = pickNumber(
      contract.right === "CALL" ? ticker.optionCallOpenInterest : ticker.optionPutOpenInterest,
      ticker.optionOpenInterest,
      ticker.openInterest,
      ticker.futuresOpenInterest
    );

    return {
      symbol: contract.symbol.toUpperCase(),
      expiration: contract.expiration,
      strike: contract.strike,
      right: contract.right,
      bid: Number(Math.max(0.01, bid).toFixed(4)),
      ask: Number(Math.max(0.01, Math.max(ask, bid)).toFixed(4)),
      last: Number(last.toFixed(4)),
      mid: Number(mid.toFixed(4)),
      volume: Math.max(0, Math.round(optionVolume ?? 0)),
      openInterest: Math.max(0, Math.round(optionOpenInterest ?? 0)),
      impliedVol: impliedVol !== null && Number.isFinite(impliedVol) ? impliedVol : null,
      delta: null,
      gamma: null
    };
  }

  private optionContractIdentity(contract: {
    symbol: string;
    expiration: string;
    strike: number;
    right: "CALL" | "PUT";
  }): string {
    return `${contract.symbol.toUpperCase()}|${this.normalizeOptionExpiration(contract.expiration)}|${this.normalizeOptionRight(contract.right)}|${Number(contract.strike).toFixed(4)}`;
  }

  private isInvalidOptionContractCoolingDown(contractKey: string): boolean {
    if (this.invalidOptionContracts.size > 400) {
      const now = Date.now();
      for (const [key, value] of this.invalidOptionContracts.entries()) {
        if (value.untilMs <= now) this.invalidOptionContracts.delete(key);
      }
    }

    const entry = this.invalidOptionContracts.get(contractKey);
    if (!entry) return false;
    const remainingMs = entry.untilMs - Date.now();
    if (remainingMs <= 0) {
      this.invalidOptionContracts.delete(contractKey);
      return false;
    }

    this.warnThrottled(
      `option_contract_invalid_skip_${contractKey}`,
      `IBKR option quote skipped for contract marked invalid (${contractKey}) for another ${Math.ceil(remainingMs / 60_000)}m.`,
      2 * 60_000
    );
    return true;
  }

  getOptionQuoteReadiness(nowMs = Date.now()): { allowed: boolean; reason: string } {
    if (!this.isEnabled()) return { allowed: false, reason: "ibkr_disabled" };
    if (this.requestCooldownUntilMs > nowMs) {
      return { allowed: false, reason: "request_cooldown_active" };
    }
    if (this.marketDataDelayedOnly) {
      return { allowed: false, reason: "delayed_only_mode" };
    }
    if (nowMs < this.optionQuoteEntitlementUntilMs) {
      return { allowed: false, reason: "option_quote_entitlement_backoff" };
    }
    if (this.queueTasks.length >= 140) {
      return { allowed: false, reason: "queue_backlog" };
    }
    return { allowed: true, reason: "ready" };
  }

  private parsePositionRow(row: IbPositionRowMap[string]): IbkrPositionSnapshot | null {
    const contract = row.contract ?? {};
    const conId = asFiniteNumber(contract.conId);
    const symbol = String(contract.symbol ?? "").trim().toUpperCase();
    const secType = String(contract.secType ?? "").trim().toUpperCase();
    const position = asFiniteNumber(row.position) ?? 0;

    if (conId === null || conId <= 0 || symbol.length === 0) return null;

    const parsedMultiplier = asFiniteNumber(contract.multiplier);
    const multiplier =
      parsedMultiplier ?? (secType === "OPT" || secType === "FOP" ? 100 : 1);
    const right = normalizeOptionRightForModel(contract.right);
    const expiration = normalizeExpirationForModel(contract.lastTradeDateOrContractMonth);
    const avgCost = asFiniteNumber(row.avgCost) ?? 0;
    const portfolio = this.portfolioValues.get(conId);

    return {
      conId,
      symbol,
      secType,
      expiration,
      strike: asFiniteNumber(contract.strike) ?? undefined,
      right,
      multiplier,
      position,
      avgCost,
      marketPrice: portfolio?.marketPrice,
      marketValue: portfolio?.marketValue,
      unrealizedPnl: portfolio?.unrealizedPnl,
      realizedPnl: portfolio?.realizedPnl,
      marketDataUpdatedAt: portfolio?.updatedAt
    };
  }

  private contractForPosition(position: IbkrPositionSnapshot): Record<string, unknown> | null {
    if (position.secType === "STK") {
      return Contract.stock({
        symbol: position.symbol,
        exchange: "SMART",
        currency: "USD"
      });
    }

    if (position.secType === "OPT" && position.expiration && position.strike && position.right) {
      return Contract.option({
        symbol: position.symbol,
        lastTradeDateOrContractMonth: this.normalizeOptionExpiration(position.expiration),
        strike: position.strike,
        right: this.normalizeOptionRight(position.right),
        exchange: "SMART",
        currency: "USD",
        multiplier: position.multiplier
      });
    }

    return null;
  }

  private accountValueNumber(key: string, currencyPreference = "USD"): number | undefined {
    const preferred = [...this.accountValues.entries()]
      .filter(([, value]) => value.currency === currencyPreference)
      .find(([mapKey]) => mapKey.includes(`:${key}:`));
    if (preferred) {
      const parsed = asFiniteNumber(preferred[1].value);
      if (parsed !== null) return parsed;
    }

    const fallback = [...this.accountValues.entries()].find(([mapKey]) =>
      mapKey.includes(`:${key}:`)
    );
    if (!fallback) return undefined;

    const parsed = asFiniteNumber(fallback[1].value);
    return parsed ?? undefined;
  }

  private inferAccountCode(): string | undefined {
    const withAccount = [...this.accountValues.values()].find(
      (entry) => entry.accountCode.trim().length > 0
    );
    return withAccount?.accountCode;
  }

  private async ensureAccountUpdatesSubscription(): Promise<void> {
    if (!this.isEnabled()) return;
    if (this.accountUpdatesSubscribedPort === this.activePort) return;

    const now = Date.now();
    if (now < this.nextAccountSubscriptionAttemptAt) return;
    if (this.isRequestCoolingDown("account updates subscription")) {
      this.nextAccountSubscriptionAttemptAt = now + this.accountSubscriptionRetryMs;
      return;
    }

    const client = this.getClient(this.activePort);
    this.ensureRawFieldsetStream(client);
    const startedMs = Date.now();
    const startedAt = new Date(startedMs).toISOString();
    try {
      const accountCode = this.inferAccountCode() ?? "";
      await this.enqueueRequest({
        channel: "account",
        operation: "reqAccountUpdates",
        run: async () => {
          await client.reqAccountUpdates({
            subscribe: true,
            accountCode
          });
        }
      });
      this.accountUpdatesSubscribedPort = this.activePort;
      this.nextAccountSubscriptionAttemptAt = 0;
      this.onRequestSuccess();
      this.logIbkrExternalRequest({
        startedMs,
        startedAt,
        operation: "reqAccountUpdates",
        reason: "Subscribe to IBKR account updates stream",
        requestPayload: {
          subscribe: true,
          accountCode,
          port: this.activePort
        },
        status: "success"
      });
    } catch (error) {
      const message = (error as Error).message;
      const accountCode = this.inferAccountCode() ?? "";
      this.accountUpdatesSubscribedPort = null;
      this.logIbkrExternalRequest({
        startedMs,
        startedAt,
        operation: "reqAccountUpdates",
        reason: "Subscribe to IBKR account updates stream",
        requestPayload: {
          subscribe: true,
          accountCode,
          port: this.activePort
        },
        status: "error",
        errorMessage: message
      });
      if (this.isConnectivityError(error)) {
        this.enterRequestCooldown(message);
        this.nextAccountSubscriptionAttemptAt = now + this.accountSubscriptionRetryMs;
      } else {
        this.nextAccountSubscriptionAttemptAt = now + 7_500;
      }
      this.warnThrottled("account_updates_subscribe_failed", `IBKR account updates subscription failed: ${message}`);
    }
  }

  private launchTargetConfig(
    target: "gateway" | "tws"
  ): { appName: string; execPath: string } {
    if (target === "tws") {
      return {
        appName: settings.ibkrTwsAppName,
        execPath: settings.ibkrTwsExecPath
      };
    }
    return {
      appName: settings.ibkrGatewayAppName,
      execPath: settings.ibkrGatewayExecPath
    };
  }

  private spawnDetached(command: string, args: string[], cwd?: string): Promise<void> {
    return new Promise((resolve, reject) => {
      const child = spawn(command, args, {
        detached: true,
        stdio: "ignore",
        cwd
      });

      child.once("error", (error) => reject(error));
      child.once("spawn", () => {
        child.unref();
        resolve();
      });
    });
  }

  private runCommand(
    command: string,
    args: string[],
    timeoutMs = 8_000
  ): Promise<{ ok: boolean; code: number | null; stderr: string }> {
    return new Promise((resolve) => {
      const child = spawn(command, args, {
        stdio: ["ignore", "pipe", "pipe"]
      });

      let resolved = false;
      let stderr = "";
      let timer: Timer;
      const finish = (ok: boolean, code: number | null): void => {
        if (resolved) return;
        resolved = true;
        clearTimeout(timer);
        resolve({ ok, code, stderr: stderr.trim() });
      };

      child.stderr?.on("data", (chunk) => {
        stderr += String(chunk);
      });

      child.once("error", (error) => {
        stderr = `${stderr}\n${(error as Error).message}`;
        finish(false, 1);
      });

      child.once("close", (code) => {
        finish(code === 0, code);
      });

      timer = setTimeout(() => {
        child.kill();
        finish(false, null);
      }, timeoutMs);
    });
  }

  private discoverMacAppPaths(pattern: RegExp): string[] {
    const directories = ["/Applications", join(os.homedir(), "Applications")];
    const discovered: string[] = [];

    for (const directory of directories) {
      if (!existsSync(directory)) continue;

      const entries = readdirSync(directory, { withFileTypes: true });
      for (const entry of entries) {
        const fullPath = join(directory, entry.name);
        const isUninstaller = /uninstaller/i.test(entry.name);

        if (entry.isDirectory() && entry.name.toLowerCase().endsWith(".app")) {
          if (pattern.test(entry.name) && !isUninstaller) discovered.push(fullPath);
          continue;
        }

        if (!entry.isDirectory()) continue;
        if (!pattern.test(entry.name)) continue;

        try {
          const nested = readdirSync(fullPath, { withFileTypes: true });
          for (const child of nested) {
            if (!child.isDirectory()) continue;
            if (!child.name.toLowerCase().endsWith(".app")) continue;
            if (/uninstaller/i.test(child.name)) continue;
            if (!pattern.test(child.name)) continue;
            const childPath = join(fullPath, child.name);
            discovered.push(childPath);
          }
        } catch {
          // ignore unreadable directories
        }
      }
    }

    return uniq(discovered);
  }

  private macLaunchCandidates(target: "gateway" | "tws"): { names: string[]; paths: string[] } {
    const configured =
      target === "gateway" ? settings.ibkrGatewayAppName : settings.ibkrTwsAppName;
    const configuredNormalized = configured.replace(/\.app$/i, "").trim();

    const defaults =
      target === "gateway"
        ? ["IB Gateway", "IB Gateway Stable", "IB Gateway Latest", "IBKR Gateway"]
        : ["Trader Workstation", "Trader Workstation Stable", "Trader Workstation Latest", "TWS"];

    const discoveredPaths =
      target === "gateway"
        ? this.discoverMacAppPaths(/ib.*gateway/i)
        : this.discoverMacAppPaths(/trader workstation|tws/i);

    const discoveredNames = discoveredPaths.map((path) =>
      basename(path).replace(/\.app$/i, "")
    );

    return {
      names: uniq(
        [configuredNormalized, ...defaults, ...discoveredNames].filter((item) => item.length > 0)
      ),
      paths: uniq(discoveredPaths)
    };
  }

  private discoverExecutablesInTree(rootDirectory: string, fileName: string, maxDepth = 2): string[] {
    if (!existsSync(rootDirectory)) return [];
    const normalizedFile = fileName.trim().toLowerCase();
    if (!normalizedFile) return [];

    const discovered: string[] = [];
    const walk = (directory: string, depth: number): void => {
      if (depth > maxDepth) return;

      let entries: Dirent[];
      try {
        entries = readdirSync(directory, { withFileTypes: true });
      } catch {
        return;
      }

      for (const entry of entries) {
        const fullPath = join(directory, entry.name);
        if (entry.isFile() && entry.name.toLowerCase() === normalizedFile) {
          discovered.push(fullPath);
          continue;
        }
        if (!entry.isDirectory()) continue;
        if (entry.name.startsWith(".")) continue;
        if (/uninstall|uninstaller/i.test(entry.name)) continue;
        walk(fullPath, depth + 1);
      }
    };

    walk(rootDirectory, 0);
    return uniq(discovered);
  }

  private windowsLaunchCandidates(target: "gateway" | "tws"): { names: string[]; paths: string[] } {
    const configured =
      target === "gateway" ? settings.ibkrGatewayAppName : settings.ibkrTwsAppName;
    const configuredNormalized = configured.replace(/\.exe$/i, "").trim();
    const executableName = target === "gateway" ? "ibgateway.exe" : "tws.exe";
    const defaultNames =
      target === "gateway"
        ? ["IB Gateway", "IBKR Gateway", "IB Gateway Stable", "IB Gateway Latest"]
        : ["Trader Workstation", "TWS", "Trader Workstation Stable", "Trader Workstation Latest"];

    const programFiles = process.env.ProgramFiles ?? "C:\\Program Files";
    const programFilesX86 = process.env["ProgramFiles(x86)"] ?? "C:\\Program Files (x86)";
    const localAppData = process.env.LOCALAPPDATA ?? join(os.homedir(), "AppData", "Local");
    const defaultRoots = uniq(
      [
        "C:\\Jts",
        target === "gateway" ? "C:\\Jts\\ibgateway" : "C:\\Jts\\tws",
        join(programFiles, "IB Gateway"),
        join(programFilesX86, "IB Gateway"),
        join(localAppData, "Programs", "IB Gateway"),
        join(programFiles, "Trader Workstation"),
        join(programFilesX86, "Trader Workstation"),
        join(localAppData, "Programs", "Trader Workstation")
      ].filter((value) => value.trim().length > 0)
    );

    const directPathHints = uniq(
      defaultRoots.flatMap((root) => [
        join(root, executableName),
        join(root, "bin", executableName),
        join(root, "jars", executableName)
      ])
    );
    const directHits = directPathHints.filter((candidate) => existsSync(candidate));
    const discoveredViaScan = defaultRoots.flatMap((root) =>
      this.discoverExecutablesInTree(root, executableName, root.toLowerCase().includes("\\jts") ? 4 : 2)
    );

    return {
      names: uniq([configuredNormalized, ...defaultNames].filter((value) => value.length > 0)),
      paths: uniq([...directHits, ...discoveredViaScan])
    };
  }

  private async launchMacByName(
    appName: string
  ): Promise<{ ok: boolean; detail: string }> {
    const result = await this.runCommand("open", ["-a", appName]);
    if (result.ok) return { ok: true, detail: "ok" };
    return {
      ok: false,
      detail: result.stderr || (result.code === null ? "timeout" : `exit_code:${result.code}`)
    };
  }

  private async launchMacByPath(execPath: string): Promise<{ ok: boolean; detail: string }> {
    const result = await this.runCommand("open", [execPath]);
    if (result.ok) return { ok: true, detail: "ok" };
    return {
      ok: false,
      detail: result.stderr || (result.code === null ? "timeout" : `exit_code:${result.code}`)
    };
  }

  private async launchWindowsByPath(
    execPath: string
  ): Promise<{ ok: boolean; detail: string }> {
    if (!existsSync(execPath)) {
      return { ok: false, detail: "not_found" };
    }

    const escapedPath = execPath.replace(/'/g, "''");
    const result = await this.runCommand("powershell.exe", [
      "-NoProfile",
      "-NonInteractive",
      "-ExecutionPolicy",
      "Bypass",
      "-Command",
      `Start-Process -FilePath '${escapedPath}'`
    ]);
    if (result.ok) return { ok: true, detail: "ok" };

    try {
      await this.spawnDetached(execPath, []);
      return { ok: true, detail: "ok_detached" };
    } catch (error) {
      const fallbackDetail = (error as Error).message;
      return {
        ok: false,
        detail: result.stderr || fallbackDetail || (result.code === null ? "timeout" : `exit_code:${result.code}`)
      };
    }
  }

  private async launchWindowsByName(
    appName: string
  ): Promise<{ ok: boolean; detail: string }> {
    const escapedName = appName.replace(/'/g, "''");
    const result = await this.runCommand("powershell.exe", [
      "-NoProfile",
      "-NonInteractive",
      "-ExecutionPolicy",
      "Bypass",
      "-Command",
      `Start-Process -FilePath '${escapedName}'`
    ]);
    if (result.ok) return { ok: true, detail: "ok" };
    return {
      ok: false,
      detail: result.stderr || (result.code === null ? "timeout" : `exit_code:${result.code}`)
    };
  }

  async launch(target: "gateway" | "tws" = settings.ibkrLaunchTarget): Promise<IbkrLaunchResult> {
    const dryRun = settings.ibkrLaunchDryRun || settings.appEnv === "test" || Boolean(process.env.BUN_TEST);
    const platform = process.platform;

    const config = this.launchTargetConfig(target);
    let commandPreview = "";
    if (platform === "darwin") {
      commandPreview = config.execPath
        ? `${config.execPath}`
        : `open -a "${config.appName}"`;
    } else if (platform === "win32") {
      commandPreview = config.execPath
        ? `powershell Start-Process -FilePath "${config.execPath}"`
        : `Auto-discover ${target === "gateway" ? "ibgateway.exe" : "tws.exe"} and launch via Start-Process`;
    } else if (config.execPath) {
      commandPreview = config.execPath;
    } else {
      commandPreview = `Launch path not configured for ${platform}`;
    }

    if (dryRun) {
      return {
        target,
        launched: true,
        dryRun: true,
        platform,
        message:
          "Dry run mode enabled. No app was launched. Disable IBKR_LAUNCH_DRY_RUN to start IBKR app.",
        commandPreview
      };
    }

    try {
      if (platform === "darwin") {
        if (config.execPath) {
          if (!existsSync(config.execPath)) {
            return {
              target,
              launched: false,
              dryRun: false,
              platform,
              message: `Configured executable path not found: ${config.execPath}`,
              commandPreview
            };
          }

          if (config.execPath.toLowerCase().endsWith(".app")) {
            const byPath = await this.launchMacByPath(config.execPath);
            if (!byPath.ok) {
              return {
                target,
                launched: false,
                dryRun: false,
                platform,
                message: `Launch failed for app bundle path ${config.execPath}: ${byPath.detail}`,
                commandPreview
              };
            }
          } else {
            await this.spawnDetached(config.execPath, []);
          }
        } else {
          const { names, paths } = this.macLaunchCandidates(target);
          const failures: string[] = [];
          const attempts: string[] = [];

          for (const appPath of paths) {
            const result = await this.launchMacByPath(appPath);
            attempts.push(`open "${appPath}"`);
            if (result.ok) {
              const selectedApp = basename(appPath).replace(/\.app$/i, "");
              return {
                target,
                launched: true,
                dryRun: false,
                platform,
                message: `${target.toUpperCase()} launch triggered via path "${appPath}". Complete login manually in the IBKR window (2FA may be required).`,
                commandPreview: `open "${appPath}"`,
                selectedApp,
                attemptedApps: [...paths, ...names]
              };
            }
            failures.push(`${appPath}: ${result.detail}`);
          }

          for (const appName of names) {
            const result = await this.launchMacByName(appName);
            attempts.push(`open -a "${appName}"`);
            if (result.ok) {
              return {
                target,
                launched: true,
                dryRun: false,
                platform,
                message: `${target.toUpperCase()} launch triggered via "${appName}". Complete login manually in the IBKR window (2FA may be required).`,
                commandPreview: `open -a "${appName}"`,
                selectedApp: appName,
                attemptedApps: [...paths, ...names]
              };
            }
            failures.push(`${appName}: ${result.detail}`);
          }

          return {
            target,
            launched: false,
            dryRun: false,
            platform,
            message: `Unable to launch ${target.toUpperCase()} by app path/name. Attempts: ${failures.join(" | ")}`,
            commandPreview: attempts.join(" || "),
            attemptedApps: [...paths, ...names]
          };
        }
      } else if (platform === "win32") {
        const candidates = this.windowsLaunchCandidates(target);
        const attempts: string[] = [];
        const failures: string[] = [];

        if (config.execPath) {
          const byPath = await this.launchWindowsByPath(config.execPath);
          attempts.push(`Start-Process "${config.execPath}"`);
          if (byPath.ok) {
            return {
              target,
              launched: true,
              dryRun: false,
              platform,
              message: `${target.toUpperCase()} launch triggered via path "${config.execPath}". Complete login manually in the IBKR window (2FA may be required).`,
              commandPreview: `Start-Process "${config.execPath}"`,
              selectedApp: basename(config.execPath),
              attemptedApps: [config.execPath, ...candidates.paths, ...candidates.names]
            };
          }
          failures.push(`${config.execPath}: ${byPath.detail}`);
        }

        for (const execPath of candidates.paths) {
          if (config.execPath && execPath === config.execPath) continue;
          const result = await this.launchWindowsByPath(execPath);
          attempts.push(`Start-Process "${execPath}"`);
          if (result.ok) {
            return {
              target,
              launched: true,
              dryRun: false,
              platform,
              message: `${target.toUpperCase()} launch triggered via path "${execPath}". Complete login manually in the IBKR window (2FA may be required).`,
              commandPreview: `Start-Process "${execPath}"`,
              selectedApp: basename(execPath),
              attemptedApps: [config.execPath, ...candidates.paths, ...candidates.names].filter(Boolean)
            };
          }
          failures.push(`${execPath}: ${result.detail}`);
        }

        for (const appName of candidates.names) {
          const result = await this.launchWindowsByName(appName);
          attempts.push(`Start-Process "${appName}"`);
          if (result.ok) {
            return {
              target,
              launched: true,
              dryRun: false,
              platform,
              message: `${target.toUpperCase()} launch triggered via "${appName}". Complete login manually in the IBKR window (2FA may be required).`,
              commandPreview: `Start-Process "${appName}"`,
              selectedApp: appName,
              attemptedApps: [config.execPath, ...candidates.paths, ...candidates.names].filter(Boolean)
            };
          }
          failures.push(`${appName}: ${result.detail}`);
        }

        return {
          target,
          launched: false,
          dryRun: false,
          platform,
          message: `Unable to launch ${target.toUpperCase()} on Windows. Attempts: ${failures.join(" | ")}`,
          commandPreview: attempts.join(" || "),
          attemptedApps: [config.execPath, ...candidates.paths, ...candidates.names].filter(Boolean)
        };
      } else {
        if (!config.execPath) {
          return {
            target,
            launched: false,
            dryRun: false,
            platform,
            message:
              `No executable path configured for ${target} on ${platform}. Set ${
                target === "gateway" ? "IBKR_GATEWAY_EXEC_PATH" : "IBKR_TWS_EXEC_PATH"
              }.`,
            commandPreview
          };
        }
        await this.spawnDetached(config.execPath, []);
      }

      return {
        target,
        launched: true,
        dryRun: false,
        platform,
        message:
          `${target.toUpperCase()} launch triggered. Complete login manually in the IBKR window (2FA may be required).`,
        commandPreview
      };
    } catch (error) {
      return {
        target,
        launched: false,
        dryRun: false,
        platform,
        message: `Launch failed: ${(error as Error).message}`,
        commandPreview
      };
    }
  }

  private connectivityProbePorts(): number[] {
    return uniq([
      this.activePort,
      settings.ibkrPort,
      ...settings.ibkrPortCandidates
    ]);
  }

  private async probePort(
    port: number,
    timeoutMs: number
  ): Promise<{ ok: boolean; latencyMs: number | null; clientId?: number; error?: string }> {
    const tcpReachable = await this.probeTcpReachability(settings.ibkrHost, port, Math.min(800, timeoutMs));
    if (!tcpReachable) {
      return {
        ok: false,
        latencyMs: null,
        error: "Failed to connect"
      };
    }

    const client = this.getClient(port);
    const started = Date.now();
    const startedAt = new Date(started).toISOString();

    try {
      await this.withTransientRetry(
        `connectivity probe (${port})`,
        async () =>
          await this.enqueueRequest({
            channel: "connectivity",
            operation: `getCurrentTime:${port}`,
            run: async () => await client.getCurrentTime()
          }),
        {
          maxAttempts: 2
        }
      );
      this.ensureRawFieldsetStream(client);
      this.logIbkrExternalRequest({
        startedMs: started,
        startedAt,
        operation: "getCurrentTime",
        reason: "Probe IBKR API connectivity on candidate port",
        requestPayload: {
          port,
          timeoutMs
        },
        responsePayload: {
          port
        },
        status: "success"
      });
      const effectiveClientId =
        (
          client as unknown as {
            getEffectiveClientId?: () => number;
          }
        ).getEffectiveClientId?.() ?? settings.ibkrClientId;
      return { ok: true, latencyMs: Date.now() - started, clientId: effectiveClientId };
    } catch (error) {
      this.logIbkrExternalRequest({
        startedMs: started,
        startedAt,
        operation: "getCurrentTime",
        reason: "Probe IBKR API connectivity on candidate port",
        requestPayload: {
          port,
          timeoutMs
        },
        status: "error",
        errorMessage: (error as Error).message
      });
      return {
        ok: false,
        latencyMs: null,
        error: (error as Error).message
      };
    }
  }

  private async probeTcpReachability(host: string, port: number, timeoutMs: number): Promise<boolean> {
    return await new Promise<boolean>((resolve) => {
      const socket = new Socket();
      let done = false;

      const finish = (reachable: boolean): void => {
        if (done) return;
        done = true;
        socket.destroy();
        resolve(reachable);
      };

      socket.setTimeout(Math.max(200, timeoutMs));
      socket.once("connect", () => finish(true));
      socket.once("timeout", () => finish(false));
      socket.once("error", () => finish(false));

      try {
        socket.connect(port, host);
      } catch {
        finish(false);
      }
    });
  }

  async checkConnectivity(timeoutMs = 5_000): Promise<IbkrConnectionStatus> {
    const probedPorts = this.connectivityProbePorts();

    if (!this.isEnabled()) {
      const disabled: IbkrConnectionStatus = {
        enabled: false,
        host: settings.ibkrHost,
        port: settings.ibkrPort,
        clientId: settings.ibkrClientId,
        reachable: false,
        latencyMs: null,
        detectedMode: modeFromPort(settings.ibkrPort),
        probedPorts,
        message:
          isTestRuntime()
            ? "IBKR connectivity checks are disabled in test runtime."
            : "IBKR integration disabled. Set IBKR_ENABLED=true to test gateway connectivity."
      };
      this.recordConnectivityStatus(disabled);
      this.lastConnectivityStatus = this.cloneConnectivityStatus(disabled);
      this.lastConnectivityStatusAtMs = Date.now();
      return this.cloneConnectivityStatus(disabled);
    }

    const now = Date.now();
    if (
      this.lastConnectivityStatus &&
      now - this.lastConnectivityStatusAtMs < this.connectivityCacheTtlMs
    ) {
      return this.cloneConnectivityStatus(this.lastConnectivityStatus);
    }

    if (this.connectivityStatusInFlight) {
      return this.cloneConnectivityStatus(await this.connectivityStatusInFlight);
    }

    const run = async (): Promise<IbkrConnectionStatus> => {
      const errors: string[] = [];
      for (const port of probedPorts) {
        const result = await this.probePort(port, timeoutMs);
        if (result.ok) {
          this.selectActivePort(port);
          this.onRequestSuccess();
          const sessionMode = modeFromPort(port);
          const connectedClientId = result.clientId ?? settings.ibkrClientId;
          const clientIdHint =
            connectedClientId === settings.ibkrClientId
              ? ""
              : ` using fallback clientId ${connectedClientId} (configured ${settings.ibkrClientId}).`;
          const connected: IbkrConnectionStatus = {
            enabled: true,
            host: settings.ibkrHost,
            port,
            clientId: connectedClientId,
            reachable: true,
            latencyMs: result.latencyMs,
            detectedMode: sessionMode,
            probedPorts,
            message:
              `Connected to IBKR API session on port ${port}` +
              (sessionMode === "unknown" ? "." : ` (${sessionMode.toUpperCase()}).`) +
              clientIdHint
          };
          this.pruneClients([port]);
          this.recordConnectivityStatus(connected);
          return connected;
        }
        errors.push(`${port}: ${result.error ?? "unreachable"}`);
      }

      this.enterRequestCooldown(errors.join(" | "), 5_000);
      const timeoutOnly =
        errors.length > 0 &&
        errors.every((error) => error.toLowerCase().includes("timeout"));
      const clientIdHint =
        timeoutOnly && settings.ibkrClientId === 1
          ? " Try setting IBKR_CLIENT_ID to a unique non-1 value (for example 137) and refresh runtime."
          : "";
      if (timeoutOnly) {
        this.dropTransportClients();
      }

      const disconnected: IbkrConnectionStatus = {
        enabled: this.isEnabled(),
        host: settings.ibkrHost,
        port: this.activePort,
        clientId: settings.ibkrClientId,
        reachable: false,
        latencyMs: null,
        detectedMode: modeFromPort(this.activePort),
        probedPorts,
        message: `Unable to connect on probed ports. ${errors.join(" | ")}${clientIdHint}`
      };
      this.recordConnectivityStatus(disconnected);
      return disconnected;
    };

    const request = run().finally(() => {
      if (this.connectivityStatusInFlight === request) {
        this.connectivityStatusInFlight = null;
      }
    });
    this.connectivityStatusInFlight = request;

    const result = await request;
    this.lastConnectivityStatus = this.cloneConnectivityStatus(result);
    this.lastConnectivityStatusAtMs = Date.now();
    return this.cloneConnectivityStatus(result);
  }

  async getQuote(symbol: string): Promise<IbkrQuote | null> {
    if (!this.isEnabled()) return null;
    if (this.isRequestCoolingDown("quote request")) return null;

    const symbolUpper = symbol.toUpperCase();
    const blockedUntilMs = this.quoteSubscriptionBlockedUntilBySymbol.get(symbolUpper) ?? 0;
    if (blockedUntilMs > Date.now()) {
      const remainingMs = blockedUntilMs - Date.now();
      this.warnThrottled(
        `quote_subscription_blocked_${symbolUpper}`,
        `IBKR quote request skipped for ${symbolUpper} due to subscription backoff (${Math.ceil(remainingMs / 60_000)}m remaining).`,
        2 * 60_000
      );
      return null;
    }
    const startedMs = Date.now();
    const startedAt = new Date(startedMs).toISOString();

    try {
      await this.setMarketDataType(this.marketDataDelayedOnly ? 3 : 1);
      let ticker: IbTickerSnapshot;

      try {
        ticker = (await this.enqueueRequest({
          channel: "quote",
          operation: `getMarketDataSnapshot:${symbolUpper}`,
          run: async () =>
            (await this.getClient().getMarketDataSnapshot({
              contract: Contract.stock(symbolUpper),
              regulatorySnapshot: false
            })) as IbTickerSnapshot
        })) as IbTickerSnapshot;
      } catch (error) {
        if (!this.isSubscriptionError(error)) throw error;

        this.marketDataDelayedOnly = true;
        this.warnThrottled(
          "quote_subscription_delayed_only",
          "IBKR real-time market data is not entitled. Falling back to delayed snapshots where available.",
          10 * 60_000
        );
        await this.setMarketDataType(3);
        try {
          ticker = (await this.enqueueRequest({
            channel: "quote",
            operation: `getMarketDataSnapshot:${symbolUpper}:delayed`,
            run: async () =>
              (await this.getClient().getMarketDataSnapshot({
                contract: Contract.stock(symbolUpper),
                regulatorySnapshot: false
              })) as IbTickerSnapshot
          })) as IbTickerSnapshot;
        } catch (delayedError) {
          if (!this.isDelayedDataNotEnabledError(delayedError)) throw delayedError;
          await this.setMarketDataType(4);
          ticker = (await this.enqueueRequest({
            channel: "quote",
            operation: `getMarketDataSnapshot:${symbolUpper}:delayed_frozen`,
            run: async () =>
              (await this.getClient().getMarketDataSnapshot({
                contract: Contract.stock(symbolUpper),
                regulatorySnapshot: false
              })) as IbTickerSnapshot
          })) as IbTickerSnapshot;
        }
      }

      const last = pickNumber(
        ticker.last,
        ticker.delayedLast,
        ticker.close,
        ticker.delayedClose,
        ticker.markPrice
      );
      if (last === null || last <= 0) return null;

      const bid = pickNumber(ticker.bid, ticker.delayedBid, last) ?? last;
      const ask = pickNumber(ticker.ask, ticker.delayedAsk, last) ?? last;
      const volume = pickNumber(ticker.volume, ticker.delayedVolume, ticker.rtVolume) ?? 0;
      this.onRequestSuccess();

      const result: IbkrQuote = {
        symbol: symbolUpper,
        last,
        bid,
        ask: Math.max(ask, bid),
        volume: Math.max(0, Math.round(volume))
      };
      this.quoteSubscriptionBlockedUntilBySymbol.delete(symbolUpper);

      this.logIbkrExternalRequest({
        startedMs,
        startedAt,
        operation: "getMarketDataSnapshot",
        reason: `Fetch quote snapshot for ${symbolUpper}`,
        requestPayload: {
          symbol: symbolUpper,
          delayedOnly: this.marketDataDelayedOnly
        },
        responsePayload: {
          symbol: result.symbol,
          last: result.last,
          bid: result.bid,
          ask: result.ask,
          volume: result.volume
        },
        status: "success"
      });

      return result;
    } catch (error) {
      const message = (error as Error).message;
      this.logIbkrExternalRequest({
        startedMs,
        startedAt,
        operation: "getMarketDataSnapshot",
        reason: `Fetch quote snapshot for ${symbolUpper}`,
        requestPayload: {
          symbol: symbolUpper,
          delayedOnly: this.marketDataDelayedOnly
        },
        status: "error",
        errorMessage: message
      });
      if (this.isSubscriptionError(error)) {
        this.marketDataDelayedOnly = true;
        this.quoteSubscriptionBlockedUntilBySymbol.set(
          symbolUpper,
          Date.now() + this.quoteSubscriptionBackoffMs
        );
        const hint = this.isDelayedDataNotEnabledError(error)
          ? " Enable delayed market data for API in TWS/Gateway, or subscribe to live market data for this exchange."
          : "";
        this.warnThrottled(
          "quote_subscription_unavailable",
          `IBKR quote request failed for ${symbolUpper}: ${message}${hint}`
        );
        return null;
      }

      if (this.isConnectivityError(error)) {
        this.enterRequestCooldown(message);
        this.warnThrottled("quote_connectivity_failed", `IBKR quote request failed for ${symbolUpper}: ${message}`);
        return null;
      }

      const key = message.toLowerCase().includes("timeout")
        ? "quote_timeout_failed"
        : `quote_failed_${symbolUpper}`;
      this.warnThrottled(key, `IBKR quote request failed for ${symbolUpper}: ${message}`);
      return null;
    }
  }

  private normalizeScannerSymbols(symbols: string[], limit: number): string[] {
    const safeLimit = Math.max(1, Math.min(60, Math.round(limit)));
    return uniq(
      symbols
        .map((symbol) => symbol.trim().toUpperCase())
        .filter((symbol) => /^[A-Z][A-Z0-9.\-]{0,14}$/.test(symbol))
    ).slice(0, safeLimit);
  }

  private async getScannerSymbolsViaTwsSocket(params: {
    scannerRequestBase: Omit<IbkrScannerRequest, "scanCode" | "limit">;
    scanCodes: IbScannerCode[];
    limit: number;
  }): Promise<{ symbols: string[]; failedCodes: Array<{ code: IbScannerCode; message: string }> }> {
    if (this.isRequestCoolingDown("scanner request")) {
      return {
        symbols: [],
        failedCodes: [
          {
            code: "COOLDOWN",
            message: `scanner request cooling down (${this.requestCooldownReason || "recent failure"})`
          }
        ]
      };
    }

    const rows: IbScannerRow[] = [];
    const failedCodes: Array<{ code: IbScannerCode; message: string }> = [];

    for (const code of params.scanCodes) {
      const numberOfRows = Math.max(10, Math.min(25, params.limit + 4));
      let codeSucceeded = false;
      let lastError: unknown = null;
      for (let attempt = 0; attempt < 2; attempt += 1) {
        try {
          const nextRows = await this.enqueueRequest({
            channel: "scanner",
            operation: `getMarketScanner:${String(code)}`,
            run: async () =>
              await this.getClient().getMarketScanner({
                ...params.scannerRequestBase,
                scanCode: code,
                numberOfRows
              })
          });
          rows.push(...nextRows);
          codeSucceeded = true;
          break;
        } catch (error) {
          lastError = error;
          if (attempt === 0 && this.isDuplicateScannerSubscriptionError(error)) {
            this.warnThrottled(
              "scanner_duplicate_subscription",
              `IBKR scanner duplicate subscription detected (${String(code)}). Recycling transport and retrying once.`
            );
            this.dropTransportClients();
            await this.sleep(450);
            continue;
          }
          break;
        }
      }

      if (!codeSucceeded && lastError) {
        const message = (lastError as Error).message;
        failedCodes.push({ code, message });

        if (this.isConnectivityError(lastError)) {
          this.enterRequestCooldown(message);
          this.warnThrottled("scanner_connectivity_failed", `IBKR scanner request failed: ${message}`);
          continue;
        }

        if (this.isDuplicateScannerSubscriptionError(lastError)) {
          this.warnThrottled(
            "scanner_duplicate_subscription",
            `IBKR scanner duplicate subscription persisted (${String(code)}): ${message}`
          );
          continue;
        }

        if (this.isSubscriptionError(lastError)) {
          this.warnThrottled(
            "scanner_subscription_failed",
            `IBKR scanner request failed due to entitlements/subscription: ${message}`
          );
          continue;
        }

        this.warnThrottled("scanner_generic_failed", `IBKR scanner request failed: ${message}`);
        continue;
      }

      const uniqueCount = this.normalizeScannerSymbols(
        rows.map((row) => row.symbol),
        params.limit
      ).length;
      if (uniqueCount >= params.limit) break;
    }

    const symbols = this.normalizeScannerSymbols(
      rows.map((row) => row.symbol),
      params.limit
    );
    return { symbols, failedCodes };
  }

  async getScannerSymbols(request: IbkrScannerRequest = {}): Promise<string[]> {
    return (await this.getScannerSymbolsWithSource(request)).symbols;
  }

  async getScannerSymbolsWithSource(request: IbkrScannerRequest = {}): Promise<IbkrScannerResult> {
    if (!this.isEnabled()) {
      return {
        symbols: [],
        source: "none",
        fallbackReason: "IBKR integration is disabled."
      };
    }
    const now = Date.now();
    if (this.scannerBackoffUntilMs > now) {
      const remainingMs = this.scannerBackoffUntilMs - now;
      this.warnThrottled(
        "scanner_backoff_active",
        `IBKR scanner request skipped for ${Math.ceil(remainingMs / 1000)}s due to repeated scanner failures.`,
        15_000
      );
      if (this.scannerCache?.symbols.length) {
        return {
          symbols: this.scannerCache.symbols.slice(
            0,
            Math.max(1, Math.min(60, Math.round(request.limit ?? 15)))
          ),
          source: this.scannerLastSource === "none" ? "tws_socket" : this.scannerLastSource,
          fallbackReason: this.scannerLastFallbackReason
        };
      }
      return {
        symbols: [],
        source: "none",
        fallbackReason: "scanner backoff active"
      };
    }

    const limit = Math.max(1, Math.min(60, Math.round(request.limit ?? 15)));
    const scanCodes: IbScannerCode[] =
      request.scanCode !== undefined
        ? [request.scanCode]
        : ["MOST_ACTIVE"];
    const scannerRequestBase: Omit<IbkrScannerRequest, "scanCode" | "limit"> = {
      instrument: request.instrument ?? "STK",
      locationCode: request.locationCode ?? "STK.US.MAJOR",
      abovePrice: request.abovePrice ?? 5,
      belowPrice: request.belowPrice ?? 1_500,
      aboveVolume: request.aboveVolume ?? 150_000,
      stockTypeFilter: request.stockTypeFilter
    };

    const cacheKey = JSON.stringify({
      ...scannerRequestBase,
      scanCodes,
      limit
    });
    if (this.scannerCache?.key === cacheKey && this.scannerCache.expiresAt > Date.now()) {
      return {
        symbols: this.scannerCache.symbols.slice(0, limit),
        source: this.scannerLastSource === "none" ? "tws_socket" : this.scannerLastSource,
        fallbackReason: this.scannerLastFallbackReason
      };
    }

    const inFlight = this.scannerInFlightByKey.get(cacheKey);
    if (inFlight) {
      const symbols = await inFlight;
      return {
        symbols: symbols.slice(0, limit),
        source: this.scannerLastSource === "none" ? "tws_socket" : this.scannerLastSource,
        fallbackReason: this.scannerLastFallbackReason
      };
    }

    const startedMs = Date.now();
    const startedAt = new Date(startedMs).toISOString();
    this.scannerLastAttemptAtMs = startedMs;

    const scannerRequest = (async (): Promise<string[]> => {
      const previousSource = this.scannerLastSource;
      const sourceErrors: Array<{ source: IbkrScannerSource; message: string }> = [];
      let source: IbkrScannerSource = "none";
      const twsResult = await this.getScannerSymbolsViaTwsSocket({
        scannerRequestBase,
        scanCodes,
        limit
      });
      const symbols = twsResult.symbols;
      const twsFailedCodes = twsResult.failedCodes;
      if (twsFailedCodes.length > 0) {
        sourceErrors.push({
          source: "tws_socket",
          message: twsFailedCodes
            .map((entry) => `${String(entry.code)}: ${entry.message}`)
            .join(" | ")
            .slice(0, 700)
        });
      }
      if (symbols.length > 0) {
        source = "tws_socket";
      }

      if (symbols.length > 0) {
        this.scannerCache = {
          key: cacheKey,
          symbols,
          expiresAt: Date.now() + this.scannerCacheTtlMs
        };
        this.onRequestSuccess();
        this.scannerLastSuccessAtMs = Date.now();
        this.scannerLastErrorAtMs = 0;
        this.scannerLastErrorMessage = "";
        this.scannerFailureStreak = 0;
        this.scannerBackoffUntilMs = 0;
        this.scannerLastSource = source;
        this.scannerLastFallbackReason =
          sourceErrors.length > 0
            ? sourceErrors.map((entry) => `${entry.source}: ${entry.message}`).join(" | ").slice(0, 700)
            : "";

        this.logIbkrExternalRequest({
          startedMs,
          startedAt,
          operation: "getMarketScanner",
          reason: "Discover dynamic symbols from IBKR market scanner",
          requestPayload: {
            ...scannerRequestBase,
            scanCodes,
            limit
          },
          responsePayload: {
            scannerSource: source,
            symbols: symbols.slice(0, 12),
            failedCodes: twsFailedCodes.map((entry) => ({
              code: entry.code,
              message: entry.message.slice(0, 140)
            })),
            sourceErrors: sourceErrors.map((entry) => ({
              source: entry.source,
              message: entry.message.slice(0, 180)
            }))
          },
          status: "success"
        });

        return symbols;
      }

      const message =
        sourceErrors.length > 0
          ? sourceErrors
              .map((entry) => `${entry.source}: ${entry.message}`)
              .join(" | ")
              .slice(0, 700)
          : "Scanner returned zero rows.";
      const onlyNoItemsRetrieved =
        sourceErrors.length > 0 &&
        sourceErrors.every((entry) =>
          entry.message.toLowerCase().includes("no items retrieved")
        );

      if (onlyNoItemsRetrieved) {
        this.scannerLastErrorAtMs = Date.now();
        this.scannerLastErrorMessage = message;
        this.scannerLastSource = "none";
        this.scannerLastFallbackReason = message;
        this.warnThrottled(
          "scanner_empty_no_items",
          "IBKR scanner returned no rows for the current market window/filter. Trying other providers.",
          30_000
        );
        this.logIbkrExternalRequest({
          startedMs,
          startedAt,
          operation: "getMarketScanner",
          reason: "Discover dynamic symbols from IBKR market scanner",
          requestPayload: {
            ...scannerRequestBase,
            scanCodes,
            limit
          },
          responsePayload: {
            sourceErrors: sourceErrors.map((entry) => ({
              source: entry.source,
              message: entry.message.slice(0, 180)
            })),
            noItemsRetrieved: true
          },
          status: "error",
          errorMessage: message
        });
        if (this.scannerCache?.symbols.length) {
          this.scannerLastSource = previousSource === "none" ? "tws_socket" : previousSource;
          return this.scannerCache.symbols.slice(0, limit);
        }
        return [];
      }

      this.scannerLastErrorAtMs = Date.now();
      this.scannerLastErrorMessage = message;
      this.scannerFailureStreak += 1;
      this.scannerLastSource = "none";
      this.scannerLastFallbackReason = message;
      const cooldownMs = Math.min(
        this.scannerFailureCooldownMaxMs,
        this.scannerFailureCooldownBaseMs * 2 ** Math.max(0, this.scannerFailureStreak - 1)
      );
      this.scannerBackoffUntilMs = Date.now() + cooldownMs;
      this.warnThrottled(
        "scanner_failure_backoff",
        `IBKR scanner entering backoff for ${Math.ceil(cooldownMs / 1000)}s after repeated failures (streak=${this.scannerFailureStreak}).`,
        10_000
      );

      this.logIbkrExternalRequest({
        startedMs,
        startedAt,
          operation: "getMarketScanner",
          reason: "Discover dynamic symbols from IBKR market scanner",
          requestPayload: {
            ...scannerRequestBase,
            scanCodes,
            limit
          },
        responsePayload: {
          sourceErrors: sourceErrors.map((entry) => ({
            source: entry.source,
            message: entry.message.slice(0, 180)
          }))
        },
        status: "error",
        errorMessage: message
      });

      if (this.scannerCache?.symbols.length) {
        this.warnThrottled(
          "scanner_stale_cache_fallback",
          "IBKR scanner failed; using stale cached scanner symbols."
        );
        this.scannerLastSource = previousSource === "none" ? "tws_socket" : previousSource;
        return this.scannerCache.symbols.slice(0, limit);
      }

      return [];
    })().finally(() => {
      this.scannerInFlightByKey.delete(cacheKey);
    });

    this.scannerInFlightByKey.set(cacheKey, scannerRequest);
    const symbols = await scannerRequest;
    return {
      symbols: symbols.slice(0, limit),
      source: this.scannerLastSource,
      fallbackReason: this.scannerLastFallbackReason
    };
  }

  async getRecentDailyCloses(symbol: string, bars = 60): Promise<number[]> {
    const dailyBars = await this.getRecentDailyBars(symbol, bars);
    return dailyBars
      .map((bar) => asFiniteNumber(bar.close))
      .filter((value): value is number => value !== null && value > 0)
      .slice(-bars);
  }

  private normalizeHistoricalBar(raw: IbHistoricalBar): DailyBar | null {
    const close = asFiniteNumber(raw.close);
    if (close === null || close <= 0) return null;
    const highRaw = asFiniteNumber(raw.high);
    const lowRaw = asFiniteNumber(raw.low);
    const openRaw = asFiniteNumber(raw.open);
    const high = highRaw !== null && highRaw > 0 ? Math.max(highRaw, close) : close;
    const low = lowRaw !== null && lowRaw > 0 ? Math.min(lowRaw, close, high) : close;
    const open =
      openRaw !== null && openRaw > 0
        ? Math.max(Math.min(openRaw, high), low)
        : close;
    const volume = Math.max(0, Math.round(asFiniteNumber(raw.volume) ?? 0));
    const timestamp =
      typeof raw.date === "string" && raw.date.trim().length > 0
        ? raw.date.trim()
        : null;

    return {
      timestamp,
      open: Number(open.toFixed(6)),
      high: Number(high.toFixed(6)),
      low: Number(low.toFixed(6)),
      close: Number(close.toFixed(6)),
      volume
    };
  }

  async getRecentDailyBars(symbol: string, bars = 60): Promise<DailyBar[]> {
    if (!this.isEnabled()) return [];
    if (this.historicalFractionalRulesUnsupported) return [];
    if (this.isRequestCoolingDown("historical request")) return [];

    const symbolUpper = symbol.toUpperCase();
    const startedMs = Date.now();
    const startedAt = new Date(startedMs).toISOString();
    try {
      const durationDays = Math.max(5, Math.min(365, Math.ceil(bars * 1.5)));
      const historical = (await this.withTransientRetry(
        `historical closes (${symbolUpper})`,
        async () =>
          await this.enqueueRequest({
            channel: "historical",
            operation: `getHistoricalData:${symbolUpper}`,
            run: async () =>
              (await this.getClient().getHistoricalData({
                contract: Contract.stock(symbolUpper),
                endDateTime: "",
                duration: `${durationDays} D`,
                barSizeSetting: "1 day",
                whatToShow: "TRADES",
                useRth: 1,
                formatDate: 1
              })) as IbHistoricalData
          }),
        {
          maxAttempts: 2
        }
      )) as IbHistoricalData;

      const normalizedBars = (historical.bars ?? [])
        .map((bar) => this.normalizeHistoricalBar(bar))
        .filter((value): value is DailyBar => value !== null);
      this.onRequestSuccess();
      const sliced = normalizedBars.slice(-bars);
      this.logIbkrExternalRequest({
        startedMs,
        startedAt,
        operation: "getHistoricalData",
        reason: `Fetch ${bars} daily bars for ${symbolUpper}`,
        requestPayload: {
          symbol: symbolUpper,
          bars,
          durationDays
        },
        responsePayload: {
          barsReturned: sliced.length
        },
        status: "success"
      });
      return sliced;
    } catch (error) {
      const message = (error as Error).message;
      const normalized = message.toLowerCase();

      this.logIbkrExternalRequest({
        startedMs,
        startedAt,
        operation: "getHistoricalData",
        reason: `Fetch ${bars} daily bars for ${symbolUpper}`,
        requestPayload: {
          symbol: symbolUpper,
          bars
        },
        status: "error",
        errorMessage: message
      });

      if (normalized.includes(HISTORICAL_FRACTIONAL_WARNING)) {
        this.historicalFractionalRulesUnsupported = true;
        if (!this.historicalFractionalRulesWarned) {
          logger.warn(
            `IBKR historical bars disabled for this session due to API compatibility warning: ${message}`
          );
          this.historicalFractionalRulesWarned = true;
        }
        return [];
      }

      if (this.isConnectivityError(error)) {
        this.enterRequestCooldown(message);
        this.warnThrottled("historical_connectivity_failed", `IBKR historical request failed for ${symbolUpper}: ${message}`);
        return [];
      }

      const key = normalized.includes("timeout") ? "historical_timeout_failed" : `historical_failed_${symbolUpper}`;
      this.warnThrottled(key, `IBKR historical request failed for ${symbolUpper}: ${message}`);
      return [];
    }
  }

  async getOptionContracts(
    symbol: string,
    underlyingPrice: number,
    dteMin: number,
    dteMax: number
  ): Promise<Array<{ symbol: string; expiration: string; strike: number; right: "C" | "P" }>> {
    if (!this.isEnabled()) return [];
    if (this.isRequestCoolingDown("option chain request")) return [];
    const symbolUpper = symbol.toUpperCase();
    const startedMs = Date.now();
    const startedAt = new Date(startedMs).toISOString();
    try {
      const details = (await this.withTransientRetry(
        `option contract details (${symbolUpper})`,
        async () =>
          await this.enqueueRequest({
            channel: "option_chain",
            operation: `getContractDetails:${symbolUpper}`,
            run: async () =>
              (await this.getClient().getContractDetails(Contract.stock(symbolUpper))) as IbContractDetails[]
          }),
        {
          maxAttempts: 2
        }
      )) as IbContractDetails[];
      const underlyingConId = details
        .map((row) => asFiniteNumber(row?.contract?.conId))
        .find((value): value is number => value !== null && value > 0);
      if (!underlyingConId) return [];

      const secDefRaw = (await this.withTransientRetry(
        `option secdef params (${symbolUpper})`,
        async () =>
          await this.enqueueRequest({
            channel: "option_chain",
            operation: `getSecDefOptParams:${symbolUpper}`,
            run: async () =>
              (await this.getClient().getSecDefOptParams({
                contract: Contract.stock({ symbol: symbolUpper, conId: underlyingConId }),
                futFopExchange: "",
                exchange: "SMART"
              })) as IbSecDefOptParam[] | IbSecDefOptParam
          }),
        {
          maxAttempts: 2
        }
      )) as IbSecDefOptParam[] | IbSecDefOptParam;

      const secDefRows = Array.isArray(secDefRaw) ? secDefRaw : [secDefRaw];
      const selected = secDefRows.find((row) => row?.exchange === "SMART") ?? secDefRows[0];
      if (!selected) return [];

      const now = new Date();
      const expirations = (selected.expirations ?? [])
        .map((raw) => ({ raw, date: parseExpirationDate(raw) }))
        .filter((item): item is { raw: string; date: Date } => item.date !== null)
        .map((item) => ({ ...item, dte: dteBetween(item.date, now) }))
        .filter((item) => item.dte >= dteMin && item.dte <= dteMax)
        .sort((a, b) => a.dte - b.dte)
        .slice(0, 4)
        .map((item) => item.raw);
      if (expirations.length === 0) return [];

      const strikes = [...new Set(selected.strikes ?? [])]
        .map((strike) => asFiniteNumber(strike))
        .filter((strike): strike is number => strike !== null && strike > 0)
        .sort((a, b) => Math.abs(a - underlyingPrice) - Math.abs(b - underlyingPrice))
        .slice(0, 14)
        .sort((a, b) => a - b);
      if (strikes.length === 0) return [];

      const rows: Array<{ symbol: string; expiration: string; strike: number; right: "C" | "P" }> = [];
      for (const expiration of expirations) {
        for (const strike of strikes) {
          rows.push({ symbol: symbolUpper, expiration, strike, right: "C" });
          rows.push({ symbol: symbolUpper, expiration, strike, right: "P" });
        }
      }
      this.onRequestSuccess();
      this.logIbkrExternalRequest({
        startedMs,
        startedAt,
        operation: "getSecDefOptParams",
        reason: `Fetch option chain contracts for ${symbolUpper}`,
        requestPayload: {
          symbol: symbolUpper,
          dteMin,
          dteMax,
          underlyingPrice
        },
        responsePayload: {
          contractCount: rows.length
        },
        status: "success"
      });
      return rows;
    } catch (error) {
      const message = (error as Error).message;
      this.logIbkrExternalRequest({
        startedMs,
        startedAt,
        operation: "getSecDefOptParams",
        reason: `Fetch option chain contracts for ${symbolUpper}`,
        requestPayload: {
          symbol: symbolUpper,
          dteMin,
          dteMax,
          underlyingPrice
        },
        status: "error",
        errorMessage: message
      });
      if (this.isConnectivityError(error)) {
        this.enterRequestCooldown(message);
        this.warnThrottled("option_chain_connectivity_failed", `IBKR option chain request failed for ${symbolUpper}: ${message}`);
        return [];
      }
      this.warnThrottled(
        message.toLowerCase().includes("timeout") ? "option_chain_timeout_failed" : `option_chain_failed_${symbolUpper}`,
        `IBKR option chain request failed for ${symbolUpper}: ${message}`
      );
      return [];
    }
  }

  private async marketSnapshotForContract(
    contract: Record<string, unknown>
  ): Promise<IbTickerSnapshot | null> {
    if (!this.isEnabled()) return null;
    if (this.isRequestCoolingDown("market snapshot request")) return null;
    const symbol = String(contract.symbol ?? "").toUpperCase();
    const secType = String(contract.secType ?? "");
    const startedMs = Date.now();
    const startedAt = new Date(startedMs).toISOString();

    await this.setMarketDataType(this.marketDataDelayedOnly ? 3 : 1);
    try {
      const ticker = (await this.withTransientRetry(
        `market snapshot (${symbol || "UNKNOWN"})`,
        async () =>
          await this.enqueueRequest({
            channel: "quote",
            operation: `getMarketDataSnapshot:${symbol || "UNKNOWN"}:${secType || "SEC"}`,
            run: async () =>
              (await this.getClient().getMarketDataSnapshot({
                contract,
                regulatorySnapshot: false
              })) as IbTickerSnapshot
          }),
        {
          maxAttempts: 2
        }
      )) as IbTickerSnapshot;
      this.onRequestSuccess();
      this.logIbkrExternalRequest({
        startedMs,
        startedAt,
        operation: "getMarketDataSnapshot",
        reason: `Fetch contract market snapshot for ${symbol || "UNKNOWN"}`,
        requestPayload: {
          symbol,
          secType,
          delayedOnly: this.marketDataDelayedOnly
        },
        status: "success"
      });
      return ticker;
    } catch (error) {
      const message = (error as Error).message;
      if (!this.isSubscriptionError(error)) {
        if (this.isConnectivityError(error)) {
          this.enterRequestCooldown(message);
        }
        this.logIbkrExternalRequest({
          startedMs,
          startedAt,
          operation: "getMarketDataSnapshot",
          reason: `Fetch contract market snapshot for ${symbol || "UNKNOWN"}`,
          requestPayload: {
            symbol,
            secType,
            delayedOnly: this.marketDataDelayedOnly
          },
          status: "error",
          errorMessage: message
        });
        throw error;
      }
    }

    this.marketDataDelayedOnly = true;
    await this.setMarketDataType(3);
    let delayedTicker: IbTickerSnapshot;
    try {
      delayedTicker = (await this.withTransientRetry(
        `delayed market snapshot (${symbol || "UNKNOWN"})`,
        async () =>
          await this.enqueueRequest({
            channel: "quote",
            operation: `getMarketDataSnapshot:${symbol || "UNKNOWN"}:${secType || "SEC"}:delayed`,
            run: async () =>
              (await this.getClient().getMarketDataSnapshot({
                contract,
                regulatorySnapshot: false
              })) as IbTickerSnapshot
          }),
        {
          maxAttempts: 2
        }
      )) as IbTickerSnapshot;
    } catch (delayedError) {
      if (!this.isDelayedDataNotEnabledError(delayedError)) throw delayedError;
      await this.setMarketDataType(4);
      delayedTicker = (await this.withTransientRetry(
        `delayed frozen market snapshot (${symbol || "UNKNOWN"})`,
        async () =>
          await this.enqueueRequest({
            channel: "quote",
            operation: `getMarketDataSnapshot:${symbol || "UNKNOWN"}:${secType || "SEC"}:delayed_frozen`,
            run: async () =>
              (await this.getClient().getMarketDataSnapshot({
                contract,
                regulatorySnapshot: false
              })) as IbTickerSnapshot
          }),
        {
          maxAttempts: 2
        }
      )) as IbTickerSnapshot;
    }
    this.onRequestSuccess();
    this.logIbkrExternalRequest({
      startedMs,
      startedAt,
      operation: "getMarketDataSnapshot",
      reason: `Fetch contract market snapshot for ${symbol || "UNKNOWN"} (delayed mode)`,
      requestPayload: {
        symbol,
        secType,
        delayedOnly: true
      },
      status: "success"
    });
    return delayedTicker;
  }

  async getOptionQuote(contract: {
    symbol: string;
    expiration: string;
    strike: number;
    right: "CALL" | "PUT";
  }): Promise<IbkrOptionQuote | null> {
    const readiness = this.getOptionQuoteReadiness(Date.now());
    if (!readiness.allowed) {
      this.warnThrottled(
        `option_quote_readiness_${readiness.reason}`,
        `IBKR option quote request skipped (${readiness.reason}).`,
        90_000
      );
      return null;
    }
    if (this.isRequestCoolingDown("option quote request")) return null;
    const contractKey = this.optionContractIdentity(contract);
    if (this.isInvalidOptionContractCoolingDown(contractKey)) return null;
    if (Date.now() < this.optionQuoteEntitlementUntilMs) {
      this.warnThrottled(
        "option_quote_entitlement_backoff",
        `IBKR option quote requests paused for ${Math.ceil((this.optionQuoteEntitlementUntilMs - Date.now()) / 60_000)}m after subscription failure (${this.optionQuoteEntitlementReason}).`,
        2 * 60_000
      );
      return null;
    }

    try {
      const ibContract = Contract.option({
        symbol: contract.symbol.toUpperCase(),
        lastTradeDateOrContractMonth: this.normalizeOptionExpiration(contract.expiration),
        strike: contract.strike,
        right: this.normalizeOptionRight(contract.right),
        exchange: "SMART",
        currency: "USD",
        multiplier: 100
      });

      const ticker = await this.marketSnapshotForContract(ibContract);
      if (!ticker) return null;
      this.optionQuoteEntitlementUntilMs = 0;
      this.optionQuoteEntitlementReason = "";
      return this.toOptionQuote(contract, ticker);
    } catch (error) {
      const message = (error as Error).message;
      if (this.isSubscriptionError(error)) {
        this.marketDataDelayedOnly = true;
        this.optionQuoteEntitlementUntilMs =
          Date.now() + this.optionQuoteEntitlementBackoffMs;
        this.optionQuoteEntitlementReason = message;
        const hint = this.isDelayedDataNotEnabledError(error)
          ? " Enable delayed market data for API in TWS/Gateway, or subscribe to live market data for this exchange."
          : " Option quote requests will be paused for 15 minutes; broker/account mark data will be used where available.";
        this.warnThrottled(
          "option_quote_subscription_failed",
          `IBKR option quote request failed for ${contract.symbol} ${contract.expiration} ${contract.strike} ${contract.right}: ${message}${hint}`
        );
        return null;
      }
      if (this.isConnectivityError(error)) {
        this.enterRequestCooldown(message);
        this.warnThrottled(
          "option_quote_connectivity_failed",
          `IBKR option quote request failed for ${contract.symbol} ${contract.expiration} ${contract.strike} ${contract.right}: ${message}`
        );
        return null;
      }
      if (this.isNoSecurityDefinitionError(error)) {
        const untilMs = Date.now() + this.invalidOptionContractBackoffMs;
        this.invalidOptionContracts.set(contractKey, {
          untilMs,
          reason: message
        });
        this.warnThrottled(
          `option_quote_invalid_contract_${contractKey}`,
          `IBKR option contract not recognized (${contract.symbol} ${contract.expiration} ${contract.strike} ${contract.right}). Suppressing retries for 6h.`,
          30 * 60_000
        );
        return null;
      }
      this.warnThrottled(
        message.toLowerCase().includes("timeout") ? "option_quote_timeout_failed" : "option_quote_generic_failed",
        `IBKR option quote request failed for ${contract.symbol} ${contract.expiration} ${contract.strike} ${contract.right}: ${message}`
      );
      return null;
    }
  }

  async getOptionMidPrice(contract: {
    symbol: string;
    expiration: string;
    strike: number;
    right: "CALL" | "PUT";
  }): Promise<number | null> {
    const quote = await this.getOptionQuote(contract);
    return quote?.mid ?? null;
  }

  async getPositionsSnapshot(): Promise<IbkrPositionSnapshot[]> {
    if (!this.isEnabled()) return [];
    if (this.positionsRequestUnsupported) return [];
    const now = Date.now();
    if (
      this.lastPositionsSnapshotAtMs > 0 &&
      now - this.lastPositionsSnapshotAtMs < this.positionsSnapshotTtlMs
    ) {
      return this.clonePositionsSnapshot(this.lastPositionsSnapshot);
    }
    if (
      this.isQueueBacklogged() &&
      this.lastPositionsSnapshot.length > 0 &&
      now - this.lastPositionsSnapshotAtMs < this.positionsSnapshotTtlMs * 3
    ) {
      this.warnThrottled(
        "positions_deferred_queue_backlog",
        `IBKR positions refresh deferred due to queue backlog (depth=${this.queueTasks.length + this.queueActiveWorkers}).`,
        20_000
      );
      return this.clonePositionsSnapshot(this.lastPositionsSnapshot);
    }
    if (this.positionsSnapshotInFlight) {
      return this.clonePositionsSnapshot(await this.positionsSnapshotInFlight);
    }
    if (this.isRequestCoolingDown("positions request")) {
      if (this.lastPositionsSnapshot.length > 0) {
        return this.clonePositionsSnapshot(this.lastPositionsSnapshot);
      }
      return [];
    }

    const request = this.fetchPositionsSnapshot();
    this.positionsSnapshotInFlight = request;
    try {
      const snapshot = await request;
      this.lastPositionsSnapshot = this.clonePositionsSnapshot(snapshot);
      this.lastPositionsSnapshotAtMs = Date.now();
      return this.clonePositionsSnapshot(snapshot);
    } finally {
      this.positionsSnapshotInFlight = null;
    }
  }

  private async fetchPositionsSnapshot(): Promise<IbkrPositionSnapshot[]> {
    const startedMs = Date.now();
    const startedAt = new Date(startedMs).toISOString();

    try {
      await this.ensureAccountUpdatesSubscription();
      const rows = (await this.withTransientRetry(
        "positions snapshot",
        async () =>
          await this.enqueueRequest({
            channel: "positions",
            operation: "getPositions",
            run: async () => (await this.getClient(this.activePort).getPositions()) as IbPositionRowMap
          }),
        {
          maxAttempts: 2
        }
      )) as IbPositionRowMap;
      this.onRequestSuccess();
      const positions = Object.values(rows)
        .map((row) => this.parsePositionRow(row))
        .filter((row): row is IbkrPositionSnapshot => row !== null);
      this.logIbkrExternalRequest({
        startedMs,
        startedAt,
        operation: "getPositions",
        reason: "Fetch broker positions snapshot",
        requestPayload: {
          port: this.activePort
        },
        responsePayload: {
          positions: positions.length
        },
        status: "success"
      });
      return this.clonePositionsSnapshot(positions);
    } catch (error) {
      const message = (error as Error).message;
      const normalized = message.toLowerCase();
      this.logIbkrExternalRequest({
        startedMs,
        startedAt,
        operation: "getPositions",
        reason: "Fetch broker positions snapshot",
        requestPayload: {
          port: this.activePort
        },
        status: "error",
        errorMessage: message
      });
      if (normalized.includes("does not support positions request")) {
        this.positionsRequestUnsupported = true;
        this.warnThrottled(
          "positions_unsupported",
          "IBKR API session does not support positions request. Positions sync will rely on account updates."
        );
        return [];
      }
      if (this.isConnectivityError(error)) {
        this.enterRequestCooldown(message);
        this.warnThrottled("positions_connectivity_failed", `IBKR positions request failed: ${message}`);
        if (this.lastPositionsSnapshot.length > 0) {
          return this.clonePositionsSnapshot(this.lastPositionsSnapshot);
        }
        return [];
      }
      this.warnThrottled("positions_generic_failed", `IBKR positions request failed: ${message}`);
      if (this.lastPositionsSnapshot.length > 0) {
        return this.clonePositionsSnapshot(this.lastPositionsSnapshot);
      }
      return [];
    }
  }

  async getAccountSnapshot(): Promise<IbkrAccountSnapshot | null> {
    if (!this.isEnabled()) return null;

    const now = Date.now();
    if (
      this.lastAccountSnapshot &&
      now - this.lastAccountSnapshotAtMs < this.accountSnapshotTtlMs
    ) {
      return { ...this.lastAccountSnapshot };
    }

    if (this.isQueueBacklogged() && this.lastAccountSnapshot) {
      this.warnThrottled(
        "account_snapshot_deferred_queue_backlog",
        `IBKR account snapshot refresh deferred due to queue backlog (depth=${this.queueTasks.length + this.queueActiveWorkers}).`,
        20_000
      );
      return { ...this.lastAccountSnapshot };
    }

    if (this.accountSnapshotInFlight) {
      const inFlight = await this.accountSnapshotInFlight;
      return inFlight ? { ...inFlight } : null;
    }

    const request = this.fetchAccountSnapshot();
    this.accountSnapshotInFlight = request;
    try {
      const snapshot = await request;
      if (snapshot) {
        this.lastAccountSnapshot = { ...snapshot };
        this.lastAccountSnapshotAtMs = Date.now();
        return { ...snapshot };
      }
      return null;
    } finally {
      this.accountSnapshotInFlight = null;
    }
  }

  private async fetchAccountSnapshot(): Promise<IbkrAccountSnapshot | null> {
    if (this.isRequestCoolingDown("account snapshot request") && this.accountValues.size === 0) {
      return null;
    }
    const startedMs = Date.now();
    const startedAt = new Date(startedMs).toISOString();

    await this.ensureAccountUpdatesSubscription();
    const positions = await this.getPositionsSnapshot();
    const openPositions = positions.filter((position) => Math.abs(position.position) > 1e-6);

    let unrealizedFromMarks = 0;
    for (const position of openPositions) {
      if (typeof position.unrealizedPnl === "number" && Number.isFinite(position.unrealizedPnl)) {
        unrealizedFromMarks += position.unrealizedPnl;
        continue;
      }

      const contract = this.contractForPosition(position);
      if (!contract) continue;

      try {
        const ticker = await this.marketSnapshotForContract(contract);
        if (!ticker) continue;
        const mark = this.marketMidFromTicker(ticker);
        if (mark === null) continue;

        const marketValue = mark * position.position * position.multiplier;
        position.marketPrice = mark;
        position.marketValue = marketValue;
        const estimatedUnrealized = marketValue - position.avgCost * position.position;
        position.unrealizedPnl = estimatedUnrealized;
        unrealizedFromMarks += estimatedUnrealized;
      } catch (error) {
        const message = (error as Error).message;
        if (this.isConnectivityError(error)) {
          this.enterRequestCooldown(message);
        }
        this.warnThrottled(
          `mark_to_market_${position.symbol}`,
          `IBKR mark-to-market failed for ${position.symbol} (${position.secType}): ${
            message
          }`
        );
      }
    }

    const netLiquidation = this.accountValueNumber("NetLiquidation");
    const portfolioRealized = [...this.portfolioValues.values()].reduce(
      (sum, value) => sum + value.realizedPnl,
      0
    );
    const portfolioUnrealized = [...this.portfolioValues.values()].reduce(
      (sum, value) => sum + value.unrealizedPnl,
      0
    );

    const realizedPnl =
      this.accountValueNumber("RealizedPnL") ??
      (this.portfolioValues.size > 0 ? portfolioRealized : undefined);
    const unrealizedPnl =
      this.accountValueNumber("UnrealizedPnL") ??
      (this.portfolioValues.size > 0
        ? portfolioUnrealized
        : openPositions.length > 0
        ? unrealizedFromMarks
        : undefined);
    const totalCashValue = this.accountValueNumber("TotalCashValue");
    const availableFunds = this.accountValueNumber("AvailableFunds");
    const buyingPower = this.accountValueNumber("BuyingPower");

    const snapshot: IbkrAccountSnapshot = {
      timestamp: new Date().toISOString(),
      accountCode: this.inferAccountCode(),
      netLiquidation,
      realizedPnl,
      unrealizedPnl,
      totalCashValue,
      availableFunds,
      buyingPower,
      positionCount: openPositions.length,
      source:
        netLiquidation !== undefined || this.accountValues.size > 0
          ? "account_updates"
          : openPositions.length > 0
          ? "positions_mark_to_market"
          : "unavailable"
    };

    this.logIbkrExternalRequest({
      startedMs,
      startedAt,
      operation: "getAccountSnapshot",
      reason: "Build account equity/pnl snapshot from account updates and positions",
      responsePayload: {
        accountCode: snapshot.accountCode,
        source: snapshot.source,
        positionCount: snapshot.positionCount,
        netLiquidation: snapshot.netLiquidation
      },
      status: "success"
    });

    return snapshot;
  }

  getLastScannerResult(): { source: IbkrScannerSource; fallbackReason: string } {
    return {
      source: this.scannerLastSource,
      fallbackReason: this.scannerLastFallbackReason
    };
  }

  getMarketDataEntitlementState(nowMs = Date.now()): {
    delayedOnly: boolean;
    quoteSubscriptionBackoffs: Array<{ symbol: string; until: string; remainingMs: number }>;
    optionQuoteEntitlementBackoff: {
      active: boolean;
      until: string | null;
      remainingMs: number;
      reason: string;
    };
  } {
    const quoteSubscriptionBackoffs = [...this.quoteSubscriptionBlockedUntilBySymbol.entries()]
      .map(([symbol, untilMs]) => ({
        symbol,
        untilMs,
        remainingMs: Math.max(0, untilMs - nowMs)
      }))
      .filter((entry) => entry.remainingMs > 0)
      .sort((left, right) => right.remainingMs - left.remainingMs)
      .map((entry) => ({
        symbol: entry.symbol,
        until: new Date(entry.untilMs).toISOString(),
        remainingMs: entry.remainingMs
      }));

    const optionBackoffActive = this.optionQuoteEntitlementUntilMs > nowMs;
    return {
      delayedOnly: this.marketDataDelayedOnly,
      quoteSubscriptionBackoffs,
      optionQuoteEntitlementBackoff: {
        active: optionBackoffActive,
        until: optionBackoffActive
          ? new Date(this.optionQuoteEntitlementUntilMs).toISOString()
          : null,
        remainingMs: optionBackoffActive
          ? Math.max(0, this.optionQuoteEntitlementUntilMs - nowMs)
          : 0,
        reason: optionBackoffActive ? this.optionQuoteEntitlementReason : ""
      }
    };
  }

  getRuntimeStatus(nowMs = Date.now()): {
    queue: {
      depth: number;
      activeWorkers: number;
      maxWorkers: number;
      nextGlobalStartAt: string | null;
      channelNextStarts: Record<string, string>;
    };
    requestCooldown: {
      active: boolean;
      reason: string;
      until: string | null;
      remainingMs: number;
    };
    scannerCache: {
      active: boolean;
      symbolCount: number;
      expiresAt: string | null;
      remainingMs: number;
      inFlight: number;
      source: IbkrScannerSource;
      fallbackReason: string;
      failureStreak: number;
      backoffUntil: string | null;
      backoffRemainingMs: number;
      lastAttemptAt: string | null;
      lastSuccessAt: string | null;
      lastErrorAt: string | null;
      lastErrorMessage: string;
    };
    compatibility: {
      historicalFractionalRulesUnsupported: boolean;
      historicalFractionalRulesWarned: boolean;
    };
  } {
    const toIso = (value: number | null | undefined): string | null => {
      if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) return null;
      return new Date(value).toISOString();
    };

    const cooldownActive = this.requestCooldownUntilMs > nowMs;
    const cooldownRemaining = cooldownActive
      ? Math.max(0, this.requestCooldownUntilMs - nowMs)
      : 0;

    const scannerActive =
      this.scannerCache !== null && this.scannerCache.expiresAt > nowMs;
    const scannerRemaining =
      scannerActive && this.scannerCache
        ? Math.max(0, this.scannerCache.expiresAt - nowMs)
        : 0;
    const scannerBackoffRemaining =
      this.scannerBackoffUntilMs > nowMs ? Math.max(0, this.scannerBackoffUntilMs - nowMs) : 0;

    const channelNextStarts: Record<string, string> = {};
    for (const [channel, epoch] of this.queueNextStartAtByChannel.entries()) {
      const iso = toIso(epoch);
      if (!iso) continue;
      channelNextStarts[channel] = iso;
    }

    return {
      queue: {
        depth: this.queueTasks.length,
        activeWorkers: this.queueActiveWorkers,
        maxWorkers: this.queueMaxConcurrentWorkers,
        nextGlobalStartAt: toIso(this.queueNextGlobalStartAt),
        channelNextStarts
      },
      requestCooldown: {
        active: cooldownActive,
        reason: cooldownActive ? this.requestCooldownReason : "",
        until: cooldownActive ? toIso(this.requestCooldownUntilMs) : null,
        remainingMs: cooldownRemaining
      },
      scannerCache: {
        active: scannerActive,
        symbolCount: scannerActive && this.scannerCache ? this.scannerCache.symbols.length : 0,
        expiresAt: scannerActive && this.scannerCache ? toIso(this.scannerCache.expiresAt) : null,
        remainingMs: scannerRemaining,
        inFlight: this.scannerInFlightByKey.size,
        source: this.scannerLastSource,
        fallbackReason: this.scannerLastFallbackReason,
        failureStreak: this.scannerFailureStreak,
        backoffUntil: scannerBackoffRemaining > 0 ? toIso(this.scannerBackoffUntilMs) : null,
        backoffRemainingMs: scannerBackoffRemaining,
        lastAttemptAt: toIso(this.scannerLastAttemptAtMs),
        lastSuccessAt: toIso(this.scannerLastSuccessAtMs),
        lastErrorAt: toIso(this.scannerLastErrorAtMs),
        lastErrorMessage: this.scannerLastErrorMessage
      },
      compatibility: {
        historicalFractionalRulesUnsupported: this.historicalFractionalRulesUnsupported,
        historicalFractionalRulesWarned: this.historicalFractionalRulesWarned
      }
    };
  }

  async submitPaperOrder(payload: IbkrSubmitOrderPayload): Promise<string | null> {
    if (!this.isEnabled()) {
      return `sim-${String(payload.orderId ?? "order")}`;
    }

    if (payload.quantity <= 0) {
      throw new Error("IBKR order quantity must be positive.");
    }
    if (payload.limitPrice <= 0) {
      throw new Error("IBKR order limit price must be positive.");
    }

    const contract = Contract.option({
      symbol: payload.symbol.toUpperCase(),
      lastTradeDateOrContractMonth: this.normalizeOptionExpiration(payload.expiration),
      strike: payload.strike,
      right: this.normalizeOptionRight(payload.right),
      exchange: "SMART",
      currency: "USD",
      multiplier: 100
    });

    const order = Order.limit({
      action: payload.side,
      totalQuantity: payload.quantity,
      lmtPrice: payload.limitPrice,
      tif: "DAY",
      orderRef: payload.orderId,
      transmit: true
    });
    const startedMs = Date.now();
    const startedAt = new Date(startedMs).toISOString();

    let brokerOrderId: number;
    try {
      brokerOrderId = await this.enqueueRequest({
        channel: "order",
        operation: "placeOrder",
        run: async () =>
          await this.getClient().placeOrder({
            contract,
            order
          })
      });
      this.logIbkrExternalRequest({
        startedMs,
        startedAt,
        operation: "placeOrder",
        reason: `Submit ${payload.side} option order for ${payload.symbol.toUpperCase()}`,
        requestPayload: {
          symbol: payload.symbol.toUpperCase(),
          side: payload.side,
          quantity: payload.quantity,
          limitPrice: payload.limitPrice,
          expiration: payload.expiration,
          strike: payload.strike,
          right: payload.right
        },
        responsePayload: {
          brokerOrderId
        },
        status: "success"
      });
    } catch (error) {
      this.logIbkrExternalRequest({
        startedMs,
        startedAt,
        operation: "placeOrder",
        reason: `Submit ${payload.side} option order for ${payload.symbol.toUpperCase()}`,
        requestPayload: {
          symbol: payload.symbol.toUpperCase(),
          side: payload.side,
          quantity: payload.quantity,
          limitPrice: payload.limitPrice,
          expiration: payload.expiration,
          strike: payload.strike,
          right: payload.right
        },
        status: "error",
        errorMessage: (error as Error).message
      });
      throw error;
    }
    this.ensureRawFieldsetStream(this.getClient(this.activePort));

    this.updateOrderMapping(payload.orderId, brokerOrderId);
    this.brokerOrderStatuses.set(brokerOrderId, {
      localOrderId: payload.orderId,
      brokerOrderId,
      status: "PendingSubmit",
      filled: 0,
      remaining: payload.quantity,
      avgFillPrice: 0,
      lastFillPrice: 0,
      permId: 0,
      clientId: settings.ibkrClientId,
      whyHeld: "",
      source: "submit",
      updatedAt: new Date().toISOString()
    });

    await this.refreshOpenOrderSnapshots();
    return `ibkr-paper-${brokerOrderId}`;
  }
}
