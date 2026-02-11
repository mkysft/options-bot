import { EventEmitter } from "node:events";
import {
  ConnectionState,
  EventName,
  IBApiNext,
  IBApiTickType,
  Instrument,
  LocationCode,
  MarketDataType,
  ScanCode,
  SecType,
  type Contract as StoqeyContract,
  type OpenOrder,
  type Order as StoqeyOrder,
  type Position
} from "@stoqey/ib";

export type IbContract = Record<string, unknown>;
export type IbOrder = Record<string, unknown>;
export type IbScannerInstrument = keyof typeof Instrument | Instrument | string;
export type IbScannerLocation = keyof typeof LocationCode | LocationCode | string;
export type IbScannerCode = keyof typeof ScanCode | ScanCode | number | string;

export interface IbScannerRow {
  rank: number;
  symbol: string;
  secType?: string;
  exchange?: string;
  currency?: string;
  distance?: string;
  benchmark?: string;
  projection?: string;
}

export interface IbClientOptions {
  host?: string;
  port?: number;
  clientId?: number;
  timeoutMs?: number;
}

interface RawApiLike {
  on: (event: string, listener: (...args: unknown[]) => void) => unknown;
  off?: (event: string, listener: (...args: unknown[]) => void) => unknown;
  removeListener?: (event: string, listener: (...args: unknown[]) => void) => unknown;
  reqAccountUpdates?: (subscribe: boolean, accountCode: string) => unknown;
  reqCurrentTime?: () => unknown;
}

const asNumber = (value: unknown): number | undefined => {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  }
  return undefined;
};

const tickValue = (ticks: ReadonlyMap<number, { value?: number }>, tickType: number): number | undefined => {
  const tick = ticks.get(tickType);
  return typeof tick?.value === "number" && Number.isFinite(tick.value) ? tick.value : undefined;
};

const toError = (error: unknown): Error => {
  if (error instanceof Error) return error;
  const nested = error as { message?: unknown; error?: { message?: unknown } };
  if (typeof nested?.message === "string" && nested.message.length > 0) {
    return new Error(nested.message);
  }
  if (typeof nested?.error?.message === "string" && nested.error.message.length > 0) {
    return new Error(nested.error.message);
  }
  return new Error(String(error ?? "Unknown error"));
};

const clampClientId = (value: number): number => {
  if (!Number.isFinite(value)) return 1;
  const normalized = Math.floor(value);
  if (normalized < 1) return 1;
  if (normalized > 2_147_483_647) return 2_147_483_647;
  return normalized;
};

const asEnumValue = <TEnum extends Record<string, string | number>>(
  enumLike: TEnum,
  raw: unknown,
  fallback: TEnum[keyof TEnum]
): TEnum[keyof TEnum] => {
  if (raw === undefined || raw === null) return fallback;
  if (typeof raw === "number" || typeof raw === "string") {
    const direct = enumLike[raw as keyof TEnum];
    if (direct !== undefined) return direct;

    if (typeof raw === "string") {
      const normalized = raw.trim().toUpperCase();
      const byKey = enumLike[normalized as keyof TEnum];
      if (byKey !== undefined) return byKey;

      const byValue = Object.values(enumLike).find(
        (value) => String(value).toUpperCase() === normalized
      );
      if (byValue !== undefined) return byValue as TEnum[keyof TEnum];
    }
  }
  return fallback;
};

const buildClientIdCandidates = (baseClientId: number): number[] => {
  const base = clampClientId(baseClientId);
  return [...new Set([base, base + 1, base + 10, base + 100, base + 1_000].map(clampClientId))];
};

export class Client {
  readonly _emitter = new EventEmitter();
  readonly _protocolBytes = new EventEmitter();

  private readonly apiNext: IBApiNext;
  private readonly rawApi: RawApiLike | null;
  private readonly clientIdCandidates: number[];
  private readonly timeoutMs: number;
  private activeClientId: number;
  private connectPromise: Promise<void> | null = null;
  private rawListenersAttached = false;

  private readonly onRawOrderStatus = (
    orderId: unknown,
    status: unknown,
    filled: unknown,
    remaining: unknown,
    avgFillPrice: unknown,
    permId?: unknown,
    _parentId?: unknown,
    lastFillPrice?: unknown,
    clientId?: unknown,
    whyHeld?: unknown
  ): void => {
    this._emitter.emit("orderStatus", {
      orderId: asNumber(orderId),
      status: String(status ?? "Unknown"),
      filled: asNumber(filled),
      remaining: asNumber(remaining),
      avgFillPrice: asNumber(avgFillPrice),
      lastFillPrice: asNumber(lastFillPrice),
      permId: asNumber(permId),
      clientId: asNumber(clientId),
      whyHeld: String(whyHeld ?? "")
    });
  };

  private readonly onRawAccountValue = (
    key: unknown,
    value: unknown,
    currency: unknown,
    accountName: unknown
  ): void => {
    this._protocolBytes.emit("message_fieldset", [
      6,
      "2",
      String(key ?? ""),
      String(value ?? ""),
      String(currency ?? "BASE"),
      String(accountName ?? "")
    ]);
  };

  private readonly onRawPortfolio = (...args: unknown[]): void => {
    const contract = (args[0] as Record<string, unknown> | undefined) ?? {};
    const position = args[1];
    const marketPrice = args[2];
    const marketValue = args[3];
    const averageCost = args[4];
    const unrealizedPNL = args[5];
    const realizedPNL = args[6];
    const accountName = args[7];
    this._protocolBytes.emit("message_fieldset", [
      7,
      "2",
      asNumber(contract?.conId) ?? 0,
      String(contract?.symbol ?? ""),
      String(contract?.secType ?? ""),
      "",
      "",
      asNumber(position) ?? 0,
      asNumber(marketPrice) ?? 0,
      asNumber(marketValue) ?? 0,
      asNumber(averageCost) ?? 0,
      asNumber(unrealizedPNL) ?? 0,
      asNumber(realizedPNL) ?? 0,
      String(accountName ?? "")
    ]);
  };

  private readonly onRawAccountTime = (timestamp: unknown): void => {
    this._protocolBytes.emit("message_fieldset", [8, "1", String(timestamp ?? "")]);
  };

  private readonly onRawAccountDownloadEnd = (accountName: unknown): void => {
    this._protocolBytes.emit("message_fieldset", [54, "1", String(accountName ?? "")]);
  };

  constructor(connectionParameters: IbClientOptions = {}) {
    const host = connectionParameters.host ?? "127.0.0.1";
    const port = connectionParameters.port ?? 7497;
    const configuredClientId = clampClientId(connectionParameters.clientId ?? 1);
    this.activeClientId = configuredClientId;
    this.clientIdCandidates = buildClientIdCandidates(configuredClientId);
    this.timeoutMs = connectionParameters.timeoutMs ?? 4_000;

    this.apiNext = new IBApiNext({
      host,
      port,
      reconnectInterval: 0,
      maxReqPerSec: 25
    });
    this.rawApi = (this.apiNext as unknown as { api?: RawApiLike }).api ?? null;
    this.attachRawListeners();
  }

  getEffectiveClientId(): number {
    return this.activeClientId;
  }

  private attachRawListeners(): void {
    if (!this.rawApi?.on || this.rawListenersAttached) return;
    this.rawListenersAttached = true;
    this.rawApi.on(EventName.orderStatus, this.onRawOrderStatus);
    this.rawApi.on(EventName.updateAccountValue, this.onRawAccountValue);
    this.rawApi.on(EventName.updatePortfolio, this.onRawPortfolio);
    this.rawApi.on(EventName.updateAccountTime, this.onRawAccountTime);
    this.rawApi.on(EventName.accountDownloadEnd, this.onRawAccountDownloadEnd);
  }

  private detachRawListeners(): void {
    if (!this.rawApi || !this.rawListenersAttached) return;
    const remove =
      this.rawApi.off?.bind(this.rawApi) ??
      this.rawApi.removeListener?.bind(this.rawApi);
    if (!remove) return;
    remove(EventName.orderStatus, this.onRawOrderStatus);
    remove(EventName.updateAccountValue, this.onRawAccountValue);
    remove(EventName.updatePortfolio, this.onRawPortfolio);
    remove(EventName.updateAccountTime, this.onRawAccountTime);
    remove(EventName.accountDownloadEnd, this.onRawAccountDownloadEnd);
    this.rawListenersAttached = false;
  }

  private async withTimeout<T>(promise: Promise<T>, timeoutMs = this.timeoutMs): Promise<T> {
    if (timeoutMs <= 0) return await promise;

    return await new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error("ib-tws-api: timeout"));
      }, timeoutMs);

      promise
        .then((value) => {
          clearTimeout(timer);
          resolve(value);
        })
        .catch((error) => {
          clearTimeout(timer);
          reject(error);
        });
    });
  }

  private async connectWithClientId(clientId: number, timeoutMs: number): Promise<void> {
    try {
      this.apiNext.disconnect();
    } catch {
      // ignore disconnect failures before reconnect attempts
    }

    await new Promise<void>((resolve, reject) => {
      let settled = false;
      let sawConnected = false;
      let stateSubscription: { unsubscribe: () => void } | null = null;
      let stableTimer: Timer | null = null;

      const finish = (error?: Error): void => {
        if (settled) return;
        settled = true;
        clearTimeout(connectTimeout);
        if (stableTimer) clearTimeout(stableTimer);
        stateSubscription?.unsubscribe();
        if (error) {
          try {
            this.apiNext.disconnect();
          } catch {
            // ignore disconnect failures
          }
          reject(error);
        } else {
          resolve();
        }
      };

      const connectTimeout = setTimeout(() => finish(new Error("Failed to connect")), timeoutMs);
      stateSubscription = this.apiNext.connectionState.subscribe({
        next: (state) => {
          if (state === ConnectionState.Connected) {
            sawConnected = true;
            if (stableTimer) clearTimeout(stableTimer);
            // Require a brief stable period to avoid false-positive connects on rejected client IDs.
            stableTimer = setTimeout(() => finish(), 500);
            return;
          }

          if (state === ConnectionState.Disconnected && sawConnected) {
            finish(new Error("Disconnected immediately after connect"));
          }
        },
        error: () => finish(new Error("Failed to connect"))
      });

      try {
        this.apiNext.connect(clientId);
      } catch {
        finish(new Error("Failed to connect"));
      }
    });
  }

  private async ensureConnected(timeoutMs = this.timeoutMs): Promise<void> {
    if (this.apiNext.isConnected) return;
    if (this.connectPromise) {
      await this.connectPromise;
      return;
    }

    const pending = (async () => {
      const candidates = [
        this.activeClientId,
        ...this.clientIdCandidates.filter((candidate) => candidate !== this.activeClientId)
      ];

      let lastError: Error | null = null;
      for (const candidate of candidates) {
        try {
          await this.connectWithClientId(candidate, timeoutMs);
          this.activeClientId = candidate;
          return;
        } catch (error) {
          lastError = toError(error);
        }
      }

      throw lastError ?? new Error("Failed to connect");
    })();

    this.connectPromise = pending;
    try {
      await pending;
    } finally {
      if (this.connectPromise === pending) {
        this.connectPromise = null;
      }
    }
  }

  private async request<T>(run: () => Promise<T>, timeoutMs = this.timeoutMs): Promise<T> {
    await this.ensureConnected(timeoutMs);
    try {
      return await this.withTimeout(run(), timeoutMs);
    } catch (error) {
      throw toError(error);
    }
  }

  private async requestWithClientIdFailover<T>(
    run: () => Promise<T>,
    timeoutMs = this.timeoutMs
  ): Promise<T> {
    try {
      return await this.request(run, timeoutMs);
    } catch (error) {
      const initialError = toError(error);
      if (!initialError.message.toLowerCase().includes("timeout")) {
        throw initialError;
      }

      const fallbackCandidates = this.clientIdCandidates.filter(
        (candidate) => candidate !== this.activeClientId
      );
      let lastError: Error = initialError;
      for (const candidate of fallbackCandidates) {
        try {
          await this.connectWithClientId(candidate, timeoutMs);
          this.activeClientId = candidate;
          return await this.withTimeout(run(), timeoutMs);
        } catch (fallbackError) {
          lastError = toError(fallbackError);
        }
      }
      throw lastError;
    }
  }

  disconnect(): void {
    this.detachRawListeners();
    try {
      this.rawApi?.reqAccountUpdates?.(false, "");
    } catch {
      // ignore account stream shutdown failures
    }
    try {
      this.apiNext.disconnect();
    } catch {
      // ignore disconnect failures
    }
  }

  async getCurrentTime(): Promise<number> {
    return await this.requestWithClientIdFailover(async () => {
      await this.ensureConnected();

      if (!this.rawApi?.on || !this.rawApi.reqCurrentTime) {
        return await this.apiNext.getCurrentTime();
      }

      return await this.withTimeout(
        new Promise<number>((resolve, reject) => {
          const remove =
            this.rawApi?.off?.bind(this.rawApi) ??
            this.rawApi?.removeListener?.bind(this.rawApi);
          let settled = false;

          const cleanup = (): void => {
            if (!remove) return;
            remove(EventName.currentTime, onCurrentTime);
          };

          const onCurrentTime = (value: unknown): void => {
            if (settled) return;
            settled = true;
            cleanup();
            const parsed = asNumber(value);
            if (parsed === undefined) {
              reject(new Error("Failed to connect"));
              return;
            }
            resolve(parsed);
          };

          this.rawApi?.on(EventName.currentTime, onCurrentTime);
          try {
            this.rawApi?.reqCurrentTime?.();
          } catch (error) {
            settled = true;
            cleanup();
            reject(toError(error));
          }
        }),
        this.timeoutMs
      );
    });
  }

  async reqMarketDataType(marketDataType: number): Promise<void> {
    await this.ensureConnected();
    this.apiNext.setMarketDataType(marketDataType as MarketDataType);
  }

  async getMarketDataSnapshot(p: {
    contract: IbContract;
    genericTickList?: string;
    regulatorySnapshot?: boolean;
  }): Promise<Record<string, unknown>> {
    // IBKR rejects generic tick subscriptions for snapshot requests on many sessions.
    // Use plain snapshot ticks unless a caller explicitly asks for a supported list.
    const genericTickList =
      typeof p.genericTickList === "string" ? p.genericTickList : "";

    const ticks = await this.request(
      () =>
        this.apiNext.getMarketDataSnapshot(
          p.contract as StoqeyContract,
          genericTickList,
          Boolean(p.regulatorySnapshot)
        ),
      Math.max(this.timeoutMs, 6_000)
    );

    return {
      bid: tickValue(ticks, IBApiTickType.BID),
      ask: tickValue(ticks, IBApiTickType.ASK),
      last: tickValue(ticks, IBApiTickType.LAST),
      close: tickValue(ticks, IBApiTickType.CLOSE),
      volume: tickValue(ticks, IBApiTickType.VOLUME),
      delayedBid: tickValue(ticks, IBApiTickType.DELAYED_BID),
      delayedAsk: tickValue(ticks, IBApiTickType.DELAYED_ASK),
      delayedLast: tickValue(ticks, IBApiTickType.DELAYED_LAST),
      delayedClose: tickValue(ticks, IBApiTickType.DELAYED_CLOSE),
      delayedVolume: tickValue(ticks, IBApiTickType.DELAYED_VOLUME),
      markPrice: tickValue(ticks, IBApiTickType.MARK_PRICE),
      rtVolume: tickValue(ticks, IBApiTickType.RT_TRD_VOLUME),
      optionBid: tickValue(ticks, IBApiTickType.BID_OPTION),
      optionAsk: tickValue(ticks, IBApiTickType.ASK_OPTION),
      optionLast: tickValue(ticks, IBApiTickType.LAST_OPTION),
      optionImpliedVol: tickValue(ticks, IBApiTickType.OPTION_IMPLIED_VOL),
      optionHistoricalVol: tickValue(ticks, IBApiTickType.OPTION_HISTORICAL_VOL),
      optionCallOpenInterest: tickValue(ticks, IBApiTickType.OPTION_CALL_OPEN_INTEREST),
      optionPutOpenInterest: tickValue(ticks, IBApiTickType.OPTION_PUT_OPEN_INTEREST),
      optionOpenInterest: tickValue(ticks, IBApiTickType.OPEN_INTEREST),
      optionCallVolume: tickValue(ticks, IBApiTickType.OPTION_CALL_VOLUME),
      optionPutVolume: tickValue(ticks, IBApiTickType.OPTION_PUT_VOLUME),
      avgOptionVolume: tickValue(ticks, IBApiTickType.AVG_OPT_VOLUME)
    };
  }

  async getHistoricalData(p: {
    contract: IbContract;
    endDateTime: string;
    duration: string;
    barSizeSetting: string;
    whatToShow: string;
    useRth: number;
    formatDate: number;
  }): Promise<{
    dateStart?: string;
    dateEnd?: string;
    bars?: Array<{
      date?: string;
      close?: number;
      open?: number;
      high?: number;
      low?: number;
      volume?: number;
    }>;
  }> {
    const bars = await this.request(
      () =>
        this.apiNext.getHistoricalData(
          p.contract as StoqeyContract,
          p.endDateTime === "" ? undefined : p.endDateTime,
          p.duration,
          p.barSizeSetting as never,
          p.whatToShow as never,
          p.useRth,
          p.formatDate
        ),
      Math.max(this.timeoutMs, 10_000)
    );

    return {
      bars: bars.map((bar) => ({
        date: bar.time,
        close: bar.close,
        open: bar.open,
        high: bar.high,
        low: bar.low,
        volume: bar.volume
      }))
    };
  }

  async getContractDetails(contract: IbContract): Promise<
    Array<{
      contract?: {
        conId?: number;
      };
    }>
  > {
    const rows = await this.request(() => this.apiNext.getContractDetails(contract as StoqeyContract));
    return rows.map((row) => ({
      contract: {
        conId: asNumber(row.contract?.conId)
      }
    }));
  }

  async getSecDefOptParams(p: {
    contract: IbContract;
    futFopExchange?: string;
    exchange?: string;
  }): Promise<
    | Array<{
        exchange?: string;
        tradingClass?: string;
        expirations?: string[];
        strikes?: number[];
      }>
    | {
        exchange?: string;
        tradingClass?: string;
        expirations?: string[];
        strikes?: number[];
      }
  > {
    const symbol = String(p.contract.symbol ?? "").trim().toUpperCase();
    const conId = asNumber(p.contract.conId) ?? 0;
    const secTypeRaw = String(p.contract.secType ?? SecType.STK).toUpperCase();
    const secType = (Object.values(SecType) as string[]).includes(secTypeRaw)
      ? (secTypeRaw as SecType)
      : SecType.STK;
    if (!symbol || conId <= 0) {
      throw new Error("Invalid underlying contract for reqSecDefOptParams.");
    }

    const rows = await this.request(
      () => this.apiNext.getSecDefOptParams(symbol, p.futFopExchange ?? "", secType, conId),
      Math.max(this.timeoutMs, 8_000)
    );

    const mapped = rows.map((row) => ({
      exchange: row.exchange,
      tradingClass: row.tradingClass,
      expirations: [...(row.expirations ?? [])],
      strikes: [...(row.strikes ?? [])]
    }));

    return mapped;
  }

  async placeOrder(p: { contract: IbContract; order: IbOrder }): Promise<number> {
    return await this.request(
      () => this.apiNext.placeNewOrder(p.contract as StoqeyContract, p.order as StoqeyOrder),
      Math.max(this.timeoutMs, 8_000)
    );
  }

  async cancelOrder(orderId: number): Promise<{ code?: number; message?: string }> {
    await this.request(async () => {
      this.apiNext.cancelOrder(orderId);
      return true;
    });
    return {};
  }

  async reqAccountUpdates(p: { subscribe: boolean; accountCode: string }): Promise<void> {
    await this.ensureConnected();
    if (!this.rawApi?.reqAccountUpdates) {
      throw new Error("It does not support account updates request.");
    }
    this.rawApi.reqAccountUpdates.call(this.rawApi, Boolean(p.subscribe), p.accountCode ?? "");
  }

  async getPositions(): Promise<
    Record<
      string,
      {
        contract?: Record<string, unknown>;
        position?: number;
        avgCost?: number;
      }
    >
  > {
    const allPositions = await this.request(
      async () => {
        return await new Promise<ReadonlyMap<string, Position[]>>((resolve, reject) => {
          let settled = false;
          let subscription: { unsubscribe: () => void } | null = null;

          subscription = this.apiNext.getPositions().subscribe({
            next: (update) => {
              if (settled) return;
              settled = true;
              subscription?.unsubscribe();
              resolve((update?.all as ReadonlyMap<string, Position[]>) ?? new Map());
            },
            error: (error) => {
              if (settled) return;
              settled = true;
              subscription?.unsubscribe();
              reject(error);
            }
          });
        });
      },
      Math.max(this.timeoutMs, 8_000)
    );

    const result: Record<
      string,
      {
        contract?: Record<string, unknown>;
        position?: number;
        avgCost?: number;
      }
    > = {};

    for (const [account, rows] of allPositions.entries()) {
      rows.forEach((row, index) => {
        const conId = asNumber(row.contract?.conId) ?? 0;
        result[`${account}:${conId}:${index}`] = {
          contract: row.contract as Record<string, unknown>,
          position: asNumber(row.pos) ?? 0,
          avgCost: asNumber(row.avgCost)
        };
      });
    }

    return result;
  }

  async getOpenOrders(): Promise<
    Array<{
      contract?: Record<string, unknown>;
      order?: Record<string, unknown>;
      orderState?: Record<string, unknown>;
    }>
  > {
    return await this.getAllOpenOrders();
  }

  async getAllOpenOrders(): Promise<
    Array<{
      contract?: Record<string, unknown>;
      order?: Record<string, unknown>;
      orderState?: Record<string, unknown>;
    }>
  > {
    const rows = await this.request(
      () => this.apiNext.getAllOpenOrders(),
      Math.max(this.timeoutMs, 8_000)
    );

    return rows.map((row) => this.mapOpenOrderRow(row));
  }

  async getMarketScanner(p: {
    numberOfRows?: number;
    instrument?: IbScannerInstrument;
    locationCode?: IbScannerLocation;
    scanCode?: IbScannerCode;
    abovePrice?: number;
    belowPrice?: number;
    aboveVolume?: number;
    stockTypeFilter?: string;
  }): Promise<IbScannerRow[]> {
    const numberOfRows = Math.max(1, Math.min(50, Math.round(p.numberOfRows ?? 15)));
    const instrument = asEnumValue(Instrument, p.instrument, Instrument.STK);
    const locationCode = asEnumValue(LocationCode, p.locationCode, LocationCode.STK_US_MAJOR);
    const scanCode = asEnumValue(ScanCode, p.scanCode, ScanCode.MOST_ACTIVE);

    return await this.request(
      async () => {
        return await new Promise<IbScannerRow[]>((resolve, reject) => {
          let settled = false;
          let latestRows: IbScannerRow[] = [];
          const scanner = this.apiNext.getMarketScanner({
            numberOfRows,
            instrument,
            locationCode,
            scanCode,
            abovePrice: p.abovePrice,
            belowPrice: p.belowPrice,
            aboveVolume: p.aboveVolume,
            stockTypeFilter: p.stockTypeFilter
          });

          const subscription = scanner.subscribe({
            next: (update) => {
              const rows = update?.all;
              if (!(rows instanceof Map)) return;
              const nextRows: IbScannerRow[] = [];
              for (const row of rows.values()) {
                const contract = row.contract?.contract ?? {};
                const symbol = String(contract.symbol ?? "").trim().toUpperCase();
                if (symbol.length === 0) continue;
                nextRows.push({
                  rank: Number.isFinite(row.rank) ? row.rank : 0,
                  symbol,
                  secType: String(contract.secType ?? "").trim().toUpperCase() || undefined,
                  exchange: String(contract.exchange ?? "").trim().toUpperCase() || undefined,
                  currency: String(contract.currency ?? "").trim().toUpperCase() || undefined,
                  distance: row.distance,
                  benchmark: row.benchmark,
                  projection: row.projection
                });
              }
              latestRows = nextRows;
            },
            error: (error) => {
              if (settled) return;
              settled = true;
              subscription.unsubscribe();
              reject(toError(error));
            },
            complete: () => {
              if (settled) return;
              settled = true;
              subscription.unsubscribe();
              resolve(latestRows);
            }
          });
        });
      },
      Math.max(this.timeoutMs, 10_000)
    );
  }

  private mapOpenOrderRow(row: OpenOrder): {
    contract?: Record<string, unknown>;
    order?: Record<string, unknown>;
    orderState?: Record<string, unknown>;
  } {
    return {
      contract: row.contract as Record<string, unknown>,
      order: {
        ...(row.order as Record<string, unknown>),
        orderId: row.orderId
      },
      orderState: {
        ...(row.orderState as Record<string, unknown>),
        status: row.orderStatus?.status ?? row.orderState.status,
        filled: asNumber(row.orderStatus?.filled),
        remaining: asNumber(row.orderStatus?.remaining),
        avgFillPrice: asNumber(row.orderStatus?.avgFillPrice),
        lastFillPrice: asNumber(row.orderStatus?.lastFillPrice),
        whyHeld: row.orderStatus?.whyHeld
      }
    };
  }
}

export class Contract {
  static stock(symbolOrData: string | Record<string, unknown>): IbContract {
    if (typeof symbolOrData === "string") {
      return {
        secType: SecType.STK,
        symbol: symbolOrData.toUpperCase(),
        exchange: "SMART",
        currency: "USD"
      };
    }

    const symbol = String(symbolOrData.symbol ?? "").trim().toUpperCase();
    return {
      secType: SecType.STK,
      exchange: "SMART",
      currency: "USD",
      ...symbolOrData,
      ...(symbol.length > 0 ? { symbol } : {})
    };
  }

  static option(data: Record<string, unknown>): IbContract {
    const rawRight = String(data.right ?? "").trim().toUpperCase();
    const right = rawRight === "PUT" || rawRight === "P" ? "P" : "C";
    return {
      secType: SecType.OPT,
      exchange: "SMART",
      currency: "USD",
      multiplier: 100,
      ...data,
      right
    };
  }

  static forex(pairOrData: string | Record<string, unknown>): IbContract {
    if (typeof pairOrData === "string") {
      const normalized = pairOrData.toUpperCase().replace("/", "");
      return {
        secType: SecType.CASH,
        symbol: normalized.slice(0, 3),
        currency: normalized.slice(3, 6),
        exchange: "IDEALPRO"
      };
    }
    return {
      secType: SecType.CASH,
      exchange: "IDEALPRO",
      ...pairOrData
    };
  }
}

export class Order {
  static limit(data: Record<string, unknown>): IbOrder {
    return {
      orderType: "LMT",
      ...data
    };
  }

  static market(data: Record<string, unknown>): IbOrder {
    return {
      orderType: "MKT",
      ...data
    };
  }

  static stop(data: Record<string, unknown>): IbOrder {
    return {
      orderType: "STP",
      ...data
    };
  }
}
