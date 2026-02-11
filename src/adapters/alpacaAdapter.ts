import { settings } from "../core/config";
import { logger } from "../core/logger";
import type { DailyBar } from "../types/models";
import { fetchWithApiLog } from "../utils/fetchWithApiLog";
import {
  DEFAULT_IBKR_SCANNER_CODE,
  type IbkrScannerCodeSetting
} from "../constants/scanner";

export interface AlpacaScannerResult {
  symbols: string[];
  source: "alpaca_screener" | "disabled" | "unavailable";
  note?: string;
}

export interface AlpacaQuoteResult {
  symbol: string;
  last: number;
  bid: number;
  ask: number;
  volume: number;
  source: "alpaca_quote" | "disabled" | "unavailable";
  note?: string;
}

export interface AlpacaHistoricalResult {
  symbol: string;
  closes: number[];
  source: "alpaca_historical" | "disabled" | "unavailable";
  note?: string;
}

export interface AlpacaHistoricalBarsResult {
  symbol: string;
  bars: DailyBar[];
  source: "alpaca_historical" | "disabled" | "unavailable";
  note?: string;
}

export interface AlpacaNewsArticle {
  title: string;
  source: string;
  url: string;
  publishedAt: string | null;
  summary: string;
  overallSentimentScore: number | null;
  relevanceScore: number | null;
}

export interface AlpacaNewsSentimentSnapshot {
  sentiment: number | null;
  articles: AlpacaNewsArticle[];
  source: "alpaca_news_sentiment" | "disabled" | "unavailable";
  note?: string;
}

export interface AlpacaOptionContract {
  symbol: string;
  expiration: string;
  strike: number;
  right: "CALL" | "PUT";
  bid: number;
  ask: number;
  last: number;
  volume: number;
  openInterest: number;
  impliedVol: number;
  delta: number;
  gamma: number;
  quoteSource: "alpaca_option_quote";
}

export interface AlpacaOptionChainResult {
  symbol: string;
  contracts: AlpacaOptionContract[];
  source: "alpaca_option_chain" | "disabled" | "unavailable";
  note?: string;
}

export class AlpacaAdapter {
  private readonly baseUrl = settings.alpacaDataBaseUrl || "https://data.alpaca.markets";
  private readonly tickerRegex = /^[A-Z][A-Z0-9.\-]{0,14}$/;
  private readonly optionSymbolRegex = /^[A-Z]{1,8}\d{6}[CP]\d{8}$/;
  private readonly positiveSentimentTerms = [
    "beat",
    "beats",
    "bullish",
    "upgrade",
    "upgrades",
    "record",
    "strong",
    "growth",
    "outperform",
    "rally",
    "gain",
    "gains",
    "surge",
    "optimistic",
    "momentum",
    "rebound",
    "approval",
    "profitable",
    "profit"
  ];
  private readonly negativeSentimentTerms = [
    "miss",
    "misses",
    "bearish",
    "downgrade",
    "downgrades",
    "drop",
    "plunge",
    "weak",
    "decline",
    "declines",
    "loss",
    "losses",
    "lawsuit",
    "probe",
    "investigation",
    "cut",
    "cuts",
    "warning",
    "recession",
    "default",
    "bankrupt",
    "selloff",
    "slump"
  ];

  private isTestRuntime(): boolean {
    return settings.appEnv === "test" || process.env.NODE_ENV === "test" || Boolean(process.env.BUN_TEST);
  }

  private normalizeTicker(value: unknown): string | null {
    if (typeof value !== "string") return null;
    const symbol = value.trim().toUpperCase();
    if (!this.tickerRegex.test(symbol)) return null;
    return symbol;
  }

  private normalizeOptionSymbol(value: unknown): string | null {
    if (typeof value !== "string") return null;
    const normalized = value.trim().toUpperCase().replace(/\s+/g, "");
    if (!this.optionSymbolRegex.test(normalized)) return null;
    return normalized;
  }

  private parseOccOptionSymbol(
    value: string
  ): { underlying: string; expiration: string; strike: number; right: "CALL" | "PUT" } | null {
    const normalized = this.normalizeOptionSymbol(value);
    if (!normalized) return null;

    const match = normalized.match(/^([A-Z]{1,8})(\d{2})(\d{2})(\d{2})([CP])(\d{8})$/);
    if (!match) return null;
    const year = 2000 + Number(match[2]);
    const month = Number(match[3]);
    const day = Number(match[4]);
    if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) return null;
    if (month < 1 || month > 12 || day < 1 || day > 31) return null;

    const strike = Number(match[6]) / 1000;
    if (!Number.isFinite(strike) || strike <= 0) return null;

    const expiration = `${year.toString().padStart(4, "0")}-${month
      .toString()
      .padStart(2, "0")}-${day.toString().padStart(2, "0")}`;
    return {
      underlying: match[1],
      expiration,
      strike,
      right: match[5] === "C" ? "CALL" : "PUT"
    };
  }

  private normalizeOptionExpiration(value: unknown): string | null {
    if (typeof value !== "string") return null;
    const trimmed = value.trim();
    if (!trimmed) return null;

    const direct = trimmed.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (direct) return `${direct[1]}-${direct[2]}-${direct[3]}`;

    const compact = trimmed.match(/^(\d{4})(\d{2})(\d{2})$/);
    if (compact) return `${compact[1]}-${compact[2]}-${compact[3]}`;

    return null;
  }

  private normalizeOptionRight(value: unknown): "CALL" | "PUT" | null {
    if (typeof value !== "string") return null;
    const normalized = value.trim().toUpperCase();
    if (normalized === "CALL" || normalized === "C") return "CALL";
    if (normalized === "PUT" || normalized === "P") return "PUT";
    return null;
  }

  private asRecord(value: unknown): Record<string, unknown> | null {
    if (!value || typeof value !== "object") return null;
    return value as Record<string, unknown>;
  }

  private extractOptionSnapshotRows(
    payload: unknown
  ): Array<{ optionSymbol: string | null; row: Record<string, unknown> }> {
    const result: Array<{ optionSymbol: string | null; row: Record<string, unknown> }> = [];
    const addArrayRows = (rows: unknown[]): void => {
      for (const row of rows) {
        const record = this.asRecord(row);
        if (!record) continue;
        result.push({
          optionSymbol:
            this.normalizeOptionSymbol(record.symbol) ??
            this.normalizeOptionSymbol(record.option_symbol) ??
            this.normalizeOptionSymbol(record.optionSymbol),
          row: record
        });
      }
    };
    const addObjectRows = (record: Record<string, unknown>): void => {
      for (const [key, value] of Object.entries(record)) {
        const row = this.asRecord(value);
        if (!row) continue;
        result.push({
          optionSymbol: this.normalizeOptionSymbol(key) ?? this.normalizeOptionSymbol(row.symbol),
          row
        });
      }
    };

    const root = this.asRecord(payload);
    if (!root) return result;

    const directSnapshots = root.snapshots;
    if (Array.isArray(directSnapshots)) addArrayRows(directSnapshots);
    else if (directSnapshots && typeof directSnapshots === "object") {
      addObjectRows(directSnapshots as Record<string, unknown>);
    }

    const optionsSnapshots = root.option_snapshots;
    if (Array.isArray(optionsSnapshots)) addArrayRows(optionsSnapshots);
    else if (optionsSnapshots && typeof optionsSnapshots === "object") {
      addObjectRows(optionsSnapshots as Record<string, unknown>);
    }

    const data = root.data;
    if (Array.isArray(data)) addArrayRows(data);
    else if (data && typeof data === "object") {
      const dataRecord = data as Record<string, unknown>;
      const nestedSnapshots = dataRecord.snapshots;
      if (Array.isArray(nestedSnapshots)) addArrayRows(nestedSnapshots);
      else if (nestedSnapshots && typeof nestedSnapshots === "object") {
        addObjectRows(nestedSnapshots as Record<string, unknown>);
      }
      if (result.length === 0) {
        addObjectRows(dataRecord);
      }
    }

    if (Array.isArray(root.results)) addArrayRows(root.results);
    if (Array.isArray(root.options)) addArrayRows(root.options);
    if (result.length === 0) {
      addObjectRows(root);
    }

    return result;
  }

  private daysToExpiration(expiration: string): number | null {
    const parsed = Date.parse(`${expiration}T00:00:00Z`);
    if (!Number.isFinite(parsed)) return null;
    const dte = Math.ceil((parsed - Date.now()) / 86_400_000);
    return Number.isFinite(dte) ? dte : null;
  }

  private dteDistanceFromRange(dte: number, dteMin: number, dteMax: number): number {
    if (dte < dteMin) return dteMin - dte;
    if (dte > dteMax) return dte - dteMax;
    return 0;
  }

  private summarizeDteWindow(contracts: AlpacaOptionContract[]): string {
    const dtes = contracts
      .map((entry) => this.daysToExpiration(entry.expiration))
      .filter((value): value is number => value !== null && Number.isFinite(value));
    if (dtes.length === 0) return "unknown DTE";
    const min = Math.min(...dtes);
    const max = Math.max(...dtes);
    if (min === max) return `${min} DTE`;
    return `${min}-${max} DTE`;
  }

  private parseOptionContractRow(
    symbol: string,
    row: Record<string, unknown>,
    optionSymbolHint: string | null,
    underlyingPrice?: number
  ): AlpacaOptionContract | null {
    const normalizedSymbol = this.normalizeTicker(symbol) ?? symbol.trim().toUpperCase();
    const contractSymbol =
      this.normalizeOptionSymbol(optionSymbolHint) ??
      this.normalizeOptionSymbol(row.symbol) ??
      this.normalizeOptionSymbol(row.option_symbol) ??
      this.normalizeOptionSymbol(row.optionSymbol);
    const occ = contractSymbol ? this.parseOccOptionSymbol(contractSymbol) : null;

    const expiration =
      this.normalizeOptionExpiration(row.expiration_date ?? row.expiration ?? row.expiry) ??
      occ?.expiration ??
      null;
    const strike =
      this.pickNumber(row, ["strike_price", "strike", "strikePrice"]) ??
      occ?.strike ??
      null;
    const right =
      this.normalizeOptionRight(row.type ?? row.option_type ?? row.right ?? row.side) ??
      occ?.right ??
      null;

    if (!expiration || !strike || !right || strike <= 0) return null;

    const latestQuote =
      this.asRecord(row.latest_quote) ??
      this.asRecord(row.latestQuote) ??
      this.asRecord(row.quote) ??
      this.asRecord(row.latest_quote_data);
    const latestTrade =
      this.asRecord(row.latest_trade) ??
      this.asRecord(row.latestTrade) ??
      this.asRecord(row.trade);
    const greeks = this.asRecord(row.greeks);

    const bid =
      (latestQuote
        ? this.pickNumber(latestQuote, ["bp", "bid_price", "bidPrice", "bid"])
        : null) ?? this.pickNumber(row, ["bid_price", "bidPrice", "bid", "bp"]) ?? 0;
    const ask =
      (latestQuote
        ? this.pickNumber(latestQuote, ["ap", "ask_price", "askPrice", "ask"])
        : null) ?? this.pickNumber(row, ["ask_price", "askPrice", "ask", "ap"]) ?? 0;
    const last =
      (latestTrade
        ? this.pickNumber(latestTrade, ["p", "price", "last_price", "lastPrice", "last"])
        : null) ??
      this.pickNumber(row, ["last_price", "lastPrice", "last", "lp", "price"]) ??
      (bid > 0 && ask > 0 ? (bid + ask) / 2 : bid > 0 ? bid : ask);
    const volume =
      (latestTrade
        ? this.pickNumber(latestTrade, ["s", "size", "volume"])
        : null) ??
      this.pickNumber(row, ["volume", "trade_count", "trades"]) ??
      0;
    const openInterest = this.pickNumber(row, ["open_interest", "openInterest", "oi"]) ?? 0;
    const impliedVol = this.pickNumber(row, ["implied_volatility", "impliedVolatility", "iv"]) ?? 0.2;
    const delta =
      (greeks ? this.pickNumber(greeks, ["delta"]) : null) ??
      this.deltaFallback(right, strike, Number(underlyingPrice ?? 0));
    const gamma = (greeks ? this.pickNumber(greeks, ["gamma"]) : null) ?? 0.02;

    const safeBid = Number.isFinite(bid) && bid > 0 ? bid : 0;
    const safeAsk = Number.isFinite(ask) && ask > 0 ? Math.max(ask, safeBid) : safeBid;
    const safeLast = Number.isFinite(last) && last > 0 ? last : safeAsk > 0 ? safeAsk : safeBid;
    if (safeLast <= 0 && safeBid <= 0 && safeAsk <= 0) return null;

    return {
      symbol: normalizedSymbol,
      expiration,
      strike: Number(strike.toFixed(4)),
      right,
      bid: Number(safeBid.toFixed(4)),
      ask: Number(safeAsk.toFixed(4)),
      last: Number(safeLast.toFixed(4)),
      volume: Math.max(0, Math.round(volume)),
      openInterest: Math.max(0, Math.round(openInterest)),
      impliedVol: Number(Math.max(0.01, Math.min(6, impliedVol)).toFixed(6)),
      delta: Number(Math.max(-1, Math.min(1, delta)).toFixed(6)),
      gamma: Number(Math.max(0, Math.min(5, gamma)).toFixed(6)),
      quoteSource: "alpaca_option_quote"
    };
  }

  private deltaFallback(right: "CALL" | "PUT", strike: number, underlyingPrice: number): number {
    if (!Number.isFinite(underlyingPrice) || underlyingPrice <= 0) {
      return right === "CALL" ? 0.5 : -0.5;
    }
    const moneyness = (underlyingPrice - strike) / Math.max(underlyingPrice, 1e-6);
    const base = Math.max(0.05, Math.min(0.95, 0.5 + moneyness * 4));
    return right === "CALL" ? base : -base;
  }

  async getOptionChain(
    symbol: string,
    options?: {
      dteMin?: number;
      dteMax?: number;
      maxContracts?: number;
      underlyingPrice?: number;
    }
  ): Promise<AlpacaOptionChainResult> {
    const normalizedSymbol = this.normalizeTicker(symbol) ?? symbol.trim().toUpperCase();
    if (!this.isConfigured()) {
      return {
        symbol: normalizedSymbol,
        contracts: [],
        source: "disabled",
        note: "ALPACA_API_KEY and/or ALPACA_API_SECRET not configured."
      };
    }

    const dteMin = Math.max(1, Math.min(180, Math.round(options?.dteMin ?? settings.dteMin)));
    const dteMax = Math.max(dteMin, Math.min(365, Math.round(options?.dteMax ?? settings.dteMax)));
    const maxContracts = Math.max(10, Math.min(240, Math.round(options?.maxContracts ?? 90)));
    const notes: string[] = [];
    const noteSet = new Set<string>();
    const addNote = (note: string): void => {
      const normalized = note.trim();
      if (!normalized || noteSet.has(normalized)) return;
      noteSet.add(normalized);
      notes.push(normalized);
    };
    const requestPaths = [
      `/v1beta1/options/snapshots/${encodeURIComponent(normalizedSymbol)}?feed=indicative&limit=${maxContracts}`,
      `/v1beta1/options/snapshots/${encodeURIComponent(normalizedSymbol)}?limit=${maxContracts}`
    ];

    const requestPayloadBase = {
      symbol: normalizedSymbol,
      dteMin,
      dteMax,
      maxContracts
    };

    for (const path of requestPaths) {
      const url = `${this.baseUrl.replace(/\/$/, "")}${path}`;
      try {
        const response = await fetchWithApiLog(
          url,
          {
            headers: this.headers()
          },
          {
            provider: "alpaca",
            endpoint: path,
            reason: `Fetch options chain for ${normalizedSymbol}`,
            requestPayload: requestPayloadBase
          }
        );

        if (!response.ok) {
          const detail = (await response.text()).slice(0, 220).replace(/\s+/g, " ").trim();
          addNote(`snapshot ${response.status}${detail ? `: ${detail}` : ""}`);
          continue;
        }

        const payload = await response.json();
        const rows = this.extractOptionSnapshotRows(payload);
        if (rows.length === 0) {
          addNote("Alpaca option snapshots returned no contract rows.");
          continue;
        }

        const contracts = rows
          .map((entry) =>
            this.parseOptionContractRow(
              normalizedSymbol,
              entry.row,
              entry.optionSymbol,
              options?.underlyingPrice
            )
          )
          .filter((entry): entry is AlpacaOptionContract => entry !== null);

        if (contracts.length === 0) {
          addNote("Alpaca option snapshots returned no parseable option contracts.");
          continue;
        }

        const deduped = new Map<string, AlpacaOptionContract>();
        for (const contract of contracts) {
          const key = `${contract.expiration}|${contract.right}|${contract.strike.toFixed(4)}`;
          if (!deduped.has(key)) deduped.set(key, contract);
        }
        const dedupedContracts = [...deduped.values()];
        const contractsInRange = dedupedContracts.filter((entry) => {
          const dte = this.daysToExpiration(entry.expiration);
          if (dte === null) return false;
          return dte >= dteMin && dte <= dteMax;
        });
        const usingNearestDteFallback = contractsInRange.length === 0;
        const filteredContracts = usingNearestDteFallback
          ? dedupedContracts
              .filter((entry) => {
                const dte = this.daysToExpiration(entry.expiration);
                return dte !== null && dte >= 0;
              })
              .sort((left, right) => {
                const leftDte = this.daysToExpiration(left.expiration);
                const rightDte = this.daysToExpiration(right.expiration);
                if (leftDte === null && rightDte === null) return 0;
                if (leftDte === null) return 1;
                if (rightDte === null) return -1;
                const leftDistance = this.dteDistanceFromRange(leftDte, dteMin, dteMax);
                const rightDistance = this.dteDistanceFromRange(rightDte, dteMin, dteMax);
                if (leftDistance !== rightDistance) return leftDistance - rightDistance;
                if (leftDte !== rightDte) return leftDte - rightDte;
                return right.openInterest - left.openInterest;
              })
          : contractsInRange;

        if (filteredContracts.length === 0) {
          addNote("Alpaca option snapshots returned no contracts in requested or nearby DTE range.");
          continue;
        }

        const ranked = [...filteredContracts].sort((left, right) => {
          const leftDte = this.daysToExpiration(left.expiration) ?? 999;
          const rightDte = this.daysToExpiration(right.expiration) ?? 999;
          if (leftDte !== rightDte) return leftDte - rightDte;

          if (Number.isFinite(options?.underlyingPrice) && (options?.underlyingPrice ?? 0) > 0) {
            const anchor = Number(options?.underlyingPrice ?? 0);
            const leftDistance = Math.abs(left.strike - anchor);
            const rightDistance = Math.abs(right.strike - anchor);
            if (leftDistance !== rightDistance) return leftDistance - rightDistance;
          }

          return right.openInterest - left.openInterest;
        });

        return {
          symbol: normalizedSymbol,
          contracts: ranked.slice(0, maxContracts),
          source: "alpaca_option_chain",
          note: (() => {
            if (usingNearestDteFallback) {
              addNote(
                `No contracts in requested ${dteMin}-${dteMax} DTE range; using nearest available contracts (${this.summarizeDteWindow(
                  ranked
                )}).`
              );
            }
            return notes.length > 0 ? notes.join(" | ").slice(0, 420) : undefined;
          })()
        };
      } catch (error) {
        addNote((error as Error).message);
      }
    }

    return {
      symbol: normalizedSymbol,
      contracts: [],
      source: "unavailable",
      note: notes.length > 0 ? notes.join(" | ").slice(0, 420) : "Alpaca option chain unavailable."
    };
  }

  private collectSymbols(input: unknown, output: string[], maxCount: number): void {
    if (output.length >= maxCount) return;

    if (Array.isArray(input)) {
      for (const item of input) {
        this.collectSymbols(item, output, maxCount);
        if (output.length >= maxCount) return;
      }
      return;
    }

    if (!input || typeof input !== "object") return;
    const record = input as Record<string, unknown>;
    const directSymbol = this.normalizeTicker(record.symbol) ?? this.normalizeTicker(record.ticker);
    if (directSymbol) {
      output.push(directSymbol);
      if (output.length >= maxCount) return;
    }

    for (const value of Object.values(record)) {
      if (value && typeof value === "object") {
        this.collectSymbols(value, output, maxCount);
        if (output.length >= maxCount) return;
      }
    }
  }

  private collectMoversSideSymbols(
    payload: unknown,
    side: "gainers" | "losers",
    output: string[],
    maxCount: number
  ): void {
    if (!payload || typeof payload !== "object") return;
    const record = payload as Record<string, unknown>;
    const direct = record[side];
    if (direct !== undefined) {
      this.collectSymbols(direct, output, maxCount);
      return;
    }
    this.collectSymbols(payload, output, maxCount);
  }

  private headers(): HeadersInit {
    return {
      "APCA-API-KEY-ID": settings.alpacaApiKey,
      "APCA-API-SECRET-KEY": settings.alpacaApiSecret,
      Accept: "application/json"
    };
  }

  private isConfigured(): boolean {
    if (this.isTestRuntime()) return false;
    return settings.alpacaApiKey.trim().length > 0 && settings.alpacaApiSecret.trim().length > 0;
  }

  private asFiniteNumber(value: unknown): number | null {
    const parsed = typeof value === "number" ? value : typeof value === "string" ? Number(value) : Number.NaN;
    return Number.isFinite(parsed) ? parsed : null;
  }

  private pickNumber(record: Record<string, unknown>, keys: string[]): number | null {
    for (const key of keys) {
      const candidate = this.asFiniteNumber(record[key]);
      if (candidate !== null) return candidate;
    }
    return null;
  }

  private normalizeBarClose(value: unknown): number | null {
    const close = this.asFiniteNumber(value);
    if (close === null || close <= 0) return null;
    return close;
  }

  private normalizePublishedAt(value: unknown): string | null {
    if (typeof value !== "string") return null;
    const trimmed = value.trim();
    if (!trimmed) return null;
    const parsed = Date.parse(trimmed);
    if (!Number.isFinite(parsed)) return null;
    return new Date(parsed).toISOString();
  }

  private scoreTextSentiment(text: string): number | null {
    const normalized = text.toLowerCase().replace(/[^a-z0-9\s]/g, " ");
    const tokens = normalized.split(/\s+/).filter(Boolean);
    if (tokens.length === 0) return null;

    let positive = 0;
    let negative = 0;
    for (const token of tokens) {
      if (this.positiveSentimentTerms.includes(token)) positive += 1;
      if (this.negativeSentimentTerms.includes(token)) negative += 1;
    }

    if (positive === 0 && negative === 0) return 0;
    const score = (positive - negative) / (positive + negative + 2);
    return Math.max(-1, Math.min(1, score));
  }

  private extractBarsPayload(
    payload: Record<string, unknown>,
    normalizedSymbol: string
  ): unknown[] {
    const directBars = payload.bars;
    if (Array.isArray(directBars)) return directBars;

    if (directBars && typeof directBars === "object") {
      const barsRecord = directBars as Record<string, unknown>;
      const keyedBars =
        barsRecord[normalizedSymbol] ??
        barsRecord[normalizedSymbol.toUpperCase()] ??
        barsRecord[normalizedSymbol.toLowerCase()];
      if (Array.isArray(keyedBars)) return keyedBars;

      const flattened: unknown[] = [];
      for (const value of Object.values(barsRecord)) {
        if (Array.isArray(value)) flattened.push(...value);
      }
      if (flattened.length > 0) return flattened;
    }

    const nestedData = payload.data;
    if (nestedData && typeof nestedData === "object") {
      const nestedRecord = nestedData as Record<string, unknown>;
      const nestedBars = nestedRecord.bars;
      if (Array.isArray(nestedBars)) return nestedBars;
      if (nestedBars && typeof nestedBars === "object") {
        const nestedBarsRecord = nestedBars as Record<string, unknown>;
        const keyedNestedBars =
          nestedBarsRecord[normalizedSymbol] ??
          nestedBarsRecord[normalizedSymbol.toUpperCase()] ??
          nestedBarsRecord[normalizedSymbol.toLowerCase()];
        if (Array.isArray(keyedNestedBars)) return keyedNestedBars;
      }
    }

    if (Array.isArray(payload.results)) return payload.results;
    return [];
  }

  private parseBarCloses(rows: unknown[], limit: number): number[] {
    return rows
      .map((row) => {
        if (!row || typeof row !== "object") return null;
        const record = row as Record<string, unknown>;
        return this.normalizeBarClose(record.c ?? record.close ?? record.closing_price);
      })
      .filter((value): value is number => value !== null)
      .slice(-limit);
  }

  private parseBarRows(rows: unknown[], limit: number): DailyBar[] {
    return rows
      .map((row) => {
        if (!row || typeof row !== "object") return null;
        const record = row as Record<string, unknown>;
        const close = this.normalizeBarClose(record.c ?? record.close ?? record.closing_price);
        if (close === null) return null;

        const highRaw = this.asFiniteNumber(record.h ?? record.high ?? record.high_price);
        const lowRaw = this.asFiniteNumber(record.l ?? record.low ?? record.low_price);
        const openRaw = this.asFiniteNumber(record.o ?? record.open ?? record.open_price);
        const high = highRaw !== null && highRaw > 0 ? Math.max(highRaw, close) : close;
        const low = lowRaw !== null && lowRaw > 0 ? Math.min(lowRaw, close, high) : close;
        const open =
          openRaw !== null && openRaw > 0
            ? Math.max(Math.min(openRaw, high), low)
            : close;
        const volume = Math.max(
          0,
          Math.round(this.asFiniteNumber(record.v ?? record.volume ?? record.trade_count) ?? 0)
        );
        const timestamp = this.normalizePublishedAt(
          record.t ?? record.timestamp ?? record.time ?? record.date
        );

        return {
          timestamp,
          open: Number(open.toFixed(6)),
          high: Number(high.toFixed(6)),
          low: Number(low.toFixed(6)),
          close: Number(close.toFixed(6)),
          volume
        } satisfies DailyBar;
      })
      .filter((value): value is DailyBar => value !== null)
      .slice(-limit);
  }

  private async requestJson(
    path: string,
    requestPayload: Record<string, unknown>
  ): Promise<{ payload: unknown; note?: string }> {
    const url = `${this.baseUrl.replace(/\/$/, "")}${path}`;
    const response = await fetchWithApiLog(
      url,
      {
        headers: this.headers()
      },
      {
        provider: "alpaca",
        endpoint: path,
        reason: "Discover dynamic symbols from Alpaca screener",
        requestPayload
      }
    );

    if (!response.ok) {
      let detail = "";
      try {
        detail = (await response.text()).slice(0, 220).replace(/\s+/g, " ").trim();
      } catch {
        detail = "";
      }
      return {
        payload: null,
        note: `Alpaca responded ${response.status}${detail ? `: ${detail}` : ""}`
      };
    }

    return {
      payload: await response.json()
    };
  }

  async getMarketScannerSymbols(
    limit = 15,
    mode: IbkrScannerCodeSetting = DEFAULT_IBKR_SCANNER_CODE
  ): Promise<AlpacaScannerResult> {
    if (!this.isConfigured()) {
      return {
        symbols: [],
        source: "disabled",
        note: "ALPACA_API_KEY and/or ALPACA_API_SECRET not configured."
      };
    }

    const cappedLimit = Math.max(5, Math.min(80, Math.round(limit)));
    const symbols: string[] = [];
    const notes: string[] = [];

    try {
      const mostActivesBy = mode === "TOP_TRADE_RATE" ? "trades" : "volume";
      const preferMoversOnly = mode === "TOP_PERC_GAIN" || mode === "TOP_PERC_LOSE";
      const moversSide: "gainers" | "losers" | null =
        mode === "TOP_PERC_GAIN" ? "gainers" : mode === "TOP_PERC_LOSE" ? "losers" : null;

      if (!preferMoversOnly) {
        const mostActives = await this.requestJson(
          `/v1beta1/screener/stocks/most-actives?by=${mostActivesBy}&top=${Math.max(5, Math.min(50, cappedLimit))}`,
          {
            top: Math.max(5, Math.min(50, cappedLimit)),
            by: mostActivesBy,
            mode
          }
        );
        if (mostActives.note) {
          notes.push(mostActives.note);
        } else {
          this.collectSymbols(mostActives.payload, symbols, cappedLimit * 2);
        }
      }

      if (symbols.length < cappedLimit || preferMoversOnly) {
        const moversTop = Math.max(5, Math.min(30, Math.ceil(cappedLimit / 2)));
        const movers = await this.requestJson(
          `/v1beta1/screener/stocks/movers?top=${moversTop}`,
          {
            top: moversTop,
            mode
          }
        );
        if (movers.note) {
          notes.push(movers.note);
        } else if (moversSide) {
          this.collectMoversSideSymbols(movers.payload, moversSide, symbols, cappedLimit * 2);
        } else {
          this.collectSymbols(movers.payload, symbols, cappedLimit * 2);
        }
      }

      if (symbols.length < cappedLimit && mode !== "MOST_ACTIVE") {
        notes.push(`Alpaca mode ${mode} had low coverage; appended MOST_ACTIVE fallback.`);
        const fallbackMostActives = await this.requestJson(
          `/v1beta1/screener/stocks/most-actives?by=volume&top=${Math.max(5, Math.min(50, cappedLimit))}`,
          {
            top: Math.max(5, Math.min(50, cappedLimit)),
            by: "volume",
            mode: "MOST_ACTIVE"
          }
        );
        if (fallbackMostActives.note) {
          notes.push(fallbackMostActives.note);
        } else {
          this.collectSymbols(fallbackMostActives.payload, symbols, cappedLimit * 2);
        }
      }

      const unique = [...new Set(symbols)].slice(0, cappedLimit);
      if (unique.length === 0) {
        return {
          symbols: [],
          source: "unavailable",
          note: notes.join(" | ") || "Alpaca screener returned no usable symbols."
        };
      }

      return {
        symbols: unique,
        source: "alpaca_screener",
        note: notes.length > 0 ? notes.join(" | ") : undefined
      };
    } catch (error) {
      logger.warn("Alpaca screener fetch failed", error);
      return {
        symbols: [],
        source: "unavailable",
        note: (error as Error).message
      };
    }
  }

  async getLatestQuote(symbol: string): Promise<AlpacaQuoteResult> {
    const normalizedSymbol = this.normalizeTicker(symbol) ?? symbol.trim().toUpperCase();
    if (!this.isConfigured()) {
      return {
        symbol: normalizedSymbol,
        last: 0,
        bid: 0,
        ask: 0,
        volume: 0,
        source: "disabled",
        note: "ALPACA_API_KEY and/or ALPACA_API_SECRET not configured."
      };
    }

    const path = `/v2/stocks/${encodeURIComponent(normalizedSymbol)}/quotes/latest?feed=iex`;
    const url = `${this.baseUrl.replace(/\/$/, "")}${path}`;
    try {
      const response = await fetchWithApiLog(
        url,
        {
          headers: this.headers()
        },
        {
          provider: "alpaca",
          endpoint: path,
          reason: `Fetch latest quote for ${normalizedSymbol}`,
          requestPayload: { symbol: normalizedSymbol, feed: "iex" }
        }
      );
      if (!response.ok) {
        const detail = (await response.text()).slice(0, 240).replace(/\s+/g, " ").trim();
        return {
          symbol: normalizedSymbol,
          last: 0,
          bid: 0,
          ask: 0,
          volume: 0,
          source: "unavailable",
          note: `Alpaca quote returned ${response.status}${detail ? `: ${detail}` : ""}`
        };
      }

      const payload = (await response.json()) as Record<string, unknown>;
      const quotePayload = (payload.quote ?? payload) as Record<string, unknown>;
      const bid = this.pickNumber(quotePayload, ["bp", "bid_price", "bid"]);
      const ask = this.pickNumber(quotePayload, ["ap", "ask_price", "ask"]);
      const last =
        this.pickNumber(quotePayload, ["lp", "last_price", "last"]) ??
        this.pickNumber(quotePayload, ["ap", "ask_price", "ask"]) ??
        this.pickNumber(quotePayload, ["bp", "bid_price", "bid"]);
      const size =
        this.pickNumber(quotePayload, ["as", "ask_size", "ask_sz"]) ??
        this.pickNumber(quotePayload, ["bs", "bid_size", "bid_sz"]) ??
        this.pickNumber(quotePayload, ["v", "volume"]) ??
        0;

      if (
        typeof last !== "number" ||
        !Number.isFinite(last) ||
        last <= 0 ||
        typeof bid !== "number" ||
        !Number.isFinite(bid) ||
        bid <= 0 ||
        typeof ask !== "number" ||
        !Number.isFinite(ask) ||
        ask <= 0
      ) {
        return {
          symbol: normalizedSymbol,
          last: 0,
          bid: 0,
          ask: 0,
          volume: 0,
          source: "unavailable",
          note: "Alpaca quote payload missing bid/ask/last."
        };
      }

      return {
        symbol: normalizedSymbol,
        last,
        bid,
        ask: Math.max(ask, bid),
        volume: Math.max(0, Math.round(size)),
        source: "alpaca_quote"
      };
    } catch (error) {
      logger.warn("Alpaca latest quote fetch failed", error);
      return {
        symbol: normalizedSymbol,
        last: 0,
        bid: 0,
        ask: 0,
        volume: 0,
        source: "unavailable",
        note: (error as Error).message
      };
    }
  }

  async getRecentDailyCloses(symbol: string, bars = 60): Promise<AlpacaHistoricalResult> {
    const historical = await this.getRecentDailyBars(symbol, bars);
    return {
      symbol: historical.symbol,
      closes: historical.bars.map((bar) => bar.close).filter((value) => value > 0),
      source: historical.source,
      note: historical.note
    };
  }

  async getRecentDailyBars(symbol: string, bars = 60): Promise<AlpacaHistoricalBarsResult> {
    const normalizedSymbol = this.normalizeTicker(symbol) ?? symbol.trim().toUpperCase();
    if (!this.isConfigured()) {
      return {
        symbol: normalizedSymbol,
        bars: [],
        source: "disabled",
        note: "ALPACA_API_KEY and/or ALPACA_API_SECRET not configured."
      };
    }

    const limit = Math.max(10, Math.min(500, Math.round(bars)));
    const now = new Date();
    const lookbackDays = Math.max(45, Math.min(540, Math.round(limit * 4)));
    const start = new Date(now.getTime() - lookbackDays * 24 * 60 * 60_000);
    const commonQuery = `timeframe=1Day&limit=${limit}&adjustment=raw&start=${encodeURIComponent(
      start.toISOString()
    )}&end=${encodeURIComponent(now.toISOString())}&sort=asc`;
    const fetchAttempt = async (feed: "iex" | "default"): Promise<{
      bars: DailyBar[];
      note?: string;
    }> => {
      const feedPart = feed === "iex" ? "&feed=iex" : "";
      const paths = [
        `/v2/stocks/${encodeURIComponent(normalizedSymbol)}/bars?${commonQuery}${feedPart}`,
        `/v2/stocks/bars?symbols=${encodeURIComponent(normalizedSymbol)}&${commonQuery}${feedPart}`
      ];

      let bestNote = "";
      for (const path of paths) {
        const url = `${this.baseUrl.replace(/\/$/, "")}${path}`;
        const response = await fetchWithApiLog(
          url,
          {
            headers: this.headers()
          },
          {
            provider: "alpaca",
            endpoint: path,
            reason: `Fetch recent daily closes for ${normalizedSymbol}`,
            requestPayload: {
              symbol: normalizedSymbol,
              timeframe: "1Day",
              limit,
              adjustment: "raw",
              start: start.toISOString(),
              end: now.toISOString(),
              feed
            }
          }
        );
        if (!response.ok) {
          const detail = (await response.text()).slice(0, 240).replace(/\s+/g, " ").trim();
          bestNote = `Alpaca bars returned ${response.status}${detail ? `: ${detail}` : ""}`;
          continue;
        }

        const payload = (await response.json()) as Record<string, unknown>;
        const parsedBars = this.parseBarRows(
          this.extractBarsPayload(payload, normalizedSymbol),
          limit
        );
        if (parsedBars.length > 0) {
          return {
            bars: parsedBars
          };
        }
        bestNote = "Alpaca bars payload contained no close values.";
      }

      return {
        bars: [],
        note: bestNote || "Alpaca bars payload contained no close values."
      };
    };

    try {
      const notes: string[] = [];
      const iexAttempt = await fetchAttempt("iex");
      if (iexAttempt.note) notes.push(`feed=iex: ${iexAttempt.note}`);
      let parsedBars = iexAttempt.bars;

      if (parsedBars.length === 0) {
        const defaultAttempt = await fetchAttempt("default");
        if (defaultAttempt.note) notes.push(`feed=default: ${defaultAttempt.note}`);
        parsedBars = defaultAttempt.bars;
      }

      if (parsedBars.length === 0) {
        return {
          symbol: normalizedSymbol,
          bars: [],
          source: "unavailable",
          note: notes.join(" | ") || "Alpaca bars payload contained no close values."
        };
      }

      return {
        symbol: normalizedSymbol,
        bars: parsedBars,
        source: "alpaca_historical"
      };
    } catch (error) {
      logger.warn("Alpaca historical bars fetch failed", error);
      return {
        symbol: normalizedSymbol,
        bars: [],
        source: "unavailable",
        note: (error as Error).message
      };
    }
  }

  async getNewsSentimentSnapshot(symbol: string, articleLimit = 8): Promise<AlpacaNewsSentimentSnapshot> {
    const normalizedSymbol = this.normalizeTicker(symbol) ?? symbol.trim().toUpperCase();
    if (!this.isConfigured()) {
      return {
        sentiment: null,
        articles: [],
        source: "disabled",
        note: "ALPACA_API_KEY and/or ALPACA_API_SECRET not configured."
      };
    }

    const cappedLimit = Math.max(3, Math.min(30, Math.round(articleLimit)));
    const path = `/v1beta1/news?symbols=${encodeURIComponent(normalizedSymbol)}&limit=${cappedLimit}&sort=desc`;
    const url = `${this.baseUrl.replace(/\/$/, "")}${path}`;

    try {
      const response = await fetchWithApiLog(
        url,
        {
          headers: this.headers()
        },
        {
          provider: "alpaca",
          endpoint: path,
          reason: `Fetch news sentiment for ${normalizedSymbol}`,
          requestPayload: {
            symbol: normalizedSymbol,
            limit: cappedLimit
          }
        }
      );

      if (!response.ok) {
        const detail = (await response.text()).slice(0, 240).replace(/\s+/g, " ").trim();
        return {
          sentiment: null,
          articles: [],
          source: "unavailable",
          note: `Alpaca news returned ${response.status}${detail ? `: ${detail}` : ""}`
        };
      }

      const payload = (await response.json()) as Record<string, unknown>;
      const rows = Array.isArray(payload.news)
        ? payload.news
        : Array.isArray(payload.data)
          ? payload.data
          : [];

      const scoredValues: number[] = [];
      const articles: AlpacaNewsArticle[] = [];
      for (const row of rows) {
        if (!row || typeof row !== "object") continue;
        const record = row as Record<string, unknown>;
        const headline = String(record.headline ?? record.title ?? "").trim();
        const summary = String(record.summary ?? "").trim();
        const source = String(record.source ?? record.author ?? "alpaca").trim() || "alpaca";
        const articleText = `${headline} ${summary}`.trim();
        const sentimentScore = this.scoreTextSentiment(articleText);
        if (sentimentScore !== null) scoredValues.push(sentimentScore);

        const symbols = Array.isArray(record.symbols)
          ? record.symbols
              .map((value) => this.normalizeTicker(value))
              .filter((value): value is string => Boolean(value))
          : [];
        const relevance = symbols.includes(normalizedSymbol)
          ? 1
          : symbols.length > 0
            ? 0.6
            : null;

        if (!headline && !summary) continue;
        if (articles.length >= cappedLimit) continue;
        articles.push({
          title: headline || "Untitled",
          source,
          url: String(record.url ?? "").trim(),
          publishedAt: this.normalizePublishedAt(record.created_at ?? record.updated_at ?? record.timestamp),
          summary,
          overallSentimentScore: sentimentScore,
          relevanceScore: relevance
        });
      }

      const sentiment =
        scoredValues.length > 0
          ? scoredValues.reduce((sum, value) => sum + value, 0) / scoredValues.length
          : null;

      return {
        sentiment,
        articles,
        source: "alpaca_news_sentiment",
        note: articles.length === 0 ? "Alpaca news returned no usable articles." : undefined
      };
    } catch (error) {
      logger.warn("Alpaca news sentiment fetch failed", normalizedSymbol, error);
      return {
        sentiment: null,
        articles: [],
        source: "unavailable",
        note: (error as Error).message
      };
    }
  }
}
