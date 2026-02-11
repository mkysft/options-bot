import {
  AlphaVantageAdapter,
  type AlphaNewsArticle,
  type AlphaNewsSentimentSnapshot
} from "../adapters/alphaVantageAdapter";
import { AiDiscoveryAdapter } from "../adapters/aiDiscoveryAdapter";
import {
  AlpacaAdapter,
  type AlpacaNewsSentimentSnapshot
} from "../adapters/alpacaAdapter";
import { EodhdAdapter } from "../adapters/eodhdAdapter";
import { FmpAdapter } from "../adapters/fmpAdapter";
import { FredAdapter, type FredMacroRegimeSnapshot } from "../adapters/fredAdapter";
import { IbkrAdapter, type IbkrOptionQuote } from "../adapters/ibkrAdapter";
import { SecEdgarAdapter, type SecEventSnapshot } from "../adapters/secEdgarAdapter";
import {
  DEFAULT_IBKR_SCANNER_CODE,
  type IbkrScannerCodeSetting
} from "../constants/scanner";
import { settings } from "../core/config";
import { logger } from "../core/logger";
import type { RuntimePolicyService } from "./runtimePolicyService";
import type { DailyBar, OptionContractSnapshot, OptionRight, SymbolSnapshot } from "../types/models";
import { clamp, stdDev } from "../utils/statistics";
import { nowIso } from "../utils/time";

interface CacheEntry<T> {
  value: T;
  expiresAt: number;
}

type QuoteSource = "ibkr_quote" | "alpaca_quote" | "synthetic_quote";
type HistoricalBarsSource = "ibkr_historical" | "alpaca_historical" | "synthetic_historical";
type ClosesSource = HistoricalBarsSource;
type OptionChainSource = "ibkr_option_chain" | "alpaca_option_chain" | "synthetic_option_chain";

interface SnapshotCacheValue {
  snapshot: SymbolSnapshot;
  quoteSource: QuoteSource;
}

interface ClosesCacheValue {
  closes: number[];
  source: ClosesSource;
}

interface DailyBarsCacheValue {
  bars: DailyBar[];
  source: HistoricalBarsSource;
}

interface OptionChainCacheValue {
  chain: OptionContractSnapshot[];
  source: OptionChainSource;
  note?: string;
}

type NewsSentimentSnapshot = {
  sentiment: number | null;
  articles: AlphaNewsArticle[];
  source:
    | "alpha_vantage_news_sentiment"
    | "alpaca_news_sentiment"
    | "disabled"
    | "unavailable";
  note?: string;
};

export interface SymbolSnapshotAndClosesEvidence {
  snapshot: SymbolSnapshot;
  closes: number[];
  dailyBars: DailyBar[];
  sources: {
    quote: QuoteSource;
    closes: ClosesSource;
    bars: HistoricalBarsSource;
  };
  notes: string[];
}

export interface OptionChainEvidence {
  chain: OptionContractSnapshot[];
  source: OptionChainSource;
  notes: string[];
}

export interface ContextFeaturesEvidence {
  newsSentiment: number;
  newsVelocity24h: number;
  newsSentimentDispersion: number;
  newsFreshness: number;
  eventBias: number;
  eventRisk: number;
  macroRegime: number;
  articles: AlphaNewsArticle[];
  sources: {
    newsSentiment: NewsSentimentSnapshot["source"];
    event: SecEventSnapshot["source"];
    macro: FredMacroRegimeSnapshot["source"];
  };
  notes: string[];
  raw: {
    news: NewsSentimentSnapshot;
    event: SecEventSnapshot;
    macro: FredMacroRegimeSnapshot;
  };
}

export interface DynamicUniverseResult {
  symbols: string[];
  discoveredSymbols: string[];
  scannerUsed: boolean;
  scannerSource:
    | "ibkr"
    | "fmp"
    | "eodhd"
    | "alpaca"
    | "alpha_vantage"
    | "ai_discovery"
    | "multi"
    | "none";
  ibkrScanCode: IbkrScannerCodeSetting | null;
  scannerProvidersUsed: string[];
  scannerProvidersTried: string[];
  scannerProviderRanking: Array<{ provider: string; score: number }>;
  scannerFallbackReason: string | null;
}

type ScannerProviderId =
  | "ibkr"
  | "fmp"
  | "eodhd"
  | "alpaca"
  | "alpha_vantage"
  | "ai_discovery";

interface ScannerProviderState {
  attempts: number;
  successes: number;
  consecutiveFailures: number;
  lastSuccessAtMs: number;
  lastFailureAtMs: number;
}

interface ScannerProviderResult {
  provider: ScannerProviderId;
  symbols: string[];
  note?: string;
  ibkrScanCode?: IbkrScannerCodeSetting;
}

const dayOfYear = (): number => {
  const now = new Date();
  const start = Date.UTC(now.getUTCFullYear(), 0, 0);
  const diff = now.getTime() - start;
  return Math.floor(diff / (1000 * 60 * 60 * 24));
};

const symbolSeed = (symbol: string): number =>
  symbol
    .toUpperCase()
    .split("")
    .reduce((acc, char) => acc + char.charCodeAt(0), 0);

const mulberry32 = (seed: number): (() => number) => {
  let t = seed + 0x6d2b79f5;
  return () => {
    t += 0x6d2b79f5;
    let x = Math.imul(t ^ (t >>> 15), t | 1);
    x ^= x + Math.imul(x ^ (x >>> 7), x | 61);
    return ((x ^ (x >>> 14)) >>> 0) / 4294967296;
  };
};

const isoDate = (date: Date): string => date.toISOString().slice(0, 10);

const toMacroFallbackSnapshot = (macroRegime: number): FredMacroRegimeSnapshot => ({
  macroRegime,
  source: "fallback",
  components: {
    vix: null,
    spread: null,
    vixSource: "unavailable",
    spreadSource: "unavailable"
  },
  note: "Macro component metadata unavailable from adapter."
});

export class MarketDataService {
  private readonly snapshotTtlMs = 20_000;
  private readonly closesTtlMs = 4 * 60_000;
  private readonly dailyBarsTtlMs = 4 * 60_000;
  private readonly optionChainTtlMs = 60_000;
  private readonly contextTtlMs = 5 * 60_000;
  private readonly macroTtlMs = 15 * 60_000;
  private readonly scannerIbkrQueueDepthSkipThreshold = 24;

  private readonly snapshotCache = new Map<string, CacheEntry<SnapshotCacheValue>>();
  private readonly closesCache = new Map<string, CacheEntry<ClosesCacheValue>>();
  private readonly closesInflight = new Map<string, Promise<ClosesCacheValue>>();
  private readonly dailyBarsCache = new Map<string, CacheEntry<DailyBarsCacheValue>>();
  private readonly dailyBarsInflight = new Map<string, Promise<DailyBarsCacheValue>>();
  private readonly optionChainCache = new Map<string, CacheEntry<OptionChainCacheValue>>();
  private readonly optionChainInflight = new Map<string, Promise<OptionChainCacheValue>>();
  private readonly contextCache = new Map<string, CacheEntry<ContextFeaturesEvidence>>();
  private readonly contextInflight = new Map<string, Promise<ContextFeaturesEvidence>>();
  private macroCache: CacheEntry<FredMacroRegimeSnapshot> | null = null;
  private macroInflight: Promise<FredMacroRegimeSnapshot> | null = null;
  private readonly scannerProviderState = new Map<ScannerProviderId, ScannerProviderState>();
  private runtimePolicy?: RuntimePolicyService;

  constructor(
    private readonly ibkr: IbkrAdapter = new IbkrAdapter(),
    private readonly alphaVantage: AlphaVantageAdapter = new AlphaVantageAdapter(),
    private readonly fmp: FmpAdapter = new FmpAdapter(),
    private readonly eodhd: EodhdAdapter = new EodhdAdapter(),
    private readonly alpaca: AlpacaAdapter = new AlpacaAdapter(),
    private readonly sec: SecEdgarAdapter = new SecEdgarAdapter(),
    private readonly fred: FredAdapter = new FredAdapter(),
    private readonly aiDiscovery: AiDiscoveryAdapter = new AiDiscoveryAdapter()
  ) {}

  setRuntimePolicy(runtimePolicy: RuntimePolicyService): void {
    this.runtimePolicy = runtimePolicy;
  }

  private analysisDataProvider(): "AUTO" | "ALPACA" | "IBKR" {
    const configured = String(this.runtimePolicy?.getPolicy().analysisDataProvider ?? "AUTO")
      .trim()
      .toUpperCase();
    if (configured === "AUTO" || configured === "IBKR" || configured === "ALPACA") {
      return configured;
    }
    return "AUTO";
  }

  getAnalysisDataProvider(): "AUTO" | "ALPACA" | "IBKR" {
    return this.analysisDataProvider();
  }

  private shouldAttemptIbkrAnalysisData(): boolean {
    const provider = this.analysisDataProvider();
    return provider === "AUTO" || provider === "IBKR";
  }

  private shouldAttemptAlpacaAnalysisData(): boolean {
    const provider = this.analysisDataProvider();
    return provider === "AUTO" || provider === "ALPACA";
  }

  private symbolKey(symbol: string): string {
    return symbol.toUpperCase();
  }

  private getCacheValue<T>(cache: Map<string, CacheEntry<T>>, key: string): T | null {
    const entry = cache.get(key);
    if (!entry) return null;
    if (entry.expiresAt <= Date.now()) {
      cache.delete(key);
      return null;
    }
    return entry.value;
  }

  private setCacheValue<T>(cache: Map<string, CacheEntry<T>>, key: string, value: T, ttlMs: number): T {
    cache.set(key, {
      value,
      expiresAt: Date.now() + ttlMs
    });
    return value;
  }

  private async loadWithCache<T>(
    key: string,
    ttlMs: number,
    cache: Map<string, CacheEntry<T>>,
    inflight: Map<string, Promise<T>>,
    loader: () => Promise<T>
  ): Promise<T> {
    const cached = this.getCacheValue(cache, key);
    if (cached !== null) return cached;

    const pending = inflight.get(key);
    if (pending) return pending;

    const request = (async () => {
      try {
        const value = await loader();
        return this.setCacheValue(cache, key, value, ttlMs);
      } finally {
        inflight.delete(key);
      }
    })();

    inflight.set(key, request);
    return request;
  }

  private syntheticQuote(symbol: string): { last: number; bid: number; ask: number; volume: number } {
    const seed = symbolSeed(symbol) + dayOfYear();
    const rng = mulberry32(seed);
    const base = 60 + (symbolSeed(symbol) % 300);
    const seasonal = Math.sin((new Date().getUTCHours() / 24) * Math.PI * 2) * (base * 0.004);
    const drift = (rng() - 0.5) * (base * 0.01);
    const last = Math.max(1, base + seasonal + drift);
    const spread = Math.max(0.01, last * (0.0005 + rng() * 0.0012));
    const bid = Math.max(0.01, last - spread / 2);
    const ask = last + spread / 2;
    const volume = 250_000 + Math.floor(rng() * 4_000_000);
    return { last, bid, ask, volume };
  }

  private realizedVol(closes: number[]): number {
    if (closes.length < 2) return 0.2;
    const returns: number[] = [];
    for (let index = 1; index < closes.length; index += 1) {
      const prev = closes[index - 1];
      const curr = closes[index];
      if (prev <= 0) continue;
      returns.push((curr - prev) / prev);
    }
    if (returns.length === 0) return 0.2;
    const mu = returns.reduce((acc, value) => acc + value, 0) / returns.length;
    const variance = returns.reduce((acc, value) => acc + (value - mu) ** 2, 0) / returns.length;
    return clamp(Math.sqrt(variance) * Math.sqrt(252), 0.05, 1.5);
  }

  private buildSnapshot(
    symbol: string,
    market: { last: number; bid: number; ask: number; volume: number },
    closes: number[]
  ): SymbolSnapshot {
    const rv = this.realizedVol(closes);
    const prev = closes.length >= 2 ? closes[closes.length - 2] : market.last;
    const pctChange1d = prev > 0 ? (market.last - prev) / prev : 0;
    const spreadPct = market.last > 0 ? (market.ask - market.bid) / market.last : 0;

    return {
      symbol,
      timestamp: nowIso(),
      last: Number(market.last.toFixed(4)),
      bid: Number(market.bid.toFixed(4)),
      ask: Number(market.ask.toFixed(4)),
      volume: market.volume,
      impliedVol: clamp(rv * 1.15, 0.08, 1.2),
      realizedVol: rv,
      pctChange1d,
      spreadPct
    };
  }

  private syntheticDailyBars(symbol: string, bars: number, fallbackLast?: number): DailyBar[] {
    const seed = symbolSeed(symbol) + 17;
    const rng = mulberry32(seed);
    const base = fallbackLast ?? 60 + (symbolSeed(symbol) % 300);
    const rows: DailyBar[] = [];
    let close = base;
    let previousClose = base;
    for (let index = 0; index < bars; index += 1) {
      close = Math.max(1, close * (1 + (rng() - 0.5) * 0.02));
      const high = Math.max(close, previousClose) * (1 + rng() * 0.006);
      const low = Math.min(close, previousClose) * (1 - rng() * 0.006);
      const open = previousClose;
      const volume = Math.max(0, Math.round(400_000 + rng() * 4_000_000));
      rows.push({
        timestamp: null,
        open: Number(open.toFixed(6)),
        high: Number(high.toFixed(6)),
        low: Number(Math.max(0.01, low).toFixed(6)),
        close: Number(close.toFixed(6)),
        volume
      });
      previousClose = close;
    }
    return rows;
  }

  private sanitizeDailyBars(rows: DailyBar[], limit: number): DailyBar[] {
    return rows
      .filter((row) => Number.isFinite(row.close) && row.close > 0)
      .map((row) => {
        const close = Number(row.close);
        const high =
          Number.isFinite(row.high) && row.high > 0 ? Math.max(Number(row.high), close) : close;
        const low =
          Number.isFinite(row.low) && row.low > 0 ? Math.min(Number(row.low), close, high) : close;
        const open =
          Number.isFinite(row.open) && row.open > 0
            ? Math.max(Math.min(Number(row.open), high), low)
            : close;
        const volume = Math.max(0, Math.round(Number.isFinite(row.volume) ? Number(row.volume) : 0));

        return {
          timestamp:
            typeof row.timestamp === "string" && row.timestamp.trim().length > 0
              ? row.timestamp
              : null,
          open: Number(open.toFixed(6)),
          high: Number(high.toFixed(6)),
          low: Number(low.toFixed(6)),
          close: Number(close.toFixed(6)),
          volume
        } satisfies DailyBar;
      })
      .slice(-Math.max(1, limit));
  }

  async getRecentDailyBarsDetailed(
    symbol: string,
    bars = 60,
    fallbackLast?: number
  ): Promise<DailyBarsCacheValue> {
    const symbolKey = this.symbolKey(symbol);
    const cacheKey = `${symbolKey}:${bars}`;
    return await this.loadWithCache(
      cacheKey,
      this.dailyBarsTtlMs,
      this.dailyBarsCache,
      this.dailyBarsInflight,
      async (): Promise<DailyBarsCacheValue> => {
        const normalizeClosesToBars = (closes: number[]): DailyBar[] =>
          closes
            .filter((close) => Number.isFinite(close) && close > 0)
            .map((close) => ({
              timestamp: null,
              open: Number(close.toFixed(6)),
              high: Number(close.toFixed(6)),
              low: Number(close.toFixed(6)),
              close: Number(close.toFixed(6)),
              volume: 0
            }));

        const ibkrAttempt = async (): Promise<DailyBarsCacheValue | null> => {
          try {
            const ibkrWithBars = this.ibkr as IbkrAdapter & {
              getRecentDailyBars?: (symbol: string, bars?: number) => Promise<DailyBar[]>;
            };
            const ibkrBars =
              typeof ibkrWithBars.getRecentDailyBars === "function"
                ? await ibkrWithBars.getRecentDailyBars(symbolKey, bars)
                : normalizeClosesToBars(await this.ibkr.getRecentDailyCloses(symbolKey, bars));
            const normalizedIbkrBars = this.sanitizeDailyBars(ibkrBars, bars);
            if (normalizedIbkrBars.length > 0) {
              return {
                bars: normalizedIbkrBars,
                source: "ibkr_historical"
              };
            }
          } catch (error) {
            logger.warn(
              `Historical daily bars unavailable for ${symbolKey}; fallback providers will be used.`,
              (error as Error).message
            );
          }
          return null;
        };

        const alpacaAttempt = async (): Promise<DailyBarsCacheValue | null> => {
          try {
            const alpacaWithBars = this.alpaca as AlpacaAdapter & {
              getRecentDailyBars?: (
                symbol: string,
                bars?: number
              ) => Promise<{ bars: DailyBar[]; source: "alpaca_historical" | "disabled" | "unavailable"; note?: string }>;
            };
            const alpacaBars =
              typeof alpacaWithBars.getRecentDailyBars === "function"
                ? await alpacaWithBars.getRecentDailyBars(symbolKey, bars)
                : await this.alpaca.getRecentDailyCloses(symbolKey, bars).then((result) => ({
                    bars: normalizeClosesToBars(result.closes),
                    source: result.source,
                    note: result.note
                  }));

            if (alpacaBars.source === "alpaca_historical" && alpacaBars.bars.length > 0) {
              return {
                bars: this.sanitizeDailyBars(alpacaBars.bars, bars),
                source: "alpaca_historical"
              };
            }
            if (alpacaBars.source === "unavailable" && alpacaBars.note) {
              logger.warn(
                `Alpaca historical daily bars unavailable for ${symbolKey}; synthetic fallback will be used.`,
                alpacaBars.note
              );
            }
          } catch (error) {
            logger.warn(
              `Alpaca historical daily bars unavailable for ${symbolKey}; synthetic fallback will be used.`,
              (error as Error).message
            );
          }
          return null;
        };

        const runOrder = this.analysisDataProvider() === "ALPACA"
          ? [alpacaAttempt, ibkrAttempt]
          : [ibkrAttempt, alpacaAttempt];

        for (const attempt of runOrder) {
          const useIbkrPath = attempt === ibkrAttempt;
          if (useIbkrPath && !this.shouldAttemptIbkrAnalysisData()) continue;
          if (!useIbkrPath && !this.shouldAttemptAlpacaAnalysisData()) continue;
          const result = await attempt();
          if (result) return result;
        }

        return {
          bars: this.syntheticDailyBars(symbolKey, bars, fallbackLast),
          source: "synthetic_historical"
        };
      }
    );
  }

  async getRecentDailyClosesDetailed(
    symbol: string,
    bars = 60,
    fallbackLast?: number
  ): Promise<ClosesCacheValue> {
    const symbolKey = this.symbolKey(symbol);
    const cacheKey = `${symbolKey}:${bars}`;
    return await this.loadWithCache(
      cacheKey,
      this.closesTtlMs,
      this.closesCache,
      this.closesInflight,
      async (): Promise<ClosesCacheValue> => {
        const barsEntry = await this.getRecentDailyBarsDetailed(symbolKey, bars, fallbackLast);
        return {
          closes: barsEntry.bars.map((bar) => bar.close).slice(-Math.max(1, bars)),
          source: barsEntry.source
        };
      }
    );
  }

  async getRecentDailyCloses(symbol: string, bars = 60, fallbackLast?: number): Promise<number[]> {
    const result = await this.getRecentDailyClosesDetailed(symbol, bars, fallbackLast);
    return result.closes;
  }

  async getSymbolSnapshot(symbol: string): Promise<SymbolSnapshot> {
    return (await this.getSymbolSnapshotAndCloses(symbol, 30)).snapshot;
  }

  async getSymbolSnapshotAndClosesDetailed(
    symbol: string,
    bars = 90
  ): Promise<SymbolSnapshotAndClosesEvidence> {
    const symbolKey = this.symbolKey(symbol);
    const requestedBars = Math.max(30, bars);
    const notes: string[] = [];
    let quoteErrorMessage: string | null = null;
    let quoteSourceNote: string | null = null;

    let snapshotEntry = this.getCacheValue(this.snapshotCache, symbolKey);
    let barsEntry: DailyBarsCacheValue;

    if (!snapshotEntry) {
      let quote = null as Awaited<ReturnType<IbkrAdapter["getQuote"]>>;
      let quoteSource: QuoteSource = "synthetic_quote";

      const ibkrQuoteAttempt = async (): Promise<boolean> => {
        try {
          quote = await this.ibkr.getQuote(symbolKey);
          if (quote) {
            quoteSource = "ibkr_quote";
            return true;
          }
        } catch (error) {
          quoteErrorMessage = (error as Error).message;
          logger.warn(
            `Quote unavailable for ${symbolKey}; synthetic fallback will be used.`,
            quoteErrorMessage
          );
        }
        return false;
      };

      const alpacaQuoteAttempt = async (): Promise<boolean> => {
        try {
          const alpacaQuote = await this.alpaca.getLatestQuote(symbolKey);
          if (alpacaQuote.source === "alpaca_quote" && alpacaQuote.last > 0) {
            quote = {
              symbol: symbolKey,
              last: alpacaQuote.last,
              bid: alpacaQuote.bid,
              ask: alpacaQuote.ask,
              volume: alpacaQuote.volume
            };
            quoteSource = "alpaca_quote";
            if (alpacaQuote.note) quoteSourceNote = alpacaQuote.note;
            return true;
          }
          if (alpacaQuote.note) quoteSourceNote = alpacaQuote.note;
        } catch (error) {
          quoteSourceNote = (error as Error).message;
        }
        return false;
      };

      const quoteOrder = this.analysisDataProvider() === "ALPACA"
        ? [alpacaQuoteAttempt, ibkrQuoteAttempt]
        : [ibkrQuoteAttempt, alpacaQuoteAttempt];
      for (const attempt of quoteOrder) {
        const useIbkrPath = attempt === ibkrQuoteAttempt;
        if (useIbkrPath && !this.shouldAttemptIbkrAnalysisData()) continue;
        if (!useIbkrPath && !this.shouldAttemptAlpacaAnalysisData()) continue;
        const resolved = await attempt();
        if (resolved) break;
      }

      const market = quote ?? this.syntheticQuote(symbolKey);
      barsEntry = await this.getRecentDailyBarsDetailed(symbolKey, requestedBars, market.last);
      const closesForSnapshot = barsEntry.bars.map((bar) => bar.close).slice(-30);
      snapshotEntry = this.setCacheValue(
        this.snapshotCache,
        symbolKey,
        {
          snapshot: this.buildSnapshot(symbolKey, market, closesForSnapshot),
          quoteSource
        },
        this.snapshotTtlMs
      );
    } else {
      barsEntry = await this.getRecentDailyBarsDetailed(
        symbolKey,
        requestedBars,
        snapshotEntry.snapshot.last
      );
    }

    if (snapshotEntry.quoteSource === "synthetic_quote") {
      notes.push(
        quoteErrorMessage
          ? `Underlying quote unavailable from IBKR (${quoteErrorMessage}); synthetic quote fallback was used.`
          : "Underlying quote unavailable from IBKR; synthetic quote fallback was used."
      );
      if (quoteSourceNote) notes.push(`Alpaca quote fallback unavailable (${quoteSourceNote}).`);
    } else if (snapshotEntry.quoteSource === "alpaca_quote") {
      notes.push("Underlying quote sourced from Alpaca fallback.");
      if (quoteSourceNote) notes.push(`Alpaca quote note: ${quoteSourceNote}`);
    }
    if (barsEntry.source === "synthetic_historical") {
      notes.push("Historical closes unavailable from IBKR; synthetic closes fallback was used.");
    } else if (barsEntry.source === "alpaca_historical") {
      notes.push("Historical closes sourced from Alpaca fallback.");
    }

    const slicedBars = barsEntry.bars.slice(-Math.max(1, bars));
    const slicedCloses = slicedBars.map((bar) => bar.close);
    return {
      snapshot: snapshotEntry.snapshot,
      closes: slicedCloses,
      dailyBars: slicedBars,
      sources: {
        quote: snapshotEntry.quoteSource,
        closes: barsEntry.source,
        bars: barsEntry.source
      },
      notes
    };
  }

  async getSymbolSnapshotAndCloses(
    symbol: string,
    bars = 90
  ): Promise<{ snapshot: SymbolSnapshot; closes: number[] }> {
    const result = await this.getSymbolSnapshotAndClosesDetailed(symbol, bars);
    return {
      snapshot: result.snapshot,
      closes: result.closes
    };
  }

  private deltaProxy(right: OptionRight, strike: number, underlyingPrice: number): number {
    const moneyness = (underlyingPrice - strike) / Math.max(underlyingPrice, 1e-6);
    const base = clamp(0.5 + moneyness * 4, 0.05, 0.95);
    return right === "CALL" ? base : -base;
  }

  private syntheticOptionChain(
    symbol: string,
    underlyingPrice: number,
    dteMin = settings.dteMin,
    dteMax = settings.dteMax
  ): OptionContractSnapshot[] {
    const chain: OptionContractSnapshot[] = [];
    const now = new Date();
    const targetDtes = [Math.max(dteMin, 7), 14, Math.min(dteMax, 21)];
    const strikes = [0.95, 0.975, 1, 1.025, 1.05].map((k) => Number((underlyingPrice * k).toFixed(2)));

    for (const dte of targetDtes) {
      const exp = new Date(now);
      exp.setUTCDate(now.getUTCDate() + dte);
      const expiration = isoDate(exp);

      for (const strike of strikes) {
        for (const right of ["CALL", "PUT"] as const) {
          const intrinsic =
            right === "CALL" ? Math.max(0, underlyingPrice - strike) : Math.max(0, strike - underlyingPrice);
          const extrinsic = Math.max(0.8, underlyingPrice * (0.006 + dte / 3650));
          const mid = intrinsic + extrinsic;
          const spread = Math.max(0.03, mid * 0.07);

          chain.push({
            symbol,
            expiration,
            strike,
            right,
            bid: Number(Math.max(0.01, mid - spread / 2).toFixed(2)),
            ask: Number((mid + spread / 2).toFixed(2)),
            last: Number(mid.toFixed(2)),
            volume: 200,
            openInterest: 700,
            impliedVol: Number((0.18 + (Math.abs(strike - underlyingPrice) / underlyingPrice) * 0.35).toFixed(4)),
            delta: this.deltaProxy(right, strike, underlyingPrice),
            gamma: 0.03,
            quoteSource: "synthetic_option_chain"
          });
        }
      }
    }

    return chain;
  }

  private optionContractKey(contract: {
    symbol: string;
    expiration: string;
    strike: number;
    right: OptionRight;
  }): string {
    return `${contract.symbol.toUpperCase()}|${contract.expiration.replace(/\D/g, "")}|${contract.right}|${Number(contract.strike).toFixed(4)}`;
  }

  private isLikelyListedStrike(strike: number): boolean {
    if (!Number.isFinite(strike) || strike <= 0) return false;
    const scaled = strike * 100;
    const cents = Math.round(scaled);
    if (Math.abs(cents - scaled) > 1e-4) return false;
    return cents % 5 === 0;
  }

  private selectOptionQuoteCandidates(
    chain: OptionContractSnapshot[],
    underlyingPrice: number
  ): OptionContractSnapshot[] {
    if (chain.length === 0) return [];
    const sortedExpirations = [...new Set(chain.map((contract) => contract.expiration))].sort();
    const selected: OptionContractSnapshot[] = [];
    const seen = new Set<string>();

    for (const expiration of sortedExpirations.slice(0, 1)) {
      const expirationContracts = chain.filter((contract) => contract.expiration === expiration);
      const candidateStrikes = [...new Set(expirationContracts.map((contract) => contract.strike))]
        .filter((strike) => this.isLikelyListedStrike(strike))
        .sort((a, b) => Math.abs(a - underlyingPrice) - Math.abs(b - underlyingPrice))
        .slice(0, 1);

      for (const strike of candidateStrikes) {
        for (const right of ["CALL", "PUT"] as const) {
          const contract = expirationContracts.find(
            (entry) => entry.strike === strike && entry.right === right
          );
          if (!contract) continue;
          const key = this.optionContractKey(contract);
          if (seen.has(key)) continue;
          seen.add(key);
          selected.push(contract);
          if (selected.length >= 2) return selected;
        }
      }
    }

    return selected;
  }

  private mergeOptionQuote(
    contract: OptionContractSnapshot,
    quote: IbkrOptionQuote | null
  ): OptionContractSnapshot {
    if (!quote) return contract;

    const bid = quote.bid > 0 ? quote.bid : contract.bid;
    const ask = quote.ask > 0 ? quote.ask : contract.ask;
    const last = quote.last > 0 ? quote.last : contract.last;
    return {
      ...contract,
      bid: Number(bid.toFixed(4)),
      ask: Number(Math.max(ask, bid).toFixed(4)),
      last: Number(last.toFixed(4)),
      volume: quote.volume > 0 ? quote.volume : contract.volume,
      openInterest: quote.openInterest > 0 ? quote.openInterest : contract.openInterest,
      impliedVol:
        typeof quote.impliedVol === "number" && Number.isFinite(quote.impliedVol) && quote.impliedVol > 0
          ? quote.impliedVol
          : contract.impliedVol,
      delta:
        typeof quote.delta === "number" && Number.isFinite(quote.delta) ? quote.delta : contract.delta,
      gamma:
        typeof quote.gamma === "number" && Number.isFinite(quote.gamma) ? quote.gamma : contract.gamma,
      quoteSource: "ibkr_option_quote"
    };
  }

  async getOptionChainDetailed(
    symbol: string,
    underlyingPrice: number,
    dteMin = settings.dteMin,
    dteMax = settings.dteMax
  ): Promise<OptionChainEvidence> {
    const symbolKey = this.symbolKey(symbol);
    const underlyingBucket = Math.round(underlyingPrice * 20) / 20;
    const cacheKey = `${symbolKey}:${dteMin}:${dteMax}:${underlyingBucket.toFixed(2)}`;

    const result = await this.loadWithCache(
      cacheKey,
      this.optionChainTtlMs,
      this.optionChainCache,
      this.optionChainInflight,
      async (): Promise<OptionChainCacheValue> => {
        type AttemptResult = {
          chain: OptionContractSnapshot[];
          source: OptionChainSource;
          note?: string;
        } | null;

        const ibkrAttempt = async (): Promise<AttemptResult> => {
          if (!this.shouldAttemptIbkrAnalysisData()) {
            return {
              chain: [],
              source: "synthetic_option_chain" as const,
              note: "IBKR option-chain lookup skipped (analysis provider is ALPACA)."
            };
          }

          let rows: Awaited<ReturnType<IbkrAdapter["getOptionContracts"]>> = [];
          try {
            rows = await this.ibkr.getOptionContracts(symbolKey, underlyingPrice, dteMin, dteMax);
          } catch (error) {
            const message = (error as Error).message;
            logger.warn(
              `Option chain unavailable for ${symbolKey}; fallback providers will be used.`,
              message
            );
            return {
              chain: [],
              source: "synthetic_option_chain" as const,
              note: message
            };
          }

          if (rows.length === 0) {
            return {
              chain: [],
              source: "synthetic_option_chain" as const,
              note: "IBKR option chain returned no contracts."
            };
          }

          const chain = rows.map((row, index) => {
            const right: OptionRight = row.right === "C" ? "CALL" : "PUT";
            const intrinsic =
              right === "CALL"
                ? Math.max(0, underlyingPrice - row.strike)
                : Math.max(0, row.strike - underlyingPrice);
            const extrinsic = Math.max(0.6, underlyingPrice * 0.004);
            const mid = intrinsic + extrinsic;
            const spread = Math.max(0.03, mid * 0.08);
            const expiration = `${row.expiration.slice(0, 4)}-${row.expiration.slice(
              4,
              6
            )}-${row.expiration.slice(6, 8)}`;
            return {
              symbol: row.symbol,
              expiration,
              strike: row.strike,
              right,
              bid: Number(Math.max(0.01, mid - spread / 2).toFixed(2)),
              ask: Number((mid + spread / 2).toFixed(2)),
              last: Number(mid.toFixed(2)),
              volume: 50 + (index % 20) * 5,
              openInterest: 200 + (index % 30) * 12,
              impliedVol: 0.2 + (index % 15) * 0.01,
              delta: this.deltaProxy(right, row.strike, underlyingPrice),
              gamma: 0.02,
              quoteSource: "derived_contract" as const
            };
          });

          const quoteCandidates = this.selectOptionQuoteCandidates(chain, underlyingPrice);
          const quoteByContract = new Map<string, IbkrOptionQuote>();
          const quoteNotes: string[] = [];
          const ibkrWithOptionQuotes = this.ibkr as IbkrAdapter & {
            getOptionQuoteReadiness?: (nowMs?: number) => { allowed: boolean; reason: string };
            getOptionQuote?: (contract: {
              symbol: string;
              expiration: string;
              strike: number;
              right: "CALL" | "PUT";
            }) => Promise<IbkrOptionQuote | null>;
          };
          const quoteReadiness =
            typeof ibkrWithOptionQuotes.getOptionQuoteReadiness === "function"
              ? ibkrWithOptionQuotes.getOptionQuoteReadiness(Date.now())
              : { allowed: true, reason: "unknown" };
          if (typeof ibkrWithOptionQuotes.getOptionQuote === "function" && quoteReadiness.allowed) {
            await Promise.all(
              quoteCandidates.map(async (candidate) => {
                try {
                  const quote = await ibkrWithOptionQuotes.getOptionQuote?.({
                    symbol: candidate.symbol,
                    expiration: candidate.expiration,
                    strike: candidate.strike,
                    right: candidate.right
                  });
                  if (!quote) return;
                  quoteByContract.set(this.optionContractKey(candidate), quote);
                } catch (error) {
                  const message = (error as Error).message;
                  quoteNotes.push(
                    `Quote enrichment failed for ${candidate.symbol} ${candidate.expiration} ${candidate.strike} ${candidate.right}: ${message}`
                  );
                }
              })
            );
          }

          const enrichedChain = chain.map((contract) =>
            this.mergeOptionQuote(contract, quoteByContract.get(this.optionContractKey(contract)) ?? null)
          );
          const quoteReadinessNote =
            quoteReadiness.allowed || quoteCandidates.length === 0
              ? undefined
              : `Skipped option quote enrichment (${quoteReadiness.reason}).`;
          const mergedNotes = [quoteReadinessNote, ...new Set(quoteNotes).values()]
            .filter((value): value is string => typeof value === "string" && value.trim().length > 0)
            .slice(0, 3);

          return {
            chain: enrichedChain,
            source: "ibkr_option_chain" as const,
            note: mergedNotes.length > 0 ? mergedNotes.join(" | ").slice(0, 420) : undefined
          };
        };

        const alpacaAttempt = async (): Promise<AttemptResult> => {
          if (!this.shouldAttemptAlpacaAnalysisData()) return null;

          const alpacaWithOptions = this.alpaca as AlpacaAdapter & {
            getOptionChain?: (
              symbol: string,
              options?: {
                dteMin?: number;
                dteMax?: number;
                maxContracts?: number;
                underlyingPrice?: number;
              }
            ) => Promise<{
              contracts: OptionContractSnapshot[];
              source: "alpaca_option_chain" | "disabled" | "unavailable";
              note?: string;
            }>;
          };
          if (typeof alpacaWithOptions.getOptionChain !== "function") return null;

          const result = await alpacaWithOptions.getOptionChain(symbolKey, {
            dteMin,
            dteMax,
            maxContracts: 120,
            underlyingPrice
          });
          if (result.source !== "alpaca_option_chain" || result.contracts.length === 0) {
            if (result.note) {
              logger.warn(
                `Alpaca option chain unavailable for ${symbolKey}; synthetic fallback will be used.`,
                result.note
              );
            }
            return {
              chain: [],
              source: "synthetic_option_chain" as const,
              note: result.note
            };
          }

          return {
            chain: result.contracts,
            source: "alpaca_option_chain" as const,
            note: result.note
          };
        };

        const attemptOrder =
          this.analysisDataProvider() === "ALPACA"
            ? [alpacaAttempt, ibkrAttempt]
            : [ibkrAttempt, alpacaAttempt];

        const notes: string[] = [];
        for (const attempt of attemptOrder) {
          const attemptResult = await attempt();
          if (!attemptResult) continue;
          if (attemptResult.note) notes.push(attemptResult.note);
          if (attemptResult.chain.length > 0) {
            return {
              chain: attemptResult.chain,
              source: attemptResult.source as OptionChainSource,
              note: notes.length > 0 ? notes.join(" | ").slice(0, 420) : undefined
            };
          }
        }

        return {
          chain: this.syntheticOptionChain(symbolKey, underlyingPrice, dteMin, dteMax),
          source: "synthetic_option_chain" as const,
          note: notes.length > 0 ? notes.join(" | ").slice(0, 420) : undefined
        };
      }
    );

    const enrichedQuotes = result.chain.filter(
      (contract) =>
        contract.quoteSource === "ibkr_option_quote" || contract.quoteSource === "alpaca_option_quote"
    ).length;
    const notes: string[] = [];
    if (result.source === "synthetic_option_chain") {
      notes.push("Option chain unavailable from configured providers; synthetic chain fallback was used.");
      if (result.note) notes.push(result.note);
    } else if (result.source === "alpaca_option_chain") {
      notes.push("Option chain sourced from Alpaca.");
      if (result.note) notes.push(result.note);
    } else if (enrichedQuotes === 0) {
      notes.push(
        "IBKR option chain contracts loaded, but quote enrichment returned no live contract snapshots."
      );
      if (result.note) notes.push(result.note);
    } else {
      notes.push(
        `IBKR option chain quote enrichment succeeded for ${enrichedQuotes} contracts.`
      );
    }

    return {
      chain: result.chain,
      source: result.source,
      notes
    };
  }

  async getOptionChain(
    symbol: string,
    underlyingPrice: number,
    dteMin = settings.dteMin,
    dteMax = settings.dteMax
  ): Promise<OptionContractSnapshot[]> {
    const result = await this.getOptionChainDetailed(symbol, underlyingPrice, dteMin, dteMax);
    return result.chain;
  }

  private async getMacroRegimeCachedDetailed(): Promise<FredMacroRegimeSnapshot> {
    if (this.macroCache && this.macroCache.expiresAt > Date.now()) return this.macroCache.value;
    if (this.macroInflight) return this.macroInflight;

    this.macroInflight = (async () => {
      try {
        const fredWithSnapshot = this.fred as FredAdapter & {
          getMacroRegimeSnapshot?: () => Promise<FredMacroRegimeSnapshot>;
        };
        const snapshot =
          typeof fredWithSnapshot.getMacroRegimeSnapshot === "function"
            ? await fredWithSnapshot.getMacroRegimeSnapshot()
            : toMacroFallbackSnapshot(await this.fred.getMacroRegime());

        this.macroCache = {
          value: snapshot,
          expiresAt: Date.now() + this.macroTtlMs
        };

        return snapshot;
      } finally {
        this.macroInflight = null;
      }
    })();

    return this.macroInflight;
  }

  private computeNewsVelocity24h(news: NewsSentimentSnapshot): number {
    if (news.articles.length === 0) return 0;
    const nowMs = Date.now();
    const recentCount = news.articles.filter((article) => {
      if (!article.publishedAt) return false;
      const publishedMs = Date.parse(article.publishedAt);
      if (!Number.isFinite(publishedMs)) return false;
      return nowMs - publishedMs <= 24 * 60 * 60_000;
    }).length;
    return clamp(recentCount / 8, 0, 1);
  }

  private computeNewsSentimentDispersion(news: NewsSentimentSnapshot): number {
    const values = news.articles
      .map((article) => article.overallSentimentScore)
      .filter((value): value is number => typeof value === "number" && Number.isFinite(value));
    if (values.length < 2) return 0;
    return clamp(stdDev(values), 0, 1);
  }

  private computeNewsFreshness(news: NewsSentimentSnapshot): number {
    const newestMs = news.articles
      .map((article) => {
        if (!article.publishedAt) return Number.NaN;
        return Date.parse(article.publishedAt);
      })
      .filter((value) => Number.isFinite(value))
      .sort((left, right) => right - left)[0];
    if (!Number.isFinite(newestMs)) return 0;
    const ageHours = Math.max(0, (Date.now() - newestMs) / (60 * 60_000));
    return clamp(1 - ageHours / 24, 0, 1);
  }

  async getContextFeaturesDetailed(symbol: string): Promise<ContextFeaturesEvidence> {
    const symbolKey = this.symbolKey(symbol);
    return await this.loadWithCache(
      symbolKey,
      this.contextTtlMs,
      this.contextCache,
      this.contextInflight,
      async () => {
        const alphaWithSnapshot = this.alphaVantage as AlphaVantageAdapter & {
          getNewsSentimentSnapshot?: (symbol: string, articleLimit?: number) => Promise<AlphaNewsSentimentSnapshot>;
        };
        const secWithSnapshot = this.sec as SecEdgarAdapter & {
          getEventBiasAndRiskSnapshot?: (symbol: string) => Promise<SecEventSnapshot>;
        };

        const loadNews = async (): Promise<NewsSentimentSnapshot> => {
          try {
            const alpacaWithSnapshot = this.alpaca as AlpacaAdapter & {
              getNewsSentimentSnapshot?: (
                symbol: string,
                articleLimit?: number
              ) => Promise<AlpacaNewsSentimentSnapshot>;
            };
            const loadAlpha = async (): Promise<AlphaNewsSentimentSnapshot> =>
              await (typeof alphaWithSnapshot.getNewsSentimentSnapshot === "function"
                ? alphaWithSnapshot.getNewsSentimentSnapshot(symbolKey)
                : this.alphaVantage
                    .getNewsSentiment(symbolKey)
                    .then(
                      (sentiment): AlphaNewsSentimentSnapshot => ({
                        sentiment,
                        articles: [],
                        source: sentiment === null ? "unavailable" : "alpha_vantage_news_sentiment",
                        note:
                          sentiment === null
                            ? "News sentiment unavailable from adapter."
                            : undefined
                      })
                    ));

            const loadAlpaca = async (): Promise<AlpacaNewsSentimentSnapshot> => {
              if (typeof alpacaWithSnapshot.getNewsSentimentSnapshot !== "function") {
                return {
                  sentiment: null,
                  articles: [],
                  source: "unavailable",
                  note: "Alpaca news adapter unavailable."
                };
              }
              return await alpacaWithSnapshot.getNewsSentimentSnapshot(symbolKey);
            };

            const prefersAlpaca = this.analysisDataProvider() === "ALPACA";
            const primary = prefersAlpaca ? await loadAlpaca() : await loadAlpha();
            const secondary = prefersAlpaca ? await loadAlpha() : await loadAlpaca();

            const primaryHasFeed =
              (primary.source === "alpha_vantage_news_sentiment" ||
                primary.source === "alpaca_news_sentiment") &&
              (primary.sentiment !== null || primary.articles.length > 0);
            if (primaryHasFeed) {
              return {
                sentiment: primary.sentiment,
                articles: primary.articles,
                source: primary.source,
                note: secondary.note ? `Fallback note: ${secondary.note}` : primary.note
              };
            }

            const mergedNotes = [primary.note, secondary.note].filter(
              (value): value is string => Boolean(value && value.trim().length > 0)
            );
            return {
              sentiment:
                primary.sentiment !== null
                  ? primary.sentiment
                  : secondary.sentiment !== null
                    ? secondary.sentiment
                    : null,
              articles:
                primary.articles.length > 0
                  ? primary.articles
                  : secondary.articles.length > 0
                    ? secondary.articles
                    : [],
              source:
                primary.source === "alpha_vantage_news_sentiment" ||
                primary.source === "alpaca_news_sentiment"
                  ? primary.source
                  : secondary.source === "alpha_vantage_news_sentiment" ||
                      secondary.source === "alpaca_news_sentiment"
                    ? secondary.source
                    : "unavailable",
              note: mergedNotes.length > 0 ? mergedNotes.join(" | ") : undefined
            };
          } catch (error) {
            logger.warn(`News sentiment lookup failed for ${symbolKey}`, (error as Error).message);
            return {
              sentiment: null,
              articles: [],
              source: "unavailable",
              note: (error as Error).message
            };
          }
        };

        const loadEvent = async (): Promise<SecEventSnapshot> => {
          try {
            return await (typeof secWithSnapshot.getEventBiasAndRiskSnapshot === "function"
              ? secWithSnapshot.getEventBiasAndRiskSnapshot(symbolKey)
              : this.sec.getEventBiasAndRisk(symbolKey).then((result) => ({
                  ...result,
                  source: "fallback",
                  cik: null,
                  latestFilingDate: null,
                  latestForm: null,
                  note: "Event metadata unavailable from adapter."
                } as SecEventSnapshot)));
          } catch (error) {
            logger.warn(`SEC event lookup failed for ${symbolKey}`, (error as Error).message);
            return {
              eventBias: 0,
              eventRisk: 0.5,
              source: "fallback",
              cik: null,
              latestFilingDate: null,
              latestForm: null,
              note: (error as Error).message
            };
          }
        };

        const loadMacro = async (): Promise<FredMacroRegimeSnapshot> => {
          try {
            return await this.getMacroRegimeCachedDetailed();
          } catch (error) {
            logger.warn("Macro regime lookup failed", (error as Error).message);
            return {
              ...toMacroFallbackSnapshot(0),
              note: (error as Error).message
            };
          }
        };

        const [news, event, macro] = await Promise.all([loadNews(), loadEvent(), loadMacro()]);

        const notes: string[] = [];
        if (
          news.source !== "alpha_vantage_news_sentiment" &&
          news.source !== "alpaca_news_sentiment"
        ) {
          notes.push(news.note || "News sentiment unavailable from configured providers.");
        } else if (news.source === "alpaca_news_sentiment") {
          notes.push("News sentiment sourced from Alpaca fallback.");
          if (news.note) notes.push(news.note);
        }
        if (event.source !== "sec_edgar") {
          notes.push(event.note || "SEC event context unavailable.");
        }
        if (macro.source !== "fred") {
          notes.push(macro.note || "FRED macro context unavailable.");
        }

        return {
          newsSentiment: news.sentiment ?? 0,
          newsVelocity24h: this.computeNewsVelocity24h(news),
          newsSentimentDispersion: this.computeNewsSentimentDispersion(news),
          newsFreshness: this.computeNewsFreshness(news),
          eventBias: event.eventBias,
          eventRisk: event.eventRisk,
          macroRegime: macro.macroRegime,
          articles: news.articles,
          sources: {
            newsSentiment: news.source,
            event: event.source,
            macro: macro.source
          },
          notes,
          raw: {
            news,
            event,
            macro
          }
        };
      }
    );
  }

  async getContextFeatures(symbol: string): Promise<{
    newsSentiment: number;
    newsVelocity24h: number;
    newsSentimentDispersion: number;
    newsFreshness: number;
    eventBias: number;
    eventRisk: number;
    macroRegime: number;
  }> {
    const context = await this.getContextFeaturesDetailed(symbol);
    return {
      newsSentiment: context.newsSentiment,
      newsVelocity24h: context.newsVelocity24h,
      newsSentimentDispersion: context.newsSentimentDispersion,
      newsFreshness: context.newsFreshness,
      eventBias: context.eventBias,
      eventRisk: context.eventRisk,
      macroRegime: context.macroRegime
    };
  }

  defaultUniverse(): string[] {
    return settings.universeSymbols;
  }

  private isTestRuntime(): boolean {
    return (
      settings.appEnv === "test" ||
      process.env.NODE_ENV === "test" ||
      Boolean(process.env.BUN_TEST)
    );
  }

  private isScannerProviderConfigured(provider: ScannerProviderId): boolean {
    if (this.isTestRuntime()) return true;
    switch (provider) {
      case "ibkr":
        return settings.ibkrEnabled;
      case "fmp":
        return settings.fmpApiKey.trim().length > 0;
      case "eodhd":
        return settings.eodhdApiKey.trim().length > 0;
      case "alpaca":
        return settings.alpacaApiKey.trim().length > 0 && settings.alpacaApiSecret.trim().length > 0;
      case "alpha_vantage":
        return settings.alphaVantageApiKey.trim().length > 0;
      case "ai_discovery":
        return settings.aiDiscoveryEnabled && settings.openAiApiKey.trim().length > 0;
      default:
        return false;
    }
  }

  private isUsEquityRegularSessionOpen(at = new Date()): boolean {
    const formatter = new Intl.DateTimeFormat("en-US", {
      timeZone: "America/New_York",
      weekday: "short",
      hour: "2-digit",
      minute: "2-digit",
      hourCycle: "h23"
    });
    const parts = formatter.formatToParts(at);
    const weekday = parts.find((part) => part.type === "weekday")?.value ?? "";
    const hour = Number(parts.find((part) => part.type === "hour")?.value ?? NaN);
    const minute = Number(parts.find((part) => part.type === "minute")?.value ?? NaN);
    if (!Number.isFinite(hour) || !Number.isFinite(minute)) return true;
    if (weekday === "Sat" || weekday === "Sun") return false;
    const totalMinutes = hour * 60 + minute;
    return totalMinutes >= 9 * 60 + 30 && totalMinutes < 16 * 60;
  }

  private resolveIbkrScanCodeForSession(
    selectedCode: IbkrScannerCodeSetting,
    regularSessionOpen: boolean
  ): { scanCode: IbkrScannerCodeSetting; note?: string } {
    if (regularSessionOpen) return { scanCode: selectedCode };

    if (selectedCode === "HIGH_OPEN_GAP" || selectedCode === "LOW_OPEN_GAP") {
      return {
        scanCode: DEFAULT_IBKR_SCANNER_CODE,
        note: `IBKR off-hours mode: ${selectedCode} is open-session-specific, switched to ${DEFAULT_IBKR_SCANNER_CODE}.`
      };
    }

    return {
      scanCode: selectedCode,
      note: "IBKR off-hours mode: using scanner outside US regular session; results can be thinner/staler."
    };
  }

  private providerBaseQuality(provider: ScannerProviderId): number {
    switch (provider) {
      case "ibkr":
        return 0.86;
      case "fmp":
        return 0.9;
      case "eodhd":
        return 0.84;
      case "alpaca":
        return 0.9;
      case "alpha_vantage":
        return 0.64;
      case "ai_discovery":
        return 0.58;
      default:
        return 0.5;
    }
  }

  private getScannerProviderState(provider: ScannerProviderId): ScannerProviderState {
    const existing = this.scannerProviderState.get(provider);
    if (existing) return existing;
    const created: ScannerProviderState = {
      attempts: 0,
      successes: 0,
      consecutiveFailures: 0,
      lastSuccessAtMs: 0,
      lastFailureAtMs: 0
    };
    this.scannerProviderState.set(provider, created);
    return created;
  }

  private markScannerProviderResult(provider: ScannerProviderId, success: boolean): void {
    const state = this.getScannerProviderState(provider);
    state.attempts += 1;
    if (success) {
      state.successes += 1;
      state.consecutiveFailures = 0;
      state.lastSuccessAtMs = Date.now();
      return;
    }
    state.consecutiveFailures += 1;
    state.lastFailureAtMs = Date.now();
  }

  private providerQualityScore(
    provider: ScannerProviderId,
    preferredIndex: number
  ): number {
    const state = this.getScannerProviderState(provider);
    const base = this.providerBaseQuality(provider);
    const successRate =
      state.attempts > 0 ? state.successes / Math.max(state.attempts, 1) : 0.68;
    const failurePenalty =
      provider === "ibkr"
        ? Math.min(0.62, state.consecutiveFailures * 0.2)
        : Math.min(0.26, state.consecutiveFailures * 0.07);
    const orderBonus = Math.max(0, 0.03 - preferredIndex * 0.004);
    return base + successRate * 0.2 + orderBonus - failurePenalty;
  }

  private normalizeScannerSymbols(symbols: string[], limit: number): string[] {
    const cappedLimit = Math.max(1, Math.min(80, Math.round(limit)));
    return [
      ...new Set(
        symbols
          .map((symbol) => symbol.trim().toUpperCase())
          .filter((symbol) => /^[A-Z][A-Z0-9.\-]{0,14}$/.test(symbol))
      )
    ].slice(0, cappedLimit);
  }

  private scannerProviderTimeoutMs(provider: ScannerProviderId): number {
    if (provider === "ibkr") {
      const configured = Number(settings.ibkrScannerTimeoutMs);
      const timeoutMs = Number.isFinite(configured) ? Math.round(configured) : 0;
      const minFromClient = Math.max(10_000, Math.round(settings.ibkrClientTimeoutMs * 1.2));
      return Math.max(minFromClient, Math.min(45_000, timeoutMs > 0 ? timeoutMs : minFromClient));
    }
    return 4_500;
  }

  private async withScannerProviderTimeout<T>(
    provider: ScannerProviderId,
    promise: Promise<T>
  ): Promise<T> {
    const timeoutMs = this.scannerProviderTimeoutMs(provider);
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) return await promise;

    let timer: ReturnType<typeof setTimeout> | null = null;
    try {
      return await Promise.race([
        promise,
        new Promise<T>((_resolve, reject) => {
          timer = setTimeout(() => {
            let detail = "";
            if (provider === "ibkr") {
              const runtime = this.ibkr.getRuntimeStatus();
              detail = ` (queue depth=${runtime.queue.depth}, workers=${runtime.queue.activeWorkers}/${runtime.queue.maxWorkers})`;
            }
            reject(
              new Error(
                `${provider} scanner timed out after ${Math.max(1, Math.round(timeoutMs))}ms${detail}.`
              )
            );
          }, Math.max(1, Math.round(timeoutMs)));
        })
      ]);
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  private rankedScannerProviders(): Array<{
    provider: ScannerProviderId;
    score: number;
  }> {
    const configured = settings.scannerProviderOrder as ScannerProviderId[];
    const defaults: ScannerProviderId[] = [
      "ibkr",
      "fmp",
      "eodhd",
      "alpaca",
      "alpha_vantage",
      "ai_discovery"
    ];
    const testRuntime = this.isTestRuntime();
    const orderedByPreference = testRuntime
      ? [...defaults]
      : [...new Set((configured.length > 0 ? configured : defaults))].filter(
          (provider): provider is ScannerProviderId => defaults.includes(provider as ScannerProviderId)
        );
    const expandedPreference = [...new Set([...orderedByPreference, ...defaults])];
    const configuredProviders = orderedByPreference.filter((provider) =>
      this.isScannerProviderConfigured(provider)
    );
    const ordered = configuredProviders.length > 0 ? configuredProviders : expandedPreference;
    return ordered
      .map((provider, index) => ({
        provider,
        score: this.providerQualityScore(provider, index)
      }))
      .sort((left, right) => {
        if (left.provider === "ibkr" && right.provider !== "ibkr") return -1;
        if (right.provider === "ibkr" && left.provider !== "ibkr") return 1;
        return right.score - left.score;
      });
  }

  private async scanWithProvider(
    provider: ScannerProviderId,
    limit: number,
    options?: {
      ibkrScanCode?: IbkrScannerCodeSetting;
    }
  ): Promise<ScannerProviderResult> {
    const cappedLimit = Math.max(5, Math.min(80, Math.round(limit)));

    if (provider === "ibkr") {
      if (!settings.ibkrEnabled) {
        return {
          provider,
          symbols: [],
          note: "IBKR scanner skipped: IBKR_ENABLED=false."
        };
      }
      const ibkrRuntime = this.ibkr.getRuntimeStatus();
      if (ibkrRuntime.requestCooldown.active) {
        return {
          provider,
          symbols: [],
          note: `IBKR scanner skipped: request cooldown active (${ibkrRuntime.requestCooldown.reason || "recent IBKR failures"}).`
        };
      }
      if (ibkrRuntime.queue.depth >= this.scannerIbkrQueueDepthSkipThreshold) {
        return {
          provider,
          symbols: [],
          note: `IBKR scanner skipped: queue depth ${ibkrRuntime.queue.depth} >= ${this.scannerIbkrQueueDepthSkipThreshold}.`
        };
      }
      const ibkrScanCode = options?.ibkrScanCode ?? DEFAULT_IBKR_SCANNER_CODE;
      const regularSessionOpen = this.isTestRuntime() ? true : this.isUsEquityRegularSessionOpen();
      const resolvedScanCode = this.resolveIbkrScanCodeForSession(ibkrScanCode, regularSessionOpen);
      const result = await this.ibkr.getScannerSymbolsWithSource({
        limit: Math.max(8, Math.min(40, cappedLimit)),
        scanCode: resolvedScanCode.scanCode
      });
      return {
        provider,
        symbols: this.normalizeScannerSymbols(result.symbols, cappedLimit),
        note:
          [resolvedScanCode.note, result.fallbackReason]
            .filter((value): value is string => typeof value === "string" && value.trim().length > 0)
            .join(" | ") || undefined,
        ibkrScanCode: resolvedScanCode.scanCode
      };
    }

    if (provider === "fmp") {
      const result = await this.fmp.getMarketScannerSymbols(Math.max(8, Math.min(80, cappedLimit)));
      return {
        provider,
        symbols: this.normalizeScannerSymbols(result.symbols, cappedLimit),
        note:
          result.note ||
          ((options?.ibkrScanCode ?? DEFAULT_IBKR_SCANNER_CODE) !== DEFAULT_IBKR_SCANNER_CODE
            ? `FMP scanner mode fallback (no direct mapping for ${options?.ibkrScanCode}).`
            : undefined)
      };
    }

    if (provider === "eodhd") {
      const result = await this.eodhd.getMarketScannerSymbols(Math.max(8, Math.min(80, cappedLimit)));
      return {
        provider,
        symbols: this.normalizeScannerSymbols(result.symbols, cappedLimit),
        note:
          result.note ||
          ((options?.ibkrScanCode ?? DEFAULT_IBKR_SCANNER_CODE) !== DEFAULT_IBKR_SCANNER_CODE
            ? `EODHD scanner mode fallback (no direct mapping for ${options?.ibkrScanCode}).`
            : undefined)
      };
    }

    if (provider === "alpaca") {
      const result = await this.alpaca.getMarketScannerSymbols(
        Math.max(8, Math.min(80, cappedLimit)),
        options?.ibkrScanCode ?? DEFAULT_IBKR_SCANNER_CODE
      );
      return {
        provider,
        symbols: this.normalizeScannerSymbols(result.symbols, cappedLimit),
        note: result.note
      };
    }

    if (provider === "ai_discovery") {
      const result = await this.aiDiscovery.getMarketScannerSymbols(
        Math.max(6, Math.min(40, cappedLimit)),
        options?.ibkrScanCode ?? DEFAULT_IBKR_SCANNER_CODE
      );
      return {
        provider,
        symbols: this.normalizeScannerSymbols(result.symbols, cappedLimit),
        note: result.note
      };
    }

    const alpha = await this.alphaVantage.getMarketScannerSymbols(
      Math.max(6, Math.min(60, cappedLimit))
    );
    return {
      provider,
      symbols: this.normalizeScannerSymbols(alpha.symbols, cappedLimit),
      note:
        alpha.note ||
        ((options?.ibkrScanCode ?? DEFAULT_IBKR_SCANNER_CODE) !== DEFAULT_IBKR_SCANNER_CODE
          ? `Alpha Vantage scanner mode fallback (no direct mapping for ${options?.ibkrScanCode}).`
          : undefined)
    };
  }

  async buildDynamicUniverse(
    baseUniverse: string[],
    topN: number,
    options?: {
      targetSize?: number;
      scannerLimit?: number;
      ibkrScanCode?: IbkrScannerCodeSetting;
    }
  ): Promise<DynamicUniverseResult> {
    const normalizedBase = [...new Set(baseUniverse.map((symbol) => symbol.trim().toUpperCase()).filter(Boolean))];
    const minTarget = Math.max(normalizedBase.length, topN);
    const expansionBuffer = settings.ibkrEnabled
      ? Math.max(2, Math.ceil(topN * 0.15))
      : Math.max(4, Math.ceil(topN * 0.35));
    const defaultTargetSize = minTarget + expansionBuffer;
    const absoluteCap = settings.ibkrEnabled
      ? Math.max(minTarget, topN + Math.max(4, Math.ceil(topN * 0.25)))
      : 40;
    const targetSize = Math.max(
      minTarget,
      Math.min(absoluteCap, options?.targetSize ?? defaultTargetSize)
    );
    const neededDiscovered = Math.max(0, targetSize - normalizedBase.length);

    const scannerLimit =
      options?.scannerLimit ??
      Math.max(6, Math.min(24, neededDiscovered + Math.ceil(neededDiscovered * 0.45)));
    const providerRanking = this.rankedScannerProviders();

    const ibkrScanCode = options?.ibkrScanCode ?? DEFAULT_IBKR_SCANNER_CODE;

    if (scannerLimit <= 0 || neededDiscovered <= 0) {
      return {
        symbols: normalizedBase.slice(0, targetSize),
        discoveredSymbols: [],
        scannerUsed: false,
        scannerSource: "none",
        ibkrScanCode: settings.ibkrEnabled ? ibkrScanCode : null,
        scannerProvidersUsed: [],
        scannerProvidersTried: [],
        scannerProviderRanking: providerRanking.map((row) => ({
          provider: row.provider,
          score: Number(row.score.toFixed(4))
        })),
        scannerFallbackReason: null
      };
    }

    const discovered: string[] = [];
    const providersUsed: string[] = [];
    const providersTried: string[] = [];
    const fallbackNotes: string[] = [];

    const requestedFromProvider = Math.max(
      8,
      Math.min(80, scannerLimit + Math.ceil(scannerLimit * 0.6))
    );

    for (const rankedProvider of providerRanking) {
      if (discovered.length >= neededDiscovered) break;

      providersTried.push(rankedProvider.provider);
      let result: ScannerProviderResult;
      try {
        result = await this.withScannerProviderTimeout(
          rankedProvider.provider,
          this.scanWithProvider(rankedProvider.provider, requestedFromProvider, {
            ibkrScanCode
          })
        );
      } catch (error) {
        this.markScannerProviderResult(rankedProvider.provider, false);
        fallbackNotes.push(
          `${rankedProvider.provider}: ${(error as Error).message || "provider request failed"}`
        );
        continue;
      }

      const providerSymbols = result.symbols.filter(
        (symbol) => !normalizedBase.includes(symbol) && !discovered.includes(symbol)
      );
      const providerSucceeded = providerSymbols.length > 0;
      this.markScannerProviderResult(rankedProvider.provider, providerSucceeded);

      if (providerSucceeded) {
        providersUsed.push(rankedProvider.provider);
        for (const symbol of providerSymbols) {
          if (discovered.length >= neededDiscovered) break;
          discovered.push(symbol);
        }
      }

      if (!providerSucceeded || result.note) {
        fallbackNotes.push(
          `${rankedProvider.provider}: ${result.note || "no usable symbols returned"}`
        );
      }
    }

    const scannerSource: DynamicUniverseResult["scannerSource"] =
      providersUsed.length === 0
        ? "none"
        : providersUsed.length > 1
          ? "multi"
          : (providersUsed[0] as DynamicUniverseResult["scannerSource"]);

    const merged = [...normalizedBase, ...discovered].slice(0, targetSize);

    return {
      symbols: merged,
      discoveredSymbols: discovered,
      scannerUsed: discovered.length > 0,
      scannerSource,
      ibkrScanCode: providersTried.includes("ibkr") ? ibkrScanCode : null,
      scannerProvidersUsed: [...new Set(providersUsed)],
      scannerProvidersTried: [...new Set(providersTried)],
      scannerProviderRanking: providerRanking.map((row) => ({
        provider: row.provider,
        score: Number(row.score.toFixed(4))
      })),
      scannerFallbackReason: fallbackNotes.length > 0 ? fallbackNotes.join(" | ").slice(0, 700) : null
    };
  }
}
