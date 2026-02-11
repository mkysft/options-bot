import type {
  DecisionCard,
  FeatureVector,
  OptionContractSnapshot,
  ScoreCard,
  SymbolAnalysis,
  SymbolSnapshot
} from "../types/models";
import {
  DEFAULT_IBKR_SCANNER_CODE,
  type IbkrScannerCodeSetting
} from "../constants/scanner";
import { settings } from "../core/config";
import { DecisionEngine } from "./decisionEngine";
import { FeatureEngine } from "./featureEngine";
import {
  type ContextFeaturesEvidence,
  MarketDataService,
  type OptionChainEvidence,
  type SymbolSnapshotAndClosesEvidence
} from "./marketDataService";
import { ScoringEngine } from "./scoringEngine";

interface SymbolComputation {
  symbol: string;
  snapshot: SymbolSnapshot;
  feature: FeatureVector;
  chain: OptionContractSnapshot[];
  evidence: {
    market: SymbolSnapshotAndClosesEvidence;
    optionChain: OptionChainEvidence;
    context: ContextFeaturesEvidence;
  };
}

class AnalysisSymbolTimeoutError extends Error {
  constructor(
    public readonly symbol: string,
    public readonly timeoutMs: number
  ) {
    super(`Timed out computing ${symbol} after ${timeoutMs}ms.`);
    this.name = "AnalysisSymbolTimeoutError";
  }
}

class AnalysisDiscoveryTimeoutError extends Error {
  constructor(public readonly timeoutMs: number) {
    super(`Dynamic universe discovery timed out after ${timeoutMs}ms.`);
    this.name = "AnalysisDiscoveryTimeoutError";
  }
}

export interface DetailedSymbolAnalysis extends SymbolAnalysis {
  chain: OptionContractSnapshot[];
  evidence: {
    market: SymbolSnapshotAndClosesEvidence;
    optionChain: OptionChainEvidence;
    context: ContextFeaturesEvidence;
  };
}

export interface ScanUniverseWithDiscoveryResult {
  analyses: SymbolAnalysis[];
  detailedAnalyses: DetailedSymbolAnalysis[];
  evaluatedUniverse: string[];
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
  analysisTimedOut: boolean;
  analysisAttemptedSymbols: number;
  analysisCompletedSymbols: number;
  analysisTimeoutReason: string | null;
}

export class AnalysisService {
  private scanWithDiscoveryInFlight: Promise<ScanUniverseWithDiscoveryResult> | null = null;
  private scanWithDiscoveryInFlightKey: string | null = null;

  constructor(
    private readonly marketData: MarketDataService,
    private readonly featureEngine: FeatureEngine,
    private readonly scoringEngine: ScoringEngine,
    private readonly decisionEngine: DecisionEngine
  ) {}

  private symbolComputeConcurrency(): number {
    const provider = this.marketData.getAnalysisDataProvider();
    if (provider === "ALPACA") return 4;
    return settings.ibkrEnabled ? 2 : 4;
  }

  private async mapConcurrent<T, R>(
    items: T[],
    concurrency: number,
    worker: (item: T, index: number) => Promise<R>
  ): Promise<R[]> {
    if (items.length === 0) return [];

    const limit = Math.max(1, Math.min(concurrency, items.length));
    const results = new Array<R>(items.length);
    let cursor = 0;

    const runWorker = async () => {
      while (true) {
        const index = cursor;
        cursor += 1;
        if (index >= items.length) return;
        results[index] = await worker(items[index], index);
      }
    };

    await Promise.all(Array.from({ length: limit }, () => runWorker()));
    return results;
  }

  private async withSymbolTimeout<T>(
    symbol: string,
    promise: Promise<T>,
    timeoutMs: number
  ): Promise<T> {
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
      throw new AnalysisSymbolTimeoutError(symbol, Math.max(0, Math.round(timeoutMs)));
    }

    let timer: ReturnType<typeof setTimeout> | null = null;
    try {
      return await Promise.race([
        promise,
        new Promise<T>((_resolve, reject) => {
          timer = setTimeout(() => {
            reject(new AnalysisSymbolTimeoutError(symbol, Math.round(timeoutMs)));
          }, Math.max(1, Math.round(timeoutMs)));
        })
      ]);
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  private async withDiscoveryTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) return await promise;

    let timer: ReturnType<typeof setTimeout> | null = null;
    try {
      return await Promise.race([
        promise,
        new Promise<T>((_resolve, reject) => {
          timer = setTimeout(() => {
            reject(new AnalysisDiscoveryTimeoutError(Math.max(1, Math.round(timeoutMs))));
          }, Math.max(1, Math.round(timeoutMs)));
        })
      ]);
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  private trailingReturn(closes: number[], lookback: number): number {
    if (closes.length <= lookback) return 0;
    const last = closes[closes.length - 1];
    const previous = closes[closes.length - 1 - lookback];
    if (!Number.isFinite(last) || !Number.isFinite(previous) || previous <= 0) return 0;
    return Math.max(-1, Math.min(1, last / previous - 1));
  }

  private resolveBenchmarkComputation(computations: SymbolComputation[]): SymbolComputation | null {
    if (computations.length === 0) return null;
    return (
      computations.find((entry) => entry.symbol === "SPY") ??
      computations.find((entry) => entry.symbol === "QQQ") ??
      computations
        .slice()
        .sort((left, right) => right.snapshot.volume - left.snapshot.volume)[0] ??
      null
    );
  }

  private enrichCrossSectionalSignals(computations: SymbolComputation[]): SymbolComputation[] {
    if (computations.length === 0) return [];

    const benchmarkEntry = this.resolveBenchmarkComputation(computations);
    const benchmarkSymbol = benchmarkEntry?.symbol ?? "NONE";
    const benchmarkCloses = benchmarkEntry?.evidence.market.closes ?? [];
    const benchmarkRet20 = this.trailingReturn(benchmarkCloses, 20);
    const benchmarkRet60 = this.trailingReturn(benchmarkCloses, 60);

    return computations.map((entry) => {
      const closes = entry.evidence.market.closes;
      const ownRet20 = this.trailingReturn(closes, 20);
      const ownRet60 = this.trailingReturn(closes, 60);
      const relativeStrength20d = Math.max(-1, Math.min(1, ownRet20 - benchmarkRet20));
      const relativeStrength60d = Math.max(-1, Math.min(1, ownRet60 - benchmarkRet60));

      return {
        ...entry,
        feature: {
          ...entry.feature,
          relativeStrength20d,
          relativeStrength60d
        },
        evidence: {
          ...entry.evidence,
          market: {
            ...entry.evidence.market,
            notes: [
              ...entry.evidence.market.notes,
              `relative_strength benchmark=${benchmarkSymbol} ret20=${benchmarkRet20.toFixed(4)} ret60=${benchmarkRet60.toFixed(4)}`
            ]
          }
        }
      };
    });
  }

  private async scanUniverseDetailedWithBudget(
    universe: string[],
    topN: number,
    budgetMs: number,
    perSymbolTimeoutMs: number
  ): Promise<{
    analyses: DetailedSymbolAnalysis[];
    attemptedSymbols: number;
    completedSymbols: number;
    timedOut: boolean;
    timeoutReason: string | null;
  }> {
    const symbols = [...new Set(universe.map((symbol) => symbol.toUpperCase()))];
    if (symbols.length === 0) {
      return {
        analyses: [],
        attemptedSymbols: 0,
        completedSymbols: 0,
        timedOut: false,
        timeoutReason: null
      };
    }

    const deadlineMs = Date.now() + Math.max(1, Math.round(budgetMs));
    const budgetLabelMs = `${Math.max(1, Math.round(budgetMs))}ms`;
    const workerCount = Math.max(1, Math.min(this.symbolComputeConcurrency(), symbols.length));
    const symbolTimeoutDefault = Math.max(1_000, Math.round(perSymbolTimeoutMs));
    const computations: SymbolComputation[] = [];
    let cursor = 0;
    let attempted = 0;
    let timedOut = false;

    const runWorker = async () => {
      while (true) {
        if (timedOut) return;
        const index = cursor;
        cursor += 1;
        if (index >= symbols.length) return;

        const remainingBefore = deadlineMs - Date.now();
        if (remainingBefore <= 250) {
          timedOut = true;
          return;
        }

        const symbol = symbols[index];
        attempted += 1;
        const symbolBudgetMs = Math.max(500, Math.min(symbolTimeoutDefault, remainingBefore));
        try {
          const computed = await this.withSymbolTimeout(
            symbol,
            this.computeSymbol(symbol),
            symbolBudgetMs
          );
          computations.push(computed);
        } catch (error) {
          if (error instanceof AnalysisSymbolTimeoutError) {
            timedOut = true;
            return;
          }
          continue;
        }
      }
    };

    await Promise.all(Array.from({ length: workerCount }, () => runWorker()));

    const analyses = this.rankComputations(computations)
      .slice(0, topN)
      .map(({ entry, scoreCard }) => ({
        snapshot: entry.snapshot,
        featureVector: entry.feature,
        scoreCard,
        chain: entry.chain,
        evidence: entry.evidence
      }));

    const timeoutReason =
      timedOut || computations.length < symbols.length
        ? `Analysis budget reached (${computations.length}/${symbols.length} symbols completed, budget ${budgetLabelMs}).`
        : null;

    return {
      analyses,
      attemptedSymbols: attempted,
      completedSymbols: computations.length,
      timedOut: timedOut || computations.length < symbols.length,
      timeoutReason
    };
  }

  private async computeSymbol(symbol: string): Promise<SymbolComputation> {
    const normalizedSymbol = symbol.toUpperCase();
    const market = await this.marketData.getSymbolSnapshotAndClosesDetailed(normalizedSymbol, 90);
    const optionChain = await this.marketData.getOptionChainDetailed(normalizedSymbol, market.snapshot.last);
    const context = await this.marketData.getContextFeaturesDetailed(normalizedSymbol);
    const feature = this.featureEngine.buildFeatureVector(
      market.snapshot,
      market.closes,
      optionChain.chain,
      {
        newsSentiment: context.newsSentiment,
        newsVelocity24h: context.newsVelocity24h,
        newsSentimentDispersion: context.newsSentimentDispersion,
        newsFreshness: context.newsFreshness,
        eventBias: context.eventBias,
        eventRisk: context.eventRisk,
        macroRegime: context.macroRegime
      },
      {
        dailyBars: market.dailyBars
      }
    );

    return {
      symbol: normalizedSymbol,
      snapshot: market.snapshot,
      feature,
      chain: optionChain.chain,
      evidence: {
        market,
        optionChain,
        context
      }
    };
  }

  private rankComputations(computations: SymbolComputation[]): Array<{ entry: SymbolComputation; scoreCard: ScoreCard }> {
    const enriched = this.enrichCrossSectionalSignals(computations);
    const features = enriched.map((entry) => entry.feature);
    const scores = this.scoringEngine.scoreUniverse(features);
    const scoreBySymbol = new Map(scores.map((score) => [score.symbol, score]));

    return enriched
      .filter((entry) => scoreBySymbol.has(entry.symbol))
      .map((entry) => ({
        entry,
        scoreCard: scoreBySymbol.get(entry.symbol) as ScoreCard
      }))
      .sort((left, right) => right.scoreCard.compositeScore - left.scoreCard.compositeScore);
  }

  async scanUniverse(universe: string[], topN = 10): Promise<SymbolAnalysis[]> {
    const detailed = await this.scanUniverseDetailed(universe, topN);
    return detailed.map((entry) => ({
      snapshot: entry.snapshot,
      featureVector: entry.featureVector,
      scoreCard: entry.scoreCard
    }));
  }

  async scanUniverseDetailed(universe: string[], topN = 10): Promise<DetailedSymbolAnalysis[]> {
    const symbols = [...new Set(universe.map((symbol) => symbol.toUpperCase()))];
    const computations = await this.mapConcurrent(symbols, this.symbolComputeConcurrency(), (symbol) =>
      this.computeSymbol(symbol)
    );

    return this.rankComputations(computations)
      .slice(0, topN)
      .map(({ entry, scoreCard }) => ({
        snapshot: entry.snapshot,
        featureVector: entry.feature,
        scoreCard,
        chain: entry.chain,
        evidence: entry.evidence
      }));
  }

  async scanUniverseWithDiscovery(
    universe: string[],
    topN = 10,
    options?: {
      ibkrScanCode?: IbkrScannerCodeSetting;
      analysisBudgetMs?: number;
      perSymbolTimeoutMs?: number;
      discoveryTimeoutMs?: number;
    }
  ): Promise<ScanUniverseWithDiscoveryResult> {
    const normalizedUniverse = [...new Set(universe.map((symbol) => symbol.toUpperCase()))];
    const requestKey = JSON.stringify({
      topN: Math.max(1, Math.min(100, Math.round(topN))),
      ibkrScanCode: options?.ibkrScanCode ?? null,
      universe: normalizedUniverse
    });

    if (this.scanWithDiscoveryInFlight) {
      if (this.scanWithDiscoveryInFlightKey === requestKey) {
        return await this.scanWithDiscoveryInFlight;
      }
      try {
        await this.scanWithDiscoveryInFlight;
      } catch {
        // Allow subsequent run to proceed after failed in-flight request.
      }
    }

    const run = async (): Promise<ScanUniverseWithDiscoveryResult> => {
    type DynamicUniverseResult = Awaited<ReturnType<MarketDataService["buildDynamicUniverse"]>>;
    const discoveryTimeoutMs = Number(options?.discoveryTimeoutMs ?? (settings.ibkrEnabled ? 9_000 : 5_000));

    let resolved: DynamicUniverseResult;
    try {
      resolved = await this.withDiscoveryTimeout(
        this.marketData.buildDynamicUniverse(universe, topN, options),
        discoveryTimeoutMs
      );
    } catch (error) {
      if (!(error instanceof AnalysisDiscoveryTimeoutError)) throw error;
      resolved = {
        symbols: normalizedUniverse,
        discoveredSymbols: [],
        scannerUsed: false,
        scannerSource: "none",
        ibkrScanCode: settings.ibkrEnabled
          ? options?.ibkrScanCode ?? DEFAULT_IBKR_SCANNER_CODE
          : null,
        scannerProvidersUsed: [],
        scannerProvidersTried: [],
        scannerProviderRanking: [],
        scannerFallbackReason: `dynamic_universe_timeout_${error.timeoutMs}ms`
      };
    }
    const analysisBudgetMs = Number(options?.analysisBudgetMs ?? 0);
    const perSymbolTimeoutMs = Number(
      options?.perSymbolTimeoutMs ?? (settings.ibkrEnabled ? 4_500 : 3_000)
    );
    const budgeted =
      Number.isFinite(analysisBudgetMs) && analysisBudgetMs > 0
        ? await this.scanUniverseDetailedWithBudget(
            resolved.symbols,
            topN,
            analysisBudgetMs,
            perSymbolTimeoutMs
          )
        : null;
    const detailedAnalyses =
      budgeted?.analyses ?? (await this.scanUniverseDetailed(resolved.symbols, topN));
    const analyses = detailedAnalyses.map((entry) => ({
      snapshot: entry.snapshot,
      featureVector: entry.featureVector,
      scoreCard: entry.scoreCard
    }));

    return {
      analyses,
      detailedAnalyses,
      evaluatedUniverse: resolved.symbols,
      discoveredSymbols: resolved.discoveredSymbols,
      scannerUsed: resolved.scannerUsed,
      scannerSource: resolved.scannerSource,
      ibkrScanCode: resolved.ibkrScanCode,
      scannerProvidersUsed: resolved.scannerProvidersUsed,
      scannerProvidersTried: resolved.scannerProvidersTried,
      scannerProviderRanking: resolved.scannerProviderRanking,
      scannerFallbackReason: resolved.scannerFallbackReason,
      analysisTimedOut: budgeted?.timedOut ?? false,
      analysisAttemptedSymbols: budgeted?.attemptedSymbols ?? resolved.symbols.length,
      analysisCompletedSymbols: budgeted?.completedSymbols ?? resolved.symbols.length,
      analysisTimeoutReason: budgeted?.timeoutReason ?? null
    };
    };

    const promise = run().finally(() => {
      if (this.scanWithDiscoveryInFlight === promise) {
        this.scanWithDiscoveryInFlight = null;
        this.scanWithDiscoveryInFlightKey = null;
      }
    });

    this.scanWithDiscoveryInFlight = promise;
    this.scanWithDiscoveryInFlightKey = requestKey;
    return await promise;
  }

  async scoreSymbol(symbol: string, universe: string[]): Promise<{ feature: FeatureVector; scoreCard: ScoreCard }> {
    const normalizedSymbol = symbol.toUpperCase();
    const normalized = [
      ...new Set((universe.length > 0 ? universe : [normalizedSymbol]).map((item) => item.toUpperCase()))
    ];
    const computations = await this.mapConcurrent(normalized, this.symbolComputeConcurrency(), (item) =>
      this.computeSymbol(item)
    );

    const featureBySymbol = new Map(computations.map((entry) => [entry.symbol, entry.feature]));
    const scores = this.scoringEngine.scoreUniverse(computations.map((entry) => entry.feature));
    const scoreBySymbol = new Map(scores.map((score) => [score.symbol, score]));

    if (!featureBySymbol.has(normalizedSymbol) || !scoreBySymbol.has(normalizedSymbol)) {
      const single = await this.computeSymbol(normalizedSymbol);
      return {
        feature: single.feature,
        scoreCard: this.scoringEngine.scoreSingle(single.feature, computations.map((entry) => entry.feature))
      };
    }

    return {
      feature: featureBySymbol.get(normalizedSymbol) as FeatureVector,
      scoreCard: scoreBySymbol.get(normalizedSymbol) as ScoreCard
    };
  }

  async decideSymbol(symbol: string, universe: string[]): Promise<{
    feature: FeatureVector;
    scoreCard: ScoreCard;
    decisionCard: DecisionCard;
    chain: OptionContractSnapshot[];
    optionChainSource: "ibkr_option_chain" | "alpaca_option_chain" | "synthetic_option_chain";
  }> {
    const normalizedSymbol = symbol.toUpperCase();
    const normalized = [
      ...new Set((universe.length > 0 ? universe : [normalizedSymbol]).map((item) => item.toUpperCase()))
    ];
    const computations = await this.mapConcurrent(normalized, this.symbolComputeConcurrency(), (item) =>
      this.computeSymbol(item)
    );
    let target = computations.find((entry) => entry.symbol === normalizedSymbol);
    if (!target) {
      target = await this.computeSymbol(normalizedSymbol);
      computations.push(target);
    }

    const scores = this.scoringEngine.scoreUniverse(computations.map((entry) => entry.feature));
    const scoreBySymbol = new Map(scores.map((score) => [score.symbol, score]));
    const scoreCard = scoreBySymbol.get(normalizedSymbol) as ScoreCard;
    const decisionCard = await this.decisionEngine.decide(target.feature, scoreCard);

    return {
      feature: target.feature,
      scoreCard,
      decisionCard,
      chain: target.chain,
      optionChainSource: target.evidence.optionChain.source
    };
  }
}
