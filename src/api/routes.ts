import type { FastifyInstance, FastifyReply, FastifyRequest } from "fastify";

import { settings } from "../core/config";
import type { DetailedSymbolAnalysis } from "../services/analysisService";
import type { BotPolicy } from "../services/runtimePolicyService";
import type { ApiRequestLogEntry } from "../storage/apiRequestLogStore";
import type { DecisionCard, FeatureVector, OptionContractSnapshot, ScoreCard } from "../types/models";
import {
  approveOrderRequestSchema,
  apiRequestLogsQuerySchema,
  backtestRequestSchema,
  botPolicyPatchSchema,
  decisionRequestSchema,
  envConfigPatchSchema,
  ibkrLaunchRequestSchema,
  killSwitchUpdateSchema,
  marketDataDiagnosticsQuerySchema,
  proposeOrderRequestSchema,
  recommendationsQuerySchema,
  scanRequestSchema,
  scoreRequestSchema
} from "../types/schemas";

interface ActionEndpointDefinition {
  id: string;
  label: string;
  method: string;
  endpoint: string;
}

const ACTION_ENDPOINTS: ActionEndpointDefinition[] = [
  { id: "manual_scan", label: "Manual Scan", method: "POST", endpoint: "/scan" },
  { id: "recommendations_refresh", label: "Recommendations Refresh", method: "GET", endpoint: "/recommendations" },
  { id: "manual_decision", label: "Manual Decision", method: "POST", endpoint: "/decision" },
  { id: "run_backtest", label: "Run Backtest", method: "POST", endpoint: "/backtest" },
  { id: "propose_order", label: "Propose Order", method: "POST", endpoint: "/propose-order" },
  { id: "approve_order", label: "Approve/Reject Order", method: "POST", endpoint: "/approve-order" },
  { id: "refresh_account", label: "Refresh Account Summary", method: "GET", endpoint: "/account-summary" },
  { id: "refresh_acceptance_gate", label: "Refresh Acceptance Gate", method: "GET", endpoint: "/acceptance-gate" },
  { id: "refresh_positions", label: "Refresh Positions", method: "GET", endpoint: "/positions" },
  { id: "refresh_ibkr_readiness", label: "Refresh IBKR Readiness", method: "GET", endpoint: "/ibkr-readiness" },
  { id: "refresh_risk", label: "Refresh Risk Status", method: "GET", endpoint: "/risk-status" }
];

const findActionLogTimestamps = (
  logs: ApiRequestLogEntry[],
  method: string,
  endpoint: string
): { lastAttemptAt: string | null; lastSuccessAt: string | null; lastErrorAt: string | null } => {
  const matching = logs.filter((entry) => entry.method === method && entry.endpoint === endpoint);
  const first = matching[0];
  const success = matching.find((entry) => entry.status === "success");
  const error = matching.find((entry) => entry.status === "error");
  return {
    lastAttemptAt: first?.startedAt ?? null,
    lastSuccessAt: success?.startedAt ?? null,
    lastErrorAt: error?.startedAt ?? null
  };
};

const normalizeUniverse = (
  defaults: string[],
  universe: string[] | undefined,
  symbol?: string
): string[] => {
  const base = (universe ?? defaults).map((item) => item.toUpperCase());
  if (symbol && !base.includes(symbol)) return [symbol, ...base];
  return base;
};

const isTestRuntime = (): boolean =>
  settings.appEnv === "test" || process.env.NODE_ENV === "test" || Boolean(process.env.BUN_TEST);

const requireIbkrConnection = async (
  app: FastifyInstance,
  reply: FastifyReply,
  operation: string,
  timeoutMs = 4_000
): Promise<boolean> => {
  if (isTestRuntime()) return true;

  const connectivity = await app.services.ibkrAdapter.checkConnectivity(timeoutMs);
  app.services.executionGateway.notifyConnectivityStatus(connectivity);
  if (connectivity.reachable) return true;

  await reply.code(503).send({
    error: `IBKR connection is required for ${operation}.`,
    operation,
    ibkr: connectivity
  });
  return false;
};

type EntitlementState =
  | "live"
  | "delayed"
  | "blocked"
  | "connectivity"
  | "invalid_contract"
  | "error"
  | "unknown";

interface MarketDataSignal {
  state: EntitlementState;
  lastAt: string | null;
  delayedOnly: boolean | null;
  note: string | null;
}

interface MarketDataSymbolDiagnostics {
  symbol: string;
  quote: MarketDataSignal;
  option: MarketDataSignal;
}

const createEmptySignal = (): MarketDataSignal => ({
  state: "unknown",
  lastAt: null,
  delayedOnly: null,
  note: null
});

const isSubscriptionMessage = (message: string): boolean => {
  const lower = message.toLowerCase();
  return (
    lower.includes("not subscribed") ||
    lower.includes("additional subscription") ||
    lower.includes("market data requires") ||
    lower.includes("subscription")
  );
};

const isConnectivityMessage = (message: string): boolean => {
  const lower = message.toLowerCase();
  return (
    lower.includes("timeout") ||
    lower.includes("timed out") ||
    lower.includes("failed to connect") ||
    lower.includes("not connected") ||
    lower.includes("socket")
  );
};

const isInvalidContractMessage = (message: string): boolean =>
  message.toLowerCase().includes("no security definition") ||
  message.toLowerCase().includes("contract description specified");

const asRecord = (value: unknown): Record<string, unknown> | null =>
  value && typeof value === "object" ? (value as Record<string, unknown>) : null;

const extractSymbolFromIbkrLog = (entry: ApiRequestLogEntry): string | null => {
  const payload = asRecord(entry.requestPayload);
  if (payload && typeof payload.symbol === "string" && payload.symbol.trim().length > 0) {
    return payload.symbol.trim().toUpperCase();
  }

  const reasonMatch = entry.reason.match(/\bfor\s+([A-Z][A-Z0-9.\-]{0,14})\b/i);
  if (reasonMatch?.[1]) return reasonMatch[1].toUpperCase();
  return null;
};

const classifySignalFromLog = (entry: ApiRequestLogEntry): MarketDataSignal => {
  const payload = asRecord(entry.requestPayload);
  const delayedOnly =
    payload && typeof payload.delayedOnly === "boolean" ? payload.delayedOnly : null;
  const errorMessage = String(entry.errorMessage ?? "").trim();

  if (entry.status === "success") {
    return {
      state: delayedOnly ? "delayed" : "live",
      lastAt: entry.startedAt,
      delayedOnly,
      note: delayedOnly
        ? "Delayed market data snapshot"
        : "Live market data snapshot"
    };
  }

  if (!errorMessage) {
    return {
      state: "error",
      lastAt: entry.startedAt,
      delayedOnly,
      note: "Unknown IBKR market data error."
    };
  }

  if (isInvalidContractMessage(errorMessage)) {
    return {
      state: "invalid_contract",
      lastAt: entry.startedAt,
      delayedOnly,
      note: errorMessage
    };
  }

  if (isSubscriptionMessage(errorMessage)) {
    return {
      state: "blocked",
      lastAt: entry.startedAt,
      delayedOnly,
      note: errorMessage
    };
  }

  if (isConnectivityMessage(errorMessage)) {
    return {
      state: "connectivity",
      lastAt: entry.startedAt,
      delayedOnly,
      note: errorMessage
    };
  }

  return {
    state: "error",
    lastAt: entry.startedAt,
    delayedOnly,
    note: errorMessage
  };
};

interface RecommendationIndicatorEvidence {
  id: string;
  label: string;
  available: boolean;
  source: string;
  value: Record<string, unknown>;
  note?: string;
}

interface RecommendationGateEvidence {
  id: string;
  label: string;
  passed: boolean;
  severity: "hard" | "soft";
  details: string;
  threshold?: number | string;
  actual?: number | string;
}

interface RecommendationRow {
  rank: number;
  symbol: string;
  actionable: boolean;
  suggestedAction: string;
  confidence: number;
  rationale: string;
  vetoFlags: string[];
  metrics: {
    compositeScore: number;
    directionalUpProb: number;
    directionalDownProb: number;
    spreadPct: number;
  };
  evidence: ReturnType<typeof buildRecommendationEvidence>;
}

interface AutoProposalCandidate {
  rank: number;
  symbol: string;
  action: "CALL" | "PUT";
  decisionCard: DecisionCard;
  chain: OptionContractSnapshot[];
}

interface RecommendationsScannerSummary {
  requestedUniverseSize: number;
  evaluatedUniverseSize: number;
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
  ibkrScanCode: string | null;
  scannerProvidersUsed: string[];
  scannerProvidersTried: string[];
  scannerProviderRanking: Array<{ provider: string; score: number }>;
  scannerFallbackReason: string | null;
}

interface RecommendationsExecutionMeta {
  timedOut: boolean;
  timeoutMs: number;
  source: "fresh" | "fresh_partial" | "cache_fallback" | "empty_fallback";
  timeoutReason: string | null;
  computeMs: number;
  elapsedMs: number;
  fallbackFromGeneratedAt: string | null;
  errors: Array<{
    stage: "scanner" | "analysis" | "decision" | "timeout" | "internal";
    symbol: string | null;
    message: string;
    at: string;
  }>;
  autoProposal: {
    enabled: boolean;
    attempted: number;
    created: number;
    skipped: number;
    failed: number;
    reason: string | null;
    outcomes: Array<{
      symbol: string;
      action: "CALL" | "PUT";
      status: "created" | "skipped" | "failed";
      message: string;
      orderId: string | null;
    }>;
  };
}

interface RecommendationsResponsePayload {
  generatedAt: string;
  policySnapshot: {
    minCompositeScore: number;
    minDirectionalProbability: number;
    dteMin: number;
    dteMax: number;
    ibkrScanCode: string | null;
    analysisDataProvider: string;
    autoProposeActionable: boolean;
  };
  scanner: RecommendationsScannerSummary;
  recommendations: RecommendationRow[];
  execution: RecommendationsExecutionMeta;
}

const DEFAULT_RECOMMENDATIONS_TIMEOUT_MS = 120_000;

const recommendationsTimeoutMs = (): number => {
  const parsed = Number(settings.recommendationsTimeoutMs);
  if (!Number.isFinite(parsed)) return DEFAULT_RECOMMENDATIONS_TIMEOUT_MS;
  return Math.max(0, Math.round(parsed));
};

const buildRecommendationRunError = (
  stage: "scanner" | "analysis" | "decision" | "timeout" | "internal",
  message: string,
  symbol: string | null = null
): RecommendationsExecutionMeta["errors"][number] => ({
  stage,
  symbol,
  message,
  at: new Date().toISOString()
});

class RecommendationsTimeoutError extends Error {
  constructor(
    public readonly stage: string,
    public readonly timeoutMs: number
  ) {
    super(`Recommendations timed out during ${stage} after ${timeoutMs}ms.`);
    this.name = "RecommendationsTimeoutError";
  }
}

const isRecommendationsTimeoutError = (
  error: unknown
): error is RecommendationsTimeoutError => error instanceof RecommendationsTimeoutError;

const withTimeout = async <T>(
  promise: Promise<T>,
  timeoutMs: number,
  stage: string
): Promise<T> => {
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) return await promise;

  let timer: ReturnType<typeof setTimeout> | null = null;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_resolve, reject) => {
        timer = setTimeout(() => {
          reject(new RecommendationsTimeoutError(stage, Math.round(timeoutMs)));
        }, Math.max(1, Math.round(timeoutMs)));
      })
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
};

const average = (values: number[]): number =>
  values.length > 0 ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;

const clamp01 = (value: number): number => {
  if (!Number.isFinite(value)) return 0;
  if (value < 0) return 0;
  if (value > 1) return 1;
  return value;
};

const trailingReturn = (closes: number[], lookback: number): number => {
  if (!Array.isArray(closes) || closes.length <= lookback) return 0;
  const last = closes[closes.length - 1];
  const previous = closes[closes.length - 1 - lookback];
  if (!Number.isFinite(last) || !Number.isFinite(previous) || previous <= 0) return 0;
  return last / previous - 1;
};

const parseBenchmarkFromNotes = (notes: string[]): {
  benchmarkSymbol: string | null;
  benchmarkRet20: number | null;
  benchmarkRet60: number | null;
} => {
  const line = notes.find((value) => value.startsWith("relative_strength benchmark="));
  if (!line) {
    return {
      benchmarkSymbol: null,
      benchmarkRet20: null,
      benchmarkRet60: null
    };
  }

  const match = line.match(/benchmark=([A-Z0-9.\-]+)\s+ret20=([\-0-9.]+)\s+ret60=([\-0-9.]+)/);
  if (!match) {
    return {
      benchmarkSymbol: null,
      benchmarkRet20: null,
      benchmarkRet60: null
    };
  }

  const benchmarkRet20 = Number(match[2]);
  const benchmarkRet60 = Number(match[3]);
  return {
    benchmarkSymbol: match[1] ?? null,
    benchmarkRet20: Number.isFinite(benchmarkRet20) ? benchmarkRet20 : null,
    benchmarkRet60: Number.isFinite(benchmarkRet60) ? benchmarkRet60 : null
  };
};

const summarizeOptionChain = (analysis: DetailedSymbolAnalysis): {
  totalContracts: number;
  callContracts: number;
  putContracts: number;
  avgSpreadPct: number;
  avgVolume: number;
  avgOpenInterest: number;
} => {
  const rows = analysis.chain;
  const calls = rows.filter((row) => row.right === "CALL");
  const puts = rows.filter((row) => row.right === "PUT");
  const spreadPcts = rows.map((row) => {
    const mid = row.bid > 0 && row.ask > 0 ? (row.bid + row.ask) / 2 : row.last;
    return mid > 0 ? (row.ask - row.bid) / mid : 0;
  });
  return {
    totalContracts: rows.length,
    callContracts: calls.length,
    putContracts: puts.length,
    avgSpreadPct: average(spreadPcts),
    avgVolume: average(rows.map((row) => row.volume)),
    avgOpenInterest: average(rows.map((row) => row.openInterest))
  };
};

const buildRecommendationEvidence = (
  analysis: DetailedSymbolAnalysis,
  context?: {
    minCompositeScore?: number;
    minDirectionalProbability?: number;
    suggestedAction?: string;
    confidence?: number;
  }
): {
  dataQuality: {
    passed: boolean;
    minimumAvailableIndicators: number;
    hasCoreMarketData: boolean;
    hasCoreOptionsData: boolean;
  };
  gateChecks: RecommendationGateEvidence[];
  indicatorCoverage: {
    available: number;
    total: number;
    missing: string[];
  };
  scoreDecomposition: {
    techScore: number;
    optionsScore: number;
    sentimentScore: number;
    riskPenalty: number;
    weighted: {
      tech: number;
      options: number;
      sentiment: number;
      risk: number;
    };
    confidence: number | null;
  };
  indicators: RecommendationIndicatorEvidence[];
  snapshot: DetailedSymbolAnalysis["snapshot"];
  scoreCard: DetailedSymbolAnalysis["scoreCard"];
  featureVector: DetailedSymbolAnalysis["featureVector"];
  provenance: {
    benchmarkSymbol: string | null;
    benchmarkReturns: {
      d20: number | null;
      d60: number | null;
    };
    signalSources: {
      relativeStrength: string;
      regimeStability: string;
      optionsQuality: string;
      trendStrength: string;
      volumeConfirmation: string;
      newsFlow: string;
      calibration: string;
    };
    gateModelVersion: string;
  };
  optionChain: {
    source: string;
    summary: ReturnType<typeof summarizeOptionChain>;
    sampleContracts: DetailedSymbolAnalysis["chain"];
  };
  context: {
    newsSentiment: number;
    newsVelocity24h: number;
    newsSentimentDispersion: number;
    newsFreshness: number;
    eventBias: number;
    eventRisk: number;
    macroRegime: number;
    newsArticles: DetailedSymbolAnalysis["evidence"]["context"]["articles"];
    raw: DetailedSymbolAnalysis["evidence"]["context"]["raw"];
  };
  sources: {
    quote: string;
    closes: string;
    bars: string;
    optionChain: string;
    newsSentiment: string;
    event: string;
    macro: string;
  };
  notes: string[];
} => {
  const marketEvidence = analysis.evidence.market;
  const optionEvidence = analysis.evidence.optionChain;
  const contextEvidence = analysis.evidence.context;
  const optionSummary = summarizeOptionChain(analysis);
  const optionQuoteCoverage = analysis.chain.filter(
    (contract) =>
      contract.quoteSource === "ibkr_option_quote" ||
      contract.quoteSource === "alpaca_option_quote"
  ).length;

  const hasRealQuoteSource =
    marketEvidence.sources.quote === "ibkr_quote" ||
    marketEvidence.sources.quote === "alpaca_quote";
  const hasRealClosesSource =
    marketEvidence.sources.closes === "ibkr_historical" ||
    marketEvidence.sources.closes === "alpaca_historical";
  const hasCoreMarketData = hasRealQuoteSource && hasRealClosesSource;
  const hasCoreOptionsData =
    optionEvidence.source === "ibkr_option_chain" ||
    optionEvidence.source === "alpaca_option_chain";
  const ownRet20 = trailingReturn(marketEvidence.closes, 20);
  const ownRet60 = trailingReturn(marketEvidence.closes, 60);
  const benchmarkMeta = parseBenchmarkFromNotes(marketEvidence.notes);
  const hasRelativeStrengthBenchmark = Boolean(benchmarkMeta.benchmarkSymbol);
  const minCompositeScore = Number.isFinite(Number(context?.minCompositeScore))
    ? Number(context?.minCompositeScore)
    : 63;
  const minDirectionalProbability = Number.isFinite(Number(context?.minDirectionalProbability))
    ? Number(context?.minDirectionalProbability)
    : 0.54;
  const suggestedAction = String(context?.suggestedAction ?? "NO_TRADE").toUpperCase();
  const selectedDirectionalProb =
    suggestedAction === "PUT"
      ? analysis.featureVector.directionalDownProb
      : analysis.featureVector.directionalUpProb;
  const dailyBars = Array.isArray(marketEvidence.dailyBars) ? marketEvidence.dailyBars : [];
  const averageDailyVolume20d = average(
    dailyBars
      .slice(-20)
      .map((bar) => Number(bar.volume))
      .filter((value) => Number.isFinite(value) && value > 0)
  );
  const hasDailyBars = dailyBars.length >= 15;
  const hasVolumeContext = hasCoreMarketData && Number.isFinite(averageDailyVolume20d) && averageDailyVolume20d > 0;
  const hasNewsFeed =
    contextEvidence.sources.newsSentiment === "alpha_vantage_news_sentiment" ||
    contextEvidence.sources.newsSentiment === "alpaca_news_sentiment";

  const indicators: RecommendationIndicatorEvidence[] = [
    {
      id: "trend_momentum",
      label: "Trend + Momentum",
      available: hasCoreMarketData,
      source: `${marketEvidence.sources.quote}/${marketEvidence.sources.closes}`,
      value: {
        momentum: analysis.featureVector.momentum,
        trend: analysis.featureVector.trend,
        breakoutZ: analysis.featureVector.breakoutZ,
        pctChange1d: analysis.snapshot.pctChange1d
      },
      note: hasCoreMarketData ? undefined : "Requires non-synthetic quote + historical closes."
    },
    {
      id: "volatility_regime",
      label: "Volatility + Regime",
      available: hasCoreMarketData,
      source: `${marketEvidence.sources.quote}/${marketEvidence.sources.closes}`,
      value: {
        atrPct: analysis.featureVector.atrPct,
        realizedVolPercentile: analysis.featureVector.realizedVolPercentile,
        regime: analysis.featureVector.regime,
        ivRvSpread: analysis.featureVector.ivRvSpread,
        impliedVol: analysis.snapshot.impliedVol,
        realizedVol: analysis.snapshot.realizedVol
      },
      note: hasCoreMarketData ? undefined : "Requires non-synthetic quote + historical closes."
    },
    {
      id: "trend_strength",
      label: "Trend Strength (ADX 14)",
      available: hasCoreMarketData && hasDailyBars,
      source: `${marketEvidence.sources.bars}/${marketEvidence.sources.closes}`,
      value: {
        adx14: analysis.featureVector.adx14,
        dailyBars: dailyBars.length
      },
      note:
        hasCoreMarketData && hasDailyBars
          ? undefined
          : "Requires 15+ real daily bars to compute ADX."
    },
    {
      id: "volume_confirmation",
      label: "Volume Confirmation (RVOL 20D)",
      available: hasVolumeContext,
      source: `${marketEvidence.sources.quote}/${marketEvidence.sources.bars}`,
      value: {
        relativeVolume20d: analysis.featureVector.relativeVolume20d,
        currentVolume: analysis.snapshot.volume,
        averageDailyVolume20d
      },
      note: hasVolumeContext ? undefined : "Requires non-synthetic quote and 20D volume context."
    },
    {
      id: "relative_strength",
      label: "Relative Strength vs Benchmark",
      available: hasCoreMarketData && hasRelativeStrengthBenchmark,
      source: `${marketEvidence.sources.closes}/${benchmarkMeta.benchmarkSymbol ?? "none"}`,
      value: {
        benchmarkSymbol: benchmarkMeta.benchmarkSymbol,
        symbolReturn20d: ownRet20,
        symbolReturn60d: ownRet60,
        benchmarkReturn20d: benchmarkMeta.benchmarkRet20,
        benchmarkReturn60d: benchmarkMeta.benchmarkRet60,
        relativeStrength20d: analysis.featureVector.relativeStrength20d,
        relativeStrength60d: analysis.featureVector.relativeStrength60d
      },
      note:
        hasCoreMarketData && hasRelativeStrengthBenchmark
          ? undefined
          : "Relative-strength benchmark context unavailable."
    },
    {
      id: "options_microstructure",
      label: "Options Microstructure",
      available: hasCoreOptionsData && optionQuoteCoverage > 0,
      source: optionEvidence.source,
      value: {
        liquidity: analysis.featureVector.liquidity,
        flow: analysis.featureVector.flow,
        skew: analysis.featureVector.skew,
        chainSummary: optionSummary,
        quoteEnrichment: {
          enrichedContracts: optionQuoteCoverage,
          totalContracts: analysis.chain.length
        }
      },
      note:
        !hasCoreOptionsData
          ? "Requires non-synthetic options chain."
          : optionQuoteCoverage > 0
            ? undefined
            : "Options chain metadata is available, but quote enrichment returned no tradable contract snapshots."
    },
    {
      id: "options_quality",
      label: "Options Quality",
      available: hasCoreOptionsData,
      source: optionEvidence.source,
      value: {
        optionsQuality: analysis.featureVector.optionsQuality,
        liquidity: analysis.featureVector.liquidity,
        flow: analysis.featureVector.flow,
        skew: analysis.featureVector.skew,
        avgSpreadPct: optionSummary.avgSpreadPct,
        avgVolume: optionSummary.avgVolume,
        avgOpenInterest: optionSummary.avgOpenInterest
      },
      note: hasCoreOptionsData ? undefined : "Requires non-synthetic options chain."
    },
    {
      id: "directional_model",
      label: "Directional Model",
      available: hasCoreMarketData && hasCoreOptionsData,
      source: `${marketEvidence.sources.quote}/${optionEvidence.source}`,
      value: {
        directionalUpProb: analysis.featureVector.directionalUpProb,
        directionalDownProb: analysis.featureVector.directionalDownProb
      },
      note:
        hasCoreMarketData && hasCoreOptionsData
          ? undefined
          : "Directional probabilities are computed, but backing market data is incomplete."
    },
    {
      id: "regime_stability",
      label: "Regime Stability",
      available: hasCoreMarketData,
      source: `${marketEvidence.sources.closes}/${contextEvidence.sources.macro}`,
      value: {
        regime: analysis.featureVector.regime,
        regimeStability: analysis.featureVector.regimeStability,
        macroRegime: analysis.featureVector.macroRegime,
        realizedVolPercentile: analysis.featureVector.realizedVolPercentile,
        atrPct: analysis.featureVector.atrPct
      },
      note: hasCoreMarketData ? undefined : "Requires non-synthetic quote + historical closes."
    },
    {
      id: "news_sentiment",
      label: "News Sentiment",
      available: hasNewsFeed,
      source: contextEvidence.sources.newsSentiment,
      value: {
        newsSentiment: analysis.featureVector.newsSentiment,
        articleCount: contextEvidence.articles.length
      },
      note:
        hasNewsFeed ? undefined : "News sentiment feed unavailable."
    },
    {
      id: "news_flow",
      label: "News Flow Quality",
      available: hasNewsFeed,
      source: contextEvidence.sources.newsSentiment,
      value: {
        newsVelocity24h: analysis.featureVector.newsVelocity24h,
        newsFreshness: analysis.featureVector.newsFreshness,
        newsSentimentDispersion: analysis.featureVector.newsSentimentDispersion,
        articleCount: contextEvidence.articles.length
      },
      note:
        hasNewsFeed
          ? undefined
          : "News flow metrics unavailable because sentiment feed is unavailable."
    },
    {
      id: "event_context",
      label: "Event Context",
      available: contextEvidence.sources.event === "sec_edgar",
      source: contextEvidence.sources.event,
      value: {
        eventBias: analysis.featureVector.eventBias,
        eventRisk: analysis.featureVector.eventRisk,
        latestForm: contextEvidence.raw.event.latestForm,
        latestFilingDate: contextEvidence.raw.event.latestFilingDate
      },
      note: contextEvidence.sources.event === "sec_edgar" ? undefined : "SEC filing context unavailable."
    },
    {
      id: "macro_regime",
      label: "Macro Regime",
      available: contextEvidence.sources.macro === "fred",
      source: contextEvidence.sources.macro,
      value: {
        macroRegime: analysis.featureVector.macroRegime,
        components: contextEvidence.raw.macro.components
      },
      note: contextEvidence.sources.macro === "fred" ? undefined : "FRED macro context unavailable."
    },
    {
      id: "calibration",
      label: "Confidence Calibration",
      available: true,
      source: "decision_engine_calibrated_confidence_v2",
      value: {
        confidence: Number.isFinite(Number(context?.confidence)) ? Number(context?.confidence) : null,
        directionalEdge: Math.abs(
          analysis.featureVector.directionalUpProb - analysis.featureVector.directionalDownProb
        ),
        optionsQuality: analysis.featureVector.optionsQuality,
        regimeStability: analysis.featureVector.regimeStability,
        eventRisk: analysis.featureVector.eventRisk,
        gapRisk: analysis.featureVector.gapRisk
      }
    }
  ];

  const available = indicators.filter((indicator) => indicator.available).length;
  const total = indicators.length;
  const minimumAvailableIndicators = Math.max(3, Math.ceil(total * 0.35));
  const missing = indicators.filter((indicator) => !indicator.available).map((indicator) => indicator.label);
  const hasAnyCoreFeed = hasCoreMarketData || hasCoreOptionsData;
  const passed = hasAnyCoreFeed && available >= minimumAvailableIndicators;

  const notes = [...marketEvidence.notes, ...optionEvidence.notes, ...contextEvidence.notes];
  if (!hasCoreMarketData) {
    notes.push("Core market data is synthetic or unavailable for this run.");
  } else if (
    marketEvidence.sources.quote === "alpaca_quote" ||
    marketEvidence.sources.closes === "alpaca_historical"
  ) {
    notes.push("Core market data is using Alpaca fallback sources.");
  }
  if (!hasCoreOptionsData) {
    notes.push("Core options data is synthetic or unavailable for this run.");
  }
  if (!hasAnyCoreFeed) {
    notes.push("Both core market and core options feeds are unavailable; confidence is materially reduced.");
  }
  if (!hasRelativeStrengthBenchmark) {
    notes.push("Relative-strength benchmark is unavailable for this symbol snapshot.");
  }

  const scoreDecomposition = {
    techScore: analysis.scoreCard.techScore,
    optionsScore: analysis.scoreCard.optionsScore,
    sentimentScore: analysis.scoreCard.sentimentScore,
    riskPenalty: analysis.scoreCard.riskPenalty,
    weighted: {
      tech: 0.35 * analysis.scoreCard.techScore,
      options: 0.3 * analysis.scoreCard.optionsScore,
      sentiment: 0.2 * analysis.scoreCard.sentimentScore,
      risk: -0.15 * analysis.scoreCard.riskPenalty
    },
    confidence: Number.isFinite(Number(context?.confidence)) ? Number(context?.confidence) : null
  };

  const gateChecks: RecommendationGateEvidence[] = [
    {
      id: "core_market_data",
      label: "Core Market Data",
      passed: hasCoreMarketData,
      severity: "hard",
      details: hasCoreMarketData
        ? "Quote + closes are non-synthetic."
        : "Quote/history fallback is synthetic or unavailable."
    },
    {
      id: "core_options_data",
      label: "Core Options Data",
      passed: hasCoreOptionsData,
      severity: "hard",
      details: hasCoreOptionsData
        ? "Options chain sourced from IBKR or Alpaca."
        : "Options chain is synthetic or unavailable."
    },
    {
      id: "indicator_coverage",
      label: "Indicator Coverage",
      passed: available >= minimumAvailableIndicators,
      severity: "hard",
      threshold: minimumAvailableIndicators,
      actual: available,
      details: `${available}/${total} indicators available.`
    },
    {
      id: "composite_threshold",
      label: "Composite Threshold",
      passed: analysis.scoreCard.compositeScore >= minCompositeScore,
      severity: "soft",
      threshold: minCompositeScore,
      actual: Number(analysis.scoreCard.compositeScore.toFixed(2)),
      details: `Composite score ${analysis.scoreCard.compositeScore.toFixed(2)} vs minimum ${minCompositeScore}.`
    },
    {
      id: "directional_threshold",
      label: "Directional Probability",
      passed:
        suggestedAction === "NO_TRADE"
          ? Math.max(
              analysis.featureVector.directionalUpProb,
              analysis.featureVector.directionalDownProb
            ) >= minDirectionalProbability
          : selectedDirectionalProb >= minDirectionalProbability,
      severity: "soft",
      threshold: minDirectionalProbability,
      actual: Number(selectedDirectionalProb.toFixed(3)),
      details:
        suggestedAction === "NO_TRADE"
          ? "NO_TRADE candidate; directional threshold shown for strongest side."
          : `${suggestedAction} requires p(direction) >= ${minDirectionalProbability}.`
    },
    {
      id: "data_quality_gate",
      label: "Data Quality Gate",
      passed,
      severity: "hard",
      details: passed
        ? "At least one core feed and indicator minimums satisfied."
        : "Blocked: no core feed available or indicator minimum not met."
    },
    {
      id: "options_quality_floor",
      label: "Options Quality Floor",
      passed: analysis.featureVector.optionsQuality >= -0.1,
      severity: "soft",
      threshold: -0.1,
      actual: Number(analysis.featureVector.optionsQuality.toFixed(3)),
      details: "Penalizes illiquid/low-depth options environments."
    },
    {
      id: "trend_strength_floor",
      label: "Trend Strength Floor (ADX14)",
      passed: analysis.featureVector.adx14 >= 15,
      severity: "soft",
      threshold: 15,
      actual: Number(analysis.featureVector.adx14.toFixed(2)),
      details: "Lower ADX implies weak trend conviction."
    },
    {
      id: "relative_volume_floor",
      label: "Relative Volume Floor (20D)",
      passed: analysis.featureVector.relativeVolume20d >= 0.7,
      severity: "soft",
      threshold: 0.7,
      actual: Number(analysis.featureVector.relativeVolume20d.toFixed(3)),
      details: "Low relative volume can reduce follow-through reliability."
    },
    {
      id: "news_dispersion_guard",
      label: "News Dispersion Guard",
      passed: analysis.featureVector.newsSentimentDispersion <= 0.7,
      severity: "soft",
      threshold: 0.7,
      actual: Number(analysis.featureVector.newsSentimentDispersion.toFixed(3)),
      details: "High sentiment disagreement increases signal uncertainty."
    }
  ];

  return {
    dataQuality: {
      passed,
      minimumAvailableIndicators,
      hasCoreMarketData,
      hasCoreOptionsData
    },
    gateChecks,
    indicatorCoverage: {
      available,
      total,
      missing
    },
    scoreDecomposition,
    indicators,
    snapshot: analysis.snapshot,
    scoreCard: analysis.scoreCard,
    featureVector: analysis.featureVector,
    provenance: {
      benchmarkSymbol: benchmarkMeta.benchmarkSymbol,
      benchmarkReturns: {
        d20: benchmarkMeta.benchmarkRet20,
        d60: benchmarkMeta.benchmarkRet60
      },
      signalSources: {
        relativeStrength: marketEvidence.sources.closes,
        regimeStability: `${marketEvidence.sources.closes}/${contextEvidence.sources.macro}`,
        optionsQuality: optionEvidence.source,
        trendStrength: marketEvidence.sources.bars,
        volumeConfirmation: `${marketEvidence.sources.quote}/${marketEvidence.sources.bars}`,
        newsFlow: contextEvidence.sources.newsSentiment,
        calibration: "decision_engine_calibrated_confidence_v2"
      },
      gateModelVersion: "recommendation_gate_v4"
    },
    optionChain: {
      source: optionEvidence.source,
      summary: optionSummary,
      sampleContracts: analysis.chain.slice(0, 10)
    },
    context: {
      newsSentiment: contextEvidence.newsSentiment,
      newsVelocity24h: contextEvidence.newsVelocity24h,
      newsSentimentDispersion: contextEvidence.newsSentimentDispersion,
      newsFreshness: contextEvidence.newsFreshness,
      eventBias: contextEvidence.eventBias,
      eventRisk: contextEvidence.eventRisk,
      macroRegime: contextEvidence.macroRegime,
      newsArticles: contextEvidence.articles,
      raw: contextEvidence.raw
    },
    sources: {
      quote: marketEvidence.sources.quote,
      closes: marketEvidence.sources.closes,
      bars: marketEvidence.sources.bars,
      optionChain: optionEvidence.source,
      newsSentiment: contextEvidence.sources.newsSentiment,
      event: contextEvidence.sources.event,
      macro: contextEvidence.sources.macro
    },
    notes: [...new Set(notes)].slice(0, 25)
  };
};

type ProposalFailureReason = {
  id: string;
  message: string;
  actual?: number | string | null;
  threshold?: number | string | null;
};

const toFixedNumber = (value: number, decimals: number): number =>
  Number(Number(value).toFixed(decimals));

const buildNoTradeProposalError = (
  symbol: string,
  policy: Pick<BotPolicy, "minCompositeScore" | "minDirectionalProbability">,
  feature: FeatureVector,
  scoreCard: ScoreCard,
  decisionCard: DecisionCard,
  optionChainSource: "ibkr_option_chain" | "alpaca_option_chain" | "synthetic_option_chain"
): {
  error: string;
  reasonCode: "NO_TRADE";
  summary: string;
    diagnostics: {
      symbol: string;
      evaluatedAt: string;
      optionChainSource: "ibkr_option_chain" | "alpaca_option_chain" | "synthetic_option_chain";
      policyThresholds: {
        minCompositeScore: number;
        minDirectionalProbability: number;
      };
    actuals: {
      compositeScore: number;
      directionalUpProb: number;
      directionalDownProb: number;
      strongestDirection: "UP" | "DOWN";
      strongestProbability: number;
    };
  };
  failureReasons: ProposalFailureReason[];
  rationale: string;
  vetoFlags: string[];
  suggestion: string;
} => {
  const normalizedSymbol = symbol.toUpperCase();
  const strongestDirection = feature.directionalUpProb >= feature.directionalDownProb ? "UP" : "DOWN";
  const strongestProbability = Math.max(feature.directionalUpProb, feature.directionalDownProb);

  const failureReasons: ProposalFailureReason[] = [];

  if (scoreCard.compositeScore < policy.minCompositeScore) {
    failureReasons.push({
      id: "composite_threshold",
      message: `Composite score is below threshold (${scoreCard.compositeScore.toFixed(2)} < ${policy.minCompositeScore.toFixed(2)}).`,
      actual: toFixedNumber(scoreCard.compositeScore, 2),
      threshold: toFixedNumber(policy.minCompositeScore, 2)
    });
  }

  if (
    feature.directionalUpProb < policy.minDirectionalProbability &&
    feature.directionalDownProb < policy.minDirectionalProbability
  ) {
    failureReasons.push({
      id: "directional_threshold",
      message: `Directional probability is below threshold for both sides (up=${feature.directionalUpProb.toFixed(3)}, down=${feature.directionalDownProb.toFixed(3)}, required >= ${policy.minDirectionalProbability.toFixed(3)}).`,
      actual: toFixedNumber(strongestProbability, 3),
      threshold: toFixedNumber(policy.minDirectionalProbability, 3)
    });
  }

  if (decisionCard.vetoFlags.length > 0) {
    failureReasons.push({
      id: "llm_or_policy_veto",
      message: `Judge/policy veto flags present: ${decisionCard.vetoFlags.join(", ")}.`
    });
  }

  if (failureReasons.length === 0) {
    failureReasons.push({
      id: "decision_rationale",
      message: decisionCard.rationale || "Decision engine returned NO_TRADE."
    });
  }

  return {
    error: `Order proposal blocked for ${normalizedSymbol}: decision is NO_TRADE.`,
    reasonCode: "NO_TRADE",
    summary:
      "The proposal endpoint re-runs analysis with current market state. A previously actionable recommendation can become NO_TRADE if thresholds, probabilities, or veto rules change.",
    diagnostics: {
      symbol: normalizedSymbol,
      evaluatedAt: decisionCard.timestamp,
      optionChainSource,
      policyThresholds: {
        minCompositeScore: policy.minCompositeScore,
        minDirectionalProbability: policy.minDirectionalProbability
      },
      actuals: {
        compositeScore: toFixedNumber(scoreCard.compositeScore, 2),
        directionalUpProb: toFixedNumber(feature.directionalUpProb, 3),
        directionalDownProb: toFixedNumber(feature.directionalDownProb, 3),
        strongestDirection,
        strongestProbability: toFixedNumber(strongestProbability, 3)
      }
    },
    failureReasons,
    rationale: decisionCard.rationale,
    vetoFlags: [...decisionCard.vetoFlags],
    suggestion:
      "Refresh recommendations and propose only currently actionable rows. Open row evidence to inspect gate coverage and threshold checks."
  };
};

export const registerRoutes = async (app: FastifyInstance): Promise<void> => {
  const recommendationsCache = new Map<string, RecommendationsResponsePayload>();
  const recommendationsInFlight = new Map<string, Promise<RecommendationsResponsePayload>>();
  let latestRecommendationsPayload: RecommendationsResponsePayload | null = null;
  let recommendationsGlobalInFlight:
    | { key: string; promise: Promise<RecommendationsResponsePayload> }
    | null = null;

  app.get("/health", async () => ({ status: "ok", timestamp: new Date().toISOString() }));

  app.get("/run-status", async () => {
    const nowMs = Date.now();
    const scheduler = app.services.scheduler.getRuntimeStatus(nowMs);
    const execution = app.services.executionGateway.getRuntimeStatus(nowMs);
    const ibkrRuntime = app.services.ibkrAdapter.getRuntimeStatus(nowMs);
    const internalLogs = app.services.apiRequestLogStore.list({
      direction: "internal",
      limit: 2_000
    });

    const endpointActions = ACTION_ENDPOINTS.map((definition) => {
      const timestamps = findActionLogTimestamps(
        internalLogs,
        definition.method,
        definition.endpoint
      );
      return {
        id: definition.id,
        label: definition.label,
        source: "api_route",
        method: definition.method,
        endpoint: definition.endpoint,
        ...timestamps,
        nextAvailableAt: null as string | null,
        nextAutoRunAt: null as string | null,
        frequency: {
          mode: "manual" as const,
          method: definition.method,
          endpoint: definition.endpoint
        },
        note: ""
      };
    });

    const runtimeActions = [
      {
        id: "scheduled_scan",
        label: "Scheduled Scan",
        source: "scheduler",
        method: "INTERNAL",
        endpoint: "scheduled_scan",
        lastAttemptAt: scheduler.lastRunStartedAt,
        lastSuccessAt: scheduler.lastRunStatus === "success" ? scheduler.lastRunFinishedAt : null,
        lastErrorAt: scheduler.lastRunStatus === "error" ? scheduler.lastRunFinishedAt : null,
        nextAvailableAt: scheduler.nextAutoRunAt,
        nextAutoRunAt: scheduler.nextAutoRunAt,
        frequency: {
          mode: "interval" as const,
          intervalMs:
            scheduler.intervalMs > 0
              ? scheduler.intervalMs
              : settings.scanCadenceMinutes * 60 * 1000
        },
        note:
          scheduler.lastRunStatus === "error"
            ? scheduler.lastRunError ?? "error"
            : scheduler.lastRunStatus
      },
      {
        id: "broker_status_sync",
        label: "Broker Status Sync",
        source: "execution_gateway",
        method: "INTERNAL",
        endpoint: "refreshBrokerStatuses",
        lastAttemptAt: execution.brokerStatusSync.lastRunAt,
        lastSuccessAt: execution.brokerStatusSync.lastRunAt,
        lastErrorAt: null,
        nextAvailableAt: execution.brokerStatusSync.nextAvailableAt,
        nextAutoRunAt: null,
        frequency: {
          mode: "triggered" as const,
          intervalMs: execution.brokerStatusSync.minIntervalMs,
          triggers: [
            "scheduler.runScheduledScan",
            "GET /risk-status",
            "GET /account-summary",
            "GET /positions",
            "GET /orders/recent"
          ]
        },
        note: execution.brokerStatusSync.inFlight ? "in_flight" : ""
      },
      {
        id: "account_sync",
        label: "Account Sync",
        source: "execution_gateway",
        method: "INTERNAL",
        endpoint: "syncAccountState",
        lastAttemptAt: execution.accountSync.lastRunAt,
        lastSuccessAt: execution.accountSync.lastRunAt,
        lastErrorAt: null,
        nextAvailableAt: execution.accountSync.nextAvailableAt,
        nextAutoRunAt: null,
        frequency: {
          mode: "triggered" as const,
          intervalMs: execution.accountSync.minIntervalMs,
          triggers: [
            "scheduler.runScheduledScan",
            "GET /risk-status",
            "GET /account-summary",
            "GET /orders/pending",
            "GET /orders/recent"
          ]
        },
        note: execution.accountSync.inFlight ? "in_flight" : ""
      },
      {
        id: "exit_automation",
        label: "Exit Automation",
        source: "execution_gateway",
        method: "INTERNAL",
        endpoint: "runExitAutomation",
        lastAttemptAt: execution.exitAutomation.lastRunAt,
        lastSuccessAt: execution.exitAutomation.lastRunAt,
        lastErrorAt: null,
        nextAvailableAt: execution.exitAutomation.nextAvailableAt,
        nextAutoRunAt: null,
        frequency: {
          mode: "triggered" as const,
          intervalMs: execution.exitAutomation.minIntervalMs,
          triggers: [
            "scheduler.runScheduledScan",
            "GET /risk-status",
            "GET /account-summary",
            "GET /orders/pending"
          ]
        },
        note: execution.exitAutomation.inFlight ? "in_flight" : ""
      },
      {
        id: "ibkr_request_cooldown",
        label: "IBKR Request Cooldown",
        source: "ibkr_adapter",
        method: "INTERNAL",
        endpoint: "requestCooldown",
        lastAttemptAt: null,
        lastSuccessAt: null,
        lastErrorAt: null,
        nextAvailableAt: ibkrRuntime.requestCooldown.until,
        nextAutoRunAt: null,
        frequency: {
          mode: "triggered" as const,
          triggers: [
            "IBKR request timeout",
            "IBKR connectivity failure",
            "IBKR request validation error",
            "IBKR pacing backoff activation"
          ]
        },
        note: ibkrRuntime.requestCooldown.active ? ibkrRuntime.requestCooldown.reason : "inactive"
      }
    ];

    return {
      generatedAt: new Date(nowMs).toISOString(),
      scheduler,
      execution,
      ibkrRuntime,
      actions: [...runtimeActions, ...endpointActions]
    };
  });

  app.post("/scan", async (request: FastifyRequest, reply: FastifyReply) => {
    const body = scanRequestSchema.safeParse(request.body);
    if (!body.success) {
      return reply.code(400).send({ error: body.error.flatten() });
    }

    const policy = app.services.runtimePolicy.getPolicy();
    const universe = normalizeUniverse(policy.universeSymbols, body.data.universe);
    const scanned = await app.services.analysisService.scanUniverseWithDiscovery(
      universe,
      body.data.topN,
      {
        ibkrScanCode: policy.ibkrScanCode
      }
    );
    const analyses = scanned.analyses;
    app.services.auditStore.logEvent("scan_completed", {
      universeSize: universe.length,
      evaluatedUniverseSize: scanned.evaluatedUniverse.length,
      analysisDataProvider: policy.analysisDataProvider,
      discoveredSymbols: scanned.discoveredSymbols,
      scannerUsed: scanned.scannerUsed,
      scannerSource: scanned.scannerSource,
      ibkrScanCode: scanned.ibkrScanCode,
      scannerProvidersUsed: scanned.scannerProvidersUsed,
      scannerProvidersTried: scanned.scannerProvidersTried,
      scannerProviderRanking: scanned.scannerProviderRanking,
      scannerFallbackReason: scanned.scannerFallbackReason,
      topN: body.data.topN,
      symbols: analyses.map((entry) => entry.snapshot.symbol)
    });

    return {
      generatedAt: new Date().toISOString(),
      analyses,
      scanner: {
        requestedUniverseSize: universe.length,
        evaluatedUniverseSize: scanned.evaluatedUniverse.length,
        discoveredSymbols: scanned.discoveredSymbols,
        scannerUsed: scanned.scannerUsed,
        scannerSource: scanned.scannerSource,
        ibkrScanCode: scanned.ibkrScanCode,
        scannerProvidersUsed: scanned.scannerProvidersUsed,
        scannerProvidersTried: scanned.scannerProvidersTried,
        scannerProviderRanking: scanned.scannerProviderRanking,
        scannerFallbackReason: scanned.scannerFallbackReason
      }
    };
  });

  app.post("/score", async (request: FastifyRequest, reply: FastifyReply) => {
    const body = scoreRequestSchema.safeParse(request.body);
    if (!body.success) {
      return reply.code(400).send({ error: body.error.flatten() });
    }

    const symbol = body.data.symbol.toUpperCase();
    const policy = app.services.runtimePolicy.getPolicy();
    const universe = normalizeUniverse(policy.universeSymbols, body.data.universe, symbol);
    const scored = await app.services.analysisService.scoreSymbol(symbol, universe);
    return scored;
  });

  app.post("/decision", async (request: FastifyRequest, reply: FastifyReply) => {
    const body = decisionRequestSchema.safeParse(request.body);
    if (!body.success) {
      return reply.code(400).send({ error: body.error.flatten() });
    }

    const symbol = body.data.symbol.toUpperCase();
    const policy = app.services.runtimePolicy.getPolicy();
    const universe = normalizeUniverse(policy.universeSymbols, body.data.universe, symbol);
    const outcome = await app.services.analysisService.decideSymbol(symbol, universe);

    app.services.auditStore.logEvent("decision_generated", {
      symbol,
      action: outcome.decisionCard.action,
      compositeScore: outcome.scoreCard.compositeScore,
      upProb: outcome.feature.directionalUpProb,
      downProb: outcome.feature.directionalDownProb,
      vetoFlags: outcome.decisionCard.vetoFlags
    });

    return outcome;
  });

  app.get("/recommendations", async (request: FastifyRequest, reply: FastifyReply) => {
    const query = recommendationsQuerySchema.safeParse(request.query ?? {});
    if (!query.success) {
      return reply.code(400).send({ error: query.error.flatten() });
    }

    const policy = app.services.runtimePolicy.getPolicy();
    const topN = query.data.topN ?? policy.scanTopN;
    const universe = normalizeUniverse(policy.universeSymbols, query.data.universe);
    const startedMs = Date.now();
    const totalTimeoutMs = recommendationsTimeoutMs();
    const deadlineMs =
      totalTimeoutMs > 0 ? startedMs + totalTimeoutMs : Number.POSITIVE_INFINITY;
    const remainingMs = (): number =>
      Number.isFinite(deadlineMs) ? Math.max(0, deadlineMs - Date.now()) : Number.POSITIVE_INFINITY;
    const elapsedMs = (): number => Date.now() - startedMs;
    const cacheKey = `${topN}|${policy.ibkrScanCode}|${universe.join(",")}`;
    const cached = recommendationsCache.get(cacheKey) ?? null;

    type ScanWithDiscoveryResult = Awaited<
      ReturnType<typeof app.services.analysisService.scanUniverseWithDiscovery>
    >;

    const policySnapshot: RecommendationsResponsePayload["policySnapshot"] = {
      minCompositeScore: policy.minCompositeScore,
      minDirectionalProbability: policy.minDirectionalProbability,
      dteMin: policy.dteMin,
      dteMax: policy.dteMax,
      ibkrScanCode: policy.ibkrScanCode,
      analysisDataProvider: policy.analysisDataProvider,
      autoProposeActionable: policy.autoProposeActionable
    };

    const scannerFromResult = (scanned: ScanWithDiscoveryResult | null): RecommendationsScannerSummary => {
      if (!scanned) {
        return {
          requestedUniverseSize: universe.length,
          evaluatedUniverseSize: 0,
          discoveredSymbols: [],
          scannerUsed: false,
          scannerSource: "none",
          ibkrScanCode: policy.ibkrScanCode,
          scannerProvidersUsed: [],
          scannerProvidersTried: [],
          scannerProviderRanking: [],
          scannerFallbackReason: null
        };
      }
      return {
        requestedUniverseSize: universe.length,
        evaluatedUniverseSize: scanned.evaluatedUniverse.length,
        discoveredSymbols: scanned.discoveredSymbols,
        scannerUsed: scanned.scannerUsed,
        scannerSource: scanned.scannerSource,
        ibkrScanCode: scanned.ibkrScanCode,
        scannerProvidersUsed: scanned.scannerProvidersUsed,
        scannerProvidersTried: scanned.scannerProvidersTried,
        scannerProviderRanking: scanned.scannerProviderRanking,
        scannerFallbackReason: scanned.scannerFallbackReason
      };
    };

    const buildResponse = (params: {
      scanner: RecommendationsScannerSummary;
      recommendations: RecommendationRow[];
      timedOut: boolean;
      source: RecommendationsExecutionMeta["source"];
      timeoutReason: string | null;
      fallbackFromGeneratedAt?: string | null;
      executionErrors?: RecommendationsExecutionMeta["errors"];
      autoProposal?: RecommendationsExecutionMeta["autoProposal"];
    }): RecommendationsResponsePayload => ({
      generatedAt: new Date().toISOString(),
      policySnapshot,
      scanner: params.scanner,
      recommendations: params.recommendations,
      execution: {
        timedOut: params.timedOut,
        timeoutMs: totalTimeoutMs,
        source: params.source,
        timeoutReason: params.timeoutReason,
        computeMs: elapsedMs(),
        elapsedMs: elapsedMs(),
        fallbackFromGeneratedAt: params.fallbackFromGeneratedAt ?? null,
        errors: params.executionErrors ?? [],
        autoProposal:
          params.autoProposal ?? {
            enabled: policy.autoProposeActionable,
            attempted: 0,
            created: 0,
            skipped: 0,
            failed: 0,
            reason: policy.autoProposeActionable ? "no_actionable_candidates" : "disabled_by_policy",
            outcomes: []
          }
      }
    });

    const recordAudit = (payload: RecommendationsResponsePayload): void => {
      app.services.auditStore.logEvent("recommendations_generated", {
        topN,
        universeSize: universe.length,
        analysisDataProvider: policy.analysisDataProvider,
        evaluatedUniverseSize: payload.scanner.evaluatedUniverseSize,
        discoveredSymbols: payload.scanner.discoveredSymbols,
        scannerUsed: payload.scanner.scannerUsed,
        scannerSource: payload.scanner.scannerSource,
        ibkrScanCode: payload.scanner.ibkrScanCode,
        scannerProvidersUsed: payload.scanner.scannerProvidersUsed,
        scannerProvidersTried: payload.scanner.scannerProvidersTried,
        scannerProviderRanking: payload.scanner.scannerProviderRanking,
        scannerFallbackReason: payload.scanner.scannerFallbackReason,
        recommendationCount: payload.recommendations.length,
        actionableCount: payload.recommendations.filter((entry) => entry.actionable).length,
        timedOut: payload.execution.timedOut,
        timeoutMs: payload.execution.timeoutMs,
        timeoutReason: payload.execution.timeoutReason,
        executionSource: payload.execution.source,
        elapsedMs: payload.execution.elapsedMs,
        computeMs: payload.execution.computeMs,
        autoProposeEnabled: payload.execution.autoProposal.enabled,
        autoProposeAttempted: payload.execution.autoProposal.attempted,
        autoProposeCreated: payload.execution.autoProposal.created,
        autoProposeSkipped: payload.execution.autoProposal.skipped,
        autoProposeFailed: payload.execution.autoProposal.failed,
        autoProposeReason: payload.execution.autoProposal.reason,
        executionErrors: payload.execution.errors,
        executionErrorCount: payload.execution.errors.length
      });
    };

    const fallbackFromCache = (
      reason: string,
      executionErrors: RecommendationsExecutionMeta["errors"] = []
    ): RecommendationsResponsePayload | null => {
      const latestCached = [...recommendationsCache.values()].sort((left, right) => {
        const leftTs = Date.parse(left.generatedAt);
        const rightTs = Date.parse(right.generatedAt);
        if (!Number.isFinite(leftTs) && !Number.isFinite(rightTs)) return 0;
        if (!Number.isFinite(leftTs)) return 1;
        if (!Number.isFinite(rightTs)) return -1;
        return rightTs - leftTs;
      })[0] ?? null;
      const candidate = cached ?? latestRecommendationsPayload ?? latestCached;
      if (!candidate) return null;
      return {
        ...candidate,
        generatedAt: new Date().toISOString(),
        execution: {
          timedOut: true,
          timeoutMs: totalTimeoutMs,
          source: "cache_fallback",
          timeoutReason: reason,
          computeMs: elapsedMs(),
          elapsedMs: elapsedMs(),
          fallbackFromGeneratedAt: candidate.generatedAt,
          errors: [
            ...(Array.isArray(candidate.execution.errors) ? candidate.execution.errors : []),
            ...executionErrors,
            buildRecommendationRunError("internal", reason)
          ].slice(-20),
          autoProposal: candidate.execution.autoProposal ?? {
            enabled: policy.autoProposeActionable,
            attempted: 0,
            created: 0,
            skipped: 0,
            failed: 0,
            reason: policy.autoProposeActionable ? "cache_fallback_no_autoproposal" : "disabled_by_policy",
            outcomes: []
          }
        }
      };
    };
    const runRecommendations = async (): Promise<RecommendationsResponsePayload> => {
      let scanned: ScanWithDiscoveryResult | null = null;
      let recommendations: RecommendationRow[] = [];
      let timeoutReason: string | null = null;
      let timedOut = false;
      const executionErrors: RecommendationsExecutionMeta["errors"] = [];
      const autoProposalCandidates: AutoProposalCandidate[] = [];
      const autoProposalDefaultSummary: RecommendationsExecutionMeta["autoProposal"] = {
        enabled: policy.autoProposeActionable,
        attempted: 0,
        created: 0,
        skipped: 0,
        failed: 0,
        reason: policy.autoProposeActionable ? "no_actionable_candidates" : "disabled_by_policy",
        outcomes: []
      };

      const buildDecisionCard = (
        analysis: DetailedSymbolAnalysis,
        decision: {
          action: "CALL" | "PUT" | "NO_TRADE";
          confidence: number;
          rationale: string;
          vetoFlags: string[];
        }
      ): DecisionCard => ({
        symbol: analysis.snapshot.symbol,
        timestamp: new Date().toISOString(),
        action: decision.action,
        confidence: decision.confidence,
        rationale: decision.rationale,
        vetoFlags: decision.vetoFlags,
        scoreCard: analysis.scoreCard
      });

      const maybeAutoProposeActionable = async (): Promise<
        RecommendationsExecutionMeta["autoProposal"]
      > => {
        if (!policy.autoProposeActionable) return autoProposalDefaultSummary;
        if (autoProposalCandidates.length === 0) return autoProposalDefaultSummary;

        const outcomes: RecommendationsExecutionMeta["autoProposal"]["outcomes"] = [];
        let created = 0;
        let skipped = 0;
        let failed = 0;
        let reason: string | null = null;

        const connectivity = await app.services.ibkrAdapter.checkConnectivity(5_000);
        app.services.executionGateway.notifyConnectivityStatus(connectivity);
        if (!connectivity.reachable) {
          reason = "ibkr_disconnected";
          for (const candidate of autoProposalCandidates) {
            outcomes.push({
              symbol: candidate.symbol,
              action: candidate.action,
              status: "skipped",
              message: `IBKR disconnected (${connectivity.message}).`,
              orderId: null
            });
          }
          skipped = autoProposalCandidates.length;
          return {
            enabled: true,
            attempted: autoProposalCandidates.length,
            created,
            skipped,
            failed,
            reason,
            outcomes: outcomes.slice(0, 40)
          };
        }

        try {
          await app.services.executionGateway.refreshBrokerStatuses();
          await app.services.executionGateway.syncAccountState(app.services.accountState);
        } catch (error) {
          reason = "broker_sync_failed";
          const message = (error as Error).message || "broker_sync_failed";
          for (const candidate of autoProposalCandidates) {
            outcomes.push({
              symbol: candidate.symbol,
              action: candidate.action,
              status: "failed",
              message,
              orderId: null
            });
          }
          failed = autoProposalCandidates.length;
          return {
            enabled: true,
            attempted: autoProposalCandidates.length,
            created,
            skipped,
            failed,
            reason,
            outcomes: outcomes.slice(0, 40)
          };
        }

        const brokerSnapshot = app.services.executionGateway.getLastAccountSnapshot();
        const accountEquity =
          typeof brokerSnapshot?.netLiquidation === "number" && brokerSnapshot.netLiquidation > 0
            ? brokerSnapshot.netLiquidation
            : app.services.accountState.accountEquity;
        if (!Number.isFinite(accountEquity) || accountEquity <= 0) {
          reason = "account_equity_unavailable";
          for (const candidate of autoProposalCandidates) {
            outcomes.push({
              symbol: candidate.symbol,
              action: candidate.action,
              status: "failed",
              message: "Unable to determine account equity for auto-proposal.",
              orderId: null
            });
          }
          failed = autoProposalCandidates.length;
          return {
            enabled: true,
            attempted: autoProposalCandidates.length,
            created,
            skipped,
            failed,
            reason,
            outcomes: outcomes.slice(0, 40)
          };
        }

        const activeStatuses = new Set([
          "PENDING_APPROVAL",
          "SUBMITTED_PAPER",
          "SUBMITTED_LIVE",
          "FILLED"
        ]);
        const activeSignatures = new Set(
          app.services.auditStore
            .listOrders({ limit: 2_000 })
            .filter(
              (order) =>
                order.intentType === "ENTRY" &&
                activeStatuses.has(order.status) &&
                (order.action === "CALL" || order.action === "PUT")
            )
            .map((order) => `${order.symbol.toUpperCase()}|${order.action}`)
        );

        for (const candidate of autoProposalCandidates) {
          const signature = `${candidate.symbol.toUpperCase()}|${candidate.action}`;
          if (activeSignatures.has(signature)) {
            skipped += 1;
            outcomes.push({
              symbol: candidate.symbol,
              action: candidate.action,
              status: "skipped",
              message: "Existing active entry order for symbol/action.",
              orderId: null
            });
            continue;
          }

          try {
            const order = app.services.executionGateway.proposeOrder(
              candidate.symbol,
              candidate.decisionCard,
              candidate.chain,
              accountEquity
            );
            created += 1;
            activeSignatures.add(signature);
            outcomes.push({
              symbol: candidate.symbol,
              action: candidate.action,
              status: "created",
              message: `Ticket created from recommendation rank ${candidate.rank}.`,
              orderId: order.id
            });
          } catch (error) {
            failed += 1;
            outcomes.push({
              symbol: candidate.symbol,
              action: candidate.action,
              status: "failed",
              message: (error as Error).message || "auto_propose_failed",
              orderId: null
            });
          }
        }

        return {
          enabled: true,
          attempted: autoProposalCandidates.length,
          created,
          skipped,
          failed,
          reason,
          outcomes: outcomes.slice(0, 40)
        };
      };

      try {
        const timeoutEnabled = totalTimeoutMs > 0;
        const analysisBudgetMs = timeoutEnabled
          ? Math.max(4_000, remainingMs() - 1_000)
          : 0;
        const discoveryTimeoutMs = timeoutEnabled
          ? Math.max(3_000, Math.min(10_000, remainingMs() - 8_000))
          : 0;
        const perSymbolTimeoutMs = settings.ibkrEnabled
          ? Math.max(7_500, Math.min(12_000, settings.ibkrClientTimeoutMs + 2_000))
          : 3_500;
        scanned = await withTimeout(
          app.services.analysisService.scanUniverseWithDiscovery(universe, topN, {
            ibkrScanCode: policy.ibkrScanCode,
            analysisBudgetMs,
            perSymbolTimeoutMs,
            discoveryTimeoutMs
          }),
          remainingMs(),
          "scan_universe"
        );
        if (scanned.scannerFallbackReason) {
          executionErrors.push(
            buildRecommendationRunError("scanner", scanned.scannerFallbackReason)
          );
        }

        const analyses = scanned.detailedAnalyses;
        if (scanned.analysisTimedOut) {
          timedOut = true;
          timeoutReason =
            scanned.analysisTimeoutReason ??
            `Recommendations timed out after ${elapsedMs()}ms during scan analysis.`;
          executionErrors.push(
            buildRecommendationRunError(
              "analysis",
              timeoutReason
            )
          );
        }

        const buildDeterministicFallbackDecision = (
          analysis: DetailedSymbolAnalysis,
          fallbackReason: string
        ): {
          action: "CALL" | "PUT" | "NO_TRADE";
          confidence: number;
          rationale: string;
          vetoFlags: string[];
        } => {
          const score = analysis.scoreCard.compositeScore;
          const upProb = analysis.featureVector.directionalUpProb;
          const downProb = analysis.featureVector.directionalDownProb;

          if (score >= policy.minCompositeScore && upProb >= policy.minDirectionalProbability) {
            return {
              action: "CALL",
              confidence: clamp01((Math.abs(score) / 150) * 0.6 + Math.max(upProb, downProb) * 0.4),
              rationale: `Deterministic fallback: CALL candidate (score >= ${policy.minCompositeScore}, p(up) >= ${policy.minDirectionalProbability}). ${fallbackReason}`,
              vetoFlags: ["timeout_fallback"]
            };
          }
          if (score >= policy.minCompositeScore && downProb >= policy.minDirectionalProbability) {
            return {
              action: "PUT",
              confidence: clamp01((Math.abs(score) / 150) * 0.6 + Math.max(upProb, downProb) * 0.4),
              rationale: `Deterministic fallback: PUT candidate (score >= ${policy.minCompositeScore}, p(down) >= ${policy.minDirectionalProbability}). ${fallbackReason}`,
              vetoFlags: ["timeout_fallback"]
            };
          }
          return {
            action: "NO_TRADE",
            confidence: clamp01((Math.abs(score) / 150) * 0.6 + Math.max(upProb, downProb) * 0.4),
            rationale: `Deterministic fallback: thresholds not met. ${fallbackReason}`,
            vetoFlags: ["timeout_fallback"]
          };
        };

        const appendRecommendationRow = (
          analysis: DetailedSymbolAnalysis,
          rank: number,
          decision: {
            action: "CALL" | "PUT" | "NO_TRADE";
            confidence: number;
            rationale: string;
            vetoFlags: string[];
          }
        ): void => {
          const evidence = buildRecommendationEvidence(analysis, {
            minCompositeScore: policy.minCompositeScore,
            minDirectionalProbability: policy.minDirectionalProbability,
            suggestedAction: decision.action,
            confidence: decision.confidence
          });
          let suggestedAction = decision.action;
          let rationale = decision.rationale;
          const vetoFlags = [...decision.vetoFlags];
          let actionable = decision.action !== "NO_TRADE";

          if (actionable && !evidence.dataQuality.passed) {
            suggestedAction = "NO_TRADE";
            actionable = false;
            vetoFlags.push("insufficient_real_data");
            rationale = `${decision.rationale} Data-quality gate blocked trade: ${evidence.indicatorCoverage.available}/${evidence.indicatorCoverage.total} indicators available (minimum ${evidence.dataQuality.minimumAvailableIndicators}) and at least one core non-synthetic market/options feed is required.`;
          }
          const normalizedVetoFlags = [...new Set(vetoFlags)];

          recommendations.push({
            rank,
            symbol: analysis.snapshot.symbol,
            actionable,
            suggestedAction,
            confidence: decision.confidence,
            rationale,
            vetoFlags: normalizedVetoFlags,
            metrics: {
              compositeScore: analysis.scoreCard.compositeScore,
              directionalUpProb: analysis.featureVector.directionalUpProb,
              directionalDownProb: analysis.featureVector.directionalDownProb,
              spreadPct: analysis.snapshot.spreadPct
            },
            evidence
          });

          if (actionable && (suggestedAction === "CALL" || suggestedAction === "PUT")) {
            autoProposalCandidates.push({
              rank,
              symbol: analysis.snapshot.symbol,
              action: suggestedAction,
              decisionCard: buildDecisionCard(analysis, {
                action: suggestedAction,
                confidence: decision.confidence,
                rationale,
                vetoFlags: normalizedVetoFlags
              }),
              chain: analysis.chain
            });
          }
        };

        const appendDeterministicFallbackRows = (startIndex: number, reason: string): void => {
          for (let fallbackIndex = startIndex; fallbackIndex < analyses.length; fallbackIndex += 1) {
            const fallbackAnalysis = analyses[fallbackIndex];
            const fallbackDecision = buildDeterministicFallbackDecision(fallbackAnalysis, reason);
            appendRecommendationRow(fallbackAnalysis, fallbackIndex + 1, fallbackDecision);
          }
        };

        for (let index = 0; index < analyses.length; index += 1) {
          const analysis = analyses[index];
          const budgetMs = remainingMs();
          if (totalTimeoutMs > 0 && budgetMs <= 0) {
            timedOut = true;
            timeoutReason = `Recommendations timed out after ${elapsedMs()}ms while ranking symbols (${recommendations.length}/${analyses.length} complete).`;
            executionErrors.push(buildRecommendationRunError("timeout", timeoutReason));
            appendDeterministicFallbackRows(
              index,
              "Time budget exhausted before full judge review."
            );
            break;
          }

          let decision: Awaited<ReturnType<typeof app.services.decisionEngine.decide>>;
          try {
            decision = await withTimeout(
              app.services.decisionEngine.decide(analysis.featureVector, analysis.scoreCard),
              budgetMs,
              `decide_symbol_${analysis.snapshot.symbol}`
            );
          } catch (error) {
            if (isRecommendationsTimeoutError(error)) {
              timedOut = true;
              timeoutReason = `Recommendations timed out after ${elapsedMs()}ms while scoring ${analysis.snapshot.symbol} (${recommendations.length}/${analyses.length} complete).`;
              executionErrors.push(
                buildRecommendationRunError(
                  "timeout",
                  timeoutReason,
                  analysis.snapshot.symbol
                )
              );
              appendDeterministicFallbackRows(
                index,
                `Timeout while scoring ${analysis.snapshot.symbol}; using deterministic fallback.`
              );
              break;
            }

            const errorMessage = (error as Error)?.message || String(error);
            executionErrors.push(
              buildRecommendationRunError(
                "decision",
                `Failed to score ${analysis.snapshot.symbol}: ${errorMessage}`,
                analysis.snapshot.symbol
              )
            );
            const fallbackDecision = buildDeterministicFallbackDecision(
              analysis,
              `Judge error for ${analysis.snapshot.symbol}; using deterministic fallback.`
            );
            appendRecommendationRow(analysis, index + 1, fallbackDecision);
            continue;
          }

          appendRecommendationRow(analysis, index + 1, {
            action: decision.action,
            confidence: decision.confidence,
            rationale: decision.rationale,
            vetoFlags: decision.vetoFlags
          });
        }
      } catch (error) {
        if (isRecommendationsTimeoutError(error)) {
          timedOut = true;
          timeoutReason = `Recommendations timed out after ${elapsedMs()}ms during ${error.stage}.`;
          executionErrors.push(
            buildRecommendationRunError("timeout", timeoutReason)
          );
        } else {
          const errorMessage = (error as Error)?.message || String(error);
          const fallbackReason = `Recommendations run failed: ${errorMessage}`;
          executionErrors.push(
            buildRecommendationRunError("internal", fallbackReason)
          );
          const cachedFallback = fallbackFromCache(fallbackReason, executionErrors);
          if (cachedFallback) {
            recordAudit(cachedFallback);
            latestRecommendationsPayload = cachedFallback;
            return cachedFallback;
          }
          const failedResponse = buildResponse({
            scanner: scannerFromResult(scanned),
            recommendations: [],
            timedOut: false,
            source: "empty_fallback",
            timeoutReason: null,
            executionErrors
          });
          recordAudit(failedResponse);
          latestRecommendationsPayload = failedResponse;
          return failedResponse;
        }
      }

      if (timedOut && recommendations.length === 0) {
        const reason = timeoutReason ?? "Recommendations timed out before any symbols were scored.";
        const cachedFallback = fallbackFromCache(reason, executionErrors);
        if (cachedFallback) {
          recordAudit(cachedFallback);
          latestRecommendationsPayload = cachedFallback;
          return cachedFallback;
        }

        const emptyFallback = buildResponse({
          scanner: scannerFromResult(scanned),
          recommendations: [],
          timedOut: true,
          source: "empty_fallback",
          timeoutReason: reason,
          executionErrors
        });
        recordAudit(emptyFallback);
        latestRecommendationsPayload = emptyFallback;
        return emptyFallback;
      }

      const autoProposalSummary = await maybeAutoProposeActionable();
      if (autoProposalSummary.failed > 0) {
        executionErrors.push(
          buildRecommendationRunError(
            "internal",
            `Auto-propose failed for ${autoProposalSummary.failed} candidate(s).`
          )
        );
      }

      const finalPayload = buildResponse({
        scanner: scannerFromResult(scanned),
        recommendations,
        timedOut,
        source: timedOut ? "fresh_partial" : "fresh",
        timeoutReason,
        executionErrors,
        autoProposal: autoProposalSummary
      });

      if (recommendations.length > 0) {
        recommendationsCache.set(cacheKey, finalPayload);
      }
      latestRecommendationsPayload = finalPayload;

      app.services.auditStore.logEvent("recommendations_auto_propose_summary", {
        topN,
        universeSize: universe.length,
        enabled: autoProposalSummary.enabled,
        attempted: autoProposalSummary.attempted,
        created: autoProposalSummary.created,
        skipped: autoProposalSummary.skipped,
        failed: autoProposalSummary.failed,
        reason: autoProposalSummary.reason,
        outcomes: autoProposalSummary.outcomes.slice(0, 20)
      });

      recordAudit(finalPayload);
      return finalPayload;
    };

    const existingInFlight = recommendationsInFlight.get(cacheKey);
    if (existingInFlight) {
      return await existingInFlight;
    }

    if (
      recommendationsGlobalInFlight &&
      recommendationsGlobalInFlight.key !== cacheKey
    ) {
      const globalReason = `Recommendations run already in flight (${recommendationsGlobalInFlight.key}); returning latest available snapshot.`;
      const globalErrors = [buildRecommendationRunError("internal", globalReason)];
      const cachedFallback = fallbackFromCache(globalReason, globalErrors);
      if (cachedFallback) {
        recordAudit(cachedFallback);
        return cachedFallback;
      }

      try {
        const shared = await recommendationsGlobalInFlight.promise;
        const sharedFallback: RecommendationsResponsePayload = {
          ...shared,
          generatedAt: new Date().toISOString(),
          execution: {
            timedOut: true,
            timeoutMs: totalTimeoutMs,
            source: "cache_fallback",
            timeoutReason: globalReason,
            computeMs: elapsedMs(),
            elapsedMs: elapsedMs(),
            fallbackFromGeneratedAt: shared.generatedAt,
            errors: [
              ...(Array.isArray(shared.execution.errors) ? shared.execution.errors : []),
              ...globalErrors
            ].slice(-20),
            autoProposal: shared.execution.autoProposal ?? {
              enabled: policy.autoProposeActionable,
              attempted: 0,
              created: 0,
              skipped: 0,
              failed: 0,
              reason: policy.autoProposeActionable ? "cache_fallback_no_autoproposal" : "disabled_by_policy",
              outcomes: []
            }
          }
        };
        recordAudit(sharedFallback);
        return sharedFallback;
      } catch {
        // no-op; proceed to compute if shared run failed and no cache exists.
      }
    }

    const requestPromise = runRecommendations().finally(() => {
      recommendationsInFlight.delete(cacheKey);
      if (recommendationsGlobalInFlight?.promise === requestPromise) {
        recommendationsGlobalInFlight = null;
      }
    });
    recommendationsInFlight.set(cacheKey, requestPromise);
    recommendationsGlobalInFlight = { key: cacheKey, promise: requestPromise };
    return await requestPromise;
  });

  app.get("/api-request-logs", async (request: FastifyRequest, reply: FastifyReply) => {
    const query = apiRequestLogsQuerySchema.safeParse(request.query ?? {});
    if (!query.success) {
      return reply.code(400).send({ error: query.error.flatten() });
    }

    const logs = app.services.apiRequestLogStore.list(query.data);
    return {
      generatedAt: new Date().toISOString(),
      logs
    };
  });

  app.get("/market-data-diagnostics", async (request: FastifyRequest, reply: FastifyReply) => {
    const query = marketDataDiagnosticsQuerySchema.safeParse(request.query ?? {});
    if (!query.success) {
      return reply.code(400).send({ error: query.error.flatten() });
    }

    if (!(await requireIbkrConnection(app, reply, "market-data diagnostics"))) return;

    const nowMs = Date.now();
    const cutoffMs = nowMs - query.data.windowMinutes * 60_000;
    const logs = app.services.apiRequestLogStore.list({
      provider: "ibkr",
      endpointContains: "getMarketDataSnapshot",
      limit: 2_000
    });
    const recent = logs.filter((entry) => {
      const startedAtMs = Date.parse(entry.startedAt);
      return Number.isFinite(startedAtMs) && startedAtMs >= cutoffMs;
    });

    const bySymbol = new Map<string, MarketDataSymbolDiagnostics>();
    for (const entry of recent) {
      const symbol = extractSymbolFromIbkrLog(entry);
      if (!symbol) continue;

      const payload = asRecord(entry.requestPayload);
      const secType =
        payload && typeof payload.secType === "string" ? payload.secType.toUpperCase() : "";
      const isOption = secType === "OPT";
      const signal = classifySignalFromLog(entry);

      let row = bySymbol.get(symbol);
      if (!row) {
        row = {
          symbol,
          quote: createEmptySignal(),
          option: createEmptySignal()
        };
        bySymbol.set(symbol, row);
      }

      const target = isOption ? row.option : row.quote;
      if (target.lastAt) continue;
      if (target.state !== "unknown") continue;
      if (target.note) continue;

      if (isOption) row.option = signal;
      else row.quote = signal;
    }

    const entitlement = app.services.ibkrAdapter.getMarketDataEntitlementState(nowMs);
    for (const blocked of entitlement.quoteSubscriptionBackoffs) {
      const symbol = blocked.symbol.toUpperCase();
      const existing =
        bySymbol.get(symbol) ??
        ({
          symbol,
          quote: createEmptySignal(),
          option: createEmptySignal()
        } as MarketDataSymbolDiagnostics);

      if (existing.quote.state === "unknown" || existing.quote.state === "connectivity") {
        existing.quote = {
          state: "blocked",
          lastAt: blocked.until,
          delayedOnly: true,
          note: `Quote request backoff active for ${Math.ceil(blocked.remainingMs / 1000)}s.`
        };
      }
      bySymbol.set(symbol, existing);
    }

    const symbols = [...bySymbol.values()]
      .sort((left, right) => {
        const leftAt = Date.parse(left.quote.lastAt ?? left.option.lastAt ?? "");
        const rightAt = Date.parse(right.quote.lastAt ?? right.option.lastAt ?? "");
        if (!Number.isFinite(leftAt) && !Number.isFinite(rightAt)) return left.symbol.localeCompare(right.symbol);
        if (!Number.isFinite(leftAt)) return 1;
        if (!Number.isFinite(rightAt)) return -1;
        return rightAt - leftAt;
      })
      .slice(0, query.data.limitSymbols);

    const quoteCounts = symbols.reduce(
      (acc, row) => {
        acc[row.quote.state] = (acc[row.quote.state] ?? 0) + 1;
        return acc;
      },
      {} as Record<EntitlementState, number>
    );
    const optionCounts = symbols.reduce(
      (acc, row) => {
        acc[row.option.state] = (acc[row.option.state] ?? 0) + 1;
        return acc;
      },
      {} as Record<EntitlementState, number>
    );

    const quoteLive = quoteCounts.live ?? 0;
    const quoteDelayed = quoteCounts.delayed ?? 0;
    const quoteBlocked = quoteCounts.blocked ?? 0;
    const quoteConnectivity = quoteCounts.connectivity ?? 0;

    const coreMarketStatus =
      quoteLive > 0 ? "ok" : quoteDelayed > 0 ? "delayed_only" : quoteBlocked > 0 ? "blocked" : "unknown";
    const optionLiveOrDelayed = (optionCounts.live ?? 0) + (optionCounts.delayed ?? 0);
    const coreOptionsStatus = optionLiveOrDelayed > 0 ? "ok" : "missing";

    return {
      generatedAt: new Date(nowMs).toISOString(),
      windowMinutes: query.data.windowMinutes,
      summary: {
        dataGate: coreMarketStatus === "blocked" ? "blocked" : "open",
        coreMarketStatus,
        coreOptionsStatus,
        delayedOnlyMode: entitlement.delayedOnly,
        quoteCounts: {
          live: quoteLive,
          delayed: quoteDelayed,
          blocked: quoteBlocked,
          connectivity: quoteConnectivity,
          other:
            (quoteCounts.error ?? 0) +
            (quoteCounts.invalid_contract ?? 0) +
            (quoteCounts.unknown ?? 0)
        },
        optionCounts: {
          live: optionCounts.live ?? 0,
          delayed: optionCounts.delayed ?? 0,
          blocked: optionCounts.blocked ?? 0,
          invalidContract: optionCounts.invalid_contract ?? 0,
          connectivity: optionCounts.connectivity ?? 0,
          other: (optionCounts.error ?? 0) + (optionCounts.unknown ?? 0)
        }
      },
      brokerBackoffs: entitlement,
      symbols
    };
  });

  app.post("/backtest", async (request: FastifyRequest, reply: FastifyReply) => {
    const body = backtestRequestSchema.safeParse(request.body ?? {});
    if (!body.success) {
      return reply.code(400).send({ error: body.error.flatten() });
    }

    const policy = app.services.runtimePolicy.getPolicy();
    const universe = normalizeUniverse(policy.universeSymbols, body.data.universe);
    const report = await app.services.backtestService.run({
      universe,
      lookbackDays: body.data.lookbackDays,
      slippageBps: body.data.slippageBps,
      commissionPerTrade: body.data.commissionPerTrade,
      premiumPerTrade: body.data.premiumPerTrade,
      optionLeverage: body.data.optionLeverage,
      warmupWindow: body.data.warmupWindow,
      maxGainPct: body.data.maxGainPct,
      maxLossPct: body.data.maxLossPct,
      startingEquity: body.data.startingEquity,
      sampleLimit: body.data.sampleLimit
    });

    const acceptanceGate = {
      expectedProfitFactorMin: 1.2,
      expectedMaxDrawdownPctMax: 0.12,
      passProfitFactor: report.result.profitFactor >= 1.2,
      passMaxDrawdown: report.result.maxDrawdownPct <= 0.12
    };

    app.services.auditStore.logEvent("backtest_completed", {
      universeSize: universe.length,
      lookbackDays: body.data.lookbackDays,
      pointsEvaluated: report.pointsEvaluated,
      trades: report.result.trades,
      winRate: report.result.winRate,
      netPnl: report.result.netPnl,
      maxDrawdownPct: report.result.maxDrawdownPct,
      profitFactor: report.result.profitFactor,
      acceptanceGate
    });

    return {
      ...report,
      acceptanceGate
    };
  });

  app.post("/propose-order", async (request: FastifyRequest, reply: FastifyReply) => {
    const body = proposeOrderRequestSchema.safeParse(request.body);
    if (!body.success) {
      return reply.code(400).send({
        error: "Invalid propose-order request payload.",
        reasonCode: "INVALID_REQUEST",
        details: body.error.flatten()
      });
    }
    if (!(await requireIbkrConnection(app, reply, "order proposal", 5_000))) return;

    try {
      await app.services.executionGateway.refreshBrokerStatuses();
      await app.services.executionGateway.syncAccountState(app.services.accountState);
    } catch (error) {
      return reply.code(503).send({
        error: `Unable to refresh broker/account data before proposal: ${(error as Error).message}`,
        reasonCode: "BROKER_SYNC_FAILED",
        suggestion: "Confirm IBKR connection health, then retry."
      });
    }

    const symbol = body.data.symbol.toUpperCase();
    const policy = app.services.runtimePolicy.getPolicy();
    const universe = normalizeUniverse(policy.universeSymbols, body.data.universe, symbol);
    const outcome = await app.services.analysisService.decideSymbol(symbol, universe);

    if (outcome.decisionCard.action === "NO_TRADE") {
      return reply.code(409).send(
        buildNoTradeProposalError(
          symbol,
          policy,
          outcome.feature,
          outcome.scoreCard,
          outcome.decisionCard,
          outcome.optionChainSource
        )
      );
    }
    if (outcome.optionChainSource === "synthetic_option_chain") {
      return reply.code(400).send({
        error:
          "Cannot propose order because no real option chain is available for this symbol. Synthetic option contracts are blocked for order proposals.",
        reasonCode: "OPTION_CHAIN_UNAVAILABLE",
        optionChainSource: outcome.optionChainSource,
        suggestion:
          "Switch analysis provider or retry later so IBKR/Alpaca can return a live option chain."
      });
    }

    try {
      const brokerSnapshot = app.services.executionGateway.getLastAccountSnapshot();
      const accountEquity =
        typeof brokerSnapshot?.netLiquidation === "number" && brokerSnapshot.netLiquidation > 0
          ? brokerSnapshot.netLiquidation
          : app.services.accountState.accountEquity;
      if (!Number.isFinite(accountEquity) || accountEquity <= 0) {
        return reply.code(409).send({
          error:
            "Unable to determine account equity from IBKR account data. Refresh broker/account sync and try again.",
          reasonCode: "ACCOUNT_EQUITY_UNAVAILABLE",
          suggestion: "Check IBKR account updates and entitlements, then refresh status."
        });
      }

      const order = app.services.executionGateway.proposeOrder(
        symbol,
        outcome.decisionCard,
        outcome.chain,
        accountEquity
      );
      return {
        decision: outcome.decisionCard,
        order,
        sizing: {
          accountEquity,
          source:
            typeof brokerSnapshot?.netLiquidation === "number" && brokerSnapshot.netLiquidation > 0
              ? "ibkr_account_snapshot"
              : "account_state_cache"
        }
      };
    } catch (error) {
      return reply.code(400).send({
        error: (error as Error).message,
        reasonCode: "ORDER_PROPOSAL_FAILED"
      });
    }
  });

  app.post("/approve-order", async (request: FastifyRequest, reply: FastifyReply) => {
    const body = approveOrderRequestSchema.safeParse(request.body);
    if (!body.success) {
      return reply.code(400).send({ error: body.error.flatten() });
    }

    try {
      const order = await app.services.executionGateway.approveOrder(
        body.data.orderId,
        body.data.approve,
        app.services.accountState,
        body.data.comment
      );
      return { order };
    } catch (error) {
      return reply.code(404).send({ error: (error as Error).message });
    }
  });

  app.get("/risk-status", async () => {
    await app.services.executionGateway.refreshBrokerStatuses();
    await app.services.executionGateway.syncAccountState(app.services.accountState);
    await app.services.executionGateway.runExitAutomation(app.services.accountState);
    const riskState = app.services.riskEngine.buildRiskState(
      app.services.accountState,
      app.services.executionGateway.listOpenPositions()
    );
    app.services.auditStore.saveRiskSnapshot(riskState);
    return { riskState };
  });

  app.get("/kill-switch", async () => {
    return {
      killSwitch: app.services.riskEngine.getKillSwitchState()
    };
  });

  app.post("/kill-switch", async (request: FastifyRequest, reply: FastifyReply) => {
    const body = killSwitchUpdateSchema.safeParse(request.body ?? {});
    if (!body.success) {
      return reply.code(400).send({ error: body.error.flatten() });
    }

    const killSwitch = app.services.riskEngine.setKillSwitch(body.data.enabled);
    app.services.auditStore.logEvent("kill_switch_updated", {
      enabled: killSwitch.enabled,
      updatedAt: killSwitch.updatedAt,
      reason: body.data.reason || "",
      source: "api"
    });

    return { killSwitch };
  });

  app.get("/account-summary", async (_request: FastifyRequest, reply: FastifyReply) => {
    if (!(await requireIbkrConnection(app, reply, "account summary load", 5_000))) return;

    await app.services.executionGateway.refreshBrokerStatuses();
    await app.services.executionGateway.syncAccountState(app.services.accountState);
    await app.services.executionGateway.runExitAutomation(app.services.accountState);
    const openPositions = app.services.executionGateway.listOpenPositions();
    const pendingOrders = app.services.executionGateway.listPendingOrders();
    const brokerAccount = app.services.executionGateway.getLastAccountSnapshot();
    const riskState = app.services.riskEngine.buildRiskState(
      app.services.accountState,
      openPositions
    );
    app.services.auditStore.saveRiskSnapshot(riskState);
    const acceptanceGate = app.services.acceptanceGateService.refreshSnapshot();

    const snapshots = app.services.auditStore.listRiskSnapshots(300);
    const pnlSeries =
      snapshots.length > 0
        ? snapshots.map((snapshot) => ({
            timestamp: snapshot.timestamp,
            pnl: snapshot.dayRealizedPnl + snapshot.dayUnrealizedPnl,
            drawdownPct: snapshot.dailyDrawdownPct
          }))
        : [
            {
              timestamp: riskState.timestamp,
              pnl: riskState.dayRealizedPnl + riskState.dayUnrealizedPnl,
              drawdownPct: riskState.dailyDrawdownPct
            }
          ];

    return {
      generatedAt: new Date().toISOString(),
      summary: {
        accountEquity: app.services.accountState.accountEquity,
        dayRealizedPnl: riskState.dayRealizedPnl,
        dayUnrealizedPnl: riskState.dayUnrealizedPnl,
        dayTotalPnl: riskState.dayRealizedPnl + riskState.dayUnrealizedPnl,
        dailyDrawdownPct: riskState.dailyDrawdownPct,
        halted: riskState.halted,
        haltReasons: riskState.haltReasons,
        openPositions: openPositions.length,
        openPositionsSource: "local_journal",
        brokerOpenPositions: brokerAccount?.positionCount ?? null,
        pendingApprovals: pendingOrders.length
      },
      brokerAccount,
      pnlSeries,
      acceptanceGate: {
        pass: acceptanceGate.pass,
        checks: acceptanceGate.checks,
        completedTrades: acceptanceGate.trading.completedTrades,
        observedDays: acceptanceGate.period.observedDays
      }
    };
  });

  app.get("/acceptance-gate", async () => {
    const snapshot = app.services.acceptanceGateService.refreshSnapshot();
    return {
      generatedAt: new Date().toISOString(),
      gate: snapshot
    };
  });

  app.get("/positions", async () => {
    const nowMs = Date.now();
    const connectivity = await app.services.ibkrAdapter.checkConnectivity(4_000);
    app.services.executionGateway.notifyConnectivityStatus(connectivity);
    await app.services.executionGateway.refreshBrokerStatuses();
    const localOrders = app.services.auditStore.listOrders({ limit: 2_000 });
    const localEntryOrders = localOrders.filter((order) => order.intentType === "ENTRY");
    const localExitOrders = localOrders.filter((order) => order.intentType === "EXIT");

    const toContractKey = (
      symbol: string,
      expiration: string | null | undefined,
      strike: number | null | undefined,
      right: string | null | undefined
    ): string | null => {
      if (!expiration || typeof strike !== "number" || !Number.isFinite(strike) || !right) return null;
      const normalizedExpiration = String(expiration).replace(/\D/g, "");
      if (normalizedExpiration.length < 8) return null;
      const normalizedRightRaw = String(right).trim().toUpperCase();
      const normalizedRight =
        normalizedRightRaw === "C"
          ? "CALL"
          : normalizedRightRaw === "P"
            ? "PUT"
            : normalizedRightRaw;
      if (normalizedRight !== "CALL" && normalizedRight !== "PUT") return null;
      return `${symbol.toUpperCase()}|${normalizedExpiration.slice(0, 8)}|${normalizedRight}|${Number(strike).toFixed(4)}`;
    };

    const syntheticConIdFromOrderId = (orderId: string): number => {
      const compact = String(orderId).replace(/[^a-fA-F0-9]/g, "");
      const seed = compact.slice(0, 8);
      const parsed = Number.parseInt(seed, 16);
      if (Number.isFinite(parsed) && parsed > 0) return -parsed;
      let hash = 0;
      for (const char of String(orderId)) {
        hash = (hash << 5) - hash + char.charCodeAt(0);
        hash |= 0;
      }
      return -Math.max(1, Math.abs(hash));
    };

    const localByContract = new Map<string, typeof localEntryOrders>();
    const localEntryById = new Map(localEntryOrders.map((order) => [order.id, order]));
    for (const order of localEntryOrders) {
      const key = toContractKey(
        order.symbol,
        order.optionContract.expiration,
        order.optionContract.strike,
        order.optionContract.right
      );
      if (!key) continue;
      const list = localByContract.get(key) ?? [];
      list.push(order);
      localByContract.set(key, list);
    }
    for (const list of localByContract.values()) {
      list.sort((left, right) => {
        const leftTs = new Date(left.updatedAt ?? left.createdAt).getTime();
        const rightTs = new Date(right.updatedAt ?? right.createdAt).getTime();
        return rightTs - leftTs;
      });
    }

    const exitsByParent = new Map<string, typeof localExitOrders>();
    for (const exitOrder of localExitOrders) {
      if (!exitOrder.parentOrderId) continue;
      const list = exitsByParent.get(exitOrder.parentOrderId) ?? [];
      list.push(exitOrder);
      exitsByParent.set(exitOrder.parentOrderId, list);
    }

    const brokerPositionsRaw = connectivity.reachable
      ? await app.services.ibkrAdapter.getPositionsSnapshot()
      : [];
    const brokerMarkByContract = new Map<string, number>();
    for (const position of brokerPositionsRaw) {
      const key =
        position.secType === "OPT"
          ? toContractKey(position.symbol, position.expiration, position.strike, position.right)
          : null;
      if (!key) continue;
      if (typeof position.marketPrice !== "number" || !Number.isFinite(position.marketPrice) || position.marketPrice <= 0) continue;
      brokerMarkByContract.set(key, position.marketPrice);
    }

    const optionUnderlyingSymbols = [...new Set(
      brokerPositionsRaw
        .filter((position) => position.secType === "OPT")
        .map((position) => position.symbol.toUpperCase())
    )];
    const underlyingQuoteBySymbol = new Map<
      string,
      {
        last: number | null;
        bid: number | null;
        ask: number | null;
        fetchedAt: string;
      }
    >();
    if (connectivity.reachable) {
      await Promise.all(
        optionUnderlyingSymbols.map(async (symbol) => {
          try {
            const quote = await app.services.ibkrAdapter.getQuote(symbol);
            underlyingQuoteBySymbol.set(symbol, {
              last: quote?.last ?? null,
              bid: quote?.bid ?? null,
              ask: quote?.ask ?? null,
              fetchedAt: new Date().toISOString()
            });
          } catch {
            underlyingQuoteBySymbol.set(symbol, {
              last: null,
              bid: null,
              ask: null,
              fetchedAt: new Date().toISOString()
            });
          }
        })
      );
    }

    const positionStatuses = new Set(["SUBMITTED_PAPER", "SUBMITTED_LIVE", "FILLED", "EXITED"]);
    const localPositions = await Promise.all(
      localEntryOrders
        .filter((order) => positionStatuses.has(order.status))
        .map(async (order) => {
          const entryPrice = order.avgFillPrice ?? order.limitPrice;
          const multiplier = 100;
          const contractKey = toContractKey(
            order.optionContract.symbol,
            order.optionContract.expiration,
            order.optionContract.strike,
            order.optionContract.right
          );
          const linkedExits = exitsByParent.get(order.id) ?? [];

          const entryFilledQtyRaw =
            typeof order.filledQuantity === "number" && Number.isFinite(order.filledQuantity) && order.filledQuantity > 0
              ? order.filledQuantity
              : order.quantity;
          const entryFilledQty = Math.max(0, Number(entryFilledQtyRaw || 0));

          let closedQuantity = 0;
          let realizedPnl = 0;
          for (const exitOrder of linkedExits) {
            const exitQtyRaw =
              typeof exitOrder.filledQuantity === "number" &&
              Number.isFinite(exitOrder.filledQuantity) &&
              exitOrder.filledQuantity > 0
                ? exitOrder.filledQuantity
                : exitOrder.status === "FILLED" || exitOrder.status === "EXITED"
                  ? exitOrder.quantity
                  : 0;
            const exitQty = Math.max(0, Number(exitQtyRaw || 0));
            if (exitQty <= 0) continue;

            const exitPrice = exitOrder.avgFillPrice ?? exitOrder.limitPrice;
            if (
              Number.isFinite(exitPrice) &&
              exitPrice > 0 &&
              Number.isFinite(entryPrice) &&
              entryPrice > 0
            ) {
              realizedPnl += (exitPrice - entryPrice) * exitQty * multiplier;
            }
            closedQuantity += exitQty;
          }

          const remainingQuantity = Math.max(0, entryFilledQty - closedQuantity);
          const lifecycleState =
            remainingQuantity <= 1e-6
              ? "CLOSED"
              : closedQuantity > 1e-6
                ? "PARTIALLY_CLOSED"
                : "OPEN";

          const positionUnits = remainingQuantity * multiplier;
          const brokerMark =
            contractKey && brokerMarkByContract.has(contractKey)
              ? brokerMarkByContract.get(contractKey)
              : null;
          const markPrice =
            remainingQuantity > 0 &&
            typeof brokerMark === "number" &&
            Number.isFinite(brokerMark) &&
            brokerMark > 0
              ? brokerMark
              : remainingQuantity > 0 && connectivity.reachable
                ? await app.services.ibkrAdapter.getOptionMidPrice({
                    symbol: order.optionContract.symbol,
                    expiration: order.optionContract.expiration,
                    strike: order.optionContract.strike,
                    right: order.optionContract.right
                  })
                : null;
          const entryNotional = entryPrice > 0 ? entryPrice * positionUnits : null;
          const marketValue = typeof markPrice === "number" ? markPrice * positionUnits : null;
          const estimatedUnrealizedPnl =
            entryNotional !== null && marketValue !== null ? marketValue - entryNotional : null;
          const estimatedReturnPct =
            typeof markPrice === "number" && entryPrice > 0 ? (markPrice - entryPrice) / entryPrice : null;
          const createdAtMs = new Date(order.createdAt).getTime();
          const daysOpen =
            Number.isFinite(createdAtMs) && createdAtMs > 0
              ? Number(((nowMs - createdAtMs) / (24 * 60 * 60 * 1000)).toFixed(2))
              : null;

          return {
            orderId: order.id,
            contractKey,
            symbol: order.symbol,
            action: order.action,
            side: order.side,
            status: order.status,
            lifecycleState,
            quantity: order.quantity,
            entryFilledQuantity: entryFilledQty,
            closedQuantity,
            remainingQuantity,
            expiration: order.optionContract.expiration,
            strike: order.optionContract.strike,
            right: order.optionContract.right,
            entryPrice,
            entryNotional,
            markPrice,
            marketValue,
            realizedPnl,
            estimatedUnrealizedPnl,
            estimatedReturnPct,
            linkedExitCount: linkedExits.length,
            daysOpen,
            createdAt: order.createdAt,
            updatedAt: order.updatedAt ?? order.createdAt,
            brokerOrderId: order.brokerOrderId ?? null
          };
        })
    );

    const localContractAggregates = new Map<
      string,
      {
        entryQuantity: number;
        remainingQuantity: number;
        realizedPnl: number;
      }
    >();
    for (const localPosition of localPositions) {
      if (!localPosition.contractKey) continue;
      const current = localContractAggregates.get(localPosition.contractKey) ?? {
        entryQuantity: 0,
        remainingQuantity: 0,
        realizedPnl: 0
      };
      current.entryQuantity += localPosition.entryFilledQuantity;
      current.remainingQuantity += localPosition.remainingQuantity;
      current.realizedPnl += localPosition.realizedPnl ?? 0;
      localContractAggregates.set(localPosition.contractKey, current);
    }

    const brokerOpenPositions = brokerPositionsRaw
      .filter((position) => Math.abs(position.position) > 1e-6)
      .sort((left, right) => Math.abs(right.marketValue ?? 0) - Math.abs(left.marketValue ?? 0))
      .map((position) => {
        const contractKey =
          position.secType === "OPT"
            ? toContractKey(position.symbol, position.expiration, position.strike, position.right)
            : null;
        const matchedLocalOrders = contractKey ? localByContract.get(contractKey) ?? [] : [];
        const primaryMatch = matchedLocalOrders[0];
        const aggregate = contractKey ? localContractAggregates.get(contractKey) : null;
        const marketDataUpdatedAt = position.marketDataUpdatedAt ?? null;
        const marketDataStalenessMs =
          marketDataUpdatedAt && Number.isFinite(new Date(marketDataUpdatedAt).getTime())
            ? Math.max(0, nowMs - new Date(marketDataUpdatedAt).getTime())
            : null;
        const costBasis = Number.isFinite(position.avgCost) && Number.isFinite(position.position)
          ? Math.abs(position.avgCost * position.position)
          : null;
        const estimatedFees = null;
        const totalCostInclFees =
          costBasis !== null && Number.isFinite(costBasis)
            ? costBasis + (estimatedFees ?? 0)
            : null;
        const underlyingQuote = underlyingQuoteBySymbol.get(position.symbol.toUpperCase());
        const lifecycleState =
          aggregate && aggregate.entryQuantity > aggregate.remainingQuantity + 1e-6
            ? "PARTIALLY_CLOSED"
            : "OPEN";
        const realizedPnl =
          typeof position.realizedPnl === "number"
            ? position.realizedPnl
            : aggregate
              ? aggregate.realizedPnl
              : null;

        return {
          account: position.account ?? null,
          conId: position.conId,
          symbol: position.symbol,
          secType: position.secType,
          expiration: position.expiration ?? null,
          strike: position.strike ?? null,
          right: position.right ?? null,
          multiplier: position.multiplier,
          quantity: position.position,
          side: position.position > 0 ? "LONG" : "SHORT",
          lifecycleState,
          avgCost: position.avgCost,
          marketPrice: position.marketPrice ?? null,
          marketValue: position.marketValue ?? null,
          unrealizedPnl: position.unrealizedPnl ?? null,
          realizedPnl,
          marketDataUpdatedAt,
          marketDataStalenessMs,
          costBreakdown: {
            premiumCost: costBasis,
            fees: estimatedFees,
            totalCostInclFees,
            feeStatus: "unavailable"
          },
          underlying: {
            last: underlyingQuote?.last ?? null,
            bid: underlyingQuote?.bid ?? null,
            ask: underlyingQuote?.ask ?? null,
            snapshotAt: underlyingQuote?.fetchedAt ?? null
          },
          attribution: primaryMatch
            ? {
                origin: "BOT_RECOMMENDATION",
                matchMethod: "contract_key",
                linkedOrderId: primaryMatch.id,
                linkedOrderIds: matchedLocalOrders.map((order) => order.id),
                linkedOrderStatus: primaryMatch.status,
                linkedOrderCreatedAt: primaryMatch.createdAt,
                recommendation: {
                  action: primaryMatch.decision.action,
                  confidence: primaryMatch.decision.confidence,
                  compositeScore: primaryMatch.decision.scoreCard.compositeScore,
                  rationale: primaryMatch.decision.rationale,
                  decisionTimestamp: primaryMatch.decision.timestamp
                }
              }
            : {
                origin: "EXTERNAL_OR_UNKNOWN",
                matchMethod: contractKey ? "no_contract_match" : "not_option_contract",
                linkedOrderId: null,
                linkedOrderIds: [],
                linkedOrderStatus: null,
                linkedOrderCreatedAt: null,
                recommendation: null
              }
        };
      });

    const brokerOpenContractKeys = new Set(
      brokerPositionsRaw
        .filter((position) => position.secType === "OPT" && Math.abs(position.position) > 1e-6)
        .map((position) => toContractKey(position.symbol, position.expiration, position.strike, position.right))
        .filter((value): value is string => Boolean(value))
    );

    const brokerClosedPositions = localPositions
      .filter(
        (position) =>
          position.lifecycleState === "CLOSED" &&
          position.contractKey &&
          !brokerOpenContractKeys.has(position.contractKey)
      )
      .map((position) => {
        const localOrder = localEntryById.get(position.orderId);
        const contractKey = position.contractKey ?? "";
        const normalizedExpiration = String(position.expiration ?? "").replace(/\D/g, "");
        const normalizedRight = String(position.right ?? "").toUpperCase();
        return {
          account: brokerAccount?.accountCode ?? null,
          conId: syntheticConIdFromOrderId(position.orderId),
          symbol: position.symbol,
          secType: "OPT",
          expiration: position.expiration ?? null,
          strike: position.strike ?? null,
          right: normalizedRight === "C" ? "CALL" : normalizedRight === "P" ? "PUT" : position.right ?? null,
          multiplier: 100,
          quantity: 0,
          side: "FLAT",
          lifecycleState: "CLOSED",
          avgCost: position.entryPrice ?? null,
          marketPrice: null,
          marketValue: 0,
          unrealizedPnl: 0,
          realizedPnl:
            typeof position.realizedPnl === "number" && Number.isFinite(position.realizedPnl)
              ? position.realizedPnl
              : null,
          marketDataUpdatedAt: position.updatedAt ?? null,
          marketDataStalenessMs: null,
          costBreakdown: {
            premiumCost:
              typeof position.entryNotional === "number" && Number.isFinite(position.entryNotional)
                ? position.entryNotional
                : null,
            fees: null,
            totalCostInclFees:
              typeof position.entryNotional === "number" && Number.isFinite(position.entryNotional)
                ? position.entryNotional
                : null,
            feeStatus: "unavailable"
          },
          underlying: {
            last: null,
            bid: null,
            ask: null,
            snapshotAt: null
          },
          attribution: localOrder
            ? {
                origin: "BOT_RECOMMENDATION",
                matchMethod: "local_closed_position",
                linkedOrderId: localOrder.id,
                linkedOrderIds: [localOrder.id],
                linkedOrderStatus: localOrder.status,
                linkedOrderCreatedAt: localOrder.createdAt,
                recommendation: {
                  action: localOrder.decision.action,
                  confidence: localOrder.decision.confidence,
                  compositeScore: localOrder.decision.scoreCard.compositeScore,
                  rationale: localOrder.decision.rationale,
                  decisionTimestamp: localOrder.decision.timestamp
                }
              }
            : {
                origin: "EXTERNAL_OR_UNKNOWN",
                matchMethod: "local_closed_position_without_match",
                linkedOrderId: null,
                linkedOrderIds: [],
                linkedOrderStatus: null,
                linkedOrderCreatedAt: null,
                recommendation: null
              },
          contractKey,
          brokerDerived: true
        };
      })
      .sort((left, right) => {
        const leftTs = Date.parse(String(left.marketDataUpdatedAt ?? ""));
        const rightTs = Date.parse(String(right.marketDataUpdatedAt ?? ""));
        if (Number.isFinite(leftTs) && Number.isFinite(rightTs)) return rightTs - leftTs;
        if (Number.isFinite(rightTs)) return 1;
        if (Number.isFinite(leftTs)) return -1;
        return String(right.symbol).localeCompare(String(left.symbol));
      });

    const brokerPositions = [...brokerOpenPositions, ...brokerClosedPositions];

    const brokerAccount = app.services.executionGateway.getLastAccountSnapshot();
    const brokerNetMarketValue = brokerPositions.reduce(
      (sum, position) => sum + (position.marketValue ?? 0),
      0
    );
    const brokerNetUnrealizedPnl = brokerPositions.reduce(
      (sum, position) => sum + (position.unrealizedPnl ?? 0),
      0
    );
    const brokerNetRealizedPnl = brokerPositions.reduce(
      (sum, position) => sum + (position.realizedPnl ?? 0),
      0
    );
    const brokerStalePositions = brokerOpenPositions.filter(
      (position) =>
        typeof position.marketDataStalenessMs === "number" &&
        position.marketDataStalenessMs >= 120_000
    ).length;
    const localOpenPositionsCount = localPositions.filter(
      (position) => position.lifecycleState !== "CLOSED"
    ).length;
    const localClosedPositionsCount = localPositions.filter(
      (position) => position.lifecycleState === "CLOSED"
    ).length;
    const localEstimatedUnrealizedPnl = localPositions.reduce(
      (sum, position) => sum + (position.estimatedUnrealizedPnl ?? 0),
      0
    );
    const localRealizedPnl = localPositions.reduce(
      (sum, position) => sum + (position.realizedPnl ?? 0),
      0
    );

    return {
      generatedAt: new Date().toISOString(),
      summary: {
        brokerOpenPositions: brokerOpenPositions.length,
        brokerClosedPositions: brokerClosedPositions.length,
        brokerLinkedToBotPositions: brokerPositions.filter(
          (position) => position.attribution?.origin === "BOT_RECOMMENDATION"
        ).length,
        brokerStalePositions,
        localOpenPositions: localOpenPositionsCount,
        localClosedPositions: localClosedPositionsCount,
        brokerNetMarketValue,
        brokerNetUnrealizedPnl,
        brokerNetRealizedPnl,
        localEstimatedUnrealizedPnl,
        localRealizedPnl,
        localMarkedPositions: localPositions.filter(
          (position) =>
            position.lifecycleState !== "CLOSED" && typeof position.markPrice === "number"
        ).length
      },
      connectivity: {
        reachable: connectivity.reachable,
        mode: connectivity.detectedMode ?? null,
        host: connectivity.host,
        port: connectivity.port
      },
      broker: {
        source: brokerAccount?.source ?? "unavailable",
        accountCode: brokerAccount?.accountCode ?? null,
        positions: brokerPositions
      },
      local: {
        source: "local_journal",
        positions: localPositions
      }
    };
  });

  app.get("/ibkr-status", async () => {
    const status = await app.services.ibkrAdapter.checkConnectivity();
    app.services.executionGateway.notifyConnectivityStatus(status);
    return {
      ibkr: status,
      config: {
        botMode: settings.paperMode ? "paper" : "live",
        paperMode: settings.paperMode,
        manualApprovalRequired: settings.manualApprovalRequired,
        integrations: [
          {
            id: "ibkr",
            name: "IBKR",
            configured:
              settings.ibkrEnabled &&
              settings.ibkrHost.trim().length > 0 &&
              Number.isFinite(settings.ibkrPort) &&
              settings.ibkrPort > 0,
            note: settings.ibkrEnabled
              ? "IBKR enabled."
              : "IBKR disabled. Set IBKR_ENABLED=true to use broker connectivity."
          },
          {
            id: "alpaca",
            name: "Alpaca",
            configured:
              settings.alpacaApiKey.trim().length > 0 &&
              settings.alpacaApiSecret.trim().length > 0 &&
              settings.alpacaDataBaseUrl.trim().length > 0,
            note:
              settings.alpacaApiKey.trim().length > 0 &&
              settings.alpacaApiSecret.trim().length > 0
                ? "API key + secret configured."
                : "Missing ALPACA_API_KEY or ALPACA_API_SECRET."
          },
          {
            id: "fmp",
            name: "FMP",
            configured:
              settings.fmpApiKey.trim().length > 0 &&
              settings.fmpBaseUrl.trim().length > 0,
            note:
              settings.fmpApiKey.trim().length > 0
                ? "API key configured."
                : "Missing FMP_API_KEY."
          },
          {
            id: "eodhd",
            name: "EODHD",
            configured:
              settings.eodhdApiKey.trim().length > 0 &&
              settings.eodhdBaseUrl.trim().length > 0,
            note:
              settings.eodhdApiKey.trim().length > 0
                ? "API key configured."
                : "Missing EODHD_API_KEY."
          },
          {
            id: "alpha_vantage",
            name: "Alpha Vantage",
            configured: settings.alphaVantageApiKey.trim().length > 0,
            note:
              settings.alphaVantageApiKey.trim().length > 0
                ? "API key configured."
                : "Missing ALPHA_VANTAGE_API_KEY."
          },
          {
            id: "fred",
            name: "FRED",
            configured: settings.fredApiKey.trim().length > 0,
            note:
              settings.fredApiKey.trim().length > 0
                ? "API key configured."
                : "Missing FRED_API_KEY."
          },
          {
            id: "openai",
            name: "OpenAI",
            configured: settings.openAiApiKey.trim().length > 0,
            note:
              settings.openAiApiKey.trim().length > 0
                ? `API key configured (model: ${settings.openAiModel}).`
                : "Missing OPENAI_API_KEY."
          },
          {
            id: "ai_discovery",
            name: "AI Discovery",
            configured: settings.aiDiscoveryEnabled && settings.openAiApiKey.trim().length > 0,
            note: !settings.aiDiscoveryEnabled
              ? "AI discovery provider disabled (AI_DISCOVERY_ENABLED=false)."
              : settings.openAiApiKey.trim().length > 0
                ? "OpenAI-backed scanner provider enabled."
                : "Missing OPENAI_API_KEY."
          }
        ]
      }
    };
  });

  app.get("/ibkr-readiness", async () => {
    const generatedAt = new Date().toISOString();
    const nowMs = Date.now();
    const policy = app.services.runtimePolicy.getPolicy();
    const connectivity = await app.services.ibkrAdapter.checkConnectivity(4_000);
    app.services.executionGateway.notifyConnectivityStatus(connectivity);
    const runtime = app.services.ibkrAdapter.getRuntimeStatus(nowMs);
    const entitlements = app.services.ibkrAdapter.getMarketDataEntitlementState(nowMs);

    const checks = {
      connectivity: {
        ok: connectivity.reachable,
        message: connectivity.message,
        host: connectivity.host,
        port: connectivity.port,
        mode: connectivity.detectedMode
      },
      queue: {
        ok: runtime.queue.depth < 20,
        depth: runtime.queue.depth,
        activeWorkers: runtime.queue.activeWorkers,
        maxWorkers: runtime.queue.maxWorkers,
        cooldownActive: runtime.requestCooldown.active,
        cooldownReason: runtime.requestCooldown.reason
      },
      quote: {
        ok: false,
        symbol: "SPY",
        delayedOnly: entitlements.delayedOnly,
        source: "unavailable" as "live" | "delayed_only" | "unavailable",
        error: null as string | null
      },
      historical: {
        ok: false,
        symbol: "SPY",
        bars: 0,
        error: null as string | null
      },
      scanner: {
        ok: false,
        scanCode: policy.ibkrScanCode,
        source: "none" as "tws_socket" | "none",
        symbols: [] as string[],
        fallbackReason: "",
        error: null as string | null
      },
      positions: {
        ok: false,
        count: 0,
        error: null as string | null
      }
    };

    if (connectivity.reachable) {
      try {
        const quote = await app.services.ibkrAdapter.getQuote("SPY");
        if (quote) {
          checks.quote.ok = true;
          checks.quote.source = entitlements.delayedOnly ? "delayed_only" : "live";
        }
      } catch (error) {
        checks.quote.error = (error as Error).message;
      }

      try {
        const closes = await app.services.ibkrAdapter.getRecentDailyCloses("SPY", 30);
        checks.historical.bars = closes.length;
        checks.historical.ok = closes.length >= 20;
        if (closes.length < 20) {
          checks.historical.error = "Insufficient bars returned.";
        }
      } catch (error) {
        checks.historical.error = (error as Error).message;
      }

      try {
        const scanner = await app.services.ibkrAdapter.getScannerSymbolsWithSource({
          limit: 10,
          scanCode: policy.ibkrScanCode
        });
        checks.scanner.ok = scanner.symbols.length > 0;
        checks.scanner.source = scanner.source;
        checks.scanner.symbols = scanner.symbols.slice(0, 10);
        checks.scanner.fallbackReason = scanner.fallbackReason;
      } catch (error) {
        checks.scanner.error = (error as Error).message;
      }

      try {
        const positions = await app.services.ibkrAdapter.getPositionsSnapshot();
        checks.positions.ok = true;
        checks.positions.count = positions.length;
      } catch (error) {
        checks.positions.error = (error as Error).message;
      }
    }

    const advice: string[] = [];
    if (!checks.connectivity.ok) {
      advice.push("IBKR is disconnected. Start Gateway/TWS and verify API host/port/client ID.");
    }
    if (!checks.queue.ok) {
      advice.push("IBKR queue depth is high. Wait for backlog to drain before running scans.");
    }
    if (!checks.quote.ok) {
      advice.push(
        entitlements.delayedOnly
          ? "Quote data is delayed-only. Add live US market data subscriptions for faster/scanner-friendly behavior."
          : "Quote check failed. Confirm market-data entitlements and API market-data permissions."
      );
    }
    if (!checks.historical.ok) {
      advice.push("Historical bars check failed. Verify TWS/Gateway/API version compatibility and active session.");
    }
    if (runtime.compatibility.historicalFractionalRulesUnsupported) {
      advice.push(
        "IBKR historical requests are disabled in this session due to API protocol compatibility (fractional share rules). Upgrade Gateway/TWS/API to a compatible version."
      );
    }
    if (!checks.scanner.ok) {
      advice.push("IBKR scanner returned no symbols. Use fallback providers or try a different scan code/session window.");
    }

    return {
      generatedAt,
      overall:
        checks.connectivity.ok &&
        checks.queue.ok &&
        checks.quote.ok &&
        checks.historical.ok,
      checks,
      entitlements,
      runtime,
      advice
    };
  });

  app.post("/ibkr-launch", async (request: FastifyRequest, reply: FastifyReply) => {
    const body = ibkrLaunchRequestSchema.safeParse(request.body ?? {});
    if (!body.success) {
      return reply.code(400).send({ error: body.error.flatten() });
    }

    const target = body.data.target ?? settings.ibkrLaunchTarget;
    const launch = await app.services.ibkrAdapter.launch(target);
    app.services.auditStore.logEvent("ibkr_launch_requested", {
      target,
      launched: launch.launched,
      dryRun: launch.dryRun,
      platform: launch.platform,
      message: launch.message
    });

    return { launch };
  });

  app.get("/bot-policy", async () => {
    return {
      policy: app.services.runtimePolicy.getPolicy(),
      guidelines: app.services.runtimePolicy.getGuidelines()
    };
  });

  app.patch("/bot-policy", async (request: FastifyRequest, reply: FastifyReply) => {
    const body = botPolicyPatchSchema.safeParse(request.body ?? {});
    if (!body.success) {
      return reply.code(400).send({ error: body.error.flatten() });
    }

    const updated = app.services.runtimePolicy.updatePolicy({
      ...body.data,
      universeSymbols: body.data.universeSymbols?.map((item) => item.toUpperCase())
    });
    app.services.auditStore.logEvent("bot_policy_updated", { policy: updated });
    return {
      policy: updated,
      guidelines: app.services.runtimePolicy.getGuidelines()
    };
  });

  app.post("/bot-policy/reset", async () => {
    const reset = app.services.runtimePolicy.resetPolicy();
    return {
      policy: reset,
      guidelines: app.services.runtimePolicy.getGuidelines()
    };
  });

  app.get("/env-config", async () => {
    return app.services.envConfig.snapshot();
  });

  app.patch("/env-config", async (request: FastifyRequest, reply: FastifyReply) => {
    const body = envConfigPatchSchema.safeParse(request.body ?? {});
    if (!body.success) {
      return reply.code(400).send({ error: body.error.flatten() });
    }

    try {
      const result = app.services.envConfig.updateValues(body.data.values);
      app.services.auditStore.logEvent("env_config_updated", {
        changedKeys: result.changedKeys,
        restartRequiredKeys: result.restartRequiredKeys
      });
      return result;
    } catch (error) {
      return reply.code(400).send({ error: (error as Error).message });
    }
  });

  app.post("/app/refresh", async () => {
    const runtime = app.services.envConfig.refreshRuntime();
    app.services.auditStore.logEvent("app_runtime_refreshed", {
      scanCadenceMinutes: runtime.scanCadenceMinutes
    });
    return {
      ...app.services.envConfig.snapshot(),
      runtime
    };
  });

  app.post("/app/restart", async () => {
    const restart = app.services.envConfig.prepareRestart();
    app.services.auditStore.logEvent("app_restart_requested", {
      scheduled: restart.scheduled,
      delayMs: restart.delayMs,
      message: restart.message,
      followupCommand: restart.followupCommand
    });

    if (restart.scheduled) {
      setTimeout(() => {
        process.exit(0);
      }, restart.delayMs);
    }

    return { restart };
  });

  app.get("/orders/pending", async () => {
    await app.services.executionGateway.syncAccountState(app.services.accountState);
    await app.services.executionGateway.runExitAutomation(app.services.accountState);
    return {
      orders: app.services.executionGateway.listPendingOrders()
    };
  });

  app.get("/orders/recent", async () => {
    await app.services.executionGateway.refreshBrokerStatuses();
    await app.services.executionGateway.syncAccountState(app.services.accountState);
    return {
      orders: app.services.executionGateway.listRecentOrders()
    };
  });

  app.get("/config", async () => ({
    ...app.services.runtimePolicy.getPolicy(),
    paperMode: settings.paperMode,
    manualApprovalRequired: settings.manualApprovalRequired,
    scanCadenceMinutes: settings.scanCadenceMinutes,
    timezone: settings.timezone,
    universe: app.services.runtimePolicy.getPolicy().universeSymbols
  }));
};
