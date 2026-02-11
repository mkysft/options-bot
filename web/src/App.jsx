import { useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  AlertTriangle,
  BellRing,
  ChevronLeft,
  ChevronRight,
  Clock3,
  ExternalLink,
  HelpCircle,
  CheckCircle2,
  Settings2,
  Webhook,
  X,
  Zap,
  RefreshCcw
} from "lucide-react";

import { api, ApiRequestError } from "./api";
import { Badge } from "./components/badge";
import { Button } from "./components/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "./components/card";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle
} from "./components/dialog";
import { Input } from "./components/input";
import { Popover, PopoverContent, PopoverTrigger } from "./components/popover";
import {
  Select,
  SelectItem,
  SelectPopup,
  SelectTrigger,
  SelectValue
} from "./components/select";
import { Spinner } from "./components/spinner";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "./components/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "./components/tabs";
import { ToastProvider, toastManager } from "./components/toast";

const numberFmt = new Intl.NumberFormat("en-US", { maximumFractionDigits: 2 });
const currencyFmt = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  maximumFractionDigits: 2
});
const pctFmt = (value) => `${(value * 100).toFixed(2)}%`;
const signedMoney = (value) => `${value >= 0 ? "+" : "-"}${currencyFmt.format(Math.abs(value))}`;
const formatTimestamp = (value) => (value ? new Date(value).toLocaleString() : "—");
const formatDuration = (milliseconds) => {
  const totalSeconds = Math.max(0, Math.floor(milliseconds / 1000));
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  if (minutes <= 0) return `${seconds}s`;
  if (minutes < 60) return `${minutes}m ${seconds}s`;
  const hours = Math.floor(minutes / 60);
  const remMinutes = minutes % 60;
  return `${hours}h ${remMinutes}m`;
};
const formatIntervalShort = (milliseconds) => {
  if (!Number.isFinite(milliseconds) || milliseconds <= 0) return "—";
  if (milliseconds < 1_000) return `${milliseconds}ms`;
  if (milliseconds < 60_000) {
    const seconds = milliseconds / 1_000;
    return `${seconds.toFixed(seconds % 1 === 0 ? 0 : 1)}s`;
  }
  if (milliseconds < 3_600_000) {
    const minutes = milliseconds / 60_000;
    return `${minutes.toFixed(minutes % 1 === 0 ? 0 : 1)}m`;
  }
  const hours = milliseconds / 3_600_000;
  return `${hours.toFixed(hours % 1 === 0 ? 0 : 1)}h`;
};
const formatTimestampWithCountdown = (value) => {
  if (!value) return "—";
  const targetMs = new Date(value).getTime();
  if (!Number.isFinite(targetMs)) return "—";
  const diffMs = targetMs - Date.now();
  const countdown = diffMs <= 0 ? "now" : `in ${formatDuration(diffMs)}`;
  return `${formatTimestamp(value)} (${countdown})`;
};
const formatStaleness = (milliseconds) => {
  if (!Number.isFinite(milliseconds)) return "unknown";
  if (milliseconds < 30_000) return "fresh";
  if (milliseconds < 120_000) return `${formatDuration(milliseconds)} old`;
  return `${formatDuration(milliseconds)} stale`;
};
const toDomId = (prefix, key) =>
  `${prefix}-${String(key ?? "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, "-")
    .replace(/^-+|-+$/g, "")}`;
const contractLabel = (expiration, strike, right) =>
  [expiration ?? "-", strike !== null && strike !== undefined ? numberFmt.format(strike) : "-", right ?? "-"].join(" ");

const POLICY_SECTIONS = [
  {
    title: "Scanner + Signal",
    fields: ["scanTopN", "minCompositeScore", "minDirectionalProbability"]
  },
  {
    title: "Contract Filters",
    fields: ["dteMin", "dteMax"]
  },
  {
    title: "Risk Limits",
    fields: ["maxPremiumRiskPct", "dailyDrawdownLimitPct", "correlationCapPerDirection"]
  },
  {
    title: "Exit Harness",
    fields: [
      "takeProfitPct",
      "stopLossPct",
      "maxHoldDays",
      "preEventExitWindowHours",
      "preEventSecFilingLookbackHours",
      "preEventSecFilingRiskThreshold"
    ]
  }
];

const POLICY_FIELD_META = {
  scanTopN: { integer: true, step: "1" },
  minCompositeScore: { integer: false, step: "1" },
  minDirectionalProbability: { integer: false, step: "0.01" },
  dteMin: { integer: true, step: "1" },
  dteMax: { integer: true, step: "1" },
  maxPremiumRiskPct: { integer: false, step: "0.1" },
  dailyDrawdownLimitPct: { integer: false, step: "0.1" },
  correlationCapPerDirection: { integer: true, step: "1" },
  takeProfitPct: { integer: false, step: "0.1" },
  stopLossPct: { integer: false, step: "0.1" },
  maxHoldDays: { integer: true, step: "1" },
  preEventExitWindowHours: { integer: true, step: "1" },
  preEventSecFilingLookbackHours: { integer: true, step: "1" },
  preEventSecFilingRiskThreshold: { integer: false, step: "0.01" }
};

const ALL_POLICY_FIELDS = POLICY_SECTIONS.flatMap((section) => section.fields);
const POLICY_PERCENT_FIELDS = new Set([
  "maxPremiumRiskPct",
  "dailyDrawdownLimitPct",
  "takeProfitPct",
  "stopLossPct"
]);

const isPolicyPercentField = (field) => POLICY_PERCENT_FIELDS.has(field);

const formatPolicyInputValue = (field, value) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return "";
  const normalized = isPolicyPercentField(field) ? parsed * 100 : parsed;
  return String(Number(normalized.toFixed(6)));
};

const toPolicyPayloadNumber = (field, value) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return Number.NaN;
  const normalized = isPolicyPercentField(field) ? parsed / 100 : parsed;
  return normalized;
};

const formatGuidelineRange = (field, bound) => {
  if (bound === undefined || bound === null || !Number.isFinite(Number(bound))) return "-";
  const parsed = Number(bound);
  if (!isPolicyPercentField(field)) return String(parsed);
  return `${Number((parsed * 100).toFixed(6))}%`;
};

const policyDescription = (field, guideline) => {
  const base = guideline?.description ?? "";
  if (!isPolicyPercentField(field)) return base;
  return `${base} Enter as percent (e.g. 60 = 60%).`;
};

const SCREENER_MODE_OPTIONS = [
  { value: "MOST_ACTIVE", label: "Most Active" },
  { value: "HOT_BY_VOLUME", label: "Hot by Volume" },
  { value: "TOP_PERC_GAIN", label: "Top % Gainers" },
  { value: "TOP_PERC_LOSE", label: "Top % Losers" },
  { value: "TOP_TRADE_RATE", label: "Top Trade Rate" },
  { value: "TOP_PRICE_RANGE", label: "Top Price Range" },
  { value: "HIGH_OPEN_GAP", label: "High Open Gap" },
  { value: "LOW_OPEN_GAP", label: "Low Open Gap" }
];

const DEFAULT_SCREENER_MODE = "MOST_ACTIVE";
const ANALYSIS_PROVIDER_OPTIONS = [
  { value: "ALPACA", label: "Alpaca First" },
  { value: "AUTO", label: "Auto (IBKR then Alpaca)" },
  { value: "IBKR", label: "IBKR Only" }
];
const DEFAULT_ANALYSIS_PROVIDER = "ALPACA";
const SCANNER_PROVIDER_KEY = "SCANNER_PROVIDER_ORDER";
const SCANNER_PROVIDER_CUSTOM_VALUE = "__custom__";
const SCANNER_PROVIDER_ALLOWED = [
  "ibkr",
  "fmp",
  "eodhd",
  "alpaca",
  "alpha_vantage",
  "ai_discovery"
];
const SCANNER_PROVIDER_MODE_OPTIONS = [
  {
    value: "ibkr",
    label: "IBKR Only"
  },
  {
    value: "alpaca",
    label: "Alpaca Only"
  },
  {
    value: "fmp",
    label: "FMP Only"
  },
  {
    value: "eodhd",
    label: "EODHD Only"
  },
  {
    value: "alpha_vantage",
    label: "Alpha Vantage Only"
  },
  {
    value: "ai_discovery",
    label: "AI Discovery Only"
  },
  {
    value: "ibkr,alpaca",
    label: "IBKR -> Alpaca"
  },
  {
    value: "ibkr,fmp,eodhd,alpaca,alpha_vantage,ai_discovery",
    label: "Hybrid Fallback Chain (Recommended)"
  },
  {
    value: SCANNER_PROVIDER_CUSTOM_VALUE,
    label: "Custom Order"
  }
];

const normalizeScannerProviderOrder = (value) => {
  const raw = String(value ?? "")
    .split(",")
    .map((provider) => provider.trim().toLowerCase())
    .filter((provider) => provider.length > 0 && SCANNER_PROVIDER_ALLOWED.includes(provider));
  return [...new Set(raw)].join(",");
};

const parseUniverseInput = (value) =>
  value
    .split(",")
    .map((symbol) => symbol.trim().toUpperCase())
    .filter(Boolean);

const parseTopNInput = (value) => {
  const parsed = Number.parseInt(String(value ?? "").trim(), 10);
  if (Number.isFinite(parsed) && parsed > 0) return Math.min(parsed, 100);
  return 10;
};

const POSITION_PAGE_SIZE_OPTIONS = [10, 25, 50, 100];
const POSITION_PAGE_SIZE_ITEMS = POSITION_PAGE_SIZE_OPTIONS.map((option) => ({
  label: String(option),
  value: String(option)
}));
const EMPTY_ARRAY = Object.freeze([]);

const actionBadgeVariant = (action) => {
  if (action === "CALL") return "success";
  if (action === "PUT") return "warning";
  return "outline";
};

const entitlementStateVariant = (state) => {
  if (state === "live") return "success";
  if (state === "delayed") return "warning";
  if (state === "blocked") return "destructive";
  if (state === "connectivity") return "outline";
  if (state === "invalid_contract") return "outline";
  if (state === "error") return "outline";
  return "outline";
};

const entitlementStateLabel = (state) => {
  if (state === "live") return "LIVE";
  if (state === "delayed") return "DELAYED";
  if (state === "blocked") return "BLOCKED";
  if (state === "connectivity") return "CONNECTIVITY";
  if (state === "invalid_contract") return "INVALID CONTRACT";
  if (state === "error") return "ERROR";
  return "UNKNOWN";
};

const positionLifecycleBadgeVariant = (state) => {
  if (state === "OPEN") return "success";
  if (state === "PARTIALLY_CLOSED") return "warning";
  if (state === "CLOSED") return "outline";
  return "outline";
};

const formatLifecycleLabel = (state) =>
  String(state ?? "UNKNOWN").replaceAll("_", " ");

const POSITION_STATE_FILTER_OPTIONS = [
  { value: "ALL", label: "All States" },
  { value: "OPEN", label: "Open" },
  { value: "PARTIALLY_CLOSED", label: "Partially Closed" },
  { value: "CLOSED", label: "Closed" }
];

const filterByLifecycleState = (rows, stateFilter) => {
  if (!Array.isArray(rows)) return EMPTY_ARRAY;
  const normalized = String(stateFilter ?? "ALL").toUpperCase();
  if (normalized === "ALL") return rows;
  return rows.filter((row) => String(row?.lifecycleState ?? "").toUpperCase() === normalized);
};

const JsonBox = ({ value, empty = "No data." }) => (
  <pre className="max-h-80 overflow-auto rounded-xl border border-slate-200 bg-white/70 p-3 text-xs leading-5 text-slate-800">
    {value ? JSON.stringify(value, null, 2) : empty}
  </pre>
);

const MetricTile = ({ label, value, emphasis = false }) => (
  <div className="rounded-xl border border-slate-200 bg-white/70 p-3">
    <p className="text-xs uppercase tracking-wide text-slate-500">{label}</p>
    <p className={`mt-1 text-sm ${emphasis ? "font-semibold text-slate-950" : "text-slate-800"}`}>{value}</p>
  </div>
);

const PositionsPagination = ({
  total,
  page,
  pageSize,
  totalPages,
  onPageChange,
  onPageSizeChange
}) => {
  const rangeStart = total === 0 ? 0 : (page - 1) * pageSize + 1;
  const rangeEnd = total === 0 ? 0 : Math.min(page * pageSize, total);

  return (
    <div className="flex flex-wrap items-center justify-end gap-2">
      <p className="text-xs text-slate-500">
        Showing {rangeStart}-{rangeEnd} of {total}
      </p>
      <div className="flex items-center gap-1">
        <span className="text-xs text-slate-500">Rows</span>
        <Select
          items={POSITION_PAGE_SIZE_ITEMS}
          value={String(pageSize)}
          onValueChange={(value) => {
            if (value === null || value === undefined) return;
            onPageSizeChange(Number(value));
          }}
        >
          <SelectTrigger size="sm" className="h-8 w-20 min-w-20 text-xs">
            <SelectValue />
          </SelectTrigger>
          <SelectPopup>
            {POSITION_PAGE_SIZE_ITEMS.map((item) => (
              <SelectItem key={item.value} value={item.value}>
                {item.label}
              </SelectItem>
            ))}
          </SelectPopup>
        </Select>
      </div>
      <div className="flex items-center gap-1">
        <Button
          variant="outline"
          size="icon-sm"
          disabled={page <= 1}
          onClick={() => onPageChange(page - 1)}
        >
          <ChevronLeft className="size-4" />
        </Button>
        <span className="min-w-20 text-center text-xs text-slate-600">
          Page {page}/{totalPages}
        </span>
        <Button
          variant="outline"
          size="icon-sm"
          disabled={page >= totalPages}
          onClick={() => onPageChange(page + 1)}
        >
          <ChevronRight className="size-4" />
        </Button>
      </div>
    </div>
  );
};

const TriggerListPopover = ({ triggers }) => {
  const [open, setOpen] = useState(false);
  const closeTimeoutRef = useRef(null);

  const clearCloseTimeout = () => {
    if (closeTimeoutRef.current) {
      clearTimeout(closeTimeoutRef.current);
      closeTimeoutRef.current = null;
    }
  };

  const openPopover = () => {
    clearCloseTimeout();
    setOpen(true);
  };

  const closePopoverSoon = () => {
    clearCloseTimeout();
    closeTimeoutRef.current = setTimeout(() => {
      setOpen(false);
    }, 140);
  };

  useEffect(
    () => () => {
      clearCloseTimeout();
    },
    []
  );

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger
        aria-label="Show trigger sources"
        className="inline-flex size-5 items-center justify-center rounded-sm text-amber-600 hover:bg-amber-50 focus-visible:ring-2 focus-visible:ring-amber-400/50 focus-visible:outline-none"
        onClick={() => setOpen((previous) => !previous)}
        onMouseEnter={openPopover}
        onMouseLeave={closePopoverSoon}
      >
        <Zap className="size-3.5" />
      </PopoverTrigger>
      <PopoverContent
        align="start"
        className="w-80 max-w-[80vw]"
        side="top"
        onMouseEnter={openPopover}
        onMouseLeave={closePopoverSoon}
      >
        <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">Trigger Sources</p>
        {triggers.length > 0 ? (
          <ul className="mt-2 space-y-1 text-xs text-slate-700">
            {triggers.map((trigger) => (
              <li key={trigger} className="rounded-md bg-slate-50 px-2 py-1">
                {trigger}
              </li>
            ))}
          </ul>
        ) : (
          <p className="mt-2 text-xs text-slate-600">Trigger details unavailable.</p>
        )}
      </PopoverContent>
    </Popover>
  );
};

const formatSourceLabel = (value) =>
  String(value ?? "unknown")
    .replaceAll("_", " ")
    .toUpperCase();

const recommendationErrorStageLabel = (stage) => {
  if (stage === "scanner") return "Scanner";
  if (stage === "analysis") return "Analysis";
  if (stage === "decision") return "Decision";
  if (stage === "timeout") return "Timeout";
  if (stage === "internal") return "Internal";
  return "Unknown";
};

const formatNumeric = (value, digits = 3) => {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return null;
  return parsed.toFixed(digits);
};

const formatProposeOrderError = (error) => {
  if (!(error instanceof ApiRequestError)) return String(error);

  const payload = error.payload;
  if (!payload || typeof payload !== "object") return error.message;
  const reasonCode = typeof payload.reasonCode === "string" ? payload.reasonCode : null;

  if (reasonCode === "NO_TRADE") {
    const reasons = Array.isArray(payload.failureReasons)
      ? payload.failureReasons
          .map((item) => (item && typeof item.message === "string" ? item.message.trim() : ""))
          .filter((item) => item.length > 0)
      : [];

    const thresholds = payload.diagnostics?.policyThresholds ?? null;
    const actuals = payload.diagnostics?.actuals ?? null;
    const scoreText =
      thresholds && actuals
        ? `score ${formatNumeric(actuals.compositeScore, 2) ?? "?"}/${formatNumeric(
            thresholds.minCompositeScore,
            2
          ) ?? "?"}, p(up/down) ${formatNumeric(actuals.directionalUpProb, 3) ?? "?"}/${formatNumeric(
            actuals.directionalDownProb,
            3
          ) ?? "?"} (min ${formatNumeric(thresholds.minDirectionalProbability, 3) ?? "?"})`
        : "";

    const vetoFlags = Array.isArray(payload.vetoFlags)
      ? payload.vetoFlags.filter((flag) => typeof flag === "string" && flag.trim().length > 0)
      : [];

    const parts = [
      typeof payload.error === "string" ? payload.error : error.message,
      reasons.length > 0 ? `Reasons: ${reasons.slice(0, 3).join(" | ")}` : "",
      scoreText ? `Metrics: ${scoreText}.` : "",
      vetoFlags.length > 0 ? `Veto flags: ${vetoFlags.join(", ")}.` : "",
      typeof payload.suggestion === "string" ? payload.suggestion : ""
    ].filter(Boolean);

    return parts.join(" ");
  }

  if (typeof payload.error === "string" && payload.error.trim().length > 0) {
    const suggestion =
      typeof payload.suggestion === "string" && payload.suggestion.trim().length > 0
        ? ` ${payload.suggestion}`
        : "";
    return `${payload.error}${suggestion}`;
  }

  return error.message;
};

const ACCEPTANCE_CHECK_LABELS = {
  minRunDuration: "Min 8-week Run",
  positiveExpectancy: "Positive Expectancy",
  profitFactor: "Profit Factor >= 1.2",
  maxDrawdown: "Max Drawdown <= 12%",
  noPolicyViolations: "No Policy Violations"
};

const RecommendationEvidencePanel = ({ entry }) => {
  const evidence = entry?.evidence;
  if (!evidence) {
    return (
      <div className="rounded-xl border border-slate-200 bg-white/70 p-3 text-xs text-slate-600">
        Evidence payload unavailable for this recommendation.
      </div>
    );
  }

  const coverage = evidence.indicatorCoverage ?? {};
  const quality = evidence.dataQuality ?? {};
  const indicators = Array.isArray(evidence.indicators) ? evidence.indicators : [];
  const articles = Array.isArray(evidence.context?.newsArticles) ? evidence.context.newsArticles : [];
  const notes = Array.isArray(evidence.notes) ? evidence.notes : [];
  const optionSummary = evidence.optionChain?.summary ?? {};
  const gateChecks = Array.isArray(evidence.gateChecks) ? evidence.gateChecks : [];
  const scoreDecomposition = evidence.scoreDecomposition ?? {};
  const weightedScore = scoreDecomposition.weighted ?? {};
  const provenance = evidence.provenance ?? {};
  const benchmarkReturns = provenance.benchmarkReturns ?? {};

  return (
    <div className="space-y-3 rounded-xl border border-slate-200 bg-white/85 p-3">
      <div className="flex flex-wrap items-center gap-2">
        <Badge variant={quality.passed ? "success" : "destructive"}>
          Data Gate {quality.passed ? "PASS" : "BLOCKED"}
        </Badge>
        <Badge variant={Number(coverage.available ?? 0) > 0 ? "outline" : "destructive"}>
          Indicators {Number(coverage.available ?? 0)}/{Number(coverage.total ?? 0)}
        </Badge>
        <Badge variant={quality.hasCoreMarketData ? "success" : "destructive"}>
          Core Market {quality.hasCoreMarketData ? "OK" : "MISSING"}
        </Badge>
        <Badge variant={quality.hasCoreOptionsData ? "success" : "destructive"}>
          Core Options {quality.hasCoreOptionsData ? "OK" : "MISSING"}
        </Badge>
      </div>

      <div className="grid grid-cols-1 gap-2 md:grid-cols-3 xl:grid-cols-7">
        <MetricTile label="Quote Source" value={formatSourceLabel(evidence.sources?.quote)} />
        <MetricTile label="Closes Source" value={formatSourceLabel(evidence.sources?.closes)} />
        <MetricTile label="Bars Source" value={formatSourceLabel(evidence.sources?.bars)} />
        <MetricTile label="Options Source" value={formatSourceLabel(evidence.sources?.optionChain)} />
        <MetricTile label="News Source" value={formatSourceLabel(evidence.sources?.newsSentiment)} />
        <MetricTile label="Event Source" value={formatSourceLabel(evidence.sources?.event)} />
        <MetricTile label="Macro Source" value={formatSourceLabel(evidence.sources?.macro)} />
      </div>

      <div className="grid grid-cols-1 gap-2 md:grid-cols-2 xl:grid-cols-8">
        <MetricTile label="Composite Score" value={numberFmt.format(Number(evidence.scoreCard?.compositeScore ?? 0))} emphasis />
        <MetricTile label="P(Up)" value={Number(evidence.featureVector?.directionalUpProb ?? 0).toFixed(3)} />
        <MetricTile label="P(Down)" value={Number(evidence.featureVector?.directionalDownProb ?? 0).toFixed(3)} />
        <MetricTile label="RS 20D" value={pctFmt(Number(evidence.featureVector?.relativeStrength20d ?? 0))} />
        <MetricTile label="RS 60D" value={pctFmt(Number(evidence.featureVector?.relativeStrength60d ?? 0))} />
        <MetricTile label="Regime Stability" value={Number(evidence.featureVector?.regimeStability ?? 0).toFixed(3)} />
        <MetricTile label="Options Quality" value={Number(evidence.featureVector?.optionsQuality ?? 0).toFixed(3)} />
        <MetricTile label="Spread" value={pctFmt(Number(evidence.snapshot?.spreadPct ?? 0))} />
      </div>

      <div className="grid grid-cols-1 gap-2 md:grid-cols-2 xl:grid-cols-8">
        <MetricTile label="Tech Score" value={numberFmt.format(Number(scoreDecomposition.techScore ?? 0))} />
        <MetricTile label="Options Score" value={numberFmt.format(Number(scoreDecomposition.optionsScore ?? 0))} />
        <MetricTile label="Sentiment Score" value={numberFmt.format(Number(scoreDecomposition.sentimentScore ?? 0))} />
        <MetricTile label="Risk Penalty" value={numberFmt.format(Number(scoreDecomposition.riskPenalty ?? 0))} />
        <MetricTile
          label="Weighted Sum"
          value={numberFmt.format(
            Number(weightedScore.tech ?? 0) +
              Number(weightedScore.options ?? 0) +
              Number(weightedScore.sentiment ?? 0) +
              Number(weightedScore.risk ?? 0)
          )}
          emphasis
        />
        <MetricTile
          label="Calibrated Confidence"
          value={scoreDecomposition.confidence === null ? "-" : Number(scoreDecomposition.confidence ?? 0).toFixed(3)}
        />
      </div>

      <div className="grid grid-cols-1 gap-2 md:grid-cols-2 xl:grid-cols-6">
        <MetricTile label="ADX 14" value={Number(evidence.featureVector?.adx14 ?? 0).toFixed(2)} />
        <MetricTile label="RVOL 20D" value={Number(evidence.featureVector?.relativeVolume20d ?? 0).toFixed(2)} />
        <MetricTile label="News Velocity" value={Number(evidence.featureVector?.newsVelocity24h ?? 0).toFixed(2)} />
        <MetricTile
          label="News Dispersion"
          value={Number(evidence.featureVector?.newsSentimentDispersion ?? 0).toFixed(2)}
        />
        <MetricTile label="News Freshness" value={Number(evidence.featureVector?.newsFreshness ?? 0).toFixed(2)} />
        <MetricTile label="Calibration Source" value={formatSourceLabel(provenance.signalSources?.calibration)} />
      </div>

      <div className="grid grid-cols-1 gap-2 md:grid-cols-2 xl:grid-cols-6">
        <MetricTile
          label="Chain Contracts"
          value={`${Number(optionSummary.totalContracts ?? 0)} (${Number(optionSummary.callContracts ?? 0)}C/${Number(optionSummary.putContracts ?? 0)}P)`}
        />
        <MetricTile label="Benchmark" value={String(provenance.benchmarkSymbol ?? "n/a")} />
        <MetricTile
          label="Benchmark Ret 20D"
          value={benchmarkReturns.d20 === null || benchmarkReturns.d20 === undefined ? "-" : pctFmt(Number(benchmarkReturns.d20))}
        />
        <MetricTile
          label="Benchmark Ret 60D"
          value={benchmarkReturns.d60 === null || benchmarkReturns.d60 === undefined ? "-" : pctFmt(Number(benchmarkReturns.d60))}
        />
        <MetricTile
          label="Gate Model"
          value={String(provenance.gateModelVersion ?? "unknown")}
        />
        <MetricTile label="Trend Source" value={formatSourceLabel(provenance.signalSources?.trendStrength)} />
        <MetricTile label="Volume Source" value={formatSourceLabel(provenance.signalSources?.volumeConfirmation)} />
        <MetricTile label="News Flow Source" value={formatSourceLabel(provenance.signalSources?.newsFlow)} />
      </div>

      <div className="overflow-auto rounded-xl border border-slate-200">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Gate</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Severity</TableHead>
              <TableHead>Threshold</TableHead>
              <TableHead>Actual</TableHead>
              <TableHead>Details</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {gateChecks.map((gate) => (
              <TableRow key={gate.id}>
                <TableCell className="font-medium">{gate.label}</TableCell>
                <TableCell>
                  <Badge variant={gate.passed ? "success" : gate.severity === "hard" ? "destructive" : "warning"}>
                    {gate.passed ? "PASS" : "FAIL"}
                  </Badge>
                </TableCell>
                <TableCell>{String(gate.severity ?? "soft").toUpperCase()}</TableCell>
                <TableCell>{gate.threshold === undefined ? "—" : String(gate.threshold)}</TableCell>
                <TableCell>{gate.actual === undefined ? "—" : String(gate.actual)}</TableCell>
                <TableCell className="whitespace-normal text-xs text-slate-700">{gate.details || "—"}</TableCell>
              </TableRow>
            ))}
            {gateChecks.length === 0 ? (
              <TableRow>
                <TableCell colSpan={6} className="text-center text-slate-500">
                  No gate evidence found.
                </TableCell>
              </TableRow>
            ) : null}
          </TableBody>
        </Table>
      </div>

      <div className="overflow-auto rounded-xl border border-slate-200">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Indicator</TableHead>
              <TableHead>Available</TableHead>
              <TableHead>Source</TableHead>
              <TableHead>Result</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {indicators.map((indicator) => (
              <TableRow key={indicator.id}>
                <TableCell className="font-medium">{indicator.label}</TableCell>
                <TableCell>
                  <Badge variant={indicator.available ? "success" : "outline"}>
                    {indicator.available ? "YES" : "NO"}
                  </Badge>
                </TableCell>
                <TableCell className="font-mono text-[11px]">{String(indicator.source ?? "unknown")}</TableCell>
                <TableCell className="whitespace-normal">
                  <div className="space-y-1">
                    <JsonBox value={indicator.value} empty="No values." />
                    {indicator.note ? <p className="text-xs text-slate-600">{indicator.note}</p> : null}
                  </div>
                </TableCell>
              </TableRow>
            ))}
            {indicators.length === 0 ? (
              <TableRow>
                <TableCell colSpan={4} className="text-center text-slate-500">
                  No indicator evidence found.
                </TableCell>
              </TableRow>
            ) : null}
          </TableBody>
        </Table>
      </div>

      <div className="rounded-xl border border-slate-200 bg-slate-50/80 p-3">
        <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">News Articles Used</p>
        {articles.length > 0 ? (
          <div className="mt-2 space-y-2">
            {articles.slice(0, 10).map((article, index) => (
              <div key={`${article.url || article.title}-${index}`} className="rounded-lg border border-slate-200 bg-white/80 p-2">
                <div className="flex items-start justify-between gap-2">
                  <p className="text-xs font-medium text-slate-900">{article.title || "Untitled"}</p>
                  {article.url ? (
                    <a
                      href={article.url}
                      target="_blank"
                      rel="noreferrer"
                      className="inline-flex shrink-0 items-center gap-1 text-xs text-blue-700 hover:underline"
                    >
                      Open <ExternalLink className="size-3" />
                    </a>
                  ) : null}
                </div>
                <p className="mt-1 text-[11px] text-slate-600">
                  {article.source || "unknown"} {article.publishedAt ? `| ${new Date(article.publishedAt).toLocaleString()}` : ""}
                </p>
                {article.summary ? <p className="mt-1 text-xs text-slate-700">{article.summary}</p> : null}
              </div>
            ))}
          </div>
        ) : (
          <p className="mt-2 text-xs text-slate-600">No linked news articles were available.</p>
        )}
      </div>

      {notes.length > 0 ? (
        <div className="rounded-xl border border-amber-200 bg-amber-50/70 p-3 text-xs text-amber-900">
          <p className="font-semibold uppercase tracking-wide">Data Notes</p>
          <ul className="mt-2 space-y-1">
            {notes.map((note) => (
              <li key={note}>{note}</li>
            ))}
          </ul>
        </div>
      ) : null}
    </div>
  );
};

const RecommendationDetailsDialog = ({
  open,
  entry,
  onOpenChange,
  onSetSymbol,
  onPropose,
  isProposingSymbol
}) => {
  if (!entry) return null;

  const availableIndicators = Number(entry.evidence?.indicatorCoverage?.available ?? 0);
  const totalIndicators = Number(entry.evidence?.indicatorCoverage?.total ?? 0);
  const dataGatePassed = Boolean(entry.evidence?.dataQuality?.passed);
  const symbolProposing = Boolean(isProposingSymbol?.(entry.symbol));

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-6xl">
        <DialogHeader>
          <DialogTitle className="flex flex-wrap items-center gap-2">
            <span>{entry.symbol}</span>
            <Badge variant={actionBadgeVariant(entry.suggestedAction)}>{entry.suggestedAction}</Badge>
            <Badge variant={entry.actionable ? "success" : "outline"}>
              {entry.actionable ? "ACTIONABLE" : "NO TRADE"}
            </Badge>
          </DialogTitle>
          <DialogDescription>
            Rank #{entry.rank}. Full indicator, sentiment, options, and data-quality evidence used by the recommendation engine.
          </DialogDescription>
        </DialogHeader>
        <div className="max-h-[68vh] space-y-3 overflow-auto pr-1">
          <div className="grid grid-cols-2 gap-2 md:grid-cols-6">
            <MetricTile label="Composite" value={numberFmt.format(entry.metrics.compositeScore)} emphasis />
            <MetricTile label="P(Up)" value={entry.metrics.directionalUpProb.toFixed(3)} />
            <MetricTile label="P(Down)" value={entry.metrics.directionalDownProb.toFixed(3)} />
            <MetricTile label="Spread" value={pctFmt(entry.metrics.spreadPct)} />
            <MetricTile
              label="Coverage"
              value={`${availableIndicators}/${totalIndicators || "?"}`}
            />
            <MetricTile label="Data Gate" value={dataGatePassed ? "PASS" : "BLOCKED"} />
          </div>
          <RecommendationEvidencePanel entry={entry} />
        </div>
        <DialogFooter>
          <Button
            variant="outline"
            onClick={() => {
              onSetSymbol(entry.symbol);
            }}
          >
            Use Symbol
          </Button>
          <Button disabled={symbolProposing || !entry.actionable} onClick={() => onPropose(entry.symbol)}>
            {symbolProposing && <Spinner className="h-4 w-4" />}
            {symbolProposing ? "Creating Ticket..." : "Create Ticket"}
          </Button>
          <DialogClose render={<Button variant="outline">Close</Button>} />
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};

const PnlLineChart = ({ series }) => {
  if (!Array.isArray(series) || series.length < 2) {
    return (
      <div className="rounded-xl border border-dashed border-slate-300 p-4 text-sm text-slate-500">
        Waiting for more risk snapshots to draw PnL trend.
      </div>
    );
  }

  const values = series.map((point) => point.pnl);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;

  const coordinates = series.map((point, index) => {
    const x = (index / Math.max(series.length - 1, 1)) * 100;
    const y = 100 - ((point.pnl - min) / range) * 100;
    return { x, y };
  });

  const points = coordinates.map((point) => `${point.x},${point.y}`).join(" ");
  const lastPoint = coordinates[coordinates.length - 1];
  const zeroInRange = min <= 0 && max >= 0;
  const zeroY = zeroInRange ? 100 - ((0 - min) / range) * 100 : null;

  return (
    <div className="rounded-xl border border-slate-200 bg-white/70 p-3">
      <div className="mb-2 flex items-center justify-between text-xs text-slate-500">
        <span>Day PnL Trend</span>
        <span>
          Min {signedMoney(min)} | Max {signedMoney(max)}
        </span>
      </div>
      <svg className="h-40 w-full" viewBox="0 0 100 100" preserveAspectRatio="none" role="img" aria-label="PnL line chart">
        {zeroY !== null && <line x1="0" y1={zeroY} x2="100" y2={zeroY} stroke="currentColor" className="text-slate-300" strokeDasharray="2 2" />}
        <polyline
          points={points}
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinejoin="round"
          strokeLinecap="round"
          className="text-blue-600"
        />
        <circle cx={lastPoint.x} cy={lastPoint.y} r="1.8" className="fill-blue-600" />
      </svg>
    </div>
  );
};

const queryKeys = {
  risk: ["risk-status"],
  accountSummary: ["account-summary"],
  acceptanceGate: ["acceptance-gate"],
  positions: ["positions"],
  ibkrReadiness: ["ibkr-readiness"],
  marketDataDiagnostics: ["market-data-diagnostics"],
  ibkrStatus: ["ibkr-status"],
  apiLogs: ["api-request-logs"],
  runStatus: ["run-status"],
  killSwitch: ["kill-switch"],
  pending: ["orders-pending"],
  recent: ["orders-recent"],
  policy: ["bot-policy"],
  envConfig: ["env-config"],
  recommendations: ["recommendations"]
};

export const App = () => {
  const queryClient = useQueryClient();

  const [symbol, setSymbol] = useState("SPY");
  const [scanResult, setScanResult] = useState(null);
  const [decisionResult, setDecisionResult] = useState(null);
  const [backtestResult, setBacktestResult] = useState(null);
  const [policyForm, setPolicyForm] = useState({});
  const [universeInput, setUniverseInput] = useState("");
  const [envForm, setEnvForm] = useState({});
  const [envTouchedKeys, setEnvTouchedKeys] = useState({});
  const [selectedRecommendation, setSelectedRecommendation] = useState(null);
  const [recommendationDetailsOpen, setRecommendationDetailsOpen] = useState(false);
  const [positionsOpen, setPositionsOpen] = useState(false);
  const [runTimingOpen, setRunTimingOpen] = useState(false);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [entitlementsOpen, setEntitlementsOpen] = useState(false);
  const [readinessOpen, setReadinessOpen] = useState(false);
  const [brokerPositionsPage, setBrokerPositionsPage] = useState(1);
  const [brokerPositionsPageSize, setBrokerPositionsPageSize] = useState(25);
  const [brokerPositionStateFilter, setBrokerPositionStateFilter] = useState("ALL");
  const [localPositionsPage, setLocalPositionsPage] = useState(1);
  const [localPositionsPageSize, setLocalPositionsPageSize] = useState(25);
  const [localPositionStateFilter, setLocalPositionStateFilter] = useState("ALL");
  const [settingsTab, setSettingsTab] = useState("policy");
  const [sessionMismatchDialogOpen, setSessionMismatchDialogOpen] = useState(false);
  const [orderWorkflowTab, setOrderWorkflowTab] = useState("pending");
  const [lastLaunchResult, setLastLaunchResult] = useState(null);
  const [positionsClockMs, setPositionsClockMs] = useState(() => Date.now());
  const [scanPolicyPersisting, setScanPolicyPersisting] = useState(false);
  const [scannerProviderPersisting, setScannerProviderPersisting] = useState(false);
  const [proposingBySymbol, setProposingBySymbol] = useState({});
  const [bulkProposeProgress, setBulkProposeProgress] = useState({
    running: false,
    total: 0,
    completed: 0,
    success: 0,
    failed: 0
  });
  const proposingSymbolsRef = useRef(new Set());
  const sessionMismatchNotificationRef = useRef(null);
  const lastSessionMismatchKillSwitchAttemptRef = useRef(0);
  const orderWorkflowRef = useRef(null);

  const riskQuery = useQuery({
    queryKey: queryKeys.risk,
    queryFn: api.riskStatus,
    refetchInterval: 20_000
  });

  const runStatusQuery = useQuery({
    queryKey: queryKeys.runStatus,
    queryFn: api.runStatus,
    enabled: runTimingOpen,
    refetchInterval: runTimingOpen ? 5_000 : false
  });

  const killSwitchQuery = useQuery({
    queryKey: queryKeys.killSwitch,
    queryFn: api.killSwitch,
    refetchInterval: 15_000
  });

  const ibkrStatusQuery = useQuery({
    queryKey: queryKeys.ibkrStatus,
    queryFn: api.ibkrStatus,
    refetchInterval: (query) => (query.state.data?.ibkr?.reachable ? 15_000 : 10_000)
  });

  const ibkrConnected = Boolean(ibkrStatusQuery.data?.ibkr?.reachable);

  const accountSummaryQuery = useQuery({
    queryKey: queryKeys.accountSummary,
    queryFn: api.accountSummary,
    enabled: ibkrConnected,
    refetchInterval: ibkrConnected ? 20_000 : false
  });

  const acceptanceGateQuery = useQuery({
    queryKey: queryKeys.acceptanceGate,
    queryFn: api.acceptanceGate,
    refetchInterval: 60_000
  });

  const positionsQuery = useQuery({
    queryKey: queryKeys.positions,
    queryFn: api.positions,
    refetchInterval: 20_000
  });

  const marketDataDiagnosticsQuery = useQuery({
    queryKey: queryKeys.marketDataDiagnostics,
    queryFn: () => api.marketDataDiagnostics({ windowMinutes: 360, limitSymbols: 60 }),
    enabled: ibkrConnected,
    refetchInterval: ibkrConnected ? 60_000 : false
  });

  const ibkrReadinessQuery = useQuery({
    queryKey: queryKeys.ibkrReadiness,
    queryFn: api.ibkrReadiness,
    enabled: readinessOpen,
    refetchInterval: readinessOpen ? 20_000 : false
  });

  const apiLogsQuery = useQuery({
    queryKey: queryKeys.apiLogs,
    queryFn: () => api.apiRequestLogs({ limit: 250 }),
    refetchInterval: 20_000
  });

  const pendingQuery = useQuery({
    queryKey: queryKeys.pending,
    queryFn: api.pendingOrders,
    refetchInterval: 15_000
  });

  const recentQuery = useQuery({
    queryKey: queryKeys.recent,
    queryFn: api.recentOrders,
    refetchInterval: 15_000
  });

  const policyQuery = useQuery({
    queryKey: queryKeys.policy,
    queryFn: api.botPolicy
  });

  const envConfigQuery = useQuery({
    queryKey: queryKeys.envConfig,
    queryFn: api.envConfig,
    enabled: true
  });

  useEffect(() => {
    if (!policyQuery.data?.policy) return;
    const nextPolicyForm = {};
    ALL_POLICY_FIELDS.forEach((field) => {
      nextPolicyForm[field] = formatPolicyInputValue(field, policyQuery.data.policy[field]);
    });
    nextPolicyForm.ibkrScanCode = String(
      policyQuery.data.policy.ibkrScanCode ?? DEFAULT_SCREENER_MODE
    );
    nextPolicyForm.analysisDataProvider = String(
      policyQuery.data.policy.analysisDataProvider ?? DEFAULT_ANALYSIS_PROVIDER
    );
    setPolicyForm(nextPolicyForm);
    setUniverseInput((policyQuery.data.policy.universeSymbols ?? []).join(", "));
  }, [policyQuery.data]);

  useEffect(() => {
    if (!envConfigQuery.data?.fields) return;
    const nextEnvForm = {};
    envConfigQuery.data.fields.forEach((field) => {
      nextEnvForm[field.key] = field.value ?? "";
    });
    setEnvForm(nextEnvForm);
    setEnvTouchedKeys({});
  }, [envConfigQuery.data]);

  useEffect(() => {
    if (!positionsOpen) return undefined;
    const interval = window.setInterval(() => {
      setPositionsClockMs(Date.now());
    }, 1_000);
    return () => window.clearInterval(interval);
  }, [positionsOpen]);

  const persistedPolicyTopN = parseTopNInput(policyQuery.data?.policy?.scanTopN ?? 10);
  const draftPolicyTopN = parseTopNInput(policyForm.scanTopN ?? persistedPolicyTopN);
  const persistedPolicyScanCode = String(
    policyQuery.data?.policy?.ibkrScanCode ?? DEFAULT_SCREENER_MODE
  );
  const draftPolicyScanCode = String(
    policyForm.ibkrScanCode ?? persistedPolicyScanCode ?? DEFAULT_SCREENER_MODE
  );
  const persistedAnalysisDataProvider = String(
    policyQuery.data?.policy?.analysisDataProvider ?? DEFAULT_ANALYSIS_PROVIDER
  );
  const draftAnalysisDataProvider = String(
    policyForm.analysisDataProvider ?? persistedAnalysisDataProvider ?? DEFAULT_ANALYSIS_PROVIDER
  );
  const scanTopNDirty = policyQuery.isSuccess && draftPolicyTopN !== persistedPolicyTopN;
  const scanCodeDirty = policyQuery.isSuccess && draftPolicyScanCode !== persistedPolicyScanCode;
  const analysisProviderDirty =
    policyQuery.isSuccess && draftAnalysisDataProvider !== persistedAnalysisDataProvider;
  const analysisRequiresIbkr = persistedAnalysisDataProvider === "IBKR";
  const scanBlockedByConnection = analysisRequiresIbkr && !ibkrConnected;

  const recommendationsQuery = useQuery({
    queryKey: [
      ...queryKeys.recommendations,
      persistedPolicyTopN,
      persistedPolicyScanCode,
      persistedAnalysisDataProvider
    ],
    queryFn: () => api.recommendations({ topN: persistedPolicyTopN }),
    enabled: policyQuery.isSuccess && !scanBlockedByConnection,
    refetchInterval: !scanBlockedByConnection ? 15 * 60_000 : false,
    retry: 0,
    refetchOnMount: false,
    refetchOnReconnect: false
  });
  const recommendationsButtonLabel = scanBlockedByConnection
    ? "Connect IBKR to Scan"
    : recommendationsQuery.isFetching
      ? `Scanning Top ${persistedPolicyTopN} Symbols...`
      : scanTopNDirty || scanCodeDirty || analysisProviderDirty
        ? `Re-Scan Top ${draftPolicyTopN} Symbols`
        : "Scan/Screen Market";

  const scanMutation = useMutation({
    mutationFn: (topN) => api.scan({ topN }),
    onSuccess: (data) => {
      setScanResult(data);
      queryClient.invalidateQueries({ queryKey: queryKeys.recommendations });
      toastManager.add({
        title: "Scan completed",
        description: `Loaded ${data.analyses?.length ?? 0} candidates.`,
        type: "success"
      });
    },
    onError: (error) => {
      toastManager.add({
        title: "Scan failed",
        description: String(error),
        type: "error"
      });
    }
  });

  const backtestMutation = useMutation({
    mutationFn: () =>
      api.backtest({
        lookbackDays: 220,
        slippageBps: 12,
        commissionPerTrade: 0.65,
        premiumPerTrade: 250,
        optionLeverage: 4,
        warmupWindow: 60,
        sampleLimit: 100
      }),
    onSuccess: (data) => {
      setBacktestResult(data);
      toastManager.add({
        title: "Backtest complete",
        description: `Trades: ${data?.result?.trades ?? 0} | PnL: ${signedMoney(data?.result?.netPnl ?? 0)}`,
        type: "success"
      });
    },
    onError: (error) => {
      toastManager.add({
        title: "Backtest failed",
        description: String(error),
        type: "error"
      });
    }
  });

  const decisionMutation = useMutation({
    mutationFn: () => api.decision({ symbol: symbol.trim().toUpperCase() }),
    onSuccess: (data) => {
      setDecisionResult(data);
      toastManager.add({
        title: "Decision generated",
        description: `${data?.decisionCard?.action ?? data?.decision?.action ?? "NO_TRADE"}`,
        type: "info"
      });
    },
    onError: (error) => {
      toastManager.add({
        title: "Decision failed",
        description: String(error),
        type: "error"
      });
    }
  });

  const proposeMutation = useMutation({
    mutationFn: (requestedSymbol) =>
      api.proposeOrder({
        symbol: requestedSymbol
      }),
    onSuccess: (data) => {
      setDecisionResult(data);
      queryClient.invalidateQueries({ queryKey: queryKeys.pending });
      queryClient.invalidateQueries({ queryKey: queryKeys.recent });
      queryClient.invalidateQueries({ queryKey: queryKeys.risk });
      queryClient.invalidateQueries({ queryKey: queryKeys.accountSummary });
      queryClient.invalidateQueries({ queryKey: queryKeys.acceptanceGate });
      queryClient.invalidateQueries({ queryKey: queryKeys.positions });
      toastManager.add({
        title: "Order proposed",
        description: `${data.order.symbol} ${data.order.action} x${data.order.quantity}`,
        type: "success"
      });
    },
    onError: (error) => {
      toastManager.add({
        title: "Ticket creation blocked",
        description: formatProposeOrderError(error),
        type: "error"
      });
    }
  });

  const approveMutation = useMutation({
    mutationFn: ({ orderId, approve }) => api.approveOrder({ orderId, approve, comment: "UI action" }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.pending });
      queryClient.invalidateQueries({ queryKey: queryKeys.recent });
      queryClient.invalidateQueries({ queryKey: queryKeys.risk });
      queryClient.invalidateQueries({ queryKey: queryKeys.accountSummary });
      queryClient.invalidateQueries({ queryKey: queryKeys.acceptanceGate });
      queryClient.invalidateQueries({ queryKey: queryKeys.positions });
      toastManager.add({
        title: "Order updated",
        description: "Approval action recorded.",
        type: "success"
      });
    },
    onError: (error) => {
      toastManager.add({
        title: "Approval failed",
        description: String(error),
        type: "error"
      });
    }
  });

  const killSwitchMutation = useMutation({
    mutationFn: ({ enabled }) => api.updateKillSwitch({ enabled, reason: "UI toggle" }),
    onSuccess: (data) => {
      const enabled = Boolean(data?.killSwitch?.enabled);
      queryClient.setQueryData(queryKeys.killSwitch, data);
      queryClient.invalidateQueries({ queryKey: queryKeys.risk });
      queryClient.invalidateQueries({ queryKey: queryKeys.accountSummary });
      queryClient.invalidateQueries({ queryKey: queryKeys.acceptanceGate });
      queryClient.invalidateQueries({ queryKey: queryKeys.runStatus });
      toastManager.add({
        title: enabled ? "Kill switch enabled" : "Kill switch disabled",
        description: enabled
          ? "New entries are halted until you disable the kill switch."
          : "Trading halt from kill switch has been lifted.",
        type: enabled ? "warning" : "success"
      });
    },
    onError: (error) => {
      toastManager.add({
        title: "Kill switch update failed",
        description: String(error),
        type: "error"
      });
    }
  });

  const launchIbkrMutation = useMutation({
    mutationFn: (target) => api.ibkrLaunch({ target }),
    onSuccess: (data) => {
      const launch = data?.launch;
      setLastLaunchResult(launch ?? null);
      toastManager.add({
        title: launch?.launched ? "IBKR launch requested" : "IBKR launch failed",
        description: launch?.message ?? "No launch response.",
        type: launch?.launched ? "success" : "error"
      });
      setTimeout(() => {
        queryClient.invalidateQueries({ queryKey: queryKeys.ibkrStatus });
        queryClient.invalidateQueries({ queryKey: queryKeys.marketDataDiagnostics });
        queryClient.invalidateQueries({ queryKey: queryKeys.ibkrReadiness });
      }, 1500);
    },
    onError: (error) => {
      toastManager.add({
        title: "IBKR launch failed",
        description: String(error),
        type: "error"
      });
    }
  });

  const updatePolicyMutation = useMutation({
    mutationFn: (payload) => api.updateBotPolicy(payload),
    onSuccess: (data) => {
      queryClient.setQueryData(queryKeys.policy, data);
      queryClient.invalidateQueries({ queryKey: queryKeys.recommendations });
      queryClient.invalidateQueries({ queryKey: queryKeys.risk });
      queryClient.invalidateQueries({ queryKey: queryKeys.acceptanceGate });
      toastManager.add({
        title: "Policy updated",
        description: "Risk and decision harness settings saved.",
        type: "success"
      });
    },
    onError: (error) => {
      toastManager.add({
        title: "Policy update failed",
        description: String(error),
        type: "error"
      });
    }
  });

  const resetPolicyMutation = useMutation({
    mutationFn: () => api.resetBotPolicy(),
    onSuccess: (data) => {
      queryClient.setQueryData(queryKeys.policy, data);
      queryClient.invalidateQueries({ queryKey: queryKeys.recommendations });
      queryClient.invalidateQueries({ queryKey: queryKeys.risk });
      queryClient.invalidateQueries({ queryKey: queryKeys.acceptanceGate });
      toastManager.add({
        title: "Policy reset",
        description: "Policy restored to defaults.",
        type: "info"
      });
    },
    onError: (error) => {
      toastManager.add({
        title: "Reset failed",
        description: String(error),
        type: "error"
      });
    }
  });

  const updateEnvMutation = useMutation({
    mutationFn: (payload) => api.updateEnvConfig(payload),
    onSuccess: (data) => {
      queryClient.setQueryData(queryKeys.envConfig, {
        envPath: data.envPath,
        fields: data.fields
      });
      queryClient.invalidateQueries({ queryKey: queryKeys.policy });
      queryClient.invalidateQueries({ queryKey: queryKeys.risk });
      queryClient.invalidateQueries({ queryKey: queryKeys.accountSummary });
      queryClient.invalidateQueries({ queryKey: queryKeys.acceptanceGate });
      queryClient.invalidateQueries({ queryKey: queryKeys.ibkrStatus });
      queryClient.invalidateQueries({ queryKey: queryKeys.positions });
      queryClient.invalidateQueries({ queryKey: queryKeys.recommendations });
      queryClient.invalidateQueries({ queryKey: queryKeys.marketDataDiagnostics });
      queryClient.invalidateQueries({ queryKey: queryKeys.ibkrReadiness });
      toastManager.add({
        title: "Environment saved",
        description:
          data.restartRequiredKeys?.length > 0
            ? `Saved ${data.changedKeys.length} keys. Restart recommended for ${data.restartRequiredKeys.join(", ")}.`
            : `Saved ${data.changedKeys.length} keys with live apply.`,
        type: "success"
      });
    },
    onError: (error) => {
      toastManager.add({
        title: "Environment update failed",
        description: String(error),
        type: "error"
      });
    }
  });

  const refreshRuntimeMutation = useMutation({
    mutationFn: () => api.refreshApp(),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.policy });
      queryClient.invalidateQueries({ queryKey: queryKeys.risk });
      queryClient.invalidateQueries({ queryKey: queryKeys.accountSummary });
      queryClient.invalidateQueries({ queryKey: queryKeys.acceptanceGate });
      queryClient.invalidateQueries({ queryKey: queryKeys.ibkrStatus });
      queryClient.invalidateQueries({ queryKey: queryKeys.positions });
      queryClient.invalidateQueries({ queryKey: queryKeys.recommendations });
      queryClient.invalidateQueries({ queryKey: queryKeys.marketDataDiagnostics });
      queryClient.invalidateQueries({ queryKey: queryKeys.ibkrReadiness });
      toastManager.add({
        title: "Runtime refreshed",
        description: "Runtime configuration and adapters have been refreshed from env.",
        type: "info"
      });
    },
    onError: (error) => {
      toastManager.add({
        title: "Runtime refresh failed",
        description: String(error),
        type: "error"
      });
    }
  });

  const restartAppMutation = useMutation({
    mutationFn: () => api.restartApp(),
    onSuccess: (data) => {
      const restart = data?.restart ?? {};
      toastManager.add({
        title: restart.scheduled ? "App restart scheduled" : "Restart skipped",
        description: restart.message ?? "No restart message returned.",
        type: restart.scheduled ? "warning" : "info"
      });
      if (restart.scheduled) {
        const delay = Number(restart.delayMs ?? 600);
        setTimeout(() => {
          window.location.reload();
        }, Math.max(1200, delay + 300));
      }
    },
    onError: (error) => {
      toastManager.add({
        title: "Restart request failed",
        description: String(error),
        type: "error"
      });
    }
  });

  const envFields = envConfigQuery.data?.fields ?? EMPTY_ARRAY;
  const scannerProviderField =
    envFields.find((field) => field.key === SCANNER_PROVIDER_KEY) ?? null;
  const scannerProviderSavedOrder = normalizeScannerProviderOrder(scannerProviderField?.value ?? "");
  const scannerProviderDraftOrder = normalizeScannerProviderOrder(
    envForm[SCANNER_PROVIDER_KEY] ?? scannerProviderSavedOrder
  );
  const scannerProviderModeValue =
    SCANNER_PROVIDER_MODE_OPTIONS.find(
      (option) =>
        option.value !== SCANNER_PROVIDER_CUSTOM_VALUE &&
        normalizeScannerProviderOrder(option.value) === scannerProviderDraftOrder
    )?.value ?? SCANNER_PROVIDER_CUSTOM_VALUE;
  const scannerProviderDirty = scannerProviderDraftOrder !== scannerProviderSavedOrder;

  const setScannerProviderOrderDraft = (nextOrder) => {
    const normalizedOrder = normalizeScannerProviderOrder(nextOrder);
    setEnvForm((previous) => ({
      ...previous,
      [SCANNER_PROVIDER_KEY]: normalizedOrder
    }));
    setEnvTouchedKeys((previous) => ({
      ...previous,
      [SCANNER_PROVIDER_KEY]: true
    }));
  };

  const ensureScannerProviderPersisted = async () => {
    if (!scannerProviderField) return;
    if (!scannerProviderDirty) return;
    if (updateEnvMutation.isPending) {
      throw new Error("Environment update already in progress.");
    }
    if (scannerProviderDraftOrder.length === 0) {
      throw new Error("Scanner provider order cannot be empty.");
    }

    setScannerProviderPersisting(true);
    try {
      const updated = await api.updateEnvConfig({
        values: {
          [SCANNER_PROVIDER_KEY]: scannerProviderDraftOrder
        }
      });
      queryClient.setQueryData(queryKeys.envConfig, {
        envPath: updated.envPath,
        fields: updated.fields
      });
      queryClient.invalidateQueries({ queryKey: queryKeys.recommendations });
      queryClient.invalidateQueries({ queryKey: queryKeys.runStatus });
      queryClient.invalidateQueries({ queryKey: queryKeys.ibkrReadiness });
    } finally {
      setScannerProviderPersisting(false);
    }
  };

  const ensureScanPolicyPersisted = async () => {
    if (!policyQuery.isSuccess) {
      throw new Error("Policy is still loading. Try again in a moment.");
    }
    if (!scanTopNDirty && !scanCodeDirty && !analysisProviderDirty) {
      return {
        topN: persistedPolicyTopN,
        ibkrScanCode: persistedPolicyScanCode,
        analysisDataProvider: persistedAnalysisDataProvider
      };
    }
    if (updatePolicyMutation.isPending) {
      throw new Error("Policy update already in progress.");
    }
    setScanPolicyPersisting(true);
    try {
      const updated = await api.updateBotPolicy({
        scanTopN: draftPolicyTopN,
        ibkrScanCode: draftPolicyScanCode,
        analysisDataProvider: draftAnalysisDataProvider
      });
      queryClient.setQueryData(queryKeys.policy, updated);
      queryClient.invalidateQueries({ queryKey: queryKeys.risk });
      return {
        topN: draftPolicyTopN,
        ibkrScanCode: draftPolicyScanCode,
        analysisDataProvider: draftAnalysisDataProvider
      };
    } finally {
      setScanPolicyPersisting(false);
    }
  };

  const analyses = scanResult?.analyses ?? EMPTY_ARRAY;
  const runStatus = runStatusQuery.data;
  const runActions = runStatus?.actions ?? EMPTY_ARRAY;
  const schedulerRuntime = runStatus?.scheduler;
  const ibkrRuntime = runStatus?.ibkrRuntime;
  const pendingOrders = pendingQuery.data?.orders ?? EMPTY_ARRAY;
  const recentOrders = recentQuery.data?.orders ?? EMPTY_ARRAY;
  const apiRequestLogs = apiLogsQuery.data?.logs ?? EMPTY_ARRAY;
  const positionsSummary = positionsQuery.data?.summary ?? null;
  const brokerPositions = positionsQuery.data?.broker?.positions ?? EMPTY_ARRAY;
  const localPositions = positionsQuery.data?.local?.positions ?? EMPTY_ARRAY;
  const brokerPositionsFiltered = useMemo(
    () => filterByLifecycleState(brokerPositions, brokerPositionStateFilter),
    [brokerPositions, brokerPositionStateFilter]
  );
  const localPositionsFiltered = useMemo(
    () => filterByLifecycleState(localPositions, localPositionStateFilter),
    [localPositions, localPositionStateFilter]
  );
  const brokerPositionsTotalPages = Math.max(
    1,
    Math.ceil(brokerPositionsFiltered.length / Math.max(1, brokerPositionsPageSize))
  );
  const localPositionsTotalPages = Math.max(
    1,
    Math.ceil(localPositionsFiltered.length / Math.max(1, localPositionsPageSize))
  );
  const brokerPositionsActivePage = Math.min(brokerPositionsPage, brokerPositionsTotalPages);
  const localPositionsActivePage = Math.min(localPositionsPage, localPositionsTotalPages);
  const brokerPositionsPageRows = useMemo(() => {
    const start = (brokerPositionsActivePage - 1) * brokerPositionsPageSize;
    return brokerPositionsFiltered.slice(start, start + brokerPositionsPageSize);
  }, [brokerPositionsFiltered, brokerPositionsActivePage, brokerPositionsPageSize]);
  const localPositionsPageRows = useMemo(() => {
    const start = (localPositionsActivePage - 1) * localPositionsPageSize;
    return localPositionsFiltered.slice(start, start + localPositionsPageSize);
  }, [localPositionsFiltered, localPositionsActivePage, localPositionsPageSize]);
  const marketDataDiagnostics = marketDataDiagnosticsQuery.data;
  const ibkrReadiness = ibkrReadinessQuery.data;
  const entitlementSummary = marketDataDiagnostics?.summary ?? null;
  const entitlementRows = Array.isArray(marketDataDiagnostics?.symbols)
    ? marketDataDiagnostics.symbols
    : [];
  const entitlementBackoffSymbols = Array.isArray(
    marketDataDiagnostics?.brokerBackoffs?.quoteSubscriptionBackoffs
  )
    ? marketDataDiagnostics.brokerBackoffs.quoteSubscriptionBackoffs
    : [];
  const optionEntitlementBackoff = marketDataDiagnostics?.brokerBackoffs?.optionQuoteEntitlementBackoff;
  const recommendations = recommendationsQuery.data?.recommendations ?? EMPTY_ARRAY;
  const recommendationScanner = recommendationsQuery.data?.scanner ?? null;
  const recommendationsExecution = recommendationsQuery.data?.execution ?? null;
  const recommendationRunMs = Number(
    recommendationsExecution?.computeMs ?? recommendationsExecution?.elapsedMs ?? 0
  );
  const recommendationRunSecondsLabel =
    Number.isFinite(recommendationRunMs) && recommendationRunMs > 0
      ? `${(recommendationRunMs / 1_000).toFixed(2)}s`
      : "—";
  const recommendationTimeoutMs = Number(recommendationsExecution?.timeoutMs ?? 0);
  const recommendationTimeoutBudgetLabel =
    recommendationTimeoutMs <= 0
      ? "unbounded"
      : `${(recommendationTimeoutMs / 1_000).toFixed(2)}s`;
  const recommendationRunErrors = Array.isArray(recommendationsExecution?.errors)
    ? recommendationsExecution.errors
    : EMPTY_ARRAY;
  const actionableRecommendations = recommendations.filter((entry) => entry.actionable);
  const actionableSymbols = useMemo(
    () =>
      Array.from(
        new Set(
          actionableRecommendations
            .map((entry) => String(entry.symbol ?? "").trim().toUpperCase())
            .filter(Boolean)
        )
      ),
    [actionableRecommendations]
  );
  const activeProposalCount = useMemo(
    () => Object.keys(proposingBySymbol).filter((symbolKey) => proposingBySymbol[symbolKey]).length,
    [proposingBySymbol]
  );
  const bulkProposeButtonLabel = bulkProposeProgress.running
    ? `Creating ${bulkProposeProgress.completed}/${bulkProposeProgress.total}...`
    : `Create Tickets for All Actionable (${actionableSymbols.length})`;
  const thresholdSummary = recommendationsQuery.data?.policySnapshot;
  const latestScannerLog = useMemo(
    () =>
      apiRequestLogs.find(
        (entry) =>
          (entry.provider === "alpha_vantage" && entry.endpoint === "TOP_GAINERS_LOSERS") ||
          (entry.provider === "ibkr" && entry.endpoint === "getMarketScanner") ||
          (entry.provider === "openai" &&
            entry.endpoint === "responses" &&
            String(entry.reason ?? "").toLowerCase().includes("symbol discovery"))
      ) ?? null,
    [apiRequestLogs]
  );
  const decisionSummary = useMemo(() => {
    if (!decisionResult || typeof decisionResult !== "object") return null;

    const decision =
      decisionResult.decisionCard && typeof decisionResult.decisionCard === "object"
        ? decisionResult.decisionCard
        : decisionResult.decision && typeof decisionResult.decision === "object"
          ? decisionResult.decision
          : null;

    const action = typeof decision?.action === "string" ? decision.action : null;
    const confidenceValue = Number(decision?.confidence);
    const confidence = Number.isFinite(confidenceValue) ? confidenceValue : null;
    const symbolFromOrder =
      decisionResult.order && typeof decisionResult.order.symbol === "string"
        ? decisionResult.order.symbol
        : null;
    const symbolFromFeature =
      decisionResult.feature && typeof decisionResult.feature.symbol === "string"
        ? decisionResult.feature.symbol
        : null;
    const symbolFromDiagnostics =
      decisionResult.diagnostics && typeof decisionResult.diagnostics.symbol === "string"
        ? decisionResult.diagnostics.symbol
        : null;
    const fallbackSymbol = symbol.trim().toUpperCase();
    const symbolValue = symbolFromOrder || symbolFromFeature || symbolFromDiagnostics || fallbackSymbol || null;
    const compositeScoreValue = Number(
      decisionResult.scoreCard?.compositeScore ?? decisionResult.diagnostics?.actuals?.compositeScore
    );
    const directionalUpValue = Number(
      decisionResult.feature?.directionalUpProb ?? decisionResult.diagnostics?.actuals?.directionalUpProb
    );
    const directionalDownValue = Number(
      decisionResult.feature?.directionalDownProb ?? decisionResult.diagnostics?.actuals?.directionalDownProb
    );
    const evaluatedAt =
      (typeof decision?.timestamp === "string" && decision.timestamp) ||
      (typeof decisionResult.generatedAt === "string" && decisionResult.generatedAt) ||
      (typeof decisionResult.diagnostics?.evaluatedAt === "string" &&
        decisionResult.diagnostics.evaluatedAt) ||
      null;
    const vetoFlags = Array.isArray(decision?.vetoFlags)
      ? decision.vetoFlags
          .map((flag) => String(flag))
          .filter((flag) => flag.trim().length > 0)
      : EMPTY_ARRAY;
    const rationale =
      typeof decision?.rationale === "string" && decision.rationale.trim().length > 0
        ? decision.rationale
        : null;
    const orderTicket =
      decisionResult.order && typeof decisionResult.order === "object"
        ? {
            symbol: decisionResult.order.symbol ?? null,
            action: decisionResult.order.action ?? null,
            quantity: decisionResult.order.quantity ?? null
          }
        : null;

    return {
      symbol: symbolValue,
      action,
      confidence,
      evaluatedAt,
      compositeScore: Number.isFinite(compositeScoreValue) ? compositeScoreValue : null,
      directionalUpProb: Number.isFinite(directionalUpValue) ? directionalUpValue : null,
      directionalDownProb: Number.isFinite(directionalDownValue) ? directionalDownValue : null,
      vetoFlags,
      rationale,
      orderTicket
    };
  }, [decisionResult, symbol]);

  const envFieldsByCategory = useMemo(() => {
    const grouped = {};
    envFields.forEach((field) => {
      if (!grouped[field.category]) grouped[field.category] = [];
      grouped[field.category].push(field);
    });
    return grouped;
  }, [envFields]);

  const envChangedKeys = useMemo(
    () =>
      envFields
        .filter((field) => {
          if (!envTouchedKeys[field.key]) return false;
          return (envForm[field.key] ?? "") !== (field.value ?? "");
        })
        .map((field) => field.key),
    [envFields, envForm, envTouchedKeys]
  );

  const accountSummary = accountSummaryQuery.data?.summary;
  const brokerAccount = accountSummaryQuery.data?.brokerAccount;
  const acceptanceGate = acceptanceGateQuery.data?.gate ?? null;
  const acceptanceGateSummary = accountSummaryQuery.data?.acceptanceGate ?? null;
  const acceptanceGatePass =
    typeof acceptanceGate?.pass === "boolean"
      ? acceptanceGate.pass
      : Boolean(acceptanceGateSummary?.pass);
  const acceptanceGateCompletedTrades = Number(
    acceptanceGate?.trading?.completedTrades ?? acceptanceGateSummary?.completedTrades ?? 0
  );
  const acceptanceGateObservedDays = Number(
    acceptanceGate?.period?.observedDays ?? acceptanceGateSummary?.observedDays ?? 0
  );
  const pnlSeries = accountSummaryQuery.data?.pnlSeries ?? [];
  const backtestSummary = backtestResult?.result;
  const backtestGate = backtestResult?.acceptanceGate;
  const positionsButtonCount = useMemo(() => {
    const brokerCount =
      typeof positionsSummary?.brokerOpenPositions === "number" &&
      Number.isFinite(positionsSummary.brokerOpenPositions)
        ? positionsSummary.brokerOpenPositions
        : null;
    const localCount =
      typeof positionsSummary?.localOpenPositions === "number" &&
      Number.isFinite(positionsSummary.localOpenPositions)
        ? positionsSummary.localOpenPositions
        : typeof accountSummary?.openPositions === "number" && Number.isFinite(accountSummary.openPositions)
          ? accountSummary.openPositions
          : 0;
    if (brokerCount === null) return localCount;
    return Math.max(brokerCount, localCount);
  }, [positionsSummary, accountSummary]);

  useEffect(() => {
    if (!positionsOpen) return;
    setBrokerPositionsPage((current) => Math.min(current, brokerPositionsTotalPages));
  }, [positionsOpen, brokerPositionsTotalPages]);

  useEffect(() => {
    if (!positionsOpen) return;
    setBrokerPositionsPage(1);
  }, [positionsOpen, brokerPositionStateFilter]);

  useEffect(() => {
    if (!positionsOpen) return;
    setLocalPositionsPage((current) => Math.min(current, localPositionsTotalPages));
  }, [positionsOpen, localPositionsTotalPages]);

  useEffect(() => {
    if (!positionsOpen) return;
    setLocalPositionsPage(1);
  }, [positionsOpen, localPositionStateFilter]);

  useEffect(() => {
    if (ibkrConnected) return;
    queryClient.removeQueries({ queryKey: queryKeys.accountSummary });
    queryClient.removeQueries({ queryKey: queryKeys.recommendations });
  }, [ibkrConnected, queryClient]);

  const openRiskBadge = useMemo(() => {
    const halted = Boolean(accountSummary?.halted ?? riskQuery.data?.riskState?.halted);
    return halted ? { label: "HALTED", color: "destructive" } : { label: "ACTIVE", color: "success" };
  }, [accountSummary, riskQuery.data]);

  const ibkrStatus = ibkrStatusQuery.data?.ibkr;
  const pendingApprovalCount = pendingOrders.length;
  const hasPendingApprovals = pendingApprovalCount > 0;
  const killSwitchEnabled = Boolean(killSwitchQuery.data?.killSwitch?.enabled);
  const ibkrBadge = ibkrStatus?.reachable ? { label: "CONNECTED", color: "success" } : { label: "DISCONNECTED", color: "outline" };
  const integrationStatusItems = Array.isArray(ibkrStatusQuery.data?.config?.integrations)
    ? ibkrStatusQuery.data.config.integrations
    : [];
  const ibkrSessionMode =
    ibkrStatus?.detectedMode === "paper"
      ? "PAPER"
      : ibkrStatus?.detectedMode === "live"
        ? "LIVE"
        : "UNKNOWN";
  const liveAccountConnected = Boolean(ibkrStatus?.reachable && ibkrStatus?.detectedMode === "live");
  const botMode = ibkrStatusQuery.data?.config?.botMode === "live" ? "LIVE" : "PAPER";
  const sessionEnvironmentMismatch = Boolean(
    ibkrStatus?.reachable && ibkrSessionMode !== "UNKNOWN" && ibkrSessionMode !== botMode
  );
  const sessionMismatchReason = sessionEnvironmentMismatch
    ? `session_env_mismatch:${ibkrSessionMode.toLowerCase()}_session_vs_${botMode.toLowerCase()}_env`
    : "";
  const entitlementGateBadge = useMemo(() => {
    const marketState = entitlementSummary?.coreMarketStatus;
    if (marketState === "ok") return { label: "Core Market OK", variant: "success" };
    if (marketState === "delayed_only") return { label: "Delayed Only", variant: "warning" };
    if (marketState === "blocked") return { label: "Data Gate Blocked", variant: "destructive" };
    return { label: "Data Gate Unknown", variant: "outline" };
  }, [entitlementSummary]);

  useEffect(() => {
    if (!sessionEnvironmentMismatch) {
      sessionMismatchNotificationRef.current = null;
      setSessionMismatchDialogOpen(false);
      return;
    }

    const mismatchKey = `${ibkrSessionMode}:${botMode}`;
    if (sessionMismatchNotificationRef.current === mismatchKey) return;
    sessionMismatchNotificationRef.current = mismatchKey;
    setSessionMismatchDialogOpen(true);
    toastManager.add({
      title: "Session/Env mismatch detected",
      description: `Connected IBKR session is ${ibkrSessionMode}, but bot env is ${botMode}. Kill switch will remain ON until they match.`,
      type: "warning"
    });
  }, [sessionEnvironmentMismatch, ibkrSessionMode, botMode]);

  useEffect(() => {
    if (!sessionEnvironmentMismatch) return;
    if (killSwitchEnabled || killSwitchMutation.isPending) return;
    const now = Date.now();
    if (now - lastSessionMismatchKillSwitchAttemptRef.current < 10_000) return;
    lastSessionMismatchKillSwitchAttemptRef.current = now;
    killSwitchMutation.mutate({
      enabled: true,
      reason: sessionMismatchReason || "session_env_mismatch"
    });
  }, [sessionEnvironmentMismatch, killSwitchEnabled, killSwitchMutation.isPending, sessionMismatchReason]);

  const anyBusy =
    scanMutation.isPending ||
    backtestMutation.isPending ||
    decisionMutation.isPending ||
    activeProposalCount > 0 ||
    approveMutation.isPending ||
    killSwitchMutation.isPending ||
    launchIbkrMutation.isPending ||
    updatePolicyMutation.isPending ||
    resetPolicyMutation.isPending ||
    scannerProviderPersisting ||
    updateEnvMutation.isPending ||
    refreshRuntimeMutation.isPending ||
    restartAppMutation.isPending;
  const controlRoomRefreshing =
    riskQuery.isFetching ||
    accountSummaryQuery.isFetching ||
    acceptanceGateQuery.isFetching ||
    ibkrStatusQuery.isFetching ||
    positionsQuery.isFetching ||
    runStatusQuery.isFetching ||
    killSwitchQuery.isFetching ||
    marketDataDiagnosticsQuery.isFetching ||
    ibkrReadinessQuery.isFetching;

  const buildPolicyPayload = () => {
    const payload = {};

    for (const field of ALL_POLICY_FIELDS) {
      const raw = policyForm[field];
      const parsed = toPolicyPayloadNumber(field, raw);
      if (!Number.isFinite(parsed)) {
        throw new Error(`Policy field '${field}' must be numeric.`);
      }
      payload[field] = POLICY_FIELD_META[field]?.integer ? Math.round(parsed) : parsed;
    }

    const parsedUniverse = parseUniverseInput(universeInput);
    if (parsedUniverse.length === 0) {
      throw new Error("Universe symbols cannot be empty.");
    }

    const selectedScanCode = String(policyForm.ibkrScanCode ?? persistedPolicyScanCode).trim();
    if (!SCREENER_MODE_OPTIONS.some((option) => option.value === selectedScanCode)) {
      throw new Error("Select a valid IBKR screener type.");
    }
    const selectedAnalysisProvider = String(
      policyForm.analysisDataProvider ?? persistedAnalysisDataProvider
    )
      .trim()
      .toUpperCase();
    if (!ANALYSIS_PROVIDER_OPTIONS.some((option) => option.value === selectedAnalysisProvider)) {
      throw new Error("Select a valid analysis data provider.");
    }

    payload.ibkrScanCode = selectedScanCode;
    payload.analysisDataProvider = selectedAnalysisProvider;
    payload.universeSymbols = parsedUniverse;
    return payload;
  };

  const handleSavePolicy = () => {
    try {
      const payload = buildPolicyPayload();
      updatePolicyMutation.mutate(payload);
    } catch (error) {
      toastManager.add({
        title: "Invalid policy values",
        description: String(error),
        type: "error"
      });
    }
  };

  const handleSaveEnv = () => {
    if (envChangedKeys.length === 0) {
      toastManager.add({
        title: "No env changes",
        description: "Edit one or more values before saving.",
        type: "info"
      });
      return;
    }

    const values = {};
    envChangedKeys.forEach((key) => {
      values[key] = envForm[key] ?? "";
    });
    updateEnvMutation.mutate({ values });
  };

  const markSymbolProposing = (symbol, proposing) => {
    const normalizedSymbol = String(symbol ?? "").trim().toUpperCase();
    if (!normalizedSymbol) return;
    if (proposing) {
      proposingSymbolsRef.current.add(normalizedSymbol);
    } else {
      proposingSymbolsRef.current.delete(normalizedSymbol);
    }
    setProposingBySymbol((previous) => {
      if (proposing) {
        if (previous[normalizedSymbol]) return previous;
        return {
          ...previous,
          [normalizedSymbol]: true
        };
      }
      if (!previous[normalizedSymbol]) return previous;
      const next = { ...previous };
      delete next[normalizedSymbol];
      return next;
    });
  };

  const isSymbolProposing = (symbolValue) => {
    const normalizedSymbol = String(symbolValue ?? "").trim().toUpperCase();
    if (!normalizedSymbol) return false;
    return Boolean(proposingBySymbol[normalizedSymbol]);
  };

  const proposeOrderForSymbol = async (symbolValue) => {
    const normalizedSymbol = String(symbolValue ?? "").trim().toUpperCase();
    if (!normalizedSymbol) return { symbol: "", ok: false, reason: "empty_symbol" };
    if (proposingSymbolsRef.current.has(normalizedSymbol)) {
      return { symbol: normalizedSymbol, ok: false, reason: "already_proposing" };
    }
    markSymbolProposing(normalizedSymbol, true);
    try {
      await proposeMutation.mutateAsync(normalizedSymbol);
      return { symbol: normalizedSymbol, ok: true };
    } catch {
      return { symbol: normalizedSymbol, ok: false, reason: "request_failed" };
    } finally {
      markSymbolProposing(normalizedSymbol, false);
    }
  };

  const handleProposeAllActionable = async () => {
    const candidateSymbols = actionableSymbols.filter((symbolValue) => !isSymbolProposing(symbolValue));
    if (candidateSymbols.length === 0) {
      toastManager.add({
        title: "No actionable symbols",
        description: "There are no available actionable recommendations to create tickets for.",
        type: "info"
      });
      return;
    }

    const maxWorkers = Math.min(3, candidateSymbols.length);
    let cursor = 0;
    let completed = 0;
    let success = 0;
    let failed = 0;

    setBulkProposeProgress({
      running: true,
      total: candidateSymbols.length,
      completed: 0,
      success: 0,
      failed: 0
    });

    const runWorker = async () => {
      while (cursor < candidateSymbols.length) {
        const symbolValue = candidateSymbols[cursor];
        cursor += 1;
        const result = await proposeOrderForSymbol(symbolValue);
        completed += 1;
        if (result.ok) {
          success += 1;
        } else {
          failed += 1;
        }
        setBulkProposeProgress({
          running: true,
          total: candidateSymbols.length,
          completed,
          success,
          failed
        });
      }
    };

    await Promise.all(Array.from({ length: maxWorkers }, () => runWorker()));

    setBulkProposeProgress((previous) => ({
      ...previous,
      running: false
    }));

    toastManager.add({
      title: "Bulk ticket creation finished",
      description: `${success} succeeded, ${failed} failed.`,
      type: failed > 0 ? "warning" : "success"
    });
  };

  const openRecommendationDetails = (entry) => {
    setSelectedRecommendation(entry);
    setRecommendationDetailsOpen(true);
  };

  const jumpToDecisionOutput = () => {
    setOrderWorkflowTab("decision");
    orderWorkflowRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
  };

  const renderScheduleFrequency = (item) => {
    const frequency = item?.frequency;
    if (!frequency || typeof frequency !== "object") return <span className="text-slate-400">—</span>;

    if (frequency.mode === "interval") {
      return (
        <div className="inline-flex items-center gap-2 text-slate-700">
          <Clock3 className="size-3.5 text-slate-500" />
          <span>Every {formatIntervalShort(Number(frequency.intervalMs))}</span>
        </div>
      );
    }

    if (frequency.mode === "triggered") {
      const triggers = Array.isArray(frequency.triggers)
        ? frequency.triggers.filter((value) => typeof value === "string" && value.trim().length > 0)
        : [];
      const intervalText = Number.isFinite(Number(frequency.intervalMs))
        ? formatIntervalShort(Number(frequency.intervalMs))
        : "event";

      return (
        <div className="inline-flex items-center gap-2 text-slate-700">
          <TriggerListPopover triggers={triggers} />
          <span>{intervalText}</span>
        </div>
      );
    }

    if (frequency.mode === "manual") {
      const method = String(frequency.method ?? item?.method ?? "").toUpperCase();
      const endpoint = String(frequency.endpoint ?? item?.endpoint ?? "");
      return (
        <div className="inline-flex items-center gap-2 text-slate-700">
          <Webhook className="size-3.5 text-slate-500" />
          <span className="font-mono text-[11px]">
            {method} {endpoint}
          </span>
        </div>
      );
    }

    return <span className="text-slate-400">—</span>;
  };

  return (
    <ToastProvider>
      {liveAccountConnected && (
        <div className="fixed inset-x-0 top-0 z-[70] border-b border-red-300 bg-red-600 text-white shadow">
          <div className="mx-auto flex w-full max-w-7xl items-center gap-2 px-4 py-2 text-sm font-semibold md:px-6">
            <AlertTriangle className="size-4 shrink-0" />
            <span>
              LIVE ACCOUNT CONNECTED: IBKR session is live on {ibkrStatus?.host}:{ibkrStatus?.port}.
            </span>
          </div>
        </div>
      )}
      <Dialog open={sessionMismatchDialogOpen} onOpenChange={setSessionMismatchDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <AlertTriangle className="size-4 text-amber-600" />
              Session/Env Mismatch
            </DialogTitle>
            <DialogDescription>
              Connected IBKR session is <span className="font-semibold">{ibkrSessionMode}</span>, but bot env is{" "}
              <span className="font-semibold">{botMode}</span>. Kill switch has been forced ON to block new entries.
            </DialogDescription>
            <p className="rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-900">
              Align these modes before continuing: connect to the matching IBKR port/session or change your env mode.
            </p>
          </DialogHeader>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => {
                queryClient.invalidateQueries({ queryKey: queryKeys.ibkrStatus });
                queryClient.invalidateQueries({ queryKey: queryKeys.marketDataDiagnostics });
                queryClient.invalidateQueries({ queryKey: queryKeys.ibkrReadiness });
              }}
            >
              Refresh IBKR
            </Button>
            <DialogClose render={<Button>Acknowledge</Button>} />
          </DialogFooter>
        </DialogContent>
      </Dialog>
      <Dialog open={entitlementsOpen} onOpenChange={setEntitlementsOpen}>
        <DialogContent className="max-w-5xl">
          <DialogHeader>
            <DialogTitle>Market Data Entitlements</DialogTitle>
            <DialogDescription>
              Per-symbol diagnostics from recent IBKR market-data requests (live vs delayed vs blocked).
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-3">
            <div className="grid grid-cols-2 gap-2 md:grid-cols-6">
              <MetricTile
                label="Core Market"
                value={String(entitlementSummary?.coreMarketStatus ?? "unknown").replaceAll("_", " ").toUpperCase()}
              />
              <MetricTile
                label="Core Options"
                value={String(entitlementSummary?.coreOptionsStatus ?? "unknown").replaceAll("_", " ").toUpperCase()}
              />
              <MetricTile
                label="Quote Live"
                value={String(entitlementSummary?.quoteCounts?.live ?? 0)}
              />
              <MetricTile
                label="Quote Delayed"
                value={String(entitlementSummary?.quoteCounts?.delayed ?? 0)}
              />
              <MetricTile
                label="Quote Blocked"
                value={String(entitlementSummary?.quoteCounts?.blocked ?? 0)}
              />
              <MetricTile
                label="Option Invalid"
                value={String(entitlementSummary?.optionCounts?.invalidContract ?? 0)}
              />
            </div>

            <div className="max-h-[55vh] overflow-auto rounded-xl border border-slate-200">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Symbol</TableHead>
                    <TableHead>Quote</TableHead>
                    <TableHead>Option</TableHead>
                    <TableHead>Last Quote Attempt</TableHead>
                    <TableHead>Last Option Attempt</TableHead>
                    <TableHead>Notes</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {entitlementRows.map((row) => (
                    <TableRow key={row.symbol}>
                      <TableCell className="font-medium">{row.symbol}</TableCell>
                      <TableCell>
                        <Badge variant={entitlementStateVariant(row.quote?.state)}>
                          {entitlementStateLabel(row.quote?.state)}
                        </Badge>
                      </TableCell>
                      <TableCell>
                        <Badge variant={entitlementStateVariant(row.option?.state)}>
                          {entitlementStateLabel(row.option?.state)}
                        </Badge>
                      </TableCell>
                      <TableCell className="text-xs">{formatTimestamp(row.quote?.lastAt)}</TableCell>
                      <TableCell className="text-xs">{formatTimestamp(row.option?.lastAt)}</TableCell>
                      <TableCell className="max-w-96 text-xs text-slate-600">
                        {row.quote?.note || row.option?.note || "—"}
                      </TableCell>
                    </TableRow>
                  ))}
                  {entitlementRows.length === 0 && (
                    <TableRow>
                      <TableCell colSpan={6} className="text-center text-slate-500">
                        {marketDataDiagnosticsQuery.isFetching
                          ? "Loading entitlement diagnostics..."
                          : "No entitlement diagnostics captured yet."}
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            </div>
            {entitlementBackoffSymbols.length > 0 || optionEntitlementBackoff?.active ? (
              <div className="rounded-xl border border-amber-200 bg-amber-50/70 p-3 text-xs text-amber-900">
                <p className="font-semibold uppercase tracking-wide">Active Broker Backoffs</p>
                {entitlementBackoffSymbols.length > 0 && (
                  <p className="mt-1">
                    Quote backoffs:{" "}
                    {entitlementBackoffSymbols
                      .slice(0, 8)
                      .map((entry) => `${entry.symbol} (${formatDuration(entry.remainingMs)})`)
                      .join(", ")}
                  </p>
                )}
                {optionEntitlementBackoff?.active && (
                  <p className="mt-1">
                    Option quote backoff: {formatDuration(optionEntitlementBackoff.remainingMs)} (
                    {optionEntitlementBackoff.reason || "subscription/backoff"})
                  </p>
                )}
              </div>
            ) : null}
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => queryClient.invalidateQueries({ queryKey: queryKeys.marketDataDiagnostics })}
            >
              Refresh Diagnostics
            </Button>
            <DialogClose render={<Button>Close</Button>} />
          </DialogFooter>
        </DialogContent>
      </Dialog>
      <Dialog open={readinessOpen} onOpenChange={setReadinessOpen}>
        <DialogContent className="max-w-5xl">
          <DialogHeader>
            <DialogTitle>IBKR Readiness</DialogTitle>
            <DialogDescription>
              Transport and data readiness checks for quote/history/scanner/positions.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-3">
            <div className="grid grid-cols-2 gap-2 md:grid-cols-6">
              <MetricTile
                label="Overall"
                value={ibkrReadiness?.overall ? "READY" : "NOT READY"}
                emphasis
              />
              <MetricTile
                label="Connectivity"
                value={ibkrReadiness?.checks?.connectivity?.ok ? "OK" : "FAIL"}
              />
              <MetricTile
                label="Queue"
                value={ibkrReadiness?.checks?.queue?.ok ? "OK" : "BACKLOG"}
              />
              <MetricTile
                label="Quote"
                value={ibkrReadiness?.checks?.quote?.ok ? "OK" : "FAIL"}
              />
              <MetricTile
                label="Historical"
                value={ibkrReadiness?.checks?.historical?.ok ? "OK" : "FAIL"}
              />
              <MetricTile
                label="Scanner"
                value={ibkrReadiness?.checks?.scanner?.ok ? "OK" : "FAIL"}
              />
            </div>

            {Array.isArray(ibkrReadiness?.advice) && ibkrReadiness.advice.length > 0 ? (
              <div className="rounded-xl border border-amber-200 bg-amber-50/70 p-3 text-xs text-amber-900">
                <p className="font-semibold uppercase tracking-wide">Advice</p>
                <ul className="mt-2 space-y-1">
                  {ibkrReadiness.advice.map((item) => (
                    <li key={item}>{item}</li>
                  ))}
                </ul>
              </div>
            ) : null}

            <JsonBox
              value={ibkrReadiness}
              empty={ibkrReadinessQuery.isLoading ? "Running readiness checks..." : "No readiness data yet."}
            />
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => queryClient.invalidateQueries({ queryKey: queryKeys.ibkrReadiness })}
            >
              Refresh Readiness
            </Button>
            <DialogClose render={<Button>Close</Button>} />
          </DialogFooter>
        </DialogContent>
      </Dialog>
      <RecommendationDetailsDialog
        open={recommendationDetailsOpen}
        entry={selectedRecommendation}
        onOpenChange={(nextOpen) => {
          setRecommendationDetailsOpen(nextOpen);
          if (!nextOpen) {
            setSelectedRecommendation(null);
          }
        }}
        onSetSymbol={(nextSymbol) => setSymbol(nextSymbol)}
        onPropose={(nextSymbol) => {
          void proposeOrderForSymbol(nextSymbol);
        }}
        isProposingSymbol={isSymbolProposing}
      />
      <div className={`min-h-screen p-4 md:p-6 ${liveAccountConnected ? "pt-14 md:pt-16" : ""}`}>
        <div className="mx-auto flex w-full max-w-7xl flex-col gap-4">
          <Card className="border border-slate-200 bg-white/75 backdrop-blur">
            <CardHeader className="flex items-start justify-between gap-3 md:flex-row md:items-center">
              <div>
                <CardTitle className="text-2xl font-semibold tracking-tight">Options Bot Control Room</CardTitle>
                <CardDescription>
                  Use recommendations to pick symbols for order proposals, then tune settings in the harness modal.
                </CardDescription>
              </div>
              <div className="flex flex-col items-end gap-2">
                <div className="flex items-center gap-2">
                  <Badge variant={openRiskBadge.color}>{`Risk: ${openRiskBadge.label}`}</Badge>
                  <Badge variant={ibkrBadge.color}>{`IBKR: ${ibkrBadge.label}`}</Badge>
                  <Button
                    variant="outline"
                    onClick={() => {
                      queryClient.invalidateQueries({ queryKey: queryKeys.risk });
                      queryClient.invalidateQueries({ queryKey: queryKeys.accountSummary });
                      queryClient.invalidateQueries({ queryKey: queryKeys.acceptanceGate });
                      queryClient.invalidateQueries({ queryKey: queryKeys.ibkrStatus });
                      queryClient.invalidateQueries({ queryKey: queryKeys.positions });
                      queryClient.invalidateQueries({ queryKey: queryKeys.runStatus });
                      queryClient.invalidateQueries({ queryKey: queryKeys.killSwitch });
                      queryClient.invalidateQueries({ queryKey: queryKeys.marketDataDiagnostics });
                      queryClient.invalidateQueries({ queryKey: queryKeys.ibkrReadiness });
                    }}
                  >
                    {controlRoomRefreshing ? <Spinner className="h-4 w-4" /> : <RefreshCcw className="size-4" />}
                    Refresh
                  </Button>
                </div>
                <div className="flex items-center gap-2">
                  <Button
                    variant={hasPendingApprovals ? "destructive" : "outline"}
                    onClick={() => {
                      setOrderWorkflowTab("pending");
                      orderWorkflowRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
                    }}
                  >
                    <BellRing className="size-4" />
                    Pending Approvals
                    <Badge variant={hasPendingApprovals ? "warning" : "outline"}>{pendingApprovalCount}</Badge>
                  </Button>
                  <Button
                    variant={killSwitchEnabled ? "destructive" : "outline"}
                    onClick={() => killSwitchMutation.mutate({ enabled: !killSwitchEnabled })}
                  >
                    {killSwitchMutation.isPending && <Spinner className="h-4 w-4" />}
                    {killSwitchEnabled ? "Kill Switch: ON" : "Kill Switch: OFF"}
                  </Button>
                  <Button variant="outline" onClick={() => setRunTimingOpen(true)}>
                    <Clock3 className="size-4" /> Schedule
                  </Button>
                  <Button variant="outline" onClick={() => setSettingsOpen(true)}>
                    <Settings2 className="size-4" /> Settings
                  </Button>
                  {anyBusy && <Spinner className="h-5 w-5" />}
                </div>
              </div>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="grid grid-cols-1 gap-3 md:grid-cols-[220px_120px_170px_170px_220px_1fr_auto_auto_auto]">
                <div className="space-y-1">
                  <p className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Recommendations</p>
                  <Button
                    variant="outline"
                    disabled={
                      recommendationsQuery.isFetching ||
                      updatePolicyMutation.isPending ||
                      scanPolicyPersisting ||
                      scannerProviderPersisting ||
                      !policyQuery.isSuccess ||
                      scanBlockedByConnection
                    }
                    onClick={async () => {
                      if (scanBlockedByConnection) return;
                      const hadDirtyScanInputs =
                        scanTopNDirty || scanCodeDirty || analysisProviderDirty;
                      try {
                        await ensureScannerProviderPersisted();
                        await ensureScanPolicyPersisted();
                        if (!hadDirtyScanInputs) {
                          await queryClient.cancelQueries({ queryKey: queryKeys.recommendations });
                          await recommendationsQuery.refetch();
                        }
                      } catch (error) {
                        toastManager.add({
                          title: "Scan blocked",
                          description: String(error),
                          type: "error"
                        });
                      }
                    }}
                  >
                    {recommendationsQuery.isFetching && <Spinner className="h-4 w-4" />}
                    {recommendationsButtonLabel}
                  </Button>
                </div>
                <label htmlFor="control-scan-top-n" className="block space-y-1">
                  <span className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Top N</span>
                  <Input
                    id="control-scan-top-n"
                    name="scan_top_n"
                    nativeInput
                    type="number"
                    min="1"
                    max="100"
                    disabled={!policyQuery.isSuccess || updatePolicyMutation.isPending}
                    value={policyForm.scanTopN ?? String(persistedPolicyTopN)}
                    onChange={(event) =>
                      setPolicyForm((previous) => ({
                        ...previous,
                        scanTopN: event.target.value
                      }))
                    }
                    placeholder="Top N"
                  />
                </label>
                <div className="space-y-1">
                  <label htmlFor="control-ibkr-screener-mode" className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">
                    Screener Mode
                  </label>
                  <Select
                    name="control_ibkr_scan_code"
                    items={SCREENER_MODE_OPTIONS}
                    value={policyForm.ibkrScanCode ?? persistedPolicyScanCode}
                    disabled={!policyQuery.isSuccess || updatePolicyMutation.isPending}
                    onValueChange={(value) => {
                      if (typeof value !== "string" || value.trim().length === 0) return;
                      setPolicyForm((previous) => {
                        if (previous.ibkrScanCode === value) return previous;
                        return {
                          ...previous,
                          ibkrScanCode: value
                        };
                      });
                    }}
                  >
                    <SelectTrigger id="control-ibkr-screener-mode" aria-label="Screener mode">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectPopup>
                      {SCREENER_MODE_OPTIONS.map((option) => (
                        <SelectItem key={option.value} value={option.value}>
                          {option.label}
                        </SelectItem>
                      ))}
                    </SelectPopup>
                  </Select>
                </div>
                <div className="space-y-1">
                  <label htmlFor="control-analysis-provider" className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">
                    Analysis Data
                  </label>
                  <Select
                    name="control_analysis_data_provider"
                    items={ANALYSIS_PROVIDER_OPTIONS}
                    value={policyForm.analysisDataProvider ?? persistedAnalysisDataProvider}
                    disabled={!policyQuery.isSuccess || updatePolicyMutation.isPending}
                    onValueChange={(value) => {
                      if (typeof value !== "string" || value.trim().length === 0) return;
                      setPolicyForm((previous) => {
                        if (previous.analysisDataProvider === value) return previous;
                        return {
                          ...previous,
                          analysisDataProvider: value
                        };
                      });
                    }}
                  >
                    <SelectTrigger id="control-analysis-provider" aria-label="Analysis data provider">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectPopup>
                      {ANALYSIS_PROVIDER_OPTIONS.map((option) => (
                        <SelectItem key={option.value} value={option.value}>
                          {option.label}
                        </SelectItem>
                      ))}
                    </SelectPopup>
                  </Select>
                </div>
                <div className="space-y-1">
                  <label htmlFor="control-scanner-provider-order" className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">
                    Scanner Source
                  </label>
                  <Select
                    name="control_scanner_provider_order"
                    items={SCANNER_PROVIDER_MODE_OPTIONS}
                    value={scannerProviderModeValue}
                    disabled={
                      !scannerProviderField ||
                      scannerProviderPersisting ||
                      updateEnvMutation.isPending ||
                      envConfigQuery.isLoading
                    }
                    onValueChange={(value) => {
                      if (typeof value !== "string" || value.trim().length === 0) return;
                      if (value === SCANNER_PROVIDER_CUSTOM_VALUE) {
                        setSettingsTab("environment");
                        setSettingsOpen(true);
                        return;
                      }
                      setScannerProviderOrderDraft(value);
                    }}
                  >
                    <SelectTrigger id="control-scanner-provider-order" aria-label="Scanner provider order">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectPopup>
                      {SCANNER_PROVIDER_MODE_OPTIONS.map((option) => (
                        <SelectItem key={option.value} value={option.value}>
                          {option.label}
                        </SelectItem>
                      ))}
                    </SelectPopup>
                  </Select>
                </div>
                <label htmlFor="control-symbol" className="block space-y-1">
                  <span className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Symbol</span>
                  <Input
                    id="control-symbol"
                    name="symbol"
                    value={symbol}
                    onChange={(event) => setSymbol(event.target.value)}
                    placeholder="Symbol (e.g. SPY)"
                  />
                </label>
                <div className="space-y-1">
                  <p className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Decision</p>
                  <Button
                    variant="outline"
                    disabled={decisionMutation.isPending || symbol.trim().length === 0}
                    onClick={() => decisionMutation.mutate()}
                  >
                    {decisionMutation.isPending && <Spinner className="h-4 w-4" />}
                    {decisionMutation.isPending ? "Analyzing..." : "Get Decision"}
                  </Button>
                </div>
                <div className="space-y-1">
                  <p className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Raw Analysis</p>
                  <div className="flex items-center gap-1">
                    <Button
                      disabled={
                        scanMutation.isPending ||
                        updatePolicyMutation.isPending ||
                        scanPolicyPersisting ||
                        scannerProviderPersisting ||
                        scanBlockedByConnection
                      }
                      onClick={async () => {
                        if (scanBlockedByConnection) return;
                        try {
                          await ensureScannerProviderPersisted();
                          const policyValues = await ensureScanPolicyPersisted();
                          scanMutation.mutate(policyValues.topN);
                        } catch (error) {
                          toastManager.add({
                            title: "Candidate snapshot blocked",
                            description: String(error),
                            type: "error"
                          });
                        }
                      }}
                    >
                      {scanMutation.isPending && <Spinner className="h-4 w-4" />}
                      {scanMutation.isPending ? "Running Snapshot..." : "Run Candidate Snapshot"}
                    </Button>
                    <Popover>
                      <PopoverTrigger
                        aria-label="What does candidate snapshot do?"
                        className="inline-flex size-8 items-center justify-center rounded-md border border-slate-200 text-slate-600 hover:bg-slate-50"
                      >
                        <HelpCircle className="size-4" />
                      </PopoverTrigger>
                      <PopoverContent align="start" className="w-80">
                        <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">Candidate Snapshot</p>
                        <p className="mt-2 text-xs text-slate-700">
                          Runs `POST /scan` and stores full per-symbol analysis payloads for debugging signals, sources,
                          and indicator coverage. Use this when you want raw diagnostics, not ranked recommendations.
                        </p>
                      </PopoverContent>
                    </Popover>
                  </div>
                </div>
                <div className="space-y-1">
                  <p className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Simulation</p>
                  <Button variant="outline" disabled={backtestMutation.isPending} onClick={() => backtestMutation.mutate()}>
                    {backtestMutation.isPending && <Spinner className="h-4 w-4" />}
                    {backtestMutation.isPending ? "Backtesting..." : "Run Backtest"}
                  </Button>
                </div>
              </div>

              <p className="text-xs text-slate-500">
                Scanner provider order:{" "}
                {scannerProviderDraftOrder || "not set"}
                {scannerProviderDirty ? " (will be applied on next scan)" : ""}
                {scannerProviderPersisting ? " (saving...)" : ""}
              </p>

              <div className="rounded-xl border border-slate-200 bg-white/70 p-3">
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <div>
                    <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">Latest Decision Result</p>
                    <p className="mt-1 text-xs text-slate-600">
                      Quick outcome from `Get Decision` or order proposal. Open Decision Output for full payload and logs.
                    </p>
                  </div>
                  <Button variant="outline" size="sm" onClick={jumpToDecisionOutput}>
                    <ExternalLink className="size-4" />
                    Open Decision Output
                  </Button>
                </div>
                {decisionMutation.isPending ? (
                  <div className="mt-3 flex items-center gap-2 rounded-lg border border-slate-200 bg-slate-50 p-3 text-sm text-slate-700">
                    <Spinner className="h-4 w-4" />
                    Computing decision for {symbol.trim().toUpperCase() || "selected symbol"}...
                  </div>
                ) : decisionSummary ? (
                  <div className="mt-3 space-y-3">
                    <div className="flex flex-wrap items-center gap-2">
                      <Badge variant="outline">{decisionSummary.symbol ?? "UNKNOWN"}</Badge>
                      {decisionSummary.action ? (
                        <Badge variant={actionBadgeVariant(decisionSummary.action)}>
                          {decisionSummary.action}
                        </Badge>
                      ) : (
                        <Badge variant="outline">No action</Badge>
                      )}
                      {decisionSummary.orderTicket ? (
                        <Badge variant="success">
                          Ticket {decisionSummary.orderTicket.action ?? "-"} {decisionSummary.orderTicket.symbol ?? "-"} x
                          {decisionSummary.orderTicket.quantity ?? "-"}
                        </Badge>
                      ) : null}
                    </div>
                    <div className="grid grid-cols-2 gap-2 md:grid-cols-6">
                      <MetricTile
                        label="Composite"
                        value={
                          typeof decisionSummary.compositeScore === "number"
                            ? numberFmt.format(decisionSummary.compositeScore)
                            : "—"
                        }
                        emphasis
                      />
                      <MetricTile
                        label="P(Up)"
                        value={
                          typeof decisionSummary.directionalUpProb === "number"
                            ? decisionSummary.directionalUpProb.toFixed(3)
                            : "—"
                        }
                      />
                      <MetricTile
                        label="P(Down)"
                        value={
                          typeof decisionSummary.directionalDownProb === "number"
                            ? decisionSummary.directionalDownProb.toFixed(3)
                            : "—"
                        }
                      />
                      <MetricTile
                        label="Confidence"
                        value={
                          typeof decisionSummary.confidence === "number"
                            ? decisionSummary.confidence.toFixed(3)
                            : "—"
                        }
                      />
                      <MetricTile
                        label="Veto Flags"
                        value={String(decisionSummary.vetoFlags.length)}
                      />
                      <MetricTile
                        label="Evaluated"
                        value={decisionSummary.evaluatedAt ? formatTimestamp(decisionSummary.evaluatedAt) : "—"}
                      />
                    </div>
                    {decisionSummary.rationale ? (
                      <p className="rounded-lg border border-slate-200 bg-slate-50 p-2 text-xs text-slate-700">
                        {decisionSummary.rationale}
                      </p>
                    ) : null}
                    {decisionSummary.vetoFlags.length > 0 ? (
                      <p className="text-xs text-amber-700">
                        Veto flags: {decisionSummary.vetoFlags.join(", ")}
                      </p>
                    ) : null}
                  </div>
                ) : (
                  <p className="mt-3 rounded-lg border border-dashed border-slate-300 p-3 text-xs text-slate-500">
                    No decision has been requested yet.
                  </p>
                )}
              </div>
            </CardContent>
          </Card>

          <div className="grid grid-cols-1 gap-4 xl:grid-cols-3">
            <Card className="xl:col-span-2">
              <CardHeader className="flex items-start justify-between gap-3 md:flex-row md:items-center">
                <div>
                  <CardTitle>Account Summary</CardTitle>
                  <CardDescription>
                    Equity, intraday PnL, and risk posture snapshot. PnL graph is sourced from stored risk snapshots.
                  </CardDescription>
                </div>
                <Button variant="outline" size="sm" onClick={() => setPositionsOpen(true)}>
                  Positions ({positionsButtonCount})
                </Button>
              </CardHeader>
              <CardContent className="space-y-4">
                {!ibkrConnected && (
                  <div className="rounded-xl border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-900">
                    Account summary sync is paused until IBKR is connected.
                  </div>
                )}
                <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
                  <MetricTile
                    label="Account Equity"
                    value={ibkrConnected ? currencyFmt.format(accountSummary?.accountEquity ?? 0) : "—"}
                    emphasis
                  />
                  <MetricTile label="Day Total" value={ibkrConnected ? signedMoney(accountSummary?.dayTotalPnl ?? 0) : "—"} emphasis />
                  <MetricTile label="Day Realized" value={ibkrConnected ? signedMoney(accountSummary?.dayRealizedPnl ?? 0) : "—"} />
                  <MetricTile label="Day Unrealized" value={ibkrConnected ? signedMoney(accountSummary?.dayUnrealizedPnl ?? 0) : "—"} />
                </div>
                <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
                  <div className="rounded-xl border border-slate-200 bg-white/70 p-3">
                    <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">Risk and Exposure</p>
                    <div className="mt-2 grid grid-cols-[1fr_auto] gap-x-3 gap-y-1 text-xs">
                      <span className="text-slate-500">Drawdown</span>
                      <span className="text-right font-medium text-slate-800">
                        {pctFmt(accountSummary?.dailyDrawdownPct ?? 0)}
                      </span>
                      <span className="text-slate-500">Open Positions (Local)</span>
                      <span className="text-right font-medium text-slate-800">
                        {String(accountSummary?.openPositions ?? 0)}
                      </span>
                      <span className="text-slate-500">Open Positions (Broker)</span>
                      <span className="text-right font-medium text-slate-800">
                        {accountSummary?.brokerOpenPositions === null ||
                        accountSummary?.brokerOpenPositions === undefined
                          ? "n/a"
                          : String(accountSummary.brokerOpenPositions)}
                      </span>
                      <span className="text-slate-500">Pending Approvals</span>
                      <span className="text-right font-medium text-slate-800">
                        {String(accountSummary?.pendingApprovals ?? 0)}
                      </span>
                      <span className="text-slate-500">Acceptance Gate</span>
                      <span className="text-right">
                        <Badge variant={acceptanceGatePass ? "success" : "warning"}>
                          {acceptanceGatePass ? "PASS" : "IN PROGRESS"}
                        </Badge>
                      </span>
                      <span className="text-slate-500">Gate Progress</span>
                      <span className="text-right font-medium text-slate-800">
                        {`${acceptanceGateObservedDays.toFixed(1)}d / ${acceptanceGateCompletedTrades} trades`}
                      </span>
                      <span className="text-slate-500">Halt Reasons</span>
                      <span className="text-right font-medium text-slate-800">
                        {accountSummary?.haltReasons?.join(", ") || "none"}
                      </span>
                    </div>
                  </div>
                  <div className="rounded-xl border border-slate-200 bg-white/70 p-3">
                    <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">Broker Link</p>
                    <div className="mt-2 grid grid-cols-[1fr_auto] gap-x-3 gap-y-1 text-xs">
                      <span className="text-slate-500">Broker Source</span>
                      <span className="text-right font-medium text-slate-800">
                        {brokerAccount?.source ?? "unavailable"}
                      </span>
                      <span className="text-slate-500">Broker Account</span>
                      <span className="text-right font-medium text-slate-800">
                        {brokerAccount?.accountCode ?? "n/a"}
                      </span>
                      <span className="text-slate-500">IBKR Session</span>
                      <span className="text-right font-medium text-slate-800">{ibkrSessionMode}</span>
                      <span className="text-slate-500">Bot Env</span>
                      <span className="text-right font-medium text-slate-800">{botMode}</span>
                    </div>
                  </div>
                </div>
                <PnlLineChart series={pnlSeries} />
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle>IBKR Connectivity</CardTitle>
                <CardDescription>
                  Connectivity and launcher controls for IBKR Gateway/TWS.
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-2 text-xs">
                <div className="flex items-center gap-2">
                  <Badge variant={ibkrBadge.color}>{`${ibkrBadge.label}`}</Badge>
                  <Badge variant="outline">{`Session: ${ibkrSessionMode}`}</Badge>
                  <Badge variant={botMode === "LIVE" ? "destructive" : "outline"}>{`Bot Env: ${botMode}`}</Badge>
                  {sessionEnvironmentMismatch ? (
                    <Badge variant="destructive" title="Session/Env mismatch">
                      <AlertTriangle className="size-3.5" />
                    </Badge>
                  ) : null}
                  {ibkrStatusQuery.isFetching && <Spinner className="h-4 w-4" />}
                </div>
                <div className="grid grid-cols-2 gap-x-3 gap-y-1 rounded-xl border border-slate-200 bg-white/70 p-2 text-xs">
                  <span className="text-slate-500">Enabled</span>
                  <span className="text-right font-medium text-slate-800">{String(Boolean(ibkrStatus?.enabled))}</span>
                  <span className="text-slate-500">Host:Port</span>
                  <span className="text-right font-medium text-slate-800">{`${ibkrStatus?.host ?? "-"}:${ibkrStatus?.port ?? "-"}`}</span>
                  <span className="text-slate-500">Client ID</span>
                  <span className="text-right font-medium text-slate-800">{String(ibkrStatus?.clientId ?? "-")}</span>
                  <span className="text-slate-500">Latency (ms)</span>
                  <span className="text-right font-medium text-slate-800">{String(ibkrStatus?.latencyMs ?? "-")}</span>
                </div>
                <p className="rounded-xl border border-slate-200 bg-white/70 p-2 text-xs text-slate-600">
                  {ibkrStatus?.message ?? "IBKR status unavailable."}
                </p>
                {!!ibkrStatus?.probedPorts?.length && (
                  <p className="rounded-xl border border-slate-200 bg-white/70 p-2 text-xs text-slate-600">
                    Probed ports: {ibkrStatus.probedPorts.join(", ")}
                  </p>
                )}
                <div className="rounded-xl border border-slate-200 bg-white/70 p-2">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <Badge variant={entitlementGateBadge.variant}>{entitlementGateBadge.label}</Badge>
                    <div className="flex items-center gap-2">
                      <Button size="sm" variant="outline" onClick={() => setReadinessOpen(true)}>
                        Readiness {ibkrReadiness?.overall ? "OK" : "Check"}
                      </Button>
                      <Button
                        size="sm"
                        variant="outline"
                        disabled={!ibkrConnected}
                        onClick={() => setEntitlementsOpen(true)}
                      >
                        Entitlements ({entitlementRows.length})
                      </Button>
                    </div>
                  </div>
                  <p className="mt-2 text-xs text-slate-600">
                    Quotes: live {String(entitlementSummary?.quoteCounts?.live ?? 0)} | delayed{" "}
                    {String(entitlementSummary?.quoteCounts?.delayed ?? 0)} | blocked{" "}
                    {String(entitlementSummary?.quoteCounts?.blocked ?? 0)}
                  </p>
                  <p className="mt-1 text-xs text-slate-600">
                    Options: live {String(entitlementSummary?.optionCounts?.live ?? 0)} | delayed{" "}
                    {String(entitlementSummary?.optionCounts?.delayed ?? 0)} | blocked{" "}
                    {String(entitlementSummary?.optionCounts?.blocked ?? 0)}
                  </p>
                  <p className="mt-1 text-xs text-slate-600">
                    Delayed-only mode: {marketDataDiagnostics?.brokerBackoffs?.delayedOnly ? "yes" : "no"}
                  </p>
                </div>
                <div className="rounded-xl border border-slate-200 bg-white/70 p-2">
                  <p className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">
                    Integration Readiness
                  </p>
                  <div className="mt-2 grid grid-cols-2 gap-x-3 gap-y-1.5">
                    {integrationStatusItems.map((integration) => (
                      <div
                        key={integration.id}
                        className="inline-flex items-center gap-1.5 text-xs text-slate-700"
                        title={integration.note || ""}
                      >
                        {integration.configured ? (
                          <CheckCircle2 className="size-3.5 text-emerald-600" />
                        ) : (
                          <HelpCircle className="size-3.5 text-amber-600" />
                        )}
                        <span>{integration.name}</span>
                      </div>
                    ))}
                    {integrationStatusItems.length === 0 && (
                      <p className="col-span-2 text-xs text-slate-500">Integration status unavailable.</p>
                    )}
                  </div>
                </div>
                <div className="flex flex-wrap gap-2">
                  <Button
                    size="sm"
                    disabled={launchIbkrMutation.isPending}
                    onClick={() => launchIbkrMutation.mutate("gateway")}
                  >
                    {launchIbkrMutation.isPending && <Spinner className="h-4 w-4" />}
                    {launchIbkrMutation.isPending ? "Launching..." : "Launch Gateway"}
                  </Button>
                  <Button
                    size="sm"
                    variant="outline"
                    disabled={launchIbkrMutation.isPending}
                    onClick={() => launchIbkrMutation.mutate("tws")}
                  >
                    {launchIbkrMutation.isPending && <Spinner className="h-4 w-4" />}
                    {launchIbkrMutation.isPending ? "Launching..." : "Launch TWS"}
                  </Button>
                  <Button
                    size="sm"
                    variant="outline"
                    disabled={launchIbkrMutation.isPending}
                    onClick={() => {
                      queryClient.invalidateQueries({ queryKey: queryKeys.ibkrStatus });
                      queryClient.invalidateQueries({ queryKey: queryKeys.marketDataDiagnostics });
                      queryClient.invalidateQueries({ queryKey: queryKeys.ibkrReadiness });
                    }}
                  >
                    Refresh IBKR
                  </Button>
                </div>
                {lastLaunchResult && (
                  <div className="rounded-xl border border-slate-200 bg-white/70 p-2 text-xs text-slate-600">
                    <p className="font-medium text-slate-800">
                      Last launch: {String(lastLaunchResult.target).toUpperCase()} /{" "}
                      {lastLaunchResult.launched ? "success" : "failed"}
                    </p>
                    <p>{lastLaunchResult.message}</p>
                    {lastLaunchResult.selectedApp && <p>Selected app: {lastLaunchResult.selectedApp}</p>}
                    {lastLaunchResult.commandPreview && <p>Command: {lastLaunchResult.commandPreview}</p>}
                  </div>
                )}
              </CardContent>
            </Card>
          </div>

          <Card className="order-2">
            <CardHeader className="flex items-start justify-between gap-3 md:flex-row md:items-center">
              <div>
                <CardTitle>Recommendations</CardTitle>
                <CardDescription>
                  {thresholdSummary
                    ? `Create order tickets only for actionable rows. Current gates: score >= ${thresholdSummary.minCompositeScore}, direction >= ${thresholdSummary.minDirectionalProbability}.`
                    : "Create order tickets only for actionable rows where deterministic rules and judge pass."}
                </CardDescription>
              </div>
              <div className="flex items-center gap-2">
                <Badge variant={activeProposalCount > 0 ? "warning" : "outline"}>
                  Tickets in progress: {activeProposalCount}
                </Badge>
                <Button
                  size="sm"
                  disabled={
                    !ibkrConnected ||
                    recommendationsQuery.isFetching ||
                    actionableSymbols.length === 0 ||
                    bulkProposeProgress.running
                  }
                  onClick={() => {
                    void handleProposeAllActionable();
                  }}
                >
                  {(bulkProposeProgress.running || activeProposalCount > 0) && <Spinner className="h-4 w-4" />}
                  {bulkProposeButtonLabel}
                </Button>
              </div>
            </CardHeader>
            <CardContent className="overflow-auto">
              <div className="mb-3 rounded-xl border border-slate-200 bg-white/70 p-3 text-xs">
                <div className="flex flex-wrap items-center gap-2">
                  <Badge variant={recommendationScanner?.scannerUsed ? "success" : "outline"}>
                    Scanner {recommendationScanner?.scannerUsed ? "USED" : "NOT USED"}
                  </Badge>
                  <Badge variant="outline">
                    Source: {String(recommendationScanner?.scannerSource ?? "none").replace("_", " ")}
                  </Badge>
                  <span className="text-slate-600">
                    Requested: {String(recommendationScanner?.requestedUniverseSize ?? "-")}
                  </span>
                  <span className="text-slate-600">
                    Evaluated: {String(recommendationScanner?.evaluatedUniverseSize ?? "-")}
                  </span>
                  <span className="text-slate-600">
                    Discovered: {String(recommendationScanner?.discoveredSymbols?.length ?? 0)}
                  </span>
                  <span className="text-slate-600">
                    Screener Mode: {String(recommendationScanner?.ibkrScanCode ?? "n/a")}
                  </span>
                </div>
                <p className="mt-2 text-slate-600">
                  {recommendationScanner?.discoveredSymbols?.length
                    ? `Discovered symbols: ${recommendationScanner.discoveredSymbols.join(", ")}`
                    : "Discovered symbols: none (scanner returned no usable additions)."}
                </p>
                <p className="mt-1 text-slate-600">
                  Providers used:{" "}
                  {Array.isArray(recommendationScanner?.scannerProvidersUsed) &&
                  recommendationScanner.scannerProvidersUsed.length > 0
                    ? recommendationScanner.scannerProvidersUsed.join(", ")
                    : "none"}
                </p>
                <p className="mt-1 text-slate-600">
                  Providers tried:{" "}
                  {Array.isArray(recommendationScanner?.scannerProvidersTried) &&
                  recommendationScanner.scannerProvidersTried.length > 0
                    ? recommendationScanner.scannerProvidersTried.join(", ")
                    : "none"}
                </p>
                {Array.isArray(recommendationScanner?.scannerProviderRanking) &&
                recommendationScanner.scannerProviderRanking.length > 0 ? (
                  <p className="mt-1 text-slate-600">
                    Quality order:{" "}
                    {recommendationScanner.scannerProviderRanking
                      .map((entry) => `${entry.provider} (${Number(entry.score ?? 0).toFixed(2)})`)
                      .join(", ")}
                  </p>
                ) : null}
                {recommendationScanner?.scannerFallbackReason ? (
                  <p className="mt-1 text-slate-600">
                    Scanner fallback note: {recommendationScanner.scannerFallbackReason}
                  </p>
                ) : null}
                <div className="mt-2 rounded-lg border border-slate-200 bg-slate-50 p-2 text-slate-600">
                  <p>
                    Recommendation runtime:{" "}
                    {recommendationRunSecondsLabel}{" "}
                    | Source: {String(recommendationsExecution?.source ?? "unknown")} | Timed out:{" "}
                    {recommendationsExecution?.timedOut ? "yes" : "no"} | Budget:{" "}
                    {recommendationTimeoutBudgetLabel}
                  </p>
                  {recommendationsExecution?.timeoutReason ? (
                    <p className="mt-1 text-amber-700">
                      Timeout reason: {recommendationsExecution.timeoutReason}
                    </p>
                  ) : null}
                  {recommendationRunErrors.length > 0 ? (
                    <div className="mt-1 space-y-1">
                      <p className="font-medium text-red-700">
                        Run errors ({recommendationRunErrors.length}):
                      </p>
                      <ul className="space-y-1">
                        {recommendationRunErrors.slice(0, 8).map((runError, index) => (
                          <li
                            key={`${runError.at ?? "unknown"}-${index}`}
                            className="rounded border border-red-100 bg-red-50/80 px-2 py-1 text-red-700"
                          >
                            [{recommendationErrorStageLabel(runError.stage)}]
                            {runError.symbol ? ` ${runError.symbol}` : ""}:{" "}
                            {runError.message || "unknown error"}
                          </li>
                        ))}
                      </ul>
                    </div>
                  ) : (
                    <p className="mt-1 text-slate-500">
                      {recommendationsQuery.isFetching
                        ? "Recommendation run in progress..."
                        : "No execution errors reported in the latest run."}
                    </p>
                  )}
                </div>
                {latestScannerLog ? (
                  <div className="mt-2 rounded-lg border border-slate-200 bg-slate-50 p-2 text-slate-600">
                    <p>
                      Last scanner call: {new Date(latestScannerLog.startedAt).toLocaleString()} |{" "}
                      {latestScannerLog.status.toUpperCase()} | {latestScannerLog.durationMs}ms
                    </p>
                    {latestScannerLog.status === "error" && latestScannerLog.errorMessage ? (
                      <p className="mt-1 text-red-700">{latestScannerLog.errorMessage}</p>
                    ) : null}
                  </div>
                ) : (
                  <p className="mt-2 text-slate-500">No scanner API log entries yet.</p>
                )}
              </div>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Rank</TableHead>
                    <TableHead>Symbol</TableHead>
                    <TableHead>Signal</TableHead>
                    <TableHead>Actionable</TableHead>
                    <TableHead>Coverage</TableHead>
                    <TableHead>Composite</TableHead>
                    <TableHead>P(Up)</TableHead>
                    <TableHead>P(Down)</TableHead>
                    <TableHead>Spread</TableHead>
                    <TableHead className="text-right">Order</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {recommendations.map((entry) => {
                    const key = `${entry.symbol}-${entry.rank}`;
                    const availableIndicators = Number(entry.evidence?.indicatorCoverage?.available ?? 0);
                    const totalIndicators = Number(entry.evidence?.indicatorCoverage?.total ?? 0);
                    const dataGatePassed = Boolean(entry.evidence?.dataQuality?.passed);
                    const symbolProposing = isSymbolProposing(entry.symbol);

                    return (
                      <TableRow
                        key={key}
                        className="cursor-pointer hover:bg-slate-50/80"
                        onClick={() => openRecommendationDetails(entry)}
                      >
                        <TableCell>{entry.rank}</TableCell>
                        <TableCell className="font-medium text-blue-700">{entry.symbol}</TableCell>
                        <TableCell>
                          <Badge variant={actionBadgeVariant(entry.suggestedAction)}>{entry.suggestedAction}</Badge>
                        </TableCell>
                        <TableCell>
                          <Badge variant={entry.actionable ? "success" : "outline"}>{entry.actionable ? "YES" : "NO"}</Badge>
                        </TableCell>
                        <TableCell>
                          <Badge variant={dataGatePassed ? "success" : "destructive"}>
                            {availableIndicators}/{totalIndicators || "?"}
                          </Badge>
                        </TableCell>
                        <TableCell>{numberFmt.format(entry.metrics.compositeScore)}</TableCell>
                        <TableCell>{entry.metrics.directionalUpProb.toFixed(3)}</TableCell>
                        <TableCell>{entry.metrics.directionalDownProb.toFixed(3)}</TableCell>
                        <TableCell>{pctFmt(entry.metrics.spreadPct)}</TableCell>
                        <TableCell className="text-right">
                          <Button
                            size="sm"
                            disabled={symbolProposing || !entry.actionable}
                            onClick={(event) => {
                              event.stopPropagation();
                              void proposeOrderForSymbol(entry.symbol);
                            }}
                          >
                            {symbolProposing && <Spinner className="h-4 w-4" />}
                            {symbolProposing ? "Creating..." : "Create Ticket"}
                          </Button>
                        </TableCell>
                      </TableRow>
                    );
                  })}
                  {recommendations.length === 0 && (
                    <TableRow>
                      <TableCell colSpan={10} className="text-center text-slate-500">
                        {!ibkrConnected
                          ? "Recommendations paused until IBKR is connected."
                          : recommendationsQuery.isLoading || recommendationsQuery.isFetching
                          ? `Running recommendation engine for Top ${persistedPolicyTopN}...`
                          : recommendationsExecution?.timeoutReason
                          ? `No recommendations returned. ${recommendationsExecution.timeoutReason}`
                          : "No recommendations yet."}
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
              <p className="mt-2 text-xs text-slate-500">
                Click any recommendation row to open full evidence details in a modal.
              </p>
              <p className="mt-3 text-xs text-slate-500">
                Actionable symbols now: {actionableRecommendations.map((entry) => entry.symbol).join(", ") || "none"}.
              </p>
            </CardContent>
          </Card>

          <Card ref={orderWorkflowRef} className="order-1">
            <CardHeader>
              <CardTitle>Order Workflow</CardTitle>
              <CardDescription>Manual approvals are required in phase one.</CardDescription>
            </CardHeader>
            <CardContent>
              <Tabs value={orderWorkflowTab} onValueChange={setOrderWorkflowTab} className="w-full">
                <TabsList>
                  <TabsTrigger value="pending">Pending</TabsTrigger>
                  <TabsTrigger value="recent">Recent</TabsTrigger>
                  <TabsTrigger value="api-logs">API Logs</TabsTrigger>
                  <TabsTrigger value="decision">Decision Output</TabsTrigger>
                  <TabsTrigger value="risk">Risk Snapshot</TabsTrigger>
                  <TabsTrigger value="acceptance-gate">Acceptance Gate</TabsTrigger>
                  <TabsTrigger value="backtest">Backtest</TabsTrigger>
                  <TabsTrigger value="raw">Candidate Snapshot</TabsTrigger>
                </TabsList>
                <TabsContent value="pending" className="mt-3">
                  <div className="overflow-auto">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>Order</TableHead>
                          <TableHead>Symbol</TableHead>
                          <TableHead>Type</TableHead>
                          <TableHead>Action</TableHead>
                          <TableHead>Side</TableHead>
                          <TableHead>Qty</TableHead>
                          <TableHead>Limit</TableHead>
                          <TableHead>Status</TableHead>
                          <TableHead className="text-right">Decision</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {pendingOrders.map((order) => (
                          <TableRow key={order.id}>
                            <TableCell className="font-mono text-xs">{order.id.slice(0, 8)}...</TableCell>
                            <TableCell>{order.symbol}</TableCell>
                            <TableCell>{order.intentType}</TableCell>
                            <TableCell>{order.action}</TableCell>
                            <TableCell>{order.side}</TableCell>
                            <TableCell>{order.quantity}</TableCell>
                            <TableCell>{numberFmt.format(order.limitPrice)}</TableCell>
                            <TableCell>{order.status}</TableCell>
                            <TableCell className="text-right">
                              <div className="flex justify-end gap-2">
                                <Button
                                  size="sm"
                                  onClick={() => approveMutation.mutate({ orderId: order.id, approve: true })}
                                >
                                  Approve
                                </Button>
                                <Button
                                  size="sm"
                                  variant="destructive"
                                  onClick={() => approveMutation.mutate({ orderId: order.id, approve: false })}
                                >
                                  Reject
                                </Button>
                              </div>
                            </TableCell>
                          </TableRow>
                        ))}
                        {pendingOrders.length === 0 && (
                          <TableRow>
                            <TableCell colSpan={9} className="text-center text-slate-500">
                              No pending orders.
                            </TableCell>
                          </TableRow>
                        )}
                      </TableBody>
                    </Table>
                  </div>
                </TabsContent>
                <TabsContent value="recent" className="mt-3">
                  <JsonBox value={recentOrders.slice(0, 10)} empty="No recent orders." />
                </TabsContent>
                <TabsContent value="api-logs" className="mt-3 space-y-3">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <p className="text-xs text-slate-500">
                      Showing latest {apiRequestLogs.length} internal/external API requests.
                    </p>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => queryClient.invalidateQueries({ queryKey: queryKeys.apiLogs })}
                    >
                      Refresh Logs
                    </Button>
                  </div>
                  <div className="overflow-auto">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>When</TableHead>
                          <TableHead>Direction</TableHead>
                          <TableHead>Provider</TableHead>
                          <TableHead>Method</TableHead>
                          <TableHead>Endpoint</TableHead>
                          <TableHead>Why</TableHead>
                          <TableHead>Status</TableHead>
                          <TableHead>Duration</TableHead>
                          <TableHead>Error</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {apiRequestLogs.map((log) => (
                          <TableRow key={log.id}>
                            <TableCell className="text-xs">{new Date(log.startedAt).toLocaleString()}</TableCell>
                            <TableCell>
                              <Badge variant={log.direction === "external" ? "warning" : "outline"}>
                                {String(log.direction).toUpperCase()}
                              </Badge>
                            </TableCell>
                            <TableCell className="text-xs">{log.provider}</TableCell>
                            <TableCell className="text-xs">{log.method}</TableCell>
                            <TableCell className="max-w-56 text-xs">{log.endpoint}</TableCell>
                            <TableCell className="max-w-72 text-xs">{log.reason}</TableCell>
                            <TableCell>
                              <Badge variant={log.status === "success" ? "success" : "destructive"}>
                                {log.status === "success"
                                  ? `OK${log.statusCode ? ` (${log.statusCode})` : ""}`
                                  : `ERROR${log.statusCode ? ` (${log.statusCode})` : ""}`}
                              </Badge>
                            </TableCell>
                            <TableCell className="text-xs">{log.durationMs}ms</TableCell>
                            <TableCell className="max-w-72 text-xs text-red-700">
                              {log.errorMessage ?? "-"}
                            </TableCell>
                          </TableRow>
                        ))}
                        {apiRequestLogs.length === 0 && (
                          <TableRow>
                            <TableCell colSpan={9} className="text-center text-slate-500">
                              {apiLogsQuery.isLoading ? "Loading API logs..." : "No API request logs yet."}
                            </TableCell>
                          </TableRow>
                        )}
                      </TableBody>
                    </Table>
                  </div>
                </TabsContent>
                <TabsContent value="decision" className="mt-3">
                  <JsonBox value={decisionResult} empty="No decision output yet." />
                </TabsContent>
                <TabsContent value="risk" className="mt-3">
                  <JsonBox value={riskQuery.data?.riskState} empty="Risk data unavailable." />
                </TabsContent>
                <TabsContent value="acceptance-gate" className="mt-3 space-y-3">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <p className="text-xs text-slate-500">
                      Tracks paper-run acceptance criteria for live-shadow readiness.
                    </p>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => queryClient.invalidateQueries({ queryKey: queryKeys.acceptanceGate })}
                    >
                      Refresh Gate
                    </Button>
                  </div>
                  {acceptanceGate ? (
                    <>
                      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
                        <MetricTile
                          label="Overall"
                          value={acceptanceGate.pass ? "PASS" : "IN PROGRESS"}
                          emphasis
                        />
                        <MetricTile
                          label="Observed Days"
                          value={numberFmt.format(Number(acceptanceGate.period?.observedDays ?? 0))}
                        />
                        <MetricTile
                          label="Completed Trades"
                          value={String(acceptanceGate.trading?.completedTrades ?? 0)}
                        />
                        <MetricTile
                          label="Violations"
                          value={String(acceptanceGate.violations?.count ?? 0)}
                        />
                        <MetricTile
                          label="Expectancy"
                          value={
                            acceptanceGate.trading?.expectancy === null
                              ? "n/a"
                              : signedMoney(Number(acceptanceGate.trading.expectancy))
                          }
                        />
                        <MetricTile
                          label="Profit Factor"
                          value={
                            acceptanceGate.trading?.profitFactor === null
                              ? "n/a"
                              : numberFmt.format(Number(acceptanceGate.trading.profitFactor))
                          }
                        />
                        <MetricTile
                          label="Max Drawdown"
                          value={pctFmt(Number(acceptanceGate.risk?.maxDrawdownPct ?? 0))}
                        />
                        <MetricTile
                          label="Generated"
                          value={formatTimestamp(acceptanceGate.generatedAt)}
                        />
                      </div>

                      <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
                        <div className="rounded-xl border border-slate-200 bg-white/70 p-3">
                          <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">Checks</p>
                          <div className="mt-2 space-y-2">
                            {Object.entries(ACCEPTANCE_CHECK_LABELS).map(([key, label]) => {
                              const passed = Boolean(acceptanceGate.checks?.[key]);
                              return (
                                <div key={key} className="flex items-center justify-between gap-2 text-xs">
                                  <span className="text-slate-700">{label}</span>
                                  <Badge variant={passed ? "success" : "warning"}>
                                    {passed ? "PASS" : "PENDING"}
                                  </Badge>
                                </div>
                              );
                            })}
                          </div>
                        </div>
                        <div className="rounded-xl border border-slate-200 bg-white/70 p-3">
                          <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">Notes</p>
                          {Array.isArray(acceptanceGate.notes) && acceptanceGate.notes.length > 0 ? (
                            <ul className="mt-2 space-y-1 text-xs text-slate-700">
                              {acceptanceGate.notes.map((note) => (
                                <li key={note}>{note}</li>
                              ))}
                            </ul>
                          ) : (
                            <p className="mt-2 text-xs text-slate-600">No additional notes.</p>
                          )}
                        </div>
                      </div>

                      <div className="overflow-auto rounded-xl border border-slate-200">
                        <Table>
                          <TableHeader>
                            <TableRow>
                              <TableHead>When</TableHead>
                              <TableHead>Type</TableHead>
                              <TableHead>Reasons</TableHead>
                              <TableHead>Source</TableHead>
                            </TableRow>
                          </TableHeader>
                          <TableBody>
                            {(acceptanceGate.violations?.items ?? []).slice(-25).map((item, index) => (
                              <TableRow key={`${item.timestamp}-${index}`}>
                                <TableCell className="text-xs">{formatTimestamp(item.timestamp)}</TableCell>
                                <TableCell className="text-xs">{item.type}</TableCell>
                                <TableCell className="text-xs">{(item.reasons ?? []).join(", ") || "—"}</TableCell>
                                <TableCell className="text-xs">{item.source || "unknown"}</TableCell>
                              </TableRow>
                            ))}
                            {(acceptanceGate.violations?.items ?? []).length === 0 ? (
                              <TableRow>
                                <TableCell colSpan={4} className="text-center text-slate-500">
                                  No violation records found.
                                </TableCell>
                              </TableRow>
                            ) : null}
                          </TableBody>
                        </Table>
                      </div>
                    </>
                  ) : (
                    <JsonBox
                      value={acceptanceGateQuery.data}
                      empty={acceptanceGateQuery.isLoading ? "Loading acceptance gate..." : "No acceptance gate data."}
                    />
                  )}
                </TabsContent>
                <TabsContent value="backtest" className="mt-3 space-y-3">
                  {backtestSummary ? (
                    <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
                      <MetricTile label="Trades" value={String(backtestSummary.trades)} />
                      <MetricTile label="Win Rate" value={pctFmt(backtestSummary.winRate)} />
                      <MetricTile label="Net PnL" value={signedMoney(backtestSummary.netPnl)} emphasis />
                      <MetricTile label="Expectancy" value={signedMoney(backtestSummary.expectancy)} />
                      <MetricTile label="Profit Factor" value={numberFmt.format(backtestSummary.profitFactor)} />
                      <MetricTile label="Max DD" value={pctFmt(backtestSummary.maxDrawdownPct)} />
                      <MetricTile
                        label="Gate PF>=1.2"
                        value={backtestGate?.passProfitFactor ? "PASS" : "FAIL"}
                      />
                      <MetricTile
                        label="Gate DD<=12%"
                        value={backtestGate?.passMaxDrawdown ? "PASS" : "FAIL"}
                      />
                    </div>
                  ) : null}
                  <JsonBox value={backtestResult} empty="Run backtest to view walk-forward simulation output." />
                </TabsContent>
                <TabsContent value="raw" className="mt-3">
                  <JsonBox
                    value={analyses}
                    empty="Run candidate snapshot to inspect full candidate payload."
                  />
                </TabsContent>
              </Tabs>
            </CardContent>
          </Card>
        </div>
      </div>

      <Dialog open={runTimingOpen} onOpenChange={setRunTimingOpen}>
        <DialogContent className="max-w-6xl p-0">
          <Card className="max-h-[88vh] overflow-hidden border-0 shadow-none">
            <CardHeader className="flex items-start justify-between gap-3 md:flex-row md:items-center">
              <div>
                <CardTitle className="flex items-center gap-2">
                  <Clock3 className="size-4" /> Action Schedule
                </CardTitle>
                <CardDescription>
                  Track when key actions last ran, when they can run again, and when scheduled runs trigger.
                </CardDescription>
              </div>
              <div className="flex flex-wrap gap-2">
                <Button
                  variant="outline"
                  onClick={() => queryClient.invalidateQueries({ queryKey: queryKeys.runStatus })}
                >
                  {runStatusQuery.isFetching && <Spinner className="h-4 w-4" />}
                  Refresh Schedule
                </Button>
                <Button variant="outline" onClick={() => setRunTimingOpen(false)}>
                  <X className="size-4" /> Close
                </Button>
              </div>
            </CardHeader>
            <CardContent className="max-h-[calc(88vh-110px)] space-y-3 overflow-auto">
              <div className="grid grid-cols-1 gap-3 md:grid-cols-3">
                <MetricTile
                  label="Scheduled Scan"
                  value={schedulerRuntime?.lastRunStatus?.toUpperCase() ?? "UNKNOWN"}
                />
                <MetricTile
                  label="Next Auto Scan"
                  value={formatTimestampWithCountdown(schedulerRuntime?.nextAutoRunAt)}
                />
                <MetricTile
                  label="IBKR Cooldown"
                  value={
                    ibkrRuntime?.requestCooldown?.active
                      ? `${formatDuration(ibkrRuntime.requestCooldown.remainingMs)} (${ibkrRuntime.requestCooldown.reason || "cooldown"})`
                      : "inactive"
                  }
                />
              </div>
              <div className="overflow-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Action</TableHead>
                      <TableHead>Frequency / Trigger</TableHead>
                      <TableHead>Last Attempt</TableHead>
                      <TableHead>Last Success</TableHead>
                      <TableHead>Last Error</TableHead>
                      <TableHead>Next Available</TableHead>
                      <TableHead>Next Auto</TableHead>
                      <TableHead>Note</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {runActions.map((item) => (
                      <TableRow key={item.id}>
                        <TableCell className="font-medium">{item.label}</TableCell>
                        <TableCell className="max-w-80 text-xs text-slate-600">
                          {renderScheduleFrequency(item)}
                        </TableCell>
                        <TableCell className="text-xs">{formatTimestamp(item.lastAttemptAt)}</TableCell>
                        <TableCell className="text-xs">{formatTimestamp(item.lastSuccessAt)}</TableCell>
                        <TableCell className="text-xs">{formatTimestamp(item.lastErrorAt)}</TableCell>
                        <TableCell className="text-xs">
                          {formatTimestampWithCountdown(item.nextAvailableAt)}
                        </TableCell>
                        <TableCell className="text-xs">
                          {formatTimestampWithCountdown(item.nextAutoRunAt)}
                        </TableCell>
                        <TableCell className="max-w-56 text-xs text-slate-600">
                          {item.note || "—"}
                        </TableCell>
                      </TableRow>
                    ))}
                    {runActions.length === 0 && (
                      <TableRow>
                        <TableCell colSpan={8} className="text-center text-slate-500">
                          {runStatusQuery.isLoading ? "Loading run timing..." : "No run timing data yet."}
                        </TableCell>
                      </TableRow>
                    )}
                  </TableBody>
                </Table>
              </div>
            </CardContent>
          </Card>
        </DialogContent>
      </Dialog>

      <Dialog open={positionsOpen} onOpenChange={setPositionsOpen}>
        <DialogContent className="max-w-6xl p-0">
          <Card className="max-h-[88vh] overflow-hidden border-0 shadow-none">
              <CardHeader className="flex items-start justify-between gap-3 md:flex-row md:items-center">
                <div>
                  <CardTitle>Positions ({positionsButtonCount})</CardTitle>
                  <CardDescription>
                    Broker and local position ledger with open/partial/closed state plus realized and unrealized PnL.
                  </CardDescription>
                </div>
                <Button variant="outline" onClick={() => setPositionsOpen(false)}>
                  <X className="size-4" /> Close
                </Button>
              </CardHeader>
              <CardContent className="max-h-[calc(88vh-110px)] space-y-3 overflow-auto">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <div className="space-y-0.5">
                    <p className="text-xs text-slate-500">
                      Source: {positionsQuery.data?.broker?.source ?? "unavailable"} | Account:{" "}
                      {positionsQuery.data?.broker?.accountCode ?? "n/a"} | Link:{" "}
                      {positionsQuery.data?.connectivity?.reachable ? "connected" : "disconnected"}
                    </p>
                    <p className="text-xs text-slate-500">
                      Last fetched:{" "}
                      {positionsQuery.dataUpdatedAt
                        ? new Date(positionsQuery.dataUpdatedAt).toLocaleString()
                        : "—"}
                      {positionsQuery.isFetching ? " (refreshing...)" : ""} | Payload:{" "}
                      {positionsQuery.data?.generatedAt
                        ? new Date(positionsQuery.data.generatedAt).toLocaleString()
                        : "—"}
                    </p>
                  </div>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => {
                      queryClient.invalidateQueries({ queryKey: queryKeys.positions });
                      queryClient.invalidateQueries({ queryKey: queryKeys.accountSummary });
                      queryClient.invalidateQueries({ queryKey: queryKeys.ibkrStatus });
                      queryClient.invalidateQueries({ queryKey: queryKeys.marketDataDiagnostics });
                    }}
                  >
                    {positionsQuery.isFetching && <Spinner className="h-4 w-4" />}
                    Refresh Positions
                  </Button>
                </div>

                <div className="grid grid-cols-2 gap-3 md:grid-cols-8">
                  <MetricTile
                    label="Broker Open"
                    value={String(positionsSummary?.brokerOpenPositions ?? brokerPositions.length)}
                  />
                  <MetricTile
                    label="Broker Closed"
                    value={String(positionsSummary?.brokerClosedPositions ?? 0)}
                  />
                  <MetricTile
                    label="Broker Linked"
                    value={String(positionsSummary?.brokerLinkedToBotPositions ?? 0)}
                  />
                  <MetricTile
                    label="Local Open"
                    value={String(positionsSummary?.localOpenPositions ?? localPositions.length)}
                  />
                  <MetricTile
                    label="Local Closed"
                    value={String(positionsSummary?.localClosedPositions ?? 0)}
                  />
                  <MetricTile
                    label="Broker Unrealized"
                    value={signedMoney(positionsSummary?.brokerNetUnrealizedPnl ?? 0)}
                    emphasis
                  />
                  <MetricTile
                    label="Broker Realized"
                    value={signedMoney(positionsSummary?.brokerNetRealizedPnl ?? 0)}
                    emphasis
                  />
                  <MetricTile
                    label="Local Est. Unrealized"
                    value={signedMoney(positionsSummary?.localEstimatedUnrealizedPnl ?? 0)}
                    emphasis
                  />
                  <MetricTile
                    label="Local Realized"
                    value={signedMoney(positionsSummary?.localRealizedPnl ?? 0)}
                    emphasis
                  />
                </div>

                <div className="rounded-xl border border-slate-200 bg-white/70 p-3">
                  <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
                    <div className="flex flex-wrap items-center gap-2">
                      <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                        Broker Positions ({brokerPositionsFiltered.length}/{brokerPositions.length})
                      </p>
                      <div className="flex items-center gap-1">
                        <span className="text-xs text-slate-500">State</span>
                        <Select
                          name="broker_position_state_filter"
                          items={POSITION_STATE_FILTER_OPTIONS}
                          value={brokerPositionStateFilter}
                          onValueChange={(value) =>
                            setBrokerPositionStateFilter(
                              typeof value === "string" && value.trim().length > 0
                                ? value
                                : "ALL"
                            )
                          }
                        >
                          <SelectTrigger
                            id="broker-position-state-filter"
                            aria-label="Broker position state filter"
                            className="h-8 min-w-44 bg-white text-xs"
                          >
                            <SelectValue />
                          </SelectTrigger>
                          <SelectPopup>
                            {POSITION_STATE_FILTER_OPTIONS.map((option) => (
                              <SelectItem key={option.value} value={option.value}>
                                {option.label}
                              </SelectItem>
                            ))}
                          </SelectPopup>
                        </Select>
                      </div>
                    </div>
                    <PositionsPagination
                      total={brokerPositionsFiltered.length}
                      page={brokerPositionsActivePage}
                      pageSize={brokerPositionsPageSize}
                      totalPages={brokerPositionsTotalPages}
                      onPageChange={(nextPage) =>
                        setBrokerPositionsPage(Math.max(1, Math.min(nextPage, brokerPositionsTotalPages)))
                      }
                      onPageSizeChange={(nextPageSize) => {
                        const safePageSize = Number.isFinite(nextPageSize) && nextPageSize > 0 ? nextPageSize : 25;
                        setBrokerPositionsPageSize(safePageSize);
                        setBrokerPositionsPage(1);
                      }}
                    />
                  </div>
                  <div className="overflow-auto">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>Symbol</TableHead>
                          <TableHead>Type</TableHead>
                          <TableHead>Contract</TableHead>
                          <TableHead>Strike / Underlying</TableHead>
                          <TableHead>Qty</TableHead>
                          <TableHead>Avg Cost</TableHead>
                          <TableHead>Total Cost</TableHead>
                          <TableHead>Mark</TableHead>
                          <TableHead>Mkt Value</TableHead>
                          <TableHead>Unrealized</TableHead>
                          <TableHead>Realized</TableHead>
                          <TableHead>State</TableHead>
                          <TableHead>Staleness</TableHead>
                          <TableHead>Origin</TableHead>
                          <TableHead>Recommendation Link</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {brokerPositionsPageRows.map((position) => {
                          const marketDataUpdatedAtMs = position.marketDataUpdatedAt
                            ? new Date(position.marketDataUpdatedAt).getTime()
                            : NaN;
                          const marketDataStalenessMs = Number.isFinite(marketDataUpdatedAtMs)
                            ? Math.max(0, positionsClockMs - marketDataUpdatedAtMs)
                            : typeof position.marketDataStalenessMs === "number"
                              ? position.marketDataStalenessMs
                              : null;

                          return (
                          <TableRow key={`${position.conId}:${position.symbol}`}>
                            <TableCell className="font-medium">{position.symbol}</TableCell>
                            <TableCell>{position.secType}</TableCell>
                            <TableCell className="text-xs">
                              {position.secType === "OPT"
                                ? contractLabel(position.expiration, position.strike, position.right)
                                : "-"}
                            </TableCell>
                            <TableCell className="text-xs">
                              {position.secType === "OPT" ? (
                                <div className="space-y-0.5">
                                  <p>Strike {position.strike !== null ? numberFmt.format(position.strike) : "-"}</p>
                                  <p className="text-slate-600">
                                    Underlying{" "}
                                    {typeof position.underlying?.last === "number"
                                      ? currencyFmt.format(position.underlying.last)
                                      : "-"}
                                  </p>
                                </div>
                              ) : (
                                "-"
                              )}
                            </TableCell>
                            <TableCell>{numberFmt.format(position.quantity)}</TableCell>
                            <TableCell>{currencyFmt.format(position.avgCost ?? 0)}</TableCell>
                            <TableCell className="text-xs">
                              {typeof position.costBreakdown?.totalCostInclFees === "number"
                                ? currencyFmt.format(position.costBreakdown.totalCostInclFees)
                                : "-"}
                              {position.costBreakdown?.feeStatus === "unavailable" ? (
                                <p className="text-[10px] text-slate-500">fees unavailable</p>
                              ) : null}
                            </TableCell>
                            <TableCell>
                              {typeof position.marketPrice === "number"
                                ? currencyFmt.format(position.marketPrice)
                                : "-"}
                            </TableCell>
                            <TableCell>
                              {typeof position.marketValue === "number" ? signedMoney(position.marketValue) : "-"}
                            </TableCell>
                            <TableCell
                              className={
                                typeof position.unrealizedPnl === "number"
                                  ? position.unrealizedPnl >= 0
                                    ? "text-emerald-700"
                                    : "text-red-700"
                                  : ""
                              }
                            >
                              {typeof position.unrealizedPnl === "number"
                                ? signedMoney(position.unrealizedPnl)
                                : "-"}
                            </TableCell>
                            <TableCell
                              className={
                                typeof position.realizedPnl === "number"
                                  ? position.realizedPnl >= 0
                                    ? "text-emerald-700"
                                    : "text-red-700"
                                  : ""
                              }
                            >
                              {typeof position.realizedPnl === "number"
                                ? signedMoney(position.realizedPnl)
                                : "-"}
                            </TableCell>
                            <TableCell>
                              <Badge variant={positionLifecycleBadgeVariant(position.lifecycleState)}>
                                {formatLifecycleLabel(position.lifecycleState)}
                              </Badge>
                            </TableCell>
                            <TableCell className="text-xs">
                              {typeof marketDataStalenessMs === "number" ? (
                                <Badge
                                  variant={
                                    marketDataStalenessMs >= 120_000
                                      ? "destructive"
                                      : marketDataStalenessMs >= 30_000
                                        ? "warning"
                                        : "success"
                                  }
                                >
                                  {formatStaleness(marketDataStalenessMs)}
                                </Badge>
                              ) : (
                                <span className="text-slate-400">unknown</span>
                              )}
                              {Number.isFinite(marketDataUpdatedAtMs) ? (
                                <p className="text-[10px] text-slate-500">
                                  tick {new Date(marketDataUpdatedAtMs).toLocaleTimeString()}
                                </p>
                              ) : null}
                            </TableCell>
                            <TableCell>
                              <Badge
                                variant={
                                  position.attribution?.origin === "BOT_RECOMMENDATION"
                                    ? "success"
                                    : "outline"
                                }
                              >
                                {position.attribution?.origin === "BOT_RECOMMENDATION"
                                  ? "BOT-LINKED"
                                  : "EXTERNAL/UNKNOWN"}
                              </Badge>
                            </TableCell>
                            <TableCell className="max-w-64 text-xs">
                              {position.attribution?.linkedOrderId ? (
                                <div className="space-y-0.5">
                                  <p className="font-mono text-[11px]">
                                    {position.attribution.linkedOrderId.slice(0, 8)}...
                                  </p>
                                  <p className="text-slate-500">
                                    {position.attribution.recommendation?.action ?? "-"} · score{" "}
                                    {typeof position.attribution.recommendation?.compositeScore === "number"
                                      ? numberFmt.format(position.attribution.recommendation.compositeScore)
                                      : "-"}
                                  </p>
                                </div>
                              ) : (
                                <span className="text-slate-400">—</span>
                              )}
                            </TableCell>
                          </TableRow>
                          );
                        })}
                        {brokerPositionsFiltered.length === 0 && (
                          <TableRow>
                            <TableCell colSpan={15} className="text-center text-slate-500">
                              {positionsQuery.isLoading
                                ? "Loading broker positions..."
                                : brokerPositions.length === 0
                                  ? "No broker positions returned."
                                  : "No broker positions match the selected state filter."}
                            </TableCell>
                          </TableRow>
                        )}
                      </TableBody>
                    </Table>
                  </div>
                </div>

                <div className="rounded-xl border border-slate-200 bg-white/70 p-3">
                  <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
                    <div>
                      <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                        Local Position Ledger ({localPositionsFiltered.length}/{localPositions.length})
                      </p>
                      <p className="text-xs text-slate-500">
                        Marked open positions: {positionsSummary?.localMarkedPositions ?? 0} /{" "}
                        {positionsSummary?.localOpenPositions ?? localPositions.length}
                      </p>
                    </div>
                    <div className="flex items-center gap-1">
                      <span className="text-xs text-slate-500">State</span>
                      <Select
                        name="local_position_state_filter"
                        items={POSITION_STATE_FILTER_OPTIONS}
                        value={localPositionStateFilter}
                        onValueChange={(value) =>
                          setLocalPositionStateFilter(
                            typeof value === "string" && value.trim().length > 0
                              ? value
                              : "ALL"
                          )
                        }
                      >
                        <SelectTrigger
                          id="local-position-state-filter"
                          aria-label="Local position state filter"
                          className="h-8 min-w-44 bg-white text-xs"
                        >
                          <SelectValue />
                        </SelectTrigger>
                        <SelectPopup>
                          {POSITION_STATE_FILTER_OPTIONS.map((option) => (
                            <SelectItem key={option.value} value={option.value}>
                              {option.label}
                            </SelectItem>
                          ))}
                        </SelectPopup>
                      </Select>
                    </div>
                    <PositionsPagination
                      total={localPositionsFiltered.length}
                      page={localPositionsActivePage}
                      pageSize={localPositionsPageSize}
                      totalPages={localPositionsTotalPages}
                      onPageChange={(nextPage) =>
                        setLocalPositionsPage(Math.max(1, Math.min(nextPage, localPositionsTotalPages)))
                      }
                      onPageSizeChange={(nextPageSize) => {
                        const safePageSize = Number.isFinite(nextPageSize) && nextPageSize > 0 ? nextPageSize : 25;
                        setLocalPositionsPageSize(safePageSize);
                        setLocalPositionsPage(1);
                      }}
                    />
                  </div>
                  <div className="overflow-auto">
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>Order</TableHead>
                          <TableHead>Symbol</TableHead>
                          <TableHead>Contract</TableHead>
                          <TableHead>Status</TableHead>
                          <TableHead>State</TableHead>
                          <TableHead>Qty</TableHead>
                          <TableHead>Remaining</TableHead>
                          <TableHead>Entry</TableHead>
                          <TableHead>Mark</TableHead>
                          <TableHead>Unrealized</TableHead>
                          <TableHead>Realized</TableHead>
                          <TableHead>Return</TableHead>
                          <TableHead>Age</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {localPositionsPageRows.map((position) => (
                          <TableRow key={position.orderId}>
                            <TableCell className="font-mono text-xs">{position.orderId.slice(0, 8)}...</TableCell>
                            <TableCell className="font-medium">{position.symbol}</TableCell>
                            <TableCell className="text-xs">
                              {contractLabel(position.expiration, position.strike, position.right)}
                            </TableCell>
                            <TableCell>{position.status}</TableCell>
                            <TableCell>
                              <Badge variant={positionLifecycleBadgeVariant(position.lifecycleState)}>
                                {formatLifecycleLabel(position.lifecycleState)}
                              </Badge>
                            </TableCell>
                            <TableCell>{position.entryFilledQuantity}</TableCell>
                            <TableCell>{position.remainingQuantity}</TableCell>
                            <TableCell>{currencyFmt.format(position.entryPrice ?? 0)}</TableCell>
                            <TableCell>
                              {typeof position.markPrice === "number" ? currencyFmt.format(position.markPrice) : "-"}
                            </TableCell>
                            <TableCell
                              className={
                                typeof position.estimatedUnrealizedPnl === "number"
                                  ? position.estimatedUnrealizedPnl >= 0
                                    ? "text-emerald-700"
                                    : "text-red-700"
                                  : ""
                              }
                            >
                              {typeof position.estimatedUnrealizedPnl === "number"
                                ? signedMoney(position.estimatedUnrealizedPnl)
                                : "-"}
                            </TableCell>
                            <TableCell
                              className={
                                typeof position.realizedPnl === "number"
                                  ? position.realizedPnl >= 0
                                    ? "text-emerald-700"
                                    : "text-red-700"
                                  : ""
                              }
                            >
                              {typeof position.realizedPnl === "number"
                                ? signedMoney(position.realizedPnl)
                                : "-"}
                            </TableCell>
                            <TableCell>
                              {typeof position.estimatedReturnPct === "number"
                                ? pctFmt(position.estimatedReturnPct)
                                : "-"}
                            </TableCell>
                            <TableCell>
                              {typeof position.daysOpen === "number" ? `${position.daysOpen.toFixed(2)}d` : "-"}
                            </TableCell>
                          </TableRow>
                        ))}
                        {localPositionsFiltered.length === 0 && (
                          <TableRow>
                            <TableCell colSpan={13} className="text-center text-slate-500">
                              {localPositions.length === 0
                                ? "No local position history tracked yet."
                                : "No local positions match the selected state filter."}
                            </TableCell>
                          </TableRow>
                        )}
                      </TableBody>
                    </Table>
                  </div>
                </div>
              </CardContent>
          </Card>
        </DialogContent>
      </Dialog>

      <Dialog open={settingsOpen} onOpenChange={setSettingsOpen}>
        <DialogContent className="max-w-5xl p-0">
          <Card className="max-h-[88vh] overflow-hidden border-0 shadow-none">
              <CardHeader className="flex items-start justify-between gap-3 md:flex-row md:items-center">
                <div>
                  <CardTitle>Risk Guidelines + Harness Settings</CardTitle>
                  <CardDescription>
                    Tune thresholds, risk limits, and universe. Changes apply to next scan/decision/proposal cycle.
                  </CardDescription>
                </div>
                <Button variant="outline" onClick={() => setSettingsOpen(false)}>
                  <X className="size-4" /> Close
                </Button>
              </CardHeader>
              <CardContent className="max-h-[calc(88vh-110px)] overflow-auto">
                <Tabs value={settingsTab} onValueChange={setSettingsTab} className="w-full">
                  <TabsList>
                    <TabsTrigger value="policy">Policy</TabsTrigger>
                    <TabsTrigger value="environment">Environment</TabsTrigger>
                  </TabsList>

                  <TabsContent value="policy" className="mt-4 space-y-4">
                    <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
                      {POLICY_SECTIONS.map((section) => (
                        <div key={section.title} className="rounded-xl border border-slate-200 p-3">
                          <h3 className="mb-3 font-semibold text-slate-900">{section.title}</h3>
                          <div className="space-y-3">
                            {section.fields.map((field) => {
                              const guideline = policyQuery.data?.guidelines?.[field] ?? {};
                              const policyInputId = toDomId("policy", field);
                              const inputMin =
                                guideline.min !== undefined && guideline.min !== null
                                  ? isPolicyPercentField(field)
                                    ? Number(guideline.min) * 100
                                    : guideline.min
                                  : undefined;
                              const inputMax =
                                guideline.max !== undefined && guideline.max !== null
                                  ? isPolicyPercentField(field)
                                    ? Number(guideline.max) * 100
                                    : guideline.max
                                  : undefined;
                              return (
                                <label key={field} htmlFor={policyInputId} className="block space-y-1">
                                  <span className="text-sm font-medium text-slate-800">{guideline.label ?? field}</span>
                                  <Input
                                    id={policyInputId}
                                    name={field}
                                    nativeInput
                                    type="number"
                                    step={POLICY_FIELD_META[field]?.step ?? "0.01"}
                                    min={inputMin}
                                    max={inputMax}
                                    value={policyForm[field] ?? ""}
                                    onChange={(event) =>
                                      setPolicyForm((previous) => ({
                                        ...previous,
                                        [field]: event.target.value
                                      }))
                                    }
                                  />
                                  <span className="text-xs text-slate-500">
                                    {policyDescription(field, guideline)}
                                    {guideline.min !== undefined || guideline.max !== undefined
                                      ? ` Range: ${formatGuidelineRange(field, guideline.min)} to ${formatGuidelineRange(field, guideline.max)}.`
                                      : ""}
                                  </span>
                                </label>
                              );
                            })}
                          </div>
                        </div>
                      ))}
                    </div>

                    <label className="block space-y-1">
                      <span className="text-sm font-medium text-slate-800">Screener Type</span>
                      <Select
                        name="policy_ibkr_scan_code"
                        items={SCREENER_MODE_OPTIONS}
                        value={policyForm.ibkrScanCode ?? persistedPolicyScanCode}
                        onValueChange={(value) => {
                          if (typeof value !== "string" || value.trim().length === 0) return;
                          setPolicyForm((previous) => {
                            if (previous.ibkrScanCode === value) return previous;
                            return {
                              ...previous,
                              ibkrScanCode: value
                            };
                          });
                        }}
                      >
                        <SelectTrigger id="policy-ibkr-scan-code" aria-label="Policy screener type">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectPopup>
                          {SCREENER_MODE_OPTIONS.map((option) => (
                            <SelectItem key={option.value} value={option.value}>
                              {option.label} ({option.value})
                            </SelectItem>
                          ))}
                        </SelectPopup>
                      </Select>
                      <span className="text-xs text-slate-500">
                        Applies a provider-specific screener mode mapping (IBKR + external providers like Alpaca).
                      </span>
                    </label>

                    <label className="block space-y-1">
                      <span className="text-sm font-medium text-slate-800">Analysis Data Provider</span>
                      <Select
                        name="policy_analysis_data_provider"
                        items={ANALYSIS_PROVIDER_OPTIONS}
                        value={policyForm.analysisDataProvider ?? persistedAnalysisDataProvider}
                        onValueChange={(value) => {
                          if (typeof value !== "string" || value.trim().length === 0) return;
                          setPolicyForm((previous) => {
                            if (previous.analysisDataProvider === value) return previous;
                            return {
                              ...previous,
                              analysisDataProvider: value
                            };
                          });
                        }}
                      >
                        <SelectTrigger id="policy-analysis-provider" aria-label="Policy analysis provider">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectPopup>
                          {ANALYSIS_PROVIDER_OPTIONS.map((option) => (
                            <SelectItem key={option.value} value={option.value}>
                              {option.label}
                            </SelectItem>
                          ))}
                        </SelectPopup>
                      </Select>
                      <span className="text-xs text-slate-500">
                        Controls quote/history/options analysis source. Alpaca mode bypasses IBKR analysis requests.
                      </span>
                    </label>

                    <label className="block space-y-1">
                      <span className="text-sm font-medium text-slate-800">Universe Symbols (comma-separated)</span>
                      <Input
                        id="policy-universe-symbols"
                        name="universe_symbols"
                        value={universeInput}
                        onChange={(event) => setUniverseInput(event.target.value)}
                        placeholder="SPY, QQQ, IWM, AAPL, MSFT"
                      />
                      <span className="text-xs text-slate-500">
                        These symbols feed the scanner and recommendation engine.
                      </span>
                    </label>

                    <div className="flex flex-wrap gap-2">
                      <Button
                        disabled={updatePolicyMutation.isPending || resetPolicyMutation.isPending}
                        onClick={handleSavePolicy}
                      >
                        {updatePolicyMutation.isPending && <Spinner className="h-4 w-4" />}
                        {updatePolicyMutation.isPending ? "Saving Policy..." : "Save Policy"}
                      </Button>
                      <Button
                        variant="outline"
                        disabled={updatePolicyMutation.isPending || resetPolicyMutation.isPending}
                        onClick={() => resetPolicyMutation.mutate()}
                      >
                        {resetPolicyMutation.isPending && <Spinner className="h-4 w-4" />}
                        {resetPolicyMutation.isPending ? "Resetting..." : "Reset Defaults"}
                      </Button>
                    </div>
                  </TabsContent>

                  <TabsContent value="environment" className="mt-4 space-y-4">
                    {envConfigQuery.isLoading && (
                      <div className="flex items-center gap-2 rounded-xl border border-slate-200 bg-white/70 p-3 text-sm text-slate-700">
                        <Spinner className="h-4 w-4" />
                        Loading environment settings...
                      </div>
                    )}

                    {envConfigQuery.isError && (
                      <div className="rounded-xl border border-red-200 bg-red-50 p-3 text-sm text-red-700">
                        Failed to load env configuration: {String(envConfigQuery.error)}
                      </div>
                    )}

                    {!envConfigQuery.isLoading && !envConfigQuery.isError && (
                      <>
                        <div className="rounded-xl border border-slate-200 bg-white/70 p-3 text-xs text-slate-600">
                          Edit `.env` values directly from the UI. Save applies live where supported, and marks keys that still require restart.
                        </div>

                        {Object.entries(envFieldsByCategory).map(([category, fields]) => (
                          <div key={category} className="rounded-xl border border-slate-200 p-3">
                            <h3 className="mb-3 font-semibold text-slate-900">{category}</h3>
                            <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
                              {fields.map((field) => {
                                const envInputId = toDomId("env", field.key);
                                const setEnvFieldValue = (nextValue) => {
                                  setEnvForm((previous) => ({
                                    ...previous,
                                    [field.key]: nextValue
                                  }));
                                  setEnvTouchedKeys((previous) => ({
                                    ...previous,
                                    [field.key]: true
                                  }));
                                };

                                return (
                                  <label key={field.key} htmlFor={envInputId} className="block space-y-1">
                                    <div className="flex flex-wrap items-center gap-2">
                                      <span className="text-sm font-medium text-slate-800">{field.label}</span>
                                      <Badge variant={field.liveApply ? "success" : "warning"}>
                                        {field.liveApply ? "Live apply" : "Restart required"}
                                      </Badge>
                                    </div>
                                    <Input
                                      id={envInputId}
                                      name={field.key}
                                      nativeInput
                                      type={
                                        field.type === "number"
                                          ? "number"
                                          : field.type === "secret"
                                            ? "password"
                                            : "text"
                                      }
                                      value={envForm[field.key] ?? ""}
                                      onChange={(event) => {
                                        const nextValue = event.target.value;
                                        setEnvFieldValue(nextValue);
                                      }}
                                      placeholder={field.placeholder ?? ""}
                                    />
                                    <span className="text-xs text-slate-500">{field.description}</span>
                                    <span className="font-mono text-[11px] text-slate-400">{field.key}</span>
                                  </label>
                                );
                              })}
                            </div>
                          </div>
                        ))}

                        <div className="flex flex-wrap gap-2">
                          <Button
                            disabled={updateEnvMutation.isPending || envChangedKeys.length === 0}
                            onClick={handleSaveEnv}
                          >
                            {updateEnvMutation.isPending && <Spinner className="h-4 w-4" />}
                            {updateEnvMutation.isPending ? "Saving Env..." : "Save Env"}
                          </Button>
                          <Button
                            variant="outline"
                            disabled={refreshRuntimeMutation.isPending}
                            onClick={() => refreshRuntimeMutation.mutate()}
                          >
                            {refreshRuntimeMutation.isPending && <Spinner className="h-4 w-4" />}
                            {refreshRuntimeMutation.isPending ? "Refreshing..." : "Refresh Runtime"}
                          </Button>
                          <Button
                            variant="destructive"
                            disabled={restartAppMutation.isPending}
                            onClick={() => restartAppMutation.mutate()}
                          >
                            {restartAppMutation.isPending && <Spinner className="h-4 w-4" />}
                            {restartAppMutation.isPending ? "Restarting..." : "Restart App"}
                          </Button>
                        </div>

                        <p className="text-xs text-slate-500">
                          Env file: {envConfigQuery.data?.envPath ?? "-"} | Changed keys:{" "}
                          {envChangedKeys.join(", ") || "none"}
                        </p>
                      </>
                    )}
                  </TabsContent>
                </Tabs>
              </CardContent>
          </Card>
        </DialogContent>
      </Dialog>
    </ToastProvider>
  );
};
