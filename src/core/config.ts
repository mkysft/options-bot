import { mkdirSync } from "node:fs";
import { dirname } from "node:path";

const parseBool = (value: string | undefined, fallback: boolean): boolean => {
  if (value === undefined) return fallback;
  return ["1", "true", "yes", "y", "on"].includes(value.toLowerCase());
};

const parseNumber = (value: string | undefined, fallback: number): number => {
  if (value === undefined) return fallback;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};

const parseCsv = (value: string | undefined, fallback: string[]): string[] => {
  if (!value) return fallback;
  return value
    .split(",")
    .map((v) => v.trim().toUpperCase())
    .filter(Boolean);
};

const parseScannerProviders = (
  value: string | undefined,
  fallback: string[]
): string[] => {
  const allowed = new Set(["ibkr", "fmp", "eodhd", "alpaca", "alpha_vantage", "ai_discovery"]);
  const raw = !value
    ? fallback
    : value
        .split(",")
        .map((provider) => provider.trim().toLowerCase())
        .filter(Boolean);
  const filtered = raw.filter((provider) => allowed.has(provider));
  return [...new Set(filtered)];
};

const parseLaunchTarget = (value: string | undefined): "gateway" | "tws" => {
  if (!value) return "gateway";
  return value.toLowerCase() === "tws" ? "tws" : "gateway";
};

const parsePortList = (value: string | undefined): number[] => {
  if (!value) return [];
  return value
    .split(",")
    .map((item) => Number(item.trim()))
    .filter((port) => Number.isInteger(port) && port > 0 && port <= 65535);
};

const isTestRuntime = (env: NodeJS.ProcessEnv): boolean => {
  const appEnv = (env.APP_ENV ?? "").toLowerCase();
  const nodeEnv = (env.NODE_ENV ?? "").toLowerCase();
  const bunTest = (env.BUN_TEST ?? "").toLowerCase();
  return appEnv === "test" || nodeEnv === "test" || bunTest === "1" || bunTest === "true";
};

const defaultUniverse = [
  "SPY",
  "QQQ",
  "IWM",
  "DIA",
  "XLK",
  "XLF",
  "XLE",
  "XLI",
  "SMH",
  "AAPL",
  "MSFT",
  "NVDA",
  "AMZN",
  "META",
  "GOOGL",
  "TSLA",
  "JPM",
  "UNH",
  "AVGO"
];

const buildSettings = (env: NodeJS.ProcessEnv) => {
  const ibkrClientTimeoutMs = parseNumber(env.IBKR_CLIENT_TIMEOUT_MS, 6_000);
  const ibkrScannerTimeoutMs = parseNumber(
    env.IBKR_SCANNER_TIMEOUT_MS,
    Math.max(10_000, Math.round(ibkrClientTimeoutMs * 1.8))
  );

  return {
    appName: env.APP_NAME ?? "options-bot",
    appHost: env.APP_HOST ?? "127.0.0.1",
    appPort: parseNumber(env.APP_PORT, 8000),
    appEnv: (env.APP_ENV ?? "dev") as "dev" | "test" | "prod",
    timezone: env.TIMEZONE ?? "America/Jamaica",

    paperMode: parseBool(env.PAPER_MODE, true),
    manualApprovalRequired: parseBool(env.MANUAL_APPROVAL_REQUIRED, true),
    scanCadenceMinutes: parseNumber(env.SCAN_CADENCE_MINUTES, 15),

    dbPath: env.DB_PATH ?? (isTestRuntime(env) ? "./data/options_bot.test.sqlite" : "./data/options_bot.sqlite"),
    jsonlAuditPath: env.JSONL_AUDIT_PATH ?? (isTestRuntime(env) ? "./data/audit.test.jsonl" : "./data/audit.jsonl"),

    ibkrEnabled: parseBool(env.IBKR_ENABLED, false),
    ibkrHost: env.IBKR_HOST ?? "127.0.0.1",
    ibkrPort: parseNumber(env.IBKR_PORT, 7497),
    ibkrPortCandidates: parsePortList(env.IBKR_PORT_CANDIDATES),
    ibkrClientId: parseNumber(env.IBKR_CLIENT_ID, 137),
    ibkrClientTimeoutMs,
    ibkrScannerTimeoutMs,
    ibkrQueueGlobalMinIntervalMs: parseNumber(env.IBKR_QUEUE_GLOBAL_MIN_INTERVAL_MS, 25),
    ibkrQueueMaxConcurrent: parseNumber(env.IBKR_QUEUE_MAX_CONCURRENT, 3),
    ibkrQueueQuoteIntervalMs: parseNumber(env.IBKR_QUEUE_QUOTE_INTERVAL_MS, 30),
    ibkrQueueOptionChainIntervalMs: parseNumber(env.IBKR_QUEUE_OPTION_CHAIN_INTERVAL_MS, 50),
    ibkrQueueHistoricalIntervalMs: parseNumber(env.IBKR_QUEUE_HISTORICAL_INTERVAL_MS, 120),
    ibkrQueueScannerIntervalMs: parseNumber(env.IBKR_QUEUE_SCANNER_INTERVAL_MS, 120),
    ibkrRetryMaxAttempts: parseNumber(env.IBKR_RETRY_MAX_ATTEMPTS, 2),
    ibkrRetryBaseDelayMs: parseNumber(env.IBKR_RETRY_BASE_DELAY_MS, 180),
    ibkrRetryMaxDelayMs: parseNumber(env.IBKR_RETRY_MAX_DELAY_MS, 1_200),
    ibkrLaunchTarget: parseLaunchTarget(env.IBKR_LAUNCH_TARGET),
    ibkrGatewayAppName: env.IBKR_GATEWAY_APP_NAME ?? "IB Gateway",
    ibkrTwsAppName: env.IBKR_TWS_APP_NAME ?? "Trader Workstation",
    ibkrGatewayExecPath: env.IBKR_GATEWAY_EXEC_PATH ?? "",
    ibkrTwsExecPath: env.IBKR_TWS_EXEC_PATH ?? "",
    ibkrLaunchDryRun: parseBool(env.IBKR_LAUNCH_DRY_RUN, false),

    alphaVantageApiKey: env.ALPHA_VANTAGE_API_KEY ?? "",
    aiDiscoveryEnabled: parseBool(env.AI_DISCOVERY_ENABLED, true),
    aiDiscoveryCacheTtlMinutes: parseNumber(env.AI_DISCOVERY_CACHE_TTL_MINUTES, 10),
    fmpApiKey: env.FMP_API_KEY ?? "",
    fmpBaseUrl: env.FMP_BASE_URL ?? "https://financialmodelingprep.com",
    eodhdApiKey: env.EODHD_API_KEY ?? "",
    eodhdBaseUrl: env.EODHD_BASE_URL ?? "https://eodhd.com/api/screener",
    alpacaApiKey: env.ALPACA_API_KEY ?? "",
    alpacaApiSecret: env.ALPACA_API_SECRET ?? "",
    alpacaDataBaseUrl: env.ALPACA_DATA_BASE_URL ?? "https://data.alpaca.markets",
    scannerProviderOrder: parseScannerProviders(
      env.SCANNER_PROVIDER_ORDER,
      ["ibkr", "fmp", "eodhd", "alpaca", "alpha_vantage", "ai_discovery"]
    ),
    fredApiKey: env.FRED_API_KEY ?? "",
    openAiApiKey: env.OPENAI_API_KEY ?? "",
    openAiModel: env.OPENAI_MODEL ?? "gpt-4.1-mini",
    openAiReviewMaxConcurrency: parseNumber(env.OPENAI_REVIEW_MAX_CONCURRENCY, 4),
    openAiReviewMinIntervalMs: parseNumber(env.OPENAI_REVIEW_MIN_INTERVAL_MS, 120),
    openAiReviewBatchWindowMs: parseNumber(env.OPENAI_REVIEW_BATCH_WINDOW_MS, 120),
    openAiReviewBatchSize: parseNumber(env.OPENAI_REVIEW_BATCH_SIZE, 8),
    recommendationsTimeoutMs: parseNumber(env.RECOMMENDATIONS_TIMEOUT_MS, 120_000),

    universeSymbols: parseCsv(env.UNIVERSE_SYMBOLS, defaultUniverse),

    dteMin: parseNumber(env.DTE_MIN, 7),
    dteMax: parseNumber(env.DTE_MAX, 21),
    maxPremiumRiskPct: parseNumber(env.MAX_PREMIUM_RISK_PCT, 0.02),
    dailyDrawdownLimitPct: parseNumber(env.DAILY_DRAWDOWN_LIMIT_PCT, 0.05),
    takeProfitPct: parseNumber(env.TAKE_PROFIT_PCT, 0.6),
    stopLossPct: parseNumber(env.STOP_LOSS_PCT, 0.35),
    maxHoldDays: parseNumber(env.MAX_HOLD_DAYS, 5),
    correlationCapPerDirection: parseNumber(env.CORRELATION_CAP_PER_DIRECTION, 2),
    preEventExitWindowHours: parseNumber(env.PRE_EVENT_EXIT_WINDOW_HOURS, 24),
    preEventSecFilingLookbackHours: parseNumber(env.PRE_EVENT_SEC_FILING_LOOKBACK_HOURS, 72),
    preEventSecFilingRiskThreshold: parseNumber(env.PRE_EVENT_SEC_FILING_RISK_THRESHOLD, 0.55)
  };
};

export type AppSettings = ReturnType<typeof buildSettings>;

const ensureStoragePaths = (config: AppSettings): void => {
  mkdirSync(dirname(config.dbPath), { recursive: true });
  mkdirSync(dirname(config.jsonlAuditPath), { recursive: true });
};

export const settings: AppSettings = buildSettings(process.env);
ensureStoragePaths(settings);

export const reloadSettingsFromEnv = (): AppSettings => {
  const next = buildSettings(process.env);
  Object.assign(settings, next);
  ensureStoragePaths(settings);
  return settings;
};
