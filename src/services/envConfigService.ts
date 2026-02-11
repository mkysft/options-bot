import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

import { settings, reloadSettingsFromEnv } from "../core/config";
import type { BotPolicy, RuntimePolicyService } from "./runtimePolicyService";
import type { BotScheduler } from "./scheduler";
import type { IbkrAdapter } from "../adapters/ibkrAdapter";
import type { ExecutionGateway } from "./executionGateway";

type EnvFieldType = "text" | "number" | "boolean" | "secret" | "csv";

interface EnvFieldDefinition {
  key: string;
  label: string;
  description: string;
  category: "Application" | "Trading" | "IBKR" | "Data";
  type: EnvFieldType;
  requiresRestart: boolean;
  placeholder?: string;
}

export interface EnvFieldSnapshot extends EnvFieldDefinition {
  value: string;
  liveApply: boolean;
}

export interface EnvConfigSnapshot {
  envPath: string;
  fields: EnvFieldSnapshot[];
}

export interface RuntimeRefreshResult {
  refreshedAt: string;
  scanCadenceMinutes: number;
  policy: BotPolicy;
}

export interface EnvConfigUpdateResult extends EnvConfigSnapshot {
  changedKeys: string[];
  liveAppliedKeys: string[];
  restartRequiredKeys: string[];
  restartRecommended: boolean;
  runtime: RuntimeRefreshResult;
}

export interface AppRestartResult {
  scheduled: boolean;
  delayMs: number;
  message: string;
  followupCommand: string;
}

const ENV_FIELDS: EnvFieldDefinition[] = [
  {
    key: "APP_NAME",
    label: "App Name",
    description: "Displayed name used in startup logs.",
    category: "Application",
    type: "text",
    requiresRestart: true
  },
  {
    key: "APP_HOST",
    label: "App Host",
    description: "Server bind host (restart required).",
    category: "Application",
    type: "text",
    requiresRestart: true
  },
  {
    key: "APP_PORT",
    label: "App Port",
    description: "Server bind port (restart required).",
    category: "Application",
    type: "number",
    requiresRestart: true
  },
  {
    key: "APP_ENV",
    label: "App Environment",
    description: "Runtime environment tag (dev/test/prod).",
    category: "Application",
    type: "text",
    requiresRestart: true
  },
  {
    key: "TIMEZONE",
    label: "Timezone",
    description: "Display and schedule timezone identifier.",
    category: "Application",
    type: "text",
    requiresRestart: false,
    placeholder: "America/Jamaica"
  },
  {
    key: "SCAN_CADENCE_MINUTES",
    label: "Scan Cadence Minutes",
    description: "Scheduler loop cadence. Applied live on refresh/save.",
    category: "Application",
    type: "number",
    requiresRestart: false
  },
  {
    key: "RECOMMENDATIONS_TIMEOUT_MS",
    label: "Recommendations Timeout (ms)",
    description:
      "Overall timeout for /recommendations runs. Set to 0 to disable total timeout and allow full run completion.",
    category: "Application",
    type: "number",
    requiresRestart: false,
    placeholder: "120000"
  },
  {
    key: "DB_PATH",
    label: "SQLite Path",
    description: "Primary SQLite storage file path.",
    category: "Application",
    type: "text",
    requiresRestart: true
  },
  {
    key: "JSONL_AUDIT_PATH",
    label: "Audit JSONL Path",
    description: "Append-only audit trail file path.",
    category: "Application",
    type: "text",
    requiresRestart: true
  },

  {
    key: "PAPER_MODE",
    label: "Paper Mode",
    description: "If false, live submissions are no longer blocked by mode gate.",
    category: "Trading",
    type: "boolean",
    requiresRestart: false
  },
  {
    key: "MANUAL_APPROVAL_REQUIRED",
    label: "Manual Approval Required",
    description: "Current workflow always requires approval in phase one.",
    category: "Trading",
    type: "boolean",
    requiresRestart: false
  },
  {
    key: "UNIVERSE_SYMBOLS",
    label: "Universe Symbols",
    description: "Comma-separated scanner universe symbols.",
    category: "Trading",
    type: "csv",
    requiresRestart: false,
    placeholder: "SPY,QQQ,IWM,AAPL,MSFT"
  },
  {
    key: "DTE_MIN",
    label: "DTE Min",
    description: "Minimum days-to-expiry for contract filtering.",
    category: "Trading",
    type: "number",
    requiresRestart: false
  },
  {
    key: "DTE_MAX",
    label: "DTE Max",
    description: "Maximum days-to-expiry for contract filtering.",
    category: "Trading",
    type: "number",
    requiresRestart: false
  },
  {
    key: "MAX_PREMIUM_RISK_PCT",
    label: "Max Premium Risk %",
    description: "Per-trade premium risk cap as fraction of equity.",
    category: "Trading",
    type: "number",
    requiresRestart: false
  },
  {
    key: "DAILY_DRAWDOWN_LIMIT_PCT",
    label: "Daily Drawdown Limit %",
    description: "Daily loss limit before new entries halt.",
    category: "Trading",
    type: "number",
    requiresRestart: false
  },
  {
    key: "TAKE_PROFIT_PCT",
    label: "Take Profit %",
    description: "Default take-profit harness percentage.",
    category: "Trading",
    type: "number",
    requiresRestart: false
  },
  {
    key: "STOP_LOSS_PCT",
    label: "Stop Loss %",
    description: "Default stop-loss harness percentage.",
    category: "Trading",
    type: "number",
    requiresRestart: false
  },
  {
    key: "MAX_HOLD_DAYS",
    label: "Max Hold Days",
    description: "Default max hold period before force exit.",
    category: "Trading",
    type: "number",
    requiresRestart: false
  },
  {
    key: "PRE_EVENT_EXIT_WINDOW_HOURS",
    label: "Pre-event Exit Window (hours)",
    description: "Auto-propose exits this many hours before known binary events (0 disables).",
    category: "Trading",
    type: "number",
    requiresRestart: false
  },
  {
    key: "PRE_EVENT_SEC_FILING_LOOKBACK_HOURS",
    label: "SEC Filing Exit Lookback (hours)",
    description: "Auto-propose exits after recent high-risk SEC filings in this lookback window (0 disables).",
    category: "Trading",
    type: "number",
    requiresRestart: false
  },
  {
    key: "PRE_EVENT_SEC_FILING_RISK_THRESHOLD",
    label: "SEC Filing Risk Threshold",
    description: "Minimum SEC filing risk score needed to trigger filing-based exits.",
    category: "Trading",
    type: "number",
    requiresRestart: false
  },
  {
    key: "CORRELATION_CAP_PER_DIRECTION",
    label: "Correlation Cap",
    description: "Max same-direction correlated open positions.",
    category: "Trading",
    type: "number",
    requiresRestart: false
  },

  {
    key: "IBKR_ENABLED",
    label: "IBKR Enabled",
    description: "Enables live IBKR connectivity probes and broker operations.",
    category: "IBKR",
    type: "boolean",
    requiresRestart: false
  },
  {
    key: "IBKR_HOST",
    label: "IBKR Host",
    description: "Host for TWS/Gateway API socket.",
    category: "IBKR",
    type: "text",
    requiresRestart: false
  },
  {
    key: "IBKR_PORT",
    label: "IBKR Primary Port",
    description: "Primary API port used for IBKR connectivity.",
    category: "IBKR",
    type: "number",
    requiresRestart: false
  },
  {
    key: "IBKR_PORT_CANDIDATES",
    label: "IBKR Port Candidates",
    description: "Extra comma-separated ports to probe.",
    category: "IBKR",
    type: "csv",
    requiresRestart: false,
    placeholder: "4002,7497,4001,7496"
  },
  {
    key: "IBKR_CLIENT_ID",
    label: "IBKR Client ID",
    description: "API client ID for IBKR sessions. Use a unique non-1 value to avoid collisions.",
    category: "IBKR",
    type: "number",
    requiresRestart: false,
    placeholder: "137"
  },
  {
    key: "IBKR_CLIENT_TIMEOUT_MS",
    label: "IBKR Client Timeout (ms)",
    description: "Socket/API request timeout used by the IBKR client wrapper.",
    category: "IBKR",
    type: "number",
    requiresRestart: false,
    placeholder: "6000"
  },
  {
    key: "IBKR_SCANNER_TIMEOUT_MS",
    label: "IBKR Scanner Timeout (ms)",
    description:
      "Timeout budget for IBKR scanner provider inside dynamic symbol discovery. Should usually be >= client timeout.",
    category: "IBKR",
    type: "number",
    requiresRestart: false,
    placeholder: "18000"
  },
  {
    key: "IBKR_QUEUE_GLOBAL_MIN_INTERVAL_MS",
    label: "Queue Global Min (ms)",
    description: "Global minimum spacing between outbound IBKR requests.",
    category: "IBKR",
    type: "number",
    requiresRestart: false,
    placeholder: "25"
  },
  {
    key: "IBKR_QUEUE_MAX_CONCURRENT",
    label: "Queue Max Concurrent",
    description: "Maximum concurrent queued IBKR operations.",
    category: "IBKR",
    type: "number",
    requiresRestart: false,
    placeholder: "3"
  },
  {
    key: "IBKR_QUEUE_QUOTE_INTERVAL_MS",
    label: "Quote Interval (ms)",
    description: "Per-channel minimum spacing for quote requests.",
    category: "IBKR",
    type: "number",
    requiresRestart: false,
    placeholder: "30"
  },
  {
    key: "IBKR_QUEUE_OPTION_CHAIN_INTERVAL_MS",
    label: "Chain Interval (ms)",
    description: "Per-channel minimum spacing for options chain requests.",
    category: "IBKR",
    type: "number",
    requiresRestart: false,
    placeholder: "50"
  },
  {
    key: "IBKR_QUEUE_HISTORICAL_INTERVAL_MS",
    label: "Historical Interval (ms)",
    description: "Per-channel minimum spacing for historical requests.",
    category: "IBKR",
    type: "number",
    requiresRestart: false,
    placeholder: "120"
  },
  {
    key: "IBKR_QUEUE_SCANNER_INTERVAL_MS",
    label: "Scanner Interval (ms)",
    description: "Per-channel minimum spacing for scanner requests.",
    category: "IBKR",
    type: "number",
    requiresRestart: false,
    placeholder: "120"
  },
  {
    key: "IBKR_RETRY_MAX_ATTEMPTS",
    label: "Retry Max Attempts",
    description: "Max attempts for transient IBKR request failures (timeouts/connectivity).",
    category: "IBKR",
    type: "number",
    requiresRestart: false,
    placeholder: "2"
  },
  {
    key: "IBKR_RETRY_BASE_DELAY_MS",
    label: "Retry Base Delay (ms)",
    description: "Base delay before retrying transient IBKR request failures.",
    category: "IBKR",
    type: "number",
    requiresRestart: false,
    placeholder: "180"
  },
  {
    key: "IBKR_RETRY_MAX_DELAY_MS",
    label: "Retry Max Delay (ms)",
    description: "Maximum retry delay for transient IBKR request failures.",
    category: "IBKR",
    type: "number",
    requiresRestart: false,
    placeholder: "1200"
  },
  {
    key: "IBKR_LAUNCH_TARGET",
    label: "IBKR Launch Target",
    description: "Default launch target when API trigger is used (gateway/tws).",
    category: "IBKR",
    type: "text",
    requiresRestart: false,
    placeholder: "gateway"
  },
  {
    key: "IBKR_GATEWAY_APP_NAME",
    label: "Gateway App Name",
    description: "macOS app name fallback for Gateway launching.",
    category: "IBKR",
    type: "text",
    requiresRestart: false
  },
  {
    key: "IBKR_TWS_APP_NAME",
    label: "TWS App Name",
    description: "macOS app name fallback for TWS launching.",
    category: "IBKR",
    type: "text",
    requiresRestart: false
  },
  {
    key: "IBKR_GATEWAY_EXEC_PATH",
    label: "Gateway Exec Path",
    description: "Absolute executable path (or .app bundle path) for Gateway.",
    category: "IBKR",
    type: "text",
    requiresRestart: false
  },
  {
    key: "IBKR_TWS_EXEC_PATH",
    label: "TWS Exec Path",
    description: "Absolute executable path (or .app bundle path) for TWS.",
    category: "IBKR",
    type: "text",
    requiresRestart: false
  },
  {
    key: "IBKR_LAUNCH_DRY_RUN",
    label: "IBKR Launch Dry Run",
    description: "If true, launch calls are simulated and never open apps.",
    category: "IBKR",
    type: "boolean",
    requiresRestart: false
  },

  {
    key: "ALPHA_VANTAGE_API_KEY",
    label: "Alpha Vantage API Key",
    description: "News/sentiment data API key.",
    category: "Data",
    type: "secret",
    requiresRestart: false
  },
  {
    key: "AI_DISCOVERY_ENABLED",
    label: "AI Discovery Enabled",
    description: "Enable OpenAI-backed scanner provider in the discovery chain.",
    category: "Data",
    type: "boolean",
    requiresRestart: false
  },
  {
    key: "AI_DISCOVERY_CACHE_TTL_MINUTES",
    label: "AI Discovery Cache TTL (minutes)",
    description: "How long AI-generated symbol lists are cached.",
    category: "Data",
    type: "number",
    requiresRestart: false,
    placeholder: "10"
  },
  {
    key: "FMP_API_KEY",
    label: "FMP API Key",
    description: "Financial Modeling Prep screener API key.",
    category: "Data",
    type: "secret",
    requiresRestart: false
  },
  {
    key: "FMP_BASE_URL",
    label: "FMP Base URL",
    description: "Override FMP base URL.",
    category: "Data",
    type: "text",
    requiresRestart: false,
    placeholder: "https://financialmodelingprep.com"
  },
  {
    key: "EODHD_API_KEY",
    label: "EODHD API Key",
    description: "EODHD screener API key.",
    category: "Data",
    type: "secret",
    requiresRestart: false
  },
  {
    key: "EODHD_BASE_URL",
    label: "EODHD Base URL",
    description: "Override EODHD screener base URL.",
    category: "Data",
    type: "text",
    requiresRestart: false,
    placeholder: "https://eodhd.com/api/screener"
  },
  {
    key: "ALPACA_API_KEY",
    label: "Alpaca API Key",
    description: "Alpaca data/screener API key id.",
    category: "Data",
    type: "secret",
    requiresRestart: false
  },
  {
    key: "ALPACA_API_SECRET",
    label: "Alpaca API Secret",
    description: "Alpaca data/screener API secret.",
    category: "Data",
    type: "secret",
    requiresRestart: false
  },
  {
    key: "ALPACA_DATA_BASE_URL",
    label: "Alpaca Data URL",
    description: "Override Alpaca market data base URL.",
    category: "Data",
    type: "text",
    requiresRestart: false,
    placeholder: "https://data.alpaca.markets"
  },
  {
    key: "SCANNER_PROVIDER_ORDER",
    label: "Scanner Provider Order",
    description:
      "Comma-separated scanner providers: ibkr,fmp,eodhd,alpaca,alpha_vantage,ai_discovery",
    category: "Data",
    type: "csv",
    requiresRestart: false,
    placeholder: "ibkr,fmp,eodhd,alpaca,alpha_vantage,ai_discovery"
  },
  {
    key: "FRED_API_KEY",
    label: "FRED API Key",
    description: "Macro regime data API key.",
    category: "Data",
    type: "secret",
    requiresRestart: false
  },
  {
    key: "OPENAI_API_KEY",
    label: "OpenAI API Key",
    description: "LLM judge API key.",
    category: "Data",
    type: "secret",
    requiresRestart: false
  },
  {
    key: "OPENAI_MODEL",
    label: "OpenAI Model",
    description: "Model used by the LLM judge.",
    category: "Data",
    type: "text",
    requiresRestart: false,
    placeholder: "gpt-4.1-mini"
  },
  {
    key: "OPENAI_REVIEW_MAX_CONCURRENCY",
    label: "OpenAI Max Concurrency",
    description: "Maximum concurrent LLM review requests.",
    category: "Data",
    type: "number",
    requiresRestart: false,
    placeholder: "4"
  },
  {
    key: "OPENAI_REVIEW_MIN_INTERVAL_MS",
    label: "OpenAI Min Interval (ms)",
    description: "Minimum spacing between outbound LLM review requests.",
    category: "Data",
    type: "number",
    requiresRestart: false,
    placeholder: "120"
  },
  {
    key: "OPENAI_REVIEW_BATCH_WINDOW_MS",
    label: "OpenAI Batch Window (ms)",
    description: "How long to collect pending reviews before sending a batch request.",
    category: "Data",
    type: "number",
    requiresRestart: false,
    placeholder: "120"
  },
  {
    key: "OPENAI_REVIEW_BATCH_SIZE",
    label: "OpenAI Batch Size",
    description: "Maximum number of symbol reviews bundled in one LLM request.",
    category: "Data",
    type: "number",
    requiresRestart: false,
    placeholder: "8"
  }
];

const ENV_FIELD_KEY_SET = new Set(ENV_FIELDS.map((field) => field.key));
const ENV_FIELDS_BY_KEY = new Map(ENV_FIELDS.map((field) => [field.key, field]));

const isTestRuntime = (): boolean =>
  settings.appEnv === "test" || process.env.NODE_ENV === "test" || Boolean(process.env.BUN_TEST);

const serializeEnvValue = (value: string): string => {
  if (value.length === 0) return "";
  if (/^[A-Za-z0-9_./:@+-]*$/.test(value)) return value;
  const escaped = value
    .replace(/\\/g, "\\\\")
    .replace(/\n/g, "\\n")
    .replace(/\r/g, "\\r")
    .replace(/\t/g, "\\t")
    .replace(/"/g, '\\"');
  return `"${escaped}"`;
};

const parseStoredValue = (raw: string): string => {
  const value = raw.trim();
  if (value.length === 0) return "";

  if (value.startsWith('"') && value.endsWith('"') && value.length >= 2) {
    const inner = value.slice(1, -1);
    return inner
      .replace(/\\n/g, "\n")
      .replace(/\\r/g, "\r")
      .replace(/\\t/g, "\t")
      .replace(/\\"/g, '"')
      .replace(/\\\\/g, "\\");
  }

  if (value.startsWith("'") && value.endsWith("'") && value.length >= 2) {
    return value.slice(1, -1);
  }

  const inlineComment = value.search(/\s#/);
  if (inlineComment >= 0) return value.slice(0, inlineComment).trim();
  return value;
};

const normalizeAppEnv = (input: string): string => {
  if (input === "prod") return "prod";
  if (input === "test") return "test";
  return "dev";
};

export class EnvConfigService {
  constructor(
    private readonly runtimePolicy: RuntimePolicyService,
    private readonly scheduler: BotScheduler,
    private readonly ibkrAdapter: IbkrAdapter,
    private readonly executionGateway: ExecutionGateway
  ) {}

  private envPath(): string {
    const configured = process.env.ENV_CONFIG_PATH?.trim();
    if (configured) return resolve(configured);
    return resolve(process.cwd(), ".env");
  }

  private readEnvLines(): string[] {
    const file = this.envPath();
    if (!existsSync(file)) return [];
    const content = readFileSync(file, "utf8");
    return content.length > 0 ? content.split(/\r?\n/) : [];
  }

  private writeEnvLines(lines: string[]): void {
    const file = this.envPath();
    const normalizedLines = [...lines];
    while (normalizedLines.length > 0 && normalizedLines[normalizedLines.length - 1] === "") {
      normalizedLines.pop();
    }
    const text = `${normalizedLines.join("\n")}\n`;
    writeFileSync(file, text, "utf8");
  }

  private envKeyIndexes(lines: string[]): Map<string, number> {
    const indexes = new Map<string, number>();
    for (let index = 0; index < lines.length; index += 1) {
      const line = lines[index];
      const match = line.match(/^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=/);
      if (!match) continue;
      indexes.set(match[1], index);
    }
    return indexes;
  }

  private readKnownValuesFromFile(): Record<string, string> {
    const lines = this.readEnvLines();
    const values: Record<string, string> = {};

    for (const line of lines) {
      const match = line.match(/^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$/);
      if (!match) continue;
      const key = match[1];
      if (!ENV_FIELD_KEY_SET.has(key)) continue;
      values[key] = parseStoredValue(match[2] ?? "");
    }

    return values;
  }

  private syncProcessEnvFromFile(): void {
    const values = this.readKnownValuesFromFile();
    for (const field of ENV_FIELDS) {
      const next = values[field.key];
      if (next === undefined) continue;
      process.env[field.key] = next;
    }
  }

  private buildPolicyPatchFromEnvKeys(changedKeys: string[]): Partial<BotPolicy> {
    const changed = new Set(changedKeys);
    const patch: Partial<BotPolicy> = {};

    if (changed.has("DTE_MIN")) patch.dteMin = settings.dteMin;
    if (changed.has("DTE_MAX")) patch.dteMax = settings.dteMax;
    if (changed.has("MAX_PREMIUM_RISK_PCT")) patch.maxPremiumRiskPct = settings.maxPremiumRiskPct;
    if (changed.has("DAILY_DRAWDOWN_LIMIT_PCT")) {
      patch.dailyDrawdownLimitPct = settings.dailyDrawdownLimitPct;
    }
    if (changed.has("CORRELATION_CAP_PER_DIRECTION")) {
      patch.correlationCapPerDirection = settings.correlationCapPerDirection;
    }
    if (changed.has("TAKE_PROFIT_PCT")) patch.takeProfitPct = settings.takeProfitPct;
    if (changed.has("STOP_LOSS_PCT")) patch.stopLossPct = settings.stopLossPct;
    if (changed.has("MAX_HOLD_DAYS")) patch.maxHoldDays = settings.maxHoldDays;
    if (changed.has("PRE_EVENT_EXIT_WINDOW_HOURS")) {
      patch.preEventExitWindowHours = settings.preEventExitWindowHours;
    }
    if (changed.has("PRE_EVENT_SEC_FILING_LOOKBACK_HOURS")) {
      patch.preEventSecFilingLookbackHours = settings.preEventSecFilingLookbackHours;
    }
    if (changed.has("PRE_EVENT_SEC_FILING_RISK_THRESHOLD")) {
      patch.preEventSecFilingRiskThreshold = settings.preEventSecFilingRiskThreshold;
    }
    if (changed.has("UNIVERSE_SYMBOLS")) patch.universeSymbols = [...settings.universeSymbols];

    return patch;
  }

  private applyRuntime(changedKeys: string[] = []): RuntimeRefreshResult {
    this.syncProcessEnvFromFile();

    if (process.env.APP_ENV) {
      process.env.APP_ENV = normalizeAppEnv(process.env.APP_ENV);
    }

    reloadSettingsFromEnv();

    const policyPatch = this.buildPolicyPatchFromEnvKeys(changedKeys);
    const policy =
      Object.keys(policyPatch).length > 0
        ? this.runtimePolicy.updatePolicy(policyPatch)
        : this.runtimePolicy.getPolicy();

    this.scheduler.reloadInterval();
    this.ibkrAdapter.reloadConfiguration();
    this.executionGateway.reloadBrokerConfiguration();

    return {
      refreshedAt: new Date().toISOString(),
      scanCadenceMinutes: settings.scanCadenceMinutes,
      policy
    };
  }

  snapshot(): EnvConfigSnapshot {
    const fields: EnvFieldSnapshot[] = ENV_FIELDS.map((field) => ({
      ...field,
      value: process.env[field.key] ?? "",
      liveApply: !field.requiresRestart
    }));

    return {
      envPath: this.envPath(),
      fields
    };
  }

  refreshRuntime(): RuntimeRefreshResult {
    return this.applyRuntime([]);
  }

  updateValues(values: Record<string, string>): EnvConfigUpdateResult {
    const normalizedEntries = Object.entries(values).map(([key, raw]) => [key.trim(), String(raw ?? "")]);
    const fileValues = this.readKnownValuesFromFile();

    for (const [key] of normalizedEntries) {
      if (!ENV_FIELD_KEY_SET.has(key)) {
        throw new Error(`Unsupported env key: ${key}`);
      }
    }

    const changedKeys = normalizedEntries
      .filter(([key, value]) => (fileValues[key] ?? process.env[key] ?? "") !== value)
      .map(([key]) => key);

    if (changedKeys.length > 0) {
      const lines = this.readEnvLines();
      const indexes = this.envKeyIndexes(lines);

      for (const [key, value] of normalizedEntries) {
        const nextLine = `${key}=${serializeEnvValue(value)}`;
        const existingIndex = indexes.get(key);
        if (existingIndex === undefined) {
          lines.push(nextLine);
          indexes.set(key, lines.length - 1);
        } else {
          lines[existingIndex] = nextLine;
        }

        process.env[key] = value;
      }

      this.writeEnvLines(lines);
    }

    const runtime = this.applyRuntime(changedKeys);
    const restartRequiredKeys = changedKeys.filter(
      (key) => Boolean(ENV_FIELDS_BY_KEY.get(key)?.requiresRestart)
    );
    const liveAppliedKeys = changedKeys.filter(
      (key) => !Boolean(ENV_FIELDS_BY_KEY.get(key)?.requiresRestart)
    );

    return {
      ...this.snapshot(),
      changedKeys,
      liveAppliedKeys,
      restartRequiredKeys,
      restartRecommended: restartRequiredKeys.length > 0,
      runtime
    };
  }

  prepareRestart(): AppRestartResult {
    if (isTestRuntime()) {
      return {
        scheduled: false,
        delayMs: 0,
        message: "Restart skipped in test runtime.",
        followupCommand: "bun run dev"
      };
    }

    return {
      scheduled: true,
      delayMs: 600,
      message:
        "Server restart scheduled. If no process manager is supervising this process, run bun run dev/start manually.",
      followupCommand: "bun run dev"
    };
  }
}
