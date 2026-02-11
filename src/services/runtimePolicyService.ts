import { settings } from "../core/config";
import {
  DEFAULT_IBKR_SCANNER_CODE,
  type IbkrScannerCodeSetting,
  isIbkrScannerCodeSetting
} from "../constants/scanner";

export type AnalysisDataProviderSetting = "AUTO" | "ALPACA" | "IBKR";

const ANALYSIS_DATA_PROVIDERS = ["AUTO", "ALPACA", "IBKR"] as const;

const isAnalysisDataProviderSetting = (value: unknown): value is AnalysisDataProviderSetting =>
  typeof value === "string" &&
  (ANALYSIS_DATA_PROVIDERS as readonly string[]).includes(value.toUpperCase());

export interface BotPolicy {
  scanTopN: number;
  ibkrScanCode: IbkrScannerCodeSetting;
  analysisDataProvider: AnalysisDataProviderSetting;
  minCompositeScore: number;
  minDirectionalProbability: number;
  dteMin: number;
  dteMax: number;
  maxPremiumRiskPct: number;
  dailyDrawdownLimitPct: number;
  correlationCapPerDirection: number;
  takeProfitPct: number;
  stopLossPct: number;
  maxHoldDays: number;
  preEventExitWindowHours: number;
  preEventSecFilingLookbackHours: number;
  preEventSecFilingRiskThreshold: number;
  universeSymbols: string[];
}

const clamp = (value: number, min: number, max: number): number => Math.max(min, Math.min(max, value));

const uniqueUpperSymbols = (symbols: string[]): string[] => {
  const normalized = symbols
    .map((symbol) => symbol.trim().toUpperCase())
    .filter((symbol) => symbol.length > 0);
  return [...new Set(normalized)];
};

interface PolicyPersistenceStore {
  getAppState<T>(key: string): T | null;
  setAppState(key: string, payload: unknown): void;
}

export class RuntimePolicyService {
  private static readonly policyStateKey = "runtime_policy_v1";
  private readonly defaults: BotPolicy;
  private policy: BotPolicy;

  constructor(private readonly persistence?: PolicyPersistenceStore) {
    const defaults: BotPolicy = {
      scanTopN: 10,
      ibkrScanCode: DEFAULT_IBKR_SCANNER_CODE,
      analysisDataProvider: "ALPACA",
      minCompositeScore: 70,
      minDirectionalProbability: 0.57,
      dteMin: settings.dteMin,
      dteMax: settings.dteMax,
      maxPremiumRiskPct: settings.maxPremiumRiskPct,
      dailyDrawdownLimitPct: settings.dailyDrawdownLimitPct,
      correlationCapPerDirection: settings.correlationCapPerDirection,
      takeProfitPct: settings.takeProfitPct,
      stopLossPct: settings.stopLossPct,
      maxHoldDays: settings.maxHoldDays,
      preEventExitWindowHours: settings.preEventExitWindowHours,
      preEventSecFilingLookbackHours: settings.preEventSecFilingLookbackHours,
      preEventSecFilingRiskThreshold: settings.preEventSecFilingRiskThreshold,
      universeSymbols: [...settings.universeSymbols]
    };

    this.defaults = defaults;
    this.policy = { ...defaults };
    this.loadPersistedPolicy();
  }

  private buildPolicy(current: BotPolicy, patch: Partial<BotPolicy>): BotPolicy {
    const next: BotPolicy = {
      scanTopN: clamp(Math.round(patch.scanTopN ?? current.scanTopN), 1, 100),
      ibkrScanCode: isIbkrScannerCodeSetting(patch.ibkrScanCode)
        ? patch.ibkrScanCode
        : current.ibkrScanCode,
      analysisDataProvider: isAnalysisDataProviderSetting(patch.analysisDataProvider)
        ? (patch.analysisDataProvider.toUpperCase() as AnalysisDataProviderSetting)
        : current.analysisDataProvider,
      minCompositeScore: clamp(patch.minCompositeScore ?? current.minCompositeScore, -300, 300),
      minDirectionalProbability: clamp(
        patch.minDirectionalProbability ?? current.minDirectionalProbability,
        0.5,
        0.99
      ),
      dteMin: clamp(Math.round(patch.dteMin ?? current.dteMin), 1, 90),
      dteMax: clamp(Math.round(patch.dteMax ?? current.dteMax), 1, 120),
      maxPremiumRiskPct: clamp(patch.maxPremiumRiskPct ?? current.maxPremiumRiskPct, 0.001, 0.2),
      dailyDrawdownLimitPct: clamp(
        patch.dailyDrawdownLimitPct ?? current.dailyDrawdownLimitPct,
        0.005,
        0.5
      ),
      correlationCapPerDirection: clamp(
        Math.round(patch.correlationCapPerDirection ?? current.correlationCapPerDirection),
        1,
        10
      ),
      takeProfitPct: clamp(patch.takeProfitPct ?? current.takeProfitPct, 0.05, 5),
      stopLossPct: clamp(patch.stopLossPct ?? current.stopLossPct, 0.05, 1),
      maxHoldDays: clamp(Math.round(patch.maxHoldDays ?? current.maxHoldDays), 1, 30),
      preEventExitWindowHours: clamp(
        Math.round(patch.preEventExitWindowHours ?? current.preEventExitWindowHours),
        0,
        168
      ),
      preEventSecFilingLookbackHours: clamp(
        Math.round(
          patch.preEventSecFilingLookbackHours ?? current.preEventSecFilingLookbackHours
        ),
        0,
        168
      ),
      preEventSecFilingRiskThreshold: clamp(
        patch.preEventSecFilingRiskThreshold ?? current.preEventSecFilingRiskThreshold,
        0.1,
        1
      ),
      universeSymbols: patch.universeSymbols
        ? uniqueUpperSymbols(patch.universeSymbols)
        : [...current.universeSymbols]
    };

    if (next.universeSymbols.length === 0) {
      next.universeSymbols = [...current.universeSymbols];
    }

    if (next.dteMax < next.dteMin) {
      next.dteMax = next.dteMin;
    }

    return next;
  }

  private persistPolicy(): void {
    if (!this.persistence) return;
    this.persistence.setAppState(RuntimePolicyService.policyStateKey, this.policy);
  }

  private loadPersistedPolicy(): void {
    if (!this.persistence) return;
    const persisted = this.persistence.getAppState<Partial<BotPolicy>>(
      RuntimePolicyService.policyStateKey
    );
    if (!persisted) return;
    this.policy = this.buildPolicy(this.defaults, persisted);
  }

  getPolicy(): BotPolicy {
    return {
      ...this.policy,
      universeSymbols: [...this.policy.universeSymbols]
    };
  }

  getGuidelines(): Record<string, { label: string; description: string; min?: number; max?: number }> {
    return {
      scanTopN: {
        label: "Scan Top N",
        description: "How many candidates the scanner returns.",
        min: 1,
        max: 100
      },
      ibkrScanCode: {
        label: "Screener Type",
        description:
          "Scanner mode mapped per provider (IBKR code + provider-specific fallbacks)."
      },
      analysisDataProvider: {
        label: "Analysis Data Provider",
        description:
          "Primary provider used for per-symbol analysis (quote/history/options). ALPACA avoids IBKR analysis calls."
      },
      minCompositeScore: {
        label: "Min Composite Score",
        description: "Minimum deterministic score before a trade can be considered.",
        min: -300,
        max: 300
      },
      minDirectionalProbability: {
        label: "Min Direction Probability",
        description: "Minimum up/down probability required for CALL/PUT.",
        min: 0.5,
        max: 0.99
      },
      dteMin: {
        label: "Min DTE",
        description: "Minimum days-to-expiry for contract filtering.",
        min: 1,
        max: 90
      },
      dteMax: {
        label: "Max DTE",
        description: "Maximum days-to-expiry for contract filtering.",
        min: 1,
        max: 120
      },
      maxPremiumRiskPct: {
        label: "Max Premium Risk %",
        description: "Max account equity risked in premium per trade.",
        min: 0.001,
        max: 0.2
      },
      dailyDrawdownLimitPct: {
        label: "Daily Drawdown Limit %",
        description: "Trading halts when day loss exceeds this percent.",
        min: 0.005,
        max: 0.5
      },
      correlationCapPerDirection: {
        label: "Correlation Cap",
        description: "Max same-direction correlated positions.",
        min: 1,
        max: 10
      },
      takeProfitPct: {
        label: "Take Profit %",
        description: "Target gain for exits (strategy harness value).",
        min: 0.05,
        max: 5
      },
      stopLossPct: {
        label: "Stop Loss %",
        description: "Max allowed loss for exits (strategy harness value).",
        min: 0.05,
        max: 1
      },
      maxHoldDays: {
        label: "Max Hold Days",
        description: "Maximum holding period before forced exit.",
        min: 1,
        max: 30
      },
      preEventExitWindowHours: {
        label: "Pre-event Exit Window (hours)",
        description:
          "Auto-propose exit before known binary events (0 disables). Exits still require manual approval.",
        min: 0,
        max: 168
      },
      preEventSecFilingLookbackHours: {
        label: "SEC Filing Exit Lookback (hours)",
        description:
          "Auto-propose exits after high-risk recent SEC filings inside this lookback window (0 disables).",
        min: 0,
        max: 168
      },
      preEventSecFilingRiskThreshold: {
        label: "SEC Filing Risk Threshold",
        description:
          "Minimum SEC filing risk score required to trigger filing-based exit automation.",
        min: 0.1,
        max: 1
      }
    };
  }

  updatePolicy(patch: Partial<BotPolicy>): BotPolicy {
    const current = this.getPolicy();
    this.policy = this.buildPolicy(current, patch);
    this.persistPolicy();
    return this.getPolicy();
  }

  resetPolicy(): BotPolicy {
    this.policy = {
      ...this.defaults,
      universeSymbols: [...this.defaults.universeSymbols]
    };
    this.persistPolicy();
    return this.getPolicy();
  }
}
