import type { AccountState, OrderIntent, RiskState, TradeAction } from "../types/models";
import { nowIso } from "../utils/time";
import { RuntimePolicyService } from "./runtimePolicyService";

interface KillSwitchPersistenceStore {
  getAppState<T>(key: string): T | null;
  setAppState(key: string, payload: unknown): void;
}

export class RiskEngine {
  private static readonly killSwitchStateKey = "kill_switch_v1";
  private killSwitch = false;
  private killSwitchUpdatedAt: string | null = null;

  constructor(
    private readonly runtimePolicy: RuntimePolicyService,
    private readonly persistence?: KillSwitchPersistenceStore
  ) {
    this.loadPersistedKillSwitch();
  }

  private persistKillSwitch(): void {
    if (!this.persistence) return;
    this.persistence.setAppState(RiskEngine.killSwitchStateKey, {
      enabled: this.killSwitch,
      updatedAt: this.killSwitchUpdatedAt
    });
  }

  private loadPersistedKillSwitch(): void {
    if (!this.persistence) return;
    const persisted = this.persistence.getAppState<{
      enabled?: boolean;
      updatedAt?: string | null;
    }>(RiskEngine.killSwitchStateKey);
    if (!persisted) return;
    this.killSwitch = Boolean(persisted.enabled);
    this.killSwitchUpdatedAt = persisted.updatedAt ?? null;
  }

  setKillSwitch(enabled: boolean): { enabled: boolean; updatedAt: string } {
    this.killSwitch = enabled;
    this.killSwitchUpdatedAt = nowIso();
    this.persistKillSwitch();
    return {
      enabled: this.killSwitch,
      updatedAt: this.killSwitchUpdatedAt
    };
  }

  getKillSwitchState(): { enabled: boolean; updatedAt: string | null } {
    return {
      enabled: this.killSwitch,
      updatedAt: this.killSwitchUpdatedAt
    };
  }

  buildRiskState(account: AccountState, openPositions: OrderIntent[]): RiskState {
    const policy = this.runtimePolicy.getPolicy();
    const equity = Math.max(account.accountEquity, 1);
    const dayTotal = account.dayRealizedPnl + account.dayUnrealizedPnl;
    const dailyDrawdownPct = Math.max(0, -dayTotal / equity);

    const reasons: string[] = [];
    if (this.killSwitch) reasons.push("kill_switch_active");
    if (dailyDrawdownPct >= policy.dailyDrawdownLimitPct) reasons.push("daily_drawdown_limit_breached");

    return {
      timestamp: nowIso(),
      accountEquity: account.accountEquity,
      dayRealizedPnl: account.dayRealizedPnl,
      dayUnrealizedPnl: account.dayUnrealizedPnl,
      dailyDrawdownPct,
      halted: reasons.length > 0,
      haltReasons: reasons,
      openPositions: openPositions.length,
      openSameDirectionCorrelated: this.maxSameDirectionCorrelated(openPositions)
    };
  }

  maxContractsForPremium(limitPrice: number, accountEquity: number): number {
    const policy = this.runtimePolicy.getPolicy();
    if (limitPrice <= 0 || accountEquity <= 0) return 0;
    const maxRiskDollars = accountEquity * policy.maxPremiumRiskPct;
    return Math.max(Math.floor(maxRiskDollars / (limitPrice * 100)), 0);
  }

  validateOrder(
    order: OrderIntent,
    riskState: RiskState,
    openPositions: OrderIntent[]
  ): { allowed: boolean; reasons: string[] } {
    const policy = this.runtimePolicy.getPolicy();
    const reasons: string[] = [];

    if (riskState.halted) reasons.push(...riskState.haltReasons);

    const premiumRisk = order.quantity * order.limitPrice * 100;
    if (premiumRisk > riskState.accountEquity * policy.maxPremiumRiskPct) {
      reasons.push("premium_risk_exceeds_per_trade_limit");
    }

    if (this.wouldBreachCorrelationCap(order, openPositions)) {
      reasons.push("correlation_cap_breached");
    }

    return { allowed: reasons.length === 0, reasons };
  }

  private symbolGroup(symbol: string): string {
    const s = symbol.toUpperCase();
    if (["SPY", "QQQ", "IWM", "DIA", "SMH", "XLK"].includes(s)) return "broad_tech_etf";
    if (["XLF", "JPM"].includes(s)) return "financials";
    if (["XLE"].includes(s)) return "energy";
    if (["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "AVGO", "TSLA"].includes(s)) {
      return "mega_cap_tech";
    }
    return "other";
  }

  private maxSameDirectionCorrelated(openPositions: OrderIntent[]): number {
    const buckets = new Map<string, number>();
    for (const position of openPositions) {
      if (position.intentType !== "ENTRY") continue;
      const key = `${this.symbolGroup(position.symbol)}:${position.action}`;
      buckets.set(key, (buckets.get(key) ?? 0) + 1);
    }
    return Math.max(0, ...buckets.values());
  }

  private wouldBreachCorrelationCap(candidate: OrderIntent, openPositions: OrderIntent[]): boolean {
    if (candidate.intentType !== "ENTRY") return false;
    const policy = this.runtimePolicy.getPolicy();
    const group = this.symbolGroup(candidate.symbol);
    const direction = candidate.action as TradeAction;
    let count = 0;

    for (const position of openPositions) {
      if (position.intentType !== "ENTRY") continue;
      if (position.action === direction && this.symbolGroup(position.symbol) === group) count += 1;
    }

    return count >= policy.correlationCapPerDirection;
  }
}
