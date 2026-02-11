import { describe, expect, test } from "bun:test";

import type { DecisionCard, OptionContractSnapshot, OrderIntent, ScoreCard, TradeAction } from "../types/models";
import { RiskEngine } from "../services/riskEngine";
import { RuntimePolicyService } from "../services/runtimePolicyService";

const makeOrder = (symbol: string, action: TradeAction, quantity = 1, limitPrice = 2): OrderIntent => {
  const scoreCard: ScoreCard = {
    symbol,
    timestamp: new Date().toISOString(),
    techScore: 80,
    optionsScore: 80,
    sentimentScore: 60,
    riskPenalty: 10,
    compositeScore: 80
  };

  const decision: DecisionCard = {
    symbol,
    timestamp: new Date().toISOString(),
    action,
    confidence: 0.7,
    rationale: "test",
    vetoFlags: [],
    scoreCard
  };

  const optionContract: OptionContractSnapshot = {
    symbol,
    expiration: "2026-03-20",
    strike: 100,
    right: action === "CALL" ? "CALL" : "PUT",
    bid: 1.9,
    ask: 2.1,
    last: 2,
    volume: 200,
    openInterest: 700,
    impliedVol: 0.22,
    delta: 0.35,
    gamma: 0.03
  };

  return {
    id: crypto.randomUUID(),
    createdAt: new Date().toISOString(),
    intentType: "ENTRY",
    side: "BUY",
    symbol,
    action,
    optionContract,
    quantity,
    limitPrice,
    status: "PENDING_APPROVAL",
    riskNotes: [],
    decision
  };
};

describe("RiskEngine", () => {
  test("max contracts respects 2 percent premium cap", () => {
    const engine = new RiskEngine(new RuntimePolicyService());
    const contracts = engine.maxContractsForPremium(2, 100_000);
    expect(contracts).toBe(10);
  });

  test("daily drawdown breach halts trading", () => {
    const engine = new RiskEngine(new RuntimePolicyService());
    const state = engine.buildRiskState(
      { accountEquity: 100_000, dayRealizedPnl: -4_000, dayUnrealizedPnl: -2_000 },
      []
    );

    expect(state.halted).toBeTrue();
    expect(state.haltReasons).toContain("daily_drawdown_limit_breached");
  });

  test("correlation cap blocks third same-group direction position", () => {
    const engine = new RiskEngine(new RuntimePolicyService());
    const open = [makeOrder("SPY", "CALL"), makeOrder("QQQ", "CALL")];
    const candidate = makeOrder("SMH", "CALL");
    const riskState = engine.buildRiskState(
      { accountEquity: 100_000, dayRealizedPnl: 0, dayUnrealizedPnl: 0 },
      open
    );

    const check = engine.validateOrder(candidate, riskState, open);
    expect(check.allowed).toBeFalse();
    expect(check.reasons).toContain("correlation_cap_breached");
  });

  test("kill switch halts trading when enabled", () => {
    const engine = new RiskEngine(new RuntimePolicyService());
    engine.setKillSwitch(true);

    const state = engine.buildRiskState(
      { accountEquity: 100_000, dayRealizedPnl: 0, dayUnrealizedPnl: 0 },
      []
    );

    expect(state.halted).toBeTrue();
    expect(state.haltReasons).toContain("kill_switch_active");
  });
});
