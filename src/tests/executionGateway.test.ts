import { afterEach, describe, expect, test } from "bun:test";
import { rmSync } from "node:fs";

import type {
  IbkrAccountSnapshot,
  IbkrOrderStatusSnapshot,
  IbkrSubmitOrderPayload
} from "../adapters/ibkrAdapter";
import { AuditStore } from "../storage/auditStore";
import { type AccountState, type DecisionCard, type OptionContractSnapshot, type OrderIntent } from "../types/models";
import { nowIso } from "../utils/time";
import { ExecutionGateway } from "../services/executionGateway";
import { RiskEngine } from "../services/riskEngine";
import { RuntimePolicyService } from "../services/runtimePolicyService";

class FakeIbkrAdapter {
  optionMidPrice: number | null = null;
  accountSnapshot: IbkrAccountSnapshot | null = null;
  orderStatuses: Record<string, IbkrOrderStatusSnapshot> = {};

  reloadConfiguration(): void {}

  async refreshOrderStatuses(localOrderIds: string[]): Promise<Record<string, IbkrOrderStatusSnapshot>> {
    const next: Record<string, IbkrOrderStatusSnapshot> = {};
    for (const localOrderId of localOrderIds) {
      if (!this.orderStatuses[localOrderId]) continue;
      next[localOrderId] = this.orderStatuses[localOrderId];
    }
    return next;
  }

  async submitPaperOrder(payload: IbkrSubmitOrderPayload): Promise<string> {
    return `fake-${payload.orderId}`;
  }

  async getOptionMidPrice(): Promise<number | null> {
    return this.optionMidPrice;
  }

  async getAccountSnapshot(): Promise<IbkrAccountSnapshot | null> {
    return this.accountSnapshot;
  }
}

class FakeAlphaVantageAdapter {
  nextEventDate: string | null = null;
  source = "test";

  async getNextEarningsDate(_symbol?: string): Promise<{ eventDate: string | null; source: string }> {
    return {
      eventDate: this.nextEventDate,
      source: this.source
    };
  }
}

const baseContract: OptionContractSnapshot = {
  symbol: "SPY",
  expiration: "2026-02-20",
  strike: 600,
  right: "CALL",
  bid: 1.95,
  ask: 2.05,
  last: 2,
  volume: 500,
  openInterest: 2_500,
  impliedVol: 0.22,
  delta: 0.34,
  gamma: 0.02
};

const baseDecision: DecisionCard = {
  symbol: "SPY",
  timestamp: nowIso(),
  action: "CALL",
  confidence: 0.64,
  rationale: "Momentum and options flow aligned.",
  vetoFlags: [],
  scoreCard: {
    symbol: "SPY",
    timestamp: nowIso(),
    techScore: 80,
    optionsScore: 75,
    sentimentScore: 62,
    riskPenalty: 30,
    compositeScore: 78
  }
};

const baseAccountState = (): AccountState => ({
  accountEquity: 100_000,
  dayRealizedPnl: 0,
  dayUnrealizedPnl: 0
});

const makeStore = (): { store: AuditStore; dbPath: string; jsonlPath: string } => {
  const token = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const dbPath = `/tmp/options-bot-gateway-test-${token}.sqlite`;
  const jsonlPath = `/tmp/options-bot-gateway-test-${token}.jsonl`;
  return {
    store: new AuditStore(dbPath, jsonlPath),
    dbPath,
    jsonlPath
  };
};

const makeEntry = (overrides: Partial<OrderIntent> = {}): OrderIntent => ({
  id: overrides.id ?? crypto.randomUUID(),
  createdAt: overrides.createdAt ?? nowIso(),
  updatedAt: nowIso(),
  intentType: "ENTRY",
  side: "BUY",
  symbol: "SPY",
  action: "CALL",
  optionContract: baseContract,
  quantity: 1,
  limitPrice: 2,
  status: "FILLED",
  riskNotes: [],
  decision: baseDecision,
  ...overrides
});

const createdFiles: string[] = [];

afterEach(() => {
  while (createdFiles.length > 0) {
    const file = createdFiles.pop();
    if (!file) continue;
    rmSync(file, { force: true });
  }
});

describe("ExecutionGateway", () => {
  test("proposeOrder sets ENTRY and BUY metadata", () => {
    const { store, dbPath, jsonlPath } = makeStore();
    createdFiles.push(dbPath, jsonlPath);
    const fakeIbkr = new FakeIbkrAdapter();
    const fakeAlpha = new FakeAlphaVantageAdapter();
    const gateway = new ExecutionGateway(
      store,
      new RiskEngine(new RuntimePolicyService()),
      new RuntimePolicyService(),
      fakeIbkr as never,
      fakeAlpha as never
    );

    const order = gateway.proposeOrder("SPY", baseDecision, [baseContract], 100_000);
    expect(order.intentType).toBe("ENTRY");
    expect(order.side).toBe("BUY");
    expect(order.status).toBe("PENDING_APPROVAL");
  });

  test("runExitAutomation proposes an EXIT order when take-profit threshold is breached", async () => {
    const { store, dbPath, jsonlPath } = makeStore();
    createdFiles.push(dbPath, jsonlPath);
    const fakeIbkr = new FakeIbkrAdapter();
    fakeIbkr.optionMidPrice = 3.4;
    const fakeAlpha = new FakeAlphaVantageAdapter();

    const gateway = new ExecutionGateway(
      store,
      new RiskEngine(new RuntimePolicyService()),
      new RuntimePolicyService(),
      fakeIbkr as never,
      fakeAlpha as never
    );

    const entry = makeEntry();
    store.saveOrder(entry);

    const proposed = await gateway.runExitAutomation(baseAccountState());
    expect(proposed.length).toBe(1);
    expect(proposed[0].intentType).toBe("EXIT");
    expect(proposed[0].side).toBe("SELL");
    expect(proposed[0].parentOrderId).toBe(entry.id);
    expect(proposed[0].exitReason).toBe("take_profit");
  });

  test("runExitAutomation proposes pre-event EXIT when binary event is inside policy window", async () => {
    const { store, dbPath, jsonlPath } = makeStore();
    createdFiles.push(dbPath, jsonlPath);
    const fakeIbkr = new FakeIbkrAdapter();
    fakeIbkr.optionMidPrice = 2.02;
    const fakeAlpha = new FakeAlphaVantageAdapter();
    const tomorrow = new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
    fakeAlpha.nextEventDate = tomorrow;

    const policy = new RuntimePolicyService();
    policy.updatePolicy({
      preEventExitWindowHours: 48,
      takeProfitPct: 5,
      stopLossPct: 0.8,
      maxHoldDays: 30
    });

    const gateway = new ExecutionGateway(
      store,
      new RiskEngine(policy),
      policy,
      fakeIbkr as never,
      fakeAlpha as never
    );

    const entry = makeEntry({ symbol: "AAPL", optionContract: { ...baseContract, symbol: "AAPL" } });
    store.saveOrder(entry);

    const proposed = await gateway.runExitAutomation(baseAccountState());
    expect(proposed.length).toBe(1);
    expect(proposed[0].intentType).toBe("EXIT");
    expect(proposed[0].exitReason).toBe("pre_event");
    expect(proposed[0].riskNotes.join(" ")).toContain("eventDate=");
  });

  test("syncAccountState applies broker values to in-memory account state", async () => {
    const { store, dbPath, jsonlPath } = makeStore();
    createdFiles.push(dbPath, jsonlPath);
    const fakeIbkr = new FakeIbkrAdapter();
    const fakeAlpha = new FakeAlphaVantageAdapter();
    fakeIbkr.accountSnapshot = {
      timestamp: nowIso(),
      accountCode: "DU123456",
      netLiquidation: 101_250,
      realizedPnl: 320,
      unrealizedPnl: -140,
      positionCount: 2,
      source: "account_updates"
    };

    const gateway = new ExecutionGateway(
      store,
      new RiskEngine(new RuntimePolicyService()),
      new RuntimePolicyService(),
      fakeIbkr as never,
      fakeAlpha as never
    );

    const account = baseAccountState();
    await gateway.syncAccountState(account);

    expect(account.accountEquity).toBe(101_250);
    expect(account.dayRealizedPnl).toBe(320);
    expect(account.dayUnrealizedPnl).toBe(-140);
    expect(gateway.getLastAccountSnapshot()?.accountCode).toBe("DU123456");
  });

  test("refreshBrokerStatuses marks parent ENTRY as EXITED after filled EXIT order", async () => {
    const { store, dbPath, jsonlPath } = makeStore();
    createdFiles.push(dbPath, jsonlPath);
    const fakeIbkr = new FakeIbkrAdapter();
    const fakeAlpha = new FakeAlphaVantageAdapter();

    const gateway = new ExecutionGateway(
      store,
      new RiskEngine(new RuntimePolicyService()),
      new RuntimePolicyService(),
      fakeIbkr as never,
      fakeAlpha as never
    );

    const parent = makeEntry();
    const exit: OrderIntent = {
      ...makeEntry({
        id: crypto.randomUUID(),
        intentType: "EXIT",
        side: "SELL",
        parentOrderId: parent.id,
        exitReason: "take_profit",
        status: "SUBMITTED_PAPER"
      })
    };

    store.saveOrder(parent);
    store.saveOrder(exit);

    fakeIbkr.orderStatuses[exit.id] = {
      localOrderId: exit.id,
      brokerOrderId: 918273,
      status: "Filled",
      filled: exit.quantity,
      remaining: 0,
      avgFillPrice: 3.22,
      lastFillPrice: 3.22,
      permId: 1,
      clientId: 1,
      whyHeld: "",
      source: "event",
      updatedAt: nowIso()
    };

    await gateway.refreshBrokerStatuses();

    const reloadedParent = store.getOrder(parent.id);
    expect(reloadedParent?.status).toBe("EXITED");
  });
});
