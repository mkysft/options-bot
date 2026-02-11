import { describe, expect, test } from "bun:test";

import { WalkForwardSimulator, type WalkForwardPoint, makeFeaturePoint } from "../backtest/simulator";
import { nowIso } from "../utils/time";

const point = (symbol: string, up: number, down: number, realizedMovePct: number): WalkForwardPoint => ({
  symbol,
  timestamp: nowIso(),
  feature: makeFeaturePoint(symbol, 0.3, 0.2, 0.1, up, down),
  realizedMovePct
});

describe("WalkForwardSimulator", () => {
  test("returns empty metrics when there are not enough points", () => {
    const simulator = new WalkForwardSimulator();
    const result = simulator.run([]);
    expect(result.trades).toBe(0);
    expect(result.netPnl).toBe(0);
  });

  test("produces positive PnL when directional signal aligns with realized move", () => {
    const simulator = new WalkForwardSimulator();
    const points: WalkForwardPoint[] = [];

    for (let i = 0; i < 85; i += 1) {
      points.push(point("SPY", 0.72, 0.28, 0.018));
    }

    const result = simulator.run(points, {
      warmupWindow: 40,
      minCompositeScore: -100,
      minDirectionalProbability: 0.55,
      optionLeverage: 5,
      slippageBps: 8,
      commissionPerTrade: 0.5,
      premiumPerTrade: 300
    });

    expect(result.trades).toBeGreaterThan(0);
    expect(result.wins).toBeGreaterThan(0);
    expect(result.netPnl).toBeGreaterThan(0);
    expect(result.profitFactor).toBeGreaterThan(1);
  });
});
