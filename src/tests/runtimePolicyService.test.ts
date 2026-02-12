import { describe, expect, test } from "bun:test";

import { RuntimePolicyService } from "../services/runtimePolicyService";

class InMemoryPolicyStore {
  private readonly data = new Map<string, string>();

  getAppState<T>(key: string): T | null {
    const value = this.data.get(key);
    if (!value) return null;
    return JSON.parse(value) as T;
  }

  setAppState(key: string, payload: unknown): void {
    this.data.set(key, JSON.stringify(payload));
  }
}

describe("RuntimePolicyService persistence", () => {
  test("loads previously persisted policy values on startup", () => {
    const store = new InMemoryPolicyStore();
    const first = new RuntimePolicyService(store);

    const updated = first.updatePolicy({
      minCompositeScore: 83,
      minDirectionalProbability: 0.62,
      scanTopN: 17,
      ibkrScanCode: "TOP_PERC_GAIN",
      universeSymbols: ["spy", "qqq"]
    });

    expect(updated.minCompositeScore).toBe(83);
    expect(updated.minDirectionalProbability).toBe(0.62);
    expect(updated.scanTopN).toBe(17);
    expect(updated.ibkrScanCode).toBe("TOP_PERC_GAIN");
    expect(updated.universeSymbols).toEqual(["SPY", "QQQ"]);

    const second = new RuntimePolicyService(store);
    const reloaded = second.getPolicy();
    expect(reloaded.minCompositeScore).toBe(83);
    expect(reloaded.minDirectionalProbability).toBe(0.62);
    expect(reloaded.scanTopN).toBe(17);
    expect(reloaded.ibkrScanCode).toBe("TOP_PERC_GAIN");
    expect(reloaded.universeSymbols).toEqual(["SPY", "QQQ"]);
  });

  test("persists reset back to defaults", () => {
    const store = new InMemoryPolicyStore();
    const service = new RuntimePolicyService(store);

    service.updatePolicy({ minCompositeScore: 88, scanTopN: 19 });
    const reset = service.resetPolicy();
    expect(reset.minCompositeScore).toBe(63);
    expect(reset.scanTopN).toBe(10);

    const reloaded = new RuntimePolicyService(store).getPolicy();
    expect(reloaded.minCompositeScore).toBe(63);
    expect(reloaded.scanTopN).toBe(10);
  });
});
