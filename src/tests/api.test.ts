import { afterEach, describe, expect, test } from "bun:test";
import { rmSync, writeFileSync } from "node:fs";

let app: Awaited<ReturnType<(typeof import("../server"))["buildApp"]>> | null = null;

afterEach(async () => {
  if (app) {
    await app.close();
    app = null;
  }
});

describe("API routes", () => {
  test("ui route redirects to dashboard index", async () => {
    process.env.BUN_TEST = "1";
    const serverModule = await import("../server");
    app = await serverModule.buildApp();

    const response = await app.inject({ method: "GET", url: "/ui" });
    expect(response.statusCode).toBe(302);
    expect(response.headers.location).toBe("/ui/");
  });

  test("ui index route returns html body", async () => {
    process.env.BUN_TEST = "1";
    const serverModule = await import("../server");
    app = await serverModule.buildApp();

    const response = await app.inject({ method: "GET", url: "/ui/index.html" });
    expect(response.statusCode).toBe(200);
    expect(response.headers["content-type"]).toContain("text/html");
    expect(response.body).toContain("<!doctype html>");
    expect(response.body.length).toBeGreaterThan(100);
  });

  test("ui bundle asset is reachable when build output exists", async () => {
    process.env.BUN_TEST = "1";
    const serverModule = await import("../server");
    app = await serverModule.buildApp();

    const htmlResponse = await app.inject({ method: "GET", url: "/ui/" });
    expect(htmlResponse.statusCode).toBe(200);
    const html = htmlResponse.body;

    if (html.includes("Frontend Build Not Found")) {
      expect(html).toContain("bun run ui:build");
      return;
    }

    const assetMatch = html.match(/\/ui\/assets\/index-[^"]+\.js/);
    expect(assetMatch).toBeTruthy();
    const assetUrl = assetMatch?.[0] as string;

    const assetResponse = await app.inject({ method: "GET", url: assetUrl });
    expect(assetResponse.statusCode).toBe(200);
    expect(assetResponse.body.length).toBeGreaterThan(1000);
  });

  test("health endpoint responds ok", async () => {
    process.env.BUN_TEST = "1";
    const serverModule = await import("../server");
    app = await serverModule.buildApp();

    const response = await app.inject({ method: "GET", url: "/health" });
    expect(response.statusCode).toBe(200);

    const payload = response.json();
    expect(payload.status).toBe("ok");
  });

  test("run-status endpoint returns scheduler/action timing payload", async () => {
    process.env.BUN_TEST = "1";
    const serverModule = await import("../server");
    app = await serverModule.buildApp();

    const response = await app.inject({ method: "GET", url: "/run-status" });
    expect(response.statusCode).toBe(200);

    const payload = response.json();
    expect(payload.scheduler).toBeDefined();
    expect(payload.execution).toBeDefined();
    expect(payload.ibkrRuntime).toBeDefined();
    expect(Array.isArray(payload.actions)).toBeTrue();
    expect(typeof payload.actions[0]?.frequency).toBe("object");
    expect(typeof payload.actions[0]?.frequency?.mode).toBe("string");
  });

  test("scan endpoint returns analyses", async () => {
    process.env.BUN_TEST = "1";
    const serverModule = await import("../server");
    app = await serverModule.buildApp();

    const response = await app.inject({
      method: "POST",
      url: "/scan",
      payload: {
        universe: ["SPY", "QQQ", "AAPL"],
        topN: 3
      }
    });

    expect(response.statusCode).toBe(200);
    const payload = response.json();
    expect(Array.isArray(payload.analyses)).toBeTrue();
    expect(payload.analyses.length).toBeGreaterThan(0);
    expect(payload.analyses[0].snapshot).toBeDefined();
    expect(typeof payload.scanner?.scannerUsed).toBe("boolean");
    expect(typeof payload.scanner?.scannerSource).toBe("string");
    expect(
      payload.scanner?.ibkrScanCode === null || typeof payload.scanner?.ibkrScanCode === "string"
    ).toBeTrue();
  });

  test("recommendations endpoint returns ranked suggestions with actionability", async () => {
    process.env.BUN_TEST = "1";
    const serverModule = await import("../server");
    app = await serverModule.buildApp();

    const response = await app.inject({
      method: "GET",
      url: "/recommendations?topN=2&universe=SPY,QQQ"
    });

    expect(response.statusCode).toBe(200);
    const payload = response.json();
    expect(Array.isArray(payload.recommendations)).toBeTrue();
    expect(payload.recommendations.length).toBeGreaterThan(0);
    expect(payload.recommendations[0].symbol).toBeString();
    expect(typeof payload.recommendations[0].actionable).toBe("boolean");
    expect(payload.recommendations[0].evidence).toBeDefined();
    expect(typeof payload.recommendations[0].evidence.indicatorCoverage.available).toBe("number");
    expect(typeof payload.recommendations[0].evidence.indicatorCoverage.total).toBe("number");
    expect(Array.isArray(payload.recommendations[0].evidence.indicators)).toBeTrue();
    expect(typeof payload.recommendations[0].evidence.dataQuality.passed).toBe("boolean");
    expect(Array.isArray(payload.recommendations[0].evidence.gateChecks)).toBeTrue();
    expect(payload.recommendations[0].evidence.scoreDecomposition).toBeDefined();
    expect(payload.recommendations[0].evidence.provenance).toBeDefined();
    expect(typeof payload.recommendations[0].evidence.featureVector.relativeStrength20d).toBe("number");
    expect(typeof payload.recommendations[0].evidence.featureVector.optionsQuality).toBe("number");
    expect(typeof payload.scanner?.scannerUsed).toBe("boolean");
    expect(typeof payload.scanner?.scannerSource).toBe("string");
    expect(typeof payload.execution?.timedOut).toBe("boolean");
    expect(typeof payload.execution?.timeoutMs).toBe("number");
    expect(typeof payload.execution?.source).toBe("string");
    expect(typeof payload.execution?.elapsedMs).toBe("number");
    expect(typeof payload.execution?.computeMs).toBe("number");
    expect(Array.isArray(payload.execution?.errors)).toBeTrue();
    expect(
      payload.scanner?.ibkrScanCode === null || typeof payload.scanner?.ibkrScanCode === "string"
    ).toBeTrue();
    expect(Array.isArray(payload.scanner?.scannerProvidersUsed)).toBeTrue();
    expect(Array.isArray(payload.scanner?.scannerProvidersTried)).toBeTrue();
  });

  test("api request logs endpoint returns internal/external request log rows", async () => {
    process.env.BUN_TEST = "1";
    const serverModule = await import("../server");
    app = await serverModule.buildApp();

    await app.inject({ method: "GET", url: "/health" });
    await app.inject({ method: "GET", url: "/recommendations?topN=1&universe=SPY" });

    const response = await app.inject({
      method: "GET",
      url: "/api-request-logs?limit=25"
    });

    expect(response.statusCode).toBe(200);
    const payload = response.json();
    expect(Array.isArray(payload.logs)).toBeTrue();
    expect(payload.logs.length).toBeGreaterThan(0);
    expect(typeof payload.logs[0].direction).toBe("string");
    expect(typeof payload.logs[0].reason).toBe("string");
    expect(typeof payload.logs[0].status).toBe("string");
  });

  test("market data diagnostics endpoint returns entitlement summary payload", async () => {
    process.env.BUN_TEST = "1";
    const serverModule = await import("../server");
    app = await serverModule.buildApp();

    const response = await app.inject({
      method: "GET",
      url: "/market-data-diagnostics?windowMinutes=180&limitSymbols=25"
    });

    expect(response.statusCode).toBe(200);
    const payload = response.json();
    expect(payload.summary).toBeDefined();
    expect(typeof payload.summary.coreMarketStatus).toBe("string");
    expect(typeof payload.summary.coreOptionsStatus).toBe("string");
    expect(Array.isArray(payload.symbols)).toBeTrue();
    expect(payload.brokerBackoffs).toBeDefined();
    expect(typeof payload.brokerBackoffs.delayedOnly).toBe("boolean");
  });

  test("backtest endpoint returns walk-forward report", async () => {
    process.env.BUN_TEST = "1";
    const serverModule = await import("../server");
    app = await serverModule.buildApp();

    const response = await app.inject({
      method: "POST",
      url: "/backtest",
      payload: {
        universe: ["SPY", "QQQ"],
        lookbackDays: 140,
        warmupWindow: 40,
        premiumPerTrade: 200,
        sampleLimit: 25
      }
    });

    expect(response.statusCode).toBe(200);
    const payload = response.json();
    expect(Array.isArray(payload.symbolsUsed)).toBeTrue();
    expect(payload.symbolsUsed.length).toBeGreaterThan(0);
    expect(typeof payload.pointsEvaluated).toBe("number");
    expect(payload.result).toBeDefined();
    expect(typeof payload.result.trades).toBe("number");
    expect(payload.acceptanceGate).toBeDefined();
  });

  test("bot policy supports patch and reset", async () => {
    process.env.BUN_TEST = "1";
    const serverModule = await import("../server");
    app = await serverModule.buildApp();

    const before = await app.inject({ method: "GET", url: "/bot-policy" });
    expect(before.statusCode).toBe(200);
    const beforePayload = before.json();
    expect(beforePayload.policy).toBeDefined();

    const patch = await app.inject({
      method: "PATCH",
      url: "/bot-policy",
      payload: {
        minCompositeScore: 65,
        maxPremiumRiskPct: 0.015,
        ibkrScanCode: "TOP_PERC_GAIN"
      }
    });
    expect(patch.statusCode).toBe(200);
    const patchPayload = patch.json();
    expect(patchPayload.policy.minCompositeScore).toBe(65);
    expect(patchPayload.policy.maxPremiumRiskPct).toBe(0.015);
    expect(patchPayload.policy.ibkrScanCode).toBe("TOP_PERC_GAIN");

    const reset = await app.inject({
      method: "POST",
      url: "/bot-policy/reset"
    });
    expect(reset.statusCode).toBe(200);
    const resetPayload = reset.json();
    expect(resetPayload.policy.minCompositeScore).not.toBe(65);
    expect(resetPayload.policy.maxPremiumRiskPct).not.toBe(0.015);
    expect(resetPayload.policy.ibkrScanCode).not.toBe("TOP_PERC_GAIN");

    const afterReset = await app.inject({ method: "GET", url: "/bot-policy" });
    expect(afterReset.statusCode).toBe(200);
    const afterResetPayload = afterReset.json();
    expect(afterResetPayload.policy.minCompositeScore).toBe(resetPayload.policy.minCompositeScore);
    expect(afterResetPayload.policy.maxPremiumRiskPct).toBe(resetPayload.policy.maxPremiumRiskPct);
  });

  test("account summary includes summary metrics and pnl series", async () => {
    process.env.BUN_TEST = "1";
    const serverModule = await import("../server");
    app = await serverModule.buildApp();

    const response = await app.inject({
      method: "GET",
      url: "/account-summary"
    });

    expect(response.statusCode).toBe(200);
    const payload = response.json();
    expect(payload.summary).toBeDefined();
    expect(typeof payload.summary.accountEquity).toBe("number");
    expect(Array.isArray(payload.pnlSeries)).toBeTrue();
    expect(payload.pnlSeries.length).toBeGreaterThan(0);
  });

  test("acceptance gate endpoint returns persisted gate snapshot", async () => {
    process.env.BUN_TEST = "1";
    const serverModule = await import("../server");
    app = await serverModule.buildApp();

    const response = await app.inject({
      method: "GET",
      url: "/acceptance-gate"
    });

    expect(response.statusCode).toBe(200);
    const payload = response.json();
    expect(payload.generatedAt).toBeString();
    expect(payload.gate).toBeDefined();
    expect(typeof payload.gate.pass).toBe("boolean");
    expect(payload.gate.checks).toBeDefined();
    expect(payload.gate.trading).toBeDefined();
    expect(payload.gate.risk).toBeDefined();
  });

  test("kill-switch endpoint toggles and returns current state", async () => {
    process.env.BUN_TEST = "1";
    const serverModule = await import("../server");
    app = await serverModule.buildApp();

    const initial = await app.inject({ method: "GET", url: "/kill-switch" });
    expect(initial.statusCode).toBe(200);
    const initialPayload = initial.json();
    expect(typeof initialPayload.killSwitch.enabled).toBe("boolean");

    const enable = await app.inject({
      method: "POST",
      url: "/kill-switch",
      payload: { enabled: true, reason: "test toggle" }
    });
    expect(enable.statusCode).toBe(200);
    const enablePayload = enable.json();
    expect(enablePayload.killSwitch.enabled).toBeTrue();

    const disable = await app.inject({
      method: "POST",
      url: "/kill-switch",
      payload: { enabled: false, reason: "test toggle off" }
    });
    expect(disable.statusCode).toBe(200);
    const disablePayload = disable.json();
    expect(disablePayload.killSwitch.enabled).toBeFalse();
  });

  test("positions endpoint returns broker/local position payload", async () => {
    process.env.BUN_TEST = "1";
    const serverModule = await import("../server");
    app = await serverModule.buildApp();

    const response = await app.inject({
      method: "GET",
      url: "/positions"
    });

    expect(response.statusCode).toBe(200);
    const payload = response.json();
    expect(payload.summary).toBeDefined();
    expect(typeof payload.summary.brokerOpenPositions).toBe("number");
    expect(typeof payload.summary.brokerLinkedToBotPositions).toBe("number");
    expect(typeof payload.summary.localOpenPositions).toBe("number");
    expect(payload.broker).toBeDefined();
    expect(payload.local).toBeDefined();
    expect(Array.isArray(payload.broker.positions)).toBeTrue();
    expect(Array.isArray(payload.local.positions)).toBeTrue();
  });

  test("ibkr status endpoint is reachable", async () => {
    process.env.BUN_TEST = "1";
    const serverModule = await import("../server");
    app = await serverModule.buildApp();

    const response = await app.inject({
      method: "GET",
      url: "/ibkr-status"
    });

    expect(response.statusCode).toBe(200);
    const payload = response.json();
    expect(payload.ibkr).toBeDefined();
    expect(typeof payload.ibkr.enabled).toBe("boolean");
    expect(typeof payload.ibkr.reachable).toBe("boolean");
  });

  test("ibkr readiness endpoint returns diagnostic checks", async () => {
    process.env.BUN_TEST = "1";
    const serverModule = await import("../server");
    app = await serverModule.buildApp();

    const response = await app.inject({
      method: "GET",
      url: "/ibkr-readiness"
    });

    expect(response.statusCode).toBe(200);
    const payload = response.json();
    expect(typeof payload.overall).toBe("boolean");
    expect(payload.checks).toBeDefined();
    expect(payload.checks.connectivity).toBeDefined();
    expect(payload.checks.queue).toBeDefined();
    expect(payload.checks.quote).toBeDefined();
    expect(payload.checks.historical).toBeDefined();
    expect(payload.checks.scanner).toBeDefined();
    expect(payload.checks.positions).toBeDefined();
    expect(payload.runtime).toBeDefined();
  });

  test("ibkr launch endpoint returns launch response", async () => {
    process.env.BUN_TEST = "1";
    const serverModule = await import("../server");
    app = await serverModule.buildApp();

    const response = await app.inject({
      method: "POST",
      url: "/ibkr-launch",
      payload: { target: "gateway" }
    });

    expect(response.statusCode).toBe(200);
    const payload = response.json();
    expect(payload.launch).toBeDefined();
    expect(typeof payload.launch.target).toBe("string");
    expect(typeof payload.launch.launched).toBe("boolean");
    expect(typeof payload.launch.message).toBe("string");
  });

  test("env config endpoints support snapshot, patch, refresh, and restart hooks", async () => {
    process.env.BUN_TEST = "1";
    const previousEnvPath = process.env.ENV_CONFIG_PATH;
    const envPath = `/tmp/options-bot-env-test-${Date.now()}.env`;
    writeFileSync(
      envPath,
      ["IBKR_ENABLED=true", "IBKR_HOST=127.0.0.1", "IBKR_PORT=7497", "SCAN_CADENCE_MINUTES=15"].join("\n"),
      { encoding: "utf8" }
    );
    process.env.ENV_CONFIG_PATH = envPath;

    try {
      const serverModule = await import("../server");
      app = await serverModule.buildApp();

      const policyPatch = await app.inject({
        method: "PATCH",
        url: "/bot-policy",
        payload: {
          minCompositeScore: 63,
          maxPremiumRiskPct: 0.013
        }
      });
      expect(policyPatch.statusCode).toBe(200);

      const snapshot = await app.inject({
        method: "GET",
        url: "/env-config"
      });
      expect(snapshot.statusCode).toBe(200);
      const snapshotPayload = snapshot.json();
      expect(Array.isArray(snapshotPayload.fields)).toBeTrue();
      expect(snapshotPayload.fields.length).toBeGreaterThan(0);

      const patch = await app.inject({
        method: "PATCH",
        url: "/env-config",
        payload: {
          values: {
            IBKR_PORT: "4002",
            SCAN_CADENCE_MINUTES: "20"
          }
        }
      });
      expect(patch.statusCode).toBe(200);
      const patchPayload = patch.json();
      expect(Array.isArray(patchPayload.changedKeys)).toBeTrue();
      expect(patchPayload.changedKeys).toContain("IBKR_PORT");
      expect(Array.isArray(patchPayload.liveAppliedKeys)).toBeTrue();

      const policyAfterEnvPatch = await app.inject({
        method: "GET",
        url: "/bot-policy"
      });
      expect(policyAfterEnvPatch.statusCode).toBe(200);
      const policyAfterEnvPatchPayload = policyAfterEnvPatch.json();
      expect(policyAfterEnvPatchPayload.policy.minCompositeScore).toBe(63);
      expect(policyAfterEnvPatchPayload.policy.maxPremiumRiskPct).toBe(0.013);

      const refresh = await app.inject({
        method: "POST",
        url: "/app/refresh"
      });
      expect(refresh.statusCode).toBe(200);
      const refreshPayload = refresh.json();
      expect(refreshPayload.runtime).toBeDefined();
      expect(typeof refreshPayload.runtime.scanCadenceMinutes).toBe("number");

      const policyAfterRefresh = await app.inject({
        method: "GET",
        url: "/bot-policy"
      });
      expect(policyAfterRefresh.statusCode).toBe(200);
      const policyAfterRefreshPayload = policyAfterRefresh.json();
      expect(policyAfterRefreshPayload.policy.minCompositeScore).toBe(63);
      expect(policyAfterRefreshPayload.policy.maxPremiumRiskPct).toBe(0.013);

      const restart = await app.inject({
        method: "POST",
        url: "/app/restart"
      });
      expect(restart.statusCode).toBe(200);
      const restartPayload = restart.json();
      expect(restartPayload.restart.scheduled).toBeFalse();
    } finally {
      if (previousEnvPath === undefined) {
        delete process.env.ENV_CONFIG_PATH;
      } else {
        process.env.ENV_CONFIG_PATH = previousEnvPath;
      }
      rmSync(envPath, { force: true });
    }
  });
});
