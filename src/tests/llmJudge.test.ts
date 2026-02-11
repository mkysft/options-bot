import { afterEach, describe, expect, test } from "bun:test";

import { settings } from "../core/config";
import { LlmJudge } from "../services/llmJudge";
import type { FeatureVector, ScoreCard } from "../types/models";
import { nowIso } from "../utils/time";

const baseFeature: FeatureVector = {
  symbol: "SPY",
  timestamp: nowIso(),
  momentum: 0.2,
  trend: 0.3,
  adx14: 28,
  regime: 0.1,
  regimeStability: 0.55,
  atrPct: 0.02,
  realizedVolPercentile: 0.52,
  breakoutZ: 0.5,
  relativeStrength20d: 0.16,
  relativeStrength60d: 0.11,
  relativeVolume20d: 1.2,
  ivRvSpread: 0.02,
  liquidity: 0.7,
  flow: 0.4,
  skew: 0.1,
  optionsQuality: 0.62,
  newsSentiment: 0.15,
  newsVelocity24h: 0.45,
  newsSentimentDispersion: 0.2,
  newsFreshness: 0.6,
  eventBias: 0,
  macroRegime: 0.1,
  spreadPct: 0.001,
  eventRisk: 0.2,
  gapRisk: 0.2,
  directionalUpProb: 0.61,
  directionalDownProb: 0.39
};

const baseScore: ScoreCard = {
  symbol: "SPY",
  timestamp: nowIso(),
  techScore: 80,
  optionsScore: 72,
  sentimentScore: 65,
  riskPenalty: 28,
  compositeScore: 79
};

const originalKey = settings.openAiApiKey;
const originalModel = settings.openAiModel;
const originalFetch = globalThis.fetch;
const originalBunTest = process.env.BUN_TEST;
const originalNodeEnv = process.env.NODE_ENV;

afterEach(() => {
  settings.openAiApiKey = originalKey;
  settings.openAiModel = originalModel;
  globalThis.fetch = originalFetch;
  if (originalBunTest === undefined) {
    delete process.env.BUN_TEST;
  } else {
    process.env.BUN_TEST = originalBunTest;
  }

  if (originalNodeEnv === undefined) {
    delete process.env.NODE_ENV;
  } else {
    process.env.NODE_ENV = originalNodeEnv;
  }
});

describe("LlmJudge", () => {
  test("uses heuristic when API key is empty", async () => {
    settings.openAiApiKey = "";
    const judge = new LlmJudge();

    const result = await judge.review(baseFeature, baseScore, "CALL");
    expect(result.confirmed).toBeTrue();
    expect(result.vetoFlags.length).toBe(0);
  });

  test("enters cooldown on 429 and skips follow-up API calls", async () => {
    settings.openAiApiKey = "test-key";
    settings.openAiModel = "gpt-test";
    process.env.BUN_TEST = "";
    process.env.NODE_ENV = "development";

    let calls = 0;
    globalThis.fetch = (async () => {
      calls += 1;
      return new Response(
        JSON.stringify({
          error: {
            message: "Rate limit exceeded"
          }
        }),
        {
          status: 429,
          headers: {
            "content-type": "application/json",
            "retry-after": "2"
          }
        }
      );
    }) as unknown as typeof fetch;

    const judge = new LlmJudge();
    const first = await judge.review(baseFeature, baseScore, "CALL");
    const second = await judge.review(baseFeature, baseScore, "CALL");

    expect(first.confirmed).toBeTrue();
    expect(second.confirmed).toBeTrue();
    expect(calls).toBe(1);
  });

  test("parses code-fenced JSON responses without failing", async () => {
    settings.openAiApiKey = "test-key";
    settings.openAiModel = "gpt-test";
    process.env.BUN_TEST = "";
    process.env.NODE_ENV = "development";

    globalThis.fetch = (async () =>
      new Response(
        JSON.stringify({
          output_text:
            "```json\n{\"reviews\":[{\"confirmed\":true,\"vetoFlags\":[],\"rationale\":\"ok\"}]}\n```"
        }),
        {
          status: 200,
          headers: {
            "content-type": "application/json"
          }
        }
      )) as unknown as typeof fetch;

    const judge = new LlmJudge();
    const result = await judge.review(baseFeature, baseScore, "CALL");
    expect(result.confirmed).toBeTrue();
    expect(result.vetoFlags.length).toBe(0);
  });
});
