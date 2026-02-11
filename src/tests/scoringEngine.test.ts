import { describe, expect, test } from "bun:test";

import type { FeatureVector } from "../types/models";
import { ScoringEngine } from "../services/scoringEngine";

const makeFeature = (
  symbol: string,
  momentum: number,
  trend: number,
  liquidity: number,
  sentiment: number,
  spreadPct: number
): FeatureVector => ({
  symbol,
  timestamp: new Date().toISOString(),
  momentum,
  trend,
  adx14: 22,
  regime: 0.1,
  regimeStability: 0.5,
  atrPct: 0.02,
  realizedVolPercentile: 0.55,
  breakoutZ: 0.3,
  relativeStrength20d: momentum * 0.9,
  relativeStrength60d: momentum * 0.7,
  relativeVolume20d: 1.1,
  ivRvSpread: 0.02,
  liquidity,
  flow: 0.4,
  skew: 0.1,
  optionsQuality: 0.6,
  newsSentiment: sentiment,
  newsVelocity24h: 0.4,
  newsSentimentDispersion: 0.2,
  newsFreshness: 0.6,
  eventBias: 0,
  macroRegime: 0,
  spreadPct,
  eventRisk: 0.2,
  gapRisk: 0.2,
  directionalUpProb: 0.6,
  directionalDownProb: 0.4
});

describe("ScoringEngine", () => {
  test("higher-quality candidate scores above weaker candidate", () => {
    const engine = new ScoringEngine();
    const features = [
      makeFeature("A", 0.2, 0.2, 0.8, 0.3, 0.001),
      makeFeature("B", -0.2, -0.1, 0.2, -0.2, 0.01),
      makeFeature("C", 0, 0, 0.5, 0, 0.005)
    ];

    const scores = engine.scoreUniverse(features);
    const bySymbol = new Map(scores.map((score) => [score.symbol, score]));

    expect(bySymbol.get("A")!.compositeScore).toBeGreaterThan(bySymbol.get("C")!.compositeScore);
    expect(bySymbol.get("C")!.compositeScore).toBeGreaterThan(bySymbol.get("B")!.compositeScore);
  });

  test("singleton universe returns zero-centered score", () => {
    const engine = new ScoringEngine();
    const scores = engine.scoreUniverse([makeFeature("ONLY", 0.1, 0.1, 0.8, 0.1, 0.002)]);

    expect(scores).toHaveLength(1);
    expect(scores[0].compositeScore).toBe(0);
  });
});
