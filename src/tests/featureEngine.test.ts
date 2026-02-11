import { describe, expect, test } from "bun:test";

import type { OptionContractSnapshot, SymbolSnapshot } from "../types/models";
import { FeatureEngine } from "../services/featureEngine";

describe("FeatureEngine", () => {
  test("ema and rsi indicate uptrend", () => {
    const engine = new FeatureEngine();
    const prices = Array.from({ length: 60 }, (_, idx) => 100 + idx);

    const ema20 = engine.ema(prices, 20);
    const ema50 = engine.ema(prices, 50);
    const rsi14 = engine.rsi(prices, 14);

    expect(ema20).toBeGreaterThan(ema50);
    expect(rsi14).toBeGreaterThanOrEqual(70);
  });

  test("buildFeatureVector outputs bounded directional probabilities", () => {
    const engine = new FeatureEngine();

    const snapshot: SymbolSnapshot = {
      symbol: "SPY",
      timestamp: new Date().toISOString(),
      last: 500,
      bid: 499.9,
      ask: 500.1,
      volume: 3_000_000,
      impliedVol: 0.24,
      realizedVol: 0.2,
      pctChange1d: 0.01,
      spreadPct: 0.0004
    };

    const closes = Array.from({ length: 90 }, (_, idx) => 480 + idx * 0.5);
    const dailyBars = closes.map((close, index) => {
      const previous = index > 0 ? closes[index - 1] : close;
      return {
        timestamp: null,
        open: previous,
        high: Math.max(close, previous) * 1.004,
        low: Math.min(close, previous) * 0.996,
        close,
        volume: 2_000_000 + index * 2_000
      };
    });

    const chain: OptionContractSnapshot[] = [
      {
        symbol: "SPY",
        expiration: "2026-03-20",
        strike: 500,
        right: "CALL",
        bid: 5,
        ask: 5.2,
        last: 5.1,
        volume: 400,
        openInterest: 1200,
        impliedVol: 0.23,
        delta: 0.42,
        gamma: 0.03
      },
      {
        symbol: "SPY",
        expiration: "2026-03-20",
        strike: 500,
        right: "PUT",
        bid: 4.8,
        ask: 5,
        last: 4.9,
        volume: 390,
        openInterest: 1150,
        impliedVol: 0.24,
        delta: -0.4,
        gamma: 0.03
      }
    ];

    const feature = engine.buildFeatureVector(snapshot, closes, chain, {
      newsSentiment: 0.15,
      newsVelocity24h: 0.4,
      newsSentimentDispersion: 0.2,
      newsFreshness: 0.6,
      eventBias: 0,
      eventRisk: 0.2,
      macroRegime: 0.1
    }, {
      dailyBars
    });

    expect(feature.liquidity).toBeLessThanOrEqual(1);
    expect(feature.liquidity).toBeGreaterThanOrEqual(-1);
    expect(feature.adx14).toBeGreaterThanOrEqual(0);
    expect(feature.relativeVolume20d).toBeGreaterThan(0);
    expect(feature.directionalUpProb).toBeGreaterThanOrEqual(0);
    expect(feature.directionalUpProb).toBeLessThanOrEqual(1);
    expect(feature.directionalDownProb).toBeGreaterThanOrEqual(0);
    expect(feature.directionalDownProb).toBeLessThanOrEqual(1);
  });
});
