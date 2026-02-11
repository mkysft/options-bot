import type { FeatureVector } from "../types/models";
import { ScoringEngine } from "../services/scoringEngine";
import { clamp } from "../utils/statistics";
import { nowIso } from "../utils/time";

export interface WalkForwardPoint {
  symbol: string;
  timestamp: string;
  feature: FeatureVector;
  realizedMovePct: number;
}

export interface WalkForwardSettings {
  minCompositeScore: number;
  minDirectionalProbability: number;
  optionLeverage: number;
  maxGainPct: number;
  maxLossPct: number;
  slippageBps: number;
  commissionPerTrade: number;
  premiumPerTrade: number;
  warmupWindow: number;
  startingEquity: number;
  sampleLimit: number;
}

export interface BacktestTradeSample {
  timestamp: string;
  symbol: string;
  action: "CALL" | "PUT";
  compositeScore: number;
  directionalProb: number;
  realizedMovePct: number;
  grossReturnPct: number;
  netReturnPct: number;
  pnl: number;
}

export interface BacktestResult {
  trades: number;
  wins: number;
  losses: number;
  winRate: number;
  netPnl: number;
  endingEquity: number;
  maxDrawdown: number;
  maxDrawdownPct: number;
  profitFactor: number;
  expectancy: number;
  samples: BacktestTradeSample[];
}

export class WalkForwardSimulator {
  private readonly scorer = new ScoringEngine();

  private defaultSettings(): WalkForwardSettings {
    return {
      minCompositeScore: 70,
      minDirectionalProbability: 0.57,
      optionLeverage: 4,
      maxGainPct: 0.6,
      maxLossPct: 0.35,
      slippageBps: 12,
      commissionPerTrade: 0.65,
      premiumPerTrade: 250,
      warmupWindow: 60,
      startingEquity: 10_000,
      sampleLimit: 100
    };
  }

  run(points: WalkForwardPoint[], partialSettings?: Partial<WalkForwardSettings>): BacktestResult {
    const settings: WalkForwardSettings = {
      ...this.defaultSettings(),
      ...(partialSettings ?? {})
    };

    if (points.length === 0 || points.length <= settings.warmupWindow) {
      return {
        trades: 0,
        wins: 0,
        losses: 0,
        winRate: 0,
        netPnl: 0,
        endingEquity: settings.startingEquity,
        maxDrawdown: 0,
        maxDrawdownPct: 0,
        profitFactor: 0,
        expectancy: 0,
        samples: []
      };
    }

    const equityCurve: number[] = [settings.startingEquity];
    let wins = 0;
    let losses = 0;
    let grossProfit = 0;
    let grossLoss = 0;
    const samples: BacktestTradeSample[] = [];

    for (let index = settings.warmupWindow; index < points.length; index += 1) {
      const point = points[index];
      const historyFeatures = points
        .slice(Math.max(0, index - settings.warmupWindow), index)
        .map((entry) => entry.feature);
      const score = this.scorer.scoreSingle(point.feature, historyFeatures);

      let signal = 0;
      let action: "CALL" | "PUT" | null = null;
      let directionalProb = 0;

      if (
        score.compositeScore >= settings.minCompositeScore &&
        point.feature.directionalUpProb >= settings.minDirectionalProbability
      ) {
        signal = 1;
        action = "CALL";
        directionalProb = point.feature.directionalUpProb;
      } else if (
        score.compositeScore >= settings.minCompositeScore &&
        point.feature.directionalDownProb >= settings.minDirectionalProbability
      ) {
        signal = -1;
        action = "PUT";
        directionalProb = point.feature.directionalDownProb;
      }

      if (signal === 0 || action === null) {
        equityCurve.push(equityCurve[equityCurve.length - 1]);
        continue;
      }

      const grossReturnPct = signal * point.realizedMovePct * settings.optionLeverage;
      const cappedGrossReturnPct = clamp(grossReturnPct, -settings.maxLossPct, settings.maxGainPct);
      const slippagePct = (settings.slippageBps * 2) / 10_000;
      const netReturnPct = cappedGrossReturnPct - slippagePct;
      const pnl = settings.premiumPerTrade * netReturnPct - settings.commissionPerTrade;

      if (pnl > 0) {
        wins += 1;
        grossProfit += pnl;
      }
      if (pnl < 0) {
        losses += 1;
        grossLoss += Math.abs(pnl);
      }

      equityCurve.push(equityCurve[equityCurve.length - 1] + pnl);

      if (samples.length < settings.sampleLimit) {
        samples.push({
          timestamp: point.timestamp,
          symbol: point.symbol,
          action,
          compositeScore: score.compositeScore,
          directionalProb,
          realizedMovePct: point.realizedMovePct,
          grossReturnPct: cappedGrossReturnPct,
          netReturnPct,
          pnl
        });
      }
    }

    let peak = equityCurve[0];
    let maxDrawdown = 0;
    for (const value of equityCurve) {
      peak = Math.max(peak, value);
      maxDrawdown = Math.max(maxDrawdown, peak - value);
    }

    const trades = wins + losses;
    const endingEquity = equityCurve[equityCurve.length - 1];
    const netPnl = endingEquity - settings.startingEquity;

    return {
      trades,
      wins,
      losses,
      winRate: trades > 0 ? wins / trades : 0,
      netPnl,
      endingEquity,
      maxDrawdown,
      maxDrawdownPct: settings.startingEquity > 0 ? maxDrawdown / settings.startingEquity : 0,
      profitFactor: grossLoss > 0 ? grossProfit / grossLoss : grossProfit > 0 ? Infinity : 0
      ,
      expectancy: trades > 0 ? netPnl / trades : 0,
      samples
    };
  }
}

export const makeFeaturePoint = (
  symbol: string,
  momentum: number,
  trend: number,
  regime: number,
  up: number,
  down: number
): FeatureVector => ({
  symbol,
  timestamp: nowIso(),
  momentum,
  trend,
  adx14: 24,
  regime,
  regimeStability: 0.45,
  atrPct: 0.02,
  realizedVolPercentile: 0.5,
  breakoutZ: 0.2,
  relativeStrength20d: momentum * 0.9,
  relativeStrength60d: momentum * 0.7,
  relativeVolume20d: 1.1,
  ivRvSpread: 0.03,
  liquidity: 0.8,
  flow: 0.5,
  skew: 0.1,
  optionsQuality: 0.55,
  newsSentiment: 0.1,
  newsVelocity24h: 0.35,
  newsSentimentDispersion: 0.22,
  newsFreshness: 0.5,
  eventBias: 0,
  macroRegime: 0,
  spreadPct: 0.001,
  eventRisk: 0.2,
  gapRisk: 0.2,
  directionalUpProb: up,
  directionalDownProb: down
});
