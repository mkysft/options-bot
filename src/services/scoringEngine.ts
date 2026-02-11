import type { FeatureVector, ScoreCard } from "../types/models";
import { zScore } from "../utils/statistics";
import { nowIso } from "../utils/time";

export class ScoringEngine {
  private normalize(value: number, floor = -1, ceil = 1): number {
    return Math.max(floor, Math.min(ceil, value));
  }

  private rawTech(feature: FeatureVector): number {
    const breakoutComponent = this.normalize(feature.breakoutZ / 3, -1.25, 1.25);
    const adxSignal = this.normalize((feature.adx14 - 20) / 25, -1, 1);
    const relativeVolumeSignal = this.normalize((feature.relativeVolume20d - 1) / 1.5, -1, 1);
    return (
      feature.momentum +
      feature.trend +
      feature.regime +
      0.45 * breakoutComponent +
      0.45 * feature.relativeStrength20d +
      0.35 * feature.relativeStrength60d +
      0.25 * feature.regimeStability +
      0.25 * adxSignal +
      0.15 * relativeVolumeSignal
    );
  }

  private rawOptions(feature: FeatureVector): number {
    const ivRvComponent = this.normalize(feature.ivRvSpread * 2.4);
    return (
      feature.liquidity +
      feature.flow +
      feature.skew +
      0.45 * ivRvComponent +
      0.85 * feature.optionsQuality
    );
  }

  private rawSentiment(feature: FeatureVector): number {
    return (
      feature.newsSentiment +
      0.35 * feature.newsVelocity24h +
      0.2 * feature.newsFreshness -
      0.45 * feature.newsSentimentDispersion +
      feature.eventBias +
      0.3 * feature.macroRegime +
      0.2 * feature.regimeStability
    );
  }

  private rawRisk(feature: FeatureVector): number {
    const highVolPenalty = Math.max(0, feature.realizedVolPercentile - 0.72) * 1.1;
    const weakOptionsPenalty = Math.max(0, -feature.optionsQuality) * 0.7;
    const unstableRegimePenalty = Math.max(0, -feature.regimeStability) * 0.65;
    const lowRelativeVolumePenalty = Math.max(0, 0.8 - feature.relativeVolume20d) * 0.45;
    const sentimentDispersionPenalty = Math.max(0, feature.newsSentimentDispersion - 0.35) * 0.7;
    return (
      feature.spreadPct * 8 +
      feature.eventRisk +
      feature.gapRisk +
      highVolPenalty +
      weakOptionsPenalty +
      unstableRegimePenalty +
      lowRelativeVolumePenalty +
      sentimentDispersionPenalty
    );
  }

  scoreUniverse(features: FeatureVector[]): ScoreCard[] {
    if (features.length === 0) return [];

    const techPopulation = features.map((feature) => this.rawTech(feature));
    const optionsPopulation = features.map((feature) => this.rawOptions(feature));
    const sentimentPopulation = features.map((feature) => this.rawSentiment(feature));
    const riskPopulation = features.map((feature) => this.rawRisk(feature));

    return features.map((feature) => {
      const techScore = zScore(this.rawTech(feature), techPopulation) * 100;
      const optionsScore = zScore(this.rawOptions(feature), optionsPopulation) * 100;
      const sentimentScore = zScore(this.rawSentiment(feature), sentimentPopulation) * 100;
      const riskPenalty = zScore(this.rawRisk(feature), riskPopulation) * 100;

      const compositeScore =
        0.35 * techScore +
        0.3 * optionsScore +
        0.2 * sentimentScore -
        0.15 * riskPenalty;

      return {
        symbol: feature.symbol,
        timestamp: nowIso(),
        techScore,
        optionsScore,
        sentimentScore,
        riskPenalty,
        compositeScore
      };
    });
  }

  scoreSingle(feature: FeatureVector, universeFeatures: FeatureVector[]): ScoreCard {
    const cards = this.scoreUniverse(universeFeatures.length > 0 ? universeFeatures : [feature]);
    return cards.find((card) => card.symbol === feature.symbol) ?? cards[0];
  }
}
