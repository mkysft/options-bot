import type { DecisionCard, FeatureVector, ScoreCard, TradeAction } from "../types/models";
import { clamp, sigmoid } from "../utils/statistics";
import { nowIso } from "../utils/time";
import { LlmJudge } from "./llmJudge";
import { RuntimePolicyService } from "./runtimePolicyService";

export class DecisionEngine {
  constructor(
    private readonly runtimePolicy: RuntimePolicyService,
    private readonly llmJudge: LlmJudge = new LlmJudge()
  ) {}

  private calibratedConfidence(feature: FeatureVector, score: ScoreCard): number {
    const compositeSignal = clamp(score.compositeScore / 120, -1.4, 1.4);
    const directionalEdge = Math.abs(feature.directionalUpProb - feature.directionalDownProb);
    const trendStrengthSignal = clamp((feature.adx14 - 20) / 25, -1, 1);
    const relativeVolumeSignal = clamp((feature.relativeVolume20d - 1) / 1.5, -1, 1);
    const qualitySignal =
      0.35 * feature.optionsQuality +
      0.3 * feature.regimeStability +
      0.2 * feature.relativeStrength20d +
      0.15 * feature.relativeStrength60d +
      0.16 * trendStrengthSignal +
      0.08 * relativeVolumeSignal +
      0.12 * feature.newsVelocity24h +
      0.08 * feature.newsFreshness -
      0.16 * feature.newsSentimentDispersion;
    const riskPenalty =
      0.42 * feature.eventRisk +
      0.34 * feature.gapRisk +
      Math.min(0.22, feature.spreadPct * 125) +
      0.18 * Math.max(0, feature.newsSentimentDispersion - 0.35) +
      0.12 * Math.max(0, 0.7 - feature.relativeVolume20d);

    const raw = 1.1 * compositeSignal + 1.7 * directionalEdge + qualitySignal - riskPenalty;
    return clamp(sigmoid(raw), 0.02, 0.99);
  }

  async decide(feature: FeatureVector, score: ScoreCard): Promise<DecisionCard> {
    const policy = this.runtimePolicy.getPolicy();
    let action: TradeAction = "NO_TRADE";
    let rationale = "No trade: deterministic thresholds not satisfied.";

    if (
      score.compositeScore >= policy.minCompositeScore &&
      feature.directionalUpProb >= policy.minDirectionalProbability
    ) {
      action = "CALL";
      rationale = `Deterministic rules favor CALL candidate (score >= ${policy.minCompositeScore}, p(up) >= ${policy.minDirectionalProbability}).`;
    } else if (
      score.compositeScore >= policy.minCompositeScore &&
      feature.directionalDownProb >= policy.minDirectionalProbability
    ) {
      action = "PUT";
      rationale = `Deterministic rules favor PUT candidate (score >= ${policy.minCompositeScore}, p(down) >= ${policy.minDirectionalProbability}).`;
    }

    const confidence = this.calibratedConfidence(feature, score);

    if (action !== "NO_TRADE") {
      const judgement = await this.llmJudge.review(feature, score, action, {
        weakCompositeFloor: policy.minCompositeScore
      });
      if (!judgement.confirmed) {
        return {
          symbol: feature.symbol,
          timestamp: nowIso(),
          action: "NO_TRADE",
          confidence,
          rationale: `Vetoed by judge: ${judgement.rationale}`,
          vetoFlags: judgement.vetoFlags,
          scoreCard: score
        };
      }
      rationale = `${rationale} Judge confirmation: ${judgement.rationale}`;
    }

    return {
      symbol: feature.symbol,
      timestamp: nowIso(),
      action,
      confidence,
      rationale,
      vetoFlags: [],
      scoreCard: score
    };
  }
}
