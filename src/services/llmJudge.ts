import { settings } from "../core/config";
import { logger } from "../core/logger";
import type { FeatureVector, ScoreCard, TradeAction } from "../types/models";
import { fetchWithApiLog } from "../utils/fetchWithApiLog";

export interface LlmJudgement {
  confirmed: boolean;
  vetoFlags: string[];
  rationale: string;
}

interface LlmReviewContext {
  weakCompositeFloor?: number;
}

interface PendingLlmReview {
  id: string;
  feature: FeatureVector;
  score: ScoreCard;
  action: TradeAction;
  context: LlmReviewContext;
  resolve: (value: LlmJudgement) => void;
}

type LlmApiError = Error & {
  status?: number;
  retryAfterMs?: number;
};

const MAX_REASON_TEXT = 700;

export class LlmJudge {
  private rateLimitedUntilMs = 0;
  private lastRateLimitLogMs = 0;
  private lastRemoteAttemptMs = 0;
  private remoteReviewsInFlight = 0;
  private pendingReviews: PendingLlmReview[] = [];
  private flushTimer: ReturnType<typeof setTimeout> | null = null;
  private reviewSequence = 0;
  private readonly testRuntime =
    settings.appEnv === "test" ||
    process.env.NODE_ENV === "test" ||
    Boolean(process.env.BUN_TEST);

  private get maxRemoteInFlight(): number {
    return Math.max(1, Math.min(16, Math.round(settings.openAiReviewMaxConcurrency)));
  }

  private get minRemoteIntervalMs(): number {
    return Math.max(0, Math.round(settings.openAiReviewMinIntervalMs));
  }

  private get batchWindowMs(): number {
    return Math.max(0, Math.round(settings.openAiReviewBatchWindowMs));
  }

  private get batchSize(): number {
    return Math.max(1, Math.min(32, Math.round(settings.openAiReviewBatchSize)));
  }

  review(
    feature: FeatureVector,
    score: ScoreCard,
    action: TradeAction,
    context: LlmReviewContext = {}
  ): Promise<LlmJudgement> {
    const apiKey = settings.openAiApiKey;
    if (action === "NO_TRADE") {
      return Promise.resolve({
        confirmed: true,
        vetoFlags: [],
        rationale: "No trade candidate by deterministic policy."
      });
    }

    if (!apiKey || this.testRuntime) {
      return Promise.resolve(this.heuristicReview(feature, score, context));
    }

    const now = Date.now();
    if (now < this.rateLimitedUntilMs) {
      if (now - this.lastRateLimitLogMs > 60_000) {
        logger.warn(
          `LLM review skipped due to cooldown after rate-limit; next attempt after ${new Date(
            this.rateLimitedUntilMs
          ).toISOString()}`
        );
        this.lastRateLimitLogMs = now;
      }
      return Promise.resolve(this.heuristicReview(feature, score, context));
    }

    return this.enqueueReview(feature, score, action, context);
  }

  private enqueueReview(
    feature: FeatureVector,
    score: ScoreCard,
    action: TradeAction,
    context: LlmReviewContext
  ): Promise<LlmJudgement> {
    return new Promise((resolve) => {
      this.pendingReviews.push({
        id: `llm-${++this.reviewSequence}`,
        feature,
        score,
        action,
        context,
        resolve
      });

      if (this.pendingReviews.length >= this.batchSize) {
        this.scheduleFlush(0);
      } else {
        this.scheduleFlush(this.batchWindowMs);
      }
    });
  }

  private scheduleFlush(delayMs: number): void {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }

    this.flushTimer = setTimeout(() => {
      this.flushTimer = null;
      void this.flushQueue();
    }, Math.max(0, delayMs));
  }

  private resolveBatchWithHeuristic(batch: PendingLlmReview[]): void {
    for (const request of batch) {
      request.resolve(this.heuristicReview(request.feature, request.score, request.context));
    }
  }

  private async flushQueue(): Promise<void> {
    if (this.pendingReviews.length === 0) return;

    const now = Date.now();
    if (now < this.rateLimitedUntilMs) {
      const batch = this.pendingReviews.splice(0, this.batchSize);
      this.resolveBatchWithHeuristic(batch);
      if (this.pendingReviews.length > 0) this.scheduleFlush(this.batchWindowMs);
      return;
    }

    if (this.remoteReviewsInFlight >= this.maxRemoteInFlight) {
      this.scheduleFlush(35);
      return;
    }

    const sinceLastAttempt = now - this.lastRemoteAttemptMs;
    if (sinceLastAttempt < this.minRemoteIntervalMs) {
      this.scheduleFlush(this.minRemoteIntervalMs - sinceLastAttempt);
      return;
    }

    const batch = this.pendingReviews.splice(0, this.batchSize);
    if (batch.length === 0) return;

    const apiKey = settings.openAiApiKey;
    if (!apiKey) {
      this.resolveBatchWithHeuristic(batch);
      if (this.pendingReviews.length > 0) this.scheduleFlush(this.batchWindowMs);
      return;
    }

    this.lastRemoteAttemptMs = Date.now();
    this.remoteReviewsInFlight += 1;

    try {
      const resultById = await this.remoteReviewBatch(batch, apiKey, settings.openAiModel);
      for (const request of batch) {
        const judgement = resultById.get(request.id);
        request.resolve(
          judgement ?? this.heuristicReview(request.feature, request.score, request.context)
        );
      }
    } catch (error) {
      const apiError = error as LlmApiError;
      if (apiError.status === 429) {
        const cooldownMs = this.resolveCooldownMs(apiError.retryAfterMs);
        this.rateLimitedUntilMs = Math.max(this.rateLimitedUntilMs, Date.now() + cooldownMs);
        this.lastRateLimitLogMs = Date.now();
        logger.warn(`LLM review rate-limited (429). Using heuristic for ${cooldownMs}ms.`);
      } else {
        logger.warn("LLM review failed, using heuristic", error);
      }
      this.resolveBatchWithHeuristic(batch);
    } finally {
      this.remoteReviewsInFlight = Math.max(0, this.remoteReviewsInFlight - 1);
      if (this.pendingReviews.length > 0) {
        this.scheduleFlush(this.batchWindowMs);
      }
    }
  }

  private resolveCooldownMs(retryAfterMs?: number): number {
    const fallbackMs = 60_000;
    const minMs = 5_000;
    const maxMs = 10 * 60_000;
    const raw = retryAfterMs ?? fallbackMs;
    return Math.max(minMs, Math.min(raw, maxMs));
  }

  private parseRetryAfterMs(value: string | null): number | undefined {
    if (!value) return undefined;
    const trimmed = value.trim();
    if (!trimmed) return undefined;

    const asSeconds = Number(trimmed);
    if (Number.isFinite(asSeconds)) {
      return Math.max(0, Math.round(asSeconds * 1000));
    }

    const asDate = Date.parse(trimmed);
    if (Number.isFinite(asDate)) {
      return Math.max(0, asDate - Date.now());
    }

    return undefined;
  }

  private parseResetDurationMs(value: string | null): number | undefined {
    if (!value) return undefined;
    const trimmed = value.trim();
    if (!trimmed) return undefined;

    const numeric = Number(trimmed);
    if (Number.isFinite(numeric)) {
      if (numeric <= 0) return 0;
      return numeric <= 1_000 ? Math.round(numeric * 1000) : Math.round(numeric);
    }

    let totalMs = 0;
    let matches = 0;
    const regex = /(\d+(?:\.\d+)?)(ms|s|m|h)/gi;
    for (const match of trimmed.matchAll(regex)) {
      const valuePart = Number(match[1]);
      const unit = String(match[2]).toLowerCase();
      if (!Number.isFinite(valuePart)) continue;
      matches += 1;
      if (unit === "ms") totalMs += valuePart;
      else if (unit === "s") totalMs += valuePart * 1000;
      else if (unit === "m") totalMs += valuePart * 60_000;
      else if (unit === "h") totalMs += valuePart * 3_600_000;
    }
    if (matches > 0) return Math.max(0, Math.round(totalMs));

    const asDate = Date.parse(trimmed);
    if (Number.isFinite(asDate)) return Math.max(0, asDate - Date.now());
    return undefined;
  }

  private applyRateLimitHeaders(headers: Headers): void {
    const remainingRaw = headers.get("x-ratelimit-remaining-requests");
    const resetRaw = headers.get("x-ratelimit-reset-requests");
    const remaining = remainingRaw === null ? Number.NaN : Number(remainingRaw);
    const resetMs = this.parseResetDurationMs(resetRaw);

    if (Number.isFinite(remaining) && remaining <= 0 && typeof resetMs === "number" && resetMs > 0) {
      this.rateLimitedUntilMs = Math.max(this.rateLimitedUntilMs, Date.now() + resetMs);
    }
  }

  private createApiError(
    status: number,
    retryAfterMs?: number,
    detail?: string
  ): LlmApiError {
    const suffix = detail ? `: ${detail}` : "";
    const error = new Error(`OpenAI responses API returned ${status}${suffix}`) as LlmApiError;
    error.status = status;
    error.retryAfterMs = retryAfterMs;
    return error;
  }

  private heuristicReview(
    feature: FeatureVector,
    score: ScoreCard,
    context: LlmReviewContext
  ): LlmJudgement {
    const vetoFlags: string[] = [];
    if (feature.eventRisk >= 0.82) vetoFlags.push("high_event_risk");
    if (feature.spreadPct >= 0.02) vetoFlags.push("wide_underlying_spread");
    if (feature.gapRisk >= 0.88) vetoFlags.push("elevated_gap_risk");
    if (feature.adx14 < 10) vetoFlags.push("weak_trend_strength");
    if (feature.relativeVolume20d < 0.45) vetoFlags.push("low_relative_volume");
    if (feature.newsSentimentDispersion > 0.9) vetoFlags.push("high_sentiment_dispersion");
    if (
      typeof context.weakCompositeFloor === "number" &&
      Number.isFinite(context.weakCompositeFloor) &&
      score.compositeScore < context.weakCompositeFloor - 6
    ) {
      vetoFlags.push("weak_composite");
    }

    if (vetoFlags.length > 0) {
      return {
        confirmed: false,
        vetoFlags,
        rationale: "Heuristic veto due to elevated event, spread, gap, or score risk."
      };
    }

    return {
      confirmed: true,
      vetoFlags: [],
      rationale: "Heuristic confirmation passed."
    };
  }

  private extractOutputText(payload: {
    output_text?: string;
    output?: Array<{ content?: Array<{ text?: string }> }>;
  }): string {
    return (
      payload.output_text ??
      payload.output?.flatMap((item) => item.content ?? []).map((item) => item.text ?? "").join("\n") ??
      ""
    );
  }

  private parseJsonPayload(text: string): unknown {
    const trimmed = text.trim();
    const candidates: string[] = [];
    if (trimmed.length > 0) candidates.push(trimmed);

    const codeBlockMatch = trimmed.match(/```(?:json)?\s*([\s\S]*?)```/i);
    if (codeBlockMatch?.[1]) candidates.push(codeBlockMatch[1].trim());

    const firstObj = trimmed.indexOf("{");
    const lastObj = trimmed.lastIndexOf("}");
    if (firstObj >= 0 && lastObj > firstObj) {
      candidates.push(trimmed.slice(firstObj, lastObj + 1).trim());
    }

    const firstArr = trimmed.indexOf("[");
    const lastArr = trimmed.lastIndexOf("]");
    if (firstArr >= 0 && lastArr > firstArr) {
      candidates.push(trimmed.slice(firstArr, lastArr + 1).trim());
    }

    for (const candidate of [...new Set(candidates)]) {
      if (!candidate) continue;
      try {
        return JSON.parse(candidate);
      } catch {
        // try next candidate shape
      }
    }

    throw new SyntaxError(`Unable to parse JSON from LLM response: ${trimmed.slice(0, 140)}`);
  }

  private normalizeJudgement(row: unknown, fallbackId: string): { id: string; judgement: LlmJudgement } | null {
    if (!row || typeof row !== "object") return null;
    const record = row as {
      id?: unknown;
      confirmed?: unknown;
      vetoFlags?: unknown;
      rationale?: unknown;
    };

    const idRaw = typeof record.id === "string" && record.id.trim().length > 0 ? record.id.trim() : fallbackId;
    const confirmed = Boolean(record.confirmed);
    const vetoFlags = Array.isArray(record.vetoFlags)
      ? record.vetoFlags.map((flag) => String(flag))
      : [];
    const rationale = String(record.rationale ?? "LLM review response parsed.").slice(0, MAX_REASON_TEXT);

    if (!confirmed && vetoFlags.length === 0) vetoFlags.push("llm_veto_unspecified");
    return {
      id: idRaw,
      judgement: {
        confirmed,
        vetoFlags,
        rationale
      }
    };
  }

  private parseBatchResponse(
    parsed: unknown,
    batch: PendingLlmReview[]
  ): Map<string, LlmJudgement> {
    const byId = new Map<string, LlmJudgement>();
    const validIds = new Set(batch.map((request) => request.id));
    const fallbackId = batch[0]?.id ?? "llm-unknown";

    const reviewsRaw = (() => {
      if (Array.isArray(parsed)) return parsed;
      if (parsed && typeof parsed === "object") {
        const object = parsed as { reviews?: unknown };
        if (Array.isArray(object.reviews)) return object.reviews;
        return [parsed];
      }
      return [];
    })();

    for (const row of reviewsRaw) {
      const normalized = this.normalizeJudgement(row, fallbackId);
      if (!normalized) continue;
      if (!validIds.has(normalized.id)) continue;
      byId.set(normalized.id, normalized.judgement);
    }

    return byId;
  }

  private buildBatchPrompt(batch: PendingLlmReview[]): string {
    const lines = [
      "Review the following options candidates.",
      "You can only CONFIRM or VETO each candidate.",
      "Do not override risk policy.",
      'Return ONLY JSON as: {"reviews":[{"id":"...","confirmed":true,"vetoFlags":[],"rationale":"..."}]}',
      "Do not wrap JSON in markdown.",
      "Candidates:"
    ];

    for (const request of batch) {
      lines.push(
        [
          `id=${request.id}`,
          `symbol=${request.feature.symbol}`,
          `action=${request.action}`,
          `composite_score=${request.score.compositeScore.toFixed(2)}`,
          `up_prob=${request.feature.directionalUpProb.toFixed(3)}`,
          `down_prob=${request.feature.directionalDownProb.toFixed(3)}`,
          `adx14=${request.feature.adx14.toFixed(2)}`,
          `rvol20d=${request.feature.relativeVolume20d.toFixed(2)}`,
          `rs20=${request.feature.relativeStrength20d.toFixed(3)}`,
          `rs60=${request.feature.relativeStrength60d.toFixed(3)}`,
          `news_velocity=${request.feature.newsVelocity24h.toFixed(3)}`,
          `news_dispersion=${request.feature.newsSentimentDispersion.toFixed(3)}`,
          `news_freshness=${request.feature.newsFreshness.toFixed(3)}`,
          `regime_stability=${request.feature.regimeStability.toFixed(3)}`,
          `options_quality=${request.feature.optionsQuality.toFixed(3)}`,
          `event_risk=${request.feature.eventRisk.toFixed(3)}`,
          `gap_risk=${request.feature.gapRisk.toFixed(3)}`,
          `spread_pct=${request.feature.spreadPct.toFixed(4)}`
        ].join(" | ")
      );
    }

    return lines.join("\n");
  }

  private async remoteReviewBatch(
    batch: PendingLlmReview[],
    apiKey: string,
    model: string
  ): Promise<Map<string, LlmJudgement>> {
    const responseBody = {
      model,
      input: [
        {
          role: "system",
          content: "You are a risk reviewer for options entries. You can only confirm or veto."
        },
        {
          role: "user",
          content: this.buildBatchPrompt(batch)
        }
      ]
    };

    const symbols = [...new Set(batch.map((request) => request.feature.symbol))];
    const response = await fetchWithApiLog(
      "https://api.openai.com/v1/responses",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/json"
        },
        body: JSON.stringify(responseBody)
      },
      {
        provider: "openai",
        endpoint: "responses",
        reason: `LLM risk review batch (${batch.length} candidates)`,
        requestPayload: {
          model,
          batchSize: batch.length,
          symbols
        }
      }
    );

    this.applyRateLimitHeaders(response.headers);

    if (!response.ok) {
      const retryAfterMs =
        this.parseRetryAfterMs(response.headers.get("retry-after")) ??
        this.parseResetDurationMs(response.headers.get("x-ratelimit-reset-requests"));
      let detail = "";
      try {
        detail = (await response.text()).slice(0, 320).replace(/\s+/g, " ").trim();
      } catch {
        detail = "";
      }
      throw this.createApiError(response.status, retryAfterMs, detail);
    }

    const payload = (await response.json()) as {
      output_text?: string;
      output?: Array<{ content?: Array<{ text?: string }> }>;
    };
    const text = this.extractOutputText(payload);
    const parsed = this.parseJsonPayload(text);
    return this.parseBatchResponse(parsed, batch);
  }
}
