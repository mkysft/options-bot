import type {
  DailyBar,
  FeatureVector,
  OptionContractSnapshot,
  SymbolSnapshot
} from "../types/models";
import { clamp, sigmoid } from "../utils/statistics";
import { nowIso } from "../utils/time";

export class FeatureEngine {
  ema(values: number[], period: number): number {
    if (values.length === 0) return 0;
    const alpha = 2 / (period + 1);
    let emaValue = values[0];
    for (const value of values.slice(1)) {
      emaValue = alpha * value + (1 - alpha) * emaValue;
    }
    return emaValue;
  }

  rsi(values: number[], period = 14): number {
    if (values.length <= period) return 50;
    const gains: number[] = [];
    const losses: number[] = [];

    for (let index = 1; index < values.length; index += 1) {
      const change = values[index] - values[index - 1];
      gains.push(Math.max(0, change));
      losses.push(Math.max(0, -change));
    }

    const avgGain = gains.slice(-period).reduce((acc, value) => acc + value, 0) / period;
    const avgLoss = losses.slice(-period).reduce((acc, value) => acc + value, 0) / period;
    if (avgLoss === 0) return 100;
    const rs = avgGain / avgLoss;
    return 100 - 100 / (1 + rs);
  }

  atrPct(values: number[], period = 14): number {
    if (values.length < 2) return 0;
    const tr: number[] = [];
    for (let i = 1; i < values.length; i += 1) {
      tr.push(Math.abs(values[i] - values[i - 1]));
    }
    const window = tr.slice(-period);
    const avgTr = window.reduce((acc, value) => acc + value, 0) / Math.max(window.length, 1);
    const last = values[values.length - 1];
    return last > 0 ? avgTr / last : 0;
  }

  private realizedVolPercentile(values: number[], window = 20): number {
    if (values.length < window + 5) return 0.5;
    const returns: number[] = [];
    for (let index = 1; index < values.length; index += 1) {
      const prev = values[index - 1];
      const curr = values[index];
      if (prev <= 0) continue;
      returns.push((curr - prev) / prev);
    }
    if (returns.length < window + 4) return 0.5;

    const rollingVol: number[] = [];
    for (let index = window - 1; index < returns.length; index += 1) {
      const sample = returns.slice(index - window + 1, index + 1);
      const mean = sample.reduce((sum, value) => sum + value, 0) / sample.length;
      const variance =
        sample.reduce((sum, value) => sum + (value - mean) ** 2, 0) / sample.length;
      rollingVol.push(Math.sqrt(variance) * Math.sqrt(252));
    }
    if (rollingVol.length === 0) return 0.5;

    const latest = rollingVol[rollingVol.length - 1];
    const belowOrEqual = rollingVol.filter((value) => value <= latest).length;
    return clamp(belowOrEqual / rollingVol.length, 0, 1);
  }

  private breakoutZ(values: number[]): number {
    if (values.length < 20) return 0;
    const window = values.slice(-20);
    const mu = window.reduce((acc, value) => acc + value, 0) / window.length;
    const variance = window.reduce((acc, value) => acc + (value - mu) ** 2, 0) / window.length;
    const sigma = Math.sqrt(variance);
    if (sigma === 0) return 0;
    return (values[values.length - 1] - mu) / sigma;
  }

  private optionMetrics(chain: OptionContractSnapshot[]): {
    liquidity: number;
    flow: number;
    skew: number;
    quality: number;
  } {
    if (chain.length === 0) {
      return { liquidity: 0, flow: 0, skew: 0, quality: 0 };
    }

    const preferred = chain.filter(
      (contract) =>
        contract.quoteSource === "ibkr_option_quote" ||
        contract.quoteSource === "alpaca_option_quote"
    );
    const sample = preferred.length > 0 ? preferred : chain;

    const spreads: number[] = [];
    const flowValues: number[] = [];
    const callIv: number[] = [];
    const putIv: number[] = [];
    const openInterestValues: number[] = [];
    const quoteCoverage = chain.length > 0 ? preferred.length / chain.length : 0;

    for (const contract of sample) {
      const mid = contract.bid > 0 && contract.ask > 0 ? (contract.bid + contract.ask) / 2 : contract.last;
      const spreadPct = mid > 0 ? (contract.ask - contract.bid) / mid : 1;
      spreads.push(spreadPct);
      flowValues.push(contract.volume / Math.max(contract.openInterest, 1));
      openInterestValues.push(contract.openInterest);
      if (contract.right === "CALL") callIv.push(contract.impliedVol);
      if (contract.right === "PUT") putIv.push(contract.impliedVol);
    }

    const avgSpread = spreads.reduce((acc, value) => acc + value, 0) / spreads.length;
    const avgFlow = flowValues.reduce((acc, value) => acc + value, 0) / flowValues.length;
    const avgOpenInterest =
      openInterestValues.reduce((acc, value) => acc + value, 0) /
      Math.max(1, openInterestValues.length);
    const meanCallIv = callIv.length ? callIv.reduce((acc, value) => acc + value, 0) / callIv.length : 0;
    const meanPutIv = putIv.length ? putIv.reduce((acc, value) => acc + value, 0) / putIv.length : 0;
    const spreadQuality = clamp(1 - avgSpread * 8, -1, 1);
    const flowQuality = clamp((avgFlow - 0.45) / 1.35, -1, 1);
    const depthQuality = clamp(Math.log10(Math.max(1, avgOpenInterest + 1)) / 3.2, 0, 1);
    const quoteCoverageQuality = clamp(quoteCoverage * 2 - 1, -1, 1);
    const quality = clamp(
      0.42 * spreadQuality +
        0.24 * flowQuality +
        0.2 * quoteCoverageQuality +
        0.14 * (depthQuality * 2 - 1),
      -1,
      1
    );

    return {
      liquidity: spreadQuality,
      flow: flowQuality,
      skew: clamp(
        (meanCallIv - meanPutIv) / Math.max((meanCallIv + meanPutIv) / 2, 1e-6),
        -1,
        1
      ),
      quality
    };
  }

  private trailingReturn(closes: number[], lookback: number): number {
    if (closes.length <= lookback) return 0;
    const last = closes[closes.length - 1];
    const previous = closes[closes.length - 1 - lookback];
    if (!Number.isFinite(last) || !Number.isFinite(previous) || previous <= 0) return 0;
    return clamp(last / previous - 1, -1, 1);
  }

  private relativeStrength(closes: number[], benchmarkCloses: number[] | undefined, lookback: number): number {
    const ownReturn = this.trailingReturn(closes, lookback);
    const benchmarkReturn = benchmarkCloses ? this.trailingReturn(benchmarkCloses, lookback) : 0;
    return clamp(ownReturn - benchmarkReturn, -1, 1);
  }

  private adx(dailyBars: DailyBar[], period = 14): number {
    if (dailyBars.length <= period + 1) return 20;

    const trs: number[] = [];
    const plusDMs: number[] = [];
    const minusDMs: number[] = [];
    for (let index = 1; index < dailyBars.length; index += 1) {
      const prev = dailyBars[index - 1];
      const current = dailyBars[index];
      const highDiff = current.high - prev.high;
      const lowDiff = prev.low - current.low;
      const plusDM = highDiff > lowDiff && highDiff > 0 ? highDiff : 0;
      const minusDM = lowDiff > highDiff && lowDiff > 0 ? lowDiff : 0;
      const trueRange = Math.max(
        current.high - current.low,
        Math.abs(current.high - prev.close),
        Math.abs(current.low - prev.close)
      );
      trs.push(Math.max(trueRange, 0));
      plusDMs.push(Math.max(plusDM, 0));
      minusDMs.push(Math.max(minusDM, 0));
    }

    if (trs.length < period) return 20;
    let smoothedTr = trs.slice(0, period).reduce((sum, value) => sum + value, 0);
    let smoothedPlus = plusDMs.slice(0, period).reduce((sum, value) => sum + value, 0);
    let smoothedMinus = minusDMs.slice(0, period).reduce((sum, value) => sum + value, 0);
    const dxValues: number[] = [];

    for (let index = period; index < trs.length; index += 1) {
      smoothedTr = smoothedTr - smoothedTr / period + trs[index];
      smoothedPlus = smoothedPlus - smoothedPlus / period + plusDMs[index];
      smoothedMinus = smoothedMinus - smoothedMinus / period + minusDMs[index];
      if (smoothedTr <= 0) continue;

      const plusDi = (100 * smoothedPlus) / smoothedTr;
      const minusDi = (100 * smoothedMinus) / smoothedTr;
      const denominator = plusDi + minusDi;
      const dx = denominator <= 0 ? 0 : (100 * Math.abs(plusDi - minusDi)) / denominator;
      dxValues.push(dx);
    }

    if (dxValues.length === 0) return 20;
    if (dxValues.length < period) {
      return clamp(dxValues.reduce((sum, value) => sum + value, 0) / dxValues.length, 0, 100);
    }

    let adx = dxValues.slice(0, period).reduce((sum, value) => sum + value, 0) / period;
    for (const dx of dxValues.slice(period)) {
      adx = (adx * (period - 1) + dx) / period;
    }
    return clamp(adx, 0, 100);
  }

  private relativeVolume(snapshotVolume: number, dailyBars: DailyBar[], lookback = 20): number {
    const trailing = dailyBars
      .slice(-Math.max(5, lookback))
      .map((bar) => bar.volume)
      .filter((volume) => Number.isFinite(volume) && volume > 0);
    if (trailing.length === 0) return 1;
    const averageVolume =
      trailing.reduce((sum, value) => sum + value, 0) / Math.max(1, trailing.length);
    if (!Number.isFinite(averageVolume) || averageVolume <= 0) return 1;
    return clamp(snapshotVolume / averageVolume, 0.05, 6);
  }

  buildFeatureVector(
    snapshot: SymbolSnapshot,
    closes: number[],
    chain: OptionContractSnapshot[],
    context: {
      newsSentiment: number;
      newsVelocity24h: number;
      newsSentimentDispersion: number;
      newsFreshness: number;
      eventBias: number;
      eventRisk: number;
      macroRegime: number;
    },
    options?: {
      benchmarkCloses?: number[];
      dailyBars?: DailyBar[];
    }
  ): FeatureVector {
    const benchmarkCloses = options?.benchmarkCloses;
    const dailyBars = options?.dailyBars ?? [];
    const ema20 = this.ema(closes, 20);
    const ema50 = this.ema(closes, 50);
    const momentum = snapshot.last > 0 ? (ema20 - ema50) / snapshot.last : 0;

    const rsi14 = this.rsi(closes, 14);
    const trend = (rsi14 - 50) / 50;

    const atrPct = this.atrPct(closes, 14);
    const realizedVolPercentile = this.realizedVolPercentile(closes, 20);
    const breakoutZ = this.breakoutZ(closes);
    const regime = clamp(snapshot.pctChange1d * 3 - atrPct, -1, 1);
    const adx14 = this.adx(dailyBars, 14);
    const adxSignal = clamp((adx14 - 20) / 25, -1, 1);
    const relativeVolume20d = this.relativeVolume(snapshot.volume, dailyBars, 20);
    const relativeVolumeSignal = clamp((relativeVolume20d - 1) / 1.5, -1, 1);
    const regimeStability = clamp(
      1 -
        Math.abs(realizedVolPercentile - 0.5) * 1.7 -
        atrPct * 2.4 +
        context.macroRegime * 0.25 +
        adxSignal * 0.22 +
        relativeVolumeSignal * 0.08 -
        context.newsSentimentDispersion * 0.08,
      -1,
      1
    );
    const relativeStrength20d = this.relativeStrength(closes, benchmarkCloses, 20);
    const relativeStrength60d = this.relativeStrength(closes, benchmarkCloses, 60);
    const ivRvSpread = snapshot.impliedVol - snapshot.realizedVol;

    const { liquidity, flow, skew, quality: optionsQuality } = this.optionMetrics(chain);

    const gapRisk = clamp(atrPct * 12 + Math.abs(snapshot.pctChange1d) * 2, 0, 1);
    const directionalConviction = Math.sign(momentum + trend + breakoutZ / 3 || snapshot.pctChange1d || 0);

    const directionSignal =
      2.2 * momentum +
      1.2 * trend +
      0.8 * (breakoutZ / 2) +
      0.35 * clamp(ivRvSpread * 2.4, -1, 1) +
      0.6 * context.newsSentiment +
      0.22 * context.newsVelocity24h +
      0.16 * context.newsFreshness -
      0.32 * context.newsSentimentDispersion +
      0.5 * context.macroRegime +
      0.4 * skew -
      0.22 * directionalConviction * adxSignal +
      0.18 * directionalConviction * relativeVolumeSignal -
      0.8 * context.eventRisk;

    const directionalUpProb = clamp(sigmoid(directionSignal), 0.01, 0.99);
    const directionalDownProb = clamp(sigmoid(-directionSignal), 0.01, 0.99);

    return {
      symbol: snapshot.symbol,
      timestamp: nowIso(),
      momentum,
      trend,
      adx14,
      regime,
      regimeStability,
      atrPct,
      realizedVolPercentile,
      breakoutZ,
      relativeStrength20d,
      relativeStrength60d,
      relativeVolume20d,
      ivRvSpread,
      liquidity,
      flow,
      skew,
      optionsQuality,
      newsSentiment: context.newsSentiment,
      newsVelocity24h: context.newsVelocity24h,
      newsSentimentDispersion: context.newsSentimentDispersion,
      newsFreshness: context.newsFreshness,
      eventBias: context.eventBias,
      macroRegime: context.macroRegime,
      spreadPct: snapshot.spreadPct,
      eventRisk: context.eventRisk,
      gapRisk,
      directionalUpProb,
      directionalDownProb
    };
  }
}
