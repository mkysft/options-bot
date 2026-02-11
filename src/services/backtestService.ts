import { WalkForwardSimulator, type BacktestResult, type WalkForwardPoint } from "../backtest/simulator";
import { clamp, sigmoid } from "../utils/statistics";
import { MarketDataService } from "./marketDataService";
import { RuntimePolicyService } from "./runtimePolicyService";

export interface BacktestRunOptions {
  universe: string[];
  lookbackDays: number;
  slippageBps: number;
  commissionPerTrade: number;
  premiumPerTrade: number;
  optionLeverage: number;
  warmupWindow: number;
  maxGainPct: number;
  maxLossPct: number;
  startingEquity: number;
  sampleLimit: number;
}

export interface BacktestRunResult {
  generatedAt: string;
  symbolsUsed: string[];
  pointsEvaluated: number;
  settings: BacktestRunOptions;
  result: BacktestResult;
}

const ema = (values: number[], period: number): number => {
  if (values.length === 0) return 0;
  const alpha = 2 / (period + 1);
  let emaValue = values[0];
  for (const value of values.slice(1)) {
    emaValue = alpha * value + (1 - alpha) * emaValue;
  }
  return emaValue;
};

const rsi = (values: number[], period = 14): number => {
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
};

const atrPct = (values: number[], period = 14): number => {
  if (values.length < 2) return 0;
  const tr: number[] = [];
  for (let i = 1; i < values.length; i += 1) {
    tr.push(Math.abs(values[i] - values[i - 1]));
  }
  const window = tr.slice(-period);
  const avgTr = window.reduce((acc, value) => acc + value, 0) / Math.max(window.length, 1);
  const last = values[values.length - 1];
  return last > 0 ? avgTr / last : 0;
};

const breakoutZ = (values: number[]): number => {
  if (values.length < 20) return 0;
  const window = values.slice(-20);
  const mu = window.reduce((acc, value) => acc + value, 0) / window.length;
  const variance = window.reduce((acc, value) => acc + (value - mu) ** 2, 0) / window.length;
  const sigma = Math.sqrt(variance);
  if (sigma === 0) return 0;
  return (values[values.length - 1] - mu) / sigma;
};

const isoAtIndex = (index: number, total: number): string => {
  const ageDays = Math.max(0, total - index);
  return new Date(Date.now() - ageDays * 24 * 60 * 60 * 1000).toISOString();
};

export class BacktestService {
  private readonly simulator = new WalkForwardSimulator();

  constructor(
    private readonly marketData: MarketDataService,
    private readonly runtimePolicy: RuntimePolicyService
  ) {}

  private buildPoint(symbol: string, history: number[], nextClose: number, index: number, total: number): WalkForwardPoint {
    const last = history[history.length - 1];
    const prev = history[history.length - 2] ?? last;
    const ema20 = ema(history, 20);
    const ema50 = ema(history, 50);
    const rsi14 = rsi(history, 14);
    const atr = atrPct(history, 14);
    const momentum = last > 0 ? (ema20 - ema50) / last : 0;
    const trend = (rsi14 - 50) / 50;
    const regime = clamp((last - prev) / Math.max(prev, 1e-6) - atr, -1, 1);
    const directionSignal = 2.1 * momentum + 1.2 * trend + 0.8 * regime - 0.6 * atr;

    const directionalUpProb = clamp(sigmoid(directionSignal), 0.01, 0.99);
    const directionalDownProb = clamp(sigmoid(-directionSignal), 0.01, 0.99);
    const movePct = (nextClose - last) / Math.max(last, 1e-6);

    return {
      symbol,
      timestamp: isoAtIndex(index, total),
      feature: {
        symbol,
        timestamp: isoAtIndex(index, total),
        momentum,
        trend,
        adx14: clamp(16 + Math.abs(momentum) * 42, 5, 65),
        regime,
        regimeStability: clamp(1 - atr * 5, -1, 1),
        atrPct: atr,
        realizedVolPercentile: 0.5,
        breakoutZ: breakoutZ(history),
        relativeStrength20d: clamp(momentum * 0.9, -1, 1),
        relativeStrength60d: clamp(momentum * 0.7, -1, 1),
        relativeVolume20d: clamp(1 + Math.abs(movePct) * 4.5, 0.3, 3),
        ivRvSpread: atr * 0.35,
        liquidity: clamp(1 - atr * 8, -1, 1),
        flow: clamp(Math.abs(movePct) * 9, -1, 1),
        skew: clamp(momentum * 0.6, -1, 1),
        optionsQuality: clamp(0.45 + momentum * 0.5 - atr * 3, -1, 1),
        newsSentiment: clamp(momentum * 0.8, -1, 1),
        newsVelocity24h: clamp(Math.abs(momentum) * 1.2, 0, 1),
        newsSentimentDispersion: clamp(0.2 + atr * 2.4, 0, 1),
        newsFreshness: 0.55,
        eventBias: 0,
        macroRegime: clamp(regime * 0.4, -1, 1),
        spreadPct: clamp(atr * 0.15, 0.0004, 0.02),
        eventRisk: clamp(atr * 6, 0, 1),
        gapRisk: clamp(Math.abs(movePct) * 8, 0, 1),
        directionalUpProb,
        directionalDownProb
      },
      realizedMovePct: movePct
    };
  }

  private async symbolPoints(symbol: string, lookbackDays: number, warmupWindow: number): Promise<WalkForwardPoint[]> {
    const barsRequested = Math.max(lookbackDays + warmupWindow + 80, 160);
    const closes = await this.marketData.getRecentDailyCloses(symbol, barsRequested);
    if (closes.length < warmupWindow + 30) return [];

    const start = Math.max(warmupWindow, 50);
    const points: WalkForwardPoint[] = [];
    for (let index = start; index < closes.length - 1; index += 1) {
      const history = closes.slice(0, index + 1);
      const nextClose = closes[index + 1];
      points.push(this.buildPoint(symbol, history, nextClose, index, closes.length));
    }
    return points;
  }

  async run(partialOptions: Partial<BacktestRunOptions> = {}): Promise<BacktestRunResult> {
    const policy = this.runtimePolicy.getPolicy();
    const settings: BacktestRunOptions = {
      universe: partialOptions.universe ?? policy.universeSymbols,
      lookbackDays: partialOptions.lookbackDays ?? 220,
      slippageBps: partialOptions.slippageBps ?? 12,
      commissionPerTrade: partialOptions.commissionPerTrade ?? 0.65,
      premiumPerTrade: partialOptions.premiumPerTrade ?? 250,
      optionLeverage: partialOptions.optionLeverage ?? 4,
      warmupWindow: partialOptions.warmupWindow ?? 60,
      maxGainPct: partialOptions.maxGainPct ?? policy.takeProfitPct,
      maxLossPct: partialOptions.maxLossPct ?? policy.stopLossPct,
      startingEquity: partialOptions.startingEquity ?? 10_000,
      sampleLimit: partialOptions.sampleLimit ?? 100
    };

    const symbols = [...new Set(settings.universe.map((symbol) => symbol.toUpperCase()).filter(Boolean))];
    const allPointsNested = await Promise.all(
      symbols.map((symbol) => this.symbolPoints(symbol, settings.lookbackDays, settings.warmupWindow))
    );
    const points = allPointsNested.flat().sort((left, right) =>
      left.timestamp.localeCompare(right.timestamp) || left.symbol.localeCompare(right.symbol)
    );

    const result = this.simulator.run(points, {
      minCompositeScore: policy.minCompositeScore,
      minDirectionalProbability: policy.minDirectionalProbability,
      optionLeverage: settings.optionLeverage,
      maxGainPct: settings.maxGainPct,
      maxLossPct: settings.maxLossPct,
      slippageBps: settings.slippageBps,
      commissionPerTrade: settings.commissionPerTrade,
      premiumPerTrade: settings.premiumPerTrade,
      warmupWindow: settings.warmupWindow,
      startingEquity: settings.startingEquity,
      sampleLimit: settings.sampleLimit
    });

    return {
      generatedAt: new Date().toISOString(),
      symbolsUsed: symbols,
      pointsEvaluated: points.length,
      settings,
      result
    };
  }
}
