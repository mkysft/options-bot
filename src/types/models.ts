export type OptionRight = "CALL" | "PUT";
export type TradeAction = "CALL" | "PUT" | "NO_TRADE";
export type OrderStatus =
  | "PENDING_APPROVAL"
  | "REJECTED_BY_USER"
  | "BLOCKED_RISK"
  | "SUBMITTED_PAPER"
  | "SUBMITTED_LIVE"
  | "FILLED"
  | "CANCELLED"
  | "EXITED";

export type OrderIntentType = "ENTRY" | "EXIT";
export type OrderSide = "BUY" | "SELL";

export interface SymbolSnapshot {
  symbol: string;
  timestamp: string;
  last: number;
  bid: number;
  ask: number;
  volume: number;
  impliedVol: number;
  realizedVol: number;
  pctChange1d: number;
  spreadPct: number;
}

export interface OptionContractSnapshot {
  symbol: string;
  expiration: string;
  strike: number;
  right: OptionRight;
  bid: number;
  ask: number;
  last: number;
  volume: number;
  openInterest: number;
  impliedVol: number;
  delta: number;
  gamma: number;
  quoteSource?:
    | "ibkr_option_quote"
    | "alpaca_option_quote"
    | "derived_contract"
    | "synthetic_option_chain";
}

export interface DailyBar {
  timestamp: string | null;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface FeatureVector {
  symbol: string;
  timestamp: string;
  momentum: number;
  trend: number;
  adx14: number;
  regime: number;
  regimeStability: number;
  atrPct: number;
  realizedVolPercentile: number;
  breakoutZ: number;
  relativeStrength20d: number;
  relativeStrength60d: number;
  relativeVolume20d: number;
  ivRvSpread: number;
  liquidity: number;
  flow: number;
  skew: number;
  optionsQuality: number;
  newsSentiment: number;
  newsVelocity24h: number;
  newsSentimentDispersion: number;
  newsFreshness: number;
  eventBias: number;
  macroRegime: number;
  spreadPct: number;
  eventRisk: number;
  gapRisk: number;
  directionalUpProb: number;
  directionalDownProb: number;
}

export interface ScoreCard {
  symbol: string;
  timestamp: string;
  techScore: number;
  optionsScore: number;
  sentimentScore: number;
  riskPenalty: number;
  compositeScore: number;
}

export interface DecisionCard {
  symbol: string;
  timestamp: string;
  action: TradeAction;
  confidence: number;
  rationale: string;
  vetoFlags: string[];
  scoreCard: ScoreCard;
}

export interface OrderIntent {
  id: string;
  createdAt: string;
  updatedAt?: string;
  intentType: OrderIntentType;
  side: OrderSide;
  parentOrderId?: string;
  exitReason?: string;
  symbol: string;
  action: TradeAction;
  optionContract: OptionContractSnapshot;
  quantity: number;
  limitPrice: number;
  filledQuantity?: number;
  avgFillPrice?: number;
  brokerOrderId?: number;
  status: OrderStatus;
  riskNotes: string[];
  decision: DecisionCard;
}

export interface RiskState {
  timestamp: string;
  accountEquity: number;
  dayRealizedPnl: number;
  dayUnrealizedPnl: number;
  dailyDrawdownPct: number;
  halted: boolean;
  haltReasons: string[];
  openPositions: number;
  openSameDirectionCorrelated: number;
}

export interface AuditRecord {
  id: string;
  timestamp: string;
  eventType: string;
  payload: Record<string, unknown>;
}

export interface SymbolAnalysis {
  snapshot: SymbolSnapshot;
  featureVector: FeatureVector;
  scoreCard: ScoreCard;
}

export interface AccountState {
  accountEquity: number;
  dayRealizedPnl: number;
  dayUnrealizedPnl: number;
}
