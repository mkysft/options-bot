import { settings } from "../core/config";
import type { AccountState } from "../types/models";
import { AuditStore } from "../storage/auditStore";
import { ApiRequestLogStore, apiRequestLogStore } from "../storage/apiRequestLogStore";
import { IbkrAdapter } from "../adapters/ibkrAdapter";
import { BacktestService } from "./backtestService";
import { AnalysisService } from "./analysisService";
import { DecisionEngine } from "./decisionEngine";
import { ExecutionGateway } from "./executionGateway";
import { FeatureEngine } from "./featureEngine";
import { LlmJudge } from "./llmJudge";
import { MarketDataService } from "./marketDataService";
import { RiskEngine } from "./riskEngine";
import { ScoringEngine } from "./scoringEngine";
import { BotScheduler } from "./scheduler";
import { RuntimePolicyService } from "./runtimePolicyService";
import { EnvConfigService } from "./envConfigService";
import { AcceptanceGateService } from "./acceptanceGateService";

export interface ServiceContainer {
  auditStore: AuditStore;
  apiRequestLogStore: ApiRequestLogStore;
  ibkrAdapter: IbkrAdapter;
  marketData: MarketDataService;
  backtestService: BacktestService;
  featureEngine: FeatureEngine;
  scoringEngine: ScoringEngine;
  decisionEngine: DecisionEngine;
  analysisService: AnalysisService;
  runtimePolicy: RuntimePolicyService;
  acceptanceGateService: AcceptanceGateService;
  riskEngine: RiskEngine;
  executionGateway: ExecutionGateway;
  scheduler: BotScheduler;
  envConfig: EnvConfigService;
  accountState: AccountState;
}

export const buildContainer = (): ServiceContainer => {
  const auditStore = new AuditStore();
  const requestLogStore = apiRequestLogStore;
  const ibkrAdapter = new IbkrAdapter();
  const runtimePolicy = new RuntimePolicyService(auditStore);
  const acceptanceGateService = new AcceptanceGateService(auditStore);
  const marketData = new MarketDataService(ibkrAdapter);
  marketData.setRuntimePolicy(runtimePolicy);
  const backtestService = new BacktestService(marketData, runtimePolicy);
  const featureEngine = new FeatureEngine();
  const scoringEngine = new ScoringEngine();
  const decisionEngine = new DecisionEngine(runtimePolicy, new LlmJudge());
  const analysisService = new AnalysisService(marketData, featureEngine, scoringEngine, decisionEngine);
  const riskEngine = new RiskEngine(runtimePolicy, auditStore);
  const executionGateway = new ExecutionGateway(auditStore, riskEngine, runtimePolicy, ibkrAdapter);
  const accountState: AccountState = {
    accountEquity: 0,
    dayRealizedPnl: 0,
    dayUnrealizedPnl: 0
  };
  const scheduler = new BotScheduler(
    analysisService,
    auditStore,
    runtimePolicy,
    executionGateway,
    accountState,
    ibkrAdapter,
    acceptanceGateService
  );
  const envConfig = new EnvConfigService(runtimePolicy, scheduler, ibkrAdapter, executionGateway);

  const isTestRuntime =
    settings.appEnv === "test" || process.env.NODE_ENV === "test" || Boolean(process.env.BUN_TEST);
  if (!isTestRuntime) scheduler.start();

  return {
    auditStore,
    apiRequestLogStore: requestLogStore,
    ibkrAdapter,
    marketData,
    backtestService,
    featureEngine,
    scoringEngine,
    decisionEngine,
    analysisService,
    runtimePolicy,
    acceptanceGateService,
    riskEngine,
    executionGateway,
    scheduler,
    envConfig,
    accountState
  };
};
