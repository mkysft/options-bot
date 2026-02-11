import { settings } from "../core/config";
import { logger } from "../core/logger";
import type { AccountState } from "../types/models";
import { AuditStore } from "../storage/auditStore";
import { AnalysisService } from "./analysisService";
import { ExecutionGateway } from "./executionGateway";
import { RuntimePolicyService } from "./runtimePolicyService";
import { IbkrAdapter } from "../adapters/ibkrAdapter";
import { AcceptanceGateService } from "./acceptanceGateService";

export class BotScheduler {
  private timer: Timer | null = null;
  private intervalMs = 0;
  private nextRunAtMs: number | null = null;
  private lastRunStartedAtMs: number | null = null;
  private lastRunFinishedAtMs: number | null = null;
  private lastRunError: string | null = null;
  private runInFlight = false;

  constructor(
    private readonly analysisService: AnalysisService,
    private readonly auditStore: AuditStore,
    private readonly runtimePolicy: RuntimePolicyService,
    private readonly executionGateway: ExecutionGateway,
    private readonly accountState: AccountState,
    private readonly ibkrAdapter: IbkrAdapter,
    private readonly acceptanceGate?: AcceptanceGateService
  ) {}

  start(): void {
    if (this.timer) return;
    this.intervalMs = settings.scanCadenceMinutes * 60 * 1000;
    this.nextRunAtMs = Date.now() + this.intervalMs;

    this.timer = setInterval(async () => {
      await this.runScheduledScan();
    }, this.intervalMs);

    logger.info(`Scheduler started: every ${settings.scanCadenceMinutes} minutes`);
  }

  stop(): void {
    if (!this.timer) return;
    clearInterval(this.timer);
    this.timer = null;
    this.nextRunAtMs = null;
  }

  reloadInterval(): void {
    const wasRunning = Boolean(this.timer);
    this.stop();
    if (wasRunning) this.start();
  }

  getRuntimeStatus(nowMs = Date.now()): {
    running: boolean;
    intervalMs: number;
    intervalMinutes: number;
    inFlight: boolean;
    lastRunStartedAt: string | null;
    lastRunFinishedAt: string | null;
    lastRunStatus: "idle" | "running" | "success" | "error";
    lastRunError: string | null;
    nextAutoRunAt: string | null;
    nextAutoRunInMs: number | null;
  } {
    const running = Boolean(this.timer);
    const lastRunStatus =
      this.runInFlight
        ? "running"
        : this.lastRunError
          ? "error"
          : this.lastRunFinishedAtMs
            ? "success"
            : "idle";

    const nextInMs =
      running && this.nextRunAtMs !== null ? Math.max(0, this.nextRunAtMs - nowMs) : null;

    return {
      running,
      intervalMs: this.intervalMs,
      intervalMinutes: this.intervalMs > 0 ? this.intervalMs / (60 * 1000) : settings.scanCadenceMinutes,
      inFlight: this.runInFlight,
      lastRunStartedAt:
        this.lastRunStartedAtMs !== null ? new Date(this.lastRunStartedAtMs).toISOString() : null,
      lastRunFinishedAt:
        this.lastRunFinishedAtMs !== null ? new Date(this.lastRunFinishedAtMs).toISOString() : null,
      lastRunStatus,
      lastRunError: this.lastRunError,
      nextAutoRunAt: this.nextRunAtMs !== null ? new Date(this.nextRunAtMs).toISOString() : null,
      nextAutoRunInMs: nextInMs
    };
  }

  private async runScheduledScan(): Promise<void> {
    this.runInFlight = true;
    const startedMs = Date.now();
    this.lastRunStartedAtMs = startedMs;
    if (this.intervalMs > 0) {
      this.nextRunAtMs = startedMs + this.intervalMs;
    }

    try {
      await this.runScheduledScanInternal();
      this.lastRunError = null;
    } catch (error) {
      this.lastRunError = (error as Error).message;
      logger.error("Scheduled scan failed", error);
    } finally {
      this.lastRunFinishedAtMs = Date.now();
      this.runInFlight = false;
    }
  }

  private async runScheduledScanInternal(): Promise<void> {
    const connectivity = await this.ibkrAdapter.checkConnectivity(4_000);
    this.executionGateway.notifyConnectivityStatus(connectivity);
    if (!connectivity.reachable) {
      this.auditStore.logEvent("scheduled_scan_skipped", {
        generatedAt: new Date().toISOString(),
        reason: "ibkr_disconnected",
        host: connectivity.host,
        port: connectivity.port,
        message: connectivity.message
      });
      return;
    }

    await this.executionGateway.refreshBrokerStatuses();
    await this.executionGateway.syncAccountState(this.accountState);
    const exitOrders = await this.executionGateway.runExitAutomation(this.accountState);

    const policy = this.runtimePolicy.getPolicy();
    const scanned = await this.analysisService.scanUniverseWithDiscovery(
      policy.universeSymbols,
      Math.min(policy.scanTopN, 5),
      {
        ibkrScanCode: policy.ibkrScanCode
      }
    );
    const analyses = scanned.analyses;
    this.auditStore.logEvent("scheduled_scan", {
      generatedAt: new Date().toISOString(),
      accountEquity: this.accountState.accountEquity,
      dayRealizedPnl: this.accountState.dayRealizedPnl,
      dayUnrealizedPnl: this.accountState.dayUnrealizedPnl,
      exitOrdersProposed: exitOrders.length,
      evaluatedUniverseSize: scanned.evaluatedUniverse.length,
      discoveredSymbols: scanned.discoveredSymbols,
      scannerUsed: scanned.scannerUsed,
      scannerSource: scanned.scannerSource,
      ibkrScanCode: scanned.ibkrScanCode,
      scannerProvidersUsed: scanned.scannerProvidersUsed,
      scannerProvidersTried: scanned.scannerProvidersTried,
      topCandidates: analyses.map((entry) => ({
        symbol: entry.snapshot.symbol,
        compositeScore: entry.scoreCard.compositeScore,
        upProb: entry.featureVector.directionalUpProb,
        downProb: entry.featureVector.directionalDownProb
      }))
    });
    this.acceptanceGate?.refreshSnapshot();
  }
}
