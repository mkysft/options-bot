import { z } from "zod";
import { IBKR_SCANNER_CODES } from "../constants/scanner";

export const scanRequestSchema = z.object({
  universe: z.array(z.string().min(1)).optional(),
  topN: z.number().int().positive().max(100).default(10)
});

export const scoreRequestSchema = z.object({
  symbol: z.string().min(1),
  universe: z.array(z.string().min(1)).optional()
});

export const decisionRequestSchema = z.object({
  symbol: z.string().min(1),
  universe: z.array(z.string().min(1)).optional()
});

export const backtestRequestSchema = z.object({
  universe: z.array(z.string().min(1)).optional(),
  lookbackDays: z.number().int().min(60).max(1_000).default(220),
  slippageBps: z.number().min(0).max(100).default(12),
  commissionPerTrade: z.number().min(0).max(25).default(0.65),
  premiumPerTrade: z.number().min(10).max(10_000).default(250),
  optionLeverage: z.number().min(0.5).max(20).default(4),
  warmupWindow: z.number().int().min(20).max(260).default(60),
  maxGainPct: z.number().min(0.05).max(5).optional(),
  maxLossPct: z.number().min(0.05).max(2).optional(),
  startingEquity: z.number().min(500).max(10_000_000).default(10_000),
  sampleLimit: z.number().int().min(10).max(500).default(100)
});

export const proposeOrderRequestSchema = z.object({
  symbol: z.string().min(1),
  universe: z.array(z.string().min(1)).optional()
});

export const approveOrderRequestSchema = z.object({
  orderId: z.string().uuid(),
  approve: z.boolean(),
  comment: z.string().max(400).default("")
});

export const ibkrLaunchRequestSchema = z.object({
  target: z.enum(["gateway", "tws"]).optional()
});

export const recommendationsQuerySchema = z.object({
  topN: z.coerce.number().int().positive().max(100).optional(),
  universe: z
    .string()
    .optional()
    .transform((value) =>
      value
        ?.split(",")
        .map((symbol) => symbol.trim().toUpperCase())
        .filter(Boolean)
    )
});

export const apiRequestLogsQuerySchema = z.object({
  limit: z.coerce.number().int().min(1).max(2_000).optional(),
  direction: z.enum(["internal", "external"]).optional(),
  status: z.enum(["success", "error"]).optional(),
  provider: z.string().trim().min(1).max(128).optional(),
  endpointContains: z.string().trim().min(1).max(256).optional()
});

export const marketDataDiagnosticsQuerySchema = z.object({
  windowMinutes: z.coerce.number().int().min(15).max(24 * 60).default(360),
  limitSymbols: z.coerce.number().int().min(5).max(200).default(40)
});

export const botPolicyPatchSchema = z
  .object({
    scanTopN: z.number().int().positive().max(100).optional(),
    ibkrScanCode: z.enum(IBKR_SCANNER_CODES).optional(),
    analysisDataProvider: z.enum(["AUTO", "ALPACA", "IBKR"]).optional(),
    autoProposeActionable: z.boolean().optional(),
    minCompositeScore: z.number().min(-300).max(300).optional(),
    minDirectionalProbability: z.number().min(0.5).max(0.99).optional(),
    dteMin: z.number().int().min(1).max(90).optional(),
    dteMax: z.number().int().min(1).max(120).optional(),
    maxPremiumRiskPct: z.number().min(0.001).max(0.2).optional(),
    dailyDrawdownLimitPct: z.number().min(0.005).max(0.5).optional(),
    correlationCapPerDirection: z.number().int().min(1).max(10).optional(),
    takeProfitPct: z.number().min(0.05).max(5).optional(),
    stopLossPct: z.number().min(0.05).max(1).optional(),
    maxHoldDays: z.number().int().min(1).max(30).optional(),
    preEventExitWindowHours: z.number().int().min(0).max(168).optional(),
    preEventSecFilingLookbackHours: z.number().int().min(0).max(168).optional(),
    preEventSecFilingRiskThreshold: z.number().min(0.1).max(1).optional(),
    universeSymbols: z.array(z.string().min(1)).optional()
  })
  .refine((value) => Object.keys(value).length > 0, {
    message: "At least one policy field must be provided."
  });

export const envConfigPatchSchema = z
  .object({
    values: z.record(
      z
        .string()
        .regex(/^[A-Z_][A-Z0-9_]*$/, "Env keys must be uppercase snake_case."),
      z.string().max(4096)
    )
  })
  .refine((value) => Object.keys(value.values).length > 0, {
    message: "At least one env value must be provided."
  });

export const killSwitchUpdateSchema = z.object({
  enabled: z.boolean(),
  reason: z.string().max(300).default("")
});
