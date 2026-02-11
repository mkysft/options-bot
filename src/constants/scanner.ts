export const IBKR_SCANNER_CODES = [
  "MOST_ACTIVE",
  "HOT_BY_VOLUME",
  "TOP_PERC_GAIN",
  "TOP_PERC_LOSE",
  "TOP_TRADE_RATE",
  "TOP_PRICE_RANGE",
  "HIGH_OPEN_GAP",
  "LOW_OPEN_GAP"
] as const;

export type IbkrScannerCodeSetting = (typeof IBKR_SCANNER_CODES)[number];

export const DEFAULT_IBKR_SCANNER_CODE: IbkrScannerCodeSetting = "MOST_ACTIVE";

export const isIbkrScannerCodeSetting = (
  value: unknown
): value is IbkrScannerCodeSetting =>
  typeof value === "string" &&
  (IBKR_SCANNER_CODES as readonly string[]).includes(value);
