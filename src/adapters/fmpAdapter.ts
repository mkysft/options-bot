import { settings } from "../core/config";
import { logger } from "../core/logger";
import { fetchWithApiLog } from "../utils/fetchWithApiLog";

export interface FmpScannerResult {
  symbols: string[];
  source: "fmp_company_screener" | "disabled" | "unavailable";
  note?: string;
}

export class FmpAdapter {
  private readonly baseUrl = settings.fmpBaseUrl || "https://financialmodelingprep.com";
  private readonly tickerRegex = /^[A-Z][A-Z0-9.\-]{0,14}$/;

  private normalizeTicker(value: unknown): string | null {
    if (typeof value !== "string") return null;
    const symbol = value.trim().toUpperCase();
    if (!this.tickerRegex.test(symbol)) return null;
    return symbol;
  }

  async getMarketScannerSymbols(limit = 15): Promise<FmpScannerResult> {
    const apiKey = settings.fmpApiKey;
    if (!apiKey) {
      return {
        symbols: [],
        source: "disabled",
        note: "FMP_API_KEY not configured."
      };
    }

    const cappedLimit = Math.max(5, Math.min(80, Math.round(limit)));
    const params = new URLSearchParams({
      apikey: apiKey,
      limit: String(cappedLimit),
      marketCapMoreThan: "1000000000",
      volumeMoreThan: "250000",
      priceMoreThan: "5",
      isActivelyTrading: "true"
    });

    try {
      const url = `${this.baseUrl.replace(/\/$/, "")}/stable/company-screener?${params.toString()}`;
      const response = await fetchWithApiLog(url, undefined, {
        provider: "fmp",
        endpoint: "company-screener",
        reason: "Discover dynamic symbols from FMP company screener",
        requestPayload: {
          limit: cappedLimit
        }
      });

      if (!response.ok) {
        return {
          symbols: [],
          source: "unavailable",
          note: `FMP responded ${response.status}.`
        };
      }

      const payload = (await response.json()) as unknown;
      const rows = Array.isArray(payload)
        ? payload
        : payload && typeof payload === "object" && Array.isArray((payload as { data?: unknown[] }).data)
          ? ((payload as { data: unknown[] }).data ?? [])
          : [];

      const symbols: string[] = [];
      for (const row of rows) {
        if (!row || typeof row !== "object") continue;
        const rowRecord = row as Record<string, unknown>;
        const symbol =
          this.normalizeTicker(rowRecord.symbol) ??
          this.normalizeTicker(rowRecord.ticker) ??
          this.normalizeTicker(rowRecord.code);
        if (!symbol) continue;
        symbols.push(symbol);
      }

      const unique = [...new Set(symbols)].slice(0, cappedLimit);
      if (unique.length === 0) {
        return {
          symbols: [],
          source: "unavailable",
          note: "FMP screener returned no usable symbols."
        };
      }

      return {
        symbols: unique,
        source: "fmp_company_screener"
      };
    } catch (error) {
      logger.warn("FMP screener fetch failed", error);
      return {
        symbols: [],
        source: "unavailable",
        note: (error as Error).message
      };
    }
  }
}
