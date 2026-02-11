import { settings } from "../core/config";
import { logger } from "../core/logger";
import { fetchWithApiLog } from "../utils/fetchWithApiLog";

export interface EodhdScannerResult {
  symbols: string[];
  source: "eodhd_screener" | "disabled" | "unavailable";
  note?: string;
}

export class EodhdAdapter {
  private readonly baseUrl = settings.eodhdBaseUrl || "https://eodhd.com/api/screener";
  private readonly tickerRegex = /^[A-Z][A-Z0-9.\-]{0,14}$/;

  private normalizeTicker(value: unknown): string | null {
    if (typeof value !== "string") return null;
    const raw = value.trim().toUpperCase();
    if (!raw) return null;
    const compact = raw.includes(".") ? raw.split(".")[0] : raw;
    if (!this.tickerRegex.test(compact)) return null;
    return compact;
  }

  async getMarketScannerSymbols(limit = 15): Promise<EodhdScannerResult> {
    const apiKey = settings.eodhdApiKey;
    if (!apiKey) {
      return {
        symbols: [],
        source: "disabled",
        note: "EODHD_API_KEY not configured."
      };
    }

    const cappedLimit = Math.max(5, Math.min(80, Math.round(limit)));
    const filters = [
      ["exchange", "=", "us"],
      ["market_capitalization", ">", 1000000000],
      ["avgvol_1d", ">", 250000],
      ["adjusted_close", ">", 5]
    ];

    const params = new URLSearchParams({
      api_token: apiKey,
      limit: String(cappedLimit),
      offset: "0",
      sort: "market_capitalization.desc",
      filters: JSON.stringify(filters),
      fmt: "json"
    });

    try {
      const url = `${this.baseUrl.replace(/\/$/, "")}?${params.toString()}`;
      const response = await fetchWithApiLog(url, undefined, {
        provider: "eodhd",
        endpoint: "screener",
        reason: "Discover dynamic symbols from EODHD screener",
        requestPayload: {
          limit: cappedLimit,
          filters
        }
      });

      if (!response.ok) {
        return {
          symbols: [],
          source: "unavailable",
          note: `EODHD responded ${response.status}.`
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
          this.normalizeTicker(rowRecord.code) ??
          this.normalizeTicker(rowRecord.symbol) ??
          this.normalizeTicker(rowRecord.ticker);
        if (!symbol) continue;
        symbols.push(symbol);
      }

      const unique = [...new Set(symbols)].slice(0, cappedLimit);
      if (unique.length === 0) {
        return {
          symbols: [],
          source: "unavailable",
          note: "EODHD screener returned no usable symbols."
        };
      }

      return {
        symbols: unique,
        source: "eodhd_screener"
      };
    } catch (error) {
      logger.warn("EODHD screener fetch failed", error);
      return {
        symbols: [],
        source: "unavailable",
        note: (error as Error).message
      };
    }
  }
}
