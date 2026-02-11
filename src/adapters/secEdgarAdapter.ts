import { logger } from "../core/logger";
import { fetchWithApiLog } from "../utils/fetchWithApiLog";

interface EdgarTickerMapEntry {
  ticker?: string;
  cik_str?: number;
}

export interface SecEventSnapshot {
  eventBias: number;
  eventRisk: number;
  source: "sec_edgar" | "fallback";
  cik: string | null;
  latestFilingDate: string | null;
  latestForm: string | null;
  note?: string;
}

export class SecEdgarAdapter {
  private readonly userAgent = "options-bot research contact@example.com";
  private tickerMap = new Map<string, string>();
  private loaded = false;

  private async loadTickerMap(): Promise<void> {
    if (this.loaded) return;
    this.loaded = true;
    try {
      const response = await fetchWithApiLog(
        "https://www.sec.gov/files/company_tickers.json",
        {
          headers: { "User-Agent": this.userAgent, "Accept-Encoding": "gzip, deflate" }
        },
        {
          provider: "sec_edgar",
          endpoint: "company_tickers",
          reason: "Load SEC ticker-to-CIK map"
        }
      );
      if (!response.ok) return;
      const payload = (await response.json()) as Record<string, EdgarTickerMapEntry>;
      for (const entry of Object.values(payload)) {
        const symbol = entry.ticker?.toUpperCase();
        const cik = String(entry.cik_str ?? "").padStart(10, "0");
        if (symbol) this.tickerMap.set(symbol, cik);
      }
    } catch (error) {
      logger.warn("SEC ticker map load failed", error);
    }
  }

  async getEventBiasAndRiskSnapshot(symbol: string): Promise<SecEventSnapshot> {
    await this.loadTickerMap();
    const symbolKey = symbol.toUpperCase();
    const cik = this.tickerMap.get(symbolKey);
    if (!cik) {
      return {
        eventBias: 0,
        eventRisk: 0.2,
        source: "fallback",
        cik: null,
        latestFilingDate: null,
        latestForm: null,
        note: "Ticker not found in SEC mapping."
      };
    }

    try {
      const url = `https://data.sec.gov/submissions/CIK${cik}.json`;
      const response = await fetchWithApiLog(
        url,
        {
          headers: { "User-Agent": this.userAgent, "Accept-Encoding": "gzip, deflate" }
        },
        {
          provider: "sec_edgar",
          endpoint: `submissions/CIK${cik}`,
          reason: `Fetch filing context for ${symbolKey}`,
          requestPayload: {
            symbol: symbolKey,
            cik
          }
        }
      );
      if (!response.ok) {
        return {
          eventBias: 0,
          eventRisk: 0.2,
          source: "fallback",
          cik,
          latestFilingDate: null,
          latestForm: null,
          note: `SEC responded ${response.status}.`
        };
      }
      const payload = (await response.json()) as {
        filings?: { recent?: { filingDate?: string[]; form?: string[] } };
      };
      const filingDate = payload.filings?.recent?.filingDate?.[0];
      const form = payload.filings?.recent?.form?.[0] ?? "";
      if (!filingDate) {
        return {
          eventBias: 0,
          eventRisk: 0.2,
          source: "fallback",
          cik,
          latestFilingDate: null,
          latestForm: form || null,
          note: "Recent filing metadata unavailable."
        };
      }

      const latest = new Date(filingDate).getTime();
      const daysSince = Math.max(0, Math.floor((Date.now() - latest) / (1000 * 60 * 60 * 24)));
      const binaryForms = new Set(["8-K", "10-Q", "10-K", "S-1", "DEF 14A"]);
      const eventRisk = binaryForms.has(form) && daysSince <= 3 ? 0.55 : 0.2;
      return {
        eventBias: 0,
        eventRisk,
        source: "sec_edgar",
        cik,
        latestFilingDate: filingDate,
        latestForm: form || null
      };
    } catch (error) {
      logger.warn("SEC submission fetch failed", symbolKey, error);
      return {
        eventBias: 0,
        eventRisk: 0.2,
        source: "fallback",
        cik,
        latestFilingDate: null,
        latestForm: null,
        note: (error as Error).message
      };
    }
  }

  async getEventBiasAndRisk(symbol: string): Promise<{ eventBias: number; eventRisk: number }> {
    const snapshot = await this.getEventBiasAndRiskSnapshot(symbol);
    return {
      eventBias: snapshot.eventBias,
      eventRisk: snapshot.eventRisk
    };
  }
}
