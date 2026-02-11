import { settings } from "../core/config";
import { logger } from "../core/logger";
import { fetchWithApiLog } from "../utils/fetchWithApiLog";

export interface AlphaNewsArticle {
  title: string;
  source: string;
  url: string;
  publishedAt: string | null;
  summary: string;
  overallSentimentScore: number | null;
  relevanceScore: number | null;
}

export interface AlphaNewsSentimentSnapshot {
  sentiment: number | null;
  articles: AlphaNewsArticle[];
  source: "alpha_vantage_news_sentiment" | "disabled" | "unavailable";
  note?: string;
}

export class AlphaVantageAdapter {
  private readonly baseUrl = "https://www.alphavantage.co/query";
  private readonly dateOnlyRegex = /^\d{4}-\d{2}-\d{2}$/;
  private readonly tickerRegex = /^[A-Z][A-Z0-9.\-]{0,14}$/;

  private parseCsvRow(line: string): string[] {
    const output: string[] = [];
    let current = "";
    let inQuotes = false;

    for (let index = 0; index < line.length; index += 1) {
      const char = line[index];
      if (char === '"') {
        if (inQuotes && line[index + 1] === '"') {
          current += '"';
          index += 1;
        } else {
          inQuotes = !inQuotes;
        }
        continue;
      }
      if (char === "," && !inQuotes) {
        output.push(current.trim());
        current = "";
        continue;
      }
      current += char;
    }

    output.push(current.trim());
    return output;
  }

  private normalizeDate(value: string): string | null {
    const trimmed = value.trim();
    if (!this.dateOnlyRegex.test(trimmed)) return null;
    const parsed = Date.parse(`${trimmed}T00:00:00Z`);
    if (!Number.isFinite(parsed)) return null;
    return trimmed;
  }

  private normalizePublishedAt(value: unknown): string | null {
    if (typeof value !== "string") return null;
    const trimmed = value.trim();
    if (!trimmed) return null;
    const compact = trimmed.replace(/\D/g, "");
    if (compact.length < 8) return null;
    if (compact.length >= 14) {
      const iso = `${compact.slice(0, 4)}-${compact.slice(4, 6)}-${compact.slice(6, 8)}T${compact.slice(8, 10)}:${compact.slice(10, 12)}:${compact.slice(12, 14)}Z`;
      const ms = Date.parse(iso);
      if (Number.isFinite(ms)) return new Date(ms).toISOString();
    }
    const isoDate = `${compact.slice(0, 4)}-${compact.slice(4, 6)}-${compact.slice(6, 8)}T00:00:00Z`;
    const ms = Date.parse(isoDate);
    if (!Number.isFinite(ms)) return null;
    return new Date(ms).toISOString();
  }

  private normalizeTicker(value: unknown): string | null {
    if (typeof value !== "string") return null;
    const symbol = value.trim().toUpperCase();
    if (!this.tickerRegex.test(symbol)) return null;
    return symbol;
  }

  private extractTopMoverSymbols(payload: unknown, limit: number): string[] {
    if (!payload || typeof payload !== "object") return [];
    const data = payload as Record<string, unknown>;
    const groups = [data.top_gainers, data.top_losers, data.most_actively_traded];
    const symbols: string[] = [];

    for (const group of groups) {
      if (!Array.isArray(group)) continue;
      for (const row of group) {
        if (!row || typeof row !== "object") continue;
        const ticker = this.normalizeTicker((row as Record<string, unknown>).ticker);
        if (ticker) symbols.push(ticker);
      }
    }

    return [...new Set(symbols)].slice(0, Math.max(1, Math.min(60, Math.round(limit))));
  }

  async getMarketScannerSymbols(limit = 15): Promise<{
    symbols: string[];
    source: "alpha_vantage_top_movers" | "disabled" | "unavailable";
    note?: string;
  }> {
    const apiKey = settings.alphaVantageApiKey;
    if (!apiKey) {
      return {
        symbols: [],
        source: "disabled",
        note: "ALPHA_VANTAGE_API_KEY not configured."
      };
    }

    const params = new URLSearchParams({
      function: "TOP_GAINERS_LOSERS",
      apikey: apiKey
    });

    try {
      const url = `${this.baseUrl}?${params.toString()}`;
      const response = await fetchWithApiLog(url, undefined, {
        provider: "alpha_vantage",
        endpoint: "TOP_GAINERS_LOSERS",
        reason: "Discover dynamic symbols from Alpha Vantage top movers feed",
        requestPayload: { limit }
      });
      if (!response.ok) {
        return {
          symbols: [],
          source: "unavailable",
          note: `Alpha Vantage returned ${response.status}.`
        };
      }

      const payload = (await response.json()) as Record<string, unknown>;
      const note = [payload.Note, payload.Information, payload["Error Message"]]
        .filter((item): item is string => typeof item === "string" && item.trim().length > 0)
        .join(" | ");
      if (note.length > 0) {
        return {
          symbols: [],
          source: "unavailable",
          note
        };
      }

      const symbols = this.extractTopMoverSymbols(payload, limit);
      if (symbols.length === 0) {
        return {
          symbols: [],
          source: "unavailable",
          note: "Top movers feed returned no symbols."
        };
      }

      return {
        symbols,
        source: "alpha_vantage_top_movers"
      };
    } catch (error) {
      logger.warn("Alpha Vantage top movers fetch failed", error);
      return {
        symbols: [],
        source: "unavailable",
        note: (error as Error).message
      };
    }
  }

  async getNextEarningsDate(
    symbol: string
  ): Promise<{ eventDate: string | null; source: "alpha_vantage_earnings_calendar" | "disabled" | "unavailable" }> {
    const apiKey = settings.alphaVantageApiKey;
    if (!apiKey) return { eventDate: null, source: "disabled" };

    const symbolKey = symbol.trim().toUpperCase();
    const params = new URLSearchParams({
      function: "EARNINGS_CALENDAR",
      symbol: symbolKey,
      horizon: "3month",
      apikey: apiKey
    });

    try {
      const url = `${this.baseUrl}?${params.toString()}`;
      const response = await fetchWithApiLog(url, undefined, {
        provider: "alpha_vantage",
        endpoint: "EARNINGS_CALENDAR",
        reason: `Fetch next earnings date for ${symbolKey}`,
        requestPayload: { symbol: symbolKey, horizon: "3month" }
      });
      if (!response.ok) return { eventDate: null, source: "unavailable" };

      const raw = await response.text();
      if (!raw) return { eventDate: null, source: "unavailable" };

      // API throttling/notice payloads can come back as JSON strings.
      if (raw.trimStart().startsWith("{")) {
        try {
          const payload = JSON.parse(raw) as Record<string, unknown>;
          if (
            typeof payload.Note === "string" ||
            typeof payload.Information === "string" ||
            typeof payload["Error Message"] === "string"
          ) {
            return { eventDate: null, source: "unavailable" };
          }
        } catch {
          // fall through and attempt CSV parsing
        }
      }

      const lines = raw
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter(Boolean);
      if (lines.length <= 1) return { eventDate: null, source: "unavailable" };

      const header = this.parseCsvRow(lines[0]).map((value) => value.toLowerCase());
      const symbolIndex = header.indexOf("symbol");
      const reportDateIndex = header.indexOf("reportdate");
      if (symbolIndex < 0 || reportDateIndex < 0) {
        return { eventDate: null, source: "unavailable" };
      }

      const today = new Date();
      today.setUTCHours(0, 0, 0, 0);
      const todayMs = today.getTime();

      let nextDate: string | null = null;
      let nextMs = Number.POSITIVE_INFINITY;
      for (let index = 1; index < lines.length; index += 1) {
        const row = this.parseCsvRow(lines[index]);
        const rowSymbol = (row[symbolIndex] ?? "").trim().toUpperCase();
        if (rowSymbol !== symbolKey) continue;
        const normalizedDate = this.normalizeDate(row[reportDateIndex] ?? "");
        if (!normalizedDate) continue;
        const eventMs = Date.parse(`${normalizedDate}T00:00:00Z`);
        if (!Number.isFinite(eventMs)) continue;
        if (eventMs < todayMs) continue;
        if (eventMs < nextMs) {
          nextMs = eventMs;
          nextDate = normalizedDate;
        }
      }

      return {
        eventDate: nextDate,
        source: "alpha_vantage_earnings_calendar"
      };
    } catch (error) {
      logger.warn("Alpha Vantage earnings calendar fetch failed", symbolKey, error);
      return { eventDate: null, source: "unavailable" };
    }
  }

  async getNewsSentimentSnapshot(symbol: string, articleLimit = 8): Promise<AlphaNewsSentimentSnapshot> {
    const apiKey = settings.alphaVantageApiKey;
    if (!apiKey) {
      return {
        sentiment: null,
        articles: [],
        source: "disabled",
        note: "ALPHA_VANTAGE_API_KEY not configured."
      };
    }
    const symbolKey = symbol.toUpperCase();
    const params = new URLSearchParams({
      function: "NEWS_SENTIMENT",
      tickers: symbolKey,
      apikey: apiKey,
      limit: "25",
      sort: "LATEST"
    });

    try {
      const url = `${this.baseUrl}?${params.toString()}`;
      const response = await fetchWithApiLog(url, undefined, {
        provider: "alpha_vantage",
        endpoint: "NEWS_SENTIMENT",
        reason: `Fetch news sentiment for ${symbolKey}`,
        requestPayload: {
          symbol: symbolKey,
          limit: 25
        }
      });
      if (!response.ok) {
        return {
          sentiment: null,
          articles: [],
          source: "unavailable",
          note: `Alpha Vantage returned ${response.status}.`
        };
      }
      const payload = (await response.json()) as {
        feed?: Array<
          {
            title?: string;
            source?: string;
            url?: string;
            summary?: string;
            time_published?: string;
            overall_sentiment_score?: string;
            ticker_sentiment?: Array<{
              ticker?: string;
              ticker_sentiment_score?: string;
              relevance_score?: string;
            }>;
          }
        >;
        Note?: string;
        Information?: string;
        ["Error Message"]?: string;
      };
      const note = [payload.Note, payload.Information, payload["Error Message"]]
        .filter((item): item is string => typeof item === "string" && item.trim().length > 0)
        .join(" | ");
      if (note.length > 0) {
        return {
          sentiment: null,
          articles: [],
          source: "unavailable",
          note
        };
      }

      const values: number[] = [];
      const articles: AlphaNewsArticle[] = [];
      for (const item of payload.feed ?? []) {
        let articleSentiment: number | null = null;
        let articleRelevance: number | null = null;
        const overallSentimentRaw = Number(item.overall_sentiment_score ?? Number.NaN);
        const overallSentiment = Number.isFinite(overallSentimentRaw)
          ? overallSentimentRaw
          : null;
        for (const row of item.ticker_sentiment ?? []) {
          if (row.ticker?.toUpperCase() !== symbolKey) continue;
          const parsed = Number(row.ticker_sentiment_score ?? 0);
          const relevance = Number(row.relevance_score ?? Number.NaN);
          if (Number.isFinite(parsed)) values.push(parsed);
          if (Number.isFinite(parsed) && articleSentiment === null) articleSentiment = parsed;
          if (Number.isFinite(relevance) && articleRelevance === null) articleRelevance = relevance;
        }
        if (articleSentiment === null && overallSentiment !== null) {
          articleSentiment = overallSentiment;
          values.push(overallSentiment);
        }
        if (articleSentiment === null) continue;
        if (articles.length >= Math.max(1, Math.min(20, Math.round(articleLimit)))) continue;
        articles.push({
          title: String(item.title ?? "").trim() || "Untitled",
          source: String(item.source ?? "unknown").trim() || "unknown",
          url: String(item.url ?? "").trim(),
          publishedAt: this.normalizePublishedAt(item.time_published),
          summary: String(item.summary ?? "").trim(),
          overallSentimentScore: overallSentiment,
          relevanceScore: articleRelevance
        });
      }
      const sentiment =
        values.length === 0 ? null : values.reduce((acc, value) => acc + value, 0) / values.length;
      return {
        sentiment,
        articles,
        source: "alpha_vantage_news_sentiment"
      };
    } catch (error) {
      logger.warn("Alpha Vantage sentiment fetch failed", symbol, error);
      return {
        sentiment: null,
        articles: [],
        source: "unavailable",
        note: (error as Error).message
      };
    }
  }

  async getNewsSentiment(symbol: string): Promise<number | null> {
    const snapshot = await this.getNewsSentimentSnapshot(symbol);
    return snapshot.sentiment;
  }
}
