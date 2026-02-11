import {
  DEFAULT_IBKR_SCANNER_CODE,
  type IbkrScannerCodeSetting
} from "../constants/scanner";
import { settings } from "../core/config";
import { logger } from "../core/logger";
import { fetchWithApiLog } from "../utils/fetchWithApiLog";

export interface AiDiscoveryScannerResult {
  symbols: string[];
  source: "openai_symbol_discovery" | "disabled" | "unavailable";
  note?: string;
}

interface DiscoveryCacheEntry {
  key: string;
  symbols: string[];
  generatedAtMs: number;
  expiresAtMs: number;
}

const MIN_COOLDOWN_MS = 5_000;
const MAX_COOLDOWN_MS = 10 * 60_000;

export class AiDiscoveryAdapter {
  private readonly tickerRegex = /^[A-Z][A-Z0-9.\-]{0,14}$/;
  private cache: DiscoveryCacheEntry | null = null;
  private inFlight: Promise<AiDiscoveryScannerResult> | null = null;
  private inFlightKey: string | null = null;
  private rateLimitedUntilMs = 0;

  private cacheTtlMs(): number {
    const minutes = Number(settings.aiDiscoveryCacheTtlMinutes ?? 10);
    if (!Number.isFinite(minutes) || minutes <= 0) return 10 * 60_000;
    return Math.max(30_000, Math.round(minutes * 60_000));
  }

  private parseRetryAfterMs(value: string | null): number | undefined {
    if (!value) return undefined;
    const trimmed = value.trim();
    if (!trimmed) return undefined;

    const asSeconds = Number(trimmed);
    if (Number.isFinite(asSeconds)) {
      return Math.max(0, Math.round(asSeconds * 1000));
    }

    const asDate = Date.parse(trimmed);
    if (Number.isFinite(asDate)) {
      return Math.max(0, asDate - Date.now());
    }

    return undefined;
  }

  private normalizeTicker(value: unknown): string | null {
    if (typeof value !== "string") return null;
    const symbol = value.trim().toUpperCase();
    if (!symbol) return null;
    if (!this.tickerRegex.test(symbol)) return null;
    return symbol;
  }

  private collectSymbols(input: unknown, output: string[], maxCount: number): void {
    if (output.length >= maxCount) return;

    if (Array.isArray(input)) {
      for (const item of input) {
        this.collectSymbols(item, output, maxCount);
        if (output.length >= maxCount) return;
      }
      return;
    }

    if (typeof input === "string") {
      const normalized = this.normalizeTicker(input);
      if (normalized) output.push(normalized);
      return;
    }

    if (!input || typeof input !== "object") return;

    const record = input as Record<string, unknown>;
    const direct =
      this.normalizeTicker(record.symbol) ??
      this.normalizeTicker(record.ticker) ??
      this.normalizeTicker(record.code);
    if (direct) output.push(direct);

    for (const [key, value] of Object.entries(record)) {
      if (["symbols", "tickers", "ideas", "candidates", "rows", "list"].includes(key)) {
        this.collectSymbols(value, output, maxCount);
      }
    }
  }

  private extractOutputText(payload: {
    output_text?: string;
    output?: Array<{ content?: Array<{ text?: string }> }>;
  }): string {
    return (
      payload.output_text ??
      payload.output?.flatMap((item) => item.content ?? []).map((item) => item.text ?? "").join("\n") ??
      ""
    );
  }

  private parseJsonPayload(text: string): unknown {
    const trimmed = text.trim();
    const candidates: string[] = [];
    if (trimmed.length > 0) candidates.push(trimmed);

    const codeBlockMatch = trimmed.match(/```(?:json)?\s*([\s\S]*?)```/i);
    if (codeBlockMatch?.[1]) candidates.push(codeBlockMatch[1].trim());

    const firstObj = trimmed.indexOf("{");
    const lastObj = trimmed.lastIndexOf("}");
    if (firstObj >= 0 && lastObj > firstObj) {
      candidates.push(trimmed.slice(firstObj, lastObj + 1).trim());
    }

    const firstArr = trimmed.indexOf("[");
    const lastArr = trimmed.lastIndexOf("]");
    if (firstArr >= 0 && lastArr > firstArr) {
      candidates.push(trimmed.slice(firstArr, lastArr + 1).trim());
    }

    for (const candidate of [...new Set(candidates)]) {
      if (!candidate) continue;
      try {
        return JSON.parse(candidate);
      } catch {
        // try next
      }
    }

    throw new SyntaxError(`Unable to parse AI discovery JSON response: ${trimmed.slice(0, 160)}`);
  }

  private scanModeHint(mode: IbkrScannerCodeSetting): string {
    switch (mode) {
      case "TOP_PERC_GAIN":
        return "bias toward strongest recent gainers";
      case "TOP_PERC_LOSE":
        return "bias toward strongest recent decliners";
      case "HOT_BY_VOLUME":
      case "MOST_ACTIVE":
        return "bias toward highest traded volume and liquidity";
      case "TOP_TRADE_RATE":
        return "bias toward symbols with elevated trading activity and frequent prints";
      case "TOP_PRICE_RANGE":
        return "bias toward symbols with expanded intraday ranges";
      case "HIGH_OPEN_GAP":
        return "bias toward upside gap candidates";
      case "LOW_OPEN_GAP":
        return "bias toward downside gap candidates";
      default:
        return "bias toward liquid high-attention symbols";
    }
  }

  private buildPrompt(limit: number, mode: IbkrScannerCodeSetting): string {
    const currentDate = new Date().toISOString().slice(0, 10);
    return [
      "You are building a US options scanning universe.",
      `Current date: ${currentDate}.`,
      `Goal: return up to ${limit} US-listed stocks/ETFs that are liquid and suitable for short-term options analysis.`,
      `Selection mode: ${mode} (${this.scanModeHint(mode)}).`,
      "Hard rules:",
      "- Use only real US symbols.",
      "- Prefer highly liquid names with active options chains.",
      "- Avoid penny stocks, warrants, rights, and obvious illiquid tickers.",
      "- Include both ETFs and large-cap stocks when relevant.",
      'Return strict JSON only: {"symbols":["SPY","QQQ"],"note":"optional short note"}',
      "Do not include markdown."
    ].join("\n");
  }

  private async requestSymbols(
    limit: number,
    mode: IbkrScannerCodeSetting,
    apiKey: string
  ): Promise<AiDiscoveryScannerResult> {
    const model = settings.openAiModel || "gpt-4.1-mini";
    const responseBody = {
      model,
      input: [
        {
          role: "system",
          content: "Return only compact JSON and never include markdown formatting."
        },
        {
          role: "user",
          content: this.buildPrompt(limit, mode)
        }
      ]
    };

    const response = await fetchWithApiLog(
      "https://api.openai.com/v1/responses",
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${apiKey}`,
          "Content-Type": "application/json"
        },
        body: JSON.stringify(responseBody)
      },
      {
        provider: "openai",
        endpoint: "responses",
        reason: `AI symbol discovery (${limit} symbols, mode=${mode})`,
        requestPayload: {
          model,
          limit,
          mode
        }
      }
    );

    if (!response.ok) {
      const retryAfterMs = this.parseRetryAfterMs(response.headers.get("retry-after"));
      if (response.status === 429) {
        const cooldownMs = Math.max(
          MIN_COOLDOWN_MS,
          Math.min(MAX_COOLDOWN_MS, retryAfterMs ?? 60_000)
        );
        this.rateLimitedUntilMs = Date.now() + cooldownMs;
      }

      let detail = "";
      try {
        detail = (await response.text()).slice(0, 280).replace(/\s+/g, " ").trim();
      } catch {
        detail = "";
      }

      return {
        symbols: [],
        source: "unavailable",
        note: `OpenAI responded ${response.status}${detail ? `: ${detail}` : ""}`
      };
    }

    const payload = (await response.json()) as {
      output_text?: string;
      output?: Array<{ content?: Array<{ text?: string }> }>;
    };
    const text = this.extractOutputText(payload);
    const parsed = this.parseJsonPayload(text);

    const symbols: string[] = [];
    this.collectSymbols(parsed, symbols, limit * 3);
    const unique = [...new Set(symbols)].slice(0, limit);
    if (unique.length === 0) {
      return {
        symbols: [],
        source: "unavailable",
        note: "AI discovery returned no usable symbols."
      };
    }

    const note =
      parsed && typeof parsed === "object" && typeof (parsed as { note?: unknown }).note === "string"
        ? (parsed as { note: string }).note.trim().slice(0, 180)
        : undefined;

    return {
      symbols: unique,
      source: "openai_symbol_discovery",
      note: note && note.length > 0 ? note : undefined
    };
  }

  async getMarketScannerSymbols(
    limit = 15,
    mode: IbkrScannerCodeSetting = DEFAULT_IBKR_SCANNER_CODE
  ): Promise<AiDiscoveryScannerResult> {
    if (!settings.aiDiscoveryEnabled) {
      return {
        symbols: [],
        source: "disabled",
        note: "AI_DISCOVERY_ENABLED=false."
      };
    }

    const apiKey = settings.openAiApiKey.trim();
    if (!apiKey) {
      return {
        symbols: [],
        source: "disabled",
        note: "OPENAI_API_KEY not configured."
      };
    }

    const cappedLimit = Math.max(5, Math.min(40, Math.round(limit)));
    const requestKey = `${mode}|${cappedLimit}`;
    const nowMs = Date.now();

    if (this.cache && this.cache.key === requestKey && this.cache.expiresAtMs > nowMs) {
      return {
        symbols: this.cache.symbols.slice(0, cappedLimit),
        source: "openai_symbol_discovery",
        note: `cached_${Math.max(0, Math.round((nowMs - this.cache.generatedAtMs) / 1000))}s`
      };
    }

    if (nowMs < this.rateLimitedUntilMs) {
      return {
        symbols: [],
        source: "unavailable",
        note: `OpenAI discovery cooldown active for ${Math.ceil((this.rateLimitedUntilMs - nowMs) / 1000)}s`
      };
    }

    if (this.inFlight && this.inFlightKey === requestKey) {
      return await this.inFlight;
    }

    this.inFlightKey = requestKey;
    this.inFlight = this.requestSymbols(cappedLimit, mode, apiKey);

    try {
      const result = await this.inFlight;
      if (result.source === "openai_symbol_discovery" && result.symbols.length > 0) {
        this.cache = {
          key: requestKey,
          symbols: result.symbols.slice(0, cappedLimit),
          generatedAtMs: nowMs,
          expiresAtMs: nowMs + this.cacheTtlMs()
        };
      }
      return result;
    } catch (error) {
      logger.warn("AI discovery request failed", error);
      return {
        symbols: [],
        source: "unavailable",
        note: (error as Error).message
      };
    } finally {
      this.inFlight = null;
      this.inFlightKey = null;
    }
  }
}
