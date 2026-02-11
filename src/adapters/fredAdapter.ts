import { settings } from "../core/config";
import { fetchWithApiLog } from "../utils/fetchWithApiLog";
import { clamp } from "../utils/statistics";

type FredSeriesSource = "fred" | "disabled" | "unavailable";

interface FredSeriesSnapshot {
  value: number | null;
  source: FredSeriesSource;
  note?: string;
}

export interface FredMacroRegimeSnapshot {
  macroRegime: number;
  source: "fred" | "fallback";
  components: {
    vix: number | null;
    spread: number | null;
    vixSource: FredSeriesSource;
    spreadSource: FredSeriesSource;
  };
  note?: string;
}

export class FredAdapter {
  private readonly baseUrl = "https://api.stlouisfed.org/fred/series/observations";

  private async latest(seriesId: string): Promise<FredSeriesSnapshot> {
    const apiKey = settings.fredApiKey;
    if (!apiKey) return { value: null, source: "disabled", note: "FRED_API_KEY not configured." };
    const params = new URLSearchParams({
      series_id: seriesId,
      api_key: apiKey,
      file_type: "json",
      sort_order: "desc",
      limit: "5"
    });

    try {
      const url = `${this.baseUrl}?${params.toString()}`;
      const response = await fetchWithApiLog(url, undefined, {
        provider: "fred",
        endpoint: seriesId,
        reason: `Fetch latest macro series ${seriesId}`,
        requestPayload: {
          seriesId
        }
      });
      if (!response.ok) {
        return {
          value: null,
          source: "unavailable",
          note: `FRED responded ${response.status}.`
        };
      }
      const payload = (await response.json()) as {
        observations?: Array<{ value?: string }>;
      };
      for (const row of payload.observations ?? []) {
        if (!row.value || row.value === ".") continue;
        const parsed = Number(row.value);
        if (Number.isFinite(parsed)) {
          return {
            value: parsed,
            source: "fred"
          };
        }
      }
      return {
        value: null,
        source: "unavailable",
        note: "Series returned no numeric values."
      };
    } catch {
      return {
        value: null,
        source: "unavailable",
        note: "FRED request failed."
      };
    }
  }

  async getMacroRegimeSnapshot(): Promise<FredMacroRegimeSnapshot> {
    const [vix, spread] = await Promise.all([this.latest("VIXCLS"), this.latest("T10Y2Y")]);
    if (vix.value === null && spread.value === null) {
      return {
        macroRegime: 0,
        source: "fallback",
        components: {
          vix: null,
          spread: null,
          vixSource: vix.source,
          spreadSource: spread.source
        },
        note: [vix.note, spread.note].filter(Boolean).join(" | ") || "Macro data unavailable."
      };
    }

    const vixComponent = vix.value === null ? 0 : clamp((20 - vix.value) / 10, -1, 1);
    const spreadComponent = spread.value === null ? 0 : clamp(spread.value / 1.5, -1, 1);
    const note = [vix.note, spread.note].filter(Boolean).join(" | ");
    return {
      macroRegime: clamp(0.6 * vixComponent + 0.4 * spreadComponent, -1, 1),
      source: vix.source === "fred" || spread.source === "fred" ? "fred" : "fallback",
      components: {
        vix: vix.value,
        spread: spread.value,
        vixSource: vix.source,
        spreadSource: spread.source
      },
      note: note || undefined
    };
  }

  async getMacroRegime(): Promise<number> {
    const snapshot = await this.getMacroRegimeSnapshot();
    return snapshot.macroRegime;
  }
}
