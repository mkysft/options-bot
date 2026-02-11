import { describe, expect, test } from "bun:test";

import { settings } from "../core/config";
import { MarketDataService } from "../services/marketDataService";

process.env.BUN_TEST = "1";

class FakeIbkrAdapter {
  quoteCalls = 0;
  closesCalls = 0;
  optionChainCalls = 0;
  scannerCalls = 0;

  async getQuote(symbol: string) {
    this.quoteCalls += 1;
    return {
      symbol,
      last: 100,
      bid: 99.9,
      ask: 100.1,
      volume: 1_000_000
    };
  }

  async getRecentDailyCloses(_symbol: string, bars: number): Promise<number[]> {
    this.closesCalls += 1;
    return Array.from({ length: bars }, (_, index) => 90 + index * 0.1);
  }

  async getOptionContracts(symbol: string): Promise<Array<{ symbol: string; expiration: string; strike: number; right: "C" | "P" }>> {
    this.optionChainCalls += 1;
    return [
      { symbol, expiration: "20260220", strike: 100, right: "C" },
      { symbol, expiration: "20260220", strike: 100, right: "P" }
    ];
  }

  async getScannerSymbols(): Promise<string[]> {
    this.scannerCalls += 1;
    return ["XLE", "XLF", "SMH"];
  }

  getLastScannerResult(): { source: "tws_socket" | "none"; fallbackReason: string } {
    return {
      source: "tws_socket",
      fallbackReason: ""
    };
  }
}

class FakeAlphaVantageAdapter {
  calls = 0;
  scannerCalls = 0;

  async getNewsSentiment(): Promise<number> {
    this.calls += 1;
    return 0.25;
  }

  async getMarketScannerSymbols(): Promise<{
    symbols: string[];
    source: "alpha_vantage_top_movers" | "disabled" | "unavailable";
    note?: string;
  }> {
    this.scannerCalls += 1;
    return {
      symbols: ["XLE", "XLF", "SMH", "XLV"],
      source: "alpha_vantage_top_movers"
    };
  }
}

class FakeSecEdgarAdapter {
  calls = 0;

  async getEventBiasAndRisk(): Promise<{ eventBias: number; eventRisk: number }> {
    this.calls += 1;
    return { eventBias: 0.1, eventRisk: 0.2 };
  }
}

class FakeFredAdapter {
  calls = 0;

  async getMacroRegime(): Promise<number> {
    this.calls += 1;
    return 0.05;
  }
}

class FakeFmpAdapter {
  scannerCalls = 0;

  async getMarketScannerSymbols(): Promise<{
    symbols: string[];
    source: "fmp_company_screener" | "disabled" | "unavailable";
    note?: string;
  }> {
    this.scannerCalls += 1;
    return {
      symbols: ["XLV", "XLU", "XLB"],
      source: "fmp_company_screener"
    };
  }
}

class FakeEmptyAdapter {
  scannerCalls = 0;

  async getMarketScannerSymbols(): Promise<{
    symbols: string[];
    source:
      | "eodhd_screener"
      | "alpaca_screener"
      | "fmp_company_screener"
      | "alpha_vantage_top_movers"
      | "disabled"
      | "unavailable";
    note?: string;
  }> {
    this.scannerCalls += 1;
    return {
      symbols: [],
      source: "unavailable",
      note: "test-empty"
    };
  }
}

class FakeAlpacaModeAdapter {
  scannerCalls = 0;
  lastMode: string | null = null;

  async getMarketScannerSymbols(limit = 15, mode?: string): Promise<{
    symbols: string[];
    source: "alpaca_screener" | "disabled" | "unavailable";
    note?: string;
  }> {
    this.scannerCalls += 1;
    this.lastMode = mode ?? null;
    return {
      symbols: ["XOP", "ARKK", "TQQQ"].slice(0, Math.max(1, Math.min(3, limit))),
      source: "alpaca_screener"
    };
  }
}

class FakeAiDiscoveryAdapter {
  scannerCalls = 0;
  lastMode: string | null = null;

  async getMarketScannerSymbols(limit = 15, mode?: string): Promise<{
    symbols: string[];
    source: "openai_symbol_discovery" | "disabled" | "unavailable";
    note?: string;
  }> {
    this.scannerCalls += 1;
    this.lastMode = mode ?? null;
    return {
      symbols: ["SOXL", "SOXS", "ARKK", "IWM", "SMH"].slice(0, Math.max(1, Math.min(5, limit))),
      source: "openai_symbol_discovery"
    };
  }
}

class FakeAlphaUnavailableSentimentAdapter {
  calls = 0;

  async getNewsSentimentSnapshot(): Promise<{
    sentiment: null;
    articles: [];
    source: "unavailable";
    note: string;
  }> {
    this.calls += 1;
    return {
      sentiment: null,
      articles: [],
      source: "unavailable",
      note: "alpha unavailable in test"
    };
  }
}

class FakeAlpacaNewsSentimentAdapter {
  calls = 0;

  async getNewsSentimentSnapshot(symbol: string): Promise<{
    sentiment: number;
    articles: Array<{
      title: string;
      source: string;
      url: string;
      publishedAt: string | null;
      summary: string;
      overallSentimentScore: number | null;
      relevanceScore: number | null;
    }>;
    source: "alpaca_news_sentiment";
  }> {
    this.calls += 1;
    return {
      sentiment: 0.31,
      source: "alpaca_news_sentiment",
      articles: [
        {
          title: `${symbol.toUpperCase()} headline`,
          source: "alpaca",
          url: "",
          publishedAt: null,
          summary: "test summary",
          overallSentimentScore: 0.31,
          relevanceScore: 1
        }
      ]
    };
  }
}

class FakeAlpacaOptionsAdapter {
  optionCalls = 0;

  async getOptionChain(symbol: string): Promise<{
    symbol: string;
    contracts: Array<{
      symbol: string;
      expiration: string;
      strike: number;
      right: "CALL" | "PUT";
      bid: number;
      ask: number;
      last: number;
      volume: number;
      openInterest: number;
      impliedVol: number;
      delta: number;
      gamma: number;
      quoteSource: "alpaca_option_quote";
    }>;
    source: "alpaca_option_chain";
  }> {
    this.optionCalls += 1;
    return {
      symbol,
      source: "alpaca_option_chain",
      contracts: [
        {
          symbol,
          expiration: "2026-02-20",
          strike: 100,
          right: "CALL",
          bid: 2.1,
          ask: 2.2,
          last: 2.15,
          volume: 120,
          openInterest: 2_100,
          impliedVol: 0.33,
          delta: 0.52,
          gamma: 0.03,
          quoteSource: "alpaca_option_quote"
        },
        {
          symbol,
          expiration: "2026-02-20",
          strike: 100,
          right: "PUT",
          bid: 1.8,
          ask: 1.95,
          last: 1.88,
          volume: 98,
          openInterest: 1_900,
          impliedVol: 0.31,
          delta: -0.48,
          gamma: 0.029,
          quoteSource: "alpaca_option_quote"
        }
      ]
    };
  }
}

describe("MarketDataService caching", () => {
  test("reuses cached snapshot+closes within TTL", async () => {
    const ibkr = new FakeIbkrAdapter();
    const service = new MarketDataService(ibkr as never);

    const first = await service.getSymbolSnapshotAndCloses("SPY", 90);
    const second = await service.getSymbolSnapshotAndCloses("SPY", 90);

    expect(first.snapshot.symbol).toBe("SPY");
    expect(first.closes.length).toBe(90);
    expect(second.closes.length).toBe(90);
    expect(ibkr.quoteCalls).toBe(1);
    expect(ibkr.closesCalls).toBe(1);
  });

  test("caches option chain for same symbol/price bucket", async () => {
    const ibkr = new FakeIbkrAdapter();
    const service = new MarketDataService(ibkr as never);

    const first = await service.getOptionChain("SPY", 100);
    const second = await service.getOptionChain("SPY", 100.01);

    expect(first.length).toBeGreaterThan(0);
    expect(second.length).toBeGreaterThan(0);
    expect(ibkr.optionChainCalls).toBe(1);
  });

  test("caches context per symbol and reuses macro across symbols", async () => {
    const ibkr = new FakeIbkrAdapter();
    const alpha = new FakeAlphaVantageAdapter();
    const sec = new FakeSecEdgarAdapter();
    const fred = new FakeFredAdapter();
    const service = new MarketDataService(
      ibkr as never,
      alpha as never,
      undefined as never,
      undefined as never,
      undefined as never,
      sec as never,
      fred as never
    );

    await service.getContextFeatures("SPY");
    await service.getContextFeatures("SPY");
    await service.getContextFeatures("QQQ");

    expect(alpha.calls).toBe(2);
    expect(sec.calls).toBe(2);
    expect(fred.calls).toBe(1);
  });

  test("falls back to Alpaca news sentiment when Alpha Vantage is unavailable", async () => {
    const ibkr = new FakeIbkrAdapter();
    const alpha = new FakeAlphaUnavailableSentimentAdapter();
    const alpaca = new FakeAlpacaNewsSentimentAdapter();
    const sec = new FakeSecEdgarAdapter();
    const fred = new FakeFredAdapter();
    const service = new MarketDataService(
      ibkr as never,
      alpha as never,
      undefined as never,
      undefined as never,
      alpaca as never,
      sec as never,
      fred as never
    );

    const context = await service.getContextFeaturesDetailed("SPY");
    expect(context.sources.newsSentiment).toBe("alpaca_news_sentiment");
    expect(context.newsSentiment).toBe(0.31);
    expect(context.articles.length).toBeGreaterThan(0);
    expect(alpha.calls).toBe(1);
    expect(alpaca.calls).toBe(1);
  });

  test("uses Alpaca options chain when analysis provider is ALPACA", async () => {
    const ibkr = new FakeIbkrAdapter();
    const alpaca = new FakeAlpacaOptionsAdapter();
    const service = new MarketDataService(
      ibkr as never,
      undefined as never,
      undefined as never,
      undefined as never,
      alpaca as never
    );
    service.setRuntimePolicy({
      getPolicy: () => ({ analysisDataProvider: "ALPACA" })
    } as never);

    const optionEvidence = await service.getOptionChainDetailed("SPY", 100, 7, 21);
    expect(optionEvidence.source).toBe("alpaca_option_chain");
    expect(optionEvidence.chain.length).toBeGreaterThan(0);
    expect(optionEvidence.chain[0]?.quoteSource).toBe("alpaca_option_quote");
    expect(alpaca.optionCalls).toBe(1);
    expect(ibkr.optionChainCalls).toBe(0);
  });

  test("dynamic universe prefers external scanner symbols", async () => {
    const ibkr = new FakeIbkrAdapter();
    const alpha = new FakeAlphaVantageAdapter();
    const fmp = new FakeEmptyAdapter();
    const eodhd = new FakeEmptyAdapter();
    const alpaca = new FakeEmptyAdapter();
    const service = new MarketDataService(
      ibkr as never,
      alpha as never,
      fmp as never,
      eodhd as never,
      alpaca as never
    );

    const result = await service.buildDynamicUniverse(["SPY", "QQQ"], 5, {
      targetSize: 6,
      scannerLimit: 4
    });

    expect(result.symbols.length).toBeGreaterThanOrEqual(5);
    expect(result.discoveredSymbols.length).toBeGreaterThan(0);
    expect(result.scannerUsed).toBe(true);
    expect(result.scannerSource).toBe("alpha_vantage");
    expect(typeof result.scannerFallbackReason === "string" || result.scannerFallbackReason === null).toBeTrue();
    expect(Array.isArray(result.scannerProvidersTried)).toBeTrue();
    expect(result.scannerProvidersTried.length).toBeGreaterThan(0);
    expect(result.scannerProviderRanking[0]?.provider).toBe("ibkr");
    expect(alpha.scannerCalls).toBe(1);
    expect(ibkr.scannerCalls).toBe(0);
  });

  test("dynamic universe uses first successful provider in scanner chain", async () => {
    const ibkr = new FakeIbkrAdapter();
    const alpha = new FakeEmptyAdapter();
    const fmp = new FakeFmpAdapter();
    const eodhd = new FakeEmptyAdapter();
    const alpaca = new FakeEmptyAdapter();
    const service = new MarketDataService(
      ibkr as never,
      alpha as never,
      fmp as never,
      eodhd as never,
      alpaca as never
    );

    const result = await service.buildDynamicUniverse(["SPY", "QQQ"], 5, {
      targetSize: 6,
      scannerLimit: 4
    });

    expect(result.scannerUsed).toBeTrue();
    expect(result.scannerSource === "fmp" || result.scannerSource === "multi").toBeTrue();
    expect(result.discoveredSymbols.includes("XLV")).toBeTrue();
    expect(result.scannerProvidersUsed.includes("fmp")).toBeTrue();
    expect(fmp.scannerCalls).toBe(1);
  });

  test("dynamic universe forwards screener mode into alpaca provider", async () => {
    const ibkr = new FakeIbkrAdapter();
    const alpha = new FakeEmptyAdapter();
    const fmp = new FakeEmptyAdapter();
    const eodhd = new FakeEmptyAdapter();
    const alpaca = new FakeAlpacaModeAdapter();
    const service = new MarketDataService(
      ibkr as never,
      alpha as never,
      fmp as never,
      eodhd as never,
      alpaca as never
    );

    const result = await service.buildDynamicUniverse(["SPY", "QQQ"], 5, {
      targetSize: 6,
      scannerLimit: 4,
      ibkrScanCode: "TOP_PERC_GAIN"
    });

    expect(result.scannerUsed).toBeTrue();
    expect(result.scannerProvidersUsed.includes("alpaca")).toBeTrue();
    expect(alpaca.scannerCalls).toBe(1);
    expect(alpaca.lastMode).toBe("TOP_PERC_GAIN");
  });

  test("dynamic universe can use ai discovery provider when others are unavailable", async () => {
    const previousOrder = [...settings.scannerProviderOrder];
    settings.scannerProviderOrder = [
      "ibkr",
      "fmp",
      "eodhd",
      "alpaca",
      "alpha_vantage",
      "ai_discovery"
    ];

    try {
      const ibkr = new FakeIbkrAdapter();
      const alpha = new FakeEmptyAdapter();
      const fmp = new FakeEmptyAdapter();
      const eodhd = new FakeEmptyAdapter();
      const alpaca = new FakeEmptyAdapter();
      const aiDiscovery = new FakeAiDiscoveryAdapter();
      const service = new MarketDataService(
        ibkr as never,
        alpha as never,
        fmp as never,
        eodhd as never,
        alpaca as never,
        undefined as never,
        undefined as never,
        aiDiscovery as never
      );

      const result = await service.buildDynamicUniverse(["SPY", "QQQ"], 5, {
        targetSize: 6,
        scannerLimit: 4,
        ibkrScanCode: "HOT_BY_VOLUME"
      });

      expect(result.scannerUsed).toBeTrue();
      expect(result.scannerProvidersUsed.includes("ai_discovery")).toBeTrue();
      expect(result.discoveredSymbols.length).toBeGreaterThan(0);
      expect(result.scannerSource === "ai_discovery" || result.scannerSource === "multi").toBeTrue();
      expect(aiDiscovery.scannerCalls).toBe(1);
      expect(aiDiscovery.lastMode).toBe("HOT_BY_VOLUME");
    } finally {
      settings.scannerProviderOrder = previousOrder;
    }
  });
});
