export class ApiRequestError extends Error {
  constructor(status, statusText, message, payload = null, rawBody = "") {
    super(message);
    this.name = "ApiRequestError";
    this.status = status;
    this.statusText = statusText;
    this.payload = payload;
    this.rawBody = rawBody;
  }
}

const extractErrorMessage = (status, statusText, payload, rawBody) => {
  const fallback = `${status} ${statusText}: ${rawBody}`;
  if (!payload || typeof payload !== "object") return fallback;
  if (typeof payload.error === "string" && payload.error.trim().length > 0) {
    return payload.error;
  }
  if (typeof payload.message === "string" && payload.message.trim().length > 0) {
    return payload.message;
  }
  return fallback;
};

const request = async (url, options = {}) => {
  const response = await fetch(url, {
    headers: { "Content-Type": "application/json" },
    ...options
  });

  if (!response.ok) {
    const rawBody = await response.text();
    let payload = null;
    try {
      payload = rawBody ? JSON.parse(rawBody) : null;
    } catch {
      payload = null;
    }
    throw new ApiRequestError(
      response.status,
      response.statusText,
      extractErrorMessage(response.status, response.statusText, payload, rawBody),
      payload,
      rawBody
    );
  }

  return response.json();
};

const toQueryString = (params = {}) => {
  const query = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value === undefined || value === null || value === "") return;
    if (Array.isArray(value)) {
      if (value.length > 0) query.set(key, value.join(","));
      return;
    }
    query.set(key, String(value));
  });

  const encoded = query.toString();
  return encoded ? `?${encoded}` : "";
};

export const api = {
  health: () => request("/health"),
  runStatus: () => request("/run-status"),
  riskStatus: () => request("/risk-status"),
  killSwitch: () => request("/kill-switch"),
  updateKillSwitch: (payload) =>
    request("/kill-switch", {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  accountSummary: () => request("/account-summary"),
  acceptanceGate: () => request("/acceptance-gate"),
  positions: () => request("/positions"),
  ibkrReadiness: () => request("/ibkr-readiness"),
  marketDataDiagnostics: (params = {}) =>
    request(`/market-data-diagnostics${toQueryString(params)}`),
  ibkrStatus: () => request("/ibkr-status"),
  ibkrLaunch: (payload = {}) =>
    request("/ibkr-launch", {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  pendingOrders: () => request("/orders/pending"),
  recentOrders: () => request("/orders/recent"),
  scan: (payload = { topN: 10 }) => request("/scan", { method: "POST", body: JSON.stringify(payload) }),
  score: (payload) => request("/score", { method: "POST", body: JSON.stringify(payload) }),
  decision: (payload) => request("/decision", { method: "POST", body: JSON.stringify(payload) }),
  recommendations: (params = {}) => request(`/recommendations${toQueryString(params)}`),
  apiRequestLogs: (params = {}) => request(`/api-request-logs${toQueryString(params)}`),
  backtest: (payload = {}) =>
    request("/backtest", {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  botPolicy: () => request("/bot-policy"),
  updateBotPolicy: (payload) =>
    request("/bot-policy", {
      method: "PATCH",
      body: JSON.stringify(payload)
    }),
  resetBotPolicy: () =>
    request("/bot-policy/reset", {
      method: "POST"
    }),
  envConfig: () => request("/env-config"),
  updateEnvConfig: (payload) =>
    request("/env-config", {
      method: "PATCH",
      body: JSON.stringify(payload)
    }),
  refreshApp: () =>
    request("/app/refresh", {
      method: "POST"
    }),
  restartApp: () =>
    request("/app/restart", {
      method: "POST"
    }),
  proposeOrder: (payload) =>
    request("/propose-order", {
      method: "POST",
      body: JSON.stringify(payload)
    }),
  approveOrder: (payload) =>
    request("/approve-order", {
      method: "POST",
      body: JSON.stringify(payload)
    })
};
