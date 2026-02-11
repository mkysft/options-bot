import { apiRequestLogStore } from "../storage/apiRequestLogStore";
import { nowIso } from "./time";

interface FetchApiLogMeta {
  provider: string;
  reason: string;
  endpoint?: string;
  correlationId?: string;
  requestPayload?: unknown;
  responsePayload?: unknown;
}

export const fetchWithApiLog = async (
  url: string,
  init: RequestInit | undefined,
  meta: FetchApiLogMeta
): Promise<Response> => {
  const startedAt = nowIso();
  const startedMs = Date.now();
  const method = (init?.method ?? "GET").toUpperCase();

  try {
    const response = await fetch(url, init);
    const finishedAt = nowIso();
    const durationMs = Date.now() - startedMs;

    apiRequestLogStore.log({
      startedAt,
      finishedAt,
      durationMs,
      direction: "external",
      provider: meta.provider,
      method,
      endpoint: meta.endpoint ?? url,
      reason: meta.reason,
      status: response.ok ? "success" : "error",
      statusCode: response.status,
      correlationId: meta.correlationId,
      requestPayload: meta.requestPayload,
      responsePayload: meta.responsePayload
    });

    return response;
  } catch (error) {
    const finishedAt = nowIso();
    const durationMs = Date.now() - startedMs;

    apiRequestLogStore.log({
      startedAt,
      finishedAt,
      durationMs,
      direction: "external",
      provider: meta.provider,
      method,
      endpoint: meta.endpoint ?? url,
      reason: meta.reason,
      status: "error",
      correlationId: meta.correlationId,
      requestPayload: meta.requestPayload,
      responsePayload: meta.responsePayload,
      errorMessage: (error as Error).message
    });

    throw error;
  }
};
