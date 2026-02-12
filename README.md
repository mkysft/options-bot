# Options Bot (TypeScript + Bun)

Paper-first options decision engine designed for IBKR with deterministic scoring, hard risk controls, and manual order approval.

## Stack

- Runtime: Bun (Node-compatible)
- API: Fastify + Zod
- Storage: SQLite (`bun:sqlite`) + JSONL audit
- Frontend: React (JSX) + Tailwind CSS + React Query
- UI Components: local `coss` components copied via `shadcn add @coss/*` (owned source in `web/src/components`)

## Endpoints

- `POST /scan`
- `POST /score`
- `POST /decision`
- `POST /backtest`
- `POST /propose-order`
- `POST /approve-order`
- `GET /risk-status`
- `GET /account-summary`
- `GET /market-data-diagnostics`
- `GET /ibkr-status`
- `POST /ibkr-launch`
- `GET /orders/pending`
- `GET /orders/recent`

## Quickstart

```bash
cp .env.example .env
bun install
bun run dev
```

Open [http://127.0.0.1:5173/ui/](http://127.0.0.1:5173/ui/)

## Frontend Commands

```bash
# Build React UI to src/ui-dist
bun run ui:build

# Frontend-only dev server (Vite)
bun run ui:dev
```

- `bun run dev` starts both:
  - Vite dev server (`http://127.0.0.1:5173/ui/`) for HMR
  - Backend watcher on `http://127.0.0.1:8000`
- In dev, Vite proxies API requests to backend, so UI edits hot-reload without restarting.
- `bun run start` serves the built UI from `src/ui-dist` and does not use Vite.

## Desktop (Electron)

```bash
# Desktop dev mode (starts backend + Vite + Electron window)
bun run electron:dev

# Attach Electron to an already-running backend/UI
bun run electron:dev:attach

# Desktop app mode (starts built server UI via /ui)
bun run electron:start
```

- `electron:dev` runs managed backend startup using `bun run dev`.
- `electron:start` runs managed backend startup using `bun run start`.
- Optional env overrides:
  - `ELECTRON_MANAGED_BACKEND=0`
  - `ELECTRON_UI_URL_DEV`
  - `ELECTRON_UI_URL_APP`
  - `ELECTRON_HEALTH_URL`
  - `ELECTRON_STARTUP_TIMEOUT_MS`

## Testing

```bash
bun test
```

## Logging

- `LOG_FORMAT=pretty` for local readable logs (default in non-prod).
- `LOG_FORMAT=json` for machine-readable logs.
- `LOG_LEVEL` supports `debug`, `info`, `warn`, `error`.

## Notes

- Phase-1 is paper-only and manual approval only.
- IBKR adapter now performs live quote/historical/option-chain API calls when enabled, with retry/backoff and queue-aware pacing.
- Market-data fallback prefers real Alpaca quote/history before synthetic generation when IBKR quote/history is unavailable.
- coss ui docs are copy/paste-first and Tailwind-based. This repo uses manual local component source (no external `coss` npm UI package).

## IBKR App Launch Automation

- The dashboard can trigger local launch of IBKR Gateway or TWS via `POST /ibkr-launch`.
- Login remains manual (username/password/2FA), which aligns with official IBKR constraints.
- For macOS, defaults use app names:
  - `IBKR_GATEWAY_APP_NAME=IB Gateway`
  - `IBKR_TWS_APP_NAME=Trader Workstation`
- For non-macOS, set executable paths:
  - `IBKR_GATEWAY_EXEC_PATH`
  - `IBKR_TWS_EXEC_PATH`
- If your API port is custom, set `IBKR_PORT` and optionally `IBKR_PORT_CANDIDATES` (comma-separated) so auto-connectivity probing can detect the active session.
- Retry controls (live-adjustable in Settings -> Environment):
  - `IBKR_RETRY_MAX_ATTEMPTS`
  - `IBKR_RETRY_BASE_DELAY_MS`
  - `IBKR_RETRY_MAX_DELAY_MS`

## Scanner Discovery

- Symbol discovery now uses a provider chain with quality-aware ordering and fallback:
  - `ibkr` -> `fmp` -> `eodhd` -> `alpaca` -> `alpha_vantage` -> `ai_discovery` (default order)
- Configure scanner providers and keys:
  - `SCANNER_PROVIDER_ORDER` (comma-separated provider ids)
  - `ALPHA_VANTAGE_API_KEY`
  - `FMP_API_KEY`
  - `EODHD_API_KEY`
  - `ALPACA_API_KEY` + `ALPACA_API_SECRET`
  - `AI_DISCOVERY_ENABLED` + `AI_DISCOVERY_CACHE_TTL_MINUTES` (OpenAI-backed symbol source)
- Optional provider URLs:
  - `FMP_BASE_URL`
  - `EODHD_BASE_URL`
  - `ALPACA_DATA_BASE_URL`
- The Recommendations panel shows scanner source, providers used/tried, quality ranking, and fallback notes.
- API Logs include per-provider request rows for dynamic discovery calls.
