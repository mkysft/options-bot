# Options Bot Master Plan (Initial Plan + Implemented Updates)

Last updated: 2026-02-10 (America/Jamaica)

## 1) Original Goal and Constraints (from initial plan)
- Build an end-to-end AI-assisted options decision bot for IBKR.
- Paper-first, manual approval required for all entries/exits.
- Universe focused on liquid ETFs + mega caps.
- 1-14 DTE swing style with deterministic risk gates.
- Budget-conscious external data approach.
- Hybrid decision model: rules/scoring first, LLM judge second (confirm/veto only).

Locked operating profile:
- Paper-first
- Manual approval
- 15-min rescoring cadence
- Per-trade risk cap ~2% of equity
- Daily loss halt ~5%
- TP/SL + max hold harness
- Local runtime on Mac + IBKR Gateway/TWS

## 2) Initial Architecture Blueprint
1. Market data service (IBKR + low-cost external sources)
2. Feature engine (technical + options + sentiment + event)
3. Scoring engine (weighted deterministic composite)
4. LLM judge (structured confirm/veto)
5. Execution gateway (order proposal + approval + submit)
6. Risk engine (hard limits, halt logic)
7. Journal/audit (SQLite + JSONL)
8. Approval UI (local dashboard)

## 3) Technology and Product Direction Changes Applied
- Backend implemented in TypeScript/Bun (instead of Python).
- Frontend implemented in React/JSX.
- UI stack uses local coss/shadcn-style copied components + Tailwind.
- API fetching uses React Query where applicable.
- `@biotechusa/pdo-ui` is not used.

## 4) Implemented So Far (Completed)

### 4.1 Backend/API
Implemented endpoints:
- `POST /scan`
- `POST /score`
- `POST /decision`
- `POST /backtest`
- `POST /propose-order`
- `POST /approve-order`
- `GET /risk-status`
- `GET /account-summary`
- `GET /recommendations`
- `GET /ibkr-status`
- `POST /ibkr-launch`
- `GET /orders/pending`
- `GET /orders/recent`
- `GET /bot-policy`, `PATCH /bot-policy`, `POST /bot-policy/reset`
- `GET /env-config`, `PATCH /env-config`, `POST /app/refresh`, `POST /app/restart`
- `GET /api-request-logs`

### 4.2 Data/Feature/Decision Pipeline
- Deterministic feature vector construction implemented.
- Weighted scoring model implemented.
- Decision policy implemented (CALL/PUT/NO_TRADE thresholds via runtime policy).
- LLM judge integrated with heuristic fallback.
- 429 handling implemented with cooldown fallback.
- Additional local throttle/pacing added to reduce 429 bursts.

### 4.3 Execution/Risk
- Order proposal flow implemented.
- Manual approval/rejection flow implemented.
- Risk checks applied pre-submit.
- Persistent manual kill switch implemented (API + UI toggle + risk halt integration).
- Paper submission integration implemented.
- Broker order status refresh and mapping implemented.
- Exit automation implemented (take-profit / stop-loss / max-hold) with pending approval exits.
- Pre-event exit automation implemented (earnings-calendar driven) using configurable window hours.
- Parent entry auto-marked exited after filled exit order.

### 4.4 Account/PnL Sync
- Account snapshot sync from IBKR implemented.
- Position snapshot integration implemented.
- Risk snapshot history persisted.
- Account summary endpoint includes PnL series for UI graph.

### 4.5 Policy + Runtime Config
- Runtime policy persistence implemented in SQLite app_state.
- Policy reset/update endpoints implemented.
- Settings/env editor with live apply + refresh + restart hooks implemented.
- Scheduler interval reload on runtime refresh implemented.
- Added event-exit controls:
  - `preEventExitWindowHours` (earnings pre-event window; 0 disables)
  - `preEventSecFilingLookbackHours` (SEC filing post-event lookback; 0 disables)
  - `preEventSecFilingRiskThreshold` (minimum SEC risk score to trigger filing-based exits)

### 4.6 Acceptance Gate
- Acceptance gate service implemented with persistent snapshots in `app_state` (`acceptance_gate_v1`).
- Metrics tracked: observed run duration, completed/open trades, expectancy, profit factor, drawdown, violations.
- API endpoint implemented: `GET /acceptance-gate`.
- Scheduler now refreshes gate snapshots during scheduled runs.
- Account summary now includes acceptance-gate summary fields.
- UI now exposes Acceptance Gate metrics/checks/violations in a dedicated Order Workflow tab.

### 4.7 UI (Control Room)
- Recommendations table with actionable flag.
- Order workflow tabs (Pending/Recent/Decision/Risk/Backtest/Raw/API logs).
- Settings modal with tabs:
  - Policy/guidelines harness controls
  - Env variables editor
- Top-level Settings trigger implemented.
- Top-level kill switch control implemented.
- Account summary card implemented.
- PnL line chart implemented.
- IBKR panel includes:
  - connection badge
  - paper/live session mode
  - launch Gateway/TWS actions
  - launch result details
- IBKR readiness modal implemented (queue/data/connectivity checks + advice).

### 4.8 IBKR Integration Improvements
- Connectivity probing across candidate ports implemented.
- Auto selection of active reachable port implemented.
- Connectivity cooldown/retry behavior implemented.
- Added recovery probing after connectivity failures.
- Added logging when connectivity is established/lost and when active port switches.
- Reduced scan concurrency under IBKR-enabled mode to lower timeout pressure.
- IB transport migrated from `ib-tws-api` to `@stoqey/ib` through a local compatibility client.
- Compatibility layer preserves existing adapter contract and account/order event handling.
- Added centralized IBKR request queue with per-channel pacing (connectivity/quotes/history/options/positions/account/orders/scanner).
- Added IBKR market scanner integration and scanner-driven universe expansion for `/scan`, `/recommendations`, and scheduled scans.
- Added startup reconciliation pass to map local submitted orders against broker statuses/positions and recover lifecycle state after restart.
- Added `/recommendations` in-flight dedupe + stale-cache fallback to prevent duplicate heavy scans from concurrent UI/API requests.
- Added global `/recommendations` in-flight guard across different query keys to prevent overlapping heavy runs and reduce queue storms.
- Added scanner provider hardening with per-provider timeout and IBKR queue/cooldown-aware scanner skip before fallback providers.
- Added short-TTL/in-flight caching for broker positions snapshots to reduce duplicate IBKR queue load from concurrent account/positions refreshes.
- Added `AnalysisService.scanUniverseWithDiscovery` serialization/dedupe so scheduler/manual scans do not overlap and saturate IBKR.
- Added connectivity check caching/in-flight dedupe to reduce repeated multi-port probes during UI/API bursts.
- Added short-TTL account snapshot caching + in-flight dedupe to reduce repeated account sync pressure.
- Added queue-aware deferral for low-priority open-order/positions/account refreshes when backlog is high.
- Added reconnect-aware execution reconciliation hook: when IBKR connectivity is restored, startup reconciliation is re-armed and sync windows reset for fast state convergence.
- Added transient retry strategy for noisy IBKR operations (connectivity probes, open orders, positions, quote snapshots, historical bars, option chain details).
- Added IBKR runtime compatibility flagging for historical fractional-share protocol warnings in readiness/runtime diagnostics.

### 4.9 Observability and Audit
- Persistent internal + external API request log store implemented.
- Request metadata captured: when, why, endpoint, status, duration, error.
- Logging wrappers integrated for:
  - IBKR operations
  - external HTTP adapters (OpenAI, Alpha Vantage, FRED, SEC)
  - internal API routes via Fastify hooks
- API logs exposed in UI tab.
- Added IBKR readiness diagnostics endpoint: `GET /ibkr-readiness` (connectivity, queue health, quote/historical/scanner/positions checks + advice).
- Added readiness advice signal for historical API compatibility blockers (fractional-share protocol warning).

### 4.11 Real-Data Fallback Quality
- Added Alpaca real-data fallback adapters for:
  - latest quote snapshots
  - daily historical closes
- Market data pipeline now prefers real fallback data before synthetic generation when IBKR quote/history is unavailable.
- Recommendation evidence/data-quality logic now accepts non-synthetic real market sources (IBKR or Alpaca) for core market-data checks.

### 4.10 Testing/Validation
- Typecheck passes.
- Unit/integration test suite passing (current local run: 34 passed).
- UI build passes.

## 5) Confirmed Remaining Work (External / Account-side)

### 5.1 Broker/account prerequisites (cannot be solved purely in code)
1. IBKR protocol compatibility validation in your live environment:
- Runtime transport is migrated to `@stoqey/ib` and compatibility warnings are surfaced in diagnostics.
- If the min-version 163 warning still appears on your host session, Gateway/TWS host upgrade is still required.

2. Market data entitlement coverage:
- Code now handles entitlement failures more gracefully and falls back to delayed/non-synthetic sources where possible.
- Full live US stock/options API quote coverage still depends on your IBKR market data subscriptions and account permissions.

## 6) Status
- All in-repo implementation items from this plan are now implemented.
- Remaining blockers are account/environment prerequisites above.

## 7) Operational Notes
- Keep phase-1 paper-only + manual approvals.
- Keep LLM as confirm/veto only; deterministic risk gates remain authoritative.
- Continue using API logs tab as source-of-truth for request success/error chronology.

## 8) Security Follow-up
- Rotate and replace any exposed API key values in `.env` immediately.

## 9) Phase 2 Plan (Live Shadow + Model Quality)

### 9.1 Objective
- Run the full strategy against live market conditions while keeping manual approval and paper-first safeguards.
- Improve recommendation quality, evidence coverage, and operational stability under real session conditions.

### 9.2 Scope
- In scope:
  - Live-data shadowing with recommendation, scoring, risk, and journaling fully active.
  - Manual approvals remain mandatory for all entries/exits.
  - Scanner/recommendation reliability tuning and signal-quality upgrades.
- Out of scope:
  - Fully autonomous live entries.
  - Removal of deterministic risk gates.

### 9.3 Workstreams
1. Data reliability + transport hardening:
- Tune IBKR queue pacing from observed latency/timeout telemetry.
- Reduce stale-data exposure with stronger freshness checks and confidence penalties.
- Continue scanner fallback hardening by session state and provider health.

2. Recommendation quality upgrades:
- Add/expand indicators:
  - Relative strength vs benchmark/sector
  - Trend persistence and volatility-regime context
  - Options liquidity quality percentile and flow anomaly
  - Symbol/session quality score used as a pre-trade gate
- Improve direction confidence calibration and NO_TRADE quality rules.

3. Explainability and audit:
- Ensure each recommendation row includes:
  - indicator availability matrix
  - source provenance
  - gating pass/fail reasons
  - relevant news/events used in decisioning

4. Operational monitoring:
- Add practical alert thresholds in logs/dashboard for:
  - queue depth spikes
  - timeout rate
  - scanner fail streak
  - stale market-data ratio

### 9.4 Milestone Sequence
1. Week 1-2:
- Data freshness gates + scanner/provider runtime tuning.

2. Week 3-4:
- Indicator set expansion + confidence calibration updates.

3. Week 5-8:
- Continuous shadow run, weekly policy recalibration, acceptance tracking.

### 9.5 Acceptance Criteria (Phase 2 Exit Gate)
- Minimum 8 weeks shadow/paper run.
- Positive expectancy after costs.
- Profit factor > 1.2.
- Max drawdown <= 12%.
- No risk-policy violations.
- No sustained timeout storm after restart/reconnect windows.

## 10) Phase 3 Plan (Controlled Live Execution)

### 10.1 Objective
- Transition from shadow to controlled live execution with strict staged risk controls.

### 10.2 Rollout Stages
1. Stage A: Live-read + manual execution only
- Bot continues recommendations/proposals/audit; user executes manually.

2. Stage B: Live submission with manual approvals
- Bot submits live orders only after explicit user approval.
- Manual approval remains required for entries and exits by default.

3. Stage C: Optional constrained automation
- Optional auto-exit enablement (TP/SL/max-hold/pre-event) under reduced risk limits.
- Entries remain manual-approval unless explicitly changed later.

### 10.3 Live Risk Defaults (Initial)
- Per-trade risk reduced below phase-1 default (recommended initial range: 0.5%-1.0%).
- Daily loss halt reduced (recommended initial range: 2%-3%).
- Session/environment mismatch keeps kill switch active until resolved.

### 10.4 Phase 3 Deliverables
- Live/paper/shadow attribution cleanly separated in reporting.
- Order lifecycle accuracy and reconciliation stability under reconnect events.
- Incident runbook:
  - entitlement loss
  - connectivity drop
  - queue saturation
  - emergency kill-switch workflow

### 10.5 Acceptance Criteria (Phase 3 Stabilization)
- 30+ live trades with no policy violations.
- Reconnect recovery within target operational window.
- Local vs broker lifecycle reconciliation remains consistent.
- Kill switch and halt behavior validated by controlled drills.

## 11) Phase Transition Checklists

### 11.1 Entry Gate to Phase 2
- IBKR connectivity stable in your environment.
- Protocol compatibility warning (min version 163 path) resolved or explicitly accepted as a known blocker for specific operations.
- Market-data entitlements aligned with target instruments.

### 11.2 Entry Gate to Phase 3
- Phase 2 acceptance criteria met and documented.
- Live risk limits configured to reduced starting values.
- Manual approval path and kill-switch controls tested end-to-end.
