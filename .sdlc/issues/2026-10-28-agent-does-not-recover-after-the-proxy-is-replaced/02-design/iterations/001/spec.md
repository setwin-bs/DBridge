---
artifact: spec
iteration: "001"
status: approved
approved_by: "joaozpalma <joao.palma@setwin.pt>"
approved_at: "2026-10-28T12:15:28Z"
reason: ""
supersedes: ""
superseded_by: ""
docs:
  - docs/README.md
  - docs/conventions.md
  - docs/audiences/README.md
  - docs/architecture.md
  - docs/concepts.md
  - docs/commands.md
  - docs/configuration.md
date: 2026-10-28
---

# Spec — Agent does not recover after the proxy is replaced

## Context

- Intent (business rules, requirements, open questions): [`../../../01-plan/intent.md`](../../../01-plan/intent.md)
- Origin (verbatim trigger): [`../../../01-plan/origin.md`](../../../01-plan/origin.md)
- Mockups: none — this change has no user-facing interface.

## Why this iteration

Iteration 001.

## Architecture

The change keeps the existing wire protocol — 16-byte token on connect, `ping`/`pong`
keepalive, `connect`/`ready` handshake, raw byte forwarding — and adds liveness where it was
missing. Two independent sides must each be able to detect a dead peer:

**Agent (`agent.py`)**

- The idle control loop (`proxy_reader.read(7)` waiting for the next `ping`) gets a deadline:
  `asyncio.wait_for(..., CONTROL_IDLE_TIMEOUT_SECONDS)`. Default 45 s = two missed 21 s pings
  plus margin; on expiry the agent logs `control_timeout`, closes the control socket and goes
  through the normal reconnect path (new `run_agent` task via the existing queue).
- The deadline applies only while the agent is in the pool waiting for `connect`. Once
  `connect` arrives, the loop hands the socket to `forward()` and the data path governs
  (which has no default timeout).
- `SO_KEEPALIVE` is enabled on the control socket as a kernel-level backstop, tuned where the
  platform allows it: Windows `SIO_KEEPALIVE_VALS` (idle 30 s, interval 10 s), Linux
  `TCP_KEEPIDLE`/`TCP_KEEPINTVL`/`TCP_KEEPCNT`. Best effort: a failure to tune is logged at
  DEBUG and never blocks the connection. The app-level deadline is the guarantee.
- `wait_closed()` calls are bounded (`asyncio.wait_for(..., 5)`) so a socket in a terminal
  state cannot wedge the reconnection path.
- Opening the local service (`open_connection(svc_host, svc_port)`) no longer uses
  `return_exceptions=True` followed by tuple unpacking. A failure is logged with its real
  exception, the control connection is closed without `ready`, and the pool is refilled.
- Exactly one replacement connection is queued per lost control connection. Today the
  handshake-failure branch queues one and `finally` queues another because
  `new_agent_spawned` is still `False`, so every failed handshake doubles the number of
  reconnecting agents.
- `FORWARD_IDLE_TIMEOUT_SECONDS` defaults to `0` (no cutoff). The option remains.

**Proxy (`proxy.py`)**

- After sending `connect`, the proxy waits for `ready` with a new
  `READY_TIMEOUT_SECONDS` (default 30). On expiry it logs `bridge_aborted
  reason=agent_not_ready_timeout`, closes both sides and keeps the pool intact.
- The agent handshake reads exactly 16 bytes (`readexactly(16)`); a short or malformed
  handshake is logged and closed instead of raising from `client_connected_cb`. Authentication
  of the agents port stays out of scope.
- Port 4260 becomes a minimal HTTP/1.1 endpoint: `200 OK` when the pool has at least one
  agent, `503 Service Unavailable` when it has none, body `ready` / `not ready`. Port 3130
  remains a plain TCP liveness endpoint.
- The pool logs its size at INFO on registration, assignment and discard, and a periodic
  `pool_size=N` line every `POOL_REPORT_INTERVAL_SECONDS` (default 60, 0 disables).
- `FORWARD_IDLE_TIMEOUT_SECONDS` defaults to `0` (no cutoff).

**Deployment impact**

- The proxy image is rebuilt from `main` and pulled with `latest`/`Always`; because a mutable
  tag does not change the pod template, deploy must restart the proxy pods after the image is
  published. The agent is a PyInstaller one-file exe installed as an NSSM service on each
  tenant host and is upgraded manually, host by host.
- The readiness probe type is hardcoded in `setwin-bs/helm-charts`
  (`charts/dbridge/templates/deployment.yaml`) and configured through
  `setwin-bs/argocd-apps` (`ar-prod/values/dbridge.yaml`). Deploy ships paired PRs: the chart
  gains a probe-type option and the values select `httpGet` for readiness. The runtime only
  reflects it after Argo syncs.
- No protocol change: any order of proxy and agent upgrade keeps working (GR-6).

## Data model

None. The proxy is stateless beyond the in-memory agent pool; no schema, no migration, no
persisted data.

## APIs / routes

| Endpoint | Method | Auth | Response |
|---|---|---|---|
| `:4260/` (readiness) | GET | none, in-cluster | `200 OK` + `ready` when the pool is non-empty; `503 Service Unavailable` + `not ready` when empty |
| `:3130/` (liveness) | TCP connect | none, in-cluster | existing plain-text greeting, unchanged |

The client (`:9000`) and agent (`:7000`) TCP protocols are unchanged.

## Audiences and screens affected

- `platform operators` — new: readiness is meaningful; empty pool is visible and marks the
  tenant not-ready; logs carry timestamps and pool size.
- `developers` — new configuration variables and the reconnect/liveness behaviour.

No end-user screens. Both audiences are new to the docs registry; the docs fleet registers
them.

## UI / UX

None. No user interface; no mockups, no Storybook.

## Security

- No new external surface. The readiness endpoint exposes only a boolean over an
  in-cluster port.
- The agents port remains unauthenticated: any peer that sends 16 bytes is registered as an
  agent (an HTTP probe was observed being accepted). **This is explicitly out of scope for
  this issue (intent D-6) and is a deferred future task**, tracked as a separate security
  issue when it is opened — not part of this fix. The handshake change here only removes the
  crash on short reads; it does not authenticate anyone.
- No new dependencies, no secrets, no PII.

## Observability

- Logging format becomes `%(asctime)s %(levelname)s %(message)s` with `converter = time.gmtime`
  (UTC) on both processes.
- New/changed lines: `agent=<token> control_timeout ...`, `agent=<token> state=reconnecting
  reason=...`, `bridge_aborted ... reason=agent_not_ready_timeout`, `pool_size=N` on change
  and every 60 s, `service_connection_failed exception=...`.
- No metrics or traces are added; the code base has no instrumentation and a Prometheus
  endpoint is out of scope. The readiness signal is the alerting hook: an empty pool marks the
  tenant not-ready, which existing Argo/Kubernetes alerting can consume.

## Testing strategy

- Unit, `test_agent.py` (new, `unittest.IsolatedAsyncioTestCase`, short deadlines): silence
  from the proxy triggers a timeout and a reconnect; an abrupt proxy close triggers a
  reconnect; a silent stream with `FORWARD_IDLE_TIMEOUT_SECONDS=0` still delivers later
  bytes; a local-service failure logs and closes without `ready`.
- Unit, `test_proxy.py` (extended): an agent that never sends `ready` is closed within
  `READY_TIMEOUT_SECONDS` without reducing the pool; readiness returns 503/200 with an
  empty/non-empty pool; a short handshake is closed without an unhandled exception.
- Integration/manual: with a real agent connected, delete the proxy pod; assert that the new
  proxy logs `state=registered` for every expected agent within 60 s, with no action on the
  agent host. Run in staging first, then on one tenant.
- CI: add a `test` job to `.github/workflows/build-publish-proxy.yml` running
  `python -m unittest -v` on Python 3.14 before the image build.
- Tests never exercise the real cluster; the pod-deletion check is manual and captured as
  evidence in phase 04.

## Acceptance criteria

- AC-1: Given an agent registered and idle, when the proxy stops sending pings, the agent
  detects the loss within `CONTROL_IDLE_TIMEOUT_SECONDS` + 2 s, logs
  `control_timeout`/`reconnecting`, closes the socket and opens a new connection.
- AC-2: Given a new proxy process with no agents, when an agent with the new binary is
  running, it registers `MIN_THREADS` control connections within 60 s without human action.
- AC-3: Given `FORWARD_IDLE_TIMEOUT_SECONDS=0`, a bridge that carries no bytes for 30 s stays
  open and delivers a byte sent afterwards.
- AC-4: Given an agent that receives `connect` and never sends `ready`, the proxy closes the
  client and the agent within `READY_TIMEOUT_SECONDS`; the pool loses no slot permanently and
  the event is logged as `agent_not_ready_timeout`.
- AC-5: `GET :4260` returns `200` with at least one agent in the pool and `503` with none; the
  helm chart renders an `httpGet` readiness probe on port 4260.
- AC-6: Every log line includes a UTC timestamp and level, and the pool size is logged on
  change and at least once per 60 s at INFO.
- AC-7: A failure to open the local service produces one `service_connection_failed` line
  with the real exception and closes the control connection cleanly (no traceback).
- AC-8: A handshake shorter than 16 bytes or a reset mid-handshake closes the connection
  without an unhandled exception in `client_connected_cb`.
- AC-9: Proxy and agent running mixed versions still bridge successfully (no wire-format
  change).
- AC-10: A lost control connection produces exactly one reconnect attempt; after ten
  consecutive handshake failures the number of live control connections does not grow.

## Estimate

The board is intentionally not configured for this repository (`.sdlc/config.json`
`board.checked` records the human's decision), so no board fields exist to estimate against.
Engineering size: **S–M** — two Python files, one new test file, CI test job, six docs, and
two files in the helm chart repository.

## Flagged concerns

- FC-1: With a single replica per tenant, `503` readiness removes the only Service endpoint,
  so clients get `connection refused` instead of a 7 s wait and `socket hang up`. This is the
  intended BR-5 signal and changes the n8n error text. Accepted by the human at the spec gate.
- FC-2: The readiness change lives in `setwin-bs/helm-charts` + `setwin-bs/argocd-apps`;
  until Argo syncs, the cluster keeps the `tcpSocket` probe and the visibility half of this
  fix is not live. Accepted at the spec gate; deploy opens the paired PRs.
- FC-3: Windows keepalive tuning is best effort and platform-specific. The app-level deadline
  is the guarantee, so a failed tuning is not a defect. Accepted at the spec gate.
- FC-4: The agents port stays unauthenticated (intent D-6). Accepted at the intent gate:
  security is out of scope for this issue and remains a deferred future task.
- FC-5: The 45 s deadline means a tenant can be unserved for up to ~46 s after a connection
  loss. That is inside the 60 s business rule but visibly slower than immediate failover.
  Accepted at the spec gate.
- FC-6: The reference docs (`commands.md`, `configuration.md`) already carry the spec'd
  defaults, marked as new/changed, before the code lands. Build must re-verify them against
  the implementation and restamp `verified_against`; until then they describe the target, not
  HEAD. Accepted at the spec gate.

## Resolved open questions

- OQ-1 (how to detect a dead connection without FIN/RST) → application-level deadline on the
  control read, with OS keepalive as a backstop (Architecture, AC-1).
- OQ-2 (configurable deadline and value) → `CONTROL_IDLE_TIMEOUT_SECONDS`, default 45 s,
  validated `> 0`; 45 s sits two missed pings above the 21 s cadence (Architecture, AC-1).
- OQ-3 (where the probe change lives) → `setwin-bs/helm-charts` chart plus
  `setwin-bs/argocd-apps` values; paired PRs in deploy (Architecture, FC-2, AC-5).

## Decisions log

- D-1: Control-connection liveness is an app-level read deadline on the agent, backed by OS
  keepalive; default 45 s (human + design, 2026-10-28).
- D-2: Readiness is a real HTTP endpoint (200/503) and the probe becomes `httpGet`; the chart
  change ships as paired PRs (human, 2026-10-28).
- D-3: `FORWARD_IDLE_TIMEOUT_SECONDS` defaults to 0 on both sides; the option stays
  (intent D-2).
- D-4: The proxy bounds the wait for `ready` at 30 s and never leaks the agent (intent D-4).
- D-5: The wire protocol does not change; mixed proxy/agent versions must bridge (GR-6).
- D-6: Reconnect keeps the existing fixed retry delay with a small random jitter added to
  avoid synchronized bursts; no exponential backoff (design, 2026-10-28).
- D-7: The docs contract is created in this phase for the areas this change touches
  (index, architecture, reference) plus the audience registry; areas with no content in this
  repository are deferred to the deploy sweep (design, 2026-10-28).
- D-8: A CI test job is added to the proxy workflow, since none exists today (design,
  2026-10-28).
- D-9: The double replacement queued by the handshake-failure path is fixed in this change
  (AC-10), because it can grow the agent pool without bound while a proxy flaps (design,
  2026-10-28).
