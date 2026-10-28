---
artifact: outcome
iteration: "001"
status: approved
reason: ""
next: test
docs:
  - docs/architecture.md
  - docs/concepts.md
  - docs/commands.md
  - docs/configuration.md
date: 2026-10-28
---

# Outcome — Agent does not recover after the proxy is replaced

## What was done

Implemented the whole spec in the working tree, uncommitted (the build phase commits only
what the human approves).

- **Agent (`agent.py`)** — control frames are read as exact 4-byte prefixes (`ping` vs
  `conn`+`ect`) with `CONTROL_IDLE_TIMEOUT_SECONDS` (default 45) applied to the frame;
  silence logs `state=control_timeout` and reconnects. `SO_KEEPALIVE` is enabled with
  Windows `SIO_KEEPALIVE_VALS` / Linux `TCP_KEEPIDLE` tuning, best effort. Exactly one
  replacement is queued per lost connection (`replacement_queued`), removing the double
  queue in the handshake-failure path. A failed local-service dial logs
  `service_connection_failed` with the real exception instead of a `TypeError` from
  unpacking a `gather(return_exceptions=True)` result. `wait_closed()` is bounded at 5 s.
  `FORWARD_IDLE_TIMEOUT_SECONDS` defaults to 0; the reconnect sleep keeps the fixed delay
  and adds up to 0.5 s jitter. Logs are UTC with level.
- **Proxy (`proxy.py`)** — waits for `ready` with `READY_TIMEOUT_SECONDS` (default 30) while
  discarding a stale `pong`, and logs `bridge_aborted` with
  `reason=agent_not_ready_timeout` (expiry), `agent_not_ready_closed` (EOF/reset) or
  `agent_not_ready` (wrong reply); the short-handshake crash is gone (`readexactly(16)` under a
  10 s bound); port 4260 answers
  HTTP 200/503 based on the pool and 3130 stays a plain TCP liveness endpoint; `pool_size`
  is logged on registration/assignment/discard and periodically
  (`POOL_REPORT_INTERVAL_SECONDS`, default 60); `FORWARD_IDLE_TIMEOUT_SECONDS` defaults to 0;
  logs are UTC with level.
- **Tests** — `test_agent.py` (13 cases) and `test_proxy.py` (9 cases, 3 pre-existing) run
  green: 22 tests in 5.429 s.
- **CI** — `build-publish-proxy.yml` gained a `test` job (Python 3.14,
  `python -m unittest discover -v`); `build` now `needs: test`.
- **Docs** — architecture, concepts, commands and configuration now describe the implemented
  behaviour, including the readiness HTTP contract, the new variables and the defaults.

Not done here (deploy or a separate issue): the helm/Argo `httpGet` probe change, the agent
binary rollout on tenant hosts, agent-port authentication.

## Deviations from plan.md

| Planned | Actual | Reason |
|---|---|---|
| Restamp `verified_against` on all seven docs after the build commit (step 9) | Left at `6f1fec7`; `sdlc_docs_check` reports no stale docs | The build phase is instructed not to commit (commits wait for the human), so there is no commit to stamp; the restamp must be part of the commit that lands the code in test/deploy. |
| Spec Observability names `agent=<token> state=reconnecting reason=...` | The agent emits `state=control_timeout`, `state=closed_by_proxy` and `connection_error exception=...` | The event names are more precise for each loss path; the spec wording can be corrected in a later iteration if needed. |
| Plan names `test_keepalive_is_best_effort` | Implemented as `test_enable_keepalive_is_best_effort` | Name only. |
| Plan step 2 tests `read_control_frame` accumulation | Added `test_buffered_frames_are_not_glued` and changed the reader to exact 4-byte prefixes after the first version glued `ping` + `connect`; `test_ready_skips_stale_pong` caught the same over-read on the proxy side | A real defect found while running the plan's tests; documented here instead of silently changing behaviour. |
| Keepalive, one replacement per loss, retry delay on every failure | Kept, and hardened after the `sdlc-verifier` pass: the raw socket is unwrapped before `ioctl` (TransportSocket has none), every pre-bridge failure now closes the socket, waits the jittered delay and only then queues the replacement (fixes connection accumulation), `await_ready` also catches `OSError`, zombie cleanup and shutdown use the bounded `close_writer`, and the handshake/`bridge_aborted` reasons carry the actual outcome. | The verifier measured the Windows `AttributeError` and thousands of accumulated connections on the first version; these are corrections to the plan's implementation, not scope changes. |

## Evidence links

- Base commit: `5ede129` (plan approved); implementation in the working tree, uncommitted.
- Unit tests: `python -m unittest discover -v` → `Ran 22 tests in 5.429s / OK`
  (log: `04-test/iterations/001/evidence/01-unit-tests.log`).
- Smoke run (`/tmp/opencode/smoke.sh`): readiness `200` / body `ready`; echo round-trip
  `hello`; proxy killed → agent retried (`ConnectionRefusedError`) → re-registered on the new
  proxy → readiness `200` again and `again` echoed through the bridge. Agent logs carry UTC
  timestamps.
- Docs check: `sdlc_docs_check` → `Checked 7 docs in docs. Stale: none.`
- Workflow YAML parses (`yaml.safe_load`) with the new `test` job.

## Follow-ups

- Deploy: paired PRs in `setwin-bs/helm-charts` (probe type option) and
  `setwin-bs/argocd-apps` (`httpGet` for readiness), then restart the proxy pods so they pull
  the rebuilt `latest` image and upgrade the agents one host at a time.
- Security: authenticate the agents port and reject non-agent handshakes — out of scope by
  intent D-6, still a deferred future task.
- Docs: restamp `verified_against` in the commit that lands this change; add the agent
  workflow test job if the PyInstaller path should be covered by CI too.
- Operability: consider a Prometheus gauge for `pool_size` in a later issue; this change
  only logs it.
