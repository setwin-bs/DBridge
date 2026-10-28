---
artifact: plan
iteration: "001"
status: approved
approved_by: "joaozpalma <joao.palma@setwin.pt>"
approved_at: "2026-10-28T12:16:53Z"
spec: "001"
reason: ""
next: ""
date: 2026-10-28
---

# Plan — Agent does not recover after the proxy is replaced

## Inputs

- Intent: [`../../../01-plan/intent.md`](../../../01-plan/intent.md)
- Spec: [`../../../02-design/iterations/001/spec.md`](../../../02-design/iterations/001/spec.md)
- Mockups: none (no user-facing interface)

## Why this iteration

Iteration 001.

## Files that change

| Path | Action | Why |
|---|---|---|
| `agent.py` | edit | Control-connection deadline, keepalive, one replacement per loss, bounded close, real service error, UTC logs, new defaults |
| `proxy.py` | edit | `ready` timeout that skips stale `pong`, `readexactly` handshake, HTTP readiness, pool logs, UTC logs, new defaults |
| `test_agent.py` | new | Unit tests for the agent liveness, reconnect, one-replacement rule and no-cutoff data path |
| `test_proxy.py` | edit | Readiness 200/503, `ready` timeout without pool leak, short handshake, stale-`pong` skip |
| `.github/workflows/build-publish-proxy.yml` | edit | Add the `test` job before the image build |
| `docs/architecture.md` | edit | Replace current-behaviour text with the implemented liveness/readiness design |
| `docs/concepts.md` | edit | Add/confirm the control-timeout and readiness terms |
| `docs/commands.md` | edit | Readiness HTTP contract, test command, CI job |
| `docs/configuration.md` | edit | Verify the three new variables and the changed defaults against the code |
| `docs/README.md` | edit | Restamp `verified_against` after the build commit |
| `docs/audiences/README.md` | edit | Restamp `verified_against` after the build commit |

Not in this repository, handled by deploy (spec FC-2): `setwin-bs/helm-charts`
(`charts/dbridge/templates/deployment.yaml`, probe type) and `setwin-bs/argocd-apps`
(`ar-prod/values/dbridge.yaml`, `httpGet` selection).

## Order of work

1. **Agent logging and configuration.** Add `%(asctime)s %(levelname)s %(message)s` with
   `logging.Formatter.converter = time.gmtime`; add `--control-idle-timeout` /
   `CONTROL_IDLE_TIMEOUT_SECONDS` (default 45, validated `> 0` and `<= 300`) and change
   `--forward-idle-timeout` / `FORWARD_IDLE_TIMEOUT_SECONDS` default to `0`.
   Verify: `python -m unittest -v` still green; `python agent.py --help` shows the new flag
   and default.

2. **Agent control frame + deadline + keepalive.** Add `read_control_frame(reader, deadline)`
   that accumulates up to 7 bytes until it has exactly `ping` or `connect` (so a fragmented
   `connect` is not mistaken for a failure), applies the deadline to the whole frame, and
   raises `asyncio.TimeoutError` on silence. Add `enable_keepalive(writer)` (`SO_KEEPALIVE`;
   Windows `SIO_KEEPALIVE_VALS` idle 30 s / interval 10 s; Linux `TCP_KEEPIDLE` 30,
   `TCP_KEEPINTVL` 10, `TCP_KEEPCNT` 3; failures logged at DEBUG). Use the frame reader in
   `run_agent`; on timeout log `state=control_timeout` and take the reconnect path instead of
   logging `handshake_failed`.
   Verify: `test_agent.py` cases `test_control_timeout_reconnects`,
   `test_fragmented_connect_is_accepted`, `test_keepalive_is_best_effort`.

3. **One replacement per lost connection and a clean service error.** Replace
   `new_agent_spawned` with a single `replacement_queued` flag set wherever `queue.put(1)`
   happens, so the handshake-failure branch and `finally` cannot both queue. Replace the
   `gather(..., return_exceptions=True)` + tuple unpack with a direct
   `asyncio.open_connection`, catching `OSError` to log `service_connection_failed` with the
   real exception and close the control connection without sending `ready`. Add a bounded
   `close_writer(writer)` helper (`wait_for(wait_closed(), 5)`).
   Verify: `test_agent.py` cases `test_service_failure_queues_single_replacement`,
   `test_handshake_failure_queues_single_replacement`.

4. **Agent reconnect jitter.** Keep the fixed retry delay and add `random.uniform(0, 0.5)` s
   to the sleep on every reconnect path (design D-6).
   Verify: unit test asserts the sleep helper is called with a value in
   `[retry, retry + 0.5]`.

5. **Proxy logging, configuration and pool report.** Same UTC format; add
   `--ready-timeout` / `READY_TIMEOUT_SECONDS` (default 30, validated `> 0` and `<= 300`),
   `--pool-report-interval` / `POOL_REPORT_INTERVAL_SECONDS` (default 60, validated `>= 0`
   and `<= 300`, `0` disables), and change `FORWARD_IDLE_TIMEOUT_SECONDS` default to `0`.
   Add `Context.report_pool(interval)` logging `pool_size=N` at INFO every interval, and log
   `pool_size` on add/pop/discard.
   Verify: `python proxy.py --help`; `test_proxy.py::test_pool_report_logs_size`.

6. **Proxy readiness over HTTP.** Rewrite `handle_readiness` to read the request line with a
   short bound, then answer `HTTP/1.1 200 OK` / `HTTP/1.1 503 Service Unavailable` with
   `Content-Type: text/plain`, `Content-Length`, `Connection: close` and body `ready` /
   `not ready`, based on `context.length()`. Keep `handle_liveness` unchanged (plain TCP).
   Verify: `test_proxy.py::test_readiness_returns_503_when_empty`,
   `test_readiness_returns_200_with_agent`.

7. **Proxy `ready` wait and handshake.** Add `await_ready(reader, timeout)` that repeatedly
   reads 5-byte frames, discards `pong` (the keepalive race), accepts exactly `ready`, and
   raises on timeout/EOF. Use it after sending `connect`; on failure log
   `bridge_aborted reason=agent_not_ready_timeout` and close both sides with bounded waits.
   Change `add_agent` to `readexactly(16)` inside a 10 s bound, logging
   `handshake_failed reason=short_token bytes=N` and closing on
   `IncompleteReadError`/`ValueError`.
   Verify: `test_proxy.py::test_ready_timeout_closes_without_pool_loss`,
   `test_ready_skips_stale_pong`, `test_short_handshake_is_closed`.

8. **CI test job.** Add a `test` job to `.github/workflows/build-publish-proxy.yml`
   (ubuntu-latest, `actions/setup-python@v7` with `3.14`, `python -m unittest discover -v`)
   and make `build` `needs: test`.
   Verify: the workflow file parses (`python -c "import yaml,sys;yaml.safe_load(...)"`) and the
   test command passes locally.

9. **Docs.** Run the docs fleet for `docs-architecture` and `docs-reference`: replace the
   "current behaviour" text with the implemented design, confirm every new variable and
   default in `configuration.md`, and update the readiness contract in `commands.md`. Restamp
   `verified_against` on all seven docs after the build commit.
   Verify: `sdlc_docs_check` reports no stale doc except the ones intentionally restamped at
   the next commit.

10. **End-to-end smoke.** Start the proxy on non-default ports and one agent against a local
    TCP echo service; confirm registration, a bridged echo, `GET :4260` → 200, then stop the
    proxy and confirm the agent logs `control_timeout`/reconnect and registers again on a
    fresh proxy within 60 s. Confirm a 30 s silent stream survives with
    `FORWARD_IDLE_TIMEOUT_SECONDS=0`.
    Verify: transcript captured for phase 04.

## Dependencies and migrations

- No new runtime dependencies. `socket`, `random`, `time` are already imported or stdlib.
- No data migration, no schema.
- Compatibility: the wire protocol is unchanged. Old proxy + new agent works (the old proxy
  pings every 21 s; the new agent's 45 s deadline is safe). New proxy + old agent works (the
  old agent answers ping and `ready` as before). Rollout may go in either order.
- Rollout sequencing: publish the proxy image, restart the proxy pods, upgrade agents one host
  at a time (spec D-5, FC-2). The helm `httpGet` change ships separately.

## Tests mapped to acceptance criteria

| AC | Test (file / name) | Level |
|---|---|---|
| AC-1 | `test_agent.py::test_control_timeout_reconnects` | unit |
| AC-2 | manual: delete proxy pod in staging, agents re-register < 60 s (step 10) | integration |
| AC-3 | `test_agent.py::test_silent_stream_survives_without_idle_timeout` | unit |
| AC-4 | `test_proxy.py::test_ready_timeout_closes_without_pool_loss` | unit |
| AC-5 | `test_proxy.py::test_readiness_returns_200_with_agent` + `test_readiness_returns_503_when_empty` | unit |
| AC-6 | `test_proxy.py::test_pool_report_logs_size` + log-format assertion in `test_agent.py` | unit |
| AC-7 | `test_agent.py::test_service_failure_queues_single_replacement` | unit |
| AC-8 | `test_proxy.py::test_short_handshake_is_closed` | unit |
| AC-9 | manual: old proxy + new agent, new proxy + old agent (step 10) | manual |
| AC-10 | `test_agent.py::test_handshake_failure_queues_single_replacement` | unit |

## Risks

- **HTTP readiness vs the old tcpSocket probe.** Until the chart changes, the probe still
  opens a TCP connection; the handler must tolerate a client that never sends a request and a
  client that closes early (wrap reads/writes in `try/except OSError`). The old probe keeps
  passing, so there is no window where the pod is falsely unready.
- **Stale `pong` between keepalive and `connect`.** Handled by `await_ready` discarding
  `pong`; the test pins it.
- **`readexactly` on a peer that sends fewer bytes and disconnects.** The 10 s bound plus
  `IncompleteReadError` handling keeps the handler from leaking.
- **Windows keepalive ioctl differences.** Guarded by `hasattr` and `try/except OSError`;
  the app deadline remains the guarantee.
- **Flaky timing tests.** Deadlines in tests are 0.2–0.5 s and assertions have a 2 s ceiling;
  no real network, only loopback.
- **Idle timeout 0.** Long-lived idle client connections now stay open; the OS/DB and the
  client pool own their lifetime. Accepted by intent D-2.

## Rollback

- Code: revert `agent.py`, `proxy.py`, the tests and the workflow to the previous commit.
  The wire protocol did not change, so a proxy rollback is safe while new agents run, and an
  agent rollback is safe while the new proxy runs.
- Docs: revert the paths listed in `outcome.md`'s `docs:`.
- Helm: the probe PR can be reverted independently; the proxy keeps accepting TCP probes, so
  no outage either way.
- No data to restore.
