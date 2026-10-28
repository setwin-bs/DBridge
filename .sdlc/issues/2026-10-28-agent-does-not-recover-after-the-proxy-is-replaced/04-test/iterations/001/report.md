---
artifact: report
iteration: "001"
status: green
spec: "001"
build: "001"
reason: ""
next: deploy
docs:
  - docs/architecture.md
  - docs/concepts.md
  - docs/commands.md
  - docs/configuration.md
date: 2026-10-28
---

# Test report — Agent does not recover after the proxy is replaced

## Environment

- Commit / branch: `5ede129` plus the uncommitted working tree / `2026-10-28-agent-does-not-recover-after-the-proxy-is-replaced`
- OS: Linux (Ubuntu 24.04), loopback only; no cluster or Docker used for this run
- Runtime: Python 3.14.8; `unittest.IsolatedAsyncioTestCase`; no third-party test dependencies
- Fixtures: a local asyncio TCP echo server on `127.0.0.1:9999`; proxy/agent on non-default ports (`3151/4281/7111/9111`, plus isolated `3152/4282/7112/9112` and `3153-3155` for compatibility)
- Non-default timings in the integration runs (chosen for speed, not the production defaults): `CONTROL_IDLE_TIMEOUT_SECONDS=3`, `READY_TIMEOUT_SECONDS=2`, `RETRY_DELAY_SECONDS=0.5`, `MIN_THREADS=1`, `PING_PONG_INTERVAL_SECONDS=1`, `FORWARD_IDLE_TIMEOUT_SECONDS=0`
- Existing images/build: `Dockerfile.proxy` (Python 3.14) and the PyInstaller agent (3.13) were not rebuilt for this phase; the code is pure stdlib and the same files

## Traceability

| AC | Test case | Result |
|---|---|---|
| AC-1 | `test_agent.py::test_control_timeout_reconnects`, `test_failed_connection_closes_before_replacement`, `test_retry_delay_applies_to_control_timeout` | pass — silent proxy → `state=control_timeout`, socket closed, one replacement after the jittered delay |
| AC-2 | `06-reconnect.txt` — kill the proxy, start a fresh one, agent re-registers with no human action | pass — 1 registration, readiness 200, echo works; reconnect took ≈4.4 s (limit 60 s) |
| AC-3 | `test_agent.py::test_silent_stream_survives_without_idle_timeout` + `03-bridge-and-silent-stream.txt` | pass — 30.0 s with no bytes in either direction, then `end` echoed |
| AC-4 | `test_proxy.py::test_ready_timeout_closes_without_pool_loss` + `04-ready-timeout.txt` | pass — client closed at 2.0 s, `bridge_aborted reason=agent_not_ready_timeout`, pool usable afterwards |
| AC-5 | `test_proxy.py::test_readiness_returns_503_when_empty` / `..._200_with_agent`, `02-...txt` | pass for the in-repo endpoint (503/200 + bodies). The `httpGet` helm probe is a deploy deliverable (spec FC-2, separate repository) and cannot be tested here |
| AC-6 | `test_proxy.py::test_pool_report_logs_size` + `05-logs.txt` | pass — UTC `%(asctime)s %(levelname)s` lines, `pool_size=N` on change and every interval |
| AC-7 | `test_agent.py::test_service_failure_queues_single_replacement` | pass — one `service_connection_failed … exception=ConnectionRefusedError(…)`, no `TypeError`, one replacement |
| AC-8 | `test_proxy.py::test_short_handshake_is_closed` + `02-...txt` | pass — `handshake_failed reason=short_token bytes=3`, connection closed, no traceback |
| AC-9 | `07-compatibility.txt` — old proxy (HEAD) + new agent, new proxy + old agent | pass — echo bridged in both directions |
| AC-10 | `test_agent.py::test_failures_do_not_accumulate_connections` | pass — concurrent `spawn_agents` harness: peak 1 live connection, and the verifier confirmed the harness fails against both pre-fix variants (peak 460 / 1183) |

## Test matrix

| Area | Cases executed | Result | Notes |
|---|---|---|---|
| Happy path | registration, readiness 200, echo through the bridge, replacement after a bridge | pass | `02-...txt`, `03-...txt`, unit suite |
| Boundaries | 30 s silence with idle timeout 0; fragmented `con`+`nect`; buffered `pingconnect`; buffered `pongready`; empty pool; exact 16-byte token; `--control-idle-timeout 0`, `--ready-timeout 0`, `--pool-report-interval -1` | pass | `08-config-validation.txt`: all three invalid values exit 2 with the validator message; frame tests are unit cases |
| Errors | control timeout, EOF (`closed_by_proxy`), reset, short handshake, stalled handshake, `ready` timeout, wrong `ready` reply, service dial failure, ten consecutive failures | pass | every path closes with bounded waits and queues one replacement; `await_ready` also survives `OSError` |
| Permissions | none applicable | n/a | the agents port is unauthenticated by decision (intent D-6); `ALLOWED_AGENTS`/`ALLOWED_CLIENTS` were left at `*`. Deferred security work |
| Integrations | proxy ↔ agent ↔ echo, readiness HTTP, old tcpSocket probe against the new HTTP port, old/new binaries both directions | pass | `02-...txt`, `06-...txt`, `07-...txt` |
| UI | none | n/a | no user interface |

## Regression notes

- Pre-existing pool lifecycle (`test_proxy.py::AgentPoolLifecycleTest`, 3 cases): healthy agent assigned, reset agent discarded, empty pool returns `None` — pass, unchanged.
- Ping/pong keepalive with the ping task's 5 s pong timeout — pass; no `ping_loop_failed` outside the reset test.
- `FORWARD_IDLE_TIMEOUT_SECONDS=0` on both sides — pass (30 s silence); the `>0` cutoff path is unchanged and still logs `idle_timeout_seconds`.
- `no_agent_available` path (client waits 10 × 0.7 s then closes): code unchanged; covered by the empty-pool unit test, not re-timed here.
- Zombie sweep (`clean_zombies`): the only changes are a bounded close and a `pool_size` log; not exercised end-to-end in this phase (no `ZOMBIES_CLEAN_INTERVAL` run), reviewed by the verifier.
- Mixed versions: old proxy/new agent and new proxy/old agent both bridge — the wire protocol is unchanged.
- Old `tcpSocket` readiness probe still connects against the HTTP port and the proxy stays alive.

## Raw evidence

```console
$ python -m unittest discover -v
Ran 22 tests in 5.429s

OK

$ curl -s -w '\nhttp=%{http_code}\n' http://127.0.0.1:4281/      # no agent
not ready
http=503
$ curl -s -w '\nhttp=%{http_code}\n' http://127.0.0.1:4281/      # one agent
ready
http=200

$ printf 'hello' | nc 127.0.0.1 9111
hello

$ # 30 s with no bytes, then a byte
silent_seconds=30.0 bridge_alive=yes

$ # client against an agent that never sends ready
client_closed=yes elapsed=2.0s tail=b''
2026-10-28 13:45:16,278 ERROR bridge_aborted agent_token=… client_peer=… reason=agent_not_ready_timeout

$ # proxy killed, fresh proxy started
1                                        # registrations on the fresh proxy
http=200
again                                    # echo after the restart

$ # compatibility
oldproxy-newagent
agent=… state=registered peer=127.0.0.1:49234
newproxy-oldagent
2026-10-28 13:45:33,415 INFO agent=… state=registered peer=127.0.0.1:58032
tcp_probe=ok
```

Full transcripts, the tracked+untracked diff and the exact code snapshots are in
[`./evidence/`](./evidence/): `01-unit-tests.log`, `02-readiness-and-handshake.txt`,
`03-bridge-and-silent-stream.txt`, `04-ready-timeout.txt`, `05-logs.txt`,
`06-reconnect.txt`, `07-compatibility.txt`, `08-config-validation.txt`,
`00-implementation.diff`, `agent.py`, `proxy.py`, `test_agent.py`, `test_proxy.py`,
`build-publish-proxy.yml`.

## Notes

- **Independent verification**: four `sdlc-verifier` passes. The first found three blockers
  (Windows `TransportSocket` has no `ioctl`, so keepalive would crash every Windows agent;
  connection accumulation on repeated failures; no retry delay on EOF). All were fixed and
  re-verified; the final verdict is `approved-with-comments` with a single doc nit
  (`4.7 s` vs `5.429 s`), corrected. The verifier also proved the AC-10 harness is
  discriminating: pre-fix variants peak at 460 and 1,183 live connections; the fixed code
  peaks at 1.
- **One flaky test fixed during the phase**: the AC-10 harness initially ran a 1.0 s window
  and could accept only 2 connections under the jitter; widened to 2.0 s (assertions
  unchanged), 30/30 focused and 8/8 full-suite runs passed in the verifier's check.
- **Docs corrected in this phase**: the build docs still described the earlier
  queue-before-close order and a single `agent_not_ready_timeout` reason; architecture,
  concepts, commands and configuration now match the code. `sdlc_docs_check` reports no
  stale docs.
- **Not testable in this repository**: the helm `httpGet` probe and the Argo sync (deploy),
  the agent binary rollout on the tenant hosts (deploy), and the real n8n/SQL Server
  statement that motivated the 15 s cutoff (covered by the 30 s silence test).
- **Follow-up before merge**: restamp `verified_against` during the deploy docs sweep (the
  landing commit itself cannot carry its own hash); open the deferred agents-port
  authentication issue; open the paired helm/argocd PRs in deploy.
