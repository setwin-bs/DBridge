---
title: Concepts
audience: [developers, platform operators]
update_when:
  - a new domain term is introduced
  - a definition or its boundaries change
validated_against: ["agent.py", "proxy.py"]
verified_against: 31d14ba
screenshots: false
---

# Concepts


The domain language of DBridge, one row per term. The third column points at the identifiers
that implement the term, so a reader can move from the word to the code. Definitions describe the
implemented code; the flows that use these terms are in [architecture.md](architecture.md).

## Glossary

| Term | Definition | Where it lives |
|---|---|---|
| **agent** | The process on a tenant host that dials out to the proxy and bridges assigned connections to a local service. One agent process holds several control connections; the connection, not the process, is what the proxy pools. An agent gives up on a control connection after `CONTROL_IDLE_TIMEOUT_SECONDS` of silence. | `agent.py`; one `run_agent` task per control connection, created by `spawn_agents` |
| **agent pool** | The proxy's in-memory set of registered agent connections available for assignment, in arrival order. It is never persisted and is empty after a proxy restart until agents register again. Its size is logged on registration, assignment and discard, and periodically. | `proxy.Context.agents`, a `list[Agent]` guarded by `Context.lock`; `Context.length()`, `Context.report_pool()` |
| **assignment** | Taking one connection out of the pool for one client: the oldest entry is removed, its keepalive task is stopped, and the proxy sends `connect`. The connection is not returned to the pool afterwards. | `Context.pop_agent()`, `handle_client`; log `state=assigned remaining_pool=N` |
| **bridge** | One client connection carried end to end through the proxy and the agent to the tenant service. Each process runs two one-way copy loops for it; when one direction ends it closes its destination and the other unwinds. There is no idle cutoff unless `FORWARD_IDLE_TIMEOUT_SECONDS` is set above 0. | `handle_client` (proxy), the `forward()` gather in `run_agent`; log `bridge_started` / `bridge_finished`, `bridge=preparing` / `bridge=finished` |
| **client** | Any TCP peer of the proxy's client port that speaks the database protocol; n8n in production. The proxy never parses what it sends. | `:9000` (`PORT_CLIENTS`), `handle_client` |
| **control connection** | The persistent outbound TCP connection an agent opens to the proxy's agents port, identified by its token, kept alive by `ping`/`pong` while idle, and promoted to the data path when the proxy sends `connect`. The same socket plays both roles. While idle it has a read deadline, so a proxy that vanishes without closing the socket is still detected. | `handle_agent` and `Context.add_agent` (proxy), `asyncio.open_connection` and `read_control_frame` in `run_agent` (agent) |
| **control timeout** | The agent-side deadline on the wait for the proxy's next control frame: `ping` or `connect`. When it expires the agent logs `state=control_timeout`, closes the control socket with a bounded wait and queues one replacement, which registers against whatever proxy is running. Default 45 s, two missed 21 s pings plus margin. | `read_control_frame()`, `CONTROL_IDLE_TIMEOUT_SECONDS` / `--control-idle-timeout` in `run_agent` |
| **data stream** | The raw bytes a bridge carries after the handshake, in each direction: `client->agent` and `agent->client` on the proxy, `proxy->service` and `service->proxy` on the agent. It follows TCP, not SQL statement boundaries. | `proxy.forward()` and `agent.forward()`, called with a `direction` label |
| **forward loop** | The copy routine used in both processes: read up to 4096 bytes, write, drain, repeat until EOF or an error, then close the destination. Its optional idle cutoff is the only data-path timer, and it is off by default (`FORWARD_IDLE_TIMEOUT_SECONDS=0`). | `forward()` in `proxy.py` and `agent.py` |
| **keepalive (ping/pong)** | The proxy-driven liveness check on an idle control connection: `ping` immediately after registration and then every `PING_PONG_INTERVAL_SECONDS`, `pong` expected within 5 s. It runs only while the connection is in the pool. A `pong` already in flight when the keepalive stops can still be buffered; the proxy skips it while waiting for `ready`. | `Agent.run_ping_pong()`, `await_ready()`; started by `add_agent`, stopped by `pop_agent` |
| **liveness probe** | The signal that the proxy process is up. It answers a connect on its port with a fixed greeting and says nothing about the pool. | `:3130` (`PORT_LIVENESS`), `handle_liveness` |
| **OS keepalive** | Kernel-level `SO_KEEPALIVE` on the agent's control socket, tuned where the platform allows it (Linux `TCP_KEEPIDLE` 30 s, `TCP_KEEPINTVL` 10 s, `TCP_KEEPCNT` 3; Windows `SIO_KEEPALIVE_VALS` idle 30 s, interval 10 s). Best effort: a failed tuning is logged at DEBUG and never blocks the connection. A backstop, not the guarantee — the control timeout is. | `enable_keepalive()` in `agent.py` |
| **pool size** | The number of agent connections currently registered in the pool. It is logged at INFO on registration, assignment and discard, and periodically every `POOL_REPORT_INTERVAL_SECONDS` (60 s by default; `0` disables only the periodic line). | `Context.length()`, `Context.report_pool()`; logs `pool_size=N`, `remaining_pool=N` |
| **proxy** | The process clients reach. It owns the pool, assigns agents and forwards bytes; one proxy deployment serves one tenant. | `proxy.py`, `Context` singleton |
| **readiness probe** | The signal that the proxy has at least one agent to assign. It is an HTTP/1.1 endpoint: `200 OK` with body `ready` when the pool is non-empty, `503 Service Unavailable` with body `not ready` when it is empty. It reads the request head for up to 2 s and also answers a bare TCP connect. | `:4260` (`PORT_READINESS`), `handle_readiness` |
| **ready** | The five-byte message an agent sends after it has connected to the local service, telling the proxy it may start the bridge. The proxy waits for it for at most `READY_TIMEOUT_SECONDS` (30 s) after `connect`, skipping a stale `pong`, before any client byte is forwarded. | `run_agent` (agent), `handle_client` and `await_ready` (proxy) |
| **registration** | The proxy's acceptance of a new control connection: read exactly 16 bytes within a 10 s bound as a UUID, create the agent entry, start its keepalive, append it to the pool. A short handshake is logged as `handshake_failed reason=short_token bytes=N`; a stalled or reset one as `reason=handshake_error`. Both are closed. It is not authenticated and no token is checked against a known value. | `Context.add_agent()`; log `state=registered peer=...` then `pool_size=N` |
| **replacement connection** | The fresh `run_agent` task an agent queues after a control connection ends or is promoted to a bridge. `queue_replacement()` sets a flag, so exactly one replacement is queued per lost connection and `spawn_agents` starts it with a new token. Before the bridge, `finally` closes the failed socket, waits `RETRY_DELAY_SECONDS` plus 0–0.5 s of jitter, then queues the successor; a service-dial failure has no extra delay because its replacement was queued at `connect`. | `queue_replacement()` and `replacement_queued` in `run_agent`, `spawn_agents`, `reconnect_delay()` |
| **service** | The tenant's database endpoint that the agent dials when assigned. The agent, not the proxy, opens this connection; the proxy never learns its address. A failed dial is logged as `service_connection_failed` with the real exception and the control connection is closed without `ready`. | `SERVICE_HOST` / `SERVICE_PORT`, `asyncio.open_connection` in `run_agent` |
| **tenant** | One customer environment whose database access goes through its own proxy deployment and the agents on its hosts, for example `jle-beeshop` or `seteras-beeshop`. Not a code identifier: isolation is per deployment, and log lines identify the actor by agent token, not by tenant name. | No identifier in `agent.py` or `proxy.py` |
| **token** | The 16 raw bytes of a `uuid.UUID` an agent writes first on a control connection; the proxy reads exactly 16 bytes and uses the value to label the connection in every log line. It identifies a connection, not a credential, and changes on every reconnect. | Written by `run_agent` (`token.bytes`); read by `Context.add_agent` (`readexactly(16)`, then `uuid.UUID(bytes=...)`) |
| **zombie** | A pool entry whose socket is already dead: the reader is at EOF or the writer is closing. Assignment skips and discards them; a background sweep can remove them earlier. | `Context.pop_agent()`; `Context.clean_zombies()` with `ZOMBIES_CLEAN_INTERVAL` |

## Usage notes

- **agent vs agent pool vs bridge.** The pool holds idle connections; a bridge consumes one of
  them for its whole lifetime. One agent connection therefore serves at most one client, and a
  long bridge reduces the idle count until a replacement connection registers.
- **control connection vs data stream.** They are the same socket. Before assignment it carries
  `ping`/`pong` under the control deadline; after `connect` and `ready` the same bytes become the
  raw data stream, and only `FORWARD_IDLE_TIMEOUT_SECONDS` governs the data path (off by default).
  A change to one phase affects the other.
- **A lost connection vs an idle one.** At the application level both look like silence. The
  control timeout is what turns silence into a decision, so reconnection does not depend on the
  peer closing the socket or on a FIN/RST arriving. The OS keepalive is only a backstop.
- **Replacement, not retry.** The agent keeps its capacity with a fresh `run_agent` task queued by
  the connection that ends; it does not retry on the same socket. Exactly one replacement is
  queued per lost connection, and only after the failed socket is closed and the jittered delay
  has elapsed, so a failed handshake cannot multiply connections.
- **tenant vs proxy.** One tenant maps to one proxy deployment and its own pool; tenants are not
  multiplexed inside a single pool. Nothing in the code carries a tenant field, so a log line is
  attributed to a tenant through the deployment it came from plus the agent token.
- **token is not a secret.** It is not verified, not shared in advance and not reused: it is a
  per-connection label. Do not use "credential" or "session token" for it.
- **readiness vs liveness.** Liveness answers "the process is up" with a TCP greeting; readiness
  answers "there is an agent to serve a client" with an HTTP status. A bare TCP probe still
  connects to `:4260` and passes; only an HTTP `GET` observes the `503`.
- **"thread" is a legacy name.** `MIN_THREADS` / `--min-threads` is the number of initial agent
  tasks, not OS threads. The implementation is asyncio throughout; prefer "agent connections" when
  describing it.
