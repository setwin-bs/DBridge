---
title: Concepts
audience: [developers, platform operators]
update_when:
  - a new domain term is introduced
  - a definition or its boundaries change
validated_against: ["agent.py", "proxy.py"]
verified_against: 6f1fec7
screenshots: false
---

# Concepts


The domain language of DBridge, one row per term. The third column points at the identifiers
that implement the term, so a reader can move from the word to the code. Definitions describe
what the code does at `6f1fec7`; the flows that use these terms are in
[architecture.md](architecture.md).

## Glossary

| Term | Definition | Where it lives |
|---|---|---|
| **agent** | The process on a tenant host that dials out to the proxy and bridges assigned connections to a local service. One agent process holds several control connections; the connection, not the process, is what the proxy pools. | `agent.py`; one `run_agent` task per control connection, created by `spawn_agents` |
| **agent pool** | The proxy's in-memory set of registered agent connections available for assignment, in arrival order. It is never persisted and is empty after a proxy restart until agents register again. | `proxy.Context.agents`, a `list[Agent]` guarded by `Context.lock`; `Context.length()` |
| **assignment** | Taking one connection out of the pool for one client: the oldest entry is removed, its keepalive task is stopped, and the proxy sends `connect`. The connection is not returned to the pool afterwards. | `Context.pop_agent()`, `handle_client`; log `state=assigned remaining_pool=N` |
| **bridge** | One client connection carried end to end through the proxy and the agent to the tenant service. Each process runs two one-way copy loops for it; when one direction ends it closes its destination and the other unwinds. | `handle_client` (proxy), the `forward()` gather in `run_agent`; log `bridge_started` / `bridge_finished`, `bridge=preparing` / `bridge=finished` |
| **client** | Any TCP peer of the proxy's client port that speaks the database protocol; n8n in production. The proxy never parses what it sends. | `:9000` (`PORT_CLIENTS`), `handle_client` |
| **control connection** | The persistent outbound TCP connection an agent opens to the proxy's agents port, identified by its token, kept alive by `ping`/`pong` while idle, and promoted to the data path when the proxy sends `connect`. The same socket plays both roles. | `handle_agent` and `Context.add_agent` (proxy), `asyncio.open_connection` in `run_agent` (agent) |
| **data stream** | The raw bytes a bridge carries after the handshake, in each direction: `client->agent` and `agent->client` on the proxy, `proxy->service` and `service->proxy` on the agent. It follows TCP, not SQL statement boundaries. | `proxy.forward()` and `agent.forward()`, called with a `direction` label |
| **forward loop** | The copy routine used in both processes: read up to 4096 bytes, write, drain, repeat until EOF or an error, then close the destination. Its optional idle cutoff is the only data-path timer. | `forward()` in `proxy.py` and `agent.py` |
| **keepalive (ping/pong)** | The proxy-driven liveness check on an idle control connection: `ping` every `PING_PONG_INTERVAL_SECONDS`, `pong` expected within 5 s. It runs only while the connection is in the pool. | `Agent.run_ping_pong()`; started by `add_agent`, stopped by `pop_agent` |
| **liveness probe** | The signal that the proxy process is up. It answers a connect on its port with a fixed greeting and says nothing about the pool. | `:3130` (`PORT_LIVENESS`), `handle_liveness` |
| **proxy** | The process clients reach. It owns the pool, assigns agents and forwards bytes; one proxy deployment serves one tenant. | `proxy.py`, `Context` singleton |
| **readiness probe** | The signal that the proxy has at least one agent to assign. Today it writes one of two plain-text sentences and closes; it is not an HTTP endpoint and returns no status code. | `:4260` (`PORT_READINESS`), `handle_readiness` |
| **ready** | The five-byte message an agent sends after it has connected to the local service, telling the proxy it may start the bridge. The proxy waits for it before any client byte is forwarded. | `run_agent` (agent), `handle_client` (proxy) |
| **registration** | The proxy's acceptance of a new control connection: read 16 bytes as a UUID, create the agent entry, start its keepalive, append it to the pool. It is not authenticated and no token is checked against a known value. | `Context.add_agent()`; log `state=registered peer=...` |
| **service** | The tenant's database endpoint that the agent dials when assigned. The agent, not the proxy, opens this connection; the proxy never learns its address. | `SERVICE_HOST` / `SERVICE_PORT`, `asyncio.open_connection` in `run_agent` |
| **tenant** | One customer environment whose database access goes through its own proxy deployment and the agents on its hosts, for example `jle-beeshop` or `seteras-beeshop`. Not a code identifier: isolation is per deployment, and log lines identify the actor by agent token, not by tenant name. | No identifier in `agent.py` or `proxy.py` |
| **token** | The 16 raw bytes of a `uuid.UUID` an agent writes first on a control connection; the proxy reads 16 bytes and uses the value to label the connection in every log line. It identifies a connection, not a credential, and changes on every reconnect. | Written by `run_agent` (`token.bytes`); read by `Context.add_agent` (`uuid.UUID(bytes=...)`) |
| **zombie** | A pool entry whose socket is already dead: the reader is at EOF or the writer is closing. Assignment skips and discards them; a background sweep can remove them earlier. | `Context.pop_agent()`; `Context.clean_zombies()` with `ZOMBIES_CLEAN_INTERVAL` |

## Usage notes

- **agent vs agent pool vs bridge.** The pool holds idle connections; a bridge consumes one of
  them for its whole lifetime. One agent connection therefore serves at most one client, and a
  long bridge reduces the idle count until a replacement connection registers.
- **control connection vs data stream.** They are the same socket. Before assignment it carries
  `ping`/`pong`; after `connect` and `ready` the same bytes become the raw data stream. A change to
  one phase affects the other.
- **tenant vs proxy.** One tenant maps to one proxy deployment and its own pool; tenants are not
  multiplexed inside a single pool. Nothing in the code carries a tenant field, so a log line is
  attributed to a tenant through the deployment it came from plus the agent token.
- **token is not a secret.** It is not verified, not shared in advance and not reused: it is a
  per-connection label. Do not use "credential" or "session token" for it.
- **readiness vs liveness.** Liveness answers "the process is up"; readiness answers "there is an
  agent to serve a client". Today both ports answer any TCP connect, so only reading the readiness
  body distinguishes an empty pool.
- **"thread" is a legacy name.** `MIN_THREADS` / `--min-threads` is the number of initial agent
  tasks, not OS threads. The implementation is asyncio throughout; prefer "agent connections" when
  describing it.
