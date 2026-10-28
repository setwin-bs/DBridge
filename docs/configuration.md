---
title: "Configuration"
audience: [platform operators, developers]
update_when:
  - an environment variable is added, renamed or removed
  - a config file, flag or default changes
  - a Dockerfile changes a default baked into an image
  - a validation rule changes
validated_against: ["agent.py", "proxy.py", "Dockerfile.proxy", "Dockerfile.agent", "docker-compose.yml"]
verified_against: 31d14ba
screenshots: false
---

# Configuration


Both processes are configured from the command line and the environment only — there is no
configuration file, no profile and no remote config service. Each flag's default is read from
its environment variable at startup (see `parse_args()` in `proxy.py` and `agent.py`), so the
precedence is always:

1. **CLI flag** on the command line,
2. **environment variable** of the same name,
3. **built-in default** — or the `ENV` baked into the image, which is the same mechanism.

One behaviour to know before relying on a variable: validators run on **flags only**. A numeric
variable that cannot be parsed silently falls back to the built-in default, and a value that
parses but is out of range is accepted as-is (e.g. `MIN_THREADS=0` starts no control
connections, `PING_PONG_INTERVAL_SECONDS=0` pings without pause). A typo in a deployment manifest
therefore produces defaults, not an error; the same bad value on the command line exits `2`.

## Environment variables

### Proxy (`proxy.py`)

| Variable | CLI flag | Default | Validation / notes | Description |
|---|---|---|---|---|
| `HOST` | `-x`, `--host` | `0.0.0.0` | any bind address | Address all four listeners bind to. |
| `PORT_LIVENESS` | `-l`, `--port-liveness` | `3130` | integer | Plain-TCP liveness probe port. |
| `PORT_READINESS` | `-r`, `--port-readiness` | `4260` | integer | Readiness port; HTTP `200` with `ready` / `503` with `not ready`. |
| `PORT_AGENTS` | `-a`, `--port-agents` | `7000` | integer | Agent control connections. |
| `PORT_CLIENTS` | `-p`, `--port-clients` | `9000` | integer | Client data connections. |
| `ALLOWED_AGENTS` | `-q`, `--allow-agent` | `*` | `*`, or a list of hostnames/IPs | Peers allowed on the agents port. The environment value is a single string matched with `in`; use the flag (space-separated, `nargs='+'`) for more than one host. |
| `ALLOWED_CLIENTS` | `-i`, `--allow-client` | `*` | as above | Peers allowed on the clients port. |
| `WAIT_AGENT_MAX_TRIES` | `-t`, `--wait-agent-max-tries` | `10` | integer | Attempts to pop an agent before the client is closed with `no_agent_available`. |
| `WAIT_AGENT_RETRY_INTERVAL` | `-j`, `--wait-agent-retry-interval` | `0.7` | `> 0` and `<= 300` seconds | Sleep between those attempts. |
| `ZOMBIES_CLEAN_INTERVAL` | `-z`, `--zombies-clean-interval` | `0` in code; `90` in `Dockerfile.proxy` | `>= 0` and `<= 300` seconds; the flag also accepts the word `off` (= `0`) | Background sweep that drops agents whose socket is at EOF. `0` disables the sweep; zombies are then only discarded when a client is assigned an agent. |
| `PING_PONG_INTERVAL_SECONDS` | `-u`, `--ping-pong-interval` | `21` | `> 0` and `< 30` seconds | Ping cadence per pooled agent; the agent answers `pong` within 5 s. |
| `FORWARD_IDLE_TIMEOUT_SECONDS` | `-f`, `--forward-idle-timeout` | `0` | `>= 0` seconds | Idle cutoff on a client↔service data stream. `0` disables it. |
| `READY_TIMEOUT_SECONDS` | `--ready-timeout` | `30` | `> 0` and `<= 300` seconds | How long the proxy waits for an agent's `ready` after `connect`, skipping a stale `pong`. On expiry it logs `bridge_aborted reason=agent_not_ready_timeout`, closes both sides and leaves the rest of the pool untouched; the popped agent has already left the pool and is not re-queued. |
| `POOL_REPORT_INTERVAL_SECONDS` | `--pool-report-interval` | `60` | `>= 0` and `<= 300` seconds; `0` disables | Periodic `pool_size=N` INFO line, in addition to the log on every registration, assignment and discard. |
| `LOG_LEVEL` | `-v`, `--log-level` | `INFO` | any name in `logging.getLevelNamesMapping()`, case-insensitive (`DEBUG`, `WARNING`, …) | Log verbosity; lines carry a UTC timestamp and level. |
| `USE_SSL` | `-s`, `--ssl` | `no` (`false`) | boolean strings, e.g. `yes`/`y`/`true`/`on`/`1` and `no`/`n`/`false`/`off`/`0` (see `parse_bool`) | TLS on the **agents and clients** ports only; liveness and readiness stay plain. |
| `SSL_CERT` | `-c`, `--cert` | unset | path to a PEM certificate | Required when `USE_SSL` is on (`load_cert_chain`). |
| `SSL_KEY` | `-k`, `--key` | unset | path to a PEM private key | Required when `USE_SSL` is on. |

### Agent (`agent.py`)

| Variable | CLI flag | Default | Validation / notes | Description |
|---|---|---|---|---|
| `PROXY_HOST` | `-x`, `--proxy-host` | **required** — no default in code; `Dockerfile.agent` sets `0.0.0.0` | hostname or IP | Proxy address. Missing flag and variable → exit `2`. |
| `PROXY_PORT` | `-p`, `--proxy-port` | `7000` | integer | Proxy agents port, matching the proxy's `PORT_AGENTS`. |
| `HEALTH_PORT` | `-e`, `--health-port` | `4000` | integer | Agent health listener. It always binds `localhost`, whatever the value. |
| `SERVICE_HOST` | `-d`, `--service-host` | `localhost` | hostname or IP | Local service to bridge to. |
| `SERVICE_PORT` | `-b`, `--service-port` | **required** — no default in code; `Dockerfile.agent` sets `3306` | integer | Local service port. Missing flag and variable → exit `2`. |
| `MIN_THREADS` | `-n`, `--min-threads` | `os.cpu_count()` (falls back to `1`); `Dockerfile.agent` sets `3` | integer `> 0` | Control connections opened at start. A replacement is queued whenever a connection ends or is handed to a bridge, so the agent keeps itself at that level. |
| `RETRY_DELAY_SECONDS` | `-r`, `--retry-delay-seconds` | `1.0` | `> 0` and `<= 300` seconds | Base delay before reconnecting after any pre-bridge failure (connect/TLS error, control timeout, EOF, reset, failed handshake). The failed socket is closed first, then the delay elapses, then one replacement is queued. A random jitter of up to 0.5 s is added so a whole tenant's agents do not retry in lockstep. |
| `CONTROL_IDLE_TIMEOUT_SECONDS` | `--control-idle-timeout` | `45` | `> 0` and `<= 300` seconds | Deadline for one complete control frame from the proxy (`ping` or `connect`) while the agent waits in the pool. On expiry the agent logs `state=control_timeout`, closes the control socket and reconnects. Keep it above the proxy's `PING_PONG_INTERVAL_SECONDS`; 45 s is two missed 21 s pings plus margin. |
| `FORWARD_IDLE_TIMEOUT_SECONDS` | `-f`, `--forward-idle-timeout` | `0` | `>= 0` seconds | Idle cutoff on a bridged data stream. `0` never cuts a slow statement. |
| `LOG_LEVEL` | `-v`, `--log-level` | `INFO` | any name in `logging.getLevelNamesMapping()`, case-insensitive | Log verbosity; lines carry a UTC timestamp and level. |
| `USE_SSL` | `-s`, `--ssl` | `no` (`false`) | boolean strings, as on the proxy | TLS to the proxy. |
| `SSL_CERT` | `-c`, `--cert` | unset | path to a **CA** certificate (PEM) | Used as `create_default_context(cafile=...)`. Required when `USE_SSL` is on. The agent presents no client certificate. |

`proxy.py --help` and `agent.py --help` print the same defaults as resolved at startup, so a
deployment can be checked with the binary itself rather than against this table.

## Configuration files

None. Neither process reads a file, and there is no `.env.example` in the repository.

| File | Scope | Precedence | Notes |
|---|---|---|---|
| `Dockerfile.proxy` | proxy image | `ENV` values are ordinary defaults; flags and runtime `-e` override them | Exports `HOST`, all four `PORT_*`, `ALLOWED_*`, `WAIT_AGENT_*`, `PING_PONG_INTERVAL_SECONDS`, `ZOMBIES_CLEAN_INTERVAL=90`, `LOG_LEVEL`, `USE_SSL`. |
| `Dockerfile.agent` | agent image | same | Exports `PROXY_HOST=0.0.0.0`, `PROXY_PORT`, `HEALTH_PORT`, `SERVICE_HOST`, `SERVICE_PORT=3306`, `MIN_THREADS=3`, `RETRY_DELAY_SECONDS`, `LOG_LEVEL`, `USE_SSL`. |
| `docker-compose.yml` | local dev databases only | n/a | Sets MySQL/PostgreSQL/MariaDB/MSSQL credentials and ports; no proxy or agent service. |

Two defaults differ between running from source and running the image — check which one you are
running:

| Setting | From source | In the image |
|---|---|---|
| `ZOMBIES_CLEAN_INTERVAL` (proxy) | `0` (no background sweep) | `90` seconds |
| `MIN_THREADS` (agent) | `os.cpu_count()` | `3` |

Neither `Dockerfile` sets the variables added with the liveness work, so an image runs the
code defaults: proxy `READY_TIMEOUT_SECONDS=30`, `POOL_REPORT_INTERVAL_SECONDS=60`,
`FORWARD_IDLE_TIMEOUT_SECONDS=0`; agent `CONTROL_IDLE_TIMEOUT_SECONDS=45`,
`FORWARD_IDLE_TIMEOUT_SECONDS=0`. Override them with `-e` or the flags.

## Feature flags

There are no feature flags and no remote gating: every behaviour is fixed by the code and the
settings above. The only "off" switches are the numeric `0` values, and they are configuration,
not rollout flags.

| Setting | Default | `0` means | Owner |
|---|---|---|---|
| `FORWARD_IDLE_TIMEOUT_SECONDS` (proxy and agent) | `0` | no idle cut-off — a slow statement is never severed | platform operators |
| `POOL_REPORT_INTERVAL_SECONDS` (proxy) | `60` | no periodic `pool_size` line; changes are still logged | platform operators |
| `ZOMBIES_CLEAN_INTERVAL` (proxy) | `0` in code, `90` in the image | no background sweep; zombies are discarded on assignment only | platform operators |

## Secrets policy

- **Nothing secret lives in this repository.** No `.env` or `.env.example` is committed; the
  passwords in `docker-compose.yml` are throwaway local values for the dev databases.
- **TLS material is referenced by path, never embedded.** `SSL_CERT`/`SSL_KEY` (proxy) and
  `SSL_CERT` (agent CA) name files that the operator mounts into the container or points the
  Windows service at; never copy real certificates or keys into an image, a `Dockerfile` or an
  issue artifact. The proxy's `Dockerfile.proxy` only carries commented placeholders
  (`# ENV SSL_CERT=proxy.crt`, `# ENV SSL_KEY=proxy.key`) for that reason.
- **Real values come from the deployment**, not from git: the Helm values in
  `setwin-bs/argocd-apps` for the proxy and the NSSM service environment on the tenant host for
  the agent. Rotation is done there and takes effect on restart; there is no hot reload — both
  processes read their configuration once at startup.
- **The agents port is unauthenticated**; `ALLOWED_AGENTS` is the only admission control and it
  defaults to `*`. Treat the port as in-cluster only until the deferred authentication work lands.
