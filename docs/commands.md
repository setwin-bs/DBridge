---
title: "Commands"
audience: [developers]
update_when:
  - a command, script, workflow or listening port is added, changed or removed
  - flags, arguments or defaults change
validated_against: ["proxy.py", "agent.py", ".github/workflows/**", "docker-compose.yml", "Dockerfile.*"]
verified_against: 6f1fec7
screenshots: false
---

# Commands


How to run the two processes in this repository — the proxy (`proxy.py`) and the tenant agent
(`agent.py`) — plus the dev stack, the probes and the tests. Every command runs from the
repository root unless stated otherwise; the toolchain is Python 3.14 asyncio, with no
third-party dependency.

Flags and environment variables are listed in full in
[configuration.md](configuration.md).

## CLI & scripts

| Command | Description | Example |
|---|---|---|
| `python proxy.py` | Run the proxy with every default; see the port table below | `python proxy.py` |
| `python proxy.py --help` | Print every proxy flag and its default | `python proxy.py --help` |
| `python agent.py` | Run the agent; `-x/--proxy-host` and `-b/--service-port` are required unless set in the environment | `python agent.py -x proxy.internal -b 3306` |
| `python agent.py --help` | Print every agent flag and its default | `python agent.py --help` |
| `docker build -f Dockerfile.proxy -t dbridge-proxy .` | Build the proxy image (`python:3.14-alpine`, `CMD python proxy.py`) | `docker build -f Dockerfile.proxy -t dbridge-proxy .` |
| `docker build -f Dockerfile.agent -t dbridge-agent .` | Build the agent image (`python:3.14-alpine`, `CMD python agent.py`) | `docker build -f Dockerfile.agent -t dbridge-agent .` |
| `docker compose up -d` | Start the four dev databases defined in `docker-compose.yml` | `docker compose up -d mysql` |
| `docker compose down` | Stop them; `-v` also deletes their volumes | `docker compose down -v` |
| `python -m unittest discover -v` | Discover and run `test*.py` from the repository root | `python -m unittest discover -v` |
| `python -m unittest -v test_proxy` | Run a single test module | `python -m unittest -v test_proxy` |
| `pyinstaller dbridge_agent.spec` | Build the one-file `dbridge_agent` Windows executable from the committed spec | `pyinstaller dbridge_agent.spec` |

## Running the proxy

```bash
$ python proxy.py
```

The proxy binds four ports on `--host` and starts one asyncio server per port. It runs until it
is signalled: `SIGINT` takes the graceful path (pool and servers closed, exit `0`), `SIGTERM` is
not handled and kills the process without running that path.

| Port | Flag / variable | Purpose |
|---|---|---|
| 3130 | `-l/--port-liveness`, `PORT_LIVENESS` | Plain-TCP liveness probe |
| 4260 | `-r/--port-readiness`, `PORT_READINESS` | Readiness probe — HTTP `200` with body `ready` / `503` with body `not ready` |
| 7000 | `-a/--port-agents`, `PORT_AGENTS` | Agent control connections (raw TCP, 16-byte token handshake) |
| 9000 | `-p/--port-clients`, `PORT_CLIENTS` | Client data connections (raw TCP) |

A typical development run that keeps the proxy off the default ports and admits only
`127.0.0.1`:

```bash
$ python proxy.py -x 127.0.0.1 -a 17000 -p 19000 -l 13130 -r 14260 -q 127.0.0.1 -i 127.0.0.1
```

Logging is `logging.basicConfig` at `--log-level` (default `INFO`). Each line is
prefixed with a UTC timestamp and the level, format `%(asctime)s %(levelname)s %(message)s`
with `time.gmtime`, and the pool reports its size on registration, assignment and discard,
plus a periodic `pool_size=N` line every `POOL_REPORT_INTERVAL_SECONDS` (`0` disables the
periodic line):

```text
2026-10-28 10:34:57,120 INFO proxy_started host=0.0.0.0 agents_port=7000 clients_port=9000
2026-10-28 10:35:12,004 INFO agent=3f9c2a7e-... state=registered peer=10.0.0.7:51234
2026-10-28 10:35:12,005 INFO agent=3f9c2a7e-... pool_size=1
2026-10-28 10:36:12,004 INFO pool_size=1
```

The container image runs the same command; its `Dockerfile.proxy` exports the default
environment (`HOST`, the four `PORT_*`, `ALLOWED_*`, `WAIT_AGENT_*`,
`PING_PONG_INTERVAL_SECONDS`, `ZOMBIES_CLEAN_INTERVAL=90`, `LOG_LEVEL`, `USE_SSL`), so
`docker run` needs no arguments to start, only published ports:

```bash
$ docker build -f Dockerfile.proxy -t dbridge-proxy .
$ docker run --rm -p 3130:3130 -p 4260:4260 -p 7000:7000 -p 9000:9000 dbridge-proxy
```

## Running the agent

The agent opens `MIN_THREADS` control connections to the proxy, and opens a connection to the
local service only when the proxy sends `connect`.

```bash
$ python agent.py -x proxy.internal -b 3306
```

With no arguments the two required values come from `PROXY_HOST` and `SERVICE_PORT`; if neither
the flag nor the variable is present, `argparse` exits `2`:

```bash
$ PROXY_HOST=proxy.internal SERVICE_PORT=3306 MIN_THREADS=2 python agent.py
```

The health endpoint always binds `localhost` on `--health-port` (default 4000) — it is not
exposed on the network interface, even though `Dockerfile.agent` declares `EXPOSE 4000`. A check
of the published port from the host therefore fails by design; probe it from inside the
container (or on the host process) instead.

Container:

```bash
$ docker build -f Dockerfile.agent -t dbridge-agent .
$ docker run --rm -e PROXY_HOST=proxy.internal -e SERVICE_PORT=3306 dbridge-agent
```

`Dockerfile.agent` bakes `SERVICE_PORT=3306` and `MIN_THREADS=3` into the image, so pass `-e` to
override them. On tenant hosts the agent ships as a PyInstaller one-file executable built in CI
(`.github/workflows/build-pyinstaller-agent.yml`, `pyinstaller --onefile --console agent.py`,
uploaded as `agent.exe`; the committed `dbridge_agent.spec` builds the same executable named
`dbridge_agent`). PyInstaller is not a declared dependency of this repository — the workflow
installs it, and `pyinstaller dbridge_agent.spec` only works on a machine where it is installed.

## Docker compose dev stack

`docker-compose.yml` defines **only databases** — there is no proxy or agent service in it.
Use it to have a local service for the agent to bridge to:

| Service | Image | Host port | Database / credentials |
|---|---|---|---|
| `mysql` | `mysql:latest` | 3306 | `foobar` / `alan` (throwaway dev values in the file) |
| `postgresql` | `postgres:latest` | 5432 | `foobar` / `alan` |
| `mariadb` | `mariadb:latest` | 3307 → container 3306 | `foobar` / `alan` |
| `mssql` | `mcr.microsoft.com/mssql/server:2022-latest` | 1433 | `sa` / `YourStrong!Passw0rd` (throwaway dev value) |

```bash
$ docker compose up -d mysql          # one service
$ docker compose up -d                # all four
$ docker compose config --services    # mariadb, mssql, mysql, postgresql
$ docker compose down -v              # stop and drop the named volumes
```

A full local loop — proxy on the default ports, agent bridging to the compose MySQL:

```bash
$ docker compose up -d mysql
$ python proxy.py &
$ python agent.py -x 127.0.0.1 -p 7000 -b 3306
```

## Probes

### Liveness — `:3130`, plain TCP

The handler writes one greeting and closes; it says nothing about the pool, so the pod stays
live with zero agents.

```bash
$ nc -w 2 127.0.0.1 3130
🚀 alive and forwarding packets like a champ!
```

### Readiness — `:4260`, HTTP

The port answers a minimal HTTP/1.1 request: `200 OK` with body `ready` when the pool holds at
least one agent, `503 Service Unavailable` with body `not ready` when it is empty. It consumes
the request up to `\r\n\r\n` (bounded at 2 s), so both `curl` and a bare TCP connect get an
answer; the response carries `Content-Type: text/plain; charset=utf-8`,
`Content-Length` and `Connection: close`.

```bash
$ curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:4260/
200
$ curl -s http://127.0.0.1:4260/
ready
```

An empty pool is a `503` with body `not ready`, which removes the tenant from the Service
endpoints — that is the intended signal (see [architecture.md](architecture.md)). The
Kubernetes readiness probe must be `httpGet` on port 4260; the chart lives in
`setwin-bs/helm-charts` (`charts/dbridge/templates/deployment.yaml`) and is configured through
`setwin-bs/argocd-apps`, so the manifest change ships from there, not from this repository.

### Agent health — `:4000`, plain TCP, loopback only

```bash
$ nc -w 2 127.0.0.1 4000
💅 slaying and thriving, PERIOD.
```

## Tests

The suite is `unittest`-based and needs no dependency beyond the standard library. It runs on
Python 3.14 (`python:3.14-alpine` in both images; both test modules use
`IsolatedAsyncioTestCase`). `test_proxy.py` covers pool lifecycle, readiness `200`/`503`, the
stale-`pong` skip, the `ready` timeout and the short handshake; `test_agent.py` covers the
control-frame deadline and fragmentation, reconnect, close-before-replacement ordering, the
jittered retry delay on timeout and EOF, bounded connection growth under repeated failures,
keepalive on a real `StreamWriter`, and the `FORWARD_IDLE_TIMEOUT_SECONDS=0` data path.

```bash
$ python -m unittest discover -v | tail -4
----------------------------------------------------------------------
Ran 22 tests in 5.429s

OK
```

The run is verbose and prints expected `WARNING`/`ERROR` lines from the failure-path tests
(e.g. `agent=... ping_loop_failed closing_connection`, `state=control_timeout`,
`bridge_aborted ... reason=agent_not_ready_timeout`); they are the expected paths, not
failures.

`build-publish-proxy.yml` runs the same command in its `test` job on every push and pull
request, and the image `build` job depends on it (`needs: test`). The agent and PyInstaller
workflows still have no test job.

## Conventions

- **No HTTP API beyond readiness.** The client (`:9000`) and agent (`:7000`) ports are raw TCP
  with a private handshake — 16 bytes of token, then `ping`/`pong`/`connect`/`ready`. The only
  HTTP surface is the readiness port, documented above; there is nothing to authenticate or
  send a JSON body to. See [architecture.md](architecture.md) for the protocol.
- **Signals and exit codes:** `0` after `SIGINT` (the `KeyboardInterrupt` handler closes the
  servers and pool); `2` for an `argparse` usage error — unknown flag,
  missing required `-x`/`-b` on the agent, or a value rejected by a validator; `1` for an
  unhandled exception. `SIGTERM` is not handled (killed with signal status, no shutdown path).
- **Flags beat the environment:** every flag's default is read from its environment variable at
  startup, so a flag on the command line always wins. A malformed *numeric* variable
  is silently replaced by the built-in default; a malformed *flag* exits `2`. See
  [configuration.md](configuration.md).
- **Logging:** `INFO` by default, `-v/--log-level` accepts any name from
  `logging.getLevelNamesMapping()` (`DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`), case
  insensitive. `DEBUG` adds the shutdown and pool bookkeeping lines.
