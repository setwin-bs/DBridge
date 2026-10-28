# 0001. Detect a dead agent control connection with an application deadline

## Status

accepted

## Date

2026-10-28

## Context

On 2026-10-28 the `jle-beeshop` and `seteras-beeshop` proxies were replaced. The proxies were
healthy, but their agent pools stayed empty for 49 and 80 minutes: the agent processes on the
tenant hosts kept a half-open control connection and never noticed the proxy was gone. The
Windows service reported `Running` the whole time. Recovery required a human restarting the
service.

The control connection is idle except for the proxy's `ping` every 21 seconds; the agent only
replies `pong`. A FIN or RST can be lost when a pod's network namespace disappears or when
connection tracking still points at an address that no longer exists. TCP keepalive is not
enabled by default and Windows keeps an idle connection for two hours before probing. So
neither the OS nor the protocol gave the agent a reason to wake up.

The same incident exposed a second cut: `FORWARD_IDLE_TIMEOUT_SECONDS` defaulted to 15 on the
agent, so any statement that produced no bytes for 15 seconds was severed — indistinguishable
from a crash to the client.

## Decision

We will give the agent its own liveness timer and remove the implicit data cutoff:

- the agent reads one complete control frame (`ping` or `connect`) with
  `CONTROL_IDLE_TIMEOUT_SECONDS` (45 s by default, two missed 21 s pings plus margin). On
  expiry it closes the connection and reconnects through the normal replacement path;
- `SO_KEEPALIVE` is enabled as a kernel-level backstop, tuned where the platform allows it
  (Windows `SIO_KEEPALIVE_VALS`, Linux `TCP_KEEPIDLE`/`TCP_KEEPINTVL`/`TCP_KEEPCNT`);
- a connection that fails before the bridge is closed first, then waits
  `RETRY_DELAY_SECONDS` plus up to 0.5 s of jitter, and only then queues exactly one
  replacement;
- `FORWARD_IDLE_TIMEOUT_SECONDS` defaults to 0 on both sides; any cutoff is explicit;
- the proxy bounds its wait for `ready` at `READY_TIMEOUT_SECONDS` (30 s).

This decides liveness and pacing for the agent–proxy control connection. It does not decide
authentication of the agents port, which remains open and is tracked as separate work.

## Consequences

Positive: a tenant recovers from a proxy replacement or a silently dropped connection within
about 46 seconds, with no human action. Slow statements complete. Logs now say which agent
lost which connection and why. Failed connections cannot accumulate.

Negative: detection can take up to 45 seconds, so failover is not instant. Idle client
connections now live until the client or the database closes them, which increases the number
of open sockets on the agent host. A hung database connection no longer benefits from the old
15-second cutoff and must rely on the database's or the client's own timeouts.

Follow-up: the exact deadline is configurable per agent; operators must keep it above the
proxy's `PING_PONG_INTERVAL_SECONDS`. Authentication of the agents port is still outstanding.

## Alternatives considered

- **Rely on FIN/RST only.** Rejected: the incident is precisely a case where the close never
  reached the agent, and a pod replacement can lose it.
- **Rely on OS TCP keepalive only.** Rejected: it is off by default, Windows probes only after
  two idle hours, and tuning it is platform-specific. It stays as a backstop, not the
  guarantee.
- **Shorten the proxy ping interval.** Rejected: it adds traffic on every idle connection and
  still gives the agent nothing to do when the proxy itself is gone.
- **Keep a short data cutoff and make it configurable.** Rejected for the default: a database
  statement that is slow is not a failed one, and cutting it presents as a socket hang-up with
  no useful error.
