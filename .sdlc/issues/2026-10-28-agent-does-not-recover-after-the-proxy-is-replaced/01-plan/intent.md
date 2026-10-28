---
artifact: intent
status: accepted
approved_by: "joaozpalma <joao.palma@setwin.pt>"
approved_at: "2026-10-28T12:01:38Z"
revision: 1
docs: []
date: 2026-10-28
---

# Intent — Agent does not recover after the proxy is replaced

## Problem

On 2026-10-28, tenants `jle-beeshop` and `seteras-beeshop` lost database connectivity after their proxy pods were replaced (10:34:57Z and 10:35:49Z). The proxies were healthy and reachable, but their agent pools stayed empty because the agent processes on the tenant hosts did not re-establish their connections. Every client attempt failed with `no_agent_available` after roughly 7 seconds, and n8n reported `Connection lost - socket hang up`. Both tenants only recovered when a human restarted the agent service via RDP: jle after 49 minutes, seteras after 80 minutes.

The same logs show a second, independent failure: a statement that produced no bytes for 15 seconds was cut by the agent (`duration=15.02s bytes_client_to_agent=576 bytes_agent_to_client=0`), so slow queries fail with the same symptom and no clear cause.

The platform did not signal either failure. The Kubernetes pod stayed `Ready` because the readiness probe only checks that the port answers, and the Windows service stayed `Running` because NSSM only sees that the process exists. A running process and a connected agent were treated as the same thing.

## Proposed outcome

A tenant's database access survives a proxy replacement and any temporary loss of the agent's control connection: the agent reconnects on its own within 60 seconds, without a human touching the tenant host. Slow statements are never severed merely for being slow; any cut-off is an explicit, configurable choice. An operator can see that a tenant has no available agents from the readiness signal and from logs that carry timestamps and pool size.

## Users / personas

- **Tenant end users** (through n8n workflows): they need syncs to complete instead of failing on `socket hang up`.
- **Platform operators**: they need to see a degraded tenant before a customer reports it, and not spend an RDP session restarting a Windows service to recover one.
- **Support / developers**: they need a timestamped record of which agent dropped, when, and why, so the next incident is diagnosable in minutes.

## Business rules

- BR-1: Any loss of an agent's control connection — proxy replacement, network interruption, NAT/conntrack expiry, host sleep — is detected and re-established automatically within 60 seconds, with no human action.
- BR-2: A tenant whose proxy is replaced serves queries again within 60 seconds of the replacement, without any action on the tenant host.
- BR-3: The bridge does not terminate a statement because it is slow. Any timeout is explicit and configurable, and the default is no timeout.
- BR-4: A client connection that cannot be paired with a ready agent within a bounded time is closed with a clear failure and never shrinks the available agent pool permanently.
- BR-5: An empty agent pool is reported as not-ready by the tenant's readiness signal; it is never reported as ready.
- BR-6: Every automatic disconnect and reconnect is logged with a UTC timestamp, the tenant, the agent token and the reason.
- BR-7: Existing tenants keep their endpoints, credentials and client configuration; n8n and other clients need no change.

## General requirements

- GR-1: The agent detects a dead control connection within the 60-second deadline and reconnects, logging the detection and the attempt.
- GR-2: The agent's data path applies no idle cut-off unless a value is explicitly configured; an idle pooled client connection is not closed behind the client's back.
- GR-3: The proxy bounds its wait for a bridged agent to become ready; a stuck bridge releases its client and its agent within a bounded time.
- GR-4: The readiness endpoint reflects the pool (ready with at least one agent, not-ready with zero) and the Kubernetes probe reads that state.
- GR-5: Proxy and agent logs include timestamps and the current pool size at INFO level.
- GR-6: Proxy and agent can be rolled out independently; a mixed pair (old proxy with new agent or the reverse) keeps working during the rollout.

## Constraints

- The proxy runs as a container image deployed through Argo CD; the agent runs as an NSSM-managed Windows service on tenant hosts (`C:\HLINK`) and is upgraded manually, one host at a time. A one-time manual rollout is acceptable; auto-update is not required.
- Recovery target is 60 seconds after any connection loss.
- No change to client (n8n) configuration, database credentials, ports or tenant endpoints.
- Python 3.14 asyncio code base; new runtime dependencies are avoided unless clearly justified.
- The Kubernetes probe and image definitions live outside this repository; the deploy phase must hand over the required manifest change.

## Out of scope

- Authenticating the agents port and rejecting non-agent handshakes (deferred security work; `GET / HTTP/1.1` was accepted as an agent token and an HTTP probe was registered as an agent — filed separately when the security issue is opened).
- Automatic update of the agent binary on the tenant hosts.
- Changes to n8n's connection pool or its retry configuration.
- Project board and time tracking.

## Open questions

- OQ-1: How should the agent detect a dead connection when no FIN/RST arrives — application-level expectation of the proxy's ping, TCP keepalive, or both? (owner: design, blocking: no)
- OQ-2: Should the 60-second deadline be configurable per agent, and what value leaves margin against a 21-second ping interval? (owner: design, blocking: no)
- OQ-3: Which deployment repository carries the readiness-probe change, and is that change part of this issue's deploy step? (owner: platform, blocking: yes for deploy)

## Decisions log

- D-1: Self-healing must complete within 60 seconds and cover any loss of the control connection, not only proxy replacements (human, 2026-10-28).
- D-2: No default query cut-off; timeouts are explicit and configurable (human, 2026-10-28).
- D-3: Visibility is readiness plus timestamped logs with pool size; paging is not part of this issue (human, 2026-10-28).
- D-4: The proxy-side pool leak is in scope, so a hung bridge cannot silently empty the pool (human, 2026-10-28).
- D-5: Agent upgrade is a one-time manual rollout per host; auto-update is out of scope (human, 2026-10-28).
- D-6: Agent-port authentication is not part of this issue; it is deferred to a separate security issue, not filed in this run (human, 2026-10-28).
- D-7: The task is tracked as a local issue because GitHub issues are disabled on `setwin-bs/DBridge`; no project board is used (human, 2026-10-28).

## References

- Origin (verbatim trigger + metadata): [`./origin.md`](./origin.md)
- Incident evidence: proxy logs of `jle-beeshop` and `seteras-beeshop`, 2026-10-28 10:34–11:55 UTC
