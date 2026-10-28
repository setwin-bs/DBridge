# Changelog

All notable changes to this project are documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
User-facing release notes per published version live under `docs/releases/` (created by the
release cut).

## [Unreleased]

### Added

- The proxy's readiness endpoint (`:4260`) now returns HTTP `200` when at least one agent is
  available and `503` when the pool is empty, so a tenant with no agents is visible instead of
  silently ready.

### Changed

- The agent now detects a lost control connection within 45 seconds and reconnects on its own,
  even when the proxy disappears without closing the socket. A replaced proxy no longer leaves
  a tenant down until someone restarts the Windows service.
- A statement is no longer cut after 15 seconds of silence: the data path has no default idle
  cutoff on either side, and slow queries complete.
- Proxy and agent logs carry UTC timestamps, levels and the current pool size.

### Fixed

- A bridge that never becomes ready no longer keeps its agent out of the pool permanently.
- Repeated handshake failures no longer multiply the agent's live connections, and a failed
  local-service dial logs the real error instead of a `TypeError`.
- A short handshake closes cleanly instead of raising from the proxy's connection handler.
- The readiness port still accepts a bare TCP probe, so an older `tcpSocket` probe keeps
  working.

(local issue 2026-10-28-agent-does-not-recover-after-the-proxy-is-replaced)
