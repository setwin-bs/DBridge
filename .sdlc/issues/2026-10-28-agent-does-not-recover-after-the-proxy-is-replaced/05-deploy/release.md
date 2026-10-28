---
artifact: release
status: in-review
approved_by: ""
approved_at: ""
spec: "001"
build: "001"
report: "001"
docs:
  - docs/README.md
  - docs/conventions.md
  - docs/audiences/README.md
  - docs/architecture.md
  - docs/concepts.md
  - docs/commands.md
  - docs/configuration.md
date: 2026-10-28
---

# Release — Agent does not recover after the proxy is replaced

## PR

- Link: pending
- Branch: `2026-10-28-agent-does-not-recover-after-the-proxy-is-replaced`
- Merge commit: pending

## Review verdict tally

REVIEW: 2026-10-28-agent-does-not-recover-after-the-proxy-is-replaced @ 31d14ba (working tree)
TALLY: blocker=0 important=0 minor=0 nit=1
VERDICT: approved-with-comments

| Severity | Count | Open | Disposition |
|---|---|---|---|
| blocker | 3 | 0 | Two code blockers (Windows keepalive crash, connection accumulation) and one missing test report, all found in the first verifier pass and fixed; the report is this phase's artifact. |
| important | 1 | 0 | Missing retry delay on the EOF path; fixed and covered by `test_retry_delay_applies_to_eof`. |
| minor | 4 | 0 | Bounded closes, `pool_size` on the zombie sweep, `await_ready` `OSError`, outcome counts; all fixed. |
| nit | 4 | 1 | The remaining nit (a timing cited as 4.7 s instead of 5.429 s) was fixed; the final pass counts one open nit, now closed. |

Four verifier passes were run; the last returned `approved-with-comments` with only the
timing nit, corrected afterwards. The AC-10 harness was proven to fail against both pre-fix
variants (peaks of 460 and 1,183 live connections).

## Iterations

- design 001 · build 001 · test 001 — no rework iterations.
- The verifier findings were fixed inside build 001 before the report existed; no green report
  was invalidated, so no new iteration was opened. The corrections are recorded in
  `03-build/iterations/001/outcome.md`.

## Docs changed

All seven docs were written or corrected during this issue and are stamped
`verified_against: 31d14ba` (the green-report commit; the code has not changed since):

- `docs/README.md` — manifest and maintenance contract (created)
- `docs/conventions.md` — language, audiences, no themes (created)
- `docs/audiences/README.md` — `developers` and `platform operators` (created)
- `docs/architecture.md` — connection lifecycle, timeouts, probes, ADR index (created, then
  corrected to the implemented close→delay→queue order and the three `bridge_aborted` reasons)
- `docs/concepts.md` — glossary including control timeout, replacement, readiness (created,
  then corrected)
- `docs/commands.md` — run, test, probes, 22-test suite and CI test job (created, then updated)
- `docs/configuration.md` — every flag/env/default, including the three new variables (created,
  then updated)

No area returned `needs-review`; `sdlc_docs_check` reports no stale doc.

## ADRs added

- `adrs/0001-agent-connection-liveness.md` — accepted; linked from `docs/architecture.md`.
- `adrs/0000-template.md` — the MADR template.

## Deploy notes

1. Merge this PR. CI builds and publishes `ghcr.io/setwin-bs/DBridge-proxy:latest` and the
   Windows agent exe (the proxy workflow now runs the 22-test suite before building).
2. Restart the four proxy pods (`kubectl -n dbridge rollout restart deploy/<tenant>`) so they
   pull the rebuilt `latest` image. Because the tag is mutable, a merge alone does not roll
   the pods.
3. Upgrade the agent on each tenant Windows host (NSSM `DBRIDGE` service, `C:\HLINK`) one host
   at a time. The wire protocol is unchanged, so the order does not matter; a new agent works
   against an old proxy and vice versa (compatibility tested).
4. Only after the new proxy image is live on a tenant, apply the readiness-probe change in
   `setwin-bs/helm-charts` + `setwin-bs/argocd-apps` (`httpGet` on 4260). Applying `httpGet`
   before the image is live would mark the old plain-text endpoint NotReady and remove the
   tenant's Service endpoints.
5. No database migration, no credential or endpoint change, no n8n configuration change.

## Rollback plan

- Revert the merge commit and rebuild/publish the image, then restart the proxy pods. The wire
  protocol did not change, so a proxy rollback is safe while new agents run and an agent
  rollback is safe while the new proxy runs.
- Revert the helm probe change independently; the new readiness endpoint still accepts a bare
  TCP probe, so the old `tcpSocket` probe keeps working either way.
- No data to restore. Time to effect: one image build plus one pod restart per tenant.

## Post-release checks

- For each tenant: `GET :4260` returns `200 ready` with agents connected, `503 not ready`
  with none; readiness is no longer green with an empty pool.
- Kill one proxy pod and confirm the tenant's agents re-register within 60 s with no action on
  the Windows host (`state=registered` in the pod log), and that a client bridges again.
- Confirm `pool_size=N` appears at INFO every 60 s and on registration/assignment.
- Run a statement that takes longer than 15 s (or a 30 s idle bridge) and confirm it completes
  instead of `socket hang up`.
- Watch for `no_agent_available`, `agent_not_ready_*` and `state=control_timeout` spikes for
  24 h after the rollout; the expected steady state is none.
- Owner on call: platform operators; the security follow-up (agents-port authentication) is
  tracked separately and is not part of this ship.
