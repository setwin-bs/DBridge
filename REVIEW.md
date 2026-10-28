# REVIEW.md — Review & verification instructions

> Applies to every change in this repository, human- or agent-authored.
> The reviewer runs with fresh context and did not write the change.
> Versioned policy: the tech lead / policy owner approves edits to this file.
> Used by: `/sdlc-deploy` (pre-PR passes), the `sdlc-verifier` agent, and any
> reviewer tagged on a PR.

## 0. Ground truth — read before reviewing

Review the diff against the issue artifacts, not against personal taste. Design, build and
test keep every attempt as a numbered iteration: **read the latest iteration (the highest
`NNN`) of each phase**, and read the earlier ones when the current artifact references them —
they are the record of why a rework happened.

| Artifact | What it gives you |
|---|---|
| `.sdlc/issues/<id>/01-plan/intent.md` | business rules, users, what success means |
| `.sdlc/issues/<id>/02-design/iterations/<latest>/spec.md` (+ `mockups/`) | technical spec, acceptance criteria, approved UI |
| `.sdlc/issues/<id>/03-build/iterations/<latest>/plan.md` + `outcome.md` | planned approach and explained deviations |
| `.sdlc/issues/<id>/04-test/iterations/<latest>/report.md` (+ `evidence/`) | test matrix, results, regressions |
| `docs/README.md` | documentation contract (what must be updated) |
| `adrs/` | architectural decisions that still hold |

Each artifact starts with a YAML metadata block (`status`, `reason`, `next`, links to the
iteration it implements or tested). Trust the latest iteration's `status`; a `superseded`
spec or a `rejected` outcome with an empty `reason` is itself a finding.

If a required artifact is missing, that is a finding (usually `blocker`) before
anything else.

## 1. Review passes

Run every pass. Tag each finding with its pass and severity.
One finding = one defect, one location, one fix.

### Pass 1 — Correctness & bugs
- Logic errors, off-by-one, boundaries (empty, null, zero, huge, unicode).
- Error paths, retries, timeouts, partial failure, ordering, idempotency.
- State: cache invalidation, stale reads, migrations, rollback.
- Every finding needs a concrete failure scenario (inputs → wrong behavior).

### Pass 2 — Security & privacy
- Authentication/authorization on every new path; object-level access checks.
- Injection (SQL/command/template), path traversal, SSRF, unsafe
  deserialization, XSS.
- Secrets: never in code, logs or diffs; environment handling correct.
- PII: not in logs, error messages, analytics or test fixtures.
- New dependencies: justified, maintained, pinned; flag supply-chain risk.

### Pass 3 — Compliance with the artifacts
- The diff implements the plan of the latest build iteration; every deviation
  appears in that iteration's `outcome.md` with a reason.
- Every acceptance criterion in the latest spec iteration is satisfied (trace it
  explicitly). If the latest spec supersedes an earlier one, review against the
  latest, and check that the superseded one names the reason.
- Business rules in `intent.md` are respected; contradictions are escalated,
  not silently resolved.
- Existing ADRs are respected (or a new ADR is proposed).
- UI matches the approved mockups of the latest design iteration (their `screenshots/` are
  the evidence), the storybook scaffold, stories and their components were promoted at the
  design gate, and every remaining `mockups/new/` piece was promoted per its `PROMOTE.md` into
  the app source and `docs/ui/components/`; design guidelines and code conventions are
  followed.

### Pass 4 — Tests & evidence (verification)
- The latest test iteration's `report.md` exists and is green, and it tested the
  latest build iteration (its `build` field).
- Traceability: every acceptance criterion maps to at least one executed case.
- Coverage of: happy path, boundaries, invalid input, permissions,
  integrations, and UI flows for user-visible behavior.
- Regression: neighboring flows exercised (or the omission explicitly
  justified).
- Evidence is literal: exact commands + observed output; screenshots/video
  for UI — not the words “tests pass”.
- Test integrity: no skipped/deleted tests, no removed assertions, no lowered
  thresholds or coverage, no blindly updated snapshots. A fix changes the
  code, not the test; any exception requires written justification here.
- Missing or weak evidence is a `blocker`, not a nit.

### Pass 5 — Documentation impact
- Docs updated per the `docs/README.md` contract (README, architecture,
  concepts, commands, manuals, guides, tutorials, faqs, roadmap, design, ui,
  infrastructure — as applicable).
- New architectural decision → ADR in `adrs/`.
- New idea or deferred work → `docs/roadmap.md` (never silently dropped).
- Every doc a phase changed is listed in that artifact's `docs:` frontmatter; a doc changed
  without a recorded entry, or left behind after a rejected/superseded/dropped attempt or a
  rollback, is a finding.

## 2. Severity

| Severity | Meaning | Action |
|---|---|---|
| `blocker` | Wrong behavior, security/data loss, broken test integrity, missing evidence. | Fix before merge. |
| `important` | Real defect or policy breach, limited blast radius. | Fix or record written justification. |
| `minor` | Worth fixing, not blocking. | Author decides. |
| `nit` | Polish. Report at most 5; summarize the rest as a count. | Optional. |

If you cannot point to a concrete failure scenario, or to the artifact it
violates, it is not `blocker`/`important`.

## 3. What not to report

- Generated code, lockfiles, vendored or minified files.
- Anything the formatter, linter, typechecker or CI already enforces.
- Style and naming preferences not encoded in team conventions.
- Speculative refactors or “while you're here” scope expansion.
- Missing unit tests for behavior that cannot be tested at that level when an
  integration/e2e test covers it (check first).

## 4. Output format

```
REVIEW: <branch> @ <commit> (issue .sdlc/issues/<id>)
FINDINGS:
[blocker]   correctness — path/file.ext:LINE — failure scenario; suggested fix
[important] security    — path/file.ext:LINE — scenario; suggested fix
[minor]     compliance  — ...
[nit]       ...
TALLY: blocker=0 important=2 minor=3 nit=4
VERDICT: changes-requested | approved-with-comments | approved
```

Verdict rules: any blocker → `changes-requested`. `approved` requires zero
blockers and zero unacknowledged importants.

## 5. Fix loop (author side)

- Address findings in code; rerun the full check suite.
- Code changes after a closed attempt are a **new iteration**, never a silent edit:
  a new build iteration (and its test iteration) when the code deviates from the plan,
  or a new test iteration alone when only evidence must be refreshed. Close the
  previous attempt with its status and reason; never overwrite a green report.
- Within that new iteration, if the implementation deviates from its plan, update the plan
  in the same commit and record the deviation in the iteration's `outcome.md`.
- Never weaken a test to clear a finding (fix mode blocks test edits).
- Re-request review; the reviewer re-checks the fixes and the tally only.

## 6. Reviewer conduct

- Report, don't rewrite (unless explicitly asked to fix).
- Prefer questions over assumptions when the artifacts are ambiguous — but
  only after reading section 0 fully.
- One finding per issue; no duplicates across passes; highest severity wins.
- Keep findings factual: what breaks, where, under which conditions.
