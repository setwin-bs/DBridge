<!-- sdlc:start -->
<!-- Managed by the AI-native SDLC framework; keep this block in sync via sdlc_setup,
     do not edit it by hand. -->

## AI-native SDLC

This repository uses the AI-native SDLC. Every feature, bugfix and chore starts from a
GitHub issue and leaves a versioned artifact chain in `.sdlc/issues/<id>-<slug>/`. Design,
build and test keep every attempt as a numbered iteration, so rework never erases why an
earlier attempt failed.

Read first: [`.sdlc/README.md`](.sdlc/README.md) (layout, iterations, configuration) and
[`REVIEW.md`](REVIEW.md) (review passes, severities, verdict rules).

Phase commands: `/sdlc` (status/next), `/sdlc-plan`, `/sdlc-design`, `/sdlc-build`,
`/sdlc-test`, `/sdlc-deploy` (the flow ends at its PR).

Non-negotiables:

1. Issue first — no work without an issue: a GitHub issue when connected, otherwise a local issue (`.sdlc/issues/<YYYY-MM-DD>-<slug>/`); the trigger goes verbatim into `01-plan/origin.md`.
2. No code before the plan of the latest build iteration is approved — with the approver recorded in its `approved_by` / `approved_at`; the design iteration it implements must be approved.
3. No "done" before the latest test iteration's `report.md` is green with literal evidence.
4. Never weaken tests — fix the code, not the check.
5. Docs and ADRs are updated before the PR, per the `docs/README.md` contract.
6. Every deviation from the current plan is recorded in that iteration's `outcome.md` in the same commit.
7. A failed attempt is closed with a `reason` and a `next` phase; the next attempt is a new iteration — never overwrite a closed one.
8. Everything written into the repository or to GitHub is in English — artifacts, commit messages, issues, comments, PRs and release notes — never mirroring the language of the issue or the conversation.

Repo-specific instructions elsewhere in this file win over this block.

<!-- sdlc:end -->
