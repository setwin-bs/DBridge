<!-- Root file. Plain markdown on purpose — no contract frontmatter (GitHub renders it). -->

# Contributing to DBridge

Welcome. This guide is for everyone who changes this repository — humans and agents — and it wires you into the process that gates every change.

## Quick start

Get from zero to a running local instance: [docs/README.md](docs/README.md).

## Workflow

Every change starts from a GitHub issue and follows the SDLC artifact chain in `.sdlc/issues/<id>-<slug>/`:

1. **Plan** — capture the issue verbatim in `01-plan/origin.md`, then grill it into `01-plan/intent.md`.
2. **Design & build** — an approved `02-design/iterations/<NNN>/spec.md` and an approved `03-build/iterations/<NNN>/plan.md` before any code, each recording its approver in `approved_by` / `approved_at`; implement in small, verified slices.
3. **Test & ship** — a green `04-test/iterations/<NNN>/report.md` with literal evidence before the PR, then release and close out after merge.

Design, build and test number every attempt (`001`, `002`, …); a rework adds a new iteration and closes the previous one with a reason, rather than overwriting it.

Details: [`.sdlc/README.md`](.sdlc/README.md).

## Branches and commits

- Branch: `<id>-<slug>`, e.g. `123-fix-login-redirect`.
- Commit messages reference the issue — `#123` — and keep one logical change per commit.

## Tests

<!-- Agent: fill in the actual commands from package.json / Makefile. -->

- Run the repo checks before opening a PR: `<test command>`, `<lint command>`, `<typecheck command>`.
- Never weaken a test to make it pass — fix the code instead. See [REVIEW.md](REVIEW.md).

## Review

Every PR passes the five review passes in [REVIEW.md](REVIEW.md) (correctness, security & privacy, artifact compliance, tests & evidence, documentation impact) and gets a verdict: `changes-requested`, `approved-with-comments` or `approved`. Any `blocker` blocks the merge.

## Docs

Documentation is not optional: every PR updates the docs whose `update_when` triggers fired, per the [docs contract](docs/README.md).

## Questions

Open an issue — that is the single channel where questions, ideas and bugs all start.
