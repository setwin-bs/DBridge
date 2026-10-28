# .sdlc — AI-native SDLC workspace

<!-- Seeded by the framework (sdlc_setup). Safe to edit; keep the layout and phase table in
     sync with framework updates. Full rules: `../AGENTS.md` (SDLC block) and `../REVIEW.md`. -->

Every task in this repository is tracked as an issue — a GitHub issue when connected,
otherwise a local issue — and becomes a versioned artifact chain. Design, build and test
keep every attempt as a numbered **iteration**, so a rework never erases the attempt that
failed: it is closed with a reason and the next attempt is a new number. Nothing is
implemented before its plan is approved; nothing is done before its test report is green.

## Modes (auto-detected)

- **full** — git + GitHub: each started issue gets a branch `<id|date>-<slug>` created by
  `/sdlc-new` and, for a GitHub issue, linked to it with `gh issue develop` (the branch
  shows under the issue's Development section); worktree on request, plus commits, PRs,
  GitHub Releases, roadmap → issues. A **filed-only** issue (capture) lives on GitHub only
  and writes nothing in the repository — no branch, no `.sdlc/` entry, no clock.
- **git-local** — git without GitHub: same branch-per-issue flow; commits and local tags;
  no PR; releases stay local.
- **no-scm** — no git required: no branches/commits/PRs; `.sdlc/active` names the current
  issue and is cleared by `/sdlc-deploy` when the issue ships; the artifacts are the audit
  trail and phases record dates (`verified_against` may be a date instead of a SHA).

`.sdlc/config.json` can force the mode with `"scm": "github" | "git" | "none"`.

## Language

Everything the framework writes is in **English**: the artifacts under `.sdlc/`, the commit
messages, and every issue, comment, PR, release note and CHANGELOG entry. This holds whatever
language the issue, the conversation or `origin.md` is in — never mirror the source. The one
exception is `origin.md`, a verbatim snapshot that keeps the source's language; the docs
language in `docs/conventions.md` governs the product documentation only.

## Layout

```text
.sdlc/
├── .gitignore                        # ignores `active` — never commit the pointer
├── active                            # no-git workspaces only: current issue folder
│                                     #   cleared by /sdlc-deploy when the issue ships (ignored)
├── config.json                       # scm mode, github, commands, guards, docs settings
├── README.md                         # this file
└── issues/
    └── <id>-<slug>/                  # GitHub issue: 123-fix-login-timeout
        │                             # Local issue:  2026-10-07-fix-login-timeout
        ├── 01-plan/
        │   ├── origin.md             # verbatim trigger + metadata footer
        │   └── intent.md             # proto-spec: business + application requirements
        ├── 02-design/
        │   └── iterations/
        │       └── 001/
        │           ├── spec.md       # technical design; respects intent business rules
        │           └── mockups/      # self-contained UI snapshot: reused/ (pinned), new/
        │                             # (candidates), screens/, screenshots/, PROMOTE.md
        ├── 03-build/
        │   └── iterations/
        │       └── 001/
        │           ├── plan.md       # step-by-step; approved before any code
        │           └── outcome.md    # what shipped + evidence + deviations
        ├── 04-test/
        │   └── iterations/
        │       └── 001/
        │           ├── report.md     # matrix, results, regressions, evidence
        │           └── evidence/     # logs, screenshots, videos
        └── 05-deploy/
            └── release.md            # PR (when GitHub), review tally, rollout/rollback
```

Local issues use `<YYYY-MM-DD>-<slug>`; same-day duplicates get `-2`, `-3`. GitHub issues
keep `<id>-<slug>`. The issue folder name **is** the identifier used across artifacts and
commits (`#123` only when a GitHub issue actually exists).

## UI mockups and Storybook

Design produces a self-contained snapshot under `mockups/`: `reused/` pins the existing
components, styles and stories the design depends on, `new/` holds the changed or new
components, pages and styles (the promotion candidates), `screens/` renders each screen/state
at the project's own breakpoints, `screenshots/` is the durable record, and `PROMOTE.md` maps
each piece to its canonical target.

The canonical component library is Storybook at the repo root — `storybook/` for the stories
and `.storybook/` for the config (see `docs/ui/README.md`). Design makes sure every component
it touches has a story: it asks before creating Storybook in a project that has none, and asks
once per session before adding stories for the components that lack them (both opt-in, both a
flagged concern when declined). On approval of the mockups, the Storybook scaffold, the stories
and the components those stories import are promoted into the project so every story renders;
the pages, routes and app wiring promote during build (03), together with
`docs/ui/components/`, and the snapshot is then frozen. The screens are never merged as such.

## Documentation

Documentation is written by the phase that changes it: each phase loads `sdlc-docs` — one
dedicated agent per area, in parallel — when its work changes documented behavior, and lists
the paths it changed in its artifact's `docs:` frontmatter. When the attempt is rejected,
superseded, dropped or rolled back, that same list is reverted with it, so docs never outlive
the change they describe. Deploy only does the final sweep and the `CHANGELOG.md` entry.

## Iterations

02-design, 03-build and 04-test number every attempt: `001`, `002`, … (three digits,
zero-padded so the latest is always the highest). The **latest number is the current
attempt**; filenames inside never change. A phase that is redone adds a new folder and
closes the previous one — nothing is ever deleted, so the reason a rework happened survives
even without git.

- Iteration folders are created by the phase itself when it starts, never pre-seeded.
- Numbers do not have to match across phases: a build records which design iteration it
  implements (`spec`), a report which build it tested (`build`).
- 01-plan and 05-deploy have no iterations: the intent is the business contract (a changed
  business rule is a new issue) and `release.md` is a single record updated in place.

## Artifact metadata

Every artifact starts with a YAML block. Common fields:

| Field | Meaning |
|---|---|
| `artifact` | `intent`, `spec`, `plan`, `outcome`, `report`, `release`, `lessons` |
| `iteration` | `"001"`, `"002"`, … on spec / plan / outcome / report only |
| `status` | the artifact's lifecycle value (table below) |
| `approved_by` / `approved_at` | the human who passed a gate (`Name <email>`, from git config) and when (ISO-8601 UTC); set only on a human-gated status |
| `reason` | one line; **mandatory** for `rejected` / `superseded` / `dropped` / `red` |
| `next` | where the flow goes: `design`, `build`, `test`, `deploy`, `none` |
| `date` | the day the status was last set |
| `docs` | repo-relative docs and ADRs this phase changed (`[]` when none); reverted when the attempt is rejected, superseded, dropped or rolled back |
| `spec` / `build` | on a plan: the design iteration it implements; on a report: the build it tested |
| `supersedes` / `superseded_by` | links between design iterations |

| Artifact | Lifecycle |
|---|---|
| intent | `draft` → `accepted` → `dropped` |
| spec | `draft` → `approved` → `superseded` \| `dropped` |
| plan | `draft` → `approved` \| `rejected` \| `dropped` |
| outcome | `approved` (finished, handed to test) → `rejected` \| `dropped` |
| report | `green` \| `red` |
| release | `pre-PR` → `in-review` → `merged` |

The human gates are `intent` `accepted`, `spec` and `plan` `approved`, and
`release` when the human passes its PR or acceptance gate. The edit that sets them also fills
`approved_by` / `approved_at`; a gate with no approver is not closed. `outcome` `approved` is
technical completion and `report` `green` is evidence — neither is an approval.

Every iteration above `001` also carries a **"Why this iteration"** section that quotes the
reason the previous one was closed. That section plus the closing `reason` is the trace of
what happened and why.

## Phase → artifact → gate

| Command | Phase | Artifact | Gate |
|---|---|---|---|
| `/sdlc-new <n \| title>` | setup | capture: GitHub issue + board · start: + `origin.md`, branch | — |
| `/sdlc-plan` | 01 | `01-plan/intent.md` | intent accepted |
| `/sdlc-design` | 02 | `02-design/iterations/<NNN>/spec.md` + `mockups/` | spec approved |
| `/sdlc-build` | 03 | `03-build/iterations/<NNN>/plan.md` → code → `outcome.md` | plan approved |
| `/sdlc-test` | 04 | `04-test/iterations/<NNN>/report.md` + `evidence/` | all green, no regressions |
| `/sdlc-deploy` | 05 | final docs sweep + CHANGELOG + `release.md` (+ PR when GitHub) | human review / merge |
| `/sdlc-deploy release` | 05 | `docs/releases/v<X.Y.Z>.md` + CHANGELOG (+ tag when git) | release manager |
| `/sdlc` | meta | status / next-step summary | — |

Every human gate records who passed it: `approved_by` is `git config user.name` plus
`git config user.email` as `Name <email>`, confirmed with the human at the gate (ask when git
config gives neither, or when the workspace has no git — never invent one), and `approved_at`
is `date -u +%Y-%m-%dT%H:%M:%SZ`.

The flow ends at `/sdlc-deploy`'s PR. `/sdlc-maintain` (phase 06) is **deprecated**: it stays
installed as a manual post-release toolbox (lessons, incidents) but nothing routes to it and
`06-maintain/` is not seeded.

## Starting an issue

`/sdlc-new` **always asks first, on every run and never by inference** — the issue number says
which issue, not whether to start it — whether the issue is **only being filed** or **also
being started**:

- **capture** — the issue is filed **on GitHub only** and nothing else: the issue (English
  title and body — problem, approach and acceptance criteria as far as the description gives
  them), its board item, at least one label and the milestone answer. **Nothing is written in
  the repository**
  — no `.sdlc/` entry, no `origin.md`, no branch, no intent, no clock, not even `board.json`.
  It is backlog. Start it later with `/sdlc-new <n>`, where the question is asked again:
  choose start.
- **start** — the issue is filed, and `/sdlc-new` writes `.sdlc/issues/<n>-<slug>/` with
  `origin.md` and creates the branch `<id|date>-<slug>` (linked with `gh issue develop` when
  GitHub is connected; worktree on request). `/sdlc-plan` then runs the grill and writes
  `intent.md`.

In both modes the GitHub issue goes on the project board (it asks which one when there are
several) and carries at least one label; `/sdlc-new` asks whether it takes a milestone. The
assignee is optional: it asks whether to assign it to you, to someone else, or to leave it
unassigned, and never guesses a login. There is no capture without GitHub — a local issue
lives in the repository, so filing it is starting it.

- **GitHub**: create the issue (or pick one with `/gh-issues`) and run `/sdlc-new <n>`.
- **Local**: run `/sdlc-new <title>` — it creates `.sdlc/issues/<YYYY-MM-DD>-<slug>/`;
  describe or paste the request and `origin.md` records it verbatim with `origin: local`.
- Then run `/sdlc-plan` and follow the phase commands above. `/sdlc` shows the active issue
  (branch → `.sdlc/active` → most recent), the phase and the missing artifacts; an issue that
  was only filed has no local folder yet, and `/sdlc-new <n>` with start creates it.

## Issue index

<!-- One row per started issue; keep it current (sdlc-status can regenerate it). -->

| Issue | Slug | Phase | Status |
|---|---|---|---|
| 2026-10-28 | agent-does-not-recover-after-the-proxy-is-replaced | 03-build | spec 001 approved |

## Configuration

`.sdlc/config.json` holds:

- `scm` — `auto` (default), or force `github` / `git` / `none`.
- `git.worktree` — `true` creates a sibling worktree per issue for parallel work (default `false`).
- `github.repo` — `owner/name`, selected by `/gh-connect` (null in local mode).
- `commands` — `test`, `lint`, `build`, `app` used by the build/test/deploy phases.
- `guard.protectedPaths` / `guard.testGlobs` — enforced by the guard plugin.
- `docs` — which docs agents run (`all` or a list), disabled areas, prompt context on/off.

Secrets never live here — only names and non-sensitive settings.
