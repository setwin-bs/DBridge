---
title: Documentation — index & maintenance contract
audience: [everyone, agent]
update_when:
  - a doc under docs/ is added, removed or renamed
  - a doc's audience, update triggers or validated_against globs change
  - conventions.md, the audience registry or the docs fleet changes
validated_against: ["docs/**", "agent.py", "proxy.py"]
verified_against: 31d14ba
screenshots: false
---

# Documentation — index & maintenance contract

`docs/` documents DBridge: the asyncio database proxy (`proxy.py`) and the tenant agent
(`agent.py`) that bridges client connections to tenant services. The repository has no root
`README.md`; this file is the entry point. Every doc starts with contract frontmatter — `title`,
`audience`, `update_when`, `validated_against`, `verified_against`, `screenshots` — so a reader
and an agent can see who it is for and when it goes stale.

Two answers live where the work happens:

- [conventions.md](conventions.md) — the language, the readers, and why this product has no
  themes and no accessibility standard to claim.
- [audiences/README.md](audiences/README.md) — the registry of who reads these docs
  (`developers`, `platform operators`, plus the framework's `agent`).

## Who each doc is for

One layer, because the product has one kind of reader:

- **People who run and build it** — every doc in the tree: architecture, domain language,
  commands, configuration. Technical, verified against the code.
- **People who use it** — tenant teams reaching their database through n8n, an external product
  this repository does not document. No `manuals/`, `tutorials/` or `faqs.md` exists; if a
  tenant-facing guide is ever needed, `docs-audience` registers that audience first.

## How this documentation is built

1. It runs when a change makes a doc wrong, in any phase — not on a schedule and not only before
   a release. The deploy sweep is a safety net over the whole issue, not the moment docs happen.
2. It reads before it asks: [conventions.md](conventions.md) and the audience registry answer most
   questions, and an answer written down is never asked again.
3. No answer, no invention: the doc is marked for review with the exact question, and the rest of
   the run continues.
4. Every path the fleet touched, scaffolding included, goes into the calling phase artifact's
   `docs:` list — the rollback handle for a rejected or superseded attempt.
5. The first run in a repo that predates this contract is a migration run: each area is reviewed
   once as a whole. Later runs touch only what the change reaches.

## Manifest

| Doc | Audience | Update when | Validated against |
|---|---|---|---|
| [README.md](README.md) | everyone, agent | a doc is added, removed or renamed; an audience, trigger or glob here changes; the fleet changes | `docs/**`, `agent.py`, `proxy.py` |
| [audiences/README.md](audiences/README.md) | everyone, agent | an audience is added, renamed or removed; its role or technical level changes | `docs/**` |
| [conventions.md](conventions.md) | everyone, agent | language, readers, theme or accessibility answers change | `AGENTS.md`, `agent.py`, `proxy.py` |
| [architecture.md](architecture.md) | platform operators, developers | a module or dependency appears; the wire protocol, control connection or pool changes; a timeout, default or port changes; the deployment shape or CI publishing changes | `agent.py`, `proxy.py`, `docker-compose.yml`, `Dockerfile.*`, `.github/workflows/**` |
| [concepts.md](concepts.md) | platform operators, developers | a domain term is introduced, or a definition or its boundaries change | `agent.py`, `proxy.py` |
| [commands.md](commands.md) | developers | a command, script, workflow or listening port is added, changed or removed; flags, arguments or defaults change | `proxy.py`, `agent.py`, `.github/workflows/**`, `docker-compose.yml`, `Dockerfile.*` |
| [configuration.md](configuration.md) | platform operators, developers | an environment variable, config file, flag or default changes; a Dockerfile default changes; a validation rule changes | `agent.py`, `proxy.py`, `Dockerfile.proxy`, `Dockerfile.agent`, `docker-compose.yml` |

## Areas not in this tree

- **Deferred to the deploy sweep** — `getting-started.md`, `guides/**`, `roadmap.md`,
  `releases/**`, `infrastructure/**`, and the repository front door (`README.md`,
  `CONTRIBUTING.md`, `SECURITY.md`). No rows above because no files exist; a row for a doc that is
  not there would make the manifest lie.
- **Not created, because the product has no such audience** — `manuals/**`, `tutorials/**`,
  `faqs.md` (see [audiences/README.md](audiences/README.md)).
- **Conditional docs this repo does not need** — `api.md` (the only HTTP surface is the readiness
  probe on `:4260`, documented in [configuration.md](configuration.md); client and agent ports are
  raw TCP), `data-model.md` (the pool is in memory, nothing is persisted, no schema or migration),
  `legal/**` (DBridge has no end-user policy surface), `design/**` and `ui/**` (no interface).

## Docs agents

Areas in use here. The remaining fleet agents start with their area in the deploy sweep.

| Agent | Owns | Validates against |
|---|---|---|
| `docs-index` | `README.md`, `conventions.md` | docs tree, frontmatter, conventions |
| `docs-architecture` | `architecture.md`, `concepts.md` | `agent.py`, `proxy.py` |
| `docs-reference` | `commands.md`, `configuration.md` | CLI flags, environment variables, ports, workflows |

## Images

None, and none required: `screenshots: false` on every doc because there is no interface to
capture. Diagrams, when a doc needs one, are Mermaid code blocks; command and log output are code
blocks. A doc that ever needs an image marks `TODO(screenshot): <what>` and the docs check keeps it
listed until the image exists.

## How to update

1. **Read the issue artifacts** — `.sdlc/issues/<id>-<slug>/`: `01-plan/intent.md`, the latest
   `02-design/iterations/<NNN>/spec.md`, `03-build` `plan.md` + `outcome.md`, `04-test` `report.md`.
   Earlier outcomes explain why an attempt was closed.
2. **Read the conventions and the registry** — they answer the questions this run would otherwise
   ask.
3. **Update the docs whose `update_when` fired**, and correct a defect found on the way (an
   unregistered audience, a moved file, a stale row).
4. **Set `verified_against`** to the commit actually checked (`git rev-parse --short HEAD`) in every
   doc updated or confirmed still correct; `null` means never verified.
5. **Run `sdlc_docs_check`** and close every hit as updated now or `needs-review` — never silently.
6. **Record the paths** in the calling phase artifact's `docs:` list.

## Freshness rules

- `validated_against` — globs whose change can invalidate the doc; keep them narrow and real here.
- `verified_against` — the commit the doc was last verified against; `null` = never verified.
- `audience` — a name registered in `docs/audiences/README.md`, or `everyone`; anything else is a
  defect the check reports.
- `screenshots: true` only when the doc embeds images — no doc here can, so all carry `false`.
- A doc whose `validated_against` paths changed since `verified_against` and was not touched by the
  issue is stale: update it, or record in the report why it is unaffected.

## Glossary

Framework terms only; domain words (agent, bridge, pool, readiness) are defined in
[concepts.md](concepts.md).

| Term | Meaning |
|---|---|
| Contract frontmatter | The YAML block every doc starts with: `title`, `audience`, `update_when`, `validated_against`, `verified_against`, `screenshots`. |
| Audience | A registered reader of these docs; `everyone` means all of them, `agent` is the framework's own reader. |
| Conventions | The answers given once — language, readers, themes, accessibility. |
| Change brief | What changed and which areas it reaches, given to the docs agents by the main agent. |
| Docs fleet | The docs agents writing and maintaining these areas in parallel. |
| `verified_against` | Commit hash of the last verification of a doc. |
| `sdlc_docs_check` | Tool reporting stale docs, unregistered audiences, missing required images and audiences in the wrong layer. |
| Artifact chain | `.sdlc/issues/<id>-<slug>/` — origin → intent → spec → plan → outcome → report → release. |
