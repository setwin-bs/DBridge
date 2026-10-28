---
title: Audiences — who reads these docs
audience: [everyone, agent]
update_when:
  - an audience is added, renamed or removed
  - an audience's role, technical level or docs change
  - the audience contract or the validation rule changes
validated_against: ["docs/**"]
verified_against: 6f1fec7
screenshots: false
---

# Audiences — who reads these docs

Every doc under `docs/` names its readers in the frontmatter field `audience:`. This file is
the only place those names are defined: a name that is not registered here is a defect, not a
new audience.

## Rules

- **`everyone`** is not an audience. It means *every audience registered here*.
- **`agent`** is registered by the framework, not by the project: it reads and maintains these
  docs.
- Technical level is one of `não técnico`, `misto`, `técnico`.

## Registry

| Audience | Role in the product | Technical level | Their docs |
|---|---|---|---|
| `agent` | Reads and maintains the documentation | `técnico` | [README.md](../README.md) |
| `developers` | Builds and maintains DBridge: `proxy.py`, `agent.py`, the container images and CI | `técnico` | [architecture.md](../architecture.md), [concepts.md](../concepts.md), [commands.md](../commands.md), [configuration.md](../configuration.md) |
| `platform operators` | Deploys the proxy, watches tenants in Kubernetes and is on call when a tenant loses database access | `técnico` | [architecture.md](../architecture.md), [concepts.md](../concepts.md), [configuration.md](../configuration.md) |

## Where each audience starts

- `developers` → [commands.md](../commands.md), then [architecture.md](../architecture.md).
- `platform operators` → [configuration.md](../configuration.md), then
  [architecture.md](../architecture.md).

There is no non-technical audience: DBridge is infrastructure, and the people who reach their
database through it use n8n, not this repository. No `manuals/`, `tutorials/` or `faqs.md` is
created. If a tenant-facing guide is ever needed, that audience is registered here first.
