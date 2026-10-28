---
title: How this repository documents
audience: [everyone, agent]
update_when:
  - the documentation language changes
  - the readers of these docs change
  - a visual theme or an accessibility standard enters the product
  - a question comes back a second time
validated_against: ["AGENTS.md", "agent.py", "proxy.py"]
verified_against: 31d14ba
screenshots: false
---

# How this repository documents

The answers the docs fleet does not ask twice. Everything else belongs to the doc that uses it.

- **Language** — English, for docs, issue artifacts, commits and code comments. Identifiers,
  commands, paths and protocol names stay English; domain terms are defined once in
  [concepts.md](concepts.md) and spelled the same way everywhere.
- **Readers** — `platform operators` (SRE on-call in `setwin-bs`, who deploy and watch the proxy)
  and `developers` (who maintain `agent.py`, `proxy.py` and the images), both technical. Both are
  registered in [audiences/README.md](audiences/README.md); there is no non-technical audience and
  no plain-language layer.
- **Visual themes** — none. DBridge has no user interface: the two processes print plain-text
  log lines and the readiness port answers HTTP with a plain-text body. No palette, no theme
  order, no screenshots anywhere; every doc carries `screenshots: false`.
- **Accessibility** — no standard is claimed, because there is no interface to conform to. The
  docs are plain Markdown, no images and no client-side script; diagrams, if any, are Mermaid
  code blocks.
