# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

@AGENTS.md

## Claude Code specifics

- `.claude/rules/*.md` load automatically: `pull-requests.md` in every session, the others when you open
  a file matching their `paths:`. Don't re-read them unless asked.
- When a PR gets human review comments worth keeping, run `/learn-from-review <PR>` to fold them into
  the rules.
- **Metals MCP** (`.mcp.json`, server `metals`). Prefer it to grepping or guessing: `compile-file` and
  `compile-module` for fast feedback, `glob-search`, `inspect`, `get-docs`, `get-usages` and
  `get-source` for symbols in the code and its dependencies, `find-dep` for library versions. Module
  names are Mill's (`postgres.document`, `pqs.functest`). For up to ~30 s after the session starts it
  may answer "No modules found" while the Mill build imports; retry a little later. If the server isn't
  connected, use the `mill` commands above.

## Metals MCP setup

Metals MCP needs `metals-mcp` on `PATH`: install Coursier (`brew install coursier/formulas/coursier &&
cs setup` on macOS), then `cs install metals-mcp`. Run `make bsp` once, and start Claude Code from the
repo root inside the direnv shell: Metals runs Mill with that shell's `JAVA_HOME`, and Mill 0.12.11
can't run on JDK 25. If the server fails on a cold build, run `mill __.compile` and reconnect with
`/mcp`.
