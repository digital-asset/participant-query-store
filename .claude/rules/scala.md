---
paths:
  - "**/*.scala"
---

# Scala and ZIO

## Failure semantics

- A transaction whose body succeeded must commit or fail: never fall back with `.ignore`,
  `.ignoreLogged` or `.orElseSucceed` on the commit path. That is how #120 lost transactions. Run every
  database transaction through `executeTransaction` (`modules/postgres/backend/.../package.scala`).
- Retries are decided by `isRecoverable` in `modules/pipeline/.../pipeline/Main.scala`; make sure a new
  failure type is not a deterministic error that would be retried forever.

## Configuration and packaging

- A new option must apply on every path that opens a connection: the pool, Flyway/`applySchema`, and
  the CLI subcommands.
- The shipped jar is ProGuard-shrunk, and functests run that jar: a new dependency or reflective access
  may need `proguardKeep`/`proguardIgnore` in `build.sc`.
