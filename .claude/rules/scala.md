---
paths:
  - "**/*.scala"
---

# Scala and ZIO

Guidelines distilled from review feedback and past bugs. Each rule names its source.

## Types and APIs

- Model closed sets as Scala 3 `enum`s, including parameterised cases, rather than sealed-trait
  hierarchies with nested objects ([#87](https://github.com/digital-asset/participant-query-store/pull/87#discussion_r4044692303)).
  Add a case to an existing ADT before creating a parallel hierarchy ([#75](https://github.com/digital-asset/participant-query-store/pull/75#discussion_r4013634815)).
- Give public members an explicit return type ([#72](https://github.com/digital-asset/participant-query-store/pull/72#discussion_r3989303043)).
- Wrap side effects such as `UUID.randomUUID()` in ZIO (`Random.nextUUID`) ([#59](https://github.com/digital-asset/participant-query-store/pull/59#discussion_r3966855170)).
- A type that holds state or identity is a `class`, not a `case class`, which reads as plain data
  ([#62](https://github.com/digital-asset/participant-query-store/pull/62#discussion_r3950287080)).

## Failure semantics

- A transaction whose body succeeded must commit or fail: never fall back with `.ignore`,
  `.ignoreLogged` or `.orElseSucceed` on the commit path. #120 lost transactions that way
  ([review](https://github.com/digital-asset/participant-query-store/pull/120#discussion_r4157178392)).
  Run every database transaction through `executeTransaction`
  (`modules/postgres/backend/.../package.scala`).
- When you change a pattern, change every call site in the same PR
  ([#120](https://github.com/digital-asset/participant-query-store/pull/120#issuecomment-5929612430),
  [#49](https://github.com/digital-asset/participant-query-store/pull/49#discussion_r3904214754)).
- Retries are decided by `isRecoverable` in `modules/pipeline/.../pipeline/Main.scala`; make sure a new
  failure type is not a deterministic error that would be retried forever ([#72](https://github.com/digital-asset/participant-query-store/pull/72#discussion_r4004268003)).
- Log ERROR only for unexpected or programming errors; expected operational failures are WARN
  ([#120](https://github.com/digital-asset/participant-query-store/pull/120#discussion_r4157016820)).

## Configuration

- A new option must apply on every path that opens a connection: the pool, Flyway/`applySchema`, and
  the CLI subcommands ([#73](https://github.com/digital-asset/participant-query-store/pull/73#discussion_r4007672573)).
- Any change to an option, its default or its description changes the CLI help snapshots:
  `modules/pqs/test/.../NavigationHelpSpec.scala` and `modules/pqs/functest/.../console/HelpSpec.scala`.
- Don't add vendor-specific integrations (e.g. a cloud provider's auth plugin) as dependencies: they
  ship in every build and image. Prefer a generic extension point, such as user-provided pgjdbc
  properties (#73) plus the user's own runtime classpath ([#70](https://github.com/digital-asset/participant-query-store/pull/70#pullrequestreview-5164236166), closed unmerged).
- The shipped jar is ProGuard-shrunk, and functests run that jar: a new dependency or reflective access
  may need `proguardKeep`/`proguardIgnore` in `build.sc`.

## Readability

- Name things specifically (`Dars`, not `Utils`) ([#38](https://github.com/digital-asset/participant-query-store/pull/38#discussion_r3860317405))
  and consistently with their siblings (`reassign_event_pk` next to `assign_event_pk`)
  ([#91](https://github.com/digital-asset/participant-query-store/pull/91#discussion_r4070053591)).
- Delete dead code and leftovers in the same PR ([#72](https://github.com/digital-asset/participant-query-store/pull/72#discussion_r3989333827),
  [#106](https://github.com/digital-asset/participant-query-store/pull/106#discussion_r4081053478)).
- Comments explain why something is the way it is ([#83](https://github.com/digital-asset/participant-query-store/pull/83#discussion_r4035900315)). A TODO
  names its tracking issue: `// TODO #74: …` ([#75](https://github.com/digital-asset/participant-query-store/pull/75#discussion_r4013817671)).
- In new code, call named methods with dot syntax (`x.is(y)`), not infix as older specs do; symbolic
  operators such as `===` stay infix ([#125](https://github.com/digital-asset/participant-query-store/pull/125#discussion_r4185349718)).
- Put logging and timing for a step inside that step's existing trace span
  ([#115](https://github.com/digital-asset/participant-query-store/pull/115#discussion_r4131259903)).
