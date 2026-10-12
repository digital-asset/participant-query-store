---
paths:
  - "modules/pqs/functest/**"
  - "modules/utils/func-test/**"
  - "modules/**/test/**"
---

# Tests

- Extend `SharedMultiSyncLedgerSpec` only for behaviour that needs several synchronizers, and
  `SharedLedgerAndPostgresTest` otherwise. `FuncTestDefault` starts no infrastructure;
  `FuncTestStandalone` gives each test its own.
- Use `retryUntilTimeout` only for state still settling when the step returns, e.g. data written by a
  running `Pqs.pipeline`; after `Pqs.runPipeline`, which waits for PQS to exit, assert directly.
- In new code, call named methods with dot syntax (`x.is(y)`), not infix as older specs do; symbolic
  operators such as `===` stay infix.
- Version gates (`DamlSdk.onlyDamlLfVersion`, `onlyPostgresVersion`, `onlyCantonVersion`) report a
  non-matching test as ignored, not failed. PR CI runs one fixed combination (`functest.forkEnv` in
  `build.sc`) and the compatibility matrix only runs after merge, so a gated test may never run before
  merge: #93 shipped an upgrade bug that way. Keep at least one ungated test on every critical path.
