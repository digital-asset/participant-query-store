---
paths:
  - "modules/pqs/functest/**"
  - "modules/utils/func-test/**"
  - "modules/**/test/**"
---

# Tests

Guidelines distilled from review feedback and past bugs. Each rule names its source.

- A fix comes with a test that fails without it. If no test can catch the problem yet, that test is
  part of the work ([#61](https://github.com/digital-asset/participant-query-store/pull/61#issuecomment-5567864730), added in #71).
- Extend `SharedMultiSyncLedgerSpec` only for behaviour that needs several synchronizers, and
  `SharedLedgerAndPostgresTest` otherwise ([#125](https://github.com/digital-asset/participant-query-store/pull/125#discussion_r4165985433)).
  `FuncTestDefault` starts no infrastructure; `FuncTestStandalone` gives each test its own.
- Assert exact values, not just presence ([#91](https://github.com/digital-asset/participant-query-store/pull/91#discussion_r4070204196)).
  Use `retryUntilTimeout` only for state still settling when the step returns, e.g. data written by a
  running `Pqs.pipeline`; after `Pqs.runPipeline`, which waits for PQS to exit, assert directly
  ([#125](https://github.com/digital-asset/participant-query-store/pull/125#discussion_r4185349718)).
- A functest helper that checks one id returns `Boolean` (`Ledger.isPackageVetted`), not a response for
  each test to filter ([#125](https://github.com/digital-asset/participant-query-store/pull/125#discussion_r4185376486)).
- Split a spec that keeps growing by feature and share its `shared` layer
  ([#109](https://github.com/digital-asset/participant-query-store/pull/109#discussion_r4091494370), which
  created `ReassignmentPruningSpec`). Delete tests made redundant by stronger ones: SchemaDumpSpec
  replaced most of SchemaSpec ([#83](https://github.com/digital-asset/participant-query-store/pull/83#discussion_r4034883705), #88).
- Version gates (`DamlSdk.onlyDamlLfVersion`, `onlyPostgresVersion`, `onlyCantonVersion`) report a
  non-matching test as ignored, not failed. PR CI runs one fixed combination (`functest.forkEnv` in
  `build.sc`) and the compatibility matrix only runs after merge, so a gated test may never run before
  merge: #93 shipped an upgrade bug that way. Keep at least one ungated test on every critical path
  (#106).
