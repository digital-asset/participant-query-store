# Preparing a change for review

- Every user-visible change gets an `UNRELEASED.md` entry in the matching section, grouped under the
  feature's heading when there is one. Operational changes count too, such as a new `ANALYZE` during
  ingestion. Test- or CI-only PRs take the `no-release-note` label instead of an entry.
- PR CI runs the functests on one combination (Postgres 17, Canton protocol 36, LF 2.4, Java 17). The
  compatibility matrix (`modules/compatibility/parameters.csv`) adds Postgres 14, 15, 16 and 18,
  Canton 3.5 and 3.6 (protocols 34 and 35), LF 2.1 to 2.3 and Java 21, but runs only after merge unless
  an engineer dispatches `matrix-compatibility-test.yml` on the branch. Assess whether the change could
  behave differently on one of those versions: SQL or migrations (the 3.4 and 3.5 upgrade cases of
  `FlywayMigrationSpec` run only in the matrix), Ledger API or gRPC usage, Daml-LF decoding (contract
  keys, interfaces, upgrades), JVM-specific code, a dependency or Canton/SDK bump, or a test gated to
  versions PR CI doesn't run. If it could, add a bullet to the PR description naming the versions at
  risk and why, e.g. "Matrix run recommended: the new migration rewrites `__contracts`, which only the
  3.4 and 3.5 upgrade cases exercise", so the engineer can decide to dispatch it and link the run.
  Don't dispatch it unless asked.
- Dropping support for a version is a decision to state in the PR and the docs, not a side effect of
  removing a test.
- A TODO names its tracking issue, in any file: `// TODO #74: …`, `-- TODO #79: …`.
