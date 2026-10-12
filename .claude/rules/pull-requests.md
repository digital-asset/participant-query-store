# Preparing a change for review

- Every user-visible change gets an `UNRELEASED.md` entry in the matching section, grouped under the
  feature's heading when there is one. Operational changes count too, such as a new `ANALYZE` during
  ingestion. Test- or CI-only PRs take the `no-release-note` label instead of an entry.
- Anything that could behave differently on another supported Canton, Postgres or Daml-LF version needs
  a compatibility-matrix run (manual dispatch of `matrix-compatibility-test.yml`), linked in the PR.
  Dropping support for a version is a decision to state in the PR and the docs, not a side effect of
  removing a test.
