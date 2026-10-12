# Preparing a change for review

Guidelines distilled from review feedback. Each rule names its source.

- Every change in the diff must be intended and explainable. Reviewers ask "is this intentional?" about
  incidental changes, and they turned out to be bugs, e.g. lost in conflict resolution
  ([#3](https://github.com/digital-asset/participant-query-store/pull/3#discussion_r3869903887),
  [#56](https://github.com/digital-asset/participant-query-store/pull/56#discussion_r3949874865)). When a
  non-obvious change is deliberate, say why in the PR or a comment
  ([#43](https://github.com/digital-asset/participant-query-store/pull/43#discussion_r3861954982)).
- Every user-visible change gets an `UNRELEASED.md` entry ([#6](https://github.com/digital-asset/participant-query-store/pull/6#pullrequestreview-4723937253)),
  in the matching section, grouped under the feature's heading when there is one
  ([#83](https://github.com/digital-asset/participant-query-store/pull/83#discussion_r4025410934)).
  Operational changes count too, such as a new `ANALYZE` during ingestion
  ([#115](https://github.com/digital-asset/participant-query-store/pull/115#discussion_r4131274142)).
  Test- or CI-only PRs take the `no-release-note` label instead of an entry
  ([#71](https://github.com/digital-asset/participant-query-store/pull/71#discussion_r3976107872)).
- Anything that could behave differently on another supported Canton, Postgres or Daml-LF version needs
  a compatibility-matrix run (manual dispatch of `matrix-compatibility-test.yml`), linked in the PR
  ([#136](https://github.com/digital-asset/participant-query-store/pull/136#discussion_r4194015900)).
  Dropping support for a version is a decision to state in the PR and the docs, not a side effect of
  removing a test ([#59](https://github.com/digital-asset/participant-query-store/pull/59#discussion_r3966911150)).
