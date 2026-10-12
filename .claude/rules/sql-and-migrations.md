---
paths:
  - "modules/postgres/**"
  - "**/*.sql"
---

# SQL, migrations and query performance

PQS exists to make SQL over large ledgers fast, so every SQL change is a performance change: check the
plan, measure on realistic volume, and announce the cost. Each rule names the migration, PR or review
comment it comes from.

## Flyway migrations (`modules/postgres/document/resources/db/migration/`)

- `V###__*.sql` (three digits: `build.sc` derives the schema version from the file name) holds DDL and
  data fixes only. Functions and procedures live in `R__functions.sql`; views, triggers and trigger
  functions in `R__views_and_triggers.sql`. The migration README's `R__01_…`/`R__50_…` names are stale.
- Don't edit a `V` file once it is merged: snapshots are published from every merge and Flyway validates
  checksums (the legacy-header list in `.scaffold/libs/python/copyright-headers.json` exists for the same
  reason). Merged-but-unreleased V045 was edited in #93, #115 and #117; treat that as an exception and
  say so in the PR.
- `V` files run before any `R__` file, i.e. against the previous release's functions, views and
  triggers. Drop dependents before what they depend on, don't call from a `V` file a function whose
  definition changes in the same release, and keep every name old code reads, such as the
  `scribe.instance` setting. #93 broke upgrades from 3.6 by dropping `__insert_archive_fn()` while
  3.6's trigger still used it; #52 renamed the setting and broke V040.
- `create or replace` can't change a function's result columns: drop it in a `V` file and let `R__`
  recreate it (V047). To delete an object, remove it from `R__` and add the `drop` in a `V` file.
- PL/pgSQL bodies are not checked against the schema at `create` time: when dropping or renaming a
  column, grep every `R__` body (V011 kept inserting into the dropped `witnesses` column; fixed in V012).
- The `__initialize_*` routines re-run on every start: keep them idempotent and cheap (V002, V019).
- Support Postgres 14–18 (the compatibility matrix): no version-specific DDL, and an enum value added in
  a migration can't be used in the same transaction (V043).
- Never write `public.` or any schema qualifier: PQS runs in the schema chosen by
  `--target-postgres-schema` (V024; catalog queries filter on `current_schema()`). Don't copy DDL from
  `schema-dump.sql`, which pg_dump qualifies with `public.`.
- Data migrations carry redundant `where` guards derived from known invariants, so they provably can't
  touch the wrong rows, with a comment saying why (V042). Unsupported data fails with an actionable
  `raise exception` (V019).
- Each new `V` file gets a line in the "SQL Migration" section of `UNRELEASED.md` with an
  **[Impact: …]** estimate: "Metadata-only … Instantaneous", or time per N rows, naming any full-table
  rewrite or index rebuild ([#30](https://github.com/digital-asset/participant-query-store/pull/30#discussion_r3831062445);
  V045: "~2 min/10M contracts"). Prefer metadata-only DDL: constant-default `add column`, `add value`, `rename`.
- After any migration change run `make generate-schema-dump` and review the `schema-dump.sql` diff as
  the real schema change. SchemaDumpSpec runs on Postgres 17 only and covers fresh installs only (#64).
- Upgrade bugs only show up when migrating a released schema. `FlywayMigrationSpec` migrates datastores
  created by released PQS images; its 3.4 and 3.5 cases are LF-gated and never run in PR CI, the 3.6
  case does (#93, #106). Images up to 3.5 can't decode LF 2.3+ (contract keys), so recreate such state
  with SQL instead (the V042 case).

## Schema and query performance

- `__contracts` and `__exercises` are LIST-partitioned by `tpe_pk` (one partition per template,
  interface or choice), `__reassignments` by `contract_tpe_pk`; there is no default partition. Give
  queries a partition key the planner knows at plan time: that is why the `__contract_tpe` lookups are
  deliberately `immutable` (V027). Join `__contracts` on `(contract_id, tpe_pk)`, never `contract_id`
  alone (V017).
- A new per-template partitioned table needs a `V` backfill of partitions for the existing
  `__contract_tpe` rows and a change to `__initialize_contract_tpe` (V046).
- Declare indexes on the partitioned parent (V032). Point-in-time reads use the GiST index on the
  generated range `life_ix`, which is partial on `not divulged_only`: restate that predicate. Every new
  access path needs an index ([#109](https://github.com/digital-asset/participant-query-store/pull/109#discussion_r4091370490)),
  and write the index method explicitly, e.g. `using btree` ([#115](https://github.com/digital-asset/participant-query-store/pull/115#discussion_r4130295069)).
- Evaluate per-query values once by wrapping them in a scalar subquery:
  `life_ix @> (select __nearest_ix_floor(...))` made `active` 10x faster at 1M contracts (#36).
- Reuse helpers such as `__activated_at_ix(c)` and `__deactivated_at_ix(c)` instead of re-implementing
  them ([#115](https://github.com/digital-asset/participant-query-store/pull/115#discussion_r4130277962)).
  They inline to `coalesce(...)`, which can't use an index on the underlying column, so check
  `explain` wherever the predicate should be an index condition.
- Benchmark read functions and the ingestion path against the previous release on realistic volume,
  and put the numbers in the PR: `distinct on` cost `active` 70%
  ([#117](https://github.com/digital-asset/participant-query-store/pull/117#discussion_r4141291936)), and
  multi-sync ingestion ran 3x slower until planner statistics were refreshed (#115).
- No foreign keys between fact tables and no cascading deletes; prune, reset and cleanup delete
  explicitly (V020, V023). A new table holding an `ix` or event pk must be added to
  `__delete_transactions_after`/`_before`, `prune_archived_to_offset` (and `_dry_run`),
  `prune_to_offset` and `reset_to_offset` (#109, #111).
- Bound every fact-table read by the watermark (V034): batches commit out of order, so rows above it
  exist.
- `create index concurrently` can't run inside a function or transaction; for operator-run index
  builds, emit the DDL instead (`print_create_index_for_contract`, #61).

## Semantics that have caused bugs

- Since multi-sync (V045) a contract can have several `__contracts` rows, and "deactivated" means
  archived or unassigned. Audit every use of `archived_*` columns and every query that assumes one row
  per contract (#116, #117, [#89](https://github.com/digital-asset/participant-query-store/pull/89#discussion_r4061396257)).
- `reassignment_counter` is NULL on rows written before 3.7, and Postgres sorts NULLs first in `desc`
  order: write `desc nulls last` ([#117](https://github.com/digital-asset/participant-query-store/pull/117#discussion_r4133683905)).
- An `update … from` must match at most one source row per target row; otherwise pick one with
  `lateral (… limit 1)` (#115).
- A `strict` function called with a NULL argument returns no row at all, e.g. the prune functions when
  `nearest_offset(...)` is NULL (#50, handled in `Prune.scala`).
- The SQL API (the `contract` type, read functions, prune results) is public: append new columns at
  the end, never insert or reorder ([#109](https://github.com/digital-asset/participant-query-store/pull/109#discussion_r4091385715)),
  and mark breaking changes `*BREAKING*` in `UNRELEASED.md`.
- PQS is a single writer, fenced by `__watermark.instance_id` matching the session setting
  `scribe.instance` (V005; rotated by `beforeMigrate.sql`, startup cleanup and `reset_to_offset`). The
  watermark trigger updates `__contracts` rows by `ctid`, which is only safe while nothing else updates
  them. The deprecated `prune_to_offset` does (`__delete_transactions_before`), so it must not run
  during ingestion.
