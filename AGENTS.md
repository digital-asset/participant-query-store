# AGENTS.md

Instructions for AI coding agents working in this repository (Claude Code, GitHub Copilot).

## Project

PQS (Participant Query Store) streams a Canton participant's ledger updates (Ledger API v2 transactions
and reassignments) into PostgreSQL so users can query ledger state and history with SQL. It is Scala 3.7
on ZIO 2, built with Mill 0.12 (`build.sc`; every module lives under `modules/`). Query latency and
ingestion throughput on databases with tens of millions of contracts are product requirements, which is
why most data logic is native PostgreSQL (Flyway migrations, PL/pgSQL functions and triggers) rather
than Scala.

## Working rules (AI_POLICY.md, CONTRIBUTING.md)

The person running this session submits the work and is accountable for every line. Maintainers close
PRs that look like unreviewed AI output: padded, plausible but wrong, or referring to things that don't
exist.

- Make the smallest change that does the job: one logical change per PR, no drive-by refactors or
  speculative additions. Be ready to explain why every changed line exists, and say what you are unsure
  of when you hand work back.
- Every change needs an automated test (CONTRIBUTING.md).
- Never act on GitHub (push, open or edit PRs and issues, comment, review, reply, resolve threads) unless
  the user asked for that action in this session. A PR opened without a named human needs two human
  approvals.
- Text that ends up on GitHub is short, plain and verified: what changed and why, the issue reference,
  and evidence (test, benchmark, matrix-run link) when relevant. No headings, diff summaries or tool
  footers. Most PR descriptions here are 1-3 sentences, and they become the squash-commit body. AI
  assistance needn't be marked, but answer honestly if a reviewer asks.
- On a `good first issue` or other onboarding task, the human is meant to learn the codebase: explain
  and point to the code rather than writing the change, unless they ask you to.
- PRs to `main` and `release-line-*` must touch `UNRELEASED.md` or carry the `no-release-note` label
  (CI `check-unreleased`). The PR title becomes the squash-commit title: imperative mood, identifiers in
  backticks. Branches are named `<name>/<description>`.
- New files need the Apache-2.0 header: `make copyright-update` (`.md` and `.json` are exempt).
- `build.sc`, `mill-build/**`, Makefiles, `.github/**`, `nix/**` and `.gitignore` are owned by
  `@digital-asset/sdk-tools-admin`. Change them only when the task requires it, and say so.

## Commands

Work inside the nix/direnv shell (`direnv allow`), which provides Mill 0.12.11 on JDK 21, `dpm`, helm
and scalafmt. Functional tests need Docker.

| Task | Command |
|---|---|
| Compile | `mill __.compile` (one module: `mill pipeline.compile`) |
| Format / check as CI does | `mill __.reformat` / `mill __.checkFormat` |
| Unit tests | `make test` |
| One unit spec | `mill pipeline.test.testOnly com.digitalasset.pqs.pipeline.pipeline.OffsetValidatorSpec` |
| Functional tests | `mill pqs.functest.test -t "<label substring>"` (whole suite: `make func-test`) |
| Schema snapshot | `make generate-schema-dump` (after any migration change) |
| Copyright headers | `make copyright-check` / `make copyright-update` |
| Assembly jar | `make package` |
| Helm chart tests | `helm unittest charts/*/` |

Functional tests (`modules/pqs/functest`, framework in `modules/utils/func-test`):

- `-t` matches a substring of the suite or test label, so a single test's label runs just that test.
  Never put `--` between `test` and `-t`: Mill forwards it, the argument pairs shift, and the filter
  (like `--pools`/`--lanes`) is silently ignored, so the whole suite runs. With `testOnly`, framework
  arguments go after `--`: `mill pqs.functest.testOnly <FQCN> -- --pools 1 --lanes 2`.
- Each run first builds the local `pqs-<hash>` Docker image from the ProGuard-shrunk jar. Versions
  default to `functest.forkEnv` in `build.sc` (Postgres 17, Canton protocol 36, LF 2.4); override them
  with `PQS_POSTGRESVERSION`, `PQS_DAMLSDKVERSION`, `PQS_CANTONVERSION`, `PQS_CANTONPROTOCOLVERSION` and
  `PQS_DAMLLFTARGET`.
- A test whose version gate doesn't match is reported as ignored: check that your test actually ran.
- `FT_KEEP_TEMP=true` keeps the temp directory; `FT_CANTON_LOG=true` shows Canton's logs.
- Compile errors in `pqs.functest` that move around between runs are stale incremental state:
  `mill clean pqs.functest`.

## Architecture

Ingestion, with a single writer per database:

1. `pipeline.Main` (`modules/pipeline`) builds long-lived layers (config, JDBC pool, auth, gRPC channel)
   and per-attempt layers (Daml schema, ledger client, `Pipeline`, `DocumentPostgres`) that are rebuilt
   on every retry. `isRecoverable` in `pipeline/Main.scala` decides which failures are retried.
2. `UpdateService` (`modules/zio-daml`) opens one Ledger API v2 `GetUpdates` stream with transactions and
   reassignments. `--pipeline-datasource=TransactionStream` gets creates and archives;
   `TransactionTreeStream` gets ledger effects, including exercises. The gRPC stubs are generated in
   `ledger-api-zio` (ScalaPB with a custom ZIO plugin, no warts or scalafmt).
3. `Pipeline.run` validates offsets, deletes rows above the last watermark (crash cleanup), seeds an
   empty database that starts after genesis from the active contract set at the start offset, then
   numbers every update with a dense `ix`.
4. `DocumentPostgres` (`modules/postgres/document`) converts in parallel, batches 10,000 models or
   200 ms, and writes each batch in one transaction with COPY into `__transactions`, `__events`,
   `__contracts`, `__exercises`, `__tmp_deactivated_contracts` and `__reassignments`. Batches commit out
   of order. `__watermark` advances only over contiguous `ix`, and its trigger `__update_watermark_fn`
   applies the staged archives and unassigns and enforces the writer fence.
5. The SQL read API, pruning (`pqs datastore postgres-document prune`), redaction and `reset_to_offset`
   are PL/pgSQL in `R__functions.sql` and `R__views_and_triggers.sql`
   (`modules/postgres/document/resources/db/migration/`). Read the generated
   `modules/postgres/document/resources/db/schema-dump.sql` to see the whole database.

Concepts that span files:

- `offset` is the sparse ledger offset; `ix` is PQS's gap-free ordinal, used by every row reference
  (`created_at_ix`, ...). Readers only see data up to the watermark.
- `__contracts`, `__exercises` and `__reassignments` are LIST-partitioned per template or choice. The
  partitions are created at startup from the Daml packages (`__initialize_*`, `SqlSchema.scala`).
- Since multi-sync (3.7) a contract can have several `__contracts` rows (each create or assign starts
  one), and "deactivated" means archived or unassigned.
- Single writer: event pks are allocated from `max(pk)+1`, and `__watermark.instance_id` must match the
  per-connection setting `scribe.instance`.
- CLI: `pqs pipeline ledger postgres-document ...`, `pqs datastore postgres-document schema apply|show`
  and `... prune`. Options are read from CLI arguments, then system properties, then `PQS_*` environment
  variables, then HOCON (`--config`). Changing an option changes the help snapshots in
  `NavigationHelpSpec` and the functest `HelpSpec`.
- Also: `app-blocks/*` (CLI tree, zio-config, logging, o11y, `safe-equals`), `auth` (OAuth or static
  token), `charts/` (Helm), `dashboards/` (Grafonnet; `lib-shared/` and the tooling files come from a
  copier template, so edit `lib/` and `dashboards/` only). `.github/workflows/matrix-compatibility-test.yml`
  is generated from `modules/compatibility/parameters.csv` (`make render-matrix-workflow`).

## Scala tooling and conventions

- **ZIO, not direct style.** Effects, concurrency and resources use `ZIO`, `ZLayer`
  (`ZLayer.fromFunction(Foo.apply)`, `ZLayer.scoped`), `ZStream` and `Scope`; errors travel in the ZIO
  error channel (mostly `Task`), not as `Either` return values. Don't introduce Ox, Tapir, Magnum,
  PureConfig, MacWire or HikariCP. Build with Mill, never sbt, and don't change compiler flags in
  `build.sc`.
- WartRemover runs as errors in every module, tests included: no `null`, `asInstanceOf`/`isInstanceOf`,
  `Option.get`, partial collection methods (`head`, `last`, ...), `return`, `while`, or `==` on `Any`;
  use `===`/`=/=` from `app-blocks/safe-equals`. Suppress a wart only with
  `@SuppressWarnings(Array("org.wartremover.warts.<Name>"))` and a reason.
- Scala 3 braceless syntax with `-new-syntax` (`if ... then`), `given`/`using` in new code,
  `final case class Service(deps)` with `ZLayer.fromFunction`, one `object XxxSpec extends ZIOSpecDefault`
  per file.
- CI compiles with `-Xfatal-warnings` (only when `CI` is set), so an unused import passes locally and
  fails in CI. Fix every warning.
- Bytecode targets Java 17 (`-Xtarget:17`; the functest base image is `eclipse-temurin:17`) while nix
  ships JDK 21: don't use JDK 18+ APIs.
- User-facing text (CLI output, SQL `raise` messages, log lines) reuses the wording of neighbouring
  messages.

## SQL and performance

Read `.claude/rules/sql-and-migrations.md` before writing SQL anywhere. The essentials:

- `V###__` migrations hold DDL and data fixes only and never change once merged. Functions, procedures,
  views and triggers live in the two `R__` files. `V` files run against the previous release's `R__`
  objects. After any change, run `make generate-schema-dump` and add an `UNRELEASED.md` "SQL Migration"
  line with an **[Impact: ...]** estimate.
- Queries on the partitioned tables must let the planner prune partitions and use an index. Benchmark
  read functions and the ingestion path against the previous release and put the numbers in the PR.

## Guidelines from review

`.claude/rules/` holds project-specific rules learned from past incidents and code review. Read `pull-requests.md` before preparing a PR, and the others before editing the files named
in their `paths:` frontmatter: `scala.md` (Scala), `tests.md` (tests), `sql-and-migrations.md`
(`modules/postgres/**`, SQL).
