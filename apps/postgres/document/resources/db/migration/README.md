# Migration Conventions

## Versioned migrations (`V___`)

- **Purpose**: DDL changes — tables, columns, indexes, constraints, composite types.
- **Behavior**: Run once, in order, immutable. Never edit after merge.
- **Rule**: Must NOT contain `CREATE [OR REPLACE] FUNCTION` or `CREATE [OR REPLACE] PROCEDURE` (post-V035).

## Repeatable migrations (`R__`)

- **Purpose**: Functions, procedures, triggers, and views.
- **Behavior**: Re-run whenever file checksum changes, after all versioned migrations.
- **Location**:
  - `R__functions.sql`: all functions (except triggered functions) and procedures
  - `R__views_and_triggers.sql`: all views and their triggers, including triggered functions

PostgreSQL is fully compatible with this split and does not require deeper layered categorization for these definitions. This two-file model is the simplest to reason about and the easiest place to add new objects.


## Adding a new function, trigger or procedure

1. Edit `R__01_functions_triggers.sql`.
2. If the function needs DDL changes (new column, type change), create a new `V__` migration for the DDL.
3. The `R__` file will be re-applied automatically on next Flyway run if its checksum changed.

## Adding a new view

Follow the same steps as adding a new function (above), but edit `R__50_views.sql`.

## Deleting a function, procedure, or view

1. Remove the object definition from the relevant repeatable file:
	- `R__01_functions_triggers.sql` for functions/procedures/triggers.
	- `R__50_views.sql` for views.
2. Add a new versioned `V__` migration with the matching drop statement:
	- `DROP FUNCTION ...`
	- `DROP PROCEDURE ...`
	- `DROP VIEW ...`

# Which release?

To get release version to schema version correspondence, issue the following command:

```bash
for sql in *.sql; do echo "$(git tag --sort=version:refname --contains "$(git log --diff-filter=A --format=%H -- $sql)" | head -1) - $sql"; done
```

from root:
```bash
for sql in postgres/document/resources/db/migration/*.sql; do echo "$(git tag --sort=version:refname --contains "$(git log --diff-filter=A --format=%H -- $sql)" | head -1) - $sql"; done
```