# Table of Contents

- [Contributing to the PQS repository](#contributing-to-the-pqs-repository)
  - [Picking up issues](#picking-up-issues)
  - [Opening PRs](#opening-prs)
  - [Opening new issues](#opening-new-issues)
  - [Testing](#testing)
  - [Git Hygiene](#git-hygiene)
  - [Branch Naming](#branch-naming)
  - [DB Migrations](#db-migrations)
  - [Copyright Headers](#copyright-headers)

# Contributing to the PQS repository

In order to setup your development environment, please see the [Engineer Setup](ENGINEER_SETUP.md).

## Picking up issues

If you are planning to work on an issue please assign yourself to it (if you are able to) or leave a comment, to
avoid duplicate work across contributors. If the issue is not new, it is also a good idea to reach out to the
core contributors before working on it, to check how relevant it still is, and whether it is something worth
working on.

## Opening PRs

- Keep patches small, focused, and clearly motivated. One logical change per PR.
- Reference the issue that the PR addresses.
- Keep the text in your PR concise, write only what is useful for the reviewers to read. Specifically,
  avoid AI-generated text in PR descriptions. See our [AI contribution guidelines](AI_POLICY.md).

## Opening new issues

Please note that GH issues are not meant for Q&A, but rather for tracking future and current work items.
Issues that are purely a question may be closed with no further comments.
Please also consult our [AI contribution guidelines](AI_POLICY.md) for guidelines on AI-created issues.

## Testing

Every contribution must be tested in an automated test.

## Git Hygiene

### Branch Naming

If you are a PQS Contributor and therefore have write permissions to the PQS repo directly,
please prefix branch names by your name, followed by a slash and a descriptive name:

`<yourname>/<descriptivename>`

For example, if Bob is working on issue 4242 to "fix FooTest", he could name his branch:

`bob/fix-footest/4242`.

### Merge Strategy

PRs are merged into `main` using **squash and merge**. This collapses all commits in the PR into a single commit on `main`, keeping the history clean and linear.

### Squash-and-Merge Commit Messages

Because the squash commit is the only record of the change on `main`, its message matters.
Follow these conventions:

* **Title (first line):** A concise summary of the change, written in imperative mood (e.g., "Add external transaction hash to Scan API", not "Added …" or "Adds …"). GitHub defaults the title to the PR title, so make sure the PR title is descriptive.
* **Body:** GitHub auto-populates the body with the PR description.

## DB Migrations

Refer to [the main README on migrations](apps/postgres/document/resources/db/migration/README.md).

## Copyright Headers

All source files **must** carry the required Apache-2.0 copyright header. Contributors are
responsible for adding the correct header to any new file **before** committing it. A pre-commit
hook (`check-copyright-headers`) and CI both verify that headers are present — a missing or invalid
header will fail the check.

To add or fix the headers locally, run:

```bash
make copyright-update
```

This inserts the required header into any file that is missing one. Re-run the check with
`make copyright-check` (the same check CI runs) to confirm everything is compliant before you
push.

- The header text and the file-type/exclude configuration live in
  `.scaffold/libs/python/copyright-headers.json`.
- Any new or currently unsupported file extension must either be given the header or be added to the
  config's excludes.
