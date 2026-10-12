---
name: learn-from-review
description: Use when a pull request in this repository has human code-review comments worth keeping for future agents, e.g. after addressing review feedback, or when asked to update the guidelines, best practices or .claude/rules from a PR review.
argument-hint: "<PR number or URL>"
allowed-tools: Bash(bash .claude/skills/learn-from-review/scripts/fetch-review-feedback.sh *)
---

# Learn from review

Turn the human review feedback on one PR into guidelines in `.claude/rules/`, so the next agent doesn't
get the same comment. Maintainers read these files, and AI_POLICY.md applies to them: every rule must be
verified, sourced and short.

## 1. Fetch

```bash
bash .claude/skills/learn-from-review/scripts/fetch-review-feedback.sh <PR number or URL>
```

This prints review summaries, every inline thread with its resolved/outdated state, diff context and
replies, and the conversation comments, without bots. Without an argument it uses the current branch's
PR. Then look at the code as merged (`gh pr diff <PR>`, or the files on `main`); you don't need the
intermediate commits.

## 2. Classify each thread

Read the whole thread, then the final code, and put the thread in exactly one bucket. The final code
and the replies decide whether a suggestion was adopted; GitHub's resolved flag does not.

| Bucket | Test | Action |
|---|---|---|
| Rule | The suggestion is in the final code or agreed in a reply, and a reviewer would make the same comment on a different PR | Write a rule, or extend the closest one |
| Covered | An existing rule, CLAUDE.md, scalafmt, WartRemover or CI already says it | If that rule doesn't link this PR yet, add this comment's link; no new text |
| One-off | The fix only concerns this diff: a typo, an unused import, a rename, a local bug, an answered question | Skip |
| Not adopted | Declined, superseded by a later suggestion, or absent from the final code; or the PR is not merged | Skip |

If the review or the merged code shows that an existing rule is wrong or too broad, correct that rule.

## 3. Write

Each rule is one bullet of this shape:

```markdown
- <What to do, in the imperative>; <the failure it prevents, in one clause> ([#<PR>](<comment URL>)).
```

- Keep it to two lines, about 40 words. The link carries the history: no story of the PR, no quotes,
  no counts, and only the identifiers needed to apply the rule.
- Generalize as far as the evidence goes and no further: a comment about one helper is a rule about that
  kind of code, not about all code.
- If existing code doesn't follow the rule yet, begin it with "In new code," so nobody refactors old
  code unasked.
- Put it in the file whose `paths` cover the code the comment was on, under the closest section:
  `scala.md` (Scala), `sql-and-migrations.md` (`modules/postgres/**`, `*.sql`), `tests.md` (test
  sources) or `pull-requests.md` (release notes and PR scope; loaded in every session). If none fits,
  propose a new file in your report instead of creating one.
- Check that every identifier, path and command you name exists (`git grep`, or Metals `glob-search`).

## 4. Report, then stop

Reply with:

1. each rule added or changed, quoted, with its file;
2. each thread considered, with its bucket and a one-line reason;
3. anything the human must decide: conflicting rules, a proposed new file.

Leave the edits uncommitted for the human to review. Don't reply to, react to or resolve review
threads, and don't commit or push, unless the user asks.
