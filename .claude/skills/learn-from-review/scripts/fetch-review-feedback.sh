#!/usr/bin/env bash
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Prints the human review feedback of one pull request as plain text: review summaries, inline
# review threads (resolved/outdated state, diff context and every reply) and conversation comments.
# Bot accounts are dropped. Read-only: it runs a GraphQL query, never a mutation.
#
# Usage: fetch-review-feedback.sh [PR number or URL]   (default: the PR of the current branch)

set -euo pipefail

pr="${1:-}"
if [[ -z "$pr" ]]; then
  pr="$(gh pr view --json number --jq .number)"
fi
pr="${pr%%#*}" # accept a PR URL, with or without an anchor
pr="${pr%/}"
pr="${pr##*/}"

repo="$(gh repo view --json owner,name --jq '.owner.login + "/" + .name')"

# shellcheck disable=SC2016 # $owner, $name and $number are GraphQL variables
query='
query($owner: String!, $name: String!, $number: Int!) {
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      number title url state
      author { login }
      reviews(first: 100) {
        pageInfo { hasNextPage }
        nodes { author { __typename login } state body url }
      }
      reviewThreads(first: 100) {
        pageInfo { hasNextPage }
        nodes {
          isResolved isOutdated path line originalLine
          comments(first: 50) {
            nodes { author { __typename login } body url diffHunk }
          }
        }
      }
      comments(first: 100) {
        pageInfo { hasNextPage }
        nodes { author { __typename login } body url }
      }
    }
  }
}'

gh api graphql \
  -f query="$query" \
  -F owner="${repo%%/*}" \
  -F name="${repo##*/}" \
  -F number="$pr" \
  --jq '
    def human: .author != null and .author.__typename != "Bot";
    def who($pr_author): .author.login + (if .author.login == $pr_author then " (PR author)" else "" end);
    def hunk: (.diffHunk // "" | split("\n") | .[-6:] | map("    | " + .) | join("\n"));
    def truncated($what): if .pageInfo.hasNextPage then "WARNING: more than 100 \($what); only the first 100 are shown" else empty end;
    .data.repository.pullRequest
    | .author.login as $a
    | "PR #\(.number): \(.title)\n\(.url) [\(.state)] by \($a)\n",
      (.reviews | truncated("reviews")),
      (.reviewThreads | truncated("review threads")),
      (.comments | truncated("conversation comments")),
      "== Review summaries ==",
      (.reviews.nodes[] | select(human and (.body | length) > 0)
        | "- \(who($a)) [\(.state)] \(.url)\n  \(.body | gsub("\n"; "\n  "))"),
      "",
      "== Inline review threads ==",
      (.reviewThreads.nodes[]
        | select(any(.comments.nodes[]; human))
        | "--- \(.path):\(.line // .originalLine) [\(if .isResolved then "resolved" else "unresolved" end)\(if .isOutdated then ", outdated" else "" end)]",
          (.comments.nodes[0] | hunk),
          (.comments.nodes[] | select(human) | "  > \(who($a)): \(.body | gsub("\n"; "\n    "))\n    \(.url)")),
      "",
      "== Conversation comments ==",
      (.comments.nodes[] | select(human)
        | "- \(who($a)) \(.url)\n  \(.body | gsub("\n"; "\n  "))")
  '
