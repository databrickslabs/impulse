#!/usr/bin/env bash
#
# Sync a fork PR to a branch in databrickslabs/impulse and open a test PR.
#
# CI (lint + unit tests) is skipped for PRs from forks, because GitHub does not
# issue an OIDC token or pass secrets to fork-triggered runs, so JFrog auth — and
# therefore dependency installation — cannot run. This script pushes the fork's
# code to a branch in the main repo, where it is no longer a fork PR and thus gets
# OIDC/secrets, so the full suite runs against the real (JFrog/protected-runner)
# environment.
#
# SECURITY: only run this AFTER reviewing the fork's code.
#
# Usage:
#   .github/scripts/fork-sync-pr.sh <PR_NUMBER>     # or: make fork-sync PR=<number>
#
# Prerequisites:
#   - gh CLI installed and authenticated (gh auth login)
#   - Run from a clone of databrickslabs/impulse (or your fork with upstream configured)
#
# First run: Creates fork-test/pr-<N> branch and opens a test PR.
# Subsequent runs: Syncs latest changes from the fork PR to the same branch (force push).
#
set -e

UPSTREAM_REPO="databrickslabs/impulse"
BASE_BRANCH="main"

if [ -z "$1" ]; then
  echo "Usage: $0 <PR_NUMBER>"
  echo ""
  echo "Example: $0 123"
  echo ""
  echo "Syncs fork PR #123 to branch fork-test/pr-123 in $UPSTREAM_REPO,"
  echo "creating a test PR so lint/unit tests can run with JFrog/OIDC."
  exit 1
fi

PR_NUMBER="$1"
SYNC_BRANCH="fork-test/pr-${PR_NUMBER}"

# Verify we're in a git repo
if ! git rev-parse --git-dir >/dev/null 2>&1; then
  echo "Error: Not in a git repository. Run from a clone of $UPSTREAM_REPO or your fork."
  exit 1
fi

# Capture the initial branch so we can restore it on exit (success or failure).
# `--show-current` is empty in detached-HEAD state; fall back to the commit SHA in that case.
ORIGINAL_REF=$(git branch --show-current)
if [ -z "$ORIGINAL_REF" ]; then
  ORIGINAL_REF=$(git rev-parse HEAD)
fi
trap 'git checkout --quiet "$ORIGINAL_REF" 2>/dev/null || true' EXIT

# Verify gh is installed
if ! command -v gh >/dev/null 2>&1; then
  echo "Error: gh CLI is required. Install from https://cli.github.com/"
  exit 1
fi

# Ensure upstream remote points to main repo
if ! git remote get-url upstream >/dev/null 2>&1; then
  echo "Adding upstream remote: $UPSTREAM_REPO"
  git remote add upstream "https://github.com/${UPSTREAM_REPO}.git"
fi

UPSTREAM_URL=$(git remote get-url upstream)
if [[ "$UPSTREAM_URL" != *"${UPSTREAM_REPO}"* ]]; then
  echo "Error: upstream remote does not point to $UPSTREAM_REPO"
  echo "  current: $UPSTREAM_URL"
  exit 1
fi

# Fetch and checkout the fork PR branch (PR lives in upstream repo)
echo "Fetching PR #${PR_NUMBER} from fork..."
gh pr checkout "$PR_NUMBER" --repo "$UPSTREAM_REPO"

# Create or update sync branch
git checkout -B "$SYNC_BRANCH"

# Push to upstream (force to overwrite with latest from fork)
echo "Pushing to upstream/${SYNC_BRANCH}..."
git push --force upstream "HEAD:${SYNC_BRANCH}"

# Create test PR if it does not exist
EXISTING=$(gh pr list --repo "$UPSTREAM_REPO" --head "$SYNC_BRANCH" --state open --json number -q '.[0].number' 2>/dev/null || true)
if [ -z "$EXISTING" ]; then
  # Ensure the labels exist (no-op if already present)
  gh label create do-not-merge --repo "$UPSTREAM_REPO" --color B60205 --description "Never merge this PR" 2>/dev/null || true
  gh label create fork-test --repo "$UPSTREAM_REPO" --color 0E8A16 --description "Mirror of a fork PR for CI testing" 2>/dev/null || true

  PR_URL=$(gh pr view "$PR_NUMBER" --repo "$UPSTREAM_REPO" --json url -q '.url')
  PR_TITLE=$(gh pr view "$PR_NUMBER" --repo "$UPSTREAM_REPO" --json title -q '.title')
  TEST_PR_TITLE="Fork test: PR #${PR_NUMBER} - ${PR_TITLE}"
  echo "Creating test PR..."
  gh pr create --repo "$UPSTREAM_REPO" \
    --base "$BASE_BRANCH" \
    --head "$SYNC_BRANCH" \
    --title "$TEST_PR_TITLE" \
    --label "do-not-merge" \
    --label "fork-test" \
    --body "Automated sync from fork PR for CI testing.

Original PR: ${PR_URL}

Lint and unit tests run on this PR (they are skipped for fork PRs). Do not merge this
PR — merge the original fork PR once its CI is green here."
  echo "Test PR created."
else
  echo "Test PR already exists: #${EXISTING}"
  echo "Branch ${SYNC_BRANCH} has been updated with latest changes from fork PR #${PR_NUMBER}."
fi