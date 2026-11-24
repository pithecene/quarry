#!/usr/bin/env bash
# Add a table of the packages in dist to the job's summary.
# Usage: scripts/ci/package-summary.sh

set -euo pipefail

{
  echo "## 📦 Dry Run Package Summary"
  echo
  echo "| File | Size | SHA256 |"
  echo "|------|------|--------|"
  for f in dist/*.tar.gz dist/*.tgz; do
    sha=$(cut -d' ' -f1 "$f.sha256")
    echo "| $(basename "$f") | $(du -h "$f" | cut -f1) | \`${sha:0:16}...\` |"
  done
  echo
  echo "✅ **Dry run complete.** Ready to tag and release."
} >> "$GITHUB_STEP_SUMMARY"
