#!/usr/bin/env bash
# Run the CLI parity tests, and fail when none exist, which would pass otherwise.
# Usage: scripts/ci/cli-parity.sh

set -euo pipefail

cd quarry
tests=$(go test ./cli/cmd -list TestCLIParity)
if ! grep -q '^TestCLIParity' <<< "$tests"; then
  echo "::error::No TestCLIParity* tests found; the parity gate requires at least one"
  exit 1
fi
go test ./cli/cmd -run Parity -v
