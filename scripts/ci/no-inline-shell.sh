#!/usr/bin/env bash
# Fail when YAML holds shell: a command block, or a command that substitutes, pipes,
# chains, or redirects. Shell belongs in scripts/, where shellcheck reads it.
# Usage: scripts/ci/no-inline-shell.sh

set -euo pipefail

block='^[[:space:]]*(- )?(run:|-) [|>][+-]?$'
logic='^[[:space:]]*(- )?(run|sh): .*(\$\(|`|&&|\|\||;|[<>]| \| )'
task='^[[:space:]]+- .*(\$\(|`|&&|\|\||;|[<>]| \| )'
found=$(
  grep -HnE -e "$block" -e "$logic" .github/workflows/*.y*ml .github/actions/*/action.yml Taskfile.yaml || true
  grep -HnE -e "$task" Taskfile.yaml || true
)
if [[ -n "$found" ]]; then
  echo "$found"
  echo "::error::Shell in YAML; move it to a script in scripts/"
  exit 1
fi
