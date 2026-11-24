#!/usr/bin/env bash
# Fail when a workflow, composite action, or Taskfile holds shell. A command there
# is one line without shell metacharacters, quoted or not, and without a GitHub
# expression, which would expand into the command. Anything more belongs in
# scripts/, where shellcheck reads it.
# Usage: scripts/ci/no-inline-shell.sh

set -euo pipefail

block='^[[:space:]]*(- )?((run|cmd|sh):|-)[[:space:]]*[|>]'
meta='(\$\(|\$\{|[`&|;<>])'
command="^[[:space:]]*(- )?(run|cmd|sh):.*$meta"
item="^[[:space:]]+- .*$meta"

# grep exits 1 for no match, and above 1 for a failed read, which must not pass.
scan() { grep -HnE "$@" || [[ $? -eq 1 ]]; }

files=()
while IFS= read -r file; do files+=("$file"); done < <(
  find .github \( -path '.github/workflows/*' -name '*.y*ml' \) -o -name action.yml -o -name action.yaml
)
found=$(scan -e "$block" -e "$command" "${files[@]}" Taskfile.yaml)
found+=$(scan -e "$item" Taskfile.yaml)
if [[ -n "$found" ]]; then
  echo "$found"
  echo "::error::Shell in YAML; move it to a script in scripts/"
  exit 1
fi
