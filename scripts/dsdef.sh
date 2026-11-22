#!/usr/bin/env bash
# Give a library declaration a docsync id in this repository's style and
# print the id, ready for a `ds:block` citation in a guide.
# `task docs:def -- <args>` runs it; run it from the repository root.
#
#   scripts/dsdef.sh <file>#<Symbol> [stable|api|frozen] [--label <label>]
#
# - Symbol: a function, type, const, var or method (`Client.Start`). A method
#   on a generic receiver names the type parameters: `MachineCtx[D].Raise`.
# - stability: stable (the default: a body, value or list change flags the
#   sentence; right for behaviour, defaults, limits and lists) or api (only a
#   signature, type, rename or value change flags it; right for a sentence
#   about a type's shape or a function's parameters).
# - label: the readable part of the id. By default the package prefix plus
#   the symbol, as the existing defs are named: the root package bare
#   (`supports`), recorder `rec-`, sqlrecorder `sqlrec-`, recordertest
#   `rectest-`, middleware `mw-`, sqlstore `sql-`, any other package its last
#   path element; type parameters are dropped (`MachineCtx[D].Raise` gives
#   `machinectx-raise`). --label sets it when that is not what you want.
# - `ds def` writes `// ds:def ...`, which godoc would show. This turns it
#   into `//ds:def ...` (a Go directive, which godoc hides) and runs gofmt,
#   which puts a blank `//` line between the doc comment and the directive.
# - Idempotent: a declaration that already has a def prints its id and
#   nothing is written.
# - One writer at a time: the edit runs under an exclusive flock on
#   <git dir>/dsdef.lock, so several people or tools giving defs in one
#   checkout never rewrite the same Go file at once (each waits up to
#   lockWait seconds). Without flock on PATH (macOS ships none) it runs
#   unlocked and says so.
set -euo pipefail

# lockWait is how long a second writer waits for the first before failing:
# one def takes well under a second, so two minutes means a stuck holder.
lockWait=120

usage='usage: scripts/dsdef.sh <file>#<Symbol> [stable|api|frozen] [--label <label>]'
target="" stability=stable label=""
while [ $# -gt 0 ]; do
  case $1 in
    --label) label=${2:?$usage}; shift 2 ;;
    --label=*) label=${1#--label=}; shift ;;
    stable|api|frozen) stability=$1; shift ;;
    -*) echo "$usage" >&2; exit 2 ;;
    *) [ -z "$target" ] || { echo "$usage" >&2; exit 2; }; target=$1; shift ;;
  esac
done
case $target in *?#?*) ;; *) echo "$usage" >&2; exit 2 ;; esac
file=${target%%#*}; sym=${target#*#}
[ -f "$file" ] || { echo "dsdef: no file $file (run from the repository root)" >&2; exit 2; }
command -v ds >/dev/null || { echo "dsdef needs the ds CLI: go install github.com/ubgo/docsync/cli/cmd/ds@latest" >&2; exit 1; }

if [ -z "$label" ]; then
  dir=$(dirname "$file")
  case $dir in
    .) prefix="" ;;
    recorder) prefix="rec-" ;;
    recorder/sqlrecorder) prefix="sqlrec-" ;;
    recorder/recordertest) prefix="rectest-" ;;
    middleware) prefix="mw-" ;;
    sqlstore) prefix="sql-" ;;
    *) prefix="$(basename "$dir")-" ;;
  esac
  label=$(printf '%s%s' "$prefix" "$sym" | sed 's/\[[^]]*\]//g' | tr '[:upper:]' '[:lower:]' | tr '.' '-')
fi

if command -v flock >/dev/null; then
  exec 9>"$(git rev-parse --git-dir)/dsdef.lock"
  flock -w "$lockWait" 9 || { echo "dsdef: another def still holds the lock after ${lockWait}s" >&2; exit 1; }
else
  echo "dsdef: flock not found, running without the lock (give one def at a time)" >&2
fi

id=$(ds def "$target" --owner @khanakia --stability "$stability" --label "$label")
if grep -q "// ds:def id=$id" "$file"; then
  sed -i.bak "s#^\([[:space:]]*\)// ds:def id=$id#\1//ds:def id=$id#" "$file" && rm -f "$file.bak"
  gofmt -w "$file"
fi
echo "$id"
