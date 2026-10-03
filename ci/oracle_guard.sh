#!/bin/sh
# Oracle guard (SPEC.md, "Parity harness and correctness tests").
#
# Usage: ci/oracle_guard.sh <target-ref> [<head>]
#
# Diffs <head> (default HEAD) against its merge base with <target-ref> and fails
# if any protected path is touched. CI always runs the TARGET branch's copy of
# this script (git show origin/<target>:ci/oracle_guard.sh), never the MR's.
# A directory entry (trailing slash) protects everything under it; any other
# entry is an exact path.
set -eu

PROTECTED="
parity/golden/
parity/replay/
parity/examples/
parity/probes/
legacy-spark/
contracts/
proto/
baseline/manifest.json
privacy/
tools/
ci/
.github/workflows/
.gitlab-ci.yml
"

target=${1:?usage: ci/oracle_guard.sh <target-ref> [<head>]}
head=${2:-HEAD}

base=$(git merge-base "$target" "$head")
echo "oracle guard: target $target ($(git rev-parse --short "$target")), head $(git rev-parse --short "$head"), merge base $(git rev-parse --short "$base")"

changed=$(git diff --name-only --no-renames "$base" "$head")
total=$(printf '%s\n' "$changed" | grep -c . || true)

touched=""
for path in $changed; do
  for rule in $PROTECTED; do
    case $rule in
      */) case $path in "$rule"*) touched="$touched$path
"; break ;; esac ;;
      *)  [ "$path" = "$rule" ] && { touched="$touched$path
"; break; } ;;
    esac
  done
done

count=$(printf '%s' "$touched" | grep -c . || true)
if [ "$count" -gt 0 ]; then
  echo "FAIL: $count protected file(s) touched (of $total changed):"
  printf '%s' "$touched" | sed 's/^/  /'
  echo "Protected paths: $(echo $PROTECTED | tr '\n' ' ')"
  exit 1
fi
echo "PASS: 0 protected files touched (of $total changed)"
