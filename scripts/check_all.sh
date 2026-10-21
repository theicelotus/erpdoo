#!/bin/bash
# Rebuild erpdoo and run every check against the dev stack (docker compose up
# -d --build first), one after the other, with a summary at the end. Exits
# non-zero if any check failed. CI runs the same checks as separate steps.
#
#   scripts/check_all.sh
#
# PYTHON overrides the interpreter (default: .venv/bin/python if it exists).
cd "$(dirname "$0")/.." || exit 1
PYTHON=${PYTHON:-$( [ -x .venv/bin/python ] && echo .venv/bin/python || echo python )}
checks=(unit smoke replay delivery custom_table backfill)
declare -A command=(
  [unit]="$PYTHON -m pytest -q"
  [smoke]="$PYTHON scripts/smoke_test.py"
  [replay]="$PYTHON scripts/replay_test.py"
  [delivery]="$PYTHON scripts/delivery_test.py"
  [custom_table]="$PYTHON scripts/custom_table_test.py"
  [backfill]="$PYTHON scripts/backfill_test.py"
)
declare -A result
docker compose up -d --build erpdoo >/dev/null 2>&1 || { echo "could not rebuild erpdoo"; exit 1; }
for name in "${checks[@]}"; do
  echo "=== $name"
  if ${command[$name]}; then result[$name]=ok; else result[$name]=FAIL; fi
done
echo "=== summary"
status=0
for name in "${checks[@]}"; do
  echo "$name: ${result[$name]}"
  [ "${result[$name]}" = ok ] || status=1
done
exit $status
