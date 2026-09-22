#!/usr/bin/env bash
# Full experiment-1 sweep suite.
set -u
cd "$(dirname "$0")/.."
ALL=rdfc,rdfc-direct,rdfc-split,native,native-loose
MAIN=rdfc,native,native-loose

run() {
  echo "############ sweep: $1 (arms=$2 reps=$3) ############"
  timeout 1800 node dist/bench/run.js "$1" --reps="$3" --arms="$2"
  echo
}

run startup "$ALL"  5
run rate    "$MAIN" 3
run depth   "$MAIN" 3
run payload "$MAIN" 3
run stream  "rdfc,native" 3
run work        "$MAIN" 3
run work-depth  "rdfc,native" 3
echo "ALL SWEEPS DONE"
