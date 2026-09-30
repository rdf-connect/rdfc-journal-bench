#!/usr/bin/env bash
# Experiment 2 in full: the deployed Blue-bike pipeline under every system.
#
#   ./bench/all-bluebike.sh
#
# Environment:
#   BENCH_RUNS   working directories (default /dev/shm/rdfc-bench, i.e. tmpfs)
#   REPS         repetitions per configuration (default 5)
#   SIZES        snapshots per run for the size sweep (default 5,10,20,30,60)
#   UNITS        snapshots per task for the granularity sweep (default 1,5,15,30)
#   ARRIVAL      ms between arrivals in the freshness runs (default 10000)
#   FRESH_N      snapshots in the freshness runs (default 60)
#   INTERVALS    arrivals between scheduled runs (default 2,6,12,30)
#
# Results land in results/*.json and results/*.log. Per-run directories are
# under BENCH_RUNS and are not needed afterwards.
set -u
cd "$(dirname "$0")/.."

RUNS=${BENCH_RUNS:-/dev/shm/rdfc-bench}
REPS=${REPS:-5}
SIZES=${SIZES:-5,10,20,30,60}
UNITS=${UNITS:-1,5,15,30}
ARRIVAL=${ARRIVAL:-10000}
FRESH_N=${FRESH_N:-60}
INTERVALS=${INTERVALS:-2,6,12,30}
export BENCH_RUNS=$RUNS

fail() { echo "  !! $*" >&2; exit 1; }

echo "== checking the machine"
[ -d dist/bench ] || fail "not built: npm ci && npm run build"
[ -f rml-processor-jvm/build/libs/rml-map.jar ] || fail "missing rml-map.jar: (cd rml-processor-jvm && gradle shadowJar cliJar)"
[ -f shacl-processor-ts/lib/cli.js ] || fail "missing shacl CLI: (cd shacl-processor-ts && npm ci && npm run build)"
[ -f vendor/jvm-runner.jar ] || fail "missing vendor/jvm-runner.jar (see README)"
[ -x vendor/nextflow ] || echo "  -- no vendor/nextflow: the nextflow arm will be skipped"
[ -x .venv/bin/cwltool ] || fail "missing cwltool venv (see README)"
ls data/bluebike-*.ndjson >/dev/null 2>&1 || fail "no recording in data/: run bench/record-bluebike.js first"

# The harness replays the most recent recording, not all of them together;
# a recorder running past midnight starts a new file.
ARCHIVE=$(ls data/bluebike-*.ndjson | sort | tail -1)
SNAPSHOTS=$(wc -l < "$ARCHIVE")
echo "  archive: $SNAPSHOTS snapshots in $ARCHIVE"
LARGEST=${SIZES##*,}
[ "$SNAPSHOTS" -ge "$LARGEST" ] || fail "archive holds $SNAPSHOTS snapshots, the sweep asks for $LARGEST"

# CPU time comes from a transient systemd scope; without a user session the
# harness silently falls back to sampling and reports no CPU at all.
if systemd-run --user --scope --quiet true 2>/dev/null; then
  echo "  cgroup accounting: available"
else
  echo "  !! systemd-run --user does not work: no CPU time will be recorded."
  echo "     On a server over SSH: loginctl enable-linger \$USER"
fi

case "$RUNS" in
  /dev/shm/*) echo "  run directories on tmpfs: $RUNS" ;;
  *) echo "  -- run directories are NOT on tmpfs ($RUNS); file-passing arms will be measured against the disk" ;;
esac

ARMS=shell,rdfc,cwl-batch,cwl-scatter,toil-batch,toil-scatter,streamflow-batch,streamflow-scatter
[ -x vendor/nextflow ] && ARMS=$ARMS,nextflow

mkdir -p "$RUNS" results

echo
echo "== 1/3 size sweep: every system, $SIZES snapshots, $REPS reps"
node dist/bench/run-bluebike.js --ns="$SIZES" --reps="$REPS" --arms="$ARMS" \
  2>&1 | tee results/server-size.log

echo
echo "== 2/3 granularity: snapshots per task $UNITS at ${LARGEST} snapshots"
: > results/server-granularity.log
# Each call writes its own raw file: results/bluebike.json is the size sweep's.
mkdir -p results/granularity
for u in ${UNITS//,/ }; do
  for arm in cwl-scatter nextflow; do
    [ "$arm" = nextflow ] && [ ! -x vendor/nextflow ] && continue
    node dist/bench/run-bluebike.js --ns="$LARGEST" --reps="$REPS" --arms="$arm" --unit="$u" \
      --out="results/granularity/u$u-$arm.json" 2>/dev/null | grep -E "^ +$LARGEST " | sed "s/^/unit=$u /" | tee -a results/server-granularity.log
  done
done

echo
echo "== 3/3 freshness: $FRESH_N snapshots arriving every ${ARRIVAL}ms"
node dist/bench/run-freshness.js --n="$FRESH_N" --arrival="$ARRIVAL" \
  --arms=shell,rdfc --intervals="$INTERVALS" --modes=rebuild,incremental \
  2>&1 | tee results/server-freshness.log

echo
echo "== done. results/server-{size,granularity,freshness}.log"
echo "   raw: results/bluebike.json, results/granularity/*.json, results/freshness.json"
echo "   every run reports 'correct'; a run that is not correct is not a result."
