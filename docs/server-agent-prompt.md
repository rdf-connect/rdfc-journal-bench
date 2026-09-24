# Prompt for the agent running the benchmark on the server

Paste the block below. It assumes a Linux machine with systemd, cgroup v2,
Node.js 26, OpenJDK 22, Python 3.12 and 3.14, `uv`, `gradle`, `curl` and git.

---

You are running experiment 2 of the RDF-Connect benchmark on this server. The
repository is `rdfc-journal-bench`. Read `README.md` and
`docs/future-work.md` first; they describe the workload, the arms and the open
questions.

**Set the machine up** exactly as `README.md` "Setting up a machine" says:
clone with submodules, `npm ci && npm run build`, build both processor
checkouts as CLIs, download the JVM runner jars and Nextflow into `vendor/`,
and create the three Python environments. Then:

    loginctl enable-linger $USER     # CPU time comes from a user systemd scope
    export BENCH_RUNS=/dev/shm/rdfc-bench

Record the input before anything else, because everything replays it:

    node dist/bench/record-bluebike.js --out data/bluebike-$(date +%F).ndjson --for=24h

Let it finish. It polls a public API once a minute and appends one snapshot per
line; 24 h is about 1 440 snapshots and 75 MB. Do not shorten it below 6 h, and
do not run two recorders at once. If it rolls past midnight it starts a second
file, and the harness replays only the most recent one.

**Then run the suite:**

    ./bench/all-bluebike.sh

It checks its prerequisites, prints what it found, and runs three phases: a
size sweep over every system, a granularity sweep (snapshots per task), and the
freshness experiment (streaming against the batch workflow re-run on a
schedule). Defaults are in the script header and can be overridden by
environment variables, e.g. `REPS=5 SIZES=5,10,20,60,120 ./bench/all-bluebike.sh`.

Expect several hours. The scattered arms are the slow ones: they start a
process per stage per snapshot, and Toil adds about 15 s of fixed overhead per
invocation. A run that exceeds 30 minutes is killed and reported as a timeout;
that is a result, not a failure.

**How to read the output.** Every run reports a `correct` column. It compares
what was published against what the input implies, computed without running any
of the systems. **A run that is not correct is not a measurement**; report it
rather than averaging it away.

**If runs come back incorrect, check these before anything else:**

1. **Is the checkout current?** Two fixes matter. `bench/bluebike.ts` must
   compare `[station.bikes_available, station.name]`, not the whole station
   record: what counts as a change is decided by the member shape, and
   `bikes_in_use` reaches the member only through the station's total capacity,
   which the shape does not descend into. And the member shape and focus query
   must be sent with every dump, not once — `DumpsToFeed` consumes one of each
   per dump and silently holds back any dump that arrives without them.
2. **Creates right, updates short?** That is the oracle, almost always. Confirm
   with the diagnostic in step 4 before touching any processor.
3. **Everything zero?** The pipeline produced nothing. Look in the run
   directory under `$BENCH_RUNS`: `log.txt` for the RDF-Connect arm,
   `cwltool.log` / `toil.stderr` / `streamflow.log` / `nextflow.log` for the
   others.
4. **The diagnostic that settles it.** Take a member whose update is missing,
   map the two snapshots either side of it in isolation with
   `rml-processor-jvm/bin/rml-map --mapping anchor/bluebike.rml.ttl`, and diff
   the member's quads. Either the change is inside what the member shape
   reaches, in which case a processor is at fault, or it is not, in which case
   the oracle is.

**Do not weaken the oracle to make runs pass.** It is the only check that does
not depend on the systems under test, and both real bugs found so far were
caught by it disagreeing with them.

**Report back:** the three logs (`results/server-{size,granularity,freshness}.log`),
the raw JSON (`results/bluebike.json`, `results/freshness.json`), the machine's
CPU, memory and kernel, the versions of Node, Java, cwltool, Toil, StreamFlow
and Nextflow, and how many snapshots the archive held. Note anything that timed
out or came back incorrect, with the relevant log excerpt.

**Do not** change the workload, the mapping, the shapes or the oracle. If
something seems wrong with them, say so and stop; those choices are the
benchmark's ground truth and are discussed in `docs/future-work.md`.

---

## Known traps, in short

| Symptom | Cause |
|---|---|
| No CPU column | no user systemd session: `loginctl enable-linger $USER` |
| Slow, high disk numbers | `BENCH_RUNS` not on tmpfs |
| Updates short, creates exact | the oracle predates `3c260b2` |
| Only the first snapshot is published | shape/query not sent per dump (`55efee6`) |
| Nextflow reports too many updates | gather not ordered; channels are unordered |
| Toil far slower at the same CPU | it serialises where the others parallelise |
