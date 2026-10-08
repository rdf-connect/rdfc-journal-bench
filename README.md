# RDF-Connect journal benchmarks

Benchmarks for the RDF-Connect journal paper, extending the SofLiM4KG'24
workshop paper ([CEUR Vol-3830, paper 1](https://ceur-ws.org/Vol-3830/paper1.pdf)).
That paper's future work proposed the central orchestrator that now exists;
these experiments measure what it costs and how it compares to other frameworks.

## Experiment 1 — RDF-Connect against itself (implemented)

Quantifies the orchestration overhead by running **the same processor classes**
two ways: through `pipeline.ttl` + orchestrator + `js-runner` over gRPC, and
wired by hand with in-memory channels. `Reader`/`Writer` are interfaces, so the
processors are byte-identical between arms and only the transport differs.

- Design and findings: **[docs/benchmark-1-results.md](docs/benchmark-1-results.md)**
- Raw data: `results/*.json`, consolidated in `results/summary.{md,csv}`

RDF-Connect adds a roughly **fixed ~0.23 ms per message per hop**, plus
**~1.55 s fixed startup**. Whether that matters depends entirely on the
processors:

| Work per message | Overhead |
|---|---|
| 0 (do-nothing processors) | ×96.8 — 99 % of runtime is framework |
| 100 µs | ×4.0 |
| 10 ms | ×1.08 — 7.5 % |
| 100 ms | ×1.02 — 1.5 % |

**METG(50 %) ≈ 0.4 ms** of work per processor per message: below it the
framework is the bottleneck, above it the processor is. Realistic RML/SHACL
processors sit well above that line.

⚠️ **The ×60–120 figures are a worst case** produced by processors that do no
work at all, and should never be quoted without the work sweep beside them.
See the framing note at the top of the results document.

Also found: a **hard 4 MB message ceiling** that fails with exit code 0.

## Experiment 2 — RDF-Connect against other frameworks (in progress)

Runs one pipeline under several systems and compares what the composition
costs. The design problem is that RDF-Connect streams and workflow managers
batch, so a single pipeline at a single size picks the winner before anything
runs; the answer is to sweep the workload and report a surface, following Task
Bench (Slaughter et al., SC'20).

- Design: **[docs/benchmark-2-design.md](docs/benchmark-2-design.md)**
- State, results and open questions: **[docs/future-work.md](docs/future-work.md)**
- Draft of the paper section: **[docs/section-6-draft.tex](docs/section-6-draft.tex)**

**The workload** is the pipeline behind the deployed `CA-Blue-Bike-LDES`:
snapshots of a bike-sharing API are lifted with the deployment's own RML
mapping, validated, turned into a feed of changes, annotated as a stream,
fragmented by time, and published as an LDES on disk. All six stages are
published processors across two runtimes. A two-stage pipeline (`anchor/`)
remains as calibration and as the workload with known-invalid records.

**The systems**, all running the same processors over the same recorded input:

| Arm | What it is |
|---|---|
| `shell` | the six tools in one pipe, metadata channels as FIFOs |
| `rdfc` | orchestrator, JVM runner for the mapper, Node.js runner for the rest |
| `cwl-batch` / `cwl-scatter` | cwltool, one task per stage / per chunk |
| `toil-batch` / `toil-scatter` | the same workflows on Toil |
| `streamflow-batch` / `streamflow-scatter` | the same workflows on StreamFlow |
| `nextflow` | Nextflow, stages overlapping through channels |

Every run is checked against expected output derived from the input, so a
system that is fast because it did less work is reported as failed.

## Next steps

See **[TODO.md](TODO.md)** and **[docs/future-work.md](docs/future-work.md)**.

## Layout

```
src/processors.ts    BenchGenerator / BenchPassThrough / BenchSink — shared by both arms
src/inmem.ts         in-memory Reader/Writer with the framework's exact ack semantics
src/native.ts        hand-wired driver (arm B), replicating Runner's lifecycle
src/genpipeline.ts   emits pipeline.ttl for arm A (and for the anchor pipeline)
src/anchor.ts        sources and sinks for the experiment-2 rdfc pipelines
src/genbluebike.ts   emits pipeline.ttl for the realistic (Blue-bike) pipeline
processors.ttl       processor descriptions + SHACL shapes + runner definitions
anchor/              workloads: RML mappings and SHACL shapes
steps/               per-stage processor descriptions, shared by the CLI arms
cwl/                 CWL tools and workflows (used by cwltool, Toil, StreamFlow)
nextflow/            the same pipeline in Nextflow DSL2
bench/run.ts         sweep harness (experiment 1 and the anchor pipeline)
bench/run-bluebike.ts  sweep harness (the realistic pipeline)
bench/bluebike.ts    the realistic workload: archive, per-arm commands, oracle
bench/record-bluebike.ts  records the Blue-bike API into a replayable archive
bench/rdfc-proc.ts   runs any JS processor as a CLI, configured from RDF
bench/proc.ts        runs a command: wall clock, line timestamps, CPU, memory, disk
bench/gen-records.ts generator for the synthetic workload
bench/stages.ts      cost of a single stage as a CLI tool
bench/summarize.ts   raw JSON -> paper-ready tables
bench/all.sh         experiment-1 suite
```

Generated and downloaded content is not in the repository: `node_modules/`,
`dist/`, `vendor/`, `data/`, `.venv*/`, `pipelines/`, and the per-run
directories under `results/`. Only result summaries are versioned. The one
exception in `pipelines/` is `rml-index-only.ttl`, the RmlMapper definition
the generated pipelines import in place of the jar.

## Setting up a machine

Tested with Node.js 26, OpenJDK 22, Python 3.14 (plus 3.12 for StreamFlow),
`uv`, `gradle` and `curl` on Linux with systemd and cgroup v2.

```bash
# 1. the repository and the two processor checkouts (submodules on cli-tools)
git clone --recurse-submodules <this repo> && cd rdfc-journal-bench
npm ci && npm run build

# 2. the processors, built as libraries and as CLIs
(cd rml-processor-jvm && gradle shadowJar cliJar)
(cd shacl-processor-ts && npm ci && npm run build)

# 3. the JVM runner (the published runner definition would curl it on every
#    start) and Nextflow
mkdir -p vendor
J=https://javadoc.jitpack.io/com/github/rdf-connect/jvm-runner/runner/master-SNAPSHOT
curl -L -o vendor/jvm-runner.jar $J/runner-master-SNAPSHOT-all.jar
curl -L -o vendor/jvm-runner-index.jar $J/runner-master-SNAPSHOT-index.jar
curl -s https://get.nextflow.io -o vendor/nextflow && chmod +x vendor/nextflow

# 4. the workflow runners, each in its own environment
python3 -m venv .venv && .venv/bin/pip install cwltool
uv venv -p 3.14 .venv-toil && uv pip install -p .venv-toil "toil[cwl]"
uv venv -p 3.12 .venv-streamflow && \
  uv pip install -p .venv-streamflow --prerelease=allow "streamflow==0.2.0rc3"

# 5. the input: record it, or copy an existing archive into data/
node dist/bench/record-bluebike.js --out data/bluebike-$(date +%F).ndjson --for=24h
```

Two things to check on a server:

- **CPU time needs a user systemd session.** Each measured command runs in a
  transient scope so the kernel accounts for every descendant. Over SSH that
  requires `loginctl enable-linger $USER`; without it the harness falls back to
  sampling and reports no CPU time.
- **Put the run directories on tmpfs**, so the file-passing systems are
  measured against the systems rather than against the disk:
  `export BENCH_RUNS=/dev/shm/rdfc-bench`. Allow a few hundred MB, more for
  long archives.

**gRPC message limit (patched).** RDF-Connect leaves gRPC's 4 MiB receive
limit unset, so larger messages fail (the orchestrator exits 0; a JVM runner
exits and the pipeline hangs). The benchmark runs a patched stack with a
256 MiB limit, overridable with `RDFC_MAX_MESSAGE_BYTES`:

- orchestrator and js-runner: `patches/`, reapplied by `npm ci` (postinstall
  runs `patch-package`);
- JVM runner: `patches-jvm/jvm-runner-max-message.patch` against
  `rdf-connect/jvm-runner` at `f5b6df6`. The jars downloaded in step 3 are
  **unpatched**; build instead:

  ```bash
  git clone https://github.com/rdf-connect/jvm-runner vendor/jvm-runner-src
  cd vendor/jvm-runner-src && git checkout f5b6df6 \
    && git apply ../../patches-jvm/jvm-runner-max-message.patch \
    && ./gradlew :runner:shadowJar :runner:indexJar
  cp runner/build/libs/runner-0.0.4-all.jar ../jvm-runner.jar
  cp runner/build/libs/index-0.0.4-index.jar ../jvm-runner-index.jar
  ```

  The patch also lets JDK 21 build it, targeting the same Java 11 bytecode.

Reproducibility gap: `rml-processor-jvm` resolves the RMLMapper and the runner
types from JitPack at `master-SNAPSHOT`, so two machines can build against
different code. Pin these to commit hashes before the final runs.

## Running

```bash
# experiment 1
./bench/all.sh
node dist/bench/summarize.js

# experiment 1 sweeps individually
node dist/bench/run.js <sweep> --reps=3 --arms=rdfc,native
# sweeps: startup | rate | depth | payload | stream | work | work-depth | smoke
# arms:   rdfc | rdfc-direct | rdfc-split | native | native-loose
```

```bash
# experiment 2, the realistic pipeline
export BENCH_RUNS=/dev/shm/rdfc-bench
node dist/bench/run-bluebike.js --ns=5,10,20,30 --reps=3
node dist/bench/run-bluebike.js --ns=30 --reps=3 --arms=cwl-scatter --unit=5
# arms: shell | rdfc | cwl-batch | cwl-scatter | toil-batch | toil-scatter
#       | streamflow-batch | streamflow-scatter | nextflow
# --unit is snapshots per task for the workflow managers; the streaming arms
#   always work a snapshot at a time.

# experiment 2, the two-stage calibration pipeline
node dist/bench/run.js anchor-unit --reps=3 --arms=shell,rdfc,cwl-scatter
# sweeps: anchor-smoke | anchor-check | anchor-quick | anchor-n | anchor-unit
node dist/bench/stages.js --ns=1,100,1000,10000   # cost of one stage, cold vs warm
```

## Issues found while benchmarking

These are framework bugs surfaced by the experiments, worth fixing independently
of the paper:

1. **Remote ontology fetch on every pipeline start** — `readQuads([RDFC.namespace])`
   costs 500–800 ms on the critical path and makes startup require network.
2. **Messages over 4 MB fail, and the orchestrator exits 0** — gRPC's default
   receive limit is left unset; the failure is silent to any caller.
3. **Cross-runner shutdown race** — a runner that exits right after closing its
   writer can lose the close, hanging the downstream runner forever.
   Worked around here with `BenchGenerator.lingerMs`.
4. **No pipeline parallelism** — acknowledgements chain the full length of the
   pipeline, so stages run in lock-step and per-message time is the *sum* of all
   stages rather than the slowest one. Deep pipelines gain nothing from multiple
   cores. A bounded in-flight window per channel would fix it.
5. **Streaming channel slower than buffered messages** — the per-chunk await in
   `writer.ts` (already carrying a `TODO`) makes it 2.6× slower at 64 KB.

## License

MIT — see [LICENSE](LICENSE).
