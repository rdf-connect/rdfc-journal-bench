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

Comparison with CWL and other pipeline frameworks. The design addresses the
central difficulty head-on: RDF-Connect is streaming and CWL is batch, so
*choosing a pipeline chooses the winner*. The answer is to parameterise the
workload and report a surface rather than a point, following Task Bench
(Slaughter et al., SC'20).

Experiment 1 is the same benchmark with the target set restricted to
`{rdfc, native}`: the hand-wired driver is the **zero-overhead reference** that
every other framework's efficiency is measured against, and the `work` sweep is
already the METG measurement. Extending to CWL, Nextflow and a shell pipeline
means adding targets behind the existing `Workload` parameterisation, not
building a second harness.

- Design: **[docs/benchmark-2-design.md](docs/benchmark-2-design.md)**

## Next steps

See **[TODO.md](TODO.md)** — the experiment 2 work plan.

## Layout

```
src/processors.ts    BenchGenerator / BenchPassThrough / BenchSink — shared by both arms
src/inmem.ts         in-memory Reader/Writer with the framework's exact ack semantics
src/native.ts        hand-wired driver (arm B), replicating Runner's lifecycle
src/genpipeline.ts   emits pipeline.ttl for arm A (and for the anchor pipeline)
src/anchor.ts        AnchorSource / AnchorSink — ends of the experiment-2 rdfc pipelines
src/genbluebike.ts   emits pipeline.ttl for the realistic (Blue-bike) pipeline
steps/               per-stage processor descriptions, shared by the shell and CWL arms
cwl/                 CWL tools and workflows for both pipelines
processors.ttl       processor descriptions + SHACL shapes + runner definitions
anchor/              experiment-2 workload: RML mapping + SHACL shapes
bench/run.ts         sweep harness
bench/anchor.ts      experiment-2 inputs, shell command, reference output
bench/gen-records.ts experiment-2 record generator
bench/stages.ts      cost of each experiment-2 stage as a CLI tool
bench/proc.ts        run a shell pipeline: wall clock, line timestamps, peak RSS
bench/rdfc-proc.ts   run any JS processor as a CLI, configured from RDF
bench/bluebike.ts    the realistic workload: archive, arms, correctness oracle
bench/run-bluebike.ts  sweep for the realistic pipeline
bench/record-bluebike.ts  records the Blue-bike API into a replayable archive
bench/summarize.ts   raw JSON -> paper-ready tables
bench/all.sh         full suite
```

## Running

```bash
npm install
npm run build
./bench/all.sh
node dist/bench/summarize.js
```

Individual sweeps:

```bash
node dist/bench/run.js <sweep> --reps=3 --arms=rdfc,native
# sweeps: startup | rate | depth | payload | stream | smoke
# arms:   rdfc | rdfc-direct | rdfc-split | native | native-loose
```

Experiment 2 needs the two processor checkouts built as CLIs, and the JVM
runner in `vendor/` (the official runner definition would `curl` it on every
start). The checkouts are git submodules, pinned to their `cli-tools` branch:

```bash
git clone --recurse-submodules <this repo>   # or, in a clone: git submodule update --init
(cd rml-processor-jvm && gradle shadowJar cliJar)
(cd shacl-processor-ts && npm ci && npm run build)
mkdir -p vendor
J=https://javadoc.jitpack.io/com/github/rdf-connect/jvm-runner/runner/master-SNAPSHOT
curl -L -o vendor/jvm-runner.jar $J/runner-master-SNAPSHOT-all.jar
curl -L -o vendor/jvm-runner-index.jar $J/runner-master-SNAPSHOT-index.jar

# the realistic pipeline (Blue-bike: map, validate, change detection, SDS,
# fragmentation, LDES publication), all four arms:
node dist/bench/record-bluebike.js --out data/bluebike-$(date +%F).ndjson --for=1h
node dist/bench/run-bluebike.js --ns=5,10,20,30 --reps=2
node dist/bench/run-bluebike.js --ns=30 --reps=1 --arms=cwl-scatter --unit=5

# the two-stage anchor pipeline (calibration and ground truth):
node dist/bench/run.js anchor-unit --reps=3 --arms=shell,rdfc,cwl-scatter
# sweeps: anchor-smoke | anchor-check | anchor-quick | anchor-n | anchor-unit
# arms:   shell | rdfc | cwl-scatter | toil-scatter | streamflow-scatter
#         (| cwl-batch: equals cwl-scatter at unit = n)

The CWL runners each live in their own venv:

```bash
python3 -m venv .venv && .venv/bin/pip install cwltool
uv venv -p 3.14 .venv-toil && uv pip install -p .venv-toil "toil[cwl]"
uv venv -p 3.12 .venv-streamflow && \
  uv pip install -p .venv-streamflow --prerelease=allow "streamflow==0.2.0rc3"
```
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
