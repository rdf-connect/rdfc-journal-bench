# Experiment 2 — status, gaps, and proposed next steps

For discussion. It summarises what the benchmark can do today, what a
journal reviewer would still object to, and a proposal for a realistic
pipeline and dataset to replace the current two-stage one. Open decisions are
collected at the end.

Related: `docs/benchmark-2-design.md` (methodology), `docs/section-6-draft.tex`
(draft of the paper section), `TODO.md` (task list).

---

## 1. Where we are

**Pipeline (B1 today):** synthetic sensor-observation records → RML mapping
(JVM, `rml-processor-jvm`) → SHACL validation (TS, `shacl-processor-ts`) →
file. Two stages, records of ~250 B, up to 10⁴ records.

**Systems compared**, all running the same executables:

| Arm | What it is |
|---|---|
| `rdfc` | orchestrator + JVM runner + Node.js runner |
| `shell` | `rml-map \| shacl-validate` — RDF-Connect minus the orchestrator |
| `cwl-scatter` | cwltool: split → map/validate per chunk (sub-workflow) → gather |
| `toil-scatter` | the same `scatter.cwl` on Toil 9.5.0 |
| `streamflow-scatter` | the same `scatter.cwl` on StreamFlow 0.2.0rc3 |
| in-process driver | experiment 1's zero-overhead reference (synthetic pipeline only) |

**Swept parameter:** records per unit — one message (RDF-Connect), one line
(shell), one task (CWL). From 1 (streaming) to all records (batch).

**Methodological choices already settled:**

- Same executables in every arm: the RDF-Connect processors, and CLIs built
  from the same library calls (`rml-map`, `shacl-validate`).
- Same semantics in every arm: the SHACL stage has the Validate processor's
  two outputs (data of fully conforming units, one report per rejected unit)
  and its forwarding rule. No finer-grained dropping invented by the benchmark.
- Correctness is checked on every run against a reference derived from which
  records the generator broke, not from any arm. All arms pass.
- No separate CWL-batch arm: CWL-scatter at "all records per unit" is the
  batch workflow plus split and gather (~0.3 s measured).
- Three CWL runners, so that results are not attributable to one
  implementation.

**Indicative results** (n = 1 000, all records valid, tmpfs, 1 rep, laptop;
not publishable as is):

| Records per unit | shell | rdfc | cwltool | StreamFlow |
|---:|---:|---:|---:|---:|
| 1 | 2.0 s | 10.6 s | 738 s | failed at gather (739 s) |
| 10 | 1.8 s | 6.9 s | 74 s | 78 s |
| 100 | 1.8 s | 5.7 s | 10.4 s | 10.1 s |
| 1 000 | 1.9 s | 5.3 s | 3.4 s | 3.0 s |

- One cold process ≈ 1 500 streamed records of work.
- CWL-scatter pays ~0.75 s per task (cwltool, StreamFlow), ~1.5 s (Toil).
- RDF-Connect: ~4–5 s fixed start-up, ~6 ms per message at one record per
  unit (shell pipe: ~1.3 ms).
- Orchestrator overhead (experiment 1): 0.23 ms/message; minimum effective task
  granularity (50 % overhead) 0.42 ms of work per message.

## 2. What a reviewer would still object to

Ranked by how much it undermines the paper.

1. **The main design goal is not tested.** "Streaming by default" is the
   framework's first claim, but only bounded batch runs are measured, where
   RDF-Connect loses to the shell pipe everywhere and to CWL at large units.
   Latency under a continuous source (B2) is not built.
2. **The claimed advantages are not visible at this scale.** RDF-Connect uses
   *more* memory (1.2 GB vs 0.4 GB shell), intermediate storage is a few MB,
   and time to first output (4–6 s) is worse than CWL's makespan on small
   inputs. The separation only appears at 10⁵+ records.
3. **The benefit-side metrics are missing.** Definition size and change effort
   (Table 5 of the paper) are where declarative composition should win, and
   they are cheap to measure. Without them the section measures only cost.
4. **Parallelism confounds makespan.** CWL-scatter runs up to 8 tasks at
   once; RDF-Connect processes one message at a time per channel. CPU time
   and cores used must be reported (not collected yet).
5. **The workload is narrow:** one small mapping, two stages, tiny synthetic
   records. No record-size or depth sweep. → §3 below.
6. **Setup is not publication-grade:** 1 rep, a laptop with CPU frequency
   scaling, and unpinned versions (RMLMapper and JVM runner from JitPack
   `master-SNAPSHOT`).
7. **Known framework limitations should be disclosed:** the 4 MB message
   ceiling failing with exit code 0, the cross-runner shutdown race, the
   stream channel being 2.6× slower than buffered messages (which the SHACL
   stage uses for every message).

## 3. Proposal: a realistic pipeline

### 3.1 The pipeline

The public RDF-Connect pipelines in the paper's Table 4 share one skeleton
(`eu-op-vocab-feed`, `belgium-dcat-ap-feed`, `eurobis-geo-tree`, MuMo, the
MareGraph mirrors):

```
harvest ─▶ lift ─▶ (change detection) ─▶ Sdsify ─▶ Bucketize ─▶ publish
```

Proposed benchmark pipeline — that skeleton plus the quality gate, built only
from published processors:

| # | Stage | Processor | Runtime | KE task (paper §5.5) |
|---|---|---|---|---|
| 1 | Harvest + decompress | `GlobRead` + `GunzipFile` | JS | harvesting |
| 2 | Semantic lifting | `RmlMapper` | JVM | semantic lifting |
| 3 | Quality gate | `Validate` | JS | quality gate |
| 4 | Change detection | `DumpsToFeed` | JS | change management |
| 5 | Stream annotation | `Sdsify` | JS | — |
| 6 | Fragmentation | `Bucketize` (time-based) | JS | fragmentation |
| 7 | Publication | `LdesDiskWriter` (static LDES) | JS | publication |
| (8) | Materialisation | `SPARQLIngest` → Oxigraph | JS | materialisation |

Seven stages over two runtimes, covering five of the six KE tasks. Stage 8 is
an external stateful service; proposed as a variant outside the measured
window.

### 3.2 The data: Blue-bike (decided 2026-09-23)

The source of the deployed `CA-Blue-Bike-LDES` pipeline (Table 4):
`https://api.blue-bike.be/pub/location`, ~330 Belgian stations per snapshot,
each with `bikes_available` / `bikes_in_use` and a `last_seen` timestamp.

- **The mapping is the deployed one.** `anchor/bluebike.rml.ttl` is that
  pipeline's `bluebike.rml.ttl` (9 triples maps, GBFS/mobivoc/schema
  vocabularies, FnO functions) with one line changed: the WoT/HTTP source
  becomes the pipeline channel. One snapshot maps to ~2 030 quads in 1.7 s,
  FnO functions included, so the mapping stage is a real one, not ours.
- **The archive.** `bench/record-bluebike.ts` polls once a minute (the
  deployment polls every 15 s) and appends one snapshot per NDJSON line:
  ~54 KB, ~330 records. A day is ~470 k records and ~78 MB, so 10³–10⁵ records
  come from a day, 10⁶ from a few days.
- **Change rate (measured).** ~3.6 of 327 stations change per minute, so
  change detection sees a low, realistic event rate: B2 measures latency on it
  and replays the archive at speed for throughput. B1 therefore replays
  snapshots rather than changes.
- **Only 70 of 327 stations carry a `last_seen`**, and the deployed mapping
  keys reports on it, so a snapshot yields ~70 members, ~100 k/day rather than
  the ~470 k a naive count suggests. Timestamps span 2023–2025, so time-based
  fragmentation produces year buckets.
- **Open: reaching 10⁷.** A recorded archive of that size needs ~3 weeks. If
  the sweep must reach 10⁷, supplement with bulk trip dumps (Bluebikes Boston
  or Citi Bike, monthly CSVs of millions of trips), at the cost of a second
  operator and a CSV mapping.

- **B1 (bulk): Citi Bike trip histories.** Public monthly CSV dumps of a few
  million trips each. A day, a week, a month, or several months give 10³ to
  10⁷ real records. CSV means the lifting uses RML's CSV reference formulation
  rather than our synthetic JSON. Trips are immutable events, so B1 skips
  stage 4.
- **B2 (continuous): GBFS `station_status` snapshots.** ~2 000 stations,
  republished every minute: snapshot → changes is exactly what `DumpsToFeed`
  is for, so B2 exercises all seven stages. For repeatable runs, record one day
  of snapshots once (~1 440 files) and replay it at real or accelerated speed.
  Metric: per-change latency from snapshot arrival to published LDES member.
  CWL runs it as re-triggered micro-batches at 2–3 intervals.

**Status (2026-09-23): the seven-stage chain runs end to end** on recorded
snapshots, every stage a published processor driven by `rdfc-proc`:

```
snapshots → rml-map → Validate → DumpsToFeed → Sdsify → Bucketize → LdesDiskWriter
             (JVM)     (JS)        (JS)          (JS)     (JS)        (JS)
```

5 snapshots → 5 mapped dumps → 70 feed activities → 70 SDS members → a static
LDES on disk, in ~5 s (six process starts). Step descriptions are in `steps/`,
shapes in `anchor/bluebike-shapes.ttl`.

Configuration notes worth keeping: `DumpsToFeed`'s "extract" strategy only
finds DCAT-AP types, so members are selected with a SPARQL query on its
`focusNodes` channel; `Sdsify` and `Bucketize` therefore key on
`as:published`, as the deployed DCAT-AP feeds do.

**Encodings.** All three run the chain and publish the *identical* 70 members
(5 snapshots, 1 rep, indicative only):

| Arm | Wall | Notes |
|---|---:|---|
| shell pipe | 2.8 s | six stages in one pipe; metadata channels are FIFOs, so stages overlap |
| cwl-batch | 6.4 s | `cwl/bluebike-batch.cwl`, one task per stage |
| RDF-Connect | 9.2 s | `src/genbluebike.ts`; JVM runner for the mapper, js-runner for the other five |
| cwl-scatter (1 snapshot/task) | 11.7 s | `cwl/bluebike-scatter.cwl` |
| sequential CLIs | 5.0 s | the same stages one after another, for reference |

**Sweep on the deep pipeline** (`results/bluebike.json`, 2 reps, tmpfs,
medians, quiet logs, CPU from cgroup; 32/32 runs published the expected
members):

| snapshots | arm | wall | CPU | cores | disk |
|---:|---|---:|---:|---:|---:|
| 5 | shell | 2.4 s | 11.7 s | 5.0 | – |
| 5 | rdfc | 7.8 s | 19.6 s | 2.5 | – |
| 5 | cwl-batch | 5.9 s | 12.4 s | 2.1 | 4.0 MB |
| 5 | cwl-scatter | 9.6 s | 51.1 s | 5.3 | 5.9 MB |
| 30 | shell | 6.4 s | 30.1 s | 4.8 | – |
| 30 | rdfc | 11.4 s | 29.4 s | 2.6 | – |
| 30 | cwl-batch | 9.8 s | 21.8 s | 2.3 | 20.6 MB |
| 30 | cwl-scatter | 48.1 s | 319.8 s | 6.9 | 32.0 MB |

- **RDF-Connect and the shell pipe cost the same CPU** (29.4 vs 30.1 s at
  n=30). RDF-Connect's longer makespan comes from using 2.6 cores against 4.8:
  a channel admits one message at a time, so less of the pipeline runs at once.
  That points at the known "no pipeline parallelism" limitation, not at gRPC.
- **CWL-scatter burns 15× the CPU of CWL-batch** for identical output.
- **Intermediate storage** still grows linearly for CWL (4.0 → 20.6 MB) and is
  zero for the streaming arms.
- **Note on the workload:** the published member set stays 70 across 30
  snapshots (a member IRI is keyed on `last_seen`; only `bikes_available`
  changes minute to minute). Report per-snapshot cost, not cost per member.

**The crossover** (n = 30, 1 rep): CWL-scatter by snapshots per task, against
RDF-Connect at 11.4 s wall / 29.4 s CPU and the shell pipe at 6.4 s / 30.1 s:

| snapshots/task | 1 | 5 | 15 | 30 (all) |
|---|---:|---:|---:|---:|
| wall | 45.4 s | 15.8 s | 9.8 s | 9.1 s |
| CPU | 310.9 s | 94.4 s | 45.2 s | 21.0 s |
| cores | 6.8 | 6.0 | 4.6 | 2.3 |

Granularity costs CWL 15× the CPU (310.9 vs 21.0 s). RDF-Connect works at one
snapshot per message throughout for 29.4 s of CPU: 1.4× CWL's cheapest run,
and a tenth of what CWL spends to match its granularity.

**Definition size** (non-comment lines; `steps/*.ttl` holds the per-stage
configuration the CLI arms need, which the RDF-Connect pipeline carries inline):

| Arm | composition | stage configuration | total |
|---|---:|---:|---:|
| RDF-Connect | 110 (one `pipeline.ttl`, 120 triples) | included | **110** |
| shell | 15 (generator code) | 85 | 100 |
| CWL | 201 (6 tools + workflow) | 85 | **286** |

CWL needs 2.6× the description for the same pipeline: every stage needs a tool
description as well as its configuration, and the stateful stages need their
state wired explicitly through inputs and outputs. The shell pipe is shortest
and offers no validation, typing or provenance at all, which is the honest
framing: definition size alone is not the argument, but it is the metric
Table~5 promises and it favours the declarative description.

**Change effort** (adding one published processor to the pipeline, artefacts and
method in `docs/change-effort/`):

| Arm | diff | files |
|---|---:|---:|
| RDF-Connect | 10 lines in `pipeline.ttl` | 1 |
| shell | 1 line in the pipe + 9-line step description | 2 |
| CWL | 7 lines in the workflow + a 15-line tool + 9-line step description | 3 |

Verified rather than counted: the modified RDF-Connect pipeline runs and still
publishes exactly the expected members; the modified CWL workflow validates.

**Capability finding — stateful stages cannot be scattered.** Change detection
keeps the previous state of every member, the bucketiser keeps its fragment
state, and the writer appends to a published tree. CWL scatter runs its tasks
independently and cannot thread a value from one to the next (there is no
fold), so only the stateless prefix (map, validate) scatters and the stateful
tail runs as single tasks over the gathered stream. In a streaming system those
stages simply keep their state. This is a structural limit, not a speed
difference, and belongs next to "unbounded input is inexpressible" in the
capability table.

**Cost of making state explicit.** In CWL each stateful stage stages its
predecessor's state in (a LevelDB directory, a state file, the published LDES
tree) and hands it on as an output, so state is copied between tasks. That is
worth measuring as intermediate storage as n grows.

**New finding — log relaying is not free.** The mapper warns about every
station without a `last_seen` (257 of 327). The shell arm sends that to
/dev/null; RDF-Connect relays runner stderr to the orchestrator, which logged
**13 023 lines for 5 snapshots**. At a day of snapshots that is millions of
lines through gRPC, and it is inside the measured window. Either report it as
a cost of central log aggregation, or quiet the mapper in every arm equally
and report it separately. Decide before the timed runs.

### 3.3 What has to be built

1. **A generic processor-to-CLI adapter (`rdfc-proc`).** ✅ built
   (`bench/rdfc-proc.ts`). A step is described in Turtle, like a pipeline, and
   its arguments are materialised with rdf-lens through the processor's own
   SHACL shapes, as js-runner does — which is what makes arguments such as a
   bucketiser's fragmentation strategy (an RDF subgraph, not a value) work.
   Channels bind to stdin/stdout/files; step descriptions are in `steps/`. CWL and the shell pipe
   need a command-line tool per stage. Instead of one wrapper per processor:
   run any JS processor as a CLI, with stdin/stdout as its reader/writer, its
   configuration from a JSON file, and experiment 1's in-memory channels
   (`src/inmem.ts`) in between. Keeps the same-executables rule by
   construction, and is reusable by anyone comparing against RDF-Connect.
   (`rml-map` already covers the JVM mapper.) Verified: running `Validate`
   through it produces byte-identical output to the hand-written
   `shacl-validate` CLI, which also confirms that CLI mirrors the processor.
   The Blue-bike chain (map → Sdsify → Bucketize → LdesDiskWriter) runs end to
   end through it: 3 snapshots → 210 SDS members → a static LDES on disk with
   time-based fragments.

   Two things it had to handle, both worth a line in the paper as costs of
   wrapping processors as tools:
   - `Bucketize` logs to **stdout**, which corrupts a stdout data channel; the
     adapter captures the real stdout for data and diverts the processor's own
     writes to stderr.
   - `rml-map` reads raw JSON records while every later stage reads NDJSON
     lines that are JSON strings; the framing changes at the mapping stage.
2. **State handling for stateful stages.** `Bucketize` keeps fragment state
   across messages; `DumpsToFeed` keeps the previous snapshot. In a stream this
   just works. In CWL-scatter, parallel chunks cannot share state, so those
   stages must serialise or pass state files from task to task. Proposal: make
   the state an explicit file input/output of those CWL tools and report what
   it costs. This is a real finding — file-based batch systems struggle with
   stateful incremental KE steps — as long as the output stays identical across
   systems.
3. **Mappings and shapes** for trips (CSV) and GBFS (JSON).
4. **A correctness reference for real data.** The generator trick no longer
   works; use a canonicalised batch output, cross-check the systems against
   each other, plus spot checks.
5. **Non-deterministic output.** `DumpsToFeed` stamps each activity with
   `as:published` = now, and `Sdsify` adds a transaction id with a timestamp, so the deep pipeline's output is not
   byte-identical across runs. The correctness check has to canonicalise those
   away (or compare modulo the SDS bookkeeping quads).
6. **Wide gathers.** StreamFlow runs a step as `sh -c "<command>"`, one
   argument, so a gather over ~1 000 chunk files exceeds Linux's 128 KB
   per-argument limit and fails after all tasks have run (n = 1 000, unit = 1).
   A deeper pipeline gathers at every stateful stage, so either gather through
   a manifest file (changes the CWL for every runner) or report the limit as
   a finding. cwltool passes arguments as a list and is not affected.
7. **Data recording** for B2 (one day of GBFS snapshots, published with the
   benchmark).

Rough effort: a few days, of which the adapter and the CWL state handling are
the bulk.

### 3.4 What it fixes

| Gap (§2) | Addressed by |
|---|---|
| 1 streaming untested | B2 on a real, continuous source |
| 2 advantages invisible | 10⁵–10⁷ real records; deep pipeline materialises 6 intermediates in CWL |
| 5 narrow workload | 7 stages, real CSV/JSON, larger records |
| — per-hop cost | depth makes the orchestrator's per-hop cost measurable |

Items 3, 4, 6 and 7 are independent of the pipeline choice and should be done
regardless (§4).

## 4. Independent of the pipeline choice

- **Change effort and definition size.** Lines/triples of `pipeline.ttl` vs
  the CWL files plus wrappers; the diff needed to swap one implementation (e.g.
  the TS SHACL validator for the JVM one): one declaration in RDF-Connect, a
  new tool description in CWL. Cheap, and it carries the paper's thesis.
- **Measurement setup.** A server with a pinned CPU governor; ≥ 5 interleaved
  repetitions, medians with IQR; CPU time and cores used per run; pinned
  versions (commit hashes) of every component; tmpfs for run directories
  (switch exists: `BENCH_RUNS=/dev/shm/...`).
- **Parallelism.** Report CWL both unrestricted and capped to RDF-Connect's
  core usage; the scatter/gather width sweep (TODO §0) is RDF-Connect's own
  answer and would strengthen the comparison.
- **Invalid data.** Timing sweeps with all-valid data (otherwise large units
  are rejected whole and forward nothing); correctness runs with ~5 % broken
  records.
- **Disclose limitations** from experiment 1 in the paper's Limitations.
- **Paper updates.** Table 1 and §6.1 still say "CWL (cwltool)" only; B3 would
  fold into B1 (already JVM + JS); the threats to validity in §6 and §7 should
  be merged.

## 5. Open decisions

1. **Domain.** Bike sharing (Citi Bike trips + GBFS), or a domain closer to an
   existing pipeline, e.g. DCAT-AP catalogs (`belgium-dcat-ap-feed`) — real
   dumps, but smaller volumes?
2. **Python.** Add a Python enrichment step (the outline's B3)? Only template
   Python processors exist, so we would write one.
3. **SPARQL materialisation** inside or outside the measured window?
4. **B2 for CWL.** Measure micro-batch emulation, or report unbounded input as
   a capability boundary only?
5. **Scale.** 10³–10⁷ for the streaming systems; CWL-scatter at one record per
   unit is infeasible beyond ~10⁴ (30 min cap). Accept and report as such?
6. **Hardware.** Which server, and who pins the setup?
7. **Scope for this paper.** All of §3, or the realistic B1 now and B2 as
   future work?
