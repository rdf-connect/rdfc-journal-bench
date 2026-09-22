# TODO — experiment 2

Starting point for the framework comparison. Experiment 1 is done
(`docs/benchmark-1-results.md`); the design rationale is in
`docs/benchmark-2-design.md`.

Local checkouts already in place:
`./rml-processor-jvm/` (Java, warm JVM, streaming) and
`./shacl-processor-ts/` (TS, `shacl-engine`).

---

## 0. Optional: scatter/gather width sweep

Not part of experiment 2, but the result most likely to change how the paper
reads — the first configuration where RDF-Connect should *beat* the hand-wired
baseline, because a single-process JS driver can't parallelise CPU-bound work.

- [ ] `Scatter` processor: N writers, round-robin, N-deep in-flight window
      (must be hand-rolled — awaiting each write re-serialises the pipeline)
- [ ] `Gather` processor: N readers, merge
- [ ] `width` parameter in `src/genpipeline.ts`; N workers across N runner IRIs
- [ ] Control arm: same N workers in **one** runner (expect no speedup — one event loop)
- [ ] Sweep width 1/2/4/8 at 10 ms work; find where the orchestrator saturates

Channel model constraints (already verified in the orchestrator source):
1 writer → 1 reader per channel, `addReader` throws on a second reader, no
competing-consumer semantics. Hence explicit scatter/gather.

---

## 1. Adapt the processors to CLI tools

**Rule:** wrap the *same library calls* as the processors, and do **not** keep
warm state across invocations — that warm state is the variable being measured.

- [x] `shacl-validate` CLI (start here — trivial extraction, proves the pattern)
      ```
      shacl-validate --shapes s.ttl [--mime text/turtle] [--report r.ttl] [--fatal] [--batch]
      ```
      Core is already isolated in `shacl-processor-ts/src/index.ts`:
      load shapes → `new Validator(shapes)` → per record: parse → `validate({dataset})`
      → forward if `conforms`.

- [x] `rml-map` CLI (new main class + gradle CLI task)
      ```
      rml-map --mapping m.ttl [--base IRI] [--format nquads] [--batch]
      ```
      Warm-up to do once per process: FnO `functionAgent`, parsed `rmlStores`,
      `recordsFactory`. Per record: `new Executor(...)` + `execute()`.

      Done: `shacl-processor-ts/src/cli.ts` (bin `shacl-validate`, run as
      `node lib/cli.js`; library calls shared with the processor via
      `src/core.ts`) and `rml-processor-jvm/src/main/java/org/example/RmlMap.java`
      (`gradle cliJar` → `build/libs/rml-map.jar`, launcher `bin/rml-map`).
      Interface as built:
      - both speak NDJSON: `rml-map` reads one JSON record per line; every
        output line (both tools) is a JSON string of N-Quads.
      - `rml-map` exposes stdin as a logical source named `--source` (default
        `stdin`) holding a JSON **array**: `[record]` per line when streaming,
        `[r1,…,rN]` with `--batch`. Mappings iterate `$[*]`, so one mapping
        serves both modes. Only the default target is written.
      - `shacl-validate --mime` defaults to `application/n-quads` (the processor
        defaults to `text/turtle`); pass the same mime to the rdfc arm.
      - `shacl-validate` follows the Validate processor exactly: a unit (a
        line, or all input with `--batch`) that conforms goes to stdout
        (`outgoing`); one that does not is dropped and its report goes to
        `--report` (`report`), both as NDJSON — so streaming and batch forward the
        same records (checked on a 3-record smoke test, quad sets identical).
      - mappings are parsed from a stream with no base IRI, as in the processor:
        relative IRIs like `<#x>` need an `@base`.

`--batch` = one tool execution over the whole input (one RML `execute()` over
the full source, one SHACL validation), which is how a batch workflow really
uses these tools and so is CWL's *best* case. Warm-up count is decided by how
many processes CWL starts (single task vs `scatter`), not by this flag: plain
streaming mode with the whole file on stdin also warms up once, but still does
N small executions. Streaming-whole-file vs `--batch` therefore also isolates
the tool's own per-execution cost.

- [x] Diff outputs of `--batch` vs streaming. With all records valid they
      forward the same quads (`bench/stages.ts`). With invalid records they
      differ by design: SHACL gives one verdict per validated unit, so a batch
      with one broken record is rejected whole — the reference accounts for
      this per grouping. RML batch would allow cross-record joins; the anchor
      mapping has none.

The four encodings all come from one binary:

| Arm | Invocation | Warm-up paid |
|---|---|---|
| RDF-Connect | processor in warm runner | once |
| shell pipe | CLI streaming, stdin→stdout | once |
| CWL-batch | CLI `--batch`, one task | once |
| CWL-scatter | CLI per record, N tasks | N times |

Framing: one record = one NDJSON line (a JSON string of N-Quads, default
graph). Identical cost in every arm, so it can't bias the result.

---

## 1b. Stage costs as CLI tools — done (`bench/stages.ts`)

Workload in `anchor/` (sensor observations, default graph, ~5 % invalid), generator `bench/gen-records.ts`. Results: `results/stages/stages.md`
(n = 1…10 000, 3 reps, medians).

- One cold process: rml-map ≈ 1.03 s, shacl-validate ≈ 0.28 s, whole pipe
  ≈ 1.07 s. Warm streamed record through the pipe ≈ 0.7 ms. **One process start
  ≈ 1 500 warm records** → CWL-scatter at 1 record/unit is spawn-bound by three
  orders of magnitude, before counting cwltool's own per-job overhead.
- Streaming vs batch give identical quads at every n (rml, shacl, pipe).
- Time to first output: pipe-stream flat at ~1.1 s; pipe-batch grows with n
  (8.5 s at n = 10 000). Peak RSS at n = 10 000: 0.9 GB streaming vs 1.4 GB batch.
- Per-record time in a stream *falls* over the run (rml 1.6 → 0.36 ms/record,
  JIT warm-up), so no O(n) time growth (§3). rml-map RSS does grow with n
  (186 → 629 MB), but so does batch; needs a heap measurement to tell the
  `recordsCache` leak apart from lazy GC.

## 2. Wire up the targets

- [x] **shell pipe** — `rml-map | shacl-validate | sink`. This is the anchor
      pipeline's `native`: a warm streaming process is RDF-Connect minus the
      orchestrator, so shell-vs-rdfc isolates orchestration cost exactly as
      native-vs-rdfc did in experiment 1.
- [x] **RDF-Connect** — pipeline.ttl using the JVM runner for RML + js-runner
      for SHACL (the multilingual case, which is the point)
      `rdfc` arm on the anchor sweeps: `AnchorSource`/`AnchorSink` in
      `src/anchor.ts`, pipeline from `renderAnchorPipeline`. The JVM runner is
      a local copy of the official definition that runs `vendor/jvm-runner.jar`
      instead of curling it on start. Output matches the reference exactly at
      unit = 1.
- [x] **cwltool** — `CommandLineTool` per stage, both encodings, `--no-container`.
      Done: `cwl/` (tools, `batch.cwl`, `scatter.cwl` → `chunk.cwl` subworkflow
      per chunk), arms `cwl-batch` / `cwl-scatter` in `bench/run.ts`, cwltool in
      `.venv/`. Peak intermediate disk is sampled with `du`; split/gather step
      times come from cwltool's log (1 s resolution only). Scatter runs are
      capped at 30 min, and a timed-out case skips its other reps. At n = 100,
      unit = 1: shell 1.1 s, cwl-batch 2.0 s, cwl-scatter 77 s.
      **Decided:** no separate `cwl-batch` arm in the sweeps. CWL-scatter at
      unit = all records is the batch workflow plus split + gather (measured
      ≈ 0.3 s at n = 1000: three extra trivial jobs, one extra copy of the input
      on disk). State that in the paper as "how people would write it"; every
      unit sweep must end at unit = count. `cwl/batch.cwl` and the arm stay
      available for showing the file.
      `--batch` is our CLI's flag, not CWL's. CWL never splits files: `scatter`
      runs over an array input. CWL-scatter therefore needs an explicit split
      step (`split -l 1` + `outputBinding` glob → `File[]`), the scattered
      stages, and a gather step (`cat`). Keep the split inside the measured
      workflow (RDF-Connect's source does the equivalent in-window) and report
      it as its own step. Run scatter with `--parallel` (default is sequential).
      Venv already verified working: cwltool 3.2.20260720092025 on Python 3.14.
- [x] Launchers in `bench/run.ts`; generalise `reportWork` to N targets
      Done so far: `shell` arm (`bench/anchor.ts`), sweeps `anchor-smoke`,
      `anchor-n`, `anchor-unit`; `reportWork` takes any arms against `native`;
      anchor runs are reported against `shell` and checked against a batch
      reference output (`correct` column). Records per unit = records per input
      line (`[r1,…,rk]`); both CLIs accept that, and the output does not
      depend on the grouping.
      **Decided:** every arm has Validate's two outputs, data of fully
      conforming units + one report per rejected unit, with the processor's
      rule (no finer-grained dropping). The reference is derived from which
      records the generator broke, per grouping the arm validates with
      (`cwl-batch` = everything at once), and checked exactly for every arm:
      data quads and report count. `anchor-check` runs it on all four arms.
- [x] Second and third CWL runner, same `cwl/scatter.cwl`:
      `toil-scatter` (Toil 9.5.0, `.venv-toil`, Python 3.14) and
      `streamflow-scatter` (StreamFlow 0.2.0rc3, `.venv-streamflow`, Python 3.12;
      the only stable release, 0.1.6, is years old and does not run on 3.13+).
      All three pass `anchor-check`. At n = 100 (1 rep): unit 1 cwltool 73 s,
      Toil 152 s, StreamFlow 75 s; unit 100 (one task) 2.7 / 19.0 / 2.4 s.
      Portability notes for the paper — the same valid CWL needed two changes
      for StreamFlow, both harmless for the others:
      - StreamFlow wrote the tool's stderr into the stdout file → `stderr:` is
        now named explicitly in both tools.
      - StreamFlow stages a gathered `File[]` into one directory, so chunk
        outputs with the same basename overwrote each other → per-chunk
        outputs are named after the chunk.
      Also: Toil needs `--retryCount 0` (it silently retries failed jobs with
      more memory), StreamFlow ignores TMPDIR (workdir set via a generated
      `streamflow.yml`), and output paths are read from each runner's result
      JSON because their `--outdir` layouts differ.
- [ ] (optional) Nextflow — not CWL; would need its own pipeline definition

**Knob that matters:** records per unit = 1 / 10 / 100 / 1000 / all. At 1 it's
streaming (RDF-Connect's warm JVM vs CWL spawning a JVM per record); at "all"
it's CWL-batch and CWL should win. **The crossover is the headline number** —
and it's immune to "you picked the pipeline", because it's the swept parameter.

Measured window excludes YARRRML→RML (pre-generate it) and the SPARQL INSERT
(external stateful service — serialize to file instead; run it separately if
wanted).

---

## 3. Observations to record along the way

Not blockers — note the behaviour, state it in the paper, fix later.

- [ ] **Is `executeOnce()` O(1) or O(n) per record?** Each trigger builds a new
      `Executor` and runs `execute()`. If `CacheReader`/`MyRecordsFactory`
      accumulate and re-map everything per trigger, per-record cost grows with
      n. Just check per-record time is flat at n = 10/100/1000 and report what
      it is.
      Lead: `RecordsFactory.recordsCache` is keyed by `Access`, and
      `ReaderAccess` has no `equals`/`hashCode`, so every execution adds a cache
      entry that is never hit or evicted — expect memory O(n), time probably flat.
- [ ] **The >5 MB stream path.** `App.java` uses
      `DEFAULT_TARGET_STREAM_THRESHOLD_BYTES = 5 MB`: below it `writer.chunk()`,
      above it `writer.stream()` in 1 MB chunks. Experiment 1 measured the
      stream channel as **2.6× slower than buffered messages at 64 KB** (the
      per-chunk `await` in `writer.ts`, which already carries a `TODO`). Check
      whether crossing 5 MB shows a visible step in the anchor results — if so,
      that TODO is a measurable win on a real pipeline, not a microbenchmark
      curiosity.
- [ ] `shacl-processor-ts` reads via `incoming.streams()`, so the SHACL stage is
      on the slow channel for *every* message regardless of size.

---

## Decisions already made

- SHACL engine: `shacl-engine` via `shacl-processor-ts`, same code in every arm
  (settled by using this repo — no need to bring in Jena).
- RML: the JVM processor, not `RMLMapperJS`. The JS one stages to temp files and
  spawns a JVM per execution cycle, which would make RDF-Connect structurally
  identical to CWL at that stage and measure nothing.
- Target set: `native` + `rdfc` + shell + CWL is the minimum defensible set.
  Nextflow/Toil optional; Beam dropped (job-server setup not worth it).

## Open framework issues from experiment 1

See README "Issues found while benchmarking" — remote ontology fetch on every
start, 4 MB message ceiling failing with exit code 0, cross-runner shutdown
race, no pipeline parallelism, slow stream channel.
