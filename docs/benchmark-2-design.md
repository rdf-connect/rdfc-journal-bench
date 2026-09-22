# Experiment 2 — RDF-Connect against other pipeline frameworks

## The problem with the obvious approach

The instinct is to pick one pipeline, express it in RDF-Connect and in CWL, and
report which finishes first. That number would be close to meaningless, and the
reason is worth stating precisely because it is the central methodological
issue of this experiment:

**RDF-Connect and CWL are optimised for different points in the workload space,
so the choice of pipeline picks the winner before any code runs.**

- CWL is a *batch DAG over files*. A step is a command-line tool; it starts when
  all its inputs exist as complete files and it ends before its consumer starts.
  Its per-item cost is zero — there are no items, only files — but its per-step
  cost is a process spawn, and its intermediate state is materialised on disk.
- RDF-Connect is *streaming over long-lived processors*. Its per-step cost is
  paid once at startup; its per-item cost is a message round trip through the
  orchestrator; stages overlap, and intermediate state is never materialised.

So: give both a single 10 GB file and one transformation, and CWL wins — it
spawns one process while RDF-Connect pays orchestration cost for nothing. Give
both a million small records through six stages, and RDF-Connect wins by
orders of magnitude — CWL either spawns a million processes or gives up
streaming entirely. Either result is an artefact of the chosen pipeline.

## What to do instead

Three commitments, each addressing one way the comparison can be rigged.

### 1. Parameterise the workload; report a surface, not a point

Follow the approach of Task Bench (Slaughter et al., SC'20): rather than
selecting a benchmark application, define a workload with explicit knobs and
report performance across the grid. The reader can then locate their own
workload on the surface instead of trusting ours.

Knobs:

| Symbol | Meaning | Range |
|---|---|---|
| `n` | number of data items | 1 … 10^6 |
| `s` | bytes per item | 10 B … 10 MB |
| `d` | pipeline depth (stages) | 1 … 8 |
| `w` | per-item compute, µs of real work | 0 … 10^5 |

`w` is the important one. When `w` is large the frameworks converge, because
real computation dominates and orchestration is noise. When `w` is small they
separate, because all that is left is overhead. Sweeping `w` turns "which is
faster" into "at what task granularity does each framework stop being the
bottleneck" — a question with a stable, pipeline-independent answer.

Headline metric, borrowed from Task Bench:

> **METG (minimum effective task granularity)**: the smallest per-item work `w`
> at which a framework still sustains ≥ 50 % of the efficiency of the
> no-framework baseline.

METG is one number per framework, it is not a function of a pipeline we chose,
and it is exactly the number a practitioner needs in order to decide whether a
framework suits their data granularity.

### 2. Hold the computation constant; vary only the connective tissue

Every framework must invoke **the same executables** doing **the same work**.
For the realistic anchor pipeline that means the identical RMLMapper JAR and
the identical SHACL validator in both arms — wrapped as an RDF-Connect
processor on one side and as a CWL `CommandLineTool` on the other.

Anything measured after that is orchestration, which is what we claim to be
comparing. Without this rule the experiment silently compares a Java SHACL
validator to a JavaScript one and reports the difference as a framework result.

One consequence must be reported rather than hidden: RDF-Connect starts a
runtime **once** and keeps it alive, while CWL starts the tool **per task**. For
a JVM tool that is seconds of JVM startup on every invocation. That is a real
architectural difference, not a measurement artefact — but it only bites CWL
when the data is fine-grained, which is precisely what the `n`/`w` sweep makes
visible instead of asserting.

### 3. Separate capability from performance

Where a framework structurally cannot express a workload, say so and stop; do
not manufacture a number. An unbounded stream — the pipeline runs forever and
emits results continuously — is not slow in CWL, it is *inexpressible*, because
a CWL step cannot produce output before its input file is complete. Report that
as a capability boundary in a table, next to the quantitative results.

## Systems to compare

| System | Paradigm | Multi-language | Streaming | Declarative | Role in the experiment |
|---|---|---|---|---|---|
| **Hand-wired driver** | in-process calls | no | yes | no | **zero-overhead reference** — the efficiency denominator |
| Unix shell pipeline | streaming | yes | yes | no | cross-process floor |
| RDF-Connect | streaming messages | yes | yes | yes (RDF) | subject |
| CWL + cwltool | batch DAG, files | yes | no | yes (YAML) | the standard; primary comparison |
| CWL + Toil or StreamFlow | batch DAG, files | yes | no | yes | guards against blaming cwltool's speed on CWL |
| Nextflow | dataflow channels | yes | partial | DSL | closest paradigm match to RDF-Connect |
| Apache Beam | dataflow | yes (verbose) | yes | no (code) | optional; the only true streaming + multilingual rival |

Three inclusions deserve justification.

**The hand-wired driver from experiment 1 belongs here as a target, not just as
a separate experiment.** Task Bench's design is precisely that every system is
an implementation of one parameterised workload, and METG is defined relative to
a no-framework reference. Experiment 1 already built that reference: the same
processor classes driven directly with in-memory channels. Treating it as one
more target in this grid means

- efficiency for *every* system is measured against the same denominator, so
  the numbers are directly comparable rather than pairwise;
- experiment 1's work sweep is no longer a separate methodology bolted on — it
  is this benchmark with the target set restricted to `{rdfc, native}`;
- the reference is not itself a framework with its own quirks, so it cannot
  flatter or penalise any particular competitor.

It measures the floor that no distributed design can reach (it is in-process and
zero-copy), which is exactly what a denominator should be. The shell pipeline
then gives the realistic floor for anything that does cross a process boundary.

**The shell pipeline (`a | b | c`) is the most valuable *realistic* baseline.**
It is streaming, it is multilingual, it has essentially no framework overhead,
and it is exactly the "ad-hoc scripting" the RDF-Connect papers argue against.
Measuring against it answers the question a sceptical reviewer will actually
ask: *what does the declarative, reusable, validated pipeline description
cost me over just piping the programs together?* Experiment 1 establishes the
in-process floor; the shell pipeline is the cross-process floor.

**A second CWL runner is not optional.** cwltool is a Python reference
implementation. Without a second runner, any CWL result is open to the
objection that we benchmarked an implementation rather than a paradigm.

### Relationship to experiment 1

Experiment 1 is this experiment with two targets and the realistic-anchor half
removed. Its `work` sweep already reports per-message overhead as a function of
per-message compute for `{rdfc, native}`, and yields RDF-Connect's METG. The
harness in `bench/run.ts` therefore wants generalising rather than replacing:
add a target per framework behind the same `Workload` parameterisation
(`n`, `s`, `d`, `w`), and the existing sweeps, result schema and summariser all
carry over. Only the per-target launcher is new.

## Two CWL encodings, reported separately

A subtlety that must not be glossed over: "the same pipeline in CWL" is
ambiguous, and the ambiguity is worth several orders of magnitude.

- **CWL-batch** — one task per stage, processing the whole dataset. CWL's
  natural idiom and its best case. Not streaming: stage *k+1* cannot start
  until stage *k* has fully finished.
- **CWL-scatter** — `scatter` over the `n` items, one task per item. This is
  CWL contorted into a streaming shape. It preserves per-item granularity and
  pays a process spawn for every item.

Report both. RDF-Connect should land near CWL-batch in cost while retaining
CWL-scatter's granularity — that is the actual claim the framework makes, and
this encoding split is what tests it.

## Metrics

| Metric | Why it matters |
|---|---|
| Throughput (items/s, steady state) | headline performance |
| Time to first result | streaming's structural advantage; CWL cannot emit before stage 1 ends |
| **Peak RSS + peak intermediate disk** | structural, near-implementation-independent |
| Total wall time | what a user experiences |
| METG | pipeline-independent summary |

The memory/disk metric is the most robust result available here and is worth
foregrounding. A file-passing batch system must materialise every intermediate
result: its footprint grows as **O(n)**. A streaming system's footprint is
**O(1)** in `n`. That separation follows from the architecture, so it cannot be
explained away as cwltool being a slow implementation, and it does not depend on
constant factors at all. It is the one result no reviewer can attribute to
tuning.

## The realistic anchor pipeline

Alongside the synthetic sweep, run one recognisable pipeline: the knowledge
graph construction case from the RDF-Connect papers —

```
source records → YARRRML→RML → RML mapping → SHACL validation → serialize / SPARQL INSERT
```

It is a fair choice because it is genuinely in *both* frameworks' wheelhouse:
file-oriented ETL of this shape is CWL's home territory in bioinformatics, and
it is the use case RDF-Connect was built for. Same JARs and libraries on both
sides, per rule 2.

Feed it at several granularities (one dump vs. a stream of records) to place
the realistic pipeline onto the synthetic surface, rather than treating it as a
separate result.

## Threats to validity — state these in the paper

1. **Implementation maturity is not paradigm.** Mitigated by a second CWL runner.
2. **Containers.** cwltool defaults to Docker per task. Run `--no-container`
   for the core comparison and report containerised numbers separately; do not
   silently charge CWL for image startup.
3. **Author bias.** We wrote RDF-Connect. Mitigations: fix and publish the
   parameter grid *before* running; publish all raw measurements; and
   explicitly report the regions where RDF-Connect loses.
4. **Filesystem caching** favours the file-passing systems on repeat runs and
   penalises them on cold ones. Pin it: run on tmpfs, or drop caches between
   repetitions, and say which.
5. **Startup amortisation.** RDF-Connect's ~1.7 s fixed startup (experiment 1)
   is pure loss on short pipelines. The sweep must extend down to small `n` so
   this shows up rather than being averaged away.

## Expected findings

Worth writing down in advance, so the experiment can falsify them:

- **Large `n`, small `s`, deep `d`** → RDF-Connect wins by orders of magnitude;
  CWL-scatter is spawn-bound and CWL-batch cannot overlap stages.
- **Small `n`, large `s`, shallow `d`** → CWL wins; RDF-Connect's startup and
  per-message costs buy nothing.
- **Memory** → RDF-Connect flat in `n`, CWL linear. The cleanest separation.
- **METG** → RDF-Connect's per-item cost is a gRPC round trip (~0.1–1 ms, per
  experiment 1), so its METG should land near the millisecond. CWL's is a
  process spawn, so tens to hundreds of milliseconds. Expect roughly two orders
  of magnitude between them, and near-parity once per-item work exceeds ~1 s.
- **Against the shell pipeline** → RDF-Connect is meaningfully slower. Report
  it plainly; it is the price of the declarative description, and the paper's
  argument is that the price buys reusability and validation, not speed.

## References

- Slaughter et al., *Task Bench: A Parameterized Benchmark for Evaluating
  Parallel Runtime Performance*, SC'20. <https://arxiv.org/abs/1908.05790>
- Jackson, Kavoussanakis & Wallace, *Using prototyping to choose a
  bioinformatics workflow management system*, PLoS Comput Biol 17(2), 2021.
  <https://doi.org/10.1371/journal.pcbi.1008622>
- Colonnelli et al., *StreamFlow: cross-breeding cloud with HPC*.
  <https://arxiv.org/abs/2002.01558>
