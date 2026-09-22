# Experiment 1 — what RDF-Connect's orchestration costs

## Question

RDF-Connect routes every message from a processor through the orchestrator over
gRPC, even when producer and consumer live in the same runner process. How much
does that cost, and where does it go?

## Read the headline numbers correctly

**Every slowdown figure below except the work sweep is measured with processors
that do no work at all.** The generator emits a message, the sink counts it.
There is no parsing, no mapping, no validation — nothing but the framework. So
by construction ~99 % of the measured time *is* framework, and the resulting
ratios (×60–120) are the **worst case the architecture can produce**, not a
number any real pipeline will experience.

That is the correct way to measure overhead in isolation, and it is why the
decomposition below is meaningful. But quoted on its own, "RDF-Connect is 100×
slower" would be actively misleading about real pipelines, where a processor
doing 10 ms of work per message sees **7.5 %** overhead and one doing 100 ms sees
**1.5 %** (measured — see [When processors do real work](#when-processors-do-real-work)).

Any text drawn from this document should carry both halves.

## Method

Both arms run **the same compiled processor classes** over the same workload.
`Reader` and `Writer` are interfaces in `@rdfc/js-runner`, so the baseline can
inject in-memory implementations and leave the processors untouched. The only
independent variable is how processors are instantiated and connected.

| Arm | Instantiation | Channels |
|---|---|---|
| `rdfc` | `pipeline.ttl` → orchestrator → one `js-runner` subprocess | gRPC via orchestrator |
| `rdfc-direct` | same, runner launched without `npx` | gRPC via orchestrator |
| `rdfc-split` | orchestrator → two `js-runner` subprocesses | gRPC, crossing a process boundary |
| `native` | hand-wired driver, no RDF, no orchestrator | in-memory, same ack semantics |
| `native-loose` | hand-wired driver | in-memory, no one-message-in-flight rule |

The hand-wired driver reproduces `Runner`'s lifecycle exactly — `init()` awaited
per processor in declaration order, `transform()` started but not awaited, then
`produce()`, then the transforms — so the difference is transport and
configuration, not scheduling.

The in-memory `Writer` preserves the framework's contract: a write resolves only
once every consumer has actually consumed the message, mirroring the
`GlobalAck` → `LocalAck` round trip. `native-loose` drops only that rule.

Measurements: each write is timed by the producer (a write only resolves after
the full round trip, so this is the channel's latency distribution); the sink
timestamps first and last arrival. Throughput is taken over the steady-state
window (first → last message), excluding startup. Medians over 3–5 repetitions.

Hardware: 8 cores, 15 GB RAM, Linux 7.1.3, Node v26.4.0,
`@rdfc/js-runner` 3.3.0, `@rdfc/orchestrator-js` 2.2.2.

## Results

### Fixed startup cost: ~1.55 s

| Arm | Spawn → first message at sink |
|---|---|
| `rdfc` | 1666 ms |
| `rdfc-direct` | 1595 ms |
| `rdfc-split` (2 runners) | 1767 ms |
| `native` | 123 ms |

Decomposed (measured independently):

| Component | Cost | Paid |
|---|---|---|
| **Remote fetch of `https://w3id.org/rdf-connect#`** | **500–800 ms** | once per pipeline start, on the critical path |
| `npx` resolution | 331 ms | per runner |
| Login shell — runners spawn via `bash -l -c` | 118 ms | per runner |
| Bare Node boot | 19 ms | per process |

The single largest component is a **network request on every pipeline start**.
`start()` in the orchestrator does:

```js
const quads         = await readQuads([iri.toString()])   // local pipeline file
const ontologyQuads = await readQuads([RDFC.namespace])   // ← remote HTTP
```

It is fully serial, before the gRPC server binds and before any runner is
spawned. Caching the ontology locally would remove roughly a third of startup
and would also make pipeline startup work offline.

Two smaller findings: `npx` costs only ~70 ms of end-to-end startup (it overlaps
with orchestrator setup), so it is not worth optimising; and runners are spawned
with `spawn('bash', ['-l', '-c', cmd])`, so every runner pays for a full login
shell and inherits whatever the user's profile prints.

### Per-message cost: 60–90× at small payloads

| Messages | `rdfc` | `native` | Slowdown |
|---|---|---|---|
| 1 000 | 3 243 msg/s | 280 000 msg/s | ×86 |
| 5 000 | 5 420 msg/s | 325 000 msg/s | ×60 |
| 20 000 | 5 634 msg/s | 419 000 msg/s | ×74 |

RDF-Connect sustains roughly **5 000 small messages per second** on one channel,
at ~0.15 ms median round-trip latency (p99 ~0.4–1.1 ms). The in-memory baseline
is ~0.002 ms.

`native-loose` is indistinguishable from `native` (×0.9–1.0). The strict
one-message-in-flight discipline is therefore free *when the transport is free*.
Its cost in RDF-Connect is not the acknowledgement itself but that the
acknowledgement is a full network round trip which nothing can overlap: with one
message in flight per channel, throughput is pinned at `1 / RTT` regardless of
available bandwidth or cores. Windowing or batching acknowledgements is the
obvious lever, and the only one that would change the shape of these numbers.

### Depth: each hop is another round trip

| Depth | `rdfc` msg/s | median write latency |
|---|---|---|
| 0 | 4 106 | 0.186 ms |
| 1 | 2 354 | 0.356 ms |
| 2 | 1 733 | 0.475 ms |
| 4 | 1 070 | 0.800 ms |
| 8 | 657 | 1.324 ms |

Latency is linear in depth at **≈0.14 ms per additional hop**; throughput falls
as `1/(d+1)`. Because stages cannot overlap while a channel allows only one
message in flight, a deep pipeline serialises: the ×64 penalty at depth 0
becomes ×123 at depth 8.

This is the clearest optimisation target. Producer and consumer are frequently
in the *same* runner process, yet every message still makes two hops through the
orchestrator. A same-runner fast path — which earlier RDF-Connect versions had,
as JS in-memory channels — would recover most of this. The `native` arm is
precisely a measurement of that missing fast path's value.

### Payload: cost grows sharply with size

| Payload | `rdfc` msg/s | Effective |
|---|---|---|
| 0 B | 4 164 | — |
| 1 KB | 4 125 | 4 MB/s |
| 64 KB | 1 607 | 105 MB/s |
| 1 MB | 136 | 136 MB/s |
| 10 MB | **fails** | — |

Below ~1 KB, cost is per-message and payload is free. Above that, serialisation
and copying dominate, plateauing around 105–136 MB/s.

The ×2184 ratio at 1 MB should not be read as "2184× of avoidable overhead".
The in-memory channel passes a *reference* to the same `Uint8Array` and does
literally no per-byte work, so it is a zero-copy floor, not an achievable
target for any design that isolates processors in separate processes. What the
number does show is that RDF-Connect's per-byte cost is real and that large
payloads are where it is felt.

### Hard 4 MB message ceiling

10 MB messages fail outright:

```
8 RESOURCE_EXHAUSTED: Received message larger than max (10485832 vs 4194304)
```

This is gRPC's default `max_receive_message_length`, unset by RDF-Connect. Two
problems beyond the limit itself:

1. **The orchestrator exits with code 0.** The pipeline delivered zero messages
   and crashed, and a caller inspecting the exit status sees success. Any
   CI use or shell composition around `rdfc` will silently pass.
2. **No fallback.** The framework has a streaming channel intended for large
   payloads, but an oversized `buffer()` write is not routed to it and the error
   message does not mention it.

Raising the limit is a one-line channel option; a clear error naming
`writer.stream()` would be more useful still.

### The stream channel is slower than buffer messages

| Workload | `buffer` | `stream` |
|---|---|---|
| 1 MB × N | 136 msg/s | 103 msg/s |
| 64 KB × N | 1 607 msg/s | 611 msg/s |

The dedicated streaming path — the one designed for large payloads — is
**2.6× slower at 64 KB and 1.3× slower at 1 MB** than an ordinary buffered
message. The cause is visible in `writer.ts`: each chunk awaits its own control
message before the next is sent.

```ts
for await (const msg of buffer) {
  const processedPromise = new Promise((res) => stream.once('data', res))
  await writeStreamMessageChunk({ data: { data: t(msg) } })
  await processedPromise            // ← one RTT per chunk, never overlapped
}
```

There is already a `TODO: don't await to allow consuming processors to read and
handle in parallel` on that loop. These numbers quantify what that TODO costs:
today the streaming channel's only advantage over a buffer message is dodging
the 4 MB ceiling, and it pays for that with throughput.

### A cross-runner shutdown race

The `rdfc-split` arm initially hung forever. The generator's runner sends its
close and the process exits immediately; `writer.close()` resolves once the
message reaches the local gRPC socket, not once the orchestrator has received
it, so the close is lost with the exiting process. The downstream runner's
reader never terminates, `transform()` never returns, and the orchestrator waits
forever.

It only reproduces when a runner has no work left after closing its writer —
which is why the single-runner arm never showed it: the sink's pending
`transform()` kept that process alive long enough to flush.

Benchmark workaround: `BenchGenerator.lingerMs` keeps the runner alive 300 ms
after closing. The real fix belongs in the runner — flush pending writes, or
await orchestrator acknowledgement of the close, before exiting.

## When processors do real work

The zero-work figures answer "what does the framework cost?". They do not answer
"does the framework matter?". To get at that, each consuming processor spends a
configurable amount of real CPU per message (`bench:workUs`, a synchronous busy
loop — chosen over `setTimeout` because the target workloads, RML mapping and
SHACL validation, are compute-bound and a synchronous processor genuinely does
block its runner).

Single stage (depth 0), median of 3 repetitions:

| Work per message | RDF-Connect | hand-wired | Added | Overhead share | Slowdown |
|---|---|---|---|---|---|
| 0 | 0.234 ms | 0.002 ms | 0.232 ms | 99.0 % | ×96.8 |
| 10 µs | 0.269 ms | 0.014 ms | 0.255 ms | 94.7 % | ×19.0 |
| 100 µs | 0.414 ms | 0.104 ms | 0.310 ms | 74.9 % | ×4.0 |
| 1 ms | 1.552 ms | 1.008 ms | 0.544 ms | 35.1 % | ×1.54 |
| 10 ms | 10.87 ms | 10.06 ms | 0.813 ms | 7.5 % | ×1.08 |
| 100 ms | 102.6 ms | 101.1 ms | 1.549 ms | 1.5 % | ×1.02 |

Five stages each doing the work (depth 4):

| Work per message per stage | RDF-Connect | hand-wired | Added | Overhead share | Slowdown |
|---|---|---|---|---|---|
| 0 | 1.070 ms | 0.010 ms | 1.060 ms | 99.1 % | ×108.8 |
| 100 µs | 1.541 ms | 0.516 ms | 1.025 ms | 66.5 % | ×3.0 |
| 1 ms | 7.200 ms | 5.032 ms | 2.168 ms | 30.1 % | ×1.43 |
| 10 ms | 53.96 ms | 50.16 ms | 3.795 ms | 7.0 % | ×1.08 |

The framework's contribution is **roughly constant in absolute terms** — about
0.23 ms per message per hop — so its *share* collapses as soon as processors do
anything. The ×100 headline and the ×1.01 result are the same system; only the
workload moved.

Borrowing Task Bench's summary metric:

> **METG(50 %) ≈ 0.4 ms** of work per processor per message
> (0.35–0.42 ms across repeated sweeps).

Below ~0.4 ms of work, RDF-Connect is the bottleneck. Above it, the processor
is. The value is sensitive to the noisiest rows of the sweep, so it is worth
quoting as a rough threshold rather than a precise constant — the shape of the
curve is the robust result, not the crossing point. For calibration: a SHACL validation or an RML mapping of a single member is
typically **one to three orders of magnitude** above that threshold, which puts
realistic knowledge-graph pipelines firmly in the regime where the framework
costs a few percent.

Two honest qualifications:

- The added cost creeps up at high work (0.23 ms → 1.55 ms) because the
  synchronous busy loop blocks the runner's event loop, delaying gRPC handling.
  A processor doing asynchronous I/O would not show this.
- The comparison is against an **in-process** baseline, so this is the cost of
  process isolation as such. It is not a claim that 0.23 ms is irreducible —
  a same-runner fast path would remove most of it.

### Stages do not overlap

The depth-4 rows expose something more consequential than overhead. With five
stages each doing 10 ms of work, per-message time is **53.96 ms** — the *sum* of
all five stages, not the *maximum* of them. The hand-wired arm behaves
identically (50.16 ms). A pipelined execution would approach ~10 ms per message
in steady state, because stage *k* would work on message *i+1* while stage *k+1*
works on message *i*.

RDF-Connect gets no pipeline parallelism at all, and this follows from the
channel contract rather than from gRPC. A reader's acknowledgement fires only
once the consumer's loop body has completed:

```ts
yield item
onComplete()   // consumer has resumed, i.e. its body finished
```

and that body includes the consumer's own downstream write, which awaits *its*
acknowledgement. Acknowledgements therefore chain the whole length of the
pipeline: the generator's write does not resolve until the final sink has
finished. With one message in flight per channel, the entire pipeline
degenerates to lock-step.

This explains the exact `1/(d+1)` throughput scaling in the depth sweep, and it
means a deep pipeline gains nothing from having multiple cores. For a framework
whose central claim is streaming, this is worth addressing before the constant
factors are: allowing a bounded window of in-flight messages per channel would
let stages overlap and would turn depth from a multiplier into (roughly) a
no-op.

Note this is a property of the *contract*, so the hand-wired baseline inherits
it — it is not counted anywhere in the overhead figures above. It is a separate,
and probably larger, opportunity.

Where this genuinely bites is **fine-grained streaming**: per-triple or
per-sensor-reading messages doing microseconds of work each. There, RDF-Connect
is the bottleneck by two orders of magnitude, and batching messages into larger
units is the only fix that works today.

## Summary for the paper

| Dimension | Overhead vs. hand-wired |
|---|---|
| Startup | +1.55 s fixed (≈⅓ of it a remote ontology fetch) |
| Small messages (≤1 KB) | ×60–90; ~5 000 msg/s, ~0.15 ms RTT |
| Per additional hop | +0.14 ms; throughput ∝ 1/(d+1) |
| Large payloads | plateau ~105–136 MB/s; hard failure above 4 MB |
| Strict ack discipline | free in-memory; the binding constraint once a network is involved |
| **With 10 ms/message of real work** | **×1.08 (7.5 % overhead)** |
| **With 100 ms/message of real work** | **×1.01 (1.5 % overhead)** |
| **METG(50 %)** | **≈0.4 ms of work per processor per message** |

The headline ratios are a worst case measured with do-nothing processors, and
must never be quoted without the work sweep beside them. The defensible claim is
narrower and stronger: *RDF-Connect adds a roughly fixed ~0.23 ms per message per
hop; whether that matters is entirely a function of how much work a processor
does, and it stops mattering above ~0.4 ms.*

Beyond that, RDF-Connect's costs are dominated by **architecture, not
implementation**. Process isolation and a central router are what make runners
language-agnostic and pipelines declarative; the ×60–90 figure is the price of
that, and most of it is recoverable only by relaxing isolation (a same-runner
fast path) or relaxing the protocol (windowed acknowledgements). Startup is the
exception — a third of it is an avoidable network round trip.

## Reproducing

```bash
npm install && npm run build
./bench/all.sh                 # all sweeps, including work
node dist/bench/summarize.js   # -> results/summary.{md,csv}
```
