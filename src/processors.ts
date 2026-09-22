/**
 * Benchmark processors, shared verbatim by both arms of experiment 1.
 *
 * The *same* compiled classes are instantiated by the RDF-Connect orchestrator
 * (via pipeline.ttl + js-runner) and by the hand-wired driver. Nothing in here
 * knows which arm it is running in: the only difference is which `Reader` /
 * `Writer` implementation gets injected.
 */
import { Processor, type Reader, type Writer } from '@rdfc/js-runner'
import { writeFileSync } from 'fs'

/** Absolute epoch milliseconds with sub-millisecond precision. */
export function nowAbs(): number {
  return performance.timeOrigin + performance.now()
}

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return NaN
  const idx = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))
  return sorted[idx]
}

export function summarize(samples: number[]) {
  const s = [...samples].sort((a, b) => a - b)
  const sum = s.reduce((a, b) => a + b, 0)
  return {
    n: s.length,
    mean: sum / s.length,
    min: s[0],
    p50: percentile(s, 50),
    p90: percentile(s, 90),
    p99: percentile(s, 99),
    max: s[s.length - 1],
  }
}

/**
 * Burn `us` microseconds of CPU.
 *
 * Deliberately synchronous and CPU-bound: the processors this framework targets
 * (RML mapping, SHACL validation, parsing) are compute-bound, and a synchronous
 * processor genuinely does block its runner's event loop. A `setTimeout` would
 * model I/O waiting instead and would let the runtime overlap work with
 * transport, flattering the framework.
 *
 * The accumulator is returned so the optimiser cannot elide the loop.
 */
export function busyWork(us: number): number {
  if (!us || us <= 0) return 0
  const end = performance.now() + us / 1000
  let acc = 0
  while (performance.now() < end) {
    for (let i = 0; i < 1000; i++) acc += Math.sqrt(i)
  }
  return acc
}

function emit(path: string, payload: unknown) {
  writeFileSync(path, JSON.stringify(payload, null, 2))
}

type GeneratorArgs = {
  writer: Writer
  count: number
  size: number
  mode: string
  resultPath: string
  /**
   * Milliseconds to stay alive after closing the writer.
   *
   * Workaround for a cross-runner shutdown race: `writer.close()` resolves once
   * the close message has been handed to the local gRPC socket, not once the
   * orchestrator has received it. When the generator is the only processor in
   * its runner, the runner process exits immediately afterwards and the close
   * is lost, so the downstream runner's reader never terminates. Lingering
   * gives the socket time to flush. Does not affect the measured window, which
   * is first -> last message observed at the sink.
   */
  lingerMs?: number
}

/**
 * Produces `count` messages of `size` bytes and records how long each
 * individual write took.
 *
 * Because a write only resolves once the message has been fully handled
 * downstream, each sample is a complete producer -> consumer -> producer round
 * trip. Over many messages this yields the latency distribution of the channel.
 */
export class BenchGenerator extends Processor<GeneratorArgs> {
  async init(): Promise<void> {}
  async transform(): Promise<void> {}

  async produce(this: GeneratorArgs & this): Promise<void> {
    const count = Number(this.count)
    const size = Number(this.size)
    const mode = String(this.mode)

    // Build the payload once so allocation is not part of the measurement.
    const text = 'x'.repeat(size)
    const bytes = new TextEncoder().encode(text)

    const latencies: number[] = new Array(count)
    const startAbs = nowAbs()

    for (let i = 0; i < count; i++) {
      const t0 = performance.now()
      if (mode === 'buffer') {
        await this.writer.buffer(bytes)
      } else if (mode === 'stream') {
        const chunk = bytes
        await this.writer.stream(
          (async function* () {
            yield chunk
          })(),
        )
      } else {
        await this.writer.string(text)
      }
      latencies[i] = performance.now() - t0
    }

    const endAbs = nowAbs()
    await this.writer.close()

    const linger = Number(this.lingerMs ?? 0)
    if (linger > 0) await new Promise((res) => setTimeout(res, linger))

    emit(this.resultPath, {
      role: 'generator',
      count,
      size,
      mode,
      startAbs,
      endAbs,
      elapsedMs: endAbs - startAbs,
      writeLatencyMs: summarize(latencies),
    })
  }
}

type PassThroughArgs = {
  reader: Reader
  writer: Writer
  /** Microseconds of CPU work to spend on each message. */
  workUs?: number
}

/** Forwards every message; optionally doing real work first. */
export class BenchPassThrough extends Processor<PassThroughArgs> {
  async init(): Promise<void> {}

  async transform(this: PassThroughArgs & this): Promise<void> {
    const work = Number(this.workUs ?? 0)
    for await (const msg of this.reader.buffers()) {
      busyWork(work)
      await this.writer.buffer(msg)
    }
    await this.writer.close()
  }

  async produce(): Promise<void> {}
}

type SinkArgs = {
  reader: Reader
  resultPath: string
  /** Microseconds of CPU work to spend on each message. */
  workUs?: number
}

/** Terminal processor: counts messages and timestamps first/last arrival. */
export class BenchSink extends Processor<SinkArgs> {
  async init(): Promise<void> {}

  async transform(this: SinkArgs & this): Promise<void> {
    const work = Number(this.workUs ?? 0)
    let count = 0
    let bytes = 0
    let firstAbs = 0
    let lastAbs = 0

    for await (const msg of this.reader.buffers()) {
      if (count === 0) firstAbs = nowAbs()
      count++
      bytes += msg.byteLength
      busyWork(work)
      lastAbs = nowAbs()
    }

    emit(this.resultPath, {
      role: 'sink',
      count,
      bytes,
      workUs: work,
      firstAbs,
      lastAbs,
      // Steady-state window: excludes the cost of getting the first message through.
      spanMs: lastAbs - firstAbs,
    })
  }

  async produce(): Promise<void> {}
}
