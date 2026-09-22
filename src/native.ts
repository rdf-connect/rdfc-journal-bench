/**
 * Arm B of experiment 1: the same processors, wired by hand.
 *
 * No pipeline.ttl, no SHACL/rdf-lens argument extraction, no orchestrator
 * process, no runner subprocess, no gRPC. Channels are direct in-memory
 * queues. Everything else -- the processor classes and the lifecycle order in
 * which they are driven -- is identical to what `Runner` does, so the delta
 * against arm A is attributable to the framework rather than to the workload.
 *
 * Lifecycle replicated from js-runner's `Runner.addProcessor` / `Runner.start`:
 *   1. construct every processor
 *   2. `await init()` per processor, in declaration order
 *   3. call `transform()` (NOT awaited) per processor, collecting the promises
 *   4. `await Promise.all(produce())`
 *   5. `await Promise.all(transforms)`
 */
import { createLogger, type Logger } from 'winston'
import { BenchGenerator, BenchPassThrough, BenchSink, nowAbs } from './processors.js'
import { memoryChannel, MemoryReader, MemoryWriter } from './inmem.js'
import { writeFileSync } from 'fs'
import { join } from 'path'

export type NativeConfig = {
  count: number
  size: number
  mode: string
  depth: number
  resultDir: string
  relaxed?: boolean
  /** Microseconds of CPU work each consuming processor spends per message. */
  workUs?: number
}

function silentLogger(): Logger {
  // The js-runner gives each processor a winston logger whose transport ships
  // log records to the orchestrator. Nothing is logged on the hot path in these
  // processors, so a transport-less logger is the fair counterpart.
  return createLogger({ transports: [], silent: true })
}

export async function runNative(cfg: NativeConfig) {
  const startAbs = nowAbs()
  const relaxed = cfg.relaxed ?? false

  // ---- Channels -----------------------------------------------------------
  // depth = number of pass-through processors between generator and sink.
  const nChannels = cfg.depth + 1
  const writers: MemoryWriter[] = []
  const readers: MemoryReader[] = []
  for (let i = 0; i < nChannels; i++) {
    const [w, r] = memoryChannel(`urn:bench:ch${i}`, relaxed)
    writers.push(w)
    readers.push(r)
  }

  // ---- Processors ---------------------------------------------------------
  const logger = silentLogger()

  const generator = new BenchGenerator(
    {
      writer: writers[0],
      count: cfg.count,
      size: cfg.size,
      mode: cfg.mode,
      resultPath: join(cfg.resultDir, 'generator.json'),
    },
    logger,
  )

  const passthroughs = Array.from(
    { length: cfg.depth },
    (_, i) =>
      new BenchPassThrough(
        { reader: readers[i], writer: writers[i + 1], workUs: cfg.workUs ?? 0 },
        logger,
      ),
  )

  const sink = new BenchSink(
    {
      reader: readers[nChannels - 1],
      resultPath: join(cfg.resultDir, 'sink.json'),
      workUs: cfg.workUs ?? 0,
    },
    logger,
  )

  // Declaration order matters for a faithful comparison: the orchestrator
  // initialises processors in the order they appear in the pipeline.
  //
  // The lifecycle methods are declared with a polymorphic `this: T & this`, so
  // they are only callable once the args have been merged onto the instance.
  // `Runner` erases the same way when it drives a heterogeneous processor list.
  type Lifecycle = {
    init(): Promise<void>
    transform(): Promise<void>
    produce(): Promise<void>
  }
  const processors = [generator, ...passthroughs, sink] as unknown as Lifecycle[]

  // ---- Lifecycle ----------------------------------------------------------
  const initStartAbs = nowAbs()
  const transforms: Promise<unknown>[] = []
  for (const p of processors) {
    await p.init()
    transforms.push(p.transform())
  }
  const readyAbs = nowAbs()

  await Promise.all(processors.map((p) => p.produce()))
  await Promise.all(transforms)
  const doneAbs = nowAbs()

  writeFileSync(
    join(cfg.resultDir, 'driver.json'),
    JSON.stringify(
      {
        arm: relaxed ? 'native-relaxed' : 'native',
        config: cfg,
        startAbs,
        initStartAbs,
        readyAbs,
        doneAbs,
        setupMs: readyAbs - startAbs,
        runMs: doneAbs - readyAbs,
        totalMs: doneAbs - startAbs,
      },
      null,
      2,
    ),
  )
}

// CLI entry: node dist/src/native.js '<json config>'
const cfgArg = process.argv[2]
if (cfgArg) {
  const cfg: NativeConfig = JSON.parse(cfgArg)
  runNative(cfg).then(
    () => process.exit(0),
    (err) => {
      console.error(err)
      process.exit(1)
    },
  )
}
