/**
 * Benchmark harness for experiments 1 and 2.
 *
 * Arms on the synthetic pipeline (experiment 1)
 *   rdfc        orchestrator + js-runner, one runner process, pipeline.ttl
 *   rdfc-direct same, but the runner is launched without `npx` resolution
 *   rdfc-split  orchestrator + two js-runner processes (crosses a process boundary)
 *   native      processors wired by hand, in-memory channels, same ack semantics
 *   native-loose same, but without the one-message-in-flight ack discipline
 *
 * Arms on the anchor pipeline, RML → SHACL (experiment 2, bench/anchor.ts)
 *   shell       rml-map | shacl-validate, both CLIs streaming
 *   rdfc        RmlMapper on the JVM runner, Validate on js-runner (src/anchor.ts)
 *   cwl-batch   cwltool, one task per stage over the whole input (cwl/batch.cwl)
 *   cwl-scatter cwltool --parallel, one map → validate task pair per unit
 *               (cwl/scatter.cwl); split and gather are steps of the workflow
 *   toil-scatter the same scatter.cwl run by Toil, the second CWL runner
 *   streamflow-scatter  the same scatter.cwl run by StreamFlow, the third
 *
 * Every arm runs the identical processor classes over the identical workload.
 */
import { spawn } from 'child_process'
import { mkdirSync, rmSync, readFileSync, writeFileSync, existsSync, statSync } from 'fs'
import { join, resolve } from 'path'
import { renderAnchorPipeline, renderPipeline } from '../src/genpipeline.js'
import {
  ANCHOR_MAPPING,
  ANCHOR_SHAPES,
  CWL_BIN,
  OUTPUTS,
  TOIL_PATH,
  anchorInput,
  anchorReference,
  cwlCommand,
  cwlOutputs,
  digest,
  messageCount,
  shellPipe,
  streamflowCommand,
  toilCommand,
} from './anchor.js'
import { runPipe } from './proc.js'

const ROOT = resolve(import.meta.dirname, '..', '..')
const RESULTS = join(ROOT, 'results')
const PIPELINES = join(ROOT, 'pipelines')
/**
 * Per-run working directories. BENCH_RUNS moves them (and the anchor inputs,
 * see bench/anchor.ts) elsewhere, e.g. onto tmpfs so file-passing arms are not
 * measured against the disk: BENCH_RUNS=/dev/shm/rdfc-bench.
 */
const RUNS = process.env.BENCH_RUNS ?? join(RESULTS, 'runs')

export type Arm =
  | 'rdfc'
  | 'rdfc-split'
  | 'rdfc-direct'
  | 'native'
  | 'native-loose'
  | 'shell'
  | 'cwl-batch'
  | 'cwl-scatter'
  | 'toil-scatter'
  | 'streamflow-scatter'

export type Workload = {
  count: number
  size: number
  mode: string
  depth: number
  /** Microseconds of CPU work each consuming processor spends per message. */
  workUs?: number
  /** Run the anchor pipeline instead of the synthetic one. */
  pipeline?: 'anchor'
  /** Anchor only: records per unit (message, CLI execution, CWL task). */
  unit?: number
  /** Anchor only: fraction of records generated broken (default 0.05). */
  invalid?: number
}

export type RunResult = {
  arm: Arm
  workload: Workload
  rep: number
  ok: boolean
  /** Wall clock of the whole command, including process startup. */
  wallMs: number
  /** Spawn -> first message observed at the sink. Startup + first-message cost. */
  startupMs: number
  /** First -> last message at the sink: steady-state transfer window. */
  spanMs: number
  /** Messages per second in the steady-state window. */
  throughput: number
  /** Per-message round trip as seen by the producer. */
  latency: { mean: number; p50: number; p90: number; p99: number } | null
  messages: number
  bytes: number
  /** Anchor only: peak RSS of the whole process tree. */
  peakRssMb?: number
  /** Anchor only: CPU time over the process tree, and mean cores busy. */
  cpuMs?: number
  cores?: number
  /** CWL only: peak size of the intermediate files. */
  peakDiskMb?: number
  /** CWL only: wall time of named workflow steps (split, gather), from cwltool's log. */
  stepsMs?: Record<string, number>
  /** Anchor only: output quads and reports, and whether both match the reference. */
  quads?: number
  reports?: number
  correct?: boolean
  stderr?: string
}

function sh(
  cmd: string,
  args: string[],
  cwd: string,
): Promise<{ code: number; wallMs: number; spawnAbs: number; stderr: string }> {
  return new Promise((res) => {
    const spawnAbs = performance.timeOrigin + performance.now()
    const t0 = performance.now()
    const child = spawn(cmd, args, {
      cwd,
      env: { ...process.env, LOG_LEVEL: 'error' },
      stdio: ['ignore', 'ignore', 'pipe'],
    })
    let stderr = ''
    child.stderr.on('data', (d) => {
      if (stderr.length < 8000) stderr += d.toString()
    })
    child.on('close', (code) => {
      res({ code: code ?? -1, wallMs: performance.now() - t0, spawnAbs, stderr })
    })
  })
}

function readJson(path: string): any | null {
  if (!existsSync(path)) return null
  try {
    return JSON.parse(readFileSync(path, 'utf8'))
  } catch {
    return null
  }
}

function tag(w: Workload) {
  if (w.pipeline === 'anchor') {
    const inv = w.invalid !== undefined && w.invalid !== 0.05 ? `_i${w.invalid}` : ''
    return `anchor_n${w.count}_s${w.size}_u${w.unit ?? 1}${inv}`
  }
  const work = w.workUs ? `_w${w.workUs}` : ''
  return `n${w.count}_s${w.size}_${w.mode}_d${w.depth}${work}`
}

async function runOnce(
  arm: Arm,
  w: Workload,
  rep: number,
): Promise<RunResult> {
  const runDir = join(RUNS, `${arm}__${tag(w)}__r${rep}`)
  rmSync(runDir, { recursive: true, force: true })
  mkdirSync(runDir, { recursive: true })

  if (w.pipeline === 'anchor') return runAnchor(arm, w, rep, runDir)

  let out: { code: number; wallMs: number; spawnAbs: number; stderr: string }

  if (arm.startsWith('rdfc')) {
    const placement =
      arm === 'rdfc-split' ? 'split' : arm === 'rdfc-direct' ? 'direct' : 'single'
    const ttl = renderPipeline({
      ...w,
      resultDir: runDir,
      placement,
      // Only the split arm needs it: there the generator is alone in its runner.
      lingerMs: placement === 'split' ? 300 : 0,
    })
    const ttlPath = join(PIPELINES, `${arm}__${tag(w)}.ttl`)
    writeFileSync(ttlPath, ttl)
    out = await sh('npx', ['rdfc', ttlPath], ROOT)
  } else {
    const cfg = {
      ...w,
      resultDir: runDir,
      relaxed: arm === 'native-loose',
    }
    out = await sh(
      'node',
      [join(ROOT, 'dist', 'src', 'native.js'), JSON.stringify(cfg)],
      ROOT,
    )
  }

  const gen = readJson(join(runDir, 'generator.json'))
  const sink = readJson(join(runDir, 'sink.json'))
  const ok = out.code === 0 && !!sink && sink.count === w.count

  return {
    arm,
    workload: w,
    rep,
    ok,
    wallMs: out.wallMs,
    startupMs: sink ? sink.firstAbs - out.spawnAbs : NaN,
    spanMs: sink ? sink.spanMs : NaN,
    throughput: sink && sink.spanMs > 0 ? (sink.count - 1) / (sink.spanMs / 1000) : NaN,
    latency: gen
      ? {
          mean: gen.writeLatencyMs.mean,
          p50: gen.writeLatencyMs.p50,
          p90: gen.writeLatencyMs.p90,
          p99: gen.writeLatencyMs.p99,
        }
      : null,
    messages: sink ? sink.count : 0,
    bytes: sink ? sink.bytes : 0,
    stderr: ok ? undefined : out.stderr.slice(0, 2000),
  }
}

/**
 * One run of the anchor pipeline.
 *
 * shell  timestamps come from the lines arriving on the pipe's stdout.
 * rdfc   timestamps come from AnchorSink; stdout only carries logs.
 * cwl-*, toil-*, streamflow-*  batch systems: results exist only once the workflow is done.
 *
 * Correctness: both outputs must match the reference for the grouping the arm
 * validates with (bench/anchor.ts): the data quads exactly, and one report per
 * rejected unit.
 */
async function runAnchor(
  arm: Arm,
  w: Workload,
  rep: number,
  runDir: string,
): Promise<RunResult> {
  const aw = { count: w.count, size: w.size, unit: w.unit ?? 1, invalid: w.invalid ?? 0.05 }
  const input = anchorInput(aw)
  // CWL-batch validates everything as one unit, whatever the workload's grouping.
  const expected = await anchorReference(aw, arm === 'cwl-batch' ? aw.count : aw.unit)

  let dataFile = join(runDir, OUTPUTS.data)
  let reportFile = join(runDir, OUTPUTS.report)
  let run: Awaited<ReturnType<typeof runPipe>>
  let startupMs: number
  let spanMs: number
  let messages: number
  let stepsMs: Record<string, number> | undefined
  const spawnAbs = performance.timeOrigin + performance.now()

  switch (arm) {
    case 'shell': {
      run = await runPipe(shellPipe(runDir), input, dataFile)
      const st = run.stamps
      startupMs = st.length ? st[0] : NaN
      spanMs = st.length > 1 ? st[st.length - 1] - st[0] : NaN
      messages = st.length
      break
    }
    case 'rdfc': {
      const ttl = renderAnchorPipeline({
        input,
        mapping: ANCHOR_MAPPING,
        shapes: ANCHOR_SHAPES,
        resultDir: runDir,
      })
      const ttlPath = join(PIPELINES, `rdfc__${tag(w)}.ttl`)
      writeFileSync(ttlPath, ttl)
      run = await runPipe(`npx rdfc ${ttlPath}`, '/dev/null', join(runDir, 'log.txt'))
      const sink = readJson(join(runDir, 'sink.json'))
      startupMs = sink && sink.count ? sink.firstAbs - spawnAbs : NaN
      spanMs = sink && sink.count > 1 ? sink.spanMs : NaN
      messages = sink ? sink.count : 0
      break
    }
    case 'cwl-batch':
    case 'cwl-scatter': {
      run = await runPipe(
        cwlCommand(arm === 'cwl-batch' ? 'batch' : 'scatter', aw, runDir),
        '/dev/null',
        join(runDir, 'cwl-result.json'),
        {
          env: { PATH: `${CWL_BIN}:${process.env.PATH}` },
          diskDirs: [join(runDir, 'cwl-tmp'), join(runDir, 'cwl-out')],
          timeoutMs: CWL_TIMEOUT_MS,
        },
      )
      const outs = cwlOutputs(join(runDir, 'cwl-result.json'))
      dataFile = outs?.data ?? join(runDir, OUTPUTS.cwlData)
      reportFile = outs?.report ?? join(runDir, OUTPUTS.cwlReport)
      startupMs = run.wallMs
      spanMs = NaN
      messages = messageCount(dataFile)
      stepsMs = cwlSteps(join(runDir, 'cwltool.log'))
      break
    }
    case 'toil-scatter': {
      run = await runPipe(toilCommand(aw, runDir), '/dev/null', join(runDir, 'toil-result.json'), {
        env: { PATH: `${TOIL_PATH}:${process.env.PATH}` },
        diskDirs: [join(runDir, 'toil-jobstore'), join(runDir, 'toil-work')],
        timeoutMs: CWL_TIMEOUT_MS,
      })
      const outs = cwlOutputs(join(runDir, 'toil-result.json'))
      dataFile = outs?.data ?? join(runDir, OUTPUTS.cwlData)
      reportFile = outs?.report ?? join(runDir, OUTPUTS.cwlReport)
      startupMs = run.wallMs
      spanMs = NaN
      messages = messageCount(dataFile)
      break
    }
    case 'streamflow-scatter': {
      mkdirSync(runDir, { recursive: true })
      run = await runPipe(
        streamflowCommand(aw, runDir),
        '/dev/null',
        join(runDir, 'streamflow-result.json'),
        {
          env: { PATH: `${CWL_BIN}:${process.env.PATH}` },
          diskDirs: [join(runDir, 'sf-tmp')],
          timeoutMs: CWL_TIMEOUT_MS,
        },
      )
      const outs = cwlOutputs(join(runDir, 'streamflow-result.json'))
      dataFile = outs?.data ?? join(runDir, OUTPUTS.cwlData)
      reportFile = outs?.report ?? join(runDir, OUTPUTS.cwlReport)
      startupMs = run.wallMs
      spanMs = NaN
      messages = messageCount(dataFile)
      break
    }
    default:
      throw new Error(`arm '${arm}' does not run the anchor pipeline (yet)`)
  }

  const produced = run.code === 0 && existsSync(dataFile)
  const got = produced ? digest(dataFile) : null
  const reports = messageCount(reportFile)
  const dataOk = !!got && got.sha1 === expected.data.sha1
  const reportsOk = reports === expected.reports
  const correct = dataOk && reportsOk

  // Output messages are units; the steady-state window covers all but the first.
  const recordsInSpan = messages > 1 ? (w.count * (messages - 1)) / messages : 0

  return {
    arm,
    workload: w,
    rep,
    ok: correct,
    wallMs: run.wallMs,
    startupMs,
    spanMs,
    throughput: spanMs > 0 ? recordsInSpan / (spanMs / 1000) : NaN,
    latency: null,
    messages,
    bytes: produced ? statSync(dataFile).size : 0,
    peakRssMb: run.peakRssMb,
    cpuMs: run.cpuMs,
    cores: run.cores,
    peakDiskMb: run.peakDiskMb,
    stepsMs,
    quads: got?.quads,
    reports,
    correct,
    stderr: run.timedOut
      ? `timed out after ${CWL_TIMEOUT_MS / 60000} min`
      : run.code !== 0
        ? run.stderr.trim()
          ? run.stderr.slice(-2000)
          : `exit ${run.code}; the runner's log is in ${runDir}`
        : correct
          ? undefined
          : `expected ${expected.data.quads} quads and ${expected.reports} reports, ` +
            `got ${got?.quads ?? 0} quads${dataOk ? '' : ' (differ)'} and ${reports} reports`,
  }
}

/** Per-run cap for the CWL arms (cwltool and Toil): scatter at unit 1 spawns two processes per record. */
const CWL_TIMEOUT_MS = 30 * 60 * 1000

/** CWL-batch has no unit: it only runs where the grouping cannot matter. */
function armApplies(arm: Arm, w: Workload): boolean {
  if (arm !== 'cwl-batch' || w.pipeline !== 'anchor') return true
  const unit = w.unit ?? 1
  return unit === 1 || unit === w.count
}

/**
 * Wall time of the `split`, `gather` and `gather_reports` steps, from cwltool's --timestamps log:
 * first line mentioning the step to the line reporting it done.
 */
function cwlSteps(logPath: string): Record<string, number> | undefined {
  if (!existsSync(logPath)) return undefined
  const steps: Record<string, number> = {}
  // cwltool colours its log even when it is not a terminal.
  const lines = readFileSync(logPath, 'utf8').replace(/\x1b\[[0-9;]*m/g, '').split('\n')
  for (const step of ['split', 'gather', 'gather_reports']) {
    const at = (l: string) => {
      const m = l.match(/^\[(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d(?:\.\d+)?)\]/)
      return m ? Date.parse(m[1].replace(' ', 'T')) : NaN
    }
    const mine = lines.filter((l) => l.includes(`[step ${step}]`) || l.includes(`[job ${step}]`))
    const start = mine.length ? at(mine[0]) : NaN
    const done = mine.find((l) => l.includes('completed'))
    const end = done ? at(done) : NaN
    if (!isNaN(start) && !isNaN(end)) steps[step] = end - start
  }
  return Object.keys(steps).length ? steps : undefined
}

// ── Sweeps ───────────────────────────────────────────────────────────────────

const SWEEPS: Record<string, Workload[]> = {
  // Fast sanity pass.
  smoke: [{ count: 100, size: 100, mode: 'string', depth: 0 }],

  // Message rate at a fixed small payload: isolates per-message cost.
  rate: [
    { count: 1000, size: 100, mode: 'string', depth: 0 },
    { count: 5000, size: 100, mode: 'string', depth: 0 },
    { count: 20000, size: 100, mode: 'string', depth: 0 },
  ],

  // Payload scaling at a fixed message count: isolates per-byte cost.
  payload: [
    { count: 2000, size: 0, mode: 'buffer', depth: 0 },
    { count: 2000, size: 1024, mode: 'buffer', depth: 0 },
    { count: 2000, size: 65536, mode: 'buffer', depth: 0 },
    { count: 500, size: 1048576, mode: 'buffer', depth: 0 },
    { count: 100, size: 10485760, mode: 'buffer', depth: 0 },
  ],

  // Depth scaling: each extra hop is another orchestrator round trip.
  depth: [
    { count: 2000, size: 100, mode: 'string', depth: 0 },
    { count: 2000, size: 100, mode: 'string', depth: 1 },
    { count: 2000, size: 100, mode: 'string', depth: 2 },
    { count: 2000, size: 100, mode: 'string', depth: 4 },
    { count: 2000, size: 100, mode: 'string', depth: 8 },
  ],

  // Startup cost: the pipeline does essentially nothing.
  startup: [{ count: 1, size: 1, mode: 'string', depth: 0 }],

  /**
   * The headline slowdown figures are measured with processors that do no work,
   * so 100 % of the time is overhead. Real processors compute. This sweep adds
   * per-message CPU work and asks at what point the framework stops mattering.
   *
   * Message counts fall as work rises to keep each run a few seconds; the
   * reported metric is per-message, so counts are comparable across rows.
   */
  work: [
    { count: 2000, size: 1024, mode: 'buffer', depth: 0, workUs: 0 },
    { count: 2000, size: 1024, mode: 'buffer', depth: 0, workUs: 10 },
    { count: 2000, size: 1024, mode: 'buffer', depth: 0, workUs: 100 },
    { count: 1000, size: 1024, mode: 'buffer', depth: 0, workUs: 1000 },
    { count: 300, size: 1024, mode: 'buffer', depth: 0, workUs: 10000 },
    { count: 100, size: 1024, mode: 'buffer', depth: 0, workUs: 100000 },
  ],

  /** Same question, but for a deeper pipeline where every stage does work. */
  'work-depth': [
    { count: 500, size: 1024, mode: 'buffer', depth: 4, workUs: 0 },
    { count: 500, size: 1024, mode: 'buffer', depth: 4, workUs: 100 },
    { count: 300, size: 1024, mode: 'buffer', depth: 4, workUs: 1000 },
    { count: 100, size: 1024, mode: 'buffer', depth: 4, workUs: 10000 },
  ],

  // Dedicated stream-channel path.
  stream: [
    { count: 500, size: 65536, mode: 'stream', depth: 0 },
    { count: 200, size: 1048576, mode: 'stream', depth: 0 },
  ],

  // ── Experiment 2: the anchor pipeline (RML → SHACL) ──────────────────────
  // depth is the number of stages; mode the framing on every edge.

  'anchor-smoke': [anchor(100, 1)],

  // Every arm at a few groupings, small enough to run in a few minutes: checks
  // data and reports against the reference before a long sweep.
  'anchor-check': [anchor(100, 1), anchor(100, 10), anchor(100, 100)],

  // Timing indication on a small grid: every grouping at n = 1000, one rep.
  'anchor-quick': [anchor(1000, 1), anchor(1000, 10), anchor(1000, 100), anchor(1000, 1000)],
  // The same with every record valid, so every grouping forwards all data.
  'anchor-quick-valid': [
    anchor(1000, 1, 0, 0),
    anchor(1000, 10, 0, 0),
    anchor(1000, 100, 0, 0),
    anchor(1000, 1000, 0, 0),
  ],

  // Record count at one record per unit: startup vs per-record cost.
  'anchor-n': [anchor(100, 1), anchor(1000, 1), anchor(10000, 1)],

  /**
   * The headline knob: records per unit, from streaming (1) to one batch
   * (all). The crossover between frameworks is a point on this axis.
   */
  'anchor-unit': [
    anchor(10000, 1),
    anchor(10000, 10),
    anchor(10000, 100),
    anchor(10000, 1000),
    anchor(10000, 10000),
  ],
}

function anchor(count: number, unit: number, size = 0, invalid = 0.05): Workload {
  return { pipeline: 'anchor', count, size, unit, invalid, mode: 'ndjson', depth: 2 }
}

async function main() {
  const args = process.argv.slice(2)
  const sweepName = args.find((a) => !a.startsWith('-')) ?? 'smoke'
  const reps = Number(
    args.find((a) => a.startsWith('--reps='))?.split('=')[1] ?? 3,
  )
  const armsArg = args.find((a) => a.startsWith('--arms='))?.split('=')[1]
  const workloads = SWEEPS[sweepName]
  if (!workloads) {
    console.error(
      `Unknown sweep '${sweepName}'. Available: ${Object.keys(SWEEPS).join(', ')}`,
    )
    process.exit(1)
  }
  const isAnchor = workloads[0].pipeline === 'anchor'
  const arms: Arm[] = armsArg
    ? (armsArg.split(',') as Arm[])
    : isAnchor
      ? ['shell', 'rdfc', 'cwl-scatter', 'toil-scatter', 'streamflow-scatter']
      : ['rdfc', 'native']

  mkdirSync(RESULTS, { recursive: true })
  mkdirSync(PIPELINES, { recursive: true })

  const results: RunResult[] = []
  for (const w of workloads) {
    for (const arm of arms) {
      if (!armApplies(arm, w)) continue
      for (let rep = 0; rep < reps; rep++) {
        process.stderr.write(`  running ${arm} ${tag(w)} rep=${rep}\r`)
        const r = await runOnce(arm, w, rep)
        results.push(r)
        // A timed-out run will time out again; don't spend the other reps on it.
        const timedOut = r.stderr?.startsWith('timed out')
        if (!r.ok) {
          process.stderr.write(
            `\n  ! failed: ${arm} ${tag(w)} rep=${rep}\n${r.stderr ?? ''}\n`,
          )
        }
        if (timedOut) break
      }
    }
  }
  process.stderr.write('\n')

  const outPath = join(RESULTS, `${sweepName}.json`)
  writeFileSync(outPath, JSON.stringify(results, null, 2))
  if (isAnchor) reportAnchor(results)
  else report(results)
  if (results.some((r) => r.workload.workUs)) reportWork(results)
  console.log(`\nraw results -> ${outPath}`)
}

function median(xs: number[]): number {
  const s = xs.filter((x) => !isNaN(x)).sort((a, b) => a - b)
  return s.length ? s[Math.floor(s.length / 2)] : NaN
}

function report(results: RunResult[]) {
  const keys = [...new Set(results.map((r) => tag(r.workload)))]
  const arms = [...new Set(results.map((r) => r.arm))]

  const pad = (s: string, n: number) => s.padEnd(n)
  const num = (x: number, d = 1) =>
    isNaN(x) ? '   -  ' : x.toFixed(d).padStart(9)

  console.log(
    `\n${pad('workload', 26)}${pad('arm', 14)}${'wall ms'.padStart(9)}${'startup'.padStart(9)}${'msg/s'.padStart(11)}${'lat p50'.padStart(9)}${'lat p99'.padStart(9)}`,
  )
  console.log('-'.repeat(87))

  for (const k of keys) {
    const base = results.filter(
      (r) => tag(r.workload) === k && r.arm === 'native' && r.ok,
    )
    const baseThr = median(base.map((r) => r.throughput))

    for (const arm of arms) {
      const rs = results.filter(
        (r) => tag(r.workload) === k && r.arm === arm && r.ok,
      )
      if (!rs.length) {
        console.log(`${pad(k, 26)}${pad(arm, 14)}  (all runs failed)`)
        continue
      }
      const thr = median(rs.map((r) => r.throughput))
      const ratio =
        arm !== 'native' && !isNaN(baseThr) && baseThr > 0
          ? `  (x${(baseThr / thr).toFixed(1)} slower)`
          : ''
      console.log(
        `${pad(k, 26)}${pad(arm, 14)}${num(median(rs.map((r) => r.wallMs)))}${num(
          median(rs.map((r) => r.startupMs)),
        )}${num(thr, 0)}${num(median(rs.map((r) => r.latency?.p50 ?? NaN)), 3)}${num(
          median(rs.map((r) => r.latency?.p99 ?? NaN)),
          3,
        )}${ratio}`,
      )
    }
    console.log()
  }
}

/**
 * For the work sweeps, the interesting quantity is not the ratio but how much
 * real time each framework adds per message over the `native` baseline, and
 * what fraction of the total that is once the processor actually computes.
 */
function reportWork(results: RunResult[]) {
  const keys = [...new Set(results.map((r) => tag(r.workload)))]
  const arms = [...new Set(results.map((r) => r.arm))].filter((a) => a !== 'native')

  // Steady-state window divided by messages: per-message end-to-end cost.
  const per = (k: string, arm: string) => {
    const rs = results.filter((r) => tag(r.workload) === k && r.arm === arm && r.ok)
    return rs.length ? median(rs.map((r) => r.spanMs / (r.messages - 1))) : NaN
  }
  const workMsOf = (k: string) => {
    const w = results.find((r) => tag(r.workload) === k)!.workload
    return { workMs: (w.workUs ?? 0) / 1000, depth: w.depth }
  }

  console.log('\nPer-message cost as processor work grows (depth shown per row)\n')
  for (const arm of arms) {
    console.log(`  ${arm} against native`)
    console.log(
      `${'work/msg'.padStart(10)}${'depth'.padStart(7)}${(' ' + arm + ' ms/msg').padStart(17)}${'native ms/msg'.padStart(15)}${'added ms'.padStart(10)}${'overhead'.padStart(10)}${'slowdown'.padStart(10)}`,
    )
    console.log('-'.repeat(79))

    const pts: { workMs: number; overhead: number }[] = []
    for (const k of keys) {
      const cost = per(k, arm)
      const nat = per(k, 'native')
      if (isNaN(cost) || isNaN(nat)) continue
      const { workMs, depth } = workMsOf(k)
      const added = cost - nat
      const overhead = (added / cost) * 100
      if (workMs > 0) pts.push({ workMs, overhead })
      console.log(
        `${(workMs ? workMs.toFixed(3) + 'ms' : '0').padStart(10)}` +
          `${String(depth).padStart(7)}` +
          `${cost.toFixed(4).padStart(17)}` +
          `${nat.toFixed(4).padStart(15)}` +
          `${added.toFixed(4).padStart(10)}` +
          `${overhead.toFixed(1).padStart(9)}%` +
          `${('x' + (cost / nat).toFixed(2)).padStart(10)}`,
      )
    }

    // METG: the per-message work at which half the time is still framework
    // overhead, i.e. the granularity below which the framework is the
    // bottleneck. Interpolated on a log scale between the two rows that
    // bracket 50 %.
    pts.sort((a, b) => a.workMs - b.workMs)
    for (let i = 0; i + 1 < pts.length; i++) {
      const a = pts[i]
      const b = pts[i + 1]
      if (a.overhead >= 50 && b.overhead <= 50) {
        const t = (a.overhead - 50) / (a.overhead - b.overhead)
        const metg =
          10 ** (Math.log10(a.workMs) + t * (Math.log10(b.workMs) - Math.log10(a.workMs)))
        console.log(
          `\n  METG(50%) for ${arm} = ${metg.toFixed(3)} ms of work per processor per message.` +
            `\n  Below this the framework dominates; above it, the processor does.`,
        )
        break
      }
    }
    console.log()
  }
  console.log(
    '  overhead = share of the arm\'s per-message time that is framework, not work',
  )
}

/**
 * Anchor pipeline: per workload, every arm against the shell pipe, the
 * cross-process floor. ms/record is whole-run wall clock over records, so
 * startup is included; first out is spawn → first result.
 */
function reportAnchor(results: RunResult[]) {
  const steps = (rs: RunResult[]) => {
    const names = [...new Set(rs.flatMap((r) => Object.keys(r.stepsMs ?? {})))]
    const parts = names.map(
      (n) => `${n} ${median(rs.map((r) => r.stepsMs?.[n] ?? NaN)).toFixed(0)} ms`,
    )
    return parts.length ? `  [${parts.join(', ')}; 1 s resolution]` : ''
  }
  const keys = [...new Set(results.map((r) => tag(r.workload)))]
  const arms = [...new Set(results.map((r) => r.arm))]
  const num = (x: number, d = 1, w = 11) => (isNaN(x) ? '-'.padStart(w) : x.toFixed(d).padStart(w))

  console.log(
    `\n${'workload'.padEnd(26)}${'arm'.padEnd(14)}${'wall ms'.padStart(11)}${'first out'.padStart(11)}${'ms/record'.padStart(11)}${'rec/s'.padStart(11)}${'cores'.padStart(7)}${'peak MB'.padStart(11)}${'disk MB'.padStart(9)}${'quads'.padStart(9)}${'reports'.padStart(9)}${'correct'.padStart(9)}`,
  )
  console.log('-'.repeat(138))

  for (const k of keys) {
    const here = results.filter((r) => tag(r.workload) === k)
    const count = here[0].workload.count
    const baseWall = median(here.filter((r) => r.arm === 'shell' && r.ok).map((r) => r.wallMs))

    for (const arm of arms) {
      const all = here.filter((r) => r.arm === arm)
      if (!all.length) continue // not run for this workload (armApplies)
      const rs = all.filter((r) => r.ok)
      const correct = `${all.filter((r) => r.correct).length}/${all.length}`
      if (!rs.length) {
        const why = all.find((r) => r.stderr)?.stderr?.split('\n')[0] ?? ''
        console.log(`${k.padEnd(26)}${arm.padEnd(14)}  (all runs failed: ${why.slice(0, 60)})`)
        continue
      }
      const wall = median(rs.map((r) => r.wallMs))
      const ratio =
        arm !== 'shell' && !isNaN(baseWall) ? `  (x${(wall / baseWall).toFixed(2)} of shell)` : ''
      console.log(
        `${k.padEnd(26)}${arm.padEnd(14)}${num(wall, 0)}${num(median(rs.map((r) => r.startupMs)), 0)}` +
          `${num(wall / count, 3)}${num(median(rs.map((r) => r.throughput)), 0)}` +
          `${num(median(rs.map((r) => r.cores ?? NaN)), 2, 7)}${num(median(rs.map((r) => r.peakRssMb ?? NaN)), 0)}` +
          `${num(median(rs.map((r) => r.peakDiskMb ?? NaN)), 1, 9)}` +
          `${num(median(rs.map((r) => r.quads ?? NaN)), 0, 9)}${num(median(rs.map((r) => r.reports ?? NaN)), 0, 9)}` +
          `${correct.padStart(9)}${ratio}${steps(rs)}`,
      )
    }
    console.log()
  }
  console.log(
    '  correct = runs whose data quads and report count match the reference for the' +
      '\n            grouping they validate with (bench/anchor.ts); cwl-batch validates all at once',
  )
}

main()
