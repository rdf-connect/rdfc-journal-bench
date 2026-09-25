#!/usr/bin/env node
/**
 * How fresh is the published stream, and what does that freshness cost?
 *
 *   node dist/bench/run-freshness.js [--n=20] [--arrival=2000]
 *        [--arms=rdfc,shell,cwl-batch] [--intervals=5,20]
 *
 * Snapshots arrive at a fixed rate, as they do for the deployment. The
 * streaming arms consume them as they arrive. The batch arm is run on a
 * schedule, as a workflow manager is actually used: every `interval` arrivals
 * it processes what has accumulated since its last run, carrying the feed
 * state forward, which is what a scheduled CI job does.
 *
 * Freshness is measured per snapshot, from its arrival to the moment its
 * activities are published. The streaming arms are polled; a scheduled run
 * publishes when it finishes, so its snapshots become visible together. A
 * snapshot that causes no activity has nothing to publish and no freshness:
 * it is recorded as NaN and left out of the summaries.
 *
 * Cost is the CPU the arm spends over the whole window (bench/proc.ts).
 *
 * The scheduled arm runs in two modes. `incremental` processes only what
 * arrived since the last run and carries the feed state and the published tree
 * forward. `rebuild` gives every run the whole history from the start and no
 * state at all, so each invocation is self-contained: the batch idiom, at the
 * price of redoing all earlier work every time.
 */
import { appendFileSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'fs'
import { join, resolve } from 'path'
import { parseArgs } from 'util'
import { renderBluebikePipeline } from '../src/genbluebike.js'
import { runPipe } from './proc.js'
import {
  JAVA_OPTS,
  cwlCommand,
  expectedActivities,
  ldesDir,
  prepareSideInputs,
  publishedActivities,
  shellCommand,
  snapshots,
} from './bluebike.js'

const ROOT = resolve(import.meta.dirname, '..', '..')
const RESULTS = join(ROOT, 'results')
const RUNS = process.env.BENCH_RUNS ?? join(RESULTS, 'runs-freshness')
const PIPELINES = join(ROOT, 'pipelines')

type Result = {
  arm: string
  /** Scheduled arms only: arrivals between runs. */
  interval?: number
  n: number
  arrivalMs: number
  wallMs: number
  cpuMs?: number
  /** Per snapshot, arrival to publication. */
  freshnessMs: number[]
  meanFreshnessMs: number
  medianFreshnessMs: number
  p95FreshnessMs: number
  invocations: number
  activities: { creates: number; updates: number }
  expected: { creates: number; updates: number }
  correct: boolean
}

const sleep = (ms: number) => new Promise((res) => setTimeout(res, ms))
const now = () => performance.timeOrigin + performance.now()

const mean = (xs: number[]) => (xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : NaN)
const quantile = (q: number) => (xs: number[]) => {
  if (!xs.length) return NaN
  const s = [...xs].sort((a, b) => a - b)
  return s[Math.min(s.length - 1, Math.floor(q * s.length))]
}
const median = quantile(0.5)
const p95 = quantile(0.95)

/** Summary statistics over the snapshots that caused an activity. */
function summarise(freshness: number[]) {
  const xs = freshness.filter((x) => !isNaN(x))
  return { meanFreshnessMs: mean(xs), medianFreshnessMs: median(xs), p95FreshnessMs: p95(xs) }
}

/** Cumulative activities expected once snapshot i has been processed. */
function cumulativeExpected(n: number): number[] {
  const out: number[] = []
  for (let i = 1; i <= n; i++) {
    const e = expectedActivities(i)
    out.push(e.creates + e.updates)
  }
  return out
}

/**
 * Whether snapshot i causes any activity. Publication is matched by count, so
 * a snapshot that adds nothing would otherwise be credited with the moment the
 * previous one was published, a full arrival interval before it arrived.
 */
function causesActivity(cumulative: number[]): boolean[] {
  return cumulative.map((c, i) => c > (i > 0 ? cumulative[i - 1] : 0))
}

/**
 * Watches the published LDES and records when each snapshot's activities
 * appear, by matching the published count against what each prefix implies.
 */
function watchPublication(dir: string, cumulative: number[], arrivals: () => number[]) {
  const published: number[] = new Array(cumulative.length).fill(NaN)
  const causes = causesActivity(cumulative)
  let next = 0
  const timer = setInterval(() => {
    if (next >= cumulative.length) return
    const a = publishedActivities(dir)
    const total = a.creates + a.updates
    const t = now()
    while (next < cumulative.length && total >= cumulative[next]) {
      if (causes[next]) published[next] = t
      next++
    }
  }, 100)

  return () => {
    clearInterval(timer)
    const arrival = arrivals()
    return published.map((p, i) => (isNaN(p) || !arrival[i] ? NaN : p - arrival[i]))
  }
}

async function runStreaming(
  arm: 'rdfc' | 'shell',
  n: number,
  arrivalMs: number,
): Promise<Result> {
  const runDir = join(RUNS, `${arm}__n${n}_a${arrivalMs}`)
  rmSync(runDir, { recursive: true, force: true })
  mkdirSync(runDir, { recursive: true })
  prepareSideInputs(runDir, n)

  const input = join(runDir, 'snapshots.ndjson')
  writeFileSync(input, snapshots(n).map((s) => JSON.stringify(s.stations)).join('\n') + '\n')

  const cumulative = cumulativeExpected(n)
  let cmd: string
  if (arm === 'rdfc') {
    const ttl = renderBluebikePipeline({
      input,
      mapping: join(ROOT, 'anchor', 'bluebike.rml.ttl'),
      shapes: join(ROOT, 'anchor', 'bluebike-shapes.ttl'),
      focusQuery: join(runDir, 'focus.rq'),
      resultDir: runDir,
      arrivalMs,
    })
    const ttlPath = join(PIPELINES, `freshness__${arm}_n${n}.ttl`)
    writeFileSync(ttlPath, ttl)
    cmd = `npx rdfc ${ttlPath} > ${runDir}/log.txt 2>&1`
  } else {
    // The shell arm is paced through a FIFO: the pipeline reads it as an
    // ordinary file while a writer feeds it one snapshot at a time.
    const fifo = join(runDir, 'arrivals.fifo')
    const pacer =
      `node -e 'const fs=require("fs");const ls=fs.readFileSync(process.argv[1],"utf8")` +
      `.split("\\n").filter(Boolean);const out=fs.createWriteStream(process.argv[2]);let i=0;` +
      `const w=()=>{if(i>=ls.length){out.end();return};out.write(ls[i++]+"\\n");` +
      `setTimeout(w,${arrivalMs})};w()' ${input} ${fifo}`
    cmd = `rm -f ${fifo}; mkfifo ${fifo}; { ${pacer} & } ; ${shellCommand(fifo, runDir)}`
  }

  const arrivalsOf = () => {
    try {
      const src = JSON.parse(readFileSync(join(runDir, 'source.json'), 'utf8'))
      return src.arrivals as number[]
    } catch {
      // The shell arm has no source processor: arrivals are the pacer's clock.
      return Array.from({ length: n }, (_, i) => start + i * arrivalMs)
    }
  }

  const stop = watchPublication(ldesDir(arm as never, runDir), cumulative, arrivalsOf)
  const start = now()
  const run = await runPipe(cmd, '/dev/null', join(runDir, 'stdout.txt'), { env: { JAVA_OPTS } })
  await sleep(300) // let the watcher see the last write
  const freshness = stop()

  const acts = publishedActivities(ldesDir(arm as never, runDir))
  const expected = expectedActivities(n)
  return {
    arm,
    n,
    arrivalMs,
    wallMs: run.wallMs,
    cpuMs: run.cpuMs,
    freshnessMs: freshness,
    ...summarise(freshness),
    invocations: 1,
    activities: acts,
    expected,
    correct: acts.creates === expected.creates && acts.updates === expected.updates,
  }
}

/**
 * The batch workflow on a schedule: every `interval` arrivals, process what
 * has accumulated, carrying the feed state and the published tree forward.
 */
async function runScheduled(
  n: number,
  arrivalMs: number,
  interval: number,
  mode: 'incremental' | 'rebuild',
): Promise<Result> {
  const runDir = join(RUNS, `cwl-${mode}__n${n}_a${arrivalMs}_i${interval}`)
  rmSync(runDir, { recursive: true, force: true })
  mkdirSync(runDir, { recursive: true })
  prepareSideInputs(runDir, n)

  const all = snapshots(n).map((s) => JSON.stringify(s.stations))
  const arrivals: number[] = []
  const freshness: number[] = new Array(n).fill(NaN)
  const causes = causesActivity(cumulativeExpected(n))
  let cpuMs = 0
  let invocations = 0
  let done = 0

  const start = now()
  for (let i = 0; i < n; i++) arrivals.push(start + i * arrivalMs)

  while (done < n) {
    const batchEnd = Math.min(done + interval, n)
    // Wait until the last snapshot of this batch has arrived.
    const wait = arrivals[batchEnd - 1] - now()
    if (wait > 0) await sleep(wait)

    // incremental: what arrived since the last run. rebuild: the whole history.
    const from = mode === 'rebuild' ? 0 : done
    const batchDir = join(runDir, `run${invocations}`)
    mkdirSync(batchDir, { recursive: true })
    prepareSideInputs(batchDir, batchEnd - from)
    const input = join(batchDir, 'snapshots.ndjson')
    writeFileSync(input, all.slice(from, batchEnd).join('\n') + '\n')

    // Carry the state of the previous run, as an incremental job would. A
    // rebuild starts from nothing every time, which is the point of it.
    if (mode === 'incremental' && invocations > 0) {
      const prev = join(runDir, `run${invocations - 1}`, 'final')
      rmSync(join(batchDir, 'feed-state-seed'), { recursive: true, force: true })
      await runPipe(`cp -r ${join(prev, 'feed-state')} ${join(batchDir, 'feed-state-seed')}`, '/dev/null', '/dev/null')
    }

    const command = cwlCommand('cwl', 'batch', input, batchDir, 1)
    if (mode === 'incremental' && invocations > 0) {
      // Hand the previous run's published tree to this one, as a scheduled
      // job does when it appends to the feed it published before.
      appendFileSync(
        join(batchDir, 'job.yml'),
        `ldes: { class: Directory, path: ${join(runDir, `run${invocations - 1}`, 'final', 'ldes-output')} }\n`,
      )
    }
    const run = await runPipe(
      command,
      '/dev/null',
      join(batchDir, 'stdout.txt'),
      { env: { PATH: `${join(ROOT, 'cwl', 'bin')}:${process.env.PATH}`, JAVA_OPTS } },
    )
    cpuMs += run.cpuMs ?? 0
    invocations++

    const finished = now()
    for (let i = done; i < batchEnd; i++) if (causes[i]) freshness[i] = finished - arrivals[i]
    done = batchEnd
  }

  const lastDir = join(runDir, `run${invocations - 1}`, 'final')
  const acts = publishedActivities(lastDir)
  const expected = expectedActivities(n)
  return {
    arm: `cwl-${mode}`,
    interval,
    n,
    arrivalMs,
    wallMs: now() - start,
    cpuMs,
    freshnessMs: freshness,
    ...summarise(freshness),
    invocations,
    activities: acts,
    expected,
    correct: acts.creates === expected.creates && acts.updates === expected.updates,
  }
}

async function main() {
  const { values } = parseArgs({
    options: {
      n: { type: 'string', default: '20' },
      arrival: { type: 'string', default: '2000' },
      arms: { type: 'string', default: 'shell,rdfc' },
      intervals: { type: 'string', default: '5,20' },
      modes: { type: 'string', default: 'incremental' },
    },
  })
  const n = Number(values.n)
  const arrivalMs = Number(values.arrival)
  mkdirSync(RUNS, { recursive: true })
  mkdirSync(PIPELINES, { recursive: true })

  const results: Result[] = []
  for (const arm of values.arms!.split(',')) {
    process.stderr.write(`  ${arm}, continuous\r`)
    results.push(await runStreaming(arm as 'rdfc' | 'shell', n, arrivalMs))
  }
  for (const mode of values.modes!.split(',') as ('incremental' | 'rebuild')[]) {
    for (const interval of values.intervals!.split(',').map(Number)) {
      process.stderr.write(`  cwl ${mode}, every ${interval} arrivals\r`)
      results.push(await runScheduled(n, arrivalMs, interval, mode))
    }
  }
  process.stderr.write('\n')

  writeFileSync(join(RESULTS, 'freshness.json'), JSON.stringify(results, null, 2))

  const fmt = (x: number, d = 1, w = 12) => (isNaN(x) ? '-'.padStart(w) : x.toFixed(d).padStart(w))
  console.log(
    `\n${'arm'.padEnd(22)}${'runs'.padStart(6)}${'mean fresh s'.padStart(14)}${'median s'.padStart(10)}${'p95 fresh s'.padStart(13)}` +
      `${'CPU s'.padStart(9)}${'C/U'.padStart(10)}${'correct'.padStart(9)}`,
  )
  console.log('-'.repeat(93))
  for (const r of results) {
    const label = r.interval ? `${r.arm} every ${r.interval}` : `${r.arm} (continuous)`
    console.log(
      `${label.padEnd(22)}${String(r.invocations).padStart(6)}${fmt(r.meanFreshnessMs / 1000, 1, 14)}` +
        `${fmt(r.medianFreshnessMs / 1000, 1, 10)}` +
        `${fmt(r.p95FreshnessMs / 1000, 1, 13)}${fmt((r.cpuMs ?? NaN) / 1000, 1, 9)}` +
        `${`${r.activities.creates}/${r.activities.updates}`.padStart(10)}${(r.correct ? 'yes' : 'NO').padStart(9)}`,
    )
  }
  console.log(
    `\n  ${n} snapshots arriving every ${arrivalMs} ms; freshness is arrival to publication.`,
  )
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
