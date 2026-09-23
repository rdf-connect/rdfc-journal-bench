#!/usr/bin/env node
/**
 * Experiment 2, deep pipeline: the Blue-bike chain in every arm.
 *
 *   node dist/bench/run-bluebike.js [--ns=5,10,20] [--reps=3]
 *        [--arms=shell,rdfc,cwl-batch,cwl-scatter] [--unit=1]
 *
 * Every arm runs the same six published processors over the same recorded
 * snapshots (bench/bluebike.ts) and is checked against an oracle derived from
 * the input, not from any arm's output.
 *
 * `--unit` is snapshots per CWL task; the streaming arms always work a
 * snapshot at a time, as the deployed pipeline does.
 *
 * Run directories go under BENCH_RUNS when set, e.g. on tmpfs.
 */
import { mkdirSync, rmSync, writeFileSync } from 'fs'
import { join, resolve } from 'path'
import { parseArgs } from 'util'
import { renderBluebikePipeline } from '../src/genbluebike.js'
import { runPipe } from './proc.js'
import {
  type Arm,
  JAVA_OPTS,
  cwlCommand,
  expectedMembers,
  ldesDir,
  prepareInput,
  prepareSideInputs,
  publishedMembers,
  shellCommand,
} from './bluebike.js'

const ROOT = resolve(import.meta.dirname, '..', '..')
const RESULTS = join(ROOT, 'results')
const RUNS = process.env.BENCH_RUNS ?? join(RESULTS, 'runs-bluebike')
const PIPELINES = join(ROOT, 'pipelines')
const TIMEOUT_MS = 30 * 60 * 1000

type Run = {
  arm: Arm
  snapshots: number
  unit: number
  rep: number
  ok: boolean
  wallMs: number
  /** CPU time over the whole process tree, and mean cores busy. */
  cpuMs?: number
  cores?: number
  peakRssMb: number
  peakDiskMb?: number
  /** Members published, and how many were expected. */
  members: number
  expected: number
  correct: boolean
  stderr?: string
}

async function runOnce(arm: Arm, n: number, unit: number, rep: number): Promise<Run> {
  const runDir = join(RUNS, `${arm}__n${n}_u${unit}__r${rep}`)
  rmSync(runDir, { recursive: true, force: true })
  mkdirSync(runDir, { recursive: true })

  const input = prepareInput(n, runDir)
  prepareSideInputs(runDir)
  const expected = expectedMembers(n)

  let cmd: string
  const diskDirs: string[] = []
  switch (arm) {
    case 'shell':
      cmd = shellCommand(input, runDir)
      break
    case 'rdfc': {
      const ttl = renderBluebikePipeline({
        input,
        mapping: join(ROOT, 'anchor', 'bluebike.rml.ttl'),
        shapes: join(ROOT, 'anchor', 'bluebike-shapes.ttl'),
        focusQuery: join(runDir, 'focus.rq'),
        resultDir: runDir,
      })
      const ttlPath = join(PIPELINES, `bluebike__n${n}_r${rep}.ttl`)
      writeFileSync(ttlPath, ttl)
      // From the repo root: the JVM runner's command resolves ./vendor relatively.
      cmd = `npx rdfc ${ttlPath} > ${runDir}/log.txt 2>&1`
      break
    }
    case 'cwl-batch':
    case 'cwl-scatter':
      cmd = cwlCommand(arm === 'cwl-batch' ? 'batch' : 'scatter', input, runDir, unit)
      diskDirs.push(join(runDir, 'cwl-tmp'), join(runDir, 'cwl-out'))
      break
  }

  const run = await runPipe(cmd, '/dev/null', join(runDir, 'stdout.txt'), {
    env: { PATH: `${join(ROOT, 'cwl', 'bin')}:${process.env.PATH}`, JAVA_OPTS },
    diskDirs: diskDirs.length ? diskDirs : undefined,
    timeoutMs: TIMEOUT_MS,
  })

  const published = publishedMembers(ldesDir(arm, runDir))
  let correct = published.size === expected.size
  if (correct) for (const m of expected) if (!published.has(m)) { correct = false; break }

  return {
    arm,
    snapshots: n,
    unit,
    rep,
    ok: run.code === 0 && correct,
    wallMs: run.wallMs,
    cpuMs: run.cpuMs,
    cores: run.cores,
    peakRssMb: run.peakRssMb,
    peakDiskMb: run.peakDiskMb,
    members: published.size,
    expected: expected.size,
    correct,
    stderr: run.timedOut
      ? `timed out after ${TIMEOUT_MS / 60000} min`
      : run.code !== 0
        ? run.stderr.trim().slice(-500) || `exit ${run.code}; logs in ${runDir}`
        : correct
          ? undefined
          : `published ${published.size} members, expected ${expected.size}`,
  }
}

const median = (xs: number[]) => {
  const s = xs.filter((x) => !Number.isNaN(x)).sort((a, b) => a - b)
  return s.length ? s[Math.floor(s.length / 2)] : NaN
}
const fmt = (x: number, d = 0, w = 10) => (Number.isNaN(x) ? '-'.padStart(w) : x.toFixed(d).padStart(w))

async function main() {
  const { values } = parseArgs({
    options: {
      ns: { type: 'string', default: '5,10,20' },
      reps: { type: 'string', default: '3' },
      arms: { type: 'string', default: 'shell,rdfc,cwl-batch,cwl-scatter' },
      unit: { type: 'string', default: '1' },
    },
  })
  const ns = values.ns!.split(',').map(Number)
  const reps = Number(values.reps)
  const unit = Number(values.unit)
  const arms = values.arms!.split(',') as Arm[]
  mkdirSync(RUNS, { recursive: true })
  mkdirSync(PIPELINES, { recursive: true })

  const runs: Run[] = []
  for (const n of ns) {
    for (const arm of arms) {
      for (let rep = 0; rep < reps; rep++) {
        process.stderr.write(`  running ${arm} n=${n} rep=${rep}\r`)
        const r = await runOnce(arm, n, unit, rep)
        runs.push(r)
        if (!r.ok) process.stderr.write(`\n  ! ${arm} n=${n} rep=${rep}: ${r.stderr}\n`)
      }
    }
  }
  process.stderr.write('\n')

  const outPath = join(RESULTS, 'bluebike.json')
  writeFileSync(outPath, JSON.stringify(runs, null, 2))

  console.log(
    `\n${'snapshots'.padStart(9)}${'arm'.padStart(13)}${'wall ms'.padStart(10)}${'ms/snap'.padStart(9)}` +
      `${'cpu ms'.padStart(10)}${'cores'.padStart(7)}${'peak MB'.padStart(9)}${'disk MB'.padStart(9)}` +
      `${'members'.padStart(9)}${'correct'.padStart(9)}`,
  )
  console.log('-'.repeat(95))
  for (const n of ns) {
    for (const arm of arms) {
      const all = runs.filter((r) => r.arm === arm && r.snapshots === n)
      const ok = all.filter((r) => r.ok)
      const correct = `${all.filter((r) => r.correct).length}/${all.length}`
      if (!ok.length) {
        console.log(`${String(n).padStart(9)}${arm.padStart(13)}   (failed: ${all[0]?.stderr ?? ''})`)
        continue
      }
      const wall = median(ok.map((r) => r.wallMs))
      console.log(
        `${String(n).padStart(9)}${arm.padStart(13)}${fmt(wall)}${fmt(wall / n, 1, 9)}` +
          `${fmt(median(ok.map((r) => r.cpuMs ?? NaN)))}${fmt(median(ok.map((r) => r.cores ?? NaN)), 2, 7)}` +
          `${fmt(median(ok.map((r) => r.peakRssMb)), 0, 9)}${fmt(median(ok.map((r) => r.peakDiskMb ?? NaN)), 1, 9)}` +
          `${fmt(median(ok.map((r) => r.members)), 0, 9)}${correct.padStart(9)}`,
      )
    }
    console.log()
  }
  console.log('  correct = runs publishing exactly the members the input implies (bench/bluebike.ts)')
  console.log(`\nraw results -> ${outPath}`)
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
