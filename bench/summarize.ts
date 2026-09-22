/**
 * Consolidates the raw sweep output into paper-ready tables.
 *
 * Reads results/<sweep>.json produced by bench/run.js and emits:
 *   results/summary.md   - markdown tables, one per sweep
 *   results/summary.csv  - long-format rows for plotting
 *
 * Medians are reported rather than means: startup is occasionally perturbed by
 * unrelated system activity, and a median over repetitions is robust to that
 * without discarding runs by hand.
 */
import { readFileSync, writeFileSync, existsSync } from 'fs'
import { join, resolve } from 'path'

const ROOT = resolve(import.meta.dirname, '..', '..')
const RESULTS = join(ROOT, 'results')

type RunResult = {
  arm: string
  workload: {
    count: number
    size: number
    mode: string
    depth: number
    workUs?: number
  }
  ok: boolean
  wallMs: number
  startupMs: number
  spanMs: number
  throughput: number
  latency: { mean: number; p50: number; p90: number; p99: number } | null
  messages: number
  bytes: number
}

const SWEEPS = [
  'startup',
  'rate',
  'depth',
  'payload',
  'stream',
  'work',
  'work-depth',
]

function median(xs: number[]): number {
  const s = xs.filter((x) => !isNaN(x)).sort((a, b) => a - b)
  if (!s.length) return NaN
  const m = Math.floor(s.length / 2)
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2
}

function tag(w: RunResult['workload']) {
  const work = w.workUs ? ` w=${w.workUs}µs` : ''
  return `n=${w.count} s=${w.size}B ${w.mode} d=${w.depth}${work}`
}

function fmt(x: number, d = 1): string {
  if (isNaN(x)) return '—'
  if (Math.abs(x) >= 100000) return x.toExponential(2)
  return x.toFixed(d)
}

const md: string[] = ['# Experiment 1 — consolidated results', '']
const csv: string[] = [
  'sweep,arm,count,size,mode,depth,work_us,wall_ms,startup_ms,span_ms,throughput_msg_s,lat_p50_ms,lat_p99_ms,slowdown_vs_native',
]

for (const sweep of SWEEPS) {
  const path = join(RESULTS, `${sweep}.json`)
  if (!existsSync(path)) continue
  const rows: RunResult[] = JSON.parse(readFileSync(path, 'utf8'))

  md.push(`## ${sweep}`, '')
  md.push(
    '| workload | arm | wall (ms) | startup (ms) | msg/s | lat p50 (ms) | lat p99 (ms) | vs native |',
  )
  md.push('|---|---|---|---|---|---|---|---|')

  const keys = [...new Set(rows.map((r) => tag(r.workload)))]
  for (const k of keys) {
    const here = rows.filter((r) => tag(r.workload) === k && r.ok)
    const baseThr = median(
      here.filter((r) => r.arm === 'native').map((r) => r.throughput),
    )
    for (const arm of [...new Set(here.map((r) => r.arm))]) {
      const rs = here.filter((r) => r.arm === arm)
      const thr = median(rs.map((r) => r.throughput))
      const wall = median(rs.map((r) => r.wallMs))
      const start = median(rs.map((r) => r.startupMs))
      const span = median(rs.map((r) => r.spanMs))
      const p50 = median(rs.map((r) => r.latency?.p50 ?? NaN))
      const p99 = median(rs.map((r) => r.latency?.p99 ?? NaN))
      const slow =
        arm === 'native' || isNaN(baseThr) || isNaN(thr) || thr === 0
          ? NaN
          : baseThr / thr
      md.push(
        `| ${k} | ${arm} | ${fmt(wall)} | ${fmt(start)} | ${fmt(thr, 0)} | ${fmt(p50, 3)} | ${fmt(p99, 3)} | ${isNaN(slow) ? '—' : '×' + fmt(slow)} |`,
      )
      const w = rs[0].workload
      csv.push(
        [
          sweep,
          arm,
          w.count,
          w.size,
          w.mode,
          w.depth,
          w.workUs ?? 0,
          fmt(wall, 3),
          fmt(start, 3),
          fmt(span, 3),
          fmt(thr, 2),
          fmt(p50, 4),
          fmt(p99, 4),
          isNaN(slow) ? '' : fmt(slow, 2),
        ].join(','),
      )
    }
  }
  md.push('')
}

writeFileSync(join(RESULTS, 'summary.md'), md.join('\n'))
writeFileSync(join(RESULTS, 'summary.csv'), csv.join('\n'))
console.log(md.join('\n'))
console.log(`\nwrote results/summary.md and results/summary.csv`)
