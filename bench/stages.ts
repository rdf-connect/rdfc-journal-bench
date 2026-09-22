/**
 * Experiment 2, step 0: what do the two anchor stages cost as CLI tools?
 *
 *   node dist/bench/stages.js [--ns=1,100,1000,10000] [--reps=3] [--size=0]
 *
 * For every n it runs, on the anchor workload (anchor/, all records valid):
 *
 *   rml-stream     rml-map, one execution per record          (shell-pipe stage)
 *   rml-batch      rml-map --batch, one execution              (CWL-batch stage)
 *   shacl-stream   shacl-validate, one validation per record
 *   shacl-batch    shacl-validate --batch, one validation
 *   pipe-stream    rml-map | shacl-validate                    (the shell-pipe arm)
 *   pipe-batch     rml-map --batch | shacl-validate --batch
 *
 * and derives the two numbers that predict the framework crossover:
 *
 *   cold    cost of one process for one record (spawn + warm-up + 1 record);
 *           this is what CWL-scatter pays per record
 *   warm    marginal cost of one more record in an already warm stream;
 *           this is what the shell pipe (and, plus overhead, RDF-Connect) pays
 *
 * It also checks that streaming and batch produce the same quads, and whether
 * the per-record time in a stream stays flat as n grows (TODO §3).
 *
 * Peak RSS: see bench/proc.ts.
 */
import { existsSync, mkdirSync, writeFileSync } from 'fs'
import { join, resolve } from 'path'
import { parseArgs } from 'util'
import { generateRecords } from './gen-records.js'
import { quadSet, runPipe } from './proc.js'

const ROOT = resolve(import.meta.dirname, '..', '..')
const OUT = join(ROOT, 'results', 'stages')
const DATA = join(OUT, 'data')
const RUNS = join(OUT, 'out')

const RML = join(ROOT, 'rml-processor-jvm', 'bin', 'rml-map')
const SHACL = `node ${join(ROOT, 'shacl-processor-ts', 'lib', 'cli.js')}`
const MAPPING = join(ROOT, 'anchor', 'mapping.rml.ttl')
const SHAPES = join(ROOT, 'anchor', 'shapes.ttl')

const rml = (batch: boolean) => `${RML} --mapping ${MAPPING}${batch ? ' --batch' : ''}`
const shacl = (batch: boolean) => `${SHACL} --shapes ${SHAPES}${batch ? ' --batch' : ''}`

type Case = 'rml-stream' | 'rml-batch' | 'shacl-stream' | 'shacl-batch' | 'pipe-stream' | 'pipe-batch'

/** Shell command and input file for one case at size n. */
function command(c: Case, n: number): { cmd: string; input: string } {
  const records = join(DATA, `records_n${n}.ndjson`)
  const mapped = join(DATA, `mapped_n${n}.ndjson`)
  switch (c) {
    case 'rml-stream': return { cmd: rml(false), input: records }
    case 'rml-batch': return { cmd: rml(true), input: records }
    case 'shacl-stream': return { cmd: shacl(false), input: mapped }
    case 'shacl-batch': return { cmd: shacl(true), input: mapped }
    case 'pipe-stream': return { cmd: `${rml(false)} | ${shacl(false)}`, input: records }
    case 'pipe-batch': return { cmd: `${rml(true)} | ${shacl(true)}`, input: records }
  }
}

const CASES: Case[] = ['rml-stream', 'rml-batch', 'shacl-stream', 'shacl-batch', 'pipe-stream', 'pipe-batch']

type Run = {
  case: Case
  n: number
  rep: number
  ok: boolean
  wallMs: number
  /** Spawn -> first output line. */
  firstMs: number
  lines: number
  peakRssMb: number
  /** Mean gap between output lines over the first and last 10 % of lines. */
  gapFirstMs: number
  gapLastMs: number
  stderr?: string
}

// ── One run ──────────────────────────────────────────────────────────────────

async function runOnce(c: Case, n: number, rep: number, outFile: string): Promise<Run> {
  const { cmd, input } = command(c, n)
  const { code, wallMs, stamps, peakRssMb, stderr } = await runPipe(cmd, input, outFile)
  const tenth = Math.floor(stamps.length / 10)
  const gap = (from: number, to: number) => (to > from ? (stamps[to] - stamps[from]) / (to - from) : NaN)
  return {
    case: c,
    n,
    rep,
    ok: code === 0,
    wallMs,
    firstMs: stamps.length ? stamps[0] : NaN,
    lines: stamps.length,
    peakRssMb,
    gapFirstMs: tenth >= 2 ? gap(1, tenth) : NaN,
    gapLastMs: tenth >= 2 ? gap(stamps.length - tenth, stamps.length - 1) : NaN,
    stderr: code === 0 ? undefined : stderr.slice(-2000),
  }
}

// ── Equivalence of streaming and batch ───────────────────────────────────────

function sameQuads(a: string, b: string): { same: boolean; a: number; b: number } {
  const qa = quadSet(a)
  const qb = quadSet(b)
  let same = qa.size === qb.size
  if (same) for (const q of qa) if (!qb.has(q)) { same = false; break }
  return { same, a: qa.size, b: qb.size }
}

// ── Main ─────────────────────────────────────────────────────────────────────

const median = (xs: number[]) => {
  const s = xs.filter((x) => !Number.isNaN(x)).sort((a, b) => a - b)
  return s.length ? s[Math.floor(s.length / 2)] : NaN
}
const fmt = (x: number, d = 1) => (Number.isNaN(x) ? '–' : x.toFixed(d))

async function main() {
  const { values } = parseArgs({
    options: {
      ns: { type: 'string', default: '1,100,1000,10000' },
      reps: { type: 'string', default: '3' },
      size: { type: 'string', default: '0' },
    },
  })
  const ns = values.ns!.split(',').map(Number)
  if (!ns.includes(1)) ns.unshift(1) // cold cost is measured at n = 1
  const reps = Number(values.reps)
  const size = Number(values.size)

  for (const tool of [join(ROOT, 'rml-processor-jvm', 'build', 'libs', 'rml-map.jar'), join(ROOT, 'shacl-processor-ts', 'lib', 'cli.js')]) {
    if (!existsSync(tool)) throw new Error(`missing ${tool}: build rml-processor-jvm (gradle cliJar) and shacl-processor-ts (npm run build)`)
  }
  mkdirSync(DATA, { recursive: true })
  mkdirSync(RUNS, { recursive: true })

  // Inputs: records per n, and the RML output that the SHACL stage consumes.
  for (const n of ns) {
    const records = join(DATA, `records_n${n}.ndjson`)
    // All valid: a unit that fails validation is dropped whole (the Validate
    // processor's rule), so only then do streaming and batch forward the same.
    writeFileSync(records, [...generateRecords({ n, size, invalid: 0 })].join('\n') + '\n')
    const prep = await runOnce('rml-stream', n, -1, join(DATA, `mapped_n${n}.ndjson`))
    if (!prep.ok) throw new Error(`rml-map failed while preparing n=${n}:\n${prep.stderr}`)
    console.error(`prepared n=${n}`)
  }

  const runs: Run[] = []
  const checks: Record<string, unknown>[] = []
  for (const n of ns) {
    for (let rep = 0; rep < reps; rep++) {
      for (const c of CASES) {
        const outFile = join(RUNS, `${c}_n${n}_r${rep}.ndjson`)
        const r = await runOnce(c, n, rep, outFile)
        runs.push(r)
        console.error(`${c.padEnd(13)} n=${String(n).padEnd(6)} rep=${rep}  ${fmt(r.wallMs, 0)} ms  ${r.ok ? '' : 'FAILED'}`)
      }
    }
    for (const stage of ['rml', 'shacl', 'pipe']) {
      const res = sameQuads(join(RUNS, `${stage}-stream_n${n}_r0.ndjson`), join(RUNS, `${stage}-batch_n${n}_r0.ndjson`))
      checks.push({ n, stage, ...res })
    }
  }

  // ── Summary ────────────────────────────────────────────────────────────────
  const med = (c: Case, n: number, k: keyof Run) => median(runs.filter((r) => r.case === c && r.n === n && r.ok).map((r) => r[k] as number))

  const rows: string[] = []
  rows.push('| case | n | wall ms | first out ms | lines | peak RSS MB | ms/record (first 10 %) | ms/record (last 10 %) |')
  rows.push('|---|---:|---:|---:|---:|---:|---:|---:|')
  for (const c of CASES) {
    for (const n of ns) {
      rows.push(`| ${c} | ${n} | ${fmt(med(c, n, 'wallMs'), 0)} | ${fmt(med(c, n, 'firstMs'), 0)} | ${fmt(med(c, n, 'lines'), 0)} | ${fmt(med(c, n, 'peakRssMb'), 0)} | ${fmt(med(c, n, 'gapFirstMs'), 3)} | ${fmt(med(c, n, 'gapLastMs'), 3)} |`)
    }
  }

  const nMax = Math.max(...ns)
  const derived: string[] = []
  derived.push('| stage | cold ms (n=1) | warm ms/record | batch ms/record | cold / warm |')
  derived.push('|---|---:|---:|---:|---:|')
  for (const stage of ['rml', 'shacl', 'pipe'] as const) {
    const cold = med(`${stage}-stream`, 1, 'wallMs')
    const warm = nMax > 1 ? (med(`${stage}-stream`, nMax, 'wallMs') - cold) / (nMax - 1) : NaN
    const batch = nMax > 1 ? (med(`${stage}-batch`, nMax, 'wallMs') - med(`${stage}-batch`, 1, 'wallMs')) / (nMax - 1) : NaN
    derived.push(`| ${stage} | ${fmt(cold, 0)} | ${fmt(warm, 3)} | ${fmt(batch, 3)} | ${fmt(cold / warm, 0)} |`)
  }

  const md = [
    `# Stage costs (anchor workload, size=${size}, reps=${reps}, medians)`,
    '',
    ...rows,
    '',
    `Derived from n=1 and n=${nMax}. "cold / warm" is how many warm streamed records one extra process start costs.`,
    '',
    ...derived,
    '',
    '## Streaming vs batch: same quads?',
    '',
    '| n | stage | same | quads (stream) | quads (batch) |',
    '|---:|---|---|---:|---:|',
    ...checks.map((c) => `| ${c.n} | ${c.stage} | ${c.same ? 'yes' : '**NO**'} | ${c.a} | ${c.b} |`),
    '',
  ].join('\n')

  writeFileSync(join(OUT, 'stages.json'), JSON.stringify({ size, reps, ns, runs, checks }, null, 2))
  writeFileSync(join(OUT, 'stages.md'), md)
  console.log(md)
}

main().catch((e) => {
  console.error(e)
  process.exit(1)
})
