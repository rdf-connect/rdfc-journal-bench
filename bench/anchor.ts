/**
 * The anchor pipeline (RML mapping → SHACL validation) for experiment 2:
 * inputs, per-arm commands, and the reference output every arm is checked
 * against.
 *
 * A workload is `count` records of `size` bytes, a fraction `invalid` of them
 * broken, grouped `unit` records per input line — one RDF-Connect message, one
 * CLI execution, one CWL task. A line of one record is the record itself; a
 * line of several is a JSON array.
 *
 * Every arm has two outputs, as the Validate processor does: the data of every
 * unit that conforms in full (`outgoing`), and one validation report per unit
 * that does not (`report`). Both are NDJSON, one JSON string per message.
 */
import { createHash } from 'crypto'
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'fs'
import { join, resolve } from 'path'
import { generateLabelledRecords } from './gen-records.js'
import { quadSet, runPipe } from './proc.js'

const ROOT = resolve(import.meta.dirname, '..', '..')
/** Generated inputs and references; under BENCH_RUNS when set (see bench/run.ts). */
const DATA = join(process.env.BENCH_RUNS ?? join(ROOT, 'results'), 'anchor-data')

const RML = join(ROOT, 'rml-processor-jvm', 'bin', 'rml-map')
const RML_JAR = join(ROOT, 'rml-processor-jvm', 'build', 'libs', 'rml-map.jar')
const SHACL = join(ROOT, 'shacl-processor-ts', 'lib', 'cli.js')
const MAPPING = join(ROOT, 'anchor', 'mapping.rml.ttl')
const SHAPES = join(ROOT, 'anchor', 'shapes.ttl')

export type AnchorWorkload = { count: number; size: number; unit: number; invalid: number }

export const ANCHOR_MAPPING = MAPPING
export const ANCHOR_SHAPES = SHAPES

const CWL = join(ROOT, 'cwl')
const CWLTOOL = join(ROOT, '.venv', 'bin', 'cwltool')
/** Prepended to PATH for CWL runs: `rml-map` and `shacl-validate` symlinks. */
export const CWL_BIN = join(CWL, 'bin')

const tagOf = (w: AnchorWorkload) => `n${w.count}_s${w.size}_i${w.invalid}`

/** Where each arm leaves its two outputs, relative to its run directory. */
export const OUTPUTS = {
  data: 'out.ndjson',
  report: 'report.ndjson',
  cwlData: join('final', 'valid.ndjson'),
  cwlReport: join('final', 'report.ndjson'),
}

/** The shell-pipe arm: the two CLIs, streaming, stdin → stdout. */
export function shellPipe(runDir: string): string {
  return `${RML} --mapping ${MAPPING} | node ${SHACL} --shapes ${SHAPES} --report ${join(runDir, OUTPUTS.report)}`
}

/**
 * The CWL arms, run by cwltool without containers.
 *
 *   batch    cwl/batch.cwl: one task per stage over the whole input
 *   scatter  cwl/scatter.cwl: split into chunks of `unit` records, one
 *            map → validate subworkflow per chunk (--parallel), concatenate
 *
 * Both read the one-record-per-line input; the grouping is CWL's own split.
 * Intermediate files live under `runDir` (cwl-tmp/, cwl-out/) so their peak
 * size can be sampled; the results land in `runDir`/final/.
 */
export function cwlCommand(kind: 'batch' | 'scatter', w: AnchorWorkload, runDir: string): string {
  if (!existsSync(CWLTOOL)) {
    throw new Error(`missing ${CWLTOOL}: python3 -m venv .venv && .venv/bin/pip install cwltool`)
  }
  const records = anchorInput({ ...w, unit: 1 })
  const unit = kind === 'scatter' ? ` --unit ${w.unit}` : ''
  return (
    `${CWLTOOL} --no-container --parallel --timestamps` +
    ` --outdir ${runDir}/final --tmpdir-prefix ${runDir}/cwl-tmp/ --tmp-outdir-prefix ${runDir}/cwl-out/` +
    ` ${CWL}/${kind}.cwl --records ${records} --mapping ${MAPPING} --shapes ${SHAPES}${unit}` +
    ` 2> ${runDir}/cwltool.log`
  )
}

const TOIL_BIN = join(ROOT, '.venv-toil', 'bin')
/** PATH for Toil runs: the tool symlinks, and Toil's own `_toil_worker`. */
export const TOIL_PATH = `${CWL_BIN}:${TOIL_BIN}`

/**
 * CWL-scatter run by Toil (single machine), the second CWL runner: the same
 * cwl/scatter.cwl, so any difference with cwltool is the runner. Retries are
 * off (Toil would otherwise rerun a failed job with more memory, hiding the
 * failure in the timing). Intermediate files live in `runDir`/toil-jobstore
 * and `runDir`/toil-work; the results land in `runDir`/final/.
 */
export function toilCommand(w: AnchorWorkload, runDir: string): string {
  if (!existsSync(join(TOIL_BIN, 'toil-cwl-runner'))) {
    throw new Error(`missing Toil: uv venv -p 3.14 .venv-toil && uv pip install -p .venv-toil "toil[cwl]"`)
  }
  const records = anchorInput({ ...w, unit: 1 })
  return (
    `mkdir -p ${runDir}/toil-work && toil-cwl-runner --no-container --retryCount 0` +
    ` --jobStore ${runDir}/toil-jobstore --workDir ${runDir}/toil-work --outdir ${runDir}/final` +
    ` --logFile ${runDir}/toil.log` +
    ` ${CWL}/scatter.cwl --records ${records} --mapping ${MAPPING} --shapes ${SHAPES} --unit ${w.unit}` +
    ` 2> ${runDir}/toil.stderr`
  )
}

const STREAMFLOW_BIN = join(ROOT, '.venv-streamflow', 'bin')

/**
 * CWL-scatter run by StreamFlow's cwl-runner, the third CWL runner: the same
 * cwl/scatter.cwl, bound to a local deployment. StreamFlow ignores TMPDIR, so
 * the deployment's `workdir` puts its intermediate files under `runDir`, on the
 * same disk as the other runners' (the default is /tmp/streamflow, never
 * cleaned up).
 */
export function streamflowCommand(w: AnchorWorkload, runDir: string): string {
  const runner = join(STREAMFLOW_BIN, 'cwl-runner')
  if (!existsSync(runner)) {
    throw new Error(
      'missing StreamFlow: uv venv -p 3.12 .venv-streamflow && ' +
        'uv pip install -p .venv-streamflow --prerelease=allow "streamflow==0.2.0rc3"',
    )
  }
  const job = join(runDir, 'job.json')
  writeFileSync(
    job,
    JSON.stringify({
      records: { class: 'File', path: anchorInput({ ...w, unit: 1 }) },
      mapping: { class: 'File', path: MAPPING },
      shapes: { class: 'File', path: SHAPES },
      unit: w.unit,
    }),
  )
  const config = join(runDir, 'streamflow.yml')
  writeFileSync(
    config,
    [
      'version: v1.0',
      'workflows:',
      '  anchor:',
      '    type: cwl',
      '    config:',
      `      file: ${join(CWL, 'scatter.cwl')}`,
      `      settings: ${job}`,
      '    bindings:',
      '      - step: /',
      '        target:',
      '          deployment: local-bench',
      'deployments:',
      '  local-bench:',
      '    type: local',
      '    config: {}',
      `    workdir: ${join(runDir, 'sf-tmp')}`,
      '',
    ].join('\n'),
  )
  return (
    `mkdir -p ${runDir}/sf-tmp && ${runner} --streamflow-file ${config} --outdir ${runDir}/final` +
    ` ${CWL}/scatter.cwl ${job} 2> ${runDir}/streamflow.log`
  )
}

/**
 * Paths of a CWL run's two outputs, from the result JSON the runner printed.
 * Runners lay out --outdir differently (StreamFlow nests every output in its
 * own directory), so the result document is the only portable answer.
 */
export function cwlOutputs(resultJson: string): { data: string; report: string } | null {
  if (!existsSync(resultJson)) return null
  try {
    const out = JSON.parse(readFileSync(resultJson, 'utf8'))
    const path = (f: { path?: string; location?: string }) =>
      resolve(f.path ?? decodeURIComponent((f.location ?? '').replace(/^file:\/\//, '')))
    return { data: path(out.valid), report: path(out.report) }
  } catch {
    return null
  }
}

export type Digest = { quads: number; sha1: string }

function digestOf(quads: Iterable<string>): Digest {
  const sorted = [...quads].sort()
  return { quads: sorted.length, sha1: createHash('sha1').update(sorted.join('\n')).digest('hex') }
}

export function digest(outFile: string): Digest {
  return digestOf(quadSet(outFile))
}

/** Number of messages (lines) in an NDJSON output; 0 if it does not exist. */
export function messageCount(file: string): number {
  if (!existsSync(file)) return 0
  return readFileSync(file, 'utf8').split('\n').filter(Boolean).length
}

function checkTools() {
  for (const tool of [RML_JAR, SHACL]) {
    if (!existsSync(tool)) {
      throw new Error(`missing ${tool}: build rml-processor-jvm (gradle cliJar) and shacl-processor-ts (npm run build)`)
    }
  }
}

const labelled = (w: AnchorWorkload) =>
  [...generateLabelledRecords({ n: w.count, size: w.size, invalid: w.invalid })]

/** Input file for a workload, generated once and cached. */
export function anchorInput(w: AnchorWorkload): string {
  checkTools()
  mkdirSync(DATA, { recursive: true })
  const file = join(DATA, `records_${tagOf(w)}_u${w.unit}.ndjson`)
  if (existsSync(file)) return file

  const records = labelled(w).map((r) => r.line)
  const lines: string[] = []
  for (let i = 0; i < records.length; i += w.unit) {
    lines.push(w.unit === 1 ? records[i] : `[${records.slice(i, i + w.unit).join(',')}]`)
  }
  writeFileSync(file, lines.join('\n') + '\n')
  return file
}

/**
 * Every record mapped, nothing validated: one rml-map --batch run, cached per
 * (count, size, invalid). The reference data is cut from this.
 */
async function mappedQuads(w: AnchorWorkload): Promise<Set<string>> {
  const out = join(DATA, `mapped_${tagOf(w)}.ndjson`)
  if (!existsSync(out)) {
    const run = await runPipe(`${RML} --mapping ${MAPPING} --batch`, anchorInput({ ...w, unit: 1 }), out)
    if (run.code !== 0) throw new Error(`mapping for the reference failed (${tagOf(w)}):\n${run.stderr}`)
  }
  return quadSet(out)
}

export type Reference = { data: Digest; reports: number }

/**
 * What a correct run outputs, derived from which records the generator broke
 * rather than from any arm: a unit is forwarded iff none of its records is
 * broken, and yields one report otherwise. The data is the mapped quads of the
 * forwarded records (every quad's subject carries its record id).
 *
 * `unit` is the grouping the arm validates with: the workload's unit for
 * streaming arms and CWL-scatter, all records for CWL-batch.
 */
export async function anchorReference(w: AnchorWorkload, unit: number): Promise<Reference> {
  const file = join(DATA, `expected_${tagOf(w)}_u${unit}.json`)
  if (existsSync(file)) return JSON.parse(readFileSync(file, 'utf8'))

  const records = labelled(w)
  const kept = new Set<string>()
  let reports = 0
  for (let i = 0; i < records.length; i += unit) {
    const group = records.slice(i, i + unit)
    if (group.every((r) => r.valid)) group.forEach((r) => kept.add(r.id))
    else reports++
  }

  const data = [...(await mappedQuads(w))].filter((q) => {
    const m = q.match(/^<http:\/\/ex\.org\/obs\/(obs-\d+)[/>]/)
    if (!m) throw new Error(`reference: quad without a record id: ${q}`)
    return kept.has(m[1])
  })

  const ref = { data: digestOf(data), reports }
  writeFileSync(file, JSON.stringify(ref))
  return ref
}
