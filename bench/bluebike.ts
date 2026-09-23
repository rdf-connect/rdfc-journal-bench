/**
 * The Blue-bike pipeline (experiment 2, deep) for every arm:
 *
 *   snapshots → rml-map → Validate → DumpsToFeed → Sdsify → Bucketize
 *             → LdesDiskWriter
 *
 * Arms run the same published processors over the same recorded snapshots, and
 * are checked against an oracle that does not involve any of them: a member of
 * the feed is a station report, whose IRI carries the station id and its
 * `last_seen`, so the members a correct run publishes are exactly the distinct
 * (id, last_seen) pairs in the input. Timestamps the pipeline invents
 * (`as:published`, SDS transaction ids) are therefore never compared.
 */
import { existsSync, mkdirSync, readFileSync, readdirSync, writeFileSync } from 'fs'
import { join, resolve } from 'path'

const ROOT = resolve(import.meta.dirname, '..', '..')
const DATA = join(ROOT, 'data')

const RML = join(ROOT, 'rml-processor-jvm', 'bin', 'rml-map')
const PROC = join(ROOT, 'dist', 'bench', 'rdfc-proc.js')
const STEPS = join(ROOT, 'steps')
const CWL = join(ROOT, 'cwl')
const CWLTOOL = join(ROOT, '.venv', 'bin', 'cwltool')
const MAPPING = join(ROOT, 'anchor', 'bluebike.rml.ttl')
/** Quiets the mapper's per-record warnings; see bench/logback-quiet.xml. */
export const JAVA_OPTS = `-Dlogback.configurationFile=${join(ROOT, 'bench', 'logback-quiet.xml')}`
const SHAPES = join(ROOT, 'anchor', 'bluebike-shapes.ttl')

export const FOCUS_QUERY = `PREFIX hubs: <https://purl.eu/ns/mobility/passenger-transport-hubs#>
SELECT DISTINCT ?entity WHERE { ?entity a hubs:ResourceReport }`

export type Station = { id: number; last_seen: string | null }
type Snapshot = { fetchedAt: string; stations: Station[] }

/** The recorded archive, newest recording first. */
export function archive(): string {
  const files = readdirSync(DATA)
    .filter((f) => f.startsWith('bluebike-') && f.endsWith('.ndjson'))
    .sort()
  if (!files.length) {
    throw new Error(`no recording in ${DATA}: run bench/record-bluebike.ts first`)
  }
  return join(DATA, files[files.length - 1])
}

export function snapshots(n: number): Snapshot[] {
  const lines = readFileSync(archive(), 'utf8').split('\n').filter(Boolean)
  if (lines.length < n) {
    throw new Error(`archive holds ${lines.length} snapshots, ${n} requested`)
  }
  return lines.slice(0, n).map((l) => JSON.parse(l))
}

/** Input for a run: one snapshot per line, as the mapper reads it. */
export function prepareInput(n: number, runDir: string): string {
  const file = join(runDir, 'snapshots.ndjson')
  writeFileSync(file, snapshots(n).map((s) => JSON.stringify(s.stations)).join('\n') + '\n')
  return file
}

/**
 * The members a correct run publishes: one per distinct (station, last_seen),
 * as the deployed mapping's subject template builds them. Derived from the
 * input, never from a pipeline's output.
 */
export function expectedMembers(n: number): Set<string> {
  const members = new Set<string>()
  for (const snap of snapshots(n)) {
    for (const station of snap.stations) {
      if (station.last_seen) members.add(`${station.id}#${station.last_seen}`)
    }
  }
  return members
}

/**
 * What a correct run publishes, derived from the input alone: one Create per
 * distinct member, and one Update whenever a member's station data changes
 * between consecutive snapshots.
 */
export function expectedActivities(n: number): { creates: number; updates: number } {
  const snaps = snapshots(n)
  const seen = new Set<string>()
  let creates = 0
  let updates = 0
  let previous = new Map<number, string>()

  for (const snap of snaps) {
    const current = new Map<number, string>()
    for (const station of snap.stations) {
      if (!station.last_seen) continue // no member is minted without one
      const member = `${station.id}#${station.last_seen}`
      const content = JSON.stringify(station)
      current.set(station.id, content)
      if (!seen.has(member)) {
        seen.add(member)
        creates++
      } else if (previous.get(station.id) !== content) {
        updates++
      }
    }
    previous = current
  }
  return { creates, updates }
}

/** The activities a run published, counted from the LDES on disk. */
export function publishedActivities(ldesDir: string): { creates: number; updates: number } {
  let creates = 0
  let updates = 0
  const walk = (dir: string) => {
    if (!existsSync(dir)) return
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const path = join(dir, entry.name)
      if (entry.isDirectory()) walk(path)
      else if (entry.name.endsWith('.trig')) {
        const text = readFileSync(path, 'utf8')
        creates += text.match(/activitystreams#Create/g)?.length ?? 0
        updates += text.match(/activitystreams#Update/g)?.length ?? 0
      }
    }
  }
  walk(ldesDir)
  return { creates, updates }
}

/** The members a run actually published, read back from the LDES on disk. */
export function publishedMembers(ldesDir: string): Set<string> {
  const members = new Set<string>()
  const walk = (dir: string) => {
    if (!existsSync(dir)) return
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const path = join(dir, entry.name)
      if (entry.isDirectory()) walk(path)
      else if (entry.name.endsWith('.trig')) {
        const text = readFileSync(path, 'utf8')
        // The timestamp is percent-encoded in the IRI (%3A for ':').
        for (const m of text.matchAll(/resourcereports\/(\d+)#([^>\s]+)>/g)) {
          members.add(`${m[1]}#${decodeURIComponent(m[2])}`)
        }
      }
    }
  }
  walk(ldesDir)
  return members
}

export type Arm =
  | 'shell'
  | 'rdfc'
  | 'cwl-batch'
  | 'cwl-scatter'
  | 'toil-batch'
  | 'toil-scatter'
  | 'streamflow-batch'
  | 'streamflow-scatter'
  | 'nextflow'

/** The CWL runners: the same workflows, so differences are the runner. */
export type Runner = 'cwl' | 'toil' | 'streamflow'
export type Encoding = 'batch' | 'scatter'

export function splitArm(arm: Arm): { runner: Runner; encoding: Encoding } | null {
  const m = arm.match(/^(cwl|toil|streamflow)-(batch|scatter)$/)
  return m ? { runner: m[1] as Runner, encoding: m[2] as Encoding } : null
}

/**
 * Files every arm needs beside the snapshots: the member shape and the query
 * that selects members.
 *
 * One message of each per snapshot: DumpsToFeed consumes a shape and a query
 * alongside every dump, and holds back any dump that arrives without them.
 */
export function prepareSideInputs(runDir: string, n: number) {
  const shape = join(runDir, 'shape.ndjson')
  const focus = join(runDir, 'focus.ndjson')
  const line = (x: string) => JSON.stringify(x) + '\n'
  writeFileSync(shape, line(readFileSync(SHAPES, 'utf8')).repeat(n))
  writeFileSync(focus, line(FOCUS_QUERY).repeat(n))
  writeFileSync(join(runDir, 'focus.rq'), FOCUS_QUERY)
  mkdirSync(join(runDir, 'feed-state-seed'), { recursive: true })
  return { shape, focus }
}

const step = (name: string) => join(STEPS, name)

/**
 * The shell arm: the six stages in one pipe. The side channels that carry
 * metadata between stages are FIFOs, so every stage runs concurrently, as in a
 * streaming system.
 */
export function shellCommand(input: string, runDir: string): string {
  const { shape, focus } = { shape: join(runDir, 'shape.ndjson'), focus: join(runDir, 'focus.ndjson') }
  const fifo = (n: string) => join(runDir, n)
  return [
    `cd ${runDir}`,
    `rm -f ${fifo('meta.fifo')} ${fifo('bmeta.fifo')}`,
    `mkfifo ${fifo('meta.fifo')} ${fifo('bmeta.fifo')}`,
    `${RML} --mapping ${MAPPING} < ${input} 2>/dev/null` +
      ` | node ${PROC} --config ${step('validate-bluebike.ttl')} --stdin in --stdout out --write report=${runDir}/report.ndjson 2>/dev/null` +
      ` | node ${PROC} --config ${step('dumps-to-feed.ttl')} --stdin in --read shape=${shape} --read focusNodes=${focus} --stdout out 2>/dev/null` +
      ` | node ${PROC} --config ${step('sdsify-feed.ttl')} --stdin in --stdout out --write metadata=${fifo('meta.fifo')} 2>/dev/null` +
      ` | node ${PROC} --config ${step('bucketize.ttl')} --stdin in --read metadataIn=${fifo('meta.fifo')} --stdout out --write metadataOut=${fifo('bmeta.fifo')} 2>/dev/null` +
      ` | node ${PROC} --config ${step('ldes-writer.ttl')} --stdin in --read metadataIn=${fifo('bmeta.fifo')} 2>${runDir}/writer.log`,
  ].join('; ')
}

const NEXTFLOW = join(ROOT, 'vendor', 'nextflow')

/**
 * Nextflow: the closest competitor to a streaming system among the workflow
 * managers. A process still runs per item, but channels let stages overlap, so
 * a snapshot is mapped while the previous one is validated — which CWL's
 * scatter cannot do. The stateful stages still run once over the gathered
 * stream, and the gather has to be ordered explicitly: channels are unordered,
 * and change detection compares each snapshot with the one before it.
 */
export function nextflowCommand(input: string, runDir: string, unit: number): string {
  if (!existsSync(NEXTFLOW)) {
    throw new Error(`missing Nextflow: curl -s https://get.nextflow.io -o vendor/nextflow && chmod +x vendor/nextflow`)
  }
  return (
    `cd ${runDir} && NXF_HOME=${join(ROOT, 'vendor', '.nextflow')} ${NEXTFLOW} -quiet run` +
    ` ${join(ROOT, 'nextflow', 'bluebike.nf')}` +
    ` --snapshots ${input} --mapping ${MAPPING}` +
    ` --shape ${join(runDir, 'shape.ndjson')} --focus ${join(runDir, 'focus.ndjson')}` +
    ` --seed ${join(runDir, 'feed-state-seed')} --steps ${STEPS} --unit ${unit}` +
    ` --outdir ${runDir}/final -w ${runDir}/work 2> ${runDir}/nextflow.log`
  )
}

const TOIL_BIN = join(ROOT, '.venv-toil', 'bin')
const STREAMFLOW_BIN = join(ROOT, '.venv-streamflow', 'bin')

/** PATH a runner needs: the tools, and its own helper executables. */
export function runnerPath(runner: Runner): string {
  const bin = { cwl: '', toil: `:${TOIL_BIN}`, streamflow: `:${STREAMFLOW_BIN}` }[runner]
  return `${join(ROOT, 'cwl', 'bin')}${bin}`
}

/** The job document, shared by all three runners. */
function writeJob(kind: Encoding, input: string, runDir: string, unit: number): string {
  const job = join(runDir, 'job.yml')
  writeFileSync(
    job,
    [
      `snapshots: { class: File, path: ${input} }`,
      `mapping: { class: File, path: ${MAPPING} }`,
      `shape: { class: File, path: ${join(runDir, 'shape.ndjson')} }`,
      `focus: { class: File, path: ${join(runDir, 'focus.ndjson')} }`,
      `feedState: { class: Directory, path: ${join(runDir, 'feed-state-seed')} }`,
      `validateStep: "${step('validate-bluebike.ttl')}"`,
      `changesStep: "${step('dumps-to-feed.ttl')}"`,
      `sdsifyStep: "${step('sdsify-feed.ttl')}"`,
      `bucketizeStep: "${step('bucketize.ttl')}"`,
      `writerStep: "${step('ldes-writer.ttl')}"`,
      ...(kind === 'scatter' ? [`unit: ${unit}`] : []),
      '',
    ].join('\n'),
  )
  return job
}

/**
 * The CWL arms. The three runners execute the same workflow documents, so a
 * difference between them is the runner, not the description.
 *
 *   cwltool     the reference implementation
 *   Toil        a production runner; retries off, or it reruns failed jobs
 *               with more memory and hides the failure in the timing
 *   StreamFlow  bound to a local deployment; it ignores TMPDIR, so its
 *               working directory is set in the generated configuration
 */
export function cwlCommand(
  runner: Runner,
  kind: Encoding,
  input: string,
  runDir: string,
  unit: number,
): string {
  const job = writeJob(kind, input, runDir, unit)
  const workflow = `${CWL}/bluebike-${kind}.cwl`
  const outdir = `${runDir}/final`

  if (runner === 'cwl') {
    if (!existsSync(CWLTOOL)) {
      throw new Error(`missing ${CWLTOOL}: python3 -m venv .venv && .venv/bin/pip install cwltool`)
    }
    return (
      `${CWLTOOL} --no-container --parallel --timestamps --preserve-environment JAVA_OPTS` +
      ` --outdir ${outdir} --tmpdir-prefix ${runDir}/cwl-tmp/ --tmp-outdir-prefix ${runDir}/cwl-out/` +
      ` ${workflow} ${job} 2> ${runDir}/cwltool.log`
    )
  }

  if (runner === 'toil') {
    const runner = join(TOIL_BIN, 'toil-cwl-runner')
    if (!existsSync(runner)) {
      throw new Error('missing Toil: uv venv -p 3.14 .venv-toil && uv pip install -p .venv-toil "toil[cwl]"')
    }
    return (
      `mkdir -p ${runDir}/toil-work && ${runner} --no-container --retryCount 0` +
      ` --preserve-environment JAVA_OPTS` +
      ` --jobStore ${runDir}/toil-jobstore --workDir ${runDir}/toil-work --outdir ${outdir}` +
      ` --logFile ${runDir}/toil.log ${workflow} ${job} 2> ${runDir}/toil.stderr`
    )
  }

  const sfRunner = join(STREAMFLOW_BIN, 'cwl-runner')
  if (!existsSync(sfRunner)) {
    throw new Error(
      'missing StreamFlow: uv venv -p 3.12 .venv-streamflow && ' +
        'uv pip install -p .venv-streamflow --prerelease=allow "streamflow==0.2.0rc3"',
    )
  }
  const config = join(runDir, 'streamflow.yml')
  writeFileSync(
    config,
    [
      'version: v1.0',
      'workflows:',
      '  bluebike:',
      '    type: cwl',
      '    config:',
      `      file: ${workflow}`,
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
    `mkdir -p ${runDir}/sf-tmp && ${sfRunner} --streamflow-file ${config} --outdir ${outdir}` +
    ` ${workflow} ${job} 2> ${runDir}/streamflow.log`
  )
}

/**
 * Where an arm leaves the published LDES. The runners lay out --outdir
 * differently (StreamFlow nests every output in a directory of its own), so
 * for those the whole output directory is searched.
 */
export function ldesDir(arm: Arm, runDir: string): string {
  return arm === 'shell' || arm === 'rdfc' ? join(runDir, 'ldes-output') : join(runDir, 'final')
}
