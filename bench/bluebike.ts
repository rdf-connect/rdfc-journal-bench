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

export type Arm = 'shell' | 'rdfc' | 'cwl-batch' | 'cwl-scatter'

/** Files every arm needs beside the snapshots: the member shape and the query. */
export function prepareSideInputs(runDir: string) {
  const shape = join(runDir, 'shape.ndjson')
  const focus = join(runDir, 'focus.ndjson')
  writeFileSync(shape, JSON.stringify(readFileSync(SHAPES, 'utf8')) + '\n')
  writeFileSync(focus, JSON.stringify(FOCUS_QUERY) + '\n')
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

/** The CWL arms, run by cwltool without containers. */
export function cwlCommand(kind: 'batch' | 'scatter', input: string, runDir: string, unit: number): string {
  if (!existsSync(CWLTOOL)) {
    throw new Error(`missing ${CWLTOOL}: python3 -m venv .venv && .venv/bin/pip install cwltool`)
  }
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
  return (
    `${CWLTOOL} --no-container --parallel --timestamps --preserve-environment JAVA_OPTS` +
    ` --outdir ${runDir}/final --tmpdir-prefix ${runDir}/cwl-tmp/ --tmp-outdir-prefix ${runDir}/cwl-out/` +
    ` ${CWL}/bluebike-${kind}.cwl ${job} 2> ${runDir}/cwltool.log`
  )
}

/** Where each arm leaves the published LDES. */
export function ldesDir(arm: Arm, runDir: string): string {
  return arm === 'shell' || arm === 'rdfc'
    ? join(runDir, 'ldes-output')
    : join(runDir, 'final', 'ldes-output')
}
