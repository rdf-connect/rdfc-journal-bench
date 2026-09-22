/**
 * Anchor workload: sensor observations as NDJSON, one JSON record per line.
 *
 *   node dist/bench/gen-records.js --n=1000 [--size=300] [--invalid=0.05] [--seed=1]
 *
 * Deterministic for a given seed. `--size` pads the free-text `note` so every
 * record is at least that many bytes of JSON. A fraction `--invalid` of the
 * records is broken in a way anchor/shapes.ttl rejects, so the SHACL stage
 * actually filters something.
 */
import { parseArgs } from 'util'
import { pathToFileURL } from 'url'

export type GenOptions = {
  n: number
  size?: number
  invalid?: number
  seed?: number
}

const PROPERTIES: [string, string][] = [
  ['airTemperature', 'DEG_C'],
  ['relativeHumidity', 'PERCENT'],
  ['windSpeed', 'M-PER-SEC'],
  ['airPressure', 'HectoPA'],
  ['no2Concentration', 'MicroGM-PER-M3'],
]

// Each kind of breakage trips a different constraint.
const BREAKAGE: ((r: Record<string, unknown>) => void)[] = [
  (r) => (r.value = 'n/a'), // sh:datatype xsd:double
  (r) => delete r.time, // sh:minCount on sosa:resultTime
  (r) => (r.lat = 'north'), // sh:pattern on geo:asWKT
]

function mulberry32(seed: number) {
  return () => {
    seed |= 0
    seed = (seed + 0x6d2b79f5) | 0
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

export function* generateRecords(opts: GenOptions): Generator<string> {
  for (const { line } of generateLabelledRecords(opts)) yield line
}

/** The same records, each with its id and whether it was left valid. */
export function* generateLabelledRecords(
  opts: GenOptions,
): Generator<{ id: string; line: string; valid: boolean }> {
  const { n, size = 0, invalid = 0.05, seed = 1 } = opts
  const rand = mulberry32(seed)
  const t0 = Date.UTC(2026, 0, 1)

  for (let i = 0; i < n; i++) {
    const [property, unit] = PROPERTIES[Math.floor(rand() * PROPERTIES.length)]
    const record: Record<string, unknown> = {
      id: `obs-${String(i).padStart(7, '0')}`,
      sensor: `s-${String(Math.floor(rand() * 200)).padStart(3, '0')}`,
      property,
      time: new Date(t0 + i * 1000).toISOString(),
      value: Math.round(rand() * 100000) / 100,
      unit,
      lat: Math.round((50.7 + rand() * 0.8) * 1e5) / 1e5,
      lon: Math.round((2.5 + rand() * 3.4) * 1e5) / 1e5,
      note: '',
    }
    const valid = !(rand() < invalid)
    if (!valid) {
      BREAKAGE[Math.floor(rand() * BREAKAGE.length)](record)
    }

    const pad = size - JSON.stringify(record).length
    if (pad > 0) record.note = 'x'.repeat(pad)
    yield { id: record.id as string, line: JSON.stringify(record), valid }
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const { values } = parseArgs({
    options: {
      n: { type: 'string', default: '1000' },
      size: { type: 'string', default: '0' },
      invalid: { type: 'string', default: '0.05' },
      seed: { type: 'string', default: '1' },
    },
  })
  const records = generateRecords({
    n: Number(values.n),
    size: Number(values.size),
    invalid: Number(values.invalid),
    seed: Number(values.seed),
  })
  for (const line of records) process.stdout.write(line + '\n')
}
