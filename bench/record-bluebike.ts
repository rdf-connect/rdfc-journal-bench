#!/usr/bin/env node
/**
 * Records snapshots of the Blue-bike availability API into an NDJSON archive,
 * so the benchmark replays real data instead of depending on a live service.
 *
 *   node dist/bench/record-bluebike.js --out data/bluebike.ndjson [--every=60] [--for=24h]
 *
 * This is the source the deployed CA-Blue-Bike-LDES pipeline consumes
 * (https://api.blue-bike.be/pub/location); that pipeline polls it every 15 s.
 * One snapshot is an array of ~330 station records, so one line is a natural
 * unit of work; `bench/anchor.ts` regroups them for the records-per-unit sweep.
 *
 * Every line is one snapshot:
 *   {"fetchedAt": "<ISO>", "stations": [ ... ]}
 *
 * Appends, so a longer archive is made by running it again, and it never
 * rewrites what it has already recorded.
 */
import { appendFileSync, mkdirSync } from 'fs'
import { dirname } from 'path'
import { parseArgs } from 'util'

const API = 'https://api.blue-bike.be/pub/location'
const UA = 'RDF-Connect benchmark (research, IDLab Ghent University; polls once per minute)'

function seconds(spec: string): number {
  const m = spec.match(/^(\d+(?:\.\d+)?)([smhd]?)$/)
  if (!m) throw new Error(`cannot read duration '${spec}'`)
  const mult = { s: 1, m: 60, h: 3600, d: 86400, '': 1 }[m[2]]!
  return Number(m[1]) * mult
}

async function snapshot(): Promise<unknown[]> {
  const res = await fetch(API, { headers: { 'User-Agent': UA } })
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`)
  const body = await res.json()
  if (!Array.isArray(body)) throw new Error('expected a JSON array of stations')
  return body
}

async function main() {
  const { values } = parseArgs({
    options: {
      out: { type: 'string' },
      every: { type: 'string', default: '60' },
      for: { type: 'string', default: '24h' },
    },
  })
  if (!values.out) {
    console.error('Usage: record-bluebike --out <file.ndjson> [--every=60] [--for=24h]')
    process.exit(2)
  }
  const everyMs = seconds(values.every!) * 1000
  const until = Date.now() + seconds(values.for!) * 1000
  mkdirSync(dirname(values.out), { recursive: true })

  let polls = 0
  let records = 0
  let failures = 0
  while (Date.now() < until) {
    const started = Date.now()
    try {
      const stations = await snapshot()
      appendFileSync(
        values.out,
        JSON.stringify({ fetchedAt: new Date().toISOString(), stations }) + '\n',
      )
      polls++
      records += stations.length
      if (polls % 10 === 0) {
        console.error(`${polls} snapshots, ${records} records, ${failures} failures`)
      }
    } catch (err) {
      // A recording that drops a poll is still usable; a recording that stops is not.
      failures++
      console.error(`poll failed: ${(err as Error).message}`)
    }
    const wait = everyMs - (Date.now() - started)
    if (wait > 0) await new Promise((res) => setTimeout(res, wait))
  }
  console.error(`done: ${polls} snapshots, ${records} records, ${failures} failures`)
}

main().catch((err) => {
  console.error(`record-bluebike: ${err?.stack ?? err}`)
  process.exitCode = 1
})
