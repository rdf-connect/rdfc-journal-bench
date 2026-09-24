/**
 * Processors for the RDF-Connect arm of the anchor pipeline (experiment 2):
 *
 *   AnchorSource → RmlMapper (JVM) → Validate (JS) → AnchorSink
 *
 * They are the RDF-Connect counterparts of what the shell arm gets from `sh`:
 * the source plays `< input` and the sink plays `> output`. Both do the same
 * framing work as the CLIs, so the arms differ only in how the stages are
 * connected.
 */
import { Processor, type Reader, type Writer } from '@rdfc/js-runner'
import { createReadStream, createWriteStream, readFileSync, writeFileSync } from 'fs'
import { once } from 'events'
import { createInterface } from 'readline'
import { nowAbs } from './processors.js'

/**
 * The records on one input line as a JSON array: a line that already is an
 * array is kept, a single record is wrapped. Mirrors `RmlMap.arrayBody` in
 * rml-map, so both arms feed the mapper the same bytes.
 */
export function asArray(line: string): string {
  const t = line.trim()
  return t.startsWith('[') && t.endsWith(']') ? t : `[${t}]`
}

type SourceArgs = {
  /** Channel read by the RML mapper as `rdfc:mappings`. */
  mappingWriter: Writer
  /** Channel read by the RML mapper as its triggering source. */
  writer: Writer
  mappingPath: string
  /** NDJSON input: one unit (a record or an array of records) per line. */
  input: string
  resultPath: string
}

/**
 * Sends the mapping, then every input line as one message.
 *
 * The mapping goes first and is awaited: RmlMapper caches only the latest
 * message of a source, so data that arrived before the mapping would be
 * mapped once, as a single record, when the mapping comes in.
 */
export class AnchorSource extends Processor<SourceArgs> {
  async init(): Promise<void> {}
  async transform(): Promise<void> {}

  async produce(this: SourceArgs & this): Promise<void> {
    const encoder = new TextEncoder()

    await this.mappingWriter.buffer(readFileSync(this.mappingPath))
    await this.mappingWriter.close()

    let count = 0
    let firstAbs = 0
    const lines = createInterface({ input: createReadStream(this.input), crlfDelay: Infinity })
    for await (const line of lines) {
      if (!line.trim()) continue
      if (count === 0) firstAbs = nowAbs()
      await this.writer.buffer(encoder.encode(asArray(line)))
      count++
    }
    const endAbs = nowAbs()
    await this.writer.close()

    writeFileSync(
      this.resultPath,
      JSON.stringify({ role: 'source', count, firstAbs, endAbs }, null, 2),
    )
  }
}

type BluebikeSourceArgs = {
  /** Channel read by the RML mapper as `rdfc:mappings`. */
  mappingWriter: Writer
  /** Channel read by the RML mapper as its triggering source. */
  writer: Writer
  /** The member shape and the query selecting members, for DumpsToFeed. */
  shapeWriter: Writer
  focusWriter: Writer
  mappingPath: string
  shapePath: string
  focusPath: string
  input: string
  resultPath: string
  /** Milliseconds between snapshots; 0 sends them as fast as they are read. */
  arrivalMs?: number
}

/**
 * Source of the Blue-bike pipeline: the mapping once, then every snapshot.
 *
 * The member shape and the focus-node query go out with each snapshot, because
 * DumpsToFeed consumes one of each per dump and holds back a dump that arrives
 * without them. The command-line arms do the same, one message per line.
 */
export class BluebikeSource extends Processor<BluebikeSourceArgs> {
  async init(): Promise<void> {}
  async transform(): Promise<void> {}

  async produce(this: BluebikeSourceArgs & this): Promise<void> {
    const encoder = new TextEncoder()
    const shape = readFileSync(this.shapePath)
    const focus = readFileSync(this.focusPath)

    await this.mappingWriter.buffer(readFileSync(this.mappingPath))
    await this.mappingWriter.close()

    let count = 0
    let firstAbs = 0
    // When the source is paced, each snapshot's arrival is recorded, so that
    // the freshness experiment can measure publication against arrival.
    const arrivals: number[] = []
    const pace = Number(this.arrivalMs ?? 0)
    const started = nowAbs()

    const lines = createInterface({ input: createReadStream(this.input), crlfDelay: Infinity })
    for await (const line of lines) {
      if (!line.trim()) continue
      if (pace > 0) {
        const due = started + count * pace
        const wait = due - nowAbs()
        if (wait > 0) await new Promise((res) => setTimeout(res, wait))
      }
      if (count === 0) firstAbs = nowAbs()
      arrivals.push(nowAbs())
      await this.shapeWriter.buffer(shape)
      await this.focusWriter.buffer(focus)
      await this.writer.buffer(encoder.encode(asArray(line)))
      count++
    }
    const endAbs = nowAbs()
    await Promise.all([this.writer.close(), this.shapeWriter.close(), this.focusWriter.close()])

    writeFileSync(
      this.resultPath,
      JSON.stringify({ role: 'source', count, firstAbs, endAbs, arrivals }, null, 2),
    )
  }
}

type SinkArgs = {
  reader: Reader
  /** NDJSON output, one JSON string of N-Quads per message, like the CLIs. */
  outPath: string
  resultPath: string
}

/** Writes every message out and timestamps first and last arrival. */
export class AnchorSink extends Processor<SinkArgs> {
  async init(): Promise<void> {}

  async transform(this: SinkArgs & this): Promise<void> {
    const out = createWriteStream(this.outPath)
    let count = 0
    let bytes = 0
    let firstAbs = 0
    let lastAbs = 0

    for await (const msg of this.reader.strings()) {
      if (count === 0) firstAbs = nowAbs()
      count++
      bytes += msg.length
      if (!out.write(JSON.stringify(msg) + '\n')) await once(out, 'drain')
      lastAbs = nowAbs()
    }
    await new Promise((res) => out.end(res))

    writeFileSync(
      this.resultPath,
      JSON.stringify(
        { role: 'sink', count, bytes, firstAbs, lastAbs, spanMs: lastAbs - firstAbs },
        null,
        2,
      ),
    )
  }

  async produce(): Promise<void> {}
}
