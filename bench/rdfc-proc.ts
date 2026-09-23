#!/usr/bin/env node
/**
 * rdfc-proc: run any RDF-Connect JS processor as a command-line tool.
 *
 *   rdfc-proc --config step.ttl --processor <iri>
 *             [--stdin <channel>] [--stdout <channel>]
 *             [--read <channel>=<file>] [--write <channel>=<file>]
 *
 * The shell-pipe and CWL arms need one executable per pipeline stage. Writing a
 * wrapper per processor would mean re-implementing each stage, and the
 * benchmark would no longer compare the same code. This adapter instead
 * configures and drives the published processor exactly as the framework does:
 *
 *   - the step is described in Turtle, like a pipeline, and its `owl:imports`
 *     are followed (so a processor's own processors.ttl supplies its shapes);
 *   - arguments are materialised with rdf-lens through those SHACL shapes, the
 *     way js-runner's `Runner.handlePipeline` / `createProcessor` do, which is
 *     what makes arguments like a bucketiser's fragmentation strategy (an RDF
 *     subgraph, not a value) work at all;
 *   - channels become the in-memory Reader/Writer of experiment 1
 *     (src/inmem.ts), which keep the js-runner's semantics;
 *   - the lifecycle is the runner's: init, then transform (not awaited), then
 *     produce, then feed the inputs and await the rest.
 *
 * Channels named on the command line are bound to stdin, stdout or a file;
 * every other channel in the description is bound to a sink that discards.
 * A channel is matched by full IRI or by its last path/fragment segment.
 *
 * Framing is the benchmark's NDJSON: one message per line, each line a JSON
 * string. Anything the processor itself writes to stdout (some log to it
 * directly) is diverted to stderr, so the data channel stays parsable.
 */
import { createInterface } from 'readline'
import { createReadStream, createWriteStream, readFileSync } from 'fs'
import { once } from 'events'
import { Writable } from 'stream'
import { parseArgs } from 'util'
import { pathToFileURL, fileURLToPath } from 'url'
import { resolve } from 'path'
import { NamedNode, Parser } from 'n3'
import type { Quad, Term } from '@rdfjs/types'
import { empty, extractShapes } from 'rdf-lens'
import { createLogger, format, transports, type Logger } from 'winston'
import type { Any, Handler, Reader, Writer } from '@rdfc/js-runner'
import { MemoryReader, memoryChannel } from '../src/inmem.js'

const RDFC = 'https://w3id.org/rdf-connect#'
const RDFL = 'https://w3id.org/rdf-lens/ontology#'
const OWL_IMPORTS = 'http://www.w3.org/2002/07/owl#imports'
const RDF_TYPE = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'

const USAGE = `Usage: rdfc-proc --config <step.ttl> --processor <iri> [bindings]

Runs one RDF-Connect JS processor, configured from an RDF description.

  --config <file>        Turtle describing the processor instance (owl:imports followed)
  --processor <iri>      the instance to run (default: the only processor described)
  --stdin <channel>      bind this channel to stdin
  --stdout <channel>     bind this channel to stdout
  --read <channel>=<f>   bind this channel to an input file
  --write <channel>=<f>  bind this channel to an output file
  -h, --help             show this help

Channels are NDJSON: one message per line, each line a JSON string.`

const encoder = new TextEncoder()
const decoder = new TextDecoder()

// ── Writers ──────────────────────────────────────────────────────────────────

/**
 * The real stdout, captured before the processor is loaded: a processor that
 * logs to stdout would otherwise corrupt the data channel, so `main` diverts
 * process.stdout to stderr and data is written through this.
 */
const stdoutWrite = process.stdout.write.bind(process.stdout)

/** A Writer that appends every message to a stream as one NDJSON line. */
class NdjsonWriter implements Writer {
  private closed = false
  private _canceled = false
  private readonly cancelHandlers = new Set<Handler>()

  /** `out` is null for an unbound channel, or 'stdout' for the data channel. */
  constructor(
    readonly uri: string,
    private readonly out: Writable | 'stdout' | null,
  ) {}

  get canceled(): boolean {
    return this._canceled
  }

  on(event: 'cancel', listener: Handler): this {
    if (event === 'cancel') this.cancelHandlers.add(listener)
    return this
  }

  private async write(msg: string): Promise<void> {
    if (this.closed) throw new Error(`Writer for ${this.uri} is closed`)
    if (!this.out) return // unbound channel: the message is discarded
    const line = JSON.stringify(msg) + '\n'
    if (this.out === 'stdout') {
      if (!stdoutWrite(line)) await once(process.stdout, 'drain')
      return
    }
    if (!this.out.write(line)) await once(this.out, 'drain')
  }

  async string(msg: string): Promise<void> {
    await this.write(msg)
  }

  async buffer(buffer: Uint8Array): Promise<void> {
    await this.write(decoder.decode(buffer))
  }

  async stream<T = Uint8Array>(
    buffer: AsyncIterable<T>,
    transform?: (x: T) => Uint8Array,
  ): Promise<void> {
    const t = transform || ((x: unknown) => <Uint8Array>x)
    const parts: string[] = []
    for await (const chunk of buffer) parts.push(decoder.decode(t(chunk)))
    await this.write(parts.join(''))
  }

  async any(any: Any): Promise<void> {
    if ('stream' in any) return this.stream(any.stream)
    if ('buffer' in any) return this.buffer(any.buffer)
    if ('string' in any) return this.string(any.string)
  }

  async close(issued = false): Promise<void> {
    if (this.closed) return
    this.closed = true
    if (issued && !this._canceled) {
      this._canceled = true
      await Promise.all([...this.cancelHandlers].map((h) => h()))
    }
    if (this.out && this.out !== 'stdout') {
      await new Promise((res) => (this.out as Writable).end(res))
    }
  }
}

// ── Description ──────────────────────────────────────────────────────────────

/** Parses a Turtle file and everything it `owl:imports`, as the runner does. */
function importFile(file: string): Quad[] {
  const done = new Set<string>()
  const todo = [pathToFileURL(resolve(file))]
  const quads: Quad[] = []

  for (let item = todo.pop(); item !== undefined; item = todo.pop()) {
    if (done.has(item.toString())) continue
    done.add(item.toString())
    if (item.protocol !== 'file:') throw new Error(`unsupported protocol ${item.protocol}`)

    const text = readFileSync(fileURLToPath(item), 'utf8')
    const extra = new Parser({ baseIRI: item.toString() }).parse(text)
    for (const q of extra) {
      if (q.subject.value === item.toString() && q.predicate.value === OWL_IMPORTS) {
        todo.push(new URL(q.object.value))
      }
    }
    quads.push(...extra)
  }
  return quads
}

/** A channel binding given on the command line, matched by IRI or last segment. */
type Binding = { kind: 'stdin' | 'stdout' | 'read' | 'write'; channel: string; file?: string }

function matches(binding: Binding, iri: string): boolean {
  if (binding.channel === iri) return true
  const tail = iri.split(/[#/]/).pop()
  return !!tail && tail === binding.channel
}

/**
 * The processor's logger. The js-runner ships log records to the orchestrator;
 * here they go to stderr, so stdout carries only data.
 */
function stderrLogger(uri: string): Logger {
  return createLogger({
    level: process.env.LOG_LEVEL ?? 'warn',
    defaultMeta: { processor: uri },
    format: format.combine(format.timestamp(), format.simple()),
    transports: [new transports.Stream({ stream: process.stderr })],
  })
}

type Lifecycle = { init(): Promise<void>; transform(): Promise<void>; produce(): Promise<void> }

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const { values } = parseArgs({
    options: {
      config: { type: 'string' },
      processor: { type: 'string' },
      stdin: { type: 'string' },
      stdout: { type: 'string' },
      read: { type: 'string', multiple: true, default: [] },
      write: { type: 'string', multiple: true, default: [] },
      help: { type: 'boolean', short: 'h', default: false },
    },
  })
  if (values.help || !values.config) {
    console.log(USAGE)
    process.exit(values.help ? 0 : 2)
  }

  const bindings: Binding[] = []
  if (values.stdin) bindings.push({ kind: 'stdin', channel: values.stdin })
  if (values.stdout) bindings.push({ kind: 'stdout', channel: values.stdout })
  for (const spec of [...values.read!, ...values.write!]) {
    const eq = spec.indexOf('=')
    if (eq < 0) throw new Error(`--read/--write expect <channel>=<file>, got '${spec}'`)
    const channel = spec.slice(0, eq)
    const file = spec.slice(eq + 1)
    bindings.push({
      kind: values.read!.includes(spec) ? 'read' : 'write',
      channel,
      file,
    })
  }

  const quads = importFile(values.config)

  // Which instance to run: the one named, or the only one with a type that is
  // declared as a JS processor.
  const jsTypes = new Set(
    quads.filter((q) => q.predicate.value === RDFC + 'jsImplementationOf').map((q) => q.subject.value),
  )
  const instances = quads
    .filter((q) => q.predicate.value === RDF_TYPE && jsTypes.has(q.object.value))
    .map((q) => q.subject.value)
  const uri = values.processor ?? instances[0]
  if (!uri) throw new Error('no processor instance found; pass --processor <iri>')

  // Channels: bound to stdin/stdout/files, or to a sink that discards.
  const inputs: { reader: MemoryReader; file?: string }[] = []
  const outputs: NdjsonWriter[] = []

  const makeReader = (id: Term): Reader => {
    const binding = bindings.find((b) => matches(b, id.value) && (b.kind === 'stdin' || b.kind === 'read'))
    const [, reader] = memoryChannel(id.value)
    if (binding) inputs.push({ reader, file: binding.file })
    else reader.closeReader() // nothing will ever arrive on it
    return reader
  }
  const makeWriter = (id: Term): Writer => {
    const binding = bindings.find((b) => matches(b, id.value) && (b.kind === 'stdout' || b.kind === 'write'))
    const out: Writable | 'stdout' | null = !binding
      ? null
      : binding.kind === 'stdout'
        ? 'stdout'
        : createWriteStream(binding.file!)
    const w = new NdjsonWriter(id.value, out)
    outputs.push(w)
    return w
  }

  // As js-runner's Runner.handlePipeline: a channel in the description becomes
  // a live Reader/Writer. The casts are only about rdf-lens' generic types.
  const apply = {
    [RDFC + 'Reader']: (x: unknown) => makeReader((x as { id: Term }).id),
    [RDFC + 'Writer']: (x: unknown) => makeWriter((x as { id: Term }).id),
  } as unknown as Parameters<typeof extractShapes>[1]
  const cache = {
    [RDFC + 'Reader']: empty(),
    [RDFC + 'Writer']: empty(),
  } as unknown as Parameters<typeof extractShapes>[2]
  const shapes = extractShapes(quads, apply, cache)
  const args = shapes.lenses[RDFL + 'TypedExtract'].execute({ id: new NamedNode(uri), quads })

  // The class to instantiate, from the processor type's own declaration.
  const type = quads.find((q) => q.subject.value === uri && q.predicate.value === RDF_TYPE)?.object.value
  const declared = (p: string) =>
    quads.find((q) => q.subject.value === type && q.predicate.value === RDFC + p)?.object.value
  const file = declared('file')
  const clazz = declared('class')
  if (!file) throw new Error(`type ${type} does not declare rdfc:file`)

  // From here the processor's own code runs: keep its stdout out of the data.
  process.stdout.write = ((chunk: string | Uint8Array, ...rest: unknown[]) =>
    (process.stderr.write as (...a: unknown[]) => boolean)(chunk, ...rest)) as typeof process.stdout.write

  const loaded = await import(file.startsWith('file:') ? file : pathToFileURL(file).href)
  const Cls = loaded[clazz ?? 'default']
  if (!Cls) throw new Error(`${file} has no export '${clazz ?? 'default'}'`)

  const processor = new Cls(args, stderrLogger(uri)) as Lifecycle

  await processor.init()
  const transform = processor.transform()
  const produce = processor.produce()

  await Promise.all(
    inputs.map(async ({ reader, file }) => {
      const input = file ? createReadStream(file) : process.stdin
      for await (const line of createInterface({ input, crlfDelay: Infinity })) {
        if (!line.trim()) continue
        const content = JSON.parse(line)
        if (typeof content !== 'string') {
          throw new Error('Expected every NDJSON line to be a JSON string')
        }
        await reader.deliver(encoder.encode(content))
      }
      reader.closeReader()
    }),
  )

  await Promise.all([transform, produce])
  await Promise.all(outputs.map((w) => w.close()))
}

main().catch((err) => {
  console.error(`rdfc-proc: ${err?.stack ?? err}`)
  process.exitCode = 1
})
