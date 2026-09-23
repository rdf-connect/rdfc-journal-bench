#!/usr/bin/env node
/**
 * rdfc-proc: run any RDF-Connect JS processor as a command-line tool.
 *
 *   rdfc-proc --config step.json [--set key=value ...]
 *
 * The shell-pipe and CWL arms need one executable per pipeline stage. Writing a
 * wrapper per processor would mean re-implementing each stage, and the
 * benchmark would no longer be comparing the same code. This adapter instead
 * instantiates the published processor class itself and drives it through the
 * runner's lifecycle, with stdin and stdout in place of channels:
 *
 *   1. construct        2. await init()
 *   3. transform() (not awaited)        4. produce()
 *   5. feed stdin, close, await the rest
 *
 * The channels are the in-memory Reader/Writer of experiment 1 (src/inmem.ts),
 * which keep the js-runner's semantics, including "a write resolves only once
 * the consumer has consumed the message".
 *
 * Framing is the benchmark's NDJSON: one message per line, each line a JSON
 * string. `--set` overrides a dotted path in `args` (e.g. --set config.k=v).
 *
 * The config file names the processor and its arguments, with channels written
 * as placeholders:
 *
 *   {
 *     "module": "@rdfc/sds-processors-ts/lib/bucketize.js",
 *     "class": "Bucketize",
 *     "args": {
 *       "input":  { "$stdin": true },
 *       "output": { "$stdout": true },
 *       "report": { "$write": "reports.ndjson" },
 *       "savePath": "state.json"
 *     }
 *   }
 *
 * Placeholders: {"$stdin":true}, {"$stdout":true}, {"$read":"path"},
 * {"$write":"path"} for channels, and {"$iri":"..."} / {"$literal":"..."} for
 * the RDF terms some processors take (the orchestrator derives those from the
 * pipeline description). Anything else is passed through as data.
 */
import { createInterface } from 'readline'
import { createReadStream, createWriteStream, readFileSync } from 'fs'
import { once } from 'events'
import { Writable } from 'stream'
import { parseArgs } from 'util'
import { pathToFileURL } from 'url'
import { resolve } from 'path'
import { DataFactory } from 'n3'
import { createLogger, format, transports, type Logger } from 'winston'
import type { Any, Handler, Writer } from '@rdfc/js-runner'
import { MemoryReader, memoryChannel } from '../src/inmem.js'

const USAGE = `Usage: rdfc-proc --config <file.json> [--set <key=value> ...]

Runs one RDF-Connect JS processor, reading NDJSON messages from stdin and
writing them to stdout (one JSON string per line).

  --config <file>   processor module, class and arguments
  --set key=value   override args.<key> (dotted path); values parse as JSON
  -h, --help        show this help`

// ── Writers ──────────────────────────────────────────────────────────────────

const encoder = new TextEncoder()
const decoder = new TextDecoder()

/** A Writer that appends every message to a stream as one NDJSON line. */
class NdjsonWriter implements Writer {
  private closed = false
  private _canceled = false
  private readonly cancelHandlers = new Set<Handler>()

  constructor(
    readonly uri: string,
    private readonly out: Writable,
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
    if (!this.out.write(JSON.stringify(msg) + '\n')) await once(this.out, 'drain')
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
    const parts: Uint8Array[] = []
    for await (const chunk of buffer) parts.push(t(chunk))
    await this.write(parts.map((p) => decoder.decode(p)).join(''))
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
    if (this.out !== process.stdout) {
      await new Promise((res) => this.out.end(res))
    }
  }
}

// ── Config ───────────────────────────────────────────────────────────────────

type StepConfig = {
  module: string
  class: string
  args: Record<string, unknown>
}

type Placeholder =
  | { $stdin: true }
  | { $stdout: true }
  | { $read: string }
  | { $write: string }
  | { $iri: string }
  | { $literal: string }

const PLACEHOLDERS = ['$stdin', '$stdout', '$read', '$write', '$iri', '$literal']

function placeholderOf(v: unknown): Placeholder | null {
  if (!v || typeof v !== 'object' || Array.isArray(v)) return null
  const keys = Object.keys(v)
  if (keys.length !== 1) return null
  return PLACEHOLDERS.includes(keys[0]) ? (v as Placeholder) : null
}

/** Replaces channel placeholders with live channels; collects what to feed and close. */
function bindChannels(args: Record<string, unknown>) {
  const inputs: { reader: MemoryReader; file?: string }[] = []
  const outputs: Writer[] = []

  const walk = (value: unknown): unknown => {
    const ph = placeholderOf(value)
    if (ph) {
      if ('$iri' in ph) return DataFactory.namedNode(ph.$iri)
      if ('$literal' in ph) return DataFactory.literal(ph.$literal)
      if ('$stdin' in ph || '$read' in ph) {
        const [, reader] = memoryChannel(`urn:rdfc-proc:in:${inputs.length}`)
        inputs.push({ reader, file: '$read' in ph ? ph.$read : undefined })
        return reader
      }
      const file = '$write' in ph ? ph.$write : undefined
      const out = file ? createWriteStream(file) : process.stdout
      const w = new NdjsonWriter(`urn:rdfc-proc:out:${outputs.length}`, out)
      outputs.push(w)
      return w
    }
    if (Array.isArray(value)) return value.map(walk)
    if (value && typeof value === 'object') {
      return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, walk(v)]))
    }
    return value
  }

  return { args: walk(args) as Record<string, unknown>, inputs, outputs }
}

function applyOverrides(args: Record<string, unknown>, sets: string[]) {
  for (const s of sets) {
    const eq = s.indexOf('=')
    if (eq < 0) throw new Error(`--set expects key=value, got '${s}'`)
    const path = s.slice(0, eq).split('.')
    const raw = s.slice(eq + 1)
    let value: unknown
    try {
      value = JSON.parse(raw)
    } catch {
      value = raw
    }
    let target = args
    for (const k of path.slice(0, -1)) {
      if (typeof target[k] !== 'object' || target[k] === null) target[k] = {}
      target = target[k] as Record<string, unknown>
    }
    target[path[path.length - 1]] = value
  }
}

/**
 * The processor's logger. The js-runner ships log records to the orchestrator;
 * here they go to stderr, so stdout carries only data.
 */
function stderrLogger(): Logger {
  return createLogger({
    level: process.env.LOG_LEVEL ?? 'warn',
    format: format.combine(format.timestamp(), format.simple()),
    transports: [new transports.Stream({ stream: process.stderr })],
  })
}

// ── Main ─────────────────────────────────────────────────────────────────────

type Lifecycle = {
  init(): Promise<void>
  transform(): Promise<void>
  produce(): Promise<void>
}

async function main() {
  const { values } = parseArgs({
    options: {
      config: { type: 'string' },
      set: { type: 'string', multiple: true, default: [] },
      help: { type: 'boolean', short: 'h', default: false },
    },
  })
  if (values.help || !values.config) {
    console.log(USAGE)
    process.exit(values.help ? 0 : 2)
  }

  const cfg: StepConfig = JSON.parse(readFileSync(values.config, 'utf8'))
  applyOverrides(cfg.args, values.set!)
  const { args, inputs, outputs } = bindChannels(cfg.args)

  const module = cfg.module.startsWith('.')
    ? pathToFileURL(resolve(module_dir(values.config!), cfg.module)).href
    : cfg.module
  const loaded = await import(module)
  const Cls = loaded[cfg.class]
  if (!Cls) throw new Error(`${cfg.module} has no export '${cfg.class}'`)

  const processor = new Cls(args, stderrLogger()) as Lifecycle

  // The runner's order: init everything, then transform (not awaited), then
  // produce; data only flows once every processor has been initialised.
  await processor.init()
  const transform = processor.transform()
  const produce = processor.produce()

  // Feed the inputs: stdin, or a file for extra readers.
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

/** Directory of the config file, for resolving a relative module path. */
function module_dir(configPath: string): string {
  return resolve(configPath, '..')
}

main().catch((err) => {
  console.error(`rdfc-proc: ${err?.stack ?? err}`)
  process.exitCode = 1
})
