/**
 * In-memory Reader/Writer channels for RDF-Connect processors.
 *
 * These implement the exact same `Reader` / `Writer` interfaces that
 * `@rdfc/js-runner` hands to a processor, but deliver messages by direct
 * function call on the JS heap instead of routing them through the
 * orchestrator over gRPC.
 *
 * Semantics are deliberately kept identical to `WriterInstance` /
 * `ReaderInstance` so that the only variable between benchmark arms is the
 * transport:
 *
 *   - `write()` resolves only once *every* consumer has actually consumed the
 *     message (mirrors the GlobalAck -> LocalAck round trip).
 *   - a message is fanned out to every iterator handed out by the reader.
 *   - `close()` terminates the consumers' async iteration.
 *
 * `relaxed` mode drops the "await the consumer" rule while keeping everything
 * else. It isolates how much of the framework cost is the strict one-message-
 * in-flight ack discipline rather than the transport itself.
 */
import type { Reader, Writer, Any } from '@rdfc/js-runner'
import type { Handler } from '@rdfc/js-runner'

const encoder = new TextEncoder()
const decoder = new TextDecoder()

type Convertor<T> = (buf: Uint8Array) => T

const StringConv: Convertor<string> = (b) => decoder.decode(b)
const BufferConv: Convertor<Uint8Array> = (b) => b
const AnyConv: Convertor<Any> = (b) => ({ buffer: b })

type Todo<T> = { item: T; onComplete: () => void }

/**
 * Single-consumer async queue. Mirrors `MyIter` in the js-runner: `onComplete`
 * fires only after the consumer has resumed past the `yield`, which is what
 * makes the producer's await meaningful.
 */
class MemIter<T> implements AsyncIterable<T> {
  private queue: Todo<T | undefined>[] = []
  private resolveNext: ((v: undefined) => void) | null = null
  private closed = false

  constructor(private readonly convert: Convertor<T>) {}

  push(buffer: Uint8Array, onComplete: () => void) {
    if (this.closed) return onComplete()
    this.queue.push({ item: this.convert(buffer), onComplete })
    this.wake()
  }

  pushItem(item: T, onComplete: () => void) {
    if (this.closed) return onComplete()
    this.queue.push({ item, onComplete })
    this.wake()
  }

  close(onComplete: () => void) {
    if (this.closed) return onComplete()
    this.closed = true
    this.queue.push({ item: undefined, onComplete })
    this.wake()
  }

  private wake() {
    if (this.resolveNext) {
      this.resolveNext(undefined)
      this.resolveNext = null
    }
  }

  async *[Symbol.asyncIterator]() {
    for (;;) {
      if (this.queue.length > 0) {
        const { item, onComplete } = this.queue.shift()!
        if (item === undefined) {
          onComplete()
          break
        }
        yield item
        onComplete()
      } else {
        await new Promise<undefined>((res) => (this.resolveNext = res))
      }
    }
  }
}

export class MemoryReader implements Reader {
  readonly uri: string
  private consumers: MemIter<unknown>[] = []
  private closed = false

  constructor(uri: string) {
    this.uri = uri
  }

  private add<T>(conv: Convertor<T>): AsyncIterable<T> {
    const it = new MemIter(conv)
    this.consumers.push(it as MemIter<unknown>)
    return it
  }

  strings(): AsyncIterable<string> {
    return this.add(StringConv)
  }
  buffers(): AsyncIterable<Uint8Array> {
    return this.add(BufferConv)
  }
  anys(): AsyncIterable<Any> {
    return this.add(AnyConv)
  }
  streams(): AsyncIterable<AsyncGenerator<Uint8Array>> {
    const it = new MemIter<AsyncGenerator<Uint8Array>>((b) =>
      (async function* () {
        yield b
      })(),
    )
    this.consumers.push(it as unknown as MemIter<unknown>)
    return it
  }

  /** Deliver one message; resolves once every consumer has consumed it. */
  async deliver(buffer: Uint8Array): Promise<void> {
    if (this.closed) return
    await Promise.all(
      this.consumers.map(
        (c) => new Promise<void>((res) => c.push(buffer, () => res())),
      ),
    )
  }

  /** Deliver a stream message, fanned out to every consumer. */
  async deliverStream(chunks: AsyncIterable<Uint8Array>): Promise<void> {
    if (this.closed) return
    const buffered: Uint8Array[] = []
    for await (const c of chunks) buffered.push(c)
    const gen = async function* () {
      for (const c of buffered) yield c
    }
    await Promise.all(
      this.consumers.map(
        (c) =>
          new Promise<void>((res) =>
            (c as unknown as MemIter<AsyncGenerator<Uint8Array>>).pushItem(
              gen() as AsyncGenerator<Uint8Array>,
              () => res(),
            ),
          ),
      ),
    )
  }

  closeReader() {
    if (this.closed) return
    this.closed = true
    for (const c of this.consumers) c.close(() => {})
  }

  async cancel(): Promise<void> {
    this.closeReader()
  }
}

export class MemoryWriter implements Writer {
  readonly uri: string
  private _canceled = false
  private closed = false
  private readonly cancelHandlers = new Set<Handler>()

  constructor(
    uri: string,
    private readonly target: MemoryReader,
    private readonly relaxed = false,
  ) {
    this.uri = uri
  }

  get canceled(): boolean {
    return this._canceled
  }

  on(event: 'cancel', listener: Handler): this {
    if (event === 'cancel') this.cancelHandlers.add(listener)
    return this
  }

  private assertCanWrite() {
    if (this._canceled)
      throw new Error(`Writer for channel ${this.uri} was canceled`)
    if (this.closed) throw new Error(`Writer for channel ${this.uri} is closed`)
  }

  private dispatch(buf: Uint8Array): Promise<void> {
    if (this.relaxed) {
      void this.target.deliver(buf)
      return Promise.resolve()
    }
    return this.target.deliver(buf)
  }

  async buffer(buffer: Uint8Array): Promise<void> {
    this.assertCanWrite()
    await this.dispatch(buffer)
  }

  async string(msg: string): Promise<void> {
    this.assertCanWrite()
    await this.dispatch(encoder.encode(msg))
  }

  async stream<T = Uint8Array>(
    buffer: AsyncIterable<T>,
    transform?: (x: T) => Uint8Array,
  ): Promise<void> {
    this.assertCanWrite()
    const t = transform || ((x: unknown) => <Uint8Array>x)
    const mapped = (async function* () {
      for await (const m of buffer) yield t(m)
    })()
    await this.target.deliverStream(mapped)
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
      await Promise.all(Array.from(this.cancelHandlers).map((h) => h()))
    }
    this.target.closeReader()
  }
}

/** Create a connected writer/reader pair, as the runner would for a channel. */
export function memoryChannel(
  uri: string,
  relaxed = false,
): [MemoryWriter, MemoryReader] {
  const reader = new MemoryReader(uri)
  return [new MemoryWriter(uri, reader, relaxed), reader]
}
