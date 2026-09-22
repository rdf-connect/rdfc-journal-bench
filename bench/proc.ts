/**
 * Running a shell pipeline as one measured unit: wall clock, a timestamp per
 * output line, and peak RSS over the whole process tree.
 *
 * Peak RSS is the largest total resident memory of the live process tree over
 * samples taken from /proc every 20 ms (VmRSS, summed over the processes alive
 * at that moment), so short spikes between samples can be missed. Peak disk, when
 * asked for, is `du` over the given directories every 250 ms.
 */
import { execFile, spawn } from 'child_process'
import { createWriteStream, readFileSync, readdirSync } from 'fs'

export type PipeRun = {
  code: number
  wallMs: number
  /** Milliseconds since spawn at which each output line arrived. */
  stamps: number[]
  peakRssMb: number
  /** Peak size of `diskDirs`, when given. */
  peakDiskMb?: number
  /** Killed after `timeoutMs`. */
  timedOut?: boolean
  stderr: string
}

export type PipeOptions = {
  /** Kill the whole process tree after this long. */
  timeoutMs?: number
  /** Extra environment variables. */
  env?: Record<string, string>
  /** Directories whose peak total size to report (intermediate files). */
  diskDirs?: string[]
}

function children(pid: number): number[] {
  try {
    return readdirSync(`/proc/${pid}/task`).flatMap((t) =>
      readFileSync(`/proc/${pid}/task/${t}/children`, 'utf8').split(' ').filter(Boolean).map(Number),
    )
  } catch {
    return []
  }
}

function rssKb(pid: number): number {
  try {
    const m = readFileSync(`/proc/${pid}/status`, 'utf8').match(/VmRSS:\s+(\d+)/)
    return m ? Number(m[1]) : 0
  } catch {
    return 0
  }
}

function watchRss(root: number) {
  let peak = 0
  const sample = () => {
    let total = 0
    const todo = [root]
    while (todo.length) {
      const pid = todo.pop()!
      total += rssKb(pid)
      todo.push(...children(pid))
    }
    peak = Math.max(peak, total)
  }
  sample()
  const timer = setInterval(sample, 20)
  return () => {
    clearInterval(timer)
    return peak / 1024
  }
}

function watchDisk(dirs: string[]) {
  let peak = 0
  let busy = false
  const sample = () => {
    if (busy) return
    busy = true
    execFile('du', ['-sbc', ...dirs], (_err, stdout) => {
      busy = false
      const m = stdout?.match(/(\d+)\s+total/)
      if (m) peak = Math.max(peak, Number(m[1]))
    })
  }
  const timer = setInterval(sample, 250)
  return () => {
    clearInterval(timer)
    return peak / (1024 * 1024)
  }
}

/** Runs `cmd` under `sh` with stdin from `input`, writing stdout to `outFile`. */
export function runPipe(
  cmd: string,
  input: string,
  outFile: string,
  opts: PipeOptions = {},
): Promise<PipeRun> {
  return new Promise((res) => {
    const t0 = performance.now()
    const child = spawn('sh', ['-c', `exec < ${input}; ${cmd}`], {
      stdio: ['ignore', 'pipe', 'pipe'],
      env: { ...process.env, ...opts.env },
      // Own process group, so a timeout can kill the whole tree.
      detached: true,
    })
    const stopRss = watchRss(child.pid!)
    const stopDisk = opts.diskDirs ? watchDisk(opts.diskDirs) : undefined
    let timedOut = false
    const timer = opts.timeoutMs
      ? setTimeout(() => {
          timedOut = true
          try {
            process.kill(-child.pid!, 'SIGKILL')
          } catch {}
        }, opts.timeoutMs)
      : undefined
    const out = createWriteStream(outFile)

    const stamps: number[] = []
    let partial = ''
    child.stdout.on('data', (d: Buffer) => {
      const now = performance.now() - t0
      out.write(d)
      const parts = (partial + d.toString()).split('\n')
      partial = parts.pop()!
      for (let i = 0; i < parts.length; i++) stamps.push(now)
    })
    let stderr = ''
    child.stderr.on('data', (d) => {
      if (stderr.length < 4000) stderr += d.toString()
    })

    child.on('close', (code) => {
      const wallMs = performance.now() - t0
      if (timer) clearTimeout(timer)
      const peakRssMb = stopRss()
      const peakDiskMb = stopDisk?.()
      out.end(() =>
        res({ code: code ?? -1, wallMs, stamps, peakRssMb, peakDiskMb, timedOut, stderr }),
      )
    })
  })
}

/** The set of N-Quads lines in an NDJSON file whose lines are JSON strings. */
export function quadSet(file: string): Set<string> {
  const quads = new Set<string>()
  for (const line of readFileSync(file, 'utf8').split('\n')) {
    if (!line) continue
    for (const q of (JSON.parse(line) as string).split('\n')) {
      if (q.trim()) quads.add(q.trim())
    }
  }
  return quads
}
