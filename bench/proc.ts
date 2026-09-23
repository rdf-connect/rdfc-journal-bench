/**
 * Running a shell pipeline as one measured unit: wall clock, a timestamp per
 * output line, and peak RSS over the whole process tree.
 *
 * Where the kernel can account for the run, it does: the command runs inside a
 * transient systemd scope, and CPU time and peak memory come from that cgroup's
 * counters. They cover every descendant, including the thousands of short-lived
 * processes a scattered CWL workflow starts, which sampling cannot see.
 *
 * Without systemd (or with cgroup delegation unavailable) the run falls back to
 * sampling /proc every 20 ms: peak RSS is then the largest total resident size
 * of the live process tree, and CPU time is unavailable. Peak disk, when asked
 * for, is `du` over the given directories every 250 ms.
 */
import { execFile, execFileSync, spawn } from 'child_process'
import { createWriteStream, existsSync, readFileSync, readdirSync } from 'fs'
import { join } from 'path'

export type PipeRun = {
  code: number
  wallMs: number
  /** Milliseconds since spawn at which each output line arrived. */
  stamps: number[]
  peakRssMb: number
  /** CPU time of the whole process tree, when the cgroup accounted for it. */
  cpuMs?: number
  /** Mean cores busy over the run: cpuMs / wallMs. */
  cores?: number
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

/** True when a command can be run inside a transient user scope. */
function cgroupAvailable(): boolean {
  try {
    execFileSync('systemd-run', ['--user', '--scope', '--quiet', 'true'], {
      stdio: 'ignore',
      timeout: 5000,
    })
    return true
  } catch {
    return false
  }
}

const HAVE_CGROUP = cgroupAvailable()

/** Distinguishes the scopes of runs within one harness process. */
let runCounter = 0

/**
 * The cgroup directory of a named transient scope. The spawned process reports
 * the session's cgroup, not the scope's, so the scope is named and located by
 * that name instead.
 */
function scopeDir(unit: string): string | null {
  const uid = process.getuid?.() ?? 1000
  const path = join(
    '/sys/fs/cgroup/user.slice',
    `user-${uid}.slice`,
    `user@${uid}.service/app.slice`,
    `${unit}.scope`,
  )
  return existsSync(path) ? path : null
}

/**
 * CPU time and peak memory from the scope's cgroup. Both are kernel counters
 * over every descendant; they are sampled because the cgroup disappears when
 * the scope exits, so the last sample before exit is what we keep.
 */
function watchCgroup(unit: string) {
  let dir: string | null = null
  let cpuUsec = 0
  let peakBytes = 0

  const sample = () => {
    if (!dir) {
      dir = scopeDir(unit)
      if (!dir) return
    }
    try {
      const cpu = readFileSync(join(dir, 'cpu.stat'), 'utf8').match(/usage_usec (\d+)/)
      if (cpu) cpuUsec = Math.max(cpuUsec, Number(cpu[1]))
      const peak = readFileSync(join(dir, 'memory.peak'), 'utf8').trim()
      if (peak) peakBytes = Math.max(peakBytes, Number(peak))
    } catch {
      // The scope has gone; keep the last reading.
    }
  }

  sample()
  const timer = setInterval(sample, 20)
  return () => {
    clearInterval(timer)
    sample()
    return { cpuMs: cpuUsec / 1000, peakMemMb: peakBytes / (1024 * 1024) }
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
    // In a transient scope the kernel accounts for every descendant; see above.
    const unit = `rdfcbench-${process.pid}-${runCounter++}`
    const argv: [string, string[]] = HAVE_CGROUP
      ? [
          'systemd-run',
          ['--user', '--scope', '--quiet', `--unit=${unit}`, 'sh', '-c', `exec < ${input}; ${cmd}`],
        ]
      : ['sh', ['-c', `exec < ${input}; ${cmd}`]]
    const child = spawn(argv[0], argv[1], {
      stdio: ['ignore', 'pipe', 'pipe'],
      env: { ...process.env, ...opts.env },
      // Own process group, so a timeout can kill the whole tree.
      detached: true,
    })
    const stopRss = watchRss(child.pid!)
    const stopCgroup = HAVE_CGROUP ? watchCgroup(unit) : undefined
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
      const sampledRss = stopRss()
      const cg = stopCgroup?.()
      const peakDiskMb = stopDisk?.()
      out.end(() =>
        res({
          code: code ?? -1,
          wallMs,
          stamps,
          // The cgroup's peak is exact; the sampled one is the fallback.
          peakRssMb: cg && cg.peakMemMb > 0 ? cg.peakMemMb : sampledRss,
          cpuMs: cg?.cpuMs,
          cores: cg ? cg.cpuMs / wallMs : undefined,
          peakDiskMb,
          timedOut,
          stderr,
        }),
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
