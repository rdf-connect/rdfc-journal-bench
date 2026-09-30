#!/usr/bin/env node
// Where does the fixed CPU cost of the Blue Bike pipeline go?
// Runs the shell and rdfc arms as bench/run-bluebike.ts does, each in a
// transient systemd scope, and samples CPU per process and per thread, so the
// JVM's JIT compiler, GC and application threads are reported separately.
// Build first (npm run build); needs a user systemd session, as proc.ts does.
//
//   node bench/profile-cpu.mjs --ns=5,60 --reps=5 --cpus=0-3 --out=results/profile-cpu.json
//
// --cpus pins every process with taskset, e.g. to four physical cores.
import { spawn } from 'child_process'
import { existsSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'fs'
import { dirname, join, resolve } from 'path'
import { fileURLToPath } from 'url'
import { parseArgs } from 'util'

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const bb = await import(join(ROOT, 'dist/bench/bluebike.js'))
const { renderBluebikePipeline } = await import(join(ROOT, 'dist/src/genbluebike.js'))

const { values } = parseArgs({
  options: {
    ns: { type: 'string', default: '5' },
    reps: { type: 'string', default: '1' },
    arms: { type: 'string', default: 'shell,rdfc' },
    out: { type: 'string', default: join(ROOT, 'results', 'profile-cpu.json') },
    runs: { type: 'string', default: '/dev/shm/rdfc-profile' },
    // Pin every process to these CPUs (taskset list), e.g. 0-3 for four physical cores.
    cpus: { type: 'string', default: '' },
  },
})
const TICK = Number(process.env.HZ_TICK ?? 100) // clock ticks per second
const uid = process.getuid()

function scopeDir(unit) {
  const p = `/sys/fs/cgroup/user.slice/user-${uid}.slice/user@${uid}.service/app.slice/${unit}.scope`
  return existsSync(p) ? p : null
}

/** utime+stime (ms) and comm of every thread of a process. */
function threads(pid) {
  const out = []
  let tids = []
  try { tids = readdirSync(`/proc/${pid}/task`) } catch { return out }
  for (const tid of tids) {
    try {
      const s = readFileSync(`/proc/${pid}/task/${tid}/stat`, 'utf8')
      const comm = s.slice(s.indexOf('(') + 1, s.lastIndexOf(')'))
      const f = s.slice(s.lastIndexOf(')') + 2).split(' ')
      // f[0] is field 3 (state); utime is field 14, stime field 15.
      const ms = ((Number(f[11]) + Number(f[12])) * 1000) / TICK
      out.push({ tid, comm, ms })
    } catch {}
  }
  return out
}

function cmdline(pid) {
  try { return readFileSync(`/proc/${pid}/cmdline`, 'utf8').split('\0').filter(Boolean).join(' ') } catch { return '' }
}

function classifyProcess(cmd) {
  if (/^java /.test(cmd) && /jvm-runner/.test(cmd)) return 'jvm-runner'
  if (/^java /.test(cmd) && /rml-map/.test(cmd)) return 'rml-map'
  if (/^java /.test(cmd)) return 'java-other'
  if (/js-runner/.test(cmd) && !/npm|npx/.test(cmd)) return 'js-runner'
  if (/rdfc-proc\.js/.test(cmd)) {
    const m = cmd.match(/--config \S*\/steps\/(\S+)\.ttl/)
    return `rdfc-proc:${m ? m[1] : '?'}`
  }
  if (/npm-cli|npx|npm exec/.test(cmd)) return 'npx'
  if (/node\b.*\brdfc\b/.test(cmd) || /\/rdfc /.test(cmd)) return 'orchestrator'
  if (/^node /.test(cmd)) return 'node-other'
  return 'other'
}

function classifyThread(comm) {
  if (/^C[12] Compiler/.test(comm)) return 'jit'
  if (/^(GC Thread|G1 |VM Periodic|VM Thread)/.test(comm)) return 'gc/vm'
  if (/^V8 /.test(comm) || /DefaultWorke/.test(comm)) return 'v8-workers'
  return 'app'
}

async function runOnce(arm, n, rep) {
  const runDir = join(values.runs, `${arm}__n${n}__r${rep}`)
  rmSync(runDir, { recursive: true, force: true })
  mkdirSync(runDir, { recursive: true })
  const input = bb.prepareInput(n, runDir)
  bb.prepareSideInputs(runDir, n)
  let cmd
  if (arm === 'shell') cmd = bb.shellCommand(input, runDir)
  else {
    const ttl = renderBluebikePipeline({
      input,
      mapping: join(ROOT, 'anchor', 'bluebike.rml.ttl'),
      shapes: join(ROOT, 'anchor', 'bluebike-shapes.ttl'),
      focusQuery: join(runDir, 'focus.rq'),
      resultDir: runDir,
    })
    const ttlPath = join(ROOT, 'pipelines', `profile__n${n}_r${rep}.ttl`)
    writeFileSync(ttlPath, ttl)
    cmd = `npx rdfc ${ttlPath} > ${runDir}/log.txt 2>&1`
  }
  const unit = `rdfc-profile-${process.pid}-${arm}-${n}-${rep}`
  const t0 = performance.now()
  const child = spawn('systemd-run', ['--user', '--scope', '--quiet', `--unit=${unit}`, ...(values.cpus ? ['taskset', '-c', values.cpus] : []), 'sh', '-c', `exec < /dev/null; ${cmd}`], {
    cwd: ROOT,
    stdio: ['ignore', 'ignore', 'pipe'],
    env: { ...process.env, PATH: `${join(ROOT, 'cwl', 'bin')}:${process.env.PATH}`, JAVA_OPTS: bb.JAVA_OPTS },
    detached: true,
  })
  let stderr = ''
  child.stderr.on('data', (d) => { if (stderr.length < 4000) stderr += d })

  // pid -> { cmd, kind, firstSeen, lastSeen, threads: tid -> {comm, ms} }
  const procs = new Map()
  let cgCpuMs = 0
  let dir = null
  const sample = () => {
    if (!dir) dir = scopeDir(unit)
    if (!dir) return
    let pids = []
    try {
      const m = readFileSync(join(dir, 'cpu.stat'), 'utf8').match(/usage_usec (\d+)/)
      if (m) cgCpuMs = Math.max(cgCpuMs, Number(m[1]) / 1000)
      pids = readFileSync(join(dir, 'cgroup.procs'), 'utf8').split('\n').filter(Boolean)
    } catch { return }
    const t = performance.now() - t0
    for (const pid of pids) {
      let p = procs.get(pid)
      if (!p) {
        const c = cmdline(pid)
        p = { cmd: c, kind: classifyProcess(c), firstSeen: t, lastSeen: t, threads: new Map() }
        procs.set(pid, p)
      } else if (!p.cmd) {
        p.cmd = cmdline(pid)
        p.kind = classifyProcess(p.cmd)
      }
      // A process that exec()s changes its cmdline: re-read until it settles.
      if (p.kind === 'other' || p.kind === 'npx') {
        const c = cmdline(pid)
        if (c && c !== p.cmd) { p.cmd = c; p.kind = classifyProcess(c) }
      }
      p.lastSeen = t
      for (const th of threads(pid)) {
        const prev = p.threads.get(th.tid)
        if (!prev || th.ms >= prev.ms) p.threads.set(th.tid, { comm: th.comm, ms: th.ms })
      }
    }
  }
  const timer = setInterval(sample, 25)
  const code = await new Promise((res) => child.on('close', res))
  clearInterval(timer)
  sample()
  const wallMs = performance.now() - t0

  const acts = bb.publishedActivities(bb.ldesDir(arm, runDir))
  const want = bb.expectedActivities(n)
  const perProcess = [...procs.entries()].map(([pid, p]) => {
    const byClass = {}
    let total = 0
    for (const th of p.threads.values()) {
      const k = classifyThread(th.comm)
      byClass[k] = (byClass[k] ?? 0) + th.ms
      total += th.ms
    }
    return { pid, kind: p.kind, cmd: p.cmd.slice(0, 200), firstSeenMs: p.firstSeen, lastSeenMs: p.lastSeen, cpuMs: total, byClass,
      threads: [...p.threads.values()].filter((t) => t.ms > 0) }
  })
  return {
    arm, n, rep, code, wallMs, cgCpuMs,
    sampledCpuMs: perProcess.reduce((a, p) => a + p.cpuMs, 0),
    correct: acts.creates === want.creates && acts.updates === want.updates,
    activities: acts, expected: want, perProcess, stderr,
  }
}

const results = []
for (const n of values.ns.split(',').map(Number)) {
  for (let rep = 0; rep < Number(values.reps); rep++) {
    for (const arm of values.arms.split(',')) {
      process.stderr.write(`  ${arm} n=${n} rep=${rep} ... `)
      const r = await runOnce(arm, n, rep)
      results.push(r)
      writeFileSync(values.out, JSON.stringify(results, null, 1))
      const by = {}
      for (const p of r.perProcess) by[p.kind] = (by[p.kind] ?? 0) + p.cpuMs
      process.stderr.write(`wall ${(r.wallMs / 1000).toFixed(1)} s, cgroup CPU ${(r.cgCpuMs / 1000).toFixed(1)} s, sampled ${(r.sampledCpuMs / 1000).toFixed(1)} s, correct ${r.correct}\n    ` +
        Object.entries(by).sort((a, b) => b[1] - a[1]).map(([k, v]) => `${k} ${(v / 1000).toFixed(1)}`).join(', ') + '\n')
    }
  }
}

// Medians per arm and size, by component. The fixed-cost question is the
// difference between the two arms at the smallest size.
function components(r) {
  const c = { total: r.cgCpuMs / 1000, wall: r.wallMs / 1000 }
  const add = (k, v) => (c[k] = (c[k] ?? 0) + v / 1000)
  for (const p of r.perProcess) {
    if (p.kind === 'jvm-runner' || p.kind === 'rml-map') {
      for (const [cls, ms] of Object.entries(p.byClass)) add(`jvm:${cls}`, ms)
    } else if (p.kind === 'js-runner' || p.kind.startsWith('rdfc-proc')) add('node', p.cpuMs)
    else add(p.kind, p.cpuMs)
  }
  return c
}
const median = (xs) => {
  const s = [...xs].sort((a, b) => a - b)
  return s.length % 2 ? s[(s.length - 1) / 2] : (s[s.length / 2 - 1] + s[s.length / 2]) / 2
}
const groups = new Map()
for (const r of results) {
  const key = `${r.arm} n=${r.n}`
  if (!groups.has(key)) groups.set(key, [])
  groups.get(key).push(components(r))
}
const cols = ['total', 'wall', 'jvm:jit', 'jvm:app', 'jvm:gc/vm', 'node', 'orchestrator', 'npx', 'other']
console.log(`\nmedian seconds${''.padEnd(4)}` + cols.map((c) => c.padStart(13)).join(''))
for (const [key, cs] of groups) {
  console.log(key.padEnd(18) + cols.map((c) => median(cs.map((x) => x[c] ?? 0)).toFixed(1).padStart(13)).join(''))
}
