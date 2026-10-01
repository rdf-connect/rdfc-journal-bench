#!/usr/bin/env python3
"""Figures for Section 6.

    python3 bench/plots/eval.py [results-dir] [out-dir]

Defaults: server_results/ and figures/. Writes a PDF (for the paper) and a PNG
(for a quick look) of each figure:

  eval-overhead   framework overhead against work per message (work.json,
                  stages/stages.json)
  eval-growth     CPU time against snapshots, one run per size (bluebike.json,
                  or server-size.log when the raw sweep is not there)
  eval-freshness  freshness against cost on a schedule (freshness.json)
  eval-config     lines of configuration, counted from the repository
"""
import collections
import json
import math
import re
import statistics as st
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, LogLocator

ROOT = Path(__file__).resolve().parents[2]

RDFC = "#1f77b4"
SHELL = "#444444"
CWL = "#2ca02c"
REBUILD = "#d62728"
INCREMENTAL = "#ff7f0e"
GREY = "#999999"

plt.rcParams.update({
    "font.size": 8,
    "axes.labelsize": 9,
    "legend.fontsize": 7.5,
    "xtick.labelsize": 8,
    "ytick.labelsize": 8,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "pdf.fonttype": 42,
})

WIDTH = 3.4  # one column, inches


def plain_log(ax, axis="y"):
    """Log axis labelled 1, 2, 5, 10, ... rather than 10^n."""
    target = ax.yaxis if axis == "y" else ax.xaxis
    target.set_major_locator(LogLocator(base=10, subs=(1.0, 2.0, 5.0), numticks=12))
    target.set_minor_locator(LogLocator(base=10, subs=tuple(range(2, 10)), numticks=12))
    target.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
    target.set_minor_formatter(FuncFormatter(lambda v, _: ""))


def grid(ax):
    ax.grid(True, which="major", lw=0.4, alpha=0.4)


def save(fig, out, name):
    out.mkdir(parents=True, exist_ok=True)
    for ext in ("pdf", "png"):
        fig.savefig(out / f"{name}.{ext}", dpi=200 if ext == "png" else None)
    plt.close(fig)
    print("wrote", out / f"{name}.pdf")


# ---------------------------------------------------------------------------
# Overhead against work per message

def stage_costs(res):
    """Warm ms per record for the mapping and validation stages, derived as in
    stages.md: the difference between n=10000 and n=1, per extra record."""
    path = res / "stages" / "stages.json"
    if not path.exists():
        return {}
    runs = [r for r in json.load(open(path))["runs"] if r.get("ok")]
    wall = collections.defaultdict(list)
    for r in runs:
        wall[(r["case"], r["n"])].append(r["wallMs"])
    out = {}
    for stage, label in (("rml", "mapping"), ("shacl", "validation")):
        lo, hi = wall.get((f"{stage}-stream", 1)), wall.get((f"{stage}-stream", 10000))
        if lo and hi:
            out[label] = (st.median(hi) - st.median(lo)) / 9999
    return out


def overhead(res, out):
    runs = [r for r in json.load(open(res / "work.json")) if r.get("ok")]
    p50 = collections.defaultdict(list)
    for r in runs:
        if r["workload"].get("depth", 0) != 0:
            continue
        p50[(r["arm"], r["workload"].get("workUs", 0))].append(r["latency"]["p50"])
    works = sorted({w for (a, w) in p50 if a == "rdfc" and (("native", w) in p50)})
    rdfc = [st.median(p50[("rdfc", w)]) for w in works]
    native = [st.median(p50[("native", w)]) for w in works]
    share = [100 * (r - n) / r for r, n in zip(rdfc, native)]

    # Work is plotted in ms; zero work sits one decade left of the smallest
    # non-zero value and is labelled as such.
    ms = [w / 1000 for w in works]
    nonzero = [m for m in ms if m > 0]
    zero_at = min(nonzero) / 10
    xs = [m if m > 0 else zero_at for m in ms]

    # Minimum effective task granularity: work at which the share is 50 %,
    # interpolated in log(work).
    metg = None
    for (x0, s0), (x1, s1) in zip(zip(xs, share), zip(xs[1:], share[1:])):
        if s0 >= 50 >= s1 and x0 > zero_at:
            t = (s0 - 50) / (s0 - s1)
            metg = 10 ** (math.log10(x0) + t * (math.log10(x1) - math.log10(x0)))

    stages = stage_costs(res)

    fig, (a, b) = plt.subplots(2, 1, figsize=(WIDTH, 3.9), sharex=True)
    a.plot(xs, rdfc, "o-", color=RDFC, ms=3, lw=1.3, label="RDF-Connect")
    a.plot(xs, native, "s-", color=GREY, ms=3, lw=1.3, label="in process")
    a.set_yscale("log")
    a.set_ylabel("latency per message (ms)")
    a.legend(frameon=False, loc="upper left")
    plain_log(a)

    b.plot(xs, share, "o-", color=RDFC, ms=3, lw=1.3)
    b.axhline(50, color=GREY, lw=0.8, ls="--")
    if metg:
        b.axvline(metg, color=GREY, lw=0.8, ls="--")
        b.annotate(f"{metg:.2f} ms", (metg, 50), xytext=(4, 4),
                   textcoords="offset points", fontsize=7, color="#555555")
    b.set_ylabel("framework share (%)")
    b.set_ylim(0, 105)
    b.set_xlabel("work per message (ms)")

    for ax in (a, b):
        ax.set_xscale("log")
        grid(ax)
        for label, cost in stages.items():
            ax.axvline(cost, color=CWL, lw=0.9, alpha=0.8)
    for label, cost in stages.items():
        b.annotate(label, (cost, 102), xytext=(2, 0), textcoords="offset points",
                   rotation=90, va="top", ha="left", fontsize=6.5, color=CWL)

    b.set_xticks(xs)
    b.set_xticklabels(["0" if x == zero_at else f"{x:g}" for x in xs])
    b.xaxis.set_minor_locator(plt.NullLocator())
    fig.align_ylabels()
    fig.tight_layout()
    save(fig, out, "eval-overhead")


# ---------------------------------------------------------------------------
# Growth: CPU time against snapshots, one run per size

GROWTH = [
    # arm, label, colour, style, marker
    ("shell", "Shell pipeline", SHELL, "-", "s"),
    ("rdfc", "RDF-Connect", RDFC, "-", "o"),
    ("cwl-batch", "CWL, cwltool", CWL, "-", "^"),
    ("streamflow-batch", "CWL, StreamFlow", CWL, "--", "v"),
    ("toil-batch", "CWL, Toil", CWL, ":", "D"),
]


def size_medians(res):
    """{arm: {snapshots: cpu seconds}}, from the raw sweep if present, else
    from the medians printed in server-size.log."""
    raw = res / "bluebike.json"
    if raw.exists():
        rows = [r for r in json.load(open(raw)) if r.get("ok") and r.get("unit", 1) == 1]
        by = collections.defaultdict(lambda: collections.defaultdict(list))
        for r in rows:
            by[r["arm"]][r["snapshots"]].append(r["cpuMs"] / 1000)
        if len({n for arm in by.values() for n in arm}) > 2:
            return {a: {n: st.median(v) for n, v in ns.items()} for a, ns in by.items()}
    log = res / "server-size.log"
    by = collections.defaultdict(dict)
    pat = re.compile(r"^\s*(\d+)\s+(\S+)\s+(\d+)\s+[\d.]+\s+(\d+)\s+[\d.]+\s+\d+")
    for line in open(log):
        m = pat.match(line)
        if m:
            n, arm, _wall, cpu = m.groups()
            by[arm][int(n)] = int(cpu) / 1000
    print("  growth: raw sweep not found, using the medians in", log.name)
    return by


def fit(ns, ys):
    mx, my = st.mean(ns), st.mean(ys)
    slope = sum((x - mx) * (y - my) for x, y in zip(ns, ys)) / sum((x - mx) ** 2 for x in ns)
    return my - slope * mx, slope


def growth(res, out):
    by = size_medians(res)
    fig, ax = plt.subplots(figsize=(WIDTH, 3.1))
    xmax = max(n for arm in by.values() for n in arm)
    for arm, label, colour, style, marker in GROWTH:
        if arm not in by:
            continue
        ns = sorted(by[arm])
        ys = [by[arm][n] for n in ns]
        fixed, per = fit(ns, ys)
        ax.plot(ns, ys, marker, color=colour, ms=3.5, mfc="white" if style != "-" else colour,
                ls="none")
        ax.plot([0, xmax], [fixed, fixed + per * xmax], style, color=colour, lw=1.1,
                label=f"{label}: {fixed:.0f} s + {per:.2f} s/snap.")
    ax.set_xlim(0, xmax * 1.03)
    ax.set_ylim(0, None)
    ax.set_xlabel("snapshots processed in one run")
    ax.set_ylabel("CPU time (s)")
    grid(ax)
    ax.legend(frameon=False, loc="upper center", handlelength=2.2, fontsize=7,
              bbox_to_anchor=(0.45, -0.22))
    fig.tight_layout()
    save(fig, out, "eval-growth")


# ---------------------------------------------------------------------------
# Freshness against cost

def freshness(res, out):
    rows = json.load(open(res / "freshness.json"))
    streaming = [r for r in rows if not r.get("interval")]
    # The streaming arms' freshness was measured again (2026-09-30) once the
    # shell arm's pacer recorded its own arrivals; the first run timed them from
    # spawn. Their CPU stays from the first run, which is the session the
    # scheduled arms ran in, so that every CPU figure comes from one session.
    remeasured = res / "freshness-streaming.json"
    if remeasured.exists():
        again = {r["arm"]: r for r in json.load(open(remeasured))}
        for r in streaming:
            if r["arm"] in again:
                r["freshnessMs"] = again[r["arm"]]["freshnessMs"]
    arrival = rows[0]["arrivalMs"]
    n = rows[0]["n"]

    # Snapshots that cause no activity have no freshness. Newer results record
    # them as null; older ones credited them with the previous snapshot's
    # publication, which shows as a value around minus one arrival interval.
    def absent(x):
        return x is None or (isinstance(x, float) and math.isnan(x)) or x < -arrival / 2

    causes = [not any(absent(r["freshnessMs"][i]) for r in streaming) for i in range(n)]

    def values(r):
        return [x / 1000 for i, x in enumerate(r["freshnessMs"]) if causes[i] and not absent(x)]

    def p95(xs):
        s = sorted(xs)
        return s[min(len(s) - 1, int(0.95 * len(s)))]

    fig, (a, b) = plt.subplots(2, 1, figsize=(WIDTH, 4.9),
                               gridspec_kw={"height_ratios": [1.25, 1]})

    # (a) median freshness against CPU, bar to p95.
    for mode, colour, label in (("cwl-incremental", INCREMENTAL, "CWL, incremental"),
                                ("cwl-rebuild", REBUILD, "CWL, rebuild")):
        rs = sorted((r for r in rows if r["arm"] == mode), key=lambda r: r["interval"])
        if not rs:
            continue
        cpu = [r["cpuMs"] / 1000 for r in rs]
        med = [st.median(values(r)) for r in rs]
        hi = [p95(values(r)) - m for r, m in zip(rs, med)]
        a.errorbar(cpu, med, yerr=[[0] * len(med), hi], fmt="o-", color=colour, ms=3.5,
                   lw=1.1, elinewidth=0.7, capsize=0, label=label)
        for r, x, y in zip(rs, cpu, med):
            below = mode == "cwl-incremental"
            a.annotate(f"k={r['interval']}", (x, y), xytext=(-3, -10) if below else (3, 3),
                       ha="right" if below else "left", textcoords="offset points",
                       fontsize=6.5, color=colour)
    for r, colour, marker, label in ((next((s for s in streaming if s["arm"] == "rdfc"), None), RDFC, "o", "RDF-Connect"),
                                     (next((s for s in streaming if s["arm"] == "shell"), None), SHELL, "s", "Shell pipeline")):
        if r is None:
            continue
        v = values(r)
        m = st.median(v)
        a.errorbar([r["cpuMs"] / 1000], [m], yerr=[[0], [p95(v) - m]], fmt=marker, color=colour,
                   ms=4.5, elinewidth=0.7, capsize=0, label=label)
    a.set_xscale("log")
    a.set_yscale("log")
    plain_log(a, "x")
    plain_log(a)
    a.set_xlabel("CPU time over the window (s)")
    a.set_ylabel("freshness (s)")
    grid(a)
    a.legend(frameon=False, loc="lower left", fontsize=7, ncol=2, columnspacing=1.0,
             bbox_to_anchor=(0.0, 1.0))
    a.text(0.02, 0.97, "(a)", transform=a.transAxes, va="top", fontsize=8)

    # (b) freshness of each snapshot against its arrival.
    picks = [("rdfc", None, RDFC, "-", "RDF-Connect"),
             ("cwl-incremental", 2, INCREMENTAL, "-", "incremental, k=2"),
             ("cwl-rebuild", 6, REBUILD, "--", "rebuild, k=6"),
             ("cwl-rebuild", 2, REBUILD, "-", "rebuild, k=2")]
    for arm, k, colour, style, label in picks:
        r = next((r for r in rows if r["arm"] == arm and r.get("interval") == k), None)
        if r is None:
            continue
        pts = [(i * arrival / 1000 / 60, x / 1000) for i, x in enumerate(r["freshnessMs"])
               if causes[i] and not absent(x)]
        b.plot([p[0] for p in pts], [p[1] for p in pts], style, color=colour, lw=1.1,
               marker=".", ms=2.5, label=label)
    b.set_yscale("log")
    plain_log(b)
    b.set_xlabel("arrival of the snapshot (min)")
    b.set_ylabel("freshness (s)")
    grid(b)
    b.legend(frameon=False, fontsize=6.5, loc="center", ncol=2, bbox_to_anchor=(0.5, 0.33),
             columnspacing=1.0)
    b.text(0.02, 0.97, "(b)", transform=b.transAxes, va="top", fontsize=8)

    fig.align_ylabels()
    fig.tight_layout()
    save(fig, out, "eval-freshness")


# ---------------------------------------------------------------------------
# Lines of configuration

def loc(*paths):
    """Lines that are neither blank nor comments."""
    total = 0
    for p in paths:
        for line in open(ROOT / p):
            s = line.strip()
            if s and not s.startswith("#"):
                total += 1
    return total


def config(out):
    """Two layers, counted the same way for both systems: what is written once
    per processor and can be published (processor descriptions, CWL tool
    descriptions), and what is written for this pipeline."""
    # Written once per processor. The mapper's RDF-Connect description ships
    # inside its jar and is not counted; the benchmark's own processors.ttl
    # describes benchmark components, not pipeline stages, and is left out.
    descriptions = loc("shacl-processor-ts/processors.ttl",
                       "node_modules/@rdfc/dumps-to-feed-processor-ts/processor.ttl",
                       "node_modules/@rdfc/sds-processors-ts/configs/sdsify.ttl",
                       "node_modules/@rdfc/sds-processors-ts/configs/bucketizer.ttl",
                       "node_modules/@rdfc/sds-processors-ts/configs/ldes_disk_writer.ttl")
    tools = loc(*[f"cwl/bb-{s}.cwl" for s in ("map", "validate", "changes", "sdsify", "bucketize", "writer")])

    # Written per pipeline.
    pipeline = loc("pipelines/bluebike__n5_r0.ttl") if (ROOT / "pipelines/bluebike__n5_r0.ttl").exists() \
        else loc("docs/change-effort/rdfc-before.ttl")
    workflow = loc("cwl/bluebike-batch.cwl")
    job = 10  # bench/bluebike.ts writeJob: one line per workflow input
    steps = loc(*[f"steps/{s}.ttl" for s in ("validate-bluebike", "dumps-to-feed", "sdsify-feed",
                                             "bucketize", "ldes-writer")])

    # Adding skolemisation (docs/change-effort/): the new workflow input also
    # needs a line in the job file.
    change_rdfc = 10
    change_workflow, change_job, change_step = 7, 1, loc("steps/skolemize.ttl")

    print(f"  config: once per processor rdfc {descriptions} (5 of 6) vs cwl tools {tools};"
          f" per pipeline rdfc {pipeline} vs cwl {workflow}+{job}+{steps};"
          f" change rdfc {change_rdfc} vs cwl {change_workflow}+{change_job}+{change_step}")

    DESC = "#98df8a"
    JOB = "#c7c7c7"
    STEP = "#ffbb78"
    fig, axes = plt.subplots(3, 1, figsize=(WIDTH, 3.4))

    def bars(ax, rows, title, xmax):
        ax.set_xlim(0, xmax)
        for y, segs in enumerate(rows):
            left = 0
            for value, colour in segs:
                ax.barh(y, value, left=left, color=colour, edgecolor="#555555", lw=0.6, height=0.6)
                if value >= 0.05 * xmax:
                    ax.text(left + value / 2, y, str(value), ha="center", va="center", fontsize=6.5,
                            color="white" if colour == RDFC else "black")
                left += value
            if len(segs) > 1:
                ax.text(left + 0.01 * xmax, y, str(left), va="center", fontsize=7)
        ax.set_yticks(range(len(rows)))
        ax.set_yticklabels(["RDF-Connect", "CWL"])
        ax.invert_yaxis()
        ax.set_title(title, fontsize=8, loc="left")
        ax.spines["left"].set_visible(False)
        ax.tick_params(axis="y", length=0)

    scale = max(descriptions, tools, pipeline, workflow + job + steps) * 1.12
    bars(axes[0], [[(descriptions, DESC)], [(tools, DESC)]],
         "written once per processor, publishable", scale)
    bars(axes[1], [[(pipeline, RDFC)], [(workflow, RDFC), (job, JOB), (steps, STEP)]],
         "written for this pipeline", scale)
    bars(axes[2], [[(change_rdfc, RDFC)], [(change_workflow, RDFC), (change_job, JOB), (change_step, STEP)]],
         "adding one processor to the pipeline", (change_workflow + change_job + change_step) * 1.12)
    axes[2].set_xlabel("lines of configuration")

    from matplotlib.patches import Patch
    parts = {"processor / tool descriptions": DESC, "pipeline / workflow": RDFC,
             "job file": JOB, "step configuration": STEP}
    handles = [Patch(facecolor=c, edgecolor="#555555", lw=0.6, label=l) for l, c in parts.items()]
    fig.legend(handles=handles, loc="lower center", ncol=2, frameon=False, fontsize=6.5,
               bbox_to_anchor=(0.5, 0.0), handlelength=1.6, columnspacing=1.0)
    fig.tight_layout(rect=(0, 0.1, 1, 1), h_pad=0.6)
    save(fig, out, "eval-config")


def main():
    res = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "server_results"
    out = Path(sys.argv[2]) if len(sys.argv) > 2 else ROOT / "figures"
    overhead(res, out)
    growth(res, out)
    freshness(res, out)
    config(out)


if __name__ == "__main__":
    main()
