#!/usr/bin/env python3
"""Figure: how cost grows with the input, for every system.

Reads the size sweep and draws CPU time and wall-clock time against the number
of snapshots. Colour is the system, line style is the CWL engine, so the three
engines running the same workflow document sit on top of each other.

    python3 bench/plots/growth.py server_results/bluebike.json out.pdf
"""
import json, sys, collections, statistics as st
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

SERIES = [
    # arm, label, colour, style
    ("shell",              "Shell pipeline",  "#444444", "-"),
    ("rdfc",               "RDF-Connect",     "#1f77b4", "-"),
    ("cwl-batch",          "CWL, batch",      "#2ca02c", "-"),
    ("streamflow-batch",   None,              "#2ca02c", "--"),
    ("toil-batch",         None,              "#2ca02c", ":"),
    ("cwl-scatter",        "CWL, scattered",  "#d62728", "-"),
    ("streamflow-scatter", None,              "#d62728", "--"),
    ("toil-scatter",       None,              "#d62728", ":"),
    ("nextflow",           "Nextflow",        "#ff7f0e", "-"),
]

def main(src, out):
    rows = [r for r in json.load(open(src)) if r.get("ok")]
    by = collections.defaultdict(dict)
    for r in rows:
        by[r["arm"]].setdefault(r["snapshots"], []).append(r)
    med = lambda rs, f: st.median([f(r) for r in rs])

    fig, axes = plt.subplots(2, 1, figsize=(3.4, 4.35), sharex=True)
    panels = [
        (axes[0], lambda r: r["cpuMs"] / 1000, "CPU time (s)"),
        (axes[1], lambda r: r["wallMs"] / 1000, "elapsed time (s)"),
    ]
    for ax, f, ylabel in panels:
        for arm, label, colour, style in SERIES:
            if arm not in by:
                continue
            ns = sorted(by[arm])
            ys = [med(by[arm][n], f) for n in ns]
            ax.plot(ns, ys, style, color=colour, label=label,
                    marker="o" if style == "-" else None, markersize=3, linewidth=1.3)
        ax.set_yscale("log")
        ax.set_ylabel(ylabel)
        ax.set_xticks(sorted({r["snapshots"] for r in rows}))
        ax.grid(True, which="major", axis="both", lw=0.4, alpha=0.4)
        ax.tick_params(labelsize=8)
        ax.xaxis.label.set_size(9)
        ax.yaxis.label.set_size(9)

    from matplotlib.lines import Line2D
    from matplotlib.ticker import LogLocator, FuncFormatter
    for ax, _, _ in panels:
        ax.yaxis.set_major_locator(LogLocator(base=10, subs=(1.0, 2.0, 5.0), numticks=12))
        ax.yaxis.set_minor_locator(LogLocator(base=10, subs=tuple(range(2, 10)), numticks=12))
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
        ax.yaxis.set_minor_formatter(FuncFormatter(lambda v, _: ""))

    axes[1].set_xlabel("snapshots processed")

    handles, labels = axes[0].get_legend_handles_labels()
    engines = [Line2D([], [], color="#777777", linestyle=st, lw=1.3)
               for st in ("-", "--", ":")]
    fig.legend(handles + engines,
               labels + ["cwltool", "StreamFlow", "Toil (CWL engines)"],
               loc="lower center", ncol=2, fontsize=7.5, frameon=False,
               columnspacing=1.2, handlelength=2.0, borderaxespad=0.0,
               bbox_to_anchor=(0.5, 0.002))
    fig.tight_layout(rect=(0, 0.165, 1, 1.0))

    fig.savefig(out)
    print("wrote", out)

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
