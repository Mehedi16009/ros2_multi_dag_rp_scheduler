#!/usr/bin/env python3
import csv
import math
import re
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


BASE = Path("/Users/mehedihasan/rp_ws")
SUMMARY_CSV = BASE / "phase15" / "summary.csv"
OUT_DIR = BASE / "phase16"
OUT_DIR.mkdir(parents=True, exist_ok=True)


INT_FIELDS = {
    "run_id",
    "max_active_dag1",
    "max_active_dag2",
    "threads",
    "dag1_executed",
    "dag1_deferred",
    "dag1_deadline_miss",
    "dag1_avg_exec_ns",
    "dag1_max_exec_ns",
    "dag1_avg_lateness_ns",
    "dag1_max_lateness_ns",
    "dag1_hist_0_1ms",
    "dag1_hist_1_5ms",
    "dag1_hist_5_10ms",
    "dag1_hist_gt_10ms",
    "dag2_executed",
    "dag2_deferred",
    "dag2_deadline_miss",
    "dag2_avg_exec_ns",
    "dag2_max_exec_ns",
    "dag2_avg_lateness_ns",
    "dag2_max_lateness_ns",
    "dag2_hist_0_1ms",
    "dag2_hist_1_5ms",
    "dag2_hist_5_10ms",
    "dag2_hist_gt_10ms",
}

FLOAT_FIELDS = {"deadline_scale", "dag1_miss_rate", "dag2_miss_rate"}


def load_summary():
    rows = []
    with SUMMARY_CSV.open(newline="") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            row = dict(raw)
            for key in INT_FIELDS:
                row[key] = int(row[key])
            for key in FLOAT_FIELDS:
                row[key] = float(row[key])
            row["combined_miss_rate"] = (row["dag1_miss_rate"] + row["dag2_miss_rate"]) / 2.0
            row["combined_avg_lateness_ns"] = (
                row["dag1_avg_lateness_ns"] + row["dag2_avg_lateness_ns"]
            ) / 2.0
            rows.append(row)
    return rows


DAG1_RE = re.compile(
    r"DAG1 executed=(\d+) deferred=(\d+) avg_exec_ns=(\d+) max_exec_ns=(\d+) "
    r"deadline_miss_dag1=(\d+) miss_rate_dag1=([0-9.]+) avg_lateness_ns=(\d+) max_lateness_ns=(\d+)"
)
DAG2_RE = re.compile(
    r"DAG2 executed=(\d+) deferred=(\d+) avg_exec_ns=(\d+) max_exec_ns=(\d+) "
    r"deadline_miss_dag2=(\d+) miss_rate_dag2=([0-9.]+) avg_lateness_ns=(\d+) max_lateness_ns=(\d+)"
)
HIST1_RE = re.compile(r"DAG1 lateness_hist 0-1ms=(\d+) 1-5ms=(\d+) 5-10ms=(\d+) >10ms=(\d+)")
HIST2_RE = re.compile(r"DAG2 lateness_hist 0-1ms=(\d+) 1-5ms=(\d+) 5-10ms=(\d+) >10ms=(\d+)")
ACTIVE_RE = re.compile(r"Executing callback .* dag1_active=(\d+) dag2_active=(\d+)")


def parse_last_match(lines, pattern):
    last = None
    for line in lines:
        m = pattern.search(line)
        if m:
            last = m
    return last


def parse_log_metrics(log_path):
    text = log_path.read_text(errors="ignore").splitlines()
    d1 = parse_last_match(text, DAG1_RE)
    d2 = parse_last_match(text, DAG2_RE)
    h1 = parse_last_match(text, HIST1_RE)
    h2 = parse_last_match(text, HIST2_RE)

    if not (d1 and d2 and h1 and h2):
        return None

    out = {
        "dag1_executed": int(d1.group(1)),
        "dag1_deferred": int(d1.group(2)),
        "dag1_avg_exec_ns": int(d1.group(3)),
        "dag1_max_exec_ns": int(d1.group(4)),
        "dag1_deadline_miss": int(d1.group(5)),
        "dag1_miss_rate": float(d1.group(6)),
        "dag1_avg_lateness_ns": int(d1.group(7)),
        "dag1_max_lateness_ns": int(d1.group(8)),
        "dag2_executed": int(d2.group(1)),
        "dag2_deferred": int(d2.group(2)),
        "dag2_avg_exec_ns": int(d2.group(3)),
        "dag2_max_exec_ns": int(d2.group(4)),
        "dag2_deadline_miss": int(d2.group(5)),
        "dag2_miss_rate": float(d2.group(6)),
        "dag2_avg_lateness_ns": int(d2.group(7)),
        "dag2_max_lateness_ns": int(d2.group(8)),
        "dag1_hist_0_1ms": int(h1.group(1)),
        "dag1_hist_1_5ms": int(h1.group(2)),
        "dag1_hist_5_10ms": int(h1.group(3)),
        "dag1_hist_gt_10ms": int(h1.group(4)),
        "dag2_hist_0_1ms": int(h2.group(1)),
        "dag2_hist_1_5ms": int(h2.group(2)),
        "dag2_hist_5_10ms": int(h2.group(3)),
        "dag2_hist_gt_10ms": int(h2.group(4)),
    }

    max_dag1 = 0
    max_dag2 = 0
    for line in text:
        m = ACTIVE_RE.search(line)
        if m:
            max_dag1 = max(max_dag1, int(m.group(1)))
            max_dag2 = max(max_dag2, int(m.group(2)))
    out["max_dag1_active_observed"] = max_dag1
    out["max_dag2_active_observed"] = max_dag2
    return out


def write_csv(path, rows, fieldnames):
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def avg_by(rows, key, value_fn):
    groups = defaultdict(list)
    for r in rows:
        groups[r[key]].append(value_fn(r))
    return {k: sum(v) / len(v) for k, v in sorted(groups.items())}


def plot_threads_vs_combined(rows):
    data = avg_by(rows, "threads", lambda r: r["combined_miss_rate"])
    xs = sorted(data.keys())
    ys = [data[x] for x in xs]

    plt.figure(figsize=(8, 5))
    plt.plot(xs, ys, marker="o", linewidth=2)
    plt.title("Threads vs Avg Combined Miss Rate")
    plt.xlabel("Executor Threads")
    plt.ylabel("Avg Combined Miss Rate")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUT_DIR / "threads_vs_avg_combined_miss.png", dpi=150)
    plt.close()


def plot_max_active_vs_miss(rows):
    dag1 = avg_by(rows, "max_active_dag1", lambda r: r["combined_miss_rate"])
    dag2 = avg_by(rows, "max_active_dag2", lambda r: r["combined_miss_rate"])
    xs = sorted(set(dag1.keys()) | set(dag2.keys()))
    y1 = [dag1[x] for x in xs]
    y2 = [dag2[x] for x in xs]

    plt.figure(figsize=(8, 5))
    plt.plot(xs, y1, marker="o", linewidth=2, label="Vary by DAG1 max_active")
    plt.plot(xs, y2, marker="s", linewidth=2, label="Vary by DAG2 max_active")
    plt.title("max_active DAG vs Avg Combined Miss Rate")
    plt.xlabel("max_active level")
    plt.ylabel("Avg Combined Miss Rate")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUT_DIR / "max_active_vs_avg_miss.png", dpi=150)
    plt.close()


def plot_deadline_scale_vs_miss_lateness(rows):
    miss = avg_by(rows, "deadline_scale", lambda r: r["combined_miss_rate"])
    late = avg_by(rows, "deadline_scale", lambda r: r["combined_avg_lateness_ns"] / 1e6)
    xs = sorted(miss.keys())
    y_miss = [miss[x] for x in xs]
    y_late = [late[x] for x in xs]

    fig, ax1 = plt.subplots(figsize=(8, 5))
    ax1.plot(xs, y_miss, marker="o", linewidth=2, color="tab:blue", label="Avg combined miss")
    ax1.set_xlabel("Deadline Scale")
    ax1.set_ylabel("Avg Combined Miss Rate", color="tab:blue")
    ax1.tick_params(axis="y", labelcolor="tab:blue")
    ax1.grid(True, alpha=0.3)

    ax2 = ax1.twinx()
    ax2.plot(xs, y_late, marker="s", linewidth=2, color="tab:red", label="Avg combined lateness (ms)")
    ax2.set_ylabel("Avg Combined Lateness (ms)", color="tab:red")
    ax2.tick_params(axis="y", labelcolor="tab:red")

    plt.title("Deadline Scaling vs Miss/Lateness")
    fig.tight_layout()
    plt.savefig(OUT_DIR / "deadline_scale_vs_miss_lateness.png", dpi=150)
    plt.close(fig)


def plot_lateness_hist(row, suffix):
    labels = ["0-1ms", "1-5ms", "5-10ms", ">10ms"]
    dag1 = [
        row["dag1_hist_0_1ms"],
        row["dag1_hist_1_5ms"],
        row["dag1_hist_5_10ms"],
        row["dag1_hist_gt_10ms"],
    ]
    dag2 = [
        row["dag2_hist_0_1ms"],
        row["dag2_hist_1_5ms"],
        row["dag2_hist_5_10ms"],
        row["dag2_hist_gt_10ms"],
    ]

    fig, axes = plt.subplots(1, 2, figsize=(11, 4), sharey=False)
    axes[0].bar(labels, dag1, color="#4472c4")
    axes[0].set_title(f"DAG1 Lateness Buckets ({suffix})")
    axes[0].set_ylabel("Count")
    axes[0].tick_params(axis="x", rotation=20)

    axes[1].bar(labels, dag2, color="#ed7d31")
    axes[1].set_title(f"DAG2 Lateness Buckets ({suffix})")
    axes[1].tick_params(axis="x", rotation=20)

    fig.tight_layout()
    fig.savefig(OUT_DIR / f"lateness_hist_{suffix}.png", dpi=150)
    plt.close(fig)


def fmt_pct(x):
    return f"{x:.6f}"


def main():
    rows = load_summary()

    # Extract and validate run metrics directly from logs.
    extracted = []
    enforcement_rows = []
    mismatch_rows = []
    for r in rows:
        log_path = Path(r["log_path"])
        parsed = parse_log_metrics(log_path)
        if parsed is None:
            mismatch_rows.append({"run_id": r["run_id"], "reason": "missing_parsed_metrics", "log_path": str(log_path)})
            continue

        ex = {"run_id": r["run_id"], "log_path": str(log_path)}
        ex.update(parsed)
        extracted.append(ex)

        dag1_ok = parsed["max_dag1_active_observed"] <= r["max_active_dag1"]
        dag2_ok = parsed["max_dag2_active_observed"] <= r["max_active_dag2"]
        enforcement_rows.append(
            {
                "run_id": r["run_id"],
                "max_active_dag1_cfg": r["max_active_dag1"],
                "max_active_dag2_cfg": r["max_active_dag2"],
                "max_dag1_active_observed": parsed["max_dag1_active_observed"],
                "max_dag2_active_observed": parsed["max_dag2_active_observed"],
                "dag1_enforced": int(dag1_ok),
                "dag2_enforced": int(dag2_ok),
                "all_enforced": int(dag1_ok and dag2_ok),
                "log_path": str(log_path),
            }
        )

        # Consistency checks against summary.
        for key in (
            "dag1_executed",
            "dag1_deferred",
            "dag1_deadline_miss",
            "dag1_avg_exec_ns",
            "dag1_max_exec_ns",
            "dag1_avg_lateness_ns",
            "dag1_max_lateness_ns",
            "dag2_executed",
            "dag2_deferred",
            "dag2_deadline_miss",
            "dag2_avg_exec_ns",
            "dag2_max_exec_ns",
            "dag2_avg_lateness_ns",
            "dag2_max_lateness_ns",
            "dag1_hist_0_1ms",
            "dag1_hist_1_5ms",
            "dag1_hist_5_10ms",
            "dag1_hist_gt_10ms",
            "dag2_hist_0_1ms",
            "dag2_hist_1_5ms",
            "dag2_hist_5_10ms",
            "dag2_hist_gt_10ms",
        ):
            if parsed[key] != r[key]:
                mismatch_rows.append(
                    {
                        "run_id": r["run_id"],
                        "reason": f"value_mismatch:{key}",
                        "summary_value": r[key],
                        "parsed_value": parsed[key],
                        "log_path": str(log_path),
                    }
                )
                break
        if not math.isclose(parsed["dag1_miss_rate"], r["dag1_miss_rate"], rel_tol=1e-9, abs_tol=1e-9):
            mismatch_rows.append(
                {
                    "run_id": r["run_id"],
                    "reason": "value_mismatch:dag1_miss_rate",
                    "summary_value": r["dag1_miss_rate"],
                    "parsed_value": parsed["dag1_miss_rate"],
                    "log_path": str(log_path),
                }
            )
        if not math.isclose(parsed["dag2_miss_rate"], r["dag2_miss_rate"], rel_tol=1e-9, abs_tol=1e-9):
            mismatch_rows.append(
                {
                    "run_id": r["run_id"],
                    "reason": "value_mismatch:dag2_miss_rate",
                    "summary_value": r["dag2_miss_rate"],
                    "parsed_value": parsed["dag2_miss_rate"],
                    "log_path": str(log_path),
                }
            )

    write_csv(
        OUT_DIR / "log_extracted_metrics.csv",
        extracted,
        [
            "run_id",
            "dag1_executed",
            "dag1_deferred",
            "dag1_avg_exec_ns",
            "dag1_max_exec_ns",
            "dag1_deadline_miss",
            "dag1_miss_rate",
            "dag1_avg_lateness_ns",
            "dag1_max_lateness_ns",
            "dag2_executed",
            "dag2_deferred",
            "dag2_avg_exec_ns",
            "dag2_max_exec_ns",
            "dag2_deadline_miss",
            "dag2_miss_rate",
            "dag2_avg_lateness_ns",
            "dag2_max_lateness_ns",
            "dag1_hist_0_1ms",
            "dag1_hist_1_5ms",
            "dag1_hist_5_10ms",
            "dag1_hist_gt_10ms",
            "dag2_hist_0_1ms",
            "dag2_hist_1_5ms",
            "dag2_hist_5_10ms",
            "dag2_hist_gt_10ms",
            "max_dag1_active_observed",
            "max_dag2_active_observed",
            "log_path",
        ],
    )
    write_csv(
        OUT_DIR / "dag_enforcement_check.csv",
        enforcement_rows,
        [
            "run_id",
            "max_active_dag1_cfg",
            "max_active_dag2_cfg",
            "max_dag1_active_observed",
            "max_dag2_active_observed",
            "dag1_enforced",
            "dag2_enforced",
            "all_enforced",
            "log_path",
        ],
    )
    write_csv(
        OUT_DIR / "log_summary_mismatches.csv",
        mismatch_rows,
        ["run_id", "reason", "summary_value", "parsed_value", "log_path"],
    )

    # Best / worst by combined miss.
    rows_sorted = sorted(rows, key=lambda r: r["combined_miss_rate"])
    best = rows_sorted[:5]
    worst = rows_sorted[-5:]

    best_worst_rows = []
    for kind, bucket in (("best", best), ("worst", worst)):
        for r in bucket:
            best_worst_rows.append(
                {
                    "category": kind,
                    "run_id": r["run_id"],
                    "max_active_dag1": r["max_active_dag1"],
                    "max_active_dag2": r["max_active_dag2"],
                    "threads": r["threads"],
                    "deadline_scale": r["deadline_scale"],
                    "dag1_miss_rate": fmt_pct(r["dag1_miss_rate"]),
                    "dag2_miss_rate": fmt_pct(r["dag2_miss_rate"]),
                    "combined_miss_rate": fmt_pct(r["combined_miss_rate"]),
                    "dag1_executed": r["dag1_executed"],
                    "dag2_executed": r["dag2_executed"],
                    "dag1_deferred": r["dag1_deferred"],
                    "dag2_deferred": r["dag2_deferred"],
                    "log_path": r["log_path"],
                }
            )
    write_csv(
        OUT_DIR / "best_worst_configs.csv",
        best_worst_rows,
        [
            "category",
            "run_id",
            "max_active_dag1",
            "max_active_dag2",
            "threads",
            "deadline_scale",
            "dag1_miss_rate",
            "dag2_miss_rate",
            "combined_miss_rate",
            "dag1_executed",
            "dag2_executed",
            "dag1_deferred",
            "dag2_deferred",
            "log_path",
        ],
    )

    # Cross configuration export.
    write_csv(
        OUT_DIR / "cross_configuration_table.csv",
        [
            {
                "run_id": r["run_id"],
                "max_active_dag1": r["max_active_dag1"],
                "max_active_dag2": r["max_active_dag2"],
                "threads": r["threads"],
                "deadline_scale": r["deadline_scale"],
                "dag1_executed": r["dag1_executed"],
                "dag1_deferred": r["dag1_deferred"],
                "dag1_miss_rate": fmt_pct(r["dag1_miss_rate"]),
                "dag1_avg_exec_ns": r["dag1_avg_exec_ns"],
                "dag1_max_exec_ns": r["dag1_max_exec_ns"],
                "dag1_avg_lateness_ns": r["dag1_avg_lateness_ns"],
                "dag1_max_lateness_ns": r["dag1_max_lateness_ns"],
                "dag2_executed": r["dag2_executed"],
                "dag2_deferred": r["dag2_deferred"],
                "dag2_miss_rate": fmt_pct(r["dag2_miss_rate"]),
                "dag2_avg_exec_ns": r["dag2_avg_exec_ns"],
                "dag2_max_exec_ns": r["dag2_max_exec_ns"],
                "dag2_avg_lateness_ns": r["dag2_avg_lateness_ns"],
                "dag2_max_lateness_ns": r["dag2_max_lateness_ns"],
                "combined_miss_rate": fmt_pct(r["combined_miss_rate"]),
                "log_path": r["log_path"],
            }
            for r in rows_sorted
        ],
        [
            "run_id",
            "max_active_dag1",
            "max_active_dag2",
            "threads",
            "deadline_scale",
            "dag1_executed",
            "dag1_deferred",
            "dag1_miss_rate",
            "dag1_avg_exec_ns",
            "dag1_max_exec_ns",
            "dag1_avg_lateness_ns",
            "dag1_max_lateness_ns",
            "dag2_executed",
            "dag2_deferred",
            "dag2_miss_rate",
            "dag2_avg_exec_ns",
            "dag2_max_exec_ns",
            "dag2_avg_lateness_ns",
            "dag2_max_lateness_ns",
            "combined_miss_rate",
            "log_path",
        ],
    )

    # Graphs.
    plot_threads_vs_combined(rows)
    plot_max_active_vs_miss(rows)
    plot_deadline_scale_vs_miss_lateness(rows)
    plot_lateness_hist(best[0], "best")
    plot_lateness_hist(worst[-1], "worst")

    # Report.
    all_enforced = sum(int(r["all_enforced"]) for r in enforcement_rows)
    report_lines = []
    report_lines.append("# Phase 16 Final Analysis")
    report_lines.append("")
    report_lines.append(f"- Total configurations: {len(rows)}")
    report_lines.append(f"- Log/summary mismatch rows: {len(mismatch_rows)}")
    report_lines.append(
        f"- DAG restriction enforcement: {all_enforced}/{len(enforcement_rows)} runs passed"
    )
    report_lines.append("")
    report_lines.append("## Best Configurations (Lowest Combined Miss)")
    report_lines.append("")
    report_lines.append("|run_id|a1|a2|threads|dscale|dag1_miss|dag2_miss|combined_miss|")
    report_lines.append("|---:|---:|---:|---:|---:|---:|---:|---:|")
    for r in best:
        report_lines.append(
            f"|{r['run_id']}|{r['max_active_dag1']}|{r['max_active_dag2']}|{r['threads']}|"
            f"{r['deadline_scale']:.1f}|{r['dag1_miss_rate']:.6f}|{r['dag2_miss_rate']:.6f}|"
            f"{r['combined_miss_rate']:.6f}|"
        )
    report_lines.append("")
    report_lines.append("## Worst Configurations (Highest Combined Miss)")
    report_lines.append("")
    report_lines.append("|run_id|a1|a2|threads|dscale|dag1_miss|dag2_miss|combined_miss|")
    report_lines.append("|---:|---:|---:|---:|---:|---:|---:|---:|")
    for r in reversed(worst):
        report_lines.append(
            f"|{r['run_id']}|{r['max_active_dag1']}|{r['max_active_dag2']}|{r['threads']}|"
            f"{r['deadline_scale']:.1f}|{r['dag1_miss_rate']:.6f}|{r['dag2_miss_rate']:.6f}|"
            f"{r['combined_miss_rate']:.6f}|"
        )
    report_lines.append("")
    report_lines.append("## Artifacts")
    report_lines.append("")
    report_lines.append("- `cross_configuration_table.csv`")
    report_lines.append("- `best_worst_configs.csv`")
    report_lines.append("- `log_extracted_metrics.csv`")
    report_lines.append("- `dag_enforcement_check.csv`")
    report_lines.append("- `threads_vs_avg_combined_miss.png`")
    report_lines.append("- `max_active_vs_avg_miss.png`")
    report_lines.append("- `deadline_scale_vs_miss_lateness.png`")
    report_lines.append("- `lateness_hist_best.png`")
    report_lines.append("- `lateness_hist_worst.png`")

    (OUT_DIR / "final_report.md").write_text("\n".join(report_lines) + "\n")


if __name__ == "__main__":
    main()
