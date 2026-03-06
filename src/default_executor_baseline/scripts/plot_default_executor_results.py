#!/usr/bin/env python3
import csv
import math
import os
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def executor_label(executor_type: str, threads: int) -> str:
  if executor_type == "single":
    return "single"
  return f"multi_{threads}"


def load_summary_rows(input_dir: Path):
  rows = []
  for path in sorted(input_dir.glob("*_summary.csv")):
    with path.open(newline="") as f:
      reader = csv.DictReader(f)
      for raw in reader:
        row = {
          "run_id": raw["run_id"],
          "executor_type": raw["executor_type"],
          "threads": int(raw["threads"]),
          "num_tasks": int(raw["num_tasks"]),
          "period_set": raw["period_set"],
          "utilization": float(raw["utilization"]),
          "dag_depth": int(raw["dag_depth"]),
          "task_id": int(raw["task_id"]),
          "total_jobs": int(raw["total_jobs"]),
          "misses": int(raw["misses"]),
          "miss_rate": float(raw["miss_rate"]),
          "max_response_us": float(raw["maximum_response_time_us"]),
          "max_lateness_us": float(raw["maximum_lateness_us"]),
          "mean_response_us": float(raw["mean_response_time_us"]),
          "stddev_response_us": float(raw["stddev_response_time_us"]),
          "p99_response_us": float(raw["p99_response_time_us"]),
        }
        row["executor_label"] = executor_label(row["executor_type"], row["threads"])
        rows.append(row)
  return rows


def load_job_rows(input_dir: Path):
  rows = []
  for path in sorted(input_dir.glob("*_jobs.csv")):
    with path.open(newline="") as f:
      reader = csv.DictReader(f)
      for raw in reader:
        row = {
          "run_id": raw["run_id"],
          "executor_type": raw["executor_type"],
          "threads": int(raw["threads"]),
          "utilization": float(raw["utilization"]),
          "response_time_us": float(raw["response_time_us"]),
          "lateness_us": float(raw["lateness_us"]),
          "missed": int(raw["missed"]),
        }
        row["executor_label"] = executor_label(row["executor_type"], row["threads"])
        rows.append(row)
  return rows


def aggregate_run_level(summary_rows):
  grouped = defaultdict(list)
  for row in summary_rows:
    grouped[row["run_id"]].append(row)

  run_level = []
  for run_id, rows in grouped.items():
    base = rows[0]
    total_jobs = sum(r["total_jobs"] for r in rows)
    total_misses = sum(r["misses"] for r in rows)
    miss_rate = (float(total_misses) / float(total_jobs)) if total_jobs > 0 else 0.0
    max_lateness = max(r["max_lateness_us"] for r in rows)
    mean_stddev = sum(r["stddev_response_us"] for r in rows) / float(len(rows))
    run_level.append(
      {
        "run_id": run_id,
        "executor_label": base["executor_label"],
        "utilization": base["utilization"],
        "num_tasks": base["num_tasks"],
        "period_set": base["period_set"],
        "dag_depth": base["dag_depth"],
        "total_jobs": total_jobs,
        "total_misses": total_misses,
        "miss_rate": miss_rate,
        "max_lateness_us": max_lateness,
        "mean_stddev_response_us": mean_stddev,
      }
    )
  return run_level


def mean(values):
  if not values:
    return 0.0
  return sum(values) / float(len(values))


def write_csv(path: Path, rows, fieldnames):
  with path.open("w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
      writer.writerow(row)


def plot_miss_rate_vs_util(run_level, out_dir: Path):
  grouped = defaultdict(lambda: defaultdict(list))
  for row in run_level:
    grouped[row["executor_label"]][row["utilization"]].append(row["miss_rate"])

  plt.figure(figsize=(8, 5))
  for label in sorted(grouped.keys()):
    xs = sorted(grouped[label].keys())
    ys = [mean(grouped[label][x]) for x in xs]
    plt.plot(xs, ys, marker="o", linewidth=2, label=label)

  plt.title("Miss Rate vs Utilization (Default ROS 2 Executors)")
  plt.xlabel("Utilization")
  plt.ylabel("Miss Rate")
  plt.grid(alpha=0.3)
  plt.legend()
  plt.tight_layout()
  plt.savefig(out_dir / "miss_rate_vs_utilization.png", dpi=160)
  plt.close()


def plot_max_lateness_vs_util(run_level, out_dir: Path):
  grouped = defaultdict(lambda: defaultdict(list))
  for row in run_level:
    grouped[row["executor_label"]][row["utilization"]].append(row["max_lateness_us"] / 1000.0)

  plt.figure(figsize=(8, 5))
  for label in sorted(grouped.keys()):
    xs = sorted(grouped[label].keys())
    ys = [mean(grouped[label][x]) for x in xs]
    plt.plot(xs, ys, marker="s", linewidth=2, label=label)

  plt.title("Maximum Lateness vs Utilization")
  plt.xlabel("Utilization")
  plt.ylabel("Maximum Lateness (ms)")
  plt.grid(alpha=0.3)
  plt.legend()
  plt.tight_layout()
  plt.savefig(out_dir / "max_lateness_vs_utilization.png", dpi=160)
  plt.close()


def plot_response_cdf(job_rows, out_dir: Path):
  grouped = defaultdict(list)
  for row in job_rows:
    grouped[row["executor_label"]].append(row["response_time_us"])

  plt.figure(figsize=(8, 5))
  for label in sorted(grouped.keys()):
    xs = sorted(grouped[label])
    if not xs:
      continue
    ys = [(i + 1) / float(len(xs)) for i in range(len(xs))]
    plt.plot(xs, ys, linewidth=2, label=label)

  plt.title("Response-Time CDF")
  plt.xlabel("Response Time (us)")
  plt.ylabel("CDF")
  plt.grid(alpha=0.3)
  plt.legend()
  plt.tight_layout()
  plt.savefig(out_dir / "response_time_cdf.png", dpi=160)
  plt.close()


def plot_jitter_comparison(summary_rows, out_dir: Path):
  grouped = defaultdict(list)
  for row in summary_rows:
    grouped[row["executor_label"]].append(row["stddev_response_us"])

  labels = sorted(grouped.keys())
  values = [mean(grouped[label]) for label in labels]

  plt.figure(figsize=(8, 5))
  plt.bar(labels, values, color=["#4e79a7", "#f28e2b", "#59a14f"])
  plt.title("Jitter Comparison Across Executors")
  plt.xlabel("Executor")
  plt.ylabel("Mean Stddev of Response Time (us)")
  plt.grid(axis="y", alpha=0.3)
  plt.tight_layout()
  plt.savefig(out_dir / "jitter_comparison.png", dpi=160)
  plt.close()


def main():
  import argparse

  parser = argparse.ArgumentParser(description="Generate baseline executor plots from CSV logs.")
  parser.add_argument("--input-dir", default=str(Path.home() / "rp_ws" / "baseline_results"))
  parser.add_argument("--output-dir", default="")
  args = parser.parse_args()

  input_dir = Path(args.input_dir)
  output_dir = Path(args.output_dir) if args.output_dir else input_dir
  output_dir.mkdir(parents=True, exist_ok=True)

  summary_rows = load_summary_rows(input_dir)
  job_rows = load_job_rows(input_dir)
  if not summary_rows:
    raise RuntimeError(f"No *_summary.csv files found in {input_dir}")
  if not job_rows:
    raise RuntimeError(f"No *_jobs.csv files found in {input_dir}")

  run_level = aggregate_run_level(summary_rows)
  write_csv(
    output_dir / "run_level_metrics.csv",
    run_level,
    [
      "run_id",
      "executor_label",
      "utilization",
      "num_tasks",
      "period_set",
      "dag_depth",
      "total_jobs",
      "total_misses",
      "miss_rate",
      "max_lateness_us",
      "mean_stddev_response_us",
    ],
  )

  util_summary = []
  grouped = defaultdict(lambda: defaultdict(list))
  for row in run_level:
    grouped[row["executor_label"]][row["utilization"]].append(row)
  for label in sorted(grouped.keys()):
    for util in sorted(grouped[label].keys()):
      rows = grouped[label][util]
      util_summary.append(
        {
          "executor_label": label,
          "utilization": util,
          "avg_miss_rate": mean([r["miss_rate"] for r in rows]),
          "avg_max_lateness_us": mean([r["max_lateness_us"] for r in rows]),
          "avg_mean_stddev_response_us": mean([r["mean_stddev_response_us"] for r in rows]),
        }
      )
  write_csv(
    output_dir / "utilization_aggregates.csv",
    util_summary,
    [
      "executor_label",
      "utilization",
      "avg_miss_rate",
      "avg_max_lateness_us",
      "avg_mean_stddev_response_us",
    ],
  )

  plot_miss_rate_vs_util(run_level, output_dir)
  plot_max_lateness_vs_util(run_level, output_dir)
  plot_response_cdf(job_rows, output_dir)
  plot_jitter_comparison(summary_rows, output_dir)

  print(f"Generated plots and aggregate CSV files in: {output_dir}")


if __name__ == "__main__":
  main()
