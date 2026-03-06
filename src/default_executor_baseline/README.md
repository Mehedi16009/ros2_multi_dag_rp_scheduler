# Default Executor Baseline (ROS 2 Humble/Iron)

This package provides a reproducible baseline using **unmodified** ROS 2 executors:
- `rclcpp::executors::SingleThreadedExecutor`
- `rclcpp::executors::MultiThreadedExecutor`

No internal executor scheduling logic is modified.

## Build (Release)

```bash
source /opt/ros/humble/setup.bash
cd ~/rp_ws
colcon build --packages-select default_executor_baseline --cmake-args -DCMAKE_BUILD_TYPE=Release
source install/setup.bash
```

## Single Run Example

```bash
taskset -c 2 ros2 run default_executor_baseline default_executor_baseline \
  --executor single \
  --threads 1 \
  --tasks 8 \
  --period-set harmonic \
  --utilization 0.8 \
  --dag-depth 3 \
  --duration-sec 60 \
  --warmup-sec 5 \
  --wcet-variation 0.1 \
  --output-dir ~/rp_ws/baseline_results \
  --run-id baseline_single_u08
```

## Matrix + Plot Scripts

- Run matrix:
  - `bash ~/rp_ws/src/default_executor_baseline/scripts/run_default_executor_matrix.sh ~/rp_ws ~/rp_ws/baseline_results`
- Generate plots:
  - `python3 ~/rp_ws/src/default_executor_baseline/scripts/plot_default_executor_results.py --input-dir ~/rp_ws/baseline_results`

## Output Files

Per run:
- `<run_id>_jobs.csv` (per-job timing)
- `<run_id>_summary.csv` (per-task aggregates)
- `<run_id>_config.csv` (task periods/WCET/dependencies)
- `<run_id>_runinfo.txt` (run metadata)
