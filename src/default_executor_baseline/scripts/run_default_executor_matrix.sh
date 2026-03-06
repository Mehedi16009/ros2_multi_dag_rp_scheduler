#!/usr/bin/env bash
set -euo pipefail

# Example:
#   bash run_default_executor_matrix.sh /home/user/rp_ws /home/user/rp_ws/baseline_results

WS_DIR="${1:-$HOME/rp_ws}"
OUT_DIR="${2:-$WS_DIR/baseline_results}"
mkdir -p "${OUT_DIR}"

source /opt/ros/humble/setup.bash
cd "${WS_DIR}"
colcon build --packages-select default_executor_baseline --cmake-args -DCMAKE_BUILD_TYPE=Release
source install/setup.bash

# Recommended reproducibility controls on Ubuntu host.
bash "${WS_DIR}/src/default_executor_baseline/scripts/configure_cpu_environment.sh" || true

N_LIST=(8 12)
PERIOD_SETS=(harmonic nonharmonic)
UTILS=(0.4 0.6 0.8 0.9)
DAG_DEPTHS=(1 2 3 4)
WCET_VAR=0.1
DURATION_SEC=60
WARMUP_SEC=5

timestamp="$(date +%Y%m%d_%H%M%S)"
STATUS_CSV="${OUT_DIR}/matrix_status_${timestamp}.csv"
echo "run_id,executor,threads,tasks,period_set,utilization,dag_depth,status" > "${STATUS_CSV}"

run_cfg() {
  local executor="$1"
  local threads="$2"
  local tasks="$3"
  local period_set="$4"
  local util="$5"
  local depth="$6"

  local run_id="exec_${executor}_th${threads}_n${tasks}_p${period_set}_u${util}_d${depth}_${timestamp}"
  local cpuset
  if [[ "${executor}" == "single" ]]; then
    cpuset="2"
  else
    if [[ "${threads}" -eq 2 ]]; then
      cpuset="2-3"
    else
      cpuset="2-5"
    fi
  fi

  echo "[baseline] ${run_id} (cpuset=${cpuset})"
  if taskset -c "${cpuset}" \
    ros2 run default_executor_baseline default_executor_baseline \
      --executor "${executor}" \
      --threads "${threads}" \
      --tasks "${tasks}" \
      --period-set "${period_set}" \
      --utilization "${util}" \
      --dag-depth "${depth}" \
      --duration-sec "${DURATION_SEC}" \
      --warmup-sec "${WARMUP_SEC}" \
      --wcet-variation "${WCET_VAR}" \
      --output-dir "${OUT_DIR}" \
      --run-id "${run_id}"; then
    echo "${run_id},${executor},${threads},${tasks},${period_set},${util},${depth},ok" >> "${STATUS_CSV}"
  else
    echo "${run_id},${executor},${threads},${tasks},${period_set},${util},${depth},failed" >> "${STATUS_CSV}"
  fi
}

for tasks in "${N_LIST[@]}"; do
  for period_set in "${PERIOD_SETS[@]}"; do
    for util in "${UTILS[@]}"; do
      for depth in "${DAG_DEPTHS[@]}"; do
        run_cfg single 1 "${tasks}" "${period_set}" "${util}" "${depth}"
        run_cfg multi 2 "${tasks}" "${period_set}" "${util}" "${depth}"
        run_cfg multi 4 "${tasks}" "${period_set}" "${util}" "${depth}"
      done
    done
  done
done

echo "[baseline] Matrix completed. Status: ${STATUS_CSV}"
