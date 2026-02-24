#!/usr/bin/env bash
set -euo pipefail

WS_HOST="/Users/mehedihasan/rp_ws"
OUT_DIR="${WS_HOST}/phase15"
LOG_DIR="${OUT_DIR}/logs"
SUMMARY_CSV="${OUT_DIR}/summary.csv"
STATUS_LOG="${OUT_DIR}/status.log"

mkdir -p "${LOG_DIR}"

cat > "${SUMMARY_CSV}" <<'CSV'
run_id,max_active_dag1,max_active_dag2,threads,deadline_scale,build_status,run_status,dag1_executed,dag1_deferred,dag1_deadline_miss,dag1_miss_rate,dag1_avg_exec_ns,dag1_max_exec_ns,dag1_avg_lateness_ns,dag1_max_lateness_ns,dag1_hist_0_1ms,dag1_hist_1_5ms,dag1_hist_5_10ms,dag1_hist_gt_10ms,dag2_executed,dag2_deferred,dag2_deadline_miss,dag2_miss_rate,dag2_avg_exec_ns,dag2_max_exec_ns,dag2_avg_lateness_ns,dag2_max_lateness_ns,dag2_hist_0_1ms,dag2_hist_1_5ms,dag2_hist_5_10ms,dag2_hist_gt_10ms,log_path
CSV

: > "${STATUS_LOG}"

active_levels_dag1=(2 3 4 5)
active_levels_dag2=(2 3 4 5)
thread_levels=(4 6 8 10)

total_runs=$(( ${#active_levels_dag1[@]} * ${#active_levels_dag2[@]} * ${#thread_levels[@]} ))
max_runs="${MAX_RUNS:-0}"
run_id=0

parse_metric_line() {
  local line="$1"
  local dag_prefix="$2"
  local dag_key="$3"
  local regex
  regex="${dag_prefix} executed=([0-9]+) deferred=([0-9]+) avg_exec_ns=([0-9]+) max_exec_ns=([0-9]+) deadline_miss_${dag_key}=([0-9]+) miss_rate_${dag_key}=([0-9.]+) avg_lateness_ns=([0-9]+) max_lateness_ns=([0-9]+)"
  if [[ "${line}" =~ ${regex} ]]; then
    echo "${BASH_REMATCH[1]},${BASH_REMATCH[2]},${BASH_REMATCH[5]},${BASH_REMATCH[6]},${BASH_REMATCH[3]},${BASH_REMATCH[4]},${BASH_REMATCH[7]},${BASH_REMATCH[8]}"
    return 0
  fi
  echo ",,,,,,,"
  return 1
}

parse_hist_line() {
  local line="$1"
  local regex="0-1ms=([0-9]+) 1-5ms=([0-9]+) 5-10ms=([0-9]+) >10ms=([0-9]+)"
  if [[ "${line}" =~ ${regex} ]]; then
    echo "${BASH_REMATCH[1]},${BASH_REMATCH[2]},${BASH_REMATCH[3]},${BASH_REMATCH[4]}"
    return 0
  fi
  echo ",,,"
  return 1
}

for active_dag1 in "${active_levels_dag1[@]}"; do
  for active_dag2 in "${active_levels_dag2[@]}"; do
    for threads in "${thread_levels[@]}"; do
      if [[ "${max_runs}" -gt 0 && "${run_id}" -ge "${max_runs}" ]]; then
        echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] EARLY_STOP ${run_id}/${total_runs} (MAX_RUNS=${max_runs})" | tee -a "${STATUS_LOG}"
        echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] COMPLETE ${run_id}/${total_runs}" | tee -a "${STATUS_LOG}"
        exit 0
      fi

      case "${threads}" in
        4) dscale="0.8" ;;
        6) dscale="0.9" ;;
        8) dscale="1.1" ;;
        10) dscale="1.2" ;;
        *) dscale="1.0" ;;
      esac
      run_id=$((run_id + 1))

      scale_tag="${dscale/./p}"
      run_tag="r$(printf "%03d" "${run_id}")_a1_${active_dag1}_a2_${active_dag2}_t${threads}_d${scale_tag}"
      run_log_host="${LOG_DIR}/${run_tag}.log"
      build_log_host="${LOG_DIR}/${run_tag}_build.log"
      run_log_container="/root/rp_ws/phase15/logs/${run_tag}.log"
      build_log_container="/root/rp_ws/phase15/logs/${run_tag}_build.log"

      echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] START ${run_id}/${total_runs} ${run_tag}" | tee -a "${STATUS_LOG}"

      build_status="ok"
      run_status="ok"

      if ! docker run --rm --platform linux/arm64 \
        -v "${WS_HOST}:/root/rp_ws" \
        arm64v8/ros:humble \
        bash -lc "
          set -eo pipefail
          source /opt/ros/humble/setup.bash
          cd /root/rp_ws
          rm -rf build install log
          colcon build > '${build_log_container}' 2>&1
          source install/setup.bash
          ec=0
          timeout 15s env \
            RP_EXECUTOR_TRACE=1 \
            RP_EXECUTOR_MAX_ACTIVE_DAG1='${active_dag1}' \
            RP_EXECUTOR_MAX_ACTIVE_DAG2='${active_dag2}' \
            RP_EXECUTOR_THREADS='${threads}' \
            RP_EXECUTOR_DEADLINE_SCALE_DAG1='${dscale}' \
            RP_EXECUTOR_DEADLINE_SCALE_DAG2='${dscale}' \
            RP_EXECUTOR_DEADLINE_SCALE_CONTROL='${dscale}' \
            ros2 run multi_dag_demo multi_dag_demo_main \
              --ros-args --log-level warn --log-level rp_executor:=info --log-level multi_dag_demo_main:=info \
              > '${run_log_container}' 2>&1 || ec=\$?
          if [ \$ec -ne 0 ] && [ \$ec -ne 124 ]; then
            exit \$ec
          fi
          exit 0
        "; then
        build_status="failed"
        run_status="failed"
        echo "${run_id},${active_dag1},${active_dag2},${threads},${dscale},${build_status},${run_status},,,,,,,,,,,,,,,,,,,,,,,,,,${run_log_host}" >> "${SUMMARY_CSV}"
        echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] FAIL  ${run_id}/${total_runs} ${run_tag}" | tee -a "${STATUS_LOG}"
        continue
      fi

      d1_line="$(rg "DAG1 executed=" "${run_log_host}" | tail -n 1 || true)"
      d2_line="$(rg "DAG2 executed=" "${run_log_host}" | tail -n 1 || true)"
      h1_line="$(rg "DAG1 lateness_hist" "${run_log_host}" | tail -n 1 || true)"
      h2_line="$(rg "DAG2 lateness_hist" "${run_log_host}" | tail -n 1 || true)"

      d1_csv="$(parse_metric_line "${d1_line}" "DAG1" "dag1")"
      d2_csv="$(parse_metric_line "${d2_line}" "DAG2" "dag2")"
      h1_csv="$(parse_hist_line "${h1_line}")"
      h2_csv="$(parse_hist_line "${h2_line}")"

      if [[ "${d1_csv}" == ",,,,,,," ]]; then
        run_status="missing_metrics"
      fi
      if [[ "${d2_csv}" == ",,,,,,," ]]; then
        run_status="missing_metrics"
      fi
      if [[ "${h1_csv}" == ",,," ]]; then
        run_status="missing_metrics"
      fi
      if [[ "${h2_csv}" == ",,," ]]; then
        run_status="missing_metrics"
      fi

      echo "${run_id},${active_dag1},${active_dag2},${threads},${dscale},${build_status},${run_status},${d1_csv},${h1_csv},${d2_csv},${h2_csv},${run_log_host}" >> "${SUMMARY_CSV}"
      echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] DONE  ${run_id}/${total_runs} ${run_tag}" | tee -a "${STATUS_LOG}"
    done
  done
done

echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] COMPLETE ${total_runs}/${total_runs}" | tee -a "${STATUS_LOG}"
