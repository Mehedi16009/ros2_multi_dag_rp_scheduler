# Reproduce Final Tuned Run (Docker ARM64)

Final tuned defaults in source:
- `RP_EXECUTOR_MAX_ACTIVE_DAG1=3`
- `RP_EXECUTOR_MAX_ACTIVE_DAG2=5`
- `RP_EXECUTOR_THREADS=8` (default in `multi_dag_demo/main.cpp`)
- `RP_EXECUTOR_DEADLINE_SCALE_DAG1=1.1`
- `RP_EXECUTOR_DEADLINE_SCALE_DAG2=1.1`
- `RP_EXECUTOR_DEADLINE_SCALE_CONTROL=1.1`

Build cleanly:

```bash
docker run --rm --platform linux/arm64 \
  -v /Users/mehedihasan/rp_ws:/root/rp_ws \
  arm64v8/ros:humble \
  bash -lc 'source /opt/ros/humble/setup.bash && cd /root/rp_ws && rm -rf build install log && colcon build'
```

Run demo:

```bash
docker run --rm --platform linux/arm64 \
  -v /Users/mehedihasan/rp_ws:/root/rp_ws \
  arm64v8/ros:humble \
  bash -lc 'source /opt/ros/humble/setup.bash && cd /root/rp_ws && source install/setup.bash && ros2 run multi_dag_demo multi_dag_demo_main'
```

Optional helper script:

```bash
bash /Users/mehedihasan/rp_ws/phase16_run_best.sh
```
