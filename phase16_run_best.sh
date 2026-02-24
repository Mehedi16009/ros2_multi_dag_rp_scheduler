#!/usr/bin/env bash
set -euo pipefail

docker run --rm --platform linux/arm64 \
  -v /Users/mehedihasan/rp_ws:/root/rp_ws \
  arm64v8/ros:humble \
  bash -lc '
    source /opt/ros/humble/setup.bash
    cd /root/rp_ws
    source install/setup.bash
    # Uses tuned defaults from source:
    # threads=8, max_active_dag1=3, max_active_dag2=5, deadline_scale=1.1
    ros2 run multi_dag_demo multi_dag_demo_main
  '
