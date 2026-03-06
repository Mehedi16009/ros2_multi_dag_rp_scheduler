#!/usr/bin/env bash
set -euo pipefail

echo "[baseline] Configuring host CPU environment for reproducibility (Ubuntu 22.04 expected)."

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "[baseline] Non-Linux host detected; skipping governor/turbo controls."
  exit 0
fi

if command -v cpupower >/dev/null 2>&1; then
  sudo cpupower frequency-set -g performance || true
else
  echo "[baseline] cpupower not found; install linux-tools-common/linux-tools-generic for governor control."
fi

if [[ -f /sys/devices/system/cpu/intel_pstate/no_turbo ]]; then
  echo 1 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo >/dev/null || true
fi

if [[ -f /sys/devices/system/cpu/cpufreq/boost ]]; then
  echo 0 | sudo tee /sys/devices/system/cpu/cpufreq/boost >/dev/null || true
fi

echo "[baseline] Current governor state:"
if [[ -d /sys/devices/system/cpu/cpu0/cpufreq ]]; then
  cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor || true
fi
if [[ -f /sys/devices/system/cpu/intel_pstate/no_turbo ]]; then
  echo "intel_pstate/no_turbo=$(cat /sys/devices/system/cpu/intel_pstate/no_turbo)"
fi
if [[ -f /sys/devices/system/cpu/cpufreq/boost ]]; then
  echo "cpufreq/boost=$(cat /sys/devices/system/cpu/cpufreq/boost)"
fi

echo "[baseline] Note: this baseline intentionally avoids RT kernel patches and priority tuning."
