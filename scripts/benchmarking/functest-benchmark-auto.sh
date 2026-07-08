#!/usr/bin/env bash
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

# =============================================================================
# Functional Test Auto-Parallelism Benchmark
# =============================================================================
#
# Tests the automatic pool/lane formula across different Docker memory levels.
# CPUs are fixed at 10. For each memory value, sets Docker resources via the
# backend API, then runs `make func-test` with NO --pools/--lanes overrides,
# letting the formula pick automatically. Records timing, OOM status, and the
# pools×lanes the formula chose.
#
# Usage:
#   ./prompts/tmp/functest-benchmark-auto.sh
#
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
APPS_DIR="$(cd "$SCRIPT_DIR/../../apps" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/out"
DOCKER_SOCK="$HOME/Library/Containers/com.docker.docker/Data/backend.sock"
RESULTS_FILE="$RESULTS_DIR/functest-benchmark-auto-$(date +%Y%m%d-%H%M%S).txt"
OOM_PATTERN="was killed by the OOM killer"
POOLS_LANES_PATTERN="Using [0-9]+ pools with [0-9]+ lanes"
FIXED_CPUS=10

# ---------------------------------------------------------------------------
# Memory levels to test (MiB). Edit this list as needed.
# ---------------------------------------------------------------------------
MEMORY_LEVELS=(
  4096    # 4 GB
  6144    # 6 GB
  7168    # 7 GB
  8192    # 8 GB
  10240   # 10 GB
  12288   # 12 GB
  14336   # 14 GB
  16384   # 16 GB
  20480   # 20 GB
  24576   # 24 GB
  32768   # 32 GB
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

log() {
  echo "[$(date +%H:%M:%S)] $*"
}

die() {
  echo "ERROR: $*" >&2
  exit 1
}

check_prerequisites() {
  command -v docker >/dev/null 2>&1 || die "docker not found"
  command -v curl >/dev/null 2>&1 || die "curl not found"
  command -v jq >/dev/null 2>&1 || die "jq not found (brew install jq)"
  command -v make >/dev/null 2>&1 || die "make not found"
  [[ -S "$DOCKER_SOCK" ]] || die "Docker Desktop backend socket not found at: $DOCKER_SOCK"
  [[ -d "$APPS_DIR" ]] || die "apps directory not found at: $APPS_DIR"
}

get_current_resources() {
  local response
  response=$(curl -s --unix-socket "$DOCKER_SOCK" "http://localhost/app/settings" 2>/dev/null) \
    || die "Failed to query Docker Desktop API"
  local cpus mem_mib
  cpus=$(echo "$response" | jq '.vm.resources.cpus.value')
  mem_mib=$(echo "$response" | jq '.vm.resources.memoryMiB.value')
  echo "$cpus $mem_mib"
}

set_docker_resources() {
  local cpus=$1 mem_mib=$2
  local current
  current=$(get_current_resources)
  local cur_cpus cur_mem
  cur_cpus=$(echo "$current" | awk '{print $1}')
  cur_mem=$(echo "$current" | awk '{print $2}')

  if [[ "$cur_cpus" == "$cpus" && "$cur_mem" == "$mem_mib" ]]; then
    log "Docker already configured: ${cpus} CPUs, ${mem_mib} MiB — skipping"
    return 0
  fi

  log "Changing Docker resources: ${cpus} CPUs, ${mem_mib} MiB (was: ${cur_cpus} CPUs, ${cur_mem} MiB)"

  local response
  response=$(curl -s -X POST --unix-socket "$DOCKER_SOCK" \
    -H "Content-Type: application/json" \
    -d "{\"vm\":{\"resources\":{\"memoryMiB\":{\"value\":${mem_mib}},\"cpus\":{\"value\":${cpus}}}}}" \
    "http://localhost/app/settings" 2>/dev/null) \
    || die "Failed to POST settings to Docker Desktop API"

  local new_cpus new_mem
  new_cpus=$(echo "$response" | jq '.vm.resources.cpus.value')
  new_mem=$(echo "$response" | jq '.vm.resources.memoryMiB.value')
  if [[ "$new_cpus" != "$cpus" || "$new_mem" != "$mem_mib" ]]; then
    die "API did not accept new settings. Expected ${cpus}/${mem_mib}, got ${new_cpus}/${new_mem}"
  fi

  log "Waiting for Docker daemon to come back..."
  local attempts=0
  while ! docker info >/dev/null 2>&1; do
    attempts=$((attempts + 1))
    if [[ $attempts -ge 120 ]]; then
      die "Docker daemon did not recover within 120 seconds after settings change"
    fi
    sleep 1
  done

  local actual_mem
  actual_mem=$(docker info 2>/dev/null | grep "Total Memory:" | awk '{print $3 $4}')
  log "Docker ready: ${cpus} CPUs, ${actual_mem}"
}

wait_for_docker() {
  log "Waiting for Docker daemon..."
  local attempts=0
  while ! docker info >/dev/null 2>&1; do
    attempts=$((attempts + 1))
    if [[ $attempts -ge 120 ]]; then
      die "Docker daemon not available within 120 seconds"
    fi
    sleep 1
  done
}

cleanup_containers() {
  docker ps -aq --filter "name=^scribe-ft-" 2>/dev/null | xargs docker rm -f 2>/dev/null || true
  docker network ls -q --filter "name=^scribe-ft-" 2>/dev/null | xargs docker network rm 2>/dev/null || true
  docker volume ls -q --filter "name=^scribe-ft-" 2>/dev/null | xargs docker volume rm 2>/dev/null || true
}

kill_mill() {
  pkill -9 -f MillServerMain 2>/dev/null || true
  sleep 2
}

extract_pools_lanes() {
  local logfile=$1
  # Extract "Using N pools with M lanes" from test output
  local match
  match=$(grep -oE "$POOLS_LANES_PATTERN" "$logfile" 2>/dev/null | head -1) || true
  if [[ -n "$match" ]]; then
    local pools lanes
    pools=$(echo "$match" | grep -oE '[0-9]+' | head -1)
    lanes=$(echo "$match" | grep -oE '[0-9]+' | tail -1)
    echo "${pools}×${lanes}"
  else
    echo "?×?"
  fi
}

run_test() {
  local logfile=$1
  cd "$APPS_DIR"
  # No --pools/--lanes: let the formula decide
  make func-test > "$logfile" 2>&1
  return $?
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main() {
  check_prerequisites
  mkdir -p "$RESULTS_DIR"
  wait_for_docker

  local orig_cpus orig_mem
  read -r orig_cpus orig_mem <<< "$(get_current_resources)"
  log "Original Docker settings: ${orig_cpus} CPUs, ${orig_mem} MiB"

  local total=${#MEMORY_LEVELS[@]}
  log "Planned $total test run(s) at ${FIXED_CPUS} CPUs"
  log "Memory levels: ${MEMORY_LEVELS[*]} MiB"
  log "Results will be written to: $RESULTS_FILE"

  # Print header
  printf "%-10s %-12s %-10s %-6s %s\n" \
    "Memory" "Auto P×L" "Duration" "OOM?" "Status" | tee "$RESULTS_FILE"
  printf "%-10s %-12s %-10s %-6s %s\n" \
    "------" "--------" "--------" "----" "------" | tee -a "$RESULTS_FILE"

  local run_num=0

  for mem_mib in "${MEMORY_LEVELS[@]}"; do
    run_num=$((run_num + 1))
    local mem_gb
    mem_gb=$(echo "scale=0; $mem_mib / 1024" | bc)

    log ""
    log "===== Run $run_num/$total: ${FIXED_CPUS} CPUs, ${mem_gb}GB (auto pools×lanes) ====="

    kill_mill
    cleanup_containers
    set_docker_resources "$FIXED_CPUS" "$mem_mib"

    local logfile="$RESULTS_DIR/functest-auto-${run_num}-${mem_gb}gb-$(date +%Y%m%d-%H%M%S).log"
    local start_time=$SECONDS
    local exit_code=0

    log "Starting: make func-test (auto parallelism)"
    log "Log: $logfile"

    run_test "$logfile" || exit_code=$?

    local elapsed=$((SECONDS - start_time))
    local minutes=$((elapsed / 60))
    local seconds=$((elapsed % 60))
    local duration="${minutes}m${seconds}s"

    local pools_lanes
    pools_lanes=$(extract_pools_lanes "$logfile")

    local oom="no"
    if grep -q "$OOM_PATTERN" "$logfile" 2>/dev/null; then
      oom="YES"
    fi

    local status
    if [[ $exit_code -eq 0 ]]; then
      status="PASS"
    elif [[ "$oom" == "YES" ]]; then
      status="OOM"
    else
      status="FAIL($exit_code)"
    fi

    printf "%-10s %-12s %-10s %-6s %s\n" \
      "${mem_gb}GB" "$pools_lanes" "$duration" "$oom" "$status" | tee -a "$RESULTS_FILE"

    log "Finished: ${mem_gb}GB → ${pools_lanes}, $duration, OOM=$oom, status=$status"
  done

  # Restore original settings if changed
  if [[ "$orig_cpus" != "$FIXED_CPUS" || "$orig_mem" != "${MEMORY_LEVELS[-1]}" ]]; then
    log ""
    log "Restoring original Docker settings: ${orig_cpus} CPUs, ${orig_mem} MiB"
    kill_mill
    cleanup_containers
    set_docker_resources "$orig_cpus" "$orig_mem"
  fi

  log ""
  log "===== Benchmark Complete ====="
  log "Results: $RESULTS_FILE"
  echo ""
  cat "$RESULTS_FILE"
}

main "$@"
