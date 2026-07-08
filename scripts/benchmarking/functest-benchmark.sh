#!/usr/bin/env bash
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

# =============================================================================
# Functional Test Benchmark Runner
# =============================================================================
#
# Runs functional tests with different Docker resource allocations and
# pool/lane configurations, recording timing and OOM status for each run.
#
# Usage:
#   ./prompts/tmp/functest-benchmark.sh                    # use inline config
#   ./prompts/tmp/functest-benchmark.sh my-config.txt      # use external config
#
# Configuration format (inline or file):
#   Lines starting with "resources:" set Docker Desktop CPU/memory for
#   subsequent test runs. Lines with "pools lanes" define a test run.
#   Empty lines and lines starting with "#" are ignored.
#
#   Example:
#     resources: 10 8192
#     1 4
#     2 2
#     resources: 10 11264
#     2 3
#     3 4
#
#   This means:
#     - Set Docker to 10 CPUs, 8192 MiB RAM, then run 1×4, 2×2
#     - Set Docker to 10 CPUs, 11264 MiB RAM, then run 2×3, 3×4
#
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
APPS_DIR="$(cd "$SCRIPT_DIR/../../apps" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/out"
DOCKER_SOCK="$HOME/Library/Containers/com.docker.docker/Data/backend.sock"
RESULTS_FILE="$RESULTS_DIR/functest-benchmark-results-$(date +%Y%m%d-%H%M%S).txt"
OOM_PATTERN="was killed by the OOM killer"

# ---------------------------------------------------------------------------
# Default configuration (edit this, or pass a config file as $1)
# ---------------------------------------------------------------------------
read -r -d '' DEFAULT_CONFIG << 'CONFIG' || true
# Format: "resources: <cpus> <memory_mib>" sets Docker resources
#         "<pools> <lanes>" runs a test with those settings
# Memory is in MiB (e.g., 8192 = 8 GB, 11264 = 11 GB, 16384 = 16 GB)

resources: 10 8192
1 4
2 2
resources: 10 11264
2 3
3 4
CONFIG

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
  # Query the Docker Desktop backend API for current resource settings
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

  # POST to the Docker Desktop backend API — it restarts the VM internally
  local response
  response=$(curl -s -X POST --unix-socket "$DOCKER_SOCK" \
    -H "Content-Type: application/json" \
    -d "{\"vm\":{\"resources\":{\"memoryMiB\":{\"value\":${mem_mib}},\"cpus\":{\"value\":${cpus}}}}}" \
    "http://localhost/app/settings" 2>/dev/null) \
    || die "Failed to POST settings to Docker Desktop API"

  # Verify the API accepted the new values
  local new_cpus new_mem
  new_cpus=$(echo "$response" | jq '.vm.resources.cpus.value')
  new_mem=$(echo "$response" | jq '.vm.resources.memoryMiB.value')
  if [[ "$new_cpus" != "$cpus" || "$new_mem" != "$mem_mib" ]]; then
    die "API did not accept new settings. Expected ${cpus}/${mem_mib}, got ${new_cpus}/${new_mem}"
  fi

  # Wait for Docker daemon to come back (VM restarts internally)
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

run_test() {
  local pools=$1 lanes=$2 logfile=$3
  cd "$APPS_DIR"
  make func-test FUNC_TEST_CONTROLS="--pools $pools --lanes $lanes" > "$logfile" 2>&1
  return $?
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main() {
  check_prerequisites
  mkdir -p "$RESULTS_DIR"

  # Load configuration
  local config
  if [[ $# -ge 1 && -f "$1" ]]; then
    config=$(cat "$1")
    log "Using config file: $1"
  else
    config="$DEFAULT_CONFIG"
    log "Using inline config"
  fi

  # Ensure Docker is running
  wait_for_docker

  # Save original settings to restore at the end
  local orig_cpus orig_mem
  read -r orig_cpus orig_mem <<< "$(get_current_resources)"
  log "Original Docker settings: ${orig_cpus} CPUs, ${orig_mem} MiB"

  # Parse config and collect runs
  local current_cpus="" current_mem=""
  local -a runs=()   # each entry: "cpus mem pools lanes"

  while IFS= read -r line; do
    # Strip comments and whitespace
    line=$(echo "$line" | sed 's/#.*//' | xargs)
    [[ -z "$line" ]] && continue

    if [[ "$line" == resources:* ]]; then
      local rest="${line#resources:}"
      current_cpus=$(echo "$rest" | awk '{print $1}')
      current_mem=$(echo "$rest" | awk '{print $2}')
      [[ -n "$current_cpus" && -n "$current_mem" ]] || die "Invalid resources line: $line"
    else
      local pools lanes
      pools=$(echo "$line" | awk '{print $1}')
      lanes=$(echo "$line" | awk '{print $2}')
      [[ -n "$pools" && -n "$lanes" ]] || die "Invalid run line: $line"
      [[ -n "$current_cpus" ]] || die "No 'resources:' line before run: $line"
      runs+=("$current_cpus $current_mem $pools $lanes")
    fi
  done <<< "$config"

  local total=${#runs[@]}
  log "Planned $total test run(s)"
  log "Results will be written to: $RESULTS_FILE"

  # Print header
  printf "%-6s %-10s %-6s %-6s %-10s %-6s %s\n" \
    "CPUs" "Memory" "Pools" "Lanes" "Duration" "OOM?" "Status" | tee "$RESULTS_FILE"
  printf "%-6s %-10s %-6s %-6s %-10s %-6s %s\n" \
    "----" "------" "-----" "-----" "--------" "----" "------" | tee -a "$RESULTS_FILE"

  local active_cpus="" active_mem=""
  local run_num=0

  for entry in "${runs[@]}"; do
    run_num=$((run_num + 1))
    local cpus mem pools lanes
    read -r cpus mem pools lanes <<< "$entry"
    local mem_gb
    mem_gb=$(echo "scale=1; $mem / 1024" | bc)

    log ""
    log "===== Run $run_num/$total: ${cpus} CPUs, ${mem_gb}GB, ${pools}p×${lanes}l ====="

    # Change Docker resources if needed
    if [[ "$cpus" != "$active_cpus" || "$mem" != "$active_mem" ]]; then
      kill_mill
      cleanup_containers
      set_docker_resources "$cpus" "$mem"
      active_cpus="$cpus"
      active_mem="$mem"
    else
      # Clean up from previous run
      kill_mill
      cleanup_containers
      sleep 2
    fi

    # Run the test
    local logfile="$RESULTS_DIR/functest-run-${run_num}-${cpus}cpu-${mem}m-${pools}p-${lanes}l.log"
    local start_time=$SECONDS
    local exit_code=0

    log "Starting: make func-test --pools $pools --lanes $lanes"
    log "Log: $logfile"

    run_test "$pools" "$lanes" "$logfile" || exit_code=$?

    local elapsed=$((SECONDS - start_time))
    local minutes=$((elapsed / 60))
    local seconds=$((elapsed % 60))
    local duration="${minutes}m${seconds}s"

    # Check for OOM
    local oom="no"
    if grep -q "$OOM_PATTERN" "$logfile" 2>/dev/null; then
      oom="YES"
    fi

    # Determine status
    local status
    if [[ $exit_code -eq 0 ]]; then
      status="PASS"
    elif [[ "$oom" == "YES" ]]; then
      status="OOM"
    else
      status="FAIL($exit_code)"
    fi

    # Record result
    printf "%-6s %-10s %-6s %-6s %-10s %-6s %s\n" \
      "$cpus" "${mem_gb}GB" "$pools" "$lanes" "$duration" "$oom" "$status" | tee -a "$RESULTS_FILE"

    log "Finished: $duration, OOM=$oom, status=$status"
  done

  # Restore original settings if changed
  if [[ "$active_cpus" != "$orig_cpus" || "$active_mem" != "$orig_mem" ]]; then
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
