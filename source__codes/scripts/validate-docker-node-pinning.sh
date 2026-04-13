#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER_CONFIG="${1:-$ROOT_DIR/config/clusters-z4-g5-paper-sweep.csv}"
IMAGE_TAG="${2:-wsh-java:latest}"
CONTAINER_PREFIX="wsh-pincheck"

cleanup() {
  local containers
  containers="$(docker ps -aq --filter "label=org.wsh.runtime=pincheck")"
  if [[ -n "$containers" ]]; then
    docker rm -f $containers >/dev/null 2>&1 || true
  fi
}

trap cleanup EXIT

if ! docker image inspect "$IMAGE_TAG" >/dev/null 2>&1; then
  "$ROOT_DIR/scripts/build-image.sh" "$IMAGE_TAG"
fi

while IFS= read -r line; do
  [[ -n "$line" ]] || continue
  IFS=',' read -r cluster_id node_id cpu_threads io_buffer_kb memory_mb cpu_set <<< "$line"
  container_name="${CONTAINER_PREFIX}-${node_id}"
  docker run -d --rm \
    --name "$container_name" \
    --label org.wsh.runtime=pincheck \
    --cpus "$cpu_threads" \
    ${cpu_set:+--cpuset-cpus "$cpu_set"} \
    --memory "${memory_mb}m" \
    --entrypoint sh \
    "$IMAGE_TAG" \
    -lc 'while true; do sleep 3600; done' >/dev/null

  inspect_output="$(docker inspect "$container_name" --format '{{.HostConfig.CpusetCpus}}|{{.HostConfig.Memory}}|{{.HostConfig.NanoCpus}}')"
  IFS='|' read -r actual_cpuset actual_memory actual_nano_cpus <<< "$inspect_output"
  if [[ -n "${cpu_set:-}" && "$actual_cpuset" != "$cpu_set" ]]; then
    echo "Unexpected cpuset for $container_name: expected $cpu_set, got $actual_cpuset" >&2
    exit 1
  fi
  if [[ "$actual_memory" != "$(( memory_mb * 1024 * 1024 ))" ]]; then
    echo "Unexpected memory limit for $container_name: expected ${memory_mb}m, got ${actual_memory} bytes" >&2
    exit 1
  fi
  if [[ "$actual_nano_cpus" != "$(( cpu_threads * 1000000000 ))" ]]; then
    echo "Unexpected CPU quota for $container_name: expected ${cpu_threads} cores, got ${actual_nano_cpus} nano CPUs" >&2
    exit 1
  fi
  if [[ -n "${cpu_set:-}" ]]; then
    effective_cpuset="$(docker exec "$container_name" sh -lc "awk '/^Cpus_allowed_list:/ {print \$2}' /proc/self/status")"
    if [[ "$effective_cpuset" != "$cpu_set" ]]; then
      echo "Unexpected effective CPU list for $container_name: expected $cpu_set, got $effective_cpuset" >&2
      exit 1
    fi
  fi
  echo "Validated $container_name cpu_threads=$cpu_threads cpu_set=${cpu_set:-<none>} memory_mb=$memory_mb"
done < <(grep -v '^\s*#' "$CLUSTER_CONFIG" | grep -v '^\s*$')

echo "Docker node pinning validation completed for $CLUSTER_CONFIG"
