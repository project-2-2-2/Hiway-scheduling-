#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:-all}"
HADOOP_CLUSTER_WORKDIR="${HADOOP_CLUSTER_WORKDIR:-$ROOT_DIR/work/hadoop-docker-cluster}"
HADOOP_CLUSTER_IMAGE="${HADOOP_CLUSTER_IMAGE:-wsh-hadoop-cluster:3.4.3}"
CLUSTER_CONFIG="${CLUSTER_CONFIG:-$ROOT_DIR/config/clusters-z4-g5-paper-sweep.csv}"
PROJECT_NAME="${HADOOP_DOCKER_PROJECT:-hadoop-paper-cluster}"

cleanup_docker_nodes() {
  local containers
  containers="$(docker ps -aq --filter label=org.wsh.runtime=docker-node)"
  if [[ -n "$containers" ]]; then
    docker rm -f $containers >/dev/null
  fi
}

cleanup_hadoop_cluster() {
  HADOOP_DOCKER_PROJECT="$PROJECT_NAME" \
    "$ROOT_DIR/scripts/hadoop-docker-cluster.sh" down "$CLUSTER_CONFIG" "$HADOOP_CLUSTER_WORKDIR" "$HADOOP_CLUSTER_IMAGE" >/dev/null 2>&1 || true
}

case "$MODE" in
  docker-nodes)
    cleanup_docker_nodes
    ;;
  hadoop-cluster)
    cleanup_hadoop_cluster
    ;;
  all)
    cleanup_docker_nodes
    cleanup_hadoop_cluster
    ;;
  *)
    echo "Usage: $0 {docker-nodes|hadoop-cluster|all}" >&2
    exit 1
    ;;
esac
