#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE_TAG="${1:-wsh-hadoop-cluster:3.4.3}"

docker build -t "$IMAGE_TAG" -f "$ROOT_DIR/Dockerfile.hadoop-cluster" "$ROOT_DIR"

echo "Built Docker Hadoop image $IMAGE_TAG"
