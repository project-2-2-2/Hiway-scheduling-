#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE_TAG="${1:-wsh-java:latest}"

"$ROOT_DIR/scripts/build.sh"
docker build -t "$IMAGE_TAG" "$ROOT_DIR"

echo "Built Docker image $IMAGE_TAG"
