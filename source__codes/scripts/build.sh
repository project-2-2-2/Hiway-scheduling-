#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MAVEN_BIN="$("$ROOT_DIR/scripts/ensure-maven.sh")"

"$MAVEN_BIN" -Dmaven.repo.local="$ROOT_DIR/.m2/repository" clean package

echo "Built shaded jar at $ROOT_DIR/target/wsh-app.jar"
