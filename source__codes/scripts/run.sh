#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ $# -eq 0 || "${1:-}" == "--help" || "${1:-}" == "-h" || "${1:-}" == "help" ]]; then
  cat <<'EOF'
Usage:
  ./scripts/run.sh generate-data [options]
  ./scripts/run.sh run [options]
  ./scripts/run.sh compare [options]
  ./scripts/run.sh run-job [options]

Use `./scripts/run.sh <command> --help` for command-level help.
EOF
  exit 0
fi

if [[ ! -f "$ROOT_DIR/target/wsh-app.jar" ]]; then
  "$ROOT_DIR/scripts/build.sh"
fi

JAVA_OPTS_STRING="${WSH_JAVA_OPTS:-${GENE2LIFE_JAVA_OPTS:-${JAVA_OPTS:-}}}"

if [[ -n "$JAVA_OPTS_STRING" ]]; then
  read -r -a JAVA_OPTS_ARRAY <<< "$JAVA_OPTS_STRING"
  java "${JAVA_OPTS_ARRAY[@]}" -jar "$ROOT_DIR/target/wsh-app.jar" "$@"
else
  java -jar "$ROOT_DIR/target/wsh-app.jar" "$@"
fi
