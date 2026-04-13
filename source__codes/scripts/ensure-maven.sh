#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOOLS_DIR="$ROOT_DIR/.tools"
MAVEN_VERSION="${MAVEN_VERSION:-3.9.11}"
MAVEN_DIR="$TOOLS_DIR/apache-maven-$MAVEN_VERSION"
MAVEN_BIN="$MAVEN_DIR/bin/mvn"
MAVEN_TGZ="$TOOLS_DIR/apache-maven-$MAVEN_VERSION-bin.tar.gz"
MAVEN_URL="https://archive.apache.org/dist/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz"

if command -v mvn >/dev/null 2>&1; then
  command -v mvn
  exit 0
fi

mkdir -p "$TOOLS_DIR"

if [[ ! -x "$MAVEN_BIN" ]]; then
  rm -rf "$MAVEN_DIR"
  curl -fsSL "$MAVEN_URL" -o "$MAVEN_TGZ"
  tar -xzf "$MAVEN_TGZ" -C "$TOOLS_DIR"
fi

echo "$MAVEN_BIN"
