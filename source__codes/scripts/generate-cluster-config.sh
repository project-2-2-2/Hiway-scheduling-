#!/usr/bin/env bash
set -euo pipefail

OUTPUT_PATH="${1:-config/clusters-autogen.csv}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if command -v nproc >/dev/null 2>&1; then
  TOTAL_THREADS="$(nproc)"
else
  TOTAL_THREADS="$(getconf _NPROCESSORS_ONLN)"
fi

if command -v free >/dev/null 2>&1; then
  TOTAL_MEMORY_MB="$(free -m | awk '/^Mem:/ {print $2}')"
else
  TOTAL_MEMORY_MB=32768
fi

SAFE_MEMORY_MB=$(( TOTAL_MEMORY_MB * 80 / 100 ))
NODES_PER_CLUSTER="${NODES_PER_CLUSTER:-2}"

mkdir -p "$(dirname "$OUTPUT_PATH")"

THREAD_SHARE_1=$(( TOTAL_THREADS * 33 / 100 ))
THREAD_SHARE_2=$(( TOTAL_THREADS * 28 / 100 ))
THREAD_SHARE_3=$(( TOTAL_THREADS * 22 / 100 ))
THREAD_SHARE_4=$(( TOTAL_THREADS - THREAD_SHARE_1 - THREAD_SHARE_2 - THREAD_SHARE_3 ))

MEM_SHARE_1=$(( SAFE_MEMORY_MB * 34 / 100 ))
MEM_SHARE_2=$(( SAFE_MEMORY_MB * 28 / 100 ))
MEM_SHARE_3=$(( SAFE_MEMORY_MB * 22 / 100 ))
MEM_SHARE_4=$(( SAFE_MEMORY_MB - MEM_SHARE_1 - MEM_SHARE_2 - MEM_SHARE_3 ))

PER_NODE_THREADS_1=$(( THREAD_SHARE_1 / NODES_PER_CLUSTER ))
PER_NODE_THREADS_2=$(( THREAD_SHARE_2 / NODES_PER_CLUSTER ))
PER_NODE_THREADS_3=$(( THREAD_SHARE_3 / NODES_PER_CLUSTER ))
PER_NODE_THREADS_4=$(( THREAD_SHARE_4 / NODES_PER_CLUSTER ))

PER_NODE_MEM_1=$(( MEM_SHARE_1 / NODES_PER_CLUSTER ))
PER_NODE_MEM_2=$(( MEM_SHARE_2 / NODES_PER_CLUSTER ))
PER_NODE_MEM_3=$(( MEM_SHARE_3 / NODES_PER_CLUSTER ))
PER_NODE_MEM_4=$(( MEM_SHARE_4 / NODES_PER_CLUSTER ))

PER_NODE_THREADS_1=$(( PER_NODE_THREADS_1 < 1 ? 1 : PER_NODE_THREADS_1 ))
PER_NODE_THREADS_2=$(( PER_NODE_THREADS_2 < 1 ? 1 : PER_NODE_THREADS_2 ))
PER_NODE_THREADS_3=$(( PER_NODE_THREADS_3 < 1 ? 1 : PER_NODE_THREADS_3 ))
PER_NODE_THREADS_4=$(( PER_NODE_THREADS_4 < 1 ? 1 : PER_NODE_THREADS_4 ))

PER_NODE_MEM_1=$(( PER_NODE_MEM_1 < 1024 ? 1024 : PER_NODE_MEM_1 ))
PER_NODE_MEM_2=$(( PER_NODE_MEM_2 < 1024 ? 1024 : PER_NODE_MEM_2 ))
PER_NODE_MEM_3=$(( PER_NODE_MEM_3 < 1024 ? 1024 : PER_NODE_MEM_3 ))
PER_NODE_MEM_4=$(( PER_NODE_MEM_4 < 1024 ? 1024 : PER_NODE_MEM_4 ))

BUFFER_1=4096
BUFFER_2=3072
BUFFER_3=2048
BUFFER_4=1024

{
  echo "# auto-generated from host resources"
  echo "# cluster_id,node_id,cpu_threads,io_buffer_kb,memory_mb,cpu_set"
  next_cpu=0
  for i in $(seq 1 "$NODES_PER_CLUSTER"); do
    end_cpu=$(( next_cpu + PER_NODE_THREADS_1 - 1 ))
    echo "C1,c1-n${i},${PER_NODE_THREADS_1},${BUFFER_1},${PER_NODE_MEM_1},${next_cpu}-${end_cpu}"
    next_cpu=$(( end_cpu + 1 ))
  done
  for i in $(seq 1 "$NODES_PER_CLUSTER"); do
    end_cpu=$(( next_cpu + PER_NODE_THREADS_2 - 1 ))
    echo "C2,c2-n${i},${PER_NODE_THREADS_2},${BUFFER_2},${PER_NODE_MEM_2},${next_cpu}-${end_cpu}"
    next_cpu=$(( end_cpu + 1 ))
  done
  for i in $(seq 1 "$NODES_PER_CLUSTER"); do
    end_cpu=$(( next_cpu + PER_NODE_THREADS_3 - 1 ))
    echo "C3,c3-n${i},${PER_NODE_THREADS_3},${BUFFER_3},${PER_NODE_MEM_3},${next_cpu}-${end_cpu}"
    next_cpu=$(( end_cpu + 1 ))
  done
  for i in $(seq 1 "$NODES_PER_CLUSTER"); do
    end_cpu=$(( next_cpu + PER_NODE_THREADS_4 - 1 ))
    echo "C4,c4-n${i},${PER_NODE_THREADS_4},${BUFFER_4},${PER_NODE_MEM_4},${next_cpu}-${end_cpu}"
    next_cpu=$(( end_cpu + 1 ))
  done
} > "$OUTPUT_PATH"

echo "Wrote cluster config to $OUTPUT_PATH"
echo "Detected threads: $TOTAL_THREADS"
echo "Detected memory: ${TOTAL_MEMORY_MB} MB"
