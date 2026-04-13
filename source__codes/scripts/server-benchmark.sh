#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKSPACE="${WORKSPACE:-$ROOT_DIR/work/server-benchmark}"
WORKFLOW="${WORKFLOW:-gene2life}"
DATA_ROOT="${DATA_ROOT:-$WORKSPACE/data}"
SUBMISSION_MODE="${SUBMISSION_MODE:-true}"
FULL_SWEEP="${FULL_SWEEP:-false}"
if [[ "$FULL_SWEEP" == "true" ]]; then
  SUBMISSION_MODE="false"
fi
DEFAULT_PROFILE="small"
DEFAULT_COMPARE_ROUNDS="3"
DEFAULT_TRAINING_WARMUP_RUNS="1"
DEFAULT_TRAINING_MEASURE_RUNS="3"
if [[ "$FULL_SWEEP" == "true" ]]; then
  DEFAULT_PROFILE="medium"
  DEFAULT_COMPARE_ROUNDS="3"
  DEFAULT_TRAINING_WARMUP_RUNS="1"
  DEFAULT_TRAINING_MEASURE_RUNS="3"
fi
PROFILE="${PROFILE:-$DEFAULT_PROFILE}"
CLUSTER_CONFIG="${CLUSTER_CONFIG:-$ROOT_DIR/config/clusters-z4-g5-paper-sweep.csv}"
WSH_JAVA_OPTS="${WSH_JAVA_OPTS:-${GENE2LIFE_JAVA_OPTS:--Xms4g -Xmx16g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseStringDeduplication}}"
export WSH_JAVA_OPTS
COMPARE_ROUNDS="${COMPARE_ROUNDS:-$DEFAULT_COMPARE_ROUNDS}"
MAX_NODES="${MAX_NODES:-0}"
EXECUTOR="${EXECUTOR:-hdfs-docker}"
DOCKER_IMAGE="${DOCKER_IMAGE:-wsh-java:latest}"
HADOOP_CLUSTER_IMAGE="${HADOOP_CLUSTER_IMAGE:-wsh-hadoop-cluster:3.4.3}"
HADOOP_CLUSTER_WORKDIR="${HADOOP_CLUSTER_WORKDIR:-$ROOT_DIR/work/hadoop-docker-cluster}"
HADOOP_DOCKER_ACCESS_HOST="${HADOOP_DOCKER_ACCESS_HOST:-$(hostname -I 2>/dev/null | awk '{print $1}')}"
HADOOP_DOCKER_ACCESS_HOST="${HADOOP_DOCKER_ACCESS_HOST:-localhost}"
HADOOP_DOCKER_NN_PORT="${HADOOP_DOCKER_NN_PORT:-19000}"
HADOOP_DOCKER_YARN_RM_PORT="${HADOOP_DOCKER_YARN_RM_PORT:-18032}"
HADOOP_FS_DEFAULT="${HADOOP_FS_DEFAULT:-hdfs://${HADOOP_DOCKER_ACCESS_HOST}:${HADOOP_DOCKER_NN_PORT}}"
HADOOP_YARN_RM="${HADOOP_YARN_RM:-${HADOOP_DOCKER_ACCESS_HOST}:${HADOOP_DOCKER_YARN_RM_PORT}}"
HADOOP_FRAMEWORK_NAME="${HADOOP_FRAMEWORK_NAME:-yarn}"
HADOOP_ENABLE_NODE_LABELS="${HADOOP_ENABLE_NODE_LABELS:-true}"
HADOOP_KEEP_CLUSTER="${HADOOP_KEEP_CLUSTER:-false}"
TRAINING_WARMUP_RUNS="${TRAINING_WARMUP_RUNS:-$DEFAULT_TRAINING_WARMUP_RUNS}"
TRAINING_MEASURE_RUNS="${TRAINING_MEASURE_RUNS:-$DEFAULT_TRAINING_MEASURE_RUNS}"
SCHEDULERS="${SCHEDULERS:-wsh,heft}"
REUSE_DATA="${REUSE_DATA:-true}"
SKIP_BUILD="${SKIP_BUILD:-false}"
GENERATE_ARGS=()
COMPARE_EXTRA_ARGS=()
GENERATION_METADATA_FILE="$DATA_ROOT/.generation-metadata.env"

cleanup_runtime() {
  if [[ "$EXECUTOR" == "docker" ]]; then
    "$ROOT_DIR/scripts/cleanup-project-runtime.sh" docker-nodes
  elif [[ ("$EXECUTOR" == "hadoop" || "$EXECUTOR" == "hdfs-docker") && "$HADOOP_KEEP_CLUSTER" != "true" ]]; then
    HADOOP_CLUSTER_WORKDIR="$HADOOP_CLUSTER_WORKDIR" \
      HADOOP_CLUSTER_IMAGE="$HADOOP_CLUSTER_IMAGE" \
      CLUSTER_CONFIG="$CLUSTER_CONFIG" \
      "$ROOT_DIR/scripts/cleanup-project-runtime.sh" all
  elif [[ "$EXECUTOR" == "hdfs-docker" ]]; then
    "$ROOT_DIR/scripts/cleanup-project-runtime.sh" docker-nodes
  fi
}

trap cleanup_runtime EXIT

case "$WORKFLOW" in
  gene2life)
    case "$PROFILE" in
      small)
        QUERY_COUNT="${QUERY_COUNT:-192}"
        REFERENCE_RECORDS_PER_SHARD="${REFERENCE_RECORDS_PER_SHARD:-180000}"
        SEQUENCE_LENGTH="${SEQUENCE_LENGTH:-280}"
        ;;
      medium)
        QUERY_COUNT="${QUERY_COUNT:-256}"
        REFERENCE_RECORDS_PER_SHARD="${REFERENCE_RECORDS_PER_SHARD:-300000}"
        SEQUENCE_LENGTH="${SEQUENCE_LENGTH:-320}"
        ;;
      large)
        QUERY_COUNT="${QUERY_COUNT:-320}"
        REFERENCE_RECORDS_PER_SHARD="${REFERENCE_RECORDS_PER_SHARD:-500000}"
        SEQUENCE_LENGTH="${SEQUENCE_LENGTH:-360}"
        ;;
      *)
        echo "Unsupported PROFILE: $PROFILE" >&2
        exit 1
        ;;
    esac
    GENERATE_ARGS=(
      --query-count "$QUERY_COUNT"
      --reference-records-per-shard "$REFERENCE_RECORDS_PER_SHARD"
      --sequence-length "$SEQUENCE_LENGTH"
    )
    ;;
  avianflu_small)
    AVIANFLU_AUTODOCK_COUNT="${AVIANFLU_AUTODOCK_COUNT:-100}"
    case "$PROFILE" in
      small)
        RECEPTOR_FEATURE_COUNT="${RECEPTOR_FEATURE_COUNT:-192}"
        LIGAND_ATOM_COUNT="${LIGAND_ATOM_COUNT:-32}"
        ;;
      medium)
        RECEPTOR_FEATURE_COUNT="${RECEPTOR_FEATURE_COUNT:-256}"
        LIGAND_ATOM_COUNT="${LIGAND_ATOM_COUNT:-48}"
        ;;
      large)
        RECEPTOR_FEATURE_COUNT="${RECEPTOR_FEATURE_COUNT:-320}"
        LIGAND_ATOM_COUNT="${LIGAND_ATOM_COUNT:-64}"
        ;;
      *)
        echo "Unsupported PROFILE: $PROFILE" >&2
        exit 1
        ;;
    esac
    GENERATE_ARGS=(
      --avianflu-autodock-count "$AVIANFLU_AUTODOCK_COUNT"
      --receptor-feature-count "$RECEPTOR_FEATURE_COUNT"
      --ligand-atom-count "$LIGAND_ATOM_COUNT"
    )
    COMPARE_EXTRA_ARGS=(
      --avianflu-autodock-count "$AVIANFLU_AUTODOCK_COUNT"
    )
    ;;
  epigenomics)
    EPIGENOMICS_SPLIT_COUNT="${EPIGENOMICS_SPLIT_COUNT:-24}"
    case "$PROFILE" in
      small)
        READS_PER_SPLIT="${READS_PER_SPLIT:-160}"
        READ_LENGTH="${READ_LENGTH:-72}"
        REFERENCE_RECORD_COUNT="${REFERENCE_RECORD_COUNT:-480}"
        ;;
      medium)
        READS_PER_SPLIT="${READS_PER_SPLIT:-192}"
        READ_LENGTH="${READ_LENGTH:-80}"
        REFERENCE_RECORD_COUNT="${REFERENCE_RECORD_COUNT:-640}"
        ;;
      large)
        READS_PER_SPLIT="${READS_PER_SPLIT:-224}"
        READ_LENGTH="${READ_LENGTH:-88}"
        REFERENCE_RECORD_COUNT="${REFERENCE_RECORD_COUNT:-768}"
        ;;
      *)
        echo "Unsupported PROFILE: $PROFILE" >&2
        exit 1
        ;;
    esac
    GENERATE_ARGS=(
      --epigenomics-split-count "$EPIGENOMICS_SPLIT_COUNT"
      --reads-per-split "$READS_PER_SPLIT"
      --read-length "$READ_LENGTH"
      --reference-record-count "$REFERENCE_RECORD_COUNT"
    )
    COMPARE_EXTRA_ARGS=(
      --epigenomics-split-count "$EPIGENOMICS_SPLIT_COUNT"
    )
    ;;
  *)
    echo "Unsupported WORKFLOW: $WORKFLOW" >&2
    exit 1
    ;;
esac

if [[ "$SKIP_BUILD" != "true" ]]; then
  "$ROOT_DIR/scripts/build.sh"
fi

if [[ ! -f "$CLUSTER_CONFIG" ]]; then
  "$ROOT_DIR/scripts/generate-cluster-config.sh" "$CLUSTER_CONFIG"
fi

if [[ "$EXECUTOR" == "docker" ]]; then
  "$ROOT_DIR/scripts/build-image.sh" "$DOCKER_IMAGE"
elif [[ "$EXECUTOR" == "hadoop" || "$EXECUTOR" == "hdfs-docker" ]]; then
  "$ROOT_DIR/scripts/hadoop-docker-cluster.sh" up "$CLUSTER_CONFIG" "$HADOOP_CLUSTER_WORKDIR" "$HADOOP_CLUSTER_IMAGE"
  HADOOP_CONF_DIR="$HADOOP_CLUSTER_WORKDIR/host-conf"
  HADOOP_FS_DEFAULT="hdfs://${HADOOP_DOCKER_ACCESS_HOST}:${HADOOP_DOCKER_NN_PORT}"
  HADOOP_YARN_RM="${HADOOP_DOCKER_ACCESS_HOST}:${HADOOP_DOCKER_YARN_RM_PORT}"
  HADOOP_FRAMEWORK_NAME="yarn"
fi

mkdir -p "$WORKSPACE"
mkdir -p "$(dirname "$DATA_ROOT")"

DESIRED_METADATA=$(
  {
    printf 'workflow=%s\n' "$WORKFLOW"
    printf 'profile=%s\n' "$PROFILE"
    for ((i=0; i<${#GENERATE_ARGS[@]}; i+=2)); do
      key="${GENERATE_ARGS[i]#--}"
      value="${GENERATE_ARGS[i+1]}"
      printf '%s=%s\n' "$key" "$value"
    done
  } | LC_ALL=C sort
)

CURRENT_METADATA=""
if [[ -f "$GENERATION_METADATA_FILE" ]]; then
  CURRENT_METADATA="$(LC_ALL=C sort "$GENERATION_METADATA_FILE")"
fi

if [[ "$REUSE_DATA" == "true" && -d "$DATA_ROOT" && -n "$CURRENT_METADATA" && "$CURRENT_METADATA" == "$DESIRED_METADATA" ]]; then
  echo "Reusing existing dataset at $DATA_ROOT"
else
  rm -rf "$DATA_ROOT"
  "$ROOT_DIR/scripts/run.sh" generate-data \
    --workflow "$WORKFLOW" \
    --workspace "$WORKSPACE" \
    --data-root "$DATA_ROOT" \
    "${GENERATE_ARGS[@]}"
  printf '%s\n' "$DESIRED_METADATA" > "$GENERATION_METADATA_FILE"
fi

if [[ "$EXECUTOR" == "hadoop" || "$EXECUTOR" == "hdfs-docker" ]]; then
  env \
    HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_CLUSTER_WORKDIR/host-conf}" \
    HADOOP_FS_DEFAULT="$HADOOP_FS_DEFAULT" \
    HADOOP_FRAMEWORK_NAME="$HADOOP_FRAMEWORK_NAME" \
    HADOOP_YARN_RM="$HADOOP_YARN_RM" \
    HADOOP_ENABLE_NODE_LABELS="$HADOOP_ENABLE_NODE_LABELS" \
    "$ROOT_DIR/scripts/run.sh" compare \
    --workflow "$WORKFLOW" \
    --workspace "$WORKSPACE" \
    --data-root "$DATA_ROOT" \
    --cluster-config "$CLUSTER_CONFIG" \
    --rounds "$COMPARE_ROUNDS" \
    --max-nodes "$MAX_NODES" \
    --training-warmup-runs "$TRAINING_WARMUP_RUNS" \
    --training-measure-runs "$TRAINING_MEASURE_RUNS" \
    --schedulers "$SCHEDULERS" \
    --executor "$EXECUTOR" \
    --docker-image "$DOCKER_IMAGE" \
    --hadoop-conf-dir "$HADOOP_CLUSTER_WORKDIR/host-conf" \
    --hadoop-fs-default "$HADOOP_FS_DEFAULT" \
    --hadoop-framework-name "$HADOOP_FRAMEWORK_NAME" \
    --hadoop-yarn-rm "$HADOOP_YARN_RM" \
    --hadoop-enable-node-labels "$HADOOP_ENABLE_NODE_LABELS" \
    "${COMPARE_EXTRA_ARGS[@]}"
else
  "$ROOT_DIR/scripts/run.sh" compare \
    --workflow "$WORKFLOW" \
    --workspace "$WORKSPACE" \
    --data-root "$DATA_ROOT" \
    --cluster-config "$CLUSTER_CONFIG" \
    --rounds "$COMPARE_ROUNDS" \
    --max-nodes "$MAX_NODES" \
    --training-warmup-runs "$TRAINING_WARMUP_RUNS" \
    --training-measure-runs "$TRAINING_MEASURE_RUNS" \
    --schedulers "$SCHEDULERS" \
    --executor "$EXECUTOR" \
    --docker-image "$DOCKER_IMAGE" \
    "${COMPARE_EXTRA_ARGS[@]}"
fi

echo "Benchmark outputs:"
echo "  $WORKSPACE/comparison.md"
echo "  $WORKSPACE/round-01"
echo "Workflow:"
echo "  $WORKFLOW"
echo "Data root:"
echo "  $DATA_ROOT"
echo "Reuse data:"
echo "  $REUSE_DATA"
echo "Java options:"
echo "  $WSH_JAVA_OPTS"
echo "Comparison rounds:"
echo "  $COMPARE_ROUNDS"
echo "Schedulers:"
echo "  $SCHEDULERS"
echo "WSH training runs:"
echo "  warmup=$TRAINING_WARMUP_RUNS measure=$TRAINING_MEASURE_RUNS"
echo "Executor:"
echo "  $EXECUTOR"
if [[ "$EXECUTOR" == "docker" ]]; then
  echo "Docker image:"
  echo "  $DOCKER_IMAGE"
elif [[ "$EXECUTOR" == "hadoop" ]]; then
  echo "Docker Hadoop image:"
  echo "  $HADOOP_CLUSTER_IMAGE"
  echo "Hadoop host conf:"
  echo "  $HADOOP_CLUSTER_WORKDIR/host-conf"
  echo "Keep cluster after run:"
  echo "  $HADOOP_KEEP_CLUSTER"
fi
if [[ "$MAX_NODES" != "0" ]]; then
  echo "Node limit:"
  echo "  $MAX_NODES"
fi
