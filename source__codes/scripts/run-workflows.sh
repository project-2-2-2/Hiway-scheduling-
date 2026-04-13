#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_TS="${RUN_TS:-$(date '+%Y%m%d-%H%M%S')}"
COMMAND="${1:-help}"
if [[ $# -gt 0 ]]; then
  shift
fi

DEFAULT_WORKFLOWS="gene2life avianflu_small epigenomics"
DEFAULT_NODE_COUNTS="4 7 10 13"
DEFAULT_SCHEDULERS="wsh,heft"
VALID_WORKFLOWS=("gene2life" "avianflu_small" "epigenomics")

PROFILE="${PROFILE:-small}"
WORKFLOWS="${WORKFLOWS:-$DEFAULT_WORKFLOWS}"
NODE_COUNTS="${NODE_COUNTS:-$DEFAULT_NODE_COUNTS}"
COMPARE_ROUNDS="${COMPARE_ROUNDS:-3}"
TRAINING_WARMUP_RUNS="${TRAINING_WARMUP_RUNS:-1}"
TRAINING_MEASURE_RUNS="${TRAINING_MEASURE_RUNS:-3}"
SCHEDULERS="${SCHEDULERS:-$DEFAULT_SCHEDULERS}"
CLUSTER_CONFIG="${CLUSTER_CONFIG:-}"
PAPER_CLUSTER_CONFIG="${PAPER_CLUSTER_CONFIG:-$ROOT_DIR/config/clusters-z4-g5-paper-sweep.csv}"
DENSE_CLUSTER_CONFIG="${DENSE_CLUSTER_CONFIG:-$ROOT_DIR/config/clusters-z4-g5-dense-28.csv}"
BASE_WORK_DIR="${BASE_WORK_DIR:-$ROOT_DIR/work/workflow-sweep-$RUN_TS}"
LOG_DIR="${LOG_DIR:-$BASE_WORK_DIR/logs}"
DATASET_BASE_DIR="${DATASET_BASE_DIR:-$BASE_WORK_DIR/datasets}"
HADOOP_CLUSTER_WORKDIR="${HADOOP_CLUSTER_WORKDIR:-$BASE_WORK_DIR/hadoop-docker-cluster}"
PREFLIGHT_DIR="${PREFLIGHT_DIR:-$ROOT_DIR/work/workflow-preflight-$RUN_TS}"
REUSE_DATA="${REUSE_DATA:-true}"
SKIP_BUILD="${SKIP_BUILD:-${SKIP_PREBUILD:-false}}"
KEEP_CLUSTER="${KEEP_CLUSTER:-${HADOOP_KEEP_CLUSTER:-${RUN_ALL_KEEP_CLUSTER:-false}}}"
DOCKER_IMAGE="${DOCKER_IMAGE:-wsh-java:latest}"
HADOOP_CLUSTER_IMAGE="${HADOOP_CLUSTER_IMAGE:-wsh-hadoop-cluster:3.4.3}"
EXECUTOR="hdfs-docker"
PREFLIGHT_SMOKE_CLUSTER_DIR=""
PREFLIGHT_SMOKE_CLUSTER_CONFIG=""
SWEEP_MASTER_LOG=""
SWEEP_CURRENT_RUN_FILE=""

usage() {
  cat <<'EOF'
Usage:
  ./scripts/run-workflows.sh preflight [options]
  ./scripts/run-workflows.sh sweep [options]
  ./scripts/run-workflows.sh cleanup [options]

Commands:
  preflight   Build, validate Docker pinning, start the Hadoop cluster, and run a smoke compare.
  sweep       Run workflow comparisons across one or more workflows and node counts.
  cleanup     Remove repo-owned Docker node containers and the Docker Hadoop cluster.

Common options:
  --workflows <csv>              Workflow ids. Default: gene2life,avianflu_small,epigenomics
  --nodes <csv>                  Node counts. Default: 4,7,10,13
  --profile <small|medium|large> Dataset profile. Default: small
  --rounds <n>                   Compare rounds. Default: 3
  --training-warmup-runs <n>     Warmup runs per WSH training task. Default: 1
  --training-measure-runs <n>    Measured runs per WSH training task. Default: 3
  --schedulers <csv>             Scheduler ids for compare. Default: wsh,heft
  --workspace-root <path>        Root directory for sweep outputs.
  --preflight-dir <path>         Root directory for preflight outputs.
  --cluster-workdir <path>       Docker Hadoop cluster working directory.
  --cluster-config <path>        Force a single cluster config for all runs.
  --docker-image <tag>           Application image tag. Default: wsh-java:latest
  --hadoop-cluster-image <tag>   Hadoop cluster image tag. Default: wsh-hadoop-cluster:3.4.3
  --reuse-data <true|false>      Reuse matching generated datasets. Default: true
  --skip-build <true|false>      Skip build/image prebuild steps. Default: false
  --keep-cluster <true|false>    Leave the cluster running after the command. Default: false
  --help                         Show this help.

Examples:
  ./scripts/run-workflows.sh preflight
  ./scripts/run-workflows.sh sweep --workflows gene2life --nodes 4 --rounds 1
  ./scripts/run-workflows.sh sweep --workflows gene2life,avianflu_small,epigenomics --nodes 4,7,10,13
  ./scripts/run-workflows.sh cleanup
EOF
}

timestamp() {
  date '+%F %T'
}

log() {
  printf '[%s] %s\n' "$(timestamp)" "$*"
}

die() {
  echo "$*" >&2
  exit 1
}

csv_to_words() {
  printf '%s\n' "$1" | tr ',' ' ' | xargs
}

require_value() {
  local flag="$1"
  local value="${2:-}"
  [[ -n "$value" ]] || die "Missing value for $flag"
}

is_boolean() {
  [[ "$1" == "true" || "$1" == "false" ]]
}

validate_workflows() {
  local workflow
  for workflow in $WORKFLOWS; do
    case "$workflow" in
      gene2life|avianflu_small|epigenomics)
        ;;
      *)
        die "Unsupported workflow: $workflow"
        ;;
    esac
  done
}

validate_nodes() {
  local nodes
  for nodes in $NODE_COUNTS; do
    [[ "$nodes" =~ ^[0-9]+$ ]] || die "Invalid node count: $nodes"
    (( nodes > 0 )) || die "Node count must be positive: $nodes"
  done
}

validate_common_options() {
  validate_workflows
  validate_nodes
  [[ "$PROFILE" == "small" || "$PROFILE" == "medium" || "$PROFILE" == "large" ]] \
    || die "Unsupported profile: $PROFILE"
  [[ "$COMPARE_ROUNDS" =~ ^[0-9]+$ ]] || die "Invalid rounds value: $COMPARE_ROUNDS"
  [[ "$TRAINING_WARMUP_RUNS" =~ ^[0-9]+$ ]] || die "Invalid training warmup runs: $TRAINING_WARMUP_RUNS"
  [[ "$TRAINING_MEASURE_RUNS" =~ ^[0-9]+$ ]] || die "Invalid training measure runs: $TRAINING_MEASURE_RUNS"
  [[ -n "$SCHEDULERS" ]] || die "Schedulers list must not be empty"
  is_boolean "$REUSE_DATA" || die "reuse-data must be true or false: $REUSE_DATA"
  is_boolean "$SKIP_BUILD" || die "skip-build must be true or false: $SKIP_BUILD"
  is_boolean "$KEEP_CLUSTER" || die "keep-cluster must be true or false: $KEEP_CLUSTER"
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --workflows)
        require_value "$1" "${2:-}"
        WORKFLOWS="$(csv_to_words "$2")"
        shift 2
        ;;
      --nodes)
        require_value "$1" "${2:-}"
        NODE_COUNTS="$(csv_to_words "$2")"
        shift 2
        ;;
      --profile)
        require_value "$1" "${2:-}"
        PROFILE="$2"
        shift 2
        ;;
      --rounds)
        require_value "$1" "${2:-}"
        COMPARE_ROUNDS="$2"
        shift 2
        ;;
      --training-warmup-runs)
        require_value "$1" "${2:-}"
        TRAINING_WARMUP_RUNS="$2"
        shift 2
        ;;
      --training-measure-runs)
        require_value "$1" "${2:-}"
        TRAINING_MEASURE_RUNS="$2"
        shift 2
        ;;
      --schedulers)
        require_value "$1" "${2:-}"
        SCHEDULERS="$2"
        shift 2
        ;;
      --workspace-root)
        require_value "$1" "${2:-}"
        BASE_WORK_DIR="$2"
        LOG_DIR="$BASE_WORK_DIR/logs"
        DATASET_BASE_DIR="$BASE_WORK_DIR/datasets"
        HADOOP_CLUSTER_WORKDIR="$BASE_WORK_DIR/hadoop-docker-cluster"
        shift 2
        ;;
      --preflight-dir)
        require_value "$1" "${2:-}"
        PREFLIGHT_DIR="$2"
        shift 2
        ;;
      --cluster-workdir)
        require_value "$1" "${2:-}"
        HADOOP_CLUSTER_WORKDIR="$2"
        shift 2
        ;;
      --cluster-config)
        require_value "$1" "${2:-}"
        CLUSTER_CONFIG="$2"
        shift 2
        ;;
      --docker-image)
        require_value "$1" "${2:-}"
        DOCKER_IMAGE="$2"
        shift 2
        ;;
      --hadoop-cluster-image)
        require_value "$1" "${2:-}"
        HADOOP_CLUSTER_IMAGE="$2"
        shift 2
        ;;
      --reuse-data)
        require_value "$1" "${2:-}"
        REUSE_DATA="$2"
        shift 2
        ;;
      --skip-build)
        require_value "$1" "${2:-}"
        SKIP_BUILD="$2"
        shift 2
        ;;
      --keep-cluster)
        require_value "$1" "${2:-}"
        KEEP_CLUSTER="$2"
        shift 2
        ;;
      --help|-h)
        usage
        exit 0
        ;;
      *)
        die "Unknown option: $1"
        ;;
    esac
  done
}

run_with_timestamped_log() {
  local logfile="$1"
  shift
  set +e
  "$@" 2>&1 | while IFS= read -r line; do
    printf '[%s] %s\n' "$(timestamp)" "$line"
  done | tee -a "$logfile"
  local statuses=("${PIPESTATUS[@]}")
  set -e
  return "${statuses[0]}"
}

cluster_config_for_nodes() {
  local nodes="$1"
  if [[ -n "$CLUSTER_CONFIG" ]]; then
    printf '%s\n' "$CLUSTER_CONFIG"
    return
  fi
  if (( nodes > 13 )); then
    printf '%s\n' "$DENSE_CLUSTER_CONFIG"
  else
    printf '%s\n' "$PAPER_CLUSTER_CONFIG"
  fi
}

cleanup_runtime() {
  local cleanup_config="${CLUSTER_CONFIG:-$PAPER_CLUSTER_CONFIG}"
  HADOOP_CLUSTER_WORKDIR="$HADOOP_CLUSTER_WORKDIR" \
    HADOOP_CLUSTER_IMAGE="$HADOOP_CLUSTER_IMAGE" \
    CLUSTER_CONFIG="$cleanup_config" \
    "$ROOT_DIR/scripts/cleanup-project-runtime.sh" all >/dev/null 2>&1 || true
}

run_preflight() {
  local preflight_log="$PREFLIGHT_DIR/preflight.log"
  local smoke_workspace="$PREFLIGHT_DIR/benchmark-smoke"
  local smoke_cluster_dir="$PREFLIGHT_DIR/hadoop-docker-cluster"
  local smoke_cluster_config="${CLUSTER_CONFIG:-$PAPER_CLUSTER_CONFIG}"

  PREFLIGHT_SMOKE_CLUSTER_DIR="$smoke_cluster_dir"
  PREFLIGHT_SMOKE_CLUSTER_CONFIG="$smoke_cluster_config"

  mkdir -p "$PREFLIGHT_DIR"
  : > "$preflight_log"

  cleanup() {
    if [[ "$KEEP_CLUSTER" != "true" ]]; then
      HADOOP_CLUSTER_WORKDIR="$PREFLIGHT_SMOKE_CLUSTER_DIR" \
        HADOOP_CLUSTER_IMAGE="$HADOOP_CLUSTER_IMAGE" \
        CLUSTER_CONFIG="$PREFLIGHT_SMOKE_CLUSTER_CONFIG" \
        "$ROOT_DIR/scripts/cleanup-project-runtime.sh" all >/dev/null 2>&1 || true
    fi
  }

  trap cleanup EXIT

  run_with_timestamped_log "$preflight_log" echo "Submission preflight directory: $PREFLIGHT_DIR"
  run_with_timestamped_log "$preflight_log" "$ROOT_DIR/scripts/build.sh"
  run_with_timestamped_log "$preflight_log" "$ROOT_DIR/scripts/build-image.sh" "$DOCKER_IMAGE"
  run_with_timestamped_log "$preflight_log" "$ROOT_DIR/scripts/build-hadoop-cluster-image.sh" "$HADOOP_CLUSTER_IMAGE"
  run_with_timestamped_log "$preflight_log" "$ROOT_DIR/scripts/validate-docker-node-pinning.sh" "$PAPER_CLUSTER_CONFIG" "$DOCKER_IMAGE"
  run_with_timestamped_log "$preflight_log" "$ROOT_DIR/scripts/validate-docker-node-pinning.sh" "$DENSE_CLUSTER_CONFIG" "$DOCKER_IMAGE"
  run_with_timestamped_log "$preflight_log" "$ROOT_DIR/scripts/hadoop-docker-cluster.sh" up "$smoke_cluster_config" "$smoke_cluster_dir" "$HADOOP_CLUSTER_IMAGE"
  run_with_timestamped_log "$preflight_log" "$ROOT_DIR/scripts/hadoop-docker-cluster.sh" health "$smoke_cluster_config" "$smoke_cluster_dir" "$HADOOP_CLUSTER_IMAGE"
  run_with_timestamped_log "$preflight_log" "$ROOT_DIR/scripts/hadoop-docker-cluster.sh" validate "$smoke_cluster_config" "$smoke_cluster_dir" "$HADOOP_CLUSTER_IMAGE"
  run_with_timestamped_log "$preflight_log" env \
    EXECUTOR="$EXECUTOR" \
    WORKFLOW=gene2life \
    PROFILE=small \
    COMPARE_ROUNDS=1 \
    TRAINING_WARMUP_RUNS=0 \
    TRAINING_MEASURE_RUNS=1 \
    MAX_NODES=2 \
    SCHEDULERS="$SCHEDULERS" \
    SKIP_BUILD=true \
    REUSE_DATA=false \
    HADOOP_KEEP_CLUSTER=true \
    DOCKER_IMAGE="$DOCKER_IMAGE" \
    WORKSPACE="$smoke_workspace" \
    DATA_ROOT="$smoke_workspace/data" \
    CLUSTER_CONFIG="$smoke_cluster_config" \
    HADOOP_CLUSTER_WORKDIR="$smoke_cluster_dir" \
    HADOOP_CLUSTER_IMAGE="$HADOOP_CLUSTER_IMAGE" \
    "$ROOT_DIR/scripts/server-benchmark.sh"

  [[ -f "$smoke_workspace/comparison.md" ]] \
    || die "Preflight smoke did not produce $smoke_workspace/comparison.md"

  log "Submission preflight succeeded"
  log "Smoke comparison report: $smoke_workspace/comparison.md"
}

run_sweep() {
  local master_log="$LOG_DIR/master-$RUN_TS.log"
  local current_run_file="$LOG_DIR/current-run.txt"
  local completed_runs_file="$LOG_DIR/completed-runs.txt"
  local failed_run_file="$LOG_DIR/failed-run.txt"
  local workflow=""
  local nodes=""

  SWEEP_MASTER_LOG="$master_log"
  SWEEP_CURRENT_RUN_FILE="$current_run_file"

  mkdir -p "$BASE_WORK_DIR" "$LOG_DIR" "$DATASET_BASE_DIR"
  : > "$completed_runs_file"
  rm -f "$failed_run_file" "$current_run_file"

  cleanup() {
    rm -f "$SWEEP_CURRENT_RUN_FILE"
    if [[ "$KEEP_CLUSTER" != "true" ]]; then
      log "Cleaning up project Docker Hadoop cluster after sweep" | tee -a "$SWEEP_MASTER_LOG"
      cleanup_runtime
    fi
  }

  trap cleanup EXIT

  {
    log "Workflow sweep started"
    log "EXECUTOR=$EXECUTOR"
    log "PROFILE=$PROFILE"
    log "WORKFLOWS=$WORKFLOWS"
    log "NODE_COUNTS=$NODE_COUNTS"
    log "COMPARE_ROUNDS=$COMPARE_ROUNDS"
    log "TRAINING_WARMUP_RUNS=$TRAINING_WARMUP_RUNS"
    log "TRAINING_MEASURE_RUNS=$TRAINING_MEASURE_RUNS"
    log "SCHEDULERS=$SCHEDULERS"
    log "BASE_WORK_DIR=$BASE_WORK_DIR"
    log "LOG_DIR=$LOG_DIR"
    log "DATASET_BASE_DIR=$DATASET_BASE_DIR"
    log "HADOOP_CLUSTER_WORKDIR=$HADOOP_CLUSTER_WORKDIR"
    log "DOCKER_IMAGE=$DOCKER_IMAGE"
    log "HADOOP_CLUSTER_IMAGE=$HADOOP_CLUSTER_IMAGE"
    log "PAPER_CLUSTER_CONFIG=$PAPER_CLUSTER_CONFIG"
    log "DENSE_CLUSTER_CONFIG=$DENSE_CLUSTER_CONFIG"
    log "CLUSTER_CONFIG=${CLUSTER_CONFIG:-<auto>}"
    log "REUSE_DATA=$REUSE_DATA"
    log "SKIP_BUILD=$SKIP_BUILD"
    log "KEEP_CLUSTER=$KEEP_CLUSTER"
  } | tee -a "$master_log"

  if [[ "$SKIP_BUILD" != "true" ]]; then
    run_with_timestamped_log "$master_log" "$ROOT_DIR/scripts/build.sh"
    run_with_timestamped_log "$master_log" "$ROOT_DIR/scripts/build-image.sh" "$DOCKER_IMAGE"
    run_with_timestamped_log "$master_log" "$ROOT_DIR/scripts/build-hadoop-cluster-image.sh" "$HADOOP_CLUSTER_IMAGE"
  else
    log "Skipping prebuild steps because SKIP_BUILD=true" | tee -a "$master_log"
  fi

  for workflow in $WORKFLOWS; do
    local data_root="$DATASET_BASE_DIR/$workflow"
    for nodes in $NODE_COUNTS; do
      local run_name="${workflow}-nodes-${nodes}"
      local workspace="$BASE_WORK_DIR/$run_name"
      local logfile="$LOG_DIR/$run_name.log"
      local cluster_config
      cluster_config="$(cluster_config_for_nodes "$nodes")"
      : > "$logfile"

      {
        log "Starting $run_name"
        log "Using cluster config $cluster_config"
      } | tee -a "$master_log"
      printf '%s\n' "$run_name" > "$current_run_file"

      if ! run_with_timestamped_log "$logfile" env \
        WORKSPACE="$workspace" \
        WORKFLOW="$workflow" \
        DATA_ROOT="$data_root" \
        PROFILE="$PROFILE" \
        CLUSTER_CONFIG="$cluster_config" \
        MAX_NODES="$nodes" \
        COMPARE_ROUNDS="$COMPARE_ROUNDS" \
        TRAINING_WARMUP_RUNS="$TRAINING_WARMUP_RUNS" \
        TRAINING_MEASURE_RUNS="$TRAINING_MEASURE_RUNS" \
        SCHEDULERS="$SCHEDULERS" \
        REUSE_DATA="$REUSE_DATA" \
        HADOOP_KEEP_CLUSTER=true \
        SKIP_BUILD=true \
        EXECUTOR="$EXECUTOR" \
        DOCKER_IMAGE="$DOCKER_IMAGE" \
        HADOOP_CLUSTER_IMAGE="$HADOOP_CLUSTER_IMAGE" \
        HADOOP_CLUSTER_WORKDIR="$HADOOP_CLUSTER_WORKDIR" \
        "$ROOT_DIR/scripts/server-benchmark.sh"; then
        log "FAILED $run_name" | tee -a "$master_log"
        printf '%s\n' "$run_name" > "$failed_run_file"
        exit 1
      fi

      {
        log "Completed $run_name"
        log "Comparison report: $workspace/comparison.md"
      } | tee -a "$master_log"
      printf '%s\n' "$run_name" >> "$completed_runs_file"
      rm -f "$current_run_file"
    done
  done

  log "Workflow sweep completed successfully" | tee -a "$master_log"
  log "Master log: $master_log" | tee -a "$master_log"
}

run_cleanup() {
  cleanup_runtime
  log "Cleaned repo-owned Docker runtime"
}

parse_args "$@"

case "$COMMAND" in
  help|--help|-h)
    usage
    ;;
  preflight)
    validate_common_options
    run_preflight
    ;;
  sweep)
    validate_common_options
    run_sweep
    ;;
  cleanup)
    run_cleanup
    ;;
  *)
    usage
    die "Unknown command: $COMMAND"
    ;;
esac
