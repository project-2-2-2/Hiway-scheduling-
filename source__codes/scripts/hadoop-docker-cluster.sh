#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMMAND="${1:-status}"
CLUSTER_CONFIG="${2:-$ROOT_DIR/config/clusters-z4-g5-paper-sweep.csv}"
OUTPUT_DIR="${3:-$ROOT_DIR/work/hadoop-docker-cluster}"
IMAGE_TAG="${4:-wsh-hadoop-cluster:3.4.3}"
DOCKER_IMAGE="${DOCKER_IMAGE:-wsh-java:latest}"
PROJECT_NAME="${HADOOP_DOCKER_PROJECT:-hadoop-paper-cluster}"
COMPOSE_FILE="$OUTPUT_DIR/docker-compose.yml"
HOST_CONF_DIR="$OUTPUT_DIR/host-conf"
CLIENT_USER="${HADOOP_CLIENT_USER:-$(id -un)}"
ENABLE_NODE_LABELS="${HADOOP_ENABLE_NODE_LABELS:-true}"
REBUILD_IMAGE="${HADOOP_CLUSTER_REBUILD_IMAGE:-false}"
RECREATE_CLUSTER="${HADOOP_CLUSTER_RECREATE:-false}"
NAMENODE_RPC_PORT="${HADOOP_DOCKER_NN_PORT:-19000}"
NAMENODE_HTTP_PORT="${HADOOP_DOCKER_NN_HTTP_PORT:-19870}"
YARN_SCHEDULER_PORT="${HADOOP_DOCKER_YARN_SCHEDULER_PORT:-18030}"
YARN_TRACKER_PORT="${HADOOP_DOCKER_YARN_TRACKER_PORT:-18031}"
YARN_RM_PORT="${HADOOP_DOCKER_YARN_RM_PORT:-18032}"
YARN_ADMIN_PORT="${HADOOP_DOCKER_YARN_ADMIN_PORT:-18033}"
YARN_WEB_PORT="${HADOOP_DOCKER_YARN_WEB_PORT:-18088}"
ACCESS_HOST="${HADOOP_DOCKER_ACCESS_HOST:-$(hostname -I 2>/dev/null | awk '{print $1}')}"
ACCESS_HOST="${ACCESS_HOST:-localhost}"
WARMUP_ON_START="${HADOOP_CLUSTER_WARMUP_ON_START:-false}"

compose() {
  docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" "$@"
}

worker_nodes() {
  awk -F, '
    /^[[:space:]]*#/ || /^[[:space:]]*$/ { next }
    {
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", $2)
      print $2
    }
  ' "$CLUSTER_CONFIG"
}

expected_nodes() {
  grep -v '^\s*#' "$CLUSTER_CONFIG" | grep -v '^\s*$' | wc -l
}

expected_services() {
  echo $(( "$(expected_nodes)" + 1 ))
}

expected_yarn_nodes() {
  echo $(( "$(expected_nodes)" + 1 ))
}

image_exists() {
  docker image inspect "$IMAGE_TAG" >/dev/null 2>&1
}

app_image_exists() {
  docker image inspect "$DOCKER_IMAGE" >/dev/null 2>&1
}

cluster_labels() {
  awk -F, '
    /^[[:space:]]*#/ || /^[[:space:]]*$/ { next }
    {
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", $1)
      if (!seen[$1]++) {
        printf("%s%s", sep, $1)
        sep=","
      }
    }
  ' "$CLUSTER_CONFIG"
}

node_label_mapping() {
  awk -F, '
    /^[[:space:]]*#/ || /^[[:space:]]*$/ { next }
    {
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", $1)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", $2)
      printf("%s%s=%s", sep, $2, $1)
      sep=" "
    }
  ' "$CLUSTER_CONFIG"
}

print_endpoints() {
  echo "Docker Hadoop cluster is ready"
  echo "Compose file: $COMPOSE_FILE"
  echo "Host conf: $HOST_CONF_DIR"
  echo "HDFS RPC: hdfs://${ACCESS_HOST}:${NAMENODE_RPC_PORT}"
  echo "NameNode UI: http://${ACCESS_HOST}:${NAMENODE_HTTP_PORT}"
  echo "YARN RM: ${ACCESS_HOST}:${YARN_RM_PORT}"
  echo "YARN scheduler: ${ACCESS_HOST}:${YARN_SCHEDULER_PORT}"
  echo "YARN tracker: ${ACCESS_HOST}:${YARN_TRACKER_PORT}"
  echo "YARN admin: ${ACCESS_HOST}:${YARN_ADMIN_PORT}"
  echo "YARN UI: http://${ACCESS_HOST}:${YARN_WEB_PORT}"
}

running_service_count() {
  if [[ ! -f "$COMPOSE_FILE" ]]; then
    echo 0
    return
  fi
  compose ps --services --status running 2>/dev/null | wc -l
}

labels_ready() {
  [[ "$ENABLE_NODE_LABELS" == "true" ]] || return 0
  local labels
  labels="$(cluster_labels)"
  [[ -n "$labels" ]] || return 0
  compose exec -T master bash -lc "
    yarn cluster --list-node-labels >/tmp/wsh-labels.txt 2>/tmp/wsh-labels.err || exit 1
    labels='${labels}'
    IFS=',' read -r -a expected <<< \"\$labels\"
    for label in \"\${expected[@]}\"; do
      grep -q \"<\${label}:exclusivity=true>\" /tmp/wsh-labels.txt || exit 1
    done
  " >/dev/null 2>&1
}

wait_for_health() {
  local total_nodes
  total_nodes="$(expected_yarn_nodes)"
  for _ in $(seq 1 90); do
    if compose exec -T master bash -lc "timeout 25s sh -lc 'hdfs dfsadmin -safemode wait >/tmp/hdfs-safemode.txt 2>/tmp/hdfs-safemode.err && hdfs dfsadmin -report >/tmp/hdfs-report.txt 2>/tmp/hdfs-report.err && yarn node -list >/tmp/yarn-nodes.txt 2>/tmp/yarn-nodes.err'"; then
      local current_nodes
      current_nodes="$(compose exec -T master bash -lc "awk -F: '/Total Nodes/ {gsub(/ /, \"\", \$2); print \$2}' /tmp/yarn-nodes.txt" | tr -d '\r')"
      if [[ "$current_nodes" == "$total_nodes" ]]; then
        return 0
      fi
    fi
    sleep 2
  done
  return 1
}

cluster_healthy() {
  [[ -f "$COMPOSE_FILE" ]] || return 1
  [[ "$(running_service_count)" == "$(expected_services)" ]] || return 1
  wait_for_health || return 1
  labels_ready
}

ensure_image() {
  if [[ "$REBUILD_IMAGE" == "true" || "$RECREATE_CLUSTER" == "true" ]] || ! image_exists; then
    "$ROOT_DIR/scripts/build-hadoop-cluster-image.sh" "$IMAGE_TAG"
  fi
}

ensure_app_image() {
  if ! app_image_exists; then
    "$ROOT_DIR/scripts/build-image.sh" "$DOCKER_IMAGE"
  fi
}

generate_compose() {
  mkdir -p "$OUTPUT_DIR" "$HOST_CONF_DIR"

  mapfile -t CLUSTER_LINES < <(grep -v '^\s*#' "$CLUSTER_CONFIG" | grep -v '^\s*$')
  if [[ "${#CLUSTER_LINES[@]}" -eq 0 ]]; then
    echo "No cluster nodes found in $CLUSTER_CONFIG" >&2
    exit 1
  fi

  WORKER_HOSTS=()
  LABELS=()
  MAX_MEMORY_MB=0
  MAX_CPU_VCORES=0

  for line in "${CLUSTER_LINES[@]}"; do
    IFS=',' read -r cluster_id node_id cpu_threads io_buffer_kb memory_mb cpu_set <<< "$line"
    WORKER_HOSTS+=("$node_id")
    if [[ ! " ${LABELS[*]} " =~ " ${cluster_id} " ]]; then
      LABELS+=("$cluster_id")
    fi
    if (( memory_mb > MAX_MEMORY_MB )); then
      MAX_MEMORY_MB="$memory_mb"
    fi
    if (( cpu_threads > MAX_CPU_VCORES )); then
      MAX_CPU_VCORES="$cpu_threads"
    fi
    mkdir -p "$OUTPUT_DIR/${node_id}-data"
  done
  mkdir -p "$OUTPUT_DIR/master-data"

  WORKER_HOSTS_JOINED="$(IFS=,; echo "${WORKER_HOSTS[*]}")"
  LABELS_JOINED="$(IFS=,; echo "${LABELS[*]}")"

  {
    echo "services:"
    echo "  master:"
    echo "    image: ${IMAGE_TAG}"
    echo "    hostname: master"
    echo "    environment:"
    echo "      ROLE: master"
    echo "      MASTER_HOST: master"
    echo "      WORKER_HOSTS: ${WORKER_HOSTS_JOINED}"
    echo "      CLUSTER_LABELS: ${LABELS_JOINED}"
    echo "      YARN_MAX_MEMORY_MB: ${MAX_MEMORY_MB}"
    echo "      YARN_MAX_VCORES: ${MAX_CPU_VCORES}"
    echo "      NODE_MEMORY_MB: ${HADOOP_DOCKER_MASTER_MEMORY_MB:-2048}"
    echo "      NODE_CPU_VCORES: ${HADOOP_DOCKER_MASTER_VCORES:-2}"
    echo "    ports:"
    echo "      - \"${NAMENODE_RPC_PORT}:9000\""
    echo "      - \"${NAMENODE_HTTP_PORT}:9870\""
    echo "      - \"${YARN_SCHEDULER_PORT}:8030\""
    echo "      - \"${YARN_TRACKER_PORT}:8031\""
    echo "      - \"${YARN_RM_PORT}:8032\""
    echo "      - \"${YARN_ADMIN_PORT}:8033\""
    echo "      - \"${YARN_WEB_PORT}:8088\""
    echo "    volumes:"
    echo "      - ${OUTPUT_DIR}/master-data:/data"

    for line in "${CLUSTER_LINES[@]}"; do
      IFS=',' read -r cluster_id node_id cpu_threads io_buffer_kb memory_mb cpu_set <<< "$line"
      echo "  ${node_id}:"
      echo "    image: ${IMAGE_TAG}"
      echo "    hostname: ${node_id}"
      echo "    depends_on:"
      echo "      - master"
      echo "    environment:"
      echo "      ROLE: worker"
      echo "      MASTER_HOST: master"
      echo "      WORKER_HOSTS: ${WORKER_HOSTS_JOINED}"
      echo "      CLUSTER_LABELS: ${LABELS_JOINED}"
      echo "      YARN_MAX_MEMORY_MB: ${MAX_MEMORY_MB}"
      echo "      YARN_MAX_VCORES: ${MAX_CPU_VCORES}"
      echo "      NODE_MEMORY_MB: ${memory_mb}"
      echo "      NODE_CPU_VCORES: ${cpu_threads}"
      echo "    volumes:"
      echo "      - ${OUTPUT_DIR}/${node_id}-data:/data"
      echo "    mem_limit: ${memory_mb}m"
      echo "    cpus: '${cpu_threads}'"
      if [[ -n "${cpu_set:-}" ]]; then
        echo "    cpuset: \"${cpu_set}\""
      fi
    done
  } > "$COMPOSE_FILE"

  cat > "$HOST_CONF_DIR/core-site.xml" <<EOF
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://${ACCESS_HOST}:${NAMENODE_RPC_PORT}</value>
  </property>
</configuration>
EOF

  cat > "$HOST_CONF_DIR/hdfs-site.xml" <<EOF
<configuration>
  <property>
    <name>dfs.client.use.datanode.hostname</name>
    <value>false</value>
  </property>
  <property>
    <name>dfs.datanode.use.datanode.hostname</name>
    <value>false</value>
  </property>
</configuration>
EOF

  cat > "$HOST_CONF_DIR/mapred-site.xml" <<EOF
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
  <property>
    <name>mapreduce.application.classpath</name>
    <value>\$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:\$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*</value>
  </property>
  <property>
    <name>yarn.app.mapreduce.am.env</name>
    <value>JAVA_HOME=/opt/java/openjdk,HADOOP_HOME=/opt/hadoop,HADOOP_COMMON_HOME=/opt/hadoop,HADOOP_HDFS_HOME=/opt/hadoop,HADOOP_MAPRED_HOME=/opt/hadoop,HADOOP_YARN_HOME=/opt/hadoop</value>
  </property>
  <property>
    <name>mapreduce.map.env</name>
    <value>JAVA_HOME=/opt/java/openjdk,HADOOP_HOME=/opt/hadoop,HADOOP_COMMON_HOME=/opt/hadoop,HADOOP_HDFS_HOME=/opt/hadoop,HADOOP_MAPRED_HOME=/opt/hadoop,HADOOP_YARN_HOME=/opt/hadoop</value>
  </property>
  <property>
    <name>mapreduce.reduce.env</name>
    <value>JAVA_HOME=/opt/java/openjdk,HADOOP_HOME=/opt/hadoop,HADOOP_COMMON_HOME=/opt/hadoop,HADOOP_HDFS_HOME=/opt/hadoop,HADOOP_MAPRED_HOME=/opt/hadoop,HADOOP_YARN_HOME=/opt/hadoop</value>
  </property>
  <property>
    <name>mapreduce.jobtracker.staging.root.dir</name>
    <value>/user/${CLIENT_USER}/.staging</value>
  </property>
  <property>
    <name>yarn.app.mapreduce.am.staging-dir</name>
    <value>/user/${CLIENT_USER}/.staging</value>
  </property>
</configuration>
EOF

  cat > "$HOST_CONF_DIR/yarn-site.xml" <<EOF
<configuration>
  <property>
    <name>yarn.resourcemanager.scheduler.address</name>
    <value>${ACCESS_HOST}:${YARN_SCHEDULER_PORT}</value>
  </property>
  <property>
    <name>yarn.resourcemanager.resource-tracker.address</name>
    <value>${ACCESS_HOST}:${YARN_TRACKER_PORT}</value>
  </property>
  <property>
    <name>yarn.resourcemanager.address</name>
    <value>${ACCESS_HOST}:${YARN_RM_PORT}</value>
  </property>
  <property>
    <name>yarn.resourcemanager.admin.address</name>
    <value>${ACCESS_HOST}:${YARN_ADMIN_PORT}</value>
  </property>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>${ACCESS_HOST}</value>
  </property>
  <property>
    <name>yarn.webapp.address</name>
    <value>${ACCESS_HOST}:${YARN_WEB_PORT}</value>
  </property>
</configuration>
EOF
}

init_hdfs_dirs() {
  compose exec -T master bash -lc "hdfs dfs -mkdir -p /tmp /user/${CLIENT_USER} /user/${CLIENT_USER}/.staging /user/${CLIENT_USER}/wsh \
    && hdfs dfs -chown -R ${CLIENT_USER}:supergroup /user/${CLIENT_USER} \
    && hdfs dfs -chmod 755 /user/${CLIENT_USER} \
    && hdfs dfs -chmod 700 /user/${CLIENT_USER}/.staging"
}

apply_node_labels() {
  local labels mapping existing_labels missing_labels
  [[ "$ENABLE_NODE_LABELS" == "true" ]] || return 0
  labels="$(cluster_labels)"
  mapping="$(node_label_mapping)"
  existing_labels="$(compose exec -T master bash -lc "yarn cluster --list-node-labels 2>/dev/null || true" | tr -d '\r')"
  missing_labels=()
  IFS=',' read -r -a all_labels <<< "$labels"
  for label in "${all_labels[@]}"; do
    if [[ -n "$label" && "$existing_labels" != *"<${label}:exclusivity=true>"* ]]; then
      missing_labels+=("$label")
    fi
  done
  if (( ${#missing_labels[@]} > 0 )); then
    compose exec -T master bash -lc "yarn rmadmin -addToClusterNodeLabels \"$(IFS=,; echo "${missing_labels[*]}")\""
  fi
  compose exec -T master bash -lc "yarn rmadmin -replaceLabelsOnNode \"$mapping\""
}

validate_worker_connectivity() {
  local first_worker
  first_worker="$(worker_nodes | head -n 1)"
  [[ -n "$first_worker" ]]
  compose exec -T "$first_worker" bash -lc "getent hosts master >/dev/null && hdfs dfs -ls / >/dev/null"
}

validate_cpu_pinning() {
  local line cluster_id node_id cpu_threads io_buffer_kb memory_mb cpu_set container_id inspect_output actual_cpuset actual_memory actual_nano_cpus
  while IFS= read -r line; do
    [[ -n "$line" ]] || continue
    IFS=',' read -r cluster_id node_id cpu_threads io_buffer_kb memory_mb cpu_set <<< "$line"
    container_id="$(compose ps -q "$node_id")"
    [[ -n "$container_id" ]] || {
      echo "Missing container for worker $node_id" >&2
      return 1
    }
    inspect_output="$(docker inspect "$container_id" --format '{{.HostConfig.CpusetCpus}}|{{.HostConfig.Memory}}|{{.HostConfig.NanoCpus}}')"
    IFS='|' read -r actual_cpuset actual_memory actual_nano_cpus <<< "$inspect_output"
    if [[ -n "${cpu_set:-}" && "$actual_cpuset" != "$cpu_set" ]]; then
      echo "Unexpected cpuset for $node_id: expected $cpu_set, got $actual_cpuset" >&2
      return 1
    fi
    if [[ "$actual_memory" != "$(( memory_mb * 1024 * 1024 ))" ]]; then
      echo "Unexpected memory limit for $node_id: expected ${memory_mb}m, got ${actual_memory} bytes" >&2
      return 1
    fi
    if [[ "$actual_nano_cpus" != "$(( cpu_threads * 1000000000 ))" ]]; then
      echo "Unexpected CPU quota for $node_id: expected ${cpu_threads} cores, got ${actual_nano_cpus} nano CPUs" >&2
      return 1
    fi
    if [[ -n "${cpu_set:-}" ]]; then
      compose exec -T "$node_id" bash -lc "test \"\$(awk '/^Cpus_allowed_list:/ {print \$2}' /proc/self/status)\" = \"$cpu_set\""
    fi
  done < <(grep -v '^\s*#' "$CLUSTER_CONFIG" | grep -v '^\s*$')
}

ensure_cluster_ready() {
  local started_cluster="false"
  ensure_image
  generate_compose
  if [[ "$RECREATE_CLUSTER" == "true" && -f "$COMPOSE_FILE" ]]; then
    compose down -v --remove-orphans || true
  fi
  if cluster_healthy; then
    init_hdfs_dirs
    apply_node_labels
    if labels_ready; then
      print_endpoints
      return 0
    fi
  fi
  if [[ -f "$COMPOSE_FILE" ]]; then
    compose down -v --remove-orphans || true
  fi
  compose up -d --remove-orphans
  started_cluster="true"
  wait_for_health
  init_hdfs_dirs
  apply_node_labels
  if ! labels_ready; then
    echo "Docker Hadoop cluster did not expose the requested node labels after startup" >&2
    exit 1
  fi
  if [[ "$started_cluster" == "true" && "$WARMUP_ON_START" == "true" ]]; then
    validate_cluster >/dev/null
  fi
  print_endpoints
}

validate_cluster() {
  local validation_root
  validation_root="$(mktemp -d "${TMPDIR:-/tmp}/wsh-hadoop-validate.XXXXXX")"
  trap "rm -rf '$validation_root'" RETURN
  ensure_app_image
  validate_worker_connectivity
  validate_cpu_pinning
  compose exec -T master bash -lc "printf 'wsh-validation\n' > /tmp/wsh-validate.txt && hdfs dfs -put -f /tmp/wsh-validate.txt /user/${CLIENT_USER}/wsh/validate.txt && hdfs dfs -cat /user/${CLIENT_USER}/wsh/validate.txt"
  "$ROOT_DIR/scripts/run.sh" generate-data \
    --workflow gene2life \
    --workspace "$validation_root" \
    --data-root "$validation_root/data" \
    --query-count 8 \
    --reference-records-per-shard 200 \
    --sequence-length 64
  "$ROOT_DIR/scripts/run.sh" run \
    --workflow gene2life \
    --workspace "$validation_root" \
    --data-root "$validation_root/data" \
    --cluster-config "$CLUSTER_CONFIG" \
    --scheduler heft \
    --max-nodes 4 \
    --executor hdfs-docker \
    --docker-image "$DOCKER_IMAGE" \
    --hadoop-conf-dir "$HOST_CONF_DIR" \
    --hadoop-fs-default "hdfs://${ACCESS_HOST}:${NAMENODE_RPC_PORT}" \
    --hadoop-yarn-rm "${ACCESS_HOST}:${YARN_RM_PORT}" \
    --hadoop-framework-name yarn \
    --hadoop-enable-node-labels "$ENABLE_NODE_LABELS"
  compose exec -T master bash -lc "hdfs dfs -rm -f /user/${CLIENT_USER}/wsh/validate.txt >/dev/null 2>&1 || true"
  echo "Validation run completed under $validation_root/heft using hdfs-docker"
}

case "$COMMAND" in
  up)
    ensure_cluster_ready
    ;;
  down)
    if [[ -f "$COMPOSE_FILE" ]]; then
      compose down -v --remove-orphans
    fi
    ;;
  status)
    if [[ -f "$COMPOSE_FILE" ]]; then
      compose ps
    else
      echo "Compose file not found: $COMPOSE_FILE"
      exit 1
    fi
    ;;
  health)
    compose exec -T master hdfs dfsadmin -safemode get
    compose exec -T master hdfs dfsadmin -report
    compose exec -T master yarn node -list
    if [[ "$ENABLE_NODE_LABELS" == "true" ]]; then
      compose exec -T master yarn cluster --list-node-labels
    fi
    ;;
  validate)
    ensure_cluster_ready
    validate_cluster
    ;;
  *)
    echo "Usage: $0 {up|down|status|health|validate} [cluster-config] [output-dir] [image-tag]" >&2
    exit 1
    ;;
esac
