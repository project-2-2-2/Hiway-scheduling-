#!/usr/bin/env bash
set -euo pipefail

export JAVA_HOME="${JAVA_HOME:-/opt/java/openjdk}"
export HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"
export HADOOP_COMMON_HOME="${HADOOP_COMMON_HOME:-$HADOOP_HOME}"
export HADOOP_HDFS_HOME="${HADOOP_HDFS_HOME:-$HADOOP_HOME}"
export HADOOP_MAPRED_HOME="${HADOOP_MAPRED_HOME:-$HADOOP_HOME}"
export HADOOP_YARN_HOME="${HADOOP_YARN_HOME:-$HADOOP_HOME}"
export YARN_HOME="${YARN_HOME:-$HADOOP_HOME}"
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$JAVA_HOME/bin:$PATH"

ROLE="${ROLE:?ROLE must be set to master or worker}"
MASTER_HOST="${MASTER_HOST:-master}"
WORKER_HOSTS="${WORKER_HOSTS:-}"
CLUSTER_LABELS="${CLUSTER_LABELS:-}"
DATA_ROOT="${DATA_ROOT:-/data}"
HDFS_REPLICATION="${HDFS_REPLICATION:-1}"
YARN_MAX_MEMORY_MB="${YARN_MAX_MEMORY_MB:-8192}"
YARN_MAX_VCORES="${YARN_MAX_VCORES:-8}"
NODE_MEMORY_MB="${NODE_MEMORY_MB:-2048}"
NODE_CPU_VCORES="${NODE_CPU_VCORES:-2}"
NODE_LABELS_STORE_DIR="${NODE_LABELS_STORE_DIR:-file:///data/yarn/node-labels}"
WORKER_LIST_FILE="$HADOOP_CONF_DIR/workers"
NODEMANAGER_LOCAL_DIR="${DATA_ROOT}/yarn/local"
NODEMANAGER_LOG_DIR="${DATA_ROOT}/yarn/logs"
NODE_HOST="${NODE_HOST:-$(hostname -i | awk '{print $1}')}"

append_or_replace_env() {
  local file="$1"
  local key="$2"
  local value="$3"
  if grep -q "^export ${key}=" "$file"; then
    sed -i "s|^export ${key}=.*|export ${key}=${value}|" "$file"
  else
    echo "export ${key}=${value}" >> "$file"
  fi
}

render_config() {
  cat > "$HADOOP_CONF_DIR/core-site.xml" <<EOF
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://${MASTER_HOST}:9000</value>
  </property>
  <property>
    <name>hadoop.tmp.dir</name>
    <value>${DATA_ROOT}/tmp</value>
  </property>
</configuration>
EOF

  cat > "$HADOOP_CONF_DIR/hdfs-site.xml" <<EOF
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>${HDFS_REPLICATION}</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file://${DATA_ROOT}/hdfs/namenode</value>
  </property>
  <property>
    <name>dfs.namenode.rpc-bind-host</name>
    <value>0.0.0.0</value>
  </property>
  <property>
    <name>dfs.namenode.http-address</name>
    <value>${MASTER_HOST}:9870</value>
  </property>
  <property>
    <name>dfs.namenode.http-bind-host</name>
    <value>0.0.0.0</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file://${DATA_ROOT}/hdfs/datanode</value>
  </property>
  <property>
    <name>dfs.datanode.address</name>
    <value>0.0.0.0:9866</value>
  </property>
  <property>
    <name>dfs.datanode.http.address</name>
    <value>0.0.0.0:9864</value>
  </property>
  <property>
    <name>dfs.datanode.ipc.address</name>
    <value>0.0.0.0:9867</value>
  </property>
  <property>
    <name>dfs.namenode.datanode.registration.ip-hostname-check</name>
    <value>false</value>
  </property>
  <property>
    <name>dfs.client.use.datanode.hostname</name>
    <value>false</value>
  </property>
  <property>
    <name>dfs.datanode.use.datanode.hostname</name>
    <value>false</value>
  </property>
  <property>
    <name>dfs.namenode.safemode.extension</name>
    <value>0</value>
  </property>
  <property>
    <name>dfs.namenode.safemode.min.datanodes</name>
    <value>0</value>
  </property>
</configuration>
EOF

  cat > "$HADOOP_CONF_DIR/mapred-site.xml" <<EOF
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
    <value>JAVA_HOME=${JAVA_HOME},HADOOP_HOME=${HADOOP_HOME},HADOOP_MAPRED_HOME=${HADOOP_MAPRED_HOME},HADOOP_COMMON_HOME=${HADOOP_COMMON_HOME},HADOOP_HDFS_HOME=${HADOOP_HDFS_HOME},HADOOP_YARN_HOME=${HADOOP_YARN_HOME}</value>
  </property>
  <property>
    <name>mapreduce.map.env</name>
    <value>JAVA_HOME=${JAVA_HOME},HADOOP_HOME=${HADOOP_HOME},HADOOP_MAPRED_HOME=${HADOOP_MAPRED_HOME},HADOOP_COMMON_HOME=${HADOOP_COMMON_HOME},HADOOP_HDFS_HOME=${HADOOP_HDFS_HOME},HADOOP_YARN_HOME=${HADOOP_YARN_HOME}</value>
  </property>
  <property>
    <name>mapreduce.reduce.env</name>
    <value>JAVA_HOME=${JAVA_HOME},HADOOP_HOME=${HADOOP_HOME},HADOOP_MAPRED_HOME=${HADOOP_MAPRED_HOME},HADOOP_COMMON_HOME=${HADOOP_COMMON_HOME},HADOOP_HDFS_HOME=${HADOOP_HDFS_HOME},HADOOP_YARN_HOME=${HADOOP_YARN_HOME}</value>
  </property>
</configuration>
EOF

  {
    cat <<EOF
<configuration>
  <property>
    <name>yarn.scheduler.capacity.root.queues</name>
    <value>default</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.root.default.capacity</name>
    <value>100</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.root.default.maximum-capacity</name>
    <value>100</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.root.accessible-node-labels</name>
    <value>*</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.root.default.accessible-node-labels</name>
    <value>*</value>
  </property>
EOF
    if [[ -n "$CLUSTER_LABELS" ]]; then
      IFS=',' read -r -a labels <<< "$CLUSTER_LABELS"
      for label in "${labels[@]}"; do
        cat <<EOF
  <property>
    <name>yarn.scheduler.capacity.root.accessible-node-labels.${label}.capacity</name>
    <value>100</value>
  </property>
  <property>
    <name>yarn.scheduler.capacity.root.default.accessible-node-labels.${label}.capacity</name>
    <value>100</value>
  </property>
EOF
      done
    fi
    echo "</configuration>"
  } > "$HADOOP_CONF_DIR/capacity-scheduler.xml"

  cat > "$HADOOP_CONF_DIR/yarn-site.xml" <<EOF
<configuration>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>${MASTER_HOST}</value>
  </property>
  <property>
    <name>yarn.resourcemanager.address</name>
    <value>${MASTER_HOST}:8032</value>
  </property>
  <property>
    <name>yarn.resourcemanager.scheduler.address</name>
    <value>${MASTER_HOST}:8030</value>
  </property>
  <property>
    <name>yarn.resourcemanager.resource-tracker.address</name>
    <value>${MASTER_HOST}:8031</value>
  </property>
  <property>
    <name>yarn.resourcemanager.admin.address</name>
    <value>${MASTER_HOST}:8033</value>
  </property>
  <property>
    <name>yarn.webapp.address</name>
    <value>0.0.0.0:8088</value>
  </property>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
  <property>
    <name>yarn.nodemanager.bind-host</name>
    <value>0.0.0.0</value>
  </property>
  <property>
    <name>yarn.nodemanager.hostname</name>
    <value>${NODE_HOST}</value>
  </property>
  <property>
    <name>yarn.nodemanager.local-dirs</name>
    <value>${NODEMANAGER_LOCAL_DIR}</value>
  </property>
  <property>
    <name>yarn.nodemanager.log-dirs</name>
    <value>${NODEMANAGER_LOG_DIR}</value>
  </property>
  <property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>${NODE_MEMORY_MB}</value>
  </property>
  <property>
    <name>yarn.nodemanager.resource.cpu-vcores</name>
    <value>${NODE_CPU_VCORES}</value>
  </property>
  <property>
    <name>yarn.scheduler.maximum-allocation-mb</name>
    <value>${YARN_MAX_MEMORY_MB}</value>
  </property>
  <property>
    <name>yarn.scheduler.maximum-allocation-vcores</name>
    <value>${YARN_MAX_VCORES}</value>
  </property>
  <property>
    <name>yarn.scheduler.minimum-allocation-mb</name>
    <value>256</value>
  </property>
  <property>
    <name>yarn.scheduler.minimum-allocation-vcores</name>
    <value>1</value>
  </property>
  <property>
    <name>yarn.nodemanager.pmem-check-enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>yarn.nodemanager.vmem-check-enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>yarn.node-labels.enabled</name>
    <value>true</value>
  </property>
  <property>
    <name>yarn.node-labels.fs-store.root-dir</name>
    <value>${NODE_LABELS_STORE_DIR}</value>
  </property>
  <property>
    <name>yarn.node-labels.configuration-type</name>
    <value>centralized</value>
  </property>
</configuration>
EOF

  printf '%s\n' "${WORKER_HOSTS}" | tr ',' '\n' > "$WORKER_LIST_FILE"
  append_or_replace_env "$HADOOP_CONF_DIR/hadoop-env.sh" "JAVA_HOME" "${JAVA_HOME}"
  append_or_replace_env "$HADOOP_CONF_DIR/hadoop-env.sh" "HADOOP_HOME" "${HADOOP_HOME}"
  append_or_replace_env "$HADOOP_CONF_DIR/hadoop-env.sh" "HADOOP_COMMON_HOME" "${HADOOP_COMMON_HOME}"
  append_or_replace_env "$HADOOP_CONF_DIR/hadoop-env.sh" "HADOOP_HDFS_HOME" "${HADOOP_HDFS_HOME}"
  append_or_replace_env "$HADOOP_CONF_DIR/hadoop-env.sh" "HADOOP_MAPRED_HOME" "${HADOOP_MAPRED_HOME}"
  append_or_replace_env "$HADOOP_CONF_DIR/hadoop-env.sh" "HADOOP_YARN_HOME" "${HADOOP_YARN_HOME}"
  append_or_replace_env "$HADOOP_CONF_DIR/hadoop-env.sh" "YARN_HOME" "${YARN_HOME}"
  append_or_replace_env "$HADOOP_CONF_DIR/yarn-env.sh" "JAVA_HOME" "${JAVA_HOME}"
  append_or_replace_env "$HADOOP_CONF_DIR/yarn-env.sh" "HADOOP_HOME" "${HADOOP_HOME}"
  append_or_replace_env "$HADOOP_CONF_DIR/yarn-env.sh" "HADOOP_COMMON_HOME" "${HADOOP_COMMON_HOME}"
  append_or_replace_env "$HADOOP_CONF_DIR/yarn-env.sh" "HADOOP_HDFS_HOME" "${HADOOP_HDFS_HOME}"
  append_or_replace_env "$HADOOP_CONF_DIR/yarn-env.sh" "HADOOP_MAPRED_HOME" "${HADOOP_MAPRED_HOME}"
  append_or_replace_env "$HADOOP_CONF_DIR/yarn-env.sh" "HADOOP_YARN_HOME" "${HADOOP_YARN_HOME}"
  append_or_replace_env "$HADOOP_CONF_DIR/yarn-env.sh" "YARN_HOME" "${YARN_HOME}"
  append_or_replace_env "$HADOOP_CONF_DIR/mapred-env.sh" "JAVA_HOME" "${JAVA_HOME}"
  append_or_replace_env "$HADOOP_CONF_DIR/mapred-env.sh" "HADOOP_HOME" "${HADOOP_HOME}"
  append_or_replace_env "$HADOOP_CONF_DIR/mapred-env.sh" "HADOOP_COMMON_HOME" "${HADOOP_COMMON_HOME}"
  append_or_replace_env "$HADOOP_CONF_DIR/mapred-env.sh" "HADOOP_HDFS_HOME" "${HADOOP_HDFS_HOME}"
  append_or_replace_env "$HADOOP_CONF_DIR/mapred-env.sh" "HADOOP_MAPRED_HOME" "${HADOOP_MAPRED_HOME}"
  append_or_replace_env "$HADOOP_CONF_DIR/mapred-env.sh" "HADOOP_YARN_HOME" "${HADOOP_YARN_HOME}"
  append_or_replace_env "$HADOOP_CONF_DIR/mapred-env.sh" "YARN_HOME" "${YARN_HOME}"
}

mkdir -p \
  "${DATA_ROOT}/tmp" \
  "${DATA_ROOT}/hdfs/namenode" \
  "${DATA_ROOT}/hdfs/datanode" \
  "${DATA_ROOT}/yarn/node-labels" \
  "${NODEMANAGER_LOCAL_DIR}" \
  "${NODEMANAGER_LOG_DIR}"

render_config

if [[ "$ROLE" == "master" ]]; then
  if [[ ! -f "${DATA_ROOT}/hdfs/namenode/current/VERSION" ]]; then
    hdfs namenode -format -nonInteractive
  fi
  hdfs --daemon start namenode
  hdfs --daemon start secondarynamenode
  yarn --daemon start resourcemanager
  yarn --daemon start nodemanager
else
  for _ in $(seq 1 60); do
    if nc -z "$MASTER_HOST" 9000 >/dev/null 2>&1 && nc -z "$MASTER_HOST" 8032 >/dev/null 2>&1; then
      break
    fi
    sleep 2
  done
  hdfs --daemon start datanode
  yarn --daemon start nodemanager
fi

touch "$HADOOP_HOME/logs/container.log"
tail -F "$HADOOP_HOME"/logs/* "$HADOOP_HOME/logs/container.log"
