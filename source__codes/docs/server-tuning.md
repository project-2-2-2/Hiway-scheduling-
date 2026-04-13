# Server Tuning

## Recommended Baseline

Use the paper-style cluster profile first:

- `CLUSTER_CONFIG=./config/clusters-z4-g5-paper-sweep.csv`
- `WSH_JAVA_OPTS="-Xms4g -Xmx16g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseStringDeduplication"`
- `PROFILE=small` for quick validation
- `PROFILE=medium` for more stable comparison runs

The legacy `GENE2LIFE_JAVA_OPTS` env var is still accepted, but new usage should prefer `WSH_JAVA_OPTS`.

## Cluster Profiles

- `config/clusters-z4-g5-paper-sweep.csv`
  closest to the paper’s 12-worker heterogeneous layout
- `config/clusters-z4-g5-paper-sweep-scaled.csv`
  same heterogeneous pattern with larger per-node capacity
- `config/clusters-z4-g5-dense-28.csv`
  dense host-extension profile for larger overnight sweeps

## Recommended Commands

Quick preflight:

```bash
./scripts/run-workflows.sh preflight
```

Paper-style sweep:

```bash
WSH_JAVA_OPTS="-Xms4g -Xmx16g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseStringDeduplication" \
./scripts/run-workflows.sh sweep \
  --workflows gene2life,avianflu_small,epigenomics \
  --nodes 4,7,10,13 \
  --profile medium \
  --rounds 3 \
  --schedulers wsh,heft,pso,ga,energy-aware
```

Dense 28-node extension:

```bash
WSH_JAVA_OPTS="-Xms8g -Xmx24g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseStringDeduplication" \
./scripts/run-workflows.sh sweep \
  --workflows gene2life,avianflu_small,epigenomics \
  --nodes 28 \
  --profile medium \
  --cluster-config ./config/clusters-z4-g5-dense-28.csv \
  --rounds 3 \
  --schedulers wsh,heft,pso,ga,energy-aware
```

Validate the Docker Hadoop cluster before a long run:

```bash
./scripts/hadoop-docker-cluster.sh validate \
  ./config/clusters-z4-g5-paper-sweep.csv \
  ./work/hadoop-docker-cluster \
  wsh-hadoop-cluster:3.4.3
```

Validate Docker-only CPU pinning:

```bash
./scripts/validate-docker-node-pinning.sh \
  ./config/clusters-z4-g5-paper-sweep.csv \
  wsh-java:latest
```

## Practical Guidance

- Keep the dataset fixed while varying only scheduler or node count.
- Prefer `compare` or `run-workflows.sh sweep` over ad hoc back-to-back single runs.
- Use `hdfs-docker` when you want Hadoop-backed storage without the worst per-task YARN submission overhead.
- If the host starts paging, reduce dataset size before increasing heap.
- `scripts/cleanup-project-runtime.sh` removes repo-owned Docker runtime only.
