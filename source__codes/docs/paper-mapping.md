# Paper To Code Mapping

## Scope

`Main-Paper.pdf` compares `WSH` and `HEFT` across three workflows:

- `gene2life`
- `avianflu_small`
- `epigenomics`

This repository implements those workflow families in Java under `src/main/java/org/wsh/`.

## Workflow Mapping

### `gene2life`

Paper structure:

`blast1, blast2 -> clustalw1, clustalw2 -> dnapars, protpars -> drawgram1, drawgram2`

Code:

- workflow spec: `src/main/java/org/wsh/workflow/Gene2LifeWorkflowSpec.java`
- task families: `BLAST`, `CLUSTAL`, `DNAPARS`, `PROTPARS`, `DRAWGRAM`

### `avianflu_small`

Code mapping preserves the paper topology with preprocessing, `auto-grid`, and a large `autodock` fanout.

- default scale: `104` jobs
- tunable fanout: `--avianflu-autodock-count <n>`

### `epigenomics`

Code mapping preserves the split, repeated chunk processing, and merge/index/final stages.

- default scale: `100` jobs
- tunable split fanout: `--epigenomics-split-count <n>`

## Scheduling Mapping

### `HEFT`

- upward rank uses modeled execution plus communication cost
- earliest start includes predecessor transfer time across nodes or clusters
- placement minimizes earliest finish time

### `WSH`

- training tasks benchmark the first node of each cluster
- cluster ordering is derived from measured training runtimes
- upward ranks and placement include communication costs
- node expansion follows the paper-style cluster-prefix strategy

### Extensions

- `PSO`
  particle-swarm search over job priority, cluster affinity, and energy bias
- `GA`
  genetic search over the same scheduling decision space
- `energy-aware`
  communication-aware placement with an explicit energy trade-off

## Execution Mapping

- `local`
  single-host JVM execution
- `docker`
  pinned Docker logical nodes execute task bodies
- `hadoop`
  one Hadoop job per workflow task
- `hdfs-docker`
  data and intermediates in `HDFS`, task execution in pinned Docker nodes

`hdfs-docker` is the primary validated execution path in this repo.

## Metric Mapping

- `makespan`
  wall-clock difference between first job start and last job finish
- `speedup`
  sequential runtime sum divided by makespan
- `SLR`
  makespan divided by a scheduler-independent modeled critical path lower bound
- communication and energy
  modeled from job sizes and node heuristics, not host-measured telemetry

## What Still Differs From The Paper

- this is a Java reimplementation, not the original Hi-WAY codebase
- Hadoop runs inside Docker on one host, not separate physical machines
- task bodies approximate the original bioinformatics tools
- the repo adds communication-aware, metaheuristic, and energy-aware scheduler variants beyond the paper baseline
