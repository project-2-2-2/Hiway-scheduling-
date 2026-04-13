package org.wsh.execution;

import org.wsh.hadoop.HadoopJobRunner;
import org.wsh.hadoop.HdfsDockerJobRunner;
import org.wsh.model.ClusterProfile;
import org.wsh.model.JobDefinition;
import org.wsh.model.NodeProfile;
import org.wsh.scheduler.DurationModel;
import org.wsh.scheduler.TrainingBenchmarks;
import org.wsh.task.TaskExecutor;
import org.wsh.task.TaskInputs;
import org.wsh.task.TaskResult;
import org.wsh.workflow.WorkflowSpec;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class TrainingRunner {
    private static final long MEASUREMENT_NOISE_FLOOR_MS = 50L;

    private final WorkflowSpec workflowSpec;
    private final Map<String, TaskExecutor> executors;
    private final ExecutionMode executionMode;
    private final DockerNodePool dockerNodePool;
    private final HadoopJobRunner hadoopJobRunner;
    private final HdfsDockerJobRunner hdfsDockerJobRunner;
    private final int warmupRuns;
    private final int measurementRuns;

    public TrainingRunner(
            WorkflowSpec workflowSpec,
            Map<String, TaskExecutor> executors,
            ExecutionMode executionMode,
            DockerNodePool dockerNodePool,
            HadoopJobRunner hadoopJobRunner,
            HdfsDockerJobRunner hdfsDockerJobRunner,
            int warmupRuns,
            int measurementRuns) {
        this.workflowSpec = workflowSpec;
        this.executors = executors;
        this.executionMode = executionMode;
        this.dockerNodePool = dockerNodePool;
        this.hadoopJobRunner = hadoopJobRunner;
        this.hdfsDockerJobRunner = hdfsDockerJobRunner;
        this.warmupRuns = Math.max(0, warmupRuns);
        this.measurementRuns = Math.max(1, measurementRuns);
    }

    public TrainingBenchmarks benchmark(Path dataRoot, List<ClusterProfile> clusters) throws Exception {
        workflowSpec.ensureTrainingParents(dataRoot);
        Map<String, Map<String, Long>> durations = new HashMap<>();
        Map<String, String> classifications = new HashMap<>();
        for (ClusterProfile cluster : clusters) {
            NodeProfile node = cluster.firstNode();
            for (JobDefinition job : workflowSpec.definition().trainingRepresentativeJobs()) {
                TaskInputs inputs = workflowSpec.resolveTrainingInputs(job.id(), dataRoot);
                for (int warmup = 0; warmup < warmupRuns; warmup++) {
                    executeTrainingSample(job.id(), node, inputs, dataRoot);
                }
                List<Long> measuredDurations = new ArrayList<>();
                for (int sample = 0; sample < measurementRuns; sample++) {
                    long start = System.currentTimeMillis();
                    executeTrainingSample(job.id(), node, inputs, dataRoot);
                    long finish = System.currentTimeMillis();
                    measuredDurations.add(Math.max(1L, finish - start));
                }
                durations.computeIfAbsent(job.trainingProfileKey(), ignored -> new HashMap<>()).put(cluster.clusterId(), median(measuredDurations));
            }
        }
        for (JobDefinition job : workflowSpec.definition().trainingRepresentativeJobs()) {
            DurationCorrection correction = correctedDurations(job, clusters, durations.get(job.trainingProfileKey()));
            durations.put(job.trainingProfileKey(), correction.durations());
            classifications.put(job.trainingProfileKey(), correction.measured()
                    ? derivedClassification(job, correction.durations())
                    : job.taskType().defaultClassification());
        }
        return new TrainingBenchmarks(durations, classifications, warmupRuns, measurementRuns);
    }

    private TaskResult executeTrainingSample(String jobId, NodeProfile node, TaskInputs inputs, Path dataRoot) throws Exception {
        return switch (executionMode) {
            case LOCAL -> executors.get(jobId).execute(inputs, node);
            case DOCKER -> dockerNodePool.execute(workflowSpec, node, jobId, inputs);
            case HADOOP -> {
                if (hadoopJobRunner == null) {
                    throw new IllegalStateException("Hadoop executor selected without Hadoop job runner");
                }
                yield hadoopJobRunner.executeTrainingJob(workflowSpec, jobId, node, dataRoot);
            }
            case HDFS_DOCKER -> {
                if (hdfsDockerJobRunner == null) {
                    throw new IllegalStateException("HDFS Docker executor selected without HDFS Docker job runner");
                }
                yield hdfsDockerJobRunner.executeTrainingJob(workflowSpec, jobId, node, dataRoot);
            }
        };
    }

    private long median(List<Long> values) {
        List<Long> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int middle = sorted.size() / 2;
        if (sorted.size() % 2 == 1) {
            return sorted.get(middle);
        }
        return Math.round((sorted.get(middle - 1) + sorted.get(middle)) / 2.0);
    }

    private DurationCorrection correctedDurations(
            JobDefinition job,
            List<ClusterProfile> clusters,
            Map<String, Long> measuredDurations) {
        long maxMeasured = measuredDurations.values().stream().mapToLong(Long::longValue).max().orElse(0L);
        if (maxMeasured > MEASUREMENT_NOISE_FLOOR_MS) {
            return new DurationCorrection(measuredDurations, true);
        }
        Map<String, Long> fallback = new HashMap<>();
        for (ClusterProfile cluster : clusters) {
            fallback.put(cluster.clusterId(), DurationModel.estimateDuration(job, cluster.firstNode()));
        }
        return new DurationCorrection(fallback, false);
    }

    private String derivedClassification(JobDefinition job, Map<String, Long> correctedDurations) {
        String defaultClassification = job.taskType().defaultClassification();
        if (correctedDurations == null || correctedDurations.isEmpty()) {
            return defaultClassification;
        }
        long minDuration = correctedDurations.values().stream().mapToLong(Long::longValue).min().orElse(0L);
        long maxDuration = correctedDurations.values().stream().mapToLong(Long::longValue).max().orElse(0L);
        if (minDuration <= 0L) {
            return defaultClassification;
        }
        long absoluteSpread = Math.abs(maxDuration - minDuration);
        double relativeSpread = (double) absoluteSpread / minDuration;
        if (absoluteSpread <= 25L || relativeSpread <= 0.12) {
            return "io";
        }
        return "compute";
    }

    private record DurationCorrection(Map<String, Long> durations, boolean measured) {
    }
}
