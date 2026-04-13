package org.wsh.scheduler;

import org.wsh.model.ClusterProfile;
import org.wsh.model.JobDefinition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class TrainingBenchmarks {
    private final Map<String, Map<String, Long>> durationsByProfileAndCluster;
    private final Map<String, String> classifications;
    private final int warmupRuns;
    private final int measurementRuns;

    public TrainingBenchmarks(
            Map<String, Map<String, Long>> durationsByProfileAndCluster,
            Map<String, String> classifications,
            int warmupRuns,
            int measurementRuns) {
        this.durationsByProfileAndCluster = durationsByProfileAndCluster;
        this.classifications = classifications;
        this.warmupRuns = warmupRuns;
        this.measurementRuns = measurementRuns;
    }

    public long duration(JobDefinition job, String clusterId) {
        return durationsByProfileAndCluster.get(job.trainingProfileKey()).get(clusterId);
    }

    public double averageDuration(JobDefinition job) {
        return durationsByProfileAndCluster.get(job.trainingProfileKey()).values().stream().mapToLong(Long::longValue).average().orElse(0);
    }

    public String classification(JobDefinition job) {
        return classifications.getOrDefault(job.trainingProfileKey(), job.taskType().defaultClassification());
    }

    public boolean hasMeasurements(JobDefinition job) {
        return durationsByProfileAndCluster.containsKey(job.trainingProfileKey()) && !durationsByProfileAndCluster.get(job.trainingProfileKey()).isEmpty();
    }

    public int warmupRuns() {
        return warmupRuns;
    }

    public int measurementRuns() {
        return measurementRuns;
    }

    public List<String> sortedClusters(JobDefinition job, List<ClusterProfile> clusters) {
        if (!hasMeasurements(job)) {
            return staticClusterOrder(job, clusters);
        }
        return clusters.stream()
                .sorted((left, right) -> {
                    String classification = classification(job);
                    long leftDuration = duration(job, left.clusterId());
                    long rightDuration = duration(job, right.clusterId());
                    if (effectivelyEqual(leftDuration, rightDuration, classification)) {
                        return compareByModeledPreference(job, left, right);
                    }
                    int compare = Long.compare(leftDuration, rightDuration);
                    if (compare != 0) {
                        return compare;
                    }
                    int modeledCompare = compareByModeledPreference(job, left, right);
                    if (modeledCompare != 0) {
                        return modeledCompare;
                    }
                    return left.clusterId().compareTo(right.clusterId());
                })
                .map(ClusterProfile::clusterId)
                .toList();
    }

    private List<String> staticClusterOrder(JobDefinition job, List<ClusterProfile> clusters) {
        return clusters.stream()
                .sorted((left, right) -> compareByModeledPreference(job, left, right))
                .map(ClusterProfile::clusterId)
                .toList();
    }

    private int compareByModeledPreference(JobDefinition job, ClusterProfile left, ClusterProfile right) {
        long leftEstimate = DurationModel.estimateDuration(job, left.firstNode());
        long rightEstimate = DurationModel.estimateDuration(job, right.firstNode());
        int compare = Long.compare(leftEstimate, rightEstimate);
        if (compare != 0) {
            return compare;
        }
        compare = Integer.compare(right.maxCpuThreads(), left.maxCpuThreads());
        if (compare != 0) {
            return compare;
        }
        return left.clusterId().compareTo(right.clusterId());
    }

    private boolean effectivelyEqual(long leftDuration, long rightDuration, String classification) {
        long minimum = Math.max(1L, Math.min(leftDuration, rightDuration));
        double threshold = "io".equals(classification) ? 0.25 : 0.25;
        return Math.abs(leftDuration - rightDuration) <= Math.max(50L, Math.round(minimum * threshold));
    }

    public static TrainingBenchmarks empty() {
        return new TrainingBenchmarks(new HashMap<>(), new HashMap<>(), 0, 0);
    }
}
