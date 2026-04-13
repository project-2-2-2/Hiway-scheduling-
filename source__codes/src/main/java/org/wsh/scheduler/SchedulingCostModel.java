package org.wsh.scheduler;

import org.wsh.model.ClusterProfile;
import org.wsh.model.JobDefinition;
import org.wsh.model.NodeProfile;
import org.wsh.model.PlanAssignment;
import org.wsh.model.WorkflowDefinition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class SchedulingCostModel {
    private static final long SAME_CLUSTER_LATENCY_MS = 4L;
    private static final long CROSS_CLUSTER_LATENCY_MS = 18L;
    private static final double SAME_CLUSTER_NETWORK_WATTS = 6.0;
    private static final double CROSS_CLUSTER_NETWORK_WATTS = 14.0;

    private final WorkflowDefinition workflow;
    private final List<ClusterProfile> clusters;
    private final TrainingBenchmarks benchmarks;
    private final boolean preferBenchmarks;
    private final List<NodeProfile> nodes;
    private final Map<String, Double> averageExecutionCache;
    private final Map<String, Double> averageCommunicationCache;

    public SchedulingCostModel(
            WorkflowDefinition workflow,
            List<ClusterProfile> clusters,
            TrainingBenchmarks benchmarks,
            boolean preferBenchmarks) {
        this.workflow = workflow;
        this.clusters = List.copyOf(clusters);
        this.benchmarks = benchmarks == null ? TrainingBenchmarks.empty() : benchmarks;
        this.preferBenchmarks = preferBenchmarks;
        this.nodes = this.clusters.stream().flatMap(cluster -> cluster.nodes().stream()).toList();
        this.averageExecutionCache = new HashMap<>();
        this.averageCommunicationCache = new HashMap<>();
    }

    public long duration(JobDefinition job, NodeProfile node) {
        if (preferBenchmarks && benchmarks.hasMeasurements(job)) {
            return benchmarks.duration(job, node.clusterId());
        }
        return DurationModel.estimateDuration(job, node);
    }

    public double averageDuration(JobDefinition job) {
        return averageExecutionCache.computeIfAbsent(job.id(), ignored -> nodes.stream()
                .mapToLong(node -> duration(job, node))
                .average()
                .orElse(0.0));
    }

    public long dependencyReadyTime(
            JobDefinition job,
            Map<String, PlanAssignment> scheduledAssignments) {
        long ready = 0L;
        for (String dependencyId : job.dependencies()) {
            PlanAssignment dependency = scheduledAssignments.get(dependencyId);
            if (dependency == null) {
                continue;
            }
            ready = Math.max(ready, dependency.predictedFinishMillis());
        }
        return ready;
    }

    public long communicationMillis(JobDefinition parent, NodeProfile source, NodeProfile target) {
        if (source.nodeId().equals(target.nodeId())) {
            return 0L;
        }
        long bytes = Math.max(1L, parent.modeledOutputBytes());
        long throughput = Math.max(1L, Math.min(source.modeledNetworkBytesPerSecond(), target.modeledNetworkBytesPerSecond()));
        long transfer = Math.max(1L, Math.round((bytes * 1000.0) / throughput));
        if (source.clusterId().equals(target.clusterId())) {
            return SAME_CLUSTER_LATENCY_MS + transfer;
        }
        return CROSS_CLUSTER_LATENCY_MS + Math.max(1L, transfer * 3L);
    }

    public double averageCommunicationMillis(JobDefinition parent, JobDefinition child) {
        String key = parent.id() + "->" + child.id();
        return averageCommunicationCache.computeIfAbsent(key, ignored -> {
            if (nodes.size() <= 1) {
                return 0.0;
            }
            double sum = 0.0;
            long count = 0L;
            for (NodeProfile source : nodes) {
                for (NodeProfile target : nodes) {
                    sum += communicationMillis(parent, source, target);
                    count++;
                }
            }
            return count == 0 ? 0.0 : sum / count;
        });
    }

    public CommunicationTotals communicationTotals(
            JobDefinition job,
            NodeProfile node,
            Map<String, PlanAssignment> scheduledAssignments) {
        long ready = 0L;
        long total = 0L;
        long sameCluster = 0L;
        long crossCluster = 0L;
        for (String dependencyId : job.dependencies()) {
            PlanAssignment dependency = scheduledAssignments.get(dependencyId);
            if (dependency == null) {
                continue;
            }
            JobDefinition parent = workflow.job(dependencyId);
            NodeProfile source = nodeById(dependency.nodeId());
            long communication = communicationMillis(parent, source, node);
            ready = Math.max(ready, dependency.predictedFinishMillis() + communication);
            total += communication;
            if (communication > 0L) {
                if (source.clusterId().equals(node.clusterId())) {
                    sameCluster += communication;
                } else {
                    crossCluster += communication;
                }
            }
        }
        return new CommunicationTotals(ready, total, sameCluster, crossCluster);
    }

    public long readyTime(
            JobDefinition job,
            NodeProfile node,
            Map<String, PlanAssignment> scheduledAssignments) {
        return communicationTotals(job, node, scheduledAssignments).readyTimeMillis();
    }

    public double computeEnergyJoules(
            JobDefinition job,
            NodeProfile node,
            long executionMillis,
            long sameClusterCommunicationMillis,
            long crossClusterCommunicationMillis) {
        double executionSeconds = executionMillis / 1000.0;
        double sameClusterSeconds = sameClusterCommunicationMillis / 1000.0;
        double crossClusterSeconds = crossClusterCommunicationMillis / 1000.0;
        double taskLoadFactor = switch (job.taskType().defaultClassification()) {
            case "compute" -> 1.0;
            case "io" -> 0.82;
            default -> 0.9;
        };
        double activeJoules = node.modeledActivePowerWatts() * taskLoadFactor * executionSeconds;
        double idleJoules = node.modeledIdlePowerWatts() * Math.max(0.0, 0.1 * executionSeconds);
        double networkJoules = (SAME_CLUSTER_NETWORK_WATTS * sameClusterSeconds)
                + (CROSS_CLUSTER_NETWORK_WATTS * crossClusterSeconds);
        return activeJoules + idleJoules + networkJoules;
    }

    public Map<String, Double> computeUpwardRanks() {
        return computeUpwardRanks(true);
    }

    public Map<String, Double> computeUpwardRanks(boolean includeCommunication) {
        Map<String, Double> ranks = new HashMap<>();
        for (JobDefinition job : workflow.jobs()) {
            computeUpwardRank(job.id(), ranks, includeCommunication);
        }
        return ranks;
    }

    public double computeUpwardRank(String jobId, Map<String, Double> cache) {
        return computeUpwardRank(jobId, cache, true);
    }

    public double computeUpwardRank(String jobId, Map<String, Double> cache, boolean includeCommunication) {
        if (cache.containsKey(jobId)) {
            return cache.get(jobId);
        }
        JobDefinition job = workflow.job(jobId);
        double own = averageDuration(job);
        double successor = workflow.successors(jobId).stream()
                .mapToDouble(child -> (includeCommunication ? averageCommunicationMillis(job, child) : 0.0)
                        + computeUpwardRank(child.id(), cache, includeCommunication))
                .max()
                .orElse(0.0);
        double value = own + successor;
        cache.put(jobId, value);
        return value;
    }

    public long optimisticCriticalPath() {
        Map<String, Long> cache = new HashMap<>();
        long max = 0L;
        for (JobDefinition job : workflow.jobs()) {
            max = Math.max(max, optimisticCriticalPath(job.id(), cache));
        }
        return max;
    }

    private long optimisticCriticalPath(String jobId, Map<String, Long> cache) {
        if (cache.containsKey(jobId)) {
            return cache.get(jobId);
        }
        JobDefinition job = workflow.job(jobId);
        long own = nodes.stream()
                .mapToLong(node -> duration(job, node))
                .min()
                .orElse(0L);
        long successor = workflow.successors(jobId).stream()
                .mapToLong(child -> optimisticTransferLowerBound(job, child) + optimisticCriticalPath(child.id(), cache))
                .max()
                .orElse(0L);
        long value = own + successor;
        cache.put(jobId, value);
        return value;
    }

    public long optimisticTransferLowerBound(JobDefinition parent, JobDefinition child) {
        long best = Long.MAX_VALUE;
        for (NodeProfile source : nodes) {
            for (NodeProfile target : nodes) {
                best = Math.min(best, communicationMillis(parent, source, target));
            }
        }
        return best == Long.MAX_VALUE ? 0L : best;
    }

    public record CommunicationTotals(
            long readyTimeMillis,
            long totalCommunicationMillis,
            long sameClusterCommunicationMillis,
            long crossClusterCommunicationMillis) {
    }

    public NodeProfile nodeById(String nodeId) {
        return nodes.stream()
                .filter(node -> node.nodeId().equals(nodeId))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown node id: " + nodeId));
    }
}
