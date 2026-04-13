package org.wsh.scheduler;

import org.wsh.model.ClusterProfile;
import org.wsh.model.JobDefinition;
import org.wsh.model.NodeProfile;
import org.wsh.model.PlanAssignment;
import org.wsh.model.WorkflowDefinition;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class WshScheduler implements Scheduler {
    private final Mode mode;

    public WshScheduler() {
        this(Mode.PAPER);
    }

    public WshScheduler(Mode mode) {
        this.mode = mode == null ? Mode.PAPER : mode;
    }

    @Override
    public List<PlanAssignment> buildPlan(WorkflowDefinition workflow, List<ClusterProfile> clusters, TrainingBenchmarks benchmarks) {
        SchedulingCostModel costModel = new SchedulingCostModel(workflow, clusters, benchmarks, true);
        Map<String, Double> ranks = costModel.computeUpwardRanks(mode == Mode.COMM_AWARE);
        List<JobDefinition> ordered = workflow.jobs().stream()
                .sorted(Comparator.<JobDefinition>comparingDouble(job -> ranks.get(job.id())).reversed()
                        .thenComparingInt(job -> workflow.orderOf(job.id())))
                .toList();
        Map<String, Long> nodeAvailable = new HashMap<>();
        Map<String, PlanAssignment> scheduledAssignments = new HashMap<>();
        Map<String, String> assignedClusterByJob = new HashMap<>();
        Map<String, Integer> rootAssignmentsPerCluster = new HashMap<>();
        Map<String, Integer> activatedNodesPerCluster = new HashMap<>();
        List<PlanAssignment> plan = new ArrayList<>();
        for (JobDefinition job : ordered) {
            List<String> sortedClusters = adjustedClusterOrder(
                    job,
                    benchmarks.sortedClusters(job, clusters),
                    benchmarks,
                    clusters,
                    costModel,
                    assignedClusterByJob,
                    rootAssignmentsPerCluster);
            List<NodeProfile> candidates = candidateNodes(sortedClusters, clusters, activatedNodesPerCluster);
            Candidate best = null;
            for (NodeProfile node : candidates) {
                SchedulingCostModel.CommunicationTotals communicationTotals =
                        costModel.communicationTotals(job, node, scheduledAssignments);
                long communication = communicationTotals.totalCommunicationMillis();
                long ready = mode == Mode.COMM_AWARE
                        ? communicationTotals.readyTimeMillis()
                        : costModel.dependencyReadyTime(job, scheduledAssignments);
                long start = Math.max(nodeAvailable.getOrDefault(node.nodeId(), 0L), ready);
                long execution = costModel.duration(job, node);
                long finish = start + execution;
                double energy = costModel.computeEnergyJoules(
                        job,
                        node,
                        execution,
                        communicationTotals.sameClusterCommunicationMillis(),
                        communicationTotals.crossClusterCommunicationMillis());
                Candidate candidate = new Candidate(node, start, finish, communication, execution, energy);
                if (best == null || candidate.finish < best.finish) {
                    best = candidate;
                }
            }
            PlanAssignment assignment = new PlanAssignment(
                    job.id(),
                    best.node.clusterId(),
                    best.node.nodeId(),
                    best.start,
                    best.finish,
                    best.communicationMillis,
                    best.executionMillis,
                    best.energyJoules,
                    ranks.get(job.id()),
                    name(),
                    benchmarks.classification(job));
            nodeAvailable.put(best.node.nodeId(), best.finish);
            scheduledAssignments.put(job.id(), assignment);
            assignedClusterByJob.put(job.id(), best.node.clusterId());
            if (job.dependencies().isEmpty()) {
                rootAssignmentsPerCluster.merge(best.node.clusterId(), 1, Integer::sum);
            }
            String selectedClusterId = best.node.clusterId();
            ClusterProfile cluster = clusters.stream()
                    .filter(item -> item.clusterId().equals(selectedClusterId))
                    .findFirst()
                    .orElseThrow();
            int activated = activatedNodesPerCluster.getOrDefault(cluster.clusterId(), 0);
            if (activated < cluster.nodes().size() && cluster.nodes().get(activated).nodeId().equals(best.node.nodeId())) {
                activatedNodesPerCluster.put(cluster.clusterId(), activated + 1);
            }
            plan.add(assignment);
        }
        return plan;
    }

    private List<String> adjustedClusterOrder(
            JobDefinition job,
            List<String> sortedClusterIds,
            TrainingBenchmarks benchmarks,
            List<ClusterProfile> clusters,
            SchedulingCostModel costModel,
            Map<String, String> assignedClusterByJob,
            Map<String, Integer> rootAssignmentsPerCluster) {
        if (sortedClusterIds.size() <= 1 || !benchmarks.hasMeasurements(job)) {
            return sortedClusterIds;
        }
        List<String> adjusted = new ArrayList<>(sortedClusterIds);
        if (job.dependencies().size() == 1) {
            String parentClusterId = assignedClusterByJob.get(job.dependencies().get(0));
            if (parentClusterId != null
                    && adjusted.contains(parentClusterId)
                    && isCompetitiveAlternative(job, parentClusterId, adjusted.get(0), benchmarks, clusters, costModel, 0.40)) {
                moveToFront(adjusted, parentClusterId);
            }
            return adjusted;
        }
        if (!job.dependencies().isEmpty()) {
            return adjusted;
        }
        String bestClusterId = adjusted.get(0);
        String alternate = adjusted.stream()
                .filter(clusterId -> !clusterId.equals(bestClusterId))
                .filter(clusterId -> rootAssignmentsPerCluster.getOrDefault(clusterId, 0) == 0)
                .filter(clusterId -> isCompetitiveAlternative(job, clusterId, bestClusterId, benchmarks, clusters, costModel, 0.35))
                .findFirst()
                .orElse(null);
        if (alternate != null && rootAssignmentsPerCluster.getOrDefault(bestClusterId, 0) > 0) {
            moveToFront(adjusted, alternate);
        }
        return adjusted;
    }

    private boolean isCompetitiveAlternative(
            JobDefinition job,
            String candidateClusterId,
            String bestClusterId,
            TrainingBenchmarks benchmarks,
            List<ClusterProfile> clusters,
            SchedulingCostModel costModel,
            double toleranceFraction) {
        if (candidateClusterId.equals(bestClusterId)) {
            return true;
        }
        NodeProfile candidateNode = clusterFirstNode(candidateClusterId, clusters);
        NodeProfile bestNode = clusterFirstNode(bestClusterId, clusters);
        long bestDuration = benchmarks.duration(job, bestClusterId);
        long candidateDuration = benchmarks.duration(job, candidateClusterId);
        long communicationDelta = mode == Mode.COMM_AWARE
                ? Math.max(0L, costModel.communicationMillis(job, bestNode, candidateNode) / 2L)
                : 0L;
        long tolerance = Math.max(50L, Math.round(bestDuration * toleranceFraction));
        return candidateDuration <= bestDuration + tolerance + communicationDelta;
    }

    private NodeProfile clusterFirstNode(String clusterId, List<ClusterProfile> clusters) {
        return clusters.stream()
                .filter(cluster -> cluster.clusterId().equals(clusterId))
                .findFirst()
                .orElseThrow()
                .firstNode();
    }

    private void moveToFront(List<String> clusterIds, String clusterId) {
        if (!clusterIds.remove(clusterId)) {
            return;
        }
        clusterIds.add(0, clusterId);
    }

    @Override
    public String name() {
        return "WSH";
    }

    public enum Mode {
        PAPER,
        COMM_AWARE;

        public static Mode fromCliValue(String raw) {
            if (raw == null || raw.isBlank()) {
                return PAPER;
            }
            return switch (raw.trim().toLowerCase()) {
                case "paper", "baseline" -> PAPER;
                case "comm-aware", "comm_aware", "communication-aware", "communication_aware" -> COMM_AWARE;
                default -> throw new IllegalArgumentException("Unsupported WSH mode: " + raw);
            };
        }
    }

    private List<NodeProfile> candidateNodes(
            List<String> sortedClusterIds,
            List<ClusterProfile> clusters,
            Map<String, Integer> activatedNodesPerCluster) {
        List<NodeProfile> candidates = new ArrayList<>();
        for (String clusterId : sortedClusterIds) {
            ClusterProfile cluster = clusters.stream().filter(item -> item.clusterId().equals(clusterId)).findFirst().orElseThrow();
            int activated = activatedNodesPerCluster.getOrDefault(clusterId, 0);
            int activeCount = Math.min(activated, cluster.nodes().size());
            candidates.addAll(cluster.nodes().subList(0, activeCount));
            if (activeCount < cluster.nodes().size()) {
                candidates.add(cluster.nodes().get(activeCount));
                break;
            }
        }
        if (candidates.isEmpty()) {
            return clusters.stream().flatMap(cluster -> cluster.nodes().stream()).toList();
        }
        return candidates;
    }

    private record Candidate(
            NodeProfile node,
            long start,
            long finish,
            long communicationMillis,
            long executionMillis,
            double energyJoules) {
    }
}
