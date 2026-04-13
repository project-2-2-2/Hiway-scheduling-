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

public final class HeftScheduler implements Scheduler {
    @Override
    public List<PlanAssignment> buildPlan(WorkflowDefinition workflow, List<ClusterProfile> clusters, TrainingBenchmarks benchmarks) {
        SchedulingCostModel costModel = new SchedulingCostModel(workflow, clusters, benchmarks, false);
        Map<String, Double> ranks = costModel.computeUpwardRanks();
        List<JobDefinition> ordered = workflow.jobs().stream()
                .sorted(Comparator.<JobDefinition>comparingDouble(job -> ranks.get(job.id())).reversed()
                        .thenComparingInt(job -> workflow.orderOf(job.id())))
                .toList();
        List<NodeProfile> nodes = clusters.stream().flatMap(cluster -> cluster.nodes().stream()).toList();
        Map<String, Long> nodeAvailable = new HashMap<>();
        Map<String, PlanAssignment> scheduledAssignments = new HashMap<>();
        List<PlanAssignment> plan = new ArrayList<>();
        for (JobDefinition job : ordered) {
            Candidate best = null;
            for (NodeProfile node : nodes) {
                SchedulingCostModel.CommunicationTotals communicationTotals =
                        costModel.communicationTotals(job, node, scheduledAssignments);
                long communication = communicationTotals.totalCommunicationMillis();
                long ready = communicationTotals.readyTimeMillis();
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
                    job.taskType().defaultClassification());
            nodeAvailable.put(best.node.nodeId(), best.finish);
            scheduledAssignments.put(job.id(), assignment);
            plan.add(assignment);
        }
        return plan;
    }

    @Override
    public String name() {
        return "HEFT";
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
