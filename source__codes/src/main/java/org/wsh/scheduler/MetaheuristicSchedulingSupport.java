package org.wsh.scheduler;

import org.wsh.model.ClusterProfile;
import org.wsh.model.JobDefinition;
import org.wsh.model.NodeProfile;
import org.wsh.model.PlanAssignment;
import org.wsh.model.WorkflowDefinition;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public final class MetaheuristicSchedulingSupport {
    private final WorkflowDefinition workflow;
    private final List<ClusterProfile> clusters;
    private final TrainingBenchmarks benchmarks;
    private final SchedulingCostModel costModel;
    private final Map<String, Double> baseRanks;
    private final List<JobDefinition> jobs;
    private final List<NodeProfile> nodes;
    private final double rankScale;
    private final Map<String, Integer> jobIndexById;
    private final Map<String, List<String>> rankedClustersByJob;

    public MetaheuristicSchedulingSupport(
            WorkflowDefinition workflow,
            List<ClusterProfile> clusters,
            TrainingBenchmarks benchmarks) {
        this.workflow = workflow;
        this.clusters = List.copyOf(clusters);
        this.benchmarks = benchmarks == null ? TrainingBenchmarks.empty() : benchmarks;
        this.costModel = new SchedulingCostModel(workflow, clusters, this.benchmarks, true);
        this.baseRanks = costModel.computeUpwardRanks();
        this.jobs = workflow.jobs();
        this.nodes = clusters.stream().flatMap(cluster -> cluster.nodes().stream()).toList();
        this.rankScale = Math.max(1.0, baseRanks.values().stream().mapToDouble(Double::doubleValue).max().orElse(1.0) * 0.30);
        this.jobIndexById = new HashMap<>();
        this.rankedClustersByJob = new HashMap<>();
        for (int i = 0; i < jobs.size(); i++) {
            jobIndexById.put(jobs.get(i).id(), i);
            rankedClustersByJob.put(jobs.get(i).id(), sortedClusters(jobs.get(i)));
        }
    }

    public Solution randomSolution(Random random) {
        double[] priorityBias = new double[jobs.size()];
        double[] clusterBias = new double[jobs.size()];
        double[] energyBias = new double[jobs.size()];
        for (int i = 0; i < jobs.size(); i++) {
            priorityBias[i] = (random.nextDouble() * 2.0) - 1.0;
            clusterBias[i] = random.nextDouble();
            energyBias[i] = random.nextDouble();
        }
        return new Solution(priorityBias, clusterBias, energyBias);
    }

    public Solution baselineSolution() {
        return new Solution(new double[jobs.size()], new double[jobs.size()], new double[jobs.size()]);
    }

    public Evaluation evaluate(Solution solution, String schedulerName) {
        Map<String, Long> nodeAvailable = new HashMap<>();
        Map<String, PlanAssignment> scheduledAssignments = new HashMap<>();
        Set<String> scheduled = new HashSet<>();
        List<PlanAssignment> plan = new ArrayList<>();
        while (scheduled.size() < jobs.size()) {
            List<JobDefinition> readyJobs = jobs.stream()
                    .filter(job -> !scheduled.contains(job.id()))
                    .filter(job -> scheduled.containsAll(job.dependencies()))
                    .sorted(Comparator.comparingInt(job -> workflow.orderOf(job.id())))
                    .toList();
            if (readyJobs.isEmpty()) {
                throw new IllegalStateException("No schedulable jobs remain for workflow " + workflow.workflowId());
            }
            JobDefinition job = readyJobs.stream()
                    .max(Comparator.comparingDouble(candidate -> priorityScore(candidate, solution)))
                    .orElseThrow();
            Candidate best = bestNode(job, solution, nodeAvailable, scheduledAssignments);
            String classification = benchmarks.hasMeasurements(job)
                    ? benchmarks.classification(job)
                    : job.taskType().defaultClassification();
            PlanAssignment assignment = new PlanAssignment(
                    job.id(),
                    best.node.clusterId(),
                    best.node.nodeId(),
                    best.start,
                    best.finish,
                    best.communicationMillis,
                    best.executionMillis,
                    best.energyJoules,
                    baseRanks.get(job.id()),
                    schedulerName,
                    classification);
            scheduled.add(job.id());
            scheduledAssignments.put(job.id(), assignment);
            nodeAvailable.put(best.node.nodeId(), best.finish);
            plan.add(assignment);
        }
        long makespan = plan.stream().mapToLong(PlanAssignment::predictedFinishMillis).max().orElse(0L)
                - plan.stream().mapToLong(PlanAssignment::predictedStartMillis).min().orElse(0L);
        long totalCommunication = plan.stream().mapToLong(PlanAssignment::predictedCommunicationMillis).sum();
        double totalEnergy = plan.stream().mapToDouble(PlanAssignment::predictedEnergyJoules).sum();
        double objective = makespan + (0.020 * totalCommunication) + (0.14 * totalEnergy);
        return new Evaluation(solution.copy(), plan, objective, makespan, totalCommunication, totalEnergy);
    }

    public int size() {
        return jobs.size();
    }

    public double clampPriority(double value) {
        return Math.max(-1.0, Math.min(1.0, value));
    }

    public double clampUnit(double value) {
        return Math.max(0.0, Math.min(1.0, value));
    }

    private double priorityScore(JobDefinition job, Solution solution) {
        int index = jobIndexById.get(job.id());
        return baseRanks.get(job.id()) + (solution.priorityBias[index] * rankScale);
    }

    private Candidate bestNode(
            JobDefinition job,
            Solution solution,
            Map<String, Long> nodeAvailable,
            Map<String, PlanAssignment> scheduledAssignments) {
        int index = jobIndexById.get(job.id());
        List<String> rankedClusters = rankedClustersByJob.get(job.id());
        int preferredIndex = rankedClusters.size() <= 1
                ? 0
                : Math.min(rankedClusters.size() - 1, (int) Math.round(solution.clusterBias[index] * (rankedClusters.size() - 1)));
        String preferredCluster = rankedClusters.get(preferredIndex);
        double energyWeight = 0.08 + (0.30 * solution.energyBias[index]);
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
            int rankedPosition = rankedClusters.indexOf(node.clusterId());
            int clusterDistance = rankedPosition < 0 ? rankedClusters.size() : Math.abs(rankedPosition - preferredIndex);
            double clusterPenalty = clusterDistance * Math.max(10.0, execution * 0.08);
            double score = finish + (energyWeight * energy) + clusterPenalty;
            Candidate candidate = new Candidate(node, start, finish, communication, execution, energy, score);
            if (best == null || candidate.score < best.score
                    || (Double.compare(candidate.score, best.score) == 0 && candidate.finish < best.finish)
                    || (Double.compare(candidate.score, best.score) == 0 && candidate.finish == best.finish
                    && node.clusterId().equals(preferredCluster))) {
                best = candidate;
            }
        }
        return best;
    }

    private List<String> sortedClusters(JobDefinition job) {
        if (benchmarks.hasMeasurements(job)) {
            return benchmarks.sortedClusters(job, clusters);
        }
        return clusters.stream()
                .sorted(Comparator.comparingLong(cluster -> costModel.duration(job, cluster.firstNode())))
                .map(ClusterProfile::clusterId)
                .toList();
    }

    public record Solution(double[] priorityBias, double[] clusterBias, double[] energyBias) {
        public Solution {
            priorityBias = priorityBias.clone();
            clusterBias = clusterBias.clone();
            energyBias = energyBias.clone();
        }

        public Solution copy() {
            return new Solution(priorityBias, clusterBias, energyBias);
        }
    }

    public record Evaluation(
            Solution solution,
            List<PlanAssignment> plan,
            double objective,
            long makespanMillis,
            long totalCommunicationMillis,
            double totalEnergyJoules) {
    }

    private record Candidate(
            NodeProfile node,
            long start,
            long finish,
            long communicationMillis,
            long executionMillis,
            double energyJoules,
            double score) {
    }
}
