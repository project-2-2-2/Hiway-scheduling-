package org.wsh.report;

import org.wsh.model.ClusterProfile;
import org.wsh.model.JobRun;
import org.wsh.model.NodeProfile;
import org.wsh.model.PlanAssignment;
import org.wsh.model.WorkflowDefinition;
import org.wsh.scheduler.SchedulingCostModel;
import org.wsh.scheduler.TrainingBenchmarks;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class WorkflowMetrics {
    private WorkflowMetrics() {
    }

    public static RunMetrics summarize(
            WorkflowDefinition workflow,
            List<ClusterProfile> clusters,
            TrainingBenchmarks benchmarks,
            List<PlanAssignment> plan,
            List<JobRun> runs) {
        long makespan = runs.stream().mapToLong(JobRun::actualFinishMillis).max().orElse(0L)
                - runs.stream().mapToLong(JobRun::actualStartMillis).min().orElse(0L);
        long observedRuntime = runs.stream().mapToLong(JobRun::durationMillis).sum();
        SchedulingCostModel costModel = new SchedulingCostModel(workflow, clusters, benchmarks, hasBenchmarks(workflow, benchmarks));
        long singleNodeBaseline = singleNodeBaselineMillis(workflow, costModel, clusters, plan, runs);
        long criticalPathLowerBound = costModel.optimisticCriticalPath();
        long predictedCommunication = plan.stream().mapToLong(PlanAssignment::predictedCommunicationMillis).sum();
        double predictedEnergy = plan.stream().mapToDouble(PlanAssignment::predictedEnergyJoules).sum();
        double speedup = makespan == 0 ? 0.0 : (double) singleNodeBaseline / makespan;
        double slr = criticalPathLowerBound == 0 ? 0.0 : (double) makespan / criticalPathLowerBound;
        return new RunMetrics(
                makespan,
                observedRuntime,
                singleNodeBaseline,
                criticalPathLowerBound,
                predictedCommunication,
                predictedEnergy,
                speedup,
                slr);
    }

    private static boolean hasBenchmarks(WorkflowDefinition workflow, TrainingBenchmarks benchmarks) {
        if (benchmarks == null) {
            return false;
        }
        return workflow.trainingRepresentativeJobs().stream().anyMatch(benchmarks::hasMeasurements);
    }

    private static long singleNodeBaselineMillis(
            WorkflowDefinition workflow,
            SchedulingCostModel costModel,
            List<ClusterProfile> clusters,
            List<PlanAssignment> plan,
            List<JobRun> runs) {
        if (plan.isEmpty() || runs.isEmpty()) {
            return 0L;
        }
        NodeProfile baselineNode = clusters.stream()
                .flatMap(cluster -> cluster.nodes().stream())
                .min(Comparator.comparingLong(node -> workflow.jobs().stream()
                        .mapToLong(job -> costModel.duration(job, node))
                        .sum()))
                .orElse(null);
        if (baselineNode == null) {
            return 0L;
        }
        Map<String, PlanAssignment> planByJob = plan.stream()
                .collect(Collectors.toMap(PlanAssignment::jobId, Function.identity()));
        Map<String, JobRun> runByJob = runs.stream()
                .collect(Collectors.toMap(JobRun::jobId, Function.identity()));
        long total = 0L;
        for (org.wsh.model.JobDefinition job : workflow.jobs()) {
            PlanAssignment assignment = planByJob.get(job.id());
            JobRun run = runByJob.get(job.id());
            if (assignment == null || run == null) {
                continue;
            }
            NodeProfile assignedNode = costModel.nodeById(assignment.nodeId());
            long assignedModel = Math.max(1L, costModel.duration(job, assignedNode));
            long baselineModel = Math.max(1L, costModel.duration(job, baselineNode));
            total += Math.max(1L, Math.round(run.durationMillis() * (baselineModel / (double) assignedModel)));
        }
        return total;
    }

    public record RunMetrics(
            long makespanMillis,
            long observedRuntimeMillis,
            long singleNodeBaselineMillis,
            long criticalPathLowerBoundMillis,
            long predictedCommunicationMillis,
            double predictedEnergyJoules,
            double speedup,
            double slr) {
    }
}
