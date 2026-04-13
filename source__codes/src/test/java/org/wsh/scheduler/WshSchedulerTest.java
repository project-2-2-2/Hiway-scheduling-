package org.wsh.scheduler;

import org.wsh.model.ClusterProfile;
import org.wsh.model.JobDefinition;
import org.wsh.model.NodeProfile;
import org.wsh.model.PlanAssignment;
import org.wsh.model.TaskType;
import org.wsh.model.WorkflowDefinition;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class WshSchedulerTest {
    @Test
    void paperModeIgnoresCommunicationButCommAwareModeUsesIt() {
        WorkflowDefinition workflow = new WorkflowDefinition(
                "wf",
                "WF",
                List.of(
                        new JobDefinition("root", "Root", List.of(), TaskType.BLAST, 100L, 120_000_000L, "root", "in", "mid", Map.of()),
                        new JobDefinition("child", "Child", List.of("root"), TaskType.CLUSTAL, 100L, 1_024L, "child", "mid", "out", Map.of())));
        List<ClusterProfile> clusters = List.of(
                new ClusterProfile("C1", List.of(new NodeProfile("C1", "c1-n1", 1, 256, 1024))),
                new ClusterProfile("C2", List.of(new NodeProfile("C2", "c2-n1", 1, 256, 1024))));
        TrainingBenchmarks benchmarks = new TrainingBenchmarks(
                Map.of(
                        "root", Map.of("C1", 10L, "C2", 20L),
                        "child", Map.of("C1", 20L, "C2", 10L)),
                Map.of("root", "compute", "child", "compute"),
                0,
                1);

        List<PlanAssignment> paperPlan = new WshScheduler(WshScheduler.Mode.PAPER).buildPlan(workflow, clusters, benchmarks);
        List<PlanAssignment> commAwarePlan = new WshScheduler(WshScheduler.Mode.COMM_AWARE).buildPlan(workflow, clusters, benchmarks);

        assertEquals("C2", assignmentFor("child", paperPlan).clusterId());
        assertEquals("C1", assignmentFor("child", commAwarePlan).clusterId());
    }

    private static PlanAssignment assignmentFor(String jobId, List<PlanAssignment> plan) {
        return plan.stream()
                .filter(assignment -> assignment.jobId().equals(jobId))
                .findFirst()
                .orElseThrow();
    }
}
