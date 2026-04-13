package org.wsh.report;

import org.wsh.model.ClusterProfile;
import org.wsh.model.JobDefinition;
import org.wsh.model.JobRun;
import org.wsh.model.NodeProfile;
import org.wsh.model.PlanAssignment;
import org.wsh.model.TaskType;
import org.wsh.model.WorkflowDefinition;
import org.wsh.scheduler.TrainingBenchmarks;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class WorkflowMetricsTest {
    @Test
    void summarizeUsesReferenceSingleNodeBaselineEstimate() {
        WorkflowDefinition workflow = new WorkflowDefinition(
                "wf",
                "WF",
                List.of(new JobDefinition(
                        "job-a",
                        "Job A",
                        List.of(),
                        TaskType.BLAST,
                        200L,
                        1_024L,
                        "job-a",
                        "input",
                        "output",
                        Map.of())));
        List<ClusterProfile> clusters = List.of(
                new ClusterProfile("C1", List.of(new NodeProfile("C1", "n1", 4, 2048, 4096))),
                new ClusterProfile("C2", List.of(new NodeProfile("C2", "n2", 1, 256, 1024))));
        List<PlanAssignment> plan = List.of(new PlanAssignment(
                "job-a",
                "C2",
                "n2",
                0L,
                200L,
                0L,
                200L,
                10.0,
                1.0,
                "HEFT",
                "compute"));
        List<JobRun> runs = List.of(new JobRun(
                "job-a",
                "C2",
                "n2",
                "HEFT",
                0L,
                200L,
                1_000L,
                1_200L,
                200L,
                Path.of("out.tsv"),
                "output"));

        WorkflowMetrics.RunMetrics metrics = WorkflowMetrics.summarize(workflow, clusters, TrainingBenchmarks.empty(), plan, runs);

        assertEquals(200L, metrics.makespanMillis());
        assertEquals(200L, metrics.observedRuntimeMillis());
        assertEquals(50L, metrics.singleNodeBaselineMillis());
        assertEquals(50L, metrics.criticalPathLowerBoundMillis());
        assertEquals(0.25, metrics.speedup(), 0.0001);
        assertEquals(4.0, metrics.slr(), 0.0001);
    }
}
