package org.wsh.execution;

import org.wsh.cli.CliArguments;
import org.wsh.hadoop.HadoopTaskInputs;
import org.wsh.model.ClusterProfile;
import org.wsh.model.JobDefinition;
import org.wsh.model.JobRun;
import org.wsh.model.NodeProfile;
import org.wsh.model.TaskType;
import org.wsh.model.WorkflowDefinition;
import org.wsh.scheduler.DurationModel;
import org.wsh.scheduler.TrainingBenchmarks;
import org.wsh.task.TaskExecutor;
import org.wsh.task.TaskInputs;
import org.wsh.task.TaskResult;
import org.wsh.workflow.WorkflowSpec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class TrainingRunnerTest {
    @TempDir
    Path tempDir;

    @Test
    void benchmarkKeepsDefaultClassificationWhenMeasurementsAreTooNoisy() throws Exception {
        WorkflowDefinition workflow = new WorkflowDefinition(
                "wf",
                "WF",
                List.of(new JobDefinition(
                        "job-a",
                        "Job A",
                        List.of(),
                        TaskType.BLAST,
                        400L,
                        1_024L,
                        "blast-profile",
                        "input",
                        "output",
                        Map.of())));
        WorkflowSpec workflowSpec = new TinyWorkflowSpec(workflow);
        List<ClusterProfile> clusters = List.of(
                new ClusterProfile("C1", List.of(new NodeProfile("C1", "n1", 4, 2048, 4096))),
                new ClusterProfile("C2", List.of(new NodeProfile("C2", "n2", 1, 256, 1024))));

        TrainingRunner runner = new TrainingRunner(
                workflowSpec,
                workflowSpec.executors(),
                ExecutionMode.LOCAL,
                null,
                null,
                null,
                0,
                1);

        TrainingBenchmarks benchmarks = runner.benchmark(tempDir, clusters);
        JobDefinition job = workflow.job("job-a");

        assertEquals("compute", benchmarks.classification(job));
        assertEquals(DurationModel.estimateDuration(job, clusters.get(0).firstNode()), benchmarks.duration(job, "C1"));
        assertEquals(DurationModel.estimateDuration(job, clusters.get(1).firstNode()), benchmarks.duration(job, "C2"));
    }

    private static final class TinyWorkflowSpec implements WorkflowSpec {
        private final WorkflowDefinition workflow;

        private TinyWorkflowSpec(WorkflowDefinition workflow) {
            this.workflow = workflow;
        }

        @Override
        public WorkflowDefinition definition() {
            return workflow;
        }

        @Override
        public void generateData(Path dataRoot, CliArguments cli) {
        }

        @Override
        public TaskInputs resolveInputs(String jobId, Path dataRoot, Path runRoot, Map<String, Future<JobRun>> futures) {
            throw new UnsupportedOperationException("Not needed in this test");
        }

        @Override
        public TaskInputs resolveTrainingInputs(String jobId, Path dataRoot) {
            return new TaskInputs(List.of(), dataRoot.resolve("training/generated").resolve(jobId), Map.of());
        }

        @Override
        public HadoopTaskInputs resolveHadoopInputs(String jobId, String dataRoot, String runRoot) {
            throw new UnsupportedOperationException("Not needed in this test");
        }

        @Override
        public HadoopTaskInputs resolveHadoopTrainingInputs(String jobId, String dataRoot) {
            throw new UnsupportedOperationException("Not needed in this test");
        }

        @Override
        public Map<String, TaskExecutor> executors() {
            TaskExecutor executor = (inputs, nodeProfile) -> {
                Files.createDirectories(inputs.outputDirectory());
                Path output = inputs.outputDirectory().resolve(TaskType.BLAST.outputFileName());
                Files.writeString(output, "ok", StandardCharsets.UTF_8);
                return new TaskResult(output, "output");
            };
            return Map.of("job-a", executor);
        }
    }
}
