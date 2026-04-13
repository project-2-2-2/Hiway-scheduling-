package org.wsh.hadoop;

import org.wsh.execution.DockerNodePool;
import org.wsh.model.NodeProfile;
import org.wsh.task.TaskInputs;
import org.wsh.task.TaskResult;
import org.wsh.workflow.WorkflowSpec;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

public final class HdfsDockerJobRunner {
    private final DockerNodePool dockerNodePool;
    private final HadoopSupport hadoopSupport;

    public HdfsDockerJobRunner(DockerNodePool dockerNodePool, HadoopSupport hadoopSupport) {
        this.dockerNodePool = dockerNodePool;
        this.hadoopSupport = hadoopSupport;
    }

    public void syncDataRoot(Path localDataRoot) throws Exception {
        hadoopSupport.syncLocalDirectoryToHdfs(localDataRoot, hadoopSupport.normalizedDataRoot());
    }

    public TaskResult executeWorkflowJob(
            WorkflowSpec workflowSpec,
            String jobId,
            NodeProfile nodeProfile,
            Path localRunRoot,
            String hdfsRunRoot) throws Exception {
        HadoopTaskInputs inputs = workflowSpec.resolveHadoopInputs(jobId, hadoopSupport.normalizedDataRoot(), hdfsRunRoot);
        Path localOutputDirectory = localRunRoot.resolve("jobs").resolve(jobId);
        Path localStageRoot = localRunRoot.resolve(".hdfs-docker-stage").resolve(jobId);
        return execute(workflowSpec, jobId, nodeProfile, inputs, localOutputDirectory, localStageRoot);
    }

    public TaskResult executeTrainingJob(
            WorkflowSpec workflowSpec,
            String jobId,
            NodeProfile nodeProfile,
            Path localDataRoot) throws Exception {
        HadoopTaskInputs inputs = workflowSpec.resolveHadoopTrainingInputs(jobId, hadoopSupport.normalizedDataRoot());
        Path localOutputDirectory = localDataRoot.resolve("training/generated").resolve(jobId);
        Path localStageRoot = localDataRoot.resolve("training/.hdfs-docker-stage").resolve(jobId);
        return execute(workflowSpec, jobId, nodeProfile, inputs, localOutputDirectory, localStageRoot);
    }

    private TaskResult execute(
            WorkflowSpec workflowSpec,
            String jobId,
            NodeProfile nodeProfile,
            HadoopTaskInputs inputs,
            Path localOutputDirectory,
            Path localStageRoot) throws Exception {
        deleteRecursively(localStageRoot);
        deleteRecursively(localOutputDirectory);
        Files.createDirectories(localStageRoot);
        Files.createDirectories(localOutputDirectory);
        List<Path> localizedInputs = new ArrayList<>();
        try {
            for (int index = 0; index < inputs.inputs().size(); index++) {
                localizedInputs.add(hadoopSupport.localizeInput(inputs.input(index), localStageRoot.resolve("input-" + index)));
            }
            TaskInputs taskInputs = new TaskInputs(localizedInputs, localOutputDirectory, inputs.parameters());
            TaskResult result = dockerNodePool.execute(workflowSpec, nodeProfile, jobId, taskInputs);
            hadoopSupport.copyLocalDirectoryToHdfs(localOutputDirectory, inputs.outputDirectory());
            return result;
        } catch (Exception exception) {
            throw new IOException("HDFS Docker execution failed for "
                    + workflowSpec.workflowId()
                    + "/"
                    + jobId
                    + " on "
                    + nodeProfile.nodeId(), exception);
        } finally {
            deleteRecursively(localStageRoot);
        }
    }

    private static void deleteRecursively(Path path) throws IOException {
        if (path == null || !Files.exists(path)) {
            return;
        }
        try (Stream<Path> stream = Files.walk(path)) {
            stream.sorted(Comparator.reverseOrder()).forEach(candidate -> {
                try {
                    Files.deleteIfExists(candidate);
                } catch (IOException ignored) {
                }
            });
        }
    }
}
