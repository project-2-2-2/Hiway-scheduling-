package org.wsh.execution;

import org.wsh.hadoop.HadoopJobRunner;
import org.wsh.hadoop.HdfsDockerJobRunner;
import org.wsh.model.ClusterProfile;
import org.wsh.model.JobDefinition;
import org.wsh.model.JobRun;
import org.wsh.model.NodeProfile;
import org.wsh.model.PlanAssignment;
import org.wsh.model.WorkflowDefinition;
import org.wsh.task.TaskExecutor;
import org.wsh.task.TaskResult;
import org.wsh.task.TaskInputs;
import org.wsh.workflow.WorkflowSpec;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

public final class WorkflowExecutor {
    private final WorkflowSpec workflowSpec;
    private final WorkflowDefinition workflow;
    private final Map<String, TaskExecutor> executors;
    private final Map<String, NodeRuntime> runtimes;
    private final ExecutionMode executionMode;
    private final DockerNodePool dockerNodePool;
    private final HadoopJobRunner hadoopJobRunner;
    private final HdfsDockerJobRunner hdfsDockerJobRunner;
    private final String hdfsRunRoot;

    public WorkflowExecutor(
            WorkflowSpec workflowSpec,
            Map<String, TaskExecutor> executors,
            List<ClusterProfile> clusters,
            ExecutionMode executionMode,
            DockerNodePool dockerNodePool,
            HadoopJobRunner hadoopJobRunner,
            HdfsDockerJobRunner hdfsDockerJobRunner,
            String hdfsRunRoot) {
        this.workflowSpec = workflowSpec;
        this.workflow = workflowSpec.definition();
        this.executors = executors;
        this.executionMode = executionMode;
        this.dockerNodePool = dockerNodePool;
        this.hadoopJobRunner = hadoopJobRunner;
        this.hdfsDockerJobRunner = hdfsDockerJobRunner;
        this.hdfsRunRoot = hdfsRunRoot;
        this.runtimes = new HashMap<>();
        for (ClusterProfile cluster : clusters) {
            for (NodeProfile node : cluster.nodes()) {
                runtimes.put(node.nodeId(), new NodeRuntime(node));
            }
        }
    }

    public List<JobRun> execute(Path dataRoot, Path runRoot, List<PlanAssignment> plan) throws Exception {
        Files.createDirectories(runRoot.resolve("jobs"));
        List<PlanAssignment> submissionOrder = new ArrayList<>(plan);
        submissionOrder.sort(Comparator
                .comparingLong(PlanAssignment::predictedStartMillis)
                .thenComparingInt(assignment -> workflow.orderOf(assignment.jobId())));
        Map<String, Future<JobRun>> futures = new HashMap<>();
        Set<String> submitted = new HashSet<>();
        Set<String> completed = new HashSet<>();
        List<JobRun> result = new ArrayList<>();
        while (completed.size() < workflow.jobs().size()) {
            boolean submittedAny = false;
            for (PlanAssignment assignment : submissionOrder) {
                JobDefinition job = workflow.job(assignment.jobId());
                if (submitted.contains(job.id()) || !dependenciesSatisfied(job, completed)) {
                    continue;
                }
                NodeRuntime runtime = runtimes.get(assignment.nodeId());
                futures.put(assignment.jobId(), runtime.submit(
                        assignment,
                        () -> executeAssignment(assignment, dataRoot, runRoot, futures, runtime.nodeProfile())));
                submitted.add(job.id());
                submittedAny = true;
            }

            boolean completedAny = false;
            for (JobDefinition job : workflow.jobs()) {
                if (completed.contains(job.id())) {
                    continue;
                }
                Future<JobRun> future = futures.get(job.id());
                if (future == null || !future.isDone()) {
                    continue;
                }
                result.add(future.get());
                completed.add(job.id());
                completedAny = true;
            }

            if (!submittedAny && !completedAny) {
                if (completed.size() == workflow.jobs().size()) {
                    break;
                }
                Thread.sleep(10L);
            }
        }
        result.sort(Comparator.comparingLong(JobRun::actualStartMillis));
        return result;
    }

    public void close() {
        for (NodeRuntime runtime : runtimes.values()) {
            runtime.close();
        }
    }

    private boolean dependenciesSatisfied(JobDefinition job, Set<String> completed) {
        for (String dependency : job.dependencies()) {
            if (!completed.contains(dependency)) {
                return false;
            }
        }
        return true;
    }

    private TaskResult executeAssignment(
            PlanAssignment assignment,
            Path dataRoot,
            Path runRoot,
            Map<String, Future<JobRun>> futures,
            NodeProfile nodeProfile) throws Exception {
        return switch (executionMode) {
            case LOCAL -> {
                TaskInputs inputs = workflowSpec.resolveInputs(assignment.jobId(), dataRoot, runRoot, futures);
                yield executors.get(assignment.jobId()).execute(inputs, nodeProfile);
            }
            case DOCKER -> {
                TaskInputs inputs = workflowSpec.resolveInputs(assignment.jobId(), dataRoot, runRoot, futures);
                yield dockerNodePool.execute(workflowSpec, nodeProfile, assignment.jobId(), inputs);
            }
            case HADOOP -> {
                if (hadoopJobRunner == null) {
                    throw new IllegalStateException("Hadoop executor selected without Hadoop job runner");
                }
                yield hadoopJobRunner.executeWorkflowJob(
                        workflowSpec,
                        assignment.jobId(),
                        nodeProfile,
                        runRoot,
                        hdfsRunRoot);
            }
            case HDFS_DOCKER -> {
                if (hdfsDockerJobRunner == null) {
                    throw new IllegalStateException("HDFS Docker executor selected without HDFS Docker job runner");
                }
                yield hdfsDockerJobRunner.executeWorkflowJob(
                        workflowSpec,
                        assignment.jobId(),
                        nodeProfile,
                        runRoot,
                        hdfsRunRoot);
            }
        };
    }
}
