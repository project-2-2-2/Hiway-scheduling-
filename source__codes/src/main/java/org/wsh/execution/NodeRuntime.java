package org.wsh.execution;

import org.wsh.model.NodeProfile;
import org.wsh.model.JobRun;
import org.wsh.model.PlanAssignment;
import org.wsh.task.TaskResult;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public final class NodeRuntime implements AutoCloseable {
    private final NodeProfile nodeProfile;
    private final ExecutorService executorService;

    public NodeRuntime(NodeProfile nodeProfile) {
        this.nodeProfile = nodeProfile;
        this.executorService = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName("node-" + nodeProfile.nodeId());
            return thread;
        });
    }

    public Future<JobRun> submit(PlanAssignment assignment, Callable<TaskResult> taskCallable) {
        Callable<JobRun> callable = () -> {
            long start = System.currentTimeMillis();
            TaskResult result = taskCallable.call();
            long finish = System.currentTimeMillis();
            long duration = Math.max(1L, finish - start);
            return new JobRun(
                    assignment.jobId(),
                    assignment.clusterId(),
                    assignment.nodeId(),
                    assignment.schedulerName(),
                    assignment.predictedStartMillis(),
                    assignment.predictedFinishMillis(),
                    start,
                    finish,
                    duration,
                    result.outputPath(),
                    result.description());
        };
        return executorService.submit(callable);
    }

    @Override
    public void close() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
                executorService.awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException interruptedException) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public NodeProfile nodeProfile() {
        return nodeProfile;
    }
}
