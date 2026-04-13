package org.wsh.scheduler;

import org.wsh.model.ClusterProfile;
import org.wsh.model.JobDefinition;
import org.wsh.model.NodeProfile;
import org.wsh.model.TaskType;
import org.wsh.model.WorkflowDefinition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class DurationModel {
    private DurationModel() {
    }

    public static long estimateDuration(JobDefinition job, NodeProfile node) {
        double base = job.modeledCostMillis();
        double cpuFactor = Math.max(1.0, node.cpuThreads());
        double ioFactor = Math.max(1.0, node.ioBufferKb() / 256.0);
        TaskType taskType = job.taskType();
        double estimate = switch (taskType) {
            case BLAST, CLUSTAL, DNAPARS, AUTOGRID, AUTODOCK, FASTQ_TO_BFQ, MAP, PILEUP -> base / cpuFactor;
            case DRAWGRAM, PREPARE_GPF, PREPARE_DPF, FASTQ_SPLIT, FILTER_CONTAMS, SOL2SANGER, MAP_MERGE, MAQ_INDEX -> base / ioFactor;
            case PROTPARS, PREPARE_RECEPTOR -> base / ((cpuFactor * 0.6) + (ioFactor * 0.4));
        };
        return Math.max(1L, Math.round(estimate));
    }

    public static long optimisticCriticalPath(WorkflowDefinition workflow, List<ClusterProfile> clusters) {
        Map<String, Long> lowerBounds = new HashMap<>();
        for (JobDefinition job : workflow.jobs()) {
            long minDuration = clusters.stream()
                    .flatMap(cluster -> cluster.nodes().stream())
                    .mapToLong(node -> estimateDuration(job, node))
                    .min()
                    .orElse(0L);
            lowerBounds.put(job.id(), minDuration);
        }
        return criticalPath(workflow, lowerBounds);
    }

    private static long criticalPath(WorkflowDefinition workflow, Map<String, Long> durations) {
        Map<String, Long> cache = new HashMap<>();
        long max = 0L;
        for (JobDefinition job : workflow.jobs()) {
            max = Math.max(max, criticalPath(job.id(), workflow, durations, cache));
        }
        return max;
    }

    private static long criticalPath(
            String jobId,
            WorkflowDefinition workflow,
            Map<String, Long> durations,
            Map<String, Long> cache) {
        if (cache.containsKey(jobId)) {
            return cache.get(jobId);
        }
        long own = durations.getOrDefault(jobId, 0L);
        long successor = workflow.successors(jobId).stream()
                .mapToLong(job -> criticalPath(job.id(), workflow, durations, cache))
                .max()
                .orElse(0L);
        long value = own + successor;
        cache.put(jobId, value);
        return value;
    }
}
