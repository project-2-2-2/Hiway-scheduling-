package org.wsh.model;

import java.nio.file.Path;

public record JobRun(
        String jobId,
        String clusterId,
        String nodeId,
        String schedulerName,
        long predictedStartMillis,
        long predictedFinishMillis,
        long actualStartMillis,
        long actualFinishMillis,
        long durationMillis,
        Path outputPath,
        String outputDescription) {
}
