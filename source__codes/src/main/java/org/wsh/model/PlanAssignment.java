package org.wsh.model;

public record PlanAssignment(
        String jobId,
        String clusterId,
        String nodeId,
        long predictedStartMillis,
        long predictedFinishMillis,
        long predictedCommunicationMillis,
        long predictedExecutionMillis,
        double predictedEnergyJoules,
        double upwardRank,
        String schedulerName,
        String classification) {
}
