package org.wsh.scheduler;

import org.wsh.model.ClusterProfile;
import org.wsh.model.PlanAssignment;
import org.wsh.model.WorkflowDefinition;

import java.util.List;

public interface Scheduler {
    List<PlanAssignment> buildPlan(WorkflowDefinition workflow, List<ClusterProfile> clusters, TrainingBenchmarks benchmarks);

    String name();
}
