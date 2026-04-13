package org.wsh.model;

import java.util.Map;
import java.util.List;

public record JobDefinition(
        String id,
        String displayName,
        List<String> dependencies,
        TaskType taskType,
        long modeledCostMillis,
        long modeledOutputBytes,
        String trainingProfileKey,
        String paperInputHint,
        String paperOutputHint,
        Map<String, String> parameters) {
    public JobDefinition {
        parameters = parameters == null ? Map.of() : Map.copyOf(parameters);
        dependencies = List.copyOf(dependencies);
    }
}
