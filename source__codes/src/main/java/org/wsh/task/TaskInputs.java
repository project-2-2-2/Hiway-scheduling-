package org.wsh.task;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public record TaskInputs(List<Path> inputs, Path outputDirectory, Map<String, String> parameters) {
    public TaskInputs {
        inputs = List.copyOf(inputs);
        parameters = parameters == null ? Map.of() : Map.copyOf(parameters);
    }

    public Path input(int index) {
        return inputs.get(index);
    }
}
