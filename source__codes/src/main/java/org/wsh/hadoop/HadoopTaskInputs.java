package org.wsh.hadoop;

import java.util.List;
import java.util.Map;

public record HadoopTaskInputs(List<String> inputs, String outputDirectory, Map<String, String> parameters) {
    public HadoopTaskInputs {
        inputs = List.copyOf(inputs);
        parameters = parameters == null ? Map.of() : Map.copyOf(parameters);
    }

    public String input(int index) {
        return inputs.get(index);
    }
}
