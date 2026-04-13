package org.wsh.workflow;

import org.wsh.cli.CliArguments;

import java.util.Map;

public final class WorkflowRegistry {
    private WorkflowRegistry() {
    }

    public static WorkflowSpec fromCli(CliArguments cli) {
        return byId(cli.option("workflow", "gene2life"), cli.options());
    }

    public static WorkflowSpec byId(String workflowId) {
        return byId(workflowId, Map.of());
    }

    public static WorkflowSpec byId(String workflowId, Map<String, String> options) {
        return switch (workflowId.toLowerCase()) {
            case "gene2life" -> new Gene2LifeWorkflowSpec();
            case "avianflu_small" -> new AvianfluSmallWorkflowSpec(optionInt(options, "avianflu-autodock-count",
                    AvianfluSmallWorkflowSpec.DEFAULT_DOCKING_TASKS));
            case "epigenomics" -> new EpigenomicsWorkflowSpec(optionInt(options, "epigenomics-split-count",
                    EpigenomicsWorkflowSpec.DEFAULT_SPLIT_COUNT));
            default -> throw new IllegalArgumentException("Unsupported workflow: " + workflowId);
        };
    }

    private static int optionInt(Map<String, String> options, String key, int defaultValue) {
        String value = options.get(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Integer.parseInt(value);
    }
}
