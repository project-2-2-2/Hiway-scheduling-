package org.wsh.execution;

import org.wsh.workflow.WorkflowSpec;

import java.nio.file.Path;

public final class JobOutputs {
    private JobOutputs() {
    }

    public static Path outputPath(WorkflowSpec workflowSpec, String jobId, Path outputDirectory) {
        return workflowSpec.outputPath(jobId, outputDirectory);
    }

    public static String outputDescription(WorkflowSpec workflowSpec, String jobId) {
        return workflowSpec.outputDescription(jobId);
    }
}
