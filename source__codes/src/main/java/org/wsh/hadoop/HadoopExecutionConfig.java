package org.wsh.hadoop;

public record HadoopExecutionConfig(
        String hdfsDataRoot,
        String hdfsWorkspaceRoot,
        String hadoopConfDir,
        String fsDefaultFs,
        String frameworkName,
        String yarnResourceManagerAddress,
        boolean enableNodeLabels) {

    public String normalizedDataRoot() {
        return normalize(hdfsDataRoot);
    }

    public String normalizedWorkspaceRoot() {
        return normalize(hdfsWorkspaceRoot);
    }

    private static String normalize(String value) {
        if (value == null || value.isBlank()) {
            return "/";
        }
        String normalized = value.replaceAll("/{2,}", "/");
        if (!normalized.startsWith("/")) {
            normalized = "/" + normalized;
        }
        if (normalized.length() > 1 && normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }
}
