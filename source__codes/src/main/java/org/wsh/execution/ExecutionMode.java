package org.wsh.execution;

public enum ExecutionMode {
    LOCAL,
    DOCKER,
    HADOOP,
    HDFS_DOCKER;

    public static ExecutionMode fromCliValue(String value) {
        return switch (value.toLowerCase()) {
            case "local" -> LOCAL;
            case "docker" -> DOCKER;
            case "hadoop" -> HADOOP;
            case "hdfs-docker" -> HDFS_DOCKER;
            default -> throw new IllegalArgumentException("Unsupported executor: " + value);
        };
    }
}
