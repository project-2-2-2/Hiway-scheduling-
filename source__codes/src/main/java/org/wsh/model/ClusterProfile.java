package org.wsh.model;

import java.util.List;

public record ClusterProfile(String clusterId, List<NodeProfile> nodes) {
    public NodeProfile firstNode() {
        return nodes.get(0);
    }

    public int maxCpuThreads() {
        return nodes.stream().mapToInt(NodeProfile::cpuThreads).max().orElse(1);
    }
}
