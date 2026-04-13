package org.wsh.config;

import org.wsh.model.ClusterProfile;
import org.wsh.model.NodeProfile;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ClusterProfiles {
    private ClusterProfiles() {
    }

    public static List<ClusterProfile> limitRoundRobin(List<ClusterProfile> clusters, int maxNodes) {
        if (maxNodes <= 0) {
            return clusters;
        }
        int totalNodes = clusters.stream().mapToInt(cluster -> cluster.nodes().size()).sum();
        if (maxNodes >= totalNodes) {
            return clusters;
        }
        Map<String, List<NodeProfile>> selected = new LinkedHashMap<>();
        for (ClusterProfile cluster : clusters) {
            selected.put(cluster.clusterId(), new ArrayList<>());
        }
        int picked = 0;
        int level = 0;
        while (picked < maxNodes) {
            boolean addedThisLevel = false;
            for (ClusterProfile cluster : clusters) {
                if (level < cluster.nodes().size()) {
                    selected.get(cluster.clusterId()).add(cluster.nodes().get(level));
                    picked++;
                    addedThisLevel = true;
                    if (picked >= maxNodes) {
                        break;
                    }
                }
            }
            if (!addedThisLevel) {
                break;
            }
            level++;
        }
        return selected.entrySet().stream()
                .filter(entry -> !entry.getValue().isEmpty())
                .map(entry -> new ClusterProfile(entry.getKey(), List.copyOf(entry.getValue())))
                .toList();
    }
}
