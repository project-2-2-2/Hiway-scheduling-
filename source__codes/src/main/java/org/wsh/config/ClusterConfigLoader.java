package org.wsh.config;

import org.wsh.model.ClusterProfile;
import org.wsh.model.NodeProfile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ClusterConfigLoader {
    private ClusterConfigLoader() {
    }

    public static List<ClusterProfile> load(Path configPath) throws IOException {
        Map<String, List<NodeProfile>> byCluster = new LinkedHashMap<>();
        for (String rawLine : Files.readAllLines(configPath, StandardCharsets.UTF_8)) {
            String line = rawLine.trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            String[] parts = line.split(",");
            if (parts.length != 5 && parts.length != 6) {
                throw new IllegalArgumentException("Invalid cluster config line: " + line);
            }
            NodeProfile profile = new NodeProfile(
                    parts[0].trim(),
                    parts[1].trim(),
                    Integer.parseInt(parts[2].trim()),
                    Integer.parseInt(parts[3].trim()),
                    Integer.parseInt(parts[4].trim()),
                    parts.length == 6 ? parts[5].trim() : "");
            byCluster.computeIfAbsent(profile.clusterId(), ignored -> new ArrayList<>()).add(profile);
        }
        return byCluster.entrySet().stream()
                .map(entry -> new ClusterProfile(entry.getKey(), List.copyOf(entry.getValue())))
                .toList();
    }
}
