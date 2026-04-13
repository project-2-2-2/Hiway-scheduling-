package org.wsh.config;

import org.wsh.model.ClusterProfile;
import org.wsh.model.NodeProfile;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class ClusterProfilesTest {
    @Test
    void limitRoundRobinKeepsRoundRobinPrefixAcrossClusters() {
        List<ClusterProfile> clusters = List.of(
                new ClusterProfile("C1", List.of(
                        new NodeProfile("C1", "c1-n1", 4, 2048, 4096),
                        new NodeProfile("C1", "c1-n2", 4, 2048, 4096),
                        new NodeProfile("C1", "c1-n3", 4, 2048, 4096))),
                new ClusterProfile("C2", List.of(
                        new NodeProfile("C2", "c2-n1", 2, 1024, 2048),
                        new NodeProfile("C2", "c2-n2", 2, 1024, 2048))),
                new ClusterProfile("C3", List.of(
                        new NodeProfile("C3", "c3-n1", 1, 512, 1024))));

        List<ClusterProfile> limited = ClusterProfiles.limitRoundRobin(clusters, 4);

        assertEquals(List.of("c1-n1", "c1-n2"), limited.get(0).nodes().stream().map(NodeProfile::nodeId).toList());
        assertEquals(List.of("c2-n1"), limited.get(1).nodes().stream().map(NodeProfile::nodeId).toList());
        assertEquals(List.of("c3-n1"), limited.get(2).nodes().stream().map(NodeProfile::nodeId).toList());
    }
}
