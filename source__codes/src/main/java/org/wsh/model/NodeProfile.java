package org.wsh.model;

public record NodeProfile(
        String clusterId,
        String nodeId,
        int cpuThreads,
        int ioBufferKb,
        int memoryMb,
        String cpuSet) {

    public NodeProfile(String clusterId, String nodeId, int cpuThreads, int ioBufferKb, int memoryMb) {
        this(clusterId, nodeId, cpuThreads, ioBufferKb, memoryMb, "");
    }

    public int effectiveReadBufferBytes() {
        return Math.max(8 * 1024, ioBufferKb * 1024);
    }

    public long modeledNetworkBytesPerSecond() {
        return Math.max(1_000_000L, effectiveReadBufferBytes() * 48L);
    }

    public double modeledActivePowerWatts() {
        return 25.0
                + (cpuThreads * 7.5)
                + (memoryMb / 512.0)
                + ((double) ioBufferKb / 256.0) * 3.0;
    }

    public double modeledIdlePowerWatts() {
        return Math.max(12.0, modeledActivePowerWatts() * 0.38);
    }

    public boolean hasDedicatedCpuSet() {
        return cpuSet != null && !cpuSet.isBlank();
    }
}
