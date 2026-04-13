package org.wsh.execution;

import org.wsh.model.NodeProfile;
import org.wsh.task.TaskInputs;
import org.wsh.task.TaskResult;
import org.wsh.workflow.WorkflowSpec;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class DockerNodePool implements AutoCloseable {
    private final String dockerImage;
    private final Path mountRoot;
    private final String namespace;
    private final String containerUserSpec;
    private final Map<String, NodeProfile> nodesByContainerName;

    public DockerNodePool(String dockerImage, Path mountRoot, String namespace, List<NodeProfile> nodes) throws IOException, InterruptedException {
        this.dockerImage = dockerImage;
        this.mountRoot = mountRoot.toAbsolutePath();
        this.namespace = namespace.toLowerCase().replaceAll("[^a-z0-9]+", "-");
        this.containerUserSpec = detectContainerUserSpec();
        this.nodesByContainerName = new LinkedHashMap<>();
        Files.createDirectories(this.mountRoot);
        for (NodeProfile node : nodes) {
            String containerName = containerName(namespace, node);
            nodesByContainerName.put(containerName, node);
            removeIfExists(containerName);
            startContainer(containerName, node);
        }
    }

    public TaskResult execute(WorkflowSpec workflowSpec, NodeProfile nodeProfile, String jobId, TaskInputs inputs) throws IOException, InterruptedException {
        String containerName = findContainerName(nodeProfile);
        List<String> command = new java.util.ArrayList<>();
        command.add("docker");
        command.add("exec");
        command.add("--user");
        command.add(containerUserSpec);
        command.add(containerName);
        command.add("java");
        command.add("-jar");
        command.add("/app/wsh-app.jar");
        command.add("run-job");
        command.add("--workflow");
        command.add(workflowSpec.workflowId());
        for (Map.Entry<String, String> entry : workflowSpec.variantOptions().entrySet()) {
            command.add("--" + entry.getKey());
            command.add(entry.getValue());
        }
        command.add("--job");
        command.add(jobId);
        command.add("--inputs");
        command.add(inputs.inputs().stream()
                .map(path -> path.toAbsolutePath().toString())
                .collect(java.util.stream.Collectors.joining(",")));
        if (!inputs.parameters().isEmpty()) {
            command.add("--params");
            command.add(inputs.parameters().entrySet().stream()
                    .map(entry -> entry.getKey() + "=" + entry.getValue())
                    .collect(java.util.stream.Collectors.joining(",")));
        }
        command.add("--output-dir");
        command.add(inputs.outputDirectory().toAbsolutePath().toString());
        command.add("--cluster-id");
        command.add(nodeProfile.clusterId());
        command.add("--node-id");
        command.add(nodeProfile.nodeId());
        command.add("--cpu-threads");
        command.add(Integer.toString(nodeProfile.cpuThreads()));
        command.add("--io-buffer-kb");
        command.add(Integer.toString(nodeProfile.ioBufferKb()));
        command.add("--memory-mb");
        command.add(Integer.toString(nodeProfile.memoryMb()));
        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .start();
        byte[] combined = process.getInputStream().readAllBytes();
        int exit = process.waitFor();
        if (exit != 0) {
            throw new IOException("Docker exec failed with exit code " + exit + " for " + jobId
                    + System.lineSeparator() + new String(combined, StandardCharsets.UTF_8));
        }
        Path outputPath = JobOutputs.outputPath(workflowSpec, jobId, inputs.outputDirectory());
        if (!Files.exists(outputPath)) {
            throw new IOException("Expected output not found after docker exec: " + outputPath);
        }
        return new TaskResult(outputPath, JobOutputs.outputDescription(workflowSpec, jobId));
    }

    @Override
    public void close() {
        for (String containerName : nodesByContainerName.keySet()) {
            try {
                removeIfExists(containerName);
            } catch (Exception ignored) {
            }
        }
    }

    private void startContainer(String containerName, NodeProfile nodeProfile) throws IOException, InterruptedException {
        List<String> command = List.of(
                "docker", "run", "-d", "--rm");
        java.util.ArrayList<String> mutableCommand = new java.util.ArrayList<>(command);
        mutableCommand.add("--name");
        mutableCommand.add(containerName);
        mutableCommand.add("--label");
        mutableCommand.add("org.wsh.runtime=docker-node");
        mutableCommand.add("--label");
        mutableCommand.add("org.wsh.namespace=" + namespace);
        mutableCommand.add("--user");
        mutableCommand.add(containerUserSpec);
        mutableCommand.add("--cpus");
        mutableCommand.add(Integer.toString(nodeProfile.cpuThreads()));
        if (nodeProfile.hasDedicatedCpuSet()) {
            mutableCommand.add("--cpuset-cpus");
            mutableCommand.add(nodeProfile.cpuSet());
        }
        mutableCommand.add("--memory");
        mutableCommand.add(nodeProfile.memoryMb() + "m");
        mutableCommand.add("-v");
        mutableCommand.add(mountRoot.toString() + ":" + mountRoot.toString());
        mutableCommand.add("--entrypoint");
        mutableCommand.add("sh");
        mutableCommand.add(dockerImage);
        mutableCommand.add("-lc");
        mutableCommand.add("while true; do sleep 3600; done");
        Process process = new ProcessBuilder(mutableCommand)
                .redirectErrorStream(true)
                .start();
        byte[] combined = process.getInputStream().readAllBytes();
        int exit = process.waitFor();
        if (exit != 0) {
            throw new IOException("Failed to start docker node container " + containerName
                    + System.lineSeparator() + new String(combined, StandardCharsets.UTF_8));
        }
    }

    private void removeIfExists(String containerName) throws IOException, InterruptedException {
        Process process = new ProcessBuilder("docker", "rm", "-f", containerName)
                .redirectErrorStream(true)
                .start();
        process.getInputStream().readAllBytes();
        process.waitFor();
    }

    private String findContainerName(NodeProfile nodeProfile) {
        return nodesByContainerName.entrySet().stream()
                .filter(entry -> entry.getValue().nodeId().equals(nodeProfile.nodeId()))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No container registered for node " + nodeProfile.nodeId()));
    }

    private static String containerName(String namespace, NodeProfile nodeProfile) {
        String safeNamespace = namespace.toLowerCase().replaceAll("[^a-z0-9]+", "-");
        if (safeNamespace.length() > 24) {
            safeNamespace = safeNamespace.substring(0, 24);
        }
        return "g2l-" + safeNamespace + "-" + nodeProfile.nodeId().toLowerCase();
    }

    private static String detectContainerUserSpec() throws IOException, InterruptedException {
        String uid = runAndCapture("id", "-u");
        String gid = runAndCapture("id", "-g");
        return uid + ":" + gid;
    }

    private static String runAndCapture(String... command) throws IOException, InterruptedException {
        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        int exit = process.waitFor();
        if (exit != 0 || output.isEmpty()) {
            throw new IOException("Failed to resolve local user information for Docker execution");
        }
        return output;
    }
}
