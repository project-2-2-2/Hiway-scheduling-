package org.wsh.cli;

import org.wsh.config.ClusterConfigLoader;
import org.wsh.config.ClusterProfiles;
import org.wsh.execution.DockerNodePool;
import org.wsh.execution.ExecutionMode;
import org.wsh.execution.JobOutputs;
import org.wsh.execution.TrainingRunner;
import org.wsh.execution.WorkflowExecutor;
import org.wsh.hadoop.HadoopExecutionConfig;
import org.wsh.hadoop.HadoopJobRunner;
import org.wsh.hadoop.HadoopSupport;
import org.wsh.hadoop.HdfsDockerJobRunner;
import org.wsh.model.ClusterProfile;
import org.wsh.model.JobRun;
import org.wsh.model.NodeProfile;
import org.wsh.model.PlanAssignment;
import org.wsh.model.WorkflowDefinition;
import org.wsh.report.ReportWriter;
import org.wsh.report.WorkflowMetrics;
import org.wsh.report.WorkflowMetrics.RunMetrics;
import org.wsh.scheduler.EnergyAwareScheduler;
import org.wsh.scheduler.GeneticAlgorithmScheduler;
import org.wsh.scheduler.HeftScheduler;
import org.wsh.scheduler.PsoScheduler;
import org.wsh.scheduler.Scheduler;
import org.wsh.scheduler.TrainingBenchmarks;
import org.wsh.scheduler.WshScheduler;
import org.wsh.task.TaskExecutor;
import org.wsh.task.TaskInputs;
import org.wsh.workflow.WorkflowRegistry;
import org.wsh.workflow.WorkflowSpec;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class Main {
    private Main() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || isHelpCommand(args[0])) {
            printUsage(null);
            return;
        }
        CliArguments cli = CliArguments.parse(args);
        if (cli.optionBoolean("help", false)) {
            printUsage(cli.command());
            return;
        }
        switch (cli.command()) {
            case "generate-data" -> generateData(cli);
            case "run" -> runScheduler(cli);
            case "compare" -> compare(cli);
            case "run-job" -> runJob(cli);
            default -> {
                printUsage(null);
                throw new IllegalArgumentException("Unknown command: " + cli.command());
            }
        }
    }

    private static boolean isHelpCommand(String value) {
        return value.equals("--help") || value.equals("-h") || value.equalsIgnoreCase("help");
    }

    private static void printUsage(String command) {
        if (command == null || command.isBlank()) {
            System.out.println("""
                    Usage:
                      generate-data --workflow <id> [options]
                      run --workflow <id> [options]
                      compare --workflow <id> [options]
                      run-job --workflow <id> --job <id> [options]

                    Commands:
                      generate-data   Generate workflow-shaped input data.
                      run             Run one scheduler.
                      compare         Run one or more scheduler comparisons.
                      run-job         Execute a single workflow task.

                    Common options:
                      --workflow <gene2life|avianflu_small|epigenomics>
                      --workspace <path>
                      --data-root <path>
                      --cluster-config <path>
                      --max-nodes <n>
                      --executor <local|docker|hadoop|hdfs-docker>
                      --schedulers <wsh,heft,pso,ga,energy-aware>
                      --wsh-mode <paper|comm-aware>
                      --help
                    """);
            return;
        }
        switch (command) {
            case "generate-data" -> System.out.println(
                    "Usage: generate-data --workflow <id> --workspace <path> --data-root <path> [workflow-specific options]");
            case "run" -> System.out.println(
                    "Usage: run --workflow <id> --scheduler <wsh|heft|pso|ga|energy-aware> --executor <local|docker|hadoop|hdfs-docker> [--wsh-mode paper|comm-aware] [options]");
            case "compare" -> System.out.println(
                    "Usage: compare --workflow <id> --executor <local|docker|hadoop|hdfs-docker> --rounds <n> [--schedulers wsh,heft,pso,ga,energy-aware] [--wsh-mode paper|comm-aware]");
            case "run-job" -> System.out.println(
                    "Usage: run-job --workflow <id> --job <id> --output-dir <path> [options]");
            default -> printUsage(null);
        }
    }

    private static void generateData(CliArguments cli) throws Exception {
        WorkflowSpec workflowSpec = workflowSpec(cli);
        Path workspace = Path.of(cli.option("workspace", "work/demo"));
        Path dataRoot = Path.of(cli.option("data-root", workspace.resolve("data").toString()));
        Files.createDirectories(dataRoot);
        workflowSpec.generateData(dataRoot, cli);
        System.out.println("Generated " + workflowSpec.workflowId() + " data under " + dataRoot.toAbsolutePath());
    }

    private static void runScheduler(CliArguments cli) throws Exception {
        WorkflowSpec workflowSpec = workflowSpec(cli);
        Path workspace = Path.of(cli.option("workspace", "work/run"));
        runSchedulerInternal(
                workflowSpec,
                workspace,
                Path.of(cli.option("data-root", workspace.resolve("data").toString())),
                Path.of(cli.option("cluster-config", "config/clusters-server.csv")),
                cli,
                cli.option("scheduler", "wsh"),
                cli.optionInt("max-nodes", 0),
                ExecutionMode.fromCliValue(cli.option("executor", "local")),
                cli.option("docker-image", "wsh-java:latest"),
                cli.option("hdfs-data-root", ""),
                cli.option("hdfs-work-root", ""),
                cli.option("hadoop-conf-dir", System.getenv().getOrDefault("HADOOP_CONF_DIR", "")),
                cli.option("hadoop-fs-default", System.getenv().getOrDefault("HADOOP_FS_DEFAULT", "")),
                cli.option("hadoop-framework-name", System.getenv().getOrDefault("HADOOP_FRAMEWORK_NAME", "yarn")),
                cli.option("hadoop-yarn-rm", System.getenv().getOrDefault("HADOOP_YARN_RM", "")),
                cli.optionBoolean("hadoop-enable-node-labels",
                        Boolean.parseBoolean(System.getenv().getOrDefault("HADOOP_ENABLE_NODE_LABELS", "false"))),
                cli.optionInt("training-warmup-runs", 1),
                cli.optionInt("training-measure-runs", 3),
                null);
    }

    private static RunOutcome runSchedulerInternal(
            WorkflowSpec workflowSpec,
            Path workspace,
            Path dataRoot,
            Path clusterConfig,
            CliArguments cli,
            String schedulerName,
            int maxNodes,
            ExecutionMode executionMode,
            String dockerImage,
            String hdfsDataRoot,
            String hdfsWorkRoot,
            String hadoopConfDir,
            String hadoopFsDefault,
            String hadoopFrameworkName,
            String hadoopYarnRm,
            boolean hadoopEnableNodeLabels,
            int trainingWarmupRuns,
            int trainingMeasureRuns,
            TrainingBenchmarks sharedBenchmarks) throws Exception {
        WorkflowDefinition workflow = workflowSpec.definition();
        validateDataRoot(workflowSpec, dataRoot, sharedBenchmarks != null || requiresTraining(schedulerName));
        Map<String, TaskExecutor> executors = workflowSpec.executors();
        List<ClusterProfile> clusters = ClusterProfiles.limitRoundRobin(ClusterConfigLoader.load(clusterConfig), maxNodes);
        DockerNodePool dockerNodePool = executionMode == ExecutionMode.DOCKER
                || executionMode == ExecutionMode.HDFS_DOCKER
                ? new DockerNodePool(
                dockerImage,
                commonAncestor(workspace.toAbsolutePath(), dataRoot.toAbsolutePath()),
                workflow.workflowId() + "-" + schedulerName + "-" + workspace.getFileName(),
                clusters.stream().flatMap(cluster -> cluster.nodes().stream()).toList())
                : null;
        Path runRoot = workspace.resolve(schedulerName.toLowerCase());
        HadoopExecutionConfig hadoopExecutionConfig = executionMode == ExecutionMode.HADOOP
                || executionMode == ExecutionMode.HDFS_DOCKER
                ? buildHadoopExecutionConfig(
                workflowSpec,
                workspace,
                hdfsDataRoot,
                hdfsWorkRoot,
                hadoopConfDir,
                hadoopFsDefault,
                hadoopFrameworkName,
                hadoopYarnRm,
                hadoopEnableNodeLabels)
                : null;
        HadoopJobRunner hadoopJobRunner = null;
        HdfsDockerJobRunner hdfsDockerJobRunner = null;
        if (hadoopExecutionConfig != null) {
            System.out.println("Hadoop execution config:"
                    + " confDir=" + hadoopExecutionConfig.hadoopConfDir()
                    + " fsDefault=" + hadoopExecutionConfig.fsDefaultFs()
                    + " yarnRm=" + hadoopExecutionConfig.yarnResourceManagerAddress()
                    + " dataRoot=" + hadoopExecutionConfig.normalizedDataRoot()
                    + " workspaceRoot=" + hadoopExecutionConfig.normalizedWorkspaceRoot());
            HadoopSupport hadoopSupport = new HadoopSupport(hadoopExecutionConfig);
            if (executionMode == ExecutionMode.HADOOP) {
                hadoopJobRunner = new HadoopJobRunner(hadoopSupport);
                hadoopJobRunner.syncDataRoot(dataRoot);
            } else {
                hdfsDockerJobRunner = new HdfsDockerJobRunner(dockerNodePool, hadoopSupport);
                hdfsDockerJobRunner.syncDataRoot(dataRoot);
            }
        }
        try {
            Scheduler scheduler = scheduler(schedulerName, cli);
            TrainingBenchmarks benchmarks = sharedBenchmarks != null
                    ? sharedBenchmarks
                    : (requiresTraining(schedulerName)
                    ? new TrainingRunner(
                    workflowSpec,
                    executors,
                    executionMode,
                    dockerNodePool,
                    hadoopJobRunner,
                    hdfsDockerJobRunner,
                    trainingWarmupRuns,
                    trainingMeasureRuns).benchmark(dataRoot, clusters)
                    : TrainingBenchmarks.empty());
            List<PlanAssignment> plan = scheduler.buildPlan(workflow, clusters, benchmarks);
            WorkflowExecutor executor = new WorkflowExecutor(
                    workflowSpec,
                    executors,
                    clusters,
                    executionMode,
                    dockerNodePool,
                    hadoopJobRunner,
                    hdfsDockerJobRunner,
                    hadoopExecutionConfig == null
                            ? ""
                            : hadoopExecutionConfig.normalizedWorkspaceRoot() + "/" + scheduler.name().toLowerCase());
            List<JobRun> runs = executor.execute(dataRoot, runRoot, plan);
            try {
                new ReportWriter().writeRunReport(runRoot, workflow, clusters, scheduler.name(), benchmarks, plan, runs);
                System.out.println("Completed " + scheduler.name() + " " + workflow.workflowId() + " run under "
                        + runRoot.toAbsolutePath());
                return new RunOutcome(scheduler.name(), runRoot, benchmarks, plan, runs);
            } finally {
                executor.close();
            }
        } finally {
            if (dockerNodePool != null) {
                dockerNodePool.close();
            }
        }
    }

    private static void compare(CliArguments cli) throws Exception {
        WorkflowSpec workflowSpec = workflowSpec(cli);
        Path workspace = Path.of(cli.option("workspace", "work/compare"));
        Path dataRoot = Path.of(cli.option("data-root", workspace.resolve("data").toString()));
        Path clusterConfig = Path.of(cli.option("cluster-config", "config/clusters-server.csv"));
        int rounds = cli.optionInt("rounds", 3);
        int maxNodes = cli.optionInt("max-nodes", 0);
        ExecutionMode executionMode = ExecutionMode.fromCliValue(cli.option("executor", "local"));
        String dockerImage = cli.option("docker-image", "wsh-java:latest");
        String hdfsDataRoot = cli.option("hdfs-data-root", "");
        String hdfsWorkRoot = cli.option("hdfs-work-root", "");
        String hadoopConfDir = cli.option("hadoop-conf-dir", System.getenv().getOrDefault("HADOOP_CONF_DIR", ""));
        String hadoopFsDefault = cli.option("hadoop-fs-default", System.getenv().getOrDefault("HADOOP_FS_DEFAULT", ""));
        String hadoopFrameworkName = cli.option("hadoop-framework-name", System.getenv().getOrDefault("HADOOP_FRAMEWORK_NAME", "yarn"));
        String hadoopYarnRm = cli.option("hadoop-yarn-rm", System.getenv().getOrDefault("HADOOP_YARN_RM", ""));
        boolean hadoopEnableNodeLabels = cli.optionBoolean(
                "hadoop-enable-node-labels",
                Boolean.parseBoolean(System.getenv().getOrDefault("HADOOP_ENABLE_NODE_LABELS", "false")));
        int trainingWarmupRuns = cli.optionInt("training-warmup-runs", 1);
        int trainingMeasureRuns = cli.optionInt("training-measure-runs", 3);
        List<ClusterProfile> clusters = ClusterProfiles.limitRoundRobin(ClusterConfigLoader.load(clusterConfig), maxNodes);
        List<String> schedulerNames = parseCsvOption(cli.option("schedulers", "wsh,heft"));
        List<RoundOutcome> roundOutcomes = new ArrayList<>();
        for (int round = 1; round <= rounds; round++) {
            Path roundWorkspace = workspace.resolve(String.format("round-%02d", round));
            List<String> order = orderedSchedulersForRound(schedulerNames, round);
            TrainingBenchmarks sharedBenchmarks = requiresSharedBenchmarks(order)
                    ? benchmarkTraining(
                    workflowSpec,
                    roundWorkspace.resolve("_shared-training"),
                    dataRoot,
                    clusters,
                    executionMode,
                    dockerImage,
                    hdfsDataRoot,
                    hdfsWorkRoot,
                    hadoopConfDir,
                    hadoopFsDefault,
                    hadoopFrameworkName,
                    hadoopYarnRm,
                    hadoopEnableNodeLabels,
                    trainingWarmupRuns,
                    trainingMeasureRuns)
                    : TrainingBenchmarks.empty();
            List<RunOutcome> outcomes = new ArrayList<>();
            for (String schedulerName : order) {
                outcomes.add(runSchedulerInternal(
                        workflowSpec,
                        roundWorkspace,
                        dataRoot,
                        clusterConfig,
                        cli,
                        schedulerName,
                        maxNodes,
                        executionMode,
                        dockerImage,
                        hdfsDataRoot,
                        hdfsWorkRoot,
                        hadoopConfDir,
                        hadoopFsDefault,
                        hadoopFrameworkName,
                        hadoopYarnRm,
                        hadoopEnableNodeLabels,
                        trainingWarmupRuns,
                        trainingMeasureRuns,
                        sharedBenchmarks));
            }
            roundOutcomes.add(new RoundOutcome(round, String.join("->", order), outcomes));
        }
        writeComparisonReport(workspace.resolve("comparison.md"), workflowSpec.definition(), clusters, roundOutcomes);
        System.out.println("Comparison runs completed under " + workspace.toAbsolutePath());
    }

    private static void runJob(CliArguments cli) throws Exception {
        WorkflowSpec workflowSpec = workflowSpec(cli);
        String jobId = cli.option("job", "");
        Path outputDir = Path.of(cli.option("output-dir", ""));
        Files.createDirectories(outputDir);
        TaskExecutor executor = workflowSpec.executors().get(jobId);
        if (executor == null) {
            throw new IllegalArgumentException("Unknown job for workflow " + workflowSpec.workflowId() + ": " + jobId);
        }
        TaskInputs inputs = new TaskInputs(parsePathList(cli.option("inputs", "")), outputDir, parseParams(cli.option("params", "")));
        NodeProfile nodeProfile = new NodeProfile(
                cli.option("cluster-id", "docker-cluster"),
                cli.option("node-id", "docker-node"),
                cli.optionInt("cpu-threads", 1),
                cli.optionInt("io-buffer-kb", 256),
                cli.optionInt("memory-mb", 1024));
        executor.execute(inputs, nodeProfile);
        System.out.println("Completed job " + jobId + " -> " + JobOutputs.outputPath(workflowSpec, jobId, outputDir));
    }

    private static WorkflowSpec workflowSpec(CliArguments cli) {
        return WorkflowRegistry.fromCli(cli);
    }

    private static Scheduler scheduler(String name, CliArguments cli) {
        return switch (name.toLowerCase()) {
            case "heft" -> new HeftScheduler();
            case "wsh" -> new WshScheduler(WshScheduler.Mode.fromCliValue(cli.option("wsh-mode", "paper")));
            case "pso" -> new PsoScheduler(
                    cli.optionInt("pso-particles", 18),
                    cli.optionInt("pso-iterations", 28),
                    Long.parseLong(cli.option("scheduler-seed", "42")));
            case "ga", "genetic", "genetic-algorithm" -> new GeneticAlgorithmScheduler(
                    cli.optionInt("ga-population", 26),
                    cli.optionInt("ga-generations", 32),
                    Long.parseLong(cli.option("scheduler-seed", "84")));
            case "energy-aware", "green", "eco" -> new EnergyAwareScheduler(
                    Double.parseDouble(cli.option("energy-weight", "0.22")));
            default -> throw new IllegalArgumentException("Unsupported scheduler: " + name);
        };
    }

    private static void writeComparisonReport(
            Path output,
            WorkflowDefinition workflow,
            List<ClusterProfile> clusters,
            List<RoundOutcome> rounds) throws Exception {
        Map<String, List<RunOutcome>> outcomesByScheduler = new LinkedHashMap<>();
        for (RoundOutcome round : rounds) {
            for (RunOutcome outcome : round.outcomes) {
                outcomesByScheduler.computeIfAbsent(outcome.schedulerName, ignored -> new ArrayList<>()).add(outcome);
            }
        }
        Map<String, RunMetrics> averages = new LinkedHashMap<>();
        for (Map.Entry<String, List<RunOutcome>> entry : outcomesByScheduler.entrySet()) {
            List<RunMetrics> metrics = entry.getValue().stream()
                    .map(outcome -> WorkflowMetrics.summarize(workflow, clusters, outcome.benchmarks, outcome.plan, outcome.runs))
                    .toList();
            averages.put(entry.getKey(), averageMetrics(metrics));
        }
        StringBuilder body = new StringBuilder();
        body.append("# Scheduler Comparison\n\n");
        body.append("## Workflow\n\n");
        body.append("- Workflow: ").append(workflow.displayName()).append(" (`").append(workflow.workflowId()).append("`)\n\n");
        body.append("## Aggregate\n\n");
        body.append("- Rounds: ").append(rounds.size()).append('\n');
        for (Map.Entry<String, RunMetrics> entry : averages.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .toList()) {
            RunMetrics metrics = entry.getValue();
            body.append("- ").append(entry.getKey()).append(" average makespan: ").append(metrics.makespanMillis()).append(" ms\n");
            body.append("- ").append(entry.getKey()).append(" average predicted communication: ").append(metrics.predictedCommunicationMillis()).append(" ms\n");
            body.append("- ").append(entry.getKey()).append(" average predicted energy: ").append(String.format("%.2f", metrics.predictedEnergyJoules())).append(" J\n");
            body.append("- ").append(entry.getKey()).append(" average speedup: ").append(String.format("%.4f", metrics.speedup())).append('\n');
            body.append("- ").append(entry.getKey()).append(" average scheduling length ratio: ").append(String.format("%.4f", metrics.slr())).append('\n');
        }
        if (averages.containsKey("WSH") && averages.containsKey("HEFT")) {
            double makespanImprovement = percentageImprovement(averages.get("HEFT").makespanMillis(), averages.get("WSH").makespanMillis());
            body.append("- WSH makespan improvement over HEFT: ").append(String.format("%.2f%%", makespanImprovement)).append('\n');
        }
        body.append('\n');
        body.append("## Rounds\n\n");
        for (RoundOutcome round : rounds) {
            body.append("### Round ").append(round.roundNumber).append(" (").append(round.order).append(")\n\n");
            for (RunOutcome outcome : round.outcomes.stream()
                    .sorted(Comparator.comparing(item -> item.schedulerName))
                    .toList()) {
                RunMetrics metrics = WorkflowMetrics.summarize(workflow, clusters, outcome.benchmarks, outcome.plan, outcome.runs);
                body.append("- ").append(outcome.schedulerName).append(" makespan: ").append(metrics.makespanMillis()).append(" ms\n");
                body.append("- ").append(outcome.schedulerName).append(" predicted communication: ").append(metrics.predictedCommunicationMillis()).append(" ms\n");
                body.append("- ").append(outcome.schedulerName).append(" predicted energy: ").append(String.format("%.2f", metrics.predictedEnergyJoules())).append(" J\n");
                body.append("- ").append(outcome.schedulerName).append(" run directory: ").append(outcome.runRoot.toAbsolutePath()).append('\n');
            }
            body.append('\n');
        }
        Files.writeString(output, body, StandardCharsets.UTF_8);
    }

    private static double percentageImprovement(long baseline, long candidate) {
        if (baseline <= 0) {
            return 0.0;
        }
        return ((double) baseline - candidate) * 100.0 / baseline;
    }

    private static HadoopExecutionConfig buildHadoopExecutionConfig(
            WorkflowSpec workflowSpec,
            Path workspace,
            String hdfsDataRoot,
            String hdfsWorkRoot,
            String hadoopConfDir,
            String hadoopFsDefault,
            String hadoopFrameworkName,
            String hadoopYarnRm,
            boolean hadoopEnableNodeLabels) {
        String currentUser = System.getProperty("user.name", "wsh");
        String defaultRoot = "/user/" + currentUser + "/wsh";
        String dataRoot = hdfsDataRoot == null || hdfsDataRoot.isBlank()
                ? defaultRoot + "/data/" + workflowSpec.workflowId()
                : workflowSpec.normalizeHdfsPath(hdfsDataRoot);
        String workspaceRoot = hdfsWorkRoot == null || hdfsWorkRoot.isBlank()
                ? defaultRoot + "/work/" + workflowSpec.workflowId() + "/" + sanitizeWorkspace(workspace)
                : workflowSpec.normalizeHdfsPath(hdfsWorkRoot) + "/" + sanitizeWorkspace(workspace);
        return new HadoopExecutionConfig(
                dataRoot,
                workspaceRoot,
                hadoopConfDir,
                hadoopFsDefault,
                hadoopFrameworkName,
                hadoopYarnRm,
                hadoopEnableNodeLabels);
    }

    private static String sanitizeWorkspace(Path workspace) {
        String normalized = workspace.toAbsolutePath().normalize().toString().replace('\\', '/');
        normalized = normalized.replaceAll("[^A-Za-z0-9/_-]", "-");
        normalized = normalized.replaceAll("/+", "/");
        while (normalized.startsWith("/")) {
            normalized = normalized.substring(1);
        }
        return normalized.isBlank() ? "default" : normalized;
    }

    private static Path commonAncestor(Path left, Path right) {
        Path normalizedLeft = left.normalize();
        Path normalizedRight = right.normalize();
        Path candidate = normalizedLeft;
        while (candidate != null && !normalizedRight.startsWith(candidate)) {
            candidate = candidate.getParent();
        }
        if (candidate == null) {
            throw new IllegalArgumentException("Unable to determine common ancestor for " + left + " and " + right);
        }
        return candidate;
    }

    private static RunMetrics averageMetrics(List<RunMetrics> values) {
        double makespan = values.stream().mapToLong(RunMetrics::makespanMillis).average().orElse(0.0);
        double observed = values.stream().mapToLong(RunMetrics::observedRuntimeMillis).average().orElse(0.0);
        double baseline = values.stream().mapToLong(RunMetrics::singleNodeBaselineMillis).average().orElse(0.0);
        double criticalPath = values.stream().mapToLong(RunMetrics::criticalPathLowerBoundMillis).average().orElse(0.0);
        double predictedCommunication = values.stream().mapToLong(RunMetrics::predictedCommunicationMillis).average().orElse(0.0);
        double predictedEnergy = values.stream().mapToDouble(RunMetrics::predictedEnergyJoules).average().orElse(0.0);
        double speedup = values.stream().mapToDouble(RunMetrics::speedup).average().orElse(0.0);
        double slr = values.stream().mapToDouble(RunMetrics::slr).average().orElse(0.0);
        return new RunMetrics(
                Math.round(makespan),
                Math.round(observed),
                Math.round(baseline),
                Math.round(criticalPath),
                Math.round(predictedCommunication),
                predictedEnergy,
                speedup,
                slr);
    }

    private static boolean requiresTraining(String schedulerName) {
        return switch (schedulerName.toLowerCase()) {
            case "wsh", "pso", "ga", "genetic", "genetic-algorithm", "energy-aware", "green", "eco" -> true;
            default -> false;
        };
    }

    private static boolean requiresSharedBenchmarks(List<String> schedulerNames) {
        return schedulerNames.stream().anyMatch(Main::requiresTraining);
    }

    private static List<String> parseCsvOption(String raw) {
        List<String> values = new ArrayList<>();
        for (String token : raw.split(",")) {
            String trimmed = token.trim();
            if (!trimmed.isEmpty()) {
                values.add(trimmed);
            }
        }
        if (values.isEmpty()) {
            throw new IllegalArgumentException("Expected at least one scheduler name.");
        }
        return List.copyOf(values);
    }

    private static List<String> orderedSchedulersForRound(List<String> schedulerNames, int round) {
        if (round % 2 == 1) {
            return schedulerNames;
        }
        List<String> reversed = new ArrayList<>(schedulerNames);
        java.util.Collections.reverse(reversed);
        return List.copyOf(reversed);
    }

    private static List<Path> parsePathList(String raw) {
        if (raw == null || raw.isBlank()) {
            return List.of();
        }
        List<Path> paths = new ArrayList<>();
        for (String value : raw.split(",")) {
            if (!value.isBlank()) {
                paths.add(Path.of(value));
            }
        }
        return List.copyOf(paths);
    }

    private static Map<String, String> parseParams(String raw) {
        if (raw == null || raw.isBlank()) {
            return Map.of();
        }
        Map<String, String> params = new LinkedHashMap<>();
        for (String token : raw.split(",")) {
            if (token.isBlank()) {
                continue;
            }
            int separator = token.indexOf('=');
            if (separator <= 0) {
                throw new IllegalArgumentException("Invalid parameter token: " + token);
            }
            params.put(token.substring(0, separator), token.substring(separator + 1));
        }
        return Map.copyOf(params);
    }

    private static TrainingBenchmarks benchmarkTraining(
            WorkflowSpec workflowSpec,
            Path workspace,
            Path dataRoot,
            List<ClusterProfile> clusters,
            ExecutionMode executionMode,
            String dockerImage,
            String hdfsDataRoot,
            String hdfsWorkRoot,
            String hadoopConfDir,
            String hadoopFsDefault,
            String hadoopFrameworkName,
            String hadoopYarnRm,
            boolean hadoopEnableNodeLabels,
            int trainingWarmupRuns,
            int trainingMeasureRuns) throws Exception {
        validateDataRoot(workflowSpec, dataRoot, true);
        Map<String, TaskExecutor> executors = workflowSpec.executors();
        DockerNodePool dockerNodePool = executionMode == ExecutionMode.DOCKER
                || executionMode == ExecutionMode.HDFS_DOCKER
                ? new DockerNodePool(
                dockerImage,
                commonAncestor(workspace.toAbsolutePath(), dataRoot.toAbsolutePath()),
                workflowSpec.workflowId() + "-benchmarks-" + workspace.getFileName(),
                clusters.stream().flatMap(cluster -> cluster.nodes().stream()).toList())
                : null;
        HadoopExecutionConfig hadoopExecutionConfig = executionMode == ExecutionMode.HADOOP
                || executionMode == ExecutionMode.HDFS_DOCKER
                ? buildHadoopExecutionConfig(
                workflowSpec,
                workspace,
                hdfsDataRoot,
                hdfsWorkRoot,
                hadoopConfDir,
                hadoopFsDefault,
                hadoopFrameworkName,
                hadoopYarnRm,
                hadoopEnableNodeLabels)
                : null;
        HadoopJobRunner hadoopJobRunner = null;
        HdfsDockerJobRunner hdfsDockerJobRunner = null;
        if (hadoopExecutionConfig != null) {
            HadoopSupport hadoopSupport = new HadoopSupport(hadoopExecutionConfig);
            if (executionMode == ExecutionMode.HADOOP) {
                hadoopJobRunner = new HadoopJobRunner(hadoopSupport);
                hadoopJobRunner.syncDataRoot(dataRoot);
            } else {
                hdfsDockerJobRunner = new HdfsDockerJobRunner(dockerNodePool, hadoopSupport);
                hdfsDockerJobRunner.syncDataRoot(dataRoot);
            }
        }
        try {
            return new TrainingRunner(
                    workflowSpec,
                    executors,
                    executionMode,
                    dockerNodePool,
                    hadoopJobRunner,
                    hdfsDockerJobRunner,
                    trainingWarmupRuns,
                    trainingMeasureRuns).benchmark(dataRoot, clusters);
        } finally {
            if (dockerNodePool != null) {
                dockerNodePool.close();
            }
        }
    }

    private static void validateDataRoot(WorkflowSpec workflowSpec, Path dataRoot, boolean requireTrainingInputs) throws Exception {
        workflowSpec.validateDataRoot(dataRoot, requireTrainingInputs);
    }

    private record RunOutcome(
            String schedulerName,
            Path runRoot,
            TrainingBenchmarks benchmarks,
            List<PlanAssignment> plan,
            List<JobRun> runs) {
    }

    private record RoundOutcome(int roundNumber, String order, List<RunOutcome> outcomes) {
    }
}
