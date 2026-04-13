package org.wsh.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.wsh.model.NodeProfile;
import org.wsh.task.TaskExecutor;
import org.wsh.task.TaskInputs;
import org.wsh.task.TaskResult;
import org.wsh.workflow.WorkflowRegistry;
import org.wsh.workflow.WorkflowSpec;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

public final class HadoopTaskJob {
    public static final String CONF_WORKFLOW = "wsh.hadoop.workflow";
    public static final String CONF_JOB_ID = "wsh.hadoop.jobId";
    public static final String CONF_INPUT_COUNT = "wsh.hadoop.input.count";
    public static final String CONF_OUTPUT_PATH = "wsh.hadoop.output.path";
    public static final String CONF_OUTPUT_DESCRIPTION = "wsh.hadoop.output.description";
    public static final String CONF_CLUSTER_ID = "wsh.hadoop.node.clusterId";
    public static final String CONF_NODE_ID = "wsh.hadoop.node.nodeId";
    public static final String CONF_CPU_THREADS = "wsh.hadoop.node.cpuThreads";
    public static final String CONF_IO_BUFFER_KB = "wsh.hadoop.node.ioBufferKb";
    public static final String CONF_MEMORY_MB = "wsh.hadoop.node.memoryMb";
    public static final String CONF_PARAM_COUNT = "wsh.hadoop.param.count";
    public static final String CONF_WORKFLOW_OPTION_COUNT = "wsh.hadoop.workflow.option.count";

    private HadoopTaskJob() {
    }

    public static void configureTask(
            Configuration configuration,
            WorkflowSpec workflowSpec,
            String jobId,
            NodeProfile nodeProfile,
            HadoopTaskInputs inputs,
            String outputPath,
            String outputDescription) {
        configuration.set(CONF_WORKFLOW, workflowSpec.workflowId());
        configuration.set(CONF_JOB_ID, jobId);
        configuration.set(CONF_OUTPUT_PATH, outputPath);
        configuration.set(CONF_OUTPUT_DESCRIPTION, outputDescription);
        configuration.set(CONF_CLUSTER_ID, nodeProfile.clusterId());
        configuration.set(CONF_NODE_ID, nodeProfile.nodeId());
        configuration.setInt(CONF_CPU_THREADS, nodeProfile.cpuThreads());
        configuration.setInt(CONF_IO_BUFFER_KB, nodeProfile.ioBufferKb());
        configuration.setInt(CONF_MEMORY_MB, nodeProfile.memoryMb());
        configuration.setInt(CONF_INPUT_COUNT, inputs.inputs().size());
        for (int index = 0; index < inputs.inputs().size(); index++) {
            configuration.set(inputKey(index), inputs.inputs().get(index));
        }
        configuration.setInt(CONF_PARAM_COUNT, inputs.parameters().size());
        int paramIndex = 0;
        for (Map.Entry<String, String> entry : inputs.parameters().entrySet()) {
            configuration.set(paramKey(paramIndex), entry.getKey() + "=" + entry.getValue());
            paramIndex++;
        }
        configuration.setInt(CONF_WORKFLOW_OPTION_COUNT, workflowSpec.variantOptions().size());
        int workflowOptionIndex = 0;
        for (Map.Entry<String, String> entry : workflowSpec.variantOptions().entrySet()) {
            configuration.set(workflowOptionKey(workflowOptionIndex), entry.getKey() + "=" + entry.getValue());
            workflowOptionIndex++;
        }
    }

    public static Job createJob(Configuration configuration, String jobName, String controlInputPath) throws IOException {
        Job job = Job.getInstance(configuration, jobName);
        job.setJarByClass(HadoopTaskJob.class);
        job.setMapperClass(TaskMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setNumReduceTasks(0);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(controlInputPath));
        return job;
    }

    private static String inputKey(int index) {
        return "wsh.hadoop.input." + index;
    }

    private static String paramKey(int index) {
        return "wsh.hadoop.param." + index;
    }

    private static String workflowOptionKey(int index) {
        return "wsh.hadoop.workflow.option." + index;
    }

    public static final class TaskMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
            WorkflowSpec workflowSpec = WorkflowRegistry.byId(
                    configuration.get(CONF_WORKFLOW),
                    readParameters(configuration, CONF_WORKFLOW_OPTION_COUNT, HadoopTaskJob::workflowOptionKey));
            String jobId = configuration.get(CONF_JOB_ID);
            NodeProfile nodeProfile = new NodeProfile(
                    configuration.get(CONF_CLUSTER_ID),
                    configuration.get(CONF_NODE_ID),
                    configuration.getInt(CONF_CPU_THREADS, 1),
                    configuration.getInt(CONF_IO_BUFFER_KB, 256),
                    configuration.getInt(CONF_MEMORY_MB, 1024));
            ArrayList<java.nio.file.Path> localInputs = new ArrayList<>();
            java.nio.file.Path workDir = Files.createTempDirectory("wsh-hadoop-task-");
            try {
                try (FileSystem fileSystem = FileSystem.newInstance(configuration)) {
                    int inputCount = configuration.getInt(CONF_INPUT_COUNT, 0);
                    for (int index = 0; index < inputCount; index++) {
                        Path source = new Path(configuration.get(inputKey(index)));
                        java.nio.file.Path localPath = localizeInput(fileSystem, source, workDir.resolve("input-" + index));
                        localInputs.add(localPath);
                    }
                    java.nio.file.Path localOutputDir = workDir.resolve("output");
                    Files.createDirectories(localOutputDir);
                    TaskInputs taskInputs = new TaskInputs(
                            localInputs,
                            localOutputDir,
                            readParameters(configuration, CONF_PARAM_COUNT, HadoopTaskJob::paramKey));
                    TaskExecutor executor = workflowSpec.executors().get(jobId);
                    if (executor == null) {
                        throw new IllegalArgumentException("Unknown workflow job: " + workflowSpec.workflowId() + "/" + jobId);
                    }
                    TaskResult result = executor.execute(taskInputs, nodeProfile);
                    Path hdfsOutputPath = new Path(configuration.get(CONF_OUTPUT_PATH));
                    Path outputDirectory = hdfsOutputPath.getParent();
                    if (outputDirectory == null) {
                        throw new IOException("Missing HDFS output directory for " + hdfsOutputPath);
                    }
                    fileSystem.delete(outputDirectory, true);
                    fileSystem.mkdirs(outputDirectory);
                    copyDirectoryToHdfs(fileSystem, localOutputDir, outputDirectory);
                }
            } catch (Exception exception) {
                throw new IOException("Hadoop task execution failed for " + jobId, exception);
            } finally {
                deleteRecursively(workDir);
            }
        }

        private static Map<String, String> readParameters(
                Configuration configuration,
                String countKey,
                java.util.function.IntFunction<String> keyFactory) {
            int count = configuration.getInt(countKey, 0);
            Map<String, String> parameters = new LinkedHashMap<>();
            for (int index = 0; index < count; index++) {
                String[] parts = configuration.get(keyFactory.apply(index), "").split("=", 2);
                if (parts.length == 2) {
                    parameters.put(parts[0], parts[1]);
                }
            }
            return Map.copyOf(parameters);
        }

        private static java.nio.file.Path localizeInput(
                FileSystem fileSystem,
                Path source,
                java.nio.file.Path localRoot) throws IOException {
            String fileName = source.getName();
            if (fileName == null || fileName.isBlank()) {
                fileName = "input";
            }
            if (fileName.endsWith(".manifest.tsv")) {
                Files.createDirectories(localRoot);
                Path sourceDirectory = source.getParent();
                if (sourceDirectory == null) {
                    throw new IOException("Manifest input is missing a parent directory: " + source);
                }
                copyDirectoryFromHdfs(fileSystem, sourceDirectory, localRoot);
                return localRoot.resolve(fileName);
            }
            java.nio.file.Path localPath = localRoot.resolve(fileName);
            Files.createDirectories(localPath.getParent());
            fileSystem.copyToLocalFile(false, source, new Path(localPath.toAbsolutePath().toString()), true);
            return localPath;
        }

        private static void copyDirectoryFromHdfs(
                FileSystem fileSystem,
                Path sourceDirectory,
                java.nio.file.Path localDirectory) throws IOException {
            for (FileStatus status : fileSystem.listStatus(sourceDirectory)) {
                java.nio.file.Path destination = localDirectory.resolve(status.getPath().getName());
                if (status.isDirectory()) {
                    Files.createDirectories(destination);
                    copyDirectoryFromHdfs(fileSystem, status.getPath(), destination);
                } else {
                    Files.createDirectories(destination.getParent());
                    fileSystem.copyToLocalFile(false, status.getPath(), new Path(destination.toAbsolutePath().toString()), true);
                }
            }
        }

        private static void copyDirectoryToHdfs(
                FileSystem fileSystem,
                java.nio.file.Path localDirectory,
                Path hdfsDirectory) throws IOException {
            try (Stream<java.nio.file.Path> stream = Files.walk(localDirectory)) {
                for (java.nio.file.Path candidate : stream.sorted().toList()) {
                    java.nio.file.Path relative = localDirectory.relativize(candidate);
                    Path destination = relative.toString().isBlank()
                            ? hdfsDirectory
                            : new Path(hdfsDirectory, relative.toString().replace('\\', '/'));
                    if (Files.isDirectory(candidate)) {
                        fileSystem.mkdirs(destination);
                    } else {
                        Path parent = destination.getParent();
                        if (parent != null) {
                            fileSystem.mkdirs(parent);
                        }
                        fileSystem.copyFromLocalFile(false, true, new Path(candidate.toAbsolutePath().toString()), destination);
                    }
                }
            }
        }

        private static void deleteRecursively(java.nio.file.Path path) throws IOException {
            if (path == null || !Files.exists(path)) {
                return;
            }
            try (Stream<java.nio.file.Path> stream = Files.walk(path)) {
                stream.sorted(Comparator.reverseOrder()).forEach(candidate -> {
                    try {
                        Files.deleteIfExists(candidate);
                    } catch (IOException ignored) {
                    }
                });
            }
        }
    }
}
