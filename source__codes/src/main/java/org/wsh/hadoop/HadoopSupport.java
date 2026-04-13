package org.wsh.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.util.Objects;
import java.util.Comparator;
import java.util.stream.Stream;

public final class HadoopSupport {
    private final HadoopExecutionConfig executionConfig;
    private final Configuration configuration;

    public HadoopSupport(HadoopExecutionConfig executionConfig) {
        this.executionConfig = executionConfig;
        this.configuration = buildConfiguration(executionConfig);
    }

    public Configuration configuration() {
        return new Configuration(configuration);
    }

    public String normalizedDataRoot() {
        return executionConfig.normalizedDataRoot();
    }

    public String normalizedWorkspaceRoot() {
        return executionConfig.normalizedWorkspaceRoot();
    }

    public boolean enableNodeLabels() {
        return executionConfig.enableNodeLabels();
    }

    public void syncLocalDirectoryToHdfs(java.nio.file.Path localRoot, String hdfsRoot) throws Exception {
        String localMetadata = localGenerationMetadata(localRoot);
        String remoteMetadata = readHdfsTextOrNull(hdfsRoot + "/.generation-metadata.env");
        if (localMetadata != null && localMetadata.equals(remoteMetadata)) {
            return;
        }
        deleteIfExists(hdfsRoot, true);
        mkdirs(hdfsRoot);
        try (Stream<java.nio.file.Path> stream = Files.walk(localRoot)) {
            for (java.nio.file.Path path : stream.sorted().toList()) {
                if (path.equals(localRoot)) {
                    continue;
                }
                java.nio.file.Path relative = localRoot.relativize(path);
                String destination = normalize(hdfsRoot + "/" + toUnixPath(relative));
                if (Files.isDirectory(path)) {
                    mkdirs(destination);
                } else {
                    copyLocalFileToHdfs(path, destination);
                }
            }
        }
    }

    public void copyLocalFileToHdfs(java.nio.file.Path localPath, String hdfsPath) throws Exception {
        try (FileSystem fileSystem = FileSystem.get(configuration())) {
            Path target = new Path(normalize(hdfsPath));
            Path parent = target.getParent();
            if (parent != null) {
                fileSystem.mkdirs(parent);
            }
            fileSystem.copyFromLocalFile(false, true, new Path(localPath.toAbsolutePath().toString()), target);
        }
    }

    public void copyHdfsFileToLocal(String hdfsPath, java.nio.file.Path localPath) throws Exception {
        Files.createDirectories(localPath.getParent());
        try (FileSystem fileSystem = FileSystem.get(configuration())) {
            fileSystem.copyToLocalFile(false, new Path(normalize(hdfsPath)), new Path(localPath.toAbsolutePath().toString()), true);
        }
    }

    public java.nio.file.Path localizeInput(String hdfsPath, java.nio.file.Path localRoot) throws Exception {
        try (FileSystem fileSystem = FileSystem.get(configuration())) {
            Path source = new Path(normalize(hdfsPath));
            String fileName = source.getName();
            if (fileName == null || fileName.isBlank()) {
                fileName = "input";
            }
            if (fileName.endsWith(".manifest.tsv")) {
                Files.createDirectories(localRoot);
                Path sourceDirectory = source.getParent();
                if (sourceDirectory == null) {
                    throw new IOException("Manifest input is missing a parent directory: " + hdfsPath);
                }
                copyDirectoryFromHdfs(fileSystem, sourceDirectory, localRoot);
                return localRoot.resolve(fileName);
            }
            java.nio.file.Path localPath = localRoot.resolve(fileName);
            Files.createDirectories(localPath.getParent());
            fileSystem.copyToLocalFile(false, source, new Path(localPath.toAbsolutePath().toString()), true);
            return localPath;
        }
    }

    public void copyLocalDirectoryToHdfs(java.nio.file.Path localDirectory, String hdfsDirectory) throws Exception {
        try (FileSystem fileSystem = FileSystem.get(configuration())) {
            Path destinationRoot = new Path(normalize(hdfsDirectory));
            fileSystem.delete(destinationRoot, true);
            copyDirectoryToHdfs(fileSystem, localDirectory, destinationRoot);
        }
    }

    public void copyHdfsDirectoryToLocal(String hdfsDirectory, java.nio.file.Path localDirectory) throws Exception {
        deleteLocalRecursively(localDirectory);
        Files.createDirectories(localDirectory);
        try (FileSystem fileSystem = FileSystem.get(configuration())) {
            copyDirectoryFromHdfs(fileSystem, new Path(normalize(hdfsDirectory)), localDirectory);
        }
    }

    public void deleteIfExists(String hdfsPath, boolean recursive) throws Exception {
        try (FileSystem fileSystem = FileSystem.get(configuration())) {
            Path path = new Path(normalize(hdfsPath));
            if (fileSystem.exists(path)) {
                fileSystem.delete(path, recursive);
            }
        }
    }

    public void mkdirs(String hdfsPath) throws Exception {
        try (FileSystem fileSystem = FileSystem.get(configuration())) {
            fileSystem.mkdirs(new Path(normalize(hdfsPath)));
        }
    }

    public void writeUtf8(String hdfsPath, String value) throws Exception {
        try (FileSystem fileSystem = FileSystem.get(configuration())) {
            Path path = new Path(normalize(hdfsPath));
            Path parent = path.getParent();
            if (parent != null) {
                fileSystem.mkdirs(parent);
            }
            try (FSDataOutputStream outputStream = fileSystem.create(path, true)) {
                outputStream.write(value.getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    public boolean exists(String hdfsPath) throws Exception {
        try (FileSystem fileSystem = FileSystem.get(configuration())) {
            return fileSystem.exists(new Path(normalize(hdfsPath)));
        }
    }

    public String readHdfsTextOrNull(String hdfsPath) throws Exception {
        try (FileSystem fileSystem = FileSystem.get(configuration())) {
            Path path = new Path(normalize(hdfsPath));
            if (!fileSystem.exists(path)) {
                return null;
            }
            try (FSDataInputStream inputStream = fileSystem.open(path)) {
                return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
            }
        }
    }

    public void deleteChildrenIfExists(String hdfsPath) throws Exception {
        try (FileSystem fileSystem = FileSystem.get(configuration())) {
            Path path = new Path(normalize(hdfsPath));
            if (!fileSystem.exists(path)) {
                return;
            }
            for (FileStatus status : fileSystem.listStatus(path)) {
                fileSystem.delete(status.getPath(), true);
            }
        }
    }

    public String normalize(String value) {
        return normalizePath(value);
    }

    private static Configuration buildConfiguration(HadoopExecutionConfig executionConfig) {
        Configuration configuration = new Configuration();
        String javaHome = System.getenv().getOrDefault("GENE2LIFE_HADOOP_JAVA_HOME", "/opt/java/openjdk");
        String hadoopHome = System.getenv().getOrDefault("GENE2LIFE_HADOOP_HOME", "/opt/hadoop");
        String hadoopCommonHome = System.getenv().getOrDefault("GENE2LIFE_HADOOP_COMMON_HOME", hadoopHome);
        String hadoopHdfsHome = System.getenv().getOrDefault("GENE2LIFE_HADOOP_HDFS_HOME", hadoopHome);
        String hadoopMapredHome = System.getenv().getOrDefault("GENE2LIFE_HADOOP_MAPRED_HOME", hadoopHome);
        String hadoopYarnHome = System.getenv().getOrDefault("GENE2LIFE_HADOOP_YARN_HOME", hadoopHome);
        if (executionConfig.hadoopConfDir() != null && !executionConfig.hadoopConfDir().isBlank()) {
            java.nio.file.Path confDir = java.nio.file.Path.of(executionConfig.hadoopConfDir());
            addIfPresent(configuration, confDir.resolve("core-site.xml"));
            addIfPresent(configuration, confDir.resolve("hdfs-site.xml"));
            addIfPresent(configuration, confDir.resolve("mapred-site.xml"));
            addIfPresent(configuration, confDir.resolve("yarn-site.xml"));
        }
        if (executionConfig.fsDefaultFs() != null && !executionConfig.fsDefaultFs().isBlank()) {
            configuration.set("fs.defaultFS", executionConfig.fsDefaultFs());
        }
        configuration.setIfUnset("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        configuration.setIfUnset("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        configuration.setIfUnset("fs.AbstractFileSystem.hdfs.impl", "org.apache.hadoop.fs.Hdfs");
        configuration.setBoolean("fs.hdfs.impl.disable.cache", true);
        configuration.setBoolean("fs.file.impl.disable.cache", true);
        if (executionConfig.frameworkName() != null && !executionConfig.frameworkName().isBlank()) {
            configuration.set("mapreduce.framework.name", executionConfig.frameworkName());
        }
        configuration.set(
                "mapreduce.application.classpath",
                "$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*");
        String containerEnv = "JAVA_HOME=" + javaHome
                + ",HADOOP_HOME=" + hadoopHome
                + ",HADOOP_COMMON_HOME=" + hadoopCommonHome
                + ",HADOOP_HDFS_HOME=" + hadoopHdfsHome
                + ",HADOOP_MAPRED_HOME=" + hadoopMapredHome
                + ",HADOOP_YARN_HOME=" + hadoopYarnHome;
        configuration.set("yarn.app.mapreduce.am.env", containerEnv);
        configuration.set("mapreduce.map.env", containerEnv);
        configuration.set("mapreduce.reduce.env", containerEnv);
        String currentUser = System.getProperty("user.name", "wsh");
        String userStagingDir = "/user/" + currentUser + "/.staging";
        configuration.setIfUnset("mapreduce.jobtracker.staging.root.dir", userStagingDir);
        configuration.setIfUnset("yarn.app.mapreduce.am.staging-dir", userStagingDir);
        if (executionConfig.yarnResourceManagerAddress() != null
                && !executionConfig.yarnResourceManagerAddress().isBlank()) {
            String resourceManagerAddress = executionConfig.yarnResourceManagerAddress();
            configuration.set("yarn.resourcemanager.address", resourceManagerAddress);
            String[] hostPort = resourceManagerAddress.split(":", 2);
            if (hostPort.length == 2) {
                String host = hostPort[0];
                try {
                    int addressPort = Integer.parseInt(hostPort[1]);
                    configuration.set("yarn.resourcemanager.hostname", host);
                    configuration.set("yarn.resourcemanager.scheduler.address", host + ":" + Math.max(0, addressPort - 2));
                    configuration.set("yarn.resourcemanager.resource-tracker.address", host + ":" + Math.max(0, addressPort - 1));
                    configuration.set("yarn.resourcemanager.admin.address", host + ":" + (addressPort + 1));
                } catch (NumberFormatException ignored) {
                    configuration.set("yarn.resourcemanager.hostname", resourceManagerAddress);
                }
            } else {
                configuration.set("yarn.resourcemanager.hostname", resourceManagerAddress);
            }
        }
        return configuration;
    }

    private static void addIfPresent(Configuration configuration, java.nio.file.Path path) {
        if (Files.exists(path)) {
            configuration.addResource(new Path(path.toAbsolutePath().toString()));
        }
    }

    private static String localGenerationMetadata(java.nio.file.Path localRoot) throws IOException {
        java.nio.file.Path marker = localRoot.resolve(".generation-metadata.env");
        if (!Files.exists(marker, LinkOption.NOFOLLOW_LINKS)) {
            return null;
        }
        return Files.readString(marker, StandardCharsets.UTF_8);
    }

    private static String toUnixPath(java.nio.file.Path relative) {
        return relative.toString().replace('\\', '/');
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

    private static void deleteLocalRecursively(java.nio.file.Path path) throws IOException {
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

    private static String normalizePath(String value) {
        Objects.requireNonNull(value, "value");
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
