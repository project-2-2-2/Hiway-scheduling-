package org.wsh.workflow;

import org.wsh.cli.CliArguments;
import org.wsh.hadoop.HadoopTaskInputs;
import org.wsh.model.JobDefinition;
import org.wsh.model.JobRun;
import org.wsh.model.TaskType;
import org.wsh.model.WorkflowDefinition;
import org.wsh.task.TaskInputs;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Future;

public final class EpigenomicsWorkflowSpec implements WorkflowSpec {
    public static final int DEFAULT_SPLIT_COUNT = 24;
    private static final char[] DNA = {'A', 'C', 'G', 'T'};
    private final int splitCount;
    private final WorkflowDefinition definition;

    public EpigenomicsWorkflowSpec() {
        this(DEFAULT_SPLIT_COUNT);
    }

    public EpigenomicsWorkflowSpec(int splitCount) {
        this.splitCount = Math.max(1, splitCount);
        this.definition = buildDefinition(this.splitCount);
    }

    @Override
    public WorkflowDefinition definition() {
        return definition;
    }

    @Override
    public List<Path> requiredDataPaths(Path dataRoot) {
        return List.of(
                dataRoot.resolve("raw.fastq"),
                dataRoot.resolve("reference.fasta"),
                dataRoot.resolve("contaminants.tsv"));
    }

    @Override
    public List<Path> requiredTrainingDataPaths(Path dataRoot) {
        return List.of(
                dataRoot.resolve("training/raw.fastq"),
                dataRoot.resolve("training/reference.fasta"),
                dataRoot.resolve("training/contaminants.tsv"));
    }

    @Override
    public void generateData(Path dataRoot, CliArguments cli) throws Exception {
        Files.createDirectories(dataRoot.resolve("training"));
        Random random = new Random(Long.parseLong(cli.option("seed", "42")));
        int readsPerSplit = cli.optionInt("reads-per-split", 192);
        int readLength = cli.optionInt("read-length", 80);
        int referenceRecords = cli.optionInt("reference-record-count", 640);

        writeFastq(dataRoot.resolve("raw.fastq"), splitCount * readsPerSplit, readLength, random);
        writeReference(dataRoot.resolve("reference.fasta"), referenceRecords, readLength + 32, random);
        writeContaminants(dataRoot.resolve("contaminants.tsv"));

        int trainingSplits = Math.min(8, splitCount);
        writeFastq(dataRoot.resolve("training/raw.fastq"), trainingSplits * Math.max(48, readsPerSplit / 2), readLength, random);
        copyReferenceSubset(dataRoot.resolve("reference.fasta"), dataRoot.resolve("training/reference.fasta"), Math.max(128, referenceRecords / 2));
        Files.copy(dataRoot.resolve("contaminants.tsv"), dataRoot.resolve("training/contaminants.tsv"), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public TaskInputs resolveInputs(String jobId, Path dataRoot, Path runRoot, Map<String, Future<JobRun>> futures) throws Exception {
        Path outputDirectory = runRoot.resolve("jobs").resolve(jobId);
        Files.createDirectories(outputDirectory);
        if (jobId.equals("fastqSplit")) {
            return new TaskInputs(List.of(dataRoot.resolve("raw.fastq")), outputDirectory, Map.of("chunk_count", Integer.toString(splitCount())));
        }
        if (jobId.startsWith("filterContams-")) {
            return new TaskInputs(List.of(futures.get("fastqSplit").get().outputPath(), dataRoot.resolve("contaminants.tsv")),
                    outputDirectory, Map.of("chunk_id", chunkId(jobId)));
        }
        if (jobId.startsWith("sol2sanger-")) {
            return new TaskInputs(List.of(futures.get("filterContams-" + chunkId(jobId)).get().outputPath()), outputDirectory, Map.of());
        }
        if (jobId.startsWith("fastq2bfq-")) {
            return new TaskInputs(List.of(futures.get("sol2sanger-" + chunkId(jobId)).get().outputPath()), outputDirectory, Map.of());
        }
        if (jobId.startsWith("map-")) {
            return new TaskInputs(List.of(
                    futures.get("fastq2bfq-" + chunkId(jobId)).get().outputPath(),
                    dataRoot.resolve("reference.fasta")), outputDirectory, Map.of());
        }
        if (jobId.equals("mapMerge")) {
            List<Path> inputs = new ArrayList<>();
            for (int i = 1; i <= splitCount(); i++) {
                inputs.add(futures.get("map-" + pad(i)).get().outputPath());
            }
            return new TaskInputs(inputs, outputDirectory, Map.of());
        }
        if (jobId.equals("maqIndex")) {
            return new TaskInputs(List.of(futures.get("mapMerge").get().outputPath()), outputDirectory, Map.of());
        }
        if (jobId.equals("pileup")) {
            return new TaskInputs(List.of(futures.get("mapMerge").get().outputPath(), futures.get("maqIndex").get().outputPath()), outputDirectory, Map.of());
        }
        throw new IllegalArgumentException("Unknown epigenomics job: " + jobId);
    }

    @Override
    public TaskInputs resolveTrainingInputs(String jobId, Path dataRoot) {
        Path outputDirectory = dataRoot.resolve("training/generated").resolve(jobId);
        if (jobId.equals("fastqSplit")) {
            return new TaskInputs(
                    List.of(dataRoot.resolve("training/raw.fastq")),
                    outputDirectory,
                    Map.of("chunk_count", Integer.toString(trainingSplitCount())));
        }
        if (jobId.equals("filterContams-001")) {
            return new TaskInputs(List.of(trainingOutputPath(dataRoot, "fastqSplit"), dataRoot.resolve("training/contaminants.tsv")),
                    outputDirectory, Map.of("chunk_id", "001"));
        }
        if (jobId.equals("sol2sanger-001")) {
            return new TaskInputs(List.of(trainingOutputPath(dataRoot, "filterContams-001")), outputDirectory, Map.of());
        }
        if (jobId.equals("fastq2bfq-001")) {
            return new TaskInputs(List.of(trainingOutputPath(dataRoot, "sol2sanger-001")), outputDirectory, Map.of());
        }
        if (jobId.equals("map-001")) {
            return new TaskInputs(List.of(trainingOutputPath(dataRoot, "fastq2bfq-001"), dataRoot.resolve("training/reference.fasta")), outputDirectory, Map.of());
        }
        if (jobId.equals("mapMerge")) {
            List<Path> inputs = new ArrayList<>();
            for (int index = 0; index < trainingSplitCount(); index++) {
                inputs.add(trainingOutputPath(dataRoot, "map-001"));
            }
            return new TaskInputs(inputs, outputDirectory, Map.of());
        }
        if (jobId.equals("maqIndex")) {
            return new TaskInputs(List.of(trainingOutputPath(dataRoot, "mapMerge")), outputDirectory, Map.of());
        }
        if (jobId.equals("pileup")) {
            return new TaskInputs(List.of(trainingOutputPath(dataRoot, "mapMerge"), trainingOutputPath(dataRoot, "maqIndex")), outputDirectory, Map.of());
        }
        throw new IllegalArgumentException("Unknown epigenomics training job: " + jobId);
    }

    @Override
    public HadoopTaskInputs resolveHadoopInputs(String jobId, String dataRoot, String runRoot) {
        String outputDirectory = hdfsJobDirectory(runRoot, jobId);
        if (jobId.equals("fastqSplit")) {
            return new HadoopTaskInputs(
                    List.of(normalizeHdfsPath(dataRoot) + "/raw.fastq"),
                    outputDirectory,
                    Map.of("chunk_count", Integer.toString(splitCount())));
        }
        if (jobId.startsWith("filterContams-")) {
            return new HadoopTaskInputs(
                    List.of(
                            hadoopOutputPath("fastqSplit", hdfsJobDirectory(runRoot, "fastqSplit")),
                            normalizeHdfsPath(dataRoot) + "/contaminants.tsv"),
                    outputDirectory,
                    Map.of("chunk_id", chunkId(jobId)));
        }
        if (jobId.startsWith("sol2sanger-")) {
            return new HadoopTaskInputs(
                    List.of(hadoopOutputPath("filterContams-" + chunkId(jobId), hdfsJobDirectory(runRoot, "filterContams-" + chunkId(jobId)))),
                    outputDirectory,
                    Map.of());
        }
        if (jobId.startsWith("fastq2bfq-")) {
            return new HadoopTaskInputs(
                    List.of(hadoopOutputPath("sol2sanger-" + chunkId(jobId), hdfsJobDirectory(runRoot, "sol2sanger-" + chunkId(jobId)))),
                    outputDirectory,
                    Map.of());
        }
        if (jobId.startsWith("map-")) {
            return new HadoopTaskInputs(
                    List.of(
                            hadoopOutputPath("fastq2bfq-" + chunkId(jobId), hdfsJobDirectory(runRoot, "fastq2bfq-" + chunkId(jobId))),
                            normalizeHdfsPath(dataRoot) + "/reference.fasta"),
                    outputDirectory,
                    Map.of());
        }
        if (jobId.equals("mapMerge")) {
            List<String> inputs = new ArrayList<>();
            for (int i = 1; i <= splitCount(); i++) {
                String chunk = pad(i);
                inputs.add(hadoopOutputPath("map-" + chunk, hdfsJobDirectory(runRoot, "map-" + chunk)));
            }
            return new HadoopTaskInputs(inputs, outputDirectory, Map.of());
        }
        if (jobId.equals("maqIndex")) {
            return new HadoopTaskInputs(
                    List.of(hadoopOutputPath("mapMerge", hdfsJobDirectory(runRoot, "mapMerge"))),
                    outputDirectory,
                    Map.of());
        }
        if (jobId.equals("pileup")) {
            return new HadoopTaskInputs(
                    List.of(
                            hadoopOutputPath("mapMerge", hdfsJobDirectory(runRoot, "mapMerge")),
                            hadoopOutputPath("maqIndex", hdfsJobDirectory(runRoot, "maqIndex"))),
                    outputDirectory,
                    Map.of());
        }
        throw new IllegalArgumentException("Unknown epigenomics job: " + jobId);
    }

    @Override
    public HadoopTaskInputs resolveHadoopTrainingInputs(String jobId, String dataRoot) {
        String outputDirectory = normalizeHdfsPath(dataRoot) + "/training/generated/" + jobId;
        if (jobId.equals("fastqSplit")) {
            return new HadoopTaskInputs(
                    List.of(normalizeHdfsPath(dataRoot) + "/training/raw.fastq"),
                    outputDirectory,
                    Map.of("chunk_count", Integer.toString(trainingSplitCount())));
        }
        if (jobId.equals("filterContams-001")) {
            return new HadoopTaskInputs(
                    List.of(
                            hadoopTrainingOutputPath(dataRoot, "fastqSplit"),
                            normalizeHdfsPath(dataRoot) + "/training/contaminants.tsv"),
                    outputDirectory,
                    Map.of("chunk_id", "001"));
        }
        if (jobId.equals("sol2sanger-001")) {
            return new HadoopTaskInputs(List.of(hadoopTrainingOutputPath(dataRoot, "filterContams-001")), outputDirectory, Map.of());
        }
        if (jobId.equals("fastq2bfq-001")) {
            return new HadoopTaskInputs(List.of(hadoopTrainingOutputPath(dataRoot, "sol2sanger-001")), outputDirectory, Map.of());
        }
        if (jobId.equals("map-001")) {
            return new HadoopTaskInputs(
                    List.of(
                            hadoopTrainingOutputPath(dataRoot, "fastq2bfq-001"),
                            normalizeHdfsPath(dataRoot) + "/training/reference.fasta"),
                    outputDirectory,
                    Map.of());
        }
        if (jobId.equals("mapMerge")) {
            List<String> inputs = new ArrayList<>();
            for (int index = 0; index < trainingSplitCount(); index++) {
                inputs.add(hadoopTrainingOutputPath(dataRoot, "map-001"));
            }
            return new HadoopTaskInputs(inputs, outputDirectory, Map.of());
        }
        if (jobId.equals("maqIndex")) {
            return new HadoopTaskInputs(List.of(hadoopTrainingOutputPath(dataRoot, "mapMerge")), outputDirectory, Map.of());
        }
        if (jobId.equals("pileup")) {
            return new HadoopTaskInputs(
                    List.of(
                            hadoopTrainingOutputPath(dataRoot, "mapMerge"),
                            hadoopTrainingOutputPath(dataRoot, "maqIndex")),
                    outputDirectory,
                    Map.of());
        }
        throw new IllegalArgumentException("Unknown epigenomics training job: " + jobId);
    }

    private static WorkflowDefinition buildDefinition(int splitCount) {
        List<JobDefinition> jobs = new ArrayList<>();
        jobs.add(new JobDefinition("fastqSplit", "fastQSplit", List.of(), TaskType.FASTQ_SPLIT, 1_800L, 24_000L,
                "fastqSplit", "raw FASTQ", "FASTQ chunk manifest", Map.of("chunk_count", Integer.toString(splitCount))));
        for (int i = 1; i <= splitCount; i++) {
            String id = pad(i);
            jobs.add(new JobDefinition("filterContams-" + id, "filterContams " + id, List.of("fastqSplit"), TaskType.FILTER_CONTAMS, 2_100L, 384_000L,
                    "filterContams", "FASTQ chunk", "filtered chunk", Map.of("chunk_id", id)));
            jobs.add(new JobDefinition("sol2sanger-" + id, "sol2sanger " + id, List.of("filterContams-" + id), TaskType.SOL2SANGER, 1_800L, 392_000L,
                    "sol2sanger", "filtered chunk", "Sanger FASTQ", Map.of("chunk_id", id)));
            jobs.add(new JobDefinition("fastq2bfq-" + id, "fastq2bfq " + id, List.of("sol2sanger-" + id), TaskType.FASTQ_TO_BFQ, 1_800L, 512_000L,
                    "fastq2bfq", "Sanger FASTQ", "BFQ-like chunk", Map.of("chunk_id", id)));
            jobs.add(new JobDefinition("map-" + id, "map " + id, List.of("fastq2bfq-" + id), TaskType.MAP, 4_200L, 640_000L,
                    "map", "BFQ-like chunk", "alignments", Map.of("chunk_id", id)));
        }
        jobs.add(new JobDefinition("mapMerge", "mapMerge", rangeDependencies("map-", splitCount), TaskType.MAP_MERGE, 3_000L, 4_800_000L,
                "mapMerge", "mapped chunks", "merged alignments", Map.of()));
        jobs.add(new JobDefinition("maqIndex", "maqIndex", List.of("mapMerge"), TaskType.MAQ_INDEX, 2_250L, 512_000L,
                "maqIndex", "merged alignments", "alignment index", Map.of()));
        jobs.add(new JobDefinition("pileup", "pileup", List.of("mapMerge", "maqIndex"), TaskType.PILEUP, 2_700L, 768_000L,
                "pileup", "merged alignments + index", "pileup summary", Map.of()));
        return new WorkflowDefinition("epigenomics", "Epigenomics", jobs);
    }

    private static List<String> rangeDependencies(String prefix, int count) {
        List<String> ids = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            ids.add(prefix + pad(i));
        }
        return List.copyOf(ids);
    }

    private static void writeFastq(Path output, int readCount, int readLength, Random random) throws Exception {
        try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            for (int i = 0; i < readCount; i++) {
                String sequence = randomDna(readLength, random);
                String quality = randomQuality(readLength, random);
                writer.write("@read-" + String.format("%06d", i));
                writer.newLine();
                writer.write(sequence);
                writer.newLine();
                writer.write("+");
                writer.newLine();
                writer.write(quality);
                writer.newLine();
            }
        }
    }

    private static void writeReference(Path output, int recordCount, int length, Random random) throws Exception {
        try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            for (int i = 0; i < recordCount; i++) {
                writer.write(">chr-" + String.format("%04d", i) + " motif=" + randomDna(8, random));
                writer.newLine();
                writer.write(randomDna(length, random));
                writer.newLine();
            }
        }
    }

    private static void writeContaminants(Path output) throws Exception {
        Files.write(output, List.of("AAAAAA", "CCCCCC", "GGGGGG", "TTTTTT"), StandardCharsets.UTF_8);
    }

    private static void copyReferenceSubset(Path input, Path output, int records) throws Exception {
        List<String> lines = Files.readAllLines(input, StandardCharsets.UTF_8);
        int totalLines = Math.min(lines.size(), records * 2);
        Files.write(output, lines.subList(0, totalLines), StandardCharsets.UTF_8);
    }

    private static String randomDna(int length, Random random) {
        StringBuilder builder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            builder.append(DNA[random.nextInt(DNA.length)]);
        }
        return builder.toString();
    }

    private static String randomQuality(int length, Random random) {
        StringBuilder builder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            builder.append((char) (59 + random.nextInt(20)));
        }
        return builder.toString();
    }

    private static String chunkId(String jobId) {
        return jobId.substring(jobId.indexOf('-') + 1);
    }

    @Override
    public Map<String, String> variantOptions() {
        return Map.of("epigenomics-split-count", Integer.toString(splitCount));
    }

    private int trainingSplitCount() {
        return Math.min(8, splitCount);
    }

    private int splitCount() {
        return splitCount;
    }

    private static String pad(int value) {
        return String.format("%03d", value);
    }
}
