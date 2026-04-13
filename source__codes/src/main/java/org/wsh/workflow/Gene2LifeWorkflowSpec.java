package org.wsh.workflow;

import org.wsh.cli.CliArguments;
import org.wsh.data.DataGenerator;
import org.wsh.hadoop.HadoopTaskInputs;
import org.wsh.model.JobDefinition;
import org.wsh.model.JobRun;
import org.wsh.model.TaskType;
import org.wsh.model.WorkflowDefinition;
import org.wsh.task.TaskInputs;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public final class Gene2LifeWorkflowSpec implements WorkflowSpec {
    private static final WorkflowDefinition DEFINITION = new WorkflowDefinition(
            "gene2life",
            "Gene2Life",
            List.of(
                    new JobDefinition("blast1", "Blast 1", List.of(), TaskType.BLAST, 62_000L, 100_000L, "blast1",
                            "source DNA sequence", "0.1 MB blast hits", Map.of()),
                    new JobDefinition("blast2", "Blast 2", List.of(), TaskType.BLAST, 62_000L, 100_000L, "blast2",
                            "source DNA sequence", "0.1 MB blast hits", Map.of()),
                    new JobDefinition("clustalw1", "ClustalW 1", List.of("blast1"), TaskType.CLUSTAL, 90_000L, 110_000L, "clustalw1",
                            "blast1 hits", "0.1 MB alignment", Map.of()),
                    new JobDefinition("clustalw2", "ClustalW 2", List.of("blast2"), TaskType.CLUSTAL, 90_000L, 110_000L, "clustalw2",
                            "blast2 hits", "0.1 MB alignment", Map.of()),
                    new JobDefinition("dnapars", "Dnapars", List.of("clustalw1"), TaskType.DNAPARS, 19_000L, 4_096L, "dnapars",
                            "clustalw1 alignment", "4 KB DNA tree", Map.of()),
                    new JobDefinition("protpars", "Protpars", List.of("clustalw2"), TaskType.PROTPARS, 16_000L, 4_096L, "protpars",
                            "clustalw2 alignment", "4 KB protein tree", Map.of()),
                    new JobDefinition("drawgram1", "Drawgram 1", List.of("dnapars"), TaskType.DRAWGRAM, 18_000L, 35_000L, "drawgram1",
                            "dnapars tree", "35 KB tree files", Map.of()),
                    new JobDefinition("drawgram2", "Drawgram 2", List.of("protpars"), TaskType.DRAWGRAM, 18_000L, 35_000L, "drawgram2",
                            "protpars tree", "35 KB tree files", Map.of())));

    @Override
    public WorkflowDefinition definition() {
        return DEFINITION;
    }

    @Override
    public List<Path> requiredDataPaths(Path dataRoot) {
        return List.of(
                dataRoot.resolve("query.fasta"),
                dataRoot.resolve("reference-a.fasta"),
                dataRoot.resolve("reference-b.fasta"));
    }

    @Override
    public List<Path> requiredTrainingDataPaths(Path dataRoot) {
        return List.of(
                dataRoot.resolve("training/query-sample.fasta"),
                dataRoot.resolve("training/reference-a-sample.fasta"),
                dataRoot.resolve("training/reference-b-sample.fasta"));
    }

    @Override
    public void generateData(Path dataRoot, CliArguments cli) throws Exception {
        Files.createDirectories(dataRoot.getParent());
        DataGenerator generator = new DataGenerator(Long.parseLong(cli.option("seed", "42")));
        generator.generate(
                dataRoot,
                cli.optionInt("query-count", 128),
                cli.optionInt("reference-records-per-shard", 40_000),
                cli.optionInt("sequence-length", 240),
                cli.optionInt("training-fraction-percent", 25));
    }

    @Override
    public TaskInputs resolveInputs(String jobId, Path dataRoot, Path runRoot, Map<String, Future<JobRun>> futures) throws Exception {
        Path outputDirectory = runRoot.resolve("jobs").resolve(jobId);
        Files.createDirectories(outputDirectory);
        return switch (jobId) {
            case "blast1" -> new TaskInputs(List.of(dataRoot.resolve("query.fasta"), dataRoot.resolve("reference-a.fasta")), outputDirectory, Map.of());
            case "blast2" -> new TaskInputs(List.of(dataRoot.resolve("query.fasta"), dataRoot.resolve("reference-b.fasta")), outputDirectory, Map.of());
            case "clustalw1" -> new TaskInputs(List.of(futures.get("blast1").get().outputPath()), outputDirectory, Map.of());
            case "clustalw2" -> new TaskInputs(List.of(futures.get("blast2").get().outputPath()), outputDirectory, Map.of());
            case "dnapars" -> new TaskInputs(List.of(futures.get("clustalw1").get().outputPath()), outputDirectory, Map.of());
            case "protpars" -> new TaskInputs(List.of(futures.get("clustalw2").get().outputPath()), outputDirectory, Map.of());
            case "drawgram1" -> new TaskInputs(List.of(futures.get("dnapars").get().outputPath()), outputDirectory, Map.of());
            case "drawgram2" -> new TaskInputs(List.of(futures.get("protpars").get().outputPath()), outputDirectory, Map.of());
            default -> throw new IllegalArgumentException("Unknown gene2life job: " + jobId);
        };
    }

    @Override
    public TaskInputs resolveTrainingInputs(String jobId, Path dataRoot) {
        Path outputDirectory = dataRoot.resolve("training/generated").resolve(jobId);
        return switch (jobId) {
            case "blast1" -> new TaskInputs(List.of(
                    dataRoot.resolve("training/query-sample.fasta"),
                    dataRoot.resolve("training/reference-a-sample.fasta")), outputDirectory, Map.of());
            case "blast2" -> new TaskInputs(List.of(
                    dataRoot.resolve("training/query-sample.fasta"),
                    dataRoot.resolve("training/reference-b-sample.fasta")), outputDirectory, Map.of());
            case "clustalw1" -> new TaskInputs(List.of(trainingOutputPath(dataRoot, "blast1")), outputDirectory, Map.of());
            case "clustalw2" -> new TaskInputs(List.of(trainingOutputPath(dataRoot, "blast2")), outputDirectory, Map.of());
            case "dnapars" -> new TaskInputs(List.of(trainingOutputPath(dataRoot, "clustalw1")), outputDirectory, Map.of());
            case "protpars" -> new TaskInputs(List.of(trainingOutputPath(dataRoot, "clustalw2")), outputDirectory, Map.of());
            case "drawgram1" -> new TaskInputs(List.of(trainingOutputPath(dataRoot, "dnapars")), outputDirectory, Map.of());
            case "drawgram2" -> new TaskInputs(List.of(trainingOutputPath(dataRoot, "protpars")), outputDirectory, Map.of());
            default -> throw new IllegalArgumentException("Unknown gene2life training job: " + jobId);
        };
    }

    @Override
    public HadoopTaskInputs resolveHadoopInputs(String jobId, String dataRoot, String runRoot) {
        String outputDirectory = hdfsJobDirectory(runRoot, jobId);
        return switch (jobId) {
            case "blast1" -> new HadoopTaskInputs(
                    List.of(normalizeHdfsPath(dataRoot) + "/query.fasta", normalizeHdfsPath(dataRoot) + "/reference-a.fasta"),
                    outputDirectory,
                    Map.of());
            case "blast2" -> new HadoopTaskInputs(
                    List.of(normalizeHdfsPath(dataRoot) + "/query.fasta", normalizeHdfsPath(dataRoot) + "/reference-b.fasta"),
                    outputDirectory,
                    Map.of());
            case "clustalw1" -> new HadoopTaskInputs(
                    List.of(hadoopOutputPath("blast1", hdfsJobDirectory(runRoot, "blast1"))),
                    outputDirectory,
                    Map.of());
            case "clustalw2" -> new HadoopTaskInputs(
                    List.of(hadoopOutputPath("blast2", hdfsJobDirectory(runRoot, "blast2"))),
                    outputDirectory,
                    Map.of());
            case "dnapars" -> new HadoopTaskInputs(
                    List.of(hadoopOutputPath("clustalw1", hdfsJobDirectory(runRoot, "clustalw1"))),
                    outputDirectory,
                    Map.of());
            case "protpars" -> new HadoopTaskInputs(
                    List.of(hadoopOutputPath("clustalw2", hdfsJobDirectory(runRoot, "clustalw2"))),
                    outputDirectory,
                    Map.of());
            case "drawgram1" -> new HadoopTaskInputs(
                    List.of(hadoopOutputPath("dnapars", hdfsJobDirectory(runRoot, "dnapars"))),
                    outputDirectory,
                    Map.of());
            case "drawgram2" -> new HadoopTaskInputs(
                    List.of(hadoopOutputPath("protpars", hdfsJobDirectory(runRoot, "protpars"))),
                    outputDirectory,
                    Map.of());
            default -> throw new IllegalArgumentException("Unknown gene2life job: " + jobId);
        };
    }

    @Override
    public HadoopTaskInputs resolveHadoopTrainingInputs(String jobId, String dataRoot) {
        String outputDirectory = normalizeHdfsPath(dataRoot) + "/training/generated/" + jobId;
        return switch (jobId) {
            case "blast1" -> new HadoopTaskInputs(
                    List.of(
                            normalizeHdfsPath(dataRoot) + "/training/query-sample.fasta",
                            normalizeHdfsPath(dataRoot) + "/training/reference-a-sample.fasta"),
                    outputDirectory,
                    Map.of());
            case "blast2" -> new HadoopTaskInputs(
                    List.of(
                            normalizeHdfsPath(dataRoot) + "/training/query-sample.fasta",
                            normalizeHdfsPath(dataRoot) + "/training/reference-b-sample.fasta"),
                    outputDirectory,
                    Map.of());
            case "clustalw1" -> new HadoopTaskInputs(List.of(hadoopTrainingOutputPath(dataRoot, "blast1")), outputDirectory, Map.of());
            case "clustalw2" -> new HadoopTaskInputs(List.of(hadoopTrainingOutputPath(dataRoot, "blast2")), outputDirectory, Map.of());
            case "dnapars" -> new HadoopTaskInputs(List.of(hadoopTrainingOutputPath(dataRoot, "clustalw1")), outputDirectory, Map.of());
            case "protpars" -> new HadoopTaskInputs(List.of(hadoopTrainingOutputPath(dataRoot, "clustalw2")), outputDirectory, Map.of());
            case "drawgram1" -> new HadoopTaskInputs(List.of(hadoopTrainingOutputPath(dataRoot, "dnapars")), outputDirectory, Map.of());
            case "drawgram2" -> new HadoopTaskInputs(List.of(hadoopTrainingOutputPath(dataRoot, "protpars")), outputDirectory, Map.of());
            default -> throw new IllegalArgumentException("Unknown gene2life training job: " + jobId);
        };
    }
}
