package org.wsh.task;

import org.wsh.model.NodeProfile;
import org.wsh.model.TaskType;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

public final class WorkflowTaskExecutors {
    private static final Map<String, Character> CODON_TABLE = buildCodonTable();
    private static final char[] DNA = {'A', 'C', 'G', 'T'};

    private WorkflowTaskExecutors() {
    }

    public static TaskExecutor executor(TaskType taskType) {
        return switch (taskType) {
            case BLAST -> WorkflowTaskExecutors::runBlast;
            case CLUSTAL -> WorkflowTaskExecutors::runClustal;
            case DNAPARS -> WorkflowTaskExecutors::runDnaPars;
            case PROTPARS -> WorkflowTaskExecutors::runProtPars;
            case DRAWGRAM -> WorkflowTaskExecutors::runDrawgram;
            case PREPARE_RECEPTOR -> WorkflowTaskExecutors::runPrepareReceptor;
            case PREPARE_GPF -> WorkflowTaskExecutors::runPrepareGpf;
            case PREPARE_DPF -> WorkflowTaskExecutors::runPrepareDpf;
            case AUTOGRID -> WorkflowTaskExecutors::runAutogrid;
            case AUTODOCK -> WorkflowTaskExecutors::runAutodock;
            case FASTQ_SPLIT -> WorkflowTaskExecutors::runFastqSplit;
            case FILTER_CONTAMS -> WorkflowTaskExecutors::runFilterContams;
            case SOL2SANGER -> WorkflowTaskExecutors::runSol2Sanger;
            case FASTQ_TO_BFQ -> WorkflowTaskExecutors::runFastqToBfq;
            case MAP -> WorkflowTaskExecutors::runMap;
            case MAP_MERGE -> WorkflowTaskExecutors::runMapMerge;
            case MAQ_INDEX -> WorkflowTaskExecutors::runMaqIndex;
            case PILEUP -> WorkflowTaskExecutors::runPileup;
        };
    }

    private static TaskResult runBlast(TaskInputs inputs, NodeProfile nodeProfile) throws Exception {
        Files.createDirectories(inputs.outputDirectory());
        Path output = inputs.outputDirectory().resolve("hits.tsv");
        Map<String, QueryProfile> queryProfiles = loadQueries(inputs.input(0), nodeProfile);
        Map<String, PriorityQueue<Hit>> hitsByQuery = new LinkedHashMap<>();
        for (String queryId : queryProfiles.keySet()) {
            hitsByQuery.put(queryId, new PriorityQueue<>(Comparator.comparingDouble(Hit::score)));
        }
        int chunkSize = Math.max(64, Math.min(3072, (blastParallelism(nodeProfile) * 128) + (nodeProfile.memoryMb() / 48)));
        List<FastaRecord> chunk = new ArrayList<>(chunkSize);
        try (BufferedReader reader = bufferedReader(inputs.input(1), nodeProfile)) {
            FastaRecord record;
            while ((record = nextFastaRecord(reader)) != null) {
                chunk.add(record);
                if (chunk.size() >= chunkSize) {
                    processBlastChunk(chunk, queryProfiles, hitsByQuery, blastParallelism(nodeProfile));
                    chunk.clear();
                }
            }
        }
        if (!chunk.isEmpty()) {
            processBlastChunk(chunk, queryProfiles, hitsByQuery, blastParallelism(nodeProfile));
        }
        try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            writer.write("query_id\tref_id\tscore\torganism\tfunction\tsequence");
            writer.newLine();
            for (Map.Entry<String, PriorityQueue<Hit>> entry : hitsByQuery.entrySet()) {
                List<Hit> ordered = new ArrayList<>(entry.getValue());
                ordered.sort(Comparator.comparingDouble(Hit::score).reversed());
                for (Hit hit : ordered) {
                    writer.write(hit.queryId + "\t" + hit.refId + "\t" + String.format("%.5f", hit.score)
                            + "\t" + hit.organism + "\t" + hit.function + "\t" + hit.sequence);
                    writer.newLine();
                }
            }
        }
        return new TaskResult(output, "blast hits");
    }

    private static TaskResult runClustal(TaskInputs inputs, NodeProfile nodeProfile) throws Exception {
        Files.createDirectories(inputs.outputDirectory());
        Path output = inputs.outputDirectory().resolve("alignment.tsv");
        Map<String, List<Hit>> hits = readHits(inputs.input(0), nodeProfile);
        ForkJoinPool pool = new ForkJoinPool(Math.max(1, nodeProfile.cpuThreads()));
        try {
            List<String> rows = pool.submit(() -> hits.entrySet().parallelStream()
                    .map(entry -> {
                        List<String> sequences = entry.getValue().stream().map(Hit::sequence).toList();
                        String consensus = consensus(sequences);
                        double avgScore = entry.getValue().stream().mapToDouble(Hit::score).average().orElse(0.0);
                        double alignmentSignal = alignmentSignal(sequences, consensus);
                        String organism = majority(entry.getValue().stream().map(Hit::organism).toList());
                        String function = majority(entry.getValue().stream().map(Hit::function).toList());
                        return entry.getKey() + "\t" + entry.getValue().size() + "\t"
                                + String.format("%.5f", avgScore + alignmentSignal) + "\t" + organism + "\t" + function + "\t" + consensus;
                    })
                    .sorted()
                    .toList()).get();
            try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
                writer.write("query_id\thit_count\tavg_score\torganism\tfunction\tconsensus_dna");
                writer.newLine();
                for (String row : rows) {
                    writer.write(row);
                    writer.newLine();
                }
            }
        } finally {
            pool.shutdown();
        }
        return new TaskResult(output, "consensus DNA alignments");
    }

    private static TaskResult runDnaPars(TaskInputs inputs, NodeProfile nodeProfile) throws Exception {
        Path output = inputs.outputDirectory().resolve("dna-tree.newick");
        List<SequenceNode> sequences = readAlignmentSequences(inputs.input(0), nodeProfile, false);
        Files.writeString(output, buildTree(sequences), StandardCharsets.UTF_8);
        return new TaskResult(output, "DNA phylogenetic tree");
    }

    private static TaskResult runProtPars(TaskInputs inputs, NodeProfile nodeProfile) throws Exception {
        Path output = inputs.outputDirectory().resolve("protein-tree.newick");
        List<SequenceNode> sequences = readAlignmentSequences(inputs.input(0), nodeProfile, true);
        Files.writeString(output, buildTree(sequences), StandardCharsets.UTF_8);
        return new TaskResult(output, "protein phylogenetic tree");
    }

    private static TaskResult runDrawgram(TaskInputs inputs, NodeProfile nodeProfile) throws Exception {
        Path dot = inputs.outputDirectory().resolve("tree.dot");
        Path text = inputs.outputDirectory().resolve("tree.txt");
        String newick = Files.readString(inputs.input(0), StandardCharsets.UTF_8).trim();
        String dotGraph = "digraph tree {\n  rankdir=LR;\n  root [label=\"" + newick.replace("\"", "\\\"") + "\"];\n}\n";
        String layoutDigest = layoutDigest(newick);
        Files.writeString(dot, dotGraph, StandardCharsets.UTF_8);
        Files.writeString(text, "TREE\n====\n" + newick + System.lineSeparator()
                + "layout-digest=" + layoutDigest + System.lineSeparator(), StandardCharsets.UTF_8);
        return new TaskResult(text, "drawgram text tree");
    }

    private static TaskResult runPrepareReceptor(TaskInputs inputs, NodeProfile nodeProfile) throws Exception {
        Path output = inputs.outputDirectory().resolve("prepared-receptor.tsv");
        try (BufferedReader reader = bufferedReader(inputs.input(0), nodeProfile);
             BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            writer.write("feature_id\tx\ty\tz\tnormalized_weight");
            writer.newLine();
            String line = reader.readLine();
            double totalWeight = 0.0;
            List<String[]> rows = new ArrayList<>();
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                rows.add(parts);
                totalWeight += Double.parseDouble(parts[4]);
            }
            double denominator = Math.max(0.0001, totalWeight);
            for (String[] parts : rows) {
                double normalized = Double.parseDouble(parts[4]) / denominator;
                writer.write(parts[0] + "\t" + parts[1] + "\t" + parts[2] + "\t" + parts[3] + "\t" + String.format("%.8f", normalized));
                writer.newLine();
            }
        }
        return new TaskResult(output, "prepared receptor features");
    }

    private static TaskResult runPrepareGpf(TaskInputs inputs, NodeProfile nodeProfile) throws Exception {
        Path output = inputs.outputDirectory().resolve("grid-params.tsv");
        List<String[]> receptor = readTabular(inputs.input(0), nodeProfile, true);
        List<String[]> cells = readTabular(inputs.input(1), nodeProfile, true);
        try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            writer.write("cell_id\tcentroid_score");
            writer.newLine();
            for (String[] cell : cells) {
                double cellScore = receptor.stream()
                        .mapToDouble(feature -> Double.parseDouble(feature[4])
                                / (1.0 + distance(
                                Integer.parseInt(cell[1]), Integer.parseInt(cell[2]), Integer.parseInt(cell[3]),
                                Integer.parseInt(feature[1]), Integer.parseInt(feature[2]), Integer.parseInt(feature[3]))))
                        .sum();
                writer.write(cell[0] + "\t" + String.format("%.8f", cellScore));
                writer.newLine();
            }
        }
        return new TaskResult(output, "grid parameters");
    }

    private static TaskResult runPrepareDpf(TaskInputs inputs, NodeProfile nodeProfile) throws Exception {
        Path output = inputs.outputDirectory().resolve("dock-params.tsv");
        try (BufferedReader reader = bufferedReader(inputs.input(0), nodeProfile);
             BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            writer.write("family\tavg_polarity\tavg_flexibility");
            writer.newLine();
            String line = reader.readLine();
            Map<String, double[]> aggregates = new LinkedHashMap<>();
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                double[] values = aggregates.computeIfAbsent(parts[1], ignored -> new double[3]);
                values[0] += Double.parseDouble(parts[2]);
                values[1] += Double.parseDouble(parts[3]);
                values[2] += 1.0;
            }
            for (Map.Entry<String, double[]> entry : aggregates.entrySet()) {
                double[] values = entry.getValue();
                writer.write(entry.getKey() + "\t" + String.format("%.5f", values[0] / values[2])
                        + "\t" + String.format("%.5f", values[1] / values[2]));
                writer.newLine();
            }
        }
        return new TaskResult(output, "docking parameters");
    }

    private static TaskResult runAutogrid(TaskInputs inputs, NodeProfile nodeProfile) throws Exception {
        Path output = inputs.outputDirectory().resolve("grid-cells.tsv");
        try (BufferedReader reader = bufferedReader(inputs.input(0), nodeProfile);
             BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            writer.write("cell_id\tenergy");
            writer.newLine();
            String line = reader.readLine();
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                double energy = Math.log1p(Double.parseDouble(parts[1])) * nodeProfile.cpuThreads();
                writer.write(parts[0] + "\t" + String.format("%.6f", energy));
                writer.newLine();
            }
        }
        return new TaskResult(output, "autogrid cells");
    }

    private static TaskResult runAutodock(TaskInputs inputs, NodeProfile nodeProfile) throws Exception {
        Path output = inputs.outputDirectory().resolve("docking.tsv");
        Map<String, Double> cellEnergy = readKeyDoubleTable(inputs.input(0), nodeProfile);
        Map<String, double[]> families = readFamilyParams(inputs.input(1), nodeProfile);
        List<String[]> atoms = readTabular(inputs.input(2), nodeProfile, true);
        double totalCharge = atoms.stream().mapToDouble(row -> Double.parseDouble(row[4])).sum();
        double poseScore = 0.0;
        int index = 0;
        for (String[] atom : atoms) {
            double energy = cellEnergy.getOrDefault("cell-" + (index % Math.max(1, cellEnergy.size())), 0.1);
            poseScore += energy / (1.0 + Math.abs(Double.parseDouble(atom[4])));
            index++;
        }
        double familyBias = families.values().stream().mapToDouble(values -> values[0] + values[1]).average().orElse(0.5);
        double affinity = poseScore / Math.max(1, atoms.size()) - Math.abs(totalCharge) * 0.05 + familyBias;
        try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            writer.write("ligand_id\tatom_count\taffinity\tbest_pose");
            writer.newLine();
            writer.write(inputs.parameters().getOrDefault("ligand_id", "unknown") + "\t" + atoms.size()
                    + "\t" + String.format("%.6f", affinity)
                    + "\tpose-" + Math.max(1, atoms.size() / 3));
            writer.newLine();
        }
        return new TaskResult(output, "autodock results");
    }

    private static TaskResult runFastqSplit(TaskInputs inputs, NodeProfile nodeProfile) throws Exception {
        int chunkCount = Integer.parseInt(inputs.parameters().getOrDefault("chunk_count", "24"));
        Path manifest = inputs.outputDirectory().resolve("chunks.manifest.tsv");
        List<FastqRecord> records = readFastq(inputs.input(0), nodeProfile);
        List<List<FastqRecord>> chunks = new ArrayList<>();
        for (int i = 0; i < chunkCount; i++) {
            chunks.add(new ArrayList<>());
        }
        for (int i = 0; i < records.size(); i++) {
            chunks.get(i % chunkCount).add(records.get(i));
        }
        try (BufferedWriter writer = Files.newBufferedWriter(manifest, StandardCharsets.UTF_8)) {
            writer.write("chunk_id\tpath\trecord_count");
            writer.newLine();
            for (int i = 0; i < chunks.size(); i++) {
                String chunkId = String.format("%03d", i + 1);
                Path chunkPath = inputs.outputDirectory().resolve("chunk-" + chunkId + ".fastq");
                writeFastq(chunkPath, chunks.get(i));
                writer.write(chunkId + "\t" + chunkPath.getFileName() + "\t" + chunks.get(i).size());
                writer.newLine();
            }
        }
        return new TaskResult(manifest, "FASTQ chunk manifest");
    }

    private static TaskResult runFilterContams(TaskInputs inputs, NodeProfile nodeProfile) throws Exception {
        Path output = inputs.outputDirectory().resolve("filtered.fastq");
        String chunkId = inputs.parameters().get("chunk_id");
        Path chunkPath = manifestLookup(inputs.input(0), chunkId, nodeProfile);
        List<String> motifs = Files.readAllLines(inputs.input(1), StandardCharsets.UTF_8);
        List<FastqRecord> records = readFastq(chunkPath, nodeProfile).stream()
                .filter(record -> motifs.stream().noneMatch(record.sequence()::contains))
                .toList();
        writeFastq(output, records);
        return new TaskResult(output, "filtered FASTQ reads");
    }

    private static TaskResult runSol2Sanger(TaskInputs inputs, NodeProfile nodeProfile) throws Exception {
        Path output = inputs.outputDirectory().resolve("sanger.fastq");
        List<FastqRecord> converted = readFastq(inputs.input(0), nodeProfile).stream()
                .map(record -> new FastqRecord(
                        record.id(),
                        record.sequence(),
                        record.quality().chars()
                                .map(ch -> Math.max(33, Math.min(73, ch - 10)))
                                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                                .toString()))
                .toList();
        writeFastq(output, converted);
        return new TaskResult(output, "Sanger FASTQ reads");
    }

    private static TaskResult runFastqToBfq(TaskInputs inputs, NodeProfile nodeProfile) throws Exception {
        Path output = inputs.outputDirectory().resolve("reads.bfq.tsv");
        List<FastqRecord> records = readFastq(inputs.input(0), nodeProfile);
        try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            writer.write("read_id\tsequence\tavg_quality\tgc_ratio");
            writer.newLine();
            for (FastqRecord record : records) {
                double avgQuality = record.quality().chars().average().orElse(33.0) - 33.0;
                writer.write(record.id() + "\t" + record.sequence() + "\t"
                        + String.format("%.3f", avgQuality) + "\t"
                        + String.format("%.4f", gcRatio(record.sequence())));
                writer.newLine();
            }
        }
        return new TaskResult(output, "BFQ-like reads");
    }

    private static TaskResult runMap(TaskInputs inputs, NodeProfile nodeProfile) throws Exception {
        Path output = inputs.outputDirectory().resolve("alignments.tsv");
        List<ReferenceSequence> references = readReferences(inputs.input(1), nodeProfile);
        List<String[]> rows = readTabular(inputs.input(0), nodeProfile, true);
        ForkJoinPool pool = new ForkJoinPool(Math.max(1, nodeProfile.cpuThreads()));
        try {
            List<String> lines = pool.submit(() -> rows.parallelStream()
                    .map(parts -> {
                        Alignment best = bestAlignment(parts[0], parts[1], references);
                        return parts[0] + "\t" + best.refId + "\t" + best.position + "\t" + String.format("%.5f", best.score);
                    })
                    .toList()).get();
            try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
                writer.write("read_id\tref_id\tposition\tscore");
                writer.newLine();
                for (String line : lines) {
                    writer.write(line);
                    writer.newLine();
                }
            }
        } finally {
            pool.shutdown();
        }
        return new TaskResult(output, "mapped reads");
    }

    private static TaskResult runMapMerge(TaskInputs inputs, NodeProfile nodeProfile) throws Exception {
        Path output = inputs.outputDirectory().resolve("merged-alignments.tsv");
        List<String> lines = new ArrayList<>();
        for (Path input : inputs.inputs()) {
            try (BufferedReader reader = bufferedReader(input, nodeProfile)) {
                String line = reader.readLine();
                while ((line = reader.readLine()) != null) {
                    lines.add(line);
                }
            }
        }
        lines.sort(Comparator.naturalOrder());
        try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            writer.write("read_id\tref_id\tposition\tscore");
            writer.newLine();
            for (String line : lines) {
                writer.write(line);
                writer.newLine();
            }
        }
        return new TaskResult(output, "merged alignments");
    }

    private static TaskResult runMaqIndex(TaskInputs inputs, NodeProfile nodeProfile) throws Exception {
        Path output = inputs.outputDirectory().resolve("alignments.index.tsv");
        try (BufferedReader reader = bufferedReader(inputs.input(0), nodeProfile);
             BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            reader.readLine();
            Map<String, Integer> counts = new LinkedHashMap<>();
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                counts.merge(parts[1], 1, Integer::sum);
            }
            writer.write("ref_id\talignment_count");
            writer.newLine();
            for (Map.Entry<String, Integer> entry : counts.entrySet()) {
                writer.write(entry.getKey() + "\t" + entry.getValue());
                writer.newLine();
            }
        }
        return new TaskResult(output, "alignment index");
    }

    private static TaskResult runPileup(TaskInputs inputs, NodeProfile nodeProfile) throws Exception {
        Path output = inputs.outputDirectory().resolve("pileup.tsv");
        Map<String, Integer> positionCounts = new LinkedHashMap<>();
        try (BufferedReader reader = bufferedReader(inputs.input(0), nodeProfile)) {
            reader.readLine();
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                String key = parts[1] + ":" + parts[2];
                positionCounts.merge(key, 1, Integer::sum);
            }
        }
        try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            writer.write("locus\tdepth");
            writer.newLine();
            for (Map.Entry<String, Integer> entry : positionCounts.entrySet()) {
                writer.write(entry.getKey() + "\t" + entry.getValue());
                writer.newLine();
            }
        }
        return new TaskResult(output, "pileup summary");
    }

    private static Map<String, QueryProfile> loadQueries(Path queryFasta, NodeProfile nodeProfile) throws IOException {
        Map<String, QueryProfile> queries = new LinkedHashMap<>();
        try (BufferedReader reader = bufferedReader(queryFasta, nodeProfile)) {
            FastaRecord record;
            while ((record = nextFastaRecord(reader)) != null) {
                queries.put(record.id(), new QueryProfile(record.id(), record.metadata.getOrDefault("motif", ""), signature(record.sequence())));
            }
        }
        return queries;
    }

    private static void processBlastChunk(
            List<FastaRecord> chunk,
            Map<String, QueryProfile> queryProfiles,
            Map<String, PriorityQueue<Hit>> hitsByQuery,
            int parallelism) throws ExecutionException, InterruptedException {
        ForkJoinPool pool = new ForkJoinPool(Math.max(1, parallelism));
        try {
            List<Hit> hits = pool.submit(() -> chunk.parallelStream()
                    .map(record -> bestHit(record, queryProfiles))
                    .filter(hit -> hit != null)
                    .toList()).get();
            for (Hit hit : hits) {
                PriorityQueue<Hit> queue = hitsByQuery.get(hit.queryId);
                queue.offer(hit);
                while (queue.size() > 24) {
                    queue.poll();
                }
            }
        } finally {
            pool.shutdown();
        }
    }

    private static Hit bestHit(FastaRecord record, Map<String, QueryProfile> queryProfiles) {
        String motif = record.metadata.getOrDefault("motif", "");
        Hit best = null;
        for (QueryProfile profile : queryProfiles.values()) {
            if (!profile.motif.equals(motif) && !profile.signature.startsWith(signaturePrefix(record.sequence(), 2))) {
                continue;
            }
            double score = jaccard(profile.signature, signature(record.sequence()));
            if (best == null || score > best.score) {
                best = new Hit(profile.queryId, record.id(), score,
                        record.metadata.getOrDefault("organism", "unknown"),
                        record.metadata.getOrDefault("function", "unknown"),
                        record.sequence());
            }
        }
        return best;
    }

    private static Map<String, List<Hit>> readHits(Path path, NodeProfile nodeProfile) throws IOException {
        Map<String, List<Hit>> hits = new LinkedHashMap<>();
        try (BufferedReader reader = bufferedReader(path, nodeProfile)) {
            String line = reader.readLine();
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                Hit hit = new Hit(parts[0], parts[1], Double.parseDouble(parts[2]), parts[3], parts[4], parts[5]);
                hits.computeIfAbsent(hit.queryId, ignored -> new ArrayList<>()).add(hit);
            }
        }
        return hits;
    }

    private static List<SequenceNode> readAlignmentSequences(Path path, NodeProfile nodeProfile, boolean translate) throws IOException {
        List<SequenceNode> nodes = new ArrayList<>();
        try (BufferedReader reader = bufferedReader(path, nodeProfile)) {
            String line = reader.readLine();
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                String sequence = translate ? translate(parts[5]) : parts[5];
                nodes.add(new SequenceNode(parts[0], sequence));
            }
        }
        return nodes;
    }

    private static String buildTree(List<SequenceNode> nodes) {
        double[] averageDistances = averagePairwiseDistances(nodes);
        double stabilityBias = treeStabilityBias(nodes);
        List<String> labels = new ArrayList<>();
        for (int i = 0; i < nodes.size(); i++) {
            SequenceNode node = nodes.get(i);
            labels.add(node.name + ":" + String.format("%.4f", Math.max(0.1, averageDistances[i] + stabilityBias)));
        }
        labels.sort(Comparator.naturalOrder());
        return "(" + String.join(",", labels) + ");";
    }

    private static List<String[]> readTabular(Path path, NodeProfile nodeProfile, boolean skipHeader) throws IOException {
        List<String[]> rows = new ArrayList<>();
        try (BufferedReader reader = bufferedReader(path, nodeProfile)) {
            String line;
            if (skipHeader) {
                reader.readLine();
            }
            while ((line = reader.readLine()) != null) {
                rows.add(line.split("\t"));
            }
        }
        return rows;
    }

    private static Map<String, Double> readKeyDoubleTable(Path path, NodeProfile nodeProfile) throws IOException {
        Map<String, Double> table = new LinkedHashMap<>();
        for (String[] row : readTabular(path, nodeProfile, true)) {
            table.put(row[0], Double.parseDouble(row[1]));
        }
        return table;
    }

    private static Map<String, double[]> readFamilyParams(Path path, NodeProfile nodeProfile) throws IOException {
        Map<String, double[]> table = new LinkedHashMap<>();
        for (String[] row : readTabular(path, nodeProfile, true)) {
            table.put(row[0], new double[]{Double.parseDouble(row[1]), Double.parseDouble(row[2])});
        }
        return table;
    }

    private static Path manifestLookup(Path manifest, String chunkId, NodeProfile nodeProfile) throws IOException {
        try (BufferedReader reader = bufferedReader(manifest, nodeProfile)) {
            String line = reader.readLine();
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts[0].equals(chunkId)) {
                    Path chunkPath = Path.of(parts[1]);
                    return chunkPath.isAbsolute() ? chunkPath : manifest.getParent().resolve(chunkPath).normalize();
                }
            }
        }
        throw new IOException("Chunk " + chunkId + " not found in manifest " + manifest);
    }

    private static List<FastqRecord> readFastq(Path path, NodeProfile nodeProfile) throws IOException {
        List<FastqRecord> records = new ArrayList<>();
        try (BufferedReader reader = bufferedReader(path, nodeProfile)) {
            while (true) {
                String id = reader.readLine();
                if (id == null) {
                    return records;
                }
                String sequence = reader.readLine();
                reader.readLine();
                String quality = reader.readLine();
                records.add(new FastqRecord(id.substring(1), sequence, quality));
            }
        }
    }

    private static void writeFastq(Path output, List<FastqRecord> records) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            for (FastqRecord record : records) {
                writer.write("@" + record.id());
                writer.newLine();
                writer.write(record.sequence());
                writer.newLine();
                writer.write("+");
                writer.newLine();
                writer.write(record.quality());
                writer.newLine();
            }
        }
    }

    private static List<ReferenceSequence> readReferences(Path path, NodeProfile nodeProfile) throws IOException {
        List<ReferenceSequence> references = new ArrayList<>();
        try (BufferedReader reader = bufferedReader(path, nodeProfile)) {
            FastaRecord record;
            while ((record = nextFastaRecord(reader)) != null) {
                references.add(new ReferenceSequence(record.id(), record.sequence(), signature(record.sequence())));
            }
        }
        return references;
    }

    private static Alignment bestAlignment(String readId, String readSequence, List<ReferenceSequence> references) {
        String readSignature = signature(padSequence(readSequence, 12));
        Alignment best = new Alignment(readId, "unmapped", 0, 0.0);
        List<ReferenceSequence> candidates = new ArrayList<>();
        for (ReferenceSequence reference : references) {
            double score = jaccard(readSignature, reference.signature());
            if (candidates.size() < 4 || score >= best.score - 0.08) {
                candidates.add(reference);
            }
            if (score > best.score) {
                best = new Alignment(readId, reference.id(), Math.abs(readSequence.hashCode()) % Math.max(1, reference.sequence().length()), score);
            }
        }
        Alignment refined = best;
        for (ReferenceSequence candidate : candidates) {
            double structuralScore = 0.0;
            for (int round = 0; round < 3; round++) {
                structuralScore = Math.max(structuralScore, windowedSimilarity(readSequence, candidate.sequence()));
            }
            double totalScore = structuralScore + jaccard(readSignature, candidate.signature());
            if (totalScore > refined.score) {
                refined = new Alignment(
                        readId,
                        candidate.id(),
                        bestPosition(readSequence, candidate.sequence()),
                        totalScore);
            }
        }
        return refined;
    }

    private static String padSequence(String sequence, int minimum) {
        if (sequence.length() >= minimum) {
            return sequence;
        }
        StringBuilder builder = new StringBuilder(sequence);
        while (builder.length() < minimum) {
            builder.append(DNA[builder.length() % DNA.length]);
        }
        return builder.toString();
    }

    private static String majority(List<String> values) {
        return values.stream().collect(Collectors.groupingBy(value -> value, Collectors.counting()))
                .entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse("unknown");
    }

    private static String consensus(List<String> sequences) {
        if (sequences.isEmpty()) {
            return "";
        }
        int maxLength = sequences.stream().mapToInt(String::length).max().orElse(0);
        StringBuilder builder = new StringBuilder(maxLength);
        for (int i = 0; i < maxLength; i++) {
            int[] counts = new int[4];
            for (String sequence : sequences) {
                char base = i < sequence.length() ? sequence.charAt(i) : 'A';
                counts[baseIndex(base)]++;
            }
            builder.append("ACGT".charAt(maxIndex(counts)));
        }
        return builder.toString();
    }

    private static String translate(String dna) {
        StringBuilder protein = new StringBuilder();
        for (int i = 0; i + 2 < dna.length(); i += 3) {
            protein.append(CODON_TABLE.getOrDefault(dna.substring(i, i + 3), 'X'));
        }
        return protein.toString();
    }

    private static double gcRatio(String sequence) {
        long gc = sequence.chars().filter(ch -> ch == 'G' || ch == 'C').count();
        return sequence.isEmpty() ? 0.0 : (double) gc / sequence.length();
    }

    private static double distance(int x1, int y1, int z1, int x2, int y2, int z2) {
        return Math.sqrt(Math.pow(x1 - x2, 2) + Math.pow(y1 - y2, 2) + Math.pow(z1 - z2, 2));
    }

    private static int baseIndex(char base) {
        return switch (base) {
            case 'C' -> 1;
            case 'G' -> 2;
            case 'T' -> 3;
            default -> 0;
        };
    }

    private static int maxIndex(int[] counts) {
        int index = 0;
        for (int i = 1; i < counts.length; i++) {
            if (counts[i] > counts[index]) {
                index = i;
            }
        }
        return index;
    }

    private static String signature(String sequence) {
        Map<String, Integer> counts = new HashMap<>();
        for (int i = 0; i + 3 < sequence.length(); i += 2) {
            String kmer = sequence.substring(i, i + 4);
            counts.merge(kmer, 1, Integer::sum);
        }
        return counts.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed().thenComparing(Map.Entry::getKey))
                .limit(8)
                .map(Map.Entry::getKey)
                .collect(Collectors.joining("|"));
    }

    private static String signaturePrefix(String sequence, int length) {
        String signature = signature(sequence);
        return signature.length() <= length ? signature : signature.substring(0, length);
    }

    private static double jaccard(String left, String right) {
        List<String> a = List.of(left.split("\\|"));
        List<String> b = List.of(right.split("\\|"));
        long intersection = a.stream().filter(b::contains).count();
        long union = a.size() + b.size() - intersection;
        return union == 0 ? 0.0 : (double) intersection / union;
    }

    private static double alignmentSignal(List<String> sequences, String consensus) {
        if (sequences.isEmpty()) {
            return 0.0;
        }
        long mismatches = 0L;
        int refinementPasses = Math.max(96, Math.min(320, (sequences.size() * 14) + Math.max(8, consensus.length() / 4)));
        for (int pass = 0; pass < refinementPasses; pass++) {
            String pivot = sequences.get(pass % sequences.size());
            for (int i = 0; i < sequences.size(); i++) {
                mismatches += mismatchCount(sequences.get(i), consensus);
                mismatches += mismatchCount(sequences.get(i), pivot);
                for (int j = i + 1; j < sequences.size(); j++) {
                    mismatches += mismatchCount(sequences.get(i), sequences.get(j));
                }
            }
        }
        return mismatches / (double) (Math.max(1, sequences.size()) * 10_000L);
    }

    private static double treeStabilityBias(List<SequenceNode> nodes) {
        if (nodes.size() <= 1) {
            return 0.0;
        }
        int rounds = Math.max(4, Math.min(12, nodes.size() / 24 + 4));
        double total = 0.0;
        for (int round = 0; round < rounds; round++) {
            for (int i = 0; i < nodes.size(); i++) {
                SequenceNode left = nodes.get(i);
                SequenceNode right = nodes.get((i + round + 1) % nodes.size());
                total += normalizedDistance(left.sequence, right.sequence);
                total += motifSkew(left.sequence, right.sequence);
            }
        }
        return total / (Math.max(1, nodes.size()) * 250.0);
    }

    private static double[] averagePairwiseDistances(List<SequenceNode> nodes) {
        double[] totals = new double[nodes.size()];
        if (nodes.size() <= 1) {
            return totals;
        }
        for (int i = 0; i < nodes.size(); i++) {
            for (int j = i + 1; j < nodes.size(); j++) {
                double distance = normalizedDistance(nodes.get(i).sequence, nodes.get(j).sequence);
                totals[i] += distance;
                totals[j] += distance;
            }
        }
        double divisor = Math.max(1, nodes.size() - 1);
        for (int i = 0; i < totals.length; i++) {
            totals[i] /= divisor;
        }
        return totals;
    }

    private static double normalizedDistance(String left, String right) {
        int comparedLength = Math.max(left.length(), right.length());
        if (comparedLength == 0) {
            return 0.0;
        }
        return mismatchCount(left, right) / (double) comparedLength;
    }

    private static int mismatchCount(String left, String right) {
        int comparedLength = Math.max(left.length(), right.length());
        int mismatches = 0;
        for (int i = 0; i < comparedLength; i++) {
            char leftBase = i < left.length() ? left.charAt(i) : '-';
            char rightBase = i < right.length() ? right.charAt(i) : '-';
            if (leftBase != rightBase) {
                mismatches++;
            }
        }
        return mismatches;
    }

    private static double windowedSimilarity(String readSequence, String referenceSequence) {
        int bestMatches = 0;
        int limit = Math.max(1, referenceSequence.length() - readSequence.length() + 1);
        int stride = Math.max(1, readSequence.length() / 8);
        for (int start = 0; start < limit; start += stride) {
            int matches = 0;
            for (int i = 0; i < readSequence.length() && start + i < referenceSequence.length(); i++) {
                if (readSequence.charAt(i) == referenceSequence.charAt(start + i)) {
                    matches++;
                }
            }
            bestMatches = Math.max(bestMatches, matches);
        }
        return bestMatches / (double) Math.max(1, readSequence.length());
    }

    private static int bestPosition(String readSequence, String referenceSequence) {
        int bestPosition = 0;
        int bestMatches = -1;
        int limit = Math.max(1, referenceSequence.length() - readSequence.length() + 1);
        int stride = Math.max(1, readSequence.length() / 8);
        for (int start = 0; start < limit; start += stride) {
            int matches = 0;
            for (int i = 0; i < readSequence.length() && start + i < referenceSequence.length(); i++) {
                if (readSequence.charAt(i) == referenceSequence.charAt(start + i)) {
                    matches++;
                }
            }
            if (matches > bestMatches) {
                bestMatches = matches;
                bestPosition = start;
            }
        }
        return bestPosition;
    }

    private static FastaRecord nextFastaRecord(BufferedReader reader) throws IOException {
        String header = reader.readLine();
        if (header == null) {
            return null;
        }
        String sequence = reader.readLine();
        if (sequence == null) {
            throw new IOException("Invalid FASTA-like input; missing sequence line after header: " + header);
        }
        if (!header.startsWith(">")) {
            throw new IOException("Invalid FASTA-like input; expected '>' header but got: " + header);
        }
        String[] tokens = header.substring(1).split(" ");
        Map<String, String> metadata = new HashMap<>();
        for (int i = 1; i < tokens.length; i++) {
            String token = tokens[i];
            int separator = token.indexOf('=');
            if (separator > 0) {
                metadata.put(token.substring(0, separator), token.substring(separator + 1));
            }
        }
        return new FastaRecord(tokens[0], sequence, metadata);
    }

    private static BufferedReader bufferedReader(Path path, NodeProfile nodeProfile) throws IOException {
        return new BufferedReader(
                new InputStreamReader(Files.newInputStream(path), StandardCharsets.UTF_8),
                nodeProfile.effectiveReadBufferBytes());
    }

    private static int blastParallelism(NodeProfile nodeProfile) {
        return Math.max(1, Math.min(3, (nodeProfile.cpuThreads() + 1) / 2));
    }

    private static double motifSkew(String left, String right) {
        int limit = Math.min(left.length(), right.length());
        if (limit == 0) {
            return 0.0;
        }
        int score = 0;
        for (int i = 0; i < limit; i += 3) {
            if (left.charAt(i) == right.charAt(i)) {
                score++;
            } else {
                score--;
            }
        }
        return Math.abs(score) / (double) Math.max(1, limit / 3);
    }

    private static String layoutDigest(String newick) {
        long checksum = 0L;
        int rounds = Math.max(256, Math.min(2048, newick.length() * 24));
        for (int round = 0; round < rounds; round++) {
            for (int i = 0; i < newick.length(); i++) {
                checksum = (checksum * 131L) + newick.charAt((i + round) % newick.length());
                checksum ^= (checksum >>> 11);
            }
        }
        return Long.toHexString(checksum);
    }

    private static Map<String, Character> buildCodonTable() {
        Map<String, Character> table = new HashMap<>();
        table.put("TTT", 'F');
        table.put("TTC", 'F');
        table.put("TTA", 'L');
        table.put("TTG", 'L');
        table.put("CTT", 'L');
        table.put("CTC", 'L');
        table.put("CTA", 'L');
        table.put("CTG", 'L');
        table.put("ATT", 'I');
        table.put("ATC", 'I');
        table.put("ATA", 'I');
        table.put("ATG", 'M');
        table.put("GTT", 'V');
        table.put("GTC", 'V');
        table.put("GTA", 'V');
        table.put("GTG", 'V');
        table.put("TCT", 'S');
        table.put("TCC", 'S');
        table.put("TCA", 'S');
        table.put("TCG", 'S');
        table.put("CCT", 'P');
        table.put("CCC", 'P');
        table.put("CCA", 'P');
        table.put("CCG", 'P');
        table.put("ACT", 'T');
        table.put("ACC", 'T');
        table.put("ACA", 'T');
        table.put("ACG", 'T');
        table.put("GCT", 'A');
        table.put("GCC", 'A');
        table.put("GCA", 'A');
        table.put("GCG", 'A');
        table.put("TAT", 'Y');
        table.put("TAC", 'Y');
        table.put("CAT", 'H');
        table.put("CAC", 'H');
        table.put("CAA", 'Q');
        table.put("CAG", 'Q');
        table.put("AAT", 'N');
        table.put("AAC", 'N');
        table.put("AAA", 'K');
        table.put("AAG", 'K');
        table.put("GAT", 'D');
        table.put("GAC", 'D');
        table.put("GAA", 'E');
        table.put("GAG", 'E');
        table.put("TGT", 'C');
        table.put("TGC", 'C');
        table.put("TGG", 'W');
        table.put("CGT", 'R');
        table.put("CGC", 'R');
        table.put("CGA", 'R');
        table.put("CGG", 'R');
        table.put("AGT", 'S');
        table.put("AGC", 'S');
        table.put("AGA", 'R');
        table.put("AGG", 'R');
        table.put("GGT", 'G');
        table.put("GGC", 'G');
        table.put("GGA", 'G');
        table.put("GGG", 'G');
        return table;
    }

    private record FastaRecord(String id, String sequence, Map<String, String> metadata) {
    }

    private record QueryProfile(String queryId, String motif, String signature) {
    }

    private record Hit(String queryId, String refId, double score, String organism, String function, String sequence) {
    }

    private record SequenceNode(String name, String sequence) {
    }

    private record FastqRecord(String id, String sequence, String quality) {
    }

    private record ReferenceSequence(String id, String sequence, String signature) {
    }

    private record Alignment(String readId, String refId, int position, double score) {
    }
}
