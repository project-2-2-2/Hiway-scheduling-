package org.wsh.data;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;

public final class DataGenerator {
    private static final char[] DNA = {'A', 'C', 'G', 'T'};
    private static final List<String> ORGANISMS = List.of(
            "human", "mouse", "yeast", "zebrafish", "ecoli", "arabidopsis", "influenza", "salmonella");
    private static final List<String> FUNCTIONS = List.of(
            "signal", "repair", "metabolism", "transporter", "receptor", "kinase", "polymerase", "ligase");

    private final Random random;

    public DataGenerator(long seed) {
        this.random = new Random(seed);
    }

    public void generate(Path dataRoot, int queryCount, int referenceRecordsPerShard, int sequenceLength, int trainingFractionPercent)
            throws IOException {
        Files.createDirectories(dataRoot);
        Files.createDirectories(dataRoot.resolve("training"));
        List<String> motifs = buildMotifs(256, 12);
        writeFasta(dataRoot.resolve("query.fasta"), queryCount, sequenceLength, motifs, true);
        writeFasta(dataRoot.resolve("reference-a.fasta"), referenceRecordsPerShard, sequenceLength, motifs, false);
        writeFasta(dataRoot.resolve("reference-b.fasta"), referenceRecordsPerShard, sequenceLength, motifs, false);
        int trainingQueries = Math.min(queryCount, Math.max(24, (queryCount * trainingFractionPercent) / 100));
        int trainingRefs = Math.min(referenceRecordsPerShard, Math.max(1_024, (referenceRecordsPerShard * trainingFractionPercent) / 100));
        copySubset(dataRoot.resolve("query.fasta"), dataRoot.resolve("training/query-sample.fasta"), trainingQueries);
        copySubset(dataRoot.resolve("reference-a.fasta"), dataRoot.resolve("training/reference-a-sample.fasta"), trainingRefs);
        copySubset(dataRoot.resolve("reference-b.fasta"), dataRoot.resolve("training/reference-b-sample.fasta"), trainingRefs);
    }

    private void writeFasta(Path output, int recordCount, int sequenceLength, List<String> motifs, boolean queryMode)
            throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            for (int i = 0; i < recordCount; i++) {
                String motif = motifs.get(i % motifs.size());
                String sequence = sequenceWithMotif(sequenceLength, motif, queryMode && i % 7 == 0);
                String organism = ORGANISMS.get(i % ORGANISMS.size());
                String function = FUNCTIONS.get((i / 3) % FUNCTIONS.size());
                writer.write(">seq-" + String.format("%07d", i)
                        + " organism=" + organism
                        + " function=" + function
                        + " motif=" + motif);
                writer.newLine();
                writer.write(sequence);
                writer.newLine();
            }
        }
    }

    private void copySubset(Path input, Path output, int recordLimit) throws IOException {
        try (var lines = Files.lines(input, StandardCharsets.UTF_8);
             BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
            int linesToWrite = recordLimit * 2;
            int[] counter = {0};
            lines.limit(linesToWrite).forEach(line -> {
                try {
                    writer.write(line);
                    writer.newLine();
                    counter[0]++;
                } catch (IOException exception) {
                    throw new RuntimeException(exception);
                }
            });
        } catch (RuntimeException exception) {
            if (exception.getCause() instanceof IOException ioException) {
                throw ioException;
            }
            throw exception;
        }
    }

    private List<String> buildMotifs(int count, int motifLength) {
        return java.util.stream.IntStream.range(0, count)
                .mapToObj(ignored -> randomDna(motifLength))
                .toList();
    }

    private String sequenceWithMotif(int sequenceLength, String motif, boolean reverse) {
        char[] sequence = randomDna(sequenceLength).toCharArray();
        int start = random.nextInt(Math.max(1, sequenceLength - motif.length()));
        String effectiveMotif = reverse ? new StringBuilder(motif).reverse().toString() : motif;
        for (int i = 0; i < effectiveMotif.length(); i++) {
            sequence[start + i] = effectiveMotif.charAt(i);
        }
        return new String(sequence);
    }

    private String randomDna(int length) {
        StringBuilder builder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            builder.append(DNA[random.nextInt(DNA.length)]);
        }
        return builder.toString();
    }
}
