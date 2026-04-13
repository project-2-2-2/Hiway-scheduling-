package org.wsh.model;

public enum TaskType {
    BLAST("hits.tsv", "blast hits", "compute"),
    CLUSTAL("alignment.tsv", "consensus DNA alignments", "compute"),
    DNAPARS("dna-tree.newick", "DNA phylogenetic tree", "compute"),
    PROTPARS("protein-tree.newick", "protein phylogenetic tree", "io"),
    DRAWGRAM("tree.txt", "drawgram text tree", "io"),
    PREPARE_RECEPTOR("prepared-receptor.tsv", "prepared receptor features", "compute"),
    PREPARE_GPF("grid-params.tsv", "grid parameters", "io"),
    PREPARE_DPF("dock-params.tsv", "docking parameters", "io"),
    AUTOGRID("grid-cells.tsv", "autogrid cells", "compute"),
    AUTODOCK("docking.tsv", "autodock results", "compute"),
    FASTQ_SPLIT("chunks.manifest.tsv", "FASTQ chunk manifest", "io"),
    FILTER_CONTAMS("filtered.fastq", "filtered FASTQ reads", "io"),
    SOL2SANGER("sanger.fastq", "Sanger FASTQ reads", "io"),
    FASTQ_TO_BFQ("reads.bfq.tsv", "BFQ-like reads", "compute"),
    MAP("alignments.tsv", "mapped reads", "compute"),
    MAP_MERGE("merged-alignments.tsv", "merged alignments", "io"),
    MAQ_INDEX("alignments.index.tsv", "alignment index", "io"),
    PILEUP("pileup.tsv", "pileup summary", "compute");

    private final String outputFileName;
    private final String outputDescription;
    private final String defaultClassification;

    TaskType(String outputFileName, String outputDescription, String defaultClassification) {
        this.outputFileName = outputFileName;
        this.outputDescription = outputDescription;
        this.defaultClassification = defaultClassification;
    }

    public String outputFileName() {
        return outputFileName;
    }

    public String outputDescription() {
        return outputDescription;
    }

    public String defaultClassification() {
        return defaultClassification;
    }
}
