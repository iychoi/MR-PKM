package edu.arizona.cs.mrpkm.hadoop.io.format.fasta.types;

/**
 *
 * @author iychoi
 */
public class FastaRawReadLine {
    private long lineOffset;
    private String line;

    public FastaRawReadLine(long lineOffset, String line) {
        this.lineOffset = lineOffset;
        this.line = line;
    }

    public long getLineOffset() {
        return this.lineOffset;
    }

    public String getLine() {
        return this.line;
    }
}
