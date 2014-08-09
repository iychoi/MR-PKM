package edu.arizona.cs.mrpkm.fastareader.types;

/**
 *
 * @author iychoi
 */
public class FastaRead {
    private String filename;
    private long read_offset;
    private String description;
    private String sequence;
    private boolean continuous_read = false;
    
    public FastaRead(String filename) {
        this.filename = filename;
    }
    
    public FastaRead(FastaRawRead rawRead) {
        this.filename = rawRead.getFileName();
        this.read_offset = rawRead.getReadOffset();
        this.description = rawRead.getDescription();
        String pureSequence = new String();
        for (int i = 0; i < rawRead.getRawSequence().length; i++) {
            pureSequence += rawRead.getRawSequence()[i].getLine();
        }
        this.sequence = pureSequence;
        this.continuous_read = rawRead.getContinuousRead();
    }

    public String getFileName() {
        return this.filename;
    }
    
    public void setReadOffset(long offset) {
        this.read_offset = offset;
    }
    
    public long getReadOffset() {
        return this.read_offset;
    }
    
    public void setDescription(String description) {
        this.description = description;
    }
    
    public String getDescription() {
        return this.description;
    }
    
    public void setSequence(String sequence) {
        this.sequence = sequence;
    }
    
    public String getSequence() {
        return this.sequence;
    }
    
    public void setContinuousRead(boolean continuous_read) {
        this.continuous_read = continuous_read;
    }
    
    public boolean getContinuousRead() {
        return this.continuous_read;
    }
}
