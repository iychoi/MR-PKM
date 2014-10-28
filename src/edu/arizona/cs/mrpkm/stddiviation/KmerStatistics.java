package edu.arizona.cs.mrpkm.stddiviation;

/**
 *
 * @author iychoi
 */
public class KmerStatistics {
    
    private String filename;
    private long uniqueOccurance = 0;
    private long totalOccurance = 0;
    
    public KmerStatistics(String filename) {
        this.filename = filename;
    }
    
    public KmerStatistics(String filename, long uniqueOccurance, long totalOccurance) {
        this.filename = filename;
        this.uniqueOccurance = uniqueOccurance;
        this.totalOccurance = totalOccurance;
    }
    
    public String getFilename() {
        return this.filename;
    }
    
    public long getUniqueOccurance() {
        return this.uniqueOccurance;
    }
    
    public void setUniqueOccurance(long uniqueOccurance) {
        this.uniqueOccurance = uniqueOccurance;
    }
    
    public long getTotalOccurance() {
        return this.totalOccurance;
    }
    
    public void setTotalOccurance(long totalOccurance) {
        this.totalOccurance = totalOccurance;
    }
}
