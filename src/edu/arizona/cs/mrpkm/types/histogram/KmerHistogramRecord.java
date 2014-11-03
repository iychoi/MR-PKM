package edu.arizona.cs.mrpkm.types.histogram;

/**
 *
 * @author iychoi
 */
public class KmerHistogramRecord implements Comparable<KmerHistogramRecord> {
    private String kmer;
    private long count;
    
    public KmerHistogramRecord(String key, long count) {
        this.kmer = key;
        this.count = count;
    }
    
    public String getKmer() {
        return this.kmer;
    }
    
    public long getCount() {
        return this.count;
    }
    
    public void increaseCount(long count) {
        this.count += count;
    }
    
    public void increaseCount() {
        this.count++;
    }

    @Override
    public int compareTo(KmerHistogramRecord right) {
        return this.kmer.compareToIgnoreCase(right.kmer);
    }
}
