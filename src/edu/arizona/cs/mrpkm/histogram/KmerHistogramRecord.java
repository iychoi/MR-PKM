package edu.arizona.cs.mrpkm.histogram;

/**
 *
 * @author iychoi
 */
public class KmerHistogramRecord implements Comparable<KmerHistogramRecord> {
    private String key;
    private long count;
    
    public KmerHistogramRecord(String key, long count) {
        this.key = key;
        this.count = count;
    }
    
    public String getKey() {
        return this.key;
    }
    
    public long getCount() {
        return this.count;
    }

    @Override
    public int compareTo(KmerHistogramRecord right) {
        return this.key.compareTo(right.key);
    }

    public void increaseCount() {
        this.count++;
    }
}
