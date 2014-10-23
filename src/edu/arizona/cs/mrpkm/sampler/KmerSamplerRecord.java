package edu.arizona.cs.mrpkm.sampler;

/**
 *
 * @author iychoi
 */
public class KmerSamplerRecord implements Comparable<KmerSamplerRecord> {
    private String key;
    private long count;
    
    public KmerSamplerRecord(String key, long count) {
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
    public int compareTo(KmerSamplerRecord right) {
        return this.key.compareTo(right.key);
    }

    public void increaseCount() {
        this.count++;
    }
}
