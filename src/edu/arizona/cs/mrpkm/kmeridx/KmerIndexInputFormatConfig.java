package edu.arizona.cs.mrpkm.kmeridx;

import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author iychoi
 */
public class KmerIndexInputFormatConfig {
    private final static String CONF_KMER_INDEX_INPUT_FORMAT_KMER_SIZE = "edu.arizona.cs.mrpkm.kmeridx.kmer_size";
    
    private int kmerSize;
    
    public void setKmerSize(int kmerSize) {
        this.kmerSize = kmerSize;
    }
    
    public int getKmerSize() {
        return this.kmerSize;
    }
    
    public void saveTo(Configuration conf) {
        conf.setInt(CONF_KMER_INDEX_INPUT_FORMAT_KMER_SIZE, this.kmerSize);
    }
    
    public void loadFrom(Configuration conf) {
        this.kmerSize = conf.getInt(CONF_KMER_INDEX_INPUT_FORMAT_KMER_SIZE, 0);
    }
}
