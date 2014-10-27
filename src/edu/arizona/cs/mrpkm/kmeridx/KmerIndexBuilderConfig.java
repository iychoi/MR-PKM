package edu.arizona.cs.mrpkm.kmeridx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class KmerIndexBuilderConfig {
    private final static String CONF_KMER_INDEX_BUILDER_KMER_SIZE = "edu.arizona.cs.mrpkm.kmeridx.kmer_size";
    private final static String CONF_KMER_INDEX_BUILDER_READID_INDEX_PATH = "edu.arizona.cs.mrpkm.kmeridx.readididx_path";
    
    private int kmerSize;
    private String readIDIndexPath;
    
    public void setKmerSize(int kmerSize) {
        this.kmerSize = kmerSize;
    }
    
    public int getKmerSize() {
        return this.kmerSize;
    }
    
    public void setReadIDIndexPath(String path) {
        this.readIDIndexPath = path;
    }
    
    public String getReadIDIndexPath() {
        return this.readIDIndexPath;
    }
    
    public void saveTo(Configuration conf) {
        conf.setInt(CONF_KMER_INDEX_BUILDER_KMER_SIZE, this.kmerSize);
        conf.set(CONF_KMER_INDEX_BUILDER_READID_INDEX_PATH, this.readIDIndexPath);
    }
    
    public void loadFrom(Configuration conf) {
        this.kmerSize = conf.getInt(CONF_KMER_INDEX_BUILDER_KMER_SIZE, 0);
        this.readIDIndexPath = conf.get(CONF_KMER_INDEX_BUILDER_READID_INDEX_PATH);
    }
}
