package edu.arizona.cs.mrpkm.kmeridx;

import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author iychoi
 */
public class KmerIndexInputFormatConfig {
    private final static String CONF_KMER_INDEX_INPUT_FORMAT_KMER_SIZE = "edu.arizona.cs.mrpkm.kmeridx.kmer_size";
    private final static String CONF_KMER_INDEX_CHUNK_INFO_PATH = "edu.arizona.cs.mrpkm.kmeridx.kmeridx_chunkinfo";
    
    private int kmerSize;
    private String kmerIndexChunkInfoPath;
    
    public void setKmerSize(int kmerSize) {
        this.kmerSize = kmerSize;
    }
    
    public int getKmerSize() {
        return this.kmerSize;
    }
    
    public void setKmerIndexChunkInfoPath(String kmerIndexChunkInfoPath) {
        this.kmerIndexChunkInfoPath = kmerIndexChunkInfoPath;
    }
    
    public String getKmerIndexChunkInfoPath() {
        return this.kmerIndexChunkInfoPath;
    }
    
    public void saveTo(Configuration conf) {
        conf.setInt(CONF_KMER_INDEX_INPUT_FORMAT_KMER_SIZE, this.kmerSize);
        conf.set(CONF_KMER_INDEX_CHUNK_INFO_PATH, this.kmerIndexChunkInfoPath);
    }
    
    public void loadFrom(Configuration conf) {
        this.kmerSize = conf.getInt(CONF_KMER_INDEX_INPUT_FORMAT_KMER_SIZE, 0);
        this.kmerIndexChunkInfoPath = conf.get(CONF_KMER_INDEX_CHUNK_INFO_PATH);
    }
}
