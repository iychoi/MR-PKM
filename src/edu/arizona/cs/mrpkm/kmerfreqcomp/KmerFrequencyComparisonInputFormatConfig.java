package edu.arizona.cs.mrpkm.kmerfreqcomp;

import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author iychoi
 */
public class KmerFrequencyComparisonInputFormatConfig {
    private final static String CONF_KMER_SIZE = "edu.arizona.cs.mrpkm.kmerfreqcomp.kmer_size";
    private final static String CONF_INDEX_CHUNK_INFO_PATH = "edu.arizona.cs.mrpkm.kmerfreqcomp.kmeridx_chunkinfo";
    private final static String CONF_PARTITION_NUM = "edu.arizona.cs.mrpkm.kmerfreqcomp.partition_num";
    private final static String CONF_HISTOGRAM_PATH = "edu.arizona.cs.mrpkm.kmerfreqcomp.histogram_path";
    
    private int kmerSize;
    private String kmerIndexChunkInfoPath;
    private int partitions;
    private String histogramPath;
    
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
    
    public void setHistogramPath(String path) {
        this.histogramPath = path;
    }
    
    public String getHistogramPath() {
        return histogramPath;
    }
    
    public void setPartitionNum(int partitions) {
        this.partitions = partitions;
    }
    
    public int getPartitionNum() {
        return this.partitions;
    }
    
    public void saveTo(Configuration conf) {
        conf.setInt(CONF_KMER_SIZE, this.kmerSize);
        conf.set(CONF_INDEX_CHUNK_INFO_PATH, this.kmerIndexChunkInfoPath);
        conf.setInt(CONF_PARTITION_NUM, this.partitions);
        conf.set(CONF_HISTOGRAM_PATH, this.histogramPath);
    }
    
    public void loadFrom(Configuration conf) {
        this.kmerSize = conf.getInt(CONF_KMER_SIZE, 0);
        this.kmerIndexChunkInfoPath = conf.get(CONF_INDEX_CHUNK_INFO_PATH);
        this.partitions = conf.getInt(CONF_PARTITION_NUM, 0);
        this.histogramPath = conf.get(CONF_HISTOGRAM_PATH);
    }
}
