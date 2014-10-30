package edu.arizona.cs.mrpkm.kmermatch;

import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author iychoi
 */
public class KmerMatchInputFormatConfig {
    private final static String CONF_KMER_MATCH_INPUT_FORMAT_KMER_SIZE = "edu.arizona.cs.mrpkm.kmermatch.kmer_size";
    private final static String CONF_KMER_MATCH_INPUT_FORMAT_PARTITION_NUM = "edu.arizona.cs.mrpkm.kmermatch.partition_num";
    private final static String CONF_KMER_MATCH_INPUT_FORMAT_STDDEVIATION_FILTER_PATH = "edu.arizona.cs.mrpkm.kmermatch.stddeviationfilter_path";
    
    private int kmerSize;
    private int partitions;
    private String stdDeviationFilterPath;
    
    public void setKmerSize(int kmerSize) {
        this.kmerSize = kmerSize;
    }
    
    public int getKmerSize() {
        return this.kmerSize;
    }
    
    public void setPartitionNum(int partitions) {
        this.partitions = partitions;
    }
    
    public int getPartitionNum() {
        return this.partitions;
    }
    
    public void setStdDeviationFilterPath(String path) {
        this.stdDeviationFilterPath = path;
    }
    
    public String getStdDeviationFilterPath() {
        return this.stdDeviationFilterPath;
    }
    
    public void saveTo(Configuration conf) {
        conf.setInt(CONF_KMER_MATCH_INPUT_FORMAT_KMER_SIZE, this.kmerSize);
        conf.setInt(CONF_KMER_MATCH_INPUT_FORMAT_PARTITION_NUM, this.partitions);
        conf.set(CONF_KMER_MATCH_INPUT_FORMAT_STDDEVIATION_FILTER_PATH, this.stdDeviationFilterPath);
    }
    
    public void loadFrom(Configuration conf) {
        this.kmerSize = conf.getInt(CONF_KMER_MATCH_INPUT_FORMAT_KMER_SIZE, 0);
        this.partitions = conf.getInt(CONF_KMER_MATCH_INPUT_FORMAT_PARTITION_NUM, 0);
        this.stdDeviationFilterPath = conf.get(CONF_KMER_MATCH_INPUT_FORMAT_STDDEVIATION_FILTER_PATH);
    }
}
