package edu.arizona.cs.mrpkm.histogram;

import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author iychoi
 */
public class KmerHistogramWriterConfig {
    private final static String CONF_KMER_HISTOGRAM_OUTPUT_PATH = "edu.arizona.cs.mrpkm.histogram.output_path";
    private final static String CONF_KMER_HISTOGRAM_KMER_SIZE = "edu.arizona.cs.mrpkm.histogram.kmer_size";
    
    private int kmerSize;
    private String outputPath;
    
    public KmerHistogramWriterConfig() {
    }
    
    public void setKmerSize(int kmerSize) {
        this.kmerSize = kmerSize;
    }
    
    public int getKmerSize() {
        return this.kmerSize;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }
    
    public String getOutputPath() {
        return this.outputPath;
    }
    
    public void saveTo(Configuration conf) {
        conf.setInt(CONF_KMER_HISTOGRAM_KMER_SIZE, this.kmerSize);
        conf.set(CONF_KMER_HISTOGRAM_OUTPUT_PATH, this.outputPath);
    }
    
    public void loadFrom(Configuration conf) {
        this.kmerSize = conf.getInt(CONF_KMER_HISTOGRAM_KMER_SIZE, 0);
        this.outputPath = conf.get(CONF_KMER_HISTOGRAM_OUTPUT_PATH, null);
    }
}
