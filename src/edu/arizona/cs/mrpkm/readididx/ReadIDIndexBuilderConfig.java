package edu.arizona.cs.mrpkm.readididx;

import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author iychoi
 */
public class ReadIDIndexBuilderConfig {
    private final static String CONF_KMER_HISTOGRAM_OUTPUT_PATH = "edu.arizona.cs.mrpkm.readididx.histogram_path";
    private final static String CONF_KMER_SIZE = "edu.arizona.cs.mrpkm.readididx.kmer_size";
    
    private int kmerSize;
    private String histogramOutputPath;
    
    public ReadIDIndexBuilderConfig() {
    }
    
    public void setKmerSize(int kmerSize) {
        this.kmerSize = kmerSize;
    }
    
    public int getKmerSize() {
        return this.kmerSize;
    }

    public void setHistogramOutputPath(String outputPath) {
        this.histogramOutputPath = outputPath;
    }
    
    public String getHistogramOutputPath() {
        return this.histogramOutputPath;
    }
    
    public void saveTo(Configuration conf) {
        conf.setInt(CONF_KMER_SIZE, this.kmerSize);
        conf.set(CONF_KMER_HISTOGRAM_OUTPUT_PATH, this.histogramOutputPath);
    }
    
    public void loadFrom(Configuration conf) {
        this.kmerSize = conf.getInt(CONF_KMER_SIZE, 0);
        this.histogramOutputPath = conf.get(CONF_KMER_HISTOGRAM_OUTPUT_PATH, null);
    }
}
