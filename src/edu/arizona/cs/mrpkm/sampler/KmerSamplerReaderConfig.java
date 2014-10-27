package edu.arizona.cs.mrpkm.sampler;

import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author iychoi
 */
public class KmerSamplerReaderConfig {
    private final static String CONF_KMER_SAMPLER_INPUT_PATH = "edu.arizona.cs.mrpkm.kmersampler.input_path";
    
    private String inputPath;
    
    public KmerSamplerReaderConfig() {
    }
    
    public void setInputPath(String inputPath) {
        this.inputPath = inputPath;
    }
    
    public String getInputPath() {
        return this.inputPath;
    }
    
    public void saveTo(Configuration conf) {
        conf.set(CONF_KMER_SAMPLER_INPUT_PATH, this.inputPath);
    }
    
    public void loadFrom(Configuration conf) {
        this.inputPath = conf.get(CONF_KMER_SAMPLER_INPUT_PATH, null);
    }
}
