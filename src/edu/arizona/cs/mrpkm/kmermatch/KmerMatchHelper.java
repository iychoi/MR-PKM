package edu.arizona.cs.mrpkm.kmermatch;

/**
 *
 * @author iychoi
 */
public class KmerMatchHelper {
    private final static String CONF_KMER_SIZE = "edu.arizona.cs.mrpkm.kmermatch.kmer_size";
    private static final String CONF_NUM_SLICES = "edu.arizona.cs.mrpkm.kmermatch.slices";
    private static final String NUM_INPUT_FILES = "mapreduce.input.num.files";
    
    public static String getConfigurationKeyOfSliceNum() {
        return CONF_NUM_SLICES;
    }
    
    public static String getConfigurationKeyOfKmerSize() {
        return CONF_KMER_SIZE;
    }
    
    public static String getConfigurationKeyOfInputFileNum() {
        return NUM_INPUT_FILES;
    }
}
