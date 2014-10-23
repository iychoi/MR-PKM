package edu.arizona.cs.mrpkm.kmermatch;

/**
 *
 * @author iychoi
 */
public class KmerMatchHelper {
    private final static String CONF_KMER_SIZE = "edu.arizona.cs.mrpkm.kmermatch.kmer_size";
    private final static String CONF_PARTITIONER_MODE = "edu.arizona.cs.mrpkm.kmermatch.partitioner.mode";
    private final static String CONF_NUM_PARTITIONS = "edu.arizona.cs.mrpkm.kmermatch.partitioner.num_partitions";
    private final static String NUM_INPUT_FILES = "mapreduce.input.num.files";
    
    public static String getConfigurationKeyOfNumPartitions() {
        return CONF_NUM_PARTITIONS;
    }
    
    public static String getConfigurationPartitionerMode() {
        return CONF_PARTITIONER_MODE;
    }
    
    public static String getConfigurationKeyOfKmerSize() {
        return CONF_KMER_SIZE;
    }
    
    public static String getConfigurationKeyOfInputFileNum() {
        return NUM_INPUT_FILES;
    }
}
