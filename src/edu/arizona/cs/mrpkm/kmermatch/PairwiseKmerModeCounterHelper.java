package edu.arizona.cs.mrpkm.kmermatch;

import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class PairwiseKmerModeCounterHelper {
    private final static String CONF_MATCHFILTER_MIN = "edu.arizona.cs.mrpkm.pairwisekmermodecounter.matchfilter.min";
    private final static String CONF_MATCHFILTER_MAX = "edu.arizona.cs.mrpkm.pairwisekmermodecounter.matchfilter.max";
    
    // named output
    private final static String CONF_NAMED_OUTPUT_ID_PREFIX = "edu.arizona.cs.mrpkm.pairwisekmermodecounter.named_output_id_";
    private final static String CONF_NAMED_OUTPUT_NAME_PREFIX = "edu.arizona.cs.mrpkm.pairwisekmermodecounter.named_output_name_";
    
    public static String getConfigurationKeyOfMatchFilterMin() {
        return CONF_MATCHFILTER_MIN;
    }
    
    public static String getConfigurationKeyOfMatchFilterMax() {
        return CONF_MATCHFILTER_MAX;
    }
    
    public static String getConfigurationKeyOfNamedOutputName(int id) {
        return CONF_NAMED_OUTPUT_NAME_PREFIX + id;
    }
    
    public static String getConfigurationKeyOfNamedOutputID(String namedOutputName) {
        return CONF_NAMED_OUTPUT_ID_PREFIX + namedOutputName;
    }
    
    public static String getPairwiseModeCounterOutputName(Path filePath1, Path filePath2) {
        return getPairwiseModeCounterOutputName(filePath1.getName(), filePath2.getName());
    }
    
    public static String getPairwiseModeCounterOutputName(String fileName1, String fileName2) {
        return fileName1 + "-" + fileName2;
    }
}
