package edu.arizona.cs.mrpkm.kmermatch;

import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class PairwiseKmerModeCounterHelper {
    private final static String OUTPUT_NAME_SUFFIX = "pkm";
    
    public static String getPairwiseModeCounterOutputName(Path filePath1, Path filePath2) {
        return getPairwiseModeCounterOutputName(filePath1.getName(), filePath2.getName());
    }
    
    public static String getPairwiseModeCounterOutputName(String fileName1, String fileName2) {
        return fileName1 + "-" + fileName2;
    }
    
    public static String makePairwiseModeCountFileName(String filename) {
        return filename + "." + OUTPUT_NAME_SUFFIX;
    }
}
