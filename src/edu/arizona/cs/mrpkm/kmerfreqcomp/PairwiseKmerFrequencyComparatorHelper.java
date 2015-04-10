package edu.arizona.cs.mrpkm.kmerfreqcomp;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class PairwiseKmerFrequencyComparatorHelper {
    private final static String TOC_OUTPUT_NAME = "toc.json";
    private final static String OUTPUT_NAME_PREFIX = "diff";
    private final static String OUTPUT_NAME_SUFFIX = "kfc";
    
    private final static String OUTPUTPATH_EXP = ".+\\." + OUTPUT_NAME_SUFFIX + "\\.\\d+$";
    private final static Pattern OUTPUTPATH_PATTERN = Pattern.compile(OUTPUTPATH_EXP);
    
    public static boolean isPairwiseKmerFrequencyComparisonTOCFile(Path path) {
        return isPairwiseKmerFrequencyComparisonTOCFile(path.getName());
    }
    
    public static boolean isPairwiseKmerFrequencyComparisonTOCFile(String path) {
        if(path.compareToIgnoreCase(TOC_OUTPUT_NAME) == 0) {
            return true;
        }
        return false;
    }
    
    public static boolean isPairwiseKmerFrequencyComparisonFile(Path path) {
        return isPairwiseKmerFrequencyComparisonFile(path.getName());
    }
    
    public static boolean isPairwiseKmerFrequencyComparisonFile(String path) {
        Matcher matcher = OUTPUTPATH_PATTERN.matcher(path.toLowerCase());
        if(matcher.matches()) {
            return true;
        }
        return false;
    }
    
    public static String makePairwiseKmerFrequencyComparisonTOCFileName() {
        return TOC_OUTPUT_NAME;
    }
    
    public static String makePairwiseKmerFrequencyComparisonFileName(int mapreduceID) {
        return OUTPUT_NAME_PREFIX + "." + OUTPUT_NAME_SUFFIX + "." + mapreduceID;
    }
}    