package edu.arizona.cs.mrpkm.statistics;

import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class KmerStatisticsHelper {
    private final static String OUTPUT_NAME_SUFFIX = "stddv";
    private final static String COUNTER_GROUP_NAME_UNIQUE = "KmerStatisticsUnique";
    private final static String COUNTER_GROUP_NAME_TOTAL = "KmerStatisticsTotal";
    private final static String COUNTER_GROUP_NAME_SQUARE = "KmerStatisticsSquare";
    
    public static String makeStdDeviationFileName(Path inputFileName) {
        return makeStdDeviationFileName(inputFileName.getName());
    }
    
    public static String makeStdDeviationFileName(String inputFileName) {
        return inputFileName + "." + OUTPUT_NAME_SUFFIX;
    }
    
    public static String getCounterGroupNameUnique() {
        return COUNTER_GROUP_NAME_UNIQUE;
    }
    
    public static String getCounterGroupNameTotal() {
        return COUNTER_GROUP_NAME_TOTAL;
    }

    static String getCounterGroupNameSquare() {
        return COUNTER_GROUP_NAME_SQUARE;
    }
}
