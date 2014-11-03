package edu.arizona.cs.mrpkm.readididx;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class KmerHistogramHelper {
    private final static String OUTPUT_NAME_SUFFIX = "histo";
    
    private final static String HISTOGRAMPATH_EXP = ".+\\." + OUTPUT_NAME_SUFFIX + "$";
    private final static Pattern HISTOGRAMPATH_PATTERN = Pattern.compile(HISTOGRAMPATH_EXP);
    
    public static String makeHistogramFileName(String inputFileName) {
        return inputFileName + "." + OUTPUT_NAME_SUFFIX;
    }
    
    public static String getInputFileName(String filename) {
        int idx = filename.lastIndexOf("." + OUTPUT_NAME_SUFFIX);
        if(idx > 0) {
            return filename.substring(0, idx);
        }
        return filename;
    }
    
    public static boolean isHistogramFile(Path path) {
        return isHistogramFile(path.getName());
    }
    
    public static boolean isHistogramFile(String path) {
        Matcher matcher = HISTOGRAMPATH_PATTERN.matcher(path.toLowerCase());
        if(matcher.matches()) {
            return true;
        }
        return false;
    }
}
