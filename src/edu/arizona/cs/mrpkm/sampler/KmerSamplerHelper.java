package edu.arizona.cs.mrpkm.sampler;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class KmerSamplerHelper {
    private final static String OUTPUT_NAME_SUFFIX = "smpl";
    
    private final static String SAMPLINGPATH_EXP = ".+\\." + OUTPUT_NAME_SUFFIX + "$";
    private final static Pattern SAMPLINGPATH_PATTERN = Pattern.compile(SAMPLINGPATH_EXP);
    
    public static String makeSamplingFileName(String inputFileName) {
        return inputFileName + "." + OUTPUT_NAME_SUFFIX;
    }
    
    public static String getInputFileName(String filename) {
        int idx = filename.lastIndexOf("." + OUTPUT_NAME_SUFFIX);
        if(idx > 0) {
            return filename.substring(0, idx);
        }
        return filename;
    }
    
    public static boolean isSamplingFile(Path path) {
        return isSamplingFile(path.getName());
    }
    
    public static boolean isSamplingFile(String path) {
        Matcher matcher = SAMPLINGPATH_PATTERN.matcher(path.toLowerCase());
        if(matcher.matches()) {
            return true;
        }
        return false;
    }
}
