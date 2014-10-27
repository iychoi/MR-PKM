package edu.arizona.cs.mrpkm.readididx;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class ReadIDIndexHelper {
    private final static String OUTPUT_NAME_SUFFIX = "ridx";
    
    private final static String INDEXPATH_EXP = ".+\\." + OUTPUT_NAME_SUFFIX + "$";
    private final static Pattern INDEXPATH_PATTERN = Pattern.compile(INDEXPATH_EXP);
    
    public static String makeReadIDIndexFileName(String filename) {
        return filename + "." + OUTPUT_NAME_SUFFIX;
    }
    
    public static boolean isReadIDIndexFile(Path path) {
        return isReadIDIndexFile(path.getName());
    }
    
    public static boolean isReadIDIndexFile(String path) {
        Matcher matcher = INDEXPATH_PATTERN.matcher(path.toLowerCase());
        if(matcher.matches()) {
            return true;
        }
        return false;
    }
}
