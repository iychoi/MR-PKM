package edu.arizona.cs.mrpkm.readididx;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class ReadIDIndexHelper {
    // named output
    private final static String CONF_NAMED_OUTPUT_NUM = "edu.arizona.cs.mrpkm.readididx.named_outputs";
    private final static String CONF_NAMED_OUTPUT_NAME_PREFIX = "edu.arizona.cs.mrpkm.readididx.named_output_id_";
    private final static String CONF_NAMED_OUTPUT_ID_PREFIX = "edu.arizona.cs.mrpkm.readididx.named_output_name_";
    
    private final static String OUTPUT_NAME_SUFFIX = "ridx";
    
    private final static String INDEXPATH_EXP = ".+\\." + OUTPUT_NAME_SUFFIX + "$";
    private final static Pattern INDEXPATH_PATTERN = Pattern.compile(INDEXPATH_EXP);
    
    public static String getConfigurationKeyOfNamedOutputNum() {
        return CONF_NAMED_OUTPUT_NUM;
    }
    
    public static String getConfigurationKeyOfNamedOutputName(int id) {
        return CONF_NAMED_OUTPUT_NAME_PREFIX + id;
    }
    
    public static String getConfigurationKeyOfNamedOutputID(String namedOutputName) {
        return CONF_NAMED_OUTPUT_ID_PREFIX + namedOutputName;
    }
    
    public static String getReadIDIndexFileName(String inputFileName) {
        return inputFileName + "." + OUTPUT_NAME_SUFFIX;
    }
    
    public static boolean isReadIDIndexFile(Path path) {
        Matcher matcher = INDEXPATH_PATTERN.matcher(path.getName().toLowerCase());
        if(matcher.matches()) {
            return true;
        }
        return false;
    }
}
