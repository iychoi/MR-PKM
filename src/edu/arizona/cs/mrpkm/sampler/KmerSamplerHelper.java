package edu.arizona.cs.mrpkm.sampler;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class KmerSamplerHelper {
    // named output
    private final static String CONF_OUTPUT_PATH = "edu.arizona.cs.mrpkm.sampler.output_path";
    private final static String CONF_KMER_SIZE = "edu.arizona.cs.mrpkm.sampler.kmer_size";
    
    private final static String OUTPUT_NAME_SUFFIX = "smpl";
    
    private final static String INDEXPATH_EXP = ".+\\." + OUTPUT_NAME_SUFFIX + "$";
    private final static Pattern INDEXPATH_PATTERN = Pattern.compile(INDEXPATH_EXP);
    
    public static String getConfigurationKeyOfOutputPath() {
        return CONF_OUTPUT_PATH;
    }
    
    public static String getConfigurationKeyOfKmerSize() {
        return CONF_KMER_SIZE;
    }
    
    
    public static String getSamplingFileName(String inputFileName) {
        return inputFileName + "." + OUTPUT_NAME_SUFFIX;
    }
    
    public static String getFileNameWithoutExtension(String inputFileName) {
        int idx = inputFileName.lastIndexOf("." + OUTPUT_NAME_SUFFIX);
        if(idx > 0) {
            return inputFileName.substring(0, idx);
        }
        return inputFileName;
    }
    
    public static boolean isSamplingFile(Path path) {
        Matcher matcher = INDEXPATH_PATTERN.matcher(path.getName().toLowerCase());
        if(matcher.matches()) {
            return true;
        }
        return false;
    }
}
