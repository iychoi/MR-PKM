package edu.arizona.cs.mrpkm.kmeridx;

import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class KmerIndexChunkInfoHelper {
    private final static String OUTPUT_NAME_SUFFIX = "kidx_info";
    
    public static String makeKmerIndexChunkInfoFileName(Path inputFileName) {
        return makeKmerIndexChunkInfoFileName(inputFileName.getName());
    }
    
    public static String makeKmerIndexChunkInfoFileName(String inputFileName) {
        return inputFileName + "." + OUTPUT_NAME_SUFFIX;
    }
}
