package edu.arizona.cs.mrpkm.kmeridx;

import edu.arizona.cs.mrpkm.utils.FileSystemHelper;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class KmerIndexHelper {
    public static String[] generateNamedOutputStrings(Path[] inputPaths) {
        String[] namedOutputs = new String[inputPaths.length];
        for(int i=0;i<inputPaths.length;i++) {
            namedOutputs[i] = generateNamedOutputString(inputPaths[i]);
        }
        return namedOutputs;
    }
    
    public static String generateNamedOutputString(Path inputPath) {
        return FileSystemHelper.getSafeNamedOutput(inputPath.getName() + KmerIndexConstants.NAMED_OUTPUT_NAME_SUFFIX);
    }
    
    public static String generateNamedOutputString(String inputPath) {
        int lastDir = inputPath.lastIndexOf("/");
        if(lastDir >= 0) {
            return FileSystemHelper.getSafeNamedOutput(inputPath.substring(lastDir + 1) + KmerIndexConstants.NAMED_OUTPUT_NAME_SUFFIX);
        }
        return FileSystemHelper.getSafeNamedOutput(inputPath + KmerIndexConstants.NAMED_OUTPUT_NAME_SUFFIX);
    }
}
