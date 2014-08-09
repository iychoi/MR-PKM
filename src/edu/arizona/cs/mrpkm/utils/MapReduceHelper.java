package edu.arizona.cs.mrpkm.utils;

import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class MapReduceHelper {
    public static String getNameFromReduceOutput(Path reduceOutputPath) {
        return getNameFromReduceOutput(reduceOutputPath.getName());
    }
    
    public static String getNameFromReduceOutput(String reduceOutputName) {
        int index = reduceOutputName.indexOf("-r-");
        if(index > 0) {
            return reduceOutputName.substring(0, index);
        }
        return reduceOutputName;
    }
    
    public static int getReduceID(Path reduceOutputPath) {
        return getReduceID(reduceOutputPath.getName());
    }
    
    public static int getReduceID(String reduceOutputName) {
        int index = reduceOutputName.indexOf("-r-");
        if(index > 0) {
            return Integer.parseInt(reduceOutputName.substring(index + 3));
        }
        return 0;
    }
    
    public static boolean isLogFiles(Path path) {
        if(path.getName().equals("_SUCCESS")) {
            return true;
        } else if(path.getName().startsWith("part-r-")) {
            return true;
        }
        return false;
    }
}
