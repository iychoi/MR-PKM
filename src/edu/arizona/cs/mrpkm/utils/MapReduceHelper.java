package edu.arizona.cs.mrpkm.utils;

import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class MapReduceHelper {
    public static String getNameFromMapReduceOutput(Path mapreduceOutputPath) {
        return getNameFromMapReduceOutput(mapreduceOutputPath.getName());
    }
    
    public static String getNameFromMapReduceOutput(String mapreduceOutputName) {
        int midx = mapreduceOutputName.indexOf("-m-");
        if(midx > 0) {
            return mapreduceOutputName.substring(0, midx);
        }
        
        int ridx = mapreduceOutputName.indexOf("-r-");
        if(ridx > 0) {
            return mapreduceOutputName.substring(0, ridx);
        }
        
        return mapreduceOutputName;
    }
    
    public static int getMapReduceID(Path mapreduceOutputName) {
        return getMapReduceID(mapreduceOutputName.getName());
    }
    
    public static int getMapReduceID(String mapreduceOutputName) {
        int midx = mapreduceOutputName.indexOf("-m-");
        if(midx > 0) {
            return Integer.parseInt(mapreduceOutputName.substring(midx + 3));
        }
        
        int ridx = mapreduceOutputName.indexOf("-r-");
        if(ridx > 0) {
            return Integer.parseInt(mapreduceOutputName.substring(ridx + 3));
        }
        
        return 0;
    }
    
    public static boolean isLogFiles(Path path) {
        if(path.getName().equals("_SUCCESS")) {
            return true;
        } else if(path.getName().equals("_logs")) {
            return true;
        } else if(path.getName().startsWith("part-r-")) {
            return true;
        } else if(path.getName().startsWith("part-m-")) {
            return true;
        }
        return false;
    }
}
