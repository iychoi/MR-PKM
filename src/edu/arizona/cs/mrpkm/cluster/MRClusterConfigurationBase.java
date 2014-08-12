package edu.arizona.cs.mrpkm.cluster;

import edu.arizona.cs.mrpkm.utils.ClassHelper;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author iychoi
 */
public abstract class MRClusterConfigurationBase {
    
    private static String[] SEARCH_PACKAGES = {
        "edu.arizona.cs.mrpkm.cluster"
    };
    
    public static MRClusterConfigurationBase findConfiguration(String configurationName) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        if(configurationName.equalsIgnoreCase("default")) {
            configurationName = "Default";
        }
        
        Class clazz = ClassHelper.findClass("MRClusterConfiguration_" + configurationName, SEARCH_PACKAGES);
        return (MRClusterConfigurationBase) ClassHelper.getClassInstance(clazz);
    }
    
    public void setConfiguration(Configuration conf) {
        // set memory
        if(getMapReduceChildMemSize() != 0) {
            String memsize = getMapReduceChildMemSize() + "M";
            conf.set("mapred.child.java.opts", "-Xms" + memsize + " -Xmx" + memsize);
        }
        
        if(getMapReduceFileBufferSize() != 0) {
            conf.setInt("io.file.buffer.size", getMapReduceFileBufferSize());
        }
    }
    
    public abstract int getCoresPerMachine();
    public abstract int getReducerNumber(int nodes);
    public abstract int getMapReduceChildMemSize();
    public abstract int getMapReduceFileBufferSize();
}
