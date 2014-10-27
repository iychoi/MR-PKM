package edu.arizona.cs.mrpkm.cluster;

import edu.arizona.cs.mrpkm.utils.ClassHelper;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author iychoi
 */
public abstract class AMRClusterConfiguration {
    
    private static String[] SEARCH_PACKAGES = {
        "edu.arizona.cs.mrpkm.cluster"
    };
    
    public static AMRClusterConfiguration findConfiguration(String configurationName) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        Class clazz = ClassHelper.findClass("MRClusterConfiguration_" + configurationName.toLowerCase(), SEARCH_PACKAGES);
        return (AMRClusterConfiguration) ClassHelper.getClassInstance(clazz);
    }
    
    public void configureClusterParamsTo(Configuration conf) {
        // set memory
        if(!isMapReduce2()) {
            if(getMapReduceChildMemSize() != 0) {
                String memsize = getMapReduceChildMemSize() + "M";
                conf.set("mapred.child.java.opts", "-Xms" + memsize + " -Xmx" + memsize);
            }

            if(getMapReduceFileBufferSize() != 0) {
                conf.setInt("io.file.buffer.size", getMapReduceFileBufferSize());
            }
        }
    }
    
    public abstract int getCoresPerMachine();
    public abstract int getReadIndexBuilderReducerNumber(int nodes);
    public abstract int getKmerIndexBuilderReducerNumber(int nodes);
    public abstract int getPairwiseKmerModeCounterReducerNumber(int nodes);
    public abstract int getMapReduceChildMemSize();
    public abstract int getMapReduceFileBufferSize();
    public abstract boolean isMapReduce2();
}
