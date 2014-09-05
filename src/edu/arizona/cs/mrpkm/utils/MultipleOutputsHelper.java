package edu.arizona.cs.mrpkm.utils;

import edu.arizona.cs.hadoop.fs.irods.output.HirodsMultipleOutputs;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 *
 * @author iychoi
 */
public class MultipleOutputsHelper {
    
    private static final Log LOG = LogFactory.getLog(MultipleOutputsHelper.class);
    
    public static final String CONF_MULTIPLE_OUTPUTS_CLASS = "edu.arizona.cs.mrpkm.multipleoutputs.class";
    
    public static boolean isMultipleOutputs(Configuration conf) {
        return getMultipleOutputsClassString(conf).equals(MultipleOutputs.class.toString());
    }
    
    public static boolean isHirodsMultipleOutputs(Configuration conf) {
        return getMultipleOutputsClassString(conf).equals(HirodsMultipleOutputs.class.toString());
    }
    
    private static String getMultipleOutputsClassString(Configuration conf) {
        return conf.get(CONF_MULTIPLE_OUTPUTS_CLASS, "");
    }
    
    public static void setMultipleOutputsClass(Configuration conf, Class<?> clazz) {
        conf.set(CONF_MULTIPLE_OUTPUTS_CLASS, clazz.toString());
    }
}
