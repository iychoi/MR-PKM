package edu.arizona.cs.mrpkm.kmeridx;

import edu.arizona.cs.mrpkm.augment.BloomMapFileOutputFormat;
import edu.arizona.cs.mrpkm.readididx.*;
import edu.arizona.cs.mrpkm.cmdparams.PKMCmdParams;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

/**
 *
 * @author iychoi
 */
public class KmerIndexCmdParams extends PKMCmdParams {
    
    @Option(name = "-g", aliases = "--group", usage = "specify group size")
    private int groupSize = 1;
    
    public int getGroupSize() {
        return this.groupSize;
    }
    private Class outputFormat = MapFileOutputFormat.class;
        
    @Option(name = "-f", aliases = "--outputformat", usage = "specify output format")
    public void setOutputFormat(String outputFormat) throws Exception {
        if (outputFormat.equalsIgnoreCase(MapFileOutputFormat.class.getName())) {
            this.outputFormat = MapFileOutputFormat.class;
        } else if (outputFormat.equalsIgnoreCase(BloomMapFileOutputFormat.class.getName())) {
            this.outputFormat = BloomMapFileOutputFormat.class;
        } else if (outputFormat.equalsIgnoreCase("map") || outputFormat.equalsIgnoreCase("mapfile")) {
            this.outputFormat = MapFileOutputFormat.class;
        } else if (outputFormat.equalsIgnoreCase("bloom") || outputFormat.equalsIgnoreCase("bloommap") || outputFormat.equalsIgnoreCase("bloommapfile")) {
            this.outputFormat = BloomMapFileOutputFormat.class;
        } else {
            throw new Exception("given arg is not in correct data type");
        }
    }
    
    public Class getOutputFormat() {
        return this.outputFormat;
    }
    
    @Option(name = "-i", aliases = "--readidpath", required = true, usage = "specify ReadID index path")
    private String ridPath = null;
        
    public String getReadIDIndexPath() {
        return this.ridPath;
    }
    
    @Argument(metaVar = "input-path [input-path ...] output-path", usage = "input-paths and output-path")
    private List<String> paths = new ArrayList<String>();

    public String getOutputPath() {
        if(this.paths.size() > 1) {
            return this.paths.get(this.paths.size()-1);
        }
        return null;
    }

    public String[] getInputPaths() {
        if(this.paths.isEmpty()) {
            return new String[0];
        }

        String[] inpaths = new String[this.paths.size()-1];
        for(int i=0;i<this.paths.size()-1;i++) {
            inpaths[i] = this.paths.get(i);
        }
        return inpaths;
    }

    public String getCommaSeparatedInputPath() {
        String[] inputPaths = getInputPaths();
        StringBuilder CSInputPath = new StringBuilder();
        for(String inputpath : inputPaths) {
            if(CSInputPath.length() != 0) {
                CSInputPath.append(",");
            }
            CSInputPath.append(inputpath);
        }
        return CSInputPath.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(String arg : this.paths) {
            if(sb.length() != 0) {
                sb.append(", ");
            }

            sb.append(arg);
        }

        return "paths = " + sb.toString();
    }

    @Override
    public boolean checkValidity() {
        if(!super.checkValidity()) {
           return false;
        }
        
        if(this.outputFormat == null || this.ridPath == null || this.ridPath.isEmpty()) {
            return false;
        }
        
        if(this.paths == null || this.paths.isEmpty() || this.paths.size() < 2) {
            return false;
        }
        return true;
    }
}
