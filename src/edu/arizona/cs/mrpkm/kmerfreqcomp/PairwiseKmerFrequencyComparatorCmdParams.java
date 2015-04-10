package edu.arizona.cs.mrpkm.kmerfreqcomp;

import edu.arizona.cs.mrpkm.cmdparams.PKMCmdParamsBase;
import java.util.ArrayList;
import java.util.List;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

/**
 *
 * @author iychoi
 */
public class PairwiseKmerFrequencyComparatorCmdParams extends PKMCmdParamsBase {
    
    private static final int DEFAULT_PARTITIONS = 1000;
    private static final int PARTITIONS_PER_CORE = 10;

    private int partitions = DEFAULT_PARTITIONS;
    private boolean partitionsGivenByUser = false;
    
    @Option(name = "-p", aliases = "--partitions", usage = "specify the number of partitions a hadoop scheduler will split input files into")
    public void setPartitions(int partitions) {
        this.partitionsGivenByUser = true;
        this.partitions = partitions;
    }
    
    public int getPartitions() {
        return this.partitions;
    }
    
     public int getPartitions(int cores) {
        if(this.partitionsGivenByUser) {
            return this.partitions;
        } else {
            return cores * PARTITIONS_PER_CORE;
        }
    }
    
    @Option(name = "-s", aliases = "--histogrampath", required = true, usage = "specify Histogram path")
    private String histogramPath = null;
    
    public String getHistogramPath() {
        return this.histogramPath;
    }
    
    @Option(name = "-f", aliases = "--statistics", required = true, usage = "specify statistics path")
    private String statisticsPath = null;
        
    public String getStatisticsPath() {
        return this.statisticsPath;
    }
    
    @Option(name = "-u", aliases = "--chunkinfopath", required = true, usage = "specify k-mer index chunk info path")
    private String kmerIndexChunkInfoPath = null;
        
    public String getKmerIndexChunkInfoPath() {
        return this.kmerIndexChunkInfoPath;
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
        
        if(this.partitions <= 0) {
            return false;
        }
        
        if(this.histogramPath == null || this.histogramPath.isEmpty()) {
            return false;
        }
        
        if(this.statisticsPath == null || this.statisticsPath.isEmpty()) {
            return false;
        }
        
        if(this.kmerIndexChunkInfoPath == null || this.kmerIndexChunkInfoPath.isEmpty()) {
            return false;
        }
        
        if(this.paths == null || this.paths.isEmpty() || this.paths.size() < 2) {
            return false;
        }
        return true;
    }
}
