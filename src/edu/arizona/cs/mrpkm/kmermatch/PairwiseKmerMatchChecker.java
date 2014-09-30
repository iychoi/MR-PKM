package edu.arizona.cs.mrpkm.kmermatch;

import edu.arizona.cs.mrpkm.kmeridx.*;
import edu.arizona.cs.mrpkm.types.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.CompressedSequenceWritable;
import edu.arizona.cs.mrpkm.utils.FileSystemHelper;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
 *
 * @author iychoi
 */
public class PairwiseKmerMatchChecker extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(PairwiseKmerMatchChecker.class);
    
    private static class PairwiseKmerMatchChecker_Cmd_Args {
        @Option(name = "-h", aliases = "--help", usage = "print this message") 
        private boolean help = false;
        
        @Argument(metaVar = "input-path [input-path ...]", usage = "input-paths")
        private List<String> inputPath = new ArrayList<String>();
        
        public boolean isHelp() {
            return this.help;
        }
        
        public String[] getInputPaths() {
            if(this.inputPath.isEmpty()) {
                return new String[0];
            }
            
            return this.inputPath.toArray(new String[0]);
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
            for(String arg : this.inputPath) {
                if(sb.length() != 0) {
                    sb.append(", ");
                }
                
                sb.append(arg);
            }
            
            return "help = " + this.help + "\n" +
                    "paths = " + sb.toString();
        }
        
        public boolean checkValidity() {
            if(this.inputPath == null || this.inputPath.isEmpty()) {
                return false;
            }
            return true;
        }
    }
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PairwiseKmerMatchChecker(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // parse command line
        PairwiseKmerMatchChecker_Cmd_Args cmdargs = new PairwiseKmerMatchChecker_Cmd_Args();
        CmdLineParser parser = new CmdLineParser(cmdargs);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            // handling of wrong arguments
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
        }
        
        if(cmdargs.isHelp() || !cmdargs.checkValidity()) {
            parser.printUsage(System.err);
            return 1;
        }
        
        String indexPathStrings[] = cmdargs.getInputPaths();
        
        // configuration
        Configuration conf = this.getConf();
        
        Path[] indexPaths = FileSystemHelper.makePathFromString(indexPathStrings);
        // check kmerSize
        int kmerSize = -1;
        for(Path indexPath : indexPaths) {
            if(kmerSize <= 0) {
                kmerSize = KmerIndexHelper.getKmerSize(indexPath);
            } else {
                if(kmerSize != KmerIndexHelper.getKmerSize(indexPath)) {
                    throw new Exception("kmer sizes of given index files are different");
                }
            }
        }
        
        KmerSequenceSlicer slicer = new KmerSequenceSlicer(kmerSize, 1);
        KmerSequenceSlice slices[] = slicer.getSlices();
        KmerSequenceSlice slice = slices[0];
        KmerLinearMatcher matcher = new KmerLinearMatcher(indexPaths, slice, conf);
        
        LOG.info("Kmer Index Files : " + FileSystemHelper.makeCommaSeparated(indexPathStrings));
        LOG.info("Matches");
        while(matcher.nextMatch()) {
            KmerMatchResult currentMatch = matcher.getCurrentMatch();
            CompressedSequenceWritable key = currentMatch.getKey();
            CompressedIntArrayWritable[] vals = currentMatch.getVals();
            String[][] indice = currentMatch.getIndexPaths();
            
            LOG.info("> " + key.getSequence() + " in " + vals.length + " files");
            StringBuilder sb = new StringBuilder();
            for(String[] indice1 : indice) {
                if(sb.length() != 0) {
                    sb.append(", ");
                }
                sb.append(KmerIndexHelper.getFastaFileName(indice1[0]));
            }
            LOG.info(">> " + sb.toString());
        }
        
        matcher.close();
        return 0;
    }
}