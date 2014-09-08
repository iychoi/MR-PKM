package edu.arizona.cs.mrpkm.kmeridx;

import edu.arizona.cs.mrpkm.commandline.CommandLineArgumentParser;
import edu.arizona.cs.mrpkm.types.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.CompressedSequenceWritable;
import edu.arizona.cs.mrpkm.utils.FileSystemHelper;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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
public class KmerIndexChecker extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(KmerIndexChecker.class);
    
    private static class KmerIndexChecker_Cmd_Args {
        @Option(name = "-h", aliases = "--help", usage = "print this message") 
        private boolean help = false;
        
        @Argument(metaVar = "input-path [input-path ...]", usage = "input-paths")
        private List<String> paths = new ArrayList<String>();
        
        public boolean isHelp() {
            return this.help;
        }
        
        public String[] getInputPaths() {
            if(this.paths.isEmpty()) {
                return new String[0];
            }
            
            return this.paths.toArray(new String[0]);
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
            
            return "help = " + this.help + "\n" +
                    "paths = " + sb.toString();
        }
    }
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new KmerIndexChecker(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // parse command line
        KmerIndexChecker_Cmd_Args cmdargs = new KmerIndexChecker_Cmd_Args();
        CmdLineParser parser = new CmdLineParser(cmdargs);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            // handling of wrong arguments
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
        }
        
        if(cmdargs.isHelp()) {
            parser.printUsage(System.err);
            return 1;
        }
        
        String indexPathStrings[] = cmdargs.getInputPaths();
        if(indexPathStrings == null || indexPathStrings.length == 0) {
            parser.printUsage(System.err);
            return 1;
        }

        // configuration
        Configuration conf = this.getConf();
        
        Path indexPath = new Path(indexPathStrings[0]);
        FileSystem fs = indexPath.getFileSystem(conf);
        
        MultiKmerIndexReader reader = new MultiKmerIndexReader(fs, indexPathStrings, conf);
        
        LOG.info("Kmer Index Files : " + FileSystemHelper.makeCommaSeparated(reader.getIndexPaths()));
        
        CompressedSequenceWritable key = new CompressedSequenceWritable();
        CompressedIntArrayWritable val = new CompressedIntArrayWritable();
        int count = 0;
        while(reader.next(key, val)) {
            count++;
        }
        
        LOG.info("Total # of Kmer Index Entries : " + count);
        LOG.info("Entry Info");
        
        reader.reset();
        
        while(reader.next(key, val)) {
            LOG.info("> " + key.getSequence() + " : " + val.toString());
        }
        
        reader.close();
        return 0;
    }

    private void printHelp(CommandLineArgumentParser parser) {
        System.out.println(parser.getHelpMessage());
    }
}