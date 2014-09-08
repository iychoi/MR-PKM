package edu.arizona.cs.mrpkm.readididx;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
public class ReadIDIndexChecker extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(ReadIDIndexChecker.class);
    
    private static class ReadIDIndexChecker_Cmd_Args {
        @Option(name = "-h", aliases = "--help", usage = "print this message") 
        private boolean help = false;
        
        @Argument(metaVar = "input-path", usage = "input-path")
        private String inputPath = null;
        
        public boolean isHelp() {
            return this.help;
        }
        
        public String getInputPath() {
            return inputPath;
        }
        
        @Override
        public String toString() {
            return "help = " + this.help + "\n" +
                    "inputPath = " + this.inputPath;
        }
    }
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ReadIDIndexChecker(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // parse command line
        ReadIDIndexChecker_Cmd_Args cmdargs = new ReadIDIndexChecker_Cmd_Args();
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
        
        String indexPathString = cmdargs.getInputPath();
        if(indexPathString == null || indexPathString.isEmpty()) {
            parser.printUsage(System.err);
            return 1;
        }
        
        // configuration
        Configuration conf = this.getConf();
        
        Path indexPath = new Path(indexPathString);
        FileSystem fs = indexPath.getFileSystem(conf);
        
        ReadIDIndexReader reader = new ReadIDIndexReader(fs, indexPathString, conf);
        
        LOG.info("ReadID Index File : " + reader.getIndexPath());
        
        LongWritable key = new LongWritable();
        IntWritable val = new IntWritable();
        int count = 0;
        while(reader.next(key, val)) {
            count++;
        }
        
        reader.reset();
        
        LOG.info("Total # of ReadID Index Entries : " + count);
        LOG.info("Entry Info");
        
        while(reader.next(key, val)) {
            LOG.info("> " + key.get() + " - " + val.get());
        }
        
        reader.close();
        return 0;
    }
}