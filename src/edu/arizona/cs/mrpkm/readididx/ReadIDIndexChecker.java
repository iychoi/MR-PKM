package edu.arizona.cs.mrpkm.readididx;

import edu.arizona.cs.mrpkm.commandline.ArgumentParseException;
import edu.arizona.cs.mrpkm.commandline.ArgumentParserBase;
import edu.arizona.cs.mrpkm.commandline.CommandLineArgumentParser;
import edu.arizona.cs.mrpkm.commandline.HelpArgumentParser;
import edu.arizona.cs.mrpkm.commandline.SinglePathArgumentParser;
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

/**
 *
 * @author iychoi
 */
public class ReadIDIndexChecker extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(ReadIDIndexChecker.class);
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ReadIDIndexChecker(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        
        String indexPathString = null;
        
        // parse command line
        HelpArgumentParser helpParser = new HelpArgumentParser();
        SinglePathArgumentParser pathParser = new SinglePathArgumentParser();
        
        CommandLineArgumentParser parser = new CommandLineArgumentParser();
        parser.addArgumentParser(helpParser);
        parser.addArgumentParser(pathParser);
        ArgumentParserBase[] parsers = null;
        try {
            parsers = parser.parse(args);
        } catch(ArgumentParseException ex) {
            System.err.println(ex);
            return -1;
        }
        
        for(ArgumentParserBase base : parsers) {
            if(base == helpParser) {
                if(helpParser.getValue()) {
                    printHelp(parser);
                    return 0;
                }
            } else if(base == pathParser) {
                indexPathString = pathParser.getValue();
            }
        }
        
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

    private void printHelp(CommandLineArgumentParser parser) {
        System.out.println(parser.getHelpMessage());
    }
}