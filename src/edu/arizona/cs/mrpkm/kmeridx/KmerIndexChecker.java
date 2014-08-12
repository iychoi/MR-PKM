package edu.arizona.cs.mrpkm.kmeridx;

import edu.arizona.cs.mrpkm.commandline.ArgumentParseException;
import edu.arizona.cs.mrpkm.commandline.ArgumentParserBase;
import edu.arizona.cs.mrpkm.commandline.CommandLineArgumentParser;
import edu.arizona.cs.mrpkm.commandline.HelpArgumentParser;
import edu.arizona.cs.mrpkm.commandline.MultiPathArgumentParser;
import edu.arizona.cs.mrpkm.types.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.CompressedSequenceWritable;
import edu.arizona.cs.mrpkm.utils.FileSystemHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author iychoi
 */
public class KmerIndexChecker extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(KmerIndexChecker.class);
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new KmerIndexChecker(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        
        String indexPathStrings[] = null;
        
        // parse command line
        HelpArgumentParser helpParser = new HelpArgumentParser();
        MultiPathArgumentParser pathParser = new MultiPathArgumentParser();
        
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
                indexPathStrings = pathParser.getValue();
            }
        }
        
        Path indexPath = new Path(indexPathStrings[0]);
        FileSystem fs = indexPath.getFileSystem(conf);
        
        KmerIndexReader reader = new KmerIndexReader(fs, indexPathStrings, conf);
        
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