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
        
        String indexPathString = args[0];
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