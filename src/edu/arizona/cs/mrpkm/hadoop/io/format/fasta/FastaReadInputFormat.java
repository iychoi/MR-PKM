package edu.arizona.cs.mrpkm.hadoop.io.format.fasta;

import edu.arizona.cs.mrpkm.types.fasta.FastaRead;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 *
 * @author iychoi
 */
public class FastaReadInputFormat extends FileInputFormat<LongWritable, FastaRead> {

    private static final Log LOG = LogFactory.getLog(FastaReadInputFormat.class);
    
    private final static String CONF_SPLITABLE = "edu.arizona.cs.mrpkm.hadoop.io.format.fasta.splitable";
    
    @Override
    public RecordReader<LongWritable, FastaRead> createRecordReader(InputSplit split,
            TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new FastaReadReader();
    }
    
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        boolean splitable = FastaReadInputFormat.isSplitable(context.getConfiguration());
        LOG.info("splitable = " + splitable);
        if(!splitable) {
            return false;
        }
        
        CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(filename);
        if(codec != null) {
            return false;
        }
        
        return true;
    }
    
    public static void setSplitable(Configuration conf, boolean splitable) {
        conf.setBoolean(CONF_SPLITABLE, splitable);
    }
    
    public static boolean isSplitable(Configuration conf) {
        return conf.getBoolean(CONF_SPLITABLE, true);
    }
}