package edu.arizona.cs.mrpkm.readididx;

import edu.arizona.cs.mrpkm.types.MultiFileOffsetWritable;
import edu.arizona.cs.mrpkm.fastareader.types.FastaRead;
import java.io.IOException;
import java.util.Hashtable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author iychoi
 */
public class ReadIDIndexBuilderMapper extends Mapper<LongWritable, FastaRead, MultiFileOffsetWritable, NullWritable> {
    
    private static final Log LOG = LogFactory.getLog(ReadIDIndexBuilderMapper.class);
    
    private Hashtable<String, Integer> namedOutputIDCache;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.namedOutputIDCache = new Hashtable<String, Integer>();
        int numberOfOutputs = context.getConfiguration().getInt(ReadIDIndexHelper.getConfigurationKeyOfNamedOutputNum(), -1);
        if(numberOfOutputs <= 0) {
            throw new IOException("number of outputs is zero or negative");
        }
    }
    
    @Override
    protected void map(LongWritable key, FastaRead value, Context context) throws IOException, InterruptedException {
        Integer namedoutputID = this.namedOutputIDCache.get(value.getFileName());
        if (namedoutputID == null) {
            namedoutputID = context.getConfiguration().getInt(ReadIDIndexHelper.getConfigurationKeyOfNamedOutputID(value.getFileName()), -1);
            if (namedoutputID < 0) {
                throw new IOException("No named output found : " + ReadIDIndexHelper.getConfigurationKeyOfNamedOutputID(value.getFileName()));
            }
            this.namedOutputIDCache.put(value.getFileName(), namedoutputID);
        }

        context.write(new MultiFileOffsetWritable(namedoutputID, value.getReadOffset()), NullWritable.get());
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        this.namedOutputIDCache = null;
    }
}
