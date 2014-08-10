package edu.arizona.cs.mrpkm.readididx;

import edu.arizona.cs.mrpkm.types.MultiFileOffsetWritable;
import java.io.IOException;
import java.util.Hashtable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 *
 * @author iychoi
 */
public class ReadIDIndexBuilderReducer extends Reducer<MultiFileOffsetWritable, NullWritable, LongWritable, IntWritable> {
    
    private static final Log LOG = LogFactory.getLog(ReadIDIndexBuilderReducer.class);
    
    private MultipleOutputs mos;
    private Hashtable<Integer, String> namedOutputCache;
    private int[] readIDs;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.mos = new MultipleOutputs(context);
        this.namedOutputCache = new Hashtable<Integer, String>();
        int numberOfOutputs = context.getConfiguration().getInt(ReadIDIndexHelper.getConfigurationKeyOfNamedOutputNum(), -1);
        if(numberOfOutputs <= 0) {
            throw new IOException("number of outputs is zero or negative");
        }
        
        this.readIDs = new int[numberOfOutputs];
        for(int i=0;i<this.readIDs.length;i++) {
            this.readIDs[i] = 0;
        }
    }
    
    @Override
    protected void reduce(MultiFileOffsetWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        int namedoutputID = key.getFileID();
        long offset = key.getOffset();
        
        this.readIDs[namedoutputID]++;
        
        String namedOutput = this.namedOutputCache.get(namedoutputID);
        if (namedOutput == null) {
            String[] namedOutputs = context.getConfiguration().getStrings(ReadIDIndexHelper.getConfigurationKeyOfNamedOutputName(namedoutputID));
            if (namedOutputs.length != 1) {
                throw new IOException("no named output found");
            }
            namedOutput = namedOutputs[0];
            this.namedOutputCache.put(namedoutputID, namedOutput);
        }
        
        this.mos.write(namedOutput, new LongWritable(offset), new IntWritable(this.readIDs[namedoutputID]));
        //context.write(key, new Text(sb.toString()));
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        this.mos.close();
        this.namedOutputCache = null;
    }
}
