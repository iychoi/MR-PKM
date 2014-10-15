package edu.arizona.cs.mrpkm.readididx;

import edu.arizona.cs.hadoop.fs.irods.output.HirodsMultipleOutputs;
import edu.arizona.cs.mrpkm.types.CompressedLongArrayWritable;
import edu.arizona.cs.mrpkm.types.MultiFileOffsetWritable;
import edu.arizona.cs.mrpkm.utils.MultipleOutputsHelper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 *
 * @author iychoi
 */
public class ReadIDIndexBuilderReducer extends Reducer<MultiFileOffsetWritable, CompressedLongArrayWritable, LongWritable, IntWritable> {
    
    private static final Log LOG = LogFactory.getLog(ReadIDIndexBuilderReducer.class);
    
    private MultipleOutputs mos = null;
    private HirodsMultipleOutputs hmos = null;
    private Hashtable<Integer, String> namedOutputCache;
    private int[] readIDs;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        if(MultipleOutputsHelper.isMultipleOutputs(context.getConfiguration())) {
            this.mos = new MultipleOutputs(context);
        }
        
        if(MultipleOutputsHelper.isHirodsMultipleOutputs(context.getConfiguration())) {
            this.hmos = new HirodsMultipleOutputs(context);
        }
        
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
    protected void reduce(MultiFileOffsetWritable key, Iterable<CompressedLongArrayWritable> values, Context context) throws IOException, InterruptedException {
        List<Long> offsets = new ArrayList<Long>();
        
        for(CompressedLongArrayWritable value : values) {
            for(long lvalue : value.get()) {
                offsets.add(lvalue);
            }
        }
        
        Collections.sort(offsets);
        
        int namedoutputID = key.getFileID();
        
        String namedOutput = this.namedOutputCache.get(namedoutputID);
        if (namedOutput == null) {
            String[] namedOutputs = context.getConfiguration().getStrings(ReadIDIndexHelper.getConfigurationKeyOfNamedOutputName(namedoutputID));
            if (namedOutputs.length != 1) {
                throw new IOException("no named output found");
            }
            namedOutput = namedOutputs[0];
            this.namedOutputCache.put(namedoutputID, namedOutput);
        }
        
        for(long lvalue : offsets) {
            this.readIDs[namedoutputID]++;
            
            if (this.mos != null) {
                this.mos.write(namedOutput, new LongWritable(lvalue), new IntWritable(this.readIDs[namedoutputID]));
            }

            if (this.hmos != null) {
                this.hmos.write(namedOutput, new LongWritable(lvalue), new IntWritable(this.readIDs[namedoutputID]));
            }
            //context.write(new LongWritable(offset), new IntWritable(this.readIDs[namedoutputID]));
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if(this.mos != null) {
            this.mos.close();
        }
        
        if(this.hmos != null) {
            this.hmos.close();
        }
        
        this.namedOutputCache = null;
    }
}
