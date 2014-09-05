package edu.arizona.cs.mrpkm.kmeridx;

import edu.arizona.cs.hadoop.fs.irods.output.HirodsMultipleOutputs;
import edu.arizona.cs.mrpkm.types.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.CompressedSequenceWritable;
import edu.arizona.cs.mrpkm.types.MultiFileCompressedSequenceWritable;
import edu.arizona.cs.mrpkm.utils.MultipleOutputsHelper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 *
 * @author iychoi
 */
public class KmerIndexBuilderReducer extends Reducer<MultiFileCompressedSequenceWritable, CompressedIntArrayWritable, CompressedSequenceWritable, CompressedIntArrayWritable> {
    
    private static final Log LOG = LogFactory.getLog(KmerIndexBuilderReducer.class);
    
    private MultipleOutputs mos;
    private HirodsMultipleOutputs hmos = null;
    private Hashtable<Integer, String> namedOutputCache;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        if(MultipleOutputsHelper.isMultipleOutputs(context.getConfiguration())) {
            this.mos = new MultipleOutputs(context);
        }
        
        if(MultipleOutputsHelper.isHirodsMultipleOutputs(context.getConfiguration())) {
            this.hmos = new HirodsMultipleOutputs(context);
        }
        
        this.namedOutputCache = new Hashtable<Integer, String>();
    }
    
    @Override
    protected void reduce(MultiFileCompressedSequenceWritable key, Iterable<CompressedIntArrayWritable> values, Context context) throws IOException, InterruptedException {
        List<Integer> readIDs = new ArrayList<Integer>();
        
        for(CompressedIntArrayWritable value : values) {
            for(int ivalue : value.get()) {
                readIDs.add(ivalue);
            }
        }
        
        int namedoutputID = key.getFileID();
        String namedOutput = this.namedOutputCache.get(namedoutputID);
        if (namedOutput == null) {
            String[] namedOutputs = context.getConfiguration().getStrings(KmerIndexHelper.getConfigurationKeyOfNamedOutputName(namedoutputID));
            if (namedOutputs.length != 1) {
                throw new IOException("no named output found");
            }
            namedOutput = namedOutputs[0];
            this.namedOutputCache.put(namedoutputID, namedOutput);
        }
        
        CompressedSequenceWritable outKey = new CompressedSequenceWritable(key.getCompressedSequence(), key.getSequenceLength());
        
        if(this.mos != null) {
            this.mos.write(namedOutput, outKey, new CompressedIntArrayWritable(readIDs));
        }
        
        if(this.hmos != null) {
            this.hmos.write(namedOutput, outKey, new CompressedIntArrayWritable(readIDs));
        }
        //context.write(key, new Text(sb.toString()));
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
