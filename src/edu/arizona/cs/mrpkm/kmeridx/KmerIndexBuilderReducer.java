package edu.arizona.cs.mrpkm.kmeridx;

import edu.arizona.cs.hadoop.fs.irods.output.HirodsMultipleOutputs;
import edu.arizona.cs.mrpkm.types.namedoutputs.NamedOutputs;
import edu.arizona.cs.mrpkm.types.hadoop.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.hadoop.CompressedSequenceWritable;
import edu.arizona.cs.mrpkm.types.hadoop.MultiFileCompressedSequenceWritable;
import edu.arizona.cs.mrpkm.helpers.MultipleOutputsHelper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 *
 * @author iychoi
 */
public class KmerIndexBuilderReducer extends Reducer<MultiFileCompressedSequenceWritable, CompressedIntArrayWritable, CompressedSequenceWritable, CompressedIntArrayWritable> {
    
    private static final Log LOG = LogFactory.getLog(KmerIndexBuilderReducer.class);
    
    private NamedOutputs namedOutputs = null;
    private MultipleOutputs mos = null;
    private HirodsMultipleOutputs hmos = null;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        this.namedOutputs = new NamedOutputs();
        this.namedOutputs.loadFrom(conf);
        
        if(MultipleOutputsHelper.isMultipleOutputs(conf)) {
            this.mos = new MultipleOutputs(context);
        } else if(MultipleOutputsHelper.isHirodsMultipleOutputs(conf)) {
            this.hmos = new HirodsMultipleOutputs(context);
        }
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
        String namedOutput = this.namedOutputs.getRecordFromID(namedoutputID).getIdentifier();
        
        CompressedSequenceWritable outKey = new CompressedSequenceWritable(key.getCompressedSequence(), key.getSequenceLength());
        
        if(this.mos != null) {
            this.mos.write(namedOutput, outKey, new CompressedIntArrayWritable(readIDs));
        }
        
        if(this.hmos != null) {
            this.hmos.write(namedOutput, outKey, new CompressedIntArrayWritable(readIDs));
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        this.namedOutputs = null;
    
        if(this.mos != null) {
            this.mos.close();
        }
        
        if(this.hmos != null) {
            this.hmos.close();
        }
    }
}
