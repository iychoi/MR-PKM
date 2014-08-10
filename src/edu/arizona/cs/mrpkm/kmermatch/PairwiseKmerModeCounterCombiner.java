package edu.arizona.cs.mrpkm.kmermatch;

import edu.arizona.cs.mrpkm.types.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.MultiFileReadIDWritable;
import edu.arizona.cs.mrpkm.types.MutableInteger;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author iychoi
 */
public class PairwiseKmerModeCounterCombiner extends Reducer<MultiFileReadIDWritable, CompressedIntArrayWritable, MultiFileReadIDWritable, CompressedIntArrayWritable> {
    private static final Log LOG = LogFactory.getLog(PairwiseKmerModeCounterCombiner.class);
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }
    
    @Override
    protected void reduce(MultiFileReadIDWritable key, Iterable<CompressedIntArrayWritable> values, Context context) throws IOException, InterruptedException {
        Hashtable<Integer, MutableInteger> modeTable = new Hashtable<Integer, MutableInteger>();
        
        // merge
        for(CompressedIntArrayWritable value : values) {
            int[] iValue = value.get();
            int hit = iValue[0];
            int cnt = iValue[1];

            MutableInteger cntExist = modeTable.get(hit);
            if(cntExist == null) {
                modeTable.put(hit, new MutableInteger(cnt));
            } else {
                // existing
                cntExist.set(cntExist.get() + cnt);
            }
        }
        
        for(Map.Entry<Integer, MutableInteger> entry : modeTable.entrySet()) {
            int[] arr = new int[2];
            arr[0] = entry.getKey().intValue();
            arr[1] = entry.getValue().get();
            context.write(key, new CompressedIntArrayWritable(arr));
        }
        
        modeTable.clear();
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
