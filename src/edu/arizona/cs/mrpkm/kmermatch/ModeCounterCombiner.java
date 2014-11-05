package edu.arizona.cs.mrpkm.kmermatch;

import edu.arizona.cs.mrpkm.kmeridx.*;
import edu.arizona.cs.mrpkm.types.MutableInteger;
import edu.arizona.cs.mrpkm.types.hadoop.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.hadoop.MultiFileReadIDWritable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author iychoi
 */
public class ModeCounterCombiner extends Reducer<MultiFileReadIDWritable, CompressedIntArrayWritable, MultiFileReadIDWritable, CompressedIntArrayWritable> {
    
    private static final Log LOG = LogFactory.getLog(ModeCounterCombiner.class);
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }
    
    @Override
    protected void reduce(MultiFileReadIDWritable key, Iterable<CompressedIntArrayWritable> values, Context context) throws IOException, InterruptedException {
        Hashtable<Integer, MutableInteger> hitsTable = new Hashtable<Integer, MutableInteger>();
        for(CompressedIntArrayWritable value : values) {
            int[] arr_value = value.get();
            if(arr_value.length < 2 && arr_value.length % 2 != 0) {
                throw new IOException("passed value is not in correct size");
            }
            
            for(int i=0;i<(arr_value.length/2);i++) {
                MutableInteger ext = hitsTable.get(arr_value[i*2]);
                if(ext == null) {
                    hitsTable.put(arr_value[i*2], new MutableInteger(arr_value[(i*2)+1]));
                } else {
                    // has
                    ext.increase(arr_value[(i*2)+1]);
                }
            }
        }
        
        List<Integer> hits = new ArrayList<Integer>();
        for(int hitsKey : hitsTable.keySet()) {
            MutableInteger hitsVal = hitsTable.get(hitsKey);
            hits.add(hitsKey);
            hits.add(hitsVal.get());
        }
        
        context.write(key, new CompressedIntArrayWritable(hits));
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
