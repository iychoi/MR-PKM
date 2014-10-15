package edu.arizona.cs.mrpkm.readididx;

import edu.arizona.cs.mrpkm.types.CompressedLongArrayWritable;
import edu.arizona.cs.mrpkm.types.MultiFileOffsetWritable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author iychoi
 */
public class ReadIDIndexBuilderCombiner extends Reducer<MultiFileOffsetWritable, CompressedLongArrayWritable, MultiFileOffsetWritable, CompressedLongArrayWritable> {
    
    private static final Log LOG = LogFactory.getLog(ReadIDIndexBuilderCombiner.class);
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }
    
    @Override
    protected void reduce(MultiFileOffsetWritable key, Iterable<CompressedLongArrayWritable> values, Context context) throws IOException, InterruptedException {
        List<Long> offsets = new ArrayList<Long>();
        
        for(CompressedLongArrayWritable value : values) {
            for(long lvalue : value.get()) {
                offsets.add(lvalue);
            }
        }
        
        context.write(key, new CompressedLongArrayWritable(offsets));
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
