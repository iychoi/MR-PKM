package edu.arizona.cs.mrpkm.kmerfreqcomp;

import edu.arizona.cs.mrpkm.types.hadoop.DoubleArrayWritable;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author iychoi
 */
public class PairwiseKmerFrequencyComparatorCombiner extends Reducer<IntWritable, DoubleArrayWritable, IntWritable, DoubleArrayWritable> {
    
    private static final Log LOG = LogFactory.getLog(PairwiseKmerFrequencyComparatorCombiner.class);
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }
    
    @Override
    protected void reduce(IntWritable key, Iterable<DoubleArrayWritable> values, Context context) throws IOException, InterruptedException {
        double[] accumulatedDiff = null;
        
        for(DoubleArrayWritable value : values) {
            if(accumulatedDiff == null) {
                accumulatedDiff = new double[value.get().length];
                
                for(int i=0;i<value.get().length;i++) {
                    accumulatedDiff[i] = 0;
                }
            }
            double[] darr = value.get();
            
            for(int i=0;i<darr.length;i++) {
                accumulatedDiff[i] += darr[i];
            }
        }
        
        context.write(key, new DoubleArrayWritable(accumulatedDiff));
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        
    }
}
