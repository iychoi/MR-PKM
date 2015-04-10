package edu.arizona.cs.mrpkm.kmerfreqcomp;

import edu.arizona.cs.mrpkm.types.hadoop.MultiFileReadIDWritable;
import edu.arizona.cs.mrpkm.types.MutableInteger;
import edu.arizona.cs.mrpkm.types.hadoop.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.hadoop.DoubleArrayWritable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author iychoi
 */
public class PairwiseKmerFrequencyComparatorReducer extends Reducer<IntWritable, DoubleArrayWritable, Text, Text> {
    
    private static final Log LOG = LogFactory.getLog(PairwiseKmerFrequencyComparatorReducer.class);
    
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
        
        int n = (int)Math.sqrt(accumulatedDiff.length);
        for(int i=0;i<accumulatedDiff.length;i++) {
            int x = i/n;
            int y = i%n;
            
            String k = x + "-" + y;
            String v = Double.toString(accumulatedDiff[i]);
            
            context.write(new Text(k), new Text(v));
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
