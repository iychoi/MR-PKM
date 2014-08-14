package edu.arizona.cs.mrpkm.kmermatch;

import edu.arizona.cs.mrpkm.kmeridx.KmerIndexHelper;
import edu.arizona.cs.mrpkm.types.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.CompressedSequenceWritable;
import edu.arizona.cs.mrpkm.types.MultiFileReadIDWritable;
import java.io.IOException;
import java.util.Hashtable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author iychoi
 */
public class PairwiseKmerModeCounterMapper extends Mapper<CompressedSequenceWritable, KmerMatchResult, MultiFileReadIDWritable, IntWritable> {
    private static final Log LOG = LogFactory.getLog(PairwiseKmerModeCounterMapper.class);
    
    private Hashtable<String, Integer> namedOutputIDCache;
    private int matchFilterMin = 0;
    private int matchFilterMax = 0;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        this.namedOutputIDCache = new Hashtable<String, Integer>();
        this.matchFilterMin = conf.getInt(PairwiseKmerModeCounterHelper.getConfigurationKeyOfMatchFilterMin(), 0);
        this.matchFilterMax = conf.getInt(PairwiseKmerModeCounterHelper.getConfigurationKeyOfMatchFilterMax(), 0);
    }
    
    @Override
    protected void map(CompressedSequenceWritable key, KmerMatchResult value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        CompressedIntArrayWritable[] vals = value.getVals();
        if(vals.length <= 1) {
            throw new IOException("Number of pairwise match result must be larger than 1");
        }
        
        int[] count_vals_pos = new int[vals.length];
        int[] count_vals_neg = new int[vals.length];
        
        for(int i=0;i<vals.length;i++) {
            int pos = 0;
            int neg = 0;
            int[] ids = vals[i].get();
            for(int j=0;j<ids.length;j++) {
                if(j >= 0) {
                    pos++;
                } else {
                    neg++;
                }
            }
            count_vals_pos[i] = pos;
            count_vals_neg[i] = neg;
        }
        
        
        for(int i=0;i<vals.length;i++) {
            String thisFastaFileName = KmerIndexHelper.getFastaFileName(value.getIndexPaths()[i][0]);
            CompressedIntArrayWritable thisVal = vals[i];
            int[] thisValInt = thisVal.get();
            
            for(int j=0;j<vals.length;j++) {
                if(i != j) {
                    String thatFastaFileName = KmerIndexHelper.getFastaFileName(value.getIndexPaths()[j][0]);
                    CompressedIntArrayWritable thatVal = vals[j];
                    
                    String matchOutputName = PairwiseKmerModeCounterHelper.getPairwiseModeCounterOutputName(thisFastaFileName, thatFastaFileName);
                    Integer namedoutputID = this.namedOutputIDCache.get(matchOutputName);
                    if (namedoutputID == null) {
                        namedoutputID = context.getConfiguration().getInt(PairwiseKmerModeCounterHelper.getConfigurationKeyOfNamedOutputID(matchOutputName), -1);
                        if (namedoutputID < 0) {
                            throw new IOException("No named output found : " + PairwiseKmerModeCounterHelper.getConfigurationKeyOfNamedOutputID(matchOutputName));
                        }
                        this.namedOutputIDCache.put(matchOutputName, namedoutputID);
                    }
                    
                    int pos = count_vals_pos[j];
                    int neg = count_vals_neg[j];
                    
                    int forward = 0;
                    int backward = 0;
                    for(int k=0;k<thisValInt.length;k++) {
                        int readID = thisValInt[k];
                        
                        if(readID >= 0) {
                            forward = pos;
                            backward = neg;
                        } else {
                            forward = neg;
                            backward = pos;
                            readID *= -1;
                        }
                        
                        int bigger = Math.max(forward, backward);
                        
                        // apply filter
                        boolean filtered = false;
                        if(this.matchFilterMin > 0) {
                            if(bigger < this.matchFilterMin) {
                                filtered = true;
                            }
                        }
                        
                        if(this.matchFilterMax > 0) {
                            if(bigger > this.matchFilterMax) {
                                filtered = true;
                            }
                        }
                        
                        if(!filtered) {
                            context.write(new MultiFileReadIDWritable(namedoutputID, readID), new IntWritable(bigger));
                        }
                    }
                }
            }
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        this.namedOutputIDCache.clear();
        this.namedOutputIDCache = null;
    }
}
