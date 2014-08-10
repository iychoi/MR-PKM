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
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author iychoi
 */
public class PairwiseKmerModeCounterMapper extends Mapper<CompressedSequenceWritable, KmerMatchResult, MultiFileReadIDWritable, CompressedIntArrayWritable> {
    private static final Log LOG = LogFactory.getLog(PairwiseKmerModeCounterMapper.class);
    
    private Hashtable<String, Integer> namedOutputIDCache;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        this.namedOutputIDCache = new Hashtable<String, Integer>();
    }
    
    @Override
    protected void map(CompressedSequenceWritable key, KmerMatchResult value, Context context) throws IOException, InterruptedException {
        CompressedIntArrayWritable[] vals = value.getVals();
        if(vals.length <= 1) {
            throw new IOException("Number of pairwise match result must be larger than 1");
        }
        
        for(int i=0;i<vals.length;i++) {
            for(int j=0;j<vals.length;j++) {
                if(i != j) {
                    String thisFastaFileName = KmerIndexHelper.getFastaFileName(value.getIndexPaths()[i][0]);
                    String thatFastaFileName = KmerIndexHelper.getFastaFileName(value.getIndexPaths()[j][0]);
                    
                    CompressedIntArrayWritable thisVal = vals[i];
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
                    
                    int[] thisValInt = thisVal.get();
                    int[] thatValInt = thatVal.get();
                    int forward = 0;
                    int backward = 0;
                    for(int k=0;k<thisValInt.length;k++) {
                        int readID = thisValInt[k];
                        int pos = 0;
                        int neg = 0;
                        for(int l : thatValInt) {
                            if(l >= 0) {
                                pos++;
                            } else {
                                neg++;
                            }
                        }
                        
                        if(readID >= 0) {
                            forward = pos;
                            backward = neg;
                        } else {
                            forward = neg;
                            backward = pos;
                            readID *= -1;
                        }
                        
                        int bigger = Math.max(forward, backward);
                        
                        int[] arr = new int[2];
                        arr[0] = bigger;
                        arr[1] = 1;
                        context.write(new MultiFileReadIDWritable(namedoutputID, readID), new CompressedIntArrayWritable(arr));
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
