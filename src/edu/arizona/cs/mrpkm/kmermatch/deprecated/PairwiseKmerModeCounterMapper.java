package edu.arizona.cs.mrpkm.kmermatch.deprecated;

import edu.arizona.cs.mrpkm.kmeridx.KmerIndexHelper;
import edu.arizona.cs.mrpkm.kmermatch.KmerMatchResult;
import edu.arizona.cs.mrpkm.types.namedoutputs.NamedOutputs;
import edu.arizona.cs.mrpkm.types.hadoop.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.hadoop.CompressedSequenceWritable;
import edu.arizona.cs.mrpkm.types.hadoop.MultiFileReadIDWritable;
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
    
    private NamedOutputs namedOutputs = null;
    private Hashtable<String, String> fastaFilenameTable;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        this.namedOutputs = new NamedOutputs();
        this.namedOutputs.loadFrom(conf);
        
        this.fastaFilenameTable = new Hashtable<String, String>();
    }
    
    @Override
    protected void map(CompressedSequenceWritable key, KmerMatchResult value, Context context) throws IOException, InterruptedException {
        CompressedIntArrayWritable[] vals = value.getVals();
        if(vals.length <= 1) {
            throw new IOException("Number of pairwise match result must be larger than 1");
        }
        
        int[] count_pos_vals = new int[vals.length];
        int[] count_neg_vals = new int[vals.length];
        
        for(int i=0;i<vals.length;i++) {
            count_pos_vals[i] = vals[i].getPositiveEntriesCount();
            count_neg_vals[i] = vals[i].getNegativeEntriesCount();
        }
        
        for(int i=0;i<vals.length;i++) {
            String thisFastaFileName = this.fastaFilenameTable.get(value.getIndexPaths()[i][0]);
            if(thisFastaFileName == null) {
                thisFastaFileName = KmerIndexHelper.getFastaFileName(value.getIndexPaths()[i][0]);
                this.fastaFilenameTable.put(value.getIndexPaths()[i][0], thisFastaFileName);
            }
            
            CompressedIntArrayWritable thisVal = vals[i];
            int[] thisValInt = thisVal.get();
            
            for(int j=0;j<vals.length;j++) {
                if(i != j) {
                    String thatFastaFileName = this.fastaFilenameTable.get(value.getIndexPaths()[j][0]);
                    if(thatFastaFileName == null) {
                        thatFastaFileName = KmerIndexHelper.getFastaFileName(value.getIndexPaths()[j][0]);
                        this.fastaFilenameTable.put(value.getIndexPaths()[j][0], thatFastaFileName);
                    }
            
                    String matchOutputName = PairwiseKmerModeCounterHelper.getPairwiseModeCounterOutputName(thisFastaFileName, thatFastaFileName);
                    int namedoutputID = this.namedOutputs.getIDFromFilename(matchOutputName);
                    
                    for(int k=0;k<thisValInt.length;k++) {
                        int readID = thisValInt[k];
                        if(readID >= 0) {
                            // pos
                            if(count_pos_vals[j] > 0) {
                                // forward match
                                context.write(new MultiFileReadIDWritable(namedoutputID, readID), new IntWritable(count_pos_vals[j]));
                            }
                            
                            if(count_neg_vals[j] > 0) {
                                // reverse match
                                context.write(new MultiFileReadIDWritable(namedoutputID, readID), new IntWritable(-1 * count_neg_vals[j]));
                            }
                        } else {
                            // neg
                            readID *= -1;
                            if(count_pos_vals[j] > 0) {
                                // reverse match
                                context.write(new MultiFileReadIDWritable(namedoutputID, readID), new IntWritable(-1 * count_pos_vals[j]));
                            }
                            
                            if(count_neg_vals[j] > 0) {
                                // forward match
                                context.write(new MultiFileReadIDWritable(namedoutputID, readID), new IntWritable(count_neg_vals[j]));
                            }
                        }
                    }
                }
            }
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        this.namedOutputs = null;
        this.fastaFilenameTable.clear();
        this.fastaFilenameTable = null;
    }
}
