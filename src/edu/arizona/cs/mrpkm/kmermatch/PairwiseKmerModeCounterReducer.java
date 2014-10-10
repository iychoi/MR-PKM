package edu.arizona.cs.mrpkm.kmermatch;

import edu.arizona.cs.hadoop.fs.irods.output.HirodsMultipleOutputs;
import edu.arizona.cs.mrpkm.types.MultiFileReadIDWritable;
import edu.arizona.cs.mrpkm.types.MutableInteger;
import edu.arizona.cs.mrpkm.utils.MultipleOutputsHelper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 *
 * @author iychoi
 */
public class PairwiseKmerModeCounterReducer extends Reducer<MultiFileReadIDWritable, IntWritable, Text, Text> {
    private static final Log LOG = LogFactory.getLog(PairwiseKmerModeCounterReducer.class);
    
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
    protected void reduce(MultiFileReadIDWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int namedoutputID = key.getFileID();
        String namedOutput = this.namedOutputCache.get(namedoutputID);
        if (namedOutput == null) {
            String[] namedOutputs = context.getConfiguration().getStrings(PairwiseKmerModeCounterHelper.getConfigurationKeyOfNamedOutputName(namedoutputID));
            if (namedOutputs.length != 1) {
                throw new IOException("no named output found");
            }
            namedOutput = namedOutputs[0];
            this.namedOutputCache.put(namedoutputID, namedOutput);
        }
        
        List<Integer> forward_list = new ArrayList<Integer>();
        List<Integer> reverse_list = new ArrayList<Integer>();
        for(IntWritable value : values) {
            int hit = value.get();
            if(hit > 0) {
                forward_list.add(hit);
            } else if(hit < 0) {
                reverse_list.add(-1 * hit);
            }
        }
        
        int[] forward_mode = getMode(forward_list);
        int[] reverse_mode = getMode(reverse_list);
        
        // TODO: pick max count
        int larger_mode = 0;
        if(forward_mode[1] >= reverse_mode[1]) {
            larger_mode = forward_mode[0];
        } else {
            larger_mode = reverse_mode[0];
        }
        
        String out_value = String.valueOf(larger_mode);
        
        if(this.mos != null) {
            this.mos.write(namedOutput, new Text(String.valueOf(key.getReadID())), new Text(out_value));
        }

        if(this.hmos != null) {
            this.hmos.write(namedOutput, new Text(String.valueOf(key.getReadID())), new Text(out_value));
        }
        //context.write(key, value);
    }
    
    private int[] getMode(List<Integer> values) throws IOException {
        Hashtable<Integer, MutableInteger> modeTable = new Hashtable<Integer, MutableInteger>();
        
        int mode_hit = 0;
        int mode_count = 0;
        for(Integer value : values) {
            int hit = value.intValue();
            
            if(hit == 0) {
                continue;
            }

            MutableInteger cntExist = modeTable.get(hit);
            if(cntExist == null) {
                MutableInteger new_cnt = new MutableInteger(1);
                modeTable.put(hit, new_cnt);
                if(mode_hit == 0) {
                    mode_hit = hit;
                    mode_count = new_cnt.get();
                } else {
                    if(mode_count < new_cnt.get()) {
                        mode_hit = hit;
                        mode_count = new_cnt.get();
                    } else if(mode_count == new_cnt.get() && mode_hit < hit) {
                        mode_hit = hit;
                        mode_count = new_cnt.get();
                    }
                }
            } else {
                // existing
                cntExist.increase();
                
                if(hit == mode_hit) {
                    mode_count = cntExist.get();
                } else {
                    if(mode_count < cntExist.get()) {
                        mode_hit = hit;
                        mode_count = cntExist.get();
                    } else if(mode_count == cntExist.get() && mode_hit < hit) {
                        mode_hit = hit;
                        mode_count = cntExist.get();
                    }
                }
            }
        }
        
        modeTable.clear();
        
        int[] mode = new int[2];
        mode[0] = mode_hit;
        mode[1] = mode_count;
        
        return mode;
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
