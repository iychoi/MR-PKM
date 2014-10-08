package edu.arizona.cs.mrpkm.kmermatch;

import edu.arizona.cs.hadoop.fs.irods.output.HirodsMultipleOutputs;
import edu.arizona.cs.mrpkm.types.MultiFileReadIDWritable;
import edu.arizona.cs.mrpkm.types.MutableInteger;
import edu.arizona.cs.mrpkm.utils.MultipleOutputsHelper;
import java.io.IOException;
import java.util.Hashtable;
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
        
        Hashtable<Integer, MutableInteger> modeTable = new Hashtable<Integer, MutableInteger>();
        
        // merge & find MODE
        Integer modeKey = null;
        MutableInteger modeVal = null;
        for(IntWritable value : values) {
            int hit = value.get();

            MutableInteger cntExist = modeTable.get(hit);
            if(cntExist == null) {
                MutableInteger mi = new MutableInteger(1);
                modeTable.put(hit, mi);
                if(modeKey == null) {
                    modeKey = hit;
                    modeVal = mi;
                } else {
                    if(modeVal.get() < mi.get()) {
                        modeKey = hit;
                        modeVal = mi;
                    } else if(modeVal.get() == mi.get() && modeKey < hit) {
                        modeKey = hit;
                        modeVal = mi;
                    }
                }
            } else {
                // existing
                cntExist.set(cntExist.get() + 1);
                
                if(hit != modeKey) {
                    if(modeVal.get() < cntExist.get()) {
                        modeKey = hit;
                        modeVal = cntExist;
                    } else if(modeVal.get() == cntExist.get() && modeKey < hit) {
                        modeKey = hit;
                        modeVal = cntExist;
                    }
                }
            }
        }
        
        if(modeKey != null) {
            if(this.mos != null) {
                this.mos.write(namedOutput, new Text(String.valueOf(key.getReadID())), new Text(String.valueOf(modeKey)));
            }

            if(this.hmos != null) {
                this.hmos.write(namedOutput, new Text(String.valueOf(key.getReadID())), new Text(String.valueOf(modeKey)));
            }
            //context.write(key, value);
        }
        
        modeTable.clear();
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
