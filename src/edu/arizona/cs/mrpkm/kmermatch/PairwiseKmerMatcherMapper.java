package edu.arizona.cs.mrpkm.kmermatch;

import edu.arizona.cs.mrpkm.kmeridx.KmerIndexHelper;
import edu.arizona.cs.mrpkm.types.hadoop.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.hadoop.CompressedSequenceWritable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author iychoi
 */
public class PairwiseKmerMatcherMapper extends Mapper<CompressedSequenceWritable, KmerMatchResult, Text, Text> {
    
    private static final Log LOG = LogFactory.getLog(PairwiseKmerMatcherMapper.class);
    
    PairwiseKmerMatcherConfig matcherConfig;
    Hashtable<String, Integer> idCacheTable;
    private Counter reportCounter;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.matcherConfig = new PairwiseKmerMatcherConfig();
        this.matcherConfig.loadFrom(context.getConfiguration());
        
        this.idCacheTable = new Hashtable<String, Integer>();
        
        this.reportCounter = context.getCounter("PairwiseKmerMatcher", "report");
    }
    
    @Override
    protected void map(CompressedSequenceWritable key, KmerMatchResult value, Context context) throws IOException, InterruptedException {
        List<CompressedIntArrayWritable> pureVals = new ArrayList<CompressedIntArrayWritable>();
        List<String> pureIndexPaths = new ArrayList<String>();
        for(int i=0;i<value.getVals().length;i++) {
            if(!value.getVals()[i].isEmpty()) {
                pureVals.add(value.getVals()[i]);
                pureIndexPaths.add(value.getIndexPaths()[i][0]);
            }
        }
        
        if(pureVals.size() <= 1) {
            // skip
            return;
        }
        
        int[] id_arr = new int[pureVals.size()];
        
        for(int i=0;i<pureVals.size();i++) {
            int fileidInt = 0;
            String indexFilename = pureIndexPaths.get(i);
            Integer fileid = this.idCacheTable.get(indexFilename);
            if(fileid == null) {
                String fastaFilename = KmerIndexHelper.getFastaFileName(indexFilename);
                int id = this.matcherConfig.getIDFromInput(fastaFilename);
                this.idCacheTable.put(indexFilename, id);
                fileidInt = id;
            } else {
                fileidInt = fileid.intValue();
            }
            
            id_arr[i] = fileidInt;
        }
        
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<id_arr.length;i++) {
            if(sb.length() != 0) {
                sb.append("\t");
            }
            
            sb.append(id_arr[i] + ":");
            int[] valReadIDs = pureVals.get(i).get();
            int valReadIDsSize = valReadIDs.length;
            for(int j=0;j<valReadIDsSize;j++) {
                sb.append(valReadIDs[j]);
                if(j < valReadIDsSize - 1) {
                    sb.append(",");
                }
            }
            this.reportCounter.increment(1);
        }
        
        context.write(new Text(key.getSequence()), new Text(sb.toString()));
        
        pureVals.clear();
        pureVals = null;
        pureIndexPaths.clear();
        pureIndexPaths = null;
        
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        this.matcherConfig = null;
        this.idCacheTable.clear();
        this.idCacheTable = null;
    }
}
