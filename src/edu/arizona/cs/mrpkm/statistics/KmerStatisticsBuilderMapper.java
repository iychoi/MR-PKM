package edu.arizona.cs.mrpkm.statistics;

import edu.arizona.cs.mrpkm.kmeridx.KmerIndexHelper;
import edu.arizona.cs.mrpkm.types.hadoop.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.hadoop.CompressedSequenceWritable;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * @author iychoi
 */
public class KmerStatisticsBuilderMapper extends Mapper<CompressedSequenceWritable, CompressedIntArrayWritable, NullWritable, NullWritable> {
    
    private static final Log LOG = LogFactory.getLog(KmerStatisticsBuilderMapper.class);
    
    private Counter uniqueKmerCounter;
    private Counter totalKmerCounter;
    private Counter squareKmerCounter;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit)context.getInputSplit();
        String fastaFileName = KmerIndexHelper.getFastaFileName(inputSplit.getPath().getParent());
        
        this.uniqueKmerCounter = context.getCounter(KmerStatisticsHelper.getCounterGroupNameUnique(), fastaFileName);
        this.totalKmerCounter = context.getCounter(KmerStatisticsHelper.getCounterGroupNameTotal(), fastaFileName);
        this.squareKmerCounter = context.getCounter(KmerStatisticsHelper.getCounterGroupNameSquare(), fastaFileName);
    }
    
    @Override
    protected void map(CompressedSequenceWritable key, CompressedIntArrayWritable value, Context context) throws IOException, InterruptedException {
        int pos = value.getPositiveEntriesCount();
        if(pos > 0) {
            this.uniqueKmerCounter.increment(1);
            this.totalKmerCounter.increment(pos);
            this.squareKmerCounter.increment((long) Math.pow(pos, 2));
        }
        
        int neg = value.getNegativeEntriesCount();
        if(neg > 0) {
            this.uniqueKmerCounter.increment(1);
            this.totalKmerCounter.increment(neg);
            this.squareKmerCounter.increment((long) Math.pow(neg, 2));
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
