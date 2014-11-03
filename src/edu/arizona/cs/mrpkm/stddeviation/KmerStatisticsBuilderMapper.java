package edu.arizona.cs.mrpkm.stddeviation;

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
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit)context.getInputSplit();
        String fastaFileName = KmerIndexHelper.getFastaFileName(inputSplit.getPath().getParent());
        
        this.uniqueKmerCounter = context.getCounter(KmerStdDeviationHelper.getCounterGroupNameUnique(), fastaFileName);
        this.totalKmerCounter = context.getCounter(KmerStdDeviationHelper.getCounterGroupNameTotal(), fastaFileName);
    }
    
    @Override
    protected void map(CompressedSequenceWritable key, CompressedIntArrayWritable value, Context context) throws IOException, InterruptedException {
        if(value.getPositiveEntriesCount() > 0 && value.getNegativeEntriesCount() > 0) {
            this.uniqueKmerCounter.increment(4);
            this.totalKmerCounter.increment(value.getPositiveEntriesCount() * 2 + value.getNegativeEntriesCount() * 2);
        } else if(value.getPositiveEntriesCount() > 0) {
            this.uniqueKmerCounter.increment(1);
            this.totalKmerCounter.increment(value.getPositiveEntriesCount());
        } else if(value.getNegativeEntriesCount() > 0) {
            this.uniqueKmerCounter.increment(1);
            this.totalKmerCounter.increment(value.getNegativeEntriesCount());
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
