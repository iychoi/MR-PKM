package edu.arizona.cs.mrpkm.stddiviation;

import edu.arizona.cs.mrpkm.kmeridx.KmerIndexHelper;
import edu.arizona.cs.mrpkm.types.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.CompressedSequenceWritable;
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
public class KmerStatisticsMapper extends Mapper<CompressedSequenceWritable, CompressedIntArrayWritable, NullWritable, NullWritable> {
    
    private static final Log LOG = LogFactory.getLog(KmerStatisticsMapper.class);
    
    private Counter uniqueKmerCounter;
    private Counter totalKmerCounter;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit)context.getInputSplit();
        String fastaFileName = KmerIndexHelper.getFastaFileName(inputSplit.getPath().getParent());
        
        this.uniqueKmerCounter = context.getCounter(KmerStdDiviationHelper.getCounterGroupNameUnique(), fastaFileName);
        this.totalKmerCounter = context.getCounter(KmerStdDiviationHelper.getCounterGroupNameTotal(), fastaFileName);
    }
    
    @Override
    protected void map(CompressedSequenceWritable key, CompressedIntArrayWritable value, Context context) throws IOException, InterruptedException {
        this.uniqueKmerCounter.increment(1);
        this.totalKmerCounter.increment(value.get().length);
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
