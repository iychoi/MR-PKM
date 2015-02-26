package edu.arizona.cs.mrpkm.stddeviation;

import edu.arizona.cs.mrpkm.types.statistics.KmerStatisticsGroup;
import edu.arizona.cs.mrpkm.types.statistics.KmerStatistics;
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
public class KmerStdDeviationBuilderMapper extends Mapper<CompressedSequenceWritable, CompressedIntArrayWritable, NullWritable, NullWritable> {
    
    private static final Log LOG = LogFactory.getLog(KmerStdDeviationBuilderMapper.class);
    
    private Counter diffKmerCounter;
    private KmerStatisticsGroup statisticsGroup;
    private double average;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit)context.getInputSplit();
        String fastaFileName = KmerIndexHelper.getFastaFileName(inputSplit.getPath().getParent());
        
        this.diffKmerCounter = context.getCounter(KmerStdDeviationHelper.getCounterGroupNameDifferential(), fastaFileName);
        
        this.statisticsGroup = new KmerStatisticsGroup();
        this.statisticsGroup.loadFrom(context.getConfiguration());
        
        KmerStatistics statistics = this.statisticsGroup.getStatistics(fastaFileName);
        this.average = statistics.getTotalKmers() / (double)statistics.getUniqueKmers();
    }
    
    @Override
    protected void map(CompressedSequenceWritable key, CompressedIntArrayWritable value, Context context) throws IOException, InterruptedException {
        if(value.getPositiveEntriesCount() > 0) {
            double diff = value.getPositiveEntriesCount() - this.average;
            double diff2 = diff * diff;
            this.diffKmerCounter.increment((long)(diff2 * 1000));
        }
        
        if(value.getNegativeEntriesCount() > 0) {
            double diff = value.getNegativeEntriesCount() - this.average;
            double diff2 = diff * diff;
            this.diffKmerCounter.increment((long)(diff2 * 1000));
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }
}
