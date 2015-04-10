package edu.arizona.cs.mrpkm.kmerfreqcomp;

import edu.arizona.cs.mrpkm.kmeridx.KmerIndexHelper;
import edu.arizona.cs.mrpkm.statistics.KmerStatisticsHelper;
import edu.arizona.cs.mrpkm.types.hadoop.CompressedSequenceWritable;
import edu.arizona.cs.mrpkm.types.hadoop.DoubleArrayWritable;
import edu.arizona.cs.mrpkm.types.statistics.KmerStatistics;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author iychoi
 */
public class PairwiseKmerFrequencyComparatorMapper extends Mapper<CompressedSequenceWritable, KmerFrequencyComparisonResult, IntWritable, DoubleArrayWritable> {
    
    private static final Log LOG = LogFactory.getLog(PairwiseKmerFrequencyComparatorMapper.class);
    
    private PairwiseKmerFrequencyComparatorConfig comparatorConfig;
    private int valuesLen;
    private double[] diffAccumulated;
    private double[] normalizeFactor;
    private double[] avgKmerFrequency;
    private double gavgKmerFrequency;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.comparatorConfig = new PairwiseKmerFrequencyComparatorConfig();
        this.comparatorConfig.loadFrom(context.getConfiguration());
        
        this.valuesLen = 0;
        this.diffAccumulated = null;
        this.normalizeFactor = null;
        this.avgKmerFrequency = null;
    }
    
    @Override
    protected void map(CompressedSequenceWritable key, KmerFrequencyComparisonResult value, Context context) throws IOException, InterruptedException {
        if(this.valuesLen == 0) {
            this.valuesLen = value.getIndexPaths().length;
        }
        
        if(this.diffAccumulated == null) {
            this.diffAccumulated = new double[this.valuesLen * this.valuesLen];
            for(int i=0;i<this.diffAccumulated.length;i++) {
                this.diffAccumulated[i] = 0;
            }
        }
        
        if(this.avgKmerFrequency == null) {
            double sumAvg = 0;
            
            this.avgKmerFrequency = new double[this.valuesLen];
            for(int i=0;i<this.valuesLen;i++) {
                Path indexFile = new Path(value.getIndexPaths()[i][0]);
                FileSystem fs = indexFile.getFileSystem(context.getConfiguration());
                String fastaFilename = KmerIndexHelper.getFastaFileName(indexFile);
                String statisticsFilename = KmerStatisticsHelper.makeStatisticsFileName(fastaFilename);
                Path statisticsPath = new Path(this.comparatorConfig.getStatisticsPath(), statisticsFilename);
                
                KmerStatistics statistics = new KmerStatistics();
                statistics.loadFrom(statisticsPath, fs);
                
                this.avgKmerFrequency[i] = statistics.getAverage();
                
                sumAvg += this.avgKmerFrequency[i];
            }
            
            this.gavgKmerFrequency = sumAvg / (double)this.valuesLen;
        }
        
        if(this.normalizeFactor == null) {
            this.normalizeFactor = new double[this.valuesLen];
            for(int i=0;i<this.valuesLen;i++) {
                this.normalizeFactor[i] = this.gavgKmerFrequency / this.avgKmerFrequency[i];
            }
        }
        
        accumulateDifference(value.getPosVals().get(), value.getNegVals().get());
    }
    
    private void accumulateDifference(int[] pos, int[] neg) {
        for(int i=0;i<this.valuesLen;i++) {
            double val1 = pos[i] + neg[i];
            double normalized_val1 = val1 * this.normalizeFactor[i];
            
            for(int j=0;j<this.valuesLen;j++) {
                if(i != j) {
                    double val2 = pos[j] + neg[j];
                    double normalized_val2 = val2 * this.normalizeFactor[j];
                    
                    double diff = Math.abs(normalized_val1  - normalized_val2);
                    this.diffAccumulated[i*this.valuesLen + j] += diff;
                }
            }
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new IntWritable(0), new DoubleArrayWritable(this.diffAccumulated));
        
        this.comparatorConfig = null;
        this.diffAccumulated = null;
        this.avgKmerFrequency = null;
        this.normalizeFactor = null;
    }
}
