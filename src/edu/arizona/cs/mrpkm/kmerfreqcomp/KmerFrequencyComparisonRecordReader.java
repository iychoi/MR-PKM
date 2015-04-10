package edu.arizona.cs.mrpkm.kmerfreqcomp;

import edu.arizona.cs.mrpkm.types.kmerrangepartition.KmerRangePartition;
import edu.arizona.cs.mrpkm.types.hadoop.CompressedSequenceWritable;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @author iychoi
 */
public class KmerFrequencyComparisonRecordReader extends RecordReader<CompressedSequenceWritable, KmerFrequencyComparisonResult> {
    private Path[] inputIndexPaths;
    private KmerFrequencyLinearComparator comparator;
    private Configuration conf;
    private KmerFrequencyComparisonResult curResult;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        if(!(split instanceof KmerFrequencyComparisonIndexSplit)) {
            throw new IOException("split is not an instance of KmerFrequencyComparisonIndexSplit");
        }
        
        KmerFrequencyComparisonIndexSplit kmerIndexSplit = (KmerFrequencyComparisonIndexSplit) split;
        this.conf = context.getConfiguration();
        this.inputIndexPaths = kmerIndexSplit.getIndexFilePaths();
        
        KmerFrequencyComparisonInputFormatConfig inputFormatConfig = new KmerFrequencyComparisonInputFormatConfig();
        inputFormatConfig.loadFrom(this.conf);
        String indexChunkInfoPath = inputFormatConfig.getKmerIndexChunkInfoPath();
        
        KmerRangePartition partitions = kmerIndexSplit.getPartition();
        this.comparator = new KmerFrequencyLinearComparator(this.inputIndexPaths, partitions, indexChunkInfoPath, context);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean nextKeyValue() throws IOException, InterruptedException {
        this.curResult = this.comparator.stepNext();
        if(this.curResult != null) {
            return true;
        }
        return false;
    }

    @Override
    public CompressedSequenceWritable getCurrentKey() {
        if(this.curResult != null) {
            return this.curResult.getKey();
        }
        return null;
    }

    @Override
    public KmerFrequencyComparisonResult getCurrentValue() {
        return this.curResult;
    }

    @Override
    public float getProgress() throws IOException {
        return this.comparator.getProgress();
    }

    @Override
    public synchronized void close() throws IOException {
        this.comparator.close();
    }
}
