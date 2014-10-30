package edu.arizona.cs.mrpkm.kmermatch;

import edu.arizona.cs.mrpkm.kmerrangepartitioner.KmerRangePartition;
import edu.arizona.cs.mrpkm.types.CompressedSequenceWritable;
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
public class KmerMatchRecordReader extends RecordReader<CompressedSequenceWritable, KmerMatchResult> {
    private Path[] inputIndexPaths;
    private KmerLinearMatcher matcher;
    private Configuration conf;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        if(!(split instanceof KmerMatchIndexSplit)) {
            throw new IOException("split is not an instance of KmerMatchIndexSplit");
        }
        
        KmerMatchIndexSplit kmerIndexSplit = (KmerMatchIndexSplit) split;
        this.conf = context.getConfiguration();
        this.inputIndexPaths = kmerIndexSplit.getIndexFilePaths();
        
        KmerMatchInputFormatConfig inputFormatConfig = new KmerMatchInputFormatConfig();
        inputFormatConfig.loadFrom(this.conf);
        String filterPath = inputFormatConfig.getStdDeviationFilterPath();
        
        KmerRangePartition partitions = kmerIndexSplit.getPartition();
        this.matcher = new KmerLinearMatcher(this.inputIndexPaths, partitions, filterPath, this.conf);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return this.matcher.nextMatch();
    }

    @Override
    public CompressedSequenceWritable getCurrentKey() {
        return this.matcher.getCurrentMatch().getKey();
    }

    @Override
    public KmerMatchResult getCurrentValue() {
        return this.matcher.getCurrentMatch();
    }

    @Override
    public float getProgress() throws IOException {
        return this.matcher.getProgress();
    }

    @Override
    public synchronized void close() throws IOException {
        this.matcher.close();
    }
}
