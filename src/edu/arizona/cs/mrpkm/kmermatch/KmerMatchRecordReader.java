package edu.arizona.cs.mrpkm.kmermatch;

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
public class KmerMatchRecordReader extends RecordReader<CompressedSequenceWritable, KmerMatchResult> {
    private Path[] inputIndexPaths;
    private KmerLinearMatcher matcher;
    private Configuration conf;
    private KmerMatchResult curResult;

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
        String indexChunkInfoPath = inputFormatConfig.getKmerIndexChunkInfoPath();
        
        KmerRangePartition partitions = kmerIndexSplit.getPartition();
        this.matcher = new KmerLinearMatcher(this.inputIndexPaths, partitions, filterPath, indexChunkInfoPath, context);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean nextKeyValue() throws IOException, InterruptedException {
        this.curResult = this.matcher.stepNext();
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
    public KmerMatchResult getCurrentValue() {
        return this.curResult;
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
