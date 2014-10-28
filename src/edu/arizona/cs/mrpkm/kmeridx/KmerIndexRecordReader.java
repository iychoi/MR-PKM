package edu.arizona.cs.mrpkm.kmeridx;

import edu.arizona.cs.mrpkm.types.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.CompressedSequenceWritable;
import edu.arizona.cs.mrpkm.utils.FileSystemHelper;
import edu.arizona.cs.mrpkm.utils.SequenceHelper;
import java.io.IOException;
import java.math.BigInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @author iychoi
 */
public class KmerIndexRecordReader extends RecordReader<CompressedSequenceWritable, CompressedIntArrayWritable> {
    private Path[] inputIndexPaths;
    private Configuration conf;
    private AKmerIndexReader indexReader;
    private BigInteger currentProgress;
    private BigInteger progressEnd;
    private KmerIndexInputFormatConfig inputFormatConfig;
    private CompressedSequenceWritable curKey;
    private CompressedIntArrayWritable curVal;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        if(!(split instanceof KmerIndexSplit)) {
            throw new IOException("split is not an instance of KmerIndexSplit");
        }
        
        KmerIndexSplit kmerIndexSplit = (KmerIndexSplit) split;
        this.conf = context.getConfiguration();
        this.inputIndexPaths = kmerIndexSplit.getIndexFilePaths();
        
        this.inputFormatConfig = new KmerIndexInputFormatConfig();
        this.inputFormatConfig.loadFrom(this.conf);
        
        FileSystem fs = this.inputIndexPaths[0].getFileSystem(this.conf);
        if(this.inputIndexPaths.length == 1) {
            this.indexReader = new SingleKmerIndexReader(fs, FileSystemHelper.makeStringFromPath(this.inputIndexPaths)[0], this.conf);
        } else {
            this.indexReader = new MultiKmerIndexReader(fs, FileSystemHelper.makeStringFromPath(this.inputIndexPaths), this.conf);
        }
        
        this.currentProgress = BigInteger.ZERO;
        StringBuilder endKmer = new StringBuilder();
        for(int i=0;i<this.inputFormatConfig.getKmerSize();i++) {
            endKmer.append("T");
        }
        this.progressEnd = SequenceHelper.convertToBigInteger(endKmer.toString());
        
        this.curKey = null;
        this.curVal = null;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        CompressedSequenceWritable key = new CompressedSequenceWritable();
        CompressedIntArrayWritable val = new CompressedIntArrayWritable();
        boolean result = this.indexReader.next(key, val);
        
        if(result) {
            this.curKey = key;
            this.curVal = val;
        } else {
            this.curKey = null;
            this.curVal = null;
        }
        return result;
    }

    @Override
    public CompressedSequenceWritable getCurrentKey() throws IOException, InterruptedException {
        return this.curKey;
    }

    @Override
    public CompressedIntArrayWritable getCurrentValue() throws IOException, InterruptedException {
        return this.curVal;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        BigInteger divided = this.currentProgress.multiply(BigInteger.valueOf(100)).divide(this.progressEnd);
        float f = divided.floatValue() / 100;

        return Math.min(1.0f, f);
    }

    @Override
    public void close() throws IOException {
        this.indexReader.close();
    }
}
