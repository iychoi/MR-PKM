package edu.arizona.cs.mrpkm.fastareader;

import edu.arizona.cs.mrpkm.fastareader.types.FastaRawRead;
import edu.arizona.cs.mrpkm.fastareader.types.FastaRead;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @author iychoi
 */
public class FastaReadReader extends RecordReader<LongWritable, FastaRead> {

    private FastaRawReadReader rawReadReader = new FastaRawReadReader();
    
    private LongWritable key;
    private FastaRead value;
    
    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    public FastaRead getCurrentValue() throws IOException, InterruptedException {
        return this.value;
    }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {

        this.rawReadReader.initialize(genericSplit, context);
        
        this.key = null;
        this.value = null;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean retVal = this.rawReadReader.nextKeyValue();
        if(retVal) {
            FastaRawRead value = this.rawReadReader.getCurrentValue();
            if(value != null) {
                FastaRead read = new FastaRead(value);
                this.value = read;
                this.key = new LongWritable(read.getReadOffset());
            } else {
                this.key = null;
                this.value = null;
            }
        } else {
            this.key = null;
            this.value = null;
        }
        
        return retVal;
    }

    @Override
    public float getProgress() throws IOException {
        return this.rawReadReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        this.rawReadReader.close();
    }
}