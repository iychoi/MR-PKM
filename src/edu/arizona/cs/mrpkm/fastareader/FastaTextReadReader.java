package edu.arizona.cs.mrpkm.fastareader;

import edu.arizona.cs.mrpkm.fastareader.types.FastaRawRead;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @author iychoi
 */
public class FastaTextReadReader extends RecordReader<Text, Text> {

    private FastaRawReadReader rawReadReader = new FastaRawReadReader();
    
    private Text key;
    private Text value;
    
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
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
                String description = value.getDescription();
                String pureSequence = new String();
                for (int i = 0; i < value.getRawSequence().length; i++) {
                    pureSequence += value.getRawSequence()[i].getLine();
                }
                
                this.key = new Text(description);
                this.value = new Text(pureSequence);
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