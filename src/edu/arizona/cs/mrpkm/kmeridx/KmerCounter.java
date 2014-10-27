package edu.arizona.cs.mrpkm.kmeridx;

import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

/**
 *
 * @author iychoi
 */
public class KmerCounter {
    private String counterName;
    private int kmerSize;
    private long numKmers;
    private long numUniqueKmers;
    
    public KmerCounter(String counterName, int kmerSize) {
        this.counterName = counterName;
        
        this.kmerSize = kmerSize;
        this.numKmers = 0;
        this.numUniqueKmers = 0;
    }
    
    public String getCounterName() {
        return this.counterName;
    }
    
    public void takeUniqueKmer(long numKmers) {
        this.numUniqueKmers++;
        this.numKmers += numKmers;
    }
    
    public void createCounterFile(Path file, FileSystem fs) throws IOException {
        DataOutputStream writer = fs.create(file, true, 64 * 1024);
        
        new IntWritable(this.kmerSize).write(writer);
        new LongWritable(this.numKmers).write(writer);
        new LongWritable(this.numUniqueKmers).write(writer);
        
        writer.close();
    }

    public long getNumKmers() {
        return this.numKmers;
    }
    
    public long getNumUniqueKmers() {
        return this.numUniqueKmers;
    }

    public int getKmerSize() {
        return this.kmerSize;
    }
}
