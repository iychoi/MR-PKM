package edu.arizona.cs.mrpkm.stddeviation;

import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 *
 * @author iychoi
 */
public class KmerStdDeviationWriter {
    
    private String inputFilename;
    private long uniqueKmers;
    private long totalKmers;
    private double avgCounts;
    private double stdDeviation;
    
    public KmerStdDeviationWriter(String inputFilename, long uniqueKmers, long totalKmers, double avgCounts, double stdDeviation) {
        this.inputFilename = inputFilename;
        this.uniqueKmers = uniqueKmers;
        this.totalKmers = totalKmers;
        this.avgCounts = avgCounts;
        this.stdDeviation = stdDeviation;
    }
    
    public String getInputFilename() {
        return this.inputFilename;
    }
    
    public long getUniqueKmers() {
        return this.uniqueKmers;
    }
    
    public long getTotalKmers() {
        return this.totalKmers;
    }
    
    public double getAverageCounts() {
        return this.avgCounts;
    }
    
    public double getStandardDeviation() {
        return this.stdDeviation;
    }
    
    public void createOutputFile(Path file, FileSystem fs) throws IOException {
        if(!fs.exists(file.getParent())) {
            fs.mkdirs(file.getParent());
        }
        
        DataOutputStream writer = fs.create(file, true, 64 * 1024);
        
        new Text(this.inputFilename).write(writer);
        new LongWritable(this.uniqueKmers).write(writer);
        new LongWritable(this.totalKmers).write(writer);
        new DoubleWritable(this.avgCounts).write(writer);
        new DoubleWritable(this.stdDeviation).write(writer);
        
        writer.close();
    }
}
