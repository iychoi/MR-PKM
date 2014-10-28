package edu.arizona.cs.mrpkm.stddiviation;

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
public class KmerStdDiviationWriter {
    
    private String inputFilename;
    private long uniqueKmers;
    private long totalKmers;
    private double avgCounts;
    private double stdDiviation;
    
    public KmerStdDiviationWriter(String inputFilename, long uniqueKmers, long totalKmers, double avgCounts, double stdDiviation) {
        this.inputFilename = inputFilename;
        this.uniqueKmers = uniqueKmers;
        this.totalKmers = totalKmers;
        this.avgCounts = avgCounts;
        this.stdDiviation = stdDiviation;
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
    
    public double getStandardDiviation() {
        return this.stdDiviation;
    }
    
    public void createOutputFile(Path file, FileSystem fs) throws IOException {
        DataOutputStream writer = fs.create(file, true, 64 * 1024);
        
        new Text(this.inputFilename).write(writer);
        new LongWritable(this.uniqueKmers).write(writer);
        new LongWritable(this.totalKmers).write(writer);
        new DoubleWritable(this.avgCounts).write(writer);
        new DoubleWritable(this.stdDiviation).write(writer);
        
        writer.close();
    }
}
