package edu.arizona.cs.mrpkm.stddeviation;

import java.io.DataInputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

/**
 *
 * @author iychoi
 */
public class KmerStdDeviationReader {
    
    private Configuration conf;
    
    private Path inputFileName;
    private long uniqueKmers;
    private long totalKmers;
    private double avgCounts;
    private double stdDeviation;
    
    public KmerStdDeviationReader(Path inputFileName, Configuration conf) throws IOException {
        this.inputFileName = inputFileName;
        this.conf = conf;
        
        readData();
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
    
    private void readData() throws IOException {
        FileSystem inputFileSystem = this.inputFileName.getFileSystem(this.conf);
        DataInputStream reader = inputFileSystem.open(this.inputFileName);
        
        String filename = Text.readString(reader);
        this.uniqueKmers = reader.readLong();
        this.totalKmers = reader.readLong();
        this.avgCounts = reader.readDouble();
        this.stdDeviation = reader.readDouble();
        
        reader.close();
    }
}
