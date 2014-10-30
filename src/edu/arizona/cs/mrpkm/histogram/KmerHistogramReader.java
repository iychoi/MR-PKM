package edu.arizona.cs.mrpkm.histogram;

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
public class KmerHistogramReader {
    
    private Path inputFileName;
    private Configuration conf;
    
    private KmerHistogramRecord[] records;
    private long sumCounts;

    public KmerHistogramReader(Path inputFileName, Configuration conf) throws IOException {
        this.inputFileName = inputFileName;
        this.conf = conf;
        this.records = null;
        this.sumCounts = 0;
        
        readRecords();
    }
    
    public KmerHistogramRecord[] getRecords() {
        return this.records;
    }

    public long getSampleCount() {
        return this.sumCounts;
    }

    private void readRecords() throws IOException {
        FileSystem inputFileSystem = this.inputFileName.getFileSystem(this.conf);
        DataInputStream reader = inputFileSystem.open(this.inputFileName);
        
        int recordNum = reader.readInt();
        this.records = new KmerHistogramRecord[recordNum];
        this.sumCounts = 0;
        for(int i=0;i<recordNum;i++) {
            String key = Text.readString(reader);
            long cnt = reader.readLong();
            
            this.records[i] = new KmerHistogramRecord(key, cnt);
            this.sumCounts += cnt;
        }
        reader.close();
    }
}
