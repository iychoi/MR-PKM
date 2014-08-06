package edu.arizona.cs.mrpkm.readididx;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;

/**
 *
 * @author iychoi
 */
public class ReadIDIndexReader implements java.io.Closeable {
    
    private static final Log LOG = LogFactory.getLog(ReadIDIndexReader.class);
    
    private String indexPath;
    private MapFile.Reader mapfileReader;
    
    public ReadIDIndexReader(FileSystem fs, String indexPath, Configuration conf) throws IOException {
        initialize(fs, indexPath, conf);
    }
    
    private void initialize(FileSystem fs, String indexPath, Configuration conf) throws IOException {
        this.indexPath = indexPath;
        
        // create a new MapFile reader
        this.mapfileReader = new MapFile.Reader(fs, indexPath, conf);
    }
    
    public String getIndexPath() {
        return this.indexPath;
    }
    
    public int findReadID(long offset) throws ReadIDNotFoundException {
        LongWritable key = new LongWritable(offset);
        IntWritable value = new IntWritable();
        try {
            this.mapfileReader.get(key, value);
            return value.get();
        } catch (IOException ex) {
            LOG.error(ex);
        }

        throw new ReadIDNotFoundException("ReadID is not found");
    }

    @Override
    public void close() throws IOException {
        this.mapfileReader.close();
    }
}

class ReadIDIndexCachedObject {
    private int readID;
    private long recordOffset;
    
    public ReadIDIndexCachedObject(int readID, long recordOffset) {
        this.readID = readID;
        this.recordOffset = recordOffset;
    }
    
    public int getReadID() {
        return this.readID;
    }
    
    public long getRecordOffset() {
        return this.recordOffset;
    }
}
