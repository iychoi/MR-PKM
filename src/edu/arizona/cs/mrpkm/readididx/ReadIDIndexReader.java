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
    
    private FileSystem fs;
    private String indexPath;
    private Configuration conf;
    private MapFile.Reader mapfileReader;
    
    public ReadIDIndexReader(FileSystem fs, String indexPath, Configuration conf) throws IOException {
        initialize(fs, indexPath, conf);
    }
    
    private void initialize(FileSystem fs, String indexPath, Configuration conf) throws IOException {
        this.fs = fs;
        this.indexPath = indexPath;
        this.conf = conf;
        
        // create a new AugMapFile reader
        this.mapfileReader = new MapFile.Reader(fs, indexPath, conf);
    }
    
    public String getIndexPath() {
        return this.indexPath;
    }
    
    public void reset() throws IOException {
        this.mapfileReader.reset();
    }
    
    public boolean next(LongWritable key, IntWritable val) throws IOException {
        return this.mapfileReader.next(key, val);
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
