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
    private LongWritable curKey;
    private int entryNum;
    
    public ReadIDIndexReader(FileSystem fs, String indexPath, Configuration conf) throws IOException {
        initialize(fs, indexPath, conf);
    }
    
    private void initialize(FileSystem fs, String indexPath, Configuration conf) throws IOException {
        this.fs = fs;
        this.indexPath = indexPath;
        this.conf = conf;
        
        // create a new AugMapFile reader
        this.mapfileReader = new MapFile.Reader(fs, indexPath, conf);
        this.curKey = null;
        this.entryNum = -1;
    }
    
    public String getIndexPath() {
        return this.indexPath;
    }
    
    public int getEntryNum() {
        if(this.entryNum < 0) {
            try {
                this.mapfileReader.reset();
                LongWritable key = new LongWritable();
                IntWritable val = new IntWritable();
                int count = 0;
                while(next(key, val)) {
                    count++;
                }

                if(this.curKey != null) {
                    this.mapfileReader.seek(this.curKey);
                }
                this.entryNum = count;
                return count;
            } catch (IOException ex) {
                LOG.error(ex);
                return 0;
            }
        } else {
            return this.entryNum;
        }
    }
    
    public void reset() throws IOException {
        this.mapfileReader.reset();
        this.curKey = null;
    }
    
    public boolean next(LongWritable key, IntWritable val) throws IOException {
        boolean result = this.mapfileReader.next(key, val);
        this.curKey = key;
        return result;
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
