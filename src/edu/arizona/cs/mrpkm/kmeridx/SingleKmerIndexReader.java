package edu.arizona.cs.mrpkm.kmeridx;

import edu.arizona.cs.mrpkm.types.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.CompressedSequenceWritable;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.MapFile;

/**
 *
 * @author iychoi
 */
public class SingleKmerIndexReader extends AKmerIndexReader {
    
    private static final Log LOG = LogFactory.getLog(SingleKmerIndexReader.class);
    
    private static final int BUFFER_SIZE = 1000;
    
    private FileSystem fs;
    private String indexPath;
    private Configuration conf;
    private MapFile.Reader mapfileReader;
    private BlockingQueue<BufferEntry> buffer = new LinkedBlockingQueue<BufferEntry>();
    private boolean eof;
    
    public SingleKmerIndexReader(FileSystem fs, String indexPath, Configuration conf) throws IOException {
        initialize(fs, indexPath, null, conf);
    }
    
    public SingleKmerIndexReader(FileSystem fs, String indexPath, CompressedSequenceWritable beginKey, Configuration conf) throws IOException {
        initialize(fs, indexPath, beginKey, conf);
    }
    
    public SingleKmerIndexReader(FileSystem fs, String indexPath, String beginKey, Configuration conf) throws IOException {
        initialize(fs, indexPath, new CompressedSequenceWritable(beginKey), conf);
    }
    
    private void initialize(FileSystem fs, String indexPath, CompressedSequenceWritable beginKey, Configuration conf) throws IOException {
        this.fs = fs;
        this.indexPath = indexPath;
        this.conf = conf;

        this.mapfileReader = new MapFile.Reader(fs, indexPath, conf);
        if(beginKey != null) {
            mapfileReader.seek(beginKey);
        }
        
        this.eof = false;
        fillBuffer();
    }
    
    private void fillBuffer() throws IOException {
        if(!this.eof) {
            for(int i=0;i<BUFFER_SIZE;i++) {
                CompressedSequenceWritable key = new CompressedSequenceWritable();
                CompressedIntArrayWritable val = new CompressedIntArrayWritable();
                if(this.mapfileReader.next(key, val)) {
                    BufferEntry entry = new BufferEntry(key, val);
                    if(!this.buffer.offer(entry)) {
                        throw new IOException("buffer is full");
                    }
                } else {
                    // EOF
                    this.eof = true;
                }
            }
            this.eof = false;
        }
    }
    
    @Override
    public void reset() throws IOException {
        this.mapfileReader.reset();
        this.buffer.clear();
        this.eof = false;
        fillBuffer();
    }

    @Override
    public void close() throws IOException {
        if(this.mapfileReader != null) {
            this.mapfileReader.close();
            this.mapfileReader = null;
        }
        
        if(this.buffer != null) {
            this.buffer.clear();
            this.buffer = null;
        }
    }
    
    @Override
    public String[] getIndexPaths() {
        return new String[] {this.indexPath};
    }
    
    @Override
    public void seek(String sequence) throws IOException {
        seek(new CompressedSequenceWritable(sequence));
    }
    
    @Override
    public void seek(CompressedSequenceWritable key) throws IOException {
        this.mapfileReader.seek(key);
        this.buffer.clear();
        this.eof = false;
        fillBuffer();
    }
    
    @Override
    public boolean next(CompressedSequenceWritable key, CompressedIntArrayWritable val) throws IOException {
        BufferEntry entry = this.buffer.poll();
        if(entry != null) {
            key.set(entry.getKey());
            val.set(entry.getVal());
            return true;
        }
        
        fillBuffer();
        entry = this.buffer.poll();
        if(entry != null) {
            key.set(entry.getKey());
            val.set(entry.getVal());
            return true;
        }
        return false;
    }
    
    private class BufferEntry {
        private CompressedSequenceWritable key;
        private CompressedIntArrayWritable val;
        
        public BufferEntry(CompressedSequenceWritable key, CompressedIntArrayWritable val) {
            this.key = key;
            this.val = val;
        }
        
        public CompressedSequenceWritable getKey() {
            return this.key;
        }
        
        public CompressedIntArrayWritable getVal() {
            return this.val;
        }
    }
}