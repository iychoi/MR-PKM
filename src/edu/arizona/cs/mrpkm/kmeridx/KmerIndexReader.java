package edu.arizona.cs.mrpkm.kmeridx;

import edu.arizona.cs.mrpkm.types.KmerRecord;
import edu.arizona.cs.mrpkm.types.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.CompressedSequenceWritable;
import java.io.Closeable;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.MapFile;

/**
 *
 * @author iychoi
 */
public class KmerIndexReader implements Closeable {
    
    private static final Log LOG = LogFactory.getLog(KmerIndexReader.class);
        
    private String[] indexPaths;
    private MapFile.Reader[] mapfileReaders;
    
    private CompressedSequenceWritable currentKey = null;
    private CompressedIntArrayWritable currentVal = null;
    
    private CompressedSequenceWritable[] keys;
    private CompressedIntArrayWritable[] vals;
    
    private int currentIndex;
    
    public KmerIndexReader(FileSystem fs, String[] indexPaths, Configuration conf) throws IOException {
        initialize(fs, indexPaths, conf);
    }
    
    private void initialize(FileSystem fs, String[] indexPaths, Configuration conf) throws IOException {
        this.indexPaths = indexPaths;

        // create new AugMapFile readers
        this.mapfileReaders = new MapFile.Reader[indexPaths.length];
        for(int i=0;i<indexPaths.length;i++) {
            this.mapfileReaders[i] = new MapFile.Reader(fs, indexPaths[i], conf);
        }
        
        this.keys = new CompressedSequenceWritable[indexPaths.length];
        this.vals = new CompressedIntArrayWritable[indexPaths.length];
        
        fillKV();
    }
    
    private void fillKV() throws IOException {
        for(int i=0;i<this.indexPaths.length;i++) {
            CompressedSequenceWritable key = new CompressedSequenceWritable();
            CompressedIntArrayWritable val = new CompressedIntArrayWritable();
            
            if(this.mapfileReaders[i].next(key, val)) {
                this.keys[i] = key;
                this.vals[i] = val;
            } else {
                this.keys[i] = null;
                this.vals[i] = null;
            }
        }
        
        CompressedSequenceWritable minKey = null;
        this.currentIndex = -1;
        for(int i=0;i<this.keys.length;i++) {
            if(this.keys[i] != null) {
                if(minKey == null) {
                    minKey = this.keys[i];
                    this.currentIndex = i;
                } else {
                    if(minKey.compareTo(this.keys[i]) > 0) {
                        minKey = this.keys[i];
                        this.currentIndex = i;
                    }
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        if(this.mapfileReaders != null) {
            for(int i=0;i<this.mapfileReaders.length;i++) {
                this.mapfileReaders[i].close();
                this.mapfileReaders[i] = null;
            }
            this.mapfileReaders = null;
        }
    }
    
    public String[] getIndexPaths() {
        return this.indexPaths;
    }
    
    public void seek(String sequence) throws IOException {
        seek(new CompressedSequenceWritable(sequence));
    }
    
    public void seek(CompressedSequenceWritable key) throws IOException {
        for(MapFile.Reader reader : this.mapfileReaders) {
            reader.seek(key);
        }
        
        fillKV();
    }
    
    public KmerRecord[] getCurrentRecords() {
        int[] values = this.currentVal.get();
        KmerRecord[] records = new KmerRecord[values.length];
        
        for(int i=0;i<values.length;i++) {
            records[i] = new KmerRecord(this.currentKey.getSequence(), values[i]);
        }
        
        return records;
    }
    
    public CompressedSequenceWritable getCurrentKey() {
        return this.currentKey;
    }
    
    public CompressedIntArrayWritable getCurrentValue() {
        return this.currentVal;
    }
    
    public boolean next() throws IOException {
        if(this.currentIndex < 0) {
            this.currentKey = null;
            this.currentVal = null;
            return false;
        }
        
        this.currentKey = this.keys[this.currentIndex];
        this.currentVal = this.vals[this.currentIndex];
        
        CompressedSequenceWritable key = new CompressedSequenceWritable();
        CompressedIntArrayWritable val = new CompressedIntArrayWritable();
        
        if(this.mapfileReaders[this.currentIndex].next(key, val)) {
            this.keys[this.currentIndex] = key;
            this.vals[this.currentIndex] = val;
        } else {
            this.keys[this.currentIndex] = null;
            this.vals[this.currentIndex] = null;
        }
        
        CompressedSequenceWritable minKey = null;
        this.currentIndex = -1;
        for(int i=0;i<this.keys.length;i++) {
            if(this.keys[i] != null) {
                if(minKey == null) {
                    minKey = this.keys[i];
                    this.currentIndex = i;
                } else {
                    if(minKey.compareTo(this.keys[i]) > 0) {
                        minKey = this.keys[i];
                        this.currentIndex = i;
                    }
                }
            }
        }
        
        if(this.currentKey != null) {
            return true;
        } else {
            return false;
        }
    }
}
