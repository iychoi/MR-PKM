package edu.arizona.cs.mrpkm.kmermatch;

import edu.arizona.cs.mrpkm.kmeridx.KmerIndexHelper;
import edu.arizona.cs.mrpkm.kmeridx.KmerIndexReader;
import edu.arizona.cs.mrpkm.types.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.CompressedSequenceWritable;
import edu.arizona.cs.mrpkm.utils.FileSystemHelper;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class KmerLinearMatcher {
    private Path[] inputIndexPaths;
    private KmerSequenceSlice slice;
    private Configuration conf;
    
    private KmerIndexReader[] readers;
    private BigInteger sliceSize;
    private BigInteger currentProgress;
    
    private KmerMatchResult curMatch;
    private CompressedSequenceWritable[] stepKeys;
    private CompressedIntArrayWritable[] stepVals;
    private boolean stepStarted;

    
    public KmerLinearMatcher(Path[] inputIndexPaths, KmerSequenceSlice slice, Configuration conf) throws IOException {
        initialize(inputIndexPaths, slice, conf);
    }
    
    private void initialize(Path[] inputIndexPaths, KmerSequenceSlice slice, Configuration conf) throws IOException {
        this.inputIndexPaths = inputIndexPaths;
        this.slice = slice;
        this.conf = conf;
        
        Path[][] indice = KmerIndexHelper.groupKmerIndice(this.inputIndexPaths);
        this.readers = new KmerIndexReader[indice.length];
        for(int i=0;i<indice.length;i++) {
            FileSystem fs = indice[i][0].getFileSystem(this.conf);
            this.readers[i] = new KmerIndexReader(fs, FileSystemHelper.makeStringFromPath(indice[i]), this.conf);
            this.readers[i].seek(this.slice.getBeginKmer());
        }
        
        this.sliceSize = slice.getSliceSize();
        this.currentProgress = BigInteger.ZERO;
        this.curMatch = null;
        this.stepKeys = new CompressedSequenceWritable[this.readers.length];
        this.stepVals = new CompressedIntArrayWritable[this.readers.length];
        this.stepStarted = false;
    }
    
    public void reset() throws IOException {
        for(KmerIndexReader reader : this.readers) {
            reader.seek(this.slice.getBeginKmer());
        }
        
        this.currentProgress = BigInteger.ZERO;
        this.curMatch = null;
        this.stepKeys = new CompressedSequenceWritable[this.readers.length];
        this.stepVals = new CompressedIntArrayWritable[this.readers.length];
        this.stepStarted = false;
    }
    
    public boolean nextMatch() throws IOException {
        while(this.sliceSize.compareTo(this.currentProgress) > 0) {
            if(step()) {
                // find min key to find matching keys
                CompressedSequenceWritable minKey = null;
                List<Integer> minKeyIndice = new ArrayList<Integer>();

                for (int i = 0; i < this.stepKeys.length; i++) {
                    if (this.stepKeys[i] != null) {
                        if (minKey == null) {
                            minKey = this.stepKeys[i];
                            minKeyIndice.clear();
                            minKeyIndice.add(i);
                        } else {
                            if (minKey.compareTo(this.stepKeys[i]) == 0) {
                                minKeyIndice.add(i);
                            } else if (minKey.compareTo(this.stepKeys[i]) > 0) {
                                minKey = this.stepKeys[i];
                                minKeyIndice.clear();
                                minKeyIndice.add(i);
                            }
                        }
                    }
                }
                
                if(minKey == null) {
                    //EOF
                    this.curMatch = null;
                    return false;
                }

                // check matching
                if (minKeyIndice.size() > 1) {
                    CompressedIntArrayWritable[] minVals = new CompressedIntArrayWritable[minKeyIndice.size()];
                    String[][] minIndexPaths = new String[minKeyIndice.size()][];
                    
                    int valIdx = 0;
                    for (int idx : minKeyIndice) {
                        minVals[valIdx] = this.stepVals[idx];
                        minIndexPaths[valIdx] = this.readers[idx].getIndexPaths();
                        valIdx++;
                    }
                    
                    this.curMatch = new KmerMatchResult(minKey, minVals, minIndexPaths);
                    return true;
                }
            } else {
                this.curMatch = null;
                return false;
            }
        }
        
        this.curMatch = null;
        return false;
    }
    
    private boolean step() throws IOException {
        boolean hasKey = false;
        if(!this.stepStarted) {
            for(int i=0;i<this.readers.length;i++) {
                // fill first
                CompressedSequenceWritable key = new CompressedSequenceWritable();
                CompressedIntArrayWritable val = new CompressedIntArrayWritable();
                if(this.readers[i].next(key, val)) {
                    this.stepKeys[i] = key;
                    this.stepVals[i] = val;
                    hasKey = true;
                } else {
                    this.stepKeys[i] = null;
                    this.stepVals[i] = null;
                }
            }
            
            this.stepStarted = true;
            return hasKey;
        } else {
            // find min key
            CompressedSequenceWritable minKey = null;
            List<Integer> minKeyIndice = new ArrayList<Integer>();
        
            for(int i=0;i<this.readers.length;i++) {
                if(this.stepKeys[i] != null) {
                    if (minKey == null) {
                        minKey = this.stepKeys[i];
                        minKeyIndice.clear();
                        minKeyIndice.add(i);
                    } else {
                        if (minKey.compareTo(this.stepKeys[i]) == 0) {
                            minKeyIndice.add(i);
                        } else if (minKey.compareTo(this.stepKeys[i]) > 0) {
                            minKey = this.stepKeys[i];
                            minKeyIndice.clear();
                            minKeyIndice.add(i);
                        } else {
                            hasKey = true;
                        }
                    }
                }
            }
            
            if (minKey != null) {
                // move min pointers
                for(int idx : minKeyIndice) {
                    CompressedSequenceWritable key = new CompressedSequenceWritable();
                    CompressedIntArrayWritable val = new CompressedIntArrayWritable();
                    if(this.readers[idx].next(key, val)) {
                        if(this.slice.getEndKmer().compareTo(key.getSequence()) >= 0) {
                            this.stepKeys[idx] = key;
                            this.stepVals[idx] = val;
                            hasKey = true;
                        } else {
                            this.stepKeys[idx] = null;
                            this.stepVals[idx] = null;
                        }
                    } else {
                        this.stepKeys[idx] = null;
                        this.stepVals[idx] = null;
                    }
                }
                
                return hasKey;
            } else {
                //EOF
                return false;
            }
        }
    }
    
    public KmerMatchResult getCurrentMatch() {
        return this.curMatch;
    }
    
    public float getProgress() {
        if (this.sliceSize.compareTo(this.currentProgress) <= 0) {
            return 0.0f;
        } else {
            BigDecimal off = new BigDecimal(this.currentProgress).setScale(2);
            BigDecimal size = new BigDecimal(this.sliceSize).setScale(2);
            return Math.min(1.0f, off.divide(size).floatValue());
        }
    }
    
    public void close() throws IOException {
        for(KmerIndexReader reader : this.readers) {
            reader.close();
        }
    }
}
