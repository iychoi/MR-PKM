package edu.arizona.cs.mrpkm.kmermatch;

import edu.arizona.cs.mrpkm.kmeridx.KmerIndexHelper;
import edu.arizona.cs.mrpkm.kmeridx.KmerIndexReader;
import edu.arizona.cs.mrpkm.types.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.CompressedSequenceWritable;
import edu.arizona.cs.mrpkm.utils.FileSystemHelper;
import edu.arizona.cs.mrpkm.utils.SequenceHelper;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class KmerLinearMatcher {
    
    private static final Log LOG = LogFactory.getLog(KmerLinearMatcher.class);
    
    private static final int REPORT_FREQUENCY = 1000;
    
    private Path[] inputIndexPaths;
    private KmerSequenceSlice slice;
    private Configuration conf;
    
    private KmerIndexReader[] readers;
    private BigInteger sliceSize;
    private BigInteger currentProgress;
    private BigInteger beginSequence;
    private CompressedSequenceWritable endSequence;
    
    private KmerMatchResult curMatch;
    private CompressedSequenceWritable[] stepKeys;
    private CompressedIntArrayWritable[] stepVals;
    private boolean stepStarted;
    private int reportCounter;

    
    public KmerLinearMatcher(Path[] inputIndexPaths, KmerSequenceSlice slice, Configuration conf) throws IOException {
        initialize(inputIndexPaths, slice, conf);
    }
    
    private void initialize(Path[] inputIndexPaths, KmerSequenceSlice slice, Configuration conf) throws IOException {
        this.inputIndexPaths = inputIndexPaths;
        this.slice = slice;
        this.conf = conf;
        
        Path[][] indice = KmerIndexHelper.groupKmerIndice(this.inputIndexPaths);
        this.readers = new KmerIndexReader[indice.length];
        LOG.info("# of KmerIndexReader : " + indice.length);
        for(int i=0;i<indice.length;i++) {
            FileSystem fs = indice[i][0].getFileSystem(this.conf);
            this.readers[i] = new KmerIndexReader(fs, FileSystemHelper.makeStringFromPath(indice[i]), this.slice.getBeginKmer(), this.conf);
        }
        
        this.sliceSize = slice.getSliceSize();
        this.currentProgress = BigInteger.ZERO;
        this.beginSequence = SequenceHelper.convertToBigInteger(this.slice.getBeginKmer());
        this.endSequence = new CompressedSequenceWritable(this.slice.getEndKmer());
        this.curMatch = null;
        this.stepKeys = new CompressedSequenceWritable[this.readers.length];
        this.stepVals = new CompressedIntArrayWritable[this.readers.length];
        this.stepStarted = false;
        this.reportCounter = 0;
        
        LOG.info("Matcher is initialized");
        LOG.info("> Range " + this.slice.getBeginKmer() + " ~ " + this.slice.getEndKmer());
        LOG.info("> Num of Slice Entries : " + this.slice.getSliceSize().longValue());
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
        this.reportCounter = 0;
    }
    
    public boolean nextMatch() throws IOException {
        while(true) {
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
                            int comp = minKey.compareTo(this.stepKeys[i]);
                            if (comp == 0) {
                                // found same min key
                                minKeyIndice.add(i);
                            } else if (comp > 0) {
                                // found smaller one
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
                    this.currentProgress = this.sliceSize;
                    return false;
                } else {
                    if(minKey.compareTo(this.endSequence) > 0) {
                        // no more
                        this.curMatch = null;
                        this.currentProgress = this.sliceSize;
                        return false;
                    }
                }

                this.reportCounter++;
                if(this.reportCounter >= REPORT_FREQUENCY) {
                    this.currentProgress = SequenceHelper.convertToBigInteger(minKey.getSequence()).subtract(this.beginSequence);
                    LOG.info("currentSequence : " + minKey.getSequence());
                    LOG.info("currentProgress : " + this.currentProgress + "/" + this.sliceSize);
                    this.reportCounter = 0;
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
                this.currentProgress = this.sliceSize;
                return false;
            }
        }
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
                        int comp = minKey.compareTo(this.stepKeys[i]);
                        if (comp == 0) {
                            minKeyIndice.add(i);
                        } else if (comp > 0) {
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
                        this.stepKeys[idx] = key;
                        this.stepVals[idx] = val;
                        hasKey = true;
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
            BigInteger val100 = this.currentProgress.multiply(BigInteger.valueOf(100));
            BigInteger divided = val100.divide(this.sliceSize);
            float f = divided.floatValue();
            
            return Math.min(1.0f, f / 100.0f);
        }
    }
    
    public void close() throws IOException {
        for(KmerIndexReader reader : this.readers) {
            reader.close();
        }
    }
}
