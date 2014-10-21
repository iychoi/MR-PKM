package edu.arizona.cs.mrpkm.kmermatch;

import edu.arizona.cs.mrpkm.kmerrange.KmerRangeSlice;
import edu.arizona.cs.mrpkm.kmeridx.AKmerIndexReader;
import edu.arizona.cs.mrpkm.kmeridx.KmerIndexHelper;
import edu.arizona.cs.mrpkm.kmeridx.MultiKmerIndexReader;
import edu.arizona.cs.mrpkm.kmeridx.SingleKmerIndexReader;
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
    
    private static final int REPORT_FREQUENCY = 1000000;
    
    private Path[] inputIndexPaths;
    private KmerRangeSlice slice;
    private Configuration conf;
    
    private AKmerIndexReader[] readers;
    private BigInteger sliceSize;
    private BigInteger currentProgress;
    private BigInteger beginSequence;
    
    private KmerMatchResult curMatch;
    private CompressedSequenceWritable[] stepKeys;
    private CompressedIntArrayWritable[] stepVals;
    private List<Integer> stepMinKeys;
    private boolean stepStarted;
    private int reportCounter;

    
    public KmerLinearMatcher(Path[] inputIndexPaths, KmerRangeSlice slice, Configuration conf) throws IOException {
        initialize(inputIndexPaths, slice, conf);
    }
    
    private void initialize(Path[] inputIndexPaths, KmerRangeSlice slice, Configuration conf) throws IOException {
        this.inputIndexPaths = inputIndexPaths;
        this.slice = slice;
        this.conf = conf;
        
        Path[][] indice = KmerIndexHelper.groupKmerIndice(this.inputIndexPaths);
        this.readers = new AKmerIndexReader[indice.length];
        LOG.info("# of KmerIndexReader : " + indice.length);
        for(int i=0;i<indice.length;i++) {
            FileSystem fs = indice[i][0].getFileSystem(this.conf);
            if(indice[i].length == 1) {
                // better performance
                this.readers[i] = new SingleKmerIndexReader(fs, FileSystemHelper.makeStringFromPath(indice[i])[0], this.slice.getSliceBeginKmer(), this.slice.getSliceEndKmer(), this.conf);
            } else {
                this.readers[i] = new MultiKmerIndexReader(fs, FileSystemHelper.makeStringFromPath(indice[i]), this.slice.getSliceBeginKmer(), this.slice.getSliceEndKmer(), this.conf);
            }
        }
        
        this.sliceSize = slice.getSliceSize();
        this.currentProgress = BigInteger.ZERO;
        this.beginSequence = this.slice.getSliceBegin();
        this.curMatch = null;
        this.stepKeys = new CompressedSequenceWritable[this.readers.length];
        this.stepVals = new CompressedIntArrayWritable[this.readers.length];
        this.stepStarted = false;
        this.reportCounter = 0;
        
        LOG.info("Matcher is initialized");
        LOG.info("> Range " + this.slice.getSliceBeginKmer() + " ~ " + this.slice.getSliceEndKmer());
        LOG.info("> Num of Slice Entries : " + this.slice.getSliceSize().longValue());
    }
    
    /*
    public void reset() throws IOException {
        for(AKmerIndexReader reader : this.readers) {
            reader.reset();
        }
        
        this.currentProgress = BigInteger.ZERO;
        this.curMatch = null;
        this.stepKeys = new CompressedSequenceWritable[this.readers.length];
        this.stepVals = new CompressedIntArrayWritable[this.readers.length];
        this.stepStarted = false;
        this.reportCounter = 0;
    }
    */
    
    public boolean nextMatch() throws IOException {
        List<Integer> minKeyIndice = getNextMinKeys();
        while(minKeyIndice.size() > 0) {
            CompressedSequenceWritable minKey = this.stepKeys[minKeyIndice.get(0)];
            this.reportCounter++;
            if(this.reportCounter >= REPORT_FREQUENCY) {
                this.currentProgress = SequenceHelper.convertToBigInteger(minKey.getSequence()).subtract(this.beginSequence).add(BigInteger.ONE);
                this.reportCounter = 0;
                LOG.info("Reporting Progress : " + this.currentProgress);
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
            
            minKeyIndice = getNextMinKeys();
        }
        
        // step failed and no match
        this.curMatch = null;
        this.currentProgress = this.sliceSize;
        return false;
    }
    
    private List<Integer> findMinKeys() throws IOException {
        CompressedSequenceWritable minKey = null;
        List<Integer> minKeyIndice = new ArrayList<Integer>();
        for(int i=0;i<this.readers.length;i++) {
            if(this.stepKeys[i] != null) {
                if(minKey == null) {
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

        return minKeyIndice;
    }
    
    private List<Integer> getNextMinKeys() throws IOException {
        if(!this.stepStarted) {
            for(int i=0;i<this.readers.length;i++) {
                // fill first
                CompressedSequenceWritable key = new CompressedSequenceWritable();
                CompressedIntArrayWritable val = new CompressedIntArrayWritable();
                if(this.readers[i].next(key, val)) {
                    this.stepKeys[i] = key;
                    this.stepVals[i] = val;
                } else {
                    this.stepKeys[i] = null;
                    this.stepVals[i] = null;
                }
            }
            
            this.stepStarted = true;
            this.stepMinKeys = findMinKeys();
            return this.stepMinKeys;
        } else {
            // find min key
            if(this.stepMinKeys.size() == 0) {
                //EOF
                return this.stepMinKeys;
            }
            
            // move min pointers
            for (int idx : this.stepMinKeys) {
                CompressedSequenceWritable key = new CompressedSequenceWritable();
                CompressedIntArrayWritable val = new CompressedIntArrayWritable();
                if(this.readers[idx].next(key, val)) {
                    this.stepKeys[idx] = key;
                    this.stepVals[idx] = val;
                } else {
                    this.stepKeys[idx] = null;
                    this.stepVals[idx] = null;
                }
            }
            
            this.stepMinKeys = findMinKeys();
            return this.stepMinKeys;
        }
    }
    
    public KmerMatchResult getCurrentMatch() {
        return this.curMatch;
    }
    
    public float getProgress() {
        if (this.sliceSize.compareTo(this.currentProgress) <= 0) {
            return 1.0f;
        } else {
            BigInteger divided = this.currentProgress.multiply(BigInteger.valueOf(100)).divide(this.sliceSize);
            float f = divided.floatValue() / 100;
            
            return Math.min(1.0f, f);
        }
    }
    
    public void close() throws IOException {
        for(AKmerIndexReader reader : this.readers) {
            reader.close();
        }
    }
}
