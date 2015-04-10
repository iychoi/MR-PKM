package edu.arizona.cs.mrpkm.kmerfreqcomp;

import edu.arizona.cs.mrpkm.types.hadoop.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.hadoop.CompressedSequenceWritable;

/**
 *
 * @author iychoi
 */
public class KmerFrequencyComparisonResult {
    private CompressedSequenceWritable key;
    private CompressedIntArrayWritable posVals;
    private CompressedIntArrayWritable negVals;
    private String[][] indexPaths;
    
    public KmerFrequencyComparisonResult(CompressedSequenceWritable key, CompressedIntArrayWritable posVals, CompressedIntArrayWritable negVals, String[][] indexPaths) {
        this.key = key;
        this.posVals = posVals;
        this.negVals = negVals;
        this.indexPaths = indexPaths;
    }
    
    public CompressedSequenceWritable getKey() {
        return this.key;
    }
    
    public CompressedIntArrayWritable getPosVals() {
        return this.posVals;
    }
    
    public CompressedIntArrayWritable getNegVals() {
        return this.negVals;
    }
    
    public String[][] getIndexPaths() {
        return this.indexPaths;
    }
}
