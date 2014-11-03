package edu.arizona.cs.mrpkm.kmermatch;

import edu.arizona.cs.mrpkm.types.hadoop.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.hadoop.CompressedSequenceWritable;

/**
 *
 * @author iychoi
 */
public class KmerMatchResult {
    private CompressedSequenceWritable key;
    private CompressedIntArrayWritable[] vals;
    private String[][] indexPaths;
    
    public KmerMatchResult(CompressedSequenceWritable key, CompressedIntArrayWritable[] vals, String[][] indexPaths) {
        this.key = key;
        this.vals = vals;
        this.indexPaths = indexPaths;
    }
    
    public CompressedSequenceWritable getKey() {
        return this.key;
    }
    
    public CompressedIntArrayWritable[] getVals() {
        return this.vals;
    }
    
    public String[][] getIndexPaths() {
        return this.indexPaths;
    }
}
