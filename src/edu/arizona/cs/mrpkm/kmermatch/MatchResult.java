package edu.arizona.cs.mrpkm.kmermatch;

import edu.arizona.cs.mrpkm.kmeridx.types.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.kmeridx.types.CompressedSequenceWritable;

/**
 *
 * @author iychoi
 */
public class MatchResult {
    private CompressedSequenceWritable key;
    private CompressedIntArrayWritable[] vals;
    private String[][] indexPaths;
    
    public MatchResult(CompressedSequenceWritable key, CompressedIntArrayWritable[] vals, String[][] indexPaths) {
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
