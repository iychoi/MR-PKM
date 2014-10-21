package edu.arizona.cs.mrpkm.kmeridx;

import edu.arizona.cs.mrpkm.types.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.CompressedSequenceWritable;

/**
 *
 * @author iychoi
 */
public class KmerIndexBufferEntry {
    private CompressedSequenceWritable key;
    private CompressedIntArrayWritable val;

    public KmerIndexBufferEntry(CompressedSequenceWritable key, CompressedIntArrayWritable val) {
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
