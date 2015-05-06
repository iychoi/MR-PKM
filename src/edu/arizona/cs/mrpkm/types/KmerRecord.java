package edu.arizona.cs.mrpkm.types;

import edu.arizona.cs.mrpkm.helpers.SequenceHelper;

/**
 *
 * @author iychoi
 */
public class KmerRecord {
    private String sequence;
    private int readID;
    
    public KmerRecord(String sequence, int readID) {
        this.sequence = sequence;
        this.readID = readID;
    }
    
    public String getSequence() {
        return this.sequence;
    }
    
    public int getReadID() {
        return this.readID;
    }
    
    public boolean isForward() {
        return this.readID >= 0;
    }
    
    public boolean isReverseComplement() {
        return this.readID < 0;
    }
    
    public KmerRecord getOriginalForm() {
        if(this.isForward()) {
            return this;
        } else {
            String another = SequenceHelper.getReverseComplement(this.sequence);
            return new KmerRecord(another, -1 * this.readID);
        }
    }
    
    public KmerRecord getSmallerForm() {
        String another = SequenceHelper.getReverseComplement(this.sequence);
        if(another.compareTo(this.sequence) < 0) {
            return new KmerRecord(another, -1 * this.readID);
        } else {
            return this;
        }
    }
}
