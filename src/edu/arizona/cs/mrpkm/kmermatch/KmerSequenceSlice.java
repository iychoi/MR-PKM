package edu.arizona.cs.mrpkm.kmermatch;

import edu.arizona.cs.mrpkm.utils.SequenceHelper;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;
import org.apache.hadoop.io.Text;

/**
 *
 * @author iychoi
 */
public class KmerSequenceSlice {
    private int kmerSize;
    private int numSlices;
    private int sliceIndex;
    private BigInteger sliceSize;
    private BigInteger sliceBegin;
    private BigInteger sliceEnd;
    
    public KmerSequenceSlice() {
    }
    
    public KmerSequenceSlice(int kmerSize, int numSlices, int sliceIndex, BigInteger sliceSize, BigInteger sliceBegin, BigInteger sliceEnd) {
        this.kmerSize = kmerSize;
        this.numSlices = numSlices;
        this.sliceIndex = sliceIndex;
        this.sliceSize = sliceSize;
        this.sliceBegin = sliceBegin;
        this.sliceEnd = sliceEnd;
    }
    
    public int getKmerSize() {
        return this.kmerSize;
    }
    
    public int getNumSlices() {
        return this.numSlices;
    }
    
    public int getSliceIndex() {
        return this.sliceIndex;
    }
    
    public BigInteger getSliceSize() {
        return this.sliceSize;
    }
    
    public BigInteger getSliceBegin() {
        return this.sliceBegin;
    }
    
    public String getSliceBeginKmer() {
        return SequenceHelper.convertToString(this.sliceBegin, this.kmerSize);
    }
    
    public BigInteger getSliceEnd() {
        return this.sliceEnd;
    }
    
    public String getSliceEndKmer() {
        return SequenceHelper.convertToString(this.sliceEnd, this.kmerSize);
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.kmerSize);
        out.writeInt(this.numSlices);
        out.writeInt(this.sliceIndex);
        Text.writeString(out, this.sliceSize.toString());
        Text.writeString(out, this.sliceBegin.toString());
        Text.writeString(out, this.sliceEnd.toString());
    }

    public void read(DataInput in) throws IOException {
        this.kmerSize = in.readInt();
        this.numSlices = in.readInt();
        this.sliceIndex = in.readInt();
        this.sliceSize = new BigInteger(Text.readString(in));
        this.sliceBegin = new BigInteger(Text.readString(in));
        this.sliceEnd = new BigInteger(Text.readString(in));
    }
    
    @Override
    public String toString() {
        return "kmerSize : " + this.kmerSize + ", numSlices : " + this.numSlices + ", sliceIndex : " + this.sliceIndex +
                ", sliceSize : " + this.sliceSize.toString() + ", beginKmer : " + SequenceHelper.convertToString(this.sliceBegin, this.kmerSize) + ", endKmer : " + SequenceHelper.convertToString(this.sliceEnd, this.kmerSize);
    }
}
