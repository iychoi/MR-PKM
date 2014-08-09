package edu.arizona.cs.mrpkm.kmermatch;

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
    private String beginKmer;
    private String endKmer;
    
    public KmerSequenceSlice() {
    }
    
    public KmerSequenceSlice(int kmerSize, int numSlices, int sliceIndex) {
        this.kmerSize = kmerSize;
        this.numSlices = numSlices;
        this.sliceIndex = sliceIndex;
        
        calc();
    }
    
    private void calc() {
        String begin = "";
        String end = "";
        
        // calc 4^kmerSize
        BigInteger bi = BigInteger.valueOf(4).pow(this.kmerSize);
        
        BigInteger slice_width = bi.divide(BigInteger.valueOf(this.numSlices));
        if(bi.mod(BigInteger.valueOf(this.numSlices)).intValue() != 0) {
            slice_width = slice_width.add(BigInteger.ONE);
        }
        
        this.sliceSize = slice_width;
        
        
        BigInteger slice_begin = slice_width.multiply(BigInteger.valueOf(this.sliceIndex));
        BigInteger slice_end = slice_begin.add(slice_width).subtract(BigInteger.ONE);
        
        if(slice_end.compareTo(bi) >= 0) {
            slice_end = bi.subtract(BigInteger.ONE);
        }
        
        for(int i=0;i<this.kmerSize;i++) {
            int idx = slice_begin.mod(BigInteger.valueOf(4)).intValue();
            if(idx == 0) {
                begin = "A" + begin;
            } else if(idx == 1) {
                begin = "C" + begin;
            } else if(idx == 2) {
                begin = "G" + begin;
            } else if(idx == 3) {
                begin = "T" + begin;
            }
            slice_begin = slice_begin.divide(BigInteger.valueOf(4));
        }
        
        for(int i=0;i<this.kmerSize;i++) {
            int idx = slice_end.mod(BigInteger.valueOf(4)).intValue();
            if(idx == 0) {
                end = "A" + end;
            } else if(idx == 1) {
                end = "C" + end;
            } else if(idx == 2) {
                end = "G" + end;
            } else if(idx == 3) {
                end = "T" + end;
            }
            slice_end = slice_end.divide(BigInteger.valueOf(4));
        }
        
        this.beginKmer = begin;
        this.endKmer = end;
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
    
    public String getBeginKmer() {
        return this.beginKmer;
    }
    
    public String getEndKmer() {
        return this.endKmer;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.kmerSize);
        out.writeInt(this.numSlices);
        out.writeInt(this.sliceIndex);
        Text.writeString(out, this.sliceSize.toString());
        Text.writeString(out, this.beginKmer);
        Text.writeString(out, this.endKmer);
    }

    public void read(DataInput in) throws IOException {
        this.kmerSize = in.readInt();
        this.numSlices = in.readInt();
        this.sliceIndex = in.readInt();
        this.sliceSize = new BigInteger(Text.readString(in));
        this.beginKmer = Text.readString(in);
        this.endKmer = Text.readString(in);
    }
    
    @Override
    public String toString() {
        return "kmerSize : " + this.kmerSize + ", numSlices : " + this.numSlices + ", sliceIndex : " + this.sliceIndex +
                ", sliceSize : " + this.sliceSize.toString() + ", beginKmer : " + this.beginKmer + ", endKmer : " + this.endKmer;
    }
}
