package edu.arizona.cs.mrpkm.kmermatch;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class KmerSequenceSlicer {
    
    private static final Log LOG = LogFactory.getLog(KmerSequenceSlicer.class);
    
    private int kmerSize;
    private int numSlices;
    private SlicerMode mode;
    
    private List<KmerSequenceSlice> slices = new ArrayList<KmerSequenceSlice>();
    
    public enum SlicerMode {
        MODE_EQUAL_RANGE,
        MODE_EQUAL_ENTRIES
    }

    public KmerSequenceSlicer(int kmerSize, int numSlices, SlicerMode mode) {
        this.kmerSize = kmerSize;
        this.numSlices = numSlices;
        this.mode = mode;
        
        if(mode.equals(SlicerMode.MODE_EQUAL_RANGE)) {
            LOG.info("Slicer - Equal Kmer Range Mode");
            calc_equal_range();
        } else if(mode.equals(SlicerMode.MODE_EQUAL_ENTRIES)) {
            LOG.info("Slicer - Equal Kmer Entries Mode");
            calc_equal_area();
        } else {
            LOG.info("Slicer - Equal Kmer Range Mode");
            calc_equal_range();
        }
    }

    public KmerSequenceSlice[] getSlices() {
        return this.slices.toArray(new KmerSequenceSlice[0]);
    }
    
    private void calc_equal_range() {
        // calc 4^kmerSize
        BigInteger kmerend = BigInteger.valueOf(4).pow(this.kmerSize);
        
        BigInteger slice_width = kmerend.divide(BigInteger.valueOf(this.numSlices));
        if(kmerend.mod(BigInteger.valueOf(this.numSlices)).intValue() != 0) {
            slice_width = slice_width.add(BigInteger.ONE);
        }
        
        for(int i=0;i<this.numSlices;i++) {
            BigInteger slice_begin = slice_width.multiply(BigInteger.valueOf(i));
            if(slice_begin.add(slice_width).compareTo(kmerend) > 0) {
                slice_width = kmerend.subtract(slice_begin);
            } 

            BigInteger slice_end = slice_begin.add(slice_width).subtract(BigInteger.ONE);

            KmerSequenceSlice slice = new KmerSequenceSlice(this.kmerSize, this.numSlices, i, slice_width, slice_begin, slice_end);
            this.slices.add(slice);
        }
    }
    
    private void calc_equal_area() {
        // calc 4^kmerSize
        BigInteger kmerend = BigInteger.valueOf(4).pow(this.kmerSize);
        BigDecimal bdkmerend = new BigDecimal(kmerend);
        // moves between x (0~1) y (0~1)
        // sum of area (0.5)
        double kmerArea = 0.5;
        double sliceArea = kmerArea / this.numSlices;
        
        // we think triangle is horizontally flipped so calc get easier.
        double x1 = 0;
        
        List<BigInteger> widths = new ArrayList<BigInteger>();
        BigInteger widthSum = BigInteger.ZERO;
        for(int i=0;i<this.numSlices;i++) {
            // x2*x2 = 2*sliceArea + x1*x1
            double temp = (2*sliceArea) + (x1*x1);
            double x2 = Math.sqrt(temp);
            
            BigDecimal bdx1 = BigDecimal.valueOf(x1);
            BigDecimal bdx2 = BigDecimal.valueOf(x2);
            
            // if i increases, bdw will be decreased
            BigDecimal bdw = bdx2.subtract(bdx1);
            
            BigInteger bw = bdw.multiply(bdkmerend).toBigInteger();
            
            if(widthSum.add(bw).compareTo(kmerend) > 0) {
                bw = kmerend.subtract(widthSum);
            }
            
            if(i == this.numSlices - 1) {
                // last case
                if(widthSum.add(bw).compareTo(kmerend) < 0) {
                    bw = kmerend.subtract(widthSum);
                }    
            }
            
            // save it
            widths.add(bw);
            widthSum = widthSum.add(bw);
            
            x1 = x2;
        }
        
        BigInteger cur_begin = BigInteger.ZERO;
        for(int i=0;i<this.numSlices;i++) {
            BigInteger slice_width = widths.get(this.numSlices - 1 - i);
            
            BigInteger slice_begin = cur_begin;
            
            if(slice_begin.add(slice_width).compareTo(kmerend) > 0) {
                slice_width = kmerend.subtract(slice_begin);
            }
            
            BigInteger slice_end = cur_begin.add(slice_width).subtract(BigInteger.ONE);
            
            KmerSequenceSlice slice = new KmerSequenceSlice(this.kmerSize, this.numSlices, i, slice_width, slice_begin, slice_end);
            this.slices.add(slice);
            
            cur_begin = cur_begin.add(slice_width);
        }
    }
}
