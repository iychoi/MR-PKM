package edu.arizona.cs.mrpkm.tools;

import edu.arizona.cs.mrpkm.kmerrange.KmerRangeSlice;
import edu.arizona.cs.mrpkm.kmerrange.KmerRangeSlicer;
import edu.arizona.cs.mrpkm.utils.SequenceHelper;
import java.math.BigInteger;

/**
 *
 * @author iychoi
 */
public class KmerSequenceSliceTester {
    public static void main(String[] args) {
        int kmerSize = Integer.parseInt(args[0]);
        int numSlices = Integer.parseInt(args[1]);
        int slicerMode = Integer.parseInt(args[2]);
        
        KmerRangeSlicer.SlicerMode mode = KmerRangeSlicer.SlicerMode.values()[slicerMode];
        System.out.println("Slicer Mode : " + mode.toString());
        
        KmerRangeSlicer slicer = new KmerRangeSlicer(kmerSize, numSlices, mode);
        KmerRangeSlice slices[] = slicer.getSlices();
        BigInteger lastEnd = BigInteger.ZERO;
        System.out.println(slices.length);
        for(KmerRangeSlice slice : slices) {
            System.out.println("slice start");
            if(lastEnd.compareTo(BigInteger.ZERO) == 0) {
                // skip
            } else {
                if(lastEnd.compareTo(slice.getSliceBegin().subtract(BigInteger.ONE)) != 0) {
                    System.err.println("Error! lastend and begin not matching");
                    return;
                } else {
                    System.out.println("prev : " + SequenceHelper.convertToString(lastEnd, kmerSize));
                }
            }
            System.out.println(slice.toString());
            lastEnd = slice.getSliceEnd();
        }
        
        String kmerLast = "";
        for(int i=0;i<kmerSize;i++) {
            kmerLast += "T";
        }
        
        if(lastEnd.compareTo(SequenceHelper.convertToBigInteger(kmerLast)) != 0) {
            System.err.println("Error! range end is not real end");
        }
    }
}
