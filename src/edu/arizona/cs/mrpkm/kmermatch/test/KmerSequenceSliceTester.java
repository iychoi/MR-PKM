package edu.arizona.cs.mrpkm.kmermatch.test;

import edu.arizona.cs.mrpkm.kmermatch.KmerSequenceSlice;
import edu.arizona.cs.mrpkm.kmermatch.KmerSequenceSlicer;

/**
 *
 * @author iychoi
 */
public class KmerSequenceSliceTester {
    public static void main(String[] args) {
        int kmerSize = Integer.parseInt(args[0]);
        int numSlices = Integer.parseInt(args[1]);
        
        KmerSequenceSlicer slicer = new KmerSequenceSlicer(kmerSize, numSlices);
        KmerSequenceSlice slices[] = slicer.getSlices();
        for(KmerSequenceSlice slice : slices) {
            System.out.println(slice.toString());
        }
    }
}
