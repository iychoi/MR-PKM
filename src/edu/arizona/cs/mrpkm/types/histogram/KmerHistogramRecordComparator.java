package edu.arizona.cs.mrpkm.types.histogram;

import java.util.Comparator;

/**
 *
 * @author iychoi
 */
public class KmerHistogramRecordComparator implements Comparator<KmerHistogramRecord> {

    @Override
    public int compare(KmerHistogramRecord t, KmerHistogramRecord t1) {
        return t.getKmer().compareToIgnoreCase(t1.getKmer());
    }
}
