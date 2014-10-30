package edu.arizona.cs.mrpkm.kmeridx;

import edu.arizona.cs.mrpkm.types.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.CompressedSequenceWritable;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 *
 * @author iychoi
 */
public class FilteredKmerIndexReader extends AKmerIndexReader {

    private static final Log LOG = LogFactory.getLog(FilteredKmerIndexReader.class);
    
    private AKmerIndexReader kmerIndexReader;
    private double avg;
    private double stddeviation;
    private double factor;
    
    public FilteredKmerIndexReader(FileSystem fs, String[] indexPaths, Configuration conf, double avg, double stddeviation, double factor) throws IOException {
        initialize(fs, indexPaths, null, null, conf, avg, stddeviation, factor);
    }
    
    public FilteredKmerIndexReader(FileSystem fs, String[] indexPaths, CompressedSequenceWritable beginKey, CompressedSequenceWritable endKey, Configuration conf, double avg, double stddeviation, double factor) throws IOException {
        initialize(fs, indexPaths, beginKey, endKey, conf, avg, stddeviation, factor);
    }
    
    public FilteredKmerIndexReader(FileSystem fs, String[] indexPaths, String beginKey, String endKey, Configuration conf, double avg, double stddeviation, double factor) throws IOException {
        initialize(fs, indexPaths, new CompressedSequenceWritable(beginKey), new CompressedSequenceWritable(endKey), conf, avg, stddeviation, factor);
    }
    
    private void initialize(FileSystem fs, String[] indexPaths, CompressedSequenceWritable beginKey, CompressedSequenceWritable endKey, Configuration conf, double avg, double stddeviation, double factor) throws IOException {
        this.avg = avg;
        this.stddeviation = stddeviation;
        this.factor = factor;
        if(indexPaths.length == 1) {
            this.kmerIndexReader = new SingleKmerIndexReader(fs, indexPaths[0], beginKey, endKey, conf);    
        } else {
            this.kmerIndexReader = new MultiKmerIndexReader(fs, indexPaths, beginKey, endKey, conf);
        }
    }
    
    @Override
    public String[] getIndexPaths() {
        return this.kmerIndexReader.getIndexPaths();
    }

    @Override
    public boolean next(CompressedSequenceWritable key, CompressedIntArrayWritable val) throws IOException {
        CompressedSequenceWritable tempKey = new CompressedSequenceWritable();
        CompressedIntArrayWritable tempVal = new CompressedIntArrayWritable();
        
        while(this.kmerIndexReader.next(tempKey, tempVal)) {
            double diffPositive = Math.abs(this.avg - tempVal.getPositiveEntriesCount());
            double diffNegative = Math.abs(this.avg - tempVal.getNegativeEntriesCount());
            double boundary = Math.ceil(Math.abs(this.stddeviation * this.factor));
            
            if(diffPositive <= boundary && diffNegative <= boundary) {
                key.set(tempKey);
                val.set(tempVal);
                return true;
            } else if(diffPositive <= boundary) {
                key.set(tempKey);
                int[] positiveArr = new int[tempVal.getPositiveEntriesCount()];
                int j=0;
                int[] valArr = tempVal.get();
                for(int i=0;i<valArr.length;i++) {
                    if(valArr[i] >= 0) {
                        positiveArr[j] = valArr[i];
                        j++;
                    }
                }
                val.set(positiveArr);
                return true;
            } else if(diffNegative <= boundary) {
                key.set(tempKey);
                int[] negativeArr = new int[tempVal.getNegativeEntriesCount()];
                int j=0;
                int[] valArr = tempVal.get();
                for(int i=0;i<valArr.length;i++) {
                    if(valArr[i] < 0) {
                        negativeArr[j] = valArr[i];
                        j++;
                    }
                }
                val.set(negativeArr);
                return true;
            }
        }
        return false;
    }
    
    @Override
    public void close() throws IOException {
        this.kmerIndexReader.close();
    }
}
