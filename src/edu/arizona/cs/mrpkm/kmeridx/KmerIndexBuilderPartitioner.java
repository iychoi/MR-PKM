package edu.arizona.cs.mrpkm.kmeridx;

import edu.arizona.cs.mrpkm.kmerrange.KmerRangeSlice;
import edu.arizona.cs.mrpkm.kmerrange.KmerRangeSlicer;
import edu.arizona.cs.mrpkm.types.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.CompressedSequenceWritable;
import edu.arizona.cs.mrpkm.types.MultiFileCompressedSequenceWritable;
import edu.arizona.cs.mrpkm.utils.SequenceHelper;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author iychoi
 */
public class KmerIndexBuilderPartitioner extends Partitioner<MultiFileCompressedSequenceWritable, CompressedIntArrayWritable> implements Configurable {

    private static final Log LOG = LogFactory.getLog(KmerIndexBuilderPartitioner.class);
    
    private Configuration conf;
    
    private boolean initialized = false;
    private int kmerSize = 0;
    private KmerRangeSlicer slicer;
    private KmerRangeSlice[] slices;
    private CompressedSequenceWritable[] sliceEndKeys;
    
    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }
    
    private void initialize(int numReduceTasks) {
        if(!this.initialized) {
            this.kmerSize = this.conf.getInt(KmerIndexHelper.getConfigurationKeyOfKmerSize(), -1);
            if (this.kmerSize <= 0) {
                throw new RuntimeException("kmer size has to be a positive value");
            }

            this.slicer = new KmerRangeSlicer(this.kmerSize, numReduceTasks, KmerRangeSlicer.SlicerMode.MODE_WEIGHTED_RANGE);

            this.slices = this.slicer.getSlices();
            this.sliceEndKeys = new CompressedSequenceWritable[this.slices.length];
            for (int i = 0; i < this.slices.length; i++) {
                try {
                    this.sliceEndKeys[i] = new CompressedSequenceWritable(this.slices[i].getSliceEndKmer());
                } catch (IOException ex) {
                    throw new RuntimeException(ex.toString());
                }
            }

            this.initialized = true;
        }
    }
    
    @Override
    public int getPartition(MultiFileCompressedSequenceWritable key, CompressedIntArrayWritable value, int numReduceTasks) {
        if(!this.initialized) {
            initialize(numReduceTasks);
        }
        
        int partition = getPartitionIndex(key);
        if(partition < 0) {
            throw new RuntimeException("partition failed");
        }
        
        return partition;
    }

    private int getPartitionIndex(MultiFileCompressedSequenceWritable key) {
        for(int i=0;i<this.sliceEndKeys.length;i++) {
            int comp = SequenceHelper.compareSequences(key.getCompressedSequence(), this.sliceEndKeys[i].getCompressedSequence());
            if(comp <= 0) {
                return i;
            }
        }
        return -1;
    }
}
