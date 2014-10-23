package edu.arizona.cs.mrpkm.kmermatch;

import edu.arizona.cs.mrpkm.types.MultiFileReadIDWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author iychoi
 */
public class PairwiseKmerModeCounterPartitioner extends Partitioner<MultiFileReadIDWritable, IntWritable> {
    private static final Log LOG = LogFactory.getLog(PairwiseKmerModeCounterPartitioner.class);
    
    @Override
    public int getPartition(MultiFileReadIDWritable key, IntWritable value, int numReduceTasks) {
        return key.getFileID() % numReduceTasks;
    }
}
