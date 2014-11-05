package edu.arizona.cs.mrpkm.kmermatch;

import edu.arizona.cs.mrpkm.types.hadoop.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.hadoop.MultiFileReadIDWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author iychoi
 */
public class ModeCounterPartitioner extends Partitioner<MultiFileReadIDWritable, CompressedIntArrayWritable> {
    private static final Log LOG = LogFactory.getLog(ModeCounterPartitioner.class);
    
    @Override
    public int getPartition(MultiFileReadIDWritable key, CompressedIntArrayWritable value, int numReduceTasks) {
        return key.getFileID() % numReduceTasks;
    }
}
