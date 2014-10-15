package edu.arizona.cs.mrpkm.readididx;

import edu.arizona.cs.mrpkm.types.CompressedLongArrayWritable;
import edu.arizona.cs.mrpkm.types.MultiFileOffsetWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author iychoi
 */
public class ReadIDIndexBuilderPartitioner extends Partitioner<MultiFileOffsetWritable, CompressedLongArrayWritable> {

    private static final Log LOG = LogFactory.getLog(ReadIDIndexBuilderPartitioner.class);

    @Override
    public int getPartition(MultiFileOffsetWritable key, CompressedLongArrayWritable value, int numReduceTasks) {
        return key.getFileID() % numReduceTasks;
    }
}
