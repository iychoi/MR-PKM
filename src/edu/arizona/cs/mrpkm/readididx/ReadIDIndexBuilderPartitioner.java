package edu.arizona.cs.mrpkm.readididx;

import edu.arizona.cs.mrpkm.types.MultiFileOffsetWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author iychoi
 */
public class ReadIDIndexBuilderPartitioner<K, V> extends Partitioner<K, V> {

    private static final Log LOG = LogFactory.getLog(ReadIDIndexBuilderPartitioner.class);
    
    @Override
    public int getPartition(K key, V value, int numReduceTasks) {
        if(!(key instanceof MultiFileOffsetWritable)) {
            LOG.info("key is not an instance of MultiFileOffsetWritable");
        }
        
        MultiFileOffsetWritable obj = (MultiFileOffsetWritable) key;
        return obj.getFileID() % numReduceTasks;
    }
}
