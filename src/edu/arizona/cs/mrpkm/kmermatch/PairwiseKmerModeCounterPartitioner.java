package edu.arizona.cs.mrpkm.kmermatch;

import edu.arizona.cs.mrpkm.types.MultiFileReadIDWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author iychoi
 */
public class PairwiseKmerModeCounterPartitioner<K, V> extends Partitioner<K, V> {
    private static final Log LOG = LogFactory.getLog(PairwiseKmerModeCounterPartitioner.class);
    
    @Override
    public int getPartition(K key, V value, int numReduceTasks) {
        if(!(key instanceof MultiFileReadIDWritable)) {
            LOG.info("key is not an instance of MultiFileReadIDWritable");
        }
        
        MultiFileReadIDWritable obj = (MultiFileReadIDWritable) key;
        return obj.getFileID() % numReduceTasks;
    }
}
