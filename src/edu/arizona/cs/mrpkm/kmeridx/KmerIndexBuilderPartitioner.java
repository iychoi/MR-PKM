package edu.arizona.cs.mrpkm.kmeridx;

import edu.arizona.cs.mrpkm.types.MultiFileCompressedSequenceWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author iychoi
 */
public class KmerIndexBuilderPartitioner<K, V> extends Partitioner<K, V> {

    private static final Log LOG = LogFactory.getLog(KmerIndexBuilderPartitioner.class);
    
    @Override
    public int getPartition(K key, V value, int numReduceTasks) {
        if(!(key instanceof MultiFileCompressedSequenceWritable)) {
            LOG.info("key is not an instance of MultiFileCompressedFastaSequenceWritable");
        }
        
        MultiFileCompressedSequenceWritable obj = (MultiFileCompressedSequenceWritable) key;
        return obj.getFileID() % numReduceTasks;
    }
}
