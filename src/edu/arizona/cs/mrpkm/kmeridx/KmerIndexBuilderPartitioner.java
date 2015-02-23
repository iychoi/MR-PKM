package edu.arizona.cs.mrpkm.kmeridx;

import edu.arizona.cs.mrpkm.types.kmerrangepartition.KmerRangePartition;
import edu.arizona.cs.mrpkm.types.kmerrangepartition.KmerRangePartitioner;
import edu.arizona.cs.mrpkm.types.namedoutputs.NamedOutputs;
import edu.arizona.cs.mrpkm.types.histogram.KmerHistogram;
import edu.arizona.cs.mrpkm.readididx.KmerHistogramHelper;
import edu.arizona.cs.mrpkm.types.hadoop.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.hadoop.CompressedSequenceWritable;
import edu.arizona.cs.mrpkm.types.hadoop.MultiFileCompressedSequenceWritable;
import edu.arizona.cs.mrpkm.helpers.SequenceHelper;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author iychoi
 */
public class KmerIndexBuilderPartitioner extends Partitioner<MultiFileCompressedSequenceWritable, CompressedIntArrayWritable> implements Configurable {

    private static final Log LOG = LogFactory.getLog(KmerIndexBuilderPartitioner.class);
    
    private Configuration conf;
    
    private boolean initialized = false;
    private NamedOutputs namedOutputs = null;
    private KmerIndexBuilderConfig builderConfig = null;
    private KmerRangePartition[][] partitions;
    private CompressedSequenceWritable[][] partitionEndKeys;
    
    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }
    
    private void initialize() throws IOException {
        this.namedOutputs = new NamedOutputs();
        this.namedOutputs.loadFrom(conf);
        
        this.builderConfig = new KmerIndexBuilderConfig();
        this.builderConfig.loadFrom(conf);
        
        if (this.builderConfig.getKmerSize() <= 0) {
            throw new RuntimeException("kmer size has to be a positive value");
        }
        
        this.partitions = new KmerRangePartition[this.namedOutputs.getSize()][];
        this.partitionEndKeys = new CompressedSequenceWritable[this.namedOutputs.getSize()][];
    }
    
    private void initialize(int fileID, int numReduceTasks) throws IOException {
        if(this.partitionEndKeys[fileID] == null) {
            KmerHistogram histogram = null;
            // search index file
            String filename = this.namedOutputs.getRecordFromID(fileID).getFilename();
            Path histogramHadoopPath = new Path(this.builderConfig.getHistogramPath(), KmerHistogramHelper.makeHistogramFileName(filename));
            FileSystem fs = histogramHadoopPath.getFileSystem(this.conf);
            if (fs.exists(histogramHadoopPath)) {
                histogram = new KmerHistogram();
                histogram.loadFrom(histogramHadoopPath, fs);
            } else {
                throw new IOException("k-mer histogram is not found in given paths");
            }

            KmerRangePartitioner partitioner = new KmerRangePartitioner(this.builderConfig.getKmerSize(), numReduceTasks);
            this.partitions[fileID] = partitioner.getHistogramPartitions(histogram.getSortedRecords(), histogram.getKmerCount());

            this.partitionEndKeys[fileID] = new CompressedSequenceWritable[numReduceTasks];
            for (int i = 0; i < this.partitions[fileID].length; i++) {
                try {
                    this.partitionEndKeys[fileID][i] = new CompressedSequenceWritable(this.partitions[fileID][i].getPartitionEndKmer());
                } catch (IOException ex) {
                    throw new RuntimeException(ex.toString());
                }
            }
        }
    }
    
    @Override
    public int getPartition(MultiFileCompressedSequenceWritable key, CompressedIntArrayWritable value, int numReduceTasks) {
        if(!this.initialized) {
            try {
                initialize();
                this.initialized = true;
            } catch (IOException ex) {
                throw new RuntimeException(ex.toString());
            }
        }
        
        try {
            initialize(key.getFileID(), numReduceTasks);
        } catch (IOException ex) {
            throw new RuntimeException(ex.toString());
        }
        
        int partition = getPartitionIndex(key);
        if(partition < 0) {
            throw new RuntimeException("partition failed");
        }
        
        return partition;
    }

    private int getPartitionIndex(MultiFileCompressedSequenceWritable key) {
        int fileID = key.getFileID();
        for(int i=0;i<this.partitionEndKeys[fileID].length;i++) {
            int comp = SequenceHelper.compareSequences(key.getCompressedSequence(), this.partitionEndKeys[fileID][i].getCompressedSequence());
            if(comp <= 0) {
                return i;
            }
        }
        return -1;
    }
}
