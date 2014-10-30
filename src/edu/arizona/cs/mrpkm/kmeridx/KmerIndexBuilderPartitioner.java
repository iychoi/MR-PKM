package edu.arizona.cs.mrpkm.kmeridx;

import edu.arizona.cs.mrpkm.kmerrangepartitioner.KmerRangePartition;
import edu.arizona.cs.mrpkm.kmerrangepartitioner.KmerRangePartitioner;
import edu.arizona.cs.mrpkm.namedoutputs.NamedOutputs;
import edu.arizona.cs.mrpkm.histogram.KmerHistogramReader;
import edu.arizona.cs.mrpkm.histogram.KmerHistogramHelper;
import edu.arizona.cs.mrpkm.histogram.KmerHistogramReaderConfig;
import edu.arizona.cs.mrpkm.types.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.CompressedSequenceWritable;
import edu.arizona.cs.mrpkm.types.MultiFileCompressedSequenceWritable;
import edu.arizona.cs.mrpkm.utils.SequenceHelper;
import java.io.IOException;
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
    private KmerHistogramReaderConfig histogramReaderConf = null;
    private KmerIndexBuilderConfig kmerIndexBuilderConfig = null;
    private int kmerSize = 0;
    private String histogramInputPath;
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
    
    private void initialize() {
        this.namedOutputs = new NamedOutputs();
        this.namedOutputs.loadFrom(conf);
        
        this.histogramReaderConf = new KmerHistogramReaderConfig();
        this.histogramReaderConf.loadFrom(this.conf);
        
        this.kmerIndexBuilderConfig = new KmerIndexBuilderConfig();
        this.kmerIndexBuilderConfig.loadFrom(conf);
        
        this.kmerSize = this.kmerIndexBuilderConfig.getKmerSize();
        if (this.kmerSize <= 0) {
            throw new RuntimeException("kmer size has to be a positive value");
        }
        
        this.histogramInputPath = this.histogramReaderConf.getInputPath();
        
        this.partitions = new KmerRangePartition[this.namedOutputs.getSize()][];
        this.partitionEndKeys = new CompressedSequenceWritable[this.namedOutputs.getSize()][];
    }
    
    private void initialize(int fileID, int numReduceTasks) throws IOException {
        if(this.partitionEndKeys[fileID] == null) {
            KmerHistogramReader reader = null;
            // search index file
            String filename = this.namedOutputs.getNamedOutputFromID(fileID).getInputString();
            Path histogramHadoopPath = new Path(this.histogramInputPath, KmerHistogramHelper.makeHistogramFileName(filename));
            FileSystem fs = histogramHadoopPath.getFileSystem(this.conf);
            if (fs.exists(histogramHadoopPath)) {
                reader = new KmerHistogramReader(histogramHadoopPath, this.conf);
            } else {
                throw new IOException("k-mer histogram is not found in given paths");
            }

            KmerRangePartitioner partitioner = new KmerRangePartitioner(this.kmerSize, numReduceTasks);
            this.partitions[fileID] = partitioner.getHistogramPartitions(reader.getRecords(), reader.getSampleCount());

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
            initialize();
            this.initialized = true;
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
