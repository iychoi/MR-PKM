package edu.arizona.cs.mrpkm.kmeridx;

import edu.arizona.cs.mrpkm.kmerrangepartitioner.KmerRangePartition;
import edu.arizona.cs.mrpkm.kmerrangepartitioner.KmerRangePartitioner;
import edu.arizona.cs.mrpkm.namedoutputs.NamedOutputs;
import edu.arizona.cs.mrpkm.sampler.KmerSampleReader;
import edu.arizona.cs.mrpkm.sampler.KmerSamplerHelper;
import edu.arizona.cs.mrpkm.sampler.KmerSamplerReaderConfig;
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
    private KmerSamplerReaderConfig samplerConf = null;
    private KmerIndexBuilderConfig kmerIndexBuilderConfig = null;
    private int kmerSize = 0;
    private String samplePath;
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
        
        this.samplerConf = new KmerSamplerReaderConfig();
        this.samplerConf.loadFrom(this.conf);
        
        this.kmerIndexBuilderConfig = new KmerIndexBuilderConfig();
        this.kmerIndexBuilderConfig.loadFrom(conf);
        
        this.kmerSize = this.kmerIndexBuilderConfig.getKmerSize();
        if (this.kmerSize <= 0) {
            throw new RuntimeException("kmer size has to be a positive value");
        }
        
        this.samplePath = this.samplerConf.getInputPath();
        
        this.partitions = new KmerRangePartition[this.namedOutputs.getSize()][];
        this.partitionEndKeys = new CompressedSequenceWritable[this.namedOutputs.getSize()][];
    }
    
    private void initialize(int fileID, int numReduceTasks) throws IOException {
        if(this.partitionEndKeys[fileID] == null) {
            KmerSampleReader reader = null;
            // search index file
            String filename = this.namedOutputs.getNamedOutputFromID(fileID).getInputString();
            Path sampleHadoopPath = new Path(this.samplePath, KmerSamplerHelper.makeSamplingFileName(filename));
            FileSystem fs = sampleHadoopPath.getFileSystem(this.conf);
            if (fs.exists(sampleHadoopPath)) {
                reader = new KmerSampleReader(sampleHadoopPath, this.conf);
            } else {
                throw new IOException("ReadIDIndex is not found in given index paths");
            }

            KmerRangePartitioner partitioner = new KmerRangePartitioner(this.kmerSize, numReduceTasks);
            this.partitions[fileID] = partitioner.getSamplingPartitions(reader.getRecords(), reader.getSampleCount());

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
