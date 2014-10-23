package edu.arizona.cs.mrpkm.readididx;

import edu.arizona.cs.hadoop.fs.irods.output.HirodsMultipleOutputs;
import edu.arizona.cs.mrpkm.fastareader.types.FastaRead;
import edu.arizona.cs.mrpkm.sampler.KmerSampler;
import edu.arizona.cs.mrpkm.sampler.KmerSamplerHelper;
import edu.arizona.cs.mrpkm.utils.MultipleOutputsHelper;
import java.io.IOException;
import java.util.Hashtable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 *
 * @author iychoi
 */
public class UnsplitableReadIDIndexBuilderMapper extends Mapper<LongWritable, FastaRead, LongWritable, IntWritable> {
    
    private static final Log LOG = LogFactory.getLog(UnsplitableReadIDIndexBuilderMapper.class);
    
    private int kmerSize;
    private String samplingOutputPath;
    
    private MultipleOutputs mos = null;
    private HirodsMultipleOutputs hmos = null;
    private Hashtable<String, Integer> namedOutputIDCache;
    private Hashtable<Integer, String> namedOutputCache;
    private int[] readIDs;
    private KmerSampler[] samplers;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        if(MultipleOutputsHelper.isMultipleOutputs(conf)) {
            this.mos = new MultipleOutputs(context);
        } else if(MultipleOutputsHelper.isHirodsMultipleOutputs(conf)) {
            this.hmos = new HirodsMultipleOutputs(context);
        }
        
        this.namedOutputIDCache = new Hashtable<String, Integer>();
        this.namedOutputCache = new Hashtable<Integer, String>();
        int numberOfOutputs = conf.getInt(ReadIDIndexHelper.getConfigurationKeyOfNamedOutputNum(), -1);
        if(numberOfOutputs <= 0) {
            throw new IOException("number of outputs is zero or negative");
        }
        
        this.readIDs = new int[numberOfOutputs];
        for(int i=0;i<this.readIDs.length;i++) {
            this.readIDs[i] = 0;
        }
        
        this.kmerSize = conf.getInt(KmerSamplerHelper.getConfigurationKeyOfKmerSize(), -1);
        if(this.kmerSize <= 0) {
            throw new IOException("kmer size has to be a positive value");
        }
        
        this.samplers = new KmerSampler[numberOfOutputs];
        this.samplingOutputPath = conf.get(KmerSamplerHelper.getConfigurationKeyOfOutputPath());
    }
    
    @Override
    protected void map(LongWritable key, FastaRead value, Context context) throws IOException, InterruptedException {
        Integer namedoutputID = this.namedOutputIDCache.get(value.getFileName());
        if (namedoutputID == null) {
            namedoutputID = context.getConfiguration().getInt(ReadIDIndexHelper.getConfigurationKeyOfNamedOutputID(value.getFileName()), -1);
            if (namedoutputID < 0) {
                throw new IOException("No named output found : " + ReadIDIndexHelper.getConfigurationKeyOfNamedOutputID(value.getFileName()));
            }
            this.namedOutputIDCache.put(value.getFileName(), namedoutputID);
        }
        
        String namedOutput = this.namedOutputCache.get(namedoutputID);
        if (namedOutput == null) {
            namedOutput = context.getConfiguration().get(ReadIDIndexHelper.getConfigurationKeyOfNamedOutputName(namedoutputID));
            if (namedOutput == null) {
                throw new IOException("no named output found");
            }
            this.namedOutputCache.put(namedoutputID, namedOutput);
        }
        
        this.readIDs[namedoutputID]++;
        
        if (this.mos != null) {
            this.mos.write(namedOutput, new LongWritable(value.getReadOffset()), new IntWritable(this.readIDs[namedoutputID]));
        } else if (this.hmos != null) {
            this.hmos.write(namedOutput, new LongWritable(value.getReadOffset()), new IntWritable(this.readIDs[namedoutputID]));
        }
        
        if(this.samplers[namedoutputID] == null) {
            this.samplers[namedoutputID] = new KmerSampler(namedOutput, this.kmerSize);
        }
        
        this.samplers[namedoutputID].takeSample(value.getSequence());
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if(this.mos != null) {
            this.mos.close();
        }
        
        if(this.hmos != null) {
            this.hmos.close();
        }
        
        this.namedOutputIDCache = null;
        this.namedOutputCache = null;
        
        for(int i=0;i<this.samplers.length;i++) {
            if(this.samplers[i] != null) {
                if(this.samplers[i].getSampleCount() > 0) {
                    Path samplingOutputFile = new Path(this.samplingOutputPath, KmerSamplerHelper.getSamplingFileName(this.samplers[i].getSampleName()));
                    FileSystem outputFileSystem = samplingOutputFile.getFileSystem(context.getConfiguration());
        
                    this.samplers[i].createSamplingFile(samplingOutputFile, outputFileSystem);
                }
                this.samplers[i] = null;
            }
        }
        this.samplers = null;
    }
}
