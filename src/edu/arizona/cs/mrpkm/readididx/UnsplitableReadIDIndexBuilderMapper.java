package edu.arizona.cs.mrpkm.readididx;

import edu.arizona.cs.mrpkm.sampler.KmerSamplerWriterConfig;
import edu.arizona.cs.hadoop.fs.irods.output.HirodsMultipleOutputs;
import edu.arizona.cs.mrpkm.fastareader.types.FastaRead;
import edu.arizona.cs.mrpkm.namedoutputs.NamedOutputs;
import edu.arizona.cs.mrpkm.sampler.KmerSampler;
import edu.arizona.cs.mrpkm.sampler.KmerSamplerHelper;
import edu.arizona.cs.mrpkm.utils.MultipleOutputsHelper;
import java.io.IOException;
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
    
    private NamedOutputs namedOutputs = null;
    private MultipleOutputs mos = null;
    private HirodsMultipleOutputs hmos = null;
    private int[] readIDs;
    private KmerSamplerWriterConfig samplerConf;
    private KmerSampler[] samplers;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        if(MultipleOutputsHelper.isMultipleOutputs(conf)) {
            this.mos = new MultipleOutputs(context);
        } else if(MultipleOutputsHelper.isHirodsMultipleOutputs(conf)) {
            this.hmos = new HirodsMultipleOutputs(context);
        }
        
        this.namedOutputs = new NamedOutputs();
        this.namedOutputs.loadFrom(conf);
        
        this.readIDs = new int[this.namedOutputs.getSize()];
        for(int i=0;i<this.readIDs.length;i++) {
            this.readIDs[i] = 0;
        }
        
        this.samplerConf = new KmerSamplerWriterConfig();
        this.samplerConf.loadFrom(conf);
        
        if(this.samplerConf.getKmerSize() <= 0) {
            throw new IOException("kmer size has to be a positive value");
        }
        
        this.samplers = new KmerSampler[this.namedOutputs.getSize()];
    }
    
    @Override
    protected void map(LongWritable key, FastaRead value, Context context) throws IOException, InterruptedException {
        int namedoutputID = this.namedOutputs.getIDFromOutput(value.getFileName());
        String namedOutput = this.namedOutputs.getNamedOutputFromID(namedoutputID).getNamedOutputString();
        this.readIDs[namedoutputID]++;
        
        if (this.mos != null) {
            this.mos.write(namedOutput, new LongWritable(value.getReadOffset()), new IntWritable(this.readIDs[namedoutputID]));
        } else if (this.hmos != null) {
            this.hmos.write(namedOutput, new LongWritable(value.getReadOffset()), new IntWritable(this.readIDs[namedoutputID]));
        }
        
        if(this.samplers[namedoutputID] == null) {
            this.samplers[namedoutputID] = new KmerSampler(namedOutput, this.samplerConf.getKmerSize());
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
        
        for(int i=0;i<this.samplers.length;i++) {
            if(this.samplers[i] != null) {
                if(this.samplers[i].getSampleCount() > 0) {
                    String sampleName = this.samplers[i].getSampleName();
                    String sampleFileName = KmerSamplerHelper.makeSamplingFileName(sampleName);
                    LOG.info("making sampling file : " + sampleFileName);
                    Path samplingOutputFile = new Path(this.samplerConf.getOutputPath(), sampleFileName);
                    FileSystem outputFileSystem = samplingOutputFile.getFileSystem(context.getConfiguration());
        
                    this.samplers[i].createSamplingFile(samplingOutputFile, outputFileSystem);
                }
                this.samplers[i] = null;
            }
        }
        this.samplers = null;
        
        this.namedOutputs = null;
        this.samplerConf = null;
    }
}
