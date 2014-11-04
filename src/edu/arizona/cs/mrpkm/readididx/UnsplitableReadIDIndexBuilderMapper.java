package edu.arizona.cs.mrpkm.readididx;

import edu.arizona.cs.hadoop.fs.irods.output.HirodsMultipleOutputs;
import edu.arizona.cs.mrpkm.types.fasta.FastaRead;
import edu.arizona.cs.mrpkm.types.namedoutputs.NamedOutputs;
import edu.arizona.cs.mrpkm.types.histogram.KmerHistogram;
import edu.arizona.cs.mrpkm.helpers.MultipleOutputsHelper;
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
    private ReadIDIndexBuilderConfig builderConfig;
    private KmerHistogram[] histograms;
    
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
        
        this.builderConfig = new ReadIDIndexBuilderConfig();
        this.builderConfig.loadFrom(conf);
        
        if(this.builderConfig.getKmerSize() <= 0) {
            throw new IOException("kmer size has to be a positive value");
        }
        
        this.histograms = new KmerHistogram[this.namedOutputs.getSize()];
    }
    
    @Override
    protected void map(LongWritable key, FastaRead value, Context context) throws IOException, InterruptedException {
        int namedoutputID = this.namedOutputs.getIDFromFilename(value.getFileName());
        String namedOutput = this.namedOutputs.getRecordFromID(namedoutputID).getIdentifier();
        this.readIDs[namedoutputID]++;
        
        if (this.mos != null) {
            this.mos.write(namedOutput, new LongWritable(value.getReadOffset()), new IntWritable(this.readIDs[namedoutputID]));
        } else if (this.hmos != null) {
            this.hmos.write(namedOutput, new LongWritable(value.getReadOffset()), new IntWritable(this.readIDs[namedoutputID]));
        }
        
        if(this.histograms[namedoutputID] == null) {
            this.histograms[namedoutputID] = new KmerHistogram(namedOutput, this.builderConfig.getKmerSize());
        }
        
        this.histograms[namedoutputID].takeSample(value.getSequence());
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if(this.mos != null) {
            this.mos.close();
        }
        
        if(this.hmos != null) {
            this.hmos.close();
        }
        
        for(int i=0;i<this.histograms.length;i++) {
            if(this.histograms[i] != null) {
                if(this.histograms[i].getKmerCount() > 0) {
                    String sampleName = this.histograms[i].getHistogramName();
                    String histogramFileName = KmerHistogramHelper.makeHistogramFileName(sampleName);
                    LOG.info("making histogram file : " + histogramFileName);
                    Path histogramOutputFile = new Path(this.builderConfig.getHistogramOutputPath(), histogramFileName);
                    FileSystem outputFileSystem = histogramOutputFile.getFileSystem(context.getConfiguration());
        
                    this.histograms[i].saveTo(histogramOutputFile, outputFileSystem);
                }
                this.histograms[i] = null;
            }
        }
        this.histograms = null;
        
        this.namedOutputs = null;
        this.builderConfig = null;
    }
}
