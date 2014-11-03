package edu.arizona.cs.mrpkm.kmeridx;

import edu.arizona.cs.mrpkm.types.KmerRecord;
import edu.arizona.cs.mrpkm.types.hadoop.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.hadoop.MultiFileCompressedSequenceWritable;
import edu.arizona.cs.mrpkm.readididx.ReadIDIndexReader;
import edu.arizona.cs.mrpkm.readididx.ReadIDNotFoundException;
import edu.arizona.cs.mrpkm.hadoop.io.format.fasta.types.FastaRead;
import edu.arizona.cs.mrpkm.types.namedoutputs.NamedOutputs;
import edu.arizona.cs.mrpkm.readididx.ReadIDIndexHelper;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * @author iychoi
 */
public class KmerIndexBuilderMapper extends Mapper<LongWritable, FastaRead, MultiFileCompressedSequenceWritable, CompressedIntArrayWritable> {
    
    private static final Log LOG = LogFactory.getLog(KmerIndexBuilderMapper.class);
    
    private NamedOutputs namedOutputs = null;
    private KmerIndexBuilderConfig kmerIndexBuilderConfig = null;
    private int kmerSize = 0;
    private int previousReadID = -1;
    private ReadIDIndexReader readIDIndexReader;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        this.namedOutputs = new NamedOutputs();
        this.namedOutputs.loadFrom(conf);
        
        this.kmerIndexBuilderConfig = new KmerIndexBuilderConfig();
        this.kmerIndexBuilderConfig.loadFrom(conf);
        
        this.kmerSize = this.kmerIndexBuilderConfig.getKmerSize();
        if(this.kmerSize <= 0) {
            throw new IOException("kmer size has to be a positive value");
        }
        
        Path inputFilePath = ((FileSplit) context.getInputSplit()).getPath();
        Path readIDIndexPath = new Path(this.kmerIndexBuilderConfig.getReadIDIndexPath(), ReadIDIndexHelper.makeReadIDIndexFileName(inputFilePath.getName()));
        FileSystem fs = readIDIndexPath.getFileSystem(conf);
        if(fs.exists(readIDIndexPath)) {
            this.readIDIndexReader = new ReadIDIndexReader(fs, readIDIndexPath.toString(), conf);
        } else {
            throw new IOException("ReadIDIndex is not found");
        }
    }
    
    @Override
    protected void map(LongWritable key, FastaRead value, Context context) throws IOException, InterruptedException {
        int readID = 0;
        if(value.getContinuousRead()) {
            readID = this.previousReadID + 1;
        } else {
            long startOffset = value.getReadOffset();
            try {
                readID = this.readIDIndexReader.findReadID(startOffset);
            } catch (ReadIDNotFoundException ex) {
                throw new IOException("No Read ID found : offset " + startOffset);
            }
        }
        
        this.previousReadID = readID;
        
        String sequence = value.getSequence();
        int namedoutputID = this.namedOutputs.getIDFromFilename(value.getFileName());
        
        for (int i = 0; i < (sequence.length() - this.kmerSize + 1); i++) {
            String kmer = sequence.substring(i, i + this.kmerSize);
            int rid = readID;
            
            KmerRecord kmerRecord = new KmerRecord(kmer, rid);
            KmerRecord smallKmerRecord = kmerRecord.getSmallerForm();
            
            int[] rid_arr = new int[1];
            rid_arr[0] = smallKmerRecord.getReadID();
            context.write(new MultiFileCompressedSequenceWritable(namedoutputID, smallKmerRecord.getSequence()), new CompressedIntArrayWritable(rid_arr));
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        this.namedOutputs = null;
        this.kmerIndexBuilderConfig = null;
        
        this.readIDIndexReader.close();
        this.readIDIndexReader = null;
    }
}
