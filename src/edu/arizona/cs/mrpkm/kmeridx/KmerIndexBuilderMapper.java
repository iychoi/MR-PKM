package edu.arizona.cs.mrpkm.kmeridx;

import edu.arizona.cs.mrpkm.types.KmerRecord;
import edu.arizona.cs.mrpkm.types.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.MultiFileCompressedSequenceWritable;
import edu.arizona.cs.mrpkm.readididx.ReadIDIndexReader;
import edu.arizona.cs.mrpkm.readididx.ReadIDNotFoundException;
import edu.arizona.cs.mrpkm.fastareader.types.FastaRead;
import edu.arizona.cs.mrpkm.readididx.ReadIDIndexHelper;
import java.io.IOException;
import java.util.Hashtable;
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
    
    private int kmerSize;
    private Hashtable<String, Integer> namedOutputIDCache;
    private int previousReadID = -1;
    
    private ReadIDIndexReader readIDIndexReader;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.kmerSize = conf.getInt(KmerIndexHelper.getConfigurationKeyOfKmerSize(), -1);
        if(this.kmerSize <= 0) {
            throw new IOException("kmer size has to be a positive value");
        }
        
        this.namedOutputIDCache = new Hashtable<String, Integer>();
        
        Path inputFilePath = ((FileSplit) context.getInputSplit()).getPath();
        String[] readIDIndexPaths = conf.getStrings(KmerIndexHelper.getConfigurationKeyOfReadIDIndexPath(), "");
        boolean found = false;
        for(String readIDIndexPath : readIDIndexPaths) {
            // search index file
            Path indexPath = new Path(readIDIndexPath, ReadIDIndexHelper.getReadIDIndexFileName(inputFilePath.getName()));
            FileSystem fs = indexPath.getFileSystem(conf);
            if(fs.exists(indexPath)) {
                this.readIDIndexReader = new ReadIDIndexReader(fs, indexPath.toString(), conf);
                found = true;
                break;
            }
        }
        
        if(!found) {
            throw new IOException("ReadIDIndex is not found in given index paths");
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
        Integer namedoutputID = this.namedOutputIDCache.get(value.getFileName());
        if (namedoutputID == null) {
            namedoutputID = context.getConfiguration().getInt(KmerIndexHelper.getConfigurationKeyOfNamedOutputID(value.getFileName()), -1);
            if (namedoutputID < 0) {
                throw new IOException("No named output found : " + KmerIndexHelper.getConfigurationKeyOfNamedOutputID(value.getFileName()));
            }
            this.namedOutputIDCache.put(value.getFileName(), namedoutputID);
        }

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
        this.namedOutputIDCache.clear();
        this.namedOutputIDCache = null;
        
        this.readIDIndexReader.close();
        this.readIDIndexReader = null;
    }
}
