package edu.arizona.cs.mrpkm.kmeridx;

import edu.arizona.cs.mrpkm.types.hadoop.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.hadoop.CompressedSequenceWritable;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @author iychoi
 */
public class UnfilteredKmerIndexReader extends AKmerIndexReader {

    private static final Log LOG = LogFactory.getLog(UnfilteredKmerIndexReader.class);
    
    private AKmerIndexReader kmerIndexReader;
    
    public UnfilteredKmerIndexReader(FileSystem fs, String[] indexPaths, String kmerIndexChunkInfoPath, TaskAttemptContext context, Configuration conf) throws IOException {
        initialize(fs, indexPaths, kmerIndexChunkInfoPath, null, null, context, conf);
    }
    
    public UnfilteredKmerIndexReader(FileSystem fs, String[] indexPaths, String kmerIndexChunkInfoPath, CompressedSequenceWritable beginKey, CompressedSequenceWritable endKey, TaskAttemptContext context, Configuration conf) throws IOException {
        initialize(fs, indexPaths, kmerIndexChunkInfoPath, beginKey, endKey, context, conf);
    }
    
    public UnfilteredKmerIndexReader(FileSystem fs, String[] indexPaths, String kmerIndexChunkInfoPath, String beginKey, String endKey, TaskAttemptContext context, Configuration conf) throws IOException {
        initialize(fs, indexPaths, kmerIndexChunkInfoPath, new CompressedSequenceWritable(beginKey), new CompressedSequenceWritable(endKey), context, conf);
    }
    
    private void initialize(FileSystem fs, String[] indexPaths, String kmerIndexChunkInfoPath, CompressedSequenceWritable beginKey, CompressedSequenceWritable endKey, TaskAttemptContext context, Configuration conf) throws IOException {
        if(indexPaths.length == 1) {
            this.kmerIndexReader = new SingleKmerIndexReader(fs, indexPaths[0], beginKey, endKey, conf);    
        } else {
            this.kmerIndexReader = new MultiKmerIndexReader(fs, indexPaths, kmerIndexChunkInfoPath, beginKey, endKey, context, conf);
        }
    }
    
    @Override
    public String[] getIndexPaths() {
        return this.kmerIndexReader.getIndexPaths();
    }

    @Override
    public boolean next(CompressedSequenceWritable key, CompressedIntArrayWritable val) throws IOException {
        CompressedSequenceWritable tempKey = new CompressedSequenceWritable();
        CompressedIntArrayWritable tempVal = new CompressedIntArrayWritable();
        
        if(this.kmerIndexReader.next(tempKey, tempVal)) {
            key.set(tempKey);
            val.set(tempVal);
            return true;
        }
        return false;
    }
    
    @Override
    public void close() throws IOException {
        this.kmerIndexReader.close();
    }
}
