package edu.arizona.cs.mrpkm.fastareader;

import edu.arizona.cs.mrpkm.fastareader.types.FastaRead;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 *
 * @author iychoi
 */
public class FastaReadDescriptionInputFormat extends FileInputFormat<LongWritable, FastaRead> {

    @Override
    public RecordReader<LongWritable, FastaRead> createRecordReader(InputSplit split,
            TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new FastaReadDescriptionReader();
    }
    
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        CompressionCodec codec =
                new CompressionCodecFactory(context.getConfiguration()).getCodec(filename);
        return codec == null;
    }
}