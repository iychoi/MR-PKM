package edu.arizona.cs.mrpkm.recordreader;

import edu.arizona.cs.mrpkm.recordreader.types.FastaRead;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

/**
 *
 * @author iychoi
 */
public class FastaReadDescriptionReader extends RecordReader<LongWritable, FastaRead> {

    private static final Log LOG = LogFactory.getLog(FastaReadDescriptionReader.class);
    
    public static final char READ_DELIMITER = '>';
    
    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private int maxLineLength;
    private String filename;
    private boolean hasNextRecord;
    private LongWritable key;
    private FastaRead value;
    private Text prevLine;
    private long prevSize;
    private boolean isCompressed;
    private long uncompressedSize;

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    public FastaRead getCurrentValue() throws IOException, InterruptedException {
        return this.value;
    }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {

        FileSplit split = (FileSplit) genericSplit;
        Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
        this.start = split.getStart();
        this.end = this.start + split.getLength();
        final Path file = split.getPath();
        this.compressionCodecs = new CompressionCodecFactory(job);
        final CompressionCodec codec = this.compressionCodecs.getCodec(file);
        
        this.filename = file.getName();
        
        // open the file and seek to the start of the split
        FileSystem fs = file.getFileSystem(job);
        
        // get uncompressed length
        if (codec instanceof GzipCodec) {
            this.isCompressed = true;
            
            FSDataInputStream fileInCheckSize = fs.open(file);
            byte[] len = new byte[4];
            try {
                LOG.info("compressed input : " + file.getName());
                LOG.info("compressed file size : " + this.end);
                fileInCheckSize.skip(this.end - 4);
                IOUtils.readFully(fileInCheckSize, len, 0, len.length);
                this.uncompressedSize = (len[3] << 24) | (len[2] << 16) | (len[1] << 8) | len[0];
                if(this.uncompressedSize < 0) {
                    this.uncompressedSize = this.end;
                }
                LOG.info("uncompressed file size : " + this.uncompressedSize);
            } finally {
                fileInCheckSize.close();
            }
            
            this.end = Long.MAX_VALUE;
        } else if(codec != null) {
            this.isCompressed = true;
            this.end = Long.MAX_VALUE;
            this.uncompressedSize = Long.MAX_VALUE;
        } else {
            this.isCompressed = false;
        }
        
        // get inputstream
        FSDataInputStream fileIn = fs.open(file);
        
        if (codec != null) {
            this.in = new LineReader(codec.createInputStream(fileIn), job);
        } else {
            if (this.start != 0) {
                fileIn.seek(this.start);
            }
            this.in = new LineReader(fileIn, job);
        }
        
        // skip lines until we meet new record start
        while (this.start < this.end) {
            Text skipText = new Text();
            long newSize = this.in.readLine(skipText, this.maxLineLength,
                    Math.max((int) Math.min(Integer.MAX_VALUE, this.end - this.start),
                    this.maxLineLength));
            if (newSize == 0) {
                // EOF
                this.hasNextRecord = false;
                this.pos = this.end;
                break;
            }
            
            if (skipText.getLength() > 0 && skipText.charAt(0) == READ_DELIMITER) {
                this.prevLine = skipText;
                this.prevSize = newSize;
                this.hasNextRecord = true;
                this.pos = this.start;
                break;
            }
            
            this.start += newSize;
            
            if(this.start >= this.end) {
                // EOF
                this.hasNextRecord = false;
                this.pos = this.end;
                break;
            }
        }
        
        this.key = null;
        this.value = null;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // seek to new record start
        if(this.hasNextRecord) {
            this.key = new LongWritable(this.pos);
            this.value = new FastaRead(this.filename);
            
            Text description = this.prevLine;
            this.pos += this.prevSize;
            
            long readStartOffset = this.key.get();
            long descriptionStartOffset = readStartOffset + 1;
            
            long sequenceStartOffset = this.pos;
            long descriptionLen = sequenceStartOffset - descriptionStartOffset;
            
            boolean foundNextRead = false;
            while(!foundNextRead) {
                Text newLine = new Text();
                long newSize = this.in.readLine(newLine, this.maxLineLength,
                        Math.max((int) Math.min(Integer.MAX_VALUE, this.end - this.pos),
                        this.maxLineLength));
                if (newSize == 0) {
                    // EOF
                    this.prevLine = null;
                    this.prevSize = 0;
                    this.pos = this.end;
                    break;
                }

                if (newLine.getLength() > 0 && newLine.charAt(0) == READ_DELIMITER) {
                    this.prevLine = newLine;
                    this.prevSize = newSize;
                    
                    if(this.pos + newSize < this.end) {
                        foundNextRead = true;
                    } else {
                        foundNextRead = false;
                    }
                    break;
                } else {
                    // skip
                }

                this.pos += newSize;
            }
            
            this.value.setReadOffset(readStartOffset);
            this.value.setDescription(description.toString());
            this.value.setSequence(null);
            
            this.hasNextRecord = foundNextRead;
            return true;
        } else {
            this.pos = this.end;
            this.prevLine = null;
            this.prevSize = 0;
            this.key = null;
            this.value = null;
            this.hasNextRecord = false;
            return false;
        }
    }

    @Override
    public float getProgress() throws IOException {
        if(this.isCompressed) {
            if (this.start == this.uncompressedSize) {
                return 0.0f;
            } else {
                return Math.min(1.0f, (this.pos - this.start) / (float) (this.uncompressedSize - this.start));
            }
        } else {
            if (this.start == this.end) {
                return 0.0f;
            } else {
                return Math.min(1.0f, (this.pos - this.start) / (float) (this.end - this.start));
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (this.in != null) {
            this.in.close();
        }
    }
}
