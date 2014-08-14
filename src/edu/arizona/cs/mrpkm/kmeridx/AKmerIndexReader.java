package edu.arizona.cs.mrpkm.kmeridx;

import edu.arizona.cs.mrpkm.types.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.CompressedSequenceWritable;
import java.io.Closeable;
import java.io.IOException;

/**
 *
 * @author iychoi
 */
public abstract class AKmerIndexReader implements Closeable {
    public abstract void reset() throws IOException;
    @Override
    public abstract void close() throws IOException;
    public abstract String[] getIndexPaths();
    public abstract void seek(String sequence) throws IOException;
    public abstract void seek(CompressedSequenceWritable key) throws IOException;
    public abstract boolean next(CompressedSequenceWritable key, CompressedIntArrayWritable val) throws IOException;
}
