package edu.arizona.cs.mrpkm.kmermatch;

import edu.arizona.cs.mrpkm.kmerrange.KmerRangeSlice;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 *
 * @author iychoi
 */
public class KmerIndexSplit extends InputSplit implements Writable {

    private Path[] indexPaths;
    private KmerRangeSlice slice;

    public KmerIndexSplit() {    
    }
    
    public KmerIndexSplit(Path[] indexFilePaths, KmerRangeSlice slice) {
        this.indexPaths = indexFilePaths;
        this.slice = slice;
    }
    
    public Path[] getIndexFilePaths() {
        return this.indexPaths;
    }
    
    public KmerRangeSlice getSlice() {
        return this.slice;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(Path path : this.indexPaths) {
            if(sb.length() != 0) {
                sb.append(", ");
            }
            sb.append(path.toString());
        }
        return this.slice.toString() + "\n" + sb.toString();
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[] {"localhost"};
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.indexPaths.length);
        for (Path indexPath : this.indexPaths) {
            Text.writeString(out, indexPath.toString());
        }
        this.slice.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.indexPaths = new Path[in.readInt()];
        for(int i=0;i<this.indexPaths.length;i++) {
            this.indexPaths[i] = new Path(Text.readString(in));
        }
        this.slice = new KmerRangeSlice();
        this.slice.read(in);
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return this.slice.getSliceSize().longValue();
    }
}
