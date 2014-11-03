package edu.arizona.cs.mrpkm.kmermatch;

import edu.arizona.cs.mrpkm.types.kmerrangepartition.KmerRangePartition;
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
public class KmerMatchIndexSplit extends InputSplit implements Writable {

    private Path[] indexPaths;
    private KmerRangePartition partition;

    public KmerMatchIndexSplit() {    
    }
    
    public KmerMatchIndexSplit(Path[] indexFilePaths, KmerRangePartition partition) {
        this.indexPaths = indexFilePaths;
        this.partition = partition;
    }
    
    public Path[] getIndexFilePaths() {
        return this.indexPaths;
    }
    
    public KmerRangePartition getPartition() {
        return this.partition;
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
        return this.partition.toString() + "\n" + sb.toString();
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
        this.partition.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.indexPaths = new Path[in.readInt()];
        for(int i=0;i<this.indexPaths.length;i++) {
            this.indexPaths[i] = new Path(Text.readString(in));
        }
        this.partition = new KmerRangePartition();
        this.partition.read(in);
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return this.partition.getPartitionSize().longValue();
    }
}
