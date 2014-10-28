package edu.arizona.cs.mrpkm.kmeridx;

import edu.arizona.cs.mrpkm.types.MutableInteger;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
    private String[] locations;

    public KmerIndexSplit() {    
    }
    
    public KmerIndexSplit(Path[] indexFilePaths, Configuration conf) throws IOException {
        this.indexPaths = indexFilePaths;
        this.locations = findBestLocations(indexFilePaths, conf);
    }
    
    private String[] findBestLocations(Path[] indexFilePaths, Configuration conf) throws IOException {
        Hashtable<String, MutableInteger> blkLocations = new Hashtable<String, MutableInteger>();
        for(Path path : indexFilePaths) {
            FileSystem fs = path.getFileSystem(conf);
            FileStatus fileStatus = fs.getFileStatus(path);
            BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
            for(BlockLocation location : fileBlockLocations) {
                for(String host : location.getHosts()) {
                    MutableInteger cnt = blkLocations.get(host);
                    if(cnt == null) {
                        blkLocations.put(host, new MutableInteger(1));
                    } else {
                        cnt.increase();
                    }
                }
            }
        }
        
        List<String> blkLocationsArr = new ArrayList<String>();
        for(String key : blkLocations.keySet()) {
            blkLocationsArr.add(key);
        }
        
        if(blkLocationsArr.size() == 0) {
            return new String[] {"localhost"};
        } else {
            return blkLocationsArr.toArray(new String[0]);
        }
    }
    
    public Path[] getIndexFilePaths() {
        return this.indexPaths;
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
        return sb.toString();
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return locations;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.indexPaths.length);
        for (Path indexPath : this.indexPaths) {
            Text.writeString(out, indexPath.toString());
        }
        
        out.writeInt(this.locations.length);
        for (String host : this.locations) {
            Text.writeString(out, host);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.indexPaths = new Path[in.readInt()];
        for(int i=0;i<this.indexPaths.length;i++) {
            this.indexPaths[i] = new Path(Text.readString(in));
        }
        
        this.locations = new String[in.readInt()];
        for(int i=0;i<this.locations.length;i++) {
            this.locations[i] = Text.readString(in);
        }
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return Long.MAX_VALUE;
    }
}
