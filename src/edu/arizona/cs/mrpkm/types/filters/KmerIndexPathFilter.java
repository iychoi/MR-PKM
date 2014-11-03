package edu.arizona.cs.mrpkm.types.filters;

import edu.arizona.cs.mrpkm.kmeridx.KmerIndexHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 *
 * @author iychoi
 */
public class KmerIndexPathFilter implements PathFilter {

    @Override
    public boolean accept(Path path) {
        return KmerIndexHelper.isKmerIndexFile(path);
    }
}
