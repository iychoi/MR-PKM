package edu.arizona.cs.mrpkm.types.filters;

import edu.arizona.cs.mrpkm.kmermatch.PairwiseKmerMatcherHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 *
 * @author iychoi
 */
public class PairwiseKmerMatchPathFilter implements PathFilter {

    @Override
    public boolean accept(Path path) {
        return PairwiseKmerMatcherHelper.isPairwiseKmerMatchFile(path);
    }
}
