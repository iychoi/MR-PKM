package edu.arizona.cs.mrpkm.types.filters;

import edu.arizona.cs.mrpkm.readididx.ReadIDIndexHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 *
 * @author iychoi
 */
public class ReadIDIndexPathFilter implements PathFilter {

    @Override
    public boolean accept(Path path) {
        return ReadIDIndexHelper.isReadIDIndexFile(path);
    }
}
