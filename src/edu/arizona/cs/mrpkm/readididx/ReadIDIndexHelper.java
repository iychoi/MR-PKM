package edu.arizona.cs.mrpkm.readididx;

import edu.arizona.cs.mrpkm.types.ReadIDIndexPathFilter;
import static edu.arizona.cs.mrpkm.utils.FileSystemHelper.makePathFromString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class ReadIDIndexHelper {
    private final static String OUTPUT_NAME_SUFFIX = "ridx";
    
    private final static String INDEXPATH_EXP = ".+\\." + OUTPUT_NAME_SUFFIX + "$";
    private final static Pattern INDEXPATH_PATTERN = Pattern.compile(INDEXPATH_EXP);
    
    public static String makeReadIDIndexFileName(String filename) {
        return filename + "." + OUTPUT_NAME_SUFFIX;
    }
    
    public static boolean isReadIDIndexFile(Path path) {
        return isReadIDIndexFile(path.getName());
    }
    
    public static boolean isReadIDIndexFile(String path) {
        Matcher matcher = INDEXPATH_PATTERN.matcher(path.toLowerCase());
        if(matcher.matches()) {
            return true;
        }
        return false;
    }
    
    public static Path[] getAllReadIDIndexFilePaths(Configuration conf, String[] inputPaths) throws IOException {
        return getAllReadIDIndexFilePaths(conf, makePathFromString(conf, inputPaths));
    }
    
    public static Path[] getAllReadIDIndexFilePaths(Configuration conf, Path[] inputPaths) throws IOException {
        List<Path> inputFiles = new ArrayList<Path>();
        ReadIDIndexPathFilter filter = new ReadIDIndexPathFilter();
        
        for(Path path : inputPaths) {
            FileSystem fs = path.getFileSystem(conf);
            if(fs.exists(path)) {
                FileStatus status = fs.getFileStatus(path);
                if(status.isDir()) {
                    if(filter.accept(path)) {
                        inputFiles.add(path);
                    } else {
                        // check child
                        FileStatus[] entries = fs.listStatus(path);
                        for (FileStatus entry : entries) {
                            if(entry.isDir()) {
                                if (filter.accept(entry.getPath())) {
                                    inputFiles.add(entry.getPath());
                                }
                            }
                        }
                    }
                }
            }
        }
        
        Path[] files = inputFiles.toArray(new Path[0]);
        return files;
    }
}
