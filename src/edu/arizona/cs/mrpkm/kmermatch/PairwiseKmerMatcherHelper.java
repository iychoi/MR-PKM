package edu.arizona.cs.mrpkm.kmermatch;

import edu.arizona.cs.mrpkm.types.filters.KmerIndexPathFilter;
import edu.arizona.cs.mrpkm.helpers.FileSystemHelper;
import static edu.arizona.cs.mrpkm.helpers.FileSystemHelper.makePathFromString;
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
public class PairwiseKmerMatcherHelper {
    private final static String TOC_OUTPUT_NAME = "toc.json";
    private final static String OUTPUT_NAME_PREFIX = "result";
    private final static String OUTPUT_NAME_SUFFIX = "pkm";
    
    private final static String OUTPUTPATH_EXP = ".+\\." + OUTPUT_NAME_SUFFIX + "\\.\\d+$";
    private final static Pattern OUTPUTPATH_PATTERN = Pattern.compile(OUTPUTPATH_EXP);
    
    public static boolean isPairwiseKmerMatchTOCFile(Path path) {
        return isPairwiseKmerMatchTOCFile(path.getName());
    }
    
    public static boolean isPairwiseKmerMatchTOCFile(String path) {
        if(path.compareToIgnoreCase(TOC_OUTPUT_NAME) == 0) {
            return true;
        }
        return false;
    }
    
    public static boolean isPairwiseKmerMatchFile(Path path) {
        return isPairwiseKmerMatchFile(path.getName());
    }
    
    public static boolean isPairwiseKmerMatchFile(String path) {
        Matcher matcher = OUTPUTPATH_PATTERN.matcher(path.toLowerCase());
        if(matcher.matches()) {
            return true;
        }
        return false;
    }
    
    public static String makePairwiseKmerMatchTOCFileName() {
        return TOC_OUTPUT_NAME;
    }
    
    public static String makePairwiseKmerMatchFileName(int mapreduceID) {
        return OUTPUT_NAME_PREFIX + "." + OUTPUT_NAME_SUFFIX + "." + mapreduceID;
    }
    
    public static int getMatchPartID(Path matchFilePath) {
        return getMatchPartID(matchFilePath.getName());
    }
    
    public static int getMatchPartID(String matchFileName) {
        int idx = matchFileName.lastIndexOf(".");
        if(idx >= 0) {
            String partID = matchFileName.substring(idx + 1);
            return Integer.parseInt(partID);
        }
        return -1;
    }
    
    public static Path[] getAllPairwiseKmerMatchFilePaths(Configuration conf, String inputPathsCommaSeparated) throws IOException {
        return getAllPairwiseKmerMatchFilePaths(conf, makePathFromString(conf, FileSystemHelper.splitCommaSeparated(inputPathsCommaSeparated)));
    }
    
    public static Path[] getAllPairwiseKmerMatchFilePaths(Configuration conf, String[] inputPaths) throws IOException {
        return getAllPairwiseKmerMatchFilePaths(conf, makePathFromString(conf, inputPaths));
    }
    
    public static Path[] getAllPairwiseKmerMatchFilePaths(Configuration conf, Path[] inputPaths) throws IOException {
        List<Path> inputFiles = new ArrayList<Path>();
        KmerIndexPathFilter filter = new KmerIndexPathFilter();
        
        for(Path path : inputPaths) {
            FileSystem fs = path.getFileSystem(conf);
            if(fs.exists(path)) {
                FileStatus status = fs.getFileStatus(path);
                if(status.isDir()) {
                    // check child
                    FileStatus[] entries = fs.listStatus(path);
                    for (FileStatus entry : entries) {
                        if(entry.isDir()) {
                            if (filter.accept(entry.getPath())) {
                                inputFiles.add(entry.getPath());
                            }
                        }
                    }
                } else {
                    if (filter.accept(status.getPath())) {
                        inputFiles.add(status.getPath());
                    }
                }
            }
        }
        
        Path[] files = inputFiles.toArray(new Path[0]);
        return files;
    }
}
