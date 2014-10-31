package edu.arizona.cs.mrpkm.kmeridx;

import edu.arizona.cs.mrpkm.types.KmerIndexPathFilter;
import edu.arizona.cs.mrpkm.utils.FileSystemHelper;
import static edu.arizona.cs.mrpkm.utils.FileSystemHelper.makePathFromString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;

/**
 *
 * @author iychoi
 */
public class KmerIndexHelper {
    private final static String OUTPUT_NAME_SUFFIX = "kidx";
    
    private final static String INDEXPATH_EXP = ".+\\." + OUTPUT_NAME_SUFFIX + "\\.\\d+$";
    private final static Pattern INDEXPATH_PATTERN = Pattern.compile(INDEXPATH_EXP);
    
    public static boolean isKmerIndexFile(Path path) {
        return isKmerIndexFile(path.getName());
    }
    
    public static boolean isKmerIndexFile(String path) {
        Matcher matcher = INDEXPATH_PATTERN.matcher(path.toLowerCase());
        if(matcher.matches()) {
            return true;
        }
        return false;
    }
    
    public static String makeKmerIndexFileName(Path inputFilePath, int kmerSize, int mapreduceID) {
        return makeKmerIndexFileName(inputFilePath.getName(), kmerSize, mapreduceID);
    }
    
    public static String makeKmerIndexFileName(String inputFileName, int kmerSize, int mapreduceID) {
        return inputFileName + "." + kmerSize + "." + OUTPUT_NAME_SUFFIX + "." + mapreduceID;
    }
    
    public static String getFastaFileName(Path indexFilePath) {
        return getFastaFileName(indexFilePath.getName());
    }
    
    public static String getFastaFileName(String indexFileName) {
        int idx = indexFileName.lastIndexOf("." + OUTPUT_NAME_SUFFIX);
        if(idx >= 0) {
            String part = indexFileName.substring(0, idx);
            int idx2 = part.lastIndexOf(".");
            if(idx2 >= 0) {
                String fastaFilePath = part.substring(0, idx2);
                int idx3 = fastaFilePath.lastIndexOf("/");
                if(idx3 >= 0) {
                    return fastaFilePath.substring(idx3 + 1);
                } else {
                    return fastaFilePath;
                }
            }
        }
        return null;
    }
    
    public static boolean isSameKmerIndex(Path index1, Path index2) {
        return isSameKmerIndex(index1.getName(), index2.getName());
    }
    
    public static boolean isSameKmerIndex(String index1, String index2) {
        int idx1 = index1.lastIndexOf(".");
        int idx2 = index2.lastIndexOf(".");
        
        if(idx1 >= 0 && idx2 >= 0) {
            String partIdx1 = index1.substring(0, idx1);
            String partIdx2 = index2.substring(0, idx2);

            return partIdx1.equals(partIdx2);
        }
        
        return false;
    }
    
    public static int getKmerSize(Path indexFilePath) {
        return getKmerSize(indexFilePath.getName());
    }
    
    public static int getKmerSize(String indexFileName) {
        int idx = indexFileName.lastIndexOf("." + OUTPUT_NAME_SUFFIX);
        if(idx >= 0) {
            String part = indexFileName.substring(0, idx);
            int idx2 = part.lastIndexOf(".");
            if(idx2 >= 0) {
                return Integer.parseInt(part.substring(idx2 + 1));
            }
        }
        return -1;
    }
    
    public static int getIndexPartID(Path indexFilePath) {
        return getIndexPartID(indexFilePath.getName());
    }
    
    public static int getIndexPartID(String indexFileName) {
        int idx = indexFileName.lastIndexOf(".");
        if(idx >= 0) {
            String partID = indexFileName.substring(idx + 1);
            return Integer.parseInt(partID);
        }
        return -1;
    }
    
    public static Path[] getAllKmerIndexFilePaths(Configuration conf, String inputPathsCommaSeparated) throws IOException {
        return getAllKmerIndexFilePaths(conf, makePathFromString(conf, FileSystemHelper.splitCommaSeparated(inputPathsCommaSeparated)));
    }
    
    public static Path[] getAllKmerIndexFilePaths(Configuration conf, String[] inputPaths) throws IOException {
        return getAllKmerIndexFilePaths(conf, makePathFromString(conf, inputPaths));
    }
    
    public static Path[] getAllKmerIndexFilePaths(Configuration conf, Path[] inputPaths) throws IOException {
        List<Path> inputFiles = new ArrayList<Path>();
        KmerIndexPathFilter filter = new KmerIndexPathFilter();
        
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
    
    public static Path[] getAllKmerIndexDataFilePaths(Configuration conf, String[] inputPaths) throws IOException {
        return getAllKmerIndexDataFilePaths(conf, makePathFromString(conf, inputPaths));
    }
    
    public static Path[] getAllKmerIndexDataFilePaths(Configuration conf, Path[] inputPaths) throws IOException {
        List<Path> inputFiles = new ArrayList<Path>();
        KmerIndexPathFilter filter = new KmerIndexPathFilter();
        
        for(Path path : inputPaths) {
            FileSystem fs = path.getFileSystem(conf);
            if(fs.exists(path)) {
                FileStatus status = fs.getFileStatus(path);
                if(status.isDir()) {
                    if(filter.accept(path)) {
                        inputFiles.add(new Path(path, MapFile.DATA_FILE_NAME));
                    } else {
                        // check child
                        FileStatus[] entries = fs.listStatus(path);
                        for (FileStatus entry : entries) {
                            if(entry.isDir()) {
                                if (filter.accept(entry.getPath())) {
                                    inputFiles.add(new Path(entry.getPath(), MapFile.DATA_FILE_NAME));
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
    
    public static Path[][] groupKmerIndice(Path[] inputIndexPaths) {
        List<Path[]> groups = new ArrayList<Path[]>();
        
        List<Path> sortedInputIndexPaths = sortPaths(inputIndexPaths);
        List<Path> group = new ArrayList<Path>();
        for(Path path: sortedInputIndexPaths) {
            if(group.isEmpty()) {
                group.add(path);
            } else {
                Path prev = group.get(0);
                if(isSameKmerIndex(prev, path)) {
                    group.add(path);
                } else {
                    groups.add(group.toArray(new Path[0]));
                    group.clear();
                    group.add(path);
                }
            }
        }
        
        if(!group.isEmpty()) {
            groups.add(group.toArray(new Path[0]));
            group.clear();
        }
        
        return groups.toArray(new Path[0][0]);
    }
    
    public static List<Path> sortPaths(Path[] paths) {
        List<Path> pathList = new ArrayList<Path>();
        pathList.addAll(Arrays.asList(paths));
        
        Collections.sort(pathList, new Comparator<Path>() {

            @Override
            public int compare(Path t, Path t1) {
                String ts = t.getName();
                String t1s = t1.getName();

                return ts.compareTo(t1s);
            }
        });
        return pathList;
    }
}
