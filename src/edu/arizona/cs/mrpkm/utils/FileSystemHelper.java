package edu.arizona.cs.mrpkm.utils;

import edu.arizona.cs.mrpkm.types.FastaPathFilter;
import edu.arizona.cs.mrpkm.types.KmerIndexPathFilter;
import edu.arizona.cs.mrpkm.types.ReadIDIndexPathFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class FileSystemHelper {
    public static String makeCommaSeparated(Path[] strs) {
        if(strs == null) {
            return null;
        }
        
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<strs.length;i++) {
            sb.append(strs[i].toString());
            if(i < strs.length - 1) {
                sb.append(",");
            }
        }
        return sb.toString();
    }
    
    public static String makeCommaSeparated(String[] strs) {
        if(strs == null) {
            return null;
        }
        
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<strs.length;i++) {
            sb.append(strs[i]);
            if(i < strs.length - 1) {
                sb.append(",");
            }
        }
        return sb.toString();
    }
    
    public static String[] splitCommaSeparated(String comma_separated_input) {
        String[] inputs = comma_separated_input.split(",");
        return inputs;
    }
    
    public static Path[] makePathFromString(String[] pathStrings) {
        Path[] paths = new Path[pathStrings.length];
        for(int i=0;i<pathStrings.length;i++) {
            paths[i] = new Path(pathStrings[i]);
        }
        return paths;
    }
    
    public static String[] makeStringFromPath(Path[] paths) {
        String[] pathStrings = new String[paths.length];
        for(int i=0;i<paths.length;i++) {
            pathStrings[i] = paths[i].toString();
        }
        return pathStrings;
    }
    
    public static Path[] getAllFastaFilePaths(Configuration conf, String[] inputPaths) throws IOException {
        return getAllFastaFilePaths(conf, makePathFromString(inputPaths));
    }
    
    public static Path[] getAllFastaFilePaths(Configuration conf, Path[] inputPaths) throws IOException {
        List<Path> inputFiles = new ArrayList<Path>();
        FastaPathFilter filter = new FastaPathFilter();
        
        for(Path path : inputPaths) {
            FileSystem fs = path.getFileSystem(conf);
            FileStatus status = fs.getFileStatus(path);
            if(status.isDir()) {
                FileStatus[] entries = fs.listStatus(path);
                for(FileStatus entry : entries) {
                    if(filter.accept(entry.getPath())) {
                        inputFiles.add(entry.getPath());
                    }
                }
            } else {
                if(filter.accept(path)) {
                    inputFiles.add(path);
                }
            }
        }
        
        Path[] files = inputFiles.toArray(new Path[0]);
        return files;
    }
    
    public static Path[] getAllReadIDIndexFilePaths(Configuration conf, String[] inputPaths) throws IOException {
        return getAllReadIDIndexFilePaths(conf, makePathFromString(inputPaths));
    }
    
    public static Path[] getAllReadIDIndexFilePaths(Configuration conf, Path[] inputPaths) throws IOException {
        List<Path> inputFiles = new ArrayList<Path>();
        ReadIDIndexPathFilter filter = new ReadIDIndexPathFilter();
        
        for(Path path : inputPaths) {
            FileSystem fs = path.getFileSystem(conf);
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
        
        Path[] files = inputFiles.toArray(new Path[0]);
        return files;
    }
    
    public static Path[] getAllKmerIndexFilePaths(Configuration conf, String[] inputPaths) throws IOException {
        return getAllKmerIndexFilePaths(conf, makePathFromString(inputPaths));
    }
    
    public static Path[] getAllKmerIndexFilePaths(Configuration conf, Path[] inputPaths) throws IOException {
        List<Path> inputFiles = new ArrayList<Path>();
        KmerIndexPathFilter filter = new KmerIndexPathFilter();
        
        for(Path path : inputPaths) {
            FileSystem fs = path.getFileSystem(conf);
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
        
        Path[] files = inputFiles.toArray(new Path[0]);
        return files;
    }
}
