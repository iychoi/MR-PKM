package edu.arizona.cs.mrpkm.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

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
    
    public static String getSafeNamedOutput(String input) {
        StringBuffer sb = new StringBuffer();
        
        for (char ch : input.toCharArray()) {
            boolean isSafe = false;
            if ((ch >= 'A') && (ch <= 'Z')) {
                isSafe = true;
            } else if ((ch >= 'a') && (ch <= 'z')) {
                isSafe = true;
            } else if ((ch >= '0') && (ch <= '9')) {
                isSafe = true;
            }
            
            if(isSafe) {
                sb.append(ch);
            }
        }
        
        return sb.toString();
    }
    
    public static Path[] getAllInputPaths(Configuration conf, String[] inputPaths, PathFilter filter) throws IOException {
        List<Path> inputFiles = new ArrayList<Path>();
        
        for(String path : inputPaths) {
            Path inputFile = new Path(path);
            FileSystem fs = inputFile.getFileSystem(conf);
            FileStatus status = fs.getFileStatus(inputFile);
            if (status.isDir()) {
                FileStatus[] entries = fs.listStatus(inputFile);
                for (FileStatus entry : entries) {
                    if (filter.accept(entry.getPath())) {
                        inputFiles.add(entry.getPath());
                    }
                }
            } else {
                if (filter.accept(inputFile)) {
                    inputFiles.add(inputFile);
                }
            }
        }
        
        Path[] files = inputFiles.toArray(new Path[0]);
        return files;
    }
    
    public static Path[] getAllInputPaths(Configuration conf, Path[] inputPaths, PathFilter filter) throws IOException {
        List<Path> inputFiles = new ArrayList<Path>();
        
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
    
    public static Path[] getNamedOutputPaths(Path outputPath, Configuration conf, String[] namedOutputs) throws IOException {
        FileSystem fs = outputPath.getFileSystem(conf);
        FileStatus status = fs.getFileStatus(outputPath);
        
        List<Path> outputFiles = new ArrayList<Path>();
        if (status.isDir()) {
            FileStatus[] entries = fs.listStatus(outputPath);
            for (FileStatus entry : entries) {
                for (String namedOutput : namedOutputs) {
                    if (entry.getPath().getName().startsWith(namedOutput + "-r-")) {
                        outputFiles.add(entry.getPath());
                    }
                }
            }
        } else {
            throw new IOException("path not found : " + outputPath.toString());
        }
        
        Path[] files = outputFiles.toArray(new Path[0]);
        return files;
    }
    
    public static Path[] getLogOutputPaths(Path outputPath, Configuration conf) throws IOException {
        FileSystem fs = outputPath.getFileSystem(conf);
        FileStatus status = fs.getFileStatus(outputPath);
        
        List<Path> outputFiles = new ArrayList<Path>();
        if (status.isDir()) {
            FileStatus[] entries = fs.listStatus(outputPath);
            for (FileStatus entry : entries) {
                if(entry.getPath().getName().equals("_SUCCESS")) {
                    outputFiles.add(entry.getPath());
                } else if(entry.getPath().getName().startsWith("part-r-")) {
                    outputFiles.add(entry.getPath());
                }
            }
        } else {
            throw new IOException("path not found : " + outputPath.toString());
        }
        
        Path[] files = outputFiles.toArray(new Path[0]);
        return files;
    }
    
    public static String getNamedOutputFromMROutputName(String mrOutputName) {
        int index = mrOutputName.indexOf("-r-");
        if(index > 0) {
            return mrOutputName.substring(0, index);
        }
        return mrOutputName;
    }
}
