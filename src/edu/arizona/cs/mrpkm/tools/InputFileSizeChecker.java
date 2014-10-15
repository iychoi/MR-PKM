package edu.arizona.cs.mrpkm.tools;

import edu.arizona.cs.mrpkm.utils.FileSystemHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author iychoi
 */
public class InputFileSizeChecker extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new InputFileSizeChecker(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        // configuration
        Configuration conf = this.getConf();
        
        String inputPath = args[0];
        
        // Inputs
        String[] paths = FileSystemHelper.splitCommaSeparated(inputPath);
        Path[] inputFiles = FileSystemHelper.getAllFastaFilePaths(conf, paths);
        
        long accuSize = 0;
        int count = 0;
        for(Path input : inputFiles) {
            FileSystem fs = input.getFileSystem(conf);
            long len = fs.getFileStatus(input).getLen();
            accuSize += len;
            
            System.out.println("> " + input.toString() + " : " + len);
            count++;
        }
        
        System.out.println("Sum " + count + " files : " + accuSize);
        return 0;
    }
}
