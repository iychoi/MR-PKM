package edu.arizona.cs.mrpkm.readididx;

import edu.arizona.cs.mrpkm.cluster.MRClusterConfiguration;
import edu.arizona.cs.mrpkm.types.MultiFileOffsetWritable;
import edu.arizona.cs.mrpkm.fastareader.FastaReadDescriptionInputFormat;
import edu.arizona.cs.mrpkm.types.NamedOutput;
import edu.arizona.cs.mrpkm.types.NamedOutputs;
import edu.arizona.cs.mrpkm.utils.FileSystemHelper;
import edu.arizona.cs.mrpkm.utils.MapReduceHelper;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author iychoi
 */
public class ReadIDIndexBuilder extends Configured implements Tool {
    
    private static final Log LOG = LogFactory.getLog(ReadIDIndexBuilder.class);
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ReadIDIndexBuilder(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        String clusterConfiguration = null;
        String inputPath = null;
        String outputPath = null;
        
        if(args.length == 2) {
            clusterConfiguration = "default";
            inputPath = args[0];
            outputPath = args[1];
        } else if(args.length >= 3) {
            clusterConfiguration = args[0];
            inputPath = "";
            for(int i=1;i<args.length-1;i++) {
                if(!inputPath.equals("")) {
                    inputPath += ",";
                }
                inputPath += args[i];
            }
            outputPath = args[args.length - 1];
        } else {
            throw new Exception("Argument is not properly given");
        }
        
        Configuration conf = this.getConf();
        
        // configuration
        MRClusterConfiguration clusterConfig = MRClusterConfiguration.findConfiguration(clusterConfiguration);
        clusterConfig.setConfiguration(conf);
        
        Job job = new Job(conf, "ReadID Index Builder");
        job.setJarByClass(ReadIDIndexBuilder.class);

        // Identity Mapper & Reducer
        job.setMapperClass(ReadIDIndexBuilderMapper.class);
        job.setReducerClass(ReadIDIndexBuilderReducer.class);

        job.setMapOutputKeyClass(MultiFileOffsetWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        
        // Specify key / value
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);
        
        // Inputs
        String[] paths = FileSystemHelper.splitCommaSeparated(inputPath);
        Path[] inputFiles = FileSystemHelper.getAllFastaFilePaths(conf, paths);
        
        FileInputFormat.addInputPaths(job, FileSystemHelper.makeCommaSeparated(inputFiles));
        
        NamedOutputs namedOutputs = new NamedOutputs();
        LOG.info("Input files : " + inputFiles.length);
        for(int i=0;i<inputFiles.length;i++) {
            LOG.info("> " + inputFiles[i].toString());
            namedOutputs.addNamedOutput(inputFiles[i]);
        }
        
        job.setInputFormatClass(FastaReadDescriptionInputFormat.class);
        
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        LOG.info("regist new ConfigString : " + ReadIDIndexHelper.getConfigurationKeyOfNamedOutputNum());
        job.getConfiguration().setInt(ReadIDIndexHelper.getConfigurationKeyOfNamedOutputNum(), namedOutputs.getSize());
        
        int id = 0;
        for(NamedOutput namedOutput : namedOutputs.getAllNamedOutput()) {
            LOG.info("regist new named output : " + namedOutput.getNamedOutputString());

            job.getConfiguration().setStrings(ReadIDIndexHelper.getConfigurationKeyOfNamedOutputName(id), namedOutput.getNamedOutputString());
            LOG.info("regist new ConfigString : " + ReadIDIndexHelper.getConfigurationKeyOfNamedOutputName(id));
            
            job.getConfiguration().setInt(ReadIDIndexHelper.getConfigurationKeyOfNamedOutputID(namedOutput.getInputString()), id);
            LOG.info("regist new ConfigString : " + ReadIDIndexHelper.getConfigurationKeyOfNamedOutputID(namedOutput.getInputString()));
            
            MultipleOutputs.addNamedOutput(job, namedOutput.getNamedOutputString(), MapFileOutputFormat.class, LongWritable.class, IntWritable.class);
            id++;
        }
        
        job.setNumReduceTasks(1);
        
        // Execute job and return status
        boolean result = job.waitForCompletion(true);
        
        // commit results
        commit(new Path(outputPath), conf, namedOutputs);
        
        return result ? 0 : 1;
    }

    private void commit(Path outputPath, Configuration conf, NamedOutputs namedOutputs) throws IOException {
        FileSystem fs = outputPath.getFileSystem(conf);
        
        FileStatus status = fs.getFileStatus(outputPath);
        if (status.isDir()) {
            FileStatus[] entries = fs.listStatus(outputPath);
            for (FileStatus entry : entries) {
                Path entryPath = entry.getPath();
                
                // remove unnecessary outputs
                if(MapReduceHelper.isLogFiles(entryPath)) {
                    fs.delete(entryPath, true);
                } else {
                    // rename outputs
                    NamedOutput namedOutput = namedOutputs.getNamedOutputByMROutput(entryPath.getName());
                    if(namedOutput != null) {
                        Path toPath = new Path(entryPath.getParent(), ReadIDIndexHelper.getReadIDIndexFileName(namedOutput.getInputString()));
                        
                        LOG.info("output : " + entryPath.toString());
                        LOG.info("renamed to : " + toPath.toString());
                        fs.rename(entryPath, toPath);
                    }
                }
            }
        } else {
            throw new IOException("path not found : " + outputPath.toString());
        }
    }
}
