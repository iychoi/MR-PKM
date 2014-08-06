package edu.arizona.cs.mrpkm.readididx;

import edu.arizona.cs.mrpkm.cluster.MRClusterConfiguration;
import edu.arizona.cs.mrpkm.readididx.types.MultiFileOffsetWritable;
import edu.arizona.cs.mrpkm.recordreader.FastaReadDescriptionInputFormat;
import edu.arizona.cs.mrpkm.utils.FastaPathFilter;
import edu.arizona.cs.mrpkm.utils.FileSystemHelper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
        }
        
        Configuration conf = this.getConf();
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
        Path[] inputFiles = FileSystemHelper.getAllInputPaths(conf, paths, new FastaPathFilter());
        String[] namedOutputs = ReadIDIndexHelper.generateNamedOutputStrings(inputFiles);
        
        FileInputFormat.addInputPaths(job, FileSystemHelper.makeCommaSeparated(inputFiles));
        FileInputFormat.setInputPathFilter(job, FastaPathFilter.class);
        
        LOG.info("Input files : " + inputFiles.length);
        for(Path inputFile : inputFiles) {
            LOG.info("> " + inputFile.toString());
        }
        
        job.setInputFormatClass(FastaReadDescriptionInputFormat.class);
        
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        LOG.info("regist new ConfigString : " + ReadIDIndexConstants.CONF_NAMED_OUTPUTS_NUM);
        job.getConfiguration().setInt(ReadIDIndexConstants.CONF_NAMED_OUTPUTS_NUM, namedOutputs.length);
        
        int id = 0;
        for(String namedOutput : namedOutputs) {
            LOG.info("regist new named output : " + namedOutput);
            
            job.getConfiguration().setStrings(ReadIDIndexConstants.CONF_NAMED_OUTPUT_ID_PREFIX + id, namedOutput);
            LOG.info("regist new ConfigString : " + ReadIDIndexConstants.CONF_NAMED_OUTPUT_ID_PREFIX + id);
            
            job.getConfiguration().setInt(ReadIDIndexConstants.CONF_NAMED_OUTPUT_NAME_PREFIX + namedOutput, id);
            LOG.info("regist new ConfigString : " + ReadIDIndexConstants.CONF_NAMED_OUTPUT_NAME_PREFIX + namedOutput);
            
            MultipleOutputs.addNamedOutput(job, namedOutput, MapFileOutputFormat.class, LongWritable.class, IntWritable.class);
            id++;
        }
        
        job.setNumReduceTasks(1);
        
        // Execute job and return status
        boolean result = job.waitForCompletion(true);
        
        // commit results
        commit(new Path(outputPath), conf, namedOutputs);
        
        return result ? 0 : 1;
    }

    private void commit(Path outputPath, Configuration conf, String[] namedOutputs) throws IOException {
        FileSystem fs = outputPath.getFileSystem(conf);
        
        Path[] outputFiles = FileSystemHelper.getNamedOutputPaths(outputPath, conf, namedOutputs);
        Path[] logOutputFiles = FileSystemHelper.getLogOutputPaths(outputPath, conf);
        
        // rename
        for(Path outputFile : outputFiles) {
            LOG.info("output : " + outputFile.toString());
            LOG.info("renamed to : " + FileSystemHelper.getNamedOutputFromMROutputName(outputFile.toString()));
            
            fs.rename(outputFile, new Path(FileSystemHelper.getNamedOutputFromMROutputName(outputFile.toString())));
        }
        
        // delete
        for(Path logOutputFile : logOutputFiles) {
            fs.delete(logOutputFile, true);
        }
    }
}
