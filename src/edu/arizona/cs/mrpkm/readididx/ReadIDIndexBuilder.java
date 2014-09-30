package edu.arizona.cs.mrpkm.readididx;

import edu.arizona.cs.hadoop.fs.irods.HirodsFileSystem;
import edu.arizona.cs.hadoop.fs.irods.output.HirodsFileOutputFormat;
import edu.arizona.cs.hadoop.fs.irods.output.HirodsMapFileOutputFormat;
import edu.arizona.cs.hadoop.fs.irods.output.HirodsMultipleOutputs;
import edu.arizona.cs.mrpkm.cluster.AMRClusterConfiguration;
import edu.arizona.cs.mrpkm.cluster.MRClusterConfiguration_default;
import edu.arizona.cs.mrpkm.types.MultiFileOffsetWritable;
import edu.arizona.cs.mrpkm.fastareader.FastaReadDescriptionInputFormat;
import edu.arizona.cs.mrpkm.types.NamedOutput;
import edu.arizona.cs.mrpkm.types.NamedOutputs;
import edu.arizona.cs.mrpkm.utils.FileSystemHelper;
import edu.arizona.cs.mrpkm.utils.MapReduceHelper;
import edu.arizona.cs.mrpkm.utils.MultipleOutputsHelper;
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
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
 *
 * @author iychoi
 */
public class ReadIDIndexBuilder extends Configured implements Tool {
    
    private static final Log LOG = LogFactory.getLog(ReadIDIndexBuilder.class);
    
    private static class ReadIDIndexBuilder_Cmd_Args {
        @Option(name = "-h", aliases = "--help", usage = "print this message") 
        private boolean help = false;
        
        private AMRClusterConfiguration cluster = new MRClusterConfiguration_default();
        
        @Option(name = "-c", aliases = "--cluster", usage = "specify cluster configuration")
        public void setCluster(String clusterConf) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
            this.cluster = AMRClusterConfiguration.findConfiguration(clusterConf);
        }
        
        @Argument(metaVar = "input-path [input-path ...] output-path", usage = "input-paths and output-path")
        private List<String> paths = new ArrayList<String>();
        
        public boolean isHelp() {
            return this.help;
        }
        
        public AMRClusterConfiguration getConfiguration() {
            return this.cluster;
        }
        
        public String getOutputPath() {
            if(this.paths.size() > 1) {
                return this.paths.get(this.paths.size()-1);
            }
            
            return null;
        }
        
        public String[] getInputPaths() {
            if(this.paths.isEmpty()) {
                return new String[0];
            }
            
            String[] inpaths = new String[this.paths.size()-1];
            for(int i=0;i<this.paths.size()-1;i++) {
                inpaths[i] = this.paths.get(i);
            }
            
            return inpaths;
        }
        
        public String getCommaSeparatedInputPath() {
            String[] inputPaths = getInputPaths();
            StringBuilder CSInputPath = new StringBuilder();
            for(String inputpath : inputPaths) {
                if(CSInputPath.length() != 0) {
                    CSInputPath.append(",");
                }
                
                CSInputPath.append(inputpath);
            }
            
            return CSInputPath.toString();
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for(String arg : this.paths) {
                if(sb.length() != 0) {
                    sb.append(", ");
                }
                
                sb.append(arg);
            }
            
            return "help = " + this.help + "\n" +
                    "paths = " + sb.toString();
        }
        
        public boolean checkValidity() {
            if(this.cluster == null || 
                    this.paths == null || this.paths.isEmpty() ||
                    this.paths.size() < 2) {
                return false;
            }
            return true;
        }
    }
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ReadIDIndexBuilder(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        // parse command line
        ReadIDIndexBuilder_Cmd_Args cmdargs = new ReadIDIndexBuilder_Cmd_Args();
        CmdLineParser parser = new CmdLineParser(cmdargs);
        CmdLineException parseException = null;
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            // handling of wrong arguments
            parseException = e;
        }
        
        if(cmdargs.isHelp() || !cmdargs.checkValidity()) {
            parser.printUsage(System.err);
            return 1;
        }
        
        if(parseException != null) {
            System.err.println(parseException.getMessage());
            parser.printUsage(System.err);
            return 1;
        }
        
        String inputPath = cmdargs.getCommaSeparatedInputPath();
        String outputPath = cmdargs.getOutputPath();
        AMRClusterConfiguration clusterConfig = cmdargs.getConfiguration();
        
        // configuration
        Configuration conf = this.getConf();
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
        
        Path outputHadoopPath = new Path(outputPath);
        FileSystem outputFileSystem = outputHadoopPath.getFileSystem(conf);
        if(outputFileSystem instanceof HirodsFileSystem) {
            LOG.info("Use H-iRODS");
            HirodsFileOutputFormat.setOutputPath(job, outputHadoopPath);
            job.setOutputFormatClass(HirodsMapFileOutputFormat.class);
            MultipleOutputsHelper.setMultipleOutputsClass(job.getConfiguration(), HirodsMultipleOutputs.class);
        } else {
            FileOutputFormat.setOutputPath(job, outputHadoopPath);
            job.setOutputFormatClass(MapFileOutputFormat.class);
            MultipleOutputsHelper.setMultipleOutputsClass(job.getConfiguration(), MultipleOutputs.class);
        }
        
        LOG.info("regist new ConfigString : " + ReadIDIndexHelper.getConfigurationKeyOfNamedOutputNum());
        job.getConfiguration().setInt(ReadIDIndexHelper.getConfigurationKeyOfNamedOutputNum(), namedOutputs.getSize());
        
        int id = 0;
        for(NamedOutput namedOutput : namedOutputs.getAllNamedOutput()) {
            LOG.info("regist new named output : " + namedOutput.getNamedOutputString());

            job.getConfiguration().setStrings(ReadIDIndexHelper.getConfigurationKeyOfNamedOutputName(id), namedOutput.getNamedOutputString());
            LOG.info("regist new ConfigString : " + ReadIDIndexHelper.getConfigurationKeyOfNamedOutputName(id));
            
            job.getConfiguration().setInt(ReadIDIndexHelper.getConfigurationKeyOfNamedOutputID(namedOutput.getInputString()), id);
            LOG.info("regist new ConfigString : " + ReadIDIndexHelper.getConfigurationKeyOfNamedOutputID(namedOutput.getInputString()));
            
            if(outputFileSystem instanceof HirodsFileSystem) {
                HirodsMultipleOutputs.addNamedOutput(job, namedOutput.getNamedOutputString(), HirodsMapFileOutputFormat.class, LongWritable.class, IntWritable.class);
            } else {
                MultipleOutputs.addNamedOutput(job, namedOutput.getNamedOutputString(), MapFileOutputFormat.class, LongWritable.class, IntWritable.class);
            }
            id++;
        }
        
        job.setNumReduceTasks(1);
        
        // Execute job and return status
        boolean result = job.waitForCompletion(true);
        
        // commit results
        if(result) {
            commit(outputHadoopPath, conf, namedOutputs);
        }
        
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
