package edu.arizona.cs.mrpkm.kmeridx;

import edu.arizona.cs.hadoop.fs.irods.HirodsFileSystem;
import edu.arizona.cs.hadoop.fs.irods.output.HirodsFileOutputFormat;
import edu.arizona.cs.hadoop.fs.irods.output.HirodsMapFileOutputFormat;
import edu.arizona.cs.hadoop.fs.irods.output.HirodsMultipleOutputs;
import edu.arizona.cs.mrpkm.augment.BloomMapFileOutputFormat;
import edu.arizona.cs.mrpkm.augment.HirodsBloomMapFileOutputFormat;
import edu.arizona.cs.mrpkm.cluster.AMRClusterConfiguration;
import edu.arizona.cs.mrpkm.cluster.MRClusterConfiguration_default;
import edu.arizona.cs.mrpkm.types.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.CompressedSequenceWritable;
import edu.arizona.cs.mrpkm.types.MultiFileCompressedSequenceWritable;
import edu.arizona.cs.mrpkm.fastareader.FastaReadInputFormat;
import edu.arizona.cs.mrpkm.notification.EmailNotification;
import edu.arizona.cs.mrpkm.notification.EmailNotificationException;
import edu.arizona.cs.mrpkm.types.NamedOutput;
import edu.arizona.cs.mrpkm.types.NamedOutputs;
import edu.arizona.cs.mrpkm.utils.FileSystemHelper;
import edu.arizona.cs.mrpkm.utils.MapReduceHelper;
import edu.arizona.cs.mrpkm.utils.MultipleOutputsHelper;
import edu.arizona.cs.mrpkm.utils.RunningTimeHelper;
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
public class KmerIndexBuilder extends Configured implements Tool {
    
    private static final Log LOG = LogFactory.getLog(KmerIndexBuilder.class);
    
    private static class KmerIndexBuilder_Cmd_Args {
        
        private static final int DEFAULT_KMERSIZE = 20;
        
        @Option(name = "-h", aliases = "--help", usage = "print this message") 
        private boolean help = false;
        
        private AMRClusterConfiguration cluster = new MRClusterConfiguration_default();
        
        @Option(name = "-c", aliases = "--cluster", usage = "specify cluster configuration")
        public void setCluster(String clusterConf) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
            this.cluster = AMRClusterConfiguration.findConfiguration(clusterConf);
        }
        
        @Option(name = "-k", aliases = "--kmersize", usage = "specify kmer size")
        private int kmersize = DEFAULT_KMERSIZE;
        
        @Option(name = "-n", aliases = "--nodenum", usage = "specify the number of hadoop slaves")
        private int nodes = 1;
        
        @Option(name = "-g", aliases = "--group", usage = "specify group size")
        private int groupSize = 1;
        
        private Class outputFormat = MapFileOutputFormat.class;
        
        @Option(name = "-f", aliases = "--outputformat", usage = "specify output format")
        public void setOutputFormat(String outputFormat) throws Exception {
            if (outputFormat.equalsIgnoreCase(MapFileOutputFormat.class.getName())) {
                this.outputFormat = MapFileOutputFormat.class;
            } else if (outputFormat.equalsIgnoreCase(BloomMapFileOutputFormat.class.getName())) {
                this.outputFormat = BloomMapFileOutputFormat.class;
            } else if (outputFormat.equalsIgnoreCase("map") || outputFormat.equalsIgnoreCase("mapfile")) {
                this.outputFormat = MapFileOutputFormat.class;
            } else if (outputFormat.equalsIgnoreCase("bloom") || outputFormat.equalsIgnoreCase("bloommap") || outputFormat.equalsIgnoreCase("bloommapfile")) {
                this.outputFormat = BloomMapFileOutputFormat.class;
            } else {
                throw new Exception("given arg is not in correct data type");
            }
        }
        
        @Option(name = "-i", aliases = "--readidpath", required = true, usage = "specify ReadID index path")
        private String ridPath = null;
        
        @Option(name = "--notifyemail", usage = "specify email address for job notification")
        private String notificationEmail;
        
        @Option(name = "--notifypassword", usage = "specify email password for job notification")
        private String notificationPassword;
        
        @Argument(metaVar = "input-path [input-path ...] output-path", usage = "input-paths and output-path")
        private List<String> paths = new ArrayList<String>();
        
        public boolean isHelp() {
            return this.help;
        }
        
        public AMRClusterConfiguration getConfiguration() {
            return this.cluster;
        }
        
        public int getKmerSize() {
            return this.kmersize;
        }
        
        public int getNodes() {
            return this.nodes;
        }
        
        public int getGroupSize() {
            return this.groupSize;
        }
        
        public Class getOutputFormat() {
            return this.outputFormat;
        }
        
        public String getReadIDIndexPath() {
            return this.ridPath;
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
        
        public boolean needNotification() {
            return (notificationEmail != null);
        }
        
        public String getNotificationEmail() {
            return notificationEmail;
        }
        
        public String getNotificationPassword() {
            return notificationPassword;
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
                    this.kmersize <= 0 ||
                    this.nodes <= 0 ||
                    this.outputFormat == null ||
                    this.ridPath == null || this.ridPath.isEmpty() ||
                    this.paths == null || this.paths.isEmpty() ||
                    this.paths.size() < 2) {
                return false;
            }
            return true;
        }
    }
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new KmerIndexBuilder(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        long beginTime = RunningTimeHelper.getCurrentTime();
        
        // parse command line
        KmerIndexBuilder_Cmd_Args cmdargs = new KmerIndexBuilder_Cmd_Args();
        CmdLineParser parser = new CmdLineParser(cmdargs);
        CmdLineException parseException = null;
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
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
        
        int kmerSize = cmdargs.getKmerSize();
        int nodeSize = cmdargs.getNodes();
        Class outputFormat = cmdargs.getOutputFormat();
        String readIDIndexPath = cmdargs.getReadIDIndexPath();
        String inputPath = cmdargs.getCommaSeparatedInputPath();
        String outputPath = cmdargs.getOutputPath();
        AMRClusterConfiguration clusterConfig = cmdargs.getConfiguration();
        int numReducers = clusterConfig.getKmerIndexBuilderReducerNumber(nodeSize);
        int groupSize = cmdargs.getGroupSize();
        
        // configuration
        Configuration conf = this.getConf();
        clusterConfig.setConfiguration(conf);
        
        conf.setInt(KmerIndexHelper.getConfigurationKeyOfKmerSize(), kmerSize);
        conf.setStrings(KmerIndexHelper.getConfigurationKeyOfReadIDIndexPath(), readIDIndexPath);
        
        String[] paths = FileSystemHelper.splitCommaSeparated(inputPath);
        Path[] inputFiles = FileSystemHelper.getAllFastaFilePaths(conf, paths);
        
        int rounds = inputFiles.length / groupSize;
        if(inputFiles.length % groupSize != 0) {
            rounds++;
        }
        
        boolean job_result = true;
        Job lastJob = null;
        
        for(int round=0;round<rounds;round++) {
            Path[] roundInputFiles = getRoundInputFiles(round, groupSize, inputFiles);
            String roundOutputPath = outputPath + "_round" + round;
            
            Job job = new Job(conf, "Kmer Index Builder Round " + round + " of " + rounds);
            job.setJarByClass(KmerIndexBuilder.class);

            // Mapper & Reducer
            job.setMapperClass(KmerIndexBuilderMapper.class);
            job.setCombinerClass(KmerIndexBuilderCombiner.class);
            job.setPartitionerClass(KmerIndexBuilderPartitioner.class);
            job.setReducerClass(KmerIndexBuilderReducer.class);

            job.setMapOutputKeyClass(MultiFileCompressedSequenceWritable.class);
            job.setMapOutputValueClass(CompressedIntArrayWritable.class);

            // Specify key / value
            job.setOutputKeyClass(CompressedSequenceWritable.class);
            job.setOutputValueClass(CompressedIntArrayWritable.class);

            // Inputs
            FileInputFormat.addInputPaths(job, FileSystemHelper.makeCommaSeparated(roundInputFiles));

            NamedOutputs namedOutputs = new NamedOutputs();
            LOG.info("Input files : " + roundInputFiles.length);
            for (int i = 0; i < roundInputFiles.length; i++) {
                LOG.info("> " + roundInputFiles[i].toString());
                namedOutputs.addNamedOutput(roundInputFiles[i]);
            }
            
            job.setInputFormatClass(FastaReadInputFormat.class);

            Path outputHadoopPath = new Path(roundOutputPath);
            FileSystem outputFileSystem = outputHadoopPath.getFileSystem(job.getConfiguration());
            if (outputFileSystem instanceof HirodsFileSystem) {
                LOG.info("Use H-iRODS");
                HirodsFileOutputFormat.setOutputPath(job, outputHadoopPath);
                if (outputFormat.equals(MapFileOutputFormat.class)) {
                    job.setOutputFormatClass(HirodsMapFileOutputFormat.class);
                } else if (outputFormat.equals(BloomMapFileOutputFormat.class)) {
                    job.setOutputFormatClass(HirodsBloomMapFileOutputFormat.class);
                }
                MultipleOutputsHelper.setMultipleOutputsClass(job.getConfiguration(), HirodsMultipleOutputs.class);
            } else {
                FileOutputFormat.setOutputPath(job, outputHadoopPath);
                if (outputFormat.equals(MapFileOutputFormat.class)) {
                    job.setOutputFormatClass(MapFileOutputFormat.class);
                } else if (outputFormat.equals(BloomMapFileOutputFormat.class)) {
                    job.setOutputFormatClass(BloomMapFileOutputFormat.class);
                }
                MultipleOutputsHelper.setMultipleOutputsClass(job.getConfiguration(), MultipleOutputs.class);
            }

            int id = 0;
            for (NamedOutput namedOutput : namedOutputs.getAllNamedOutput()) {
            LOG.info("regist new named output : " + namedOutput.getNamedOutputString());

                job.getConfiguration().setStrings(KmerIndexHelper.getConfigurationKeyOfNamedOutputName(id), namedOutput.getNamedOutputString());
                LOG.info("regist new ConfigString : " + KmerIndexHelper.getConfigurationKeyOfNamedOutputName(id));

                job.getConfiguration().setInt(KmerIndexHelper.getConfigurationKeyOfNamedOutputID(namedOutput.getInputString()), id);
                LOG.info("regist new ConfigString : " + KmerIndexHelper.getConfigurationKeyOfNamedOutputID(namedOutput.getInputString()));

                if (outputFileSystem instanceof HirodsFileSystem) {
                    if (outputFormat.equals(MapFileOutputFormat.class)) {
                        HirodsMultipleOutputs.addNamedOutput(job, namedOutput.getNamedOutputString(), HirodsMapFileOutputFormat.class, CompressedSequenceWritable.class, CompressedIntArrayWritable.class);
                    } else if (outputFormat.equals(BloomMapFileOutputFormat.class)) {
                        HirodsMultipleOutputs.addNamedOutput(job, namedOutput.getNamedOutputString(), HirodsBloomMapFileOutputFormat.class, CompressedSequenceWritable.class, CompressedIntArrayWritable.class);
                    }
                } else {
                    MultipleOutputs.addNamedOutput(job, namedOutput.getNamedOutputString(), outputFormat, CompressedSequenceWritable.class, CompressedIntArrayWritable.class);
                }

                id++;
            }

            job.setNumReduceTasks(numReducers);

            // Execute job and return status
            boolean result = job.waitForCompletion(true);
            
            lastJob = job;

            // commit results
            if (result) {
                commitRoundOutputFiles(new Path(roundOutputPath), new Path(outputPath), job.getConfiguration(), namedOutputs, kmerSize);
            }
            
            if(!result) {
                LOG.error("job failed at round " + round + " of " + inputFiles.length);
                job_result = false;
                break;
            }
        }
        
        long endTime = RunningTimeHelper.getCurrentTime();
        
        // notify
        if (cmdargs.needNotification()) {
            EmailNotification emailNotification = new EmailNotification(cmdargs.getNotificationEmail(), cmdargs.getNotificationPassword());
            emailNotification.setJob(lastJob);
            emailNotification.setJobBeginTime(beginTime);
            emailNotification.setJobFinishTime(endTime);
            try {
                emailNotification.send();
            } catch (EmailNotificationException ex) {
                LOG.error(ex);
            }
        }

        return job_result ? 0 : 1;
    }
    
    private Path[] getRoundInputFiles(int round, int groupSize, Path[] inputFiles) {
        List<Path> arr = new ArrayList<Path>();
        
        int start = round * groupSize;
        for(int i=0;i<groupSize;i++) {
            if(start+i < inputFiles.length) {
                arr.add(inputFiles[start + i]);
            } else {
                break;
            }
        }
        
        return arr.toArray(new Path[0]);
    }
    
    private void commitRoundOutputFiles(Path MROutputPath, Path finalOutputPath, Configuration conf, NamedOutputs namedOutputs, int kmerSize) throws IOException {
        FileSystem fs = MROutputPath.getFileSystem(conf);
        if(!fs.exists(finalOutputPath)) {
            fs.mkdirs(finalOutputPath);
        }
        
        FileStatus status = fs.getFileStatus(MROutputPath);
        if (status.isDir()) {
            FileStatus[] entries = fs.listStatus(MROutputPath);
            for (FileStatus entry : entries) {
                Path entryPath = entry.getPath();
                
                // remove unnecessary outputs
                if(MapReduceHelper.isLogFiles(entryPath)) {
                    fs.delete(entryPath, true);
                } else {
                    // rename outputs
                    NamedOutput namedOutput = namedOutputs.getNamedOutputByMROutput(entryPath);
                    if(namedOutput != null) {
                        int mapreduceID = MapReduceHelper.getMapReduceID(entryPath);
                        Path toPath = new Path(finalOutputPath, KmerIndexHelper.getKmerIndexFileName(namedOutput.getInputString(), kmerSize, mapreduceID));
                        
                        LOG.info("output : " + entryPath.toString());
                        LOG.info("renamed to : " + toPath.toString());
                        fs.rename(entryPath, toPath);
                    }
                }
            }
        } else {
            throw new IOException("path not found : " + MROutputPath.toString());
        }
        
        fs.delete(MROutputPath, true);
    }
}
