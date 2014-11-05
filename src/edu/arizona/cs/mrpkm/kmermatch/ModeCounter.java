package edu.arizona.cs.mrpkm.kmermatch;

import edu.arizona.cs.hadoop.fs.irods.output.HirodsFileOutputFormat;
import edu.arizona.cs.hadoop.fs.irods.output.HirodsMultipleOutputs;
import edu.arizona.cs.hadoop.fs.irods.output.HirodsTextOutputFormat;
import edu.arizona.cs.mrpkm.cluster.AMRClusterConfiguration;
import edu.arizona.cs.mrpkm.helpers.FileSystemHelper;
import edu.arizona.cs.mrpkm.helpers.MapReduceHelper;
import edu.arizona.cs.mrpkm.helpers.MultipleOutputsHelper;
import edu.arizona.cs.mrpkm.notification.EmailNotification;
import edu.arizona.cs.mrpkm.notification.EmailNotificationException;
import edu.arizona.cs.mrpkm.types.hadoop.MultiFileReadIDWritable;
import edu.arizona.cs.mrpkm.types.namedoutputs.NamedOutputRecord;
import edu.arizona.cs.mrpkm.types.namedoutputs.NamedOutputs;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author iychoi
 */
public class ModeCounter extends Configured implements Tool {
    
    private static final Log LOG = LogFactory.getLog(ModeCounter.class);
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ModeCounter(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        ModeCounterCmdParamsParser parser = new ModeCounterCmdParamsParser();
        ModeCounterCmdParams cmdParams = parser.parse(args);
        
        int nodeSize = cmdParams.getNodes();
        String inputPath = cmdParams.getInputPath();
        String outputPath = cmdParams.getOutputPath();
        AMRClusterConfiguration clusterConfig = cmdParams.getClusterConfig();
        //int numReducers = clusterConfig.getPairwiseKmerModeCounterReducerNumber(nodeSize);
        
        // configuration
        Configuration conf = this.getConf();
        clusterConfig.configureClusterParamsTo(conf);
        
        // TOC
        Path TOCfilePath = new Path(inputPath, PairwiseKmerMatcherHelper.makePairwiseKmerMatchTOCFileName());
        FileSystem fs = TOCfilePath.getFileSystem(conf);
        PairwiseKmerMatcherConfig matcherConfig = new PairwiseKmerMatcherConfig();
        matcherConfig.loadFrom(TOCfilePath, fs);

        Path[] inputFiles = PairwiseKmerMatcherHelper.getAllPairwiseKmerMatchFilePaths(conf, inputPath);
        
        int rounds = matcherConfig.getSize();
        
        // Register named outputs
        NamedOutputs namedOutputs = new NamedOutputs();
        for(int i=0;i<matcherConfig.getSize();i++) {
            String input = matcherConfig.getInputFromID(i);
            namedOutputs.add(input);
        }
        namedOutputs.saveTo(conf);
        
        boolean job_result = true;
        List<Job> jobs = new ArrayList<Job>();
        
        for(int round=0;round<rounds;round++) {
            String roundOutputPath = outputPath + "_round" + round;
            
            Job job = new Job(conf, "Mode Counter Round " + round + " of " + rounds);
            job.setJarByClass(ModeCounter.class);
            
            // Mapper
            job.setMapperClass(ModeCounterMapper.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapOutputKeyClass(MultiFileReadIDWritable.class);
            job.setMapOutputValueClass(IntWritable.class);
            
            // Partitioner
            job.setPartitionerClass(ModeCounterPartitioner.class);
            
            // Reducer
            job.setReducerClass(ModeCounterReducer.class);
            
            // Specify key / value
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            
            // Inputs
            FileInputFormat.addInputPaths(job, FileSystemHelper.makeCommaSeparated(inputFiles));
            
            ModeCounterConfig modeCounterConfig = new ModeCounterConfig();
            modeCounterConfig.setMasterFileID(round);
            modeCounterConfig.saveTo(job.getConfiguration());
            
            boolean hirodsOutputPath = FileSystemHelper.isHirodsFileSystemPath(job.getConfiguration(), roundOutputPath);
            if (hirodsOutputPath) {
                LOG.info("Use H-iRODS");
                HirodsFileOutputFormat.setOutputPath(job, new Path(roundOutputPath));
                job.setOutputFormatClass(HirodsTextOutputFormat.class);
                MultipleOutputsHelper.setMultipleOutputsClass(job.getConfiguration(), HirodsMultipleOutputs.class);
            } else {
                FileOutputFormat.setOutputPath(job, new Path(roundOutputPath));
                job.setOutputFormatClass(TextOutputFormat.class);
                MultipleOutputsHelper.setMultipleOutputsClass(job.getConfiguration(), MultipleOutputs.class);
            }

            for (NamedOutputRecord namedOutput : namedOutputs.getAllRecords()) {
                if (hirodsOutputPath) {
                    HirodsMultipleOutputs.addNamedOutput(job, namedOutput.getIdentifier(), HirodsTextOutputFormat.class, Text.class, Text.class);
                } else {
                    MultipleOutputs.addNamedOutput(job, namedOutput.getIdentifier(), TextOutputFormat.class, Text.class, Text.class);
                }
            }
            
            job.setNumReduceTasks(matcherConfig.getSize());
            
            // Execute job and return status
            boolean result = job.waitForCompletion(true);
            
            jobs.add(job);

            // commit results
            if (result) {
                commitRoundOutputFiles(new Path(roundOutputPath), new Path(outputPath), job.getConfiguration(), namedOutputs, round);
            }
            
            if(!result) {
                LOG.error("job failed at round " + round + " of " + rounds);
                job_result = false;
                break;
            }
        }
        
        // notify
        if (cmdParams.needNotification()) {
            EmailNotification emailNotification = new EmailNotification(cmdParams.getNotificationEmail(), cmdParams.getNotificationPassword());
            emailNotification.addJob(jobs);
            try {
                emailNotification.send();
            } catch (EmailNotificationException ex) {
                LOG.error(ex);
            }
        }
        
        return job_result ? 0 : 1;
    }
    
    private void commitRoundOutputFiles(Path MROutputPath, Path finalOutputPath, Configuration conf, NamedOutputs namedOutputs, int round) throws IOException {
        FileSystem fs = MROutputPath.getFileSystem(conf);
        if(!fs.exists(finalOutputPath)) {
            fs.mkdirs(finalOutputPath);
        }
        
        NamedOutputRecord roundMasterRecord = namedOutputs.getRecordFromID(round);
        Path roundDestPath = new Path(finalOutputPath, roundMasterRecord.getFilename());
        if(!fs.exists(roundDestPath)) {
            fs.mkdirs(roundDestPath);
        }
        
        FileStatus status = fs.getFileStatus(MROutputPath);
        if (status.isDir()) {
            FileStatus[] entries = fs.listStatus(MROutputPath);
            for (FileStatus entry : entries) {
                Path entryPath = entry.getPath();
                
                // remove unnecessary outputs
                if(MapReduceHelper.isLogFiles(entryPath)) {
                    fs.delete(entryPath, true);
                } else if(MapReduceHelper.isPartialOutputFiles(entryPath)) {
                    fs.delete(entryPath, true);
                } else {
                    // rename outputs
                    NamedOutputRecord namedOutput = namedOutputs.getRecordFromMROutput(entryPath);
                    if(namedOutput != null) {
                        Path toPath = new Path(roundDestPath, namedOutput.getFilename());
                        
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
