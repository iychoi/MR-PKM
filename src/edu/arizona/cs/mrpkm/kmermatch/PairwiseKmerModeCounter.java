package edu.arizona.cs.mrpkm.kmermatch;

import edu.arizona.cs.hadoop.fs.irods.output.HirodsFileOutputFormat;
import edu.arizona.cs.hadoop.fs.irods.output.HirodsMultipleOutputs;
import edu.arizona.cs.hadoop.fs.irods.output.HirodsTextOutputFormat;
import edu.arizona.cs.mrpkm.cluster.AMRClusterConfiguration;
import edu.arizona.cs.mrpkm.kmeridx.KmerIndexHelper;
import edu.arizona.cs.mrpkm.notification.EmailNotification;
import edu.arizona.cs.mrpkm.notification.EmailNotificationException;
import edu.arizona.cs.mrpkm.types.MultiFileReadIDWritable;
import edu.arizona.cs.mrpkm.namedoutputs.NamedOutput;
import edu.arizona.cs.mrpkm.namedoutputs.NamedOutputs;
import edu.arizona.cs.mrpkm.utils.FileSystemHelper;
import edu.arizona.cs.mrpkm.utils.MapReduceHelper;
import edu.arizona.cs.mrpkm.utils.MultipleOutputsHelper;
import java.io.IOException;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author iychoi
 */
public class PairwiseKmerModeCounter extends Configured implements Tool {
    
    private static final Log LOG = LogFactory.getLog(PairwiseKmerModeCounter.class);
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PairwiseKmerModeCounter(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        PairwiseKmerModeCounterCmdParamsParser parser = new PairwiseKmerModeCounterCmdParamsParser();
        PairwiseKmerModeCounterCmdParams cmdParams = parser.parse(args);
        
        int kmerSize = cmdParams.getKmerSize();
        int nodeSize = cmdParams.getNodes();
        AMRClusterConfiguration clusterConfig = cmdParams.getClusterConfig();
        int partitionNum = cmdParams.getPartitions(nodeSize * clusterConfig.getCoresPerMachine());
        //String histogramPath = cmdParams.getHistogramPath();
        String stddevFilterPath = cmdParams.getStdDeviationFilterPath();
        String inputPath = cmdParams.getCommaSeparatedInputPath();
        String outputPath = cmdParams.getOutputPath();
        int numReducers = clusterConfig.getPairwiseKmerModeCounterReducerNumber(nodeSize);
        
        // configuration
        Configuration conf = this.getConf();
        clusterConfig.configureClusterParamsTo(conf);
        
        /*
        KmerHistogramReaderConfig histogramReaderConfig = new KmerHistogramReaderConfig();
        histogramReaderConfig.setInputPath(histogramPath);
        histogramReaderConfig.saveTo(conf);
        */
        
        Job job = new Job(conf, "Pairwise Kmer Mode Counter");
        conf = job.getConfiguration();
        
        job.setJarByClass(PairwiseKmerModeCounter.class);

        // Mapper
        job.setMapperClass(PairwiseKmerModeCounterMapper.class);
        job.setInputFormatClass(KmerMatchInputFormat.class);
        job.setMapOutputKeyClass(MultiFileReadIDWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        // Partitioner
        job.setPartitionerClass(PairwiseKmerModeCounterPartitioner.class);
        
        // Reducer
        job.setReducerClass(PairwiseKmerModeCounterReducer.class);
        
        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        // Inputs
        Path[] inputFiles = KmerIndexHelper.getAllKmerIndexFilePaths(conf, inputPath);
        KmerMatchInputFormat.addInputPaths(job, FileSystemHelper.makeCommaSeparated(inputFiles));
        
        KmerMatchInputFormatConfig matchInputFormatConfig = new KmerMatchInputFormatConfig();
        matchInputFormatConfig.setKmerSize(kmerSize);
        matchInputFormatConfig.setPartitionNum(partitionNum);
        matchInputFormatConfig.setStdDeviationFilterPath(stddevFilterPath);
        KmerMatchInputFormat.setInputFormatConfig(job, matchInputFormatConfig);
        
        for(Path path : inputFiles) {
            LOG.info("Input : " + path.toString());
            // check kmerSize
            int myKmerSize = KmerIndexHelper.getKmerSize(path);
            if(kmerSize != myKmerSize) {
                throw new Exception("kmer sizes of given index files are different");
            }
        }
        
        Path[][] groups = KmerIndexHelper.groupKmerIndice(inputFiles);
        LOG.info("Input index groups : " + groups.length);
        
        // Register named outputs
        LOG.info("Adding named outputs");
        NamedOutputs namedOutputs = new NamedOutputs();
        for(int i=0;i<groups.length;i++) {
            Path[] thisGroup = groups[i];
            for(int j=0;j<groups.length;j++) {
                Path[] thatGroup = groups[j];
                if(i != j) {
                    String thisGroupFastaFilename = KmerIndexHelper.getFastaFileName(thisGroup[0]);
                    String thatGroupFastaFilename = KmerIndexHelper.getFastaFileName(thatGroup[0]);
                    String pairwiseModeCounterOutputName = PairwiseKmerModeCounterHelper.getPairwiseModeCounterOutputName(thisGroupFastaFilename, thatGroupFastaFilename);
                    namedOutputs.addNamedOutput(pairwiseModeCounterOutputName);
                }
            }
        }
        namedOutputs.saveTo(conf);
        
        boolean hirodsOutputPath = FileSystemHelper.isHirodsFileSystemPath(conf, outputPath);
        if(hirodsOutputPath) {
            LOG.info("Use H-iRODS");
            HirodsFileOutputFormat.setOutputPath(job, new Path(outputPath));
            job.setOutputFormatClass(HirodsTextOutputFormat.class);
            MultipleOutputsHelper.setMultipleOutputsClass(conf, HirodsMultipleOutputs.class);
        } else {
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            job.setOutputFormatClass(TextOutputFormat.class);
            MultipleOutputsHelper.setMultipleOutputsClass(conf, MultipleOutputs.class);
        }
        
        for(NamedOutput namedOutput : namedOutputs.getAllNamedOutput()) {
            if(hirodsOutputPath) {
                HirodsMultipleOutputs.addNamedOutput(job, namedOutput.getNamedOutputString(), HirodsTextOutputFormat.class, Text.class, Text.class);
            } else {
                MultipleOutputs.addNamedOutput(job, namedOutput.getNamedOutputString(), TextOutputFormat.class, Text.class, Text.class);
            }
        }
        
        job.setNumReduceTasks(numReducers);
        
        // Execute job and return status
        boolean result = job.waitForCompletion(true);

        // commit results
        if(result) {
            commit(new Path(outputPath), conf, namedOutputs);
        }
        
        // notify
        if(cmdParams.needNotification()) {
            EmailNotification emailNotification = new EmailNotification(cmdParams.getNotificationEmail(), cmdParams.getNotificationPassword());
            emailNotification.addJob(job);
            try {
                emailNotification.send();
            } catch(EmailNotificationException ex) {
                LOG.error(ex);
            }
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
                    NamedOutput namedOutput = namedOutputs.getNamedOutputByMROutput(entryPath);
                    if(namedOutput != null) {
                        Path toPath = new Path(entryPath.getParent(), PairwiseKmerModeCounterHelper.makePairwiseModeCountFileName(namedOutput.getInputString()));
                        
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
