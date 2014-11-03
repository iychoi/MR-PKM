package edu.arizona.cs.mrpkm.kmeridx;

import edu.arizona.cs.hadoop.fs.irods.output.HirodsFileOutputFormat;
import edu.arizona.cs.hadoop.fs.irods.output.HirodsMapFileOutputFormat;
import edu.arizona.cs.hadoop.fs.irods.output.HirodsMultipleOutputs;
import edu.arizona.cs.mrpkm.hadoop.io.format.bloommap.BloomMapFileOutputFormat;
import edu.arizona.cs.mrpkm.hadoop.io.format.bloommap.HirodsBloomMapFileOutputFormat;
import edu.arizona.cs.mrpkm.cluster.AMRClusterConfiguration;
import edu.arizona.cs.mrpkm.types.hadoop.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.hadoop.CompressedSequenceWritable;
import edu.arizona.cs.mrpkm.types.hadoop.MultiFileCompressedSequenceWritable;
import edu.arizona.cs.mrpkm.hadoop.io.format.fasta.FastaReadInputFormat;
import edu.arizona.cs.mrpkm.notification.EmailNotification;
import edu.arizona.cs.mrpkm.notification.EmailNotificationException;
import edu.arizona.cs.mrpkm.types.namedoutputs.NamedOutputRecord;
import edu.arizona.cs.mrpkm.types.namedoutputs.NamedOutputs;
import edu.arizona.cs.mrpkm.helpers.FileSystemHelper;
import edu.arizona.cs.mrpkm.helpers.MapReduceHelper;
import edu.arizona.cs.mrpkm.helpers.MultipleOutputsHelper;
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

/**
 *
 * @author iychoi
 */
public class KmerIndexBuilder extends Configured implements Tool {
    
    private static final Log LOG = LogFactory.getLog(KmerIndexBuilder.class);
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new KmerIndexBuilder(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        KmerIndexBuilderCmdParamsParser parser = new KmerIndexBuilderCmdParamsParser();
        KmerIndexBuilderCmdParams cmdParams = parser.parse(args);
        
        int groupSize = cmdParams.getGroupSize();
        int kmerSize = cmdParams.getKmerSize();
        int nodeSize = cmdParams.getNodes();
        Class outputFormat = cmdParams.getOutputFormat();
        String readIDIndexPath = cmdParams.getReadIDIndexPath();
        String histogramPath = cmdParams.getHistogramPath();
        String inputPath = cmdParams.getCommaSeparatedInputPath();
        String outputPath = cmdParams.getOutputPath();
        AMRClusterConfiguration clusterConfig = cmdParams.getClusterConfig();
        int numReducers = clusterConfig.getKmerIndexBuilderReducerNumber(nodeSize);
        
        // configuration
        Configuration conf = this.getConf();
        clusterConfig.configureClusterParamsTo(conf);
        
        KmerIndexBuilderConfig indexBuilderConfig = new KmerIndexBuilderConfig();
        indexBuilderConfig.setKmerSize(kmerSize);
        indexBuilderConfig.setReadIDIndexPath(readIDIndexPath);
        indexBuilderConfig.setHistogramPath(histogramPath);
        indexBuilderConfig.saveTo(conf);
        
        String[] paths = FileSystemHelper.splitCommaSeparated(inputPath);
        Path[] inputFiles = FileSystemHelper.getAllFastaFilePaths(conf, paths);
        
        int rounds = inputFiles.length / groupSize;
        if(inputFiles.length % groupSize != 0) {
            rounds++;
        }
        
        boolean job_result = true;
        List<Job> jobs = new ArrayList<Job>();
        
        for(int round=0;round<rounds;round++) {
            Path[] roundInputFiles = getRoundInputFiles(round, groupSize, inputFiles);
            String roundOutputPath = outputPath + "_round" + round;
            
            Job job = new Job(conf, "Kmer Index Builder Round " + round + " of " + rounds);
            job.setJarByClass(KmerIndexBuilder.class);

            // Mapper
            job.setMapperClass(KmerIndexBuilderMapper.class);
            job.setInputFormatClass(FastaReadInputFormat.class);
            job.setMapOutputKeyClass(MultiFileCompressedSequenceWritable.class);
            job.setMapOutputValueClass(CompressedIntArrayWritable.class);
            
            // Combiner
            job.setCombinerClass(KmerIndexBuilderCombiner.class);
            
            // Partitioner
            job.setPartitionerClass(KmerIndexBuilderPartitioner.class);
            
            // Reducer
            job.setReducerClass(KmerIndexBuilderReducer.class);

            // Specify key / value
            job.setOutputKeyClass(CompressedSequenceWritable.class);
            job.setOutputValueClass(CompressedIntArrayWritable.class);

            // Inputs
            FileInputFormat.addInputPaths(job, FileSystemHelper.makeCommaSeparated(roundInputFiles));

            LOG.info("Input files : " + roundInputFiles.length);
            for (Path roundInputFile : roundInputFiles) {
                LOG.info("> " + roundInputFile.toString());
            }

            // Register named outputs
            NamedOutputs namedOutputs = new NamedOutputs();
            namedOutputs.add(roundInputFiles);
            namedOutputs.saveTo(job.getConfiguration());
            
            boolean hirodsOutputPath = FileSystemHelper.isHirodsFileSystemPath(conf, roundOutputPath);
            if(hirodsOutputPath) {
                LOG.info("Use H-iRODS");
                HirodsFileOutputFormat.setOutputPath(job, new Path(roundOutputPath));
                if (outputFormat.equals(MapFileOutputFormat.class)) {
                    job.setOutputFormatClass(HirodsMapFileOutputFormat.class);
                } else if (outputFormat.equals(BloomMapFileOutputFormat.class)) {
                    job.setOutputFormatClass(HirodsBloomMapFileOutputFormat.class);
                }
                MultipleOutputsHelper.setMultipleOutputsClass(job.getConfiguration(), HirodsMultipleOutputs.class);
            } else {
                FileOutputFormat.setOutputPath(job, new Path(roundOutputPath));
                if (outputFormat.equals(MapFileOutputFormat.class)) {
                    job.setOutputFormatClass(MapFileOutputFormat.class);
                } else if (outputFormat.equals(BloomMapFileOutputFormat.class)) {
                    job.setOutputFormatClass(BloomMapFileOutputFormat.class);
                }
                MultipleOutputsHelper.setMultipleOutputsClass(job.getConfiguration(), MultipleOutputs.class);
            }

            for(NamedOutputRecord namedOutput : namedOutputs.getAllRecords()) {
                if(hirodsOutputPath) {
                    if (outputFormat.equals(MapFileOutputFormat.class)) {
                        HirodsMultipleOutputs.addNamedOutput(job, namedOutput.getIdentifier(), HirodsMapFileOutputFormat.class, CompressedSequenceWritable.class, CompressedIntArrayWritable.class);
                    } else if (outputFormat.equals(BloomMapFileOutputFormat.class)) {
                        HirodsMultipleOutputs.addNamedOutput(job, namedOutput.getIdentifier(), HirodsBloomMapFileOutputFormat.class, CompressedSequenceWritable.class, CompressedIntArrayWritable.class);
                    }
                } else {
                    MultipleOutputs.addNamedOutput(job, namedOutput.getIdentifier(), outputFormat, CompressedSequenceWritable.class, CompressedIntArrayWritable.class);
                }
            }
            
            job.setNumReduceTasks(numReducers);

            // Execute job and return status
            boolean result = job.waitForCompletion(true);
            
            jobs.add(job);

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
                } else if(MapReduceHelper.isPartialOutputFiles(entryPath)) {
                    fs.delete(entryPath, true);
                } else {
                    // rename outputs
                    NamedOutputRecord namedOutput = namedOutputs.getRecordFromMROutput(entryPath);
                    if(namedOutput != null) {
                        int mapreduceID = MapReduceHelper.getMapReduceID(entryPath);
                        Path toPath = new Path(finalOutputPath, KmerIndexHelper.makeKmerIndexFileName(namedOutput.getFilename(), kmerSize, mapreduceID));
                        
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
