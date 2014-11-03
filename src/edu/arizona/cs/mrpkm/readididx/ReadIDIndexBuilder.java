package edu.arizona.cs.mrpkm.readididx;

import edu.arizona.cs.hadoop.fs.irods.output.HirodsFileOutputFormat;
import edu.arizona.cs.hadoop.fs.irods.output.HirodsMapFileOutputFormat;
import edu.arizona.cs.hadoop.fs.irods.output.HirodsMultipleOutputs;
import edu.arizona.cs.mrpkm.cluster.AMRClusterConfiguration;
import edu.arizona.cs.mrpkm.hadoop.io.format.fasta.FastaReadInputFormat;
import edu.arizona.cs.mrpkm.notification.EmailNotification;
import edu.arizona.cs.mrpkm.notification.EmailNotificationException;
import edu.arizona.cs.mrpkm.types.namedoutputs.NamedOutputRecord;
import edu.arizona.cs.mrpkm.types.namedoutputs.NamedOutputs;
import edu.arizona.cs.mrpkm.helpers.FileSystemHelper;
import edu.arizona.cs.mrpkm.helpers.MapReduceHelper;
import edu.arizona.cs.mrpkm.helpers.MultipleOutputsHelper;
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
        ReadIDIndexBuilderCmdParamsParser parser = new ReadIDIndexBuilderCmdParamsParser();
        ReadIDIndexBuilderCmdParams cmdParams = parser.parse(args);
        
        int kmerSize = cmdParams.getKmerSize();
        String inputPath = cmdParams.getCommaSeparatedInputPath();
        String histogramPath = cmdParams.getHistogramPath();
        String outputPath = cmdParams.getOutputPath();
        AMRClusterConfiguration clusterConfig = cmdParams.getClusterConfig();
        
        // configuration
        Configuration conf = this.getConf();
        clusterConfig.configureClusterParamsTo(conf);
        
        Job job = new Job(conf, "ReadID Index Builder");
        conf = job.getConfiguration();
        
        job.setJarByClass(ReadIDIndexBuilder.class);

        // Mapper
        job.setMapperClass(UnsplitableReadIDIndexBuilderMapper.class);
        FastaReadInputFormat.setSplitable(conf, false);
        job.setInputFormatClass(FastaReadInputFormat.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        // Specify key / value
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);
        
        // Inputs
        Path[] inputFiles = FileSystemHelper.getAllFastaFilePaths(conf, inputPath);
        FileInputFormat.addInputPaths(job, FileSystemHelper.makeCommaSeparated(inputFiles));
        
        LOG.info("Input files : " + inputFiles.length);
        for(Path inputFile : inputFiles) {
            LOG.info("> " + inputFile.toString());
        }
        
        ReadIDIndexBuilderConfig builderConfig = new ReadIDIndexBuilderConfig();
        builderConfig.setHistogramOutputPath(histogramPath);
        builderConfig.setKmerSize(kmerSize);
        builderConfig.saveTo(conf);
        
        // Register named outputs
        NamedOutputs namedOutputs = new NamedOutputs();
        namedOutputs.add(inputFiles);
        namedOutputs.saveTo(conf);
        
        boolean hirodsOutputPath = FileSystemHelper.isHirodsFileSystemPath(conf, outputPath);
        if(hirodsOutputPath) {
            LOG.info("Use H-iRODS");
            HirodsFileOutputFormat.setOutputPath(job, new Path(outputPath));
            job.setOutputFormatClass(HirodsMapFileOutputFormat.class);
            MultipleOutputsHelper.setMultipleOutputsClass(conf, HirodsMultipleOutputs.class);
        } else {
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            job.setOutputFormatClass(MapFileOutputFormat.class);
            MultipleOutputsHelper.setMultipleOutputsClass(conf, MultipleOutputs.class);
        }
        
        for(NamedOutputRecord namedOutput : namedOutputs.getAllRecords()) {
            if(hirodsOutputPath) {
                HirodsMultipleOutputs.addNamedOutput(job, namedOutput.getIdentifier(), HirodsMapFileOutputFormat.class, LongWritable.class, IntWritable.class);
            } else {
                MultipleOutputs.addNamedOutput(job, namedOutput.getIdentifier(), MapFileOutputFormat.class, LongWritable.class, IntWritable.class);
            }
        }
        
        job.setNumReduceTasks(0);
        
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
                } else if(MapReduceHelper.isPartialOutputFiles(entryPath)) {
                    fs.delete(entryPath, true);
                } else if(KmerHistogramHelper.isHistogramFile(entryPath)) {
                    // rename histogram output
                    NamedOutputRecord namedOutput = namedOutputs.getRecord(KmerHistogramHelper.getInputFileName(entryPath.getName()));
                    if(namedOutput != null) {
                        Path toPath = new Path(entryPath.getParent(), KmerHistogramHelper.makeHistogramFileName(namedOutput.getFilename()));
                        
                        LOG.info("output : " + entryPath.toString());
                        LOG.info("renamed to : " + toPath.toString());
                        fs.rename(entryPath, toPath);
                    }
                } else {
                    // rename outputs
                    NamedOutputRecord namedOutput = namedOutputs.getRecordFromMROutput(entryPath.getName());
                    if(namedOutput != null) {
                        Path toPath = new Path(entryPath.getParent(), ReadIDIndexHelper.makeReadIDIndexFileName(namedOutput.getFilename()));
                        
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
