package edu.arizona.cs.mrpkm.statistics;

import edu.arizona.cs.mrpkm.cluster.AMRClusterConfiguration;
import edu.arizona.cs.mrpkm.kmeridx.KmerIndexHelper;
import edu.arizona.cs.mrpkm.report.notification.EmailNotification;
import edu.arizona.cs.mrpkm.report.notification.EmailNotificationException;
import edu.arizona.cs.mrpkm.helpers.FileSystemHelper;
import edu.arizona.cs.mrpkm.report.Report;
import edu.arizona.cs.mrpkm.types.statistics.KmerStdDeviation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author iychoi
 */
public class KmerStatisticsBuilder extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(KmerStatisticsBuilder.class);
    
    private List<Job> jobs = new ArrayList<Job>();
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new KmerStatisticsBuilder(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        KmerStatisticsBuilderCmdParamsParser parser = new KmerStatisticsBuilderCmdParamsParser();
        KmerStatisticsBuilderCmdParams cmdParams = parser.parse(args);
        
        int groupSize = cmdParams.getGroupSize();
        int kmerSize = cmdParams.getKmerSize();
        String inputPath = cmdParams.getCommaSeparatedInputPath();
        String outputPath = cmdParams.getOutputPath();
        AMRClusterConfiguration clusterConfig = cmdParams.getClusterConfig();
        
        // configuration
        Configuration conf = this.getConf();
        clusterConfig.configureClusterParamsTo(conf);
        
        String[] paths = FileSystemHelper.splitCommaSeparated(inputPath);
        Path[] inputFiles = KmerIndexHelper.getAllKmerIndexFilePaths(conf, paths);
        
        Path[][] groups = KmerIndexHelper.groupKmerIndice(inputFiles);
        LOG.info("Input index groups : " + groups.length);
        for(int i=0;i<groups.length;i++) {
            Path[] group = groups[i];
            LOG.info("Input index group " + i + " : " + group.length);
            for(int j=0;j<group.length;j++) {
                LOG.info("> " + group[j].toString());
            }
        }
        
        int rounds = groups.length / groupSize;
        if(groups.length % groupSize != 0) {
            rounds++;
        }
        
        int result = 1;
        for(int round=0;round<rounds;round++) {
            Path[] roundInputFiles = getRoundInputFiles(round, groupSize, groups);
            runKmerStatistics(conf, roundInputFiles, outputPath);
        }
        
        // report
        if(cmdParams.needReport()) {
            Report report = new Report();
            report.addJob(this.jobs);
            report.writeTo(cmdParams.getReportFilename());
        }
        
        // notify
        if(cmdParams.needNotification()) {
            EmailNotification emailNotification = new EmailNotification(cmdParams.getNotificationEmail(), cmdParams.getNotificationPassword());
            emailNotification.addJob(this.jobs);
            try {
                emailNotification.send();
            } catch(EmailNotificationException ex) {
                LOG.error(ex);
            }
        }
        
        return result;
    }
    
    private Path[] getRoundInputFiles(int round, int groupSize, Path[][] inputFiles) {
        List<Path> arr = new ArrayList<Path>();
        
        int start = round * groupSize;
        for(int i=0;i<groupSize;i++) {
            if(start+i < inputFiles.length) {
                for(Path path : inputFiles[start + i]) {
                    arr.add(path);
                }
            } else {
                break;
            }
        }
        
        return arr.toArray(new Path[0]);
    }
    
    private int runKmerStatistics(Configuration conf, Path[] inputIndexFiles, String outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = new Job(conf, "Kmer Statistics");
        conf = job.getConfiguration();
        
        job.setJarByClass(KmerStatisticsBuilder.class);
        
        // Mapper
        job.setMapperClass(KmerStatisticsBuilderMapper.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        
        // Specify key / value
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        
        // Inputs
        Path[] inputDataFiles = KmerIndexHelper.getAllKmerIndexDataFilePaths(conf, inputIndexFiles);
        SequenceFileInputFormat.addInputPaths(job, FileSystemHelper.makeCommaSeparated(inputDataFiles));
        
        // Outputs
        job.setOutputFormatClass(NullOutputFormat.class);
        
        job.setNumReduceTasks(0);
        
        // Execute job and return status
        boolean result = job.waitForCompletion(true);
        
        this.jobs.add(job);
        
        // check results
        if(result) {
            CounterGroup uniqueGroup = job.getCounters().getGroup(KmerStatisticsHelper.getCounterGroupNameUnique());
            CounterGroup totalGroup = job.getCounters().getGroup(KmerStatisticsHelper.getCounterGroupNameTotal());
            CounterGroup squareGroup = job.getCounters().getGroup(KmerStatisticsHelper.getCounterGroupNameSquare());
            
            Iterator<Counter> uniqueIterator = uniqueGroup.iterator();
            while(uniqueIterator.hasNext()) {
                long count = 0;
                long length = 0;
                long square = 0;
                double real_mean = 0;
                double stddev = 0;
                
                Counter uniqueCounter = uniqueIterator.next();
                //LOG.info("unique " + next.getName() + " : " + next.getValue());
                Counter totalCounter = totalGroup.findCounter(uniqueCounter.getName());
                Counter squareCounter = squareGroup.findCounter(uniqueCounter.getName());
                
                count = uniqueCounter.getValue();
                length = totalCounter.getValue();
                square = squareCounter.getValue();
                
                real_mean = (double)length / (double)count;
                // stddev = sqrt((sum(lengths ^ 2)/count) - (mean ^ 2)
                double mean = Math.pow(real_mean, 2);
                double term = (double)square / (double)count;
                stddev = Math.sqrt(term - mean);
                
                LOG.info("distinct k-mers " + uniqueCounter.getName() + " : " + count);
                LOG.info("total k-mers " + uniqueCounter.getName() + " : " + length);
                LOG.info("average " + uniqueCounter.getName() + " : " + real_mean);
                LOG.info("std-deviation " + uniqueCounter.getName() + " : " + stddev);
                
                Path outputHadoopPath = new Path(outputPath, KmerStatisticsHelper.makeStdDeviationFileName(uniqueCounter.getName()));
                FileSystem fs = outputHadoopPath.getFileSystem(conf);
                
                KmerStdDeviation stdDeviationWriter = new KmerStdDeviation();
                stdDeviationWriter.setStdDeviationName(uniqueCounter.getName());
                stdDeviationWriter.setUniqueKmers(count);
                stdDeviationWriter.setTotalKmers(length);
                stdDeviationWriter.setAverage(real_mean);
                stdDeviationWriter.setStdDeviation(stddev);
                
                stdDeviationWriter.saveTo(outputHadoopPath, fs);
            }
        }
        
        return result ? 0 : 1;
    }
}
