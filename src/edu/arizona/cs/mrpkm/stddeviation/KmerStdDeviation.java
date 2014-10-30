package edu.arizona.cs.mrpkm.stddeviation;

import edu.arizona.cs.mrpkm.cluster.AMRClusterConfiguration;
import edu.arizona.cs.mrpkm.kmeridx.KmerIndexHelper;
import edu.arizona.cs.mrpkm.notification.EmailNotification;
import edu.arizona.cs.mrpkm.notification.EmailNotificationException;
import edu.arizona.cs.mrpkm.utils.FileSystemHelper;
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
public class KmerStdDeviation extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(KmerStdDeviation.class);
    
    private List<Job> jobs = new ArrayList<Job>();
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new KmerStdDeviation(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        KmerStdDeviationCmdParamsParser parser = new KmerStdDeviationCmdParamsParser();
        KmerStdDeviationCmdParams cmdParams = parser.parse(args);
        
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
            KmerStatisticsGroup statisticsGroup = runKmerStatistics(conf, roundInputFiles);
            if(statisticsGroup != null) {
                result = runKmerStdDeviation(conf, statisticsGroup, roundInputFiles, outputPath);
                if(result != 0) {
                    LOG.error("runKmerStdDeviation failed");
                    break;
                }
            } else {
                LOG.error("runKmerStatistics failed");
                break;
            }
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
    
    private KmerStatisticsGroup runKmerStatistics(Configuration conf, Path[] inputIndexFiles) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = new Job(conf, "Kmer Statistics");
        conf = job.getConfiguration();
        
        job.setJarByClass(KmerStdDeviation.class);
        
        // Mapper
        job.setMapperClass(KmerStatisticsMapper.class);
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
            KmerStatisticsGroup statisticsGroup = new KmerStatisticsGroup();
            
            CounterGroup uniqueGroup = job.getCounters().getGroup(KmerStdDeviationHelper.getCounterGroupNameUnique());
            Iterator<Counter> uniqueIterator = uniqueGroup.iterator();
            while(uniqueIterator.hasNext()) {
                Counter next = uniqueIterator.next();
                //LOG.info("unique " + next.getName() + " : " + next.getValue());
                KmerStatistics statistic = new KmerStatistics(next.getName(), next.getValue(), 0);
                statisticsGroup.addStatistics(statistic);
            }
            
            CounterGroup totalGroup = job.getCounters().getGroup(KmerStdDeviationHelper.getCounterGroupNameTotal());
            Iterator<Counter> totalIterator = totalGroup.iterator();
            while(totalIterator.hasNext()) {
                Counter next = totalIterator.next();
                //LOG.info("total " + next.getName() + " : " + next.getValue());
                KmerStatistics statistic = statisticsGroup.getStatistics(next.getName());
                if(statistic != null) {
                    statistic.setTotalOccurance(next.getValue());
                } else {
                    statistic = new KmerStatistics(next.getName(), 0, next.getValue());
                    statisticsGroup.addStatistics(statistic);
                }
            }
            
            return statisticsGroup;
        }
        
        return null;
    }

    private int runKmerStdDeviation(Configuration conf, KmerStatisticsGroup statisticsGroup, Path[] inputIndexFiles, String outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = new Job(conf, "Kmer Standard Deviation");
        conf = job.getConfiguration();
        
        job.setJarByClass(KmerStdDeviation.class);
        
        // Mapper
        job.setMapperClass(KmerStdDeviationMapper.class);
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
        
        // Statistics
        statisticsGroup.saveTo(conf);
        
        // Execute job and return status
        boolean result = job.waitForCompletion(true);
        
        this.jobs.add(job);
        
        // check results
        if(result) {
            CounterGroup uniqueGroup = job.getCounters().getGroup(KmerStdDeviationHelper.getCounterGroupNameDifferential());
            Iterator<Counter> uniqueIterator = uniqueGroup.iterator();
            while(uniqueIterator.hasNext()) {
                Counter next = uniqueIterator.next();
                KmerStatistics statistics = statisticsGroup.getStatistics(next.getName());

                double avg = statistics.getTotalOccurance() / (double)statistics.getUniqueOccurance();
                double diffsum = next.getValue() / (double)1000;
                double distribution = diffsum / statistics.getUniqueOccurance();
                double stddeviation = Math.sqrt(distribution);
                LOG.info("average " + next.getName() + " : " + avg);
                LOG.info("std-deviation " + next.getName() + " : " + stddeviation);
                //LOG.info("diff*diff " + next.getName() + " : " + next.getValue());
                
                KmerStdDeviationWriter writer = new KmerStdDeviationWriter(next.getName(), statistics.getUniqueOccurance(), statistics.getTotalOccurance(), avg, stddeviation);
                Path outputHadoopPath = new Path(outputPath, KmerStdDeviationHelper.makeStdDeviationFileName(next.getName()));
                FileSystem fs = outputHadoopPath.getFileSystem(conf);
                writer.createOutputFile(outputHadoopPath, fs);
            }
        }
        
        return result ? 0 : 1;
    }
}
