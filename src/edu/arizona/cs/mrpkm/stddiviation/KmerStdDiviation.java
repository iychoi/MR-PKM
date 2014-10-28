package edu.arizona.cs.mrpkm.stddiviation;

import edu.arizona.cs.mrpkm.cluster.AMRClusterConfiguration;
import edu.arizona.cs.mrpkm.utils.FileSystemHelper;
import java.io.IOException;
import java.util.Iterator;
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
public class KmerStdDiviation extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(KmerStdDiviation.class);
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new KmerStdDiviation(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        KmerStdDiviationCmdParamsParser parser = new KmerStdDiviationCmdParamsParser();
        KmerStdDiviationCmdParams cmdParams = parser.parse(args);
        
        int kmerSize = cmdParams.getKmerSize();
        String inputPath = cmdParams.getCommaSeparatedInputPath();
        String outputPath = cmdParams.getOutputPath();
        AMRClusterConfiguration clusterConfig = cmdParams.getClusterConfig();
        
        // configuration
        Configuration conf = this.getConf();
        clusterConfig.configureClusterParamsTo(conf);
        
        int result = 1;
        KmerStatisticsGroup statisticsGroup = runKmerStatistics(conf, inputPath);
        if(statisticsGroup != null) {
            result = runKmerStdDiviation(conf, statisticsGroup, inputPath, outputPath);
        }
        
        return result; 
    }
    
    private KmerStatisticsGroup runKmerStatistics(Configuration conf, String inputPath) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = new Job(conf, "Kmer Statistics");
        conf = job.getConfiguration();
        
        job.setJarByClass(KmerStdDiviation.class);
        
        // Mapper
        job.setMapperClass(KmerStatisticsMapper.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        
        // Specify key / value
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        
        // Inputs
        String[] paths = FileSystemHelper.splitCommaSeparated(inputPath);
        Path[] inputFiles = FileSystemHelper.getAllKmerIndexDataFilePaths(conf, paths);
        
        SequenceFileInputFormat.addInputPaths(job, FileSystemHelper.makeCommaSeparated(inputFiles));
        
        // Outputs
        job.setOutputFormatClass(NullOutputFormat.class);
        
        job.setNumReduceTasks(0);
        
        // Execute job and return status
        boolean result = job.waitForCompletion(true);
        
        // check results
        if(result) {
            KmerStatisticsGroup statisticsGroup = new KmerStatisticsGroup();
            
            CounterGroup uniqueGroup = job.getCounters().getGroup(KmerStdDiviationHelper.getCounterGroupNameUnique());
            Iterator<Counter> uniqueIterator = uniqueGroup.iterator();
            while(uniqueIterator.hasNext()) {
                Counter next = uniqueIterator.next();
                //LOG.info("unique " + next.getName() + " : " + next.getValue());
                KmerStatistics statistic = new KmerStatistics(next.getName(), next.getValue(), 0);
                statisticsGroup.addStatistics(statistic);
            }
            
            CounterGroup totalGroup = job.getCounters().getGroup(KmerStdDiviationHelper.getCounterGroupNameTotal());
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

    private int runKmerStdDiviation(Configuration conf, KmerStatisticsGroup statisticsGroup, String inputPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = new Job(conf, "Kmer Standard Diviation");
        conf = job.getConfiguration();
        
        job.setJarByClass(KmerStdDiviation.class);
        
        // Mapper
        job.setMapperClass(KmerStdDiviationMapper.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        
        // Specify key / value
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        
        // Inputs
        String[] paths = FileSystemHelper.splitCommaSeparated(inputPath);
        Path[] inputFiles = FileSystemHelper.getAllKmerIndexDataFilePaths(conf, paths);
        
        SequenceFileInputFormat.addInputPaths(job, FileSystemHelper.makeCommaSeparated(inputFiles));
        
        // Outputs
        job.setOutputFormatClass(NullOutputFormat.class);
        
        job.setNumReduceTasks(0);
        
        // Statistics
        statisticsGroup.saveTo(conf);
        
        // Execute job and return status
        boolean result = job.waitForCompletion(true);
        
        // check results
        if(result) {
            CounterGroup uniqueGroup = job.getCounters().getGroup(KmerStdDiviationHelper.getCounterGroupNameDifferential());
            Iterator<Counter> uniqueIterator = uniqueGroup.iterator();
            while(uniqueIterator.hasNext()) {
                Counter next = uniqueIterator.next();
                KmerStatistics statistics = statisticsGroup.getStatistics(next.getName());

                double avg = statistics.getTotalOccurance() / (double)statistics.getUniqueOccurance();
                double diffsum = next.getValue() / (double)1000;
                double distribution = diffsum / statistics.getUniqueOccurance();
                double stddiviation = Math.sqrt(distribution);
                LOG.info("std-diviation " + next.getName() + " : " + stddiviation);
                //LOG.info("diff*diff " + next.getName() + " : " + next.getValue());
                
                KmerStdDiviationWriter writer = new KmerStdDiviationWriter(next.getName(), statistics.getUniqueOccurance(), statistics.getTotalOccurance(), avg, stddiviation);
                Path outputHadoopPath = new Path(outputPath, KmerStdDiviationHelper.makeStdDiviationFileName(next.getName()));
                FileSystem fs = outputHadoopPath.getFileSystem(conf);
                writer.createOutputFile(outputHadoopPath, fs);
            }
        }
        
        return result ? 0 : 1;
    }
}
