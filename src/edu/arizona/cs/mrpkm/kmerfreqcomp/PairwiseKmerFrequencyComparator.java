package edu.arizona.cs.mrpkm.kmerfreqcomp;

import edu.arizona.cs.hadoop.fs.irods.output.HirodsFileOutputFormat;
import edu.arizona.cs.hadoop.fs.irods.output.HirodsTextOutputFormat;
import edu.arizona.cs.mrpkm.cluster.AMRClusterConfiguration;
import edu.arizona.cs.mrpkm.kmeridx.KmerIndexHelper;
import edu.arizona.cs.mrpkm.report.notification.EmailNotification;
import edu.arizona.cs.mrpkm.report.notification.EmailNotificationException;
import edu.arizona.cs.mrpkm.helpers.FileSystemHelper;
import edu.arizona.cs.mrpkm.helpers.MapReduceHelper;
import edu.arizona.cs.mrpkm.report.Report;
import edu.arizona.cs.mrpkm.types.hadoop.DoubleArrayWritable;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author iychoi
 */
public class PairwiseKmerFrequencyComparator extends Configured implements Tool {
    
    private static final Log LOG = LogFactory.getLog(PairwiseKmerFrequencyComparator.class);
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PairwiseKmerFrequencyComparator(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        PairwiseKmerFrequencyComparatorCmdParamsParser parser = new PairwiseKmerFrequencyComparatorCmdParamsParser();
        PairwiseKmerFrequencyComparatorCmdParams cmdParams = parser.parse(args);
        
        int kmerSize = cmdParams.getKmerSize();
        int nodeSize = cmdParams.getNodes();
        AMRClusterConfiguration clusterConfig = cmdParams.getClusterConfig();
        int partitionNum = cmdParams.getPartitions(nodeSize * clusterConfig.getCoresPerMachine());
        String statisticsPath = cmdParams.getStatisticsPath();
        String histogramPath = cmdParams.getHistogramPath();
        String kmerIndexChunkInfoPath = cmdParams.getKmerIndexChunkInfoPath();
        String inputPath = cmdParams.getCommaSeparatedInputPath();
        String outputPath = cmdParams.getOutputPath();
        
        // configuration
        Configuration conf = this.getConf();
        clusterConfig.configureClusterParamsTo(conf);
        
        Job job = new Job(conf, "Pairwise Kmer Frequency Comparator");
        conf = job.getConfiguration();
        
        job.setJarByClass(PairwiseKmerFrequencyComparator.class);
        
        // Mapper
        job.setMapperClass(PairwiseKmerFrequencyComparatorMapper.class);
        job.setInputFormatClass(KmerFrequencyComparisonInputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleArrayWritable.class);
        
        // Combiner
        job.setCombinerClass(PairwiseKmerFrequencyComparatorCombiner.class);
        
        // Reducer
        job.setReducerClass(PairwiseKmerFrequencyComparatorReducer.class);
        
        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        // Inputs
        Path[] inputFiles = KmerIndexHelper.getAllKmerIndexFilePaths(conf, inputPath);
        KmerFrequencyComparisonInputFormat.addInputPaths(job, FileSystemHelper.makeCommaSeparated(inputFiles));
        
        PairwiseKmerFrequencyComparatorConfig comparatorConfig = new PairwiseKmerFrequencyComparatorConfig();
        Path[][] indiceGroups = KmerIndexHelper.groupKmerIndice(inputFiles);
        for(Path[] indiceGroup : indiceGroups) {
            String fastaFilename = KmerIndexHelper.getFastaFileName(indiceGroup[0]);
            comparatorConfig.addInput(fastaFilename);
        }
        comparatorConfig.setStatisticsPath(statisticsPath);
        comparatorConfig.saveTo(conf);
        
        KmerFrequencyComparisonInputFormatConfig kmerFrequencyCompareInputFormatConfig = new KmerFrequencyComparisonInputFormatConfig();
        kmerFrequencyCompareInputFormatConfig.setKmerSize(kmerSize);
        kmerFrequencyCompareInputFormatConfig.setPartitionNum(partitionNum);
        kmerFrequencyCompareInputFormatConfig.setKmerIndexChunkInfoPath(kmerIndexChunkInfoPath);
        kmerFrequencyCompareInputFormatConfig.setHistogramPath(histogramPath);
        KmerFrequencyComparisonInputFormat.setInputFormatConfig(job, kmerFrequencyCompareInputFormatConfig);
        
        for(Path path : inputFiles) {
            LOG.info("Input : " + path.toString());
            // check kmerSize
            int myKmerSize = KmerIndexHelper.getKmerSize(path);
            if(kmerSize != myKmerSize) {
                throw new Exception("kmer sizes of given index files are different");
            }
        }
        
        boolean hirodsOutputPath = FileSystemHelper.isHirodsFileSystemPath(conf, outputPath);
        if(hirodsOutputPath) {
            LOG.info("Use H-iRODS");
            HirodsFileOutputFormat.setOutputPath(job, new Path(outputPath));
            job.setOutputFormatClass(HirodsTextOutputFormat.class);
        } else {
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            job.setOutputFormatClass(TextOutputFormat.class);
        }
        
        job.setNumReduceTasks(1);
        
        // Execute job and return status
        boolean result = job.waitForCompletion(true);

        // commit results
        if(result) {
            commit(new Path(outputPath), conf);
            
            PairwiseKmerFrequencyComparatorTOC toc = new PairwiseKmerFrequencyComparatorTOC();
            String[] allInput = comparatorConfig.getAllInput();
            for(String input : allInput) {
                toc.addInput(input);
            }
            
            Path TOCfilePath = new Path(outputPath, PairwiseKmerFrequencyComparatorHelper.makePairwiseKmerFrequencyComparisonTOCFileName());
            FileSystem fs = TOCfilePath.getFileSystem(conf);
            toc.saveTo(TOCfilePath, fs);
        }
        
        // report
        if(cmdParams.needReport()) {
            Report report = new Report();
            report.addJob(job);
            report.writeTo(cmdParams.getReportFilename());
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
    
    private void commit(Path outputPath, Configuration conf) throws IOException {
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
                    // rename outputs
                    int mapreduceID = MapReduceHelper.getMapReduceID(entryPath);
                    String newName = PairwiseKmerFrequencyComparatorHelper.makePairwiseKmerFrequencyComparisonFileName(mapreduceID);
                    Path toPath = new Path(entryPath.getParent(), newName);

                    LOG.info("output : " + entryPath.toString());
                    LOG.info("renamed to : " + toPath.toString());
                    fs.rename(entryPath, toPath);
                } else {
                    // let it be
                }
            }
        } else {
            throw new IOException("path not found : " + outputPath.toString());
        }
    }
}
