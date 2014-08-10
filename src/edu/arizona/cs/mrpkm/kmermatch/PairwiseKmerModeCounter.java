package edu.arizona.cs.mrpkm.kmermatch;

import edu.arizona.cs.mrpkm.cluster.MRClusterConfiguration;
import edu.arizona.cs.mrpkm.kmeridx.KmerIndexHelper;
import edu.arizona.cs.mrpkm.types.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.MultiFileReadIDWritable;
import edu.arizona.cs.mrpkm.types.NamedOutput;
import edu.arizona.cs.mrpkm.types.NamedOutputs;
import edu.arizona.cs.mrpkm.utils.FileSystemHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
        String clusterConfiguration = null;
        String kmerIndexPath = null;
        String outputPath = null;
        int kmerSize = 0;
        int nodeSize = 0;
        
        if(args.length == 3) {
            clusterConfiguration = "default";
            nodeSize = Integer.parseInt(args[0]);
            kmerIndexPath = args[1];
            outputPath = args[2];
        } else if(args.length >= 4) {
            clusterConfiguration = args[0];
            nodeSize = Integer.parseInt(args[1]);
            kmerIndexPath = "";
            for(int i=2;i<args.length-1;i++) {
                if(!kmerIndexPath.equals("")) {
                    kmerIndexPath += ",";
                }
                kmerIndexPath += args[i];
            }
            outputPath = args[args.length - 1];
        } else {
            throw new Exception("Argument is not properly given");
        }
        
        Configuration conf = this.getConf();
        
        // configuration
        MRClusterConfiguration clusterConfig = MRClusterConfiguration.findConfiguration(clusterConfiguration);
        clusterConfig.setConfiguration(conf);

        Job job = new Job(conf, "Pairwise Kmer Mode Counter");
        job.setJarByClass(PairwiseKmerModeCounter.class);

        // Identity Mapper & Reducer
        job.setMapperClass(PairwiseKmerModeCounterMapper.class);
        job.setCombinerClass(PairwiseKmerModeCounterCombiner.class);
        job.setPartitionerClass(PairwiseKmerModeCounterPartitioner.class);
        job.setReducerClass(PairwiseKmerModeCounterReducer.class);
        
        job.setMapOutputKeyClass(MultiFileReadIDWritable.class);
        job.setMapOutputValueClass(CompressedIntArrayWritable.class);
        
        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        // Inputs
        String[] paths = FileSystemHelper.splitCommaSeparated(kmerIndexPath);
        Path[] inputFiles = FileSystemHelper.getAllKmerIndexFilePaths(conf, paths);
        
        for(Path path : inputFiles) {
            LOG.info("Input : " + path);
        }
        
        // check kmerSize
        kmerSize = -1;
        for(Path indexPath : inputFiles) {
            if(kmerSize <= 0) {
                kmerSize = KmerIndexHelper.getKmerSize(indexPath);
            } else {
                if(kmerSize != KmerIndexHelper.getKmerSize(indexPath)) {
                    throw new Exception("kmer sizes of given index files are different");
                }
            }
        }
        
        if(kmerSize <= 0) {
            throw new Exception("kmer size is not properly set");
        }
        
        KmerMatchInputFormat.addInputPaths(job, FileSystemHelper.makeCommaSeparated(inputFiles));
        KmerMatchInputFormat.setKmerSize(job, kmerSize);
        KmerMatchInputFormat.setSliceNum(job, nodeSize * 100);
        
        NamedOutputs namedOutputs = new NamedOutputs();
        Path[][] groups = KmerIndexHelper.groupKmerIndice(inputFiles);
        LOG.info("Input index groups : " + groups.length);
        for(int i=0;i<groups.length;i++) {
            Path[] group = groups[i];
            LOG.info("Input index group " + i + " : " + group.length);
            for(int j=0;j<group.length;j++) {
                LOG.info("> " + group[j].toString());
            }
        }
        
        for(int i=0;i<groups.length;i++) {
            Path[] thisGroup = groups[i];
            for(int j=0;j<groups.length;j++) {
                Path[] thatGroup = groups[j];
                if(i != j) {
                    namedOutputs.addNamedOutput(PairwiseKmerModeCounterHelper.getPairwiseModeCounterOutputName(KmerIndexHelper.getFastaFileName(thisGroup[0]), KmerIndexHelper.getFastaFileName(thatGroup[0])));
                }
            }
        }
        
        job.setInputFormatClass(KmerMatchInputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        int id = 0;
        for(NamedOutput namedOutput : namedOutputs.getAllNamedOutput()) {
            LOG.info("regist new named output : " + namedOutput.getNamedOutputString());

            job.getConfiguration().setStrings(PairwiseKmerModeCounterHelper.getConfigurationKeyOfNamedOutputName(id), namedOutput.getNamedOutputString());
            LOG.info("regist new ConfigString : " + PairwiseKmerModeCounterHelper.getConfigurationKeyOfNamedOutputName(id));
            
            job.getConfiguration().setInt(PairwiseKmerModeCounterHelper.getConfigurationKeyOfNamedOutputID(namedOutput.getInputString()), id);
            LOG.info("regist new ConfigString : " + PairwiseKmerModeCounterHelper.getConfigurationKeyOfNamedOutputID(namedOutput.getInputString()));
            
            MultipleOutputs.addNamedOutput(job, namedOutput.getNamedOutputString(), TextOutputFormat.class, Text.class, Text.class);
            id++;
        }
        
        job.setNumReduceTasks(clusterConfig.getReducerNumber(nodeSize));
        
        // Execute job and return status
        boolean result = job.waitForCompletion(true);

        // commit results
        //commit(new Path(outputPath), conf, namedOutputs, kmerSize);
        
        return result ? 0 : 1;
    }
    /*
    private void commit(Path outputPath, Configuration conf, NamedOutputs namedOutputs, int kmerSize) throws IOException {
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
                        int reduceID = MapReduceHelper.getReduceID(entryPath);
                        Path toPath = new Path(entryPath.getParent(), namedOutput.getInputString().getName() + "." + kmerSize + "." + KmerIndexConstants.NAMED_OUTPUT_NAME_SUFFIX + "." + reduceID);
                        
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
    */
}
