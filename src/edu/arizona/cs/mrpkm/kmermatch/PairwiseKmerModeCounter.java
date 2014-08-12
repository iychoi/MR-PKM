package edu.arizona.cs.mrpkm.kmermatch;

import edu.arizona.cs.mrpkm.cluster.MRClusterConfigurationBase;
import edu.arizona.cs.mrpkm.commandline.ArgumentParseException;
import edu.arizona.cs.mrpkm.commandline.ArgumentParserBase;
import edu.arizona.cs.mrpkm.commandline.ClusterConfigurationArgumentParser;
import edu.arizona.cs.mrpkm.commandline.CommandLineArgumentParser;
import edu.arizona.cs.mrpkm.commandline.HelpArgumentParser;
import edu.arizona.cs.mrpkm.commandline.MatchHitMaxFilterArgumentParser;
import edu.arizona.cs.mrpkm.commandline.MatchHitMinFilterArgumentParser;
import edu.arizona.cs.mrpkm.commandline.MultiPathArgumentParser;
import edu.arizona.cs.mrpkm.commandline.NodeSizeArgumentParser;
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
        Configuration conf = this.getConf();
        
        int kmerSize = 0;
        int nodeSize = 0;
        int matchFilterMin = 0;
        int matchFilterMax = 0;
        String inputPath = null;
        String outputPath = null;
        MRClusterConfigurationBase clusterConfig = null;
        
        // parse command line
        HelpArgumentParser helpParser = new HelpArgumentParser();
        ClusterConfigurationArgumentParser clusterParser = new ClusterConfigurationArgumentParser();
        NodeSizeArgumentParser nodeSizeParser = new NodeSizeArgumentParser();
        MatchHitMinFilterArgumentParser minFilterParser = new MatchHitMinFilterArgumentParser();
        MatchHitMaxFilterArgumentParser maxFilterParser = new MatchHitMaxFilterArgumentParser();
        MultiPathArgumentParser pathParser = new MultiPathArgumentParser(2);
        
        CommandLineArgumentParser parser = new CommandLineArgumentParser();
        parser.addArgumentParser(helpParser);
        parser.addArgumentParser(clusterParser);
        parser.addArgumentParser(nodeSizeParser);
        parser.addArgumentParser(minFilterParser);
        parser.addArgumentParser(maxFilterParser);
        parser.addArgumentParser(pathParser);
        ArgumentParserBase[] parsers = null;
        try {
            parsers = parser.parse(args);
        } catch(ArgumentParseException ex) {
            System.err.println(ex);
            return -1;
        }
        
        for(ArgumentParserBase base : parsers) {
            if(base == helpParser) {
                if(helpParser.getValue()) {
                    printHelp(parser);
                    return 0;
                }
            } else if(base == clusterParser) {
                clusterConfig = clusterParser.getValue();
            } else if(base == minFilterParser) {
                matchFilterMin = minFilterParser.getValue();
            } else if(base == maxFilterParser) {
                matchFilterMax = maxFilterParser.getValue();
            } else if(base == nodeSizeParser) {
                nodeSize = nodeSizeParser.getValue();
            } else if(base == pathParser) {
                String[] paths = pathParser.getValue();
                if (paths.length == 2) {
                    inputPath = paths[0];
                    outputPath = paths[1];
                } else if (paths.length >= 3) {
                    inputPath = "";
                    for (int i = 0; i < paths.length - 2; i++) {
                        if (!inputPath.equals("")) {
                            inputPath += ",";
                        }
                        inputPath += paths[i];
                    }
                    outputPath = paths[paths.length - 1];
                }
            }
        }
        
        // configuration
        clusterConfig.setConfiguration(conf);
        
        conf.setInt(PairwiseKmerModeCounterHelper.getConfigurationKeyOfMatchFilterMin(), matchFilterMin);
        conf.setInt(PairwiseKmerModeCounterHelper.getConfigurationKeyOfMatchFilterMax(), matchFilterMax);

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
        String[] paths = FileSystemHelper.splitCommaSeparated(inputPath);
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

    private void printHelp(CommandLineArgumentParser parser) {
        System.out.println(parser.getHelpMessage());
    }
}
