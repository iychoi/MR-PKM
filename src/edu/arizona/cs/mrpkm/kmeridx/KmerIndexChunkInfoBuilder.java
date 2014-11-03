package edu.arizona.cs.mrpkm.kmeridx;

import edu.arizona.cs.mrpkm.cluster.AMRClusterConfiguration;
import edu.arizona.cs.mrpkm.types.hadoop.CompressedSequenceWritable;
import edu.arizona.cs.mrpkm.types.indexchunkinfo.KmerIndexChunkInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author iychoi
 */
public class KmerIndexChunkInfoBuilder extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(KmerIndexChunkInfoBuilder.class);
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new KmerIndexChunkInfoBuilder(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        KmerIndexChunkInfoBuilderCmdParamsParser parser = new KmerIndexChunkInfoBuilderCmdParamsParser();
        KmerIndexChunkInfoBuilderCmdParams cmdParams = parser.parse(args);
        
        String inputPath = cmdParams.getCommaSeparatedInputPath();
        String outputPath = cmdParams.getOutputPath();
        AMRClusterConfiguration clusterConfig = cmdParams.getClusterConfig();
        
        Configuration conf = this.getConf();
        
        // Inputs
        Path[] inputFiles = KmerIndexHelper.getAllKmerIndexFilePaths(conf, inputPath);
        Path[][] indiceGroups = KmerIndexHelper.groupKmerIndice(inputFiles);
        
        for(Path[] indiceGroup : indiceGroups) {
            List<String> keys = new ArrayList<String>();
            for(Path indexFile : indiceGroup) {
                LOG.info("Reading the final key from " + indexFile.toString());
                MapFile.Reader reader = new MapFile.Reader(indexFile.getFileSystem(conf), indexFile.toString(), conf);
                CompressedSequenceWritable finalKey = new CompressedSequenceWritable();
                reader.finalKey(finalKey);
                keys.add(finalKey.getSequence());
                reader.close();
            }
            
            Collections.sort(keys);
            
            String kmerFilename = KmerIndexHelper.getFastaFileName(indiceGroup[0]);
            Path outputFile = new Path(outputPath, KmerIndexChunkInfoHelper.makeKmerIndexChunkInfoFileName(kmerFilename));
            LOG.info("Creating a kmerindex info file : " + outputFile.toString());
            KmerIndexChunkInfo chunkinfo = new KmerIndexChunkInfo();
            chunkinfo.add(keys);
            chunkinfo.saveTo(outputFile, outputFile.getFileSystem(conf));
        }

        return 0;
    }
}
