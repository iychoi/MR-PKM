package edu.arizona.cs.mrpkm.kmermatch;

import edu.arizona.cs.mrpkm.kmeridx.*;
import edu.arizona.cs.mrpkm.types.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.CompressedSequenceWritable;
import edu.arizona.cs.mrpkm.utils.FileSystemHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author iychoi
 */
public class PairwiseKmerMatchChecker extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(PairwiseKmerMatchChecker.class);
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PairwiseKmerMatchChecker(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        
        String indexPathString = args[0];
        String[] indexPathStrings = FileSystemHelper.splitCommaSeparated(indexPathString);
        Path[] indexPaths = FileSystemHelper.makePathFromString(indexPathStrings);
        // check kmerSize
        int kmerSize = -1;
        for(Path indexPath : indexPaths) {
            if(kmerSize <= 0) {
                kmerSize = KmerIndexHelper.getKmerSize(indexPath);
            } else {
                if(kmerSize != KmerIndexHelper.getKmerSize(indexPath)) {
                    throw new Exception("kmer sizes of given index files are different");
                }
            }
        }
        
        KmerSequenceSlice slice = new KmerSequenceSlice(kmerSize, 1, 0);
        
        KmerLinearMatcher matcher = new KmerLinearMatcher(indexPaths, slice, conf);
        
        LOG.info("Kmer Index Files : " + indexPathString);
        LOG.info("Matches");
        while(matcher.nextMatch()) {
            KmerMatchResult currentMatch = matcher.getCurrentMatch();
            CompressedSequenceWritable key = currentMatch.getKey();
            CompressedIntArrayWritable[] vals = currentMatch.getVals();
            String[][] indice = currentMatch.getIndexPaths();
            
            LOG.info("> " + key.getSequence() + " in " + vals.length + " files");
            StringBuilder sb = new StringBuilder();
            for(String[] indice1 : indice) {
                if(sb.length() != 0) {
                    sb.append(", ");
                }
                sb.append(KmerIndexHelper.getFastaFileName(indice1[0]));
            }
            LOG.info(">> " + sb.toString());
        }
        
        matcher.close();
        return 0;
    }
}