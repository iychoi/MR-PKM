package edu.arizona.cs.mrpkm.tools;

import edu.arizona.cs.mrpkm.types.kmerrangepartition.KmerRangePartition;
import edu.arizona.cs.mrpkm.types.kmerrangepartition.KmerRangePartitioner;
import edu.arizona.cs.mrpkm.types.histogram.KmerHistogram;
import edu.arizona.cs.mrpkm.types.histogram.KmerHistogramRecord;
import edu.arizona.cs.mrpkm.helpers.SequenceHelper;
import java.math.BigInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author iychoi
 */
public class KmerSequencePartitionerTester extends Configured implements Tool {
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new KmerSequencePartitionerTester(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        
        int kmerSize = Integer.parseInt(args[0]);
        int numPartitions = Integer.parseInt(args[1]);
        
        KmerRangePartitioner partitioner = new KmerRangePartitioner(kmerSize, numPartitions);
        KmerRangePartition[] partitions = null;
        String histogramPathString = args[3];
        Path histogramPath = new Path(histogramPathString);
        FileSystem fs = histogramPath.getFileSystem(conf);

        KmerHistogram histogram = new KmerHistogram();
        histogram.loadFrom(histogramPath, fs);
        System.out.println("total : " + histogram.getKmerCount());
        long partitionWidth = histogram.getKmerCount() / numPartitions;
        System.out.println("partitionWidth : " + partitionWidth);
        long partitionRem = partitionWidth;

        KmerHistogramRecord[] records = histogram.getSortedRecords();
        for(KmerHistogramRecord rec : records) {
            if(partitionRem - rec.getCount() <= 0) {
                System.out.println("rec : " + rec.getKmer() + " : " + rec.getCount() + " -- part : " + partitionRem);
                partitionRem = partitionWidth - (rec.getCount() - partitionRem);
            } else {
                System.out.println("rec : " + rec.getKmer() + " : " + rec.getCount());
                partitionRem -= rec.getCount();
            }
        }
        partitions = partitioner.getHistogramPartitions(records, histogram.getKmerCount());
        
        BigInteger lastEnd = BigInteger.ZERO;
        System.out.println(partitions.length);
        for(KmerRangePartition partition : partitions) {
            System.out.println("partition start");
            if(lastEnd.compareTo(BigInteger.ZERO) == 0) {
                // skip
            } else {
                if(lastEnd.compareTo(partition.getPartitionBegin().subtract(BigInteger.ONE)) != 0) {
                    System.err.println("Error! lastend and begin not matching");
                    return -1;
                } else {
                    System.out.println("prev : " + SequenceHelper.convertToString(lastEnd, kmerSize));
                }
            }
            System.out.println(partition.toString());
            lastEnd = partition.getPartitionEnd();
        }
        
        String kmerLast = "";
        for(int i=0;i<kmerSize;i++) {
            kmerLast += "T";
        }
        
        if(lastEnd.compareTo(SequenceHelper.convertToBigInteger(kmerLast)) != 0) {
            System.err.println("Error! range end is not real end");
            return -1;
        }
        return 0;
    }
}
