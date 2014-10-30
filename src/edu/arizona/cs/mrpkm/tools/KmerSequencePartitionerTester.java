package edu.arizona.cs.mrpkm.tools;

import edu.arizona.cs.mrpkm.kmerrangepartitioner.KmerRangePartition;
import edu.arizona.cs.mrpkm.kmerrangepartitioner.KmerRangePartitioner;
import edu.arizona.cs.mrpkm.kmerrangepartitioner.KmerRangePartitioner.PartitionerMode;
import edu.arizona.cs.mrpkm.histogram.KmerHistogramReader;
import edu.arizona.cs.mrpkm.histogram.KmerHistogramRecord;
import edu.arizona.cs.mrpkm.utils.SequenceHelper;
import java.math.BigInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
        int partitionerMode = Integer.parseInt(args[2]);
        
        KmerRangePartitioner.PartitionerMode mode = KmerRangePartitioner.PartitionerMode.values()[partitionerMode];
        System.out.println("Partitioner Mode : " + mode.toString());
        
        KmerRangePartitioner partitioner = new KmerRangePartitioner(kmerSize, numPartitions);
        KmerRangePartition[] partitions = null;
        if(mode.equals(PartitionerMode.MODE_EQUAL_ENTRIES)) {
            partitions = partitioner.getEqualAreaPartitions();
        } else if(mode.equals(PartitionerMode.MODE_EQUAL_RANGE)) {
            partitions = partitioner.getEqualRangePartitions();
        } else if(mode.equals(PartitionerMode.MODE_WEIGHTED_RANGE)) {
            partitions = partitioner.getWeightedRangePartitions();
        } else if(mode.equals(PartitionerMode.MODE_HISTOGRAM)) {
            String histogramPath = args[3];
            KmerHistogramReader reader = new KmerHistogramReader(new Path(histogramPath), conf);
            System.out.println("total : " + reader.getSampleCount());
            long partitionWidth = reader.getSampleCount() / numPartitions;
            System.out.println("partitionWidth : " + partitionWidth);
            long partitionRem = partitionWidth;
            for(KmerHistogramRecord rec : reader.getRecords()) {
                if(partitionRem - rec.getCount() <= 0) {
                    System.out.println("rec : " + rec.getKey() + " : " + rec.getCount() + " -- part : " + partitionRem);
                    partitionRem = partitionWidth - (rec.getCount() - partitionRem);
                } else {
                    System.out.println("rec : " + rec.getKey() + " : " + rec.getCount());
                    partitionRem -= rec.getCount();
                }
            }
            partitions = partitioner.getHistogramPartitions(reader.getRecords(), reader.getSampleCount());
        }
        
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
