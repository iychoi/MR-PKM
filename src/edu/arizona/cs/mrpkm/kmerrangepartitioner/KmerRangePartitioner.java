package edu.arizona.cs.mrpkm.kmerrangepartitioner;

import edu.arizona.cs.mrpkm.sampler.KmerSamplerRecord;
import edu.arizona.cs.mrpkm.utils.SequenceHelper;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class KmerRangePartitioner {
    
    private static final Log LOG = LogFactory.getLog(KmerRangePartitioner.class);
    
    private int kmerSize;
    private int numPartitions;

    public enum PartitionerMode {
        MODE_EQUAL_RANGE,
        MODE_EQUAL_ENTRIES,
        MODE_WEIGHTED_RANGE,
        MODE_SAMPLING,
    }

    public KmerRangePartitioner(int kmerSize, int numPartitions) {
        this.kmerSize = kmerSize;
        this.numPartitions = numPartitions;
    }
    
    public KmerRangePartition[] getSamplingPartitions(KmerSamplerRecord[] records, long samples) {
        KmerRangePartition[] partitions = new KmerRangePartition[this.numPartitions];
        
        // calc 4^kmerSize
        String As = "";
        String Ts = "";
        for(int i=0;i<this.kmerSize;i++) {
            As += "A";
            Ts += "T";
        }
        
        long partitionWidth = samples / this.numPartitions;
        long partitionWidthRemain = partitionWidth;
        int partitionIdx = 0;
        int recordIdx = 0;
        long curRecordRemain = records[0].getCount();
        BigInteger nextPartitionBegin = BigInteger.ZERO;
        while(partitionIdx < this.numPartitions) {
            long diff = partitionWidthRemain - curRecordRemain;
            if(diff > 0) {
                partitionWidthRemain -= curRecordRemain;
                recordIdx++;
                if(recordIdx < records.length) {
                    curRecordRemain = records[recordIdx].getCount();
                } else {
                    break;
                }
            } else if(diff == 0) {
                BigInteger partitionBegin = nextPartitionBegin;
                BigInteger partitionEnd = null;
                if(partitionIdx == this.numPartitions - 1) {
                    partitionEnd = SequenceHelper.convertToBigInteger(Ts);
                } else {
                    partitionEnd = SequenceHelper.convertToBigInteger((records[recordIdx].getKey() + Ts).substring(0, this.kmerSize));
                }
                BigInteger partitionSize = partitionEnd.subtract(partitionBegin);
                partitions[partitionIdx] = new KmerRangePartition(this.kmerSize, this.numPartitions, partitionIdx, partitionSize, partitionBegin, partitionEnd);
                
                nextPartitionBegin = partitionEnd.add(BigInteger.ONE);
                partitionIdx++;
                recordIdx++;
                if(recordIdx < records.length) {
                    curRecordRemain = records[recordIdx].getCount();
                } else {
                    break;
                }
                partitionWidthRemain = partitionWidth;
            } else {
                // in between
                BigInteger partitionBegin = nextPartitionBegin;
                BigInteger partitionEnd = null;
                if(partitionIdx == this.numPartitions - 1) {
                    partitionEnd = SequenceHelper.convertToBigInteger(Ts);
                } else {
                    BigInteger recordBegin = SequenceHelper.convertToBigInteger((records[recordIdx].getKey() + As).substring(0, this.kmerSize));
                    BigInteger recordEnd = SequenceHelper.convertToBigInteger((records[recordIdx].getKey() + Ts).substring(0, this.kmerSize));
                    BigInteger recordWidth = recordEnd.subtract(recordBegin);
                    BigInteger curWidth = recordWidth.multiply(BigInteger.valueOf(partitionWidthRemain)).divide(BigInteger.valueOf(records[recordIdx].getCount()));
                    
                    BigInteger bigger = null;
                    if(recordBegin.compareTo(partitionBegin) > 0) {
                        bigger = recordBegin;
                    } else {
                        bigger = partitionBegin;
                    }
                    
                    partitionEnd = bigger.add(curWidth);
                }
                BigInteger partitionSize = partitionEnd.subtract(partitionBegin);
                partitions[partitionIdx] = new KmerRangePartition(this.kmerSize, this.numPartitions, partitionIdx, partitionSize, partitionBegin, partitionEnd);
                
                nextPartitionBegin = partitionEnd.add(BigInteger.ONE);
                partitionIdx++;
                curRecordRemain -= partitionWidthRemain;
                partitionWidthRemain = partitionWidth;
            }
        }
        
        return partitions;
    }
    
    public KmerRangePartition[] getEqualRangePartitions() {
        KmerRangePartition[] partitions = new KmerRangePartition[this.numPartitions];
        
        // calc 4^kmerSize
        BigInteger kmerend = BigInteger.valueOf(4).pow(this.kmerSize);
        
        BigInteger slice_width = kmerend.divide(BigInteger.valueOf(this.numPartitions));
        if(kmerend.mod(BigInteger.valueOf(this.numPartitions)).intValue() != 0) {
            slice_width = slice_width.add(BigInteger.ONE);
        }
        
        for(int i=0;i<this.numPartitions;i++) {
            BigInteger slice_begin = slice_width.multiply(BigInteger.valueOf(i));
            if(slice_begin.add(slice_width).compareTo(kmerend) > 0) {
                slice_width = kmerend.subtract(slice_begin);
            } 

            BigInteger slice_end = slice_begin.add(slice_width).subtract(BigInteger.ONE);

            KmerRangePartition slice = new KmerRangePartition(this.kmerSize, this.numPartitions, i, slice_width, slice_begin, slice_end);
            partitions[i] = slice;
        }
        
        return partitions;
    }
    
    public KmerRangePartition[] getEqualAreaPartitions() {
        KmerRangePartition[] partitions = new KmerRangePartition[this.numPartitions];
        
        // calc 4^kmerSize
        BigInteger kmerend = BigInteger.valueOf(4).pow(this.kmerSize);
        BigDecimal bdkmerend = new BigDecimal(kmerend);
        // moves between x (0~1) y (0~1)
        // sum of area (0.5)
        double kmerArea = 0.5;
        double sliceArea = kmerArea / this.numPartitions;
        
        // we think triangle is horizontally flipped so calc get easier.
        double x1 = 0;
        
        List<BigInteger> widths = new ArrayList<BigInteger>();
        BigInteger widthSum = BigInteger.ZERO;
        for(int i=0;i<this.numPartitions;i++) {
            // x2*x2 = 2*sliceArea + x1*x1
            double temp = (2*sliceArea) + (x1*x1);
            double x2 = Math.sqrt(temp);
            
            BigDecimal bdx1 = BigDecimal.valueOf(x1);
            BigDecimal bdx2 = BigDecimal.valueOf(x2);
            
            // if i increases, bdw will be decreased
            BigDecimal bdw = bdx2.subtract(bdx1);
            
            BigInteger bw = bdw.multiply(bdkmerend).toBigInteger();
            if(bw.compareTo(BigInteger.ZERO) <= 0) {
                bw = BigInteger.ONE;
            }
            
            if(widthSum.add(bw).compareTo(kmerend) > 0) {
                bw = kmerend.subtract(widthSum);
            }
            
            if(i == this.numPartitions - 1) {
                // last case
                if(widthSum.add(bw).compareTo(kmerend) < 0) {
                    bw = kmerend.subtract(widthSum);
                }    
            }
            
            // save it
            widths.add(bw);
            widthSum = widthSum.add(bw);
            
            x1 = x2;
        }
        
        BigInteger cur_begin = BigInteger.ZERO;
        for(int i=0;i<this.numPartitions;i++) {
            BigInteger slice_width = widths.get(this.numPartitions - 1 - i);
            
            BigInteger slice_begin = cur_begin;
            
            if(slice_begin.add(slice_width).compareTo(kmerend) > 0) {
                slice_width = kmerend.subtract(slice_begin);
            }
            
            BigInteger slice_end = cur_begin.add(slice_width).subtract(BigInteger.ONE);
            
            KmerRangePartition slice = new KmerRangePartition(this.kmerSize, this.numPartitions, i, slice_width, slice_begin, slice_end);
            partitions[i] = slice;
            
            cur_begin = cur_begin.add(slice_width);
        }
        
        return partitions;
    }
    
    public KmerRangePartition[] getWeightedRangePartitions() {
        KmerRangePartition[] partitions = new KmerRangePartition[this.numPartitions];
        
        // calc 4^kmerSize
        BigInteger kmerend = BigInteger.valueOf(4).pow(this.kmerSize);
        BigDecimal bdkmerend = new BigDecimal(kmerend);
        
        List<BigDecimal> weights = new ArrayList<BigDecimal>();
        BigDecimal next_weight = BigDecimal.ONE;
        BigDecimal sum_weights = BigDecimal.ZERO;
        for(int i=0;i<this.numPartitions;i++) {
            weights.add(next_weight);
            sum_weights = sum_weights.add(next_weight);
            
            next_weight = next_weight.multiply(BigDecimal.valueOf(1.5));
        }
        
        BigInteger widthSum = BigInteger.ZERO;
        List<BigInteger> widths = new ArrayList<BigInteger>();
        for(int i=0;i<this.numPartitions;i++) {
            BigDecimal bdw = weights.get(i).divide(sum_weights, 50, BigDecimal.ROUND_HALF_UP);
            BigInteger bw = bdw.multiply(bdkmerend).toBigInteger();
            
            if(bw.compareTo(BigInteger.ZERO) <= 0) {
                bw = BigInteger.ONE;
            }
            
            if(widthSum.add(bw).compareTo(kmerend) > 0) {
                bw = kmerend.subtract(widthSum);
            }
            
            if(i == this.numPartitions - 1) {
                // last case
                if(widthSum.add(bw).compareTo(kmerend) < 0) {
                    bw = kmerend.subtract(widthSum);
                }    
            }
            
            // save it
            widths.add(bw);
            widthSum = widthSum.add(bw);
        }
        
        BigInteger cur_begin = BigInteger.ZERO;
        for(int i=0;i<this.numPartitions;i++) {
            BigInteger slice_width = widths.get(i);
            
            BigInteger slice_begin = cur_begin;
            
            if(slice_begin.add(slice_width).compareTo(kmerend) > 0) {
                slice_width = kmerend.subtract(slice_begin);
            }
            
            BigInteger slice_end = cur_begin.add(slice_width).subtract(BigInteger.ONE);
            
            KmerRangePartition slice = new KmerRangePartition(this.kmerSize, this.numPartitions, i, slice_width, slice_begin, slice_end);
            partitions[i] = slice;
            
            cur_begin = cur_begin.add(slice_width);
        }
        
        return partitions;
    }
}
