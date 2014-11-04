package edu.arizona.cs.mrpkm.kmermatch;

import edu.arizona.cs.mrpkm.types.PairwiseKmerMatchRecord;
import edu.arizona.cs.mrpkm.types.PairwiseKmerMatchRecordColumn;
import edu.arizona.cs.mrpkm.types.namedoutputs.NamedOutputs;
import edu.arizona.cs.mrpkm.types.hadoop.MultiFileReadIDWritable;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author iychoi
 */
public class ModeCounterMapper extends Mapper<LongWritable, Text, MultiFileReadIDWritable, IntWritable> {
    
    private static final Log LOG = LogFactory.getLog(ModeCounterMapper.class);
    
    private NamedOutputs namedOutputs = null;
    private ModeCounterConfig modeCounterConfig = null;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        this.namedOutputs = new NamedOutputs();
        this.namedOutputs.loadFrom(conf);
        
        this.modeCounterConfig = new ModeCounterConfig();
        this.modeCounterConfig.loadFrom(conf);
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        PairwiseKmerMatchRecord record = new PairwiseKmerMatchRecord(line);
        
        if(record.getColumnNum() <= 1) {
            throw new IOException("Number of pairwise match result must be larger than 1");
        }
        
        PairwiseKmerMatchRecordColumn masterColumn = null;
        PairwiseKmerMatchRecordColumn[] columns = record.getColumns();
        for(PairwiseKmerMatchRecordColumn column : columns) {
            if(column.getFileID() == this.modeCounterConfig.getMasterFileID()) {
                masterColumn = column;
                break;
            }
        }
        
        if(masterColumn != null) {
            int[] count_pos_vals = new int[columns.length];
            int[] count_neg_vals = new int[columns.length];

            for(int i=0;i<columns.length;i++) {
                int[] readIDs = columns[i].getReadIDs();
                int pos = 0;
                int neg = 0;
                for(int j=0;j<readIDs.length;j++) {
                    if(readIDs[j] >= 0) {
                        pos++;
                    } else {
                        neg++;
                    }
                }
                
                count_pos_vals[i] = pos;
                count_neg_vals[i] = neg;
            }

            int[] masterReadIDs = masterColumn.getReadIDs();
            
            for(int i=0;i<masterReadIDs.length;i++) {
                int readID = masterReadIDs[i];
                
                for(int j=0;j<columns.length;j++) {
                    if(columns[j].getFileID() != masterColumn.getFileID()) {
                        int fileID = columns[j].getFileID();
                        if(readID >= 0) {
                            // pos
                            if(count_pos_vals[j] > 0) {
                                // forward match
                                context.write(new MultiFileReadIDWritable(fileID, readID), new IntWritable(count_pos_vals[j]));
                            }

                            if(count_neg_vals[j] > 0) {
                                // reverse match
                                context.write(new MultiFileReadIDWritable(fileID, readID), new IntWritable(-1 * count_neg_vals[j]));
                            }
                        } else {
                            // neg
                            readID *= -1;
                            if(count_pos_vals[j] > 0) {
                                // reverse match
                                context.write(new MultiFileReadIDWritable(fileID, readID), new IntWritable(-1 * count_pos_vals[j]));
                            }

                            if(count_neg_vals[j] > 0) {
                                // forward match
                                context.write(new MultiFileReadIDWritable(fileID, readID), new IntWritable(count_neg_vals[j]));
                            }
                        }
                    }
                }
            }
        }
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        this.namedOutputs = null;
        this.modeCounterConfig = null;
    }
}
