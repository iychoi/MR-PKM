package edu.arizona.cs.mrpkm.histogram;

import edu.arizona.cs.mrpkm.utils.SequenceHelper;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 *
 * @author iychoi
 */
public class KmerHistogramWriter {
    
    private static final int SAMPLING_CHARS = 6;
    
    private String inputName;
    private int kmerSize;
    
    private Hashtable<String, KmerHistogramRecord> records;
    private List<String> recordKeyList;
    private long sumCounts;
    
    public KmerHistogramWriter(String inputName, int kmerSize) {
        this.inputName = inputName;
        
        this.kmerSize = kmerSize;
        this.records = new Hashtable<String, KmerHistogramRecord>();
        this.recordKeyList = new ArrayList<String>();
        this.sumCounts = 0;
    }
    
    public String getInputName() {
        return this.inputName;
    }
    
    public void takeSample(String sequence) {
        for (int i = 0; i < (sequence.length() - this.kmerSize + 1); i++) {
            String kmer = sequence.substring(i, i + this.kmerSize);
            String rkmer = SequenceHelper.getReverseCompliment(kmer);
            
            kmer = kmer.substring(0, SAMPLING_CHARS);
            rkmer = rkmer.substring(0, SAMPLING_CHARS);
            
            String smallkmer = kmer;
            if(rkmer.compareTo(kmer) < 0) {
                smallkmer = rkmer;
            }

            addKey(smallkmer);
        }
    }
    
    private void addKey(String key) {
        KmerHistogramRecord existRecord = this.records.get(key);
        if(existRecord == null) {
            this.records.put(key, new KmerHistogramRecord(key, 1));
            this.recordKeyList.add(key);
        } else {
            existRecord.increaseCount();
        }
        
        this.sumCounts++;
    }

    public void createHistogramFile(Path file, FileSystem fs) throws IOException {
        if(!fs.exists(file.getParent())) {
            fs.mkdirs(file.getParent());
        }
        
        DataOutputStream writer = fs.create(file, true, 64 * 1024);
        
        Collections.sort(this.recordKeyList);
        new IntWritable(this.recordKeyList.size()).write(writer);
        
        for(String key : this.recordKeyList) {
            long keyCnt = this.records.get(key).getCount();
            new Text(key).write(writer);
            new LongWritable(keyCnt).write(writer);
        }
        
        writer.close();
    }

    public long getSampleCount() {
        return this.sumCounts;
    }
}
