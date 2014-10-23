package edu.arizona.cs.mrpkm.sampler;

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
public class KmerSampler {
    
    private static final int SAMPLING_CHARS = 6;
    
    private String sampleName;
    private int kmerSize;
    
    private Hashtable<String, KmerSamplerRecord> samples;
    private List<String> sampleKeyList;
    private long sumCounts;
    
    public KmerSampler(String sampleName, int kmerSize) {
        this.sampleName = sampleName;
        
        this.kmerSize = kmerSize;
        this.samples = new Hashtable<String, KmerSamplerRecord>();
        this.sampleKeyList = new ArrayList<String>();
        this.sumCounts = 0;
    }
    
    public String getSampleName() {
        return this.sampleName;
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
        KmerSamplerRecord existRecord = this.samples.get(key);
        if(existRecord == null) {
            this.samples.put(key, new KmerSamplerRecord(key, 1));
            this.sampleKeyList.add(key);
        } else {
            existRecord.increaseCount();
        }
        
        this.sumCounts++;
    }

    public void createSamplingFile(Path file, FileSystem fs) throws IOException {
        DataOutputStream writer = fs.create(file, true, 64 * 1024);
        
        Collections.sort(this.sampleKeyList);
        new IntWritable(this.sampleKeyList.size()).write(writer);
        
        for(String key : this.sampleKeyList) {
            long keyCnt = this.samples.get(key).getCount();
            new Text(key).write(writer);
            new LongWritable(keyCnt).write(writer);
        }
        
        writer.close();
    }

    public long getSampleCount() {
        return this.sumCounts;
    }
}
