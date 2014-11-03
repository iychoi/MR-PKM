package edu.arizona.cs.mrpkm.types.histogram;

import edu.arizona.cs.mrpkm.helpers.SequenceHelper;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author iychoi
 */
public class KmerHistogram {
    private final static String CONF_HISTOGRAM_JSON = "edu.arizona.cs.mrpkm.types.histogram.kmer_histogram.json";
    
    private static final int SAMPLING_CHARS = 6;
    
    private final static String JSON_CONF_NAME = "name";
    private final static String JSON_CONF_KMER_SIZE = "kmer_size";
    private final static String JSON_CONF_RECORD_NUM = "itemcounts";
    private final static String JSON_CONF_RECORDS = "items";
    private final static String JSON_CONF_RECORD_KMER = "kmer";
    private final static String JSON_CONF_RECORD_COUNT = "count";
    
    private String histogramName;
    private int kmerSize;
    
    private Hashtable<String, KmerHistogramRecord> recordCache;
    private List<KmerHistogramRecord> recordList;
    private long kmerCount;
    
    public KmerHistogram() {
        this.recordCache = new Hashtable<String, KmerHistogramRecord>();
        this.recordList = new ArrayList<KmerHistogramRecord>();
        this.kmerCount = 0;
    }
    
    public KmerHistogram(String histogramName, int kmerSize) {
        this.histogramName = histogramName;
        this.kmerSize = kmerSize;
        
        this.recordCache = new Hashtable<String, KmerHistogramRecord>();
        this.recordList = new ArrayList<KmerHistogramRecord>();
        this.kmerCount = 0;
    }
    
    public void setHistogramName(String histogramName) {
        this.histogramName = histogramName;
    }
    
    public String getHistogramName() {
        return this.histogramName;
    }
    
    public void setKmerSize(int kmerSize) {
        this.kmerSize = kmerSize;
    }
    
    public int getKmerSize() {
        return this.kmerSize;
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

            add(smallkmer);
        }
    }
    
    private void add(String kmer) {
        KmerHistogramRecord record = this.recordCache.get(kmer);
        if(record == null) {
            record = new KmerHistogramRecord(kmer, 1);
            this.recordCache.put(kmer, record);
            this.recordList.add(record);
        } else {
            record.increaseCount();
        }
        
        this.kmerCount++;
    }
    
    public long getKmerCount() {
        return this.kmerCount;
    }
    
    public KmerHistogramRecord[] getAllRecords() {
        return this.recordList.toArray(new KmerHistogramRecord[0]);
    }
    
    public KmerHistogramRecord[] getSortedRecords() {
        List<KmerHistogramRecord> sortedRecords = new ArrayList<KmerHistogramRecord>();
        for(KmerHistogramRecord record : this.recordList) {
            sortedRecords.add(record);
        }
        Collections.sort(sortedRecords, new KmerHistogramRecordComparator());
        return sortedRecords.toArray(new KmerHistogramRecord[0]);
    }
    
    public int getSize() {
        return this.recordList.size();
    }
    
    public void loadFromJson(String json) {
        this.recordCache.clear();
        this.recordList.clear();
        
        JSONObject jsonobj = new JSONObject(json);
        this.histogramName = jsonobj.getString(JSON_CONF_NAME);
        this.kmerSize = jsonobj.getInt(JSON_CONF_KMER_SIZE);
        int records = jsonobj.getInt(JSON_CONF_RECORD_NUM);
        this.kmerCount = 0;
        
        JSONArray outputsArray = jsonobj.getJSONArray(JSON_CONF_RECORDS);
        for(int i=0;i<records;i++) {
            JSONObject itemjsonobj = (JSONObject) outputsArray.get(i);
            String kmer = itemjsonobj.getString(JSON_CONF_RECORD_KMER);
            long count = itemjsonobj.getLong(JSON_CONF_RECORD_COUNT);
            KmerHistogramRecord record = new KmerHistogramRecord(kmer, count);
            this.recordList.add(record);
            this.recordCache.put(kmer, record);
            this.kmerCount += count;
        }
    }
    
    public String createJson() {
        JSONObject jsonobj = new JSONObject();
        
        jsonobj.put(JSON_CONF_NAME, this.histogramName);
        jsonobj.put(JSON_CONF_KMER_SIZE, this.kmerSize);
        jsonobj.put(JSON_CONF_RECORD_NUM, this.recordList.size());
        
        // set array
        JSONArray outputsArray = new JSONArray();
        for(int i=0;i<this.recordList.size();i++) {
            KmerHistogramRecord record = this.recordList.get(i);
            JSONObject itemjsonobj = new JSONObject();
            itemjsonobj.put(JSON_CONF_RECORD_KMER, record.getKmer());
            itemjsonobj.put(JSON_CONF_RECORD_COUNT, record.getCount());
            outputsArray.put(itemjsonobj);
        }
        jsonobj.put(JSON_CONF_RECORDS, outputsArray);
        
        return jsonobj.toString();
    }
    
    public void saveTo(Configuration conf) {
        conf.set(CONF_HISTOGRAM_JSON, createJson());
    }
    
    public void saveTo(Path file, FileSystem fs) throws IOException {
        if(!fs.exists(file.getParent())) {
            fs.mkdirs(file.getParent());
        }
        
        DataOutputStream writer = fs.create(file, true, 64 * 1024);
        new Text(createJson()).write(writer);
        writer.close();
    }
    
    public void loadFrom(Configuration conf) {
        loadFromJson(conf.get(CONF_HISTOGRAM_JSON));
    }
    
    public void loadFrom(Path file, FileSystem fs) throws IOException {
        DataInputStream reader = fs.open(file);
        
        loadFromJson(Text.readString(reader));
        
        reader.close();
    }
}
