package edu.arizona.cs.mrpkm.types.statistics;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
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
public class KmerStdDeviationGroup {
    private final static String CONF_STDDEVIATION_GROUP_JSON = "edu.arizona.cs.mrpkm.types.statistics.kmer_stddeviation_group.json";
    
    private final static String JSON_CONF_RECORD_NUM = "itemcounts";
    private final static String JSON_CONF_RECORDS = "items";
    
    private Hashtable<String, KmerStdDeviation> recordCache;
    private List<KmerStdDeviation> recordList;
    
    public KmerStdDeviationGroup() {
        this.recordCache = new Hashtable<String, KmerStdDeviation>();
        this.recordList = new ArrayList<KmerStdDeviation>();
    }
    
    public void add(KmerStdDeviation stddeviation) {
        this.recordCache.put(stddeviation.getStdDeviationName(), stddeviation);
        this.recordList.add(stddeviation);
    }
    
    public KmerStdDeviation[] getAllRecords() {
        return this.recordList.toArray(new KmerStdDeviation[0]);
    }
    
    public KmerStdDeviation getStdDeviation(String stddeviation) {
        return this.recordCache.get(stddeviation);
    }
    
    public int getSize() {
        return this.recordList.size();
    }
    
    public void loadFromJson(String json) {
        this.recordCache.clear();
        this.recordList.clear();
        
        JSONObject jsonobj = new JSONObject(json);
        int records = jsonobj.getInt(JSON_CONF_RECORD_NUM);
        
        JSONArray outputsArray = jsonobj.getJSONArray(JSON_CONF_RECORDS);
        for(int i=0;i<records;i++) {
            JSONObject itemjsonobj = (JSONObject) outputsArray.get(i);
            KmerStdDeviation record = new KmerStdDeviation();
            record.loadFromJsonObject(itemjsonobj);
            
            this.recordList.add(record);
            this.recordCache.put(record.getStdDeviationName(), record);
        }
    }
    
    public String createJson() {
        JSONObject jsonobj = new JSONObject();
        
        jsonobj.put(JSON_CONF_RECORD_NUM, this.recordList.size());
        
        // set array
        JSONArray outputsArray = new JSONArray();
        for(int i=0;i<this.recordList.size();i++) {
            KmerStdDeviation record = this.recordList.get(i);
            outputsArray.put(record.createJsonObject());
        }
        jsonobj.put(JSON_CONF_RECORDS, outputsArray);
        
        return jsonobj.toString();
    }
    
    public void saveTo(Configuration conf) {
        conf.set(CONF_STDDEVIATION_GROUP_JSON, createJson());
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
        loadFromJson(conf.get(CONF_STDDEVIATION_GROUP_JSON));
    }
    
    public void loadFrom(Path file, FileSystem fs) throws IOException {
        DataInputStream reader = fs.open(file);
        
        loadFromJson(Text.readString(reader));
        
        reader.close();
    }
}
