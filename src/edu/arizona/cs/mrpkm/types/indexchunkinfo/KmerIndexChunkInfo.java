package edu.arizona.cs.mrpkm.types.indexchunkinfo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
public class KmerIndexChunkInfo {
    private final static String CONF_KMER_INDEX_CHUNK_INFO_JSON = "edu.arizona.cs.mrpkm.types.indexchunkinfo.kmer_index_chunkinfo.json";
    
    private final static String JSON_CONF_RECORD_NUM = "itemcounts";
    private final static String JSON_CONF_RECORDS = "items";
    private final static String JSON_CONF_RECORD_LASTKEY = "lastkey";
    
    private List<String> lastKeyList;
    
    public KmerIndexChunkInfo() {
        this.lastKeyList = new ArrayList<String>();
    }
    
    public void add(String key) {
        this.lastKeyList.add(key);
    }
    
    public void add(List<String> keys) {
        this.lastKeyList.addAll(keys);
    }
    
    public String[] getLastKeys() {
        return this.lastKeyList.toArray(new String[0]);
    }
    
    public String[] getSortedLastKeys() {
        List<String> sortedRecords = new ArrayList<String>();
        for(String lastkey : this.lastKeyList) {
            sortedRecords.add(lastkey);
        }
        Collections.sort(sortedRecords);
        return sortedRecords.toArray(new String[0]);
    }
    
    public int getSize() {
        return this.lastKeyList.size();
    }
    
    public void loadFromJson(String json) {
        this.lastKeyList.clear();
        
        JSONObject jsonobj = new JSONObject(json);
        int records = jsonobj.getInt(JSON_CONF_RECORD_NUM);
        
        JSONArray outputsArray = jsonobj.getJSONArray(JSON_CONF_RECORDS);
        for(int i=0;i<records;i++) {
            JSONObject itemjsonobj = (JSONObject) outputsArray.get(i);
            String lastKey = itemjsonobj.getString(JSON_CONF_RECORD_LASTKEY);
            this.lastKeyList.add(lastKey);
        }
    }
    
    public String createJson() {
        JSONObject jsonobj = new JSONObject();
        
        jsonobj.put(JSON_CONF_RECORD_NUM, this.lastKeyList.size());
        
        // set array
        JSONArray outputsArray = new JSONArray();
        for(int i=0;i<this.lastKeyList.size();i++) {
            String lastKey = this.lastKeyList.get(i);
            JSONObject itemjsonobj = new JSONObject();
            itemjsonobj.put(JSON_CONF_RECORD_LASTKEY, lastKey);
            outputsArray.put(itemjsonobj);
        }
        jsonobj.put(JSON_CONF_RECORDS, outputsArray);
        
        return jsonobj.toString();
    }
    
    public void saveTo(Configuration conf) {
        conf.set(CONF_KMER_INDEX_CHUNK_INFO_JSON, createJson());
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
        loadFromJson(conf.get(CONF_KMER_INDEX_CHUNK_INFO_JSON));
    }
    
    public void loadFrom(Path file, FileSystem fs) throws IOException {
        DataInputStream reader = fs.open(file);
        
        loadFromJson(Text.readString(reader));
        
        reader.close();
    }
}
