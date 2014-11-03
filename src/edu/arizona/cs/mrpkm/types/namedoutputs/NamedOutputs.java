package edu.arizona.cs.mrpkm.types.namedoutputs;

import edu.arizona.cs.mrpkm.helpers.MapReduceHelper;
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
public class NamedOutputs {
    private final static String CONF_NAMED_OUTPUT_JSON = "edu.arizona.cs.mrpkm.types.namedoutputs.named_outputs.json";
    
    private final static String JSON_CONF_OUTPUT_NUM = "itemcounts";
    private final static String JSON_CONF_OUTPUTS = "items";
    private final static String JSON_CONF_OUTPUT_ID = "id";
    private final static String JSON_CONF_OUTPUT_NAMEDOUTPUT = "namedoutput";
    private final static String JSON_CONF_OUTPUT_FILENAME = "filename";
    
    private Hashtable<String, Integer> identifierCache;
    private Hashtable<String, Integer> filenameCache;
    private List<NamedOutputRecord> recordList;
    
    public NamedOutputs() {
        this.identifierCache = new Hashtable<String, Integer>();
        this.filenameCache = new Hashtable<String, Integer>();
        this.recordList = new ArrayList<NamedOutputRecord>();
    }
    
    public void add(Path file) {
        add(file.getName());
    }
    
    public void add(Path[] files) {
        for(Path file : files) {
            add(file.getName());
        }
    }
    
    public void add(String filename) {
        String identifier = NamedOutputRecord.getSafeIdentifier(filename);
        
        if(this.identifierCache.get(identifier) == null) {
            // okey
            NamedOutputRecord record = new NamedOutputRecord(filename, identifier);
            this.identifierCache.put(identifier, this.recordList.size());
            this.filenameCache.put(filename, this.recordList.size());
            this.recordList.add(record);
        } else {
            int trial = 0;
            boolean success = false;
            while(!success) {
                trial++;
                String identifierTrial = NamedOutputRecord.getSafeIdentifier(filename + trial);
                if(this.identifierCache.get(identifierTrial) == null) {
                    // okey
                    NamedOutputRecord record = new NamedOutputRecord(filename, identifierTrial);
                    this.identifierCache.put(identifierTrial, this.recordList.size());
                    this.filenameCache.put(filename, this.recordList.size());
                    this.recordList.add(record);
                    success = true;
                    break;
                }
            }
        }
    }
    
    public NamedOutputRecord getRecord(String identifier) {
        if(this.identifierCache.get(identifier) == null) {
            return null;
        } else {
            int idx = this.identifierCache.get(identifier);
            return this.recordList.get(idx);
        }
    }
    
    public int getIDFromFilename(String filename) {
        if(this.filenameCache.get(filename) == null) {
            return -1;
        } else {
            return this.filenameCache.get(filename);
        }
    }
    
    public NamedOutputRecord getRecordFromID(int id) {
        if(this.recordList.size() <= id) {
            return null;
        } else {
            return this.recordList.get(id);    
        }
    }
    
    public NamedOutputRecord getRecordFromMROutput(Path outputFile) {
        return getRecordFromMROutput(outputFile.getName());
    }
    
    public NamedOutputRecord getRecordFromMROutput(String outputFilename) {
        String identifier = MapReduceHelper.getNameFromMapReduceOutput(outputFilename);
        if(this.identifierCache.get(identifier) == null) {
            return null;
        } else {
            int idx = this.identifierCache.get(identifier);
            return this.recordList.get(idx);
        }
    }
    
    public NamedOutputRecord[] getAllRecords() {
        return this.recordList.toArray(new NamedOutputRecord[0]);
    }
    
    public int getSize() {
        return this.recordList.size();
    }
    
    public void loadFromJson(String json) {
        this.identifierCache.clear();
        this.filenameCache.clear();
        this.recordList.clear();
        
        JSONObject jsonobj = new JSONObject(json);
        int size = jsonobj.getInt(JSON_CONF_OUTPUT_NUM);
        
        NamedOutputRecord[] recordArray = new NamedOutputRecord[size];
        JSONArray outputsArray = jsonobj.getJSONArray(JSON_CONF_OUTPUTS);
        for(int i=0;i<size;i++) {
            JSONObject itemjsonobj = (JSONObject) outputsArray.get(i);
            int id = itemjsonobj.getInt(JSON_CONF_OUTPUT_ID);
            String namedoutput = itemjsonobj.getString(JSON_CONF_OUTPUT_NAMEDOUTPUT);
            String filename = itemjsonobj.getString(JSON_CONF_OUTPUT_FILENAME);
            NamedOutputRecord record = new NamedOutputRecord(filename, namedoutput);
            recordArray[id] = record;
        }
        
        for(int i=0;i<size;i++) {
            this.recordList.add(recordArray[i]);
            this.identifierCache.put(recordArray[i].getIdentifier(), i);
            this.filenameCache.put(recordArray[i].getFilename(), i);
        }
    }
    
    public String createJson() {
        JSONObject jsonobj = new JSONObject();
        // set size
        jsonobj.put(JSON_CONF_OUTPUT_NUM, this.recordList.size());
        
        // set array
        JSONArray outputsArray = new JSONArray();
        for(int i=0;i<this.recordList.size();i++) {
            NamedOutputRecord record = this.recordList.get(i);
            JSONObject itemjsonobj = new JSONObject();
            itemjsonobj.put(JSON_CONF_OUTPUT_ID, i);
            itemjsonobj.put(JSON_CONF_OUTPUT_NAMEDOUTPUT, record.getIdentifier());
            itemjsonobj.put(JSON_CONF_OUTPUT_FILENAME, record.getFilename());
            outputsArray.put(itemjsonobj);
        }
        jsonobj.put(JSON_CONF_OUTPUTS, outputsArray);
        
        return jsonobj.toString();
    }

    public void saveTo(Configuration conf) {
        conf.set(CONF_NAMED_OUTPUT_JSON, createJson());
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
        loadFromJson(conf.get(CONF_NAMED_OUTPUT_JSON));
    }
    
    public void loadFrom(Path file, FileSystem fs) throws IOException {
        DataInputStream reader = fs.open(file);
        
        loadFromJson(Text.readString(reader));
        
        reader.close();
    }
}
