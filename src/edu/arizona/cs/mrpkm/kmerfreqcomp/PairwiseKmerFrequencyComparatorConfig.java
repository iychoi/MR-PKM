package edu.arizona.cs.mrpkm.kmerfreqcomp;

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
public class PairwiseKmerFrequencyComparatorConfig {
    private final static String CONF_INPUT_JSON = "edu.arizona.cs.mrpkm.kmerfreqcomp.kfc.input.json";

    private final static String JSON_CONF_INPUT_NUM = "itemcounts";
    private final static String JSON_CONF_INPUTS = "items";
    private final static String JSON_CONF_INPUT_ID = "id";
    private final static String JSON_CONF_INPUT_FILENAME = "filename";
    private final static String JSON_CONF_STATISTICS_PATH = "statistics_path";

    private Hashtable<String, Integer> inputTable;
    private List<String> objList;
    private String statisticsPath;

    public PairwiseKmerFrequencyComparatorConfig() {
        this.inputTable = new Hashtable<String, Integer>();
        this.objList = new ArrayList<String>();
    }
    
    public void addInput(Path input) {
        addInput(input.getName());
    }
    
    public void addInput(Path[] inputs) {
        for(Path input : inputs) {
            addInput(input.getName());
        }
    }
    
    public void addInput(String input) {
        this.inputTable.put(input, this.objList.size());
        this.objList.add(input);
    }
    
    public int getIDFromInput(String input) throws IOException {
        if(this.inputTable.get(input) == null) {
            throw new IOException("could not find id from " + input);
        } else {
            return this.inputTable.get(input);
        }
    }
    
    public String getInputFromID(int id) throws IOException {
        if(this.objList.size() <= id) {
            throw new IOException("could not find filename from " + id);
        } else {
            return this.objList.get(id);    
        }
    }
    
    public String[] getAllInput() {
        return this.objList.toArray(new String[0]);
    }
    
    public int getSize() {
        return this.objList.size();
    }
    
    public void setStatisticsPath(String path) {
        this.statisticsPath = path;
    }
    
    public String getStatisticsPath() {
        return statisticsPath;
    }
    
    public void loadFromJson(String json) {
        this.inputTable.clear();
        this.objList.clear();
        
        JSONObject jsonobj = new JSONObject(json);
        int size = jsonobj.getInt(JSON_CONF_INPUT_NUM);
        
        String[] tempArray = new String[size];
        JSONArray outputsArray = jsonobj.getJSONArray(JSON_CONF_INPUTS);
        for(int i=0;i<size;i++) {
            JSONObject itemjsonobj = (JSONObject) outputsArray.get(i);
            int id = itemjsonobj.getInt(JSON_CONF_INPUT_ID);
            tempArray[id] = itemjsonobj.getString(JSON_CONF_INPUT_FILENAME);
        }
        
        this.statisticsPath = jsonobj.getString(JSON_CONF_STATISTICS_PATH);
        
        for(int i=0;i<size;i++) {
            this.objList.add(tempArray[i]);
            this.inputTable.put(tempArray[i], i);
        }
    }
    
    public String createJson() {
        JSONObject jsonobj = new JSONObject();
        // set size
        jsonobj.put(JSON_CONF_INPUT_NUM, this.objList.size());
        
        // set array
        JSONArray outputsArray = new JSONArray();
        for(int i=0;i<this.objList.size();i++) {
            JSONObject itemjsonobj = new JSONObject();
            itemjsonobj.put(JSON_CONF_INPUT_ID, i);
            itemjsonobj.put(JSON_CONF_INPUT_FILENAME, this.objList.get(i));
            outputsArray.put(itemjsonobj);
        }
        jsonobj.put(JSON_CONF_INPUTS, outputsArray);
        
        jsonobj.put(JSON_CONF_STATISTICS_PATH, this.statisticsPath);
        
        return jsonobj.toString();
    }

    public void saveTo(Configuration conf) {
        conf.set(CONF_INPUT_JSON, createJson());
    }
    
    public void saveTo(Path file, FileSystem fs) throws IOException {
        if(!fs.exists(file.getParent())) {
            fs.mkdirs(file.getParent());
        }
        
        DataOutputStream writer = fs.create(file, true, 64 * 1024);
        
        new Text(createJson()).write(writer);
        
        writer.close();
    }
    
    public void loadFrom(Configuration conf) throws IOException {
        String json = conf.get(CONF_INPUT_JSON);
        if(json == null) {
            throw new IOException("could not load configuration string");
        }
        loadFromJson(json);
    }
    
    public void loadFrom(Path file, FileSystem fs) throws IOException {
        DataInputStream reader = fs.open(file);
        
        loadFromJson(Text.readString(reader));
        
        reader.close();
    }
}
