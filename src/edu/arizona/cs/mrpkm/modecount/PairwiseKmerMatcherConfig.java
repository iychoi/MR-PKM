package edu.arizona.cs.mrpkm.modecount;

import edu.arizona.cs.mrpkm.kmermatch.*;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author iychoi
 */
public class PairwiseKmerMatcherConfig {
    private final static String CONF_INPUT_JSON = "edu.arizona.cs.mrpkm.kmermatch.input.json";

    private final static String JSON_CONF_INPUT_NUM = "itemcounts";
    private final static String JSON_CONF_INPUTS = "items";
    private final static String JSON_CONF_INPUT_ID = "id";
    private final static String JSON_CONF_INPUT_FILENAME = "filename";

    private Hashtable<String, Integer> inputTable;
    private List<String> objList;

    public PairwiseKmerMatcherConfig() {
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
    
    public int getIDFromInput(String input) {
        if(this.inputTable.get(input) == null) {
            return -1;
        } else {
            return this.inputTable.get(input);
        }
    }
    
    public String getInputFromID(int id) {
        if(this.objList.size() <= id) {
            return null;
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
        
        return jsonobj.toString();
    }

    public void saveTo(Configuration conf) {
        conf.set(CONF_INPUT_JSON, createJson());
    }
    
    public void loadFrom(Configuration conf) {
        loadFromJson(conf.get(CONF_INPUT_JSON));
    }
}
