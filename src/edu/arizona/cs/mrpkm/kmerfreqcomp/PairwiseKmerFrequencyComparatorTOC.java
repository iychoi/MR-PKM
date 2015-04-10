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
public class PairwiseKmerFrequencyComparatorTOC {
    private final static String JSON_CONF_INPUT_NUM = "itemcounts";
    private final static String JSON_CONF_INPUTS = "items";
    private final static String JSON_CONF_INPUT_ID = "id";
    private final static String JSON_CONF_INPUT_FILENAME = "filename";

    private Hashtable<String, Integer> inputTable;
    private List<String> objList;

    public PairwiseKmerFrequencyComparatorTOC() {
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

    public void saveTo(Path file, FileSystem fs) throws IOException {
        if(!fs.exists(file.getParent())) {
            fs.mkdirs(file.getParent());
        }
        
        DataOutputStream writer = fs.create(file, true, 64 * 1024);
        
        new Text(createJson()).write(writer);
        
        writer.close();
    }
}
