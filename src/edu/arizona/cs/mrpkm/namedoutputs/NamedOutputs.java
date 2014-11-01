package edu.arizona.cs.mrpkm.namedoutputs;

import edu.arizona.cs.mrpkm.utils.MapReduceHelper;
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
public class NamedOutputs {
    private final static String CONF_NAMED_OUTPUT_JSON = "edu.arizona.cs.mrpkm.namedoutputs.named_outputs.json";
    
    private final static String JSON_CONF_OUTPUT_NUM = "itemcounts";
    private final static String JSON_CONF_OUTPUTS = "items";
    private final static String JSON_CONF_OUTPUT_ID = "id";
    private final static String JSON_CONF_OUTPUT_NAMEDOUTPUT = "namedoutput";
    private final static String JSON_CONF_OUTPUT_FILENAME = "filename";
    
    private Hashtable<String, Integer> outputnameTable;
    private Hashtable<String, Integer> outputTable;
    private List<NamedOutput> objList;
    
    public NamedOutputs() {
        this.outputnameTable = new Hashtable<String, Integer>();
        this.outputTable = new Hashtable<String, Integer>();
        this.objList = new ArrayList<NamedOutput>();
    }
    
    public void addNamedOutput(Path output) {
        addNamedOutput(output.getName());
    }
    
    public void addNamedOutput(Path[] outputs) {
        for(Path output : outputs) {
            addNamedOutput(output.getName());
        }
    }
    
    public void addNamedOutput(String output) {
        String outputName = NamedOutput.getSafeNamedOutputString(output);
        
        if(this.outputnameTable.get(outputName) == null) {
            // okey
            NamedOutput namedOutput = new NamedOutput(output, outputName);
            this.outputnameTable.put(outputName, this.objList.size());
            this.outputTable.put(output, this.objList.size());
            this.objList.add(namedOutput);
        } else {
            int trial = 0;
            boolean success = false;
            while(!success) {
                trial++;
                String outputNameTrial = NamedOutput.getSafeNamedOutputString(output + trial);
                if(this.outputnameTable.get(outputNameTrial) == null) {
                    // okey
                    NamedOutput namedOutput = new NamedOutput(output, outputNameTrial);
                    this.outputnameTable.put(outputNameTrial, this.objList.size());
                    this.outputTable.put(output, this.objList.size());
                    this.objList.add(namedOutput);
                    success = true;
                    break;
                }
            }
        }
    }
    
    public NamedOutput getNamedOutput(String outputName) {
        if(this.outputnameTable.get(outputName) == null) {
            return null;
        } else {
            int idx = this.outputnameTable.get(outputName);
            return this.objList.get(idx);
        }
    }
    
    public int getIDFromOutput(String output) {
        if(this.outputTable.get(output) == null) {
            return -1;
        } else {
            return this.outputTable.get(output);
        }
    }
    
    public NamedOutput getNamedOutputFromID(int id) {
        if(this.objList.size() <= id) {
            return null;
        } else {
            return this.objList.get(id);    
        }
    }
    
    public NamedOutput getNamedOutputByMROutput(Path outputFilePath) {
        return getNamedOutputByMROutput(outputFilePath.getName());
    }
    
    public NamedOutput getNamedOutputByMROutput(String outputFileName) {
        String name = MapReduceHelper.getNameFromMapReduceOutput(outputFileName);
        if(this.outputnameTable.get(name) == null) {
            return null;
        } else {
            int idx = this.outputnameTable.get(name);
            return this.objList.get(idx);
        }
    }
    
    public NamedOutput[] getAllNamedOutput() {
        return this.objList.toArray(new NamedOutput[0]);
    }
    
    public int getSize() {
        return this.objList.size();
    }
    
    public void loadFromJson(String json) {
        this.outputnameTable.clear();
        this.outputTable.clear();
        this.objList.clear();
        
        JSONObject jsonobj = new JSONObject(json);
        int size = jsonobj.getInt(JSON_CONF_OUTPUT_NUM);
        
        NamedOutput[] tempArray = new NamedOutput[size];
        JSONArray outputsArray = jsonobj.getJSONArray(JSON_CONF_OUTPUTS);
        for(int i=0;i<size;i++) {
            JSONObject itemjsonobj = (JSONObject) outputsArray.get(i);
            int id = itemjsonobj.getInt(JSON_CONF_OUTPUT_ID);
            String namedoutput = itemjsonobj.getString(JSON_CONF_OUTPUT_NAMEDOUTPUT);
            String filename = itemjsonobj.getString(JSON_CONF_OUTPUT_FILENAME);
            NamedOutput namedOutput = new NamedOutput(filename, namedoutput);
            tempArray[id] = namedOutput;
        }
        
        for(int i=0;i<size;i++) {
            this.objList.add(tempArray[i]);
            this.outputnameTable.put(tempArray[i].getNamedOutputString(), i);
            this.outputTable.put(tempArray[i].getInputString(), i);
        }
    }
    
    public String createJson() {
        JSONObject jsonobj = new JSONObject();
        // set size
        jsonobj.put(JSON_CONF_OUTPUT_NUM, this.objList.size());
        
        // set array
        JSONArray outputsArray = new JSONArray();
        for(int i=0;i<this.objList.size();i++) {
            NamedOutput namedoutput = this.objList.get(i);
            JSONObject itemjsonobj = new JSONObject();
            itemjsonobj.put(JSON_CONF_OUTPUT_ID, i);
            itemjsonobj.put(JSON_CONF_OUTPUT_NAMEDOUTPUT, namedoutput.getNamedOutputString());
            itemjsonobj.put(JSON_CONF_OUTPUT_FILENAME, namedoutput.getInputString());
            outputsArray.put(itemjsonobj);
        }
        jsonobj.put(JSON_CONF_OUTPUTS, outputsArray);
        
        return jsonobj.toString();
    }

    public void saveTo(Configuration conf) {
        conf.set(CONF_NAMED_OUTPUT_JSON, createJson());
    }
    
    public void loadFrom(Configuration conf) {
        loadFromJson(conf.get(CONF_NAMED_OUTPUT_JSON));
    }
}
