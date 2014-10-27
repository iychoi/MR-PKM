package edu.arizona.cs.mrpkm.namedoutputs;

import edu.arizona.cs.mrpkm.utils.MapReduceHelper;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class NamedOutputs {
    private final static String CONF_NAMED_OUTPUT_NUM = "edu.arizona.cs.mrpkm.namedoutputs.named_outputs.num";
    private final static String CONF_NAMED_OUTPUT_ID_TO_NAME_PREFIX = "edu.arizona.cs.mrpkm.namedoutputs.named_output.id_name.";
    private final static String CONF_NAMED_OUTPUT_ID_TO_FILENAME_PREFIX = "edu.arizona.cs.mrpkm.namedoutputs.named_output.id_filename.";
    
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

    public void saveTo(Configuration conf) {
        // set output num
        conf.setInt(CONF_NAMED_OUTPUT_NUM, this.objList.size());
        for(int i=0;i<this.objList.size();i++) {
            NamedOutput namedoutput = this.objList.get(i);
            conf.set(CONF_NAMED_OUTPUT_ID_TO_NAME_PREFIX + i, namedoutput.getNamedOutputString());
            conf.set(CONF_NAMED_OUTPUT_ID_TO_FILENAME_PREFIX + i, namedoutput.getInputString());
        }
    }
    
    public void loadFrom(Configuration conf) {
        this.outputnameTable.clear();
        this.outputTable.clear();
        this.objList.clear();
        
        int outputNum = conf.getInt(CONF_NAMED_OUTPUT_NUM, 0);
        for(int i=0;i<outputNum;i++) {
            String namedoutputString = conf.get(CONF_NAMED_OUTPUT_ID_TO_NAME_PREFIX + i, null);
            String output = conf.get(CONF_NAMED_OUTPUT_ID_TO_FILENAME_PREFIX + i, null);
            
            NamedOutput namedOutput = new NamedOutput(output, namedoutputString);
            this.outputnameTable.put(namedoutputString, i);
            this.outputTable.put(output, i);
            this.objList.add(namedOutput);
        }
    }
}
