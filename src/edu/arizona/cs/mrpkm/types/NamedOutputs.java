package edu.arizona.cs.mrpkm.types;

import java.util.Collection;
import java.util.Hashtable;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class NamedOutputs {
    private Hashtable<String, NamedOutput> table;
    
    public NamedOutputs() {
        this.table = new Hashtable<String, NamedOutput>();
    }
    
    public void addNamedOutput(Path output) {
        String outputName = NamedOutput.getSafeNamedOutputString(output.getName());
        
        if(this.table.get(outputName) == null) {
            // okey
            NamedOutput namedOutput = new NamedOutput(output, outputName);
            this.table.put(outputName, namedOutput);
        } else {
            int trial = 0;
            boolean success = false;
            while(!success) {
                trial++;
                String outputNameTrial = NamedOutput.getSafeNamedOutputString(output.getName() + trial);
                if(this.table.get(outputNameTrial) == null) {
                    // okey
                    NamedOutput namedOutput = new NamedOutput(output, outputNameTrial);
                    this.table.put(outputNameTrial, namedOutput);
                    success = true;
                    break;
                }
            }
        }
    }
    
    public NamedOutput getNamedOutput(String outputName) {
        return this.table.get(outputName);
    }
    
    public NamedOutput getNamedOutputByMROutputName(String outputFileName) {
        return this.table.get(getNamedOutputFromMROutputName(outputFileName));
    }
    
    private String getNamedOutputFromMROutputName(String mrOutputName) {
        int index = mrOutputName.indexOf("-r-");
        if(index > 0) {
            return mrOutputName.substring(0, index);
        }
        return mrOutputName;
    }
    
    public NamedOutput[] getAllNamedOutput() {
        Collection<NamedOutput> values = this.table.values();
        NamedOutput[] arr = values.toArray(new NamedOutput[0]);
        return arr;
    }
    
    public int getSize() {
        return this.table.size();
    }
}
