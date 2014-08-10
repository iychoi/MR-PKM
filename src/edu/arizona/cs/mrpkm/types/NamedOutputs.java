package edu.arizona.cs.mrpkm.types;

import edu.arizona.cs.mrpkm.utils.MapReduceHelper;
import java.util.Collection;
import java.util.Hashtable;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class NamedOutputs {
    private Hashtable<String, NamedOutput> table = new Hashtable<String, NamedOutput>();
    
    public NamedOutputs() {
    }
    
    public void addNamedOutput(Path output) {
        addNamedOutput(output.getName());
    }
    
    public void addNamedOutput(String output) {
        String outputName = NamedOutput.getSafeNamedOutputString(output);
        
        if(this.table.get(outputName) == null) {
            // okey
            NamedOutput namedOutput = new NamedOutput(output, outputName);
            this.table.put(outputName, namedOutput);
        } else {
            int trial = 0;
            boolean success = false;
            while(!success) {
                trial++;
                String outputNameTrial = NamedOutput.getSafeNamedOutputString(output + trial);
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
    
    public NamedOutput getNamedOutputByMROutput(Path outputFilePath) {
        return this.table.get(MapReduceHelper.getNameFromReduceOutput(outputFilePath));
    }
    
    public NamedOutput getNamedOutputByMROutput(String outputFileName) {
        return this.table.get(MapReduceHelper.getNameFromReduceOutput(outputFileName));
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
