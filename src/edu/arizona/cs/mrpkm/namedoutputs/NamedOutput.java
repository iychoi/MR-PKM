package edu.arizona.cs.mrpkm.namedoutputs;

import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class NamedOutput {
    private String inputString;
    private String namedOutputString;
    
    public NamedOutput(Path inputPath) {
        initialize(inputPath.getName(), NamedOutput.getSafeNamedOutputString(inputPath.getName()));
    }
    
    public NamedOutput(Path inputPath, String namedOutputString) {
        initialize(inputPath.getName(), namedOutputString);
    }
    
    public NamedOutput(String inputString) {
        initialize(inputString, NamedOutput.getSafeNamedOutputString(inputString));
    }
    
    public NamedOutput(String inputString, String namedOutputString) {
        initialize(inputString, namedOutputString);
    }
    
    private void initialize(String inputString, String namedOutputString) {
        this.inputString = inputString;
        this.namedOutputString = namedOutputString;
    }
    
    public String getInputString() {
        return this.inputString;
    }
    
    public String getNamedOutputString() {
        return this.namedOutputString;
    }
    
    public static String getSafeNamedOutputString(String input) {
        StringBuffer sb = new StringBuffer();
        
        for (char ch : input.toCharArray()) {
            boolean isSafe = false;
            if ((ch >= 'A') && (ch <= 'Z')) {
                isSafe = true;
            } else if ((ch >= 'a') && (ch <= 'z')) {
                isSafe = true;
            } else if ((ch >= '0') && (ch <= '9')) {
                isSafe = true;
            }
            
            if(isSafe) {
                sb.append(ch);
            }
        }
        
        return sb.toString();
    }
}
