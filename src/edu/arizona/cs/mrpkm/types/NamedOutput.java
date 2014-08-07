package edu.arizona.cs.mrpkm.types;

import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class NamedOutput {
    private Path inputPath;
    private String namedOutputString;
    
    public NamedOutput(Path inputPath) {
        initialize(inputPath, NamedOutput.getSafeNamedOutputString(inputPath.getName()));
    }
    
    public NamedOutput(Path inputPath, String namedOutputString) {
        initialize(inputPath, namedOutputString);
    }
    
    private void initialize(Path inputPath, String namedOutputString) {
        this.inputPath = inputPath;
        this.namedOutputString = namedOutputString;
    }
    
    public Path getInputPath() {
        return this.inputPath;
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
