package edu.arizona.cs.mrpkm.types.namedoutputs;

import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class NamedOutputRecord implements Comparable<NamedOutputRecord> {
    private String filename;
    private String identifier;
    
    public NamedOutputRecord(Path file) {
        initialize(file.getName(), NamedOutputRecord.getSafeIdentifier(file.getName()));
    }
    
    public NamedOutputRecord(Path file, String identifier) {
        initialize(file.getName(), identifier);
    }
    
    public NamedOutputRecord(String filename) {
        initialize(filename, NamedOutputRecord.getSafeIdentifier(filename));
    }
    
    public NamedOutputRecord(String filename, String identifier) {
        initialize(filename, identifier);
    }
    
    private void initialize(String filename, String identifier) {
        this.filename = filename;
        this.identifier = identifier;
    }
    
    public String getFilename() {
        return this.filename;
    }
    
    public String getIdentifier() {
        return this.identifier;
    }
    
    public static String getSafeIdentifier(String filename) {
        StringBuffer sb = new StringBuffer();
        
        for (char ch : filename.toCharArray()) {
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

    @Override
    public int compareTo(NamedOutputRecord right) {
        return this.identifier.compareToIgnoreCase(right.identifier);
    }
}
