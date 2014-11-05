package edu.arizona.cs.mrpkm.kmermatch;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author iychoi
 */
public class ModeCounterConfig {
    private final static String CONF_MASTER_FILE_ID = "edu.arizona.cs.mrpkm.kmermatch.modecounter.master_id";
    
    private int masterFileID;
    
    public ModeCounterConfig() {
    }
    
    public void setMasterFileID(int masterFileID) {
        this.masterFileID = masterFileID;
    }
    
    public int getMasterFileID() {
        return this.masterFileID;
    }
    
    public void saveTo(Configuration conf) {
        conf.setInt(CONF_MASTER_FILE_ID, this.masterFileID);
    }
    
    public void loadFrom(Configuration conf) throws IOException {
        int masterID = conf.getInt(CONF_MASTER_FILE_ID, -1);
        if(masterID < 0) {
            throw new IOException("could not load configuration string");
        }
        this.masterFileID = masterID;
    }
}
