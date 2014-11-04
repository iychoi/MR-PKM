package edu.arizona.cs.mrpkm.kmermatch;

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
    
    public void loadFrom(Configuration conf) {
        this.masterFileID = conf.getInt(CONF_MASTER_FILE_ID, 0);
    }
}
