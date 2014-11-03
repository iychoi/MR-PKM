package edu.arizona.cs.mrpkm.types.statistics;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.json.JSONObject;

/**
 *
 * @author iychoi
 */
public class KmerStatistics {
    
    private final static String CONF_STATISTICS_JSON = "edu.arizona.cs.mrpkm.types.statistics.kmer_statistics.json";
    
    private final static String JSON_CONF_NAME = "name";
    private final static String JSON_CONF_UNIQUE_KMERS = "unique";
    private final static String JSON_CONF_TOTAL_KMERS = "total";

    private String statisticsName;
    private long uniqueKmers = 0;
    private long totalKmers = 0;
    
    public KmerStatistics() {
        
    }
    
    public KmerStatistics(String statisticsName, long uniqueKmers, long totalKmers) {
        this.statisticsName = statisticsName;
        this.uniqueKmers = uniqueKmers;
        this.totalKmers = totalKmers;
    }
    
    public void setStatisticsName(String statisticsName) {
        this.statisticsName = statisticsName;
    }
    
    public String getStatisticsName() {
        return this.statisticsName;
    }
    
    public long getUniqueKmers() {
        return this.uniqueKmers;
    }
    
    public void setUniqueKmers(long uniqueKmers) {
        this.uniqueKmers = uniqueKmers;
    }
    
    public long getTotalKmers() {
        return this.totalKmers;
    }
    
    public void setTotalKmers(long totalKmers) {
        this.totalKmers = totalKmers;
    }
    
    public void loadFromJson(String json) {
        JSONObject jsonobj = new JSONObject(json);
        loadFromJsonObject(jsonobj);
    }
    
    public void loadFromJsonObject(JSONObject jsonobj) {
        this.statisticsName = jsonobj.getString(JSON_CONF_NAME);
        this.uniqueKmers = jsonobj.getLong(JSON_CONF_UNIQUE_KMERS);
        this.totalKmers = jsonobj.getLong(JSON_CONF_TOTAL_KMERS);
    }
    
    public String createJson() {
        return createJsonObject().toString();
    }
    
    public JSONObject createJsonObject() {
        JSONObject jsonobj = new JSONObject();
        
        jsonobj.put(JSON_CONF_NAME, this.statisticsName);
        jsonobj.put(JSON_CONF_UNIQUE_KMERS, this.uniqueKmers);
        jsonobj.put(JSON_CONF_TOTAL_KMERS, this.totalKmers);
        
        return jsonobj;
    }
    
    public void saveTo(Configuration conf) {
        conf.set(CONF_STATISTICS_JSON, createJson());
    }
    
    public void saveTo(Path file, FileSystem fs) throws IOException {
        if(!fs.exists(file.getParent())) {
            fs.mkdirs(file.getParent());
        }
        
        DataOutputStream writer = fs.create(file, true, 64 * 1024);
        new Text(createJson()).write(writer);
        writer.close();
    }
    
    public void loadFrom(Configuration conf) {
        loadFromJson(conf.get(CONF_STATISTICS_JSON));
    }
    
    public void loadFrom(Path file, FileSystem fs) throws IOException {
        DataInputStream reader = fs.open(file);
        
        loadFromJson(Text.readString(reader));
        
        reader.close();
    }
}
