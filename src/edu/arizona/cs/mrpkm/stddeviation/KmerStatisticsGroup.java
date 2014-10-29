package edu.arizona.cs.mrpkm.stddeviation;

import java.util.Hashtable;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author iychoi
 */
public class KmerStatisticsGroup {
    private final static String CONF_KMER_STATISTICS_FILENUM = "edu.arizona.cs.mrpkm.stddeviation.filenum";
    private final static String CONF_KMER_STATISTICS_ID_FILENAME_PREFIX = "edu.arizona.cs.mrpkm.stddeviation.filename.";
    private final static String CONF_KMER_STATISTICS_ID_UNIQUE_OCCURANCE_PREFIX = "edu.arizona.cs.mrpkm.stddeviation.uniqueoccurance.";
    private final static String CONF_KMER_STATISTICS_ID_TOTAL_OCCURANCE_PREFIX = "edu.arizona.cs.mrpkm.stddeviation.uniqueoccurance.";
    
    
    private Hashtable<String, KmerStatistics> statisticsTable;
    
    public KmerStatisticsGroup() {
        this.statisticsTable = new Hashtable<String, KmerStatistics>();
    }
    
    public void addStatistics(KmerStatistics statistics) {
        this.statisticsTable.put(statistics.getFilename(), statistics);
    }
    
    public KmerStatistics getStatistics(String filename) {
        return this.statisticsTable.get(filename);
    }
    
    public void saveTo(Configuration conf) {
        conf.setInt(CONF_KMER_STATISTICS_FILENUM, this.statisticsTable.keySet().size());
        
        int id = 0;
        for(String key : this.statisticsTable.keySet()) {
            KmerStatistics statistics = this.statisticsTable.get(key);
            conf.set(CONF_KMER_STATISTICS_ID_FILENAME_PREFIX + id, key);
            conf.setLong(CONF_KMER_STATISTICS_ID_UNIQUE_OCCURANCE_PREFIX + id, statistics.getUniqueOccurance());
            conf.setLong(CONF_KMER_STATISTICS_ID_TOTAL_OCCURANCE_PREFIX + id, statistics.getTotalOccurance());
            id++;
        }
    }
    
    public void loadFrom(Configuration conf) {
        this.statisticsTable.clear();
        int filenum = conf.getInt(CONF_KMER_STATISTICS_FILENUM, 0);
        for(int i=0;i<filenum;i++) {
            String filename = conf.get(CONF_KMER_STATISTICS_ID_FILENAME_PREFIX + i);
            long uniqueOccurance = conf.getLong(CONF_KMER_STATISTICS_ID_UNIQUE_OCCURANCE_PREFIX + i, 0);
            long totalOccurance = conf.getLong(CONF_KMER_STATISTICS_ID_TOTAL_OCCURANCE_PREFIX + i, 0);
            KmerStatistics statistics = new KmerStatistics(filename, uniqueOccurance, totalOccurance);
            this.statisticsTable.put(filename, statistics);
        }
    }
}
