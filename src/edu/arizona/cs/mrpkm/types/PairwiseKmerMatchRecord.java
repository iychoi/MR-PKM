package edu.arizona.cs.mrpkm.types;

import java.io.IOException;

/**
 *
 * @author iychoi
 */
public class PairwiseKmerMatchRecord {
    private String kmer;
    private PairwiseKmerMatchRecordColumn[] columns;
    private int columnNum = 0;
    
    public PairwiseKmerMatchRecord(String str) throws IOException {
        parse(str);
    }
    
    public String getKmer() {
        return this.kmer;
    }
    
    public PairwiseKmerMatchRecordColumn[] getColumns() {
        return this.columns;
    }
    
    public int getColumnNum() {
        return this.columnNum;
    }
    
    private void parse(String str) throws IOException {
        String[] strs = str.split("\\t");
        if(strs.length >= 3) {
            this.kmer = strs[0];
            this.columns = new PairwiseKmerMatchRecordColumn[strs.length - 1];
            this.columnNum = strs.length - 1;
            for(int i=1;i<strs.length;i++) {
                this.columns[i-1] = new PairwiseKmerMatchRecordColumn(strs[i]);
            }
        }
    }
}
