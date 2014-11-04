package edu.arizona.cs.mrpkm.types;

/**
 *
 * @author iychoi
 */
public class PairwiseKmerMatchRecordColumn {
    private int fileid;
    private int[] readids;
    
    public PairwiseKmerMatchRecordColumn(String str) {
        parse(str);
    }
    
    public int getFileID() {
        return this.fileid;
    }
    
    public int[] getReadIDs() {
        return this.readids;
    }
    
    private void parse(String str) {
        String[] strs = str.split(":");
        if(strs.length == 2) {
            this.fileid = Integer.parseInt(strs[0]);
            String[] readidstrs = strs[1].split(",");
            this.readids = new int[readidstrs.length];
            
            for(int i=0;i<readidstrs.length;i++) {
                this.readids[i] = Integer.parseInt(readidstrs[i]);
            }
        }
    }
}
