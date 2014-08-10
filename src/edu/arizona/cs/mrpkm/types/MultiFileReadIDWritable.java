package edu.arizona.cs.mrpkm.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import static org.apache.hadoop.io.WritableComparator.compareBytes;

/**
 *
 * @author iychoi
 */
public class MultiFileReadIDWritable extends BinaryComparable implements WritableComparable<BinaryComparable> {

    private static final Log LOG = LogFactory.getLog(MultiFileReadIDWritable.class);
    
    private int fileID;
    private int readID;
    private byte[] fullLine;
    
    private static final int ID_BYTES = 2+4;
    
    public MultiFileReadIDWritable() {}
    
    public MultiFileReadIDWritable(int fileID, int readID) throws IOException { set(fileID, readID); }
    
    /**
     * Set the value.
     */
    public void set(int fileID, int readID) {
        this.fileID = fileID;
        this.readID = readID;
        
        this.fullLine = new byte[ID_BYTES];
        this.fullLine[0] = (byte) ((this.fileID >> 8) & 0xff);
        this.fullLine[1] = (byte) (this.fileID & 0xff);
        
        this.fullLine[2] = (byte) ((this.readID >> 24) & 0xff);
        this.fullLine[3] = (byte) ((this.readID >> 16) & 0xff);
        this.fullLine[4] = (byte) ((this.readID >> 8) & 0xff);
        this.fullLine[5] = (byte) (this.readID & 0xff);
    }
    
    /**
     * Return the value.
     */
    public int getFileID() {
        return this.fileID;
    }
    
    public int getReadID() {
        return this.readID;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        this.fileID = in.readShort();
        this.readID = in.readInt();
        
        this.fullLine = new byte[ID_BYTES];
        this.fullLine[0] = (byte) ((this.fileID >> 8) & 0xff);
        this.fullLine[1] = (byte) (this.fileID & 0xff);
        
        this.fullLine[2] = (byte) ((this.readID >> 24) & 0xff);
        this.fullLine[3] = (byte) ((this.readID >> 16) & 0xff);
        this.fullLine[4] = (byte) ((this.readID >> 8) & 0xff);
        this.fullLine[5] = (byte) (this.readID & 0xff);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeShort(this.fileID);
        out.writeInt(this.readID);
    }
    
    /**
     * Returns true iff
     */
    @Override
    public boolean equals(Object o) {
        if (o instanceof MultiFileReadIDWritable) {
            return super.equals(o);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
    
    @Override
    public String toString() {
        return this.fileID + ":" + this.readID;
    }

    @Override
    public int getLength() {
        return this.fullLine.length;
    }

    @Override
    public byte[] getBytes() {
        return this.fullLine;
    }
    
    /** A Comparator optimized for MultiFileReadIDWritable. */ 
    public static class Comparator extends WritableComparator {

        public Comparator() {
            super(MultiFileReadIDWritable.class);
        }

        /**
         * Compare the buffers in serialized form.
         */
        public int compare(byte[] b1, int s1, int l1,
                byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1, l1,
                    b2, s2, l2);
        }
    }

    static {
        // register this comparator
        WritableComparator.define(MultiFileReadIDWritable.class, new Comparator());
    }
}
