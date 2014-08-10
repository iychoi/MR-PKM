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
public class MultiFileOffsetWritable extends BinaryComparable implements WritableComparable<BinaryComparable> {

    private static final Log LOG = LogFactory.getLog(MultiFileOffsetWritable.class);
    
    private int fileID;
    private long offset;
    private byte[] fullLine;
    
    private static final int ID_BYTES = 2+8;
    
    public MultiFileOffsetWritable() {}
    
    public MultiFileOffsetWritable(int fileID, long offset) throws IOException { set(fileID, offset); }
    
    /**
     * Set the value.
     */
    public void set(int fileID, long offset) {
        this.fileID = fileID;
        this.offset = offset;
        
        this.fullLine = new byte[ID_BYTES];
        this.fullLine[0] = (byte) ((this.fileID >> 8) & 0xff);
        this.fullLine[1] = (byte) (this.fileID & 0xff);
        
        this.fullLine[2] = (byte) ((this.offset >> 56) & 0xff);
        this.fullLine[3] = (byte) ((this.offset >> 48) & 0xff);
        this.fullLine[4] = (byte) ((this.offset >> 40) & 0xff);
        this.fullLine[5] = (byte) ((this.offset >> 32) & 0xff);
        this.fullLine[6] = (byte) ((this.offset >> 24) & 0xff);
        this.fullLine[7] = (byte) ((this.offset >> 16) & 0xff);
        this.fullLine[8] = (byte) ((this.offset >> 8) & 0xff);
        this.fullLine[9] = (byte) (this.offset & 0xff);
    }
    
    /**
     * Return the value.
     */
    public int getFileID() {
        return this.fileID;
    }
    
    public long getOffset() {
        return this.offset;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        this.fileID = in.readShort();
        this.offset = in.readLong();
        
        this.fullLine = new byte[ID_BYTES];
        this.fullLine[0] = (byte) ((this.fileID >> 8) & 0xff);
        this.fullLine[1] = (byte) (this.fileID & 0xff);
        
        this.fullLine[2] = (byte) ((this.offset >> 56) & 0xff);
        this.fullLine[3] = (byte) ((this.offset >> 48) & 0xff);
        this.fullLine[4] = (byte) ((this.offset >> 40) & 0xff);
        this.fullLine[5] = (byte) ((this.offset >> 32) & 0xff);
        this.fullLine[6] = (byte) ((this.offset >> 24) & 0xff);
        this.fullLine[7] = (byte) ((this.offset >> 16) & 0xff);
        this.fullLine[8] = (byte) ((this.offset >> 8) & 0xff);
        this.fullLine[9] = (byte) (this.offset & 0xff);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeShort(this.fileID);
        out.writeLong(this.offset);
    }
    
    /**
     * Returns true iff
     */
    @Override
    public boolean equals(Object o) {
        if (o instanceof MultiFileOffsetWritable) {
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
        return this.fileID + ":" + this.offset;
    }

    @Override
    public int getLength() {
        return this.fullLine.length;
    }

    @Override
    public byte[] getBytes() {
        return this.fullLine;
    }
    
    /** A Comparator optimized for MultiFileOffsetWritable. */ 
    public static class Comparator extends WritableComparator {

        public Comparator() {
            super(MultiFileOffsetWritable.class);
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
        WritableComparator.define(MultiFileOffsetWritable.class, new Comparator());
    }
}
