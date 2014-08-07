package edu.arizona.cs.mrpkm.kmeridx.types;

import edu.arizona.cs.mrpkm.utils.SequenceHelper;
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
public class MultiFileCompressedSequenceWritable extends BinaryComparable implements WritableComparable<BinaryComparable> {

    private static final Log LOG = LogFactory.getLog(MultiFileCompressedSequenceWritable.class);
    
    private int fileID;
    private byte[] compressedSequence;
    private byte[] fullLine;
    private int seqLength;
    
    private static final int LENGTH_BYTES = 1;
    private static final int ID_BYTES = 2;
    
    public MultiFileCompressedSequenceWritable() {}
    
    public MultiFileCompressedSequenceWritable(int fileID, String sequence) throws IOException { set(fileID, sequence); }
    
    public MultiFileCompressedSequenceWritable(int fileID, byte[] compressedSequence, int seqLength) { set(fileID, compressedSequence, seqLength); }
    
    /**
     * Set the value.
     */
    public void set(int fileID, byte[] compressedSequence, int seqLength) {
        this.fileID = fileID;
        this.compressedSequence = compressedSequence;
        this.seqLength = seqLength;
        
        this.fullLine = new byte[this.compressedSequence.length + ID_BYTES];
        this.fullLine[0] = (byte) ((this.fileID >> 8) & 0xff);
        this.fullLine[1] = (byte) (this.fileID & 0xff);
        System.arraycopy(this.compressedSequence, 0, this.fullLine, ID_BYTES, this.compressedSequence.length);
    }
    
    public void set(int fileID, String sequence) throws IOException {
        this.fileID = fileID;
        this.compressedSequence = SequenceHelper.compress(sequence);
        this.seqLength = sequence.length();
        
        this.fullLine = new byte[this.compressedSequence.length + ID_BYTES];
        this.fullLine[0] = (byte) ((this.fileID >> 8) & 0xff);
        this.fullLine[1] = (byte) (this.fileID & 0xff);
        System.arraycopy(this.compressedSequence, 0, this.fullLine, ID_BYTES, this.compressedSequence.length);
    }

    /**
     * Return the value.
     */
    public int getFileID() {
        return this.fileID;
    }
    
    public byte[] getCompressedSequence() {
        return this.compressedSequence;
    }
    
    public String getSequence() {
        return SequenceHelper.decompress(this.compressedSequence, this.seqLength);
    }
    
    public int getSequenceLength() {
        return this.seqLength;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.seqLength = in.readByte();
        this.fileID = in.readShort();
        int byteLen = SequenceHelper.getCompressedSize(this.seqLength);
        this.compressedSequence = new byte[byteLen];
        in.readFully(this.compressedSequence, 0, byteLen);
        
        this.fullLine = new byte[this.compressedSequence.length + ID_BYTES];
        this.fullLine[0] = (byte) ((this.fileID >> 8) & 0xff);
        this.fullLine[1] = (byte) (this.fileID & 0xff);
        System.arraycopy(this.compressedSequence, 0, this.fullLine, ID_BYTES, this.compressedSequence.length);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(this.seqLength);
        out.writeShort(this.fileID);
        out.write(this.compressedSequence);
    }
    
    /**
     * Returns true iff
     */
    @Override
    public boolean equals(Object o) {
        if (o instanceof MultiFileCompressedSequenceWritable) {
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
        return this.fileID + ":" + SequenceHelper.decompress(this.compressedSequence, this.seqLength);
    }

    @Override
    public int getLength() {
        return this.fullLine.length;
    }

    @Override
    public byte[] getBytes() {
        return this.fullLine;
    }
    
    /** A Comparator optimized for CompressedFastaSequenceWritable. */ 
    public static class Comparator extends WritableComparator {

        public Comparator() {
            super(MultiFileCompressedSequenceWritable.class);
        }

        /**
         * Compare the buffers in serialized form.
         */
        public int compare(byte[] b1, int s1, int l1,
                byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1 + LENGTH_BYTES, l1 - LENGTH_BYTES,
                    b2, s2 + LENGTH_BYTES, l2 - LENGTH_BYTES);
        }
    }

    static {
        // register this comparator
        WritableComparator.define(MultiFileCompressedSequenceWritable.class, new Comparator());
    }
}
