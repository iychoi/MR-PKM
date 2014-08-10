package edu.arizona.cs.mrpkm.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
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
public class CompressedIntArrayWritable extends BinaryComparable implements WritableComparable<BinaryComparable> {

    private static final Log LOG = LogFactory.getLog(CompressedIntArrayWritable.class);
    
    private int[] intArray;
    
    private byte[] prevBytes;
    
    public CompressedIntArrayWritable() {}
    
    public CompressedIntArrayWritable(int[] intArray) { set(intArray); }
    
    public CompressedIntArrayWritable(List<Integer> intArray) { set(intArray); }
    
    /**
     * Set the value.
     */
    public void set(int[] intArray) {
        this.intArray = intArray;
        this.prevBytes = null;
    }
    
    public void set(List<Integer> intArray) {
        int[] arr = new int[intArray.size()];
        for(int i=0;i<intArray.size();i++) {
            arr[i] = intArray.get(i);
        }
        this.intArray = arr;
        this.prevBytes = null;
    }

    /**
     * Return the value.
     */
    public int[] get() {
        return this.intArray;
    }
    
    private byte makeFlag(int count, int[] arr) throws IOException {
        byte flag = 0;
        // size
        if(count <= Byte.MAX_VALUE) {
            flag = 0x00;
        } else if(count <= Short.MAX_VALUE) {
            flag = 0x01;
        } else if(count <= Integer.MAX_VALUE) {
            flag = 0x02;
        } else {
            throw new IOException("Array size overflow");
        }
        
        // datatype
        int max = 0;
        for(int i=0;i<arr.length;i++) {
            max = Math.max(max, Math.abs(arr[i]));
        }
        
        if(max <= Byte.MAX_VALUE) {
            flag |= 0x00;
        } else if(max <= Short.MAX_VALUE) {
            flag |= 0x10;
        } else if(max <= Integer.MAX_VALUE) {
            flag |= 0x20;
        } else {
            throw new IOException("MaxValue overflow");
        }
        
        return flag;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        byte flag = in.readByte();
        int count = 0;
        if((flag & 0x0f) == 0x00) {
            count = in.readByte();
        } else if((flag & 0x0f) == 0x01) {
            count = in.readShort();
        } else if((flag & 0x0f) == 0x02) {
            count = in.readInt();
        } else {
            throw new IOException("unhandled flag");
        }
        
        int[] arr = new int[count];
        if((flag & 0xf0) == 0x00) {
            for (int i = 0; i < count; i++) {
                arr[i] = in.readByte();
            }
        } else if((flag & 0xf0) == 0x10) {
            for (int i = 0; i < count; i++) {
                arr[i] = in.readShort();
            }
        } else if((flag & 0xf0) == 0x20) {
            for (int i = 0; i < count; i++) {
                arr[i] = in.readInt();
            }
        } else {
            throw new IOException("unhandled flag");
        }
        
        this.intArray = arr;
        this.prevBytes = null;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int count = this.intArray.length;
        byte flag = makeFlag(count, this.intArray);
        out.writeByte(flag);
        
        if((flag & 0x0f) == 0x00) {
            out.writeByte(count);
        } else if((flag & 0x0f) == 0x01) {
            out.writeShort(count);
        } else if((flag & 0x0f) == 0x02) {
            out.writeInt(count);
        } else {
            throw new IOException("unhandled flag");
        }

        if((flag & 0xf0) == 0x00) {
            for (int i = 0; i < count; i++) {
                out.writeByte((byte)this.intArray[i]);
            }
        } else if((flag & 0xf0) == 0x10) {
            for (int i = 0; i < count; i++) {
                out.writeShort((short)this.intArray[i]);
            }
        } else if((flag & 0xf0) == 0x20) {
            for (int i = 0; i < count; i++) {
                out.writeInt((int)this.intArray[i]);
            }
        } else {
            throw new IOException("unhandled flag");
        }
    }
    
    /**
     * Returns true iff
     */
    @Override
    public boolean equals(Object o) {
        if (o instanceof CompressedIntArrayWritable) {
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
        String value = new String();
        
        for(int i=0;i<this.intArray.length;i++) {
            if(value.length() != 0) {
                value += ",";
            }
            value += this.intArray[i];
        }
        return value;
    }
    
    @Override
    public int getLength() {
        return this.intArray.length * 4;
    }

    @Override
    public byte[] getBytes() {
        if(this.prevBytes == null) {
            byte[] arr = new byte[this.intArray.length * 4];
            for(int i=0;i<this.intArray.length;i++) {
                int ivalue = this.intArray[i];
                arr[4*i] = (byte) ((ivalue >> 24) & 0xff);
                arr[4*i+1] = (byte) ((ivalue >> 16) & 0xff);
                arr[4*i+2] = (byte) ((ivalue >> 8) & 0xff);
                arr[4*i+3] = (byte) (ivalue & 0xff);
            }
            this.prevBytes = arr;
        }
        return prevBytes;
    }
    
    /** A Comparator optimized for IntArrayWritable. */ 
    public static class Comparator extends WritableComparator {

        public Comparator() {
            super(CompressedIntArrayWritable.class);
        }

        /**
         * Compare the buffers in serialized form.
         */
        @Override
        public int compare(byte[] b1, int s1, int l1,
                byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1, l1,
                    b2, s2, l2);
        }
    }

    static {
        // register this comparator
        WritableComparator.define(CompressedIntArrayWritable.class, new CompressedIntArrayWritable.Comparator());
    }
}
