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
public class CompressedLongArrayWritable extends BinaryComparable implements WritableComparable<BinaryComparable> {

    private static final Log LOG = LogFactory.getLog(CompressedLongArrayWritable.class);
    
    private long[] longArray;
    
    private byte[] prevBytes;
    
    public CompressedLongArrayWritable() {}
    
    public CompressedLongArrayWritable(long[] longArray) { set(longArray); }
    
    public CompressedLongArrayWritable(List<Long> longArray) { set(longArray); }
    
    /**
     * Set the value.
     */
    public void set(long[] longArray) {
        this.longArray = longArray;
        this.prevBytes = null;
    }
    
    public void set(List<Long> longArray) {
        long[] arr = new long[longArray.size()];
        for(int i=0;i<longArray.size();i++) {
            arr[i] = longArray.get(i);
        }
        this.longArray = arr;
        this.prevBytes = null;
    }
    
    public void set(CompressedLongArrayWritable that) {
        this.longArray = that.longArray;
        this.prevBytes = that.prevBytes;
    }

    /**
     * Return the value.
     */
    public long[] get() {
        return this.longArray;
    }
    
    private byte makeFlag(int count, long[] arr) throws IOException {
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
        long max = 0;
        for(int i=0;i<arr.length;i++) {
            max = Math.max(max, Math.abs(arr[i]));
        }
        
        if(max <= Byte.MAX_VALUE) {
            flag |= 0x00;
        } else if(max <= Short.MAX_VALUE) {
            flag |= 0x10;
        } else if(max <= Integer.MAX_VALUE) {
            flag |= 0x20;
        } else if(max <= Long.MAX_VALUE) {
            flag |= 0x30;
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
        
        long[] arr = new long[count];
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
        } else if((flag & 0xf0) == 0x30) {
            for (int i = 0; i < count; i++) {
                arr[i] = in.readLong();
            }
        } else {
            throw new IOException("unhandled flag");
        }
        
        this.longArray = arr;
        this.prevBytes = null;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int count = this.longArray.length;
        byte flag = makeFlag(count, this.longArray);
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
                out.writeByte((byte)this.longArray[i]);
            }
        } else if((flag & 0xf0) == 0x10) {
            for (int i = 0; i < count; i++) {
                out.writeShort((short)this.longArray[i]);
            }
        } else if((flag & 0xf0) == 0x20) {
            for (int i = 0; i < count; i++) {
                out.writeInt((int)this.longArray[i]);
            }
        } else if((flag & 0xf0) == 0x30) {
            for (int i = 0; i < count; i++) {
                out.writeLong(this.longArray[i]);
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
        if (o instanceof CompressedLongArrayWritable) {
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
        
        for(int i=0;i<this.longArray.length;i++) {
            if(value.length() != 0) {
                value += ",";
            }
            value += this.longArray[i];
        }
        return value;
    }
    
    @Override
    public int getLength() {
        return this.longArray.length * 8;
    }

    @Override
    public byte[] getBytes() {
        if(this.prevBytes == null) {
            byte[] arr = new byte[this.longArray.length * 8];
            for(int i=0;i<this.longArray.length;i++) {
                long ivalue = this.longArray[i];
                arr[4*i] = (byte) ((ivalue >> 56) & 0xff);
                arr[4*i+1] = (byte) ((ivalue >> 48) & 0xff);
                arr[4*i+2] = (byte) ((ivalue >> 40) & 0xff);
                arr[4*i+3] = (byte) ((ivalue >> 32) & 0xff);
                arr[4*i+4] = (byte) ((ivalue >> 24) & 0xff);
                arr[4*i+5] = (byte) ((ivalue >> 16) & 0xff);
                arr[4*i+6] = (byte) ((ivalue >> 8) & 0xff);
                arr[4*i+7] = (byte) (ivalue & 0xff);
            }
            this.prevBytes = arr;
        }
        return prevBytes;
    }
    
    /** A Comparator optimized for IntArrayWritable. */ 
    public static class Comparator extends WritableComparator {

        public Comparator() {
            super(CompressedLongArrayWritable.class);
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
        WritableComparator.define(CompressedLongArrayWritable.class, new CompressedLongArrayWritable.Comparator());
    }
}
