package edu.arizona.cs.mrpkm.utils;

import java.io.IOException;
import java.math.BigInteger;

/**
 *
 * @author iychoi
 */
public class SequenceHelper {
    private static char[] ComplimentCharLUT = {'T', ' ', 'G', ' ', ' ', ' ', 'C', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ',
    ' ', ' ', ' ', 'A', ' ', ' ', ' ', ' ', ' ', ' '};
    
    private static char[] convBitToCharLUT = {'A', 'C', 'G', 'T'};
    private static byte[] convCharToBitLUT = {0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
    
    public static char getCompliment(char ch) {
        return ComplimentCharLUT[((byte)ch) - 'A'];
    }
    
    public static String getCompliment(String sequence) {
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<sequence.length();i++) {
            sb.append(getCompliment(sequence.charAt(i)));
        }
        return sb.toString();
    }
    
    public static String getReverse(String sequence) {
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<sequence.length();i++) {
            sb.append(sequence.charAt(sequence.length() - i - 1));
        }
        return sb.toString();
    }
    
    public static String getReverseCompliment(String sequence) {
        StringBuilder sb = new StringBuilder();
        for(int i=0;i<sequence.length();i++) {
            sb.append(getCompliment(sequence.charAt(sequence.length() - i - 1)));
        }
        return sb.toString();
    }
    
    private static char convBitToChar(byte bits) {
        return convBitToCharLUT[bits];
    }
    
    private static byte convCharToBit(char ch) {
        return convCharToBitLUT[((byte)ch) - 'A'];
    }
    
    public static int getCompressedSize(String sequence) {
        return getCompressedSize(sequence.length());
    }
    
    public static int getCompressedSize(int sequenceLen) {
        int bytes = sequenceLen / 4;
        if(sequenceLen % 4 != 0) {
            bytes++;
        }
        return bytes;
    }
    
    public static byte[] compress(String sequence) throws IOException {
        int sequenceLen = sequence.length();
        int compressedByteLen = sequenceLen / 4;
        if(sequenceLen % 4 != 0) {
            compressedByteLen++;
        }
        
        byte[] compressedArr = new byte[compressedByteLen];
        
        for(int i=0;i<sequenceLen / 4;i++) {
            char cha = sequence.charAt(i*4);
            char chb = sequence.charAt(i*4+1);
            char chc = sequence.charAt(i*4+2);
            char chd = sequence.charAt(i*4+3);
            
            byte a = (byte) (convCharToBit(cha) << 6);
            byte b = (byte) (convCharToBit(chb) << 4);
            byte c = (byte) (convCharToBit(chc) << 2);
            byte d = convCharToBit(chd);
            
            byte bits = (byte) (a | b | c | d);
            
            compressedArr[i] = bits;
        }
        
        int start = (sequenceLen / 4) * 4;
        int left = sequenceLen % 4;
        if(left > 0) {
            char cha = sequence.charAt(start);
            byte a = (byte) (convCharToBit(cha) << 6);
            byte b = 0;
            byte c = 0;
            if(left > 1) {
                char chb = sequence.charAt(start+1);
                b = (byte) (convCharToBit(chb) << 4);
                if(left > 2) {
                    char chc = sequence.charAt(start+2);
                    c = (byte) (convCharToBit(chc) << 2);
                }
            }
            
            byte bits = (byte) (a | b | c);
            compressedArr[sequenceLen / 4] = bits;
        }
        
        return compressedArr;
    }
    
    public static String decompress(byte[] compressed, int sequenceLen) {
        byte[] byteArr = new byte[sequenceLen];
        
        int curRead = 0;
        
        for(int i=0;i<compressed.length;i++) {
            byte bits = compressed[i];
            byte a = (byte)((bits >> 6) & 0x3);
            byte b = (byte)((bits >> 4) & 0x3);
            byte c = (byte)((bits >> 2) & 0x3);
            byte d = (byte)(bits & 0x3);
            
            char cha = convBitToChar(a);
            char chb = convBitToChar(b);
            char chc = convBitToChar(c);
            char chd = convBitToChar(d);

            if(curRead < sequenceLen) {
                byteArr[curRead] = (byte) cha;
                curRead++;
            }
            if(curRead < sequenceLen) {
                byteArr[curRead] = (byte) chb;
                curRead++;
            }
            if(curRead < sequenceLen) {
                byteArr[curRead] = (byte) chc;
                curRead++;
            }
            if(curRead < sequenceLen) {
                byteArr[curRead] = (byte) chd;
                curRead++;
            }
        }
        
        return new String(byteArr);
    }
    
    public static BigInteger convertToBigInteger(String sequence) {
        BigInteger biSequence = BigInteger.ZERO;
        int kmerSize = sequence.length();
        for(int i=0;i<kmerSize;i++) {
            char ch = sequence.charAt(i);
            if(ch == 'A') {
                biSequence = biSequence.add(BigInteger.valueOf(0));
            } else if(ch == 'C') {
                biSequence = biSequence.add(BigInteger.valueOf(1));
            } else if(ch == 'G') {
                biSequence = biSequence.add(BigInteger.valueOf(2));
            } else if(ch == 'T') {
                biSequence = biSequence.add(BigInteger.valueOf(3));
            }
            
            if(i < kmerSize - 1) {
                biSequence = biSequence.multiply(BigInteger.valueOf(4));
            }
        }
        
        return biSequence;
    }
    
    public static String convertToString(BigInteger biSequence, int kmerSize) {
        String str = "";
        for(int i=0;i<kmerSize;i++) {
            int idx = biSequence.mod(BigInteger.valueOf(4)).intValue();
            if(idx == 0) {
                str = "A" + str;
            } else if(idx == 1) {
                str = "C" + str;
            } else if(idx == 2) {
                str = "G" + str;
            } else if(idx == 3) {
                str = "T" + str;
            }
            biSequence = biSequence.divide(BigInteger.valueOf(4));
        }
        
        return str;
    }
}