package edu.arizona.cs.mrpkm.helpers;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author iychoi
 */
public class RunningTimeHelper {
    public static long getCurrentTime() {
        return System.currentTimeMillis();
    }
    
    public static String getDiffTimeString(long begin, long end) {
        long diff = end - begin;
        long remain = diff;
        
        int msec = (int) (remain % 1000);
        remain /= 1000;
        int sec = (int) (remain % 60);
        remain /= 60;
        int min = (int) (remain % 60);
        remain /= 60;
        int hour = (int) (remain);
        
        return hour + "h " + min + "m " + sec + "s";
    }
    
    public static String getTimeString(long time) {
        Date date = new Date(time);
        Format format = new SimpleDateFormat("yyyy MM dd HH:mm:ss");
        return format.format(date);
    }
}
