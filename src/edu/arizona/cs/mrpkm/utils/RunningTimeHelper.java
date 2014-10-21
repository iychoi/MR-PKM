package edu.arizona.cs.mrpkm.utils;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Calendar;
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
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(diff);
        return (c.get(Calendar.HOUR_OF_DAY) + "h " + c.get(Calendar.MINUTE) + "m " + c.get(Calendar.SECOND) + "." + c.get(Calendar.MILLISECOND) + "s");
    }
    
    public static String getTimeString(long time) {
        Date date = new Date(time);
        Format format = new SimpleDateFormat("yyyy MM dd HH:mm:ss");
        return format.format(date);
    }
}
