package org.elasticsearch.river.kafka;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by luanmingming on 2018/1/11.
 */
public class TimeInterval {

    // 10min per time
    public static String minuteInterval() {
        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        String time = dateFormat.format(now);
        return time.substring(0, time.length() - 1) + "0";
    }

    // 1hour per time
    public static String hourInterval() {
        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH");
        return dateFormat.format(now);
    }

    // 1day per time
    public static String dayInterval() {
        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return dateFormat.format(now);
    }

    // 1month per time
    public static String monthInterval() {
        Date now = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM");
        return dateFormat.format(now);
    }

}
