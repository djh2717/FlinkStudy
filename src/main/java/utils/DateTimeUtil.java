package utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * @Author Djh on  2021/6/28 16:40
 * @E-Mail 1544579459.djh@gmail.com
 */
public class DateTimeUtil {


    /**
     * 用于分区dt字段.
     */
    public static String getNDayAgo(int days) {
        LocalDate now = LocalDate.now();
        LocalDate nDayAgo = now.minusDays(days);
        return nDayAgo.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
    }

    /**
     * 返回格式为 2021-07-12 用于 time_value 字段的.
     */
    public static String getNDayAgoValue(int days) {
        LocalDate now = LocalDate.now();
        LocalDate nDayAgo = now.minusDays(days);
        return nDayAgo.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    }

    public static long convertToLongTime(String timeValue) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        try {
            return simpleDateFormat.parse(timeValue).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return 0;
    }

    public static String convertStringTime(long timeValue) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return simpleDateFormat.format(timeValue);
    }

}
