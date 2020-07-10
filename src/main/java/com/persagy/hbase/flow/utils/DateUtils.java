package com.persagy.hbase.flow.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * 时间处理工具包
 */
public class DateUtils {

    /**
     * 获取两个日期之间的月份差的集合，包含起始月份与结束月份
     * @param startDate
     * @param endDate
     * @return
     */
    public static List<String> getDateMonthDiff(String startDate, String endDate){

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMM");
        List<String> list = new ArrayList();

        Date d1 = null;
        Date d2 = null;

        try {
            d1 = simpleDateFormat.parse(getDateMonth(startDate));
            d2 = simpleDateFormat.parse(getDateMonth(endDate));

            Calendar c1 = Calendar.getInstance();
            Calendar c2 = Calendar.getInstance();


            list.add(getDateMonth(startDate));

            c1.setTime(d1);
            c2.setTime(d2);

            while (c1.compareTo(c2) < 0) {
                c1.add(Calendar.MONTH, 1);
                Date ss = c1.getTime();
                String str = simpleDateFormat.format(ss);
                list.add(str);
            }

        } catch (ParseException e) {
            e.printStackTrace();
        }

        return list;
    }

    /**
     * 截取到分钟
     * @param date
     * @return
     */
    public static String getDateMinute(String date){
        return date.substring(0, 12);
    }

    /**
     * 截取到小时
     * @param date
     * @return
     */
    public static String getDateHour(String date){
        return date.substring(0, 10);
    }

    /**
     * 截取到日期
     * @param date
     * @return
     */
    public static String getDateDay(String date){
        return date.substring(0, 8);
    }

    /**
     * 截取到月份
     * @param date
     * @return
     */
    public static String getDateMonth(String date){
        return date.substring(0, 6);
    }
}
