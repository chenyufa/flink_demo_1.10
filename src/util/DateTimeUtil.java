package util;

import java.lang.reflect.Method;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ date: 2020/04/17 11:33
 * @ author: Cheney
 * @ description: 时间工具类
 */
public class DateTimeUtil {

    public static void main(String[] args) {
        System.out.println( stampToDate("1587024675000","yyyyMMdd HH") );
    }

    /**
     * 获取一段时间内的日期
     * 步伐类型 type:day,month
     * 步伐长度 size
     */
    public static HashSet<Date> findDates(Date dBegin, Date dEnd, String type, int size) {
        HashSet lDate = new HashSet();
        Calendar calBegin = Calendar.getInstance();
        calBegin.setTime(dBegin);
        Calendar calEnd = Calendar.getInstance();
        calEnd.setTime(dEnd);
        if("month".equals(type)) { //设置为当月第一天
            calBegin.set(Calendar.DATE, 1);
            calEnd.set(Calendar.DATE,1);
        }else if("week".equals(type)){
            if(calBegin.get(calBegin.DAY_OF_WEEK) == 1){//处理周日是第一天的问题
                calBegin.add(calBegin.DATE,-1);
            }
            if(calEnd.get(calEnd.DAY_OF_WEEK) == 1){
                calEnd.add(calEnd.DATE,-1);
            }
            calBegin.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
            lDate.add(calBegin.getTime());
        }
        while (dEnd.after(calBegin.getTime())) {
            lDate.add(calBegin.getTime());
            if("day".equals(type)){
                calBegin.add(Calendar.DATE, size);
            }else if("month".equals(type)){
                calBegin.set(Calendar.DATE,1);
                calBegin.add(Calendar.MONTH, size);
            }else if("year".equals(type)){
                calBegin.add(Calendar.YEAR, size);
            }else if("week".equals(type)){
                calBegin.add(Calendar.DATE, size);
            }
        }
        if("month".equals(type) || "day".equals(type)) {
            lDate.add(calEnd.getTime());
        }
        if("week".equals(type)){
            calEnd.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
            lDate.add(calEnd.getTime());
        }
        return  lDate;
    }

    /**
     * 获取一段时间内的日期
     * 步伐类型 type:day,month
     * 步伐长度 size
     */
    public static HashSet<String> findDates(String beginSTr, String endStr, String type, int size) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date dBegin = new Date();
        Date dEnd = new Date();
        try {
            dBegin = sdf.parse(beginSTr);
            dEnd = sdf.parse(endStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        HashSet lDate = new LinkedHashSet<String>();
        Calendar calBegin = Calendar.getInstance();
        calBegin.setTime(dBegin);
        Calendar calEnd = Calendar.getInstance();
        calEnd.setTime(dEnd);
        if("month".equals(type)) { //设置为当月第一天
            calBegin.set(Calendar.DATE, 1);
            calEnd.set(Calendar.DATE,1);
        }else if("week".equals(type)){
            if(calBegin.get(calBegin.DAY_OF_WEEK) == 1){//处理周日是第一天的问题
                calBegin.add(calBegin.DATE,-1);
            }
            if(calEnd.get(calEnd.DAY_OF_WEEK) == 1){
                calEnd.add(calEnd.DATE,-1);
            }
            calBegin.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
            lDate.add(sdf.format(calBegin.getTime()));
        }
        while (dEnd.after(calBegin.getTime())) {
            lDate.add(sdf.format(calBegin.getTime()));
            if("day".equals(type)){
                calBegin.add(Calendar.DATE, size);
            }else if("month".equals(type)){
                calBegin.set(Calendar.DATE,1);
                calBegin.add(Calendar.MONTH, size);
            }else if("year".equals(type)){
                calBegin.add(Calendar.YEAR, size);
            }else if("week".equals(type)){
                calBegin.add(Calendar.DATE, size);
            }
        }
        if("month".equals(type) || "day".equals(type)) {
            lDate.add(sdf.format(calEnd.getTime()));
        }
        if("week".equals(type)){
            calEnd.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
            lDate.add(sdf.format(calEnd.getTime()));
        }
        return  lDate;
    }

    /**
     * 指定格式返回当前时间的前后N个月的1号,
     * @param day 指定日期前后点
     * @param beforeNum 加减多少个月
     * @param formatStr 返回时间格式
     * @return 返回日期时间格式
     */
    public  static  String getFistMonthStrByDay(int beforeNum,String formatStr,String day){
        SimpleDateFormat format = new SimpleDateFormat(formatStr);
        if(day == null || "".equals(day.trim())){
            day = getDateStr(0,"yyyyMMdd");
        }
        Date d;
        Calendar c = Calendar.getInstance();
        try {
            d = format.parse(day);
            c.setTime(d);
            c.add(Calendar.MONTH, beforeNum);
            c.set(Calendar.DAY_OF_MONTH,1);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Date dayTime = c.getTime();
        return format.format(dayTime);
    }

    /**
     * 指定格式返回当前时间的前后N个月的最后一天,
     * @param day 指定日期前后点
     * @param beforeNum 加减多少个月
     * @param formatStr 返回时间格式
     * @return 返回日期时间格式
     */
    public  static  String getLastMonthStrByDay(int beforeNum,String formatStr,String day){
        SimpleDateFormat format = new SimpleDateFormat(formatStr);
        if(day == null || "".equals(day.trim())){
            day = getDateStr(0,"yyyyMMdd");
        }
        Date d;
        Calendar c = Calendar.getInstance();
        try {
            d = format.parse(day);
            c.setTime(d);
            c.add(Calendar.MONTH, beforeNum+1);
            c.set(Calendar.DAY_OF_MONTH,0);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Date dayTime = c.getTime();
        return format.format(dayTime);
    }

    /**
     * 指定格式返回当前时间的前后N个天的周一,
     * @param day 指定日期前后点
     * @param beforeNum 加减多少个月
     * @param formatStr 返回时间格式
     * @return 返回日期时间格式
     */
    public  static  String getFistWeekStrByDay(int beforeNum,String formatStr,String day){
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        if(day == null || "".equals(day.trim())){
            day = getDateStr(0,"yyyyMMdd");
        }
        Date d;
        Calendar c = Calendar.getInstance();
        try {
            d = format.parse(day);
            c.setTime(d);
            c.add(Calendar.DATE, beforeNum);
            c.set(Calendar.DAY_OF_WEEK,Calendar.MONDAY);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Date dayTime = c.getTime();
        return format.format(dayTime);
    }

    /**
     * 获取对应日期
     * @param startDate 传入的日期 yyyyMMdd
     * @param num num<=7：对应周几,num=8:本月第一天,num=9:本月最后一天
     * @return 对应日期
     */
    public static String getBegEndDate(Integer startDate,Integer num){
        String reNum = "20991231";
        SimpleDateFormat simpleFormat = new SimpleDateFormat("yyyyMMdd");
        Calendar calendar = Calendar.getInstance();
        try {
            calendar.setTime(simpleFormat.parse(""+startDate));
            if(num >=1 && num <=7){
                //西方习惯,周日为一周的开始一天,中国人则以周一为起始
                calendar.set(Calendar.DAY_OF_WEEK,num+1);
                //所以当获取周末时需要加上7
                if(num == 7){
                    calendar.add(Calendar.DATE,7);
                }
            }else if(num == 8){//获取日期的当月第一天
                calendar.set(Calendar.DAY_OF_MONTH,1);//本月第一天
            }else if(num == 9){
                calendar.add(Calendar.MONTH, 1);    //加一个月
                calendar.set(Calendar.DAY_OF_MONTH,1);//加一个月后的第一天
                calendar.add(Calendar.DATE, -1);    //再减一天即为本月最后一天
            }
            Date date = new Date();
            date.setTime(calendar.getTimeInMillis());
            reNum = simpleFormat.format(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return reNum;
    }

    /**
     * 指定格式返回当前时间的前后N天,
     * @param day 指定日期前后点
     * @param beforeNum 加减多少天前
     * @param formatStr 返回时间格式
     * @return 返回日期时间格式
     */
    public  static  String getDateStrByDay(int beforeNum,String formatStr,String day){
        SimpleDateFormat format = new SimpleDateFormat(formatStr);
        if(day == null || "".equals(day.trim())){
            day = getDateStr(0,"yyyyMMdd");
        }
        Date d = new Date();
        try {
            d = format.parse(day);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Calendar c = Calendar.getInstance();
        c.setTime(d);
        c.add(Calendar.DATE, beforeNum);
        Date dayTime = c.getTime();
        return format.format(dayTime);
    }

    /**
     * 指定格式返回当前时间的前后N天,
     * @param beforeNum 加减多少天前
     * @param formatStr 返回时间格式
     * @return 返回日期时间格式
     */
    public  static  String getDateStr(int beforeNum,String formatStr){
        SimpleDateFormat format = new SimpleDateFormat(formatStr);
        Calendar c = Calendar.getInstance();
        c.setTime(new Date());
        c.add(Calendar.DATE, beforeNum);
        Date d = c.getTime();
        return format.format(d);
    }

    /**
     * 指定格式返回当前时间的前后N小时,
     * @param beforeNum 加减多少小时前
     * @param formatStr 返回时间格式
     * @return 返回日期时间格式
     */
    public  static  String getDateStrByHour(int beforeNum,String formatStr){
        SimpleDateFormat format = new SimpleDateFormat(formatStr);
        Calendar c = Calendar.getInstance();
        c.setTime(new Date());
        c.add(Calendar.HOUR, beforeNum);
        Date d = c.getTime();
        return format.format(d);
    }

    /**
     * 指定格式返回当前时间的前后N分钟,
     * @param beforeNum 加减多少分钟前
     * @param formatStr 返回时间格式
     * @return 返回日期时间格式
     */
    public  static  String getDateStrByMINUTE(int beforeNum,String formatStr){
        SimpleDateFormat format = new SimpleDateFormat(formatStr);
        Calendar c = Calendar.getInstance();
        c.setTime(new Date());
        c.add(Calendar.MINUTE, beforeNum);
        Date d = c.getTime();
        return format.format(d);
    }

    /**
     * @return 获取当前时间的下一个十五分钟刻度
     */
    public static String getNext15Minute(){
        SimpleDateFormat format = new SimpleDateFormat("HH:mm");
        Calendar c = Calendar.getInstance();
        Date d = c.getTime();
        String[] hourMinute = format.format(d).split(":");
        String hour = hourMinute[0];
        int minute = Integer.parseInt(hourMinute[1]);
        String resHour = hour;
        String resMinute = "";
        if(minute>=0 && minute<=14){
            resMinute = "15";
        }else if(minute>=15 && minute<=29){
            resMinute = "30";
        }else if(minute>=30 && minute<=44){
            resMinute = "45";
        }else if(minute>=45 && minute<=59){
            String[] newHour = getDateStrByMINUTE(15,"HH:mm").split(":");
            resHour = newHour[0];
            resMinute = "00";
        }
        return resHour+":"+resMinute;
    }

    /**
     * 利用正则表达式判断字符串是否是数字
     * @param str 校验字符串
     * @return 是否数字
     */
    public static boolean isNumeric(String str){
        Pattern pattern = Pattern.compile("[0-9]*");
        Matcher isNum = pattern.matcher(str);
        if( !isNum.matches() ){
            return false;
        }
        return true;
    }

    /**
     * 将【毫秒级时间戳】转换为【指定格式】的时间
     * @param s 毫秒级时间戳
     * @param formatStr 指定格式
     * @return 可读性的时间字符串
     */
    public static String stampToDate(String s,String formatStr){
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(formatStr);
        long lt = new Long(s);
        Date date = new Date(lt);
        res = simpleDateFormat.format(date);
        return res;
    }

}
