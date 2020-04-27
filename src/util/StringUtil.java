package util;

import java.lang.reflect.Method;


/**
 * @ date: 2020/04/17 11:33
 * @ author: Cheney
 * @ description: 字符串工具类
 */
public class StringUtil {

    public static void main(String[] args) {

    }

    /**
     * 使用反射根据属性名称获取对象的属性值
     * @param fieldName 属性名称
     * @param o 对象
     * @return
     */
    public static Object getFieldValueByName(String fieldName, Object o){
        Object value;
        try {
            String firstLetter = fieldName.substring(0, 1).toUpperCase();
            String getter = "get" + firstLetter + fieldName.substring(1);
            Method method = o.getClass().getMethod(getter, new Class[] {});
            value = method.invoke(o, new Object[] {});
        }catch (Exception e){
            e.printStackTrace();
            value = 0;
        }
        return value;
    }






}
