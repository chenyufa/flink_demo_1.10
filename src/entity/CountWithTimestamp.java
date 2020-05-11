package entity;

import java.io.Serializable;
import java.util.Date;

/**
 * @ date: 2020/05/11 22:24
 * @ author: Cheney
 * @ description: 单词计数
 */
public class CountWithTimestamp implements Serializable {

    //单词
    public String key;
    //单词计数
    public long count;
    //最近更新时间
    public long lastModified;

    @Override
    public String toString() {
        return "CountWithTimestamp{" +
                "key='" + key + '\'' +
                ", count=" + count +
                ", lastModified=" + new Date(lastModified) +
                '}';
    }

}
