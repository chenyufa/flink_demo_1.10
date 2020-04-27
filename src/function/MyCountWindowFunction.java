package function;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import util.DateTimeUtil;

/**
 * @ date: 2020/04/24 16:39
 * @ author: Cheney
 * @ description:
 *   * 自定义窗口函数，封装成字符串
 *   * 第一个参数是上面 MyCountAggregate 的输出，就是商品的访问量统计
 *   * 第二个参数 输出 这里为了演示 简单输出字符串
 *   * 第三个就是 窗口类 能获取窗口结束时间
 */
public class MyCountWindowFunction implements WindowFunction<Long,String,String, TimeWindow> {

    @Override
    public void apply(String productId, TimeWindow timeWindow, Iterable<Long> input, Collector<String> collector) throws Exception {
        /*商品访问统计输出*/
        long time = timeWindow.getEnd();
        String timeFormat = DateTimeUtil.stampToDate(String.valueOf(time),"yyyyMMdd HH:mm:ss");
        collector.collect("----------------窗口时间："+timeFormat+" ----------------------");
        collector.collect("商品ID: "+productId+"  浏览量: "+input.iterator().next());
        collector.collect("                                                        ");
    }

}
