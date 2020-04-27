package main;

import entity.ProductViewData;
import function.MyCountAggregate;
import function.MyCountWindowFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @ date: 2020/04/24 16:47
 * @ author: Cheney
 * @ description: 自定义聚合函数类和窗口类，进行商品访问量的统计，根据滑动时间窗口，按照访问量排序输出
 */
public class AggregateFunctionMain {

    public  static int windowSize=3000;/*滑动窗口大小*/

    public  static int windowSlider=2000;/*滑动窗口滑动间隔*/

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //DataStream<String> sourceData = senv.socketTextStream("127.0.0.1",9110,"\n");
        //从文件读取数据，也可以从socket读取数据
        DataStream<String> sourceData = senv.readTextFile("/temp/fd/productData.log");
        DataStream<ProductViewData> productViewData = sourceData.map( (MapFunction<String, ProductViewData>) value -> {
            String[] record = value.split(",");
            return new ProductViewData(record[0], record[1], Integer.valueOf(record[2]), Long.valueOf(record[3]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ProductViewData>(){
            @Override
            public long extractAscendingTimestamp(ProductViewData element) {
                long EventTime = element.getEventTime();//事件事件作为窗口时间
                //long ProcessTime = System.currentTimeMillis();//系统时间作为窗口时间
                return EventTime;
            }
        });
        /*过滤操作类型为1  点击查看的操作*/
        //时间窗口 6秒  滑动间隔3秒
        DataStream<String> productViewCount = productViewData.filter( (FilterFunction<ProductViewData>) value -> {
            if(value.getEventType()==1){
                return true;
            }
            return false;
        }).keyBy((KeySelector<ProductViewData, String>) value -> value.productId).timeWindow(Time.milliseconds(windowSize), Time.milliseconds(windowSlider))
                /*这里按照窗口进行聚合*/
                .aggregate(new MyCountAggregate(), new MyCountWindowFunction());

        //聚合结果输出
        productViewCount.print();

        senv.execute("AggregateFunctionMain2");

    }

}
