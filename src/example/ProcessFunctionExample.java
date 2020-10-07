package example;

import datasource.MyStreamDataSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * @ date: 2020/05/11 22:30
 * @ author: Cheney
 * @ description: ProcessFunction测试
 */
public class ProcessFunctionExample {

    //执行主类
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //指定时间类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<Tuple2<String, Long>> data = env.addSource(new MyStreamDataSource()).setParallelism(1)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Long, Long>>(Time.milliseconds(0)) {
                    Long currentMaxTimestamp = 0L;
                    //定义如何提取timestamp
                    @Override
                    public long extractTimestamp(Tuple3<String, Long, Long> input) {
                        currentMaxTimestamp = Math.max(input.f2, currentMaxTimestamp);
                        return input.f2;
                    }
                })
                .map(new MapFunction<Tuple3<String, Long, Long>, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Tuple3<String, Long, Long> input) throws Exception {
                        return new Tuple2<>(input.f0, input.f1);
                    }
                });

        //data.keyBy(0).process(new CountWithTimeoutProcessFunction()).print();

        //保存被丢弃的数据
        OutputTag<Tuple2<String, Long>> outputTag = new OutputTag<Tuple2<String, Long>>("late-data"){};


        /*延迟处理举例*/
        SingleOutputStreamOperator<Tuple2<String, Long>> resultStream = data.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10))) // 10秒的时间窗口
                .sideOutputLateData(outputTag) // 收集延迟大于2s的数据
                .allowedLateness(Time.seconds(2)) // 允许延迟处理2秒
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
                    }
                });
        //resultStream.print();

        //把迟到的数据暂时打印到控制台，实际中可以保存到其他存储介质中
        DataStream<Tuple2<String, Long>> sideOutput = resultStream.getSideOutput(outputTag);
        sideOutput.print();


        env.execute();

    }

}
