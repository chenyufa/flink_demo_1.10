package main;

import entity.MessageInfo;
import function.MyKeyedProcessFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ date: 2020/04/14 16:18
 * @ author: Cheney
 * @ description: 机器告警监控
 */
public class KeyedProcessFunctionMonitor {

    /**
     * 测试方法：
     * mac命令窗口执行 nc -l 9110
     * windows nc -l -p 9110
     *
     * 192.168.1.101,2020-04-07 21:00,RUNNING
     * 192.168.1.101,2020-04-07 21:02,DEAD
     * 192.168.1.102,2020-04-07 22:03,DEAD
     * 192.168.1.101,2020-04-07 22:03,DEAD
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception{
        // 创建 execution environment
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        /*设置使用EventTime作为Flink的时间处理标准，不指定默认是ProcessTime*/
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //这里为了便于理解，设置并行度为1,默认并行度是当前机器的cpu数量
        senv.setParallelism(1);
        /*指定数据源 从socket的9000端口接收数据，先进行了不合法数据的过滤*/
        DataStream<String> sourceDS = senv.socketTextStream("127.0.0.1", 9110)
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String line) throws Exception {
                        if (null == line || "".equals(line)) {
                            return false;
                        }
                        String[] lines = line.split(",");
                        if (lines.length != 3) {
                            return false;
                        }
                        return true;
                    }
                });

        /*做了一个简单的map转换，将数据转换成MessageInfo格式，第一个字段代表是主机IP，第二个字段的代表的是消息时间，第三个字段是Regionserver状态*/
        DataStream<String> warningDS = sourceDS.map(new MapFunction<String, MessageInfo>() {
            @Override
            public MessageInfo map(String line) throws Exception {
                String[] lines = line.split(",");
                return new MessageInfo(lines[0], lines[1],lines[2]);
            }
        }).keyBy(new KeySelector<MessageInfo, String>() {
            @Override
            public String getKey(MessageInfo value) throws Exception {
                return value.getHostName();
            }
        }).process(new MyKeyedProcessFunction());


        /*打印报警信息*/
        //warningDS.print();

        senv.execute();
    }

}
