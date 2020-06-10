package main;

import com.alibaba.fastjson.JSON;
import entity.Student;
import org.apache.flink.api.common.functions.FlatMapFunction;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import util.Splitter;


/**
 * @ date: 2020/03/31 16:36
 * @ author: Cheney
 * @ description: java入口
 */
public class RunMainJava {

    public static void main(String[] args) throws Exception{
        //helloFlink();
        flatMapOperate();
        //mapOperate();
        //filterOperate();
    }

    /**
     * 基于 filter 的操作
     * filter 用于过滤数据(也可修改数据)
     * @throws Exception
     */
    public static void filterOperate() throws Exception{
        // 创建 execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // mac命令窗口执行 nc -l 9110
        // windows nc -l -p 9110
        DataStream<String> input = env.socketTextStream("192.168.1.199", 9110, "\n");

        //map 操作 json 转成实体对象
        DataStream<Student> objDataStream = input.map(string -> JSON.parseObject(string, Student.class));

        DataStream<Student> filterDataStream = objDataStream.filter(student -> {
            String name = student.name;
            if("张三".equals(name)){
                student.age = 100;
            }
            if("张三".equals(name) || "李四".equals(name)){
                return true;
            }else{
                return false;
            }
        });

        // 将结果打印到控制台，注意这里使用的是单线程打印，而非多线程
        filterDataStream.print().setParallelism(1);

        env.execute("Socket filterOperate");

    }

    /**
     * 基于 map 的操作 (主要是数据流对象的变更)
     * map操作，转换，从一个数据流转换成另一个数据流，这里是从 string --> Student
     * map方法不允许缺少数据，也就是原来多少条数据，处理后依然是多少条数据，只是用来做转换
     * @throws Exception
     */
    public static void mapOperate() throws Exception{
        // 创建 execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // mac命令窗口执行 nc -l 9110
        // windows nc -l -p 9110
        DataStream<String> input = env.socketTextStream("192.168.1.199", 9110, "\n");

        //json 转成实体对象
        DataStream<Student> objDataStream = input.map(string -> JSON.parseObject(string, Student.class));

        //转换 按key分区
        KeyedStream<Student,String> result = objDataStream.keyBy(new KeySelector<Student,String>(){
            @Override
            public String getKey(Student value) throws Exception {
                return String.valueOf(value.getPersonalID());
            }
        });
        //KeyedStream<Student,String> result2 = objDataStream.keyBy( (KeySelector<Student, String>) value -> String.valueOf( value.getPersonalID() ) );
        result.print().setParallelism(1);

        // 将结果打印到控制台，注意这里使用的是单线程打印，而非多线程

        objDataStream.print().setParallelism(1);


        env.execute("Socket mapOperate ");

    }

    /** 基于 flatMap 的操作
     * 将嵌套集合转换并平铺成非嵌套集合
     * flatMap 方法最终返回的是一个collector，而这个collector只有一层，当输入数据有嵌套的情况下，可以将数据平铺处理
     * 本地读取计数 wordCount
     * @throws Exception
     */
    public static void flatMapOperate() throws Exception{

        //final ExecutionEnvironment env2 = ExecutionEnvironment.getExecutionEnvironment();
        //env2.readTextFile("E:/word.txt");
        //创建流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取文本文件中的数据
        DataStreamSource<String> streamSource = env.readTextFile("/Users/cheney/Documents/1data/temp/word.txt");

        //进行逻辑计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream = streamSource
                .flatMap(new Splitter())
                .keyBy(0)
                .sum(1);
        dataStream.print();
        //设置程序名称
        env.execute(" Socket flatMapOperate ");

    }

    /**
     * 入门操作 | 基于 FlatMap 的 窗口输入的统计
     *
     * @throws Exception
     */
    public static void helloFlink() throws Exception{

        // 创建 execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 通过连接 socket 获取输入数据，这里连接到本地9000端口，如果9000端口已被占用，请换一个端口
        // mac命令窗口执行 nc -l 9998
        // windows nc -l -p 9000
        //DataStream text = env.socketTextStream("127.0.0.1", 9998, "\n");
        DataStream text = env.socketTextStream("192.168.1.199", 9110, "\n");

        // 解析数据，按 word 分组，开窗，聚合
        DataStream  windowCounts = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                for (String word : value.split("\\s")) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }).keyBy(0).timeWindow(Time.seconds(8)).sum(1);

        // lambda表达式
        DataStream  windowCounts2 = text.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
            for (String word : value.split("\\s")) {
                out.collect(Tuple2.of(word, 1));
            }
        }).keyBy(0).timeWindow(Time.seconds(8)).sum(1);



        // 将结果打印到控制台，注意这里使用的是单线程打印，而非多线程
        windowCounts.print().setParallelism(1);

        env.execute("Socket helloFlink");

    }

}
