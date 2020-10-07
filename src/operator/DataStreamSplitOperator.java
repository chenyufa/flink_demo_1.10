package operator;


import org.apache.flink.api.common.functions.FilterFunction;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;

/**
 * @ date: 2020/10/6 15:27
 * @ author: FatCheney
 * @ description: 分流操作
 *  1.Filter 分流; 2.Split 分流; 3.SideOutPut 分流 (官方推荐)
 * @ version: 1.0.0
 */
public class DataStreamSplitOperator {

    public static void main(String[] args) {

         //filterOperator();
         //splitOperator();
         sideOutPutOperator();
    }

    /**
     * SideOutPut 分流
     * 最新的也是最为推荐的分流方法
     */
    public static void sideOutPutOperator(){

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //模拟数据源
        List data = new ArrayList<Tuple3<Integer,Integer,Integer>>();
        data.add(new Tuple3<>(0,1,0));
        data.add(new Tuple3<>(0,1,1));
        data.add(new Tuple3<>(0,2,2));
        data.add(new Tuple3<>(0,1,3));
        data.add(new Tuple3<>(1,2,5));
        data.add(new Tuple3<>(1,2,9));
        data.add(new Tuple3<>(1,2,11));
        data.add(new Tuple3<>(1,2,13));

        DataStreamSource< Tuple3<Integer,Integer,Integer> > items = env.fromCollection(data);

        //1.定义 OutputTag , 注意是 org.apache.flink.util.OutputTag 包下
        OutputTag<Tuple3<Integer,Integer,Integer>> zeroStream = new OutputTag<Tuple3<Integer,Integer,Integer>>("zeroStream") {};
        OutputTag<Tuple3<Integer,Integer,Integer>> oneStream = new OutputTag<Tuple3<Integer,Integer,Integer>>("oneStream") {};

        //2.调用特定函数进行数据拆分
        // ProcessFunction | KeyedProcessFunction | CoProcessFunction | KeyedCoProcessFunction | ProcessWindowFunction | ProcessAllWindowFunction
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> processStream= items.process(new ProcessFunction<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
            @Override
            public void processElement(Tuple3<Integer, Integer, Integer> value, Context ctx, Collector<Tuple3<Integer, Integer, Integer>> out) throws Exception {

                if (value.f0 == 0) {
                    ctx.output(zeroStream, value);
                } else if (value.f0 == 1) {
                    ctx.output(oneStream, value);
                }
            }
        });

        DataStream<Tuple3<Integer, Integer, Integer>> zeroSideOutput = processStream.getSideOutput(zeroStream);
        DataStream<Tuple3<Integer, Integer, Integer>> oneSideOutput = processStream.getSideOutput(oneStream);

        zeroSideOutput.print();
        oneSideOutput.printToErr();


        //打印结果
        String jobName = "user defined streaming source";
        try {
            env.execute(jobName);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    /**
     * Split 分流
     * 使用 split 算子切分过的流，是不能进行二次切分
     * 假如 再次调用 split 切分，控制台会抛出以下异常
     * Exception in thread "main" java.lang.IllegalStateException: Consecutive multiple splits are not supported. Splits are deprecated. Please use side-outputs.
     * 我们在源码中可以看到注释，该方式已经废弃并且建议使用最新的 SideOutPut 进行分流操作。
     *
     */
    public static void splitOperator(){

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //模拟数据源
        List data = new ArrayList< Tuple3<Integer,Integer,Integer> >();
        data.add(new Tuple3<>(0,1,0));
        data.add(new Tuple3<>(0,1,1));
        data.add(new Tuple3<>(0,2,2));
        data.add(new Tuple3<>(0,1,3));
        data.add(new Tuple3<>(1,2,5));
        data.add(new Tuple3<>(1,2,9));
        data.add(new Tuple3<>(1,2,11));
        data.add(new Tuple3<>(1,2,13));

        DataStreamSource< Tuple3<Integer,Integer,Integer> > items = env.fromCollection(data);

        SplitStream<Tuple3<Integer, Integer, Integer>> splitStream = items.split(new OutputSelector<Tuple3<Integer, Integer, Integer>>() {
            @Override
            public Iterable<String> select(Tuple3<Integer, Integer, Integer> value) {
                List<String> tags = new ArrayList<>();
                if (value.f0 == 0) {
                    tags.add("zeroStream");
                } else if (value.f0 == 1) {
                    tags.add("oneStream");
                }
                return tags;
            }
        });

        splitStream.select("zeroStream").print();
        splitStream.select("oneStream").printToErr();

        //打印结果
        String jobName = "user defined streaming source";
        try {
            env.execute(jobName);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * Filter 分流
     * Filter 的弊端是显而易见的，为了得到我们需要的流数据，需要多次遍历原始流，这样无形中浪费了我们集群的资源。
     */
    public static void filterOperator(){

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //模拟数据源
        List data = new ArrayList< Tuple3<Integer,Integer,Integer> >();
        data.add(new Tuple3<>(0,1,0));
        data.add(new Tuple3<>(0,1,1));
        data.add(new Tuple3<>(0,2,2));
        data.add(new Tuple3<>(0,1,3));
        data.add(new Tuple3<>(1,2,5));
        data.add(new Tuple3<>(1,2,9));
        data.add(new Tuple3<>(1,2,11));
        data.add(new Tuple3<>(1,2,13));

        DataStreamSource< Tuple3<Integer,Integer,Integer> > items = env.fromCollection(data);

        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> zeroStream = items.filter( (FilterFunction< Tuple3<Integer,Integer,Integer> >) value -> value.f0 == 0 );
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> oneStream =  items.filter( (FilterFunction< Tuple3<Integer,Integer,Integer> >) value -> value.f0 == 1 );

        zeroStream.print();
        oneStream.printToErr();


        //打印结果
        String jobName = "user defined streaming source";
        try {

            env.execute(jobName);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
