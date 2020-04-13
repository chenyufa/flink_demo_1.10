package operator;

import com.alibaba.fastjson.JSON;
import entity.Student;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.ArrayList;

/**
 * @ date: 2020/04/06 20:43
 * @ author: Cheney
 * @ description: Aggregate: min()、minBy()、max()、maxBy() 滚动聚合并输出每次滚动聚合后的结果
 * min():获取的最小值，指定的field是最小，但不是最小的那条记录，示例会清晰的显示。
 * minBy():获取的最小值，同时也是最小值的那条记录。
 * max()与maxBy() 等等其他类型的区别也是一样
 */
public class DataStreamAggregateOperator {

    public static void main(String[] args) throws Exception{

         dataStream();
    }

    //流的方式
    public static void dataStream() throws Exception{

        // 创建 execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // mac命令窗口执行 nc -l 9110
        // windows nc -l -p 9110
        DataStream<String> input = env.socketTextStream("192.168.1.199", 9110, "\n");

        //json 转成实体对象
        DataStream<Student> objDataStream = input.map(string -> JSON.parseObject(string, Student.class));


        WindowedStream<Student, String, TimeWindow>  windowedStream = objDataStream
                .keyBy( (KeySelector<Student, String>) value -> "String.valueOf( value.getPersonalID() )"  )
                .timeWindow(Time.seconds(10));

        //最大值
        SingleOutputStreamOperator<Student> maxBy = windowedStream.maxBy("age");
        maxBy.print("maxBy  :");

        SingleOutputStreamOperator<Student> max = windowedStream.max("age");
        max.print("max  :");


        //最小值
        SingleOutputStreamOperator<Student> min = windowedStream.min("age");
        min.print("min  :");

        SingleOutputStreamOperator<Student> minBy = windowedStream.minBy("age");
        minBy.print("minBy  :");


        //求和
        SingleOutputStreamOperator<Student> sum = windowedStream.sum("age");
        sum.print("sum  :");

        env.execute();

    }

    //固定数据范围(没理解通)
    public static void keyStream() throws Exception{
        // 创建 execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Student> studentArrayList = new ArrayList<Student>();

        Student student1 = new Student("张三", 12345678903L, 31, 1);
        studentArrayList.add(student1);

        Student student2 = new Student("李四", 12345678904L, 43, 2);
        studentArrayList.add(student2);

        //Student student3 = new Student("李四", 12345678904L, 41, 3);
        //studentArrayList.add(student3);

        Student student4 = new Student("王五", 12345678905L, 67, 5);
        studentArrayList.add(student4);

        Student student5 = new Student("赵六", 12345678906L, 21, 6);
        studentArrayList.add(student5);

        DataStreamSource<Student> source = env.fromCollection(studentArrayList);

        // 转换: 按指定的Key 对数据重分区，将相同Key(用户)的数据分到同一个分区
        KeyedStream<Student, String> keyedStream = source.keyBy(new KeySelector<Student, String>() {
            @Override
            public String getKey(Student value) throws Exception {
                return "all";
            }
        });

        SingleOutputStreamOperator<Student> min = keyedStream.maxBy("age");
        min.print("maxBy  :");

        //最大值
        //keyedStream.max("weight").print();

        //最大值
        //keyedStream.maxBy("age").print();

        //最小值
        //keyedStream.min("weight").print();

        //minBy
        //keyedStream.minBy("weight").print();

        //求和
        //keyedStream.sum("weight").print();

        env.execute();
    }


}
