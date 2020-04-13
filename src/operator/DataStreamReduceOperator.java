package operator;

import entity.Student;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @ date: 2020/04/05 23:15
 * @ author: Cheney
 * @ description: Reduce操作
 * 基于ReduceFunction进行滚动聚合，并向下游算子输出每次滚动聚合后的结果。
 * 简单来说就是下一条计算依赖上一条的输出
 */
public class DataStreamReduceOperator {

    public static void main(String[] args) throws Exception{

        // 创建 execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 输入
        DataStreamSource<Student> source = env.fromCollection(Arrays.asList(
                new Student("张三", 12345678903L, 31, 1),
                new Student("李四", 12345678904L, 43, 2),
                new Student("李四", 12345678904L, 41, 3),
                new Student("张三", 12345678903L, 32, 4),
                new Student("王五", 12345678905L, 67, 5),
                new Student("赵六", 12345678906L, 21, 6),
                new Student("林一", 12345678901L, 12, 7),
                new Student("赵六", 12345678906L, 61, 8),
                new Student("黄八", 12345678908L, 81, 9),
                new Student("李九", 12345678909L, 91, 10),
                new Student("林一", 12345678901L, 11, 11),
                new Student("范二", 12345678902L, 21, 12),
                new Student("李四", 12345678904L, 41, 13)
        ));

        // 转换: 按指定的Key 对数据重分区，将相同Key(用户ID)的数据分到同一个分区
        KeyedStream<Student, String> result = source.keyBy(new KeySelector<Student, String>() {
            @Override
            public String getKey(Student value) throws Exception {
                //return String.valueOf( value.getPersonalID() );
                return value.getName();
            }
        });

        // 转换: Reduce滚动聚合。这里,滚动聚合每个用户对应的商品总价格。
        SingleOutputStreamOperator<Student> result2 = result.reduce((ReduceFunction<Student>) (value1, value2) -> {
            double newProductPrice = value1.getWeight() + value2.getWeight();
            return new Student(value1.getName(), value1.getPersonalID(), 0, newProductPrice);
        });

        /*SingleOutputStreamOperator<Student> result3 = result.reduce(new ReduceFunction<Student>() {
            @Override
            public Student reduce(Student value1, Student value2) throws Exception {
                double newProductPrice = value1.getWeight() + value2.getWeight();
                return new Student(value1.getName(), value1.getPersonalID(), 0, newProductPrice);
            }
        });*/

        result2.print();

        env.execute();

    }

}
