package operator;

import entity.Student;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @ date: 2020/04/03 13:36
 * @ author: Cheney
 * @ description: KeyBy: 按指定的Key对数据重分区。将同一Key的数据放到同一个分区。
 */
public class DataStreamKeyByOperator {

    public static void main(String[] args) throws Exception {

        // 创建 execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 输入
        DataStreamSource<Student> source = env.fromCollection(Arrays.asList(
                new Student("张三", 12345678903L, 31, 61.3),
                new Student("李四", 12345678904L, 43, 62.3),
                new Student("李四", 12345678904L, 41, 63.3),
                new Student("张三", 12345678903L, 32, 64.3),
                new Student("王五", 12345678905L, 67, 75.3),
                new Student("赵六", 12345678906L, 21, 76.3),
                new Student("林一", 12345678901L, 12, 77.3),
                new Student("赵六", 12345678906L, 61, 78.3),
                new Student("黄八", 12345678908L, 81, 79.3),
                new Student("李九", 12345678909L, 91, 70.3),
                new Student("林一", 12345678901L, 11, 71.3),
                new Student("范二", 12345678902L, 21, 71.3)
        ));

        // 转换: 按指定的Key 对数据重分区，将相同Key(用户ID)的数据分到同一个分区
        KeyedStream<Student, String> result = source.keyBy(new KeySelector<Student, String>() {
            @Override
            public String getKey(Student value) throws Exception {
                //return String.valueOf( value.getPersonalID() );
                return value.getName();
            }
        });

        //KeyedStream<Student, String> result2 = source.keyBy((KeySelector<Student, String>) value -> value.getName());


        result.print().setParallelism(4);

        env.execute();
    }

}
