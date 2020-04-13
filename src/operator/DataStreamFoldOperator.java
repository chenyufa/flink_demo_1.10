package operator;

import entity.Student;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @ date: 2020/04/06 20:10
 * @ author: Cheney
 * @ description: Fold: 基于初始值和自定义的FoldFunction滚动折叠后发出新值
 */
public class DataStreamFoldOperator {

    public static void main(String[] args) throws Exception{

        // 创建 execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 输入
        DataStreamSource<Student> source = env.fromCollection(Arrays.asList(
                new Student("name:张三", 12345678903L, 31, 1),
                new Student("name:李四", 12345678904L, 43, 2),
                new Student("name:李四", 12345678904L, 41, 3),
                new Student("name:张三", 12345678903L, 32, 4),
                new Student("name:王五", 12345678905L, 67, 5),
                new Student("name:赵六", 12345678906L, 21, 6),
                new Student("name:林一", 12345678901L, 12, 7),
                new Student("name:赵六", 12345678906L, 61, 8),
                new Student("name:黄八", 12345678908L, 81, 9),
                new Student("name:李九", 12345678909L, 91, 10),
                new Student("name:林一", 12345678901L, 11, 11),
                new Student("name:范二", 12345678902L, 21, 12),
                new Student("name:李四", 12345678904L, 41, 13)
        ));

        // 转换: 按指定的Key 对数据重分区，将相同Key(用户ID)的数据分到同一个分区
        KeyedStream<Student, String> keyedStream = source.keyBy(new KeySelector<Student, String>() {
            @Override
            public String getKey(Student value) throws Exception {
                //return String.valueOf( value.getPersonalID() );
                return value.getName();
            }
        });

        // 转换: Fold 基于初始值和FoldFunction滚动折叠
        // accumulator 上一段的字符串, 结合当前段滚动折叠返回
        SingleOutputStreamOperator<String> result = keyedStream.fold("体重:", new FoldFunction<Student, String>() {
            @Override
            public String fold(String accumulator, Student value) throws Exception {
                if(accumulator.startsWith("name")){
                    return accumulator + " -> " + value.getName()+":"+value.getWeight();
                }else {
                    return value.getName()+" " +accumulator + " -> " + value.getName()+":"+value.getWeight();
                }
            }
        });

        result.print();

        env.execute();

    }

}
