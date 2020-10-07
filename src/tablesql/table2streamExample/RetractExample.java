package tablesql.table2streamExample;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @ date: 2020/10/5 15:06
 * @ author: FatCheney
 * @ description: Retract stream
 * 官方定义：
 * A retract stream is a stream with two types of messages, add messages and retract messages.
 * A dynamic table is converted into an retract stream by encoding an INSERT change as add message,
 * a DELETE change as retract message, and an UPDATE change as a retract message for the updated (previous) row and an add message for the updating (new) row.
 * The following figure visualizes the conversion of a dynamic table into a retract stream.
 * @ version: 1.0.0
 */
public class RetractExample {

    /**
     * 对于toRetractStream的返回值是一个Tuple2<Boolean, T>类型，
     * 第一个元素为true表示这条数据为要插入的新数据，false表示需要删除的一条旧数据。
     * 也就是说可以把更新表中某条数据分解为先删除一条旧数据再插入一条新数据。
     *
     * @param args
     * @throws Exception
     * 输出结果：
     * (true,(Mary,1))
     * (true,(Bob,1))
     * (false,(Mary,1))
     * (true,(Mary,2))
     * (true,(Liz,1))
     * (false,(Bob,1))
     * (true,(Bob,2))
     */
    public static void main(String[] args) throws Exception {

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        env.setParallelism(1);

        DataStream<Tuple2<String, String>> data = env.fromElements(
                new Tuple2<>("Mary", "./home"),
                new Tuple2<>("Bob", "./cart"),
                new Tuple2<>("Mary", "./prod?id=1"),
                new Tuple2<>("Liz", "./home"),
                new Tuple2<>("Bob", "./prod?id=3")
        );

        Table clicksTable = tEnv.fromDataStream(data, "user,url");

        tEnv.registerTable("clicks", clicksTable);
        Table rTable = tEnv.sqlQuery("SELECT user,COUNT(url) cn FROM clicks GROUP BY user ");

        DataStream ds = tEnv.toRetractStream(rTable, TypeInformation.of(new TypeHint<Tuple2<String, Long>>(){}));
        ds.print();
        env.execute();

    }

}
