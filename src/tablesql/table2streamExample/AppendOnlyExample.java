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
 * @ date: 2020/10/5 15:00
 * @ author: FatCheney
 * @ description: Append-only stream Example
 * 官方定义：
 * A dynamic table that is only modified by INSERT changes can be converted into a stream by emitting the inserted rows.
 *
 * @ version: 1.0.0
 */
public class AppendOnlyExample {

    /**
     *
     * 如果dynamic table只包含了插入新数据的操作那么就可以转化为append-only stream，所有数据追加到stream里面。
     * @param args
     * @throws Exception
     * 输出结果：
     * (Mary,./home)
     * (Mary,./prod?id=1)
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
        Table rTable = tEnv.sqlQuery("select user,url from clicks where user='Mary'");

        DataStream ds = tEnv.toAppendStream(rTable, TypeInformation.of(new TypeHint<Tuple2<String, String>>(){}));
        ds.print();
        env.execute();

    }


}
