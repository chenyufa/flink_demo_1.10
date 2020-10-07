package tablesql.table2streamExample;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;

/**
 * @ date: 2020/10/5 15:11
 * @ author: FatCheney
 * @ description: Upsert stream
 * 官方定义：
 * An upsert stream is a stream with two types of messages, upsert messages and delete messages.
 * A dynamic table that is converted into an upsert stream requires a (possibly composite) unique key.
 * A dynamic table with unique key is converted into a stream by encoding INSERT and UPDATE changes as upsert messages and DELETE changes as delete messages.
 * The stream consuming operator needs to be aware of the unique key attribute in order to apply messages correctly.
 * The main difference to a retract stream is that UPDATE changes are encoded with a single message and hence more efficient.
 * The following figure visualizes the conversion of a dynamic table into an upsert stream.
 * @ version: 1.0.0
 */
public class UpsertExample {


    /**
     * 这种模式的返回值也是一个Tuple2<Boolean, T>类型，
     * 和 Retract的区别在于更新表中的某条数据并不会返回一条删除旧数据一条插入新数据，而是看上去真的是更新了某条数据。
     *
     * ###番外篇###： 如果修改以下SQL , UpsertStreamTableSink 会 返回值Tuple2<Boolean, T>第一个元素是false
     * 具体的原理可以查看源码
     * org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecRank
     * org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecSortLimit
     * 解析sql的时候通过下面的方法得到不同的strategy，由此影响是否需要删除原有数据的行为。
     * def getStrategy(forceRecompute: Boolean = false): RankProcessStrategy = { .....
     * 知道什么时候会产生false属性的数据，对于理解 JDBCUpsertTableSink 和 HBaseUpsertTableSink 的使用会有很大的帮助。
     *
     * @param args
     * @throws Exception
     * 输出：
     * send message:(true,(Mary,1))
     * send message:(true,(Bob,1))
     * send message:(true,(Mary,2))
     * send message:(true,(Liz,1))
     * send message:(true,(Liz,2))
     * send message:(true,(Mary,3))
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
                new Tuple2<>("Liz", "./prod?id=3"),
                new Tuple2<>("Mary", "./prod?id=7")
        );

        Table clicksTable = tEnv.fromDataStream(data, "user,url");

        tEnv.registerTable("clicks", clicksTable);

        //两个sql执行结果有差异
        String sql = "SELECT user,COUNT(url) FROM clicks GROUP BY user ";
        // 不加limit会抛出异常：Sort on a non-time-attribute field is not supported.
        String sql2 = "SELECT user, cnt FROM ( SELECT user,COUNT(url) as cnt FROM clicks GROUP BY user ) ORDER BY cnt limit 22 ";

        Table rTable = tEnv.sqlQuery(sql2);


        tEnv.registerTableSink("MemoryUpsertSink", new MemoryUpsertSink(rTable.getSchema()));
        rTable.insertInto("MemoryUpsertSink");

        env.execute();
    }

    private static class MemoryUpsertSink implements UpsertStreamTableSink<Tuple2<String, Long>> {
        private TableSchema schema;
        private String[] keyFields;
        private boolean isAppendOnly;

        private String[] fieldNames;
        private TypeInformation<?>[] fieldTypes;

        public MemoryUpsertSink() {

        }

        public MemoryUpsertSink(TableSchema schema) {
            this.schema = schema;
        }

        @Override
        public void setKeyFields(String[] keys) {
            this.keyFields = keys;
        }

        @Override
        public void setIsAppendOnly(Boolean isAppendOnly) {
            this.isAppendOnly = isAppendOnly;
        }

        @Override
        public TypeInformation<Tuple2<String, Long>> getRecordType() {
            return TypeInformation.of(new TypeHint<Tuple2<String, Long>>(){});
        }

        @Override
        public void emitDataStream(DataStream<Tuple2<Boolean, Tuple2<String, Long>>> dataStream) {
            consumeDataStream(dataStream);
        }

        @Override
        public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Tuple2<String, Long>>> dataStream) {
            return dataStream.addSink(new DataSink()).setParallelism(1);
        }

        @Override
        public TableSink<Tuple2<Boolean, Tuple2<String, Long>>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
            MemoryUpsertSink memoryUpsertSink = new MemoryUpsertSink();
            memoryUpsertSink.setFieldNames(fieldNames);
            memoryUpsertSink.setFieldTypes(fieldTypes);
            memoryUpsertSink.setKeyFields(keyFields);
            memoryUpsertSink.setIsAppendOnly(isAppendOnly);

            return memoryUpsertSink;
        }

        @Override
        public String[] getFieldNames() {
            return schema.getFieldNames();
        }

        public void setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
        }

        @Override
        public TypeInformation<?>[] getFieldTypes() {
            return schema.getFieldTypes();
        }

        public void setFieldTypes(TypeInformation<?>[] fieldTypes) {
            this.fieldTypes = fieldTypes;
        }
    }

    private static class DataSink extends RichSinkFunction<Tuple2<Boolean, Tuple2<String, Long>>> {
        public DataSink() {
        }

        @Override
        public void invoke(Tuple2<Boolean, Tuple2<String, Long>> value, Context context) throws Exception {
            System.out.println("send message:" + value);
        }
    }
}
