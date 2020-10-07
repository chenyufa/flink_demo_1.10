package tablesql.StreamTableSink;

//import org.apache.calcite.interpreter.Row;

import org.apache.flink.types.Row;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.table.types.DataType;

/**
 * @ date: 2020/10/4 22:18
 * @ author: FatCheney
 * @ description: 自定义UpsertStreamTableSink
 * * Table在内部被转换成具有Add(增加)和Retract(撤消/删除)的消息流，最终交由DataStream的SinkFunction处理。
 *      * Boolean是Add(增加)或Retract(删除)的flag(标识)。Row是真正的数据类型。
 *      * Table中的Insert被编码成一条Add消息。如Tuple2<True, Row>。
 *      * Table中的Update被编码成一条Add消息。如Tuple2<True, Row>。
 *      * 在SortLimit(即order by ... limit ...)的场景下，被编码成两条消息。一条删除消息Tuple2<False, Row>，一条增加消息Tuple2<True, Row>。
 * * * * *
 * @ version: 1.0.0
 */
public class MyUpsertStreamTableSink implements UpsertStreamTableSink<Row> {

    private TableSchema tableSchema;

    public MyUpsertStreamTableSink(String[] fieldNames, DataType[] fieldTypes) {
        this.tableSchema = TableSchema.builder().fields(fieldNames,fieldTypes).build();
    }

    @Override
    public TableSchema getTableSchema() {
        return tableSchema;
    }


    // 设置Unique Key
    // 如上SQL中有GroupBy，则这里的唯一键会自动被推导为GroupBy的字段
    @Override
    public void setKeyFields(String[] keys) {}

    // 是否只有Insert
    // 如上SQL场景，需要Update，则这里被推导为isAppendOnly=false
    @Override
    public void setIsAppendOnly(Boolean isAppendOnly) {}

    @Override
    public TypeInformation<Row> getRecordType() {
        return new RowTypeInfo(tableSchema.getFieldTypes(),tableSchema.getFieldNames());
    }

    // 已过时
    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {}

    // 最终会转换成DataStream处理
    @Override
    public DataStreamSink<Tuple2<Boolean, Row>> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        return dataStream.addSink(new SinkFunction());
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        return null;
    }

    private static class SinkFunction extends RichSinkFunction<Tuple2<Boolean, Row>> {
        public SinkFunction() {
        }

        @Override
        public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
            Boolean flag = value.f0;
            if(flag){
                System.out.println("增加... "+value);
            }else {
                System.out.println("删除... "+value);
            }
        }
    }
}
