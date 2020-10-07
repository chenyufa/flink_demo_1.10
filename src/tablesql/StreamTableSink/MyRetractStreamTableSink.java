package tablesql.StreamTableSink;

import org.apache.flink.types.Row;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;

/**
 * @ date: 2020/10/4 22:12
 * @ author: FatCheney
 * @ description: 自定义RetractStreamTableSink
 * * Table在内部被转换成具有Add(增加)和Retract(撤消/删除)的消息流，最终交由DataStream的SinkFunction处理。
 *      * DataStream里的数据格式是Tuple2类型,如Tuple2<Boolean, Row>。
 *      * Boolean是Add(增加)或Retract(删除)的flag(标识)。Row是真正的数据类型。
 *      * Table中的Insert被编码成一条Add消息。如Tuple2<True, Row>。
 *      * Table中的Update被编码成两条消息。一条删除消息Tuple2<False, Row>，一条增加消息Tuple2<True, Row>。
 * * * * *
 * @ version: 1.0.0
 */
public class MyRetractStreamTableSink implements RetractStreamTableSink<Row> {

    private TableSchema tableSchema;

    public MyRetractStreamTableSink(String[] fieldNames, DataType[] fieldTypes) {
        this.tableSchema = TableSchema.builder().fields(fieldNames,fieldTypes).build();
    }

    @Override
    public TableSchema getTableSchema() {
        return tableSchema;
    }

    // 已过时
    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        return null;
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
    public TypeInformation<Row> getRecordType() {
        return new RowTypeInfo(tableSchema.getFieldTypes(),tableSchema.getFieldNames());
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
