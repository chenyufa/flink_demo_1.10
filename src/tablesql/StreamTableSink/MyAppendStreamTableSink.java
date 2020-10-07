package tablesql.StreamTableSink;

import org.apache.calcite.interpreter.Row;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;

/**
 * @ date: 2020/10/4 22:10
 * @ author: FatCheney
 * @ description: 自定义 AppendStreamTableSink, AppendStreamTableSink 适用于表只有Insert的场景
 * @ version: 1.0.0
 */
public class MyAppendStreamTableSink implements AppendStreamTableSink<Row> {

    private TableSchema tableSchema;

    public MyAppendStreamTableSink(String[] fieldNames, DataType[] fieldTypes) {
        this.tableSchema = TableSchema.builder().fields(fieldNames,fieldTypes).build();
    }

    @Override
    public TableSchema getTableSchema() {
        return tableSchema;
    }

    @Override
    public DataType getConsumedDataType() {
        return tableSchema.toRowDataType();
    }

    // 已过时
    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        return null;
    }

    // 已过时
    @Override
    public void emitDataStream(DataStream<Row> dataStream) {}

    @Override
    public DataStreamSink<Row> consumeDataStream(DataStream<Row> dataStream) {
        return dataStream.addSink(new SinkFunction());
    }

    private static class SinkFunction extends RichSinkFunction<Row> {
        public SinkFunction() {
        }

        @Override
        public void invoke(Row value, Context context) throws Exception {
            System.out.println(value);
        }
    }

}
