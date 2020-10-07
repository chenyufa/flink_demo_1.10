package tablesql.udf;


import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @ date: 2020/10/5 10:59
 * @ author: FatCheney
 * @ description:
 * @ version: 1.0.0
 */
public class UDFWordCount extends TableFunction<Row> {

    /*设置返回类型*/
    @Override
    public TypeInformation<Row> getResultType() {
        return Types.ROW(Types.STRING, Types.INT);
    }

    /**
     * 消息处理
     * 这里的特殊处理:只取每行消息的第二个单词
     * @param line 消息
     */
    public void eval(String line){
        String[] wordSplit = line.split(",");
        if(wordSplit.length >= 2){
            Row row = new Row(2);
            row.setField(0, wordSplit[1]);
            row.setField(1, 1);
            collect(row);
        }
    }

}
