package tablesql;


import entity.CountWithTimestamp;
import entity.HunterRealInfo;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Collector;
import tablesql.StreamTableSink.MyRetractStreamTableSink;
import tablesql.udf.UDFWordCount;

/**
 * @ date: 2020/10/4 16:06
 * @ author: FatCheney
 * @ description: FlinkTable 任务 流数据查询
 * @ version: 1.0.0
 */
public class FlinkTableJobStream {

    public static void main(String[] args) {
        //csvFileToCsvFile();
        //fileToCsvFile();
        //ReadFileCreateTableStream();
        WordCountByTable();
    }

    /**
     * table API + UDF 实现 单词计数
     */
    public static void WordCountByTable(){

        StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> sourceDS = fsEnv.readTextFile("/Users/cheney/Documents/1data/temp/info_1.text");

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);
        Table table = fsTableEnv.fromDataStream(sourceDS,"inputLine");
        // 注册自定义UDF函数
        fsTableEnv.registerFunction("wordCountUDF", new UDFWordCount());

        Table wordCount= table.joinLateral("wordCountUDF(inputLine) as (word, countOne)")
                .groupBy("word")
                .select("word, countOne.sum as countN");
        /*table 转换DataStream*/
        DataStream<Tuple2<Boolean, Row>> result = fsTableEnv.toRetractStream(wordCount, Types.ROW(Types.STRING, Types.INT));

        /*统计后会在 统计记录前面加一个true  false标识 这里你可以注释掉跑下看看 对比下*/
        result.filter(new FilterFunction<Tuple2<Boolean, Row>>() {
            @Override
            public boolean filter(Tuple2<Boolean, Row> value) throws Exception {
                if(value.f0 == false){
                    return false;
                }else{
                    return true;
                }
            }
        }).print();

        try {
            fsEnv.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void fileToCsvFile() {
        //1.创建TableEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> text = env.readTextFile("/Users/cheney/Documents/1data/temp/info_1.text");

        SingleOutputStreamOperator<HunterRealInfo> dataStreamStudent = text.flatMap(new FlatMapFunction<String, HunterRealInfo>() {
            @Override
            public void flatMap(String s, Collector<HunterRealInfo> collector) throws Exception {
                String infos[] = s.split(",");
                if(StringUtils.isNotBlank(s) && infos.length == 7){
                    HunterRealInfo studentInfo = new HunterRealInfo(infos[0],infos[1],infos[2],infos[3],infos[4],infos[5],infos[6]);
                    collector.collect(studentInfo);
                }
            }
        });

        // 不推荐 tableEnv.registerTable("TheDataTable",dataTable);。旧版用法现在是创建表或者view
        //tableEnv.registerTable("hunterRealInfoTable",dataStreamStudent);
        tableEnv.createTemporaryView("hunterRealInfoTable",dataStreamStudent);

        //设置sink信息，对应字段，以及字段类型，案例中使用cvs写入
        String[] fieldNamesStream={"id","bid","realName","idCardNumber","idCardFront","status","bank"}; //dataStreamStudent 所有数据append，不会做修改
        TypeInformation[] fieldTypesStream = {Types.STRING,Types.STRING,Types.STRING,Types.STRING,Types.STRING,Types.STRING,Types.STRING};

        //String[] fieldNamesStream={"bank","statusNum"}; //dataStreamStudent 所有数据append，不会做修改
        //TypeInformation[] fieldTypesStream = {Types.STRING,Types.DOUBLE};

        //StreamTableSink tableSinkStream = new CsvTableSink("/Users/cheney/Documents/1data/temp/socketToCsvFile","  ",1, FileSystem.WriteMode.OVERWRITE);
        //tableEnv.registerTableSink("hunterRealInfoStream", fieldNamesStream, fieldTypesStream, tableSinkStream);

        //一、写入sink有两种方式，一种如下琐事，使用table.insert
        //将dataStream转换为table
        //Table table = tableEnv.fromDataStream(dataStreamStudent);
        //table.insertInto("hunterRealInfoStream");

        //二、写入sink有两种方式，另外一种如下写入，使用registerDataStream，最后使用sqlUpdate实现，效果和第一种一样
        //注册dataStreamStudent流到表中，表名为：hunterRealInfo
        tableEnv.registerDataStream("hunterRealInfo",dataStreamStudent,"id,bid,realName,idCardNumber ,idCardFront,status ,bank");
        tableEnv.sqlUpdate("insert into hunterRealInfoStream select id,bid,realName,idCardNumber ,idCardFront,status ,bank from hunterRealInfo where id='2096' ");

        //tableEnv.registerDataStream("hunterRealInfo",dataStreamStudent,"bank,statusNum");
        //tableEnv.sqlUpdate("insert into hunterRealInfoStream select bank,sum(status) statusNum from hunterRealInfo group by  bank ");

        try {
            env.execute("hunterRealInfo-job");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void csvFileToCsvFile(){

        //1.创建TableEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //制定版本planner =》  blink planner 或者 老版本 planner
        //EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        //EnvironmentSettings oldFsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        //StreamTableEnvironment tableEnv2 = (StreamTableEnvironment) TableEnvironment.create(bsSettings);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.创建一个TableSource
        //CsvTableSource: 文件路径、字段名、字段类型
        TableSource csvSource = new CsvTableSource("/Users/cheney/Documents/1data/temp/info_1.csv",
                new String[]{"id","bid","realName","idCardNumber","idCardFront","status","bank"},
                new TypeInformation[]{Types.STRING,Types.STRING,Types.STRING,Types.STRING,Types.STRING,Types.INT,Types.STRING});
        //注册一个TableSource，称为CsvTable
        tableEnv.registerTableSource("CsvTable", csvSource);


        //3.创建一个TableSink //注意 CsvTableSink 实现的接口类是 AppendStreamTableSink
        TableSink csvSink = new CsvTableSink("/Users/cheney/Documents/1data/temp/info_2.csv",",");
        //定义字段名和类型
        String[] fieldNames = {"bank", "statusNum"};
        TypeInformation[] filedTypes = {Types.STRING, Types.INT};
        //注册一个TableSink
        tableEnv.registerTableSink("CsvSinkTable", fieldNames, filedTypes, csvSink);

        //4、注册RetractStreamTableSink
        //String[] sinkFieldNames={"bank","statusNum"};
        //DataType[] sinkFieldTypes={DataTypes.STRING(),DataTypes.INT()};
        //RetractStreamTableSink<Row> myRetractStreamTableSink = new MyRetractStreamTableSink(sinkFieldNames,sinkFieldTypes);
        //tableEnv.registerTableSink("CsvSinkTable",myRetractStreamTableSink);


        //4.使用SQL操作table
        //计算bank的总status值
        /*Table revenue = tableEnv.sqlQuery("select bank,sum(status) as statusNum " +
                " from CsvTable " +
                " where status >0 " +
                " group by bank ");
        //把结果添加到TableSink中
        revenue.insertInto("CsvSinkTable");*/

        //tableEnv.sqlUpdate("INSERT into CsvSinkTable select bank,sum(status) as statusNum from CsvTable where status >0  group by bank ");

        Table revenue = tableEnv.sqlQuery("select bank,sum(status) as statusNum " +
                " from CsvTable " +
                " where status >0 " +
                " group by bank ");

        //Table select = csvSource.groupBy($("name")).select($("name"), $("age").count().as("count"));


        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(revenue, Row.class);

        System.out.println("============== tuple2DataStream =============");
        tuple2DataStream.print();

        try {
            env.execute("csvFileToCsvFile-job");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void ReadFileCreateTableStream(){
        //1.创建TableEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2、连接外部文件系统，格式，注册字段，临时表
        tableEnv.connect(new FileSystem().path("/Users/cheney/Documents/1data/temp/info_2.csv"))
                .withFormat(new OldCsv())
                .withSchema(new Schema().field("bank", DataTypes.STRING()).field("status",DataTypes.INT()))
                .inAppendMode()
                .createTemporaryTable("Orders");

        //3、读取表
        Table orders = tableEnv.from("Orders");

        //4、读取表字段
        Table select = orders.groupBy("bank").select( "bank as key,status.count as count,1L as lastModified");

        //5、转化成DataStream打印在控制台 注意：这里的Row容易导入错包！
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(select, Row.class);
        tuple2DataStream.print();

        try {
            env.execute("readFileCreateTableStream");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
