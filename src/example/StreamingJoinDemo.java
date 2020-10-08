package example;


import entity.Item;
import datasource.MyStreamingSource;

import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;


import java.util.ArrayList;
import java.util.List;

/**
 * @ date: 2020/10/5 21:13
 * @ author: FatCheney
 * @ description: 模拟双流Join
 * @ version: 1.0.0
 */
public class StreamingJoinDemo {

    public static void main(String[] args) throws Exception {

        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        SingleOutputStreamOperator<Item> source = bsEnv.addSource(new MyStreamingSource()).map(new MapFunction<Item, Item>() {
            @Override
            public Item map(Item item) throws Exception {
                return item;
            }
        });

        DataStream<Item> evenSelect = source.split(new OutputSelector<Item>() {
            @Override
            public Iterable<String> select(Item value) {
                System.out.println(" P0- evenSelect 分流中...");
                List<String> output = new ArrayList<>();
                if (value.getId() % 2 == 0) {
                    output.add("even");
                } else {
                    output.add("odd");
                }
                return output;
            }
        }).select("even");

        DataStream<Item> oddSelect = source.split(new OutputSelector<Item>() {
            @Override
            public Iterable<String> select(Item value) {
                System.out.println(" P0- oddSelect 分流中...");
                List<String> output = new ArrayList<>();
                if (value.getId() % 2 == 0) {
                    output.add("even");
                } else {
                    output.add("odd");
                }
                return output;
            }
        }).select("odd");


        bsTableEnv.createTemporaryView("evenTable", evenSelect, "id,name");
        bsTableEnv.createTemporaryView("oddTable", oddSelect, "id,name");

        System.out.println(" P0- 开始执行 join sql sqlQuery ...");
        Table queryTable = bsTableEnv.sqlQuery("select a.id as aid,a.name as aname,b.id as bid,b.name as bname from evenTable as a join oddTable as b on a.name = b.name");
        System.out.println(" P0- 结束执行 join sql sqlQuery ...");

        queryTable.printSchema();

        System.out.println(" P0- 开始执行 toRetractStream ...");
        bsTableEnv.toRetractStream(queryTable, TypeInformation.of(new TypeHint<Tuple4<Integer,String,Integer,String>>(){})).print();
        System.out.println(" P0- 结束执行 toRetractStream ...");

        bsEnv.execute("streaming sql job");

    }

}
