package example;

/**
 * @ date: 2020/10/5 21:49
 * @ author: FatCheney
 * @ description:
 * @ version: 1.0.0
 */

import entity.Item;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MyStreamingSource implements SourceFunction<Item> {

    private boolean isRunning = true;

    private Integer currentSize = 0;

    private final Integer MAX_SIZE = 20;

    public MyStreamingSource() {
        System.out.println("P1-MyStreamingSource 构造函数。。。");
    }

    public MyStreamingSource(boolean isRunning, Integer currentSize) {
        this.isRunning = isRunning;
        this.currentSize = currentSize;
    }

    /**
     * 重写run方法产生一个源源不断的数据发送源
     * @param ctx
     * @throws Exception
     */
    public void run(SourceContext<Item> ctx) throws Exception {
        while(isRunning){
            if(currentSize.equals(MAX_SIZE)){
                cancel();
            }
            currentSize ++;

            System.out.println("P1-生产数据,第"+currentSize+"条....");
            Item item = generateItem();
            ctx.collect(item);
            //每秒产生一条数据
            Thread.sleep(1000);
        }
    }
    @Override
    public void cancel() {
        isRunning = false;
    }

    //随机产生一条商品数据
    private Item generateItem(){
        int i = new Random().nextInt(100);
        ArrayList<String> list = new ArrayList();
        list.add("HAT");
        list.add("TIE");
        list.add("SHOE");
        Item item = new Item();
        item.setName(list.get(new Random().nextInt(3)));
        item.setId(i);
        return item;
    }
}


class StreamingDemo {

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
                System.out.println(" P1- evenSelect 分流中...");
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
                System.out.println(" P1- oddSelect 分流中...");
                List<String> output = new ArrayList<>();
                if (value.getId() % 2 == 0) {
                    output.add("even");
                } else {
                    output.add("odd");
                }
                return output;
            }
        }).select("odd");


        bsTableEnv.createTemporaryView("evenTable", evenSelect, "name,id");
        bsTableEnv.createTemporaryView("oddTable", oddSelect, "name,id");

        System.out.println(" P1- 开始执行 join sql sqlQuery ...");
        Table queryTable = bsTableEnv.sqlQuery("select a.id,a.name,b.id,b.name from evenTable as a join oddTable as b on a.name = b.name");
        System.out.println(" P1- 结束执行 join sql sqlQuery ...");

        queryTable.printSchema();

        System.out.println(" P1- 开始执行 toRetractStream ...");
        bsTableEnv.toRetractStream(queryTable, TypeInformation.of(new TypeHint<Tuple4<Integer,String,Integer,String>>(){})).print();
        System.out.println(" P1- 结束执行 toRetractStream ...");

        bsEnv.execute("streaming sql job");
    }

}
