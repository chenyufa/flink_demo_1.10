package datasource;

import entity.Item;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @ date: 2020/10/5 21:10
 * @ author: FatCheney
 * @ description: 模拟生产商品数据源
 * @ version: 1.0.0
 */
public class MyStreamingSource implements SourceFunction<Item> {

    private boolean isRunning = true;

    private Integer currentSize = 0;

    private final Integer MAX_SIZE = 20;


    /**
     * 重写run方法产生一个源源不断的数据发送源
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Item> ctx) throws Exception {
        while(isRunning){

            if(currentSize.equals(MAX_SIZE)){
                isRunning = false;
            }
            currentSize ++;

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

        Item item = new Item();
        item.setName("name" + i);
        item.setId(i);
        return item;
    }

}
