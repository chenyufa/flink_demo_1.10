package datasource;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @ date: 2020/10/8 16:34
 * @ author: FatCheney
 * @ description:
 * @ version: 1.0.0
 */
public class MyKafkaSource implements SourceFunction<String> {

    //private long count = 1L;
    private boolean isRunning = true;

    /**
     * 主要的方法
     * 启动一个source
     * 大部分情况下，都需要在这个run方法中实现一个循环，这样就可以循环产生数据了
     *
     * @param sourceContext
     * @throws Exception
     */
    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while(isRunning){
            //图书的排行榜
            List<String> books = new ArrayList<>();
            books.add("Java从入门到放弃");
            books.add("Scala从入门到放弃");
            books.add("Pyhton从入门到放弃");
            books.add("C++从入门到放弃");
            books.add("Php从入门到放弃");
            int i = new Random().nextInt(5);
            int j = new Random().nextInt(5);
            sourceContext.collect("person"+j+":"+books.get(i));
            //每秒产生一条数据
            Thread.sleep(1000);
        }
    }

    //取消一个cancel的时候会调用的方法
    @Override
    public void cancel() {
        isRunning = false;
    }

}
