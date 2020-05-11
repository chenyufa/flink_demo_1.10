package datasource;


import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @ date: 2020/05/11 22:00
 * @ author: Cheney
 * @ description: 模拟生产一个流数据
 */
public class MyStreamDataSource extends RichParallelSourceFunction<Tuple3<String, Long, Long>> {

    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Tuple3<String, Long, Long>> sourceContext) throws Exception {

        Tuple3[] elements = new Tuple3[]{
                Tuple3.of("a", 1L, 1000000050000L),
                Tuple3.of("a", 1L, 1000000054000L),
                Tuple3.of("a", 1L, 1000000079900L),
                Tuple3.of("a", 1L, 1000000115000L),
                Tuple3.of("b", 1L, 1000000100000L),
                Tuple3.of("b", 1L, 1000000108000L)
        };

        int count = 0;
        while (running && count < elements.length) {
            sourceContext.collect(new Tuple3<>((String) elements[count].f0, (Long) elements[count].f1, (Long) elements[count].f2));
            count++;
            Thread.sleep(10000);
        }

    }

    @Override
    public void cancel() {
        running = false;
    }


}
