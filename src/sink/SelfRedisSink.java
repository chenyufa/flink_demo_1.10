package sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

/**
 * @ date: 2020/10/17 11:24
 * @ author: FatCheney
 * @ description: RichSinkFunction 实现
 * @ version: 1.0.0
 */
public class SelfRedisSink extends RichSinkFunction {

    private transient Jedis jedis;

    public void open(Configuration config) {
        jedis = new Jedis("127.0.0.1", 6379);
    }

    public void invoke(Tuple2<String, String> value, Context context) throws Exception {
        if (!jedis.isConnected()) {
            jedis.connect();
        }
        jedis.set(value.f0, value.f1);
    }

    @Override
    public void close() throws Exception {
        jedis.close();
    }

}
