package sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @ date: 2020/10/17 11:33
 * @ author: FatCheney
 * @ description: Flink 提供的依赖包，通过实现 RedisMapper 来自定义 Redis Sink
 * RedisMapper接口的类，这个类的主要功能就是将我们自己的输入数据映射到redis的对应的类型
 * @ version: 1.0.0
 */
public class RedisSink01 implements RedisMapper<Tuple2<String, String>> {

    /**
     * 设置 Redis 数据类型
     */
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.SET);
    }

    /**
     * 设置Key
     */
    @Override
    public String getKeyFromData(Tuple2<String, String> data) {
        return data.f0;
    }

    /**
     * 设置value
     */
    @Override
    public String getValueFromData(Tuple2<String, String> data) {
        return data.f1;
    }

}
