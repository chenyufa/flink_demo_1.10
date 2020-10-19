package sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @ date: 2020/10/17 11:45
 * @ author: FatCheney
 * @ description: 使用 Bahir 提供的依赖包实现
 * 开源实现的 Redis Connector 使用非常方便，但是有些功能缺失，例如，无法使用一些 Jedis 中的高级功能如设置过期时间等。
 * RedisMapper接口的类，这个类的主要功能就是将我们自己的输入数据映射到redis的对应的类型
 * @ version: 1.0.0
 */
public class RedisSink02 implements RedisMapper<Tuple2<String, String>> {

    /**
     * 设置 Redis 数据类型
     */
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.SET);
    }

    /**
     * 设置 Key
     */
    @Override
    public String getKeyFromData(Tuple2<String, String> data) {
        return data.f0;
    }

    /**
     * 设置 value
     */
    @Override
    public String getValueFromData(Tuple2<String, String> data) {
        return data.f1;
    }

}
