package sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

import java.net.InetSocketAddress;
import java.util.HashSet;

/**
 * @ date: 2020/6/17 15:21
 * @ author: FatCheney
 * @ description: TestMain
 * @ version: 1.0.0
 */
public class TestMain {

    public static void main(String[] args) {
        //redisSinkTestByFlink();
        redisSinkTestByBahir();
        //new ElasticsearchSink.Builder<>().build();

    }

    public static void redisSinkTestByBahir(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, String>> stream = env.fromElements("Flink2","Spark2","Storm2").map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                return new Tuple2<>(s, s+"_sink2");
            }
        });
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").setPort(6379).build();
        stream.addSink(new RedisSink<>(conf, new RedisSink02()));

        try {
            env.execute("redis sink02");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void redisSinkTestByFlink(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, String>> stream = env.fromElements("Flink","Spark","Storm").map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                return new Tuple2<>(s, s+"-"+s.length());
            }
        });

        //集群方式配置
        InetSocketAddress host0 = new InetSocketAddress("host1", 6379);
        InetSocketAddress host1 = new InetSocketAddress("host2", 6379);
        InetSocketAddress host2 = new InetSocketAddress("host3", 6379);
        HashSet<InetSocketAddress> set = new HashSet<>();
        set.add(host0);
        set.add(host1);
        set.add(host2);
        //FlinkJedisClusterConfig clusterConfig = new FlinkJedisClusterConfig.Builder().setNodes(set).build();

        //单机方式配置
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").setPort(6379).build();

        stream.addSink(new RedisSink<>(conf, new RedisSink01()));

        try {
            env.execute("redis sink01");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
