package optconsumer;



import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import util.CustomDeSerializationSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @ date: 2020/10/8 20:03
 * @ author: FatCheney
 * @ description: kafka消费
 * @ version: 1.0.0
 */
public class KafkaConsumer {

    public static void main(String[] args) throws Exception {

        //method1();
        method2();

    }

    public static void method2(){
        /*环境初始化*/
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        /*启用checkpoint，这里我没有对消息体的key value进行判断，即使为空启动了checkpoint，遇到错误也会无限次重启*/
        senv.enableCheckpointing(2000);
        /*topic2不存在话会自动在kafka创建，一个分区 分区名称0*/
        FlinkKafkaConsumer<ConsumerRecord<String, String>> myConsumer=new FlinkKafkaConsumer<ConsumerRecord<String, String>>("topic_test_20201008",new CustomDeSerializationSchema(),getKafkaConfig());

        /*指定消费位点*/
        Map<KafkaTopicPartition, Long> specificStartOffsets = new HashMap<>();
        /*这里从topic3 的0分区的第一条开始消费*/
        specificStartOffsets.put(new KafkaTopicPartition("topic_test_20201008", 0), 0L);
        myConsumer.setStartFromSpecificOffsets(specificStartOffsets);

        DataStream<ConsumerRecord<String, String>> source = senv.addSource(myConsumer);
        DataStream<String> keyValue = source.map(new MapFunction<ConsumerRecord<String, String>, String>() {
            @Override
            public String map(ConsumerRecord<String, String> message) throws Exception {
                return "key"+message.key()+"  value:"+message.value();
            }
        });
        /*打印结果*/
        keyValue.print();
        /*启动执行*/
        try {
            senv.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void method1(){

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.enableCheckpointing(5000);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        // 如果你是0.8版本的Kafka，需要配置
        //properties.setProperty("zookeeper.connect", "localhost:2181");
        //设置消费组
        properties.setProperty("group.id", "group_test");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("flinkTestTopic", new SimpleStringSchema(), properties);

        //自定义的 消息序列化
        //FlinkKafkaConsumer<ConsumerRecord<String, String>> consumer = new FlinkKafkaConsumer<ConsumerRecord<String, String>>("flinkTestTopic", new CustomDeSerializationSchema(), properties);

        //设置从最早的ffset消费
        consumer.setStartFromEarliest();

        //还可以手动指定相应的 topic, partition，offset,然后从指定好的位置开始消费
        //HashMap<KafkaTopicPartition, Long> map = new HashMap<>();
        //map.put(new KafkaTopicPartition("test", 1), 10240L);

        //假如partition有多个，可以指定每个partition的消费位置
        //map.put(new KafkaTopicPartition("test", 2), 10560L);

        //然后各个partition从指定位置消费
        //consumer.setStartFromSpecificOffsets(map);

        env.addSource(consumer).flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                System.out.println(value);
            }
        });

        /*env.addSource(consumer).map(new MapFunction<ConsumerRecord<String, String>, String>() {
            @Override
            public String map(ConsumerRecord<String, String> message) throws Exception {
                System.out.println("message: "+message);
                return "key" + message.key() +"  value:" + message.value();
            }
        });*/

        try {
            env.execute("start consumer...");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Properties getKafkaConfig(){
        Properties  props=new Properties();
        props.setProperty("bootstrap.servers","127.0.0.1:9092");
        props.setProperty("group.id","group_topic_test_20201008");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("auto.offset.reset","earliest");// earliest latest
        return props;
    }

}
