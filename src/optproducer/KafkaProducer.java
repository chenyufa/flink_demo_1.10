package optproducer;

import datasource.MyKafkaSource;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @ date: 2020/10/8 16:36
 * @ author: FatCheney
 * @ description: kafka模拟生产
 * @ version: 1.0.0
 */
public class KafkaProducer {

    public static void main(String[] args){

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.enableCheckpointing(5000);

        MyKafkaSource kafkaSource = new MyKafkaSource();
        DataStreamSource<String> text = env.addSource(kafkaSource).setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // 2.0 配置 KafkaProducer
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>(
                "127.0.0.1:9092", //broker 列表
                "flinkTestTopic",           //topic
                new SimpleStringSchema()); // 消息序列化

        //写入 Kafka 时附加记录的事件时间戳
        producer.setWriteTimestampToKafka(true);
        text.addSink(producer);
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            kafkaSource.cancel();
        }

    }

}
