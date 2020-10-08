package optproducer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Date;
import java.util.Properties;

/**
 * @ date: 2020/10/8 20:32
 * @ author: FatCheney
 * @ description:
 * @ version: 1.0.0
 */

public class KafkaPartitionProducer extends Thread{

    private static long count = 500;
    private static String topic="topic_test_20201008";
    private static String brokerList="127.0.0.1:9092";

    public static void main(String[] args) {
        KafkaPartitionProducer jproducer = new KafkaPartitionProducer();
        jproducer.start();
    }

    @Override
    public void run() {
        producer();
    }
    private void producer() {
        Properties props = config();
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> record=null;
        System.out.println("kafka生产数据条数："+count);
        for (int i = 1; i <= count; i++) {
            String json = "{\"id\":" + i + ",\"ip\":\"127.0.0." + i + "\",\"date\":" + new Date().toString() + "}";
            String key ="key"+i;
            record = new ProducerRecord<String, String>(topic, key, json);
            producer.send(record, (metadata, e) -> {
                // 使用回调函数
                if (null != e) {
                    e.printStackTrace();
                }
                if (null != metadata) {
                    System.out.println(String.format("offset: %s, partition:%s, topic:%s  timestamp:%s", metadata.offset(), metadata.partition(), metadata.topic(), metadata.timestamp()));
                }
            });
        }
        producer.close();
    }

    private Properties config() {
        Properties props = new Properties();
        props.put("bootstrap.servers",brokerList);
        props.put("acks", "1");
        props.put("retries", 3);
        props.put("batch.size", 100);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 3355443);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        /*自定义分区，两种形式*/
        /*props.put("partitioner.class", PartitionUtil.class.getName());*/
        return props;
    }
}
