package util;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;


/**
 * @ date: 2020/10/8 19:57
 * @ author: FatCheney
 * @ description: 消息序列化
 * @ version: 1.0.0
 */

public class CustomDeSerializationSchema implements KafkaDeserializationSchema<ConsumerRecord<String, String>> {

    private static  String encoding = "UTF8";

    @Override
    public boolean isEndOfStream(ConsumerRecord<String, String> nextElement) {
        return false;
    }

    @Override
    public ConsumerRecord<String, String> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
       /* System.out.println("Record--partition::"+record.partition());
        System.out.println("Record--offset::"+record.offset());
        System.out.println("Record--timestamp::"+record.timestamp());
        System.out.println("Record--timestampType::"+record.timestampType());
        System.out.println("Record--checksum::"+record.checksum());
        System.out.println("Record--key::"+record.key());
        System.out.println("Record--value::"+record.value());*/
        return new ConsumerRecord(record.topic(),
                record.partition(),
                record.offset(),
                record.timestamp(),
                record.timestampType(),
                record.checksum(),
                record.serializedKeySize(),
                record.serializedValueSize(),
                /*这里我没有进行空值判断，生产一定记得处理*/
                new  String(record.key(), encoding),
                new  String(record.value(), encoding));
    }

    @Override
    public TypeInformation<ConsumerRecord<String, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<ConsumerRecord<String, String>>(){});
    }


}
