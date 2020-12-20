package com.zll.kafka.producer;

import com.zll.kafka.config.KafkaProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * <p>kafka 幂等生产者</p>
 * <p>保证单分区单回话的消息幂等</p>
 */
public class ProducerIdempotent {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL_PORT);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //配置指定开启幂等即可
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);


        //构建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        try {

            for (int i=1;i<5;i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(KafkaProperties.TOPIC, 1, "num01", " this is a demo 这是第" + i + "条消息");

                //发送
                producer.send(record);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }


        //关闭连接
        producer.close();
    }
}
