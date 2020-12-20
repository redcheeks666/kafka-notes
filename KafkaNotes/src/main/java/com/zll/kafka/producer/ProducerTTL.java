package com.zll.kafka.producer;

import com.zll.kafka.config.KafkaProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerTTL {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL_PORT);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //构建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> onTime = new ProducerRecord<>(KafkaProperties.TOPIC, null, System.currentTimeMillis(), "num01", "正常数据");
        ProducerRecord<String, String> late = new ProducerRecord<>(KafkaProperties.TOPIC, null, System.currentTimeMillis() + 20 * 1000, "num01", "迟到数据");

        try {
            producer.send(onTime).get();
            producer.send(late).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        producer.close();
    }
}
