package com.zll.kafka.producer;

import com.zll.kafka.config.KafkaProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * <p>kafka 事务生产者</p>
 */
public class ProducerTransaction {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL_PORT);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //配置transactionId
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"transactionId");


        //构建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //初始化并开启事务
        producer.initTransactions();
        producer.beginTransaction();

        try {

            for (int i=1;i<5;i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(KafkaProperties.TOPIC, 1, "num01", " this is a demo 这是第" + i + "条消息");

                //发送
                producer.send(record);

                //提交事务
                producer.commitTransaction();

            }
        } catch (Exception e) {
            //回滚
            producer.abortTransaction();
        }


        //关闭连接
        producer.close();
    }
}
