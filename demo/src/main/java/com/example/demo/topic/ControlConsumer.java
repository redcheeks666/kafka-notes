package com.example.demo.topic;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
/**
 * 控制或关闭消费zl
 *
 *
 *
 * */
public class ControlConsumer {
    private static final String brokerList="ha01:9092";
    private static final String topic="topic-demo";
    private static final String groupid="group.demo1";
    private static final AtomicBoolean isRunning=new AtomicBoolean(true);

    public static Properties initProperties(){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupid);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        return properties;
    }
    /**
     * 控制或关闭消费
     * KafkaConsumer.pause(Collection<TopicPartition> partitions);暂停分区消费
     * KafkaConsumer.resume(Collection<TopicPartition> partitions);恢复分区消费
     * KafkaConsumer.paused();获取被暂停消费的分区
     * */
    public static void main(String[] args) {
        Properties properties = initProperties();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        while (isRunning.get()){
            ConsumerRecords<String, String> records = consumer.poll(1000);

            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
                if (record.value().equals("abc")){
                    consumer.pause(Collections.singletonList(topicPartition));
                }
            }
        }
    }
}
