package com.example.demo.topic;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerComitAsync {
    private static final String brokerList="ha01:9092,ha02:9092";
    private static final String topic="topic-demo";
    private static final String groupId="group.demo";
    private static final AtomicBoolean isRunning=new AtomicBoolean(true);

    public static Properties initProperties(){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = initProperties();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Arrays.asList(topic));
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(1000);

//            List<ConsumerRecord<String, String>> ConsumerRecords = records.records(topicPartition);

                for (ConsumerRecord<String, String> consumerRecord : records) {
                    System.out.println(consumerRecord.value());
                 if (consumerRecord.value().equals("abc"))isRunning.set(false);
                }
                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        if (exception == null) {
                            System.out.println(offsets.get(topicPartition).offset());
                        } else {
                            System.out.println("提交位移失败");
                            consumer.wakeup();
                        }
                    }
                });
            }
            consumer.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.commitSync();
        }
    }
}
