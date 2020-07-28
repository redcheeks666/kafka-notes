package com.example.demo.topic;

import com.example.demo.entities.Company;
import com.example.demo.serialization.CompanyDeserializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class CuonsumerKafka {
    public static final String brokerList="ha01:9092,ha02:9092,ha03:9092";
    public static final String topic="topic-demo";
    public static final String groupId="group.demo";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        properties.put("group.id",groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"com.example.demo.serialization.CompanyDeserializer");
        //手动提交位移
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        //构建消费者
//        KafkaConsumer<String, Company> conseumer = new KafkaConsumer<>(properties);
        KafkaConsumer<String, String> conseumer = new KafkaConsumer<>(properties);
        //订阅主题
        conseumer.subscribe(Collections.singletonList(topic));
//        //循环消费
//        while (true){
//            ConsumerRecords<String, Company> consumerRecords = conseumer.poll(Duration.ofMillis(1000));
//            for (ConsumerRecord<String, Company> record : consumerRecords) {
//                System.out.println(record.value());
//                System.out.println(record.topic());
//                System.out.println(record.offset());
//                System.out.println(record.partition());
//                System.out.println(record.timestamp());
//                System.out.println(record.key());
//            }
//        }
        final int minBatchSize=30;
        List<ConsumerRecord> list = new ArrayList<>();
        //循环消费
//        while (true){
//            ConsumerRecords<String, String> consumerRecords = conseumer.poll(Duration.ofMillis(1000));
//            for (ConsumerRecord<String, String> record : consumerRecords) {
//                System.out.println(record.value());
//                System.out.println(record.topic());
//                System.out.println(record.offset());
//                System.out.println(record.partition());
//                System.out.println(record.timestamp());
//                System.out.println(record.key());
//                list.add(record);
//            }
//            if (list.size()>=minBatchSize){
//                //消息数量达到30条提交
//                conseumer.commitSync();
//                //缓冲区清零
//                list.clear();
//            }
//        }
        //分区粒度提交位移
        while (true){
            ConsumerRecords<String, String> records = conseumer.poll(1000);
            conseumer.assignment();
            //当前收到消息的所有TopicPartition{topic,partition}
            Set<TopicPartition> tps = records.partitions();
            //每一个主题的每一个分区
            for (TopicPartition tp : tps) {
                //当前主题分区的所有消息
                List<ConsumerRecord<String, String>> partitionRecords = records.records(tp);

                for (ConsumerRecord<String, String> partitionRecord : partitionRecords) {
                    System.out.println(partitionRecord.value());
                }
                //当前消息集中的最大位移
                long lastConsumerOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                //提交上面的位移
                conseumer.commitSync(Collections.singletonMap(tp,new OffsetAndMetadata(lastConsumerOffset+1)));

            }

//            conseumer.close();

        }


    }
}
