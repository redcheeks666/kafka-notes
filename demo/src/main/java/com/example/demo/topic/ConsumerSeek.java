package com.example.demo.topic;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerSeek {
    private static final String brokerList="ha01:9092";
    private static final String topic="topic-demo";
    private static final String groupId="group.demo";
    private static final AtomicBoolean isRunning=new AtomicBoolean(true);

    public static Properties initProperties(){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = initProperties();
        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(properties);

        kafkaConsumer.subscribe(Arrays.asList(topic));

        /**
         * seek（）方法中的参数partition表示分区，而offset参数用来指定从分区的哪个位置开始消费,
         * seek（）方法只能重置消费者分配到的分区的消费位置，而分区的分配是在 poll（）方法的调用过程中实现的,
         * 也就是说，在执行seek（）方法之前需要先执行一次poll（）方法，等到分配到分区之后才可以重置消费位置。
         * */
        Set<TopicPartition> assignment = new HashSet<>();
        do {
            kafkaConsumer.poll(1000);
            assignment = kafkaConsumer.assignment();
        }while (assignment.size()==0);

        //分区起始处消费
        assignment.forEach(x->kafkaConsumer.seek(x,1526));

        while (isRunning.get()){
            ConsumerRecords<String,String> records = kafkaConsumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
            }
        }
        //起始处消费等效
//        assignment.forEach(x->kafkaConsumer.seekToBeginning(Arrays.asList(x)));
//        kafkaConsumer.seekToBeginning(assignment);
        /**
         * 分区末尾处消费
         * */
//        Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(assignment);
//
//        assignment.forEach(x->kafkaConsumer.seek(x,endOffsets.get(x)));
//        while (isRunning.get()){
//            ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
//            for (ConsumerRecord<String, String> record : records) {
//                System.out.println(record.value());
//            }
//        }
    }
}
