package com.example.demo.topic;


import com.example.demo.partition.CustomAssignorPartition;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerGroup1 {
    private static final String brokerList="ha01:9092";
    private static final String topic1="tp01";
    private static final String topic2="tp02";
    private static final String topic3="tp03";
    private static final String groupId="group.demo";
    private static final AtomicBoolean isRunning=new AtomicBoolean(true);

    public static Properties initProperties(){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, GroupBroadcast.class.getName());
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = initProperties();

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        kafkaConsumer.subscribe(Arrays.asList(topic1,topic2,topic3), new ConsumerRebalanceListener() {

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.print("再均衡之前该消费者分配的分区");
                for (TopicPartition partition : partitions) {
                    System.out.printf("  %s",partition.topic());
                    System.out.printf("P%s",partition.partition());
                }
                System.out.println();
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.print("C1消费者分配到的分区");
                if (partitions != null) {
                    for (TopicPartition partition : partitions) {
                        System.out.printf("  %s",partition.topic());
                        System.out.printf("P%s",partition.partition());
                    }
                }
                System.out.println();
            }
        });


        Set<TopicPartition> assignment = new HashSet<>();
        do {
            kafkaConsumer.poll(1000);
            assignment = kafkaConsumer.assignment();
        }while (assignment.size()==0);

        System.out.print("再均衡之前该消费者分配的分区");
        for (TopicPartition topicPartition : assignment) {
            System.out.printf("  %s",topicPartition.topic());
            System.out.printf("P%s",topicPartition.partition());

        }
        System.out.println();

        while (isRunning.get()){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);

            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
            }

        }
    }
}
